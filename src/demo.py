#!/usr/bin/env python3
"""One-command Linux demonstration using synthetic data and a simulated relay."""
import argparse
import hmac
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import xml.etree.ElementTree as ET

import mock_chat_sync as chat
import mock_chat_preview as chat_preview
import mock_auto_receiver as auto_receiver
import mock_git_sync as git_sync
import mock_publications as publications
import mock_secret_sync as secret_sync
import nexus_sync as ns


def syncthing_relay(root: Path, projects: list[Path], shares: list[Path], snapshot: Path,
                    package: Path, secret_package: Path, key: bytes, codex: Path,
                    profile: Path, state: Path, key_path: Path, thread_id: str,
                    *, poll_exchange: bool = False, loopback_mss: bool = False) -> None:
    executable = shutil.which('syncthing')
    if executable is None:
        raise ns.SyncError('Syncthing не найден')
    def free_port():
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]
    ports = []
    for _ in range(64):
        candidate = free_port()
        if candidate not in ports:
            ports.append(candidate)
        if len(ports) == (9 if loopback_mss else 8):
            break
    if len(ports) != (9 if loopback_mss else 8):
        raise ns.SyncError('Не удалось выделить локальные порты')
    homes = [root / f'syncthing-{number}' for number in range(4)]
    identifiers = []
    for home in homes:
        subprocess.run([executable, 'generate', '--home', str(home), '--no-port-probing'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        identifiers.append(subprocess.check_output([executable, 'device-id', '--home', str(home)], timeout=30).decode().strip())
    ignore = Path(__file__).with_name('stignore.example').read_text(encoding='utf-8') + '\n.nexus-mock-git-workspace.json\n'
    for number, home in enumerate(homes):
        (projects[number] / '.stignore').write_text(ignore, encoding='utf-8')
        config = ET.parse(home / 'config.xml')
        document = config.getroot()
        options = document.find('options')
        options.find('listenAddress').text = f'tcp://127.0.0.1:{ports[number]}'
        for name in ('globalAnnounceEnabled', 'localAnnounceEnabled', 'relaysEnabled', 'natEnabled', 'startBrowser'):
            options.find(name).text = 'false'
        options.find('urAccepted').text = '-1'
        options.find('reconnectionIntervalS').text = '2'
        peers = (0, 2, 3) if number == 1 else (1,)
        for peer in peers:
            device = ET.SubElement(document, 'device', {'id': identifiers[peer], 'name': f'mock-{peer}'})
            destination = ports[8] if loopback_mss else ports[1]
            ET.SubElement(device, 'address').text = 'dynamic' if number == 1 else f'tcp://127.0.0.1:{destination}'
            ET.SubElement(device, 'numConnections').text = '1'
        for identity, path, mode in (('projects', projects[number], 'sendonly' if number == 0 else 'sendreceive'),
                                     ('exchange', shares[number], 'sendreceive')):
            folder = ET.SubElement(document, 'folder', {'id': identity, 'path': str(path), 'type': mode,
                'rescanIntervalS': '1', 'fsWatcherEnabled': 'false' if poll_exchange and identity == 'exchange' else 'true'})
            for member in (number, *peers):
                ET.SubElement(folder, 'device', {'id': identifiers[member]})
        config.write(home / 'config.xml', encoding='utf-8', xml_declaration=True)
    relative_snapshot = snapshot.relative_to(shares[0])
    relative_package = package.relative_to(shares[0])
    relative_secret = secret_package.relative_to(shares[0])
    processes = []
    node_processes = {}
    logs = []
    proxy = None
    def start(number):
        log = (root / f'syncthing-{number}.log').open('a', encoding='utf-8')
        logs.append(log)
        process = subprocess.Popen([executable, 'serve', '--home', str(homes[number]), '--no-browser',
            '--no-restart', '--no-upgrade', '--gui-address', f'http://127.0.0.1:{ports[number + 4]}'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        node_processes[number] = process
    def wait_for(number):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise ns.SyncError('Тестовый Syncthing завершился')
            try:
                ns.verify(shares[number] / relative_snapshot, key)
                if ((shares[number] / relative_package).read_bytes() == package.read_bytes()
                        and (shares[number] / relative_secret).read_bytes() == secret_package.read_bytes()
                        and (projects[number] / 'Bot' / 'requirements.txt').read_text() == '# synthetic demo updated\n'
                        and (projects[number] / 'Bot' / 'package.json').read_text() == '{"name":"demo-project","version":"1.0.0"}\n'
                        and any('Изменён манифест: Bot/requirements.txt' in event['changes']
                                for event in ns.project_log(shares[number], key))
                        and any('Начально наблюдался Node.js-пакет: Bot — demo-node@1.0.0' in event['changes']
                                and 'Начально наблюдался Python-пакет: Bot (.venv/lib/python3.12/site-packages) — demo_python-1.0'
                                in event['changes'] for event in ns.project_log(shares[number], key))):
                    return
            except (OSError, ValueError, ns.SyncError):
                pass
            time.sleep(0.25)
        raise ns.SyncError(f'Тестовый узел {number} не получил начальные файлы вовремя')
    try:
        start(1)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if processes[0].poll() is not None:
                raise ns.SyncError('Тестовый промежуточный узел завершился')
            try:
                with socket.create_connection(('127.0.0.1', ports[1]), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            raise ns.SyncError('Тестовый промежуточный узел не открыл порт')
        if loopback_mss:
            from mock_loopback_proxy import LoopbackMSSProxy
            proxy = LoopbackMSSProxy(ports[8], ports[1])
        start(0)
        wait_for(1)
        start(2)
        wait_for(2)
        start(3)
        wait_for(3)
        for number in (1, 2, 3):
            if ((projects[number] / 'Bot' / '.git').exists() or (projects[number] / 'Bot' / '.venv').exists()
                    or (projects[number] / 'Bot' / 'node_modules').exists()
                    or (projects[number] / '.env').exists()
                    or (projects[number] / 'Bot' / '.env').exists()
                    or (projects[number] / 'Bot' / '.npmrc').exists()):
                raise ns.SyncError('Правила исключений не сработали')
        sender_config = root / 'linux' / 'sender-config.json'
        sender_config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(projects[0]), 'codex_home': str(profile), 'shared': str(shares[0]),
            'state_dir': str(state), 'key_file': str(key_path), 'interval_seconds': 10, 'direction': 'send-only',
            'peer_roots': {'mac-mock': str(projects[2]), 'windows-mock': str(projects[3])},
            'discover_secrets': True,
            'git_discovery': True}))
        source_settings = ns.load_settings(sender_config)
        source_config_check = ns.syncthing_source_config_status(source_settings, homes[0] / 'config.xml', 'projects')
        if source_config_check['state'] != 'CONFIG_MATCH' or source_config_check['runtime_verified']:
            raise ns.SyncError('Тестовая конфигурация источника Syncthing не соответствует Send Only')
        safe_env = {name: value for name, value in os.environ.items()
                    if name not in ('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_ADMIN_KEY', 'CHATGPT_ACCESS_TOKEN')}
        safe_env['CODEX_HOME'] = str(profile)
        server = subprocess.Popen([str(codex), 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=safe_env, bufsize=0)
        observer = None
        receivers = []
        publication_watchers = []
        continuous_receivers = []
        chat_preview_watchers = []
        persistent_chat_watchers = []
        persistent_chat_profiles = {}
        live_chat_previews = {}
        replies = queue.Queue()
        def read_lines():
            try:
                for line in server.stdout:
                    replies.put(line)
            finally:
                replies.put(None)
        threading.Thread(target=read_lines, daemon=True).start()
        def rpc(number, method, params):
            server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
            server.stdin.flush()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                try:
                    line = replies.get(timeout=max(0.01, deadline - time.monotonic()))
                except queue.Empty as exc:
                    raise ns.SyncError('Тестовый Codex не ответил') from exc
                if line is None:
                    raise ns.SyncError('Тестовый Codex завершился')
                reply = json.loads(line)
                if reply.get('id') == number:
                    if 'result' not in reply:
                        raise ns.SyncError('Тестовый Codex отклонил запрос')
                    return
            raise ns.SyncError('Тестовый Codex не ответил')
        def next_snapshot(previous, *, expected_title=None):
            deadline = time.monotonic() + 35
            while time.monotonic() < deadline:
                if server.poll() is not None or observer.poll() is not None:
                    raise ns.SyncError('Тестовый отправитель завершился')
                try:
                    latest = ns.latest_authenticated_snapshot(shares[0] / 'nexus-sync' / 'snapshots' / 'linux', 'linux', key)
                    if latest and latest[0].name != previous:
                        if expected_title is not None:
                            titles = ns._parse_title_index(ns._snapshot_title_data(latest[0], latest[1], key))
                            if not any(row['id'] == thread_id and row['thread_name'] == expected_title for row in titles):
                                time.sleep(0.25)
                                continue
                        return latest
                except (OSError, ValueError, ns.SyncError):
                    pass
                time.sleep(0.25)
            raise ns.SyncError('Новый тестовый снимок не создан')
        def next_packages(previous):
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if server.poll() is not None or observer.poll() is not None:
                    raise ns.SyncError('Тестовый отправитель завершился')
                try:
                    git_markers = list(state.glob('last-git-linux-*.json'))
                    markers = [state / 'last-secrets-linux.json', *git_markers]
                    if len(git_markers) != 1:
                        raise ValueError('Ожидание Git-пакета')
                    names = []
                    for path in markers:
                        item = json.loads(path.read_text(encoding='utf-8'))
                        tag = item.get('authentication')
                        if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key)):
                            raise ValueError('Неподтверждённая отметка')
                        name = item.get('package')
                        if not isinstance(name, str) or not name.endswith('.nxs') or Path(name).name != name:
                            raise ValueError('Некорректное имя пакета')
                        names.append(name)
                    if previous is None or all(name != old for name, old in zip(names, previous)):
                        result = [shares[0] / 'nexus-sync' / 'secrets' / 'linux' / name for name in names]
                        if all(path.is_file() for path in result):
                            return names, result
                except (OSError, ValueError, TypeError, ns.SyncError):
                    pass
                time.sleep(0.25)
            raise ns.SyncError('Новый тестовый пакет не создан')
        try:
            rpc(1, 'initialize', {'clientInfo': {'name': 'nexus_demo', 'version': '0.1'},
                                  'capabilities': {'experimentalApi': True}})
            server.stdin.write(b'{"method":"initialized","params":{}}\n')
            server.stdin.flush()
            rpc(2, 'thread/resume', {'threadId': thread_id, 'cwd': str(projects[0] / 'Bot')})
            observer = subprocess.Popen([sys.executable, str(Path(ns.__file__)), 'run', '--config', str(sender_config)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            first, first_manifest = next_snapshot(None)
            previous_packages, _ = next_packages(None)
            for number in (2, 3):
                preview_root = projects[number].parent / 'live-chat-previews'
                chat_preview.init(preview_root, shares[number], projects[number], 'linux', str(projects[0]), key)
                live_chat_previews[number] = preview_root
                chat_preview_watchers.append(subprocess.Popen([sys.executable, str(Path(chat_preview.__file__)),
                    'watch', '--root', str(preview_root), '--key-file', str(key_path), '--codex-executable', str(codex),
                    '--interval', '0.5'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            def wait_chat_preview(number, expected, title):
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if server.poll() is not None or observer.poll() is not None or any(
                            process.poll() is not None for process in chat_preview_watchers):
                        raise ns.SyncError('Наблюдатель моковых чатов завершился')
                    try:
                        root = live_chat_previews[number]
                        marker = chat_preview._read_signed(root / chat_preview.CHECKPOINT, key,
                            'nexus-mock-chat-preview-checkpoint-v1')
                        prepared = root / marker['profile']
                        names = ns._parse_title_index(ns._read_title_index(prepared))
                        if (marker.get('snapshot') == expected and
                                any(row['id'] == thread_id and row['thread_name'] == title for row in names)):
                            return
                    except (OSError, ValueError, TypeError, KeyError, ns.SyncError):
                        pass
                    time.sleep(0.25)
                raise ns.SyncError('Моковый чат не подготовлен автоматически')
            for number in (2, 3):
                wait_chat_preview(number, first.name, 'Synthetic demo chat')
                mock_profile = projects[number].parent / 'live-chat-profile'
                chat.init_mock_profile(mock_profile, key, shares[number])
                persistent_chat_profiles[number] = mock_profile
                watch_command = [sys.executable, str(Path(chat.__file__)),
                    'watch-all', '--profile', str(mock_profile), '--shared', str(shares[number]),
                    '--key-file', str(key_path), '--source', 'linux', '--source-root', str(projects[0]),
                    '--target-root', str(projects[number]), '--codex-executable', str(codex), '--interval', '0.5']
                if number == 3:
                    watch_command.extend(['--also-source', 'mac', str(projects[2])])
                if number == 2:
                    watch_command.extend(['--also-source', 'windows', str(projects[3]), '--allow-mock-settings-rebase'])
                persistent_chat_watchers.append(subprocess.Popen(watch_command,
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            def wait_persistent_chat(identity):
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if server.poll() is not None or observer.poll() is not None or any(
                            process.poll() is not None for process in persistent_chat_watchers):
                        raise ns.SyncError('Наблюдатель постоянного мокового профиля завершился')
                    try:
                        ready = []
                        for target in persistent_chat_profiles.values():
                            marker = json.loads((target / chat.SYNC_MARKER / ('linux-' + identity + '.json')).read_text())
                            tag = marker.get('authentication')
                            ready.append(isinstance(tag, str) and hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                                         and identity in ns._chat_catalog(target))
                        if len(ready) == 2 and all(ready):
                            return
                    except (OSError, ValueError, TypeError, ns.SyncError):
                        pass
                    time.sleep(0.25)
                raise ns.SyncError('Чат не принят обоими постоянными моковыми профилями')
            wait_persistent_chat(thread_id)
            later_id = '019b0000-0000-7000-8000-000000000099'
            later_rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': later_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(projects[0] / 'Bot'),
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic later chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'synthetic later chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{later_id}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in later_rows), encoding='utf-8')
            wait_persistent_chat(later_id)
            mac_source = projects[2].parent / 'synthetic-mac-sender'
            (mac_source / 'sessions').mkdir(parents=True)
            mac_id = '019b0000-0000-7000-8000-000000000101'
            mac_rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': mac_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(projects[2] / 'Bot'),
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic reverse chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'synthetic reverse chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            (mac_source / 'sessions' / f'rollout-2026-01-01T00-00-00-{mac_id}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in mac_rows), encoding='utf-8')
            mac_snapshot = ns.snapshot(mac_source, shares[2], 'mac', key)
            mac_relative = mac_snapshot.relative_to(shares[2])
            mac_tag = ns.verify(mac_snapshot, key)['authentication']
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if any(process.poll() is not None for process in persistent_chat_watchers):
                    raise ns.SyncError('Наблюдатель обратного мокового чата завершился')
                try:
                    local = persistent_chat_profiles[3]
                    receipt = json.loads((local / chat.SYNC_MARKER / ('mac-' + mac_id + '.json')).read_text())
                    tag = receipt.get('authentication')
                    catalog = ns._chat_catalog(local)
                    if (all(ns.verify(shares[number] / mac_relative, key)['authentication'] == mac_tag
                            for number in (0, 1, 3))
                            and isinstance(tag, str) and hmac.compare_digest(tag, ns._manifest_tag(receipt, key))
                            and receipt.get('snapshot') == mac_snapshot.name
                            and catalog[mac_id][0]['cwd'] == str(projects[3] / 'Bot')):
                        break
                except (OSError, ValueError, TypeError, KeyError, ns.SyncError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Обратный моковый чат не доставлен')
            windows_source = projects[3].parent / 'synthetic-windows-sender'
            (windows_source / 'sessions').mkdir(parents=True)
            windows_id = '019b0000-0000-7000-8000-000000000102'
            windows_rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': windows_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(projects[3] / 'Bot'),
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic Windows first'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'synthetic Windows first', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            windows_chat = windows_source / 'sessions' / f'rollout-2026-01-01T00-00-00-{windows_id}.jsonl'
            windows_chat.write_text(''.join(json.dumps(row) + '\n' for row in windows_rows), encoding='utf-8')
            windows_state = projects[3].parent / 'windows-source-state'
            windows_state.mkdir()
            windows_config = projects[3].parent / 'windows-sender-config.json'
            windows_config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'windows',
                'nexus': str(projects[3]), 'codex_home': str(windows_source), 'shared': str(shares[3]),
                'state_dir': str(windows_state), 'key_file': str(key_path), 'direction': 'send-receive',
                'interval_seconds': 10}), encoding='utf-8')
            windows_observer = subprocess.Popen([sys.executable, str(Path(ns.__file__)), 'run', '--config', str(windows_config)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            def wait_windows_chat(previous_id, expected_text):
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if windows_observer.poll() is not None or any(p.poll() is not None for p in persistent_chat_watchers):
                        raise ns.SyncError('Моковый наблюдатель Windows завершился')
                    try:
                        latest = ns.latest_authenticated_snapshot(shares[3] / 'nexus-sync' / 'snapshots' / 'windows', 'windows', key)
                        if latest is None or latest[0].name == previous_id:
                            time.sleep(0.25)
                            continue
                        relative = latest[0].relative_to(shares[3])
                        receipt_path = persistent_chat_profiles[2] / chat.SYNC_MARKER / ('windows-' + windows_id + '.json')
                        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
                        tag = receipt.get('authentication')
                        local = ns._chat_catalog(persistent_chat_profiles[2]).get(windows_id, [])
                        if (all(ns.verify(shares[number] / relative, key)['authentication'] == latest[1]['authentication']
                                for number in (0, 1, 2))
                                and isinstance(tag, str) and hmac.compare_digest(tag, ns._manifest_tag(receipt, key))
                                and receipt.get('snapshot') == latest[0].name and len(local) == 1
                                and local[0]['cwd'] == str(projects[2] / 'Bot')
                                and expected_text in local[0]['path'].read_text(encoding='utf-8')):
                            return latest[0].name
                    except (OSError, ValueError, TypeError, ns.SyncError):
                        pass
                    time.sleep(0.25)
                raise ns.SyncError('Моковое обновление Windows не дошло до Mac')
            try:
                previous_windows = wait_windows_chat(None, 'synthetic Windows first')
                continuation = [
                    {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload': {
                        'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic Windows later'}]}},
                    {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload': {
                        'type': 'user_message', 'message': 'synthetic Windows later', 'images': [], 'local_images': [], 'text_elements': []}},
                ]
                with windows_chat.open('a', encoding='utf-8') as out:
                    out.writelines(json.dumps(row) + '\n' for row in continuation)
                wait_windows_chat(previous_windows, 'synthetic Windows later')
            finally:
                if windows_observer.poll() is None:
                    windows_observer.send_signal(signal.SIGINT)
                    windows_observer.wait(timeout=20)
            mac_project = projects[2] / 'MacProject'
            mac_project.mkdir()
            (mac_project / 'source.txt').write_text('synthetic reverse project\n', encoding='utf-8')
            (mac_project / '.env').write_text('DEMO_MAC=synthetic-only\n', encoding='utf-8')
            mac_git_env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
            mac_git_env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_TERMINAL_PROMPT': '0'})
            for command in (('init', '-q'), ('add', 'source.txt'), ('commit', '-qm', 'synthetic reverse project')):
                subprocess.run(['git', '-C', str(mac_project), '-c', 'core.hooksPath=/dev/null',
                    '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
                    *command], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, env=mac_git_env, timeout=20)
            mac_head = subprocess.check_output(['git', '-C', str(mac_project), 'rev-parse', 'HEAD']).strip()
            mac_state = projects[2].parent / 'mac-source-state'
            mac_state.mkdir()
            mac_raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac',
                'nexus': str(projects[2]), 'codex_home': str(mac_source), 'shared': str(shares[2]),
                'state_dir': str(mac_state), 'key_file': str(key_path), 'direction': 'send-receive',
                'discover_secrets': True, 'git_discovery': True, 'interval_seconds': 10}
            mac_config = ns.validate_settings(mac_raw)
            _, mac_notes = ns.supervisor_once(mac_config, None)
            if any('отложен' in note for note in mac_notes):
                raise ns.SyncError('Обратный моковый цикл отправки отложен')
            if publications.publish_all_git_markers(shares[2], mac_state, 'mac', key)['invalid']:
                raise ns.SyncError('Обратная моковая Git-публикация не подтверждена')
            publications.publish_all_secret_marker(shares[2], mac_state, 'mac', key)
            mac_git = publications.select(shares[2], 'mac', 'git', 'MacProject', key)
            mac_secret = publications.select(shares[2], 'mac', 'secret', 'MacProject/.env', key)
            if not mac_git or not mac_secret or (mac_project / '.env').read_bytes() in mac_secret[0].read_bytes():
                raise ns.SyncError('Обратные моковые пакеты не подготовлены')
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                try:
                    project_arrived = (projects[3] / 'MacProject' / 'source.txt').read_text(encoding='utf-8') == 'synthetic reverse project\n'
                    packages_arrived = all((selected := publications.select(shares[number], 'mac', kind, name, key))
                                           and selected[0].name == expected
                                           for number in (1, 3)
                                           for kind, name, expected in (('git', 'MacProject', mac_git[0].name),
                                                                       ('secret', 'MacProject/.env', mac_secret[0].name),
                                                                       ('secret-set', 'all', mac_secret[0].name)))
                    if project_arrived and packages_arrived:
                        break
                except (OSError, ValueError, ns.SyncError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Обратный моковый проект не дошёл до получателя')
            if (projects[0] / 'MacProject').exists() or (projects[1] / 'MacProject' / '.env').exists():
                raise ns.SyncError('Моковый Send Only или исключение секрета не сработали')
            mac_git_result = auto_receiver.receive_discovered_git(projects[3], shares[3], key, 'mac')
            mac_secret_result = auto_receiver.receive_discovered_secrets(projects[3], shares[3], key, 'mac', interval=0.1)
            restored_mac = projects[3] / 'MacProject'
            restored_head = subprocess.check_output(['git', '-C', str(restored_mac), 'rev-parse', 'HEAD']).strip()
            if (mac_git_result['state'] != 'APPLIED_AVAILABLE' or mac_secret_result['state'] != 'APPLIED_AVAILABLE'
                    or restored_head != mac_head or (restored_mac / '.env').read_bytes() != (mac_project / '.env').read_bytes()):
                raise ns.SyncError('Обратный моковый Git или секрет не восстановлен')
            mac_config_path = projects[2].parent / 'mac-sender-config.json'
            mac_config_path.write_text(json.dumps(mac_raw), encoding='utf-8')
            mac_watchers = []
            try:
                mac_watchers.append(subprocess.Popen([sys.executable, str(Path(ns.__file__)), 'run',
                    '--config', str(mac_config_path)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL))
                for operation in ('watch-all-git', 'watch-all-secrets'):
                    mac_watchers.append(subprocess.Popen([sys.executable, str(Path(publications.__file__)), operation,
                        '--shared', str(shares[2]), '--state', str(mac_state), '--key-file', str(key_path),
                        '--source', 'mac', '--interval', '0.2'], stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                for mode in (('--discover-git',), ('--secrets-only', '--discover-secrets')):
                    mac_watchers.append(subprocess.Popen([sys.executable, str(Path(auto_receiver.__file__)),
                        '--root', str(projects[3]), '--shared', str(shares[3]), '--key-file', str(key_path),
                        '--source', 'mac', *mode, '--watch', '--interval', '0.5'], stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                old_index = (restored_mac / '.git' / 'index').read_bytes()
                (mac_project / '.env').write_text('DEMO_MAC=synthetic-updated\n', encoding='utf-8')
                subprocess.run(['git', '-C', str(mac_project), '-c', 'core.hooksPath=/dev/null',
                    '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
                    'commit', '--allow-empty', '-qm', 'synthetic reverse update'], check=True,
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=mac_git_env, timeout=20)
                updated_mac_head = subprocess.check_output(['git', '-C', str(mac_project), 'rev-parse', 'HEAD']).strip()
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if any(process.poll() is not None for process in mac_watchers):
                        raise ns.SyncError('Обратный моковый наблюдатель завершился')
                    try:
                        latest_git = publications.select(shares[3], 'mac', 'git', 'MacProject', key)
                        latest_secret = publications.select(shares[3], 'mac', 'secret', 'MacProject/.env', key)
                        latest_set = publications.select(shares[3], 'mac', 'secret-set', 'all', key)
                        if (latest_git and latest_secret and latest_set and latest_git[1]['ordinal'] >= 2
                                and latest_secret[1]['ordinal'] >= 2 and latest_set[0].name == latest_secret[0].name):
                            head = subprocess.check_output(['git', '-C', str(restored_mac), 'rev-parse', 'HEAD']).strip()
                            refs = subprocess.check_output(['git', '-C', str(restored_mac), 'for-each-ref',
                                '--format=%(objectname)', 'refs/nexus-sync/mac']).splitlines()
                            if ((restored_mac / '.env').read_bytes() == (mac_project / '.env').read_bytes()
                                    and head == mac_head and (restored_mac / '.git' / 'index').read_bytes() == old_index
                                    and updated_mac_head in refs):
                                break
                    except (OSError, ValueError, ns.SyncError, subprocess.SubprocessError):
                        pass
                    time.sleep(0.25)
                else:
                    raise ns.SyncError('Обратные моковые изменения не получены автоматически')
            finally:
                for process in mac_watchers:
                    if process.poll() is None:
                        process.send_signal(signal.SIGINT)
                        process.wait(timeout=20)
            publication_watchers.append(subprocess.Popen([sys.executable, str(Path(publications.__file__)),
                'watch-all-git', '--shared', str(shares[0]), '--state', str(state), '--key-file', str(key_path),
                '--source', 'linux', '--interval', '0.2'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            publication_watchers.append(subprocess.Popen([sys.executable, str(Path(publications.__file__)),
                'watch-all-secrets', '--shared', str(shares[0]), '--state', str(state), '--key-file', str(key_path),
                '--source', 'linux', '--interval', '0.2'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            def wait_publication(number, kind, name, expected, ordinal):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    if any(process.poll() is not None for process in publication_watchers):
                        raise ns.SyncError('Наблюдатель моковых публикаций завершился')
                    try:
                        result = publications.select(shares[number], 'linux', kind, name, key)
                        if result and result[0].name == expected and result[1]['ordinal'] == ordinal:
                            return
                    except (OSError, ValueError, ns.SyncError):
                        pass
                    time.sleep(0.2)
                raise ns.SyncError('Моковая публикация не дошла до узла')
            wait_publication(0, 'secret', 'Bot/.env', previous_packages[0], 1)
            wait_publication(0, 'secret', 'Bot/.npmrc', previous_packages[0], 1)
            wait_publication(0, 'secret', '.env', previous_packages[0], 1)
            wait_publication(0, 'git', 'Bot', previous_packages[1], 1)
            (projects[0] / 'Bot' / '.env').write_text('DEMO_TOKEN=synthetic-updated\n', encoding='utf-8')
            git_env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
            git_env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_TERMINAL_PROMPT': '0'})
            subprocess.run(['git', '-C', str(projects[0] / 'Bot'), '-c', 'core.hooksPath=/dev/null',
                '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
                'commit', '--allow-empty', '-qm', 'synthetic update'], check=True, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=git_env, timeout=20)
            rpc(3, 'thread/name/set', {'threadId': thread_id, 'name': 'Synthetic demo renamed while open'})
            newest, manifest = next_snapshot(first.name, expected_title='Synthetic demo renamed while open')
            _, (new_secret, new_git) = next_packages(previous_packages)
            wait_publication(0, 'secret', 'Bot/.env', new_secret.name, 2)
            wait_publication(0, 'secret', 'Bot/.npmrc', new_secret.name, 2)
            wait_publication(0, 'secret', '.env', new_secret.name, 2)
            wait_publication(0, 'git', 'Bot', new_git.name, 2)
            if (projects[0] / 'Bot' / '.env').read_bytes() in new_secret.read_bytes():
                raise ns.SyncError('Пакет мокового секрета не защищён')
            for number in (2, 3):
                # The scenario deliberately waits for the new publications before starting
                # selection; local availability is not proof of the latest remote state.
                wait_publication(number, 'secret', 'Bot/.env', new_secret.name, 2)
                wait_publication(number, 'secret', 'Bot/.npmrc', new_secret.name, 2)
                wait_publication(number, 'git', 'Bot', new_git.name, 2)
                receivers.append(subprocess.Popen([sys.executable, str(Path(auto_receiver.__file__)),
                    '--root', str(projects[number]), '--shared', str(shares[number]), '--key-file', str(key_path),
                    '--source', 'linux',
                    '--project', 'Bot', '--secret-path', 'Bot/.env', '--secret-path', 'Bot/.npmrc', '--timeout', '60'],
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL))
            titles = ns._parse_title_index(ns._snapshot_title_data(newest, manifest, key))
            if not any(row['id'] == thread_id and row['thread_name'] == 'Synthetic demo renamed while open' for row in titles):
                raise ns.SyncError('Новое тестовое название не попало в снимок')
            for published, metadata in ((first, first_manifest), (newest, manifest)):
                relative = published.relative_to(shares[0])
                for number in (1, 2, 3):
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline:
                        if server.poll() is not None or observer.poll() is not None:
                            raise ns.SyncError('Тестовый отправитель завершился до доставки')
                        try:
                            if ns.verify(shares[number] / relative, key)['authentication'] == metadata['authentication']:
                                break
                        except (OSError, ValueError, ns.SyncError):
                            pass
                        time.sleep(0.25)
                    else:
                        raise ns.SyncError(f'Новый тестовый снимок не дошёл до узла {number}')
            for number in (2, 3):
                wait_chat_preview(number, newest.name, 'Synthetic demo renamed while open')
            def wait_receipts(expected):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    if server.poll() is not None or observer.poll() is not None:
                        raise ns.SyncError('Тестовый отправитель завершился')
                    try:
                        latest = ns.latest_authenticated_snapshot(shares[0] / 'nexus-sync' / 'snapshots' / 'linux', 'linux', key)
                        status = ns.configured_status(source_settings, key)
                        if (latest and latest[0].name == newest.name and status.get('confirmed_receivers') == expected
                                and status.get('expected_receivers') == 2):
                            return
                    except (OSError, ValueError, ns.SyncError):
                        pass
                    time.sleep(0.25)
                raise ns.SyncError('Подтверждения моковых получателей не совпали')
            wait_receipts(0)
            ns.receive_once(shares[2], 'mac-mock', key)
            wait_receipts(1)
            ns.receive_once(shares[3], 'windows-mock', key)
            wait_receipts(2)
            for published in (new_git, new_secret):
                relative = published.relative_to(shares[0])
                for number in (1, 2, 3):
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline:
                        if server.poll() is not None or observer.poll() is not None:
                            raise ns.SyncError('Тестовый отправитель завершился до доставки')
                        try:
                            if (shares[number] / relative).read_bytes() == published.read_bytes():
                                break
                        except OSError:
                            pass
                        time.sleep(0.25)
                    else:
                        raise ns.SyncError(f'Новый тестовый пакет не дошёл до узла {number}')
            for receiver_process in receivers:
                output, _ = receiver_process.communicate(timeout=70)
                if receiver_process.returncode or json.loads(output).get('state') != 'APPLIED_AVAILABLE':
                    raise ns.SyncError('Моковый приёмник не завершил установку')
            for number in (2, 3):
                wait_publication(number, 'secret', '.env', new_secret.name, 2)
                wait_publication(number, 'secret-set', 'all', new_secret.name, 2)
                discovered = auto_receiver.receive_discovered_secrets(projects[number], shares[number], key,
                                                                       'linux', interval=0.1)
                if (discovered['state'] != 'APPLIED_AVAILABLE' or discovered['secret_ready_count'] != 3
                        or (projects[number] / '.env').read_bytes() != (projects[0] / '.env').read_bytes()):
                    raise ns.SyncError('Корневой моковый секрет не получен автоматически')
            for number in (1, 2, 3):
                wait_publication(number, 'secret', 'Bot/.env', new_secret.name, 2)
                wait_publication(number, 'secret', 'Bot/.npmrc', new_secret.name, 2)
                wait_publication(number, 'git', 'Bot', new_git.name, 2)
            for number in (2, 3):
                continuous_receivers.append(subprocess.Popen([sys.executable, str(Path(auto_receiver.__file__)),
                    '--root', str(projects[number]), '--shared', str(shares[number]), '--key-file', str(key_path),
                    '--source', 'linux', '--project', 'Bot', '--secret-path', 'Bot/.env',
                    '--secret-path', 'Bot/.npmrc', '--watch',
                    '--interval', '0.5'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            (projects[0] / 'Bot' / '.npmrc').write_text('DEMO_NPM=synthetic-watch-update\n', encoding='utf-8')
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if any(process.poll() is not None for process in continuous_receivers) or observer.poll() is not None:
                    raise ns.SyncError('Тестовый наблюдатель завершился')
                try:
                    selected = publications.select(shares[0], 'linux', 'secret', 'Bot/.env', key)
                    if (selected and selected[1]['ordinal'] == 3
                            and all((projects[number] / 'Bot' / name).read_bytes() ==
                                    (projects[0] / 'Bot' / name).read_bytes()
                                    for number in (2, 3) for name in ('.env', '.npmrc'))):
                        new_secret = selected[0]
                        break
                except (OSError, ValueError, ns.SyncError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Наблюдатели не обновили моковый секрет')
            previous_git = {}
            for number in (2, 3):
                project = projects[number] / 'Bot'
                previous_git[number] = (subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD']),
                                        (project / '.git' / 'index').read_bytes())
            subprocess.run(['git', '-C', str(projects[0] / 'Bot'), '-c', 'core.hooksPath=/dev/null',
                '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
                'commit', '--allow-empty', '-qm', 'synthetic watched Git update'], check=True,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=git_env, timeout=20)
            new_head = subprocess.check_output(['git', '-C', str(projects[0] / 'Bot'), 'rev-parse', 'HEAD']).strip()
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if any(process.poll() is not None for process in continuous_receivers) or observer.poll() is not None:
                    raise ns.SyncError('Тестовый наблюдатель завершился')
                try:
                    selected = publications.select(shares[0], 'linux', 'git', 'Bot', key)
                    if selected and selected[1]['ordinal'] == 3:
                        ready = []
                        for number in (2, 3):
                            project = projects[number] / 'Bot'
                            refs = subprocess.check_output(['git', '-C', str(project), 'for-each-ref',
                                '--format=%(objectname)', 'refs/nexus-sync/linux']).splitlines()
                            head = subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD'])
                            index = (project / '.git' / 'index').read_bytes()
                            if (head, index) != previous_git[number]:
                                raise ns.SyncError('Моковый Git изменил HEAD или индекс')
                            received_package = shares[number] / selected[0].relative_to(shares[0])
                            receipt = auto_receiver._git_receipt(projects[number], key, 'Bot')
                            ready.append(new_head in refs and auto_receiver._git_already_installed(
                                projects[number], key, received_package, 'Bot', receipt))
                        if all(ready):
                            new_git = selected[0]
                            break
                except (OSError, ValueError, ns.SyncError, subprocess.SubprocessError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Наблюдатели не подготовили обновление мокового Git')
            for process in continuous_receivers:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=20)
            continuous_receivers.clear()
            for number in (2, 3):
                continuous_receivers.append(subprocess.Popen([sys.executable, str(Path(auto_receiver.__file__)),
                    '--root', str(projects[number]), '--shared', str(shares[number]), '--key-file', str(key_path),
                    '--source', 'linux', '--discover-git', '--watch', '--interval', '0.5'],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            later = projects[0] / 'Later'
            later.mkdir()
            (later / 'source.txt').write_text('synthetic late project\n', encoding='utf-8')
            for command in (('init', '-q'), ('add', 'source.txt'), ('commit', '-qm', 'synthetic late project')):
                subprocess.run(['git', '-C', str(later), '-c', 'core.hooksPath=/dev/null',
                    '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
                    *command], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, env=git_env, timeout=20)
            late_head = subprocess.check_output(['git', '-C', str(later), 'rev-parse', 'HEAD']).strip()
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if observer.poll() is not None or any(process.poll() is not None for process in continuous_receivers):
                    raise ns.SyncError('Наблюдатель нового мокового проекта завершился')
                try:
                    ready = []
                    for number in (2, 3):
                        project = projects[number] / 'Later'
                        if not (project / '.git').is_dir():
                            ready.append(False)
                            continue
                        head = subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD'],
                                                       stderr=subprocess.DEVNULL).strip()
                        ready.append(head == late_head and
                                     (project / 'source.txt').read_text(encoding='utf-8') == 'synthetic late project\n')
                    if all(ready):
                        break
                except (OSError, ValueError, subprocess.SubprocessError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Новый моковый Git-проект не дошёл до обоих получателей')
        finally:
            for process in persistent_chat_watchers:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=20)
            for process in chat_preview_watchers:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=20)
            for process in continuous_receivers:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=20)
            for process in publication_watchers:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=20)
            for receiver_process in receivers:
                if receiver_process.poll() is None:
                    receiver_process.send_signal(signal.SIGINT)
                    receiver_process.wait(timeout=20)
                if receiver_process.stdout is not None:
                    receiver_process.stdout.close()
            if observer is not None and observer.poll() is None:
                observer.send_signal(signal.SIGINT)
                observer.wait(timeout=20)
            server.stdin.close()
            server.wait(timeout=20)
            server.stdout.close()
        offline = node_processes[3]
        offline.terminate()
        offline.wait(timeout=20)
        processes.remove(offline)
        offline_file = projects[0] / 'offline-catchup.txt'
        offline_file.write_text('synthetic change while receiver offline\n', encoding='utf-8')
        offline_project = projects[0] / 'OfflineProject'
        offline_project.mkdir()
        (offline_project / 'requirements.txt').write_text('offline-dep==2.0\n', encoding='utf-8')
        offline_wheel = offline_project / '.venv' / 'lib' / 'python3.12' / 'site-packages' / 'offline_dep-2.0.dist-info'
        offline_wheel.mkdir(parents=True)
        (offline_wheel / 'WHEEL').write_text('Wheel-Version: 1.0\nTag: cp312-cp312-manylinux_2_17_x86_64\n', encoding='utf-8')
        event_dir = shares[0] / 'nexus-sync' / 'project-events' / 'linux'
        previous_events = set(event_dir.glob('*.nxe'))
        changes = ns.record_projects(projects[0], shares[0], state, 'linux', key=key)
        if 'Новый проект: OfflineProject' not in changes:
            raise ns.SyncError('Моковое изменение зависимостей не обнаружено')
        new_events = set(event_dir.glob('*.nxe')) - previous_events
        if len(new_events) != 1:
            raise ns.SyncError('Моковое событие зависимостей не создано')
        offline_event = new_events.pop()
        offline_event_relative = offline_event.relative_to(shares[0])
        encrypted_event = offline_event.read_bytes()
        if b'offline_dep' in encrypted_event:
            raise ns.SyncError('Моковое событие зависимостей не защищено')
        offline_chat_id = '019b0000-0000-7000-8000-000000000100'
        offline_rows = [
            {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                'id': offline_chat_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(projects[0] / 'Bot'),
                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
            {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic offline chat'}]}},
            {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                'type': 'user_message', 'message': 'synthetic offline chat', 'images': [], 'local_images': [], 'text_elements': []}},
        ]
        (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{offline_chat_id}.jsonl').write_text(
            ''.join(json.dumps(row) + '\n' for row in offline_rows), encoding='utf-8')
        offline_snapshot = ns.snapshot(profile, shares[0], 'linux', key)
        offline_relative = offline_snapshot.relative_to(shares[0])
        offline_tag = ns.verify(offline_snapshot, key)['authentication']
        offline_git_before = {}
        for number in (2, 3):
            project = projects[number] / 'Bot'
            offline_git_before[number] = (subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD']),
                                          (project / '.git' / 'index').read_bytes())
        subprocess.run(['git', '-C', str(projects[0] / 'Bot'), '-c', 'core.hooksPath=/dev/null',
            '-c', 'commit.gpgsign=false', '-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid',
            'commit', '--allow-empty', '-qm', 'synthetic offline Git update'], check=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=git_env, timeout=20)
        offline_head = subprocess.check_output(['git', '-C', str(projects[0] / 'Bot'), 'rev-parse', 'HEAD']).strip()
        git_notes = ns.git_cycle(source_settings)
        if any('отложен' in note for note in git_notes):
            raise ns.SyncError('Моковый Git-пакет отложен')
        published_git = publications.publish_all_git_markers(shares[0], state, 'linux', key)
        if published_git['invalid'] or not published_git['published']:
            raise ns.SyncError('Моковый Git-пакет не опубликован')
        selected_git = publications.select(shares[0], 'linux', 'git', 'Bot', key)
        if not selected_git or selected_git[0].name == new_git.name:
            raise ns.SyncError('Новый моковый Git-пакет не найден')
        offline_git = selected_git[0]
        offline_git_relative = offline_git.relative_to(shares[0])
        encrypted_git = offline_git.read_bytes()
        (projects[0] / '.env').write_text('DEMO_ROOT=synthetic-offline-update\n', encoding='utf-8')
        if not ns.publish_secrets_once(source_settings, key):
            raise ns.SyncError('Изменённый моковый секрет не опубликован')
        publications.publish_all_secret_marker(shares[0], state, 'linux', key)
        secret_marker = json.loads((state / 'last-secrets-linux.json').read_text(encoding='utf-8'))
        secret_tag = secret_marker.get('authentication')
        if not isinstance(secret_tag, str) or not hmac.compare_digest(secret_tag, ns._manifest_tag(secret_marker, key)):
            raise ns.SyncError('Отметка мокового секрета не подтверждена')
        offline_secret = shares[0] / 'nexus-sync' / 'secrets' / 'linux' / secret_marker['package']
        offline_secret_relative = offline_secret.relative_to(shares[0])
        encrypted_secret = offline_secret.read_bytes()
        if (projects[0] / '.env').read_bytes() in encrypted_secret:
            raise ns.SyncError('Пакет мокового секрета не защищён')
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise ns.SyncError('Работающий тестовый узел Syncthing завершился')
            try:
                if (all((projects[number] / offline_file.name).read_text(encoding='utf-8')
                        == 'synthetic change while receiver offline\n' for number in (1, 2))
                        and ns.verify(shares[1] / offline_relative, key)['authentication'] == offline_tag
                        and (shares[1] / offline_event_relative).read_bytes() == encrypted_event
                        and (shares[1] / offline_secret_relative).read_bytes() == encrypted_secret
                        and (shares[1] / offline_git_relative).read_bytes() == encrypted_git):
                    break
            except (OSError, ValueError, ns.SyncError):
                pass
            time.sleep(0.25)
        else:
            raise ns.SyncError('Изменение не дошло до промежуточного узла при выключенном получателе')
        if (projects[3] / offline_file.name).exists():
            raise ns.SyncError('Выключенный тестовый получатель неожиданно получил файл')
        if (shares[3] / offline_relative).exists():
            raise ns.SyncError('Выключенный тестовый получатель неожиданно получил снимок')
        if (shares[3] / offline_secret_relative).exists() or (projects[1] / '.env').exists():
            raise ns.SyncError('Моковый секрет оказался в неожиданном месте')
        if (shares[3] / offline_git_relative).exists():
            raise ns.SyncError('Выключенный моковый получатель неожиданно получил Git-пакет')
        if (shares[3] / offline_event_relative).exists():
            raise ns.SyncError('Выключенный моковый получатель неожиданно получил событие зависимостей')
        start(3)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise ns.SyncError('Тестовый Syncthing завершился после повторного запуска')
            try:
                if ((projects[3] / offline_file.name).read_text(encoding='utf-8') == 'synthetic change while receiver offline\n'
                        and all(ns.verify(shares[number] / offline_relative, key)['authentication'] == offline_tag
                                for number in (2, 3))
                        and all((shares[number] / offline_secret_relative).read_bytes() == encrypted_secret
                                for number in (2, 3))
                        and all((shares[number] / offline_git_relative).read_bytes() == encrypted_git
                                for number in (2, 3))
                        and all((shares[number] / offline_event_relative).read_bytes() == encrypted_event
                                and (projects[number] / 'OfflineProject' / 'requirements.txt').is_file()
                                and not (projects[number] / 'OfflineProject' / '.venv').exists()
                                for number in (2, 3))):
                    break
            except (OSError, ValueError, ns.SyncError):
                pass
            time.sleep(0.25)
        else:
            raise ns.SyncError('Выключенный тестовый получатель не догнал изменения')
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if all((selected := publications.select(shares[number], 'linux', kind, name, key))
                       and selected[0].name == offline_secret.name
                       for number in (2, 3) for kind, name in (('secret', '.env'), ('secret', 'Bot/.env'),
                                                               ('secret', 'Bot/.npmrc'), ('secret-set', 'all'))):
                    break
            except (OSError, ValueError, ns.SyncError):
                pass
            time.sleep(0.25)
        else:
            raise ns.SyncError('Публикация мокового секрета не дошла до получателей')
        for number in (2, 3):
            result = auto_receiver.receive_discovered_secrets(projects[number], shares[number], key, 'linux', interval=0.1)
            if (result['state'] != 'APPLIED_AVAILABLE'
                    or (projects[number] / '.env').read_bytes() != (projects[0] / '.env').read_bytes()):
                raise ns.SyncError('Моковый получатель не восстановил обновлённый секрет')
        new_secret = offline_secret
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if all((selected := publications.select(shares[number], 'linux', 'git', 'Bot', key))
                       and selected[0].name == offline_git.name for number in (2, 3)):
                    break
            except (OSError, ValueError, ns.SyncError):
                pass
            time.sleep(0.25)
        else:
            raise ns.SyncError('Публикация мокового Git не дошла до получателей')
        for number in (2, 3):
            result = auto_receiver.receive_discovered_git(projects[number], shares[number], key, 'linux')
            project = projects[number] / 'Bot'
            head = subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD'])
            index = (project / '.git' / 'index').read_bytes()
            refs = subprocess.check_output(['git', '-C', str(project), 'for-each-ref',
                                            '--format=%(objectname)', 'refs/nexus-sync/linux']).splitlines()
            if result['state'] != 'APPLIED_AVAILABLE' or (head, index) != offline_git_before[number] or offline_head not in refs:
                raise ns.SyncError('Моковый Git не подготовлен без изменения ветки и индекса')
        new_git = offline_git
        restored_profile = projects[3].parent / 'live-chat-profile'
        watcher = subprocess.Popen([sys.executable, str(Path(chat.__file__)), 'watch-all',
            '--profile', str(restored_profile), '--shared', str(shares[3]), '--key-file', str(key_path),
            '--source', 'linux', '--source-root', str(projects[0]), '--target-root', str(projects[3]),
            '--codex-executable', str(codex), '--interval', '0.5'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if watcher.poll() is not None:
                    raise ns.SyncError('Наблюдатель вернувшегося мокового получателя завершился')
                try:
                    marker = restored_profile / chat.SYNC_MARKER / ('linux-' + offline_chat_id + '.json')
                    item = json.loads(marker.read_text(encoding='utf-8'))
                    tag = item.get('authentication')
                    if (isinstance(tag, str) and hmac.compare_digest(tag, ns._manifest_tag(item, key))
                            and item.get('snapshot') == offline_snapshot.name
                            and offline_chat_id in ns._chat_catalog(restored_profile)):
                        break
                except (OSError, ValueError, TypeError, ns.SyncError):
                    pass
                time.sleep(0.25)
            else:
                raise ns.SyncError('Вернувшийся моковый получатель не подготовил чат')
        finally:
            if watcher.poll() is None:
                watcher.send_signal(signal.SIGINT)
                watcher.wait(timeout=20)
        (projects[2] / 'Bot' / 'receiver-note.txt').write_text('synthetic peer change\n', encoding='utf-8')
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise ns.SyncError('Тестовый Syncthing завершился')
            try:
                if (projects[3] / 'Bot' / 'receiver-note.txt').read_text() == 'synthetic peer change\n':
                    break
            except OSError:
                pass
            time.sleep(0.25)
        else:
            raise ns.SyncError('Изменение не дошло до второго получателя')
        time.sleep(2)  # Allow a rescan after the relay received the peer change.
        if (projects[0] / 'Bot' / 'receiver-note.txt').exists():
            raise ns.SyncError('Источник Send Only был изменён')
        return new_git, new_secret
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        try:
            for process in processes:
                process.wait(timeout=20)
        finally:
            if proxy is not None:
                proxy.close()
            for log in logs:
                log.close()


def demo(executable: Path, *, use_syncthing: bool = False, poll_exchange: bool = False,
         loopback_mss: bool = False) -> dict:
    if (poll_exchange or loopback_mss) and not use_syncthing:
        raise ns.SyncError('Для выбранной опции нужен режим Syncthing')
    if sys.platform != 'linux' or not shutil.which('git'):
        raise ns.SyncError('Демонстрация требует Linux и Git')
    executable = executable.resolve(strict=True)
    if not executable.is_file():
        raise ns.SyncError('Codex не найден')
    root = Path(tempfile.mkdtemp(prefix='nexus-sync-demo-'))
    source, relay, receiver, windows = (root / name for name in ('linux', 'relay', 'receiver', 'windows'))
    exchanges = [folder / 'exchange' for folder in (source, relay, receiver, windows)]
    for folder in exchanges:
        folder.mkdir(parents=True)
    key = os.urandom(32)
    key_path = root / 'demo.key'
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(key)
    nexus = source / 'Nexus'
    repo = nexus / 'Bot'
    repo.mkdir(parents=True)
    (repo / 'app.py').write_text('print("synthetic demo")\n', encoding='utf-8')
    (repo / 'requirements.txt').write_text('# synthetic demo\n', encoding='utf-8')
    (repo / 'package.json').write_text('{"name":"demo-project","version":"1.0.0"}\n', encoding='utf-8')
    (repo / '.venv').mkdir()
    (repo / '.venv' / 'local-only.txt').write_text('synthetic environment\n', encoding='utf-8')
    (repo / '.venv' / 'lib' / 'python3.12' / 'site-packages' / 'demo_python-1.0.dist-info').mkdir(parents=True)
    node_package = repo / 'node_modules' / 'demo-node'
    node_package.mkdir(parents=True)
    (node_package / 'package.json').write_text('{"name":"demo-node","version":"1.0.0"}\n', encoding='utf-8')
    env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
    env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_TERMINAL_PROMPT': '0'})
    def git(*arguments):
        result = subprocess.run(['git', '-C', str(repo), '-c', 'core.hooksPath=/dev/null',
                                 '-c', 'commit.gpgsign=false', *arguments],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        if result.returncode:
            raise ns.SyncError('Не удалось подготовить моковый Git')
    git('init', '-q')
    git('add', 'app.py', 'requirements.txt', 'package.json')
    git('-c', 'user.name=Demo', '-c', 'user.email=demo@example.invalid', 'commit', '-qm', 'synthetic demo')
    secret = repo / '.env'
    secret.write_text('DEMO_TOKEN=synthetic-only\n', encoding='utf-8')
    root_secret = nexus / '.env'
    root_secret.write_text('DEMO_ROOT=synthetic-only\n', encoding='utf-8')
    npm_secret = repo / '.npmrc'
    npm_secret.write_text('DEMO_NPM=synthetic-only\n', encoding='utf-8')
    source_profile = source / 'synthetic-profile'
    sessions = source_profile / 'sessions'
    sessions.mkdir(parents=True)
    thread_id = '019b0000-0000-7000-8000-000000000098'
    rows = [
        {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
            'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(repo),
            'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
            'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic demo chat'}]}},
        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
            'type': 'user_message', 'message': 'synthetic demo chat', 'images': [], 'local_images': [], 'text_elements': []}},
    ]
    (sessions / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl').write_text(
        ''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
    (source_profile / 'session_index.jsonl').write_text(json.dumps({
        'id': thread_id, 'thread_name': 'Synthetic demo chat', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n', encoding='utf-8')
    state = source / 'state'
    state.mkdir()
    initial_config = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
        'nexus': str(nexus), 'codex_home': str(source_profile), 'shared': str(exchanges[0]),
        'state_dir': str(state), 'key_file': str(key_path), 'direction': 'send-only',
        'discover_secrets': True, 'git_discovery': True})
    _, initial_notes = ns.supervisor_once(initial_config, None)
    if any('отложен' in note for note in initial_notes):
        raise ns.SyncError('Начальный автоматический цикл не завершён')
    initial_snapshot = ns.latest_authenticated_snapshot(exchanges[0] / 'nexus-sync' / 'snapshots' / 'linux', 'linux', key)
    git_markers = list(state.glob('last-git-linux-*.json'))
    if initial_snapshot is None or len(git_markers) != 1:
        raise ns.SyncError('Начальный автоматический снимок или Git-пакет не создан')
    snapshot = initial_snapshot[0]
    package_root = exchanges[0] / 'nexus-sync' / 'secrets' / 'linux'
    def initial_package(marker):
        data = json.loads(marker.read_text(encoding='utf-8'))
        tag, name = data.get('authentication'), data.get('package')
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key))
                or not isinstance(name, str) or Path(name).name != name or not name.endswith('.nxs')):
            raise ns.SyncError('Начальная отметка пакета не подтверждена')
        result = package_root / name
        if not result.is_file():
            raise ns.SyncError('Начальный пакет отсутствует')
        return result
    package = initial_package(git_markers[0])
    secret_package = initial_package(state / 'last-secrets-linux.json')
    if any(item.read_bytes() in secret_package.read_bytes() for item in (secret, npm_secret, root_secret)):
        raise ns.SyncError('Пакет мокового секрета не защищён')
    (repo / 'requirements.txt').write_text('# synthetic demo updated\n', encoding='utf-8')
    ns.record_projects(nexus, exchanges[0], state, 'linux', key=key)
    target_nexuses = [receiver / 'Nexus', windows / 'Nexus']
    for target, exchange in zip(target_nexuses, exchanges[2:]):
        git_sync.init_workspace(target, exchange, key)
    if use_syncthing:
        relay_nexus = relay / 'Nexus'
        relay_nexus.mkdir()
        package, secret_package = syncthing_relay(root, [nexus, relay_nexus, *target_nexuses], exchanges, snapshot,
                                                  package, secret_package, key, executable, source_profile, state,
                                                  key_path, thread_id, poll_exchange=poll_exchange,
                                                  loopback_mss=loopback_mss)
    else:
        # File copies model delivery; this does not start or configure Syncthing.
        shutil.copytree(exchanges[0], exchanges[1], dirs_exist_ok=True)
        for target, exchange in zip(target_nexuses, exchanges[2:]):
            shutil.copytree(exchanges[1], exchange, dirs_exist_ok=True)
            shutil.copytree(repo, target / 'Bot', ignore=shutil.ignore_patterns('.git', '.venv', '.env', '.npmrc', 'node_modules'))
    reports = {}
    configs = {}
    expected_chat_count = 3 if use_syncthing else 1
    for name, device, target, exchange in zip(('mac-mock', 'windows-mock'), (receiver, windows), target_nexuses, exchanges[2:]):
        copied_package = exchange / package.relative_to(exchanges[0])
        if use_syncthing:
            repeated = auto_receiver.receive(target, exchange, key, copied_package,
                exchange / secret_package.relative_to(exchanges[0]), 'Bot', 'Bot/.env', timeout=1, interval=0.1)
            if repeated['state'] != 'COMPLETE':
                raise ns.SyncError('Повторный моковый приём не подтверждён')
            git_result = {'state': 'INSTALLED'}
        else:
            git_result = git_sync.apply_git(target, exchange, key, copied_package, 'Bot')
        profile = device / 'synthetic-profile'
        chat.init_mock_profile(profile, key, exchange)
        chat_result = chat.sync_all_mock(profile, exchange, key, 'linux', str(nexus), target, executable)
        preview_root = device / 'chat-previews'
        chat_preview.init(preview_root, exchange, target, 'linux', str(nexus), key)
        preview_result = chat_preview.prepare(preview_root, key, executable)
        if preview_result['state'] != 'PREPARED' or preview_result['chats_indexed'] != expected_chat_count:
            raise ns.SyncError('Изолированная подготовка мокового чата не завершена')
        target_state = device / 'state'
        target_state.mkdir()
        raw_config = {'format': 'nexus-sync-config-v1', 'machine': name, 'nexus': str(target),
                      'codex_home': str(profile), 'shared': str(exchange), 'state_dir': str(target_state),
                      'key_file': str(key_path), 'direction': 'send-receive', 'peer_roots': {'linux': str(nexus)}}
        config = ns.validate_settings(raw_config)
        config_path = device / 'nexus-sync.json'
        config_path.write_text(json.dumps(raw_config, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        ns.receive_once(exchange, name, key)
        count = ns.prepare_received_once(config, key)
        if count < 1:
            raise ns.SyncError('Не удалось подготовить статус мокового снимка')
        status = ns.configured_status(config, key)
        if status['prepared_records'] != count:
            raise ns.SyncError('Статус мокового снимка не подтверждён')
        configs[name] = str(config_path)
        quarantine = target_state / 'secret-quarantine'
        ns.secrets_extract(exchange / secret_package.relative_to(exchanges[0]), quarantine, key, exchange)
        if (quarantine / 'Bot' / '.env').read_bytes() != secret.read_bytes():
            raise ns.SyncError('Ошибка подготовки мокового секрета')
        if (quarantine / 'Bot' / '.npmrc').read_bytes() != npm_secret.read_bytes():
            raise ns.SyncError('Ошибка подготовки мокового секрета')
        if (quarantine / '.env').read_bytes() != root_secret.read_bytes():
            raise ns.SyncError('Ошибка подготовки корневого мокового секрета')
        secret_result = secret_sync.apply_secret(target, exchange, key,
                                                 exchange / secret_package.relative_to(exchanges[0]), 'Bot/.env')
        if secret_result['state'] not in ('INSTALLED', 'UNCHANGED') or (target / 'Bot' / '.env').read_bytes() != secret.read_bytes():
            raise ns.SyncError('Ошибка восстановления мокового секрета')
        npm_result = secret_sync.apply_secret(target, exchange, key,
                                              exchange / secret_package.relative_to(exchanges[0]), 'Bot/.npmrc')
        if npm_result['state'] not in ('INSTALLED', 'UNCHANGED') or (target / 'Bot' / '.npmrc').read_bytes() != npm_secret.read_bytes():
            raise ns.SyncError('Ошибка восстановления мокового секрета')
        root_result = secret_sync.apply_secret(target, exchange, key,
                                               exchange / secret_package.relative_to(exchanges[0]), '.env')
        if root_result['state'] not in ('INSTALLED', 'UNCHANGED') or (target / '.env').read_bytes() != root_secret.read_bytes():
            raise ns.SyncError('Ошибка восстановления корневого мокового секрета')
        report = ns.write_project_report(exchange, target_state, name, ns.inventory(target), key=key, include_peers=True)
        if use_syncthing:
            report_text = report.read_text(encoding='utf-8')
            if any(value not in report_text for value in ('OfflineProject', 'offline_dep-2.0', 'manylinux_2_17_x86_64')):
                raise ns.SyncError('Моковый отчёт не содержит событие зависимостей')
        if git_result['state'] != 'INSTALLED' or chat_result['state'] != 'COMPLETE' or chat_result['processed'] != expected_chat_count:
            raise ns.SyncError('Не удалось завершить моковую демонстрацию')
        reports[name] = str(report)
    return {'state': 'COMPLETE', 'directory': str(root), 'transport': 'syncthing-loopback' if use_syncthing else 'simulated-copy',
            'initial_discovery_tested': True,
            'exchange_detection': ('polling' if poll_exchange else 'watcher') if use_syncthing else 'simulated',
            'loopback_mss': 1200 if loopback_mss else None,
            'git_restored': True, 'chats_indexed': expected_chat_count, 'secret_prepared_in_quarantine': True,
            'secret_restored_in_mock_projects': True, 'receivers_completed': 2,
            'root_secret_restored': True, 'root_secret_discovery_tested': use_syncthing,
            'automatic_sender_tested_while_open': use_syncthing,
            'automatic_chat_previews_during_sender_open_tested': use_syncthing,
            'persistent_chat_receive_tested': use_syncthing,
            'reverse_chat_transfer_tested': use_syncthing,
            'reverse_project_transfer_tested': use_syncthing,
            'reverse_project_updates_tested': use_syncthing,
            'reverse_project_auto_receive_tested': use_syncthing,
            'windows_origin_chat_updates_tested': use_syncthing,
            'offline_receiver_catchup_tested': use_syncthing,
            'offline_chat_catchup_tested': use_syncthing,
            'offline_secret_catchup_tested': use_syncthing,
            'offline_git_catchup_tested': use_syncthing,
            'offline_dependency_report_tested': use_syncthing,
            'initial_snapshot_receipts_tested': use_syncthing,
            'source_syncthing_file_checked': use_syncthing,
            'automatic_git_and_secret_updates_tested': use_syncthing,
            'automatic_available_package_receive_tested': use_syncthing,
            'continuous_mock_secret_update_tested': use_syncthing,
            'continuous_mock_git_refs_tested': use_syncthing,
            'automatic_new_git_projects_tested': use_syncthing,
            'isolated_chat_previews_prepared': 2,
            'chats_visible_in_cursor': False, 'dependency_report': reports['mac-mock'], 'dependency_reports': reports,
            'status_configs': configs}


def main() -> int:
    parser = argparse.ArgumentParser(description='Создать временную моковую демонстрацию Nexus Sync')
    parser.add_argument('--codex-executable', type=Path, default=Path(shutil.which('codex')) if shutil.which('codex') else None)
    parser.add_argument('--syncthing', action='store_true', help='четыре временных узла Syncthing на 127.0.0.1')
    parser.add_argument('--poll-exchange', action='store_true', help='только в демо: сканировать папку обмена без наблюдателя')
    parser.add_argument('--loopback-mss', action='store_true', help='только в демо: локальный посредник с TCP MSS 1200')
    args = parser.parse_args()
    try:
        if args.codex_executable is None:
            raise ns.SyncError('Codex не найден')
        print(json.dumps(demo(args.codex_executable, use_syncthing=args.syncthing, poll_exchange=args.poll_exchange,
                              loopback_mss=args.loopback_mss), ensure_ascii=False, indent=2))
        return 0
    except ns.SyncError as exc:
        print('Моковая демонстрация отложена: ' + str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, subprocess.SubprocessError):
        print('Не удалось завершить моковую демонстрацию; проверьте Git, Codex и выбранный транспорт', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
