"""Opt-in local-only Syncthing tests; never use the user's configuration."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import nexus_sync as ns
import mock_chat_sync as mock_sync
import mock_git_sync as mock_git
import mock_auto_receiver as mock_receiver
import mock_publications as mock_publications
import mock_syncthing_status


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def unique_ports(count):
    selected = set()
    for _ in range(count * 8):
        selected.add(port())
        if len(selected) == count:
            break
    if len(selected) != count:
        raise RuntimeError('Не удалось выделить разные тестовые порты')
    return list(selected)


def listener_ready(number):
    try:
        with socket.create_connection(('127.0.0.1', number), timeout=0.2):
            return True
    except OSError:
        return False


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'), 'requires opt-in local network')
class SyncthingIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex') and shutil.which('git'),
                         'requires disposable Codex and Git')
    def test_demo_with_local_mss_proxy(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('demo.py')),
                                 '--syncthing', '--loopback-mss'], capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        root = Path(output['directory'])
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.assertEqual(output['loopback_mss'], 1200)
        self.assertEqual(output['receivers_completed'], 2)
        self.assertTrue(output['continuous_mock_secret_update_tested'])
        self.assertTrue(output['continuous_mock_git_refs_tested'])
        self.assertTrue(output.get('automatic_new_git_projects_tested'))
        self.assertTrue(output.get('persistent_chat_receive_tested'))
        self.assertTrue(output.get('root_secret_discovery_tested'))
        self.assertTrue(output.get('offline_receiver_catchup_tested'))
        self.assertTrue(output.get('offline_chat_catchup_tested'))
        self.assertTrue(output.get('offline_secret_catchup_tested'))
        self.assertTrue(output.get('offline_git_catchup_tested'))
        self.assertTrue(output.get('offline_dependency_report_tested'))
        self.assertTrue(output.get('reverse_chat_transfer_tested'))
        self.assertTrue(output.get('reverse_project_transfer_tested'))
        self.assertTrue(output.get('reverse_project_updates_tested'))
        self.assertTrue(output.get('reverse_project_auto_receive_tested'))
        self.assertTrue(output.get('windows_origin_chat_updates_tested'))
        self.assertFalse(output['chats_visible_in_cursor'])
        sender_config = json.loads((root / 'linux' / 'sender-config.json').read_text())
        self.assertTrue(sender_config.get('git_discovery'))
        self.assertTrue(sender_config.get('discover_secrets'))
        self.assertFalse(sender_config.get('git_repos'))
        self.assertFalse(sender_config.get('secret_files'))
        source_config = ET.parse(root / 'syncthing-0' / 'config.xml')
        relay_config = ET.parse(root / 'syncthing-1' / 'config.xml')
        static_addresses = [address.text for device in source_config.findall('device')
                            for address in device.findall('address')
                            if (address.text or '').startswith('tcp://127.0.0.1:')]
        self.assertEqual(len(static_addresses), 1)
        self.assertNotEqual(static_addresses[0], relay_config.findtext('options/listenAddress'))
        for device in ('receiver', 'windows'):
            project = root / device / 'Nexus' / 'Later'
            self.assertTrue((project / '.git').is_dir())
            self.assertEqual((project / 'source.txt').read_text(), 'synthetic late project\n')
            watcher_status = mock_sync.watch_status(root / device / 'live-chat-profile',
                                                   root / device / 'exchange', (root / 'demo.key').read_bytes())
            self.assertEqual(watcher_status['state'], 'STOPPED')
            self.assertFalse(watcher_status['verified_in_cursor'])
        for report_path in output['dependency_reports'].values():
            report = Path(report_path).read_text(encoding='utf-8')
            self.assertIn('OfflineProject', report)
            self.assertIn('offline_dep-2.0', report)
            self.assertIn('manylinux_2_17_x86_64', report)

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex') and shutil.which('git'),
                         'requires disposable Codex and Git')
    def test_demo_can_poll_only_the_exchange_folder(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('demo.py')),
                                 '--syncthing', '--poll-exchange'], capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        root = Path(output['directory'])
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.assertEqual(output['receivers_completed'], 2)
        self.assertEqual(output['exchange_detection'], 'polling')
        self.assertFalse(output['chats_visible_in_cursor'])
        for number in range(4):
            tree = ET.parse(root / f'syncthing-{number}' / 'config.xml')
            folders = {folder.get('id'): folder for folder in tree.findall('folder')}
            self.assertEqual(folders['projects'].get('fsWatcherEnabled'), 'true')
            self.assertEqual(folders['exchange'].get('fsWatcherEnabled'), 'false')
            self.assertEqual(folders['exchange'].get('rescanIntervalS'), '1')

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex') and shutil.which('git'),
                         'requires disposable Codex and Git')
    def test_one_command_syncthing_demo(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('demo.py')), '--syncthing'],
                                capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        root = Path(output['directory'])
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.assertEqual(output['transport'], 'syncthing-loopback')
        self.assertEqual(output['chats_indexed'], 3)
        self.assertEqual(output['receivers_completed'], 2)
        self.assertTrue(output['automatic_sender_tested_while_open'])
        self.assertTrue(output['automatic_chat_previews_during_sender_open_tested'])
        self.assertTrue(output['initial_snapshot_receipts_tested'])
        self.assertTrue(output['source_syncthing_file_checked'])
        self.assertTrue(output['automatic_git_and_secret_updates_tested'])
        self.assertTrue(output['automatic_available_package_receive_tested'])
        self.assertTrue(output['continuous_mock_secret_update_tested'])
        self.assertTrue(output['continuous_mock_git_refs_tested'])
        self.assertEqual(output['isolated_chat_previews_prepared'], 2)
        self.assertTrue(output['secret_prepared_in_quarantine'])
        self.assertTrue(output['secret_restored_in_mock_projects'])
        self.assertNotIn('synthetic-only', result.stdout + result.stderr)
        self.assertNotIn('synthetic-updated', result.stdout + result.stderr)
        self.assertNotIn('synthetic-watch-update', result.stdout + result.stderr)
        self.assertTrue(output['git_restored'])
        self.assertFalse(output['chats_visible_in_cursor'])
        self.assertEqual(set(output['status_configs']), {'mac-mock', 'windows-mock'})
        for config in output['status_configs'].values():
            status = subprocess.run([sys.executable, str(Path(ns.__file__)), 'summary', '--config', config],
                                    capture_output=True, text=True, check=True)
            self.assertGreaterEqual(json.loads(status.stdout)['prepared_records'], 3)
        report = Path(output['dependency_report']).read_text()
        self.assertIn('изменён requirements.txt', report)
        self.assertIn('Локальное Python-окружение не найдено', report)
        self.assertIn('demo_python-1.0 — отмечен на linux; локальное Python-окружение отсутствует', report)
        self.assertIn('demo-node@1.0.0 — отмечен на linux; локальный node_modules отсутствует', report)
        self.assertFalse((root / 'receiver' / 'Nexus' / 'Bot' / '.venv').exists())
        self.assertFalse((root / 'receiver' / 'Nexus' / 'Bot' / 'node_modules').exists())
        self.assertTrue((root / 'windows' / 'Nexus' / 'Bot' / '.git').is_dir())
        self.assertEqual((root / 'windows' / 'Nexus' / 'Bot' / 'receiver-note.txt').read_text(), 'synthetic peer change\n')
        self.assertFalse((root / 'linux' / 'Nexus' / 'Bot' / 'receiver-note.txt').exists())
        for device in ('receiver', 'windows'):
            title_data = ns._read_title_index(root / device / 'synthetic-profile')
            names = ns._parse_title_index(title_data)
            self.assertEqual(next(row['thread_name'] for row in reversed(names)
                                  if row['id'] == '019b0000-0000-7000-8000-000000000098'),
                             'Synthetic demo renamed while open')
            self.assertEqual((root / device / 'Nexus' / 'Bot' / '.env').read_text(), 'DEMO_TOKEN=synthetic-updated\n')
            self.assertEqual((root / device / 'state' / 'secret-quarantine' / 'Bot' / '.env').read_text(),
                             'DEMO_TOKEN=synthetic-updated\n')
            self.assertEqual((root / device / 'Nexus' / 'Bot' / '.npmrc').read_text(), 'DEMO_NPM=synthetic-watch-update\n')
            self.assertEqual((root / device / 'state' / 'secret-quarantine' / 'Bot' / '.npmrc').read_text(),
                             'DEMO_NPM=synthetic-watch-update\n')
            project = root / device / 'Nexus' / 'Bot'
            source_head = subprocess.check_output(['git', '-C', str(root / 'linux' / 'Nexus' / 'Bot'), 'rev-parse', 'HEAD']).strip()
            receiver_head = subprocess.check_output(['git', '-C', str(project), 'rev-parse', 'HEAD']).strip()
            refs = subprocess.check_output(['git', '-C', str(project), 'for-each-ref',
                                            '--format=%(objectname)', 'refs/nexus-sync/linux']).splitlines()
            self.assertNotEqual(receiver_head, source_head)
            self.assertIn(source_head, refs)
        self.assertFalse((root / 'relay' / 'Nexus' / 'Bot' / '.env').exists())
        self.assertFalse((root / 'relay' / 'Nexus' / 'Bot' / '.npmrc').exists())

    @unittest.skipUnless(shutil.which('git'), 'requires Git for mock project transfer')
    def test_late_peer_receives_through_three_node_relay(self):
        with tempfile.TemporaryDirectory(prefix='nexus-syncthing-relay-') as tmp:
            root = Path(tmp)
            homes = [root / name for name in ('linux-home', 'vps-home', 'mac-home')]
            projects = [root / name for name in ('linux-projects', 'vps-projects', 'mac-projects')]
            shares = [root / name for name in ('linux-share', 'vps-share', 'mac-share')]
            available_ports = unique_ports(9)
            listens, guis, profiles = available_ports[:3], available_ports[3:6], available_ports[6:]
            ids = []
            for home in homes:
                subprocess.run(['syncthing', 'generate', '--home', str(home), '--no-port-probing'], capture_output=True, check=True)
                ids.append(subprocess.check_output(['syncthing', 'device-id', '--home', str(home)], text=True).strip())
            for index, home in enumerate(homes):
                projects[index].mkdir()
                shares[index].mkdir()
                shutil.copyfile(Path(__file__).with_name('stignore.example'), projects[index] / '.stignore')
                tree = ET.parse(home / 'config.xml')
                doc = tree.getroot()
                options = doc.find('options')
                options.find('listenAddress').text = f'tcp://127.0.0.1:{listens[index]}'
                for name in ('globalAnnounceEnabled', 'localAnnounceEnabled', 'relaysEnabled', 'natEnabled', 'startBrowser'):
                    options.find(name).text = 'false'
                options.find('urAccepted').text = '-1'
                options.find('reconnectionIntervalS').text = '2'
                peers = (0, 2) if index == 1 else (1,)
                for other in peers:
                    device = ET.SubElement(doc, 'device', {'id': ids[other], 'name': f'test-{other}'})
                    ET.SubElement(device, 'address').text = 'dynamic' if index == 1 else f'tcp://127.0.0.1:{listens[1]}'
                    ET.SubElement(device, 'numConnections').text = '1'
                for folder_id, path, folder_type in (
                    ('project-relay', projects[index], 'sendonly' if index == 0 else 'sendreceive'),
                    ('chat-relay', shares[index], 'sendreceive'),
                ):
                    folder = ET.SubElement(doc, 'folder', {'id': folder_id, 'label': folder_id, 'path': str(path), 'type': folder_type, 'rescanIntervalS': '1', 'fsWatcherEnabled': 'true'})
                    for device_index in (index, *peers):
                        ET.SubElement(folder, 'device', {'id': ids[device_index]})
                tree.write(home / 'config.xml', encoding='utf-8', xml_declaration=True)
            (projects[0] / 'example.txt').write_text('linux original\n')
            (projects[0] / 'Bot').mkdir()
            (projects[0] / 'Bot' / 'source.txt').write_text('mock project\n')
            tracked_secret = projects[0] / 'Bot' / '.env'
            tracked_secret.write_bytes(b'MOCK_TRACKED=relay-test\n')
            mock_secret = projects[0] / 'app' / '.env'
            mock_secret.parent.mkdir()
            mock_secret.write_bytes(b'MOCK_SECRET=relay-test\n')
            mock_credential = mock_secret.parent / '.npmrc'
            mock_credential.write_bytes(b'MOCK_NPM=relay-test\n')
            codex = root / 'synthetic-codex' / 'sessions'
            codex.mkdir(parents=True)
            (codex / 'rollout-test.jsonl').write_text('{"type":"session_meta","payload":{"id":"relay"}}\n')
            unfinished = codex / 'rollout-active.jsonl'
            unfinished.write_text('{"partial":')
            key = b'r' * 32
            event_state = root / 'linux-event-state'
            event_state.mkdir()
            requirements = projects[0] / 'Bot' / 'requirements.txt'
            requirements.write_text('mock==1\n')
            ambiguous = projects[0] / 'venv'
            ambiguous.mkdir()
            (ambiguous / 'requirements.txt').write_text('synthetic-ignored-project\n')
            (projects[0] / 'Bot' / 'package.json').write_text('{}')
            python_site = projects[0] / 'Bot' / '.venv' / 'lib' / 'python3.12' / 'site-packages'
            python_site.mkdir(parents=True)
            (python_site / 'preexisting-0.1.dist-info').mkdir()
            node_package = projects[0] / 'Bot' / 'node_modules' / 'mock-package'
            node_package.mkdir(parents=True)
            (node_package / 'package.json').write_text(json.dumps({'name': 'mock-package', 'version': '1.0.0'}))
            self.assertEqual(ns.record_projects(projects[0], shares[0], event_state, 'linux', key=key), [])
            source_report = ns.write_project_report(shares[0], event_state, 'linux', ns.inventory(projects[0]),
                                                    key=key, include_peers=False)
            self.assertIn('"venv": есть признак проекта; проверьте до исключения', source_report.read_text())
            baseline = event_state / 'projects-linux.json'
            old_baseline = json.loads(baseline.read_text())
            old_baseline.pop('package_baseline_version')
            old_baseline.pop('package_baseline_pending')
            baseline.write_text(json.dumps(old_baseline))
            requirements.write_text('mock==2\n')
            (python_site / 'mock_python-1.0.dist-info').mkdir()
            (node_package / 'package.json').write_text(json.dumps({'name': 'mock-package', 'version': '2.0.0',
                                                                    'os': ['linux'], 'cpu': ['x64']}))
            expected_events = ns.record_projects(projects[0], shares[0], event_state, 'linux', key=key)
            self.assertIn('Обнаружен Node.js-пакет: Bot — mock-package@2.0.0', expected_events)
            self.assertIn('Обнаружен Python-пакет: Bot (.venv/lib/python3.12/site-packages) — mock_python-1.0', expected_events)
            encrypted_event = next((shares[0] / 'nexus-sync' / 'project-events' / 'linux').glob('*.nxe'))
            self.assertNotIn(b'requirements.txt', encrypted_event.read_bytes())
            snapshot = ns.snapshot(codex.parent, shares[0], 'linux', key)
            secret_state, secret_key = root / 'linux-secret-state', root / 'linux-secret.key'
            secret_state.mkdir()
            secret_key.write_bytes(key)
            sender = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
                'nexus': str(projects[0]), 'codex_home': str(codex.parent), 'shared': str(shares[0]),
                'state_dir': str(secret_state), 'key_file': str(secret_key), 'discover_secrets': True})
            self.assertTrue(ns.publish_secrets_once(sender, key))
            package_name = json.loads((secret_state / 'last-secrets-linux.json').read_text())['package']
            secret_package = shares[0] / 'nexus-sync' / 'secrets' / 'linux' / package_name
            secret_relative = secret_package.relative_to(shares[0])
            ciphertext = secret_package.read_bytes()
            self.assertNotIn(mock_secret.read_bytes(), ciphertext)
            self.assertNotIn(mock_credential.read_bytes(), ciphertext)
            self.assertNotIn(tracked_secret.read_bytes(), ciphertext)
            repo = projects[0] / 'Bot'
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            subprocess.run(['git', '-C', str(repo), 'add', 'source.txt', '.env'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                            'user.email=mock@example.invalid', 'commit', '-qm', 'mock'], check=True)
            subprocess.run(['git', '-C', str(repo), 'branch', 'extra-mock'], check=True)
            subprocess.run(['git', '-C', str(repo), 'remote', 'add', 'origin',
                            'https://mock-secret@example.invalid/mock.git'], check=True)
            git_package = ns.git_pack(repo, shares[0], 'linux', key, nexus=projects[0])
            git_relative = git_package.relative_to(shares[0])
            self.assertNotIn(b'mock-secret', git_package.read_bytes())
            self.assertNotIn(tracked_secret.read_bytes(), git_package.read_bytes())
            mock_publications.publish(shares[0], 'linux', 'git', 'Bot', git_package, key)
            mock_publications.publish_all_secret_marker(shares[0], secret_state, 'linux', key)
            self.assertEqual(ns.verify(snapshot, key)['omitted_files'], 1)
            relative = snapshot.relative_to(shares[0])
            processes = []
            logs = []
            started = []
            def start(index):
                log = (root / f'node-{index}.log').open('w+')
                logs.append(log)
                environment = {**os.environ, 'STTRACE': 'connections:DEBUG,model:DEBUG'}
                if os.environ.get('NEXUS_DIAG_GOROUTINES') == '1':
                    environment['STPROFILER'] = f'127.0.0.1:{profiles[index]}'
                processes.append(subprocess.Popen(['syncthing', 'serve', '--home', str(homes[index]), '--no-browser', '--no-restart', '--no-upgrade', '--gui-address', f'http://127.0.0.1:{guis[index]}'], stdout=log, stderr=subprocess.STDOUT, env=environment))
                started.append(index)
            def wait_for(predicate, description, *, timeout=35):
                started_at = time.monotonic()
                deadline = started_at + timeout
                last_error = None
                while time.monotonic() < deadline:
                    if any(process.poll() is not None for process in processes):
                        self.fail('an isolated Syncthing process exited unexpectedly')
                    try:
                        if predicate():
                            if time.monotonic() - started_at > 35:
                                print(f'Тестовая передача завершилась с задержкой: {time.monotonic() - started_at:.1f} с', flush=True)
                            return
                    except (OSError, ValueError, ns.SyncError) as error:
                        last_error = type(error).__name__ + (': ' + str(error).split(':', 1)[0] if isinstance(error, ns.SyncError) else '')
                    time.sleep(0.25)
                details = []
                pulls = []
                for index, log in enumerate(logs):
                    log.flush()
                    log.seek(0)
                    pull_details = []
                    for line in log.read().splitlines():
                        lower = line.lower()
                        if 'log.pkg=model' in lower and any(word in lower for word in ('pull', 'request', 'fail', 'error')):
                            pull_details.append(f'launch {index} pull: ' + line[:300])
                        if any(word in lower for word in ('connecting', 'connected', 'connection refused',
                                                          'handshake', 'listen', 'fatal', 'dial', 'puller', 'retry')):
                            details.append(f'launch {index}: ' + line[:300])
                    pulls.extend(pull_details[-30:])
                manifest = ns.verify(snapshot, key)
                probes = {'manifest': (relative / 'manifest.json').as_posix(),
                          'payload': (relative / 'files' / (manifest['files'][0]['path'] + '.enc')).as_posix()}
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                for index in started:
                    try:
                        api_key = ET.parse(homes[index] / 'config.xml').findtext('gui/apikey')
                        def api(endpoint, params):
                            query = urllib.parse.urlencode(params)
                            req = urllib.request.Request(f'http://127.0.0.1:{guis[index]}/rest/{endpoint}?{query}',
                                                         headers={'X-API-Key': api_key})
                            with opener.open(req, timeout=1) as response:
                                return json.loads(response.read(1024 * 1024))
                        stat = api('db/status', {'folder': 'chat-relay'})
                        fields = {name: stat.get(name) for name in ('state', 'localFiles', 'globalFiles', 'needFiles', 'pullErrors')}
                        connections = api('system/connections', {}).get('connections', {})
                        fields['connections'] = [{name: item.get(name) for name in ('connected', 'inBytesTotal', 'outBytesTotal')}
                                                 for item in connections.values() if isinstance(item, dict)]
                        errors = api('folder/errors', {'folder': 'chat-relay'}).get('errors') or []
                        fields['scan_error_count'] = len(errors)
                        for label, filename in probes.items():
                            try:
                                data = api('db/file', {'folder': 'chat-relay', 'file': filename})
                                local = data.get('local') or {}
                                fields[label] = {name: local.get(name) for name in ('ignored', 'invalid', 'deleted')}
                                fields[label]['present'] = bool(local.get('name'))
                            except urllib.error.HTTPError as error:
                                fields[label] = f'HTTP {error.code}'
                                error.close()
                        details.append(f'node {index} index: {fields}')
                    except (OSError, ValueError, TypeError, ET.ParseError):
                        details.append(f'node {index} index unavailable')
                if os.environ.get('NEXUS_DIAG_GOROUTINES') == '1':
                    for index in started:
                        try:
                            with opener.open(f'http://127.0.0.1:{profiles[index]}/debug/pprof/goroutine?debug=2', timeout=2) as response:
                                dump = response.read(8 * 1024 * 1024).decode('utf-8', errors='replace')
                            relevant = []
                            for stack in dump.split('\n\n'):
                                if 'sendReceiveFolder' in stack or 'RequestGlobal' in stack:
                                    lines = stack.splitlines()
                                    functions = [line.split('(', 1)[0].strip() for line in lines[1::2]][:6]
                                    relevant.append(lines[0].split(' [', 1)[-1] + ' ' + ' -> '.join(functions))
                            pulls.append(f'node {index} goroutines: ' + repr(relevant[:20]))
                        except (OSError, ValueError) as error:
                            pulls.append(f'node {index} profiler unavailable: {type(error).__name__}')
                self.fail(description + f' (last error type: {last_error})\n' + '\n'.join(details[-20:] + pulls))
            try:
                start(1)  # Relay first; the Mac is still offline.
                wait_for(lambda: listener_ready(listens[1]), 'relay listener did not start')
                start(0)
                wait_for(lambda: ns.verify(shares[1] / relative, key), 'snapshot did not reach the relay', timeout=40)
                wait_for(lambda: (shares[1] / secret_relative).read_bytes() == ciphertext,
                         'encrypted secret did not reach the relay')
                wait_for(lambda: any(all(change in event['changes'] for change in expected_events) for event in ns.project_log(shares[1], key)),
                         'encrypted project event did not reach the relay')
                wait_for(lambda: (projects[1] / 'example.txt').read_text() == 'linux original\n', 'project did not reach the relay')
                self.assertFalse((shares[2] / relative).exists())
                start(2)
                wait_for(lambda: ns.verify(shares[2] / relative, key), 'snapshot did not reach the late peer')
                wait_for(lambda: (shares[2] / secret_relative).read_bytes() == ciphertext,
                         'encrypted secret did not reach the late peer')
                wait_for(lambda: (shares[2] / git_relative).read_bytes() == git_package.read_bytes(),
                         'encrypted Git package did not reach the late peer')
                for kind, name in (('git', 'Bot'), ('secret', 'app/.env'), ('secret', 'app/.npmrc'),
                                   ('secret', 'Bot/.env'), ('secret-set', 'all')):
                    wait_for(lambda: mock_publications.select(shares[2], 'linux', kind, name, key),
                             'mock publication did not reach the late peer')
                auto_root = root / 'multiple-secrets-receiver'
                mock_git.init_workspace(auto_root, shares[2], key)
                (auto_root / 'Bot').mkdir()
                (auto_root / 'Bot' / 'source.txt').write_text('mock project\n')
                (auto_root / 'app').mkdir()
                many = mock_receiver.receive_many_available(auto_root, shares[2], key, 'linux', 'Bot',
                                                              ['app/.env', 'app/.npmrc'], interval=0.1)
                self.assertEqual((many['state'], many['secret_ready_count']), ('APPLIED_AVAILABLE', 2))
                self.assertEqual((auto_root / 'app' / '.env').read_bytes(), mock_secret.read_bytes())
                self.assertEqual((auto_root / 'app' / '.npmrc').read_bytes(), mock_credential.read_bytes())
                self.assertEqual(subprocess.check_output(['git', '-C', str(auto_root / 'Bot'), 'show', 'HEAD:.env']),
                                 tracked_secret.read_bytes())
                plain_root = root / 'no-git-receiver'
                mock_git.init_workspace(plain_root, shares[2], key)
                (plain_root / 'app').mkdir()
                (plain_root / 'Bot').mkdir()
                without_git = mock_receiver.receive_discovered_secrets(plain_root, shares[2], key, 'linux', interval=0.1)
                self.assertEqual((without_git['state'], without_git['secret_ready_count']), ('APPLIED_AVAILABLE', 3))
                self.assertEqual((plain_root / 'app' / '.env').read_bytes(), mock_secret.read_bytes())
                self.assertEqual((plain_root / 'app' / '.npmrc').read_bytes(), mock_credential.read_bytes())
                self.assertEqual((plain_root / 'Bot' / '.env').read_bytes(), tracked_secret.read_bytes())
                self.assertFalse((plain_root / 'app' / '.git').exists())
                wait_for(lambda: any(all(change in event['changes'] for change in expected_events) for event in ns.project_log(shares[2], key)),
                         'encrypted project event did not reach the late peer')
                wait_for(lambda: any(any('Начально наблюдался Python-пакет:' in change and 'preexisting-0.1' in change
                                         for change in event['changes']) for event in ns.project_log(shares[2], key)),
                         'encrypted initial package observation did not reach the late peer')
                self.assertFalse((projects[2] / 'Bot' / 'node_modules').exists())
                receiver_codex = root / 'mac-codex'
                receiver_codex.mkdir()
                receiver_state = root / 'mac-state'
                receiver_state.mkdir()
                receiver_key = root / 'mac.key'
                receiver_key.write_bytes(key)
                receiver = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                    'nexus': str(projects[2]), 'codex_home': str(receiver_codex), 'shared': str(shares[2]),
                    'state_dir': str(receiver_state), 'key_file': str(receiver_key), 'direction': 'send-receive',
                    'peer_roots': {'linux': '/mock-linux/Nexus'}, 'prepare_secrets': True})
                git_preview = root / 'mock-git-preview'
                ns.git_preview(shares[2] / git_relative, git_preview, receiver, key)
                self.assertEqual(subprocess.check_output(['git', '-C', str(git_preview), 'rev-parse',
                    'refs/heads/extra-mock']), subprocess.check_output(['git', '-C', str(repo), 'rev-parse',
                    'refs/heads/extra-mock']))
                self.assertEqual(subprocess.check_output(['git', '-C', str(git_preview), 'remote', 'get-url',
                    'origin'], text=True).strip(), 'https://mock-secret@example.invalid/mock.git')
                wait_for(lambda: (projects[2] / 'Bot' / 'source.txt').read_text() == 'mock project\n',
                         'mock project did not reach the late peer')
                wait_for(lambda: (projects[2] / 'Bot' / 'package.json').is_file(),
                         'mock Node.js manifest did not reach the late peer')
                package_report = ns.write_project_report(shares[2], receiver_state, 'mac', ns.inventory(projects[2]),
                                                         key=key, include_peers=True, platform='darwin')
                self.assertIn('mock-package@2.0.0 — отмечен на linux; локальный node_modules отсутствует',
                              package_report.read_text())
                self.assertIn('mock_python-1.0 — отмечен на linux; локальное Python-окружение отсутствует',
                              package_report.read_text())
                self.assertIn('OS в метаданных: linux; CPU в метаданных: x64; ограничение может исключать текущую ОС',
                              package_report.read_text())
                self.assertIn('preexisting-0.1 — отмечен на linux; локальное Python-окружение отсутствует',
                              package_report.read_text())
                self.assertNotIn('mock-package@1.0.0 — отмечен на linux;', package_report.read_text())
                self.assertTrue(any('Обновлён исходный список Node.js: Bot' in event['changes']
                                    for event in ns.project_log(shares[2], key)))
                mock_git_root = root / 'mock-git-receiver'
                mock_git.init_workspace(mock_git_root, shares[2], key)
                shutil.copytree(projects[2] / 'Bot', mock_git_root / 'Bot')
                self.assertFalse((mock_git_root / 'Bot' / '.git').exists())
                self.assertEqual(mock_git.apply_git(mock_git_root, shares[2], key, shares[2] / git_relative,
                                                   'Bot')['state'], 'INSTALLED')
                self.assertEqual((mock_git_root / 'Bot' / 'source.txt').read_text(), 'mock project\n')
                self.assertEqual(subprocess.check_output(['git', '-C', str(mock_git_root / 'Bot'), 'rev-parse', 'HEAD']),
                                 subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD']))
                old_head = subprocess.check_output(['git', '-C', str(mock_git_root / 'Bot'), 'rev-parse', 'HEAD'])
                subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Mock', '-c',
                                'user.email=mock@example.invalid', 'commit', '--allow-empty', '-qm', 'next'], check=True)
                new_package = ns.git_pack(repo, shares[0], 'linux', key, nexus=projects[0])
                new_relative = new_package.relative_to(shares[0])
                wait_for(lambda: (shares[2] / new_relative).read_bytes() == new_package.read_bytes(),
                         'updated Git package did not reach the late peer')
                fetched = mock_git.fetch_git(mock_git_root, shares[2], key, shares[2] / new_relative, 'Bot', 'linux')
                self.assertEqual(fetched['state'], 'FETCHED')
                self.assertEqual(subprocess.check_output(['git', '-C', str(mock_git_root / 'Bot'), 'rev-parse', 'HEAD']), old_head)
                branch = subprocess.check_output(['git', '-C', str(repo), 'symbolic-ref', '--short', 'HEAD'], text=True).strip()
                self.assertEqual(subprocess.check_output(['git', '-C', str(mock_git_root / 'Bot'), 'rev-parse',
                    f'refs/nexus-sync/linux/{new_package.stem}/heads/{branch}']),
                    subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD']))
                self.assertEqual(mock_git.fetch_git(mock_git_root, shares[2], key, shares[2] / new_relative,
                                                    'Bot', 'linux')['state'], 'UNCHANGED')
                self.assertEqual(ns.prepare_received_secrets(receiver, key), 1)
                recovered = receiver_state / 'prepared-secrets' / 'linux' / secret_package.stem / 'files'
                self.assertEqual((recovered / 'app' / '.env').read_bytes(), mock_secret.read_bytes())
                self.assertEqual((recovered / 'app' / '.npmrc').read_bytes(), mock_credential.read_bytes())
                self.assertEqual((recovered / 'Bot' / '.env').read_bytes(), tracked_secret.read_bytes())
                plan = ns.plan_prepared_secrets(receiver, key, 'linux', secret_package.stem)
                self.assertEqual(plan['files'], [{'path': 'Bot/.env', 'state': 'MISSING'},
                                                 {'path': 'app/.env', 'state': 'MISSING'},
                                                 {'path': 'app/.npmrc', 'state': 'MISSING'}])
                self.assertNotIn('MOCK_SECRET', json.dumps(plan))
                wait_for(lambda: (projects[2] / 'example.txt').read_text() == 'linux original\n', 'project did not reach the late peer')
                self.assertFalse((projects[1] / 'app' / '.env').exists())
                self.assertFalse((projects[2] / 'app' / '.env').exists())
                self.assertFalse((projects[1] / 'app' / '.npmrc').exists())
                self.assertFalse((projects[2] / 'app' / '.npmrc').exists())
                self.assertFalse((projects[1] / 'Bot' / '.env').exists())
                self.assertFalse((projects[2] / 'Bot' / '.env').exists())
                self.assertFalse((projects[2] / 'venv' / 'requirements.txt').exists())
                self.assertEqual(ns.receive_once(shares[2], 'mac', key), 1)
                wait_for(lambda: ns.status(shares[0], 'linux', key).get('received_by') == ['mac'], 'receipt did not traverse the relay')
                self.assertEqual(ns.status(shares[0], 'linux', key)['state'], 'snapshot_unverified')
                unfinished.write_text('{"type":"session_meta","payload":{"id":"active"}}\n')
                complete = ns.snapshot(codex.parent, shares[0], 'linux', key)
                complete_relative = complete.relative_to(shares[0])
                wait_for(lambda: ns.verify(shares[2] / complete_relative, key).get('omitted_files') == 0,
                         'completed snapshot did not reach the late peer')
                self.assertEqual(ns.receive_once(shares[2], 'mac', key), 1)
                wait_for(lambda: ns.status(shares[0], 'linux', key).get('state') == 'snapshot_received',
                         'completed snapshot receipt did not traverse the relay')
                if os.environ.get('NEXUS_TEST_CODEX') == '1':
                    from test_codex_contract import read_rpc_response, stop_server
                    executable = os.environ.get('NEXUS_TEST_CODEX_EXECUTABLE') or shutil.which('codex')
                    self.assertTrue(executable, 'Codex executable is required for the optional index test')
                    paginated_source = root / 'paginated-source'
                    source_sessions = paginated_source / 'sessions' / '2026' / '01' / '01'
                    source_sessions.mkdir(parents=True)
                    thread_id = '019b0000-0000-7000-8000-000000000066'
                    rows = [
                        {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                         {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/mock-linux/Nexus/Bot',
                          'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'relay synthetic paginated message'}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': 'relay synthetic paginated message', 'images': [], 'local_images': [], 'text_elements': []}},
                        {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'relay later message'}]}},
                        {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': 'relay later message', 'images': [], 'local_images': [], 'text_elements': []}},
                    ]
                    rollout = source_sessions / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
                    rollout.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                    migrated = subprocess.run([executable, 'migrate-rollouts', '--apply', '--json'],
                        env={**os.environ, 'CODEX_HOME': str(paginated_source)}, capture_output=True, text=True)
                    self.assertEqual(migrated.returncode, 0, migrated.stderr)
                    self.assertEqual(json.loads(migrated.stdout)['outcomes'][0]['status'], 'migrated')
                    paginated = ns.snapshot(paginated_source, shares[0], 'linux', key)
                    paginated_relative = paginated.relative_to(shares[0])
                    wait_for(lambda: ns.verify(shares[2] / paginated_relative, key)['paginated_files'] == 1,
                             'paginated snapshot did not reach the late peer')
                    wait_for(lambda: (projects[2] / 'Bot' / 'source.txt').is_file(),
                             'mock project did not reach the late peer')
                    windows_id = '019b0000-0000-7000-8000-000000000068'
                    windows_source = root / 'windows-source'
                    windows_sessions = windows_source / 'sessions' / '2026' / '01' / '01'
                    windows_sessions.mkdir(parents=True)
                    windows_rows = json.loads(json.dumps(rows))
                    windows_rows[0]['payload']['id'] = windows_id
                    windows_rows[0]['payload']['cwd'] = r'D:\Nexus\Bot'
                    (windows_sessions / f'rollout-2026-01-01T00-00-00-{windows_id}.jsonl').write_text(
                        ''.join(json.dumps(row) + '\n' for row in windows_rows))
                    windows_snapshot = ns.snapshot(windows_source, shares[0], 'windows', key)
                    windows_relative = windows_snapshot.relative_to(shares[0])
                    wait_for(lambda: ns.verify(shares[2] / windows_relative, key),
                             'mock Windows snapshot did not reach the late peer')
                    local_id = '019b0000-0000-7000-8000-000000000067'
                    local_sessions = receiver_codex / 'sessions' / '2026' / '01' / '01'
                    local_sessions.mkdir(parents=True)
                    local_chat = local_sessions / f'rollout-2026-01-01T00-00-00-{local_id}.jsonl'
                    local_rows = json.loads(json.dumps(rows))
                    local_rows[0]['payload']['id'] = local_id
                    local_rows[0]['payload']['cwd'] = str(projects[2] / 'Bot')
                    local_chat.write_text(''.join(json.dumps(row) + '\n' for row in local_rows))
                    local_original = local_chat.read_bytes()
                    local_paginated = local_sessions / rollout.name
                    initial = []
                    for line in rollout.read_text().splitlines(keepends=True):
                        row = json.loads(line)
                        if row.get('type') == 'session_meta':
                            row['payload']['cwd'] = str(projects[2] / 'Bot')
                            line = json.dumps(row) + '\n'
                        if row.get('timestamp', '') < '2026-01-01T00:01:00':
                            initial.append(line)
                    local_paginated.write_text(''.join(initial))
                    paginated_original = local_paginated.read_bytes()
                    preview_root = root / 'automatic-previews'
                    preview_root.mkdir()
                    automatic = dict(receiver, auto_index_preview_dir=preview_root,
                                     auto_index_codex_executable=Path(executable), auto_index_combine_peers=True,
                                     peer_roots={'linux': '/mock-linux/Nexus', 'windows': r'D:\Nexus'})
                    notes = ns.auto_index_cycle(automatic)
                    self.assertTrue(any('объединённая тестовая копия' in note for note in notes))
                    candidates = list(preview_root.glob('combined-*'))
                    self.assertEqual(len(candidates), 1)
                    preview = candidates[0]
                    report = json.loads((preview / 'index-preview.json').read_text())
                    self.assertEqual(ns.auto_index_cycle(automatic), [])
                    self.assertEqual(report['pagination_responded'], 1)
                    self.assertFalse(report['partial'])
                    self.assertEqual(report['actions'][0]['action'], 'EXTEND_COPY')
                    self.assertEqual(report['actions'][1]['action'], 'ADD_COPY')
                    server = subprocess.Popen([executable, 'app-server', '--listen', 'stdio://'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                        env={**os.environ, 'CODEX_HOME': str(preview)})
                    try:
                        def call(request):
                            server.stdin.write((json.dumps(request) + '\n').encode())
                            server.stdin.flush()
                            return read_rpc_response(server, request['id'])
                        self.assertIn('result', call({'id': 1, 'method': 'initialize', 'params':
                            {'clientInfo': {'name': 'nexus_relay_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                        server.stdin.write(b'{"method":"initialized","params":{}}\n')
                        server.stdin.flush()
                        page = call({'id': 2, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                        self.assertIn('relay synthetic paginated message', json.dumps(page))
                        self.assertIn('relay later message', json.dumps(page))
                        listing = call({'id': 3, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                        self.assertIn(local_id, json.dumps(listing))
                        self.assertIn(thread_id, json.dumps(listing))
                        self.assertIn(windows_id, json.dumps(listing))
                    finally:
                        stop_server(server)
                    self.assertEqual(local_chat.read_bytes(), local_original)
                    self.assertEqual(local_paginated.read_bytes(), paginated_original)
                    mock_profile = root / 'standalone-mock-receiver'
                    mock_sync.init_mock_profile(mock_profile, key, shares[2])
                    mock_chat = mock_profile / local_paginated.relative_to(receiver_codex)
                    mock_chat.parent.mkdir(parents=True)
                    mock_chat.write_bytes(paginated_original)
                    first_sync = mock_sync.sync_latest_mock(mock_profile, shares[2], key, 'linux', thread_id,
                        '/mock-linux/Nexus', projects[2], Path(executable))
                    self.assertEqual(first_sync['state'], 'UPDATED')
                    self.assertTrue(first_sync['index_updated'])
                    # Continue the source chat after the first transfer. A new
                    # snapshot must produce a new combined preview, while the
                    # previous preview and the receiver profile stay intact.
                    previous_preview = (preview / local_paginated.relative_to(receiver_codex)).read_bytes()
                    additional = [
                        {'timestamp': '2026-01-01T00:02:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'relay third message'}]}},
                        {'timestamp': '2026-01-01T00:02:01.000Z', 'type': 'event_msg', 'payload':
                         {'type': 'task_started', 'turn_id': 'relay-third-turn', 'started_at': 1767225721,
                          'model_context_window': None, 'collaboration_mode_kind': 'default'}},
                        {'timestamp': '2026-01-01T00:02:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'item_completed', 'thread_id': thread_id, 'turn_id': 'relay-third-turn',
                          'item': {'type': 'UserMessage', 'id': 'relay-third-item',
                                   'content': [{'type': 'text', 'text': 'relay third message', 'text_elements': []}]},
                          'completed_at_ms': 1767225721001}},
                        {'timestamp': '2026-01-01T00:02:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'task_complete', 'turn_id': 'relay-third-turn', 'last_agent_message': None,
                          'completed_at': 1767225721}},
                    ]
                    last_ordinal = max(json.loads(line)['ordinal'] for line in rollout.read_text().splitlines())
                    for offset, row in enumerate(additional, 1):
                        row['ordinal'] = last_ordinal + offset
                    with rollout.open('a', encoding='utf-8') as stream:
                        stream.writelines(json.dumps(row) + '\n' for row in additional)
                    updated = ns.snapshot(paginated_source, shares[0], 'linux', key)
                    wait_for(lambda: ns.verify(shares[2] / updated.relative_to(shares[0]), key),
                             'updated chat snapshot did not reach the late peer')
                    updated_notes = ns.auto_index_cycle(automatic)
                    self.assertTrue(any('объединённая тестовая копия' in note for note in updated_notes))
                    newer = set(preview_root.glob('combined-*')) - {preview}
                    self.assertEqual(len(newer), 1)
                    updated_preview = newer.pop()
                    updated_report = json.loads((updated_preview / 'index-preview.json').read_text())
                    self.assertFalse(updated_report['partial'])
                    self.assertEqual(updated_report['actions'][0]['action'], 'EXTEND_COPY')
                    server = subprocess.Popen([executable, 'app-server', '--listen', 'stdio://'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                        env={**os.environ, 'CODEX_HOME': str(updated_preview)})
                    try:
                        self.assertIn('result', call({'id': 4, 'method': 'initialize', 'params':
                            {'clientInfo': {'name': 'nexus_relay_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                        server.stdin.write(b'{"method":"initialized","params":{}}\n')
                        server.stdin.flush()
                        page = call({'id': 5, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                        self.assertIn('relay third message', json.dumps(page))
                        listing = call({'id': 6, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                        self.assertIn(local_id, json.dumps(listing))
                        self.assertIn(windows_id, json.dumps(listing))
                    finally:
                        stop_server(server)
                    self.assertEqual(local_chat.read_bytes(), local_original)
                    self.assertEqual(local_paginated.read_bytes(), paginated_original)
                    self.assertEqual((preview / local_paginated.relative_to(receiver_codex)).read_bytes(), previous_preview)
                    before_rebase = mock_chat.read_bytes()
                    with self.assertRaises(ns.SyncError):
                        mock_sync.sync_latest_mock(mock_profile, shares[2], key, 'linux', thread_id,
                            '/mock-linux/Nexus', projects[2], Path(executable))
                    self.assertEqual(mock_chat.read_bytes(), before_rebase)
                    second_sync = mock_sync.sync_all_mock(mock_profile, shares[2], key, 'linux',
                        '/mock-linux/Nexus', projects[2], Path(executable), allow_mock_settings_rebase=True)
                    self.assertEqual(second_sync['state'], 'COMPLETE')
                    self.assertEqual(second_sync['results'], {'REBASED_MOCK': 1})
                    self.assertIn(b'thread_settings_applied', mock_chat.read_bytes())
                    backup_folder = mock_profile / '.nexus-mock-backups' / thread_id
                    self.assertTrue(any(path.read_bytes() == before_rebase for path in backup_folder.glob('*.jsonl')))
                    same = mock_sync.sync_latest_mock(mock_profile, shares[2], key, 'linux', thread_id,
                        '/mock-linux/Nexus', projects[2], Path(executable), allow_mock_settings_rebase=True)
                    self.assertEqual(same['state'], 'UNCHANGED')
                    server = subprocess.Popen([executable, 'app-server', '--listen', 'stdio://'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                        env={**os.environ, 'CODEX_HOME': str(mock_profile)})
                    try:
                        self.assertIn('result', call({'id': 7, 'method': 'initialize', 'params':
                            {'clientInfo': {'name': 'nexus_relay_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                        server.stdin.write(b'{"method":"initialized","params":{}}\n')
                        server.stdin.flush()
                        page = call({'id': 8, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                        self.assertIn('relay third message', json.dumps(page))
                        listing = call({'id': 9, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                        self.assertIn(thread_id, {item['id'] for item in listing['result']['data']})
                    finally:
                        stop_server(server)
                    new_id = '019b0000-0000-7000-8000-000000000069'
                    new_rows = json.loads(json.dumps(rows))
                    new_rows[0]['payload']['id'] = new_id
                    for row in new_rows[1:]:
                        payload = row['payload']
                        if row['type'] == 'response_item':
                            payload['content'][0]['text'] = 'new relay mock conversation'
                        elif payload.get('type') == 'user_message':
                            payload['message'] = 'new relay mock conversation'
                    new_file = source_sessions / f'rollout-2026-01-01T00-00-00-{new_id}.jsonl'
                    new_file.write_text(''.join(json.dumps(row) + '\n' for row in new_rows))
                    (paginated_source / 'session_index.jsonl').write_text(json.dumps({'id': new_id,
                        'thread_name': 'relay mock title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
                    added_snapshot = ns.snapshot(paginated_source, shares[0], 'linux', key)
                    added_relative = added_snapshot.relative_to(shares[0])
                    wait_for(lambda: ns.verify(shares[2] / added_relative, key), 'new chat did not reach the late peer')
                    added = mock_sync.sync_latest_mock(mock_profile, shares[2], key, 'linux', new_id,
                        '/mock-linux/Nexus', projects[2], Path(executable))
                    self.assertEqual(added['state'], 'ADDED')
                    self.assertTrue(added['index_updated'])
                    self.assertTrue(added['titles_updated'])
                    server = subprocess.Popen([executable, 'app-server', '--listen', 'stdio://'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                        env={**os.environ, 'CODEX_HOME': str(mock_profile)})
                    try:
                        self.assertIn('result', call({'id': 10, 'method': 'initialize', 'params':
                            {'clientInfo': {'name': 'nexus_relay_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                        server.stdin.write(b'{"method":"initialized","params":{}}\n')
                        server.stdin.flush()
                        listing = call({'id': 11, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                        self.assertTrue({thread_id, new_id} <= {item['id'] for item in listing['result']['data']})
                        self.assertEqual(next(item['name'] for item in listing['result']['data'] if item['id'] == new_id),
                                         'relay mock title')
                        restored = call({'id': 12, 'method': 'thread/read', 'params': {'threadId': new_id, 'includeTurns': True}})
                        self.assertIn('new relay mock conversation', json.dumps(restored))
                    finally:
                        stop_server(server)
                (projects[2] / 'example.txt').write_text('mac change\n')
                wait_for(lambda: (projects[1] / 'example.txt').read_text() == 'mac change\n', 'remote change did not reach the relay')
                time.sleep(3)
                self.assertEqual((projects[0] / 'example.txt').read_text(), 'linux original\n')
                for item in (mock_secret, mock_credential, tracked_secret):
                    item.unlink()
                self.assertEqual(mock_publications.detect_empty_secret_set(shares[0], secret_state, projects[0],
                                                                            'linux', key)['state'], 'PUBLISHED')
                wait_for(lambda: (mock_publications.select(shares[2], 'linux', 'secret-set', 'all', key) or (None, {}))[1].get('empty') is True,
                         'empty secret set did not reach the late peer')
                empty = mock_receiver.receive_discovered_secrets(plain_root, shares[2], key, 'linux', interval=0.1)
                self.assertEqual((empty['state'], empty['missing_count']), ('ATTENTION', 3))
                for item in ('app/.env', 'app/.npmrc', 'Bot/.env'):
                    self.assertTrue((plain_root / item).is_file())
            finally:
                for process in processes:
                    process.terminate()
                try:
                    for process in processes:
                        process.wait(timeout=30)
                finally:
                    for log in logs:
                        log.close()

    def test_encrypted_snapshot_arrives_at_another_syncthing_node(self):
        with tempfile.TemporaryDirectory(prefix='nexus-syncthing-test-') as tmp:
            root = Path(tmp)
            homes = [root / 'home-a', root / 'home-b']
            folders = [root / 'share-a', root / 'share-b']
            projects = [root / 'linux-nexus', root / 'mac-nexus']
            available_ports = unique_ports(4)
            listen, guis = available_ports[:2], available_ports[2:]
            ids = []
            for home in homes:
                subprocess.run(['syncthing', 'generate', '--home', str(home), '--no-port-probing'], capture_output=True, check=True)
                ids.append(subprocess.check_output(['syncthing', 'device-id', '--home', str(home)], text=True).strip())
            for index, home in enumerate(homes):
                folders[index].mkdir()
                projects[index].mkdir()
                shutil.copyfile(Path(__file__).with_name('stignore.example'), projects[index] / '.stignore')
                if index == 0:
                    (projects[index] / 'project.txt').write_text('linux source\n')
                    samples = {
                        'app/package.json': '{}', 'app/package-lock.json': '{}',
                        'app/requirements.txt': 'mock==1\n', 'app/pyproject.toml': '[project]\n',
                        'app/pom.xml': '<project/>', 'app/gradle.lockfile': '# mock\n', 'app/bun.lock': 'mock\n',
                        'app/src/main.py': 'print("mock")\n', 'app/build/source.txt': 'important\n',
                        'app/data/keep.db': 'mock data\n', 'app/.env': 'FAKE_SECRET=mock\n',
                        'app/.npmrc': 'FAKE_NPM_TOKEN=mock\n', 'app/.pypirc': 'FAKE_PYPI_TOKEN=mock\n',
                        'app/.netrc': 'FAKE_NETRC_TOKEN=mock\n', 'app/.git-credentials': 'FAKE_GIT_TOKEN=mock\n',
                        'app/.pgpass': 'FAKE_PGPASS=mock\n', 'app/.htpasswd': 'FAKE_HTPASSWD=mock\n',
                        'app/.env.example': 'FAKE_SAMPLE=mock\n', 'app/.venv/pyvenv.cfg': 'local\n',
                        'app/node_modules/lib/index.js': 'local\n', 'app/.git/config': 'local\n',
                        'app/.pub-cache/example/package.txt': 'local\n', 'app/.flutter-sdk/bin/flutter': 'local\n',
                        'app/.pytest_cache/v/cache/nodeids': 'local\n', 'app/.mypy_cache/state.json': 'local\n',
                        'app/.ruff_cache/state': 'local\n', 'app/.tox/py/bin/python': 'local\n',
                        'app/.nox/test/bin/python': 'local\n', 'app/.DS_Store': 'local\n',
                        'app/Thumbs.db': 'local\n', 'app/desktop.ini': 'local\n',
                        'app/private.pem': 'fake private material\n',
                    }
                    for relative, content in samples.items():
                        path = projects[index] / relative
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text(content)
                config = home / 'config.xml'
                tree = ET.parse(config)
                doc = tree.getroot()
                options = doc.find('options')
                options.find('listenAddress').text = f'tcp://127.0.0.1:{listen[index]}'
                for name in ('globalAnnounceEnabled', 'localAnnounceEnabled', 'relaysEnabled', 'natEnabled', 'startBrowser'):
                    options.find(name).text = 'false'
                options.find('urAccepted').text = '-1'
                options.find('reconnectionIntervalS').text = '2'
                peer = ET.SubElement(doc, 'device', {'id': ids[1-index], 'name': 'test-peer'})
                # Only A dials. Simultaneous TCP opens from two loopback peers
                # can connect the two dialers to each other instead of listeners.
                ET.SubElement(peer, 'address').text = f'tcp://127.0.0.1:{listen[1-index]}' if index == 0 else 'dynamic'
                ET.SubElement(peer, 'numConnections').text = '1'
                folder = ET.SubElement(doc, 'folder', {'id': 'nexus-test', 'label': 'test', 'path': str(folders[index]), 'type': 'sendreceive', 'rescanIntervalS': '1', 'fsWatcherEnabled': 'true'})
                for device_id in ids:
                    ET.SubElement(folder, 'device', {'id': device_id})
                project_folder = ET.SubElement(doc, 'folder', {'id': 'nexus-projects-test', 'label': 'projects', 'path': str(projects[index]), 'type': 'sendonly' if index == 0 else 'sendreceive', 'rescanIntervalS': '1', 'fsWatcherEnabled': 'true'})
                for device_id in ids:
                    ET.SubElement(project_folder, 'device', {'id': device_id})
                tree.write(config, encoding='utf-8', xml_declaration=True)
            processes = []
            logs = []
            offline = mock_syncthing_status.check(homes[0] / 'config.xml', projects[0],
                                                   'nexus-projects-test', guis[0])
            self.assertEqual(offline['state'], 'UNKNOWN')
            try:
                for index, home in enumerate(homes):
                    log = (root / f'node-{index}.log').open('w+')
                    logs.append(log)
                    processes.append(subprocess.Popen(['syncthing', 'serve', '--home', str(home), '--no-browser', '--no-restart', '--no-upgrade', '--gui-address', f'http://127.0.0.1:{guis[index]}'], stdout=log, stderr=subprocess.STDOUT, env={**os.environ, 'STTRACE': 'connections:DEBUG'}))
                codex = root / 'codex'
                sessions = codex / 'sessions'
                sessions.mkdir(parents=True)
                (sessions / 'rollout-test.jsonl').write_text('{"type":"session_meta","payload":{"id":"test"}}\n')
                key = b'x' * 32
                snapshot = ns.snapshot(codex, folders[0], 'linux', key)
                received = folders[1] / 'nexus-sync' / 'snapshots' / 'linux' / snapshot.name
                deadline = time.monotonic() + 35
                while time.monotonic() < deadline:
                    if any(process.poll() is not None for process in processes):
                        self.fail('an isolated Syncthing process exited unexpectedly')
                    try:
                        ns.verify(received, key)
                        break
                    except (OSError, ValueError, ns.SyncError):
                        time.sleep(0.25)
                else:
                    for log in logs:
                        log.flush()
                        log.seek(0)
                        print(log.read()[-5000:])
                    self.fail('the encrypted snapshot did not arrive and verify within 35 seconds')
                self.assertEqual(ns.receive_once(folders[1], 'mac', key), 1)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if ns.status(folders[0], 'linux', key).get('received_by') == ['mac']:
                        break
                    time.sleep(0.25)
                else:
                    self.fail('the signed receipt did not return to the sender')
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    observed = mock_syncthing_status.check(homes[0] / 'config.xml', projects[0],
                                                           'nexus-projects-test', guis[0])
                    if observed['state'] == 'READY':
                        break
                    time.sleep(0.25)
                else:
                    self.fail(f'mock Syncthing did not report ready: {observed}')
                self.assertEqual((observed['connected_peers'], observed['current_peers'],
                                  observed['expected_peers']), (1, 1, 1))
                self.assertFalse(observed['remote_freshness_confirmed'])
                self.assertFalse(observed['cursor_chats_confirmed'])
                summary_state = root / 'summary-state'
                summary_state.mkdir()
                summary_key = root / 'summary-key'
                summary_key.write_bytes(key)
                summary_config = root / 'summary.json'
                summary_config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
                    'nexus': str(projects[0]), 'codex_home': str(codex), 'shared': str(folders[0]),
                    'state_dir': str(summary_state), 'key_file': str(summary_key), 'direction': 'send-only'}))
                summary = subprocess.run([sys.executable, str(Path(ns.__file__)), 'summary', '--config', str(summary_config),
                    '--mock-syncthing-config', str(homes[0] / 'config.xml'), '--mock-syncthing-folder', str(projects[0]),
                    '--mock-syncthing-id', 'nexus-projects-test', '--mock-syncthing-port', str(guis[0])],
                    capture_output=True, text=True, timeout=15)
                self.assertEqual(summary.returncode, 0, summary.stderr)
                self.assertEqual(json.loads(summary.stdout)['mock_syncthing_runtime']['state'], 'READY')
                api_key = ET.parse(homes[0] / 'config.xml').findtext('gui/apikey')
                self.assertNotIn(api_key, summary.stdout + summary.stderr)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    target = projects[1] / 'project.txt'
                    if target.exists() and target.read_text() == 'linux source\n':
                        break
                    time.sleep(0.25)
                else:
                    self.fail('the mock Linux project did not arrive at the second node')
                transferable = ('app/package.json', 'app/package-lock.json', 'app/requirements.txt',
                                'app/pyproject.toml', 'app/pom.xml', 'app/gradle.lockfile', 'app/bun.lock',
                                'app/src/main.py', 'app/build/source.txt', 'app/data/keep.db')
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if all((projects[1] / name).is_file() for name in transferable):
                        break
                    time.sleep(0.25)
                else:
                    self.fail('some mock source files or dependency manifests did not arrive')
                time.sleep(2)
                for name in ('app/.env', 'app/.env.example', 'app/.npmrc', 'app/.pypirc',
                             'app/.netrc', 'app/.git-credentials', 'app/.pgpass', 'app/.htpasswd', 'app/.venv/pyvenv.cfg',
                             'app/node_modules/lib/index.js', 'app/.git/config', 'app/private.pem',
                             'app/.pub-cache/example/package.txt', 'app/.flutter-sdk/bin/flutter',
                             'app/.pytest_cache/v/cache/nodeids', 'app/.mypy_cache/state.json',
                             'app/.ruff_cache/state', 'app/.tox/py/bin/python', 'app/.nox/test/bin/python',
                             'app/.DS_Store', 'app/Thumbs.db', 'app/desktop.ini'):
                    self.assertFalse((projects[1] / name).exists(), f'ignored mock file arrived: {name}')
                for name in ('app/.venv', 'app/node_modules', 'app/.pytest_cache', 'app/.tox', 'app/.nox'):
                    self.assertFalse((projects[1] / name).exists(), f'ignored mock directory arrived: {name}')
                inventory = ns.inventory(projects[1])
                self.assertFalse(inventory['projects']['app']['local']['python_env_present'])
                self.assertFalse(inventory['projects']['app']['local']['node_modules_present'])
                reasons = {item['reason'] for item in ns.project_actions(inventory) if item['project'] == 'app'}
                self.assertIn('Локальное Python-окружение не найдено', reasons)
                self.assertIn('node_modules не найден', reasons)
                target.write_text('remote change\n')
                time.sleep(5)  # Allow rescans and propagation attempts.
                self.assertEqual((projects[0] / 'project.txt').read_text(), 'linux source\n')
            finally:
                for process in processes:
                    process.terminate()
                for process in processes:
                    process.wait(timeout=30)
                for log in logs:
                    log.close()


if __name__ == '__main__':
    unittest.main()
