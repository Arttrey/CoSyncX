"""Opt-in experiment against disposable Codex state; never uses real chats."""
import json
from contextlib import closing
import os
from pathlib import Path
import shutil
import queue
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import nexus_sync as ns
import mock_chat_sync as mock_sync
import mock_chat_preview


CODEX_EXECUTABLE = os.environ.get('NEXUS_TEST_CODEX_EXECUTABLE') or shutil.which('codex')


def read_rpc_response(process, request_id):
    """Read pipe output on Windows and Unix without a selector on a pipe."""
    if not hasattr(process, '_nexus_lines'):
        process._nexus_lines = queue.Queue()
        def pump():
            try:
                for line in process.stdout:
                    process._nexus_lines.put(line)
            finally:
                process._nexus_lines.put(None)
        threading.Thread(target=pump, daemon=True).start()
    deadline = time.monotonic() + 20
    while True:
        try:
            line = process._nexus_lines.get(timeout=max(0, deadline - time.monotonic()))
        except queue.Empty as exc:
            raise AssertionError(f'no response to {request_id}') from exc
        if line is None:
            raise AssertionError(f'app-server closed before responding to {request_id}')
        response = json.loads(line)
        if response.get('id') == request_id:
            return response


def stop_server(process):
    process.stdin.close()
    process.wait(timeout=10)
    process.stdout.close()


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and CODEX_EXECUTABLE, 'requires disposable Codex experiment')
class CodexContractTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'linux', 'mock preview receiver requires Linux')
    def test_preview_maps_windows_and_mac_paths_without_rewriting_chat_text(self):
        sources = [('windows', r'C:\Mock User\Nexus', r'C:\Mock User\Nexus\bot', '000000000096'),
                   ('mac', '/Users/mock/Nexus', '/Users/mock/Nexus/Bot', '000000000097')]
        for peer, source_root, old_cwd, suffix in sources:
            with self.subTest(source=peer), tempfile.TemporaryDirectory(prefix='nexus-path-contract-') as tmp:
                root = Path(tmp)
                shared, nexus, source = root / 'shared', root / 'Nexus', root / 'sender'
                shared.mkdir()
                (nexus / 'Bot').mkdir(parents=True)
                (source / 'sessions').mkdir(parents=True)
                identity = f'019b0000-0000-7000-8000-{suffix}'
                history = source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                historical_text = f'Исторический путь: {old_cwd}'
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': old_cwd,
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                        'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': historical_text}]}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                        'type': 'user_message', 'message': historical_text, 'images': [], 'local_images': [], 'text_elements': []}}]
                history.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows))
                before = history.read_bytes()
                (source / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'mock path title',
                    'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
                key = b'w' * 32
                ns.snapshot(source, shared, peer, key)
                output = root / 'previews'
                mock_chat_preview.init(output, shared, nexus, peer, source_root, key)
                result = mock_chat_preview.prepare(output, key, Path(CODEX_EXECUTABLE))
                self.assertEqual(result['state'], 'PREPARED')
                self.assertFalse(result['verified_in_cursor'])
                stored = ns._chat_catalog(Path(result['profile']))[identity][0]['path']
                restored = [json.loads(line) for line in stored.read_text().splitlines()]
                metadata = next(row for row in restored if row['type'] == 'session_meta')
                message = next(row for row in restored if row['type'] == 'response_item' and row['payload'].get('role') == 'user')
                self.assertEqual(metadata['payload']['cwd'], str(nexus / 'Bot'))
                self.assertEqual(message['payload']['content'][0]['text'], historical_text)
                self.assertEqual(history.read_bytes(), before)

    @unittest.skipUnless(sys.platform == 'linux', 'isolated mock preview requires Linux')
    def test_automatic_preview_indexes_each_snapshot_in_a_new_profile(self):
        with tempfile.TemporaryDirectory(prefix='nexus-chat-preview-contract-') as tmp:
            root = Path(tmp)
            shared, nexus, source = root / 'shared', root / 'Nexus', root / 'sender'
            shared.mkdir()
            (nexus / 'Bot').mkdir(parents=True)
            (source / 'sessions').mkdir(parents=True)
            identity = '019b0000-0000-7000-8000-000000000095'
            chat = source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'mock preview'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'mock preview', 'images': [], 'local_images': [], 'text_elements': []}}]
            chat.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            (source / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'mock preview title',
                                                                    'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            key = b'v' * 32
            ns.snapshot(source, shared, 'linux', key)
            previews = root / 'previews'
            mock_chat_preview.init(previews, shared, nexus, 'linux', '/source/Nexus', key)
            first = mock_chat_preview.prepare(previews, key, Path(CODEX_EXECUTABLE))
            self.assertEqual(first['state'], 'PREPARED')
            self.assertEqual(first['chats_indexed'], 1)
            self.assertFalse(first['verified_in_cursor'])
            first_profile = Path(first['profile'])
            self.assertIn(identity, ns._chat_catalog(first_profile))
            self.assertEqual(mock_chat_preview.prepare(previews, key, Path(CODEX_EXECUTABLE))['state'], 'UNCHANGED')
            key_file = root / 'key'
            key_file.write_bytes(key)
            watcher = subprocess.Popen([sys.executable, mock_chat_preview.__file__, 'watch', '--root', str(previews),
                '--key-file', str(key_file), '--codex-executable', CODEX_EXECUTABLE, '--interval', '0.5'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                chat.write_text(chat.read_text() + json.dumps({'timestamp': '2026-01-01T00:00:02.000Z',
                    'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'mock update',
                                                      'images': [], 'local_images': [], 'text_elements': []}}) + '\n')
                snapshot = ns.snapshot(source, shared, 'linux', key)
                deadline = time.monotonic() + 10
                checkpoint = None
                while time.monotonic() < deadline:
                    self.assertIsNone(watcher.poll())
                    checkpoint = mock_chat_preview._read_signed(previews / mock_chat_preview.CHECKPOINT, key,
                                                                'nexus-mock-chat-preview-checkpoint-v1')
                    if checkpoint['snapshot'] == snapshot.name:
                        break
                    time.sleep(0.1)
                self.assertEqual(checkpoint['snapshot'], snapshot.name)
            finally:
                if watcher.poll() is None:
                    watcher.send_signal(signal.SIGINT)
                watcher.wait(timeout=10)
            second_profile = previews / checkpoint['profile']
            self.assertNotEqual(first_profile, second_profile)
            self.assertIn(identity, ns._chat_catalog(second_profile))
            self.assertTrue(first_profile.is_dir())

    @unittest.skipUnless(sys.platform == 'linux', 'mock observer experiment requires Linux')
    def test_running_observer_publishes_changes_while_mock_codex_is_open(self):
        with tempfile.TemporaryDirectory(prefix='nexus-open-sender-contract-') as tmp:
            root = Path(tmp)
            nexus, profile, shared, state = (root / name for name in ('Nexus', 'profile', 'shared', 'state'))
            for directory in (nexus, profile, shared, state):
                directory.mkdir()
            project = nexus / 'Bot'
            project.mkdir()
            sessions = profile / 'sessions'
            sessions.mkdir()
            identity = '019b0000-0000-7000-8000-000000000097'
            rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic open chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'synthetic open chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            (sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in rows))
            key = b'o' * 32
            key_file = root / 'key'
            key_file.write_bytes(key)
            config = root / 'config.json'
            config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
                'nexus': str(nexus), 'codex_home': str(profile), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key_file), 'interval_seconds': 10, 'direction': 'send-only'}))
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            observer = None
            def call(number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response, str(response.get('error')))
                return response['result']
            def wait_latest(previous=None):
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    self.assertIsNone(server.poll())
                    self.assertIsNone(observer.poll())
                    try:
                        latest = ns.latest_authenticated_snapshot(shared / 'nexus-sync' / 'snapshots' / 'linux', 'linux', key)
                        if latest and latest[0].name != previous:
                            return latest
                    except (OSError, ValueError, ns.SyncError):
                        pass
                    time.sleep(0.2)
                self.fail('mock observer did not publish a complete snapshot')
            try:
                call(1, 'initialize', {'clientInfo': {'name': 'nexus_open_sender', 'version': '0.1'},
                                       'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                call(2, 'thread/resume', {'threadId': identity, 'cwd': str(project)})
                observer = subprocess.Popen([sys.executable, str(Path(ns.__file__)), 'run', '--config', str(config)],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                first, _ = wait_latest()
                active_id = '019b0000-0000-7000-8000-000000000099'
                active = sessions / f'rollout-2026-01-01T00-00-00-{active_id}.jsonl'
                payload = {'id': active_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                           'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                           'model_provider': 'openai'}
                with active.open('wb') as writer:
                    writer.write(b'{"type":"session_meta","payload":')
                    writer.flush()
                    partial, partial_manifest = wait_latest(first.name)
                    self.assertGreaterEqual(partial_manifest['omitted_files'], 1)
                    self.assertNotIn(active.relative_to(profile).as_posix(),
                                     [item['path'] for item in partial_manifest['files']])
                    self.assertIsNone(server.poll())
                    writer.write(json.dumps(payload).encode('utf-8') + b'}\n')
                    writer.flush()
                    finished, finished_manifest = wait_latest(partial.name)
                    self.assertEqual(finished_manifest['omitted_files'], 0)
                    self.assertIn(active.relative_to(profile).as_posix(),
                                  [item['path'] for item in finished_manifest['files']])
                    self.assertIsNone(server.poll())
                call(3, 'thread/name/set', {'threadId': identity, 'name': 'synthetic renamed while open'})
                second, manifest = wait_latest(finished.name)
                names = ns._parse_title_index(ns._snapshot_title_data(second, manifest, key))
                self.assertEqual(next(row['thread_name'] for row in names if row['id'] == identity),
                                 'synthetic renamed while open')
                self.assertIsNone(server.poll())
            finally:
                if observer is not None and observer.poll() is None:
                    observer.send_signal(signal.SIGINT)
                    observer.wait(timeout=20)
                stop_server(server)

    def test_index_preview_checks_archived_chat_in_state_database(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-archived-') as tmp:
            root = Path(tmp)
            incoming = root / 'incoming'
            archive = incoming / 'archived_sessions'
            archive.mkdir(parents=True)
            local, shared, state = root / 'local', root / 'shared', root / 'state'
            for folder in (local, shared, state):
                folder.mkdir()
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            key_file = root / 'key'
            key_file.write_bytes(b'a' * 32)
            thread_id = '019b0000-0000-7000-8000-000000000076'
            rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                 {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/old/Nexus/Bot',
                  'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                 {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic archived chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                 {'type': 'user_message', 'message': 'synthetic archived chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            (archive / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
            (incoming / 'session_index.jsonl').write_text(json.dumps({'id': thread_id,
                'thread_name': 'archived mock title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                'nexus': str(project.parent), 'codex_home': str(local), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key_file)})
            report = ns.index_preview(incoming, root / 'preview', '/old/Nexus', cfg, Path(CODEX_EXECUTABLE))
            self.assertFalse(report['partial'])
            self.assertEqual(report['db_unlisted'], [])
            self.assertEqual(report['title_unmatched'], [])
            relocated = root / 'relocated-archive-preview'
            (root / 'preview').rename(relocated)
            report = ns.index_preview(relocated, relocated, str(project.parent), cfg, Path(CODEX_EXECUTABLE), reindex_mock=True)
            self.assertFalse(report['partial'])
            self.assertEqual(report['db_unlisted'], [])
            self.assertEqual(report['title_unmatched'], [])
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(relocated)})
            try:
                def call(request):
                    server.stdin.write((json.dumps(request) + '\n').encode())
                    server.stdin.flush()
                    return read_rpc_response(server, request['id'])
                self.assertIn('result', call({'id': 5, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 6, 'method': 'thread/list', 'params': {'archived': True, 'useStateDbOnly': True}})
                item = next(item for item in listing['result']['data'] if item['id'] == thread_id)
                self.assertEqual(item['name'], 'archived mock title')
                self.assertTrue(Path(item['path']).is_relative_to(relocated))
                self.assertIn('archived_sessions', Path(item['path']).parts)
                reading = call({'id': 7, 'method': 'thread/read', 'params': {'threadId': thread_id, 'includeTurns': True}})
                self.assertIn('synthetic archived chat', json.dumps(reading['result']))
            finally:
                stop_server(server)

    def test_resume_external_path_does_not_import_into_disposable_profile(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-external-resume-') as tmp:
            root = Path(tmp)
            source, target = root / 'incoming', root / 'target'
            source.mkdir()
            target.mkdir()
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000077'
            rollout = source / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                 {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                  'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                 {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic external path'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                 {'type': 'user_message', 'message': 'synthetic external path', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            original = rollout.read_bytes()
            def start():
                return subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(target)})
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                return read_rpc_response(server, number)
            def init(server, number):
                response = call(server, number, 'initialize', {'clientInfo': {'name': 'nexus_path_test', 'version': '0.1'},
                    'capabilities': {'experimentalApi': True}})
                self.assertIn('result', response)
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
            server = start()
            try:
                init(server, 1)
                resumed = call(server, 2, 'thread/resume', {'threadId': thread_id, 'path': str(rollout), 'cwd': str(project)})
                self.assertEqual(resumed['result']['thread']['id'], thread_id)
                listing = call(server, 3, 'thread/list', {'limit': 100})
                self.assertNotIn(thread_id, json.dumps(listing.get('result', {})))
            finally:
                stop_server(server)
            self.assertNotEqual(rollout.read_bytes(), original)
            self.assertEqual(list(target.rglob('rollout-*.jsonl')), [])
            server = start()
            try:
                init(server, 4)
                listing = call(server, 5, 'thread/list', {'limit': 100})
                self.assertNotIn(thread_id, json.dumps(listing.get('result', {})))
                read = call(server, 6, 'thread/read', {'threadId': thread_id, 'includeTurns': True})
                self.assertIn('synthetic external path', json.dumps(read.get('result', {})))
            finally:
                stop_server(server)
            hidden = source / 'held-synthetic-history'
            rollout.rename(hidden)
            server = start()
            try:
                init(server, 7)
                read = call(server, 8, 'thread/read', {'threadId': thread_id, 'includeTurns': True})
                self.assertNotIn('synthetic external path', json.dumps(read.get('result', {})))
            finally:
                stop_server(server)
                hidden.rename(rollout)

    def test_external_agent_import_ack_is_not_success_for_raw_codex_rollout(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-import-') as tmp:
            root = Path(tmp)
            home, target = root / 'home', root / 'target'
            home.mkdir()
            target.mkdir()
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000099'
            source = root / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                     {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                      'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic import'}]}},
                    {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'synthetic import', 'images': [], 'local_images': [], 'text_elements': []}}]
            source.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            original = source.read_bytes()
            env = {**os.environ, 'HOME': str(home), 'USERPROFILE': str(home), 'CODEX_HOME': str(target)}
            env.pop('OPENAI_API_KEY', None)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env=env)
            messages = queue.Queue()
            def pump():
                for line in server.stdout:
                    messages.put(json.loads(line))
                messages.put(None)
            threading.Thread(target=pump, daemon=True).start()
            backlog = []
            def send(value):
                server.stdin.write((json.dumps(value) + '\n').encode())
                server.stdin.flush()
            def receive(predicate):
                deadline = time.monotonic() + 20
                while True:
                    for position, message in enumerate(backlog):
                        if predicate(message):
                            return backlog.pop(position)
                    try:
                        message = messages.get(timeout=max(0, deadline - time.monotonic()))
                    except queue.Empty as exc:
                        raise AssertionError('no import response or completion') from exc
                    if message is None:
                        raise AssertionError('app-server closed during import')
                    backlog.append(message)
            try:
                send({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_import_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}})
                self.assertIn('result', receive(lambda msg: msg.get('id') == 1))
                send({'method': 'initialized', 'params': {}})
                send({'id': 2, 'method': 'externalAgentConfig/import', 'params': {'migrationSource': 'codex',
                    'migrationItems': [{'itemType': 'SESSIONS', 'description': 'synthetic', 'cwd': str(project),
                        'details': {'sessions': [{'cwd': str(project), 'path': str(source), 'title': 'synthetic'}]}}]}})
                ack = receive(lambda msg: msg.get('id') == 2)
                import_id = ack['result']['importId']
                completed = receive(lambda msg: msg.get('method') == 'externalAgentConfig/import/completed'
                                    and msg.get('params', {}).get('importId') == import_id)
                outcomes = completed['params']['itemTypeResults']
                self.assertEqual(sum(len(item['successes']) for item in outcomes), 0)
                self.assertTrue(any(failure.get('failureStage') == 'session_missing'
                                    for item in outcomes for failure in item['failures']))
                send({'id': 3, 'method': 'thread/list', 'params': {'limit': 100}})
                listing = receive(lambda msg: msg.get('id') == 3)
                self.assertNotIn(thread_id, json.dumps(listing.get('result', {})))
            finally:
                stop_server(server)
            self.assertEqual(source.read_bytes(), original)

    def test_fork_by_external_path_in_disposable_profile(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-fork-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            target = root / 'target'
            source.mkdir()
            target.mkdir()
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000088'
            rollout = source / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                 {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                  'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                 {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic fork source'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                 {'type': 'user_message', 'message': 'synthetic fork source', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            original = rollout.read_bytes()
            def start():
                return subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(target)})
            server = start()
            try:
                def call(request):
                    server.stdin.write((json.dumps(request) + '\n').encode())
                    server.stdin.flush()
                    return read_rpc_response(server, request['id'])
                self.assertIn('result', call({'id': 1, 'method': 'initialize', 'params':
                    {'clientInfo': {'name': 'nexus_fork_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                fork = call({'id': 2, 'method': 'thread/fork', 'params': {'threadId': thread_id,
                    'path': str(rollout), 'cwd': str(project), 'deferGoalContinuation': True}})
                self.assertIn('result', fork, str(fork.get('error')))
                fork_id = fork['result']['thread']['id']
                self.assertNotEqual(fork_id, thread_id)
                self.assertEqual(fork['result']['cwd'], str(project))
                read = call({'id': 3, 'method': 'thread/read', 'params': {'threadId': fork_id, 'includeTurns': True}})
                self.assertIn('synthetic fork source', json.dumps(read.get('result', {})))
                injected = call({'id': 6, 'method': 'thread/inject_items', 'params': {'threadId': fork_id,
                    'items': [{'type': 'message', 'role': 'user', 'content':
                               [{'type': 'input_text', 'text': 'synthetic injected continuation'}]}]}})
                self.assertIn('result', injected, str(injected.get('error')))
                reread = call({'id': 7, 'method': 'thread/read', 'params': {'threadId': fork_id, 'includeTurns': True}})
                self.assertNotIn('synthetic injected continuation', json.dumps(reread.get('result', {})))
            finally:
                stop_server(server)
            self.assertEqual(rollout.read_bytes(), original)
            server = start()
            try:
                self.assertIn('result', call({'id': 4, 'method': 'initialize', 'params':
                    {'clientInfo': {'name': 'nexus_fork_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 5, 'method': 'thread/list', 'params': {'limit': 100}})
                self.assertIn(fork_id, json.dumps(listing.get('result', {})))
                read = call({'id': 8, 'method': 'thread/read', 'params': {'threadId': fork_id, 'includeTurns': True}})
                self.assertNotIn('synthetic injected continuation', json.dumps(read.get('result', {})))
            finally:
                stop_server(server)

    def test_preview_merge_is_readable_in_disposable_codex(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-preview-') as tmp:
            root = Path(tmp)
            local = root / 'local'
            source = root / 'source'
            local_sessions = local / 'sessions' / '2026' / '01' / '01'
            source_sessions = source / 'sessions' / '2026' / '01' / '01'
            local_sessions.mkdir(parents=True)
            source_sessions.mkdir(parents=True)
            nexus = root / 'Nexus'
            project = nexus / 'Bot'
            project.mkdir(parents=True)
            shared = root / 'shared'
            shared.mkdir()
            state = root / 'state'
            state.mkdir()
            key_file = root / 'key'
            key_file.write_bytes(b'p' * 32)
            ids = ['019b0000-0000-7000-8000-000000000011',
                   '019b0000-0000-7000-8000-000000000012',
                   '019b0000-0000-7000-8000-000000000013',
                   '019b0000-0000-7000-8000-000000000014']
            def write_chat(folder, thread_id, cwd, messages):
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                         'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z',
                                     'cwd': cwd, 'originator': 'codex_vscode', 'cli_version': '0.1.0',
                                     'source': 'vscode', 'model_provider': 'openai'}}]
                for index, message in enumerate(messages, 1):
                    stamp = f'2026-01-01T00:00:0{index}.000Z'
                    rows.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message',
                        'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                        {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                        'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
                output = folder / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
                output.write_text(''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
                return output
            local_chat = write_chat(local_sessions, ids[0], str(project), ['first synthetic'])
            original = local_chat.read_bytes()
            write_chat(local_sessions, ids[1], str(project), ['local synthetic'])
            write_chat(source_sessions, ids[0], '/old/Nexus/Bot', ['first synthetic', 'later synthetic'])
            write_chat(source_sessions, ids[2], '/old/Nexus/Bot', ['new synthetic'])
            (source / 'session_index.jsonl').write_text(json.dumps({'id': ids[2],
                'thread_name': 'synthetic chosen title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                'nexus': str(nexus), 'codex_home': str(local), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key_file), 'direction': 'send-receive',
                'peer_roots': {'linux': '/old/Nexus', 'windows': r'D:\Nexus'}})
            snapshot = ns.snapshot(source, shared, 'linux', b'p' * 32)
            self.assertEqual(ns.prepare_received_once(cfg, b'p' * 32), 1)
            preview = root / 'preview'
            report = ns.preview_merge(cfg, b'p' * 32, 'linux', snapshot.name, preview)
            self.assertFalse(report['applied'])
            self.assertFalse(report['partial'])
            self.assertEqual(report['unresolved_chats'], 0)
            legacy = ns.index_preview(source, root / 'legacy-index-preview', '/old/Nexus', cfg, Path(CODEX_EXECUTABLE))
            self.assertEqual(legacy['resumed'], 2)
            self.assertEqual(legacy['legacy_responded'], 2)
            self.assertEqual(legacy['pagination_responded'], 0)
            self.assertEqual(legacy['title_unmatched'], [])
            self.assertEqual(local_chat.read_bytes(), original)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(preview)})
            try:
                def call(request):
                    server.stdin.write((json.dumps(request) + '\n').encode())
                    server.stdin.flush()
                    return read_rpc_response(server, request['id'])
                self.assertIn('result', call({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 2, 'method': 'thread/list', 'params': {'limit': 100}})
                self.assertIn('result', listing, str(listing.get('error')))
                self.assertTrue(all(thread_id in json.dumps(listing['result']) for thread_id in ids[:3]))
                thread = call({'id': 3, 'method': 'thread/read', 'params': {'threadId': ids[0], 'includeTurns': True}})
                self.assertIn('result', thread, str(thread.get('error')))
                self.assertIn('later synthetic', json.dumps(thread['result']))
            finally:
                stop_server(server)
            windows = root / 'windows-source'
            windows_sessions = windows / 'sessions' / '2026' / '01' / '01'
            windows_sessions.mkdir(parents=True)
            write_chat(windows_sessions, ids[3], r'D:\Nexus\Bot', ['windows synthetic'])
            (windows / 'session_index.jsonl').write_text(json.dumps({'id': ids[3],
                'thread_name': 'windows mock title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            win_snapshot = ns.snapshot(windows, shared, 'windows', b'p' * 32)
            combined = root / 'combined-preview'
            multi = ns.index_multi_snapshot_preview(cfg, b'p' * 32,
                [('linux', snapshot.name), ('windows', win_snapshot.name)], combined, Path(CODEX_EXECUTABLE))
            self.assertFalse(multi['partial'])
            self.assertEqual(multi['title_unmatched'], [])
            self.assertEqual(multi['resumed'], 4)
            self.assertEqual(local_chat.read_bytes(), original)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(combined)})
            try:
                self.assertIn('result', call({'id': 10, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 11, 'method': 'thread/list', 'params': {'limit': 100}})
                self.assertTrue(all(thread_id in json.dumps(listing.get('result', {})) for thread_id in ids))
                db_listing = call({'id': 13, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                self.assertIn('result', db_listing, str(db_listing.get('error')))
                self.assertTrue(all(thread_id in json.dumps(db_listing['result']) for thread_id in ids))
                names = {item['id']: item.get('name') for item in db_listing['result']['data']}
                self.assertEqual(names[ids[2]], 'synthetic chosen title')
                self.assertEqual(names[ids[3]], 'windows mock title')
                thread = call({'id': 12, 'method': 'thread/read', 'params': {'threadId': ids[3], 'includeTurns': True}})
                self.assertIn('windows synthetic', json.dumps(thread.get('result', {})))
            finally:
                stop_server(server)
            # Two devices independently extend the same synthetic chat.
            conflict_source = write_chat(windows_sessions, ids[0], r'D:\Nexus\bot',
                                         ['first synthetic', 'windows conflicting continuation'])
            source_before = conflict_source.read_bytes()
            conflict_snapshot = ns.snapshot(windows, shared, 'windows', b'p' * 32)
            conflict_preview = root / 'conflicting-preview'
            conflict_report = ns.index_multi_snapshot_preview(cfg, b'p' * 32,
                [('linux', snapshot.name), ('windows', conflict_snapshot.name)], conflict_preview, Path(CODEX_EXECUTABLE))
            self.assertTrue(conflict_report['partial'])
            self.assertEqual(conflict_report['unresolved_chats'], 1)
            self.assertIn('CONFLICT', [item['action'] for item in conflict_report['actions']])
            self.assertEqual(local_chat.read_bytes(), original)
            self.assertEqual(conflict_source.read_bytes(), source_before)
            quarantined = root / 'conflict-quarantine'
            ns.extract(conflict_snapshot, quarantined, b'p' * 32)
            self.assertEqual((quarantined / conflict_source.relative_to(windows)).read_bytes(), source_before)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(conflict_preview)})
            try:
                self.assertIn('result', call({'id': 30, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                selected = call({'id': 31, 'method': 'thread/read', 'params': {'threadId': ids[0], 'includeTurns': True}})
                self.assertIn('later synthetic', json.dumps(selected.get('result', {})))
                self.assertNotIn('windows conflicting continuation', json.dumps(selected.get('result', {})))
            finally:
                stop_server(server)
            alternate = root / 'windows-conflict-preview'
            alternate_report = ns.index_snapshot_preview(cfg, b'p' * 32, 'windows', conflict_snapshot.name,
                                                          alternate, Path(CODEX_EXECUTABLE))
            self.assertFalse(alternate_report['local_included'])
            self.assertFalse(alternate_report['partial'])
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(alternate)})
            try:
                self.assertIn('result', call({'id': 32, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                alternative = call({'id': 33, 'method': 'thread/read', 'params': {'threadId': ids[0], 'includeTurns': True}})
                self.assertIn('windows conflicting continuation', json.dumps(alternative.get('result', {})))
                self.assertNotIn('later synthetic', json.dumps(alternative.get('result', {})))
            finally:
                stop_server(server)
            relocated = root / 'relocated-combined-preview'
            combined.rename(relocated)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(relocated)})
            try:
                self.assertIn('result', call({'id': 14, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                moved_listing = call({'id': 16, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                self.assertNotIn(ids[3], {item['id'] for item in moved_listing['result']['data']})
                moved = call({'id': 15, 'method': 'thread/read', 'params': {'threadId': ids[3], 'includeTurns': True}})
                self.assertIn('windows synthetic', json.dumps(moved.get('result', {})))
                resumed = call({'id': 17, 'method': 'thread/resume', 'params': {'threadId': ids[3], 'cwd': str(project)}})
                self.assertIn('error', resumed)
            finally:
                stop_server(server)
            config_file = root / 'reindex-config.json'
            config_file.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                'nexus': str(nexus), 'codex_home': str(local), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key_file), 'direction': 'send-receive',
                'peer_roots': {'linux': '/old/Nexus', 'windows': r'D:\Nexus'}}))
            invocation = subprocess.run([sys.executable, str(Path(ns.__file__)), 'reindex-mock-preview',
                '--profile', str(relocated), '--config', str(config_file), '--codex-executable', CODEX_EXECUTABLE],
                capture_output=True, text=True)
            self.assertEqual(invocation.returncode, 0, invocation.stderr)
            reindexed = json.loads(invocation.stdout)
            self.assertTrue(reindexed['mock_reindexed'])
            self.assertFalse(reindexed['partial'])
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(relocated)})
            try:
                self.assertIn('result', call({'id': 20, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 21, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                moved_item = next(item for item in listing['result']['data'] if item['id'] == ids[3])
                self.assertTrue(Path(moved_item['path']).is_relative_to(relocated), moved_item['path'])
                self.assertEqual(moved_item['name'], 'windows mock title')
                resumed = call({'id': 22, 'method': 'thread/resume', 'params': {'threadId': ids[3], 'cwd': str(project)}})
                self.assertIn('result', resumed, str(resumed.get('error')))
            finally:
                stop_server(server)

    def test_index_preview_follows_multiple_real_codex_turn_pages(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-pages-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            folder = source / 'sessions' / '2026' / '01' / '01'
            folder.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000099'
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                     {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                      'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}}]
            for index in range(105):
                stamp = f'2026-01-01T00:{index // 60:02d}:{index % 60:02d}.001Z'
                message = f'synthetic page message {index}'
                rows.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message',
                    'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                    {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                    'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
            (folder / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in rows))
            migration = subprocess.run([CODEX_EXECUTABLE, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, text=True)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            local, shared, state = root / 'local', root / 'shared', root / 'state'
            for directory in (local, shared, state):
                directory.mkdir()
            key = root / 'key'
            key.write_bytes(b'a' * 32)
            cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mock',
                'nexus': str(project.parent), 'codex_home': str(local), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key)})
            report = ns.index_preview(source, root / 'preview', '/source/Nexus', cfg, Path(CODEX_EXECUTABLE))
            self.assertGreaterEqual(report['turn_pages_checked'], 2)
            self.assertEqual(report['turn_items_returned'], 105)
            self.assertFalse(report['contents_verified'])

    @unittest.skipUnless(sys.platform == 'linux', 'mock receiver requires Linux')
    def test_legacy_continuation_after_settings_from_two_codex_profiles(self):
        with tempfile.TemporaryDirectory(prefix='nexus-two-codex-settings-') as tmp:
            root = Path(tmp)
            shared, source, source_nexus, nexus = (root / x for x in ('shared', 'sender', 'source-nexus', 'Nexus'))
            shared.mkdir()
            (source / 'sessions').mkdir(parents=True)
            (source_nexus / 'Bot').mkdir(parents=True)
            (nexus / 'Bot').mkdir(parents=True)
            profile = root / 'profile'
            key = b't' * 32
            mock_sync.init_mock_profile(profile, key, shared)
            identity = '019b0000-0000-7000-8000-000000000079'
            source_file = source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            initial = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(source_nexus / 'Bot'),
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'initial mock'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'initial mock', 'images': [], 'local_images': [], 'text_elements': []}}]
            source_file.write_text(''.join(json.dumps(row) + '\n' for row in initial))
            def start(home):
                return subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(home)})
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response, str(response.get('error')))
                return response['result']
            sender = start(source)
            try:
                call(sender, 1, 'initialize', {'clientInfo': {'name': 'nexus_sender_mock', 'version': '0.1'},
                                             'capabilities': {'experimentalApi': True}})
                sender.stdin.write(b'{"method":"initialized","params":{}}\n')
                sender.stdin.flush()
                call(sender, 2, 'thread/resume', {'threadId': identity, 'cwd': str(source_nexus / 'Bot')})
            finally:
                stop_server(sender)
            self.assertTrue(any(row.get('payload', {}).get('type') == 'thread_settings_applied'
                                for row in map(json.loads, source_file.read_text().splitlines())))
            ns.snapshot(source, shared, 'linux', key)
            first = mock_sync.sync_all_mock(profile, shared, key, 'linux', str(source_nexus), nexus, Path(CODEX_EXECUTABLE))
            self.assertEqual(first['state'], 'COMPLETE')
            before = ns._chat_catalog(profile)[identity][0]['path'].read_text()
            self.assertGreater(before.count('thread_settings_applied'), source_file.read_text().count('thread_settings_applied'))
            continuation = [
                {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'later mock'}]}},
                {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'later mock', 'images': [], 'local_images': [], 'text_elements': []}}]
            source_file.write_text(source_file.read_text() + ''.join(json.dumps(row) + '\n' for row in continuation))
            ns.snapshot(source, shared, 'linux', key)
            result = mock_sync.sync_all_mock(profile, shared, key, 'linux', str(source_nexus), nexus,
                                            Path(CODEX_EXECUTABLE), allow_mock_settings_rebase=True)
            self.assertEqual(result['state'], 'COMPLETE', result)
            self.assertEqual(result['results'], {'REBASED_MOCK': 1})
            receiver = start(profile)
            try:
                call(receiver, 3, 'initialize', {'clientInfo': {'name': 'nexus_receiver_mock', 'version': '0.1'},
                                                'capabilities': {'experimentalApi': True}})
                receiver.stdin.write(b'{"method":"initialized","params":{}}\n')
                receiver.stdin.flush()
                history = call(receiver, 4, 'thread/read', {'threadId': identity, 'includeTurns': True})
                self.assertIn('initial mock', json.dumps(history))
                self.assertIn('later mock', json.dumps(history))
            finally:
                stop_server(receiver)

    @unittest.skipUnless(sys.platform == 'linux', 'mock receiver requires Linux')
    def test_paginated_update_can_rebase_after_mock_local_settings(self):
        with tempfile.TemporaryDirectory(prefix='nexus-paginated-receiver-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            sessions = source / 'sessions'
            sessions.mkdir(parents=True)
            identity = '019b0000-0000-7000-8000-000000000087'
            rollout = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            records = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}}]
            for number in (1, 2):
                stamp = f'2026-01-01T00:0{number}:00.000Z'
                message = f'synthetic paginated update {number}'
                records.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message',
                    'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                    {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                    'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8')
            migration = subprocess.run([CODEX_EXECUTABLE, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, text=True, timeout=30)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            self.assertEqual(json.loads(migration.stdout)['outcomes'][0]['status'], 'migrated')
            lines = rollout.read_text(encoding='utf-8').splitlines(keepends=True)
            initial = [line for line in lines if json.loads(line).get('timestamp', '') < '2026-01-01T00:02:00']
            later = [line for line in lines if json.loads(line).get('timestamp', '') >= '2026-01-01T00:02:00']
            self.assertTrue(later)
            rollout.write_text(''.join(initial), encoding='utf-8')
            shared, nexus, profile = root / 'shared', root / 'Nexus', root / 'profile'
            shared.mkdir()
            (nexus / 'Bot').mkdir(parents=True)
            key = b'p' * 32
            mock_sync.init_mock_profile(profile, key, shared)
            (source / 'session_index.jsonl').write_text(json.dumps({'id': identity,
                'thread_name': 'synthetic paginated title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            ns.snapshot(source, shared, 'linux', key)
            first = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus, Path(CODEX_EXECUTABLE))
            self.assertEqual(first['state'], 'COMPLETE', first)
            local_file = ns._chat_catalog(profile, include_unsupported=True)[identity][0]['path']
            local_before = local_file.read_bytes()
            with rollout.open('a', encoding='utf-8') as stream:
                stream.writelines(later)
            ns.snapshot(source, shared, 'linux', key)
            second = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus,
                                             Path(CODEX_EXECUTABLE))
            self.assertEqual(second['state'], 'PENDING', second)
            self.assertEqual(local_file.read_bytes(), local_before)
            merged = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus,
                Path(CODEX_EXECUTABLE), allow_mock_settings_rebase=True)
            self.assertEqual(merged['state'], 'COMPLETE', merged)
            self.assertEqual(merged['results'], {'REBASED_MOCK': 1})
            backup = next((profile / '.nexus-mock-backups' / identity).glob('*.jsonl'))
            self.assertEqual(backup.read_bytes(), local_before)
            preview = root / 'previews'
            mock_chat_preview.init(preview, shared, nexus, 'linux', '/source/Nexus', key)
            outcome = mock_chat_preview.prepare(preview, key, Path(CODEX_EXECUTABLE))
            self.assertEqual(outcome['state'], 'PREPARED', outcome)
            self.assertFalse(outcome['verified_in_cursor'])
            for codex_home in (profile, Path(outcome['profile'])):
                server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(codex_home)})
                try:
                    def call(number, method, params):
                        server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                        server.stdin.flush()
                        return read_rpc_response(server, number)
                    self.assertIn('result', call(1, 'initialize', {'clientInfo': {'name': 'nexus_paginated', 'version': '0.1'},
                        'capabilities': {'experimentalApi': True}}))
                    server.stdin.write(b'{"method":"initialized","params":{}}\n')
                    server.stdin.flush()
                    turns = call(2, 'thread/turns/list', {'threadId': identity, 'itemsView': 'full'})
                    self.assertIn('result', turns)
                    self.assertIn('synthetic paginated update 1', json.dumps(turns['result']))
                    self.assertIn('synthetic paginated update 2', json.dumps(turns['result']))
                finally:
                    stop_server(server)

    def test_paginated_synthetic_chat_is_archived_but_not_prepared(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-paginated-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            folder = source / 'sessions' / '2026' / '01' / '01'
            folder.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000006'
            rollout = folder / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            records = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot', 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic paginated message'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'synthetic paginated message', 'images': [], 'local_images': [], 'text_elements': []}},
                {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'second synthetic paginated message'}]}},
                {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'second synthetic paginated message', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8')
            migration = subprocess.run([CODEX_EXECUTABLE, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, text=True)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            self.assertEqual(json.loads(migration.stdout)['outcomes'][0]['status'], 'migrated')
            self.assertTrue((source / 'thread_history_1.sqlite').exists())
            (source / 'session_index.jsonl').write_text(json.dumps({'id': thread_id,
                'thread_name': 'synthetic chosen title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            shared = root / 'shared'
            shared.mkdir()
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            key = b'q' * 32
            snapshot = ns.snapshot(source, shared, 'linux', key)
            self.assertEqual(ns.verify(snapshot, key)['title_index']['state'], 'captured')
            self.assertEqual(ns.verify(snapshot, key)['paginated_files'], 1)
            self.assertEqual(ns.status(shared, 'linux', key)['state'], 'snapshot_unverified')
            quarantine = root / 'quarantine'
            ns.extract(snapshot, quarantine, key)
            with self.assertRaisesRegex(ns.SyncError, 'Пагинированная история'):
                ns.prepare_chats(quarantine, root / 'refused', '/source/Nexus', str(project.parent))
            # Explicit experimental preview stays outside every real profile.
            target = root / 'target'
            target_rollout = target / rollout.relative_to(source)
            self.assertEqual(ns.prepare_chats(quarantine, target, '/source/Nexus', str(project.parent),
                                              allow_paginated_preview=True), 1)
            meta = json.loads(target_rollout.read_text(encoding='utf-8').splitlines()[0])
            self.assertEqual(meta['payload']['history_mode'], 'paginated')
            self.assertEqual(meta['payload']['cwd'], str(project))
            self.assertFalse((target / 'thread_history_1.sqlite').exists())
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(target)})
            try:
                def call(request):
                    server.stdin.write((json.dumps(request) + '\n').encode())
                    server.stdin.flush()
                    return read_rpc_response(server, request['id'])
                self.assertIn('result', call({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 2, 'method': 'thread/list', 'params': {'limit': 100}})
                self.assertIn(thread_id, json.dumps(listing))
                thread = call({'id': 3, 'method': 'thread/read', 'params': {'threadId': thread_id, 'includeTurns': True}})
                self.assertIn('synthetic paginated message', json.dumps(thread))
                result = call({'id': 4, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', result, str(result.get('error')))
                self.assertNotIn('synthetic paginated message', json.dumps(result['result']))
                resumed = call({'id': 5, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'cwd': str(project)}})
                self.assertIn('result', resumed, str(resumed.get('error')))
                after = call({'id': 6, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', after, str(after.get('error')))
                self.assertIn('synthetic paginated message', json.dumps(after['result']))
                self.assertIn('second synthetic paginated message', json.dumps(after['result']))
            finally:
                stop_server(server)
            self.assertTrue((target / 'thread_history_1.sqlite').is_file())
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(target)})
            try:
                self.assertIn('result', call({'id': 7, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                restarted = call({'id': 8, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', restarted, str(restarted.get('error')))
                self.assertIn('synthetic paginated message', json.dumps(restarted['result']))
                self.assertIn('second synthetic paginated message', json.dumps(restarted['result']))
            finally:
                stop_server(server)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(source)})
            try:
                self.assertIn('result', call({'id': 9, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                original_turns = call({'id': 10, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', original_turns, str(original_turns.get('error')))
                self.assertEqual(original_turns['result']['data'], restarted['result']['data'])
            finally:
                stop_server(server)
            local_home = root / 'other-local-profile'
            local_home.mkdir()
            state = root / 'state'
            state.mkdir()
            key_file = root / 'key'
            key_file.write_bytes(key)
            cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                'nexus': str(project.parent), 'codex_home': str(local_home), 'shared': str(shared),
                'state_dir': str(state), 'key_file': str(key_file)})
            indexed = root / 'indexed-preview'
            outcome = ns.index_preview(quarantine, indexed, '/source/Nexus', cfg, Path(CODEX_EXECUTABLE))
            self.assertEqual(outcome['resumed'], 1)
            self.assertEqual(outcome['pagination_responded'], 1)
            self.assertEqual(outcome['title_unmatched'], [])
            self.assertFalse(outcome['applied'])
            self.assertFalse(outcome['contents_verified'])
            self.assertTrue((indexed / 'thread_history_1.sqlite').is_file())
            self.assertFalse(list(local_home.iterdir()))
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(indexed)})
            try:
                self.assertIn('result', call({'id': 20, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                indexed_turns = call({'id': 21, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                listing = call({'id': 22, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                self.assertEqual(next(item.get('name') for item in listing['result']['data'] if item['id'] == thread_id), 'synthetic chosen title')
                self.assertIn('second synthetic paginated message', json.dumps(indexed_turns))
                self.assertEqual(indexed_turns['result']['data'], original_turns['result']['data'])
                archived = call({'id': 23, 'method': 'thread/archive', 'params': {'threadId': thread_id}})
                self.assertIn('result', archived, str(archived.get('error')))
            finally:
                stop_server(server)
            self.assertTrue(list((indexed / 'archived_sessions').rglob(f'rollout-*{thread_id}.jsonl')))
            relocated = root / 'relocated-paginated-archive'
            indexed.rename(relocated)
            archived_rollout = next((relocated / 'archived_sessions').rglob(f'rollout-*{thread_id}.jsonl'))
            original_archived = archived_rollout.read_bytes()
            report = ns.index_preview(relocated, relocated, str(project.parent), cfg, Path(CODEX_EXECUTABLE), reindex_mock=True)
            self.assertTrue(report['partial'])
            self.assertEqual(len(report['archived_paginated_unverified']), 1)
            self.assertEqual(archived_rollout.read_bytes(), original_archived)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(relocated)})
            try:
                self.assertIn('result', call({'id': 24, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                discover = call({'id': 29, 'method': 'thread/list', 'params': {'archived': True, 'limit': 100}})
                self.assertIn('result', discover, str(discover.get('error')))
                unarchived = call({'id': 25, 'method': 'thread/unarchive', 'params': {'threadId': thread_id}})
                self.assertIn('error', unarchived)
                self.assertEqual(archived_rollout.read_bytes(), original_archived)
            finally:
                stop_server(server)
            rebuilt = root / 'fresh-archive-preview'
            rebuilt_report = ns.index_preview(relocated, rebuilt, str(project.parent), cfg, Path(CODEX_EXECUTABLE),
                                              replay_paginated_archives=True)
            self.assertTrue(rebuilt_report['partial'])
            self.assertEqual(len(rebuilt_report['archived_paginated_replayed']), 1)
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(rebuilt)})
            try:
                self.assertIn('result', call({'id': 35, 'method': 'initialize', 'params': {'clientInfo':
                    {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call({'id': 36, 'method': 'thread/list', 'params': {'archived': True, 'useStateDbOnly': True}})
                entry = next(item for item in listing['result']['data'] if item['id'] == thread_id)
                self.assertEqual(entry['name'], 'synthetic chosen title')
                self.assertIn('archived_sessions', Path(entry['path']).parts)
                turns = call({'id': 37, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertEqual(turns['result']['data'], original_turns['result']['data'])
            finally:
                stop_server(server)
            incremental = root / 'incremental-target'
            incremental_rollout = incremental / rollout.relative_to(source)
            incremental_rollout.parent.mkdir(parents=True)
            migrated_lines = rollout.read_text(encoding='utf-8').splitlines(keepends=True)
            initial_lines, later_lines = [], []
            for line in migrated_lines:
                record = json.loads(line)
                if record.get('type') == 'session_meta':
                    record['payload']['cwd'] = str(project)
                    line = json.dumps(record) + '\n'
                (later_lines if record.get('timestamp', '') >= '2026-01-01T00:01:00' else initial_lines).append(line)
            self.assertTrue(later_lines)
            incremental_rollout.write_text(''.join(initial_lines), encoding='utf-8')
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(incremental)})
            try:
                self.assertIn('result', call({'id': 11, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}))
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                first = call({'id': 12, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'cwd': str(project)}})
                self.assertIn('result', first, str(first.get('error')))
                self.assertNotIn('second synthetic paginated message', json.dumps(first['result']))
                with incremental_rollout.open('a', encoding='utf-8') as stream:
                    stream.writelines(later_lines)
                later = call({'id': 13, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', later, str(later.get('error')))
                self.assertNotIn('second synthetic paginated message', json.dumps(later['result']))
                resumed = call({'id': 14, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'cwd': str(project)}})
                self.assertIn('result', resumed, str(resumed.get('error')))
                refreshed = call({'id': 15, 'method': 'thread/turns/list', 'params': {'threadId': thread_id, 'itemsView': 'full'}})
                self.assertIn('result', refreshed, str(refreshed.get('error')))
                self.assertIn('second synthetic paginated message', json.dumps(refreshed['result']))
            finally:
                stop_server(server)

    def test_two_running_servers_observe_paginated_history_updates(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-two-servers-') as tmp:
            root = Path(tmp)
            home = root / 'codex'
            folder = home / 'sessions' / '2026' / '01' / '01'
            folder.mkdir(parents=True)
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000088'
            rollout = folder / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            migration_home = root / 'migration-source'
            migration_rollout = migration_home / rollout.relative_to(home)
            migration_rollout.parent.mkdir(parents=True)
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}}]
            for minute, message in ((0, 'mock first message'), (1, 'mock received message')):
                stamp = f'2026-01-01T00:0{minute}:01.000Z'
                rows.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message',
                    'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                    {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                    'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
            migration_rollout.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            migration = subprocess.run([CODEX_EXECUTABLE, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(migration_home)}, capture_output=True, text=True)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            initial, incoming = [], []
            for line in migration_rollout.read_text().splitlines(keepends=True):
                record = json.loads(line)
                (incoming if record.get('timestamp', '') >= '2026-01-01T00:01:00' else initial).append(line)
            self.assertTrue(incoming)
            rollout.write_text(''.join(initial))
            shared = root / 'shared'
            shared.mkdir()
            key = b's' * 32
            processes = []
            def start():
                server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(home)})
                processes.append(server)
                return server
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response, str(response.get('error')))
                return response['result']
            try:
                first, second = start(), start()
                for server in (first, second):
                    call(server, 1, 'initialize', {'clientInfo': {'name': 'nexus_two_server_mock', 'version': '0.1'},
                                                   'capabilities': {'experimentalApi': True}})
                    server.stdin.write(b'{"method":"initialized","params":{}}\n')
                    server.stdin.flush()
                call(first, 2, 'thread/resume', {'threadId': thread_id, 'cwd': str(project)})
                self.assertIn(thread_id, call(first, 3, 'thread/loaded/list', {})['data'])
                self.assertNotIn(thread_id, call(second, 3, 'thread/loaded/list', {})['data'])
                if sys.platform == 'linux':
                    import fcntl
                    lock_file = home / 'thread-writer-locks' / (thread_id + '.lock')
                    self.assertTrue(lock_file.is_file())
                    with lock_file.open('r+b') as stream:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                before = rollout.read_bytes()
                first_snapshot = ns.snapshot(home, shared, 'linux', key)
                first_quarantine = root / 'first-snapshot'
                ns.extract(first_snapshot, first_quarantine, key)
                self.assertEqual((first_quarantine / rollout.relative_to(home)).read_bytes(), before)
                self.assertEqual(rollout.read_bytes(), before)
                self.assertIn(thread_id, call(first, 9, 'thread/loaded/list', {})['data'])
                with rollout.open('a', encoding='utf-8') as stream:
                    stream.writelines(incoming)
                second_snapshot = ns.snapshot(home, shared, 'linux', key)
                second_quarantine = root / 'second-snapshot'
                ns.extract(second_snapshot, second_quarantine, key)
                self.assertIn('mock received message', (second_quarantine / rollout.relative_to(home)).read_text())
                self.assertEqual(ns.verify(second_snapshot, key)['previous_snapshot'], first_snapshot.name)
                before_attempt = call(first, 4, 'thread/turns/list', {'threadId': thread_id, 'itemsView': 'full'})
                self.assertNotIn('mock received message', json.dumps(before_attempt))
                second.stdin.write((json.dumps({'id': 4, 'method': 'thread/resume', 'params':
                    {'threadId': thread_id, 'cwd': str(project), 'path': str(rollout)}}) + '\n').encode())
                second.stdin.flush()
                rejected = read_rpc_response(second, 4)
                self.assertIn('error', rejected)
                self.assertIn('active writer', rejected['error']['message'])
                first_page = call(first, 5, 'thread/turns/list', {'threadId': thread_id, 'itemsView': 'full'})
                self.assertNotIn('mock received message', json.dumps(first_page))
                unsubscribed = call(first, 6, 'thread/unsubscribe', {'threadId': thread_id})
                self.assertIsInstance(unsubscribed, dict)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    if thread_id not in call(first, 8, 'thread/loaded/list', {})['data']:
                        break
                    time.sleep(0.05)
                second.stdin.write((json.dumps({'id': 7, 'method': 'thread/resume', 'params':
                    {'threadId': thread_id, 'cwd': str(project), 'path': str(rollout)}}) + '\n').encode())
                second.stdin.flush()
                still_busy = read_rpc_response(second, 7)
                self.assertIn('error', still_busy)
                self.assertIn('active writer', still_busy['error']['message'])
                stop_server(first)
                processes.remove(first)
                if sys.platform == 'linux':
                    coordination = home / 'thread-writer-locks' / '.coordination.lock'
                    with coordination.open('a+b') as coordinator, lock_file.open('a+b') as stream:
                        fcntl.flock(coordinator.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        completed, responses = threading.Event(), []
                        try:
                            second.stdin.write((json.dumps({'id': 10, 'method': 'thread/resume', 'params':
                                {'threadId': thread_id, 'cwd': str(project), 'path': str(rollout)}}) + '\n').encode())
                            second.stdin.flush()
                            def wait_response():
                                responses.append(read_rpc_response(second, 10))
                                completed.set()
                            threading.Thread(target=wait_response, daemon=True).start()
                            self.assertFalse(completed.wait(0.25))
                            fcntl.flock(coordinator.fileno(), fcntl.LOCK_UN)
                            self.assertTrue(completed.wait(5))
                            self.assertIn('error', responses[0])
                            self.assertIn('active writer', responses[0]['error']['message'])
                        finally:
                            fcntl.flock(coordinator.fileno(), fcntl.LOCK_UN)
                            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
                call(second, 5, 'thread/resume', {'threadId': thread_id, 'cwd': str(project), 'path': str(rollout)})
                second_page = call(second, 6, 'thread/turns/list', {'threadId': thread_id, 'itemsView': 'full'})
                self.assertIn('mock received message', json.dumps(second_page))
            finally:
                for server in reversed(processes):
                    stop_server(server)


    @unittest.skipUnless(sys.platform == 'linux', 'mock receiver requires Linux')
    def test_legacy_settings_merge_is_readable_by_disposable_codex(self):
        with tempfile.TemporaryDirectory(prefix='nexus-legacy-settings-contract-') as tmp:
            root = Path(tmp)
            shared, source, nexus = root / 'shared', root / 'sender', root / 'Nexus'
            shared.mkdir()
            source_sessions = source / 'sessions'
            source_sessions.mkdir(parents=True)
            (nexus / 'Bot').mkdir(parents=True)
            profile = root / 'profile'
            key = b'k' * 32
            mock_sync.init_mock_profile(profile, key, shared)
            identity = '019b0000-0000-7000-8000-000000000078'
            source_file = source_sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            initial = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'first synthetic message'}]}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'first synthetic message', 'images': [], 'local_images': [], 'text_elements': []}}]
            source_file.write_text(''.join(json.dumps(row) + '\n' for row in initial))
            ns.snapshot(source, shared, 'linux', key)
            first = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus, Path(CODEX_EXECUTABLE))
            self.assertEqual(first['state'], 'COMPLETE')
            local = ns._chat_catalog(profile)[identity][0]['path']
            local_rows = [json.loads(line) for line in local.read_text().splitlines()]
            settings = [row for row in local_rows if row.get('type') == 'event_msg'
                        and row.get('payload', {}).get('type') == 'thread_settings_applied']
            self.assertTrue(settings, 'Codex did not create a local settings record')
            remote_setting = json.loads(json.dumps(settings[-1]))
            remote_setting['timestamp'] = '2026-01-01T00:00:02.000Z'
            continuation = [
                {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'second synthetic message'}]}},
                {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'second synthetic message', 'images': [], 'local_images': [], 'text_elements': []}}]
            source_file.write_text(source_file.read_text() + ''.join(json.dumps(row) + '\n' for row in [remote_setting, *continuation]))
            ns.snapshot(source, shared, 'linux', key)
            result = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus,
                                            Path(CODEX_EXECUTABLE), allow_mock_settings_rebase=True)
            self.assertEqual(result['state'], 'COMPLETE', result)
            self.assertEqual(result['results'], {'REBASED_MOCK': 1})
            server = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            try:
                def call(number, method, params):
                    server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                    server.stdin.flush()
                    response = read_rpc_response(server, number)
                    self.assertIn('result', response, str(response.get('error')))
                    return response['result']
                call(1, 'initialize', {'clientInfo': {'name': 'nexus_legacy_mock', 'version': '0.1'},
                                       'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                restored = call(2, 'thread/read', {'threadId': identity, 'includeTurns': True})
                self.assertIn('first synthetic message', json.dumps(restored))
                self.assertIn('second synthetic message', json.dumps(restored))
            finally:
                stop_server(server)

    @unittest.skipUnless(sys.platform == 'linux', 'mock watcher requires Linux')
    def test_mock_watcher_receives_chats_from_windows_and_macos_paths(self):
        with tempfile.TemporaryDirectory(prefix='nexus-multi-chat-contract-') as tmp:
            root = Path(tmp)
            shared, nexus = root / 'shared', root / 'Nexus'
            shared.mkdir()
            (nexus / 'Bot').mkdir(parents=True)
            key = b'p' * 32
            key_file = root / 'key'
            key_file.write_bytes(key)
            profile = root / 'profile'
            mock_sync.init_mock_profile(profile, key, shared)
            peers = [('windows', r'C:\Mock\Nexus', r'C:\Mock\Nexus\Bot', '000000000071'),
                     ('mac', '/Users/mock/Nexus', '/Users/mock/Nexus/Bot', '000000000072')]
            for peer, _, cwd, suffix in peers:
                source = root / peer
                sessions = source / 'sessions'
                sessions.mkdir(parents=True)
                identity = f'019b0000-0000-7000-8000-{suffix}'
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {
                    'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {
                    'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic multi-device'}]}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload': {
                    'type': 'user_message', 'message': 'synthetic multi-device', 'images': [], 'local_images': [], 'text_elements': []}}]
                (sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    ''.join(json.dumps(row) + '\n' for row in rows))
                ns.snapshot(source, shared, peer, key)
            command = [sys.executable, str(Path(mock_sync.__file__)), 'watch-all', '--profile', str(profile),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'windows',
                '--source-root', peers[0][1], '--also-source', 'mac', peers[1][1], '--target-root', str(nexus),
                '--codex-executable', CODEX_EXECUTABLE, '--interval', '0.5']
            duplicate = subprocess.run(command + ['--also-source', 'WINDOWS', peers[0][1]],
                                       capture_output=True, text=True, timeout=10)
            self.assertNotEqual(duplicate.returncode, 0)
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    self.assertIsNone(process.poll())
                    markers = [profile / mock_sync.SYNC_MARKER / (peer + '-019b0000-0000-7000-8000-' + suffix + '.json')
                               for peer, _, _, suffix in peers]
                    if all(marker.is_file() for marker in markers):
                        break
                    time.sleep(0.1)
                else:
                    self.fail('multi-source watcher did not process both mock chats')
                catalogue = ns._chat_catalog(profile)
                self.assertEqual(len(catalogue), 2)
                for items in catalogue.values():
                    self.assertEqual(items[0]['cwd'], str(nexus / 'Bot'))
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                process.wait(timeout=20)

    @unittest.skipUnless(sys.platform == 'linux', 'mock watcher requires Linux')
    def test_mock_watch_applies_later_chat_to_same_profile(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-watch-contract-') as tmp:
            root = Path(tmp)
            shared, nexus, source = root / 'shared', root / 'Nexus', root / 'sender'
            shared.mkdir()
            (nexus / 'Bot').mkdir(parents=True)
            sessions = source / 'sessions'
            sessions.mkdir(parents=True)
            profile = root / 'profile'
            key = b'w' * 32
            key_file = root / 'key'
            key_file.write_bytes(key)
            mock_sync.init_mock_profile(profile, key, shared)
            def add_chat(number):
                identity = f'019b0000-0000-7000-8000-{number:012d}'
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                    {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'mock message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'mock message', 'images': [], 'local_images': [], 'text_elements': []}}]
                (sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    ''.join(json.dumps(row) + '\n' for row in rows))
                return identity
            first = add_chat(81)
            ns.snapshot(source, shared, 'linux', key)
            command = [sys.executable, str(Path(mock_sync.__file__)), 'watch-all', '--profile', str(profile),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux',
                '--source-root', '/source/Nexus', '--target-root', str(nexus),
                '--codex-executable', CODEX_EXECUTABLE, '--interval', '0.5']
            output = (root / 'watch.log').open('w+')
            self.addCleanup(output.close)
            process = subprocess.Popen(command, stdout=output, stderr=output)
            def wait_for(identity):
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    self.assertIsNone(process.poll(), 'mock watcher exited unexpectedly')
                    marker = profile / mock_sync.SYNC_MARKER / ('linux-' + identity + '.json')
                    if marker.is_file():
                        return
                    time.sleep(0.1)
                output.flush()
                output.seek(0)
                self.fail('mock watcher did not index the chat: ' + output.read())
            try:
                wait_for(first)
                state = root / 'state'
                state.mkdir()
                config = root / 'config.json'
                config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'receiver',
                    'nexus': str(nexus), 'codex_home': str(profile), 'shared': str(shared),
                    'state_dir': str(state), 'key_file': str(key_file), 'direction': 'send-receive'}))
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    summary = subprocess.run([sys.executable, str(Path(ns.__file__)), 'summary', '--config', str(config),
                                              '--mock-chat-profile', str(profile)], capture_output=True, text=True, timeout=10)
                    self.assertEqual(summary.returncode, 0, summary.stderr)
                    watcher_state = json.loads(summary.stdout)['mock_chat_watch']
                    if watcher_state['state'] == 'COMPLETE':
                        break
                    time.sleep(0.1)
                else:
                    self.fail('mock watcher did not publish a current status')
                self.assertFalse(watcher_state['verified_in_cursor'])
                duplicate = subprocess.run(command, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(duplicate.returncode, 0)
                second = add_chat(82)
                ns.snapshot(source, shared, 'linux', key)
                wait_for(second)
                self.assertEqual(set(ns._chat_catalog(profile)), {first, second})
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    watcher_state = mock_sync.watch_status(profile, shared, key)
                    if watcher_state['state'] == 'PENDING':
                        break
                    time.sleep(0.1)
                else:
                    self.fail('mock watcher did not report the pending local history')
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                process.wait(timeout=20)
            self.assertEqual(mock_sync.watch_status(profile, shared, key)['state'], 'STOPPED')

    @unittest.skipUnless(sys.platform == 'linux', 'mock writer-lock experiment requires Linux')
    def test_mock_batch_indexes_multiple_new_chats(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-batch-contract-') as tmp:
            root = Path(tmp)
            shared = root / 'shared'
            shared.mkdir()
            nexus = root / 'Nexus'
            (nexus / 'Bot').mkdir(parents=True)
            profile = root / 'profile'
            key = b'b' * 32
            mock_sync.init_mock_profile(profile, key, shared)
            source = root / 'sender'
            sessions = source / 'sessions'
            sessions.mkdir(parents=True)
            for number in (91, 92):
                identity = f'019b0000-0000-7000-8000-{number:012d}'
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                    {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                    {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'mock batch message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload':
                    {'type': 'user_message', 'message': 'mock batch message', 'images': [], 'local_images': [], 'text_elements': []}}]
                (sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    ''.join(json.dumps(row) + '\n' for row in rows))
            (source / 'session_index.jsonl').write_text(''.join(json.dumps({
                'id': f'019b0000-0000-7000-8000-{number:012d}', 'thread_name': f'mock title {number}',
                'updated_at': '2026-01-01T00:00:00Z'}) + '\n' for number in (91, 92)))
            ns.snapshot(source, shared, 'linux', key)
            result = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus, Path(CODEX_EXECUTABLE))
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['results'], {'ADDED': 2})
            self.assertFalse(result['verified_in_cursor'])
            self.assertEqual(len(ns._chat_catalog(profile)), 2)
            repeated = mock_sync.sync_all_mock(profile, shared, key, 'linux', '/source/Nexus', nexus, Path(CODEX_EXECUTABLE))
            self.assertEqual(repeated['state'], 'COMPLETE', repeated)
            self.assertEqual(repeated['results'], {'UNCHANGED': 2})

    @unittest.skipUnless(sys.platform == 'linux', 'mock writer-lock experiment requires Linux')
    def test_mock_snapshot_application_waits_for_writer_and_is_resumable(self):
        with tempfile.TemporaryDirectory(prefix='nexus-mock-apply-contract-') as tmp:
            root = Path(tmp)
            shared = root / 'shared'
            shared.mkdir()
            nexus = root / 'Nexus'
            project = nexus / 'Bot'
            project.mkdir(parents=True)
            profile = root / 'mock-profile'
            key = b'z' * 32
            mock_sync.init_mock_profile(profile, key, shared)
            source = root / 'sender'
            source_sessions = source / 'sessions' / '2026' / '01' / '01'
            target_sessions = profile / 'sessions' / '2026' / '01' / '01'
            source_sessions.mkdir(parents=True)
            target_sessions.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000089'
            filename = f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            def messages(cwd, count):
                result = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                    {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}}]
                for minute in range(count):
                    stamp = f'2026-01-01T00:0{minute}:01.000Z'
                    message = f'mock message {minute}'
                    result.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message',
                        'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                        {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                        'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
                return ''.join(json.dumps(row) + '\n' for row in result)
            (source_sessions / filename).write_text(messages('/source/Nexus/Bot', 2))
            target = target_sessions / filename
            target.write_text(messages(str(project), 1))
            def start():
                return subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(profile)})
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                result = read_rpc_response(server, number)
                self.assertIn('result', result, str(result.get('error')))
                return result['result']
            server = start()
            try:
                call(server, 1, 'initialize', {'clientInfo': {'name': 'nexus_mock_apply', 'version': '0.1'},
                                               'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                call(server, 2, 'thread/resume', {'threadId': thread_id, 'cwd': str(project)})
                original = target.read_bytes()
                source_rows = []
                for line in original.splitlines():
                    row = json.loads(line)
                    payload = row.get('payload')
                    if row.get('type') in ('session_meta', 'turn_context') and isinstance(payload, dict) and payload.get('cwd') == str(project):
                        payload['cwd'] = '/source/Nexus/Bot'
                    source_rows.append(row)
                source_rows.extend(json.loads(line) for line in messages('/source/Nexus/Bot', 2).splitlines()[-2:])
                (source_sessions / filename).write_text(''.join(json.dumps(row) + '\n' for row in source_rows))
                snapshot = ns.snapshot(source, shared, 'linux', key)
                with self.assertRaises(ns.SyncError):
                    mock_sync.apply_mock_snapshot(profile, snapshot, shared, key, thread_id, '/source/Nexus', nexus)
                with self.assertRaises(ns.SyncError):
                    mock_sync.reindex_mock_profile(profile, shared, key, nexus, Path(CODEX_EXECUTABLE), thread_id)
                self.assertFalse((profile / 'index-preview.json').exists())
                self.assertEqual(target.read_bytes(), original)
            finally:
                stop_server(server)
            key_file = root / 'key'
            key_file.write_bytes(key)
            command = [sys.executable, str(Path(mock_sync.__file__)), 'sync-latest', '--profile', str(profile),
                '--shared', str(shared), '--key-file', str(key_file), '--source', 'linux', '--thread-id', thread_id,
                '--source-root', '/source/Nexus', '--target-root', str(nexus), '--codex-executable', CODEX_EXECUTABLE]
            invocation = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(invocation.returncode, 0, invocation.stderr)
            result = json.loads(invocation.stdout)
            self.assertEqual(result['state'], 'UPDATED')
            self.assertTrue(result['index_updated'])
            self.assertFalse(result['contents_verified'])
            self.assertFalse(result['verified_in_cursor'])
            repeated = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(repeated.returncode, 0, repeated.stderr)
            self.assertEqual(json.loads(repeated.stdout)['state'], 'UNCHANGED')
            server = start()
            try:
                call(server, 3, 'initialize', {'clientInfo': {'name': 'nexus_mock_apply', 'version': '0.1'},
                                               'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                resumed = call(server, 4, 'thread/resume', {'threadId': thread_id, 'cwd': str(project), 'path': str(target)})
                self.assertIn('mock message 1', json.dumps(resumed))
                listing = call(server, 5, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertIn(thread_id, {item['id'] for item in listing['data']})
            finally:
                stop_server(server)
            second_id = '019b0000-0000-7000-8000-000000000090'
            second_name = f'rollout-2026-01-01T00-00-00-{second_id}.jsonl'
            second_rows = [json.loads(line) for line in messages('/source/Nexus/Bot', 1).splitlines()]
            second_rows[0]['payload']['id'] = second_id
            second_rows[1]['payload']['content'][0]['text'] = 'new mock conversation'
            second_rows[2]['payload']['message'] = 'new mock conversation'
            (source_sessions / second_name).write_text(''.join(json.dumps(row) + '\n' for row in second_rows))
            (source / 'session_index.jsonl').write_text(json.dumps({'id': second_id,
                'thread_name': 'synthetic synced title', 'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            ns.snapshot(source, shared, 'linux', key)
            second_command = list(command)
            second_command[second_command.index('--thread-id') + 1] = second_id
            added = subprocess.run(second_command, capture_output=True, text=True)
            self.assertEqual(added.returncode, 0, added.stderr)
            self.assertEqual(json.loads(added.stdout)['state'], 'ADDED')
            self.assertTrue(json.loads(added.stdout)['index_updated'])
            self.assertTrue(json.loads(added.stdout)['titles_updated'])
            self.assertNotIn('synthetic synced title', added.stdout)
            server = start()
            try:
                call(server, 6, 'initialize', {'clientInfo': {'name': 'nexus_mock_apply', 'version': '0.1'},
                                               'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call(server, 7, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertTrue({thread_id, second_id} <= {item['id'] for item in listing['data']})
                self.assertEqual(next(item['name'] for item in listing['data'] if item['id'] == second_id),
                                 'synthetic synced title')
                for number, identity, message in ((8, thread_id, 'mock message 1'),
                                                   (9, second_id, 'new mock conversation')):
                    restored = call(server, number, 'thread/read', {'threadId': identity, 'includeTurns': True})
                    self.assertIn(message, json.dumps(restored))
            finally:
                stop_server(server)
            # The database may retain a chosen title even if the JSONL index is missing.
            (profile / 'session_index.jsonl').unlink(missing_ok=True)
            with self.assertRaises(ns.SyncError):
                mock_sync.reindex_mock_profile(profile, shared, key, nexus, Path(CODEX_EXECUTABLE),
                                              second_id, title_override='conflicting mock title')
            server = start()
            try:
                call(server, 10, 'initialize', {'clientInfo': {'name': 'nexus_mock_apply', 'version': '0.1'},
                                                'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                listing = call(server, 11, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertEqual(next(item['name'] for item in listing['data'] if item['id'] == second_id),
                                 'synthetic synced title')
            finally:
                stop_server(server)

    def test_existing_chat_updated_in_disposable_profile(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-update-') as tmp:
            root = Path(tmp)
            home = root / 'codex'
            sessions = home / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000005'
            rollout = sessions / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            initial = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project), 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'first synthetic message'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'first synthetic message', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            extra = [
                {'timestamp': '2026-01-01T00:01:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'second synthetic message'}]}},
                {'timestamp': '2026-01-01T00:01:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'second synthetic message', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in initial), encoding='utf-8')
            process = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(home)})
            other = None
            try:
                def call(request, server=process):
                    server.stdin.write((json.dumps(request) + '\n').encode())
                    server.stdin.flush()
                    response = read_rpc_response(server, request['id'])
                    self.assertIn('result', response, str(response.get('error')))
                    return response['result']
                call({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}})
                process.stdin.write(b'{"method":"initialized","params":{}}\n')
                process.stdin.flush()
                first = call({'id': 2, 'method': 'thread/read', 'params': {'threadId': thread_id, 'includeTurns': True}})
                self.assertIn('first synthetic message', json.dumps(first))
                resumed = call({'id': 3, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'cwd': str(project)}})
                self.assertIn('first synthetic message', json.dumps(resumed))
                loaded = call({'id': 6, 'method': 'thread/loaded/list', 'params': {}})
                self.assertIn(thread_id, loaded['data'])
                other = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(home)})
                call({'id': 10, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_other_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}}, other)
                other.stdin.write(b'{"method":"initialized","params":{}}\n')
                other.stdin.flush()
                other_loaded = call({'id': 11, 'method': 'thread/loaded/list', 'params': {}}, other)
                self.assertNotIn(thread_id, other_loaded['data'])
                with rollout.open('a', encoding='utf-8') as stream:
                    stream.writelines(json.dumps(row) + '\n' for row in extra)
                after = call({'id': 4, 'method': 'thread/read', 'params': {'threadId': thread_id, 'includeTurns': True}})
                self.assertIn('second synthetic message', json.dumps(after))
                resumed_again = call({'id': 5, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'cwd': str(project)}})
                self.assertIn('second synthetic message', json.dumps(resumed_again))
            finally:
                if other is not None:
                    stop_server(other)
                stop_server(process)

    def test_encrypted_snapshot_is_listed_in_a_fresh_disposable_target(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-transfer-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            sessions = source / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            shared = root / 'shared'
            shared.mkdir()
            target_project = root / 'target-project'
            target_project.mkdir()
            thread_id = '019b0000-0000-7000-8000-000000000003'
            name = f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            records = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source-machine/Nexus/Bot', 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'Disposable test chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'Disposable test chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            original = sessions / name
            original.write_text(''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8')
            key = b'k' * 32
            sealed = ns.snapshot(source, shared, 'linux', key)
            quarantine = root / 'quarantine'
            ns.extract(sealed, quarantine, key)
            self.assertEqual((quarantine / 'sessions' / '2026' / '01' / '01' / name).read_bytes(), original.read_bytes())
            target = root / 'disposable-target'
            self.assertEqual(ns.prepare_chats(quarantine, target, '/source-machine/Nexus/Bot', str(target_project)), 1)
            restored = target / 'sessions' / '2026' / '01' / '01' / name
            process = subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(target)})
            try:
                def send(value):
                    process.stdin.write((json.dumps(value) + '\n').encode())
                    process.stdin.flush()
                def read(request_id):
                    return read_rpc_response(process, request_id)
                send({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}})
                self.assertIn('result', read(1))
                send({'method': 'initialized', 'params': {}})
                send({'id': 2, 'method': 'thread/list', 'params': {'limit': 100}})
                listed_before = read(2).get('result', {}).get('data', [])
                row = next(item for item in listed_before if item.get('id') == thread_id)
                self.assertEqual(row.get('cwd'), str(target_project))
                send({'id': 3, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'path': str(restored), 'cwd': str(target_project)}})
                response = read(3)
                self.assertIn('result', response, str(response.get('error')))
                self.assertEqual(response['result']['cwd'], str(target_project))
                send({'id': 4, 'method': 'thread/list', 'params': {'limit': 100}})
                listing = read(4)
                listed = next(row for row in listing.get('result', {}).get('data', []) if row.get('id') == thread_id)
                self.assertEqual(listed.get('cwd'), str(target_project))
            finally:
                stop_server(process)

    def test_app_server_can_list_a_disposable_local_chat(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-contract-') as tmp:
            root = Path(tmp)
            codex = root / 'codex'
            sessions = codex / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000001'
            rollout = sessions / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            records = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project), 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'Disposable test chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'Disposable test chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            rollout.write_text(''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8')
            requests = [
                {'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}},
                {'method': 'initialized', 'params': {}},
                {'id': 2, 'method': 'thread/list', 'params': {'limit': 100}},
            ]
            process = subprocess.Popen(
                [CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                bufsize=0, env={**os.environ, 'CODEX_HOME': str(codex)},
            )
            try:
                def send(value):
                    process.stdin.write((json.dumps(value) + '\n').encode())
                    process.stdin.flush()
                def read_response(request_id):
                    return read_rpc_response(process, request_id)
                send(requests[0])
                self.assertIn('result', read_response(1) or {})
                send(requests[1])
                send(requests[2])
                listing = read_response(2)
                self.assertIsNotNone(listing, 'no thread/list response')
                self.assertIn('result', listing, str(listing.get('error')))
                initially_visible = thread_id in json.dumps(listing['result'])
                print(f'initially visible in disposable Codex: {initially_visible}')
                send({'id': 3, 'method': 'thread/resume', 'params': {'threadId': thread_id, 'path': str(rollout), 'cwd': str(project)}})
                resumed = read_response(3)
                self.assertIsNotNone(resumed, 'no thread/resume response')
                if 'error' in resumed:
                    print('resume returned an error:', resumed['error'].get('message', 'unknown'))
                else:
                    print('resume returned a result')
                    print('resume used requested local cwd:', resumed['result'].get('cwd') == str(project))
                time.sleep(1)
                send({'id': 4, 'method': 'thread/list', 'params': {'limit': 100, 'modelProviders': [], 'sourceKinds': ['cli', 'vscode', 'unknown', 'appServer']}})
                after = read_response(4)
                self.assertIsNotNone(after, 'no second thread/list response')
                print(f"visible after resume: {thread_id in json.dumps(after.get('result', {}))}")
                for database in codex.glob('state_*.sqlite'):
                    with closing(sqlite3.connect(f'{database.as_uri()}?mode=ro', uri=True)) as db:
                        columns = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
                        selected = [name for name in ('id', 'source', 'has_user_event', 'archived', 'model_provider') if name in columns]
                        if selected:
                            print('disposable catalogue rows:', db.execute('SELECT ' + ','.join(selected) + ' FROM threads').fetchall())
            finally:
                stop_server(process)

    def test_a_new_rollout_arriving_after_initial_catalogue(self):
        with tempfile.TemporaryDirectory(prefix='nexus-codex-late-') as tmp:
            root = Path(tmp)
            codex = root / 'codex'
            sessions = codex / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            project = root / 'Nexus' / 'Bot'
            project.mkdir(parents=True)
            thread_id = '019b0000-0000-7000-8000-000000000002'
            rollout = sessions / f'rollout-2026-01-01T00-00-00-{thread_id}.jsonl'
            records = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': {'id': thread_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project), 'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'Disposable test chat'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'Disposable test chat', 'images': [], 'local_images': [], 'text_elements': []}},
            ]
            existing_records = json.loads(json.dumps(records))
            existing_id = '019b0000-0000-7000-8000-000000000004'
            existing_records[0]['payload']['id'] = existing_id
            existing = sessions / f'rollout-2026-01-01T00-00-00-{existing_id}.jsonl'
            existing.write_text(''.join(json.dumps(row) + '\n' for row in existing_records), encoding='utf-8')
            incoming = root / 'incoming'
            incoming_sessions = incoming / 'sessions' / '2026' / '01' / '01'
            incoming_sessions.mkdir(parents=True)
            records[0]['payload']['cwd'] = '/old/Nexus/Bot'
            (incoming_sessions / rollout.name).write_text(''.join(json.dumps(row) + '\n' for row in records), encoding='utf-8')
            prepared = root / 'prepared'
            ns.prepare_chats(incoming, prepared, '/old/Nexus/Bot', str(project))
            def start_server():
                return subprocess.Popen([CODEX_EXECUTABLE, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env={**os.environ, 'CODEX_HOME': str(codex)})
            def send(process, value):
                process.stdin.write((json.dumps(value) + '\n').encode())
                process.stdin.flush()
            def read(process, request_id):
                return read_rpc_response(process, request_id)
            def init(process):
                send(process, {'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'nexus_test', 'version': '0.1'}, 'capabilities': {'experimentalApi': True}}})
                self.assertIn('result', read(process, 1))
                send(process, {'method': 'initialized', 'params': {}})
            def listing(process, request_id):
                send(process, {'id': request_id, 'method': 'thread/list', 'params': {'limit': 100, 'modelProviders': [], 'sourceKinds': ['cli', 'vscode', 'unknown', 'appServer']}})
                rows = read(process, request_id).get('result', {}).get('data', [])
                self.assertTrue(any(row.get('id') == existing_id for row in rows))
                match = next((row for row in rows if row.get('id') == thread_id), None)
                if match:
                    self.assertEqual(match.get('cwd'), str(project))
                return match is not None
            def stop(process):
                stop_server(process)
            server = start_server()
            try:
                init(server)
                self.assertFalse(listing(server, 2))
                shutil.copyfile(prepared / 'sessions' / '2026' / '01' / '01' / rollout.name, rollout)
                send(server, {'id': 20, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                initial_db = read(server, 20)
                self.assertNotIn(thread_id, json.dumps(initial_db.get('result', {})))
                other = start_server()
                try:
                    init(other)
                    self.assertTrue(listing(other, 3), 'second server did not discover the synthetic chat')
                finally:
                    stop(other)
                send(server, {'id': 21, 'method': 'thread/list', 'params': {'limit': 100, 'useStateDbOnly': True}})
                later_db = read(server, 21)
                self.assertIn(thread_id, json.dumps(later_db.get('result', {})))
                self.assertTrue(listing(server, 3), 'new mock chat was not listed with server running')
            finally:
                stop(server)
            server = start_server()
            try:
                init(server)
                self.assertTrue(listing(server, 4), 'new mock chat was not listed after restart')
            finally:
                stop(server)


if __name__ == '__main__':
    unittest.main()
