"""Check whether Codex can index an independent chat while another is open."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class ConcurrentImportTests(unittest.TestCase):
    def test_new_chat_is_indexed_without_disturbing_open_chat(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-concurrent-import-') as tmp:
            root = Path(tmp).resolve()
            home, project = root / 'profile', root / 'project'
            home.mkdir(); project.mkdir()
            (home / 'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            sessions = home / 'sessions' / '2026' / '01' / '01'
            sessions.mkdir(parents=True)
            ids = ['019b0000-0000-7000-8000-000000000001', '019b0000-0000-7000-8000-000000000002']
            paths = []
            for identity, message in zip(ids, ['existing conversation', 'received conversation']):
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai'}
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': message, 'images': [], 'local_images': [], 'text_elements': []}}]
                path = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                paths.append(path)
            codex = shutil.which('codex')
            migration = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(home)}, capture_output=True, timeout=30)
            self.assertEqual(migration.returncode, 0)
            # The new rollout must arrive only after the first server started.
            incoming = root / paths[1].name
            paths[1].rename(incoming)
            servers = []
            def start():
                server = subprocess.Popen([codex, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(home)})
                servers.append(server)
                return server
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                result = read_rpc_response(server, number)
                self.assertIn('result', result, str(result.get('error')))
                return result['result']
            try:
                first, second = start(), start()
                for server in servers:
                    call(server, 1, 'initialize', {'clientInfo': {'name': 'cosyncx-concurrent-test', 'version': '0.1'},
                                                   'capabilities': {'experimentalApi': True}})
                    server.stdin.write(b'{"method":"initialized","params":{}}\n'); server.stdin.flush()
                call(first, 2, 'thread/resume', {'threadId': ids[0], 'path': str(paths[0]), 'cwd': str(project)})
                existing_before = paths[0].read_bytes()
                incoming.rename(paths[1])
                call(second, 2, 'thread/resume', {'threadId': ids[1], 'path': str(paths[1]), 'cwd': str(project)})
                new_items = call(second, 3, 'thread/items/list', {'threadId': ids[1], 'limit': 100})
                old_items = call(first, 3, 'thread/items/list', {'threadId': ids[0], 'limit': 100})
                self.assertIn('received conversation', json.dumps(new_items))
                self.assertIn('existing conversation', json.dumps(old_items))
                self.assertEqual(paths[0].read_bytes(), existing_before)
                listing = call(first, 4, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertTrue(set(ids) <= {item['id'] for item in listing['data']})
                call(second, 4, 'thread/archive', {'threadId': ids[1]})
                call(second, 5, 'thread/unarchive', {'threadId': ids[1]})
                listing = call(first, 5, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertTrue(set(ids) <= {item['id'] for item in listing['data']})
                self.assertEqual(paths[0].read_bytes(), existing_before)
                old_items = call(first, 6, 'thread/items/list', {'threadId': ids[0], 'limit': 100})
                self.assertIn('existing conversation', json.dumps(old_items))
                received_bytes = paths[1].read_bytes()
                call(second, 6, 'thread/archive', {'threadId': ids[1]})
                call(second, 7, 'thread/delete', {'threadId': ids[1]})
                self.assertFalse(any((home / 'archived_sessions').rglob('*' + ids[1] + '*.jsonl')))
                listing = call(first, 7, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertNotIn(ids[1], {item['id'] for item in listing['data']})
                self.assertEqual(paths[0].read_bytes(), existing_before)
                self.assertIn('existing conversation', json.dumps(call(first, 8, 'thread/items/list',
                    {'threadId': ids[0], 'limit': 100})))
                paths[1].parent.mkdir(parents=True, exist_ok=True)
                paths[1].write_bytes(received_bytes)
                call(second, 8, 'thread/resume', {'threadId': ids[1], 'path': str(paths[1]), 'cwd': str(project)})
                self.assertIn('received conversation', json.dumps(call(second, 9, 'thread/items/list',
                    {'threadId': ids[1], 'limit': 100})))
            finally:
                for server in reversed(servers):
                    stop_server(server)


if __name__ == '__main__':
    unittest.main()
