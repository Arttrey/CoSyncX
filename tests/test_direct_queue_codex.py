import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import cosync_lineage as lineage
from cosync_direct_restore import DirectReceiver
from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class CodexQueueTests(unittest.TestCase):
    def test_busy_chat_waits_while_independent_chat_is_indexed(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-real-queue-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backup, project = (root / x for x in
                ('source', 'titles', 'graph', 'profile', 'backup', 'project'))
            for folder in (source, titles, graph, profile, backup, project): folder.mkdir(mode=0o700)
            (profile / 'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            incoming = source / 'sessions'; incoming.mkdir()
            busy, free = ('019b0000-0000-7000-8000-0000000000e1', '019b0000-0000-7000-8000-0000000000e2')
            for identity in (busy, free):
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                    {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'test message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'test message', 'images': [], 'local_images': [], 'text_elements': []}}]
                if identity == free:
                    rows.extend([{'timestamp': '2026-01-01T00:00:02.000Z', 'type': 'response_item', 'payload':
                        {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'updated message'}]}},
                        {'timestamp': '2026-01-01T00:00:02.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': 'updated message', 'images': [], 'local_images': [], 'text_elements': []}}])
                (incoming / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    ''.join(json.dumps(row) + '\n' for row in rows))
            codex = Path(shutil.which('codex'))
            migrated = subprocess.run([str(codex), 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, timeout=30)
            self.assertEqual(migrated.returncode, 0)
            prepared = root / 'prepared'
            lineage.prepare_lineage(source, prepared, busy, [('/source/Nexus', str(project))])
            local = next((prepared / 'sessions').rglob('*.jsonl'))
            target = profile / local.relative_to(prepared)
            target.parent.mkdir(parents=True)
            shutil.copyfile(local, target)
            free_prepared = root / 'prepared-free'
            lineage.prepare_lineage(source, free_prepared, free, [('/source/Nexus', str(project))])
            free_source = next((free_prepared / 'sessions').rglob('*.jsonl'))
            free_target = profile / free_source.relative_to(free_prepared)
            free_target.parent.mkdir(parents=True, exist_ok=True)
            free_target.write_text(''.join(line for line in free_source.read_text().splitlines(keepends=True)
                                           if json.loads(line).get('timestamp', '') < '2026-01-01T00:00:02'))
            seed = subprocess.Popen([str(codex), 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            def seed_call(number, method, params):
                seed.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                seed.stdin.flush()
                self.assertIn('result', read_rpc_response(seed, number))
            try:
                seed_call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-seed', 'version': '0.1'},
                                            'capabilities': {'experimentalApi': True}})
                seed.stdin.write(b'{"method":"initialized","params":{}}\n'); seed.stdin.flush()
                seed_call(2, 'thread/resume', {'threadId': free, 'path': str(free_target), 'cwd': str(project)})
            finally:
                stop_server(seed)
            (titles / 'session_index.jsonl').write_text(''.join(
                json.dumps({'id': identity, 'thread_name': 'Linux title ' + str(index)}) + '\n'
                for index, identity in enumerate((busy, free))))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            server = subprocess.Popen([str(codex), 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            def call(number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response)
                return response['result']
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backup, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=codex, folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                     'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(folder): return lambda: (folder, state, True, 'valid')
            readers = [read(folder) for folder in ('sessions', 'titles', 'graph')]
            def poll(now): return receiver.poll(readers[0], title_status_reader=readers[1], graph_status_reader=readers[2], now=now)
            try:
                call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-test', 'version': '0.1'},
                                       'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n'); server.stdin.flush()
                call(2, 'thread/resume', {'threadId': busy, 'path': str(target), 'cwd': str(project)})
                busy_before = target.read_bytes()
                with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                    self.assertEqual(poll(0)['state'], 'WAITING')
                    result = poll(1)
                    self.assertEqual(result['state'], 'COMPLETE')
                    self.assertEqual(result['restore_event']['changed_chats'], 1)
                    self.assertEqual(receiver.pending_busy_ids, {busy})
                    self.assertIn('updated message', free_target.read_text())
                    receiver.acknowledge(result['restore_event'])
                    self.assertEqual(poll(2)['state'], 'PENDING')
                self.assertIn(busy, call(3, 'thread/loaded/list', {})['data'])
                self.assertEqual(target.read_bytes(), busy_before)
                self.assertIn('test message', json.dumps(call(4, 'thread/turns/list',
                                                             {'threadId': busy, 'itemsView': 'full'})))
            finally:
                stop_server(server)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                result = poll(3)
                self.assertEqual(result['state'], 'COMPLETE')
                self.assertEqual(result['restore_event']['changed_chats'], 1)
                receiver.acknowledge(result['restore_event'])


if __name__ == '__main__':
    unittest.main()
