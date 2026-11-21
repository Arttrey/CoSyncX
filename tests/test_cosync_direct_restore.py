import json
import os
from pathlib import Path
import sqlite3
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver
from test_codex_contract import read_rpc_response, stop_server


class DirectRestoreTests(unittest.TestCase):
    def test_waits_for_syncthing_and_idle_profile_then_imports_and_confirms_index(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-direct-real-test-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            folder = source / 'sessions' / '2026' / '01' / '01'
            folder.mkdir(parents=True)
            thread = '019b0000-0000-7000-8000-000000000001'
            history = folder / f'rollout-2026-01-01T00-00-00-{thread}.jsonl'
            meta = {'id': thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            history.write_text(json.dumps({'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                                           'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT)')
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            states = {folder: {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1},
                'needFiles': 0, 'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0,
                'errors': 0, 'pullErrors': 0} for folder in ('sessions', 'titles', 'graph')}
            def reader(folder): return lambda: (folder, states[folder], True, 'valid')
            readers = (reader('sessions'), reader('titles'), reader('graph'))
            def poll(now):
                return receiver.poll(readers[0], title_status_reader=readers[1], graph_status_reader=readers[2], now=now)
            self.assertEqual(poll(0)['state'], 'WAITING')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=False):
                self.assertEqual(poll(1)['state'], 'PENDING')
            self.assertFalse((profile / 'sessions').exists())
            graph_path = graph / 'graph.json'
            empty_graph = graph_path.read_bytes()
            graph_path.write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': thread, 'child': thread}], 'omitted_missing': 0, 'omitted_archived': 0}))
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(poll(2)['state'], 'PENDING')
            self.assertFalse((profile / 'sessions').exists())
            graph_path.write_bytes(empty_graph)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('INSERT INTO threads VALUES (?)', (thread,))
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(poll(3)['state'], 'PENDING')
            self.assertFalse((backups / 'latest').exists())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('DELETE FROM threads WHERE id=?', (thread,))
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexed:
                result = poll(4)
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['restore_event']['changed_chats'], 1)
            self.assertTrue(result['restore_event']['indexed'])
            indexed.assert_called_once()
            self.assertTrue((profile / history.relative_to(source)).is_file())
            self.assertTrue((backups / 'latest' / 'state_5.sqlite').is_file())
            self.assertFalse((backups / 'direct-restore-pending.json').exists())

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_real_codex_indexes_new_direct_chat_in_disposable_profile(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-direct-index-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            (profile / 'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            thread = '019b0000-0000-7000-8000-000000000011'
            folder = source / 'sessions' / '2026' / '01' / '01'
            folder.mkdir(parents=True)
            history = folder / f'rollout-2026-01-01T00-00-00-{thread}.jsonl'
            meta = {'id': thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'received message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'received message', 'images': [], 'local_images': [], 'text_elements': []}}]
            history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'Received'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            codex = shutil.which('codex')
            migrated = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, timeout=30)
            self.assertEqual(migrated.returncode, 0)
            server = subprocess.Popen([codex, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            try:
                server.stdin.write((json.dumps({'id': 1, 'method': 'initialize', 'params': {
                    'clientInfo': {'name': 'cosyncx-direct-test', 'version': '0.1'},
                    'capabilities': {'experimentalApi': True}}}) + '\n').encode()); server.stdin.flush()
                self.assertIn('result', read_rpc_response(server, 1))
                server.stdin.write(b'{"method":"initialized","params":{}}\n'); server.stdin.flush()
                server.stdin.write(b'{"id":2,"method":"thread/list","params":{"limit":1}}\n'); server.stdin.flush()
                self.assertIn('result', read_rpc_response(server, 2))
            finally:
                stop_server(server)
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(receiver._receive(), 1)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                row = db.execute('SELECT archived FROM threads WHERE id=?', (thread,)).fetchone()
            self.assertEqual(row, (0,))
            self.assertFalse((backups / 'direct-restore-pending.json').exists())


if __name__ == '__main__':
    unittest.main()
