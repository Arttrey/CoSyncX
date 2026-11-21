"""Verify mock relationship recovery using a real isolated Codex server."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import mock_chat_sync as mock
import mock_graph_restore
from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class MockGraphRestoreContractTests(unittest.TestCase):
    def test_parent_filter_sees_restored_child_edge(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-graph-contract-') as temp:
            root = Path(temp).resolve()
            profile, shared, backup, incoming, project = (root / x for x in ('profile', 'shared', 'backup', 'incoming', 'Nexus'))
            for folder in (shared, backup, incoming, project): folder.mkdir()
            key = b'g' * 32
            mock.init_mock_profile(profile, key, shared)
            sessions = profile / 'sessions'
            sessions.mkdir()
            parent = '019b0000-0000-7000-8000-000000000001'
            child = '019b0000-0000-7000-8000-000000000002'
            files = {}
            for identity, source, parent_id in ((parent, 'vscode', None),
                    (child, {'subagent': {'other': 'guardian'}}, parent)):
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                         {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                          'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': source,
                          'model_provider': 'openai', 'parent_thread_id': parent_id}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic'}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': 'synthetic', 'images': [], 'local_images': [], 'text_elements': []}}]
                files[identity] = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                files[identity].write_text(''.join(json.dumps(row) + '\n' for row in rows))
            migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(profile)}, capture_output=True, text=True, timeout=30)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            def start():
                server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(profile)})
                call(server, 1, 'initialize', {'clientInfo': {'name': 'cosyncx-graph', 'version': '0.1'},
                                               'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                return server
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                result = read_rpc_response(server, number)
                self.assertIn('result', result, str(result.get('error')))
                return result['result']
            def listing(server, number):
                return call(server, number, 'thread/list', {'parentThreadId': parent, 'sourceKinds':
                    ['subAgent', 'subAgentOther', 'subAgentThreadSpawn', 'subAgentReview', 'subAgentCompact'],
                    'useStateDbOnly': True, 'limit': 100})['data']
            server = start()
            try:
                for number, identity in enumerate((parent, child), 2):
                    call(server, number, 'thread/resume', {'threadId': identity, 'path': str(files[identity]), 'cwd': str(project)})
                self.assertNotIn(child, [row['id'] for row in listing(server, 4)])
            finally:
                stop_server(server)
            graph = incoming / 'graph.json'
            graph.write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'created_at_ms': 1,
                'omitted_archived': 0, 'omitted_missing': 0,
                'edges': [{'parent': parent, 'child': child, 'status': 'closed'}]}))
            self.assertEqual(mock_graph_restore.apply_graph(profile, shared, backup, graph, key), 1)
            server = start()
            try:
                self.assertIn(child, [row['id'] for row in listing(server, 2)])
            finally:
                stop_server(server)


if __name__ == '__main__': unittest.main()
