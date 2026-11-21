import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest

import mock_chat_sync as mock
from mock_direct_receiver import MockDirectReceiver
import nexus_sync as ns
from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class GroupRestoreTests(unittest.TestCase):
    def test_receiver_keeps_archived_parent_archived(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-group-test-') as tmp:
            root = Path(tmp).resolve()
            profile, source, shared, backups, project = (root / x for x in
                                                          ('profile', 'source', 'shared', 'backups', 'project'))
            for path in (source, shared, backups, project):
                path.mkdir()
            key = b'g' * 32
            mock.init_mock_profile(profile, key, shared)
            (profile / 'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            parent = '019b0000-0000-7000-8000-000000000001'
            child = '019b0000-0000-7000-8000-000000000002'
            def write(home, identity, cwd, source_kind, message):
                folder = home / 'sessions' / '2026' / '01' / '01'
                folder.mkdir(parents=True, exist_ok=True)
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': source_kind,
                        'model_provider': 'openai'}
                if identity == child:
                    meta['parent_thread_id'] = parent
                role = 'assistant' if identity == child else 'user'
                part = 'output_text' if role == 'assistant' else 'input_text'
                event = {'type': 'agent_message', 'message': message} if role == 'assistant' else {
                    'type': 'user_message', 'message': message, 'images': [], 'local_images': [], 'text_elements': []}
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': role, 'content': [{'type': part, 'text': message}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload': event}]
                path = folder / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                return path
            local = write(profile, parent, str(project), 'vscode', 'local parent')
            write(source, parent, '/source/Nexus', 'vscode', 'remote parent')
            child_path = write(source, child, '/source/Nexus', {'subagent': {'other': 'guardian'}}, 'remote child')
            codex = shutil.which('codex')
            for home in (profile, source):
                run = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                                     env={**os.environ, 'CODEX_HOME': str(home)}, capture_output=True, timeout=30)
                self.assertEqual(run.returncode, 0)
            rows = [json.loads(line) for line in child_path.read_text().splitlines()]
            rows[0]['payload']['subagent_history_start_ordinal'] = next(row['ordinal'] for row in rows if row['type'] == 'response_item')
            child_path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            server = subprocess.Popen([codex, 'app-server', '--listen', 'stdio://'], stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                                      env={**os.environ, 'CODEX_HOME': str(profile)})
            def call(number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                result = read_rpc_response(server, number)
                self.assertIn('result', result)
            try:
                call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-group-test', 'version': '0.1'},
                                       'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n'); server.stdin.flush()
                call(2, 'thread/resume', {'threadId': parent, 'path': str(local), 'cwd': str(project)})
                call(3, 'thread/archive', {'threadId': parent})
            finally:
                stop_server(server)
            catalog = ns._chat_catalog(profile, include_unsupported=True)
            self.assertEqual([x['folder'] for x in catalog[parent]], ['archived_sessions'])
            self.assertNotIn(child, catalog)
            receiver = MockDirectReceiver(source=source, profile=profile, shared=shared, key=key,
                source_root='/source/Nexus', target_root=project, codex=codex,
                folder_id='chats', peer_id='vps', backup_root=backups, quiet_seconds=0)
            state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                     'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            reads = [0]
            def read():
                reads[0] += 1
                if reads[0] == 3:
                    state['sequence'] = 2
                    state['remoteSequence']['vps'] = 2
                return 'chats', dict(state, remoteSequence=dict(state['remoteSequence'])), True, 'valid'
            self.assertEqual(receiver.poll(read, now=0)['state'], 'WAITING')
            self.assertEqual(receiver.poll(read, now=1)['state'], 'PENDING')
            self.assertEqual(receiver.poll(read, now=2)['state'], 'WAITING')
            result = receiver.poll(read, now=3)
            self.assertEqual(result['state'], 'PENDING')
            self.assertNotIn('restore_event', result)
            catalog = ns._chat_catalog(profile, include_unsupported=True)
            self.assertEqual([item['folder'] for item in catalog[parent]], ['archived_sessions'])
            self.assertNotIn(child, catalog)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(dict(db.execute('SELECT id, archived FROM threads WHERE id IN (?, ?)',
                                                 (parent, child))), {parent: 1})


if __name__ == '__main__':
    unittest.main()
