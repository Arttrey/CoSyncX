import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import mock_chat_sync as mock
import mock_multigroup_restore as groups
import nexus_sync as ns
from test_codex_contract import read_rpc_response, stop_server


class GroupReportRecoveryTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_multiple_archives_become_active_and_keep_one_backup(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-multi-archive-') as tmp:
            root = Path(tmp).resolve()
            profile, source, shared, backups, project = (root / name for name in
                ('profile', 'source', 'shared', 'backups', 'project'))
            for folder in (source, shared, backups, project): folder.mkdir(mode=0o700)
            key = b'g' * 32
            mock.init_mock_profile(profile, key, shared)
            (profile / 'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            identities = ['019b0000-0000-7000-8000-0000000000b1', '019b0000-0000-7000-8000-0000000000b2']
            for home in (profile, source):
                folder = home / 'sessions'; folder.mkdir()
                for identity in identities:
                    cwd = str(project) if home == profile else '/foreign/missing'
                    message = 'local version' if home == profile else 'remote version'
                    rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                        'payload': {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                                    'model_provider': 'openai'}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                            {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                            {'type': 'user_message', 'message': message, 'images': [], 'local_images': [], 'text_elements': []}}]
                    (folder / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                        ''.join(json.dumps(row) + '\n' for row in rows))
                migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                    env={**os.environ, 'CODEX_HOME': str(home)}, capture_output=True, timeout=30)
                self.assertEqual(migration.returncode, 0)
            server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={**os.environ, 'CODEX_HOME': str(profile)})
            def call(number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response)
            try:
                call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-test', 'version': '0.1'},
                                       'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n'); server.stdin.flush()
                for number, identity in enumerate(identities):
                    path = profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                    call(2 + number * 2, 'thread/resume', {'threadId': identity, 'path': str(path), 'cwd': str(project)})
                    call(3 + number * 2, 'thread/archive', {'threadId': identity})
            finally:
                stop_server(server)
            result = groups.restore(source, profile, shared, backups, key, identities, {},
                [('/source/Nexus', str(project))], Path(shutil.which('codex')), fallback_root=project)
            self.assertEqual(result['changed_chats'], 2)
            self.assertTrue(result['path_report'].is_file())
            report = json.loads(result['path_report'].read_text())
            self.assertEqual(set(report['chats']), set(identities))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(dict(db.execute('SELECT id, archived FROM threads')), {identity: 0 for identity in identities})
            manifest = json.loads((backups / 'latest' / 'manifest.json').read_text())
            archived_files = [name for name in manifest['files'] if name.startswith('archived_sessions/')]
            self.assertEqual(len(archived_files), 2)
            self.assertTrue(all(b'local version' in (backups / 'latest' / name).read_bytes() for name in archived_files))
            self.assertFalse((backups / 'multigroup-steps').exists())

    def test_completed_report_is_published_after_interruption(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-group-report-') as tmp:
            root = Path(tmp).resolve()
            profile, shared, backups = (root / name for name in ('profile', 'shared', 'backups'))
            shared.mkdir(mode=0o700); backups.mkdir(mode=0o700)
            key = b'g' * 32
            mock.init_mock_profile(profile, key, shared)
            latest = backups / 'latest'; latest.mkdir(mode=0o700)
            manifest = latest / 'manifest.json'
            manifest.write_text(json.dumps({'version': 1, 'files': {}}))
            steps = backups / 'multigroup-steps'; steps.mkdir(mode=0o700)
            pending = steps / 'path-report.pending.json'
            pending.write_text(json.dumps({'warning': 'test workspace'}))
            os.chmod(pending, 0o600)
            name = 'mock-multigroup-path-report-' + 'a' * 32 + '.json'
            journal = backups / 'mock-multigroup-pending.json'
            data = {'version': 1, 'parents': ['019b0000-0000-7000-8000-000000000001'],
                    'children': {}, 'complete': True,
                    'backup_manifest_digest': groups.archive._stable_digest(manifest),
                    'report_name': name, 'report_digest': groups.archive._stable_digest(pending)}
            data['authentication'] = ns._manifest_tag(data, key)
            journal.write_text(json.dumps(data))
            with patch('mock_multigroup_restore.os.link', side_effect=OSError('interrupted')):
                with self.assertRaises(OSError):
                    groups.recover(profile, shared, backups, key)
            self.assertTrue(journal.is_file())
            self.assertTrue(pending.is_file())
            groups.recover(profile, shared, backups, key)
            self.assertEqual(json.loads((backups / name).read_text()), {'warning': 'test workspace'})
            self.assertEqual((backups / name).stat().st_mode & 0o077, 0)
            self.assertTrue(manifest.is_file())
            self.assertFalse(journal.exists())
            self.assertFalse(steps.exists())


if __name__ == '__main__':
    unittest.main()
