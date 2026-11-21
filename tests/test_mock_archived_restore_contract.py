"""Opt-in experiment: Codex archive state using a disposable profile only."""
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import cosync_archive_plan
import cosync_restore_backup
import mock_chat_sync
import mock_archive_backup
import mock_archive_restore
import nexus_sync
from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class ArchivedRestoreContractTests(unittest.TestCase):
    def test_archiving_parent_also_changes_linked_child(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-archive-tree-') as tmp:
            root = Path(tmp).resolve()
            profile, project = root / 'profile', root / 'Nexus'
            shared, backups = root / 'shared', root / 'backups'
            shared.mkdir()
            backups.mkdir()
            key = b'k' * 32
            mock_chat_sync.init_mock_profile(profile, key, shared)
            sessions = profile / 'sessions'
            sessions.mkdir()
            project.mkdir()
            parent = '019b0000-0000-7000-8000-000000000001'
            child = '019b0000-0000-7000-8000-000000000002'
            grandchild = '019b0000-0000-7000-8000-000000000003'
            paths = {}
            for identity, source, parent_id in ((parent, 'vscode', None),
                    (child, {'subagent': {'other': 'guardian'}}, parent),
                    (grandchild, {'subagent': {'other': 'guardian'}}, child)):
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                         'payload': {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(project),
                                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': source,
                                     'model_provider': 'openai', 'parent_thread_id': parent_id}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg',
                         'payload': {'type': 'user_message', 'message': 'synthetic', 'images': [],
                                     'local_images': [], 'text_elements': []}}]
                paths[identity] = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                paths[identity].write_text(''.join(json.dumps(row) + '\n' for row in rows))
            migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(profile)}, capture_output=True, text=True, timeout=30)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response, str(response.get('error')))
                return response['result']
            def start():
                server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(profile)})
                call(server, 1, 'initialize', {'clientInfo': {'name': 'cosyncx-archive-tree', 'version': '0.1'},
                                               'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                return server
            server = start()
            try:
                for number, identity in enumerate((parent, child, grandchild), 2):
                    call(server, number, 'thread/resume', {'threadId': identity, 'path': str(paths[identity]), 'cwd': str(project)})
            finally:
                stop_server(server)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) VALUES (?, ?, ?)',
                           (parent, child, 'closed'))
                db.execute('INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) VALUES (?, ?, ?)',
                           (child, grandchild, 'closed'))
            server = start()
            try:
                call(server, 2, 'thread/archive', {'threadId': parent})
            finally:
                stop_server(server)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                states = dict(db.execute('SELECT id, archived FROM threads WHERE id IN (?,?,?)', (parent, child, grandchild)))
            self.assertEqual(states, {parent: 1, child: 1, grandchild: 1})
            impact = cosync_archive_plan.archive_impact(profile, [parent])
            self.assertEqual(impact['threads'], {parent, child, grandchild})
            self.assertEqual(impact['archived'], 3)
            self.assertEqual(len(impact['files']), 3)
            incoming = root / 'incoming'
            incoming.mkdir()
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.restore(incoming, profile, shared, backups, key, parent,
                    [('/source/Nexus', str(project))], Path(shutil.which('codex')))
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            prepared = mock_archive_backup.prepare(profile, shared, backups, key, [parent])
            self.assertEqual(prepared['threads'], 3)
            manifest = json.loads((backups / 'latest' / 'manifest.json').read_text())['files']
            self.assertTrue(set(impact['files']) <= set(manifest))
            self.assertIn('state_5.sqlite', manifest)
            self.assertIn('thread_history_1.sqlite', manifest)
            wal = profile / 'thread_history_1.sqlite-wal'
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                self.assertEqual(db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0], 0)
            if wal.exists():
                self.assertEqual(wal.stat().st_size, 0)
            original_replace = cosync_restore_backup.replace_latest
            def empty_wal_replace(*args, **kwargs):
                verify = kwargs['pre_commit']
                def create_empty_wal(stage, saved):
                    wal.touch(exist_ok=True)
                    verify(stage, saved)
                kwargs['pre_commit'] = create_empty_wal
                return original_replace(*args, **kwargs)
            with patch.object(mock_archive_backup.backup, 'replace_latest', side_effect=empty_wal_replace):
                mock_archive_backup.prepare(profile, shared, backups, key, [parent])
            if wal.exists() and wal.stat().st_size == 0:
                wal.unlink()
            history = profile / impact['files'][0]
            original = history.read_bytes()
            previous = (backups / 'latest' / impact['files'][0]).read_bytes()
            def changing_replace(*args, **kwargs):
                verify = kwargs['pre_commit']
                def change_then_verify(stage, saved):
                    history.write_bytes(original + b'\n')
                    verify(stage, saved)
                kwargs['pre_commit'] = change_then_verify
                return original_replace(*args, **kwargs)
            with patch.object(mock_archive_backup.backup, 'replace_latest', side_effect=changing_replace):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_backup.prepare(profile, shared, backups, key, [parent])
            self.assertEqual((backups / 'latest' / impact['files'][0]).read_bytes(), previous)
            history.write_bytes(original)
            (incoming / 'sessions').mkdir()
            local_catalog = nexus_sync._chat_catalog(profile, include_unsupported=True)
            parent_file = local_catalog[parent][0]['path']
            shutil.copyfile(parent_file, incoming / 'sessions' / parent_file.name)
            args = (incoming, profile, shared, backups, key, parent,
                    [(str(project), str(project))], Path(shutil.which('codex')))
            child_file = local_catalog[child][0]['path']
            source_child = incoming / 'sessions' / child_file.name
            shutil.copyfile(child_file, source_child)
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.restore(*args)
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            source_child.unlink()
            with patch.object(mock_archive_restore.pages, 'reset_mock_projection',
                              side_effect=nexus_sync.SyncError('test group interruption')):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*args)
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                states = dict(db.execute('SELECT id, archived FROM threads WHERE id IN (?,?,?)', (parent, child, grandchild)))
            self.assertEqual(states, {parent: 1, child: 1, grandchild: 1})
            mock_archive_restore.restore(*args)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                states = dict(db.execute('SELECT id, archived FROM threads WHERE id IN (?,?,?)', (parent, child, grandchild)))
            self.assertEqual(states, {parent: 0, child: 1, grandchild: 1})
            server = start()
            try:
                call(server, 2, 'thread/archive', {'threadId': parent})
            finally:
                stop_server(server)
            server = start()
            try:
                call(server, 2, 'thread/unarchive', {'threadId': parent})
            finally:
                stop_server(server)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                states = dict(db.execute('SELECT id, archived FROM threads WHERE id IN (?,?,?)', (parent, child, grandchild)))
            self.assertEqual(states, {parent: 0, child: 0, grandchild: 0})

    def test_backup_unarchive_and_resume_remote_history(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-archive-contract-') as tmp:
            root = Path(tmp).resolve()
            profile, source, project, backups = (root / x for x in ('profile', 'source', 'Nexus', 'backups'))
            shared = root / 'shared'
            shared.mkdir()
            key = b'a' * 32
            mock_chat_sync.init_mock_profile(profile, key, shared)
            local_folder = profile / 'sessions' / '2026' / '01' / '01'
            remote_folder = source / 'sessions' / '2026' / '01' / '01'
            local_folder.mkdir(parents=True)
            remote_folder.mkdir(parents=True)
            project.mkdir()
            backups.mkdir()
            thread = '019b0000-0000-7000-8000-000000000001'
            name = f'rollout-2026-01-01T00-00-00-{thread}.jsonl'
            def write(path, cwd, message):
                rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                         'payload': {'id': thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                                     'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                                     'model_provider': 'openai'}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item',
                         'payload': {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg',
                         'payload': {'type': 'user_message', 'message': message, 'images': [],
                                     'local_images': [], 'text_elements': []}}]
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            write(local_folder / name, str(project), 'synthetic local version')
            write(remote_folder / name, '/source/Nexus', 'synthetic remote version')
            other = '019b0000-0000-7000-8000-000000000002'
            other_path = local_folder / f'rollout-2026-01-01T00-00-00-{other}.jsonl'
            write(other_path, str(project), 'unrelated synthetic chat')
            other_rows = [json.loads(line) for line in other_path.read_text().splitlines()]
            other_rows[0]['payload']['id'] = other
            other_path.write_text(''.join(json.dumps(row) + '\n' for row in other_rows))
            for home in (profile, source):
                migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                    env={**os.environ, 'CODEX_HOME': str(home)}, capture_output=True, text=True, timeout=30)
                self.assertEqual(migration.returncode, 0, migration.stderr)
            def start():
                server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={**os.environ, 'CODEX_HOME': str(profile)})
                call(server, 1, 'initialize', {'clientInfo': {'name': 'cosyncx-archive-test', 'version': '0.1'},
                                                'capabilities': {'experimentalApi': True}})
                server.stdin.write(b'{"method":"initialized","params":{}}\n')
                server.stdin.flush()
                return server
            def call(server, number, method, params):
                server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
                server.stdin.flush()
                response = read_rpc_response(server, number)
                self.assertIn('result', response, str(response.get('error')))
                return response['result']
            server = start()
            try:
                call(server, 2, 'thread/resume', {'threadId': thread, 'path': str(local_folder / name), 'cwd': str(project)})
                call(server, 4, 'thread/resume', {'threadId': other, 'path': str(other_path), 'cwd': str(project)})
                call(server, 3, 'thread/archive', {'threadId': thread})
            finally:
                stop_server(server)
            archived = list((profile / 'archived_sessions').rglob(name))
            self.assertEqual(len(archived), 1)
            duplicate = profile / 'archived_sessions' / 'duplicate' / name
            duplicate.parent.mkdir()
            shutil.copyfile(archived[0], duplicate)
            duplicate.write_bytes(duplicate.read_bytes() + b'\n')
            duplicate_data = duplicate.read_bytes()
            duplicate_name = duplicate.relative_to(profile).as_posix()
            selection = [archived[0].relative_to(profile).as_posix(), duplicate_name]
            selection += [path.name for path in profile.glob('state_*.sqlite')]
            selection += [path.name for path in profile.glob('thread_history_*.sqlite')]
            absent = '019b0000-0000-7000-8000-000000000003'
            group = mock_archive_backup.prepare(profile, shared, backups, key, [thread],
                                                extra_threads=[other, absent])
            manifest = json.loads((backups / 'latest' / 'manifest.json').read_text())['files']
            self.assertIn(other_path.relative_to(profile).as_posix(), manifest)
            self.assertEqual(group['extra_threads'], 2)
            self.assertEqual(group['absent_extra_threads'], 1)
            new_path = local_folder / f'rollout-2026-01-01T00-00-00-{absent}.jsonl'
            original_replace = cosync_restore_backup.replace_latest
            def add_new_thread(*args, **kwargs):
                verify = kwargs['pre_commit']
                def create_then_verify(stage, saved):
                    extra_rows = [json.loads(line) for line in other_path.read_text().splitlines()]
                    extra_rows[0]['payload']['id'] = absent
                    new_path.write_text(''.join(json.dumps(row) + '\n' for row in extra_rows))
                    verify(stage, saved)
                kwargs['pre_commit'] = create_then_verify
                return original_replace(*args, **kwargs)
            with patch.object(mock_archive_backup.backup, 'replace_latest', side_effect=add_new_thread):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_backup.prepare(profile, shared, backups, key, [thread],
                                                extra_threads=[other, absent])
            self.assertEqual(json.loads((backups / 'latest' / 'manifest.json').read_text())['files'], manifest)
            new_path.unlink()
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                before_other = db.execute('SELECT count(*) FROM thread_items WHERE thread_id = ?', (other,)).fetchone()[0]
            self.assertGreater(before_other, 0)
            arguments = (source, profile, shared, backups, key, thread,
                         [('/source/Nexus', str(project))], Path(shutil.which('codex')))
            with patch.object(mock_archive_restore, '_unarchive', side_effect=nexus_sync.SyncError('test interruption')):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*arguments)
            journal = backups / 'mock-archive-pending.json'
            self.assertFalse(journal.exists())
            self.assertTrue((backups / 'latest' / selection[0]).is_file())
            with patch.object(mock_archive_restore, '_unarchive',
                              side_effect=subprocess.TimeoutExpired('mock-codex', 1)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    mock_archive_restore.restore(*arguments)
            self.assertTrue(journal.is_file())
            mock_archive_restore.rollback(profile, shared, backups, key)
            self.assertFalse(journal.exists())
            with patch.object(mock_archive_restore.pages, 'reset_mock_projection',
                              side_effect=nexus_sync.SyncError('test interrupted after replacing versions')):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*arguments)
            self.assertFalse(journal.exists())
            self.assertEqual(duplicate.read_bytes(), duplicate_data)
            self.assertEqual(len(list((profile / 'archived_sessions').rglob(name))), 2)
            mock_archive_restore.restore(source, profile, shared, backups, key, thread,
                                         [('/source/Nexus', str(project))], Path(shutil.which('codex')),
                                         title='synthetic remote title')
            latest = backups / 'latest'
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            active = list((profile / 'sessions').rglob(name))
            self.assertEqual(len(active), 1)
            active_path = active[0]
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM thread_items WHERE thread_id = ?', (other,)).fetchone()[0], before_other)
            server = start()
            try:
                call(server, 2, 'thread/resume', {'threadId': thread, 'path': str(active_path), 'cwd': str(project)})
                data = call(server, 3, 'thread/turns/list', {'threadId': thread, 'itemsView': 'full', 'limit': 10})
                self.assertIn('synthetic remote version', json.dumps(data))
                listing = call(server, 4, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
                self.assertIn(thread, [item['id'] for item in listing['data']])
            finally:
                stop_server(server)
            self.assertIn('synthetic local version', (latest / selection[0]).read_text())
            self.assertEqual((latest / duplicate_name).read_bytes(), duplicate_data)
            self.assertFalse(duplicate.exists())
            server = start()
            try:
                call(server, 2, 'thread/archive', {'threadId': thread})
            finally:
                stop_server(server)
            original_index = mock_archive_restore.pages.index_thread
            def verified_then_fail(*args, **kwargs):
                original_index(*args, **kwargs)
                raise nexus_sync.SyncError('test interrupted after verified indexing')
            with patch.object(mock_archive_restore.pages, 'index_thread', side_effect=verified_then_fail):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*arguments, title='synthetic changed title')
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT archived FROM threads WHERE id=?', (thread,)).fetchone(), (1,))
            verified_contents = []
            def index_then_fail(*args, **kwargs):
                original_index(*args, **kwargs)
                restored = list((profile / 'sessions').rglob(name))
                verified_contents.append(restored[0].read_bytes())
                restored[0].write_bytes(verified_contents[0] + b'\n')
                raise nexus_sync.SyncError('test interrupted after indexing')
            with patch.object(mock_archive_restore.pages, 'index_thread', side_effect=index_then_fail):
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*arguments, title='synthetic changed title')
            self.assertTrue((backups / 'mock-archive-pending.json').is_file())
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_backup.prepare(profile, shared, backups, key, [thread])
            self.assertTrue((backups / 'latest' / 'state_5.sqlite').is_file())
            journal = backups / 'mock-archive-pending.json'
            journal_before = journal.read_bytes()
            pending = json.loads(journal_before)
            altered = dict(pending, source_digest='0' * 64)
            journal.write_text(json.dumps(altered))
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.rollback(profile, shared, backups, key)
            journal.write_bytes(journal_before)
            manifest_path = backups / 'latest' / 'manifest.json'
            manifest_before = manifest_path.read_bytes()
            manifest = json.loads(manifest_before)
            manifest['files']['state_5.sqlite'] = '0' * 64
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.rollback(profile, shared, backups, key)
            manifest_path.write_bytes(manifest_before)
            collision = profile / pending['previous_files'][0]
            collision.parent.mkdir(parents=True, exist_ok=True)
            collision.write_text('unrelated file')
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.rollback(profile, shared, backups, key)
            self.assertEqual(collision.read_text(), 'unrelated file')
            collision.unlink()
            changed = list((profile / 'sessions').rglob(name))
            self.assertEqual(len(changed), 1)
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.rollback(profile, shared, backups, key)
            self.assertTrue((backups / 'mock-archive-pending.json').is_file())
            changed[0].write_bytes(verified_contents[0])
            mock_archive_restore.rollback(profile, shared, backups, key)
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            self.assertEqual(len(list((profile / 'sessions').rglob(name))), 0)
            restored = list((profile / 'archived_sessions').rglob(name))
            self.assertEqual(len(restored), 1)
            self.assertIn('synthetic remote version', restored[0].read_text())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT archived FROM threads WHERE id=?', (thread,)).fetchone(), (1,))
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM thread_items WHERE thread_id=?', (other,)).fetchone()[0], before_other)
            source_path = remote_folder / name
            rows = [json.loads(line) for line in source_path.read_text().splitlines()]
            self.assertGreaterEqual(len(rows), 3)
            prefix, suffix = rows[:2], rows[2:]
            source_path.write_text(''.join(json.dumps(row) + '\n' for row in prefix))
            new_header = dict(prefix[0])
            new_header['ordinal'] = prefix[-1]['ordinal'] + 1
            new_header['payload'] = dict(prefix[0]['payload'], history_base={
                'thread_id': thread, 'end_byte_offset': source_path.stat().st_size,
                'end_ordinal_exclusive': new_header['ordinal']})
            for row in suffix:
                row['ordinal'] += 1
            page = '019b0000-0000-7000-8000-000000000099'
            page_path = remote_folder / (name[:-6] + '_' + page + '.jsonl')
            page_path.write_text(''.join(json.dumps(row) + '\n' for row in [new_header, *suffix]))
            with self.assertRaises(nexus_sync.SyncError):
                mock_archive_restore.restore(*arguments)
            self.assertFalse((backups / 'mock-archive-pending.json').exists())
            original_page = page_path.read_bytes()
            with page_path.open('a', encoding='utf-8') as out:
                out.write(json.dumps({'type': 'turn_context', 'ordinal': suffix[-1]['ordinal'] + 1,
                                      'payload': {'cwd': '/missing/previous/workspace'}}) + '\n')
            report_stage = root / 'report-stage'
            mock_archive_restore.pages.prepare_flattened_mock(source, report_stage, thread,
                [('/source/Nexus', str(project))], fallback_root=project)
            report = json.loads((report_stage / 'cosyncx-path-report.json').read_text())
            self.assertIn('/missing/previous/workspace', report['unmapped_working_directories'])
            page_path.write_bytes(original_page)
            page_rows = [json.loads(line) for line in page_path.read_text().splitlines()]
            page_rows[0]['payload']['cwd'] = '/missing/previous/workspace'
            page_path.write_text(''.join(json.dumps(row) + '\n' for row in page_rows))
            if os.name != 'nt':
                backups.chmod(0o755)
                with self.assertRaises(nexus_sync.SyncError):
                    mock_archive_restore.restore(*arguments, flatten_multi=True, fallback_root=project)
                self.assertFalse(journal.exists())
            backups.chmod(0o700)
            result = mock_archive_restore.restore(*arguments, flatten_multi=True, fallback_root=project)
            self.assertTrue(result['path_report'].is_file())
            self.assertEqual(result['path_report'].stat().st_mode & 0o777, 0o600)
            report = json.loads(result['path_report'].read_text())
            self.assertIn('/missing/previous/workspace', report['unmapped_working_directories'])
            self.assertEqual(report['fallback_workspace'], str(project))
            self.assertEqual(report['authentication'], nexus_sync._manifest_tag(report, key))
            server = start()
            try:
                data = call(server, 2, 'thread/turns/list', {'threadId': thread, 'itemsView': 'full', 'limit': 10})
                self.assertIn('synthetic remote version', json.dumps(data))
            finally:
                stop_server(server)
            server = start()
            try:
                call(server, 2, 'thread/archive', {'threadId': thread})
            finally:
                stop_server(server)
            previous_reports = set(backups.glob('mock-path-report-*.json'))
            original_unlink = Path.unlink
            failed = [False]
            def interrupt_journal(path, *args, **kwargs):
                if path == journal and not failed[0]:
                    failed[0] = True
                    raise OSError('synthetic interruption after report creation')
                return original_unlink(path, *args, **kwargs)
            with patch.object(Path, 'unlink', interrupt_journal):
                with self.assertRaises(OSError):
                    mock_archive_restore.restore(*arguments, flatten_multi=True, fallback_root=project)
            self.assertFalse(journal.exists())
            self.assertEqual(set(backups.glob('mock-path-report-*.json')), previous_reports)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT archived FROM threads WHERE id=?', (thread,)).fetchone(), (1,))


if __name__ == '__main__': unittest.main()
