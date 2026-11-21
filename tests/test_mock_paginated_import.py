import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock as unit_mock

import mock_chat_sync as mock
from mock_direct_receiver import MockDirectReceiver
import mock_paginated_import as importer
import mock_graph_restore
import nexus_sync as ns
from test_codex_contract import read_rpc_response, stop_server


class MockPaginatedImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cosyncx-page-import-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source, self.profile, self.shared, self.project = (self.root / x for x in ('source', 'profile', 'shared', 'Nexus'))
        folder = self.source / 'sessions' / '2026' / '01' / '01'
        folder.mkdir(parents=True)
        self.shared.mkdir()
        self.project.mkdir()
        self.key = b'p' * 32
        mock.init_mock_profile(self.profile, self.key, self.shared)
        self.thread = '019b0000-0000-7000-8000-000000000001'
        self.page = '019b0000-0000-7000-8000-000000000002'
        stem = f'rollout-2026-01-01T00-00-00-{self.thread}'
        self.first = folder / (stem + '.jsonl')
        self.last = folder / (stem + '_' + self.page + '.jsonl')
        meta = {'id': self.thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                'model_provider': 'openai', 'history_mode': 'paginated'}
        records = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta, 'ordinal': 0},
                   {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'ordinal': 1,
                    'payload': {'type': 'user_message', 'message': 'synthetic page', 'images': [], 'local_images': [], 'text_elements': []}}]
        self.first.write_text(''.join(json.dumps(item) + '\n' for item in records))
        child = dict(meta, history_base={'thread_id': self.thread, 'end_byte_offset': self.first.stat().st_size, 'end_ordinal_exclusive': 2})
        self.last.write_text(json.dumps({'timestamp': '2026-01-01T00:00:02.000Z', 'type': 'session_meta', 'payload': child, 'ordinal': 2}) + '\n')

    def test_expected_messages_keep_local_image_reference(self):
        path = '/missing/old-image.png'
        rows = [
            {'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'synthetic',
                'images': [], 'local_images': [path], 'text_elements': []}},
            {'type': 'event_msg', 'payload': {'type': 'item_completed', 'item': {'type': 'UserMessage',
                'content': [{'type': 'text', 'text': 'synthetic'}, {'type': 'local_image', 'path': path}]}}}]
        file = self.root / 'synthetic-history.jsonl'
        file.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        expected = importer._expected_visible_messages(file)
        self.assertEqual(expected[importer._text_tag('local_image', path)], 1)
        self.assertEqual(expected[importer._text_tag('user', 'synthetic')], 1)

    def test_adds_pages_and_refuses_existing_thread(self):
        self.assertEqual(importer.add_thread(self.source, self.profile, self.shared, self.key, self.thread,
                                             [('/source/Nexus', str(self.project))]), 2)
        catalog = ns._chat_catalog(self.profile, include_unsupported=True)
        self.assertEqual(ns._paged_lineage(catalog[self.thread], self.thread), 'PRESENT_UNVERIFIED')
        with self.assertRaises(ns.SyncError):
            importer.add_thread(self.source, self.profile, self.shared, self.key, self.thread,
                                [('/source/Nexus', str(self.project))])

    def test_update_backs_up_existing_pages_and_refuses_divergence(self):
        importer.add_thread(self.source, self.profile, self.shared, self.key, self.thread,
                            [('/source/Nexus', str(self.project))])
        backups = self.root / 'backups'
        backups.mkdir()
        local_last = self.profile / self.last.relative_to(self.source)
        before = local_last.read_bytes()
        later = {'timestamp': '2026-01-01T00:00:03.000Z', 'type': 'event_msg', 'ordinal': 3,
                 'payload': {'type': 'user_message', 'message': 'synthetic continuation', 'images': [],
                             'local_images': [], 'text_elements': []}}
        with self.last.open('a') as stream:
            stream.write(json.dumps(later) + '\n')
        self.assertTrue(importer.update_thread(self.source, self.profile, self.shared, backups, self.key,
                                               self.thread, [('/source/Nexus', str(self.project))]))
        self.assertEqual((backups / 'latest' / local_last.relative_to(self.profile)).read_bytes(), before)
        self.assertIn('synthetic continuation', local_last.read_text())
        with local_last.open('a') as stream:
            stream.write(json.dumps({'type': 'event_msg', 'ordinal': 4, 'payload': {'type': 'local'}}) + '\n')
        with self.last.open('a') as stream:
            stream.write(json.dumps({'type': 'event_msg', 'ordinal': 4, 'payload': {'type': 'remote'}}) + '\n')
        before = local_last.read_bytes()
        with self.assertRaises(ns.SyncError):
            importer.update_thread(self.source, self.profile, self.shared, backups, self.key,
                                   self.thread, [('/source/Nexus', str(self.project))])
        self.assertEqual(local_last.read_bytes(), before)

    def test_interrupted_update_can_be_recovered_from_latest_backup(self):
        importer.add_thread(self.source, self.profile, self.shared, self.key, self.thread,
                            [('/source/Nexus', str(self.project))])
        backups = self.root / 'backups'
        backups.mkdir()
        target = self.profile / self.last.relative_to(self.source)
        original = target.read_bytes()
        with self.last.open('a') as stream:
            stream.write(json.dumps({'type': 'event_msg', 'ordinal': 3, 'payload': {'type': 'remote'}}) + '\n')
        new_id = '019b0000-0000-7000-8000-000000000003'
        stem = self.first.stem + '_' + new_id + '.jsonl'
        child = json.loads(self.last.read_text().splitlines()[0])
        child['ordinal'] = 4
        child['payload']['history_base'] = {'thread_id': self.page, 'end_byte_offset': self.last.stat().st_size,
                                             'end_ordinal_exclusive': 4}
        self.first.with_name(stem).write_text(json.dumps(child) + '\n')
        with unit_mock.patch.object(importer.os, 'link', side_effect=OSError('synthetic interruption')):
            with self.assertRaises(OSError):
                importer.update_thread(self.source, self.profile, self.shared, backups, self.key,
                                       self.thread, [('/source/Nexus', str(self.project))])
        self.assertNotEqual(target.read_bytes(), original)
        self.assertTrue((backups / 'mock-update-pending.json').is_file())
        importer.recover_pending(self.profile, self.shared, backups, self.key)
        self.assertEqual(target.read_bytes(), original)
        self.assertFalse((backups / 'mock-update-pending.json').exists())

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_codex_reads_imported_pages(self):
        importer.add_thread(self.source, self.profile, self.shared, self.key, self.thread,
                            [('/source/Nexus', str(self.project))])
        with self.assertRaises(ns.SyncError):
            importer.index_thread(self.profile, self.shared, self.key, self.thread, Path(shutil.which('codex')))
        last = self.profile / self.last.relative_to(self.source)
        server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
            env={**os.environ, 'CODEX_HOME': str(self.profile)})
        def call(number, method, params):
            server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
            server.stdin.flush()
            return read_rpc_response(server, number)
        try:
            self.assertIn('result', call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-test', 'version': '0.1'},
                                                          'capabilities': {'experimentalApi': True}}))
            server.stdin.write(b'{"method":"initialized","params":{}}\n')
            server.stdin.flush()
            self.assertIn('result', call(2, 'thread/resume', {'threadId': self.thread, 'path': str(last), 'cwd': str(self.project)}))
            self.assertIn('result', call(3, 'thread/turns/list', {'threadId': self.thread, 'itemsView': 'full', 'limit': 10}))
            listing = call(4, 'thread/list', {'limit': 100, 'useStateDbOnly': True})
            self.assertIn(self.thread, [item['id'] for item in listing.get('result', {}).get('data', [])])
        finally:
            stop_server(server)

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_codex_lists_messages_in_both_paginated_pages(self):
        self.last.unlink()
        meta = {'id': self.thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}
        rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta}]
        for number in (1, 2):
            stamp = f'2026-01-01T00:0{number}:00.000Z'
            message = f'synthetic page message {number}'
            rows.extend([{'timestamp': stamp, 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                         'content': [{'type': 'input_text', 'text': message}]}},
                         {'timestamp': stamp, 'type': 'event_msg', 'payload': {'type': 'user_message',
                          'message': message, 'images': [], 'local_images': [], 'text_elements': []}}])
        self.first.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
            env={**os.environ, 'CODEX_HOME': str(self.source)}, capture_output=True, text=True, timeout=30)
        self.assertEqual(migration.returncode, 0, migration.stderr)
        migrated = [json.loads(line) for line in self.first.read_text().splitlines()]
        prefix, suffix = migrated[:4], migrated[4:]
        self.first.write_text(''.join(json.dumps(row) + '\n' for row in prefix))
        child = dict(prefix[0])
        child['ordinal'] = 4
        child['payload'] = dict(prefix[0]['payload'], history_base={'thread_id': self.thread,
            'end_byte_offset': self.first.stat().st_size, 'end_ordinal_exclusive': 4})
        for row in suffix: row['ordinal'] += 1
        self.last.write_text(''.join(json.dumps(row) + '\n' for row in [child, *suffix]))
        flattened = self.root / 'flattened'
        importer.prepare_flattened_mock(self.source, flattened, self.thread, [('/source/Nexus', str(self.project))])
        flattened_file = next((flattened / 'sessions').rglob('*.jsonl'))
        target = self.profile / flattened_file.relative_to(flattened)
        target.parent.mkdir(parents=True)
        shutil.copyfile(flattened_file, target)
        importer.index_thread(self.profile, self.shared, self.key, self.thread, Path(shutil.which('codex')))
        server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
            env={**os.environ, 'CODEX_HOME': str(self.profile)})
        def call(number, method, params):
            server.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
            server.stdin.flush()
            return read_rpc_response(server, number)
        try:
            call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-test', 'version': '0.1'},
                                   'capabilities': {'experimentalApi': True}})
            server.stdin.write(b'{"method":"initialized","params":{}}\n')
            server.stdin.flush()
            data = call(2, 'thread/turns/list', {'threadId': self.thread, 'itemsView': 'full', 'limit': 10})
            self.assertIn('synthetic page message 1', json.dumps(data))
            self.assertIn('synthetic page message 2', json.dumps(data))
        finally:
            stop_server(server)
        second_profile = self.root / 'second-profile'
        mock.init_mock_profile(second_profile, self.key, self.shared)
        receiver = MockDirectReceiver(source=self.source, profile=second_profile, shared=self.shared, key=self.key,
            source_root='/source/Nexus', target_root=self.project, codex=shutil.which('codex'),
            folder_id='chats', peer_id='vps', quiet_seconds=0)
        state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                 'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
        def read(): return 'chats', state, True, 'valid'
        self.assertEqual(receiver.poll(read, now=0)['state'], 'WAITING')
        result = receiver.poll(read, now=1)
        self.assertEqual(result['state'], 'COMPLETE')
        self.assertEqual(result['restore_event']['changed_chats'], 1)
        self.assertTrue(result['restore_event']['mock_only'])

        # A flattened parent still needs its separately indexed child and edge.
        child_id = '019b0000-0000-7000-8000-000000000003'
        child_path = self.profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{child_id}.jsonl'
        child_home = self.root / 'child-build'
        (child_home / 'sessions').mkdir(parents=True)
        build_child = child_home / 'sessions' / child_path.name
        child_rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                       {'id': child_id, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': str(self.project),
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'model_provider': 'openai',
                        'source': {'subagent': {'other': 'guardian'}}, 'parent_thread_id': self.thread}},
                      {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                       {'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'synthetic child answer'}]}},
                      {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                       {'type': 'agent_message', 'message': 'synthetic child answer'}}]
        build_child.write_text(''.join(json.dumps(row) + '\n' for row in child_rows))
        migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
            env={**os.environ, 'CODEX_HOME': str(child_home)}, capture_output=True, text=True, timeout=30)
        self.assertEqual(migration.returncode, 0, migration.stderr)
        child_rows = [json.loads(line) for line in build_child.read_text().splitlines()]
        start = next(row['ordinal'] for row in child_rows if row.get('type') == 'response_item')
        child_rows[0]['payload']['subagent_history_start_ordinal'] = start
        child_path.write_text(''.join(json.dumps(row) + '\n' for row in child_rows))
        importer.index_thread(self.profile, self.shared, self.key, child_id, Path(shutil.which('codex')))
        graph_dir, backups = self.root / 'graph-input', self.root / 'graph-backups'
        graph_dir.mkdir()
        backups.mkdir()
        graph = graph_dir / 'graph.json'
        graph.write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'created_at_ms': 1,
            'omitted_archived': 0, 'omitted_missing': 0,
            'edges': [{'parent': self.thread, 'child': child_id, 'status': 'closed'}]}))
        self.assertEqual(mock_graph_restore.apply_graph(self.profile, self.shared, backups, graph, self.key), 1)
        server = subprocess.Popen([shutil.which('codex'), 'app-server', '--listen', 'stdio://'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
            env={**os.environ, 'CODEX_HOME': str(self.profile)})
        try:
            call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-test', 'version': '0.1'},
                                   'capabilities': {'experimentalApi': True}})
            server.stdin.write(b'{"method":"initialized","params":{}}\n')
            server.stdin.flush()
            result = call(2, 'thread/list', {'parentThreadId': self.thread, 'sourceKinds':
                ['subAgent', 'subAgentOther', 'subAgentThreadSpawn', 'subAgentReview', 'subAgentCompact'],
                'useStateDbOnly': True, 'limit': 100})
            self.assertIn(child_id, [item['id'] for item in result['result']['data']])
            result = call(3, 'thread/turns/list', {'threadId': child_id, 'itemsView': 'full', 'limit': 10})
            self.assertIn('synthetic child answer', json.dumps(result))
        finally:
            stop_server(server)

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_explicit_workspace_fallback_is_indexed_and_report_retained(self):
        self.last.unlink()
        rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                 {'id': self.thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/missing/old/workspace',
                  'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                 {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic message'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                 {'type': 'user_message', 'message': 'synthetic message', 'images': [], 'local_images': [], 'text_elements': []}}]
        self.first.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
            env={**os.environ, 'CODEX_HOME': str(self.source)}, capture_output=True, text=True, timeout=30)
        self.assertEqual(migration.returncode, 0, migration.stderr)
        staged = self.root / 'fallback-staged'
        importer.prepare_flattened_mock(self.source, staged, self.thread,
            [('/source/Nexus', str(self.project))], fallback_root=self.project)
        report = json.loads((staged / 'cosyncx-path-report.json').read_text())
        self.assertTrue(report['fallback_used'])
        self.assertIn('/missing/old/workspace', report['unmapped_working_directories'])
        source_file = next((staged / 'sessions').rglob('*.jsonl'))
        target = self.profile / source_file.relative_to(staged)
        target.parent.mkdir(parents=True)
        shutil.copyfile(source_file, target)
        # index_thread independently requires Codex to return the chosen cwd.
        importer.index_thread(self.profile, self.shared, self.key, self.thread, Path(shutil.which('codex')))

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_direct_receiver_defers_unverified_multi_page_thread(self):
        receiver = MockDirectReceiver(source=self.source, profile=self.profile, shared=self.shared, key=self.key,
            source_root='/source/Nexus', target_root=self.project, codex=shutil.which('codex'),
            folder_id='chats', peer_id='vps', quiet_seconds=0)
        state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                 'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
        def read(): return 'chats', state, True, 'valid'
        self.assertEqual(receiver.poll(read, now=0)['state'], 'WAITING')
        result = receiver.poll(read, now=1)
        self.assertEqual(result['state'], 'PENDING')
        self.assertNotIn('restore_event', result)

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
    def test_direct_receiver_defers_multi_page_update_without_verified_index(self):
        backups = self.root / 'backups'
        backups.mkdir()
        receiver = MockDirectReceiver(source=self.source, profile=self.profile, shared=self.shared, key=self.key,
            source_root='/source/Nexus', target_root=self.project, codex=shutil.which('codex'),
            folder_id='chats', peer_id='vps', quiet_seconds=0, backup_root=backups)
        state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                 'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
        def read(): return 'chats', state, True, 'valid'
        receiver.poll(read, now=0)
        result = receiver.poll(read, now=1)
        self.assertEqual(result['state'], 'PENDING')
        self.assertNotIn('restore_event', result)


if __name__ == '__main__': unittest.main()
