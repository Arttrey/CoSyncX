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
from cosync_restore_supervisor import RestoreSupervisor
import nexus_sync as ns
from test_codex_contract import read_rpc_response, stop_server


class DirectRestoreTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform in ('linux', 'darwin'), 'requires Unix writer locks')
    def test_busy_chat_is_retried_without_blocking_an_independent_chat(self):
        import fcntl
        with tempfile.TemporaryDirectory(prefix='cosyncx-busy-queue-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project): path.mkdir(mode=0o700)
            incoming = source / 'sessions'; incoming.mkdir()
            busy, free = ('019b0000-0000-7000-8000-0000000000d1', '019b0000-0000-7000-8000-0000000000d2')
            child = '019b0000-0000-7000-8000-0000000000d3'
            orphan = '019b0000-0000-7000-8000-0000000000d4'
            for identity in (busy, free, child, orphan):
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai', 'history_mode': 'paginated'}
                if identity == child:
                    meta.update({'source': {'subagent': {'other': 'guardian'}}, 'parent_thread_id': busy})
                (incoming / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(json.dumps(
                    {'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(''.join(
                json.dumps({'id': identity, 'thread_name': 'test'}) + '\n' for identity in (busy, free, orphan)))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': busy, 'child': child, 'status': 'closed'}],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT, status TEXT)')
                db.execute('INSERT INTO threads VALUES (?, 0)', (orphan,))
            lockdir = profile / 'thread-writer-locks'; lockdir.mkdir()
            (lockdir / '.coordination.lock').touch()
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            status = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                      'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(folder): return lambda: (folder, status, True, 'valid')
            readers = [read(folder) for folder in ('sessions', 'titles', 'graph')]
            def poll(now): return receiver.poll(readers[0], title_status_reader=readers[1], graph_status_reader=readers[2], now=now)
            def index(_profile, identity, _codex, *, title, **_kwargs):
                if title is not None:
                    with (profile / 'session_index.jsonl').open('a') as stream:
                        stream.write(json.dumps({'id': identity, 'thread_name': title}) + '\n')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=index) as indexer, \
                    patch('cosync_direct_restore.mock_graph_restore._apply_graph') as apply_graph:
                with (lockdir / (busy + '.lock')).open('a+b') as stream:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.assertEqual(poll(0)['state'], 'WAITING')
                    first = poll(1)
                    self.assertEqual(first['state'], 'PENDING')
                    self.assertEqual(first['restore_event']['changed_chats'], 1)
                    self.assertEqual(indexer.call_args.args[1], free)
                    self.assertNotIn(busy, ns._chat_catalog(profile, include_unsupported=True))
                    self.assertNotIn(child, ns._chat_catalog(profile, include_unsupported=True))
                    self.assertNotIn(orphan, ns._chat_catalog(profile, include_unsupported=True))
                    apply_graph.assert_not_called()
                    receiver.acknowledge(first['restore_event'])
                    self.assertEqual(poll(2)['state'], 'PENDING')
                second = poll(3)
                self.assertEqual(second['state'], 'PENDING')
                self.assertEqual(second['restore_event']['changed_chats'], 2)
                self.assertEqual([call.args[1] for call in indexer.call_args_list], [free, busy, child])
                apply_graph.assert_called_once()
                receiver.acknowledge(second['restore_event'])
                deferred = poll(4)
                self.assertEqual(deferred['state'], 'PENDING')
                if deferred.get('acknowledge_required'):
                    receiver.acknowledge(deferred.get('restore_event'))
                with sqlite3.connect(profile / 'state_5.sqlite') as db:
                    db.execute('DELETE FROM threads WHERE id=?', (orphan,))
                third = poll(5)
                self.assertEqual(third['state'], 'COMPLETE')
                self.assertGreaterEqual(third['restore_event']['changed_chats'], 1)
                receiver.acknowledge(third['restore_event'])
            self.assertEqual(set(ns._chat_catalog(profile, include_unsupported=True)), {busy, free, child, orphan})

    def test_active_source_archived_locally_does_not_report_complete(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-pending-archived-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project): path.mkdir(mode=0o700)
            incoming, archived = source / 'sessions', profile / 'archived_sessions'
            incoming.mkdir(); archived.mkdir()
            identity = '019b0000-0000-7000-8000-0000000000c1'
            meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            raw = json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}) + '\n'
            (incoming / name).write_text(raw); (archived / name).write_text(raw)
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?, 1)', (identity,))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            status = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                      'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(folder): return lambda: (folder, status, True, 'valid')
            readers = [read(folder) for folder in ('sessions', 'titles', 'graph')]
            self.assertEqual(receiver.poll(readers[0], title_status_reader=readers[1],
                                           graph_status_reader=readers[2], now=0)['state'], 'WAITING')
            self.assertEqual(receiver.poll(readers[0], title_status_reader=readers[1],
                                           graph_status_reader=readers[2], now=1)['state'], 'PENDING')
            self.assertEqual(receiver.poll(readers[0], title_status_reader=readers[1],
                                           graph_status_reader=readers[2], now=2)['state'], 'PENDING')
            self.assertEqual((archived / name).read_text(), raw)
            self.assertFalse((backups / 'latest').exists())

    def test_cursor_start_during_batch_stops_and_recovers(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-cursor-start-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            incoming = source / 'sessions'; incoming.mkdir()
            identities = ['019b0000-0000-7000-8000-0000000000a1', '019b0000-0000-7000-8000-0000000000a2']
            for identity in identities:
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai', 'history_mode': 'paginated'}
                (incoming / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(json.dumps(
                    {'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(''.join(
                json.dumps({'id': identity, 'thread_name': 'test'}) + '\n' for identity in identities))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions', 'titles', 'graph'), peer_id='vps')
            busy = [False]
            real_link = os.link
            def link_then_start(src, dst, **kwargs):
                result = real_link(src, dst, **kwargs)
                if Path(dst).parent == profile / 'sessions':
                    busy[0] = True
                return result
            with patch('cosync_direct_restore.quiescence.is_quiet', side_effect=lambda: not busy[0]), \
                    patch('cosync_direct_restore.os.link', side_effect=link_then_start), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexer:
                with self.assertRaises(ns.SyncError):
                    DirectReceiver(**args)._receive()
                self.assertEqual(len(list((profile / 'sessions').glob('*.jsonl'))), 1)
                indexer.assert_not_called()
                self.assertEqual(json.loads((backups / 'direct-restore-pending.json').read_text())['phase'], 'writing')
            busy[0] = False
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexer:
                self.assertEqual(DirectReceiver(**args)._receive(), 2)
                self.assertEqual(indexer.call_count, 2)
            self.assertEqual(len(list((profile / 'sessions').glob('*.jsonl'))), 2)

    def test_missing_workspace_restores_with_private_warning(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-missing-workspace-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            folder = source / 'sessions'; folder.mkdir()
            identity = '019b0000-0000-7000-8000-000000000081'
            foreign = '/foreign/missing-project'
            meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': foreign,
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            history = folder / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            history.write_text(json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta',
                                           'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions', 'titles', 'graph'), peer_id='vps')
            receiver = DirectReceiver(**args)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile'):
                self.assertEqual(receiver._receive(), 1)
            saved = json.loads((profile / 'sessions' / history.name).read_text())
            self.assertEqual(saved['payload']['cwd'], str(project))
            report_path = backups / 'missing-paths.json'
            self.assertEqual(report_path.stat().st_mode & 0o077, 0)
            report = json.loads(report_path.read_text())
            self.assertEqual(report['chats'][identity]['unmapped_working_directories'], [foreign])
            self.assertEqual(receiver.path_warning_count, 1)
            self.assertEqual(DirectReceiver(**args).path_warning_count, 1)

    def test_excluded_active_chat_and_its_children_are_untouched(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-excluded-chat-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            incoming, local = source / 'sessions', profile / 'sessions'
            incoming.mkdir(); local.mkdir()
            protected = '019b0000-0000-7000-8000-000000000061'
            child = '019b0000-0000-7000-8000-000000000062'
            other = '019b0000-0000-7000-8000-000000000063'
            for identity in (protected, child, other):
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai', 'history_mode': 'paginated'}
                if identity == child:
                    meta.update({'source': {'subagent': {'other': 'guardian'}}, 'parent_thread_id': protected})
                row = {'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}
                name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                (incoming / name).write_text(json.dumps(row) + '\n')
                if identity == protected:
                    row['payload']['cwd'] = str(project)
                    (local / name).write_text(json.dumps(row) + '\n' + json.dumps(
                        {'type': 'event_msg', 'ordinal': 1, 'payload': {'type': 'agent_message', 'message': 'local'}}) + '\n')
                    protected_path = local / name
            before = protected_path.read_bytes()
            (titles / 'session_index.jsonl').write_text(''.join(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n'
                                                               for identity in (protected, other)))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': protected, 'child': child, 'status': 'closed'}],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?, 0)', (protected,))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps',
                excluded_thread_ids=[protected])
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexer:
                self.assertEqual(receiver._receive(), 1)
            indexer.assert_called_once()
            self.assertEqual(indexer.call_args.args[1], other)
            self.assertEqual(protected_path.read_bytes(), before)
            self.assertFalse((local / f'rollout-2026-01-01T00-00-00-{child}.jsonl').exists())
            self.assertTrue((local / f'rollout-2026-01-01T00-00-00-{other}.jsonl').exists())
            empty_backups = root / 'empty-backups'
            empty_backups.mkdir(mode=0o700)
            read_only = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=empty_backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps',
                excluded_thread_ids=[protected, other])
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=False):
                self.assertEqual(read_only._receive(), 0)
            self.assertEqual(protected_path.read_bytes(), before)
            self.assertFalse((empty_backups / 'latest').exists())

    def test_source_change_after_indexing_is_reconciled_before_reload(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-source-change-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            sessions = source / 'sessions'; sessions.mkdir()
            identity = '019b0000-0000-7000-8000-000000000051'
            meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            history = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            history.write_text(json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta',
                                           'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE example (id INTEGER)')
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            def index(_profile, thread, *_args, **_kwargs):
                with sqlite3.connect(profile / 'state_5.sqlite') as db:
                    db.execute('INSERT OR IGNORE INTO threads VALUES (?, 0)', (thread,))
                (profile / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'test'}) + '\n')
            first = DirectReceiver(**args)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=index), \
                    patch('cosync_direct_restore.indexer._reset_profile_projection'):
                self.assertEqual(first._receive(), 1)
                original_id = json.loads((backups / 'direct-restore-pending.json').read_text())['event_id']
                with history.open('a') as out:
                    out.write(json.dumps({'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg',
                        'ordinal': 1, 'payload': {'type': 'agent_message', 'message': 'later source'}}) + '\n')
                receiver = DirectReceiver(**args)
                self.assertEqual(receiver._receive(), 1)
                self.assertTrue((backups / 'direct-restore-carry.json').is_file())
                state = {'state': 'idle', 'sequence': 2, 'remoteSequence': {'vps': 2}, 'needFiles': 0,
                         'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
                def read(folder): return lambda: (folder, state, True, 'valid')
                readers = (read('sessions'), read('titles'), read('graph'))
                self.assertEqual(receiver.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=0)['state'], 'WAITING')
                result = receiver.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=1)
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['restore_event']['id'], original_id)
            self.assertEqual(result['restore_event']['changed_chats'], 2)
            self.assertIn(b'later source', (profile / 'sessions' / history.name).read_bytes())
            receiver.acknowledge(result['restore_event'])
            self.assertFalse((backups / 'direct-restore-carry.json').exists())
            self.assertFalse((backups / 'direct-restore-pending.json').exists())

    def test_source_change_during_interrupted_indexing_is_reconciled(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-index-new-source-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            sessions = source / 'sessions'; sessions.mkdir()
            identity = '019b0000-0000-7000-8000-000000000071'
            meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            history = sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            history.write_text(json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta',
                                           'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE example (id INTEGER)')
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            calls = []
            def index(_profile, thread, *_args, **_kwargs):
                calls.append(thread)
                if len(calls) == 1:
                    raise ns.SyncError('synthetic interruption')
                with sqlite3.connect(profile / 'state_5.sqlite') as db:
                    db.execute('INSERT OR IGNORE INTO threads VALUES (?, 0)', (thread,))
                (profile / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'test'}) + '\n')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=index), \
                    patch('cosync_direct_restore.indexer._reset_profile_projection'):
                with self.assertRaises(ns.SyncError):
                    DirectReceiver(**args)._receive()
                original = json.loads((backups / 'direct-restore-pending.json').read_text())['event_id']
                with history.open('a') as out:
                    out.write(json.dumps({'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg',
                        'ordinal': 1, 'payload': {'type': 'agent_message', 'message': 'later source'}}) + '\n')
                excluded = DirectReceiver(**args, excluded_thread_ids=[identity])
                self.assertIsNone(excluded._receive())
                self.assertEqual(len(calls), 1)
                receiver = DirectReceiver(**args)
                self.assertEqual(receiver._receive(), 1)
                carry = receiver._read_carry()
                self.assertEqual(carry, [{'id': original, 'count': 1}])
            self.assertIn(b'later source', (profile / 'sessions' / history.name).read_bytes())

    def test_resume_after_one_indexed_chat_does_not_repeat_it(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-index-resume-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            sessions = source / 'sessions'; sessions.mkdir()
            identities = ['019b0000-0000-7000-8000-000000000041', '019b0000-0000-7000-8000-000000000042']
            for identity in identities:
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai', 'history_mode': 'paginated'}
                (sessions / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}) + '\n')
            (titles / 'session_index.jsonl').write_text(''.join(
                json.dumps({'id': identity, 'thread_name': 'test'}) + '\n' for identity in identities))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            calls = []
            def index(_profile, identity, *_args, **_kwargs):
                calls.append(identity)
                if len(calls) == 2:
                    raise ns.SyncError('synthetic interruption')
                with sqlite3.connect(profile / 'state_5.sqlite') as db:
                    db.execute('INSERT INTO threads VALUES (?, 0)', (identity,))
            receiver = DirectReceiver(**args)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=index):
                with self.assertRaises(ns.SyncError):
                    receiver._receive()
            record = json.loads((backups / 'direct-restore-pending.json').read_text())
            self.assertEqual((record['phase'], record['next_index']), ('indexing', 1))
            resumed = DirectReceiver(**args)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=index):
                self.assertEqual(resumed._receive(), 2)
            self.assertEqual(calls, [identities[0], identities[1], identities[1]])
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual({row[0] for row in db.execute('SELECT id FROM threads')}, set(identities))

    def test_multipage_source_with_external_page_reference_is_deferred(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-page-reference-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            folder = source / 'sessions'; folder.mkdir()
            parent = '019b0000-0000-7000-8000-000000000031'
            page = '019b0000-0000-7000-8000-000000000032'
            child = '019b0000-0000-7000-8000-000000000033'
            meta = {'id': parent, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                    'model_provider': 'openai', 'history_mode': 'paginated'}
            first = folder / f'rollout-2026-01-01T00-00-00-{parent}.jsonl'
            first.write_text(json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta',
                                         'payload': meta, 'ordinal': 0}) + '\n')
            second = folder / f'rollout-2026-01-01T00-00-00-{parent}_{page}.jsonl'
            second_meta = dict(meta, history_base={'thread_id': parent, 'end_byte_offset': first.stat().st_size,
                                                   'end_ordinal_exclusive': 1})
            second.write_text(json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta',
                                          'payload': second_meta, 'ordinal': 1}) + '\n')
            child_meta = dict(meta, id=child, source={'subagent': {'other': 'guardian'}}, parent_thread_id=parent,
                              history_base={'thread_id': page, 'end_byte_offset': second.stat().st_size,
                                            'end_ordinal_exclusive': 2})
            (folder / f'rollout-2026-01-01T00-00-00-{child}.jsonl').write_text(
                json.dumps({'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': child_meta,
                            'ordinal': 2}) + '\n')
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': parent, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertIsNone(receiver._receive())
            self.assertFalse((profile / 'sessions').exists())
            self.assertFalse((backups / 'latest').exists())

    def test_metadata_can_be_sibling_folders_under_source_but_not_inside_sessions(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-direct-layout-') as tmp:
            root = Path(tmp).resolve()
            source, profile, backups, project = (root / name for name in ('source', 'profile', 'backups', 'project'))
            for path in (source, profile, backups, project):
                path.mkdir(mode=0o700)
            sessions, titles, graph = (source / name for name in ('sessions', 'titles', 'graph'))
            for path in (sessions, titles, graph):
                path.mkdir()
            args = dict(source=source, titles=titles, graph=graph, profile=profile, backup_root=backups,
                        key=b'k' * 32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                        folder_ids=('sessions', 'titles', 'graph'), peer_id='vps')
            DirectReceiver(**args)
            nested = sessions / 'titles'
            nested.mkdir()
            with self.assertRaises(ns.SyncError):
                DirectReceiver(**{**args, 'titles': nested})

    def test_index_archived_chat_does_not_block_unrelated_chat(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-index-archived-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            incoming = source / 'sessions'; incoming.mkdir()
            local = profile / 'sessions'; local.mkdir()
            archived, child, other = ('019b0000-0000-7000-8000-000000000091',
                '019b0000-0000-7000-8000-000000000092', '019b0000-0000-7000-8000-000000000093')
            for identity in (archived, child, other):
                meta = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                        'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                        'model_provider': 'openai', 'history_mode': 'paginated'}
                if identity == child:
                    meta.update({'source': {'subagent': {'other': 'guardian'}}, 'parent_thread_id': archived})
                row = {'timestamp': meta['timestamp'], 'type': 'session_meta', 'payload': meta, 'ordinal': 0}
                name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                (incoming / name).write_text(json.dumps(row) + '\n')
                if identity == archived:
                    row['payload']['cwd'] = str(project)
                    (local / name).write_text(json.dumps(row) + '\n')
                    protected_path = local / name
            before = protected_path.read_bytes()
            (titles / 'session_index.jsonl').write_text(''.join(json.dumps({'id': identity, 'thread_name': 'test'}) + '\n'
                                                               for identity in (archived, other)))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': archived, 'child': child, 'status': 'closed'}],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?, 1)', (archived,))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexer:
                self.assertEqual(receiver._receive(), 1)
            self.assertEqual(indexer.call_args.args[1], other)
            self.assertEqual(protected_path.read_bytes(), before)
            self.assertFalse((local / f'rollout-2026-01-01T00-00-00-{child}.jsonl').exists())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT archived FROM threads WHERE id=?', (archived,)).fetchone(), (1,))

    def test_existing_archived_history_is_not_made_active(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-keep-archive-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / name for name in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for path in (source, titles, graph, profile, backups, project):
                path.mkdir(mode=0o700)
            active, archived = source / 'sessions', profile / 'archived_sessions'
            active.mkdir(); archived.mkdir()
            thread = '019b0000-0000-7000-8000-000000000021'
            filename = f'rollout-2026-01-01T00-00-00-{thread}.jsonl'
            def record(cwd):
                return json.dumps({'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'ordinal': 0,
                    'payload': {'id': thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': cwd,
                                'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                                'model_provider': 'openai', 'history_mode': 'paginated'}}) + '\n'
            (active / filename).write_text(record('/source/Nexus'))
            archived_file = archived / filename
            archived_file.write_text(record(str(project)))
            before = archived_file.read_bytes()
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'test'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            new_thread = '019b0000-0000-7000-8000-000000000022'
            new_file = active / f'rollout-2026-01-01T00-00-00-{new_thread}.jsonl'
            new_record = json.loads(record('/source/Nexus'))
            new_record['payload']['id'] = new_thread
            new_file.write_text(json.dumps(new_record) + '\n')
            child_thread = '019b0000-0000-7000-8000-000000000023'
            child_record = json.loads(record('/source/Nexus'))
            child_record['payload'].update({'id': child_thread, 'source': {'subagent': {'other': 'guardian'}},
                                            'parent_thread_id': thread})
            child_file = active / f'rollout-2026-01-01T00-00-00-{child_thread}.jsonl'
            child_file.write_text(json.dumps(child_record) + '\n')
            orphan_thread = '019b0000-0000-7000-8000-000000000024'
            orphan = json.loads(record('/source/Nexus'))
            orphan['payload'].update({'id': orphan_thread, 'source': {'subagent': {'other': 'guardian'}},
                'parent_thread_id': '019b0000-0000-7000-8000-000000000099'})
            orphan_file = active / f'rollout-2026-01-01T00-00-00-{orphan_thread}.jsonl'
            orphan_file.write_text(json.dumps(orphan) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': thread, 'child': child_thread, 'status': 'closed'}],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with (titles / 'session_index.jsonl').open('a') as stream:
                stream.write(json.dumps({'id': new_thread, 'thread_name': 'new'}) + '\n')
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?, 1)', (thread,))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexer:
                self.assertEqual(receiver._receive(), 1)
            indexer.assert_called_once()
            self.assertEqual(indexer.call_args.args[1], new_thread)
            self.assertEqual(archived_file.read_bytes(), before)
            self.assertTrue((profile / 'sessions' / new_file.name).is_file())
            self.assertFalse((profile / 'sessions' / filename).exists())
            self.assertFalse((profile / 'sessions' / child_file.name).exists())
            self.assertFalse((profile / 'sessions' / orphan_file.name).exists())

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
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
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
                db.execute('INSERT INTO threads VALUES (?, 0)', (thread,))
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(poll(3)['state'], 'PENDING')
            self.assertFalse((backups / 'latest').exists())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('DELETE FROM threads WHERE id=?', (thread,))
            real_link = os.link
            def link_then_interrupt(*args, **kwargs):
                real_link(*args, **kwargs)
                raise ns.SyncError('interrupted during copy')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.os.link', side_effect=link_then_interrupt):
                with self.assertRaises(ns.SyncError):
                    receiver._receive()
            interrupted = profile / history.relative_to(source)
            written = interrupted.read_bytes()
            interrupted.write_bytes(written + b'local change')
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertIsNone(receiver._receive())
            self.assertEqual(interrupted.read_bytes(), written + b'local change')
            interrupted.write_bytes(written)
            unrelated = '019b0000-0000-7000-8000-000000000099'
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('INSERT INTO threads VALUES (?, 0)', (unrelated,))
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertIsNone(receiver._receive())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertIsNotNone(db.execute('SELECT id FROM threads WHERE id=?', (unrelated,)).fetchone())
            with sqlite3.connect(backups / 'latest' / 'state_5.sqlite') as saved:
                with sqlite3.connect(profile / 'state_5.sqlite') as live:
                    saved.backup(live)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexed:
                result = poll(4)
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['restore_event']['changed_chats'], 1)
            self.assertTrue(result['restore_event']['indexed'])
            indexed.assert_called_once()
            self.assertTrue((profile / history.relative_to(source)).is_file())
            self.assertTrue((backups / 'latest' / 'state_5.sqlite').is_file())
            journal = backups / 'direct-restore-pending.json'
            self.assertTrue(journal.exists())
            # Restart before Cursor receives the event: use the same event ID
            # after rechecking the source and index rather than losing it.
            restarted = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as rechecked:
                self.assertEqual(restarted.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=5)['state'], 'WAITING')
                saved_backup = backups / 'latest' / 'state_5.sqlite'
                original_backup = saved_backup.read_bytes()
                saved_backup.write_bytes(original_backup + b'changed')
                self.assertEqual(restarted.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=6)['state'], 'PENDING')
                saved_backup.write_bytes(original_backup)
                original_history = history.read_bytes()
                history.write_bytes(original_history + b'changed')
                self.assertEqual(restarted.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=7)['state'], 'PENDING')
                history.write_bytes(original_history)
                repeated = restarted.poll(readers[0], title_status_reader=readers[1],
                    graph_status_reader=readers[2], now=8)
            self.assertEqual(repeated['restore_event']['id'], result['restore_event']['id'])
            rechecked.assert_called_once()
            status_path = root / 'restore-status.json'
            supervisor = RestoreSupervisor(restarted, readers[0], status_path, mock_only=False,
                                           title_reader=readers[1], graph_reader=readers[2])
            with patch('cosync_restore_status.write_status', side_effect=OSError('synthetic write failure')):
                self.assertEqual(supervisor.tick()['state'], 'ERROR')
            self.assertTrue(journal.exists())
            self.assertEqual(supervisor.tick()['state'], 'COMPLETE')
            self.assertEqual(json.loads(status_path.read_text())['restore_event']['id'], result['restore_event']['id'])
            self.assertFalse(journal.exists())
            # A restarted watcher can verify an existing, locally extended
            # history without replacing it or requesting a window reload.
            (profile / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'test'}) + '\n')
            local = profile / history.relative_to(source)
            with local.open('a') as stream:
                stream.write(json.dumps({'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg',
                    'ordinal': 1, 'payload': {'type': 'agent_message', 'message': 'local continuation'}}) + '\n')
            local_before = local.read_bytes()
            existing = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            supervisor = RestoreSupervisor(existing, readers[0], status_path, mock_only=False,
                                           title_reader=readers[1], graph_reader=readers[2])
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as verify_existing:
                self.assertEqual(supervisor.tick()['state'], 'WAITING')
                no_change = supervisor.tick()
            self.assertEqual(no_change['state'], 'COMPLETE')
            self.assertIsNone(no_change['restore_event'])
            verify_existing.assert_called_once()
            self.assertEqual(local.read_bytes(), local_before)
            self.assertFalse(journal.exists())
            with history.open('a') as stream:
                stream.write(json.dumps({'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg',
                    'ordinal': 1, 'payload': {'type': 'agent_message', 'message': 'different remote continuation'}}) + '\n')
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE example (id INTEGER)')
            conflicting = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile') as indexed_update, \
                    patch('cosync_direct_restore.indexer._reset_profile_projection') as reset_projection:
                self.assertEqual(conflicting._receive(), 1)
            indexed_update.assert_called_once()
            reset_projection.assert_called_once()
            self.assertIn(b'different remote continuation', local.read_bytes())
            self.assertEqual((backups / 'latest' / local.relative_to(profile)).read_bytes(), local_before)

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
            meta = {'id': thread, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/missing-project',
                    'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': meta},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'received message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'received message', 'images': [], 'local_images': [], 'text_elements': []}},
                    {'timestamp': '2026-01-01T00:00:02.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'later source message'}]}},
                    {'timestamp': '2026-01-01T00:00:02.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'later source message', 'images': [], 'local_images': [], 'text_elements': []}}]
            history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            (titles / 'session_index.jsonl').write_text(json.dumps({'id': thread, 'thread_name': 'Received'}) + '\n')
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            codex = shutil.which('codex')
            migrated = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, timeout=30)
            self.assertEqual(migrated.returncode, 0)
            first, later = [], []
            for line in history.read_text().splitlines(keepends=True):
                row = json.loads(line)
                (later if row.get('timestamp', '') >= '2026-01-01T00:00:02' else first).append(line)
            self.assertTrue(later)
            history.write_text(''.join(first))
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
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile', side_effect=ns.SyncError('interrupted')):
                with self.assertRaises(ns.SyncError):
                    receiver._receive()
            journal = backups / 'direct-restore-pending.json'
            event_id = json.loads(journal.read_text())['event_id']
            # A new Syncthing metadata revision must not strand the previous
            # indexing pass, even when the source changes before it finishes.
            with (graph / 'graph.json').open('a') as output:
                output.write('\n')
            state = {'state': 'idle', 'sequence': 2, 'remoteSequence': {'vps': 2}, 'needFiles': 0,
                     'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(folder): return lambda: (folder, state, True, 'valid')
            readers = (read('sessions'), read('titles'), read('graph'))
            status_path = root / 'restore-status.json'
            supervisor = RestoreSupervisor(receiver, readers[0], status_path, mock_only=False,
                                           title_reader=readers[1], graph_reader=readers[2])
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(supervisor.tick()['state'], 'WAITING')
                self.assertEqual(supervisor.tick()['state'], 'COMPLETE')
            status = json.loads(status_path.read_text())
            self.assertEqual(status['restore_event']['id'], event_id)
            self.assertTrue(status['restore_event']['indexed'])
            self.assertEqual(status['restore_event']['changed_chats'], 1)
            self.assertEqual(status['missing_path_chats'], 1)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                row = db.execute('SELECT archived FROM threads WHERE id=?', (thread,)).fetchone()
            self.assertEqual(row, (0,))
            self.assertEqual(receiver.path_warning_count, 1)
            self.assertEqual(json.loads((backups / 'missing-paths.json').read_text())['chats'][thread]
                             ['unmapped_working_directories'], ['/source/missing-project'])
            self.assertFalse(journal.exists())
            self.assertEqual(receiver._read_carry(), [])
            target = profile / history.relative_to(source)
            original = target.read_bytes()
            existing = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(existing._receive(), 0)
            self.assertEqual(target.read_bytes(), original)
            self.assertTrue(journal.is_file())
            existing.acknowledge(None)
            self.assertFalse(journal.exists())
            with history.open('a') as out:
                out.writelines(later)
            updated = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(updated._receive(), 1)
            self.assertIn(b'later source message', target.read_bytes())
            self.assertEqual((backups / 'latest' / target.relative_to(profile)).read_bytes(), original)
            updated_record = json.loads(journal.read_text())
            updated.acknowledge({'id': updated_record['event_id'], 'indexed': True, 'changed_chats': 1})
            child = '019b0000-0000-7000-8000-000000000012'
            child_path = folder / f'rollout-2026-01-01T00-00-00-{child}.jsonl'
            child_meta = dict(meta, id=child, source={'subagent': {'other': 'guardian'}}, parent_thread_id=thread)
            child_rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': child_meta},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                 {'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'child answer'}]}},
                {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                 {'type': 'agent_message', 'message': 'child answer'}}]
            child_home = root / 'child-build'
            (child_home / 'sessions').mkdir(parents=True)
            child_build = child_home / 'sessions' / child_path.name
            child_build.write_text(''.join(json.dumps(row) + '\n' for row in child_rows))
            migrated = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(child_home)}, capture_output=True, timeout=30)
            self.assertEqual(migrated.returncode, 0, migrated.stderr.decode(errors='replace'))
            child_rows = [json.loads(line) for line in child_build.read_text().splitlines()]
            child_rows[0]['payload'].pop('subagent_history_start_ordinal', None)
            child_path.write_text(''.join(json.dumps(row) + '\n' for row in child_rows))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'edges': [{'parent': thread, 'child': child, 'status': 'closed'}],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with_graph = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(with_graph._receive(), 2)
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT parent_thread_id, status FROM thread_spawn_edges WHERE child_thread_id=?',
                                            (child,)).fetchone(), (thread, 'closed'))
            graph_record = json.loads(journal.read_text())
            with_graph.acknowledge({'id': graph_record['event_id'], 'indexed': True, 'changed_chats': 2})
            multi = '019b0000-0000-7000-8000-000000000013'
            page = '019b0000-0000-7000-8000-000000000014'
            multi_home = root / 'multi-build'
            (multi_home / 'sessions').mkdir(parents=True)
            base_name = f'rollout-2026-01-01T00-00-00-{multi}'
            multi_build = multi_home / 'sessions' / (base_name + '.jsonl')
            multi_meta = dict(meta, id=multi)
            multi_rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload': multi_meta}]
            for number in (1, 2, 3):
                stamp, message = f'2026-01-01T00:0{number}:00.000Z', f'page message {number}'
                multi_rows += [{'timestamp': stamp, 'type': 'response_item', 'payload':
                                {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': message}]}},
                               {'timestamp': stamp, 'type': 'event_msg', 'payload':
                                {'type': 'user_message', 'message': message, 'images': [], 'local_images': [], 'text_elements': []}}]
            multi_build.write_text(''.join(json.dumps(row) + '\n' for row in multi_rows))
            migrated = subprocess.run([codex, 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(multi_home)}, capture_output=True, timeout=30)
            self.assertEqual(migrated.returncode, 0)
            rows = [json.loads(line) for line in multi_build.read_text().splitlines()]
            prefix, suffix = rows[:4], rows[4:]
            first_page = folder / (base_name + '.jsonl')
            first_page.write_text(''.join(json.dumps(row) + '\n' for row in prefix))
            header = dict(prefix[0]); header['ordinal'] = 4
            header['payload'] = dict(prefix[0]['payload'], history_base={
                'thread_id': multi, 'end_byte_offset': first_page.stat().st_size, 'end_ordinal_exclusive': 4})
            for row in suffix:
                row['ordinal'] += 1
            page_later = [row for row in suffix if row.get('timestamp', '') >= '2026-01-01T00:03']
            suffix = [row for row in suffix if row not in page_later]
            self.assertTrue(page_later)
            second_page = folder / (base_name + '_' + page + '.jsonl')
            second_page.write_text(''.join(json.dumps(row) + '\n' for row in [header, *suffix]))
            with (titles / 'session_index.jsonl').open('a') as out:
                out.write(json.dumps({'id': multi, 'thread_name': 'Multi'}) + '\n')
            multi_receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(multi_receiver._receive(), 1)
            imported = next((profile / 'sessions').rglob(base_name + '.jsonl')).read_bytes()
            self.assertIn(b'page message 1', imported)
            self.assertIn(b'page message 2', imported)
            self.assertNotIn(b'page message 3', imported)
            multi_record = json.loads(journal.read_text())
            multi_receiver.acknowledge({'id': multi_record['event_id'], 'indexed': True, 'changed_chats': 1})
            with second_page.open('a') as out:
                out.write(''.join(json.dumps(row) + '\n' for row in page_later))
            updated_multi = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(updated_multi._receive(), 1)
            multi_path = next((profile / 'sessions').rglob(base_name + '.jsonl'))
            self.assertIn(b'page message 3', multi_path.read_bytes())
            self.assertEqual((backups / 'latest' / multi_path.relative_to(profile)).read_bytes(), imported)
            with (titles / 'session_index.jsonl').open('a') as out:
                out.write(json.dumps({'id': multi, 'thread_name': 'Renamed'}) + '\n')
            renamed = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(codex), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps', quiet_seconds=0)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True):
                self.assertEqual(renamed._receive(), 1)
            self.assertTrue((backups / 'direct-restore-carry.json').is_file())
            self.assertIn('Renamed', (profile / 'session_index.jsonl').read_text())


if __name__ == '__main__':
    unittest.main()
