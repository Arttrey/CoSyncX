import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import mock_chat_sync as mock_sync
import nexus_sync as ns


@unittest.skipUnless(sys.platform == 'linux', 'mock writer-lock experiment requires Linux')
class MockChatSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='nexus-mock-unit-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.shared = self.root / 'shared'
        self.shared.mkdir()
        self.nexus = self.root / 'Nexus'
        (self.nexus / 'Bot').mkdir(parents=True)
        self.profile = self.root / 'mock-profile'
        self.key = b'm' * 32
        mock_sync.init_mock_profile(self.profile, self.key, self.shared)
        self.thread_id = '019b0000-0000-7000-8000-000000000076'
        self.source = self.root / 'source'
        source_folder = self.source / 'sessions'
        source_folder.mkdir(parents=True)
        local_folder = self.profile / 'sessions'
        local_folder.mkdir()
        self.source_file = source_folder / 'rollout-mock.jsonl'
        self.local_file = local_folder / 'rollout-mock.jsonl'
        def rows(cwd, message):
            return [json.dumps({'type': 'session_meta', 'payload': {'id': self.thread_id, 'cwd': cwd}}),
                    json.dumps({'type': 'event_msg', 'payload': {'message': message}})]
        self.local_file.write_text('\n'.join(rows(str(self.nexus / 'Bot'), 'mock first')) + '\n')
        self.source_file.write_text('\n'.join(rows('/source/Nexus/Bot', 'mock first')) + '\n' +
                                    json.dumps({'type': 'event_msg', 'payload': {'message': 'mock second'}}) + '\n')
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)

    def apply(self):
        return mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
                                             self.thread_id, '/source/Nexus', self.nexus)

    def test_busy_thread_is_deferred_then_mock_extension_is_applied(self):
        import fcntl
        lock_dir = self.profile / 'thread-writer-locks'
        lock_dir.mkdir()
        original = self.local_file.read_bytes()
        for name in (self.thread_id + '.lock', '.coordination.lock'):
            with (lock_dir / name).open('a+b') as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(ns.SyncError):
                    self.apply()
                self.assertEqual(self.local_file.read_bytes(), original)
                self.assertFalse((self.profile / '.nexus-mock-backups').exists())
        result = self.apply()
        self.assertEqual(result, {'state': 'UPDATED', 'index_updated': False, 'titles_updated': False,
                                  'backup_created': True, 'mock_only': True})
        self.assertIn('mock second', self.local_file.read_text())
        backup = next((self.profile / '.nexus-mock-backups' / self.thread_id).glob('*.jsonl'))
        self.assertEqual(backup.read_bytes(), original)
        self.assertEqual(self.apply()['state'], 'UNCHANGED')

    def test_mock_watcher_status_reports_pending_and_rejects_stale_or_tampered_marker(self):
        with mock.patch.object(mock_sync.time, 'time', return_value=1000):
            mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux',
                                         {'state': 'PENDING'}, 5)
            result = mock_sync.watch_status(self.profile, self.shared, self.key)
        self.assertEqual(result['state'], 'PENDING')
        self.assertFalse(result['verified_in_cursor'])
        with mock.patch.object(mock_sync.time, 'time', return_value=1100):
            self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'STALE')
        marker = self.profile / '.nexus-mock-watch-status.json'
        data = json.loads(marker.read_text())
        data['state'] = 'COMPLETE'
        marker.write_text(json.dumps(data))
        with mock.patch.object(mock_sync.time, 'time', return_value=1000):
            self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'INVALID')

    def test_mock_watcher_status_detects_newer_local_snapshot(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'COMPLETE')
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'COMPLETE')
        self.source_file.write_text(self.source_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'newer mock'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')

    def test_mock_watcher_status_detects_local_chat_change(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'COMPLETE')
        self.local_file.write_text(self.local_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'local mock change'}}) + '\n')
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')

    def test_mock_status_checks_many_chats_with_one_local_catalogue_scan(self):
        for number in range(12):
            identity = f'019b0000-0000-7000-8000-{number + 100:012d}'
            (self.source / 'sessions' / f'rollout-{identity}.jsonl').write_text(
                json.dumps({'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/source/Nexus/Bot'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'COMPLETE')
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        real_catalogue = ns._chat_catalog
        local_scans = []
        def count_scans(path, **kwargs):
            if path == self.profile:
                local_scans.append(1)
            return real_catalogue(path, **kwargs)
        with mock.patch.object(ns, '_chat_catalog', side_effect=count_scans):
            self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'COMPLETE')
        self.assertEqual(len(local_scans), 1)
        last = self.profile / 'sessions' / 'rollout-019b0000-0000-7000-8000-000000000111.jsonl'
        last.write_text(last.read_text() + json.dumps({'type': 'event_msg', 'payload': {'message': 'changed'}}) + '\n')
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')

    def test_watcher_skips_reprocessing_unchanged_batch_and_receives_new_chat(self):
        for number in range(5):
            identity = f'019b0000-0000-7000-8000-{number + 200:012d}'
            (self.source / 'sessions' / f'rollout-{identity}.jsonl').write_text(
                json.dumps({'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/source/Nexus/Bot'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        sources = [('linux', '/source/Nexus')]
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            first = mock_sync.watch_cycle(self.profile, self.shared, self.key, sources,
                                          self.nexus, Path(sys.executable), None)
            self.assertEqual(first['state'], 'COMPLETE')
            mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', first, 5)
            with mock.patch.object(ns, 'extract', wraps=ns.extract) as extract:
                same = mock_sync.watch_cycle(self.profile, self.shared, self.key, sources,
                                             self.nexus, Path(sys.executable), first)
            self.assertEqual(same['state'], 'COMPLETE')
            self.assertEqual(extract.call_count, 1)
            new_id = '019b0000-0000-7000-8000-000000000299'
            (self.source / 'sessions' / f'rollout-{new_id}.jsonl').write_text(
                json.dumps({'type': 'session_meta', 'payload': {'id': new_id, 'cwd': '/source/Nexus/Bot'}}) + '\n')
            ns.snapshot(self.source, self.shared, 'linux', self.key)
            updated = mock_sync.watch_cycle(self.profile, self.shared, self.key, sources,
                                             self.nexus, Path(sys.executable), same)
            self.assertEqual(updated['state'], 'COMPLETE')
            self.assertIn(new_id, ns._chat_catalog(self.profile))
            mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', updated, 5)
            different_target = self.root / 'OtherNexus'
            (different_target / 'Bot').mkdir(parents=True)
            remapped = mock_sync.watch_cycle(self.profile, self.shared, self.key, sources,
                                             different_target, Path(sys.executable), updated)
            self.assertEqual(remapped['state'], 'PENDING')

    def test_mock_watcher_status_detects_title_change_and_missing_receipt(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        index = self.profile / 'session_index.jsonl'
        index.write_text(json.dumps({'id': self.thread_id, 'thread_name': 'local mock title',
                                     'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')
        index.unlink()
        receipt = self.profile / mock_sync.SYNC_MARKER / ('linux-' + self.thread_id + '.json')
        original = json.loads(receipt.read_text())
        original['local_tag'] = '0' * 64
        receipt.write_text(json.dumps(original))
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'INVALID')
        receipt.unlink()
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')

    def test_mock_watcher_status_never_confirms_missing_local_chat(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        self.local_file.unlink()
        self.assertNotEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'COMPLETE')

    def test_multi_source_watcher_status_detects_new_snapshot_from_secondary_source(self):
        mac = self.root / 'mac-source'
        (mac / 'sessions').mkdir(parents=True)
        identity = '019b0000-0000-7000-8000-000000000077'
        path = mac / 'sessions' / 'rollout-mac.jsonl'
        path.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/Users/mock/Nexus/Bot'}}) + '\n')
        ns.snapshot(mac, self.shared, 'mac', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_sources_mock(self.profile, self.shared, self.key,
                [('linux', '/source/Nexus'), ('mac', '/Users/mock/Nexus')], self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'COMPLETE')
        mock_sync.write_watch_status(self.profile, self.shared, self.key, 'linux', result, 5)
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'COMPLETE')
        path.write_text(path.read_text() + json.dumps({'type': 'event_msg', 'payload': {'message': 'new mac mock'}}) + '\n')
        ns.snapshot(mac, self.shared, 'mac', self.key)
        self.assertEqual(mock_sync.watch_status(self.profile, self.shared, self.key)['state'], 'PENDING')

    def test_multiple_sources_map_paths_and_preserve_diverged_history(self):
        self.assertEqual(self.apply()['state'], 'UPDATED')
        mac = self.root / 'mac-source'
        (mac / 'sessions').mkdir(parents=True)
        mac_chat = mac / 'sessions' / 'rollout-mock.jsonl'
        mac_rows = [
            {'type': 'session_meta', 'payload': {'id': self.thread_id, 'cwd': '/Users/mock/Nexus/Bot'}},
            {'type': 'event_msg', 'payload': {'message': 'mock first'}},
            {'type': 'event_msg', 'payload': {'message': 'mock second'}},
            {'type': 'event_msg', 'payload': {'message': 'mock mac update'}},
        ]
        mac_chat.write_text(''.join(json.dumps(row) + '\n' for row in mac_rows))
        mac_snapshot = ns.snapshot(mac, self.shared, 'mac', self.key)
        applied = mock_sync.apply_mock_snapshot(self.profile, mac_snapshot, self.shared, self.key,
            self.thread_id, '/Users/mock/Nexus', self.nexus)
        self.assertEqual(applied['state'], 'UPDATED')
        current = self.local_file.read_bytes()
        self.assertIn(b'mock mac update', current)
        self.assertIn(str(self.nexus / 'Bot').encode(), current)
        self.assertEqual(self.apply()['state'], 'LOCAL_AHEAD')
        with mock.patch.object(mock_sync, 'reindex_mock_profile') as reindex:
            pending = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(pending['state'], 'LOCAL_AHEAD')
            self.assertFalse(pending['index_updated'])
            batch = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(batch['state'], 'PENDING')
            self.assertEqual(batch['results'], {'LOCAL_AHEAD': 1})
            reindex.assert_not_called()
        self.assertEqual(self.local_file.read_bytes(), current)

        windows = self.root / 'windows-source'
        (windows / 'sessions').mkdir(parents=True)
        windows_rows = [
            {'type': 'session_meta', 'payload': {'id': self.thread_id, 'cwd': r'C:\Users\mock\Nexus\Bot'}},
            {'type': 'event_msg', 'payload': {'message': 'mock first'}},
            {'type': 'event_msg', 'payload': {'message': 'mock windows divergent'}},
        ]
        (windows / 'sessions' / 'rollout-mock.jsonl').write_text(
            ''.join(json.dumps(row) + '\n' for row in windows_rows))
        windows_snapshot = ns.snapshot(windows, self.shared, 'windows', self.key)
        with self.assertRaises(ns.SyncError):
            mock_sync.apply_mock_snapshot(self.profile, windows_snapshot, self.shared, self.key,
                self.thread_id, r'C:\Users\mock\Nexus', self.nexus)
        self.assertEqual(self.local_file.read_bytes(), current)

    def test_multi_source_receive_keeps_conflict_and_imports_independent_chat(self):
        mac = self.root / 'mac-source'
        (mac / 'sessions').mkdir(parents=True)
        second = '019b0000-0000-7000-8000-000000000077'
        for identity, message in ((self.thread_id, 'mac divergent'), (second, 'mac independent')):
            rows = [{'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/Users/mock/Nexus/Bot'}},
                    {'type': 'event_msg', 'payload': {'message': message}}]
            (mac / 'sessions' / f'rollout-{identity}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in rows))
        ns.snapshot(mac, self.shared, 'mac', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_sources_mock(self.profile, self.shared, self.key,
                [('linux', '/source/Nexus'), ('mac', '/Users/mock/Nexus')], self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'PENDING')
        self.assertEqual(result['sources_expected'], 2)
        self.assertIn('mock second', self.local_file.read_text())
        self.assertNotIn('mac divergent', self.local_file.read_text())
        self.assertIn(second, ns._chat_catalog(self.profile))

    def test_batch_keeps_conflicting_chat_but_receives_independent_chat(self):
        self.local_file.write_text(self.local_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'local branch'}}) + '\n')
        original = self.local_file.read_bytes()
        second = '019b0000-0000-7000-8000-000000000077'
        (self.source / 'sessions' / 'rollout-second.jsonl').write_text(
            json.dumps({'type': 'session_meta', 'payload': {'id': second, 'cwd': '/source/Nexus/Bot'}}) + '\n' +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'independent chat'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'PENDING')
        self.assertEqual(result['results'], {'CONFLICT': 1, 'ADDED': 1})
        self.assertEqual(self.local_file.read_bytes(), original)
        self.assertIn(second, ns._chat_catalog(self.profile))

    def test_new_chat_is_added_without_touching_other_chats(self):
        self.local_file.unlink()
        other = self.profile / 'sessions' / 'rollout-other.jsonl'
        other.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': '019b0000-0000-7000-8000-000000000077', 'cwd': str(self.nexus / 'Bot')}}) + '\n')
        before = other.read_bytes()
        result = self.apply()
        self.assertEqual(result['state'], 'ADDED')
        self.assertFalse(result['backup_created'])
        self.assertFalse(result['index_updated'])
        self.assertIn('mock second', self.local_file.read_text())
        self.assertEqual(other.read_bytes(), before)
        self.assertEqual(self.apply()['state'], 'UNCHANGED')

    def test_new_chat_does_not_overwrite_a_path_used_by_another_chat(self):
        self.local_file.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': '019b0000-0000-7000-8000-000000000077', 'cwd': str(self.nexus / 'Bot')}}) + '\n')
        before = self.local_file.read_bytes()
        with self.assertRaises((ns.SyncError, FileExistsError)):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_other_busy_mock_writer_blocks_new_chat(self):
        import fcntl
        self.local_file.unlink()
        directory = self.profile / 'thread-writer-locks'
        directory.mkdir()
        other_id = '019b0000-0000-7000-8000-000000000077'
        with (directory / (other_id + '.lock')).open('a+b') as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(ns.SyncError):
                self.apply()
        self.assertFalse(self.local_file.exists())

    def test_new_chat_refuses_symlinked_history_directory(self):
        self.local_file.unlink()
        self.local_file.parent.rmdir()
        outside = self.root / 'outside'
        outside.mkdir()
        (self.profile / 'sessions').symlink_to(outside, target_is_directory=True)
        with self.assertRaises((ns.SyncError, OSError)):
            self.apply()
        self.assertFalse(list(outside.iterdir()))

    def test_reindex_refuses_mock_profile_with_archived_chat(self):
        archived = self.profile / 'archived_sessions'
        archived.mkdir()
        (archived / 'rollout-archived.jsonl').write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': '019b0000-0000-7000-8000-000000000077', 'cwd': str(self.nexus / 'Bot')}}) + '\n')
        with mock.patch.object(ns.subprocess, 'Popen') as start:
            with self.assertRaises(ns.SyncError):
                mock_sync.reindex_mock_profile(self.profile, self.shared, self.key, self.nexus,
                                               Path(sys.executable), self.thread_id)
            start.assert_not_called()

    def test_conflict_and_unmarked_profile_do_not_write(self):
        self.local_file.write_text(self.local_file.read_text() +
                                   json.dumps({'type': 'event_msg', 'payload': {'message': 'local divergence'}}) + '\n')
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)
        (self.profile / mock_sync.MARKER).unlink()
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_marker_cannot_be_reused_for_a_different_profile(self):
        other = self.root / 'other-profile'
        other.mkdir()
        (other / mock_sync.MARKER).write_bytes((self.profile / mock_sync.MARKER).read_bytes())
        with self.assertRaises(ns.SyncError):
            mock_sync.apply_mock_snapshot(other, self.snapshot, self.shared, self.key,
                                          self.thread_id, '/source/Nexus', self.nexus)

    def test_invalid_paginated_order_is_not_applied(self):
        for path in (self.local_file, self.source_file):
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]['payload']['history_mode'] = 'paginated'
            for ordinal, row in enumerate(rows):
                row['ordinal'] = ordinal
            if path == self.source_file:
                rows[-1].pop('ordinal')
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        before = self.local_file.read_bytes()
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_legacy_mock_settings_merge_keeps_both_settings_and_later_messages(self):
        local_settings = {'type': 'event_msg', 'payload': {'type': 'thread_settings_applied', 'device': 'local'}}
        remote_settings = {'type': 'event_msg', 'payload': {'type': 'thread_settings_applied', 'device': 'remote'}}
        new_message = {'type': 'event_msg', 'payload': {'message': 'remote continuation'}}
        self.local_file.write_text(self.local_file.read_text() + json.dumps(local_settings) + '\n')
        base = self.source_file.read_text().splitlines()[:2]
        self.source_file.write_text('\n'.join(base + [json.dumps(remote_settings), json.dumps(new_message)]) + '\n')
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        original = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            self.apply()
        result = mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
            self.thread_id, '/source/Nexus', self.nexus, allow_mock_settings_rebase=True)
        self.assertEqual(result['state'], 'REBASED_MOCK')
        records = [json.loads(line) for line in self.local_file.read_text().splitlines()]
        self.assertEqual(records[2:], [local_settings, remote_settings, new_message])
        self.assertEqual(next((self.profile / '.nexus-mock-backups' / self.thread_id).glob('*.jsonl')).read_bytes(), original)
        later = {'type': 'event_msg', 'payload': {'message': 'another continuation'}}
        self.source_file.write_text(self.source_file.read_text() + json.dumps(later) + '\n')
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        result = mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
            self.thread_id, '/source/Nexus', self.nexus, allow_mock_settings_rebase=True)
        self.assertEqual(result['state'], 'REBASED_MOCK')
        records = [json.loads(line) for line in self.local_file.read_text().splitlines()]
        self.assertEqual(records[2:], [local_settings, remote_settings, new_message, later])
        before_repeat = self.local_file.read_bytes()
        backup_dir = self.profile / '.nexus-mock-backups' / self.thread_id
        backup_count = len(list(backup_dir.glob('*.jsonl')))
        repeated = mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
            self.thread_id, '/source/Nexus', self.nexus, allow_mock_settings_rebase=True)
        self.assertEqual(repeated['state'], 'LOCAL_AHEAD')
        self.assertEqual(self.local_file.read_bytes(), before_repeat)
        self.assertEqual(len(list(backup_dir.glob('*.jsonl'))), backup_count)

    def test_legacy_settings_merge_refuses_divergent_user_message(self):
        local = {'type': 'event_msg', 'payload': {'message': 'local edit'}}
        remote = {'type': 'event_msg', 'payload': {'type': 'thread_settings_applied', 'device': 'remote'}}
        self.local_file.write_text(self.local_file.read_text() + json.dumps(local) + '\n')
        self.source_file.write_text(self.source_file.read_text() + json.dumps(remote) + '\n')
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
                self.thread_id, '/source/Nexus', self.nexus, allow_mock_settings_rebase=True)
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_mock_settings_rebase_requires_opt_in_and_preserves_local_event(self):
        def meta(cwd):
            return {'ordinal': 0, 'type': 'session_meta', 'payload':
                    {'id': self.thread_id, 'cwd': cwd, 'history_mode': 'paginated'}}
        settings = {'ordinal': 1, 'type': 'event_msg', 'payload':
                    {'type': 'thread_settings_applied', 'mock_setting': 'keep'}}
        incoming = [
            {'ordinal': 1, 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                'content': [{'type': 'input_text', 'text': 'mock extra'}]}},
            {'ordinal': 2, 'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': 'mock-turn'}},
            {'ordinal': 3, 'type': 'event_msg', 'payload': {'type': 'item_completed', 'thread_id': self.thread_id,
                'turn_id': 'mock-turn', 'item': {'type': 'UserMessage', 'id': 'mock-item',
                    'content': [{'type': 'text', 'text': 'mock extra', 'text_elements': []}]}, 'completed_at_ms': 1}},
            {'ordinal': 4, 'type': 'event_msg', 'payload': {'type': 'task_complete', 'turn_id': 'mock-turn',
                'last_agent_message': None}},
        ]
        self.local_file.write_text(''.join(json.dumps(row) + '\n' for row in
                                         [meta(str(self.nexus / 'Bot')), settings]))
        self.source_file.write_text(''.join(json.dumps(row) + '\n' for row in
                                          [meta('/source/Nexus/Bot'), *incoming]))
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            pending = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(pending['state'], 'PENDING')
            self.assertEqual(self.local_file.read_bytes(), before)
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                '/source/Nexus', self.nexus, Path(sys.executable), allow_mock_settings_rebase=True)
        self.assertEqual(result['state'], 'COMPLETE')
        self.assertEqual(result['results'], {'REBASED_MOCK': 1})
        records = [json.loads(line) for line in self.local_file.read_text().splitlines()]
        self.assertEqual(records[1], settings)
        self.assertEqual(records[2]['ordinal'], 2)
        self.assertEqual(records[2]['payload']['content'][0]['text'], 'mock extra')
        backup = next((self.profile / '.nexus-mock-backups' / self.thread_id).glob('*.jsonl'))
        self.assertEqual(backup.read_bytes(), before)

    def test_mock_settings_rebase_rejects_ambiguous_previous_turn(self):
        identity = self.thread_id
        meta = {'ordinal': 0, 'type': 'session_meta', 'payload': {'id': identity, 'history_mode': 'paginated'}}
        previous = {'ordinal': 1, 'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': 'old'}}
        settings = {'ordinal': 2, 'type': 'event_msg', 'payload': {'type': 'thread_settings_applied'}}
        message = {'ordinal': 2, 'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                   'content': [{'type': 'input_text', 'text': 'test'}]}}
        close = {'ordinal': 3, 'type': 'event_msg', 'payload': {'type': 'task_complete', 'turn_id': 'old',
                 'last_agent_message': None}}
        start = {'ordinal': 4, 'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': 'new'}}
        item = {'ordinal': 5, 'type': 'event_msg', 'payload': {'type': 'item_completed', 'thread_id': identity,
                'turn_id': 'new', 'item': {'type': 'UserMessage', 'id': 'item',
                'content': [{'type': 'text', 'text': 'test', 'text_elements': []}]}, 'completed_at_ms': 1}}
        finish = {'ordinal': 6, 'type': 'event_msg', 'payload': {'type': 'task_complete', 'turn_id': 'new',
                  'last_agent_message': None}}
        self.local_file.write_text(''.join(json.dumps(row) + '\n' for row in [meta, previous, settings]))
        good = [meta, previous, message, close, start, item, finish]
        for index, change in ((3, {'turn_id': 'other'}), (3, {'last_agent_message': 'unexpected'}),
                              (5, {'thread_id': 'other'})):
            rows = json.loads(json.dumps(good))
            rows[index]['payload'].update(change)
            self.source_file.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            before = self.local_file.read_bytes()
            with self.subTest(change=change), self.assertRaises(ns.SyncError):
                mock_sync._mock_settings_rebase(self.local_file, self.source_file)
            self.assertEqual(self.local_file.read_bytes(), before)

    def test_mock_settings_rebase_rejects_unknown_local_records(self):
        def meta(cwd):
            return {'ordinal': 0, 'type': 'session_meta', 'payload':
                    {'id': self.thread_id, 'cwd': cwd, 'history_mode': 'paginated'}}
        local = {'ordinal': 1, 'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'local'}}
        incoming = {'ordinal': 1, 'type': 'event_msg', 'payload': {'type': 'task_started'}}
        self.local_file.write_text(''.join(json.dumps(row) + '\n' for row in [meta(str(self.nexus / 'Bot')), local]))
        self.source_file.write_text(''.join(json.dumps(row) + '\n' for row in [meta('/source/Nexus/Bot'), incoming]))
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            mock_sync.apply_mock_snapshot(self.profile, self.snapshot, self.shared, self.key,
                self.thread_id, '/source/Nexus', self.nexus, allow_mock_settings_rebase=True)
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_cli_applies_only_to_marked_mock_profile(self):
        key_file = self.root / 'key'
        key_file.write_bytes(self.key)
        script = str(Path(mock_sync.__file__))
        result = subprocess.run([sys.executable, script, 'apply', '--profile', str(self.profile),
            '--shared', str(self.shared), '--key-file', str(key_file), '--snapshot', str(self.snapshot),
            '--thread-id', self.thread_id, '--source-root', '/source/Nexus', '--target-root', str(self.nexus)],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['state'], 'UPDATED')
        self.assertNotIn('mock second', result.stdout + result.stderr)

    def test_tampered_snapshot_does_not_modify_mock_chat(self):
        before = self.local_file.read_bytes()
        manifest = self.snapshot / 'manifest.json'
        content = json.loads(manifest.read_text())
        content['created'] = '2000-01-01T00:00:00Z'
        manifest.write_text(json.dumps(content))
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_incomplete_snapshot_does_not_modify_mock_chat(self):
        (self.source / 'sessions' / 'rollout-incomplete.jsonl').write_bytes(b'{"partial":')
        self.snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
        self.assertEqual(ns.verify(self.snapshot, self.key)['omitted_files'], 1)
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_sync_latest_uses_authenticated_chain_and_is_idempotent(self):
        self.source_file.write_text(self.source_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'mock third'}}) + '\n')
        newest = ns.snapshot(self.source, self.shared, 'linux', self.key)
        os.utime(self.snapshot, (4102444800, 4102444800))
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}) as index:
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(result['state'], 'UPDATED')
            self.assertTrue(result['index_updated'])
            self.assertIn('mock third', self.local_file.read_text())
            marker_path = self.profile / mock_sync.SYNC_MARKER / ('linux-' + self.thread_id + '.json')
            marker = json.loads(marker_path.read_text())
            self.assertEqual(marker['snapshot'], newest.name)
            again = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(again['state'], 'UNCHANGED')
            self.assertFalse(again['index_current_verified'])
            self.assertEqual(index.call_count, 1)
        self.local_file.write_text(self.local_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'local divergent'}}) + '\n')
        before = self.local_file.read_bytes()
        result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
            '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'LOCAL_AHEAD')
        self.assertFalse(result['index_updated'])
        self.assertEqual(json.loads(marker_path.read_text()), marker)
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_sync_checkpoints_are_independent_per_mock_chat(self):
        second_id = '019b0000-0000-7000-8000-000000000077'
        second_file = self.source / 'sessions' / 'rollout-second.jsonl'
        second_file.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': second_id, 'cwd': '/source/Nexus/Bot'}}) + '\n' +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'second mock chat'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}) as index:
            first = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            second = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', second_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(first['state'], 'UPDATED')
            self.assertEqual(second['state'], 'ADDED')
            self.assertFalse(second['titles_updated'])
            for identity in (self.thread_id, second_id):
                marker = self.profile / mock_sync.SYNC_MARKER / ('linux-' + identity + '.json')
                self.assertTrue(marker.is_file())
                repeated = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', identity,
                    '/source/Nexus', self.nexus, Path(sys.executable))
                self.assertEqual(repeated['state'], 'UNCHANGED')
            self.assertEqual(index.call_count, 2)

    def test_sync_rejects_rollback_and_modified_accepted_manifest(self):
        self.source_file.write_text(self.source_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'mock third'}}) + '\n')
        newest = ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}) as index:
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                                               '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(result['state'], 'UPDATED')
            before = self.local_file.read_bytes()
            held = self.root / 'held-snapshot'
            shutil.move(newest, held)
            with self.assertRaises(ns.SyncError):
                mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                                           '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(self.local_file.read_bytes(), before)
            self.assertEqual(index.call_count, 1)
            shutil.move(held, newest)
            manifest_path = newest / 'manifest.json'
            manifest = json.loads(manifest_path.read_text())
            manifest['created'] = '2026-01-01T00:00:00+00:00'
            manifest['authentication'] = ns._manifest_tag(manifest, self.key)
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaises(ns.SyncError):
                mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                                           '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(self.local_file.read_bytes(), before)

    def test_sync_accepts_verified_descendant_of_accepted_snapshot(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            self.assertEqual(mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                             '/source/Nexus', self.nexus, Path(sys.executable))['state'], 'UPDATED')
            self.source_file.write_text(self.source_file.read_text() +
                json.dumps({'type': 'event_msg', 'payload': {'message': 'mock descendant'}}) + '\n')
            ns.snapshot(self.source, self.shared, 'linux', self.key)
            self.assertEqual(mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                             '/source/Nexus', self.nexus, Path(sys.executable))['state'], 'UPDATED')
            self.assertIn('mock descendant', self.local_file.read_text())

    def test_batch_pins_snapshot_authentication_before_changes(self):
        before = self.local_file.read_bytes()
        with self.assertRaises(ns.SyncError):
            mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable), expected_snapshot=self.snapshot.name,
                expected_manifest_tag='wrong authentication')
        self.assertEqual(self.local_file.read_bytes(), before)

    def test_batch_receives_all_chats_and_stops_on_new_snapshot(self):
        second_id = '019b0000-0000-7000-8000-000000000077'
        (self.source / 'sessions' / 'rollout-second.jsonl').write_text(
            json.dumps({'type': 'session_meta', 'payload': {'id': second_id, 'cwd': '/source/Nexus/Bot'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['total'], 2)
            self.assertEqual(result['results'], {'UPDATED': 1, 'ADDED': 1})
            repeat = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertEqual(repeat['results'], {'UNCHANGED': 2})
        self.source_file.write_text(self.source_file.read_text() +
            json.dumps({'type': 'event_msg', 'payload': {'message': 'new mock entry'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        def changed(*args):
            ns.snapshot(self.source, self.shared, 'linux', self.key)
            return {'index_updated': True}
        with mock.patch.object(mock_sync, 'reindex_mock_profile', side_effect=changed):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'PENDING')
        self.assertEqual(result['results'], {'NEWER_SNAPSHOT_PENDING': 1})
        self.assertEqual(result['processed'], 1)

    def test_title_conflict_does_not_block_another_mock_chat(self):
        def title(name):
            return json.dumps({'id': self.thread_id, 'thread_name': name, 'updated_at': '2026-01-01T00:00:00Z'}) + '\n'
        (self.source / 'session_index.jsonl').write_text(title('source mock title'))
        local_index = self.profile / 'session_index.jsonl'
        local_index.write_text(title('local mock title'))
        second = '019b0000-0000-7000-8000-000000000077'
        (self.source / 'sessions' / 'rollout-second.jsonl').write_text(
            json.dumps({'type': 'session_meta', 'payload': {'id': second, 'cwd': '/source/Nexus/Bot'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        before = self.local_file.read_bytes()
        with mock.patch.object(mock_sync, 'reindex_mock_profile', return_value={'index_updated': True}):
            result = mock_sync.sync_all_mock(self.profile, self.shared, self.key, 'linux',
                                             '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'PENDING')
        self.assertEqual(result['results'], {'TITLE_CONFLICT': 1, 'ADDED': 1})
        self.assertEqual(self.local_file.read_bytes(), before)
        self.assertEqual(local_index.read_text(), title('local mock title'))
        self.assertIn(second, ns._chat_catalog(self.profile))

    def test_title_conflict_never_changes_local_chat_or_title(self):
        def title(name):
            return json.dumps({'id': self.thread_id, 'thread_name': name, 'updated_at': '2026-01-01T00:00:00Z'}) + '\n'
        (self.source / 'session_index.jsonl').write_text(title('source mock title'))
        local_index = self.profile / 'session_index.jsonl'
        local_index.write_text(title('local mock title'))
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        before = self.local_file.read_bytes()
        with mock.patch.object(mock_sync, 'reindex_mock_profile') as index:
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'TITLE_CONFLICT')
        self.assertEqual(self.local_file.read_bytes(), before)
        self.assertEqual(local_index.read_text(), title('local mock title'))
        index.assert_not_called()

    def test_title_is_indexed_and_local_rename_invalidates_checkpoint(self):
        title = 'source mock title'
        row = lambda name: json.dumps({'id': self.thread_id, 'thread_name': name, 'updated_at': '2026-01-01T00:00:00Z'}) + '\n'
        (self.source / 'session_index.jsonl').write_text(row(title))
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        def index(*args, **kwargs):
            self.assertEqual(kwargs['title_override'], title)
            (self.profile / 'session_index.jsonl').write_text(row(title))
            return {'index_updated': True, 'title_verified': True}
        with mock.patch.object(mock_sync, 'reindex_mock_profile', side_effect=index):
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
            self.assertTrue(result['titles_updated'])
            marker = self.profile / mock_sync.SYNC_MARKER / ('linux-' + self.thread_id + '.json')
            self.assertNotIn(title, marker.read_text())
            self.assertEqual(mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))['state'], 'UNCHANGED')
            (self.profile / 'session_index.jsonl').write_text(row('local renamed title'))
            self.assertEqual(mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))['state'], 'TITLE_CONFLICT')

    def test_sync_latest_does_not_claim_success_if_index_fails(self):
        with mock.patch.object(mock_sync, 'reindex_mock_profile', side_effect=ns.SyncError('mock failure')):
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'INDEX_PENDING')
        self.assertTrue(result['backup_created'])
        self.assertFalse(result['index_updated'])
        self.assertFalse((self.profile / mock_sync.SYNC_MARKER).exists())

    def test_sync_latest_defers_busy_chat_before_indexing(self):
        import fcntl
        directory = self.profile / 'thread-writer-locks'
        directory.mkdir()
        before = self.local_file.read_bytes()
        with (directory / (self.thread_id + '.lock')).open('a+b') as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with mock.patch.object(mock_sync, 'reindex_mock_profile') as index:
                with self.assertRaises(ns.SyncError):
                    mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                        '/source/Nexus', self.nexus, Path(sys.executable))
                index.assert_not_called()
        self.assertEqual(self.local_file.read_bytes(), before)
        self.assertFalse((self.profile / mock_sync.SYNC_MARKER).exists())

    def test_sync_latest_detects_newer_snapshot_during_indexing(self):
        def reindex(*_args):
            self.source_file.write_text(self.source_file.read_text() +
                json.dumps({'type': 'event_msg', 'payload': {'message': 'mock later'}}) + '\n')
            ns.snapshot(self.source, self.shared, 'linux', self.key)
            return {'index_updated': True}
        with mock.patch.object(mock_sync, 'reindex_mock_profile', side_effect=reindex):
            result = mock_sync.sync_latest_mock(self.profile, self.shared, self.key, 'linux', self.thread_id,
                '/source/Nexus', self.nexus, Path(sys.executable))
        self.assertEqual(result['state'], 'NEWER_SNAPSHOT_PENDING')
        self.assertFalse((self.profile / mock_sync.SYNC_MARKER).exists())


if __name__ == '__main__':
    unittest.main()
