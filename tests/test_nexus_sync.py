import json
import hashlib
import io
import os
from concurrent.futures import Future
import plistlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import shutil
import subprocess
import sys
import threading
import xml.etree.ElementTree as ET

import nexus_sync as ns


class NexusSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.codex = self.root / 'codex'
        self.shared = self.root / 'shared'
        self.sessions = self.codex / 'sessions' / '2026' / '01' / '01'
        self.sessions.mkdir(parents=True)
        self.shared.mkdir()
        self.chat = self.sessions / 'rollout-test.jsonl'
        self.key = b'a' * 32
        self.chat.write_text('{"type":"session_meta","payload":{"id":"test"}}\n', encoding='utf-8')

    def record_projects(self, *args, **kwargs):
        return ns.record_projects(*args, key=self.key, **kwargs)

    def project_log(self, *args, **kwargs):
        return ns.project_log(*args, key=self.key, **kwargs)

    def write_project_report(self, *args, **kwargs):
        return ns.write_project_report(*args, key=self.key, **kwargs)

    def test_snapshot_can_be_delivered_and_detects_corruption(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(len(ns.verify(result, self.key)['files']), 1)
        payload = result / 'files' / 'sessions' / '2026' / '01' / '01' / (self.chat.name + '.enc')
        self.assertNotIn(b'session_meta', payload.read_bytes())
        with self.assertRaises(ns.SyncError):
            ns.verify(result, b'b' * 32)
        payload.write_text('changed', encoding='utf-8')
        with self.assertRaises(ns.SyncError):
            ns.verify(result)
        self.chat.write_text('{"type":"session_meta","payload":{"id":"test"}}\n'
                             '{"type":"event_msg","payload":{"message":"new mock event"}}\n')
        later = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(ns.verify(later, self.key)['previous_snapshot'], result.name)
        self.assertEqual(ns.latest_authenticated_snapshot(result.parent, 'linux', self.key)[0], later)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_ready')
        with self.assertRaises(ns.SyncError):
            ns.verify(result, self.key)
        manifest_path = result / 'manifest.json'
        tampered = json.loads(manifest_path.read_text())
        tampered['created'] = '2000-01-01T00:00:00+00:00'
        manifest_path.write_text(json.dumps(tampered))
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.shared, 'linux', self.key)

    def test_title_index_is_encrypted_and_conflicts_are_reported(self):
        record = {'id': 'test', 'thread_name': 'private mock title', 'updated_at': '2026-01-01T00:00:00Z'}
        (self.codex / 'session_index.jsonl').write_text(json.dumps(record) + '\n')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(ns.verify(snapshot, self.key)['title_index']['state'], 'captured')
        self.assertNotIn(b'private mock title', (snapshot / 'title-index.enc').read_bytes())
        self.assertNotIn('private mock title', (snapshot / 'manifest.json').read_text())
        extracted = self.root / 'extracted'
        ns.extract(snapshot, extracted, self.key)
        self.assertEqual(json.loads((extracted / 'session_index.jsonl').read_text()), record)
        other = self.root / 'other'
        other.mkdir()
        (other / 'session_index.jsonl').write_text(json.dumps(dict(record, thread_name='different')) + '\n')
        combined, conflicts = ns._merge_title_indices([(self.codex, {'test'}), (other, {'test'})])
        self.assertEqual(json.loads(combined)['thread_name'], 'private mock title')
        self.assertEqual(conflicts, [hashlib.sha256(b'test').hexdigest()])
        (snapshot / 'title-index.enc').write_bytes(b'corrupted')
        with self.assertRaises(ns.SyncError):
            ns.verify(snapshot, self.key)

    def test_incomplete_title_index_is_omitted(self):
        (self.codex / 'session_index.jsonl').write_bytes(b'{"id":')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest = ns.verify(snapshot, self.key)
        self.assertEqual(manifest['title_index']['state'], 'omitted')
        self.assertEqual(manifest['omitted_files'], 1)
        self.assertFalse((snapshot / 'title-index.enc').exists())

    def test_incomplete_chat_does_not_publish(self):
        self.chat.write_bytes(b'{"partial":')
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(list((self.shared / 'nexus-sync' / 'snapshots' / 'linux').iterdir()), [])

    def test_stable_chat_is_sent_while_other_chat_is_unfinished(self):
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'stable', 'cwd': '/linux/Nexus/Bot'}}) + '\n')
        active = self.sessions / 'rollout-active.jsonl'
        active.write_bytes(b'{"partial":')
        nexus = self.root / 'Nexus'
        (nexus / 'Bot').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        sender = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        previous, notes = ns.supervisor_once(sender, None)
        self.assertTrue(any('пропущенные истории' in note for note in notes))
        root = self.shared / 'nexus-sync' / 'snapshots' / 'linux'
        first = next(root.iterdir())
        manifest = ns.verify(first, self.key)
        self.assertEqual(manifest['omitted_files'], 1)
        self.assertEqual(len(manifest['files']), 1)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        ns.acknowledge(first, self.shared, 'mac', self.key)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        local = self.root / 'receiver-codex'
        (local / 'sessions').mkdir(parents=True)
        receiver_state = self.root / 'receiver-state'
        receiver_state.mkdir()
        receiver = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(local), 'shared': str(self.shared),
            'state_dir': str(receiver_state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'linux': '/linux/Nexus'}})
        self.assertEqual(ns.prepare_received_once(receiver, self.key), 1)
        report = ns.plan_prepared(receiver, self.key)
        self.assertEqual(report['snapshots'][0]['state'], 'PARTIAL')
        self.assertEqual(report['snapshots'][0]['omitted_files'], 1)
        self.assertEqual(ns.configured_status(receiver, self.key)['prepared_incomplete'], 1)
        preview = self.root / 'partial-preview'
        review = ns.preview_merge(receiver, self.key, 'linux', first.name, preview)
        self.assertTrue(review['partial'])
        self.assertEqual(review['omitted_files'], 1)
        self.assertTrue((preview / self.chat.relative_to(self.codex)).is_file())
        self.assertFalse((preview / active.relative_to(self.codex)).exists())
        marker_text = (state / 'last-snapshot-linux.json').read_text()
        self.assertNotIn('rollout-active', marker_text)
        self.assertNotIn('/linux/Nexus', marker_text)
        with mock.patch.object(ns, 'source_fingerprint', return_value=previous):
            ns.supervisor_once(sender, previous)
            self.assertEqual(len(list(root.iterdir())), 1)
        active.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'active', 'cwd': '/linux/Nexus/Bot'}}) + '\n')
        # Simulate unchanged metadata: the omitted-file probe must still retry.
        with mock.patch.object(ns, 'source_fingerprint', return_value=previous):
            _, notes = ns.supervisor_once(sender, previous)
        self.assertTrue(any('снимок' in note for note in notes))
        state_after = ns.status(self.shared, 'linux', self.key)
        self.assertEqual(state_after['state'], 'snapshot_ready')
        self.assertEqual(state_after['omitted_files'], 0)
        self.assertEqual(state_after['files'], 2)

    def test_paginated_history_is_archived_with_warning_and_not_planned(self):
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'cwd': '/old/Nexus', 'history_mode': 'paginated'}}) + '\n')
        legacy = self.sessions / 'rollout-legacy.jsonl'
        legacy.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': 'legacy'}}) + '\n')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest = ns.verify(snapshot, self.key)
        self.assertEqual(manifest['paginated_files'], 1)
        self.assertEqual(len(manifest['files']), 2)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        ns.acknowledge(snapshot, self.shared, 'mac', self.key)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        with self.assertRaisesRegex(ns.SyncError, 'Пагинированная история'):
            ns._chat_catalog(self.codex)
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        _, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('неподтверждённые форматы' in note for note in notes))

    def test_unknown_history_mode_is_archived_with_warning(self):
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'history_mode': 'future-format'}}) + '\n')
        (self.sessions / 'rollout-no-meta.jsonl').write_text('{"type":"event_msg"}\n')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest = ns.verify(snapshot, self.key)
        self.assertEqual(manifest['unknown_history_files'], 2)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        (self.sessions / 'rollout-no-meta.jsonl').unlink()
        with self.assertRaisesRegex(ns.SyncError, 'Неизвестный формат'):
            ns._chat_catalog(self.codex)

    def test_changed_source_is_refused(self):
        real_digest = ns._digest
        def change_before_digest(path):
            if path == self.chat:
                self.chat.write_text('{"new":true}\n', encoding='utf-8')
            return real_digest(path)
        with mock.patch.object(ns, '_digest', side_effect=change_before_digest):
            with self.assertRaises(ns.SyncError):
                ns.snapshot(self.codex, self.shared, 'linux', self.key)

    def test_unreadable_chat_does_not_hide_other_stable_chat(self):
        other = self.sessions / 'rollout-other.jsonl'
        other.write_text('{"type":"session_meta","payload":{"id":"other"}}\n')
        real_digest = ns._digest
        def inaccessible(path):
            if path == other:
                raise PermissionError('synthetic permission failure')
            return real_digest(path)
        with mock.patch.object(ns, '_digest', side_effect=inaccessible):
            snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest = ns.verify(snapshot, self.key)
        self.assertEqual(manifest['omitted_files'], 1)
        self.assertEqual(len(manifest['files']), 1)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')

    def test_unstatable_chat_entry_is_counted_as_omitted(self):
        other = self.sessions / 'rollout-inaccessible.jsonl'
        other.write_text('{"type":"session_meta","payload":{"id":"other"}}\n')
        real_stat, real_lstat = Path.stat, Path.lstat
        def denied_stat(path, *args, **kwargs):
            if path == other:
                raise PermissionError('synthetic metadata failure')
            return real_stat(path, *args, **kwargs)
        def denied_lstat(path, *args, **kwargs):
            if path == other:
                raise PermissionError('synthetic metadata failure')
            return real_lstat(path, *args, **kwargs)
        with mock.patch.object(Path, 'stat', denied_stat), mock.patch.object(Path, 'lstat', denied_lstat):
            self.assertTrue(ns.source_fingerprint(self.codex, key=self.key))
            snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest = ns.verify(snapshot, self.key)
        self.assertEqual(manifest['omitted_files'], 1)
        self.assertEqual(len(manifest['files']), 1)

    def test_manifest_cannot_escape_snapshot(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        manifest_path = result / 'manifest.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        manifest['files'][0]['path'] = 'sessions/../../secret'
        manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
        with self.assertRaises(ns.SyncError):
            ns.verify(result)

    def test_extract_only_to_new_quarantine_and_preserves_bytes(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        target = self.root / 'quarantine'
        self.assertEqual(ns.extract(result, target, self.key), 1)
        recovered = target / 'sessions' / '2026' / '01' / '01' / self.chat.name
        self.assertEqual(recovered.read_bytes(), self.chat.read_bytes())
        with self.assertRaises(ns.SyncError):
            ns.extract(result, target, self.key)
        with self.assertRaises(ns.SyncError):
            ns.extract(result, self.root / '.codex', self.key)

    def test_prepare_changes_only_structured_cwd_and_rejects_unsafe_targets(self):
        project = self.root / 'Nexus' / 'project'
        project.mkdir(parents=True)
        records = [
            {'type': 'session_meta', 'payload': {'id': 'test', 'cwd': '/old/Nexus/project'}},
            {'type': 'turn_context', 'payload': {'cwd': '/old/Nexus/project'}},
            {'type': 'response_item', 'payload': {'text': '/old/Nexus/project'}}]
        self.chat.write_text(''.join(json.dumps(record) + '\n' for record in records))
        output = self.root / 'prepared'
        self.assertEqual(ns.prepare_chats(self.codex, output, '/old/Nexus', str(project.parent)), 1)
        prepared = (output / self.chat.relative_to(self.codex)).read_text().splitlines()
        self.assertEqual(json.loads(prepared[0])['payload']['cwd'], str(project))
        self.assertEqual(json.loads(prepared[1])['payload']['cwd'], str(project))
        self.assertEqual(json.loads(prepared[2]), records[2])
        with self.assertRaises(ns.SyncError):
            ns.prepare_chats(self.codex, output, '/old/Nexus', str(project.parent))
        (self.root / '.codex').mkdir()
        with self.assertRaises(ns.SyncError):
            ns.prepare_chats(self.codex, self.root / '.codex' / 'prepared', '/old/Nexus', str(project.parent))

    def test_paginated_preview_requires_explicit_opt_in_and_rejects_unknown_formats(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        def write(mode):
            self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
                {'id': 'mock', 'cwd': '/old/Nexus/project', 'history_mode': mode}}) + '\n')
        write('paginated')
        with self.assertRaises(ns.UnsupportedHistoryError):
            ns.prepare_chats(self.codex, self.root / 'default-preview', '/old/Nexus', str(nexus))
        output = self.root / 'paginated-preview'
        self.assertEqual(ns.prepare_chats(self.codex, output, '/old/Nexus', str(nexus),
                                          allow_paginated_preview=True), 1)
        meta = json.loads((output / self.chat.relative_to(self.codex)).read_text())
        self.assertEqual(meta['payload']['cwd'], str(nexus / 'project'))
        self.assertEqual(meta['payload']['history_mode'], 'paginated')
        write('unknown-future-mode')
        with self.assertRaises(ns.UnsupportedHistoryError):
            ns.prepare_chats(self.codex, self.root / 'unknown-preview', '/old/Nexus', str(nexus),
                             allow_paginated_preview=True)

    @unittest.skipUnless(os.name == 'posix', 'POSIX mode bits required')
    def test_decrypted_chat_directories_are_private_with_permissive_umask(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'mock', 'cwd': '/old/Nexus/project'}}) + '\n')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        quarantine = self.root / 'quarantine'
        prepared = self.root / 'prepared'
        previous_umask = os.umask(0)
        try:
            ns.extract(snapshot, quarantine, self.key)
            ns.prepare_chats(quarantine, prepared, '/old/Nexus', str(nexus))
        finally:
            os.umask(previous_umask)
        self.assertEqual(quarantine.stat().st_mode & 0o777, 0o700)
        self.assertEqual(prepared.stat().st_mode & 0o777, 0o700)

    def test_index_preview_rejects_active_or_shared_paths_before_starting_process(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        incoming = self.root / 'incoming'
        incoming.mkdir()
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        with mock.patch.object(ns.subprocess, 'Popen') as popen:
            for forbidden in (nexus, self.codex, self.shared, state):
                with self.assertRaises(ns.SyncError):
                    ns.index_preview(incoming, forbidden / 'output', '/source/Nexus', cfg, Path(sys.executable))
                with self.assertRaises(ns.SyncError):
                    ns.index_preview(forbidden, self.root / 'output', '/source/Nexus', cfg, Path(sys.executable))
            popen.assert_not_called()

    def test_reindex_mock_rejects_unmarked_and_active_profiles(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        mock_profile = self.root / 'mock-profile'
        mock_profile.mkdir()
        with mock.patch.object(ns.subprocess, 'Popen') as start:
            with self.assertRaises(ns.SyncError):
                ns.index_preview(mock_profile, mock_profile, str(nexus), cfg, Path(sys.executable), reindex_mock=True)
            (self.codex / 'index-preview.json').write_text(json.dumps({'format': 'nexus-index-preview-v1', 'applied': False}))
            with self.assertRaises(ns.SyncError):
                ns.index_preview(self.codex, self.codex, str(nexus), cfg, Path(sys.executable), reindex_mock=True)
            start.assert_not_called()

    def test_index_snapshot_preview_disallows_send_only_and_unknown_peers(self):
        cfg = {'direction': 'send-only', 'peer_roots': {'linux': '/old/Nexus'}}
        with mock.patch.object(ns, 'verify') as verify:
            with self.assertRaises(ns.SyncError):
                ns.index_snapshot_preview(cfg, self.key, 'linux', 'a' * 32,
                                          self.root / 'preview', Path(sys.executable))
            cfg['direction'] = 'send-receive'
            with self.assertRaises(ns.SyncError):
                ns.index_snapshot_preview(cfg, self.key, 'unconfigured', 'a' * 32,
                                          self.root / 'preview', Path(sys.executable))
            verify.assert_not_called()

    def test_auto_index_configuration_requires_safe_receiver_paths(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        previews = self.root / 'previews'
        previews.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'auto_index_preview_dir': str(previews),
               'auto_index_codex_executable': sys.executable}
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)
        raw['direction'] = 'send-receive'
        self.assertEqual(ns.validate_settings(raw)['auto_index_preview_dir'], previews.resolve())
        raw['auto_index_preview_dir'] = str(nexus)
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)
        raw['auto_index_preview_dir'] = str(previews)
        raw.pop('auto_index_codex_executable')
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)

    def test_auto_index_keeps_partial_warning_without_rebuilding(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        previews = self.root / 'previews'
        previews.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': 'mock', 'history_mode': 'paginated'}}) + '\n')
        ns.snapshot(source.parent, self.shared, 'linux', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'linux': '/old/Nexus'}, 'auto_index_preview_dir': str(previews),
            'auto_index_codex_executable': sys.executable})
        def fake_preview(_cfg, _key, _peer, _snapshot, destination, _executable, *, include_local):
            self.assertTrue(include_local)
            destination.mkdir()
            return {'partial': True}
        with mock.patch.object(ns, 'index_snapshot_preview', side_effect=fake_preview) as index:
            first = ns.auto_index_cycle(cfg)
            second = ns.auto_index_cycle(cfg)
        self.assertTrue(any(note.startswith('Индексация отложена:') for note in first))
        self.assertTrue(any(note.startswith('Индексация отложена:') for note in second))
        self.assertEqual(index.call_count, 1)
        listing = ns.auto_preview_records(cfg, self.key)
        self.assertEqual([record['state'] for record in listing['records']], ['partial'])
        self.assertEqual(listing['records'][0]['scope'], 'peer')
        ns.write_heartbeat(cfg, self.key, second, False, False)
        self.assertIn('index', ns.heartbeat_status(cfg, self.key)['errors'])
        (source / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': 'mock'}}) + '\n')
        newer = ns.snapshot(source.parent, self.shared, 'linux', self.key)
        os.utime(newer, ns=(newer.stat().st_atime_ns, newer.stat().st_mtime_ns + 1_000_000_000))
        with mock.patch.object(ns, 'index_snapshot_preview', side_effect=fake_preview) as index:
            ns.auto_index_cycle(cfg)
        self.assertEqual(index.call_count, 1)

    def test_auto_index_does_not_starve_other_peers_on_partial_or_invalid_snapshot(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        previews = self.root / 'previews'
        previews.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': 'mock', 'history_mode': 'paginated'}}) + '\n')
        ns.snapshot(source.parent, self.shared, 'a', self.key)
        bad = ns.snapshot(source.parent, self.shared, 'b', self.key)
        ns.snapshot(source.parent, self.shared, 'c', self.key)
        (bad / 'manifest.json').write_text('{}')
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'a': '/a/Nexus', 'b': '/b/Nexus', 'c': '/c/Nexus'},
            'auto_index_preview_dir': str(previews), 'auto_index_codex_executable': sys.executable})
        def fake_preview(_cfg, _key, peer, _snapshot, destination, _executable, *, include_local):
            destination.mkdir()
            return {'partial': peer == 'a'}
        with mock.patch.object(ns, 'index_snapshot_preview', side_effect=fake_preview) as index:
            ns.auto_index_cycle(cfg)
            second = ns.auto_index_cycle(cfg)
            third = ns.auto_index_cycle(cfg)
        self.assertEqual([call.args[2] for call in index.call_args_list], ['a', 'c'])
        self.assertTrue(any('Подготовлена' in note for note in second))
        self.assertGreaterEqual(sum(note.startswith('Индексация отложена:') for note in second), 2)
        self.assertGreaterEqual(sum(note.startswith('Индексация отложена:') for note in third), 2)

    def test_auto_combined_preview_updates_when_an_offline_peer_arrives(self):
        nexus, state, previews = (self.root / name for name in ('Nexus', 'state', 'previews'))
        for directory in (nexus, state, previews):
            directory.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source / 'rollout-mock.jsonl').write_text('{"type":"session_meta","payload":{"id":"mock"}}\n')
        first = ns.snapshot(source.parent, self.shared, 'a', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'a': '/a/Nexus', 'b': '/b/Nexus'}, 'auto_index_preview_dir': str(previews),
            'auto_index_codex_executable': sys.executable, 'auto_index_combine_peers': True})
        def fake_multi(_cfg, _key, _selections, destination, _exe, *, expected_tags):
            self.assertEqual(set(expected_tags), {peer for peer, _ in _selections})
            destination.mkdir()
            return {'partial': False, 'applied': False}
        with mock.patch.object(ns, 'index_multi_snapshot_preview', side_effect=fake_multi) as index:
            initial = ns.auto_index_cycle(cfg)
            repeated = ns.auto_index_cycle(cfg)
            second = ns.snapshot(source.parent, self.shared, 'b', self.key)
            complete = ns.auto_index_cycle(cfg)
        self.assertTrue(any(note.startswith('Индексация отложена:') for note in initial))
        self.assertTrue(any(note.startswith('Индексация отложена:') for note in repeated))
        self.assertFalse(any(note.startswith('Индексация отложена:') for note in complete))
        self.assertEqual(index.call_count, 2)
        self.assertEqual(index.call_args_list[0].args[2], [('a', first.name)])
        self.assertEqual(index.call_args_list[1].args[2], [('a', first.name), ('b', second.name)])
        listing = ns.auto_preview_records(cfg, self.key)
        self.assertEqual(sorted(item['state'] for item in listing['records']), ['marker_verified', 'partial'])
        status = ns.configured_status(cfg, self.key)
        self.assertEqual(status['auto_preview_records'], 1)
        self.assertEqual(status['auto_preview_partial'], 1)
        self.assertEqual(status['auto_preview_invalid'], 0)
        partial = next(item for item in listing['records'] if item['state'] == 'partial')
        (previews / partial['path'] / 'auto-preview.json').write_text('{}')
        status = ns.configured_status(cfg, self.key)
        self.assertEqual(status['auto_preview_invalid'], 1)
        self.assertIn('содержимое и актуальность не проверены', status['message'])

    def test_failed_auto_index_can_retry_without_overwriting_failed_copy(self):
        nexus, state = self.root / 'Nexus', self.root / 'state'
        nexus.mkdir()
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source / 'rollout-mock.jsonl').write_text('{"type":"session_meta","payload":{"id":"mock"}}\n')
        ns.snapshot(source.parent, self.shared, 'linux', self.key)
        for combined in (False, True):
            with self.subTest(combined=combined):
                previews = self.root / ('combined-previews' if combined else 'peer-previews')
                previews.mkdir()
                cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
                    'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
                    'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
                    'peer_roots': {'linux': '/linux/Nexus'}, 'auto_index_preview_dir': str(previews),
                    'auto_index_codex_executable': sys.executable, 'auto_index_combine_peers': combined})
                attempts = []
                def create(destination):
                    destination.mkdir()
                    attempts.append(destination)
                    (destination / 'mock-data').write_text('keep')
                    if len(attempts) == 1:
                        raise ns.SyncError('mock transient failure')
                    return {'partial': False, 'applied': False}
                def fake_multi(_cfg, _key, _selections, destination, _exe, *, expected_tags):
                    return create(destination)
                def fake_peer(_cfg, _key, _peer, _snapshot, destination, _exe, *, include_local):
                    return create(destination)
                name = 'index_multi_snapshot_preview' if combined else 'index_snapshot_preview'
                with mock.patch.object(ns, name, side_effect=fake_multi if combined else fake_peer):
                    if combined:
                        with self.assertRaises(ns.SyncError):
                            ns.auto_index_cycle(cfg)
                    else:
                        self.assertTrue(any('отложена' in note for note in ns.auto_index_cycle(cfg)))
                    self.assertTrue(any('Подготовлена' in note for note in ns.auto_index_cycle(cfg)))
                    self.assertEqual(ns.auto_index_cycle(cfg), [])
                self.assertEqual(len(attempts), 2)
                self.assertNotEqual(attempts[0], attempts[1])
                self.assertEqual((attempts[0] / 'mock-data').read_text(), 'keep')
                records = ns.auto_preview_records(cfg, self.key)['records']
                self.assertEqual(sorted(item['state'] for item in records), ['marker_verified', 'unverified'])

    def test_auto_index_limits_repeated_failed_attempts(self):
        nexus, state, previews = (self.root / name for name in ('Nexus', 'state', 'previews'))
        for directory in (nexus, state, previews):
            directory.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source / 'rollout-mock.jsonl').write_text('{"type":"session_meta","payload":{"id":"mock"}}\n')
        ns.snapshot(source.parent, self.shared, 'linux', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
            'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state), 'key_file': str(key),
            'direction': 'send-receive', 'peer_roots': {'linux': '/old/Nexus'}, 'auto_index_preview_dir': str(previews),
            'auto_index_codex_executable': sys.executable, 'auto_index_combine_peers': True})
        def fail(_cfg, _key, _selections, destination, _executable, *, expected_tags):
            destination.mkdir()
            raise ns.SyncError('mock failure')
        with mock.patch.object(ns, 'index_multi_snapshot_preview', side_effect=fail) as index:
            for _ in range(4):
                with self.assertRaises(ns.SyncError):
                    ns.auto_index_cycle(cfg)
        self.assertEqual(index.call_count, 3)
        self.assertEqual(len(ns.auto_preview_records(cfg, self.key)['records']), 3)

    def test_auto_index_rotates_peers_even_when_first_keeps_sending(self):
        nexus, state, previews = (self.root / name for name in ('Nexus', 'state', 'previews'))
        for directory in (nexus, state, previews):
            directory.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        chat = source / 'rollout-mock.jsonl'
        chat.write_text('{"type":"session_meta","payload":{"id":"mock"}}\n')
        ns.snapshot(source.parent, self.shared, 'a', self.key)
        ns.snapshot(source.parent, self.shared, 'b', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'a': '/a/Nexus', 'b': '/b/Nexus'}, 'auto_index_preview_dir': str(previews),
            'auto_index_codex_executable': sys.executable})
        def fake_preview(_cfg, _key, _peer, _snapshot, destination, _executable, *, include_local):
            destination.mkdir()
            return {'partial': False}
        with mock.patch.object(ns, 'index_snapshot_preview', side_effect=fake_preview) as index:
            ns.auto_index_cycle(cfg)
            chat.write_text(chat.read_text() + '{"type":"event_msg","payload":{}}\n')
            newer = ns.snapshot(source.parent, self.shared, 'a', self.key)
            os.utime(newer, ns=(newer.stat().st_atime_ns, newer.stat().st_mtime_ns + 1_000_000_000))
            ns.auto_index_cycle(cfg)
        self.assertEqual([call.args[2] for call in index.call_args_list], ['a', 'b'])

    def test_index_snapshot_merge_keeps_local_divergence_and_marks_partial(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        identifier = '019b0000-0000-7000-8000-000000000066'
        def history(cwd, message):
            return (json.dumps({'type': 'session_meta', 'payload': {'id': identifier,
                    'cwd': cwd, 'history_mode': 'paginated'}}) + '\n' +
                    json.dumps({'type': 'event_msg', 'payload': {'message': message}}) + '\n')
        self.chat.write_text(history(str(nexus / 'project'), 'local'))
        original = self.chat.read_bytes()
        sender = self.root / 'sender' / 'sessions'
        sender.mkdir(parents=True)
        (sender / 'rollout-mock.jsonl').write_text(history('/old/Nexus/project', 'remote'))
        snapshot = ns.snapshot(sender.parent, self.shared, 'linux', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'linux': '/old/Nexus'}})
        def fake_index(merged, destination, source_root, passed_cfg, executable):
            content = next(merged.rglob('*.jsonl')).read_text()
            self.assertIn('local', content)
            self.assertNotIn('remote', content)
            destination.mkdir()
            return {'applied': False}
        destination = self.root / 'merge-index-preview'
        with mock.patch.object(ns, 'index_preview', side_effect=fake_index):
            report = ns.index_snapshot_preview(cfg, self.key, 'linux', snapshot.name, destination,
                                               Path(sys.executable), include_local=True)
        self.assertTrue(report['partial'])
        self.assertEqual(report['unresolved_chats'], 1)
        self.assertEqual(report['actions'][0]['action'], 'CONFLICT')
        self.assertEqual(self.chat.read_bytes(), original)
        source = sender / 'rollout-mock.jsonl'
        source.write_text(history('/old/Nexus/project', 'local') +
                          json.dumps({'type': 'event_msg', 'payload': {'message': 'later'}}) + '\n')
        extended = ns.snapshot(sender.parent, self.shared, 'linux', self.key)
        def fake_extended(merged, destination, source_root, passed_cfg, executable):
            content = next(merged.rglob('*.jsonl')).read_text()
            self.assertIn('local', content)
            self.assertIn('later', content)
            destination.mkdir()
            return {'applied': False}
        with mock.patch.object(ns, 'index_preview', side_effect=fake_extended):
            report = ns.index_snapshot_preview(cfg, self.key, 'linux', extended.name,
                self.root / 'extended-index-preview', Path(sys.executable), include_local=True)
        self.assertFalse(report['partial'])
        self.assertEqual(report['actions'][0]['action'], 'EXTEND_COPY')
        self.assertEqual(self.chat.read_bytes(), original)
        source.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': identifier,
                          'cwd': '/old/Nexus/project', 'history_mode': 'paginated'}}) + '\n')
        shorter = ns.snapshot(sender.parent, self.shared, 'linux', self.key)
        with mock.patch.object(ns, 'index_preview', side_effect=fake_index):
            report = ns.index_snapshot_preview(cfg, self.key, 'linux', shorter.name,
                self.root / 'local-longer-index-preview', Path(sys.executable), include_local=True)
        self.assertFalse(report['partial'])
        self.assertEqual(report['actions'][0]['action'], 'KEEP_LOCAL_EXTENDS')

    def test_multi_preview_reports_peer_conflict_and_rejects_duplicate_selection(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        identifier = '019b0000-0000-7000-8000-000000000066'
        def history(cwd, message):
            return (json.dumps({'type': 'session_meta', 'payload': {'id': identifier, 'cwd': cwd}}) + '\n' +
                    json.dumps({'type': 'event_msg', 'payload': {'message': message}}) + '\n')
        self.chat.write_text(history(str(nexus / 'project'), 'local'))
        original = self.chat.read_bytes()
        (self.codex / 'session_index.jsonl').write_text(json.dumps({'id': identifier, 'thread_name': 'local title'}) + '\n')
        source = self.root / 'sender' / 'sessions'
        source.mkdir(parents=True)
        (source.parent / 'session_index.jsonl').write_text(json.dumps({'id': identifier, 'thread_name': 'peer title'}) + '\n')
        chat = source / 'rollout-mock.jsonl'
        chat.write_text(history('/a/Nexus/project', 'local') +
                        json.dumps({'type': 'event_msg', 'payload': {'message': 'later'}}) + '\n')
        first = ns.snapshot(source.parent, self.shared, 'a', self.key)
        chat.write_text(history('/b/Nexus/project', 'different'))
        second = ns.snapshot(source.parent, self.shared, 'b', self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'a': '/a/Nexus', 'b': '/b/Nexus'}})
        def fake_index(merged, destination, _source_root, _cfg, _executable):
            content = next(merged.rglob('rollout-*.jsonl')).read_text()
            self.assertIn('later', content)
            self.assertNotIn('different', content)
            self.assertEqual(json.loads((merged / 'session_index.jsonl').read_text())['thread_name'], 'local title')
            destination.mkdir()
            return {'applied': False}
        with mock.patch.object(ns, 'index_preview', side_effect=fake_index):
            report = ns.index_multi_snapshot_preview(cfg, self.key, [('a', first.name), ('b', second.name)],
                                                       self.root / 'multi', Path(sys.executable))
        self.assertEqual([item['action'] for item in report['actions']], ['EXTEND_COPY', 'CONFLICT'])
        self.assertTrue(report['partial'])
        self.assertEqual(report['unresolved_chats'], 1)
        self.assertEqual(report['title_conflicts'], [hashlib.sha256(identifier.encode()).hexdigest()])
        self.assertEqual(self.chat.read_bytes(), original)
        with self.assertRaises(ns.SyncError):
            ns.index_multi_snapshot_preview(cfg, self.key, [('a', first.name), ('a', first.name)],
                                            self.root / 'duplicate', Path(sys.executable))
        with self.assertRaises(ns.SyncError):
            ns.index_multi_snapshot_preview(cfg, self.key, [('a', first.name)],
                self.root / 'changed', Path(sys.executable), expected_tags={'a': 'wrong'})
        self.assertFalse((self.root / 'changed').exists())

    def test_index_preview_does_not_mark_success_when_page_request_fails(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        incoming = self.root / 'incoming' / 'sessions'
        incoming.mkdir(parents=True)
        (incoming / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': '019b0000-0000-7000-8000-000000000006', 'cwd': '/old/Nexus/project',
                        'history_mode': 'paginated'}}) + '\n')
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        process = mock.Mock()
        process.stdin = io.BytesIO()
        process.stdout = io.BytesIO(b'{"id":1,"result":{}}\n{"id":2,"result":{}}\n{"id":3,"error":{"message":"private"}}\n')
        process.returncode = 0
        destination = self.root / 'index-preview'
        with mock.patch.object(ns.subprocess, 'Popen', return_value=process):
            with self.assertRaises(ns.SyncError):
                ns.index_preview(incoming.parent, destination, '/old/Nexus', cfg, Path(sys.executable))
        self.assertFalse((destination / 'index-preview.json').exists())

    def test_failed_mock_archive_reindex_keeps_partial_report(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        profile = self.root / 'mock-profile'
        archive = profile / 'archived_sessions'
        archive.mkdir(parents=True)
        identifier = '019b0000-0000-7000-8000-000000000006'
        (archive / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': identifier, 'cwd': str(nexus)}}) + '\n')
        marker = profile / 'index-preview.json'
        marker.write_text(json.dumps({'format': 'nexus-index-preview-v1', 'applied': False, 'partial': False}))
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        process = mock.Mock()
        process.stdin = io.BytesIO()
        lines = [{'id': 1, 'result': {}}, {'id': 2, 'result': {'data': [], 'nextCursor': None}},
                 {'id': 3, 'result': {'data': [], 'nextCursor': None}}, {'id': 4, 'result': {}},
                 {'id': 5, 'error': {'message': 'mock archive failure'}}]
        process.stdout = io.BytesIO(''.join(json.dumps(line) + '\n' for line in lines).encode())
        process.returncode = 0
        with mock.patch.object(ns.subprocess, 'Popen', return_value=process):
            with self.assertRaises(ns.SyncError):
                ns.index_preview(profile, profile, str(nexus), cfg, Path(sys.executable), reindex_mock=True)
        result = json.loads(marker.read_text())
        self.assertTrue(result['partial'])
        self.assertTrue(result['reindex_in_progress_or_failed'])
        with mock.patch.object(ns.subprocess, 'Popen') as start:
            with self.assertRaises(ns.SyncError):
                ns.index_preview(profile, profile, str(nexus), cfg, Path(sys.executable), reindex_mock=True)
            start.assert_not_called()

    def test_index_preview_marks_unlisted_database_ids_as_partial(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        incoming = self.root / 'incoming' / 'sessions'
        incoming.mkdir(parents=True)
        identifier = '019b0000-0000-7000-8000-000000000006'
        (incoming / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': identifier, 'cwd': '/old/Nexus/project', 'history_mode': 'paginated'}}) + '\n')
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        process = mock.Mock()
        process.stdin = io.BytesIO()
        process.stdout = io.BytesIO(b'{"id":1,"result":{}}\n{"id":2,"result":{}}\n'
                                    b'{"id":3,"result":{"data":[],"nextCursor":null}}\n'
                                    b'{"id":4,"result":{"data":[],"nextCursor":null}}\n')
        process.returncode = 0
        with mock.patch.object(ns.subprocess, 'Popen', return_value=process):
            report = ns.index_preview(incoming.parent, self.root / 'index-preview', '/old/Nexus',
                                      cfg, Path(sys.executable))
        self.assertTrue(report['partial'])
        self.assertEqual(report['db_unlisted'], [hashlib.sha256(identifier.encode()).hexdigest()])
        self.assertNotIn(identifier, json.dumps(report))

    def test_index_preview_follows_turn_pages_and_rejects_repeated_cursor(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        incoming = self.root / 'incoming' / 'sessions'
        incoming.mkdir(parents=True)
        identifier = '019b0000-0000-7000-8000-000000000006'
        (incoming / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': identifier, 'cwd': '/old/Nexus/project', 'history_mode': 'paginated'}}) + '\n')
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        class Capture(io.BytesIO):
            def close(self):
                self.value = self.getvalue()
                super().close()
        def run(second_cursor, destination):
            process = mock.Mock()
            process.stdin = Capture()
            lines = [{'id': 1, 'result': {}}, {'id': 2, 'result': {}},
                     {'id': 3, 'result': {'data': [{'mock': 'one'}], 'nextCursor': 'next'}},
                     {'id': 4, 'result': {'data': [{'mock': 'two'}], 'nextCursor': second_cursor}},
                     {'id': 5, 'result': {'data': [{'id': identifier}], 'nextCursor': None}}]
            process.stdout = io.BytesIO(''.join(json.dumps(line) + '\n' for line in lines).encode())
            process.returncode = 0
            with mock.patch.object(ns.subprocess, 'Popen', return_value=process):
                if second_cursor is None:
                    report = ns.index_preview(incoming.parent, destination, '/old/Nexus', cfg, Path(sys.executable))
                    self.assertEqual(report['turn_pages_checked'], 2)
                    self.assertEqual(report['turn_items_returned'], 2)
                    self.assertFalse(report['contents_verified'])
                    sent = [json.loads(line) for line in process.stdin.value.splitlines()]
                    self.assertEqual([item['params'].get('cursor') for item in sent if item.get('method') == 'thread/turns/list'], [None, 'next'])
                else:
                    with self.assertRaises(ns.SyncError):
                        ns.index_preview(incoming.parent, destination, '/old/Nexus', cfg, Path(sys.executable))
                    self.assertFalse((destination / 'index-preview.json').exists())
        run(None, self.root / 'complete-pages')
        run('next', self.root / 'repeated-cursor')

    def test_archived_paginated_preview_is_marked_unverified(self):
        nexus = self.root / 'Nexus'
        (nexus / 'project').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        incoming = self.root / 'incoming' / 'archived_sessions'
        incoming.mkdir(parents=True)
        identifier = '019b0000-0000-7000-8000-000000000006'
        (incoming / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta',
            'payload': {'id': identifier, 'cwd': '/old/Nexus/project', 'history_mode': 'paginated'}}) + '\n')
        cfg = {'nexus': nexus, 'codex_home': self.codex, 'shared': self.shared, 'state_dir': state}
        process = mock.Mock()
        process.stdin = io.BytesIO()
        lines = [{'id': 1, 'result': {}}, {'id': 2, 'result': {'data': [{'id': identifier}], 'nextCursor': None}},
                 {'id': 3, 'result': {'data': [{'id': identifier}], 'nextCursor': None}}]
        process.stdout = io.BytesIO(''.join(json.dumps(line) + '\n' for line in lines).encode())
        process.returncode = 0
        with mock.patch.object(ns.subprocess, 'Popen', return_value=process):
            report = ns.index_preview(incoming.parent, self.root / 'index-preview', '/old/Nexus',
                                      cfg, Path(sys.executable))
        self.assertTrue(report['partial'])
        self.assertEqual(report['db_unlisted'], [])
        self.assertEqual(report['archived_paginated_unverified'], [hashlib.sha256(identifier.encode()).hexdigest()])
        self.assertEqual(report['pagination_responded'], 0)

    def test_prepare_rejects_unmapped_and_missing_project(self):
        project = self.root / 'Nexus'
        project.mkdir()
        for cwd in ('/elsewhere/project', '/old/Nexus/missing'):
            self.chat.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': 'test', 'cwd': cwd}}) + '\n')
            with self.assertRaises(ns.SyncError):
                ns.prepare_chats(self.codex, self.root / 'prepared', '/old/Nexus', str(project))
            self.assertFalse((self.root / 'prepared').exists())

    def test_receipt_requires_valid_snapshot_and_does_not_claim_import(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        receipt = ns.acknowledge(result, self.shared, 'mac', self.key)
        data = json.loads(receipt.read_text(encoding='utf-8'))
        self.assertFalse(data['applied'])
        self.assertEqual(data['receiver'], 'mac')
        self.assertEqual(data['authentication'], ns._manifest_tag(data, self.key))
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['received_by'], ['mac'])
        self.assertEqual(ns.status(self.shared, 'linux')['received_by'], [])
        self.assertFalse(ns.status(self.shared, 'linux')['authenticity_verified'])
        self.assertIn('подлинность не подтверждена', ns.status(self.shared, 'linux')['message'])
        nexus = self.root / 'Nexus'
        state = self.root / 'state'
        nexus.mkdir()
        state.mkdir()
        key_file = self.root / 'key'
        key_file.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key_file),
            'peer_roots': {'mac': '/mock/mac/Nexus', 'windows': 'C:\\mock\\Nexus'}})
        partial = ns.configured_status(cfg, self.key)
        self.assertEqual((partial['confirmed_receivers'], partial['expected_receivers']), (1, 2))
        self.assertIn('1 из 2', partial['message'])
        self.assertIn('импорт в Codex не подтверждён', partial['message'])
        ns.acknowledge(result, self.shared, 'windows', self.key)
        complete = ns.configured_status(cfg, self.key)
        self.assertEqual((complete['confirmed_receivers'], complete['expected_receivers']), (2, 2))
        with self.assertRaises(FileExistsError):
            ns.acknowledge(result, self.shared, 'mac', self.key)

    def test_background_receiver_acknowledges_each_valid_peer_snapshot_once(self):
        ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(ns.receive_once(self.shared, 'mac', self.key), 1)
        self.assertEqual(ns.receive_once(self.shared, 'mac', self.key), 0)
        self.assertEqual(ns.receive_once(self.shared, 'linux', self.key), 0)

    def test_supervisor_sends_once_and_initializes_project_baseline(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        (nexus / 'requirements.txt').write_text('example==1')
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'interval_seconds': 60}
        cfg = ns.validate_settings(raw)
        previous, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('снимок' in note for note in notes))
        self.assertTrue((state / 'projects-linux.json').is_file())
        report = state / 'project-changelog-linux.md'
        self.assertIn('Python-окружение не найдено', report.read_text())
        self.assertEqual(ns.previous_supervisor_snapshot(cfg), previous)
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertEqual(notes, [])
        (nexus / 'requirements.txt').write_text('example==2')
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertTrue(any('журнал' in note for note in notes))
        self.assertIn('Изменён манифест', report.read_text())
        self.assertNotIn('example==2', report.read_text())

    def test_slow_git_worker_does_not_block_chat_cycles(self):
        started = threading.Event()
        release = threading.Event()
        cycles = []
        cfg = {'git_repos': [Path('/fake/repo')], 'interval_seconds': 10,
               'key_file': self.root / 'fake-key', 'shared': self.shared}
        def git_work(_cfg):
            started.set()
            if not release.wait(timeout=3):
                raise AssertionError('test did not release Git worker')
            return []
        def chat_work(_cfg, previous, *, include_git):
            self.assertFalse(include_git)
            cycles.append(1)
            if len(cycles) == 2:
                self.assertTrue(started.is_set())
                self.assertFalse(release.is_set())
                release.set()
            return previous, []
        def tick(_seconds):
            if len(cycles) == 1:
                self.assertTrue(started.wait(timeout=3))
            else:
                raise KeyboardInterrupt
        with mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'git_cycle', side_effect=git_work), \
             mock.patch.object(ns, 'supervisor_once', side_effect=chat_work), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat'), \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(len(cycles), 2)

    def test_windows_content_scan_does_not_block_other_supervisor_cycles(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        observations = []
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'codex_home': self.codex}
        def fingerprint_work(_cfg):
            started.set()
            if not release.wait(timeout=3):
                raise AssertionError('test did not release content scan')
            finished.set()
            return 'a' * 64
        def cycle(_cfg, previous, *, include_git, fingerprint_override):
            observations.append(fingerprint_override)
            if len(observations) == 2:
                self.assertFalse(release.is_set())
                release.set()
            return previous, []
        def tick(_seconds):
            if len(observations) == 1:
                self.assertTrue(started.wait(timeout=3))
            elif len(observations) == 2:
                self.assertTrue(finished.wait(timeout=3))
                threading.Event().wait(0.05)
            else:
                raise KeyboardInterrupt
        with mock.patch.object(ns.sys, 'platform', 'win32'), \
             mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, '_source_fingerprint_cycle', side_effect=fingerprint_work), \
             mock.patch.object(ns, 'supervisor_once', side_effect=cycle), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat') as heartbeat, \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(observations, [None, None, 'a' * 64])
        self.assertTrue(any(call.kwargs.get('fingerprint_running') is True for call in heartbeat.call_args_list))

    def test_pending_content_scan_still_updates_other_supervisor_tasks(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        (nexus / 'requirements.txt').write_text('synthetic==1\n')
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'windows',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        with mock.patch.object(ns, 'source_fingerprint', side_effect=AssertionError('unexpected inline scan')):
            previous, notes = ns.supervisor_once(cfg, None, fingerprint_override=None)
        self.assertIsNone(previous)
        self.assertTrue((state / 'projects-windows.json').is_file())
        self.assertFalse((self.shared / 'nexus-sync' / 'snapshots' / 'windows').exists())

    def test_windows_content_scan_failure_is_reported_without_reusing_old_result(self):
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'codex_home': self.codex}
        class FailedExecutor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def submit(self, *_args):
                result = Future()
                result.set_exception(ns.SyncError('synthetic private error'))
                return result
        cycles = []
        def supervisor(_cfg, previous, *, include_git, fingerprint_override):
            cycles.append(fingerprint_override)
            return previous, []
        def tick(_seconds):
            if len(cycles) == 2:
                raise KeyboardInterrupt
        with mock.patch.object(ns.sys, 'platform', 'win32'), \
             mock.patch.object(ns, 'ThreadPoolExecutor', side_effect=lambda **_kw: FailedExecutor()), \
             mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'supervisor_once', side_effect=supervisor), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat') as heartbeat, \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(cycles, [None, None])
        self.assertTrue(any('Снимок отложен:' in note for note in heartbeat.call_args.args[2]))

    def test_windows_scan_error_remains_until_successful_scan(self):
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'codex_home': self.codex}
        failed, pending, following = Future(), Future(), Future()
        failed.set_exception(ns.SyncError('synthetic failure'))
        following.set_result('c' * 64)
        sequence = iter((failed, pending, following))
        class TestExecutor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def submit(self, *_args):
                return next(sequence)
        cycles = []
        def supervisor(_cfg, previous, *, include_git, fingerprint_override):
            cycles.append(fingerprint_override)
            return previous, []
        def tick(_seconds):
            if len(cycles) == 3:
                pending.set_result('b' * 64)
            elif len(cycles) == 4:
                raise KeyboardInterrupt
        with mock.patch.object(ns.sys, 'platform', 'win32'), \
             mock.patch.object(ns, 'ThreadPoolExecutor', side_effect=lambda **_kw: TestExecutor()), \
             mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'supervisor_once', side_effect=supervisor), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat') as heartbeat, \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(cycles, [None, None, None, 'b' * 64])
        notes = [call.args[2] for call in heartbeat.call_args_list]
        self.assertFalse(any(item.startswith('Снимок отложен:') for item in notes[0]))
        self.assertTrue(any(item.startswith('Снимок отложен:') for item in notes[1]))
        self.assertTrue(any(item.startswith('Снимок отложен:') for item in notes[2]))
        self.assertFalse(any(item.startswith('Снимок отложен:') for item in notes[3]))

    def test_windows_snapshot_error_remains_while_next_scan_is_pending(self):
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'codex_home': self.codex}
        first, pending, following = Future(), Future(), Future()
        first.set_result('a' * 64)
        following.set_result('c' * 64)
        sequence = iter((first, pending, following))
        class TestExecutor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def submit(self, *_args):
                return next(sequence)
        cycles = []
        def supervisor(_cfg, previous, *, include_git, fingerprint_override):
            cycles.append(fingerprint_override)
            notes = ['Снимок отложен: SyncError'] if fingerprint_override == 'a' * 64 else []
            return previous, notes
        def tick(_seconds):
            if len(cycles) == 3:
                pending.set_result('b' * 64)
            elif len(cycles) == 4:
                raise KeyboardInterrupt
        with mock.patch.object(ns.sys, 'platform', 'win32'), \
             mock.patch.object(ns, 'ThreadPoolExecutor', side_effect=lambda **_kw: TestExecutor()), \
             mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'supervisor_once', side_effect=supervisor), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat') as heartbeat, \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(cycles, [None, 'a' * 64, None, 'b' * 64])
        notes = [call.args[2] for call in heartbeat.call_args_list]
        self.assertTrue(any(item.startswith('Снимок отложен:') for item in notes[1]))
        self.assertTrue(any(item.startswith('Снимок отложен:') for item in notes[2]))
        self.assertFalse(any(item.startswith('Снимок отложен:') for item in notes[3]))

    def test_slow_auto_index_does_not_block_chat_cycles(self):
        started, release = threading.Event(), threading.Event()
        cycles = []
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'auto_index_preview_dir': self.root / 'preview'}
        def index_work(_cfg):
            started.set()
            self.assertTrue(release.wait(timeout=3))
            return []
        def chat_work(_cfg, previous, *, include_git):
            cycles.append(1)
            if len(cycles) == 2:
                self.assertTrue(started.is_set())
                release.set()
            return previous, []
        def tick(_seconds):
            if len(cycles) == 1:
                self.assertTrue(started.wait(timeout=3))
            else:
                raise KeyboardInterrupt
        with mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'auto_index_cycle', side_effect=index_work), \
             mock.patch.object(ns, 'supervisor_once', side_effect=chat_work), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat'), \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertEqual(len(cycles), 2)

    def test_finished_auto_index_error_is_recorded_before_heartbeat(self):
        cfg = {'git_repos': [], 'interval_seconds': 10, 'key_file': self.root / 'key',
               'shared': self.shared, 'auto_index_preview_dir': self.root / 'preview'}
        class DoneExecutor:
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def submit(self, *_args):
                result = Future()
                result.set_exception(ns.SyncError('test'))
                return result
        with mock.patch.object(ns, 'ThreadPoolExecutor', side_effect=lambda **_kw: DoneExecutor()), \
             mock.patch.object(ns, 'load_settings', return_value=cfg), \
             mock.patch.object(ns, 'previous_supervisor_snapshot', return_value=None), \
             mock.patch.object(ns, 'supervisor_once', return_value=(None, [])), \
             mock.patch.object(ns, 'read_key', return_value=self.key), \
             mock.patch.object(ns, 'write_heartbeat') as heartbeat, \
             mock.patch.object(ns.time, 'sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(self.root / 'unused.json')
        self.assertTrue(any(note.startswith('Индексация отложена:') for note in heartbeat.call_args.args[2]))
        self.assertFalse(heartbeat.call_args.args[5])

    def test_signed_heartbeat_reports_stale_and_errors_without_private_notes(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        self.assertEqual(ns.configured_status(cfg, self.key)['observer']['state'], 'unknown')
        with mock.patch.object(ns.time, 'time', return_value=1000):
            ns.write_heartbeat(cfg, self.key, [], False, True, True, fingerprint_running=True)
        with mock.patch.object(ns.time, 'time', return_value=1001):
            observed = ns.configured_status(cfg, self.key)['observer']
            self.assertEqual(observed['state'], 'recent')
            self.assertTrue(observed['git_running'])
            self.assertTrue(observed['index_running'])
            self.assertTrue(observed['fingerprint_running'])
            status = ns.configured_status(dict(cfg, auto_index_preview_dir=self.root / 'preview'), self.key)
            self.assertEqual(status['index_activity'], 'running')
            self.assertEqual(status['fingerprint_activity'], 'running')
        with mock.patch.object(ns.time, 'time', return_value=1006):
            ns.write_heartbeat(cfg, self.key, ['Пакет выбранных файлов отложен: FAKE_PRIVATE'], True, False)
            observed = ns.configured_status(cfg, self.key)['observer']
            self.assertEqual(observed['state'], 'error')
            self.assertEqual(set(observed['errors']), {'secrets', 'git'})
            self.assertIsNone(observed['index_running'])
            self.assertIsNone(observed['fingerprint_running'])
            status = ns.configured_status(dict(cfg, auto_index_preview_dir=self.root / 'preview'), self.key)
            self.assertEqual(status['index_activity'], 'unknown')
        heartbeat = state / 'heartbeat-linux.json'
        self.assertNotIn('FAKE_PRIVATE', heartbeat.read_text())
        with mock.patch.object(ns.time, 'time', return_value=1300):
            self.assertEqual(ns.configured_status(cfg, self.key)['observer']['state'], 'stale')
        altered = json.loads(heartbeat.read_text())
        altered['timestamp'] += 1
        heartbeat.write_text(json.dumps(altered))
        self.assertEqual(ns.configured_status(cfg, self.key)['observer']['state'], 'unverified')
        with self.assertRaises(ns.SyncError):
            ns.write_heartbeat(cfg, self.key, [], False, False)

    def test_supervisor_updates_heartbeat_after_failure_and_recovery(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        config = self.root / 'config.json'
        config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'interval_seconds': 10}))
        self.chat.write_bytes(b'{"partial":')
        with mock.patch.object(ns.time, 'sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(config)
        cfg = ns.load_settings(config)
        self.assertEqual(ns.heartbeat_status(cfg, self.key)['state'], 'error')
        self.assertIn('snapshot', ns.heartbeat_status(cfg, self.key)['errors'])
        self.chat.write_text('{"type":"session_meta","payload":{"id":"test"}}\n')
        with mock.patch.object(ns.time, 'sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(config)
        self.assertEqual(ns.heartbeat_status(cfg, self.key)['state'], 'recent')

    def test_project_report_filters_peers_and_does_not_overwrite_foreign_file(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        state = self.root / 'state'
        state.mkdir()
        peer_state = self.root / 'peer-state'
        peer_state.mkdir()
        self.record_projects(nexus, self.shared, peer_state, 'mac')
        (project / 'package.json').write_text('{"version":"2"}')
        self.record_projects(nexus, self.shared, peer_state, 'mac')
        current = ns.inventory(nexus)
        report = self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        self.assertNotIn('— mac', report.read_text())
        self.write_project_report(self.shared, state, 'linux', current, include_peers=True)
        self.assertIn('— mac', report.read_text())
        peer_event = next((self.shared / 'nexus-sync' / 'project-events' / 'mac').glob('*.nxe'))
        saved = peer_event.read_bytes()
        peer_event.write_bytes(b'corrupted')
        self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        with self.assertRaises(ns.SyncError):
            self.write_project_report(self.shared, state, 'linux', current, include_peers=True)
        peer_event.write_bytes(saved)
        report.write_text('Мой собственный файл\n')
        with self.assertRaises(ns.SyncError):
            self.write_project_report(self.shared, state, 'linux', current, include_peers=True)
        self.assertEqual(report.read_text(), 'Мой собственный файл\n')

    def test_dependency_change_reminder_survives_existing_environment(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        lock = project / 'package-lock.json'
        lock.write_text('{"version": 1}')
        (project / 'node_modules').mkdir()
        (project / 'node_modules' / 'mock-package').mkdir()
        (project / 'node_modules' / 'mock-package' / 'package.json').write_text(
            json.dumps({'name': 'mock-package', 'version': '1.0.0'}))
        state = self.root / 'state'
        state.mkdir()
        self.record_projects(nexus, self.shared, state, 'linux')
        lock.write_text('{"version": 2}')
        self.record_projects(nexus, self.shared, state, 'linux')
        current = ns.inventory(nexus)
        current['executables'].update({'node': True, 'npm': True})
        self.assertEqual(ns.project_actions(current), [])
        report = self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        self.assertIn('app: изменён package-lock.json', report.read_text())
        self.record_projects(nexus, self.shared, state, 'linux')
        self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        self.assertIn('app: изменён package-lock.json', report.read_text())

    def test_supervisor_never_trusts_missing_old_snapshot(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        (state / 'last-snapshot-linux.json').write_text(json.dumps({'id': 'a' * 32, 'fingerprint': 'b' * 64}))
        self.assertIsNone(ns.previous_supervisor_snapshot(cfg))

    def test_supervisor_does_not_trust_changed_snapshot_marker(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        previous, _ = ns.supervisor_once(cfg, None)
        self.assertEqual(ns.previous_supervisor_snapshot(cfg), previous)
        marker_path = state / 'last-snapshot-linux.json'
        marker = json.loads(marker_path.read_text())
        marker['fingerprint'] = 'f' * 64
        marker_path.write_text(json.dumps(marker))
        self.assertIsNone(ns.previous_supervisor_snapshot(cfg))

    def test_windows_fingerprint_detects_same_metadata_content_change(self):
        original_stat = Path.stat
        saved = self.chat.stat()
        def fixed_stat(path, *args, **kwargs):
            if path == self.chat and kwargs.get('follow_symlinks', True):
                return saved
            return original_stat(path, *args, **kwargs)
        with mock.patch.object(Path, 'stat', fixed_stat):
            windows_first = ns.source_fingerprint(self.codex, key=self.key, platform='win32')
            linux_first = ns.source_fingerprint(self.codex, key=self.key, platform='linux')
            data = self.chat.read_bytes()
            self.chat.write_bytes(data.replace(b'"test"', b'"best"'))
            self.assertEqual(len(self.chat.read_bytes()), len(data))
            windows_second = ns.source_fingerprint(self.codex, key=self.key, platform='win32')
            linux_second = ns.source_fingerprint(self.codex, key=self.key, platform='linux')
        self.assertEqual(linux_first, linux_second)
        self.assertNotEqual(windows_first, windows_second)
        self.assertNotIn('best', windows_second)
        self.assertNotEqual(windows_second, ns.source_fingerprint(self.codex, key=b'b' * 32, platform='win32'))
        with self.assertRaises(ns.SyncError):
            ns.source_fingerprint(self.codex, platform='win32')

    def test_windows_fingerprint_streams_large_history_without_loading_it_all(self):
        import tracemalloc
        chunk = b'x' * (1024 * 1024)
        with self.chat.open('wb') as stream:
            for _ in range(16):
                stream.write(chunk)
        tracemalloc.start()
        try:
            fingerprint = ns.source_fingerprint(self.codex, key=self.key, platform='win32')
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(len(fingerprint), 64)
        self.assertLess(peak, 8 * 1024 * 1024)

    def test_supervisor_resends_windows_content_change_with_identical_metadata(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'windows',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        saved = self.chat.stat()
        original_stat, original_fingerprint = Path.stat, ns.source_fingerprint
        def fixed_stat(path, *args, **kwargs):
            if path == self.chat and kwargs.get('follow_symlinks', True):
                return saved
            return original_stat(path, *args, **kwargs)
        def windows_fingerprint(path, *, key=None):
            return original_fingerprint(path, key=key, platform='win32')
        with mock.patch.object(Path, 'stat', fixed_stat), mock.patch.object(ns, 'source_fingerprint', windows_fingerprint):
            previous, _ = ns.supervisor_once(cfg, None)
            self.chat.write_bytes(self.chat.read_bytes().replace(b'"test"', b'"best"'))
            updated, notes = ns.supervisor_once(cfg, previous)
        self.assertNotEqual(updated, previous)
        self.assertTrue(any('снимок' in item for item in notes))
        latest = ns.latest_authenticated_snapshot(self.shared / 'nexus-sync' / 'snapshots' / 'windows', 'windows', self.key)
        quarantine = self.root / 'mock-quarantine'
        ns.extract(latest[0], quarantine, self.key)
        self.assertIn(b'"best"', (quarantine / self.chat.relative_to(self.codex)).read_bytes())

    def test_config_rejects_a_key_inside_synced_nexus(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        key = nexus / 'key'
        key.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key)}
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)

    def test_send_only_supervisor_does_not_acknowledge_other_devices(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        ns.snapshot(self.codex, self.shared, 'mac', self.key)
        self.assertEqual(cfg['direction'], 'send-only')
        with mock.patch.object(ns, 'receive_once', side_effect=AssertionError('receive prohibited')):
            ns.supervisor_once(cfg, None)
        self.assertFalse((self.shared / 'nexus-sync' / 'receipts' / 'mac').exists())

    def test_syncthing_source_config_check_rejects_receiving_and_overlaps(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        config = self.root / 'syncthing.xml'
        cfg = {'nexus': nexus, 'direction': 'send-only'}
        document = ns.ET.Element('configuration')
        folder = ns.ET.SubElement(document, 'folder', {'id': 'projects', 'path': str(nexus), 'type': 'sendonly'})
        ns.ET.SubElement(folder, 'paused').text = 'false'
        ns.ET.SubElement(ns.ET.SubElement(document, 'gui'), 'apikey').text = 'SYNTHETIC_PRIVATE_API_KEY'
        def write():
            ns.ET.ElementTree(document).write(config, encoding='utf-8', xml_declaration=True)
        def check():
            return ns.syncthing_source_config_status(cfg, config, 'projects')
        write()
        before = config.read_bytes()
        result = check()
        self.assertEqual(result['state'], 'CONFIG_MATCH')
        self.assertFalse(result['runtime_verified'])
        self.assertNotIn('SYNTHETIC_PRIVATE_API_KEY', json.dumps(result))
        self.assertEqual(config.read_bytes(), before)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        nexus_config = self.root / 'nexus.json'
        nexus_config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-only'}))
        output = subprocess.check_output([sys.executable, ns.__file__, 'summary', '--config', str(nexus_config),
            '--syncthing-config', str(config), '--syncthing-project-folder', 'projects'], text=True)
        self.assertEqual(json.loads(output)['syncthing_source_config']['state'], 'CONFIG_MATCH')
        self.assertNotIn('SYNTHETIC_PRIVATE_API_KEY', output)
        folder.set('type', 'sendreceive')
        write()
        self.assertEqual(check()['state'], 'ATTENTION')
        self.assertFalse(check()['project_folder_send_only'])
        folder.set('type', 'sendonly')
        folder.find('paused').text = 'true'
        write()
        self.assertTrue(check()['project_folder_paused'])
        self.assertEqual(check()['state'], 'ATTENTION')
        folder.find('paused').text = 'false'
        folder.set('path', str(self.root))
        write()
        self.assertFalse(check()['project_path_matches'])
        self.assertEqual(check()['state'], 'ATTENTION')
        folder.set('path', str(nexus))
        overlap = ns.ET.SubElement(document, 'folder', {'id': 'other', 'path': str(nexus / 'project'), 'type': 'sendreceive'})
        write()
        self.assertTrue(check()['other_receive_overlap'])
        self.assertEqual(check()['state'], 'ATTENTION')
        ns.ET.SubElement(overlap, 'paused').text = 'true'
        write()
        self.assertEqual(check()['state'], 'CONFIG_MATCH')
        self.assertEqual(ns.syncthing_source_config_status(dict(cfg, direction='send-receive'), config, 'projects')['state'], 'ATTENTION')
        ns.ET.SubElement(document, 'folder', {'id': 'projects', 'path': str(nexus), 'type': 'sendonly'})
        write()
        self.assertEqual(check()['state'], 'UNKNOWN')
        config.write_text('<!DOCTYPE x [<!ENTITY a "x">]><configuration/>')
        self.assertEqual(check()['state'], 'UNKNOWN')

    def test_receiver_prepares_opted_in_snapshots_without_touching_codex(self):
        nexus = self.root / 'MacNexus'
        nexus.mkdir()
        state = self.root / 'mac-state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'cwd': '/linux/Nexus/Bot'}}) + '\n')
        original = self.chat.read_bytes()
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'direction': 'send-receive', 'peer_roots': {'linux': '/linux/Nexus'}}
        cfg = ns.validate_settings(raw)
        # Incomplete project transfer must be retried, not published as ready.
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 0)
        destination = state / 'prepared-chats' / 'linux' / snapshot.name
        self.assertFalse(destination.exists())
        (nexus / 'Bot').mkdir()
        _, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('Подготовлено локальных копий' in note for note in notes))
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 0)
        prepared = destination / self.chat.relative_to(self.codex)
        self.assertEqual(json.loads(prepared.read_text())['payload']['cwd'], str(nexus / 'Bot'))
        self.assertEqual(self.chat.read_bytes(), original)
        self.assertEqual(ns.configured_status(cfg, self.key)['prepared_records'], 1)
        self.assertIn('не импортированы', ns.configured_status(cfg, self.key)['message'])
        config_file = self.root / 'config.json'
        config_file.write_text(json.dumps(raw))
        summary = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'summary',
                                  '--config', str(config_file)], capture_output=True, text=True)
        self.assertEqual(summary.returncode, 0, summary.stderr)
        self.assertEqual(json.loads(summary.stdout)['prepared_records'], 1)
        plan = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'plan-prepared',
                               '--config', str(config_file)], capture_output=True, text=True)
        self.assertEqual(plan.returncode, 0, plan.stderr)
        report = json.loads(plan.stdout)
        self.assertFalse(report['applied'])
        self.assertTrue(report['prepared_files_verified'])
        self.assertEqual(report['snapshots'][0]['chats'][0]['relation'], 'DIVERGED')
        self.assertEqual(self.chat.read_bytes(), original)
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'cwd': str(nexus / 'Bot')}}) + '\n')
        report = ns.plan_prepared(cfg, self.key)
        self.assertEqual(report['snapshots'][0]['chats'][0]['relation'], 'EQUIVALENT_AFTER_PATH_MAPPING')
        marker = destination / 'prepared.json'
        intact = prepared.read_bytes()
        prepared.write_bytes(intact + b'{"unexpected":true}\n')
        report = ns.plan_prepared(cfg, self.key)
        self.assertFalse(report['prepared_files_verified'])
        self.assertEqual(report['snapshots'][0]['state'], 'UNAVAILABLE')
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 0)
        prepared.write_bytes(intact)
        self.assertTrue(ns.plan_prepared(cfg, self.key)['prepared_files_verified'])
        prepared.unlink()
        self.assertEqual(ns.plan_prepared(cfg, self.key)['snapshots'][0]['state'], 'UNAVAILABLE')
        prepared.write_bytes(intact)
        extra = destination / 'unexpected.txt'
        extra.write_text('unexpected')
        self.assertEqual(ns.plan_prepared(cfg, self.key)['snapshots'][0]['state'], 'UNAVAILABLE')
        extra.unlink()
        v2_marker = marker.read_text()
        legacy = json.loads(v2_marker)
        legacy['format'] = 'nexus-prepared-v1'
        legacy.pop('files')
        legacy['authentication'] = ns._manifest_tag(legacy, self.key)
        marker.write_text(json.dumps(legacy))
        self.assertEqual(ns.plan_prepared(cfg, self.key)['snapshots'][0]['state'], 'UNVERIFIED')
        marker.write_text(v2_marker)
        altered = json.loads(marker.read_text())
        altered['source'] = 'tampered'
        marker.write_text(json.dumps(altered))
        self.assertEqual(ns.configured_status(cfg, self.key)['prepared_records'], 0)
        self.assertEqual(ns.plan_prepared(cfg, self.key)['snapshots'], [])
        cfg['direction'] = 'send-only'
        with mock.patch.object(ns, 'verify', side_effect=AssertionError('receive prohibited')):
            self.assertEqual(ns.prepare_received_once(cfg, self.key), 0)

    def test_supervisor_config_cannot_be_inside_shared_folder(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key)}
        config = self.shared / 'config.json'
        config.write_text(json.dumps(raw))
        with self.assertRaises(ns.SyncError):
            ns.load_settings(config)

    def test_mixed_snapshot_prepares_supported_chats_and_reports_skips(self):
        source = self.root / 'source'
        sessions = source / 'sessions'
        sessions.mkdir(parents=True)
        normal = sessions / 'rollout-normal.jsonl'
        paginated = sessions / 'rollout-paginated.jsonl'
        normal.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': 'normal', 'cwd': '/linux/Nexus/Bot'}}) + '\n')
        paginated.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': 'paginated', 'cwd': '/linux/Nexus/Bot', 'history_mode': 'paginated'}}) + '\n')
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': 'normal', 'history_mode': 'paginated'}}) + '\n')
        local_before = self.chat.read_bytes()
        nexus = self.root / 'Nexus'
        (nexus / 'Bot').mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'direction': 'send-receive',
            'peer_roots': {'linux': '/linux/Nexus'}})
        snapshot = ns.snapshot(source, self.shared, 'linux', self.key)
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 1)
        dest = state / 'prepared-chats' / 'linux' / snapshot.name
        self.assertTrue((dest / 'sessions' / normal.name).is_file())
        self.assertFalse((dest / 'sessions' / paginated.name).exists())
        report = ns.plan_prepared(cfg, self.key)
        self.assertTrue(report['prepared_files_verified'])
        self.assertEqual(report['snapshots'][0]['state'], 'PARTIAL')
        self.assertEqual(report['snapshots'][0]['skipped'][0]['reason'], 'PAGINATED')
        self.assertEqual(report['snapshots'][0]['chats'][0]['relation'], 'LOCAL_UNSUPPORTED')
        self.assertEqual(ns.configured_status(cfg, self.key)['prepared_skipped'], 1)
        self.assertEqual(self.chat.read_bytes(), local_before)
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 0)
        normal.unlink()
        second = ns.snapshot(source, self.shared, 'linux', self.key)
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 1)
        reports = ns.plan_prepared(cfg, self.key)['snapshots']
        only_paginated = next(item for item in reports if item['snapshot'] == second.name)
        self.assertEqual(only_paginated['state'], 'PARTIAL')
        self.assertEqual(only_paginated['chats'], [])
        self.assertEqual(only_paginated['skipped'][0]['reason'], 'PAGINATED')
        self.assertEqual(ns.configured_status(cfg, self.key)['prepared_skipped'], 2)

    def test_partial_prepare_rejects_duplicate_id_across_formats(self):
        incoming = self.root / 'incoming'
        sessions = incoming / 'sessions'
        sessions.mkdir(parents=True)
        for name, mode in (('one', 'legacy'), ('two', 'paginated')):
            (sessions / f'rollout-{name}.jsonl').write_text(json.dumps({'type': 'session_meta',
                'payload': {'id': 'same', 'cwd': '/linux/Nexus', 'history_mode': mode}}) + '\n')
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        dest = self.root / 'prepared'
        with self.assertRaisesRegex(ns.SyncError, 'повторяющиеся'):
            ns.prepare_chats(incoming, dest, '/linux/Nexus', str(nexus), skip_unsupported=True, skipped=[])
        self.assertFalse(dest.exists())

    def test_chat_plan_rejects_symlinks_in_local_sessions(self):
        incoming = self.root / 'incoming'
        incoming_sessions = incoming / 'sessions'
        incoming_sessions.mkdir(parents=True)
        (incoming_sessions / 'rollout-mock.jsonl').write_text(
            '{"type":"session_meta","payload":{"id":"mock"}}\n')
        external = self.root / 'external'
        external.mkdir()
        (external / 'rollout-mock.jsonl').write_text(
            '{"type":"session_meta","payload":{"id":"mock"}}\n')
        link = self.sessions / 'linked'
        try:
            link.symlink_to(external, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest('создание символических ссылок недоступно')
        with self.assertRaises(ns.SyncError):
            ns.plan_chats(incoming, self.codex)
        link.unlink()
        link.symlink_to(external / 'rollout-mock.jsonl')
        with self.assertRaises(ns.SyncError):
            ns.plan_chats(incoming, self.codex)

    def test_snapshot_refuses_silently_skipped_history_symlinks(self):
        external = self.root / 'external-histories'
        external.mkdir()
        (external / 'rollout-mock.jsonl').write_text(
            '{"type":"session_meta","payload":{"id":"mock"}}\n')
        link = self.codex / 'archived_sessions'
        try:
            link.symlink_to(external, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest('создание символических ссылок недоступно')
        with self.assertRaises(ns.SyncError):
            ns.source_fingerprint(self.codex, key=self.key)
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.shared, 'linux', self.key)
        link.unlink()
        link = self.sessions / 'linked'
        link.symlink_to(external, target_is_directory=True)
        with self.assertRaises(ns.SyncError):
            ns.source_fingerprint(self.codex, key=self.key)
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.shared, 'linux', self.key)
        root = self.shared / 'nexus-sync' / 'snapshots' / 'linux'
        self.assertFalse(root.exists() and any(root.iterdir()))

    def test_preview_merge_keeps_conflicts_and_does_not_modify_codex(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        project.mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        source = self.root / 'source'
        incoming_sessions = source / 'sessions' / '2026' / '01' / '01'
        incoming_sessions.mkdir(parents=True)
        def history(identifier, cwd, *messages):
            rows = [{'type': 'session_meta', 'payload': {'id': identifier, 'cwd': cwd}}]
            rows.extend({'type': 'event_msg', 'payload': {'message': message}} for message in messages)
            return ''.join(json.dumps(row) + '\n' for row in rows)
        local_extend = self.sessions / 'rollout-extend.jsonl'
        local_extend.write_text(history('extend', str(project), 'base'))
        self.chat.write_text(history('conflict', str(project), 'local side'))
        (incoming_sessions / 'rollout-extend.jsonl').write_text(history('extend', '/linux/Nexus/Bot', 'base', 'later'))
        (incoming_sessions / 'rollout-conflict.jsonl').write_text(history('conflict', '/linux/Nexus/Bot', 'remote side'))
        (incoming_sessions / 'rollout-new.jsonl').write_text(history('new', '/linux/Nexus/Bot', 'new message'))
        (incoming_sessions / self.chat.name).write_text(history('collision', '/linux/Nexus/Bot', 'other chat'))
        originals = {path: path.read_bytes() for path in (local_extend, self.chat)}
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'direction': 'send-receive', 'peer_roots': {'linux': '/linux/Nexus'}}
        cfg = ns.validate_settings(raw)
        snapshot = ns.snapshot(source, self.shared, 'linux', self.key)
        self.assertEqual(ns.prepare_received_once(cfg, self.key), 1)
        config = self.root / 'config.json'
        config.write_text(json.dumps(raw))
        destination = self.root / 'preview'
        result = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'preview-merge',
            '--config', str(config), '--source', 'linux', '--snapshot', snapshot.name,
            '--destination', str(destination)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertFalse(report['applied'])
        self.assertTrue(report['partial'])
        self.assertEqual(report['unresolved_chats'], 2)
        actions = {item['chat_hash']: item['action'] for item in report['actions']}
        key_for = lambda value: hashlib.sha256(value.encode()).hexdigest()
        self.assertEqual(actions[key_for('extend')], 'EXTEND_COPY')
        self.assertEqual(actions[key_for('conflict')], 'KEEP_LOCAL')
        self.assertEqual(actions[key_for('new')], 'ADD_COPY')
        self.assertEqual(actions[key_for('collision')], 'PATH_CONFLICT')
        preview_sessions = destination / 'sessions' / '2026' / '01' / '01'
        self.assertIn('later', (preview_sessions / local_extend.name).read_text())
        self.assertIn('local side', (preview_sessions / self.chat.name).read_text())
        self.assertIn('new message', (preview_sessions / 'rollout-new.jsonl').read_text())
        self.assertEqual({path: path.read_bytes() for path in originals}, originals)
        self.assertNotIn('local side', result.stdout)
        self.assertNotIn('remote side', (destination / 'preview.json').read_text())
        with self.assertRaises(ns.SyncError):
            ns.preview_merge(cfg, self.key, 'linux', snapshot.name, self.codex / 'unsafe')
        staged = state / 'prepared-chats' / 'linux' / snapshot.name / 'sessions' / '2026' / '01' / '01' / 'rollout-new.jsonl'
        staged.write_text('tampered\n')
        with self.assertRaises(ns.SyncError):
            ns.preview_merge(cfg, self.key, 'linux', snapshot.name, self.root / 'tampered-preview')
        self.assertFalse((self.root / 'tampered-preview').exists())

    def test_init_config_accepts_explicit_peers_and_test_secrets(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        secret = nexus / '.env'
        secret.write_text('FAKE_VALUE=not-for-config')
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        config = self.root / 'receiver.json'
        script = str(Path(ns.__file__).resolve())
        base = [sys.executable, script, 'init-config', '--nexus', str(nexus), '--codex-home', str(self.codex),
                '--shared', str(self.shared), '--state-dir', str(state), '--key-file', str(key), '--machine', 'mac']
        result = subprocess.run(base + ['--output', str(config), '--direction', 'send-receive',
            '--peer-root', 'linux', '/linux/Nexus', '--peer-root', 'win', r'D:\Nexus',
            '--secret-file', str(secret), '--discover-secrets', '--git-repo', str(nexus / 'Bot'), '--discover-git'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(config.read_text())
        self.assertEqual(data['peer_roots'], {'linux': '/linux/Nexus', 'win': r'D:\Nexus'})
        self.assertEqual(data['secret_files'], [str(secret)])
        self.assertTrue(data['discover_secrets'])
        self.assertEqual(data['git_repos'], [str(nexus / 'Bot')])
        self.assertTrue(data['git_discovery'])
        self.assertNotIn('not-for-config', result.stdout + result.stderr + config.read_text())
        self.assertEqual(ns.load_settings(config)['direction'], 'send-receive')
        discovery = subprocess.run([sys.executable, script, 'discover-secrets', '--config', str(config)],
                                   capture_output=True, text=True)
        self.assertEqual(discovery.returncode, 0, discovery.stderr)
        self.assertEqual(json.loads(discovery.stdout), ['.env'])
        self.assertNotIn('not-for-config', discovery.stdout + discovery.stderr)
        duplicate = self.root / 'duplicate.json'
        result = subprocess.run(base + ['--output', str(duplicate), '--peer-root', 'linux', '/one',
                                        '--peer-root', 'linux', '/two'], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(duplicate.exists())
        default = self.root / 'source.json'
        result = subprocess.run(base + ['--output', str(default)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(default.read_text())['direction'], 'send-only')
        self.assertFalse(json.loads(default.read_text())['git_discovery'])

    def test_startup_templates_preserve_paths_and_reject_control_characters(self):
        name, unit = ns.render_startup('linux', 'linux', '/usr/bin/python3', '/tmp/a b/nexus.py', '/tmp/cfg%$x.json')
        self.assertEqual(name, 'nexus-sync-linux.service')
        self.assertIn('ExecStart=:"/usr/bin/python3" "/tmp/a b/nexus.py" "run" "--config" "/tmp/cfg%%$x.json"', unit.decode())
        self.assertIn('KillMode=mixed', unit.decode())
        self.assertIn('SendSIGKILL=no', unit.decode())
        name, raw = ns.render_startup('macos', 'mac', '/usr/bin/python3', '/tmp/a b/nexus.py', '/tmp/cfg.json')
        self.assertEqual(name, 'nexus-sync-mac.plist')
        self.assertEqual(plistlib.loads(raw)['ProgramArguments'], ['/usr/bin/python3', '/tmp/a b/nexus.py', 'run', '--config', '/tmp/cfg.json'])
        name, raw = ns.render_startup('windows', 'win', r'C:\Python 3\python.exe', r'D:\Apps & Tools\nexus.py', r'D:\Configs\nexus.json')
        self.assertEqual(name, 'nexus-sync-win.xml')
        tree = ET.fromstring(raw)
        space = {'t': 'http://schemas.microsoft.com/windows/2004/02/mit/task'}
        self.assertEqual(tree.find('.//t:Command', space).text, r'C:\Python 3\python.exe')
        self.assertEqual(tree.find('.//t:Arguments', space).text, subprocess.list2cmdline([r'D:\Apps & Tools\nexus.py', 'run', '--config', r'D:\Configs\nexus.json']))
        with self.assertRaises(ns.SyncError):
            ns.render_startup('linux', 'linux', '/usr/bin/python3', '/tmp/script\nExecStart=bad', '/tmp/config')
        with self.assertRaises(ns.SyncError):
            ns.render_startup('windows', 'win', 'relative.exe', r'D:\script.py', r'D:\config.json')

    def test_generate_startup_creates_only_an_inactive_template(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        config = self.root / 'config.json'
        config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)}))
        out = self.root / 'startup-template'
        result = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'generate-startup',
                                 '--config', str(config), '--destination', str(out)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('не установлен', result.stdout)
        self.assertEqual(len(list(out.iterdir())), 1)
        self.assertNotIn(self.key, next(out.iterdir()).read_bytes())
        self.assertFalse((self.shared / 'nexus-sync').exists())
        with self.assertRaises(ns.SyncError):
            ns.generate_startup(config, nexus / 'unsafe')
        with self.assertRaises(ns.SyncError):
            ns.generate_startup(config, out)

    def test_startup_template_supports_standalone_binary(self):
        _, unit = ns.render_startup('linux', 'test', '/opt/CoSyncX/cosyncx', None, '/tmp/config.json')
        self.assertIn('ExecStart=:"/opt/CoSyncX/cosyncx" "run" "--config" "/tmp/config.json"', unit.decode())
        _, xml = ns.render_startup('windows', 'test', r'C:\Apps\cosyncx.exe', None, r'C:\config.json')
        root = ET.fromstring(xml)
        namespace = {'t': 'http://schemas.microsoft.com/windows/2004/02/mit/task'}
        self.assertEqual(root.find('.//t:Command', namespace).text, r'C:\Apps\cosyncx.exe')
        self.assertEqual(root.find('.//t:Arguments', namespace).text,
                         subprocess.list2cmdline(['run', '--config', r'C:\config.json']))

    def test_watch_retries_after_a_partial_write_then_stops_without_duplicates(self):
        self.chat.write_bytes(b'{"partial":')
        ticks = 0
        def advance(_interval):
            nonlocal ticks
            ticks += 1
            if ticks == 1:
                self.chat.write_bytes(b'{"type":"session_meta","payload":{"id":"test"}}\n')
            if ticks == 3:
                raise KeyboardInterrupt
        with mock.patch.object(ns.time, 'sleep', side_effect=advance):
            with self.assertRaises(KeyboardInterrupt):
                ns.watch(self.codex, self.shared, 'linux', 10, self.key)
        root = self.shared / 'nexus-sync' / 'snapshots' / 'linux'
        self.assertEqual(len([p for p in root.iterdir() if p.is_dir()]), 1)

    def test_watch_rechecks_omitted_chat_without_duplicate_partial_snapshots(self):
        active = self.sessions / 'rollout-active.jsonl'
        active.write_bytes(b'{"partial":')
        ticks = 0
        def tick(_interval):
            nonlocal ticks
            ticks += 1
            if ticks == 2:
                active.write_text('{"type":"session_meta","payload":{"id":"active"}}\n')
            if ticks == 3:
                raise KeyboardInterrupt
        with mock.patch.object(ns, 'source_fingerprint', return_value='same-metadata'), \
             mock.patch.object(ns.time, 'sleep', side_effect=tick):
            with self.assertRaises(KeyboardInterrupt):
                ns.watch(self.codex, self.shared, 'linux', 10, self.key)
        root = self.shared / 'nexus-sync' / 'snapshots' / 'linux'
        counts = sorted(ns.verify(path, self.key)['omitted_files'] for path in root.iterdir())
        self.assertEqual(counts, [0, 1])

    def test_manifest_is_authenticated_against_omission(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        path = result / 'manifest.json'
        manifest = json.loads(path.read_text(encoding='utf-8'))
        manifest['machine'] = 'other'
        path.write_text(json.dumps(manifest), encoding='utf-8')
        with self.assertRaises(ns.SyncError):
            ns.verify(result, self.key)

    def test_cli_two_device_handoff_through_simulated_shared_folder(self):
        key = self.root / 'private.key'
        key.write_bytes(self.key)
        script = Path(ns.__file__).resolve()
        publish = subprocess.run([sys.executable, str(script), 'snapshot', '--codex-home', str(self.codex), '--shared', str(self.shared), '--machine', 'linux', '--key-file', str(key)], capture_output=True, text=True)
        self.assertEqual(publish.returncode, 0, publish.stderr)
        source = next((self.shared / 'nexus-sync' / 'snapshots' / 'linux').iterdir())
        received = self.root / 'receiver' / source.name
        received.parent.mkdir()
        shutil.copytree(source, received)
        check = subprocess.run([sys.executable, str(script), 'verify', str(received), '--key-file', str(key)], capture_output=True, text=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        dest = self.root / 'recovered'
        result = subprocess.run([sys.executable, str(script), 'extract', str(received), '--destination', str(dest), '--key-file', str(key)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((dest / self.chat.relative_to(self.codex)).read_bytes(), self.chat.read_bytes())

    def test_offline_relay_prepares_a_chat_without_importing_it(self):
        # Three temporary directories model separate Syncthing replicas, with
        # explicit copies standing in for network delivery and an offline Mac.
        relay = self.root / 'vps'
        mac = self.root / 'mac-share'
        relay.mkdir()
        mac.mkdir()
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'cwd': '/linux/Nexus/Bot'}}) + '\n' +
            json.dumps({'type': 'response_item', 'payload': {'text': 'fake private text'}}) + '\n')
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        relative = snapshot.relative_to(self.shared)
        shutil.copytree(snapshot, relay / relative)
        self.assertNotIn(b'fake private text', b''.join(p.read_bytes() for p in (relay / relative).rglob('*') if p.is_file()))
        shutil.copytree(relay / relative, mac / relative)
        self.assertEqual(ns.receive_once(mac, 'mac', self.key), 1)
        receipt_root = mac / 'nexus-sync' / 'receipts'
        shutil.copytree(receipt_root, relay / 'nexus-sync' / 'receipts')
        shutil.copytree(relay / 'nexus-sync' / 'receipts', self.shared / 'nexus-sync' / 'receipts')
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['received_by'], ['mac'])
        quarantine = self.root / 'quarantine'
        ns.extract(mac / relative, quarantine, self.key)
        target_project = self.root / 'mac-nexus' / 'Bot'
        target_project.mkdir(parents=True)
        prepared = self.root / 'prepared'
        self.assertEqual(ns.prepare_chats(quarantine, prepared, '/linux/Nexus', str(target_project.parent)), 1)
        records = (prepared / self.chat.relative_to(self.codex)).read_text().splitlines()
        self.assertEqual(json.loads(records[0])['payload']['cwd'], str(target_project))
        self.assertEqual(json.loads(records[1])['payload']['text'], 'fake private text')
        self.assertFalse((target_project.parent / '.codex').exists())

    @unittest.skipIf(os.name == 'nt', 'requires names unsupported by Windows')
    def test_inventory_reports_cross_platform_names_without_reading_file_contents(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'project'
        project.mkdir(parents=True)
        (project / 'report.txt').write_text('mock private contents')
        if (project / 'REPORT.txt').exists():
            self.skipTest('filesystem does not support distinct case-sensitive names')
        (project / 'REPORT.txt').write_text('different mock contents')
        (project / 'é.txt').write_text('mock')
        distinct_unicode = not (project / 'e\u0301.txt').exists()
        if distinct_unicode:
            (project / 'e\u0301.txt').write_text('mock')
        (project / 'con.txt').write_text('mock')
        (project / 'bad:name').write_text('mock')
        (project / 'trailing.').mkdir()
        ignored = project / 'node_modules'
        ignored.mkdir()
        (ignored / 'bad:name').write_text('ignored')
        (project / 'link').symlink_to(project / 'report.txt')
        current = ns.inventory(nexus)
        issues = current['portability']
        self.assertTrue(any(item['kind'] == 'name_collision' and
                            set(item['paths']) == {'project/report.txt', 'project/REPORT.txt'} for item in issues))
        if distinct_unicode:
            self.assertTrue(any(item['kind'] == 'name_collision' and
                                set(item['paths']) == {'project/é.txt', 'project/e\u0301.txt'} for item in issues))
        self.assertTrue(any(item['kind'] == 'windows_name' and item['paths'] == ['project/con.txt'] for item in issues))
        self.assertTrue(any(item['kind'] == 'windows_name' and item['paths'] == ['project/bad:name'] for item in issues))
        self.assertTrue(any(item['kind'] == 'windows_name' and item['paths'] == ['project/trailing.'] for item in issues))
        self.assertTrue(any(item['kind'] == 'symlink' and item['paths'] == ['project/link'] for item in issues))
        self.assertNotIn('node_modules', json.dumps(issues))
        self.assertNotIn('private contents', json.dumps(current))
        state = self.root / 'state'
        state.mkdir()
        report = self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        self.assertIn('Проверить переносимость путей', report.read_text())
        self.assertIn('project/bad:name', report.read_text())

    def test_python_package_changes_are_observed_without_executing_environment(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'project'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('example\n')
        site = project / '.venv' / 'lib' / 'python3.12' / 'site-packages'
        site.mkdir(parents=True)
        (site / 'example-1.0.dist-info').mkdir()
        before = ns.inventory(nexus)
        state = self.root / 'state'
        state.mkdir()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=before), [])
        (site / 'example-1.0.dist-info').rmdir()
        (site / 'example-2.0.dist-info').mkdir()
        (site / 'new_pkg-1.1.dist-info').mkdir()
        after = ns.inventory(nexus)
        events = ns.compare(before, after)
        self.assertTrue(any('Не найден Python-пакет:' in event and 'example-1.0' in event for event in events))
        self.assertTrue(any('Обнаружен Python-пакет:' in event and 'example-2.0' in event for event in events))
        self.assertTrue(any('new_pkg-1.1' in event for event in events))
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=after), events)
        report = self.write_project_report(self.shared, state, 'linux', after, include_peers=False)
        self.assertIn('new_pkg-1.1', report.read_text())
        # An unreadable or unsupported layout is not evidence of removals.
        shutil.rmtree(site)
        missing = ns.inventory(nexus)
        self.assertFalse(any('Python-пакет:' in event for event in ns.compare(after, missing)))
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=missing), [])
        site.mkdir()
        (site / 'new_pkg-1.1.dist-info').mkdir()
        recovered = self.record_projects(nexus, self.shared, state, 'linux')
        self.assertTrue(any('Не найден Python-пакет:' in event and 'example-2.0' in event for event in recovered))
        windows_site = project / 'venv' / 'Lib' / 'site-packages'
        windows_site.mkdir(parents=True)
        (windows_site / 'windows_pkg-1.0.dist-info').mkdir()
        windows = ns.inventory(nexus)
        self.assertEqual(windows['projects']['project']['local']['python_packages']
                         ['venv/Lib/site-packages'], ['windows_pkg-1.0'])

    def test_node_package_changes_are_observed_without_running_npm(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        modules = project / 'node_modules'
        package = modules / 'mock-pkg'
        package.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        def write_metadata(folder, name, version):
            (folder / 'package.json').write_text(json.dumps({'name': name, 'version': version,
                                                             'private': 'SYNTHETIC_NOT_FOR_LOGS'}))
        write_metadata(package, 'mock-pkg', '1.0.0')
        state = self.root / 'state'
        state.mkdir()
        with mock.patch.object(ns.subprocess, 'run', side_effect=AssertionError('npm must not run')):
            before = ns.inventory(nexus)
        self.assertEqual(before['projects']['app']['local']['node_packages'], ['mock-pkg@1.0.0'])
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=before), [])
        write_metadata(package, 'mock-pkg', '2.0.0')
        scoped = modules / '@demo' / 'other'
        scoped.mkdir(parents=True)
        write_metadata(scoped, '@demo/other', '1.2.3')
        after = ns.inventory(nexus)
        changes = self.record_projects(nexus, self.shared, state, 'linux', current=after)
        self.assertIn('Обнаружен Node.js-пакет: app — mock-pkg@2.0.0', changes)
        self.assertIn('Не найден Node.js-пакет: app — mock-pkg@1.0.0', changes)
        self.assertIn('Обнаружен Node.js-пакет: app — @demo/other@1.2.3', changes)
        self.assertNotIn('SYNTHETIC_NOT_FOR_LOGS', json.dumps(after))
        encrypted = next((self.shared / 'nexus-sync' / 'project-events' / 'linux').glob('*.nxe'))
        self.assertNotIn(b'mock-pkg', encrypted.read_bytes())
        (package / 'package.json').write_text('{')
        unavailable = ns.inventory(nexus)
        self.assertIsNone(unavailable['projects']['app']['local']['node_packages'])
        self.assertTrue(any(item['reason'] == 'Состав node_modules не проверен'
                            for item in ns.project_actions(unavailable)))
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=unavailable), [])
        write_metadata(package, 'mock-pkg', '3.0.0')
        recovered = self.record_projects(nexus, self.shared, state, 'linux')
        self.assertIn('Не найден Node.js-пакет: app — mock-pkg@2.0.0', recovered)
        self.assertIn('Обнаружен Node.js-пакет: app — mock-pkg@3.0.0', recovered)

    def test_first_package_inventory_reaches_peer_without_claiming_new_install(self):
        linux, mac = self.root / 'linux', self.root / 'mac'
        source, target = linux / 'Bot', mac / 'Bot'
        node = source / 'node_modules' / 'example'
        site = source / '.venv' / 'lib' / 'python3.12' / 'site-packages'
        node.mkdir(parents=True)
        site.mkdir(parents=True)
        target.mkdir(parents=True)
        for project in (source, target):
            (project / 'package.json').write_text('{}')
            (project / 'requirements.txt').write_text('mock\n')
        (node / 'package.json').write_text(json.dumps({'name': 'example', 'version': '1.0.0',
                                                       'private': 'SYNTHETIC_PRIVATE'}))
        (site / 'python_mock-2.0.dist-info').mkdir()
        source_state, target_state = self.root / 'linux-state', self.root / 'mac-state'
        source_state.mkdir()
        target_state.mkdir()
        self.assertEqual(self.record_projects(linux, self.shared, source_state, 'linux'), [])
        events = self.project_log(self.shared)
        self.assertEqual(len(events), 1)
        self.assertIn('Начально наблюдался Node.js-пакет: Bot — example@1.0.0', events[0]['changes'])
        self.assertIn('Начально наблюдался Python-пакет: Bot (.venv/lib/python3.12/site-packages) — python_mock-2.0', events[0]['changes'])
        encoded = next((self.shared / 'nexus-sync' / 'project-events' / 'linux').glob('*.nxe')).read_bytes()
        self.assertNotIn(b'example', encoded)
        self.assertNotIn('SYNTHETIC_PRIVATE', json.dumps(events))
        self.assertEqual(self.record_projects(linux, self.shared, source_state, 'linux'), [])
        self.assertEqual(len(self.project_log(self.shared)), 1)
        report = self.write_project_report(self.shared, target_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('example@1.0.0 — отмечен на linux; локальный node_modules отсутствует', report.read_text())
        self.assertIn('python_mock-2.0 — отмечен на linux; локальное Python-окружение отсутствует', report.read_text())

    def test_old_package_baseline_migrates_once_without_erasing_event_history(self):
        linux, mac = self.root / 'linux', self.root / 'mac'
        source, target = linux / 'Bot', mac / 'Bot'
        package = source / 'node_modules' / 'example'
        package.mkdir(parents=True)
        target.mkdir(parents=True)
        for project in (source, target):
            (project / 'package.json').write_text('{}')
        metadata = package / 'package.json'
        metadata.write_text(json.dumps({'name': 'example', 'version': '1.0.0'}))
        source_state, target_state = self.root / 'linux-state', self.root / 'mac-state'
        source_state.mkdir()
        target_state.mkdir()
        self.record_projects(linux, self.shared, source_state, 'linux')
        baseline = source_state / 'projects-linux.json'
        old = json.loads(baseline.read_text())
        old.pop('package_baseline_version')
        baseline.write_text(json.dumps(old))
        metadata.write_text(json.dumps({'name': 'example', 'version': '2.0.0'}))
        self.record_projects(linux, self.shared, source_state, 'linux')
        events = self.project_log(self.shared)
        self.assertEqual(len(events), 2)
        self.assertIn('Обновлён исходный список Node.js: Bot', events[-1]['changes'])
        self.assertIn('Начально наблюдался Node.js-пакет: Bot — example@2.0.0', events[-1]['changes'])
        self.assertEqual(json.loads(baseline.read_text())['package_baseline_version'], 1)
        self.assertEqual(self.record_projects(linux, self.shared, source_state, 'linux'), [])
        self.assertEqual(len(self.project_log(self.shared)), 2)
        report = self.write_project_report(self.shared, target_state, 'mac', ns.inventory(mac), include_peers=True)
        section = report.read_text().split('## Пакеты с других устройств для проверки', 1)[1].split('## Журнал наблюдений', 1)[0]
        self.assertIn('example@2.0.0', section)
        self.assertNotIn('example@1.0.0', section)

    def test_old_baseline_migration_waits_for_unavailable_package_scan(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        package = project / 'node_modules' / 'example'
        package.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        metadata = package / 'package.json'
        metadata.write_text(json.dumps({'name': 'example', 'version': '1.0.0'}))
        state = self.root / 'state'
        state.mkdir()
        self.record_projects(nexus, self.shared, state, 'linux')
        baseline = state / 'projects-linux.json'
        old = json.loads(baseline.read_text())
        old.pop('package_baseline_version')
        old.pop('package_baseline_pending')
        baseline.write_text(json.dumps(old))
        metadata.write_text('{')
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        self.assertEqual(json.loads(baseline.read_text())['package_baseline_pending']['node'], ['Bot'])
        self.assertEqual(len(self.project_log(self.shared)), 1)
        metadata.write_text(json.dumps({'name': 'example', 'version': '1.0.0'}))
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        events = self.project_log(self.shared)
        self.assertEqual(len(events), 2)
        self.assertIn('Обновлён исходный список Node.js: Bot', events[-1]['changes'])
        self.assertIn('Начально наблюдался Node.js-пакет: Bot — example@1.0.0', events[-1]['changes'])
        self.assertEqual(json.loads(baseline.read_text())['package_baseline_pending']['node'], [])
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        self.assertEqual(len(self.project_log(self.shared)), 2)

    def test_old_python_baseline_waits_for_unavailable_site(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        site = project / '.venv' / 'lib' / 'python3.12' / 'site-packages'
        site.mkdir(parents=True)
        (project / 'requirements.txt').write_text('mock\n')
        (site / 'example-1.0.dist-info').mkdir()
        state = self.root / 'state'
        state.mkdir()
        self.record_projects(nexus, self.shared, state, 'linux')
        baseline = state / 'projects-linux.json'
        old = json.loads(baseline.read_text())
        old.pop('package_baseline_version')
        old.pop('package_baseline_pending')
        baseline.write_text(json.dumps(old))
        shutil.rmtree(site)
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        self.assertEqual(json.loads(baseline.read_text())['package_baseline_pending']['python'],
                         [['Bot', '.venv/lib/python3.12/site-packages']])
        (site / 'example-1.0.dist-info').mkdir(parents=True)
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        self.assertIn('Обновлён исходный список Python: Bot (.venv/lib/python3.12/site-packages)',
                      self.project_log(self.shared)[-1]['changes'])
        self.assertEqual(json.loads(baseline.read_text())['package_baseline_pending']['python'], [])

    def test_report_compares_peer_node_events_with_local_inventory(self):
        linux, mac = self.root / 'linux', self.root / 'mac'
        source, target = linux / 'Bot', mac / 'Bot'
        source_modules = source / 'node_modules'
        source_modules.mkdir(parents=True)
        target.mkdir(parents=True)
        for project in (source, target):
            (project / 'package.json').write_text('{}')
        linux_state, mac_state = self.root / 'linux-state', self.root / 'mac-state'
        linux_state.mkdir()
        mac_state.mkdir()
        self.assertEqual(self.record_projects(linux, self.shared, linux_state, 'linux'), [])
        package = source_modules / 'example'
        package.mkdir()
        (package / 'package.json').write_text(json.dumps({'name': 'example', 'version': '1.0.0'}))
        self.record_projects(linux, self.shared, linux_state, 'linux')
        report = self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('example@1.0.0 — отмечен на linux; локальный node_modules отсутствует', report.read_text())
        local_package = target / 'node_modules' / 'example'
        local_package.mkdir(parents=True)
        (local_package / 'package.json').write_text(json.dumps({'name': 'example', 'version': '1.0.0'}))
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertNotIn('## Пакеты с других устройств для проверки', report.read_text())
        (local_package / 'package.json').write_text('{')
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('Состав node_modules не проверен', report.read_text())
        self.assertIn('example@1.0.0 — отмечен на linux; локальный состав не проверен', report.read_text())
        shutil.rmtree(package)
        self.record_projects(linux, self.shared, linux_state, 'linux')
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertNotIn('## Пакеты с других устройств для проверки', report.read_text())

    def test_report_compares_peer_python_events_across_environment_layouts(self):
        linux, mac = self.root / 'linux', self.root / 'mac'
        source, target = linux / 'Bot', mac / 'Bot'
        site = source / '.venv' / 'lib' / 'python3.12' / 'site-packages'
        site.mkdir(parents=True)
        target.mkdir(parents=True)
        for project in (source, target):
            (project / 'requirements.txt').write_text('example\n')
        linux_state, mac_state = self.root / 'linux-state', self.root / 'mac-state'
        linux_state.mkdir()
        mac_state.mkdir()
        self.assertEqual(self.record_projects(linux, self.shared, linux_state, 'linux'), [])
        (site / 'example-1.0.dist-info').mkdir()
        second_site = source / 'venv' / 'Lib' / 'site-packages'
        second_site.mkdir(parents=True)
        # A newly discovered site is baseline-only; record a subsequent addition.
        self.record_projects(linux, self.shared, linux_state, 'linux')
        (second_site / 'example-1.0.dist-info').mkdir()
        self.record_projects(linux, self.shared, linux_state, 'linux')
        report = self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('example-1.0 — отмечен на linux; локальное Python-окружение отсутствует', report.read_text())
        local_site = target / 'venv' / 'Lib' / 'site-packages'
        local_site.mkdir(parents=True)
        (local_site / 'example-1.0.dist-info').mkdir()
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertNotIn('## Пакеты с других устройств для проверки', report.read_text())
        shutil.rmtree(local_site)
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('example-1.0 — отмечен на linux; локальный состав не проверен', report.read_text())
        (site / 'example-1.0.dist-info').rmdir()
        self.record_projects(linux, self.shared, linux_state, 'linux')
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertIn('example-1.0 — отмечен на linux; локальный состав не проверен', report.read_text())
        (second_site / 'example-1.0.dist-info').rmdir()
        self.record_projects(linux, self.shared, linux_state, 'linux')
        self.write_project_report(self.shared, mac_state, 'mac', ns.inventory(mac), include_peers=True)
        self.assertNotIn('## Пакеты с других устройств для проверки', report.read_text())

    @unittest.skipIf(os.name == 'nt', 'requires symlink privileges')
    def test_node_pnpm_links_are_observed_without_following_external_links(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        modules = project / 'node_modules'
        store = modules / '.pnpm'
        real = store / 'mock-pkg@1.0.0' / 'node_modules' / 'mock-pkg'
        real.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        (real / 'package.json').write_text(json.dumps({'name': 'mock-pkg', 'version': '1.0.0'}))
        (modules / 'mock-pkg').symlink_to(os.path.relpath(real, modules), target_is_directory=True)
        scoped = store / '@demo+other@1.2.3' / 'node_modules' / '@demo' / 'other'
        scoped.mkdir(parents=True)
        (scoped / 'package.json').write_text(json.dumps({'name': '@demo/other', 'version': '1.2.3'}))
        (modules / '@demo').mkdir()
        (modules / '@demo' / 'other').symlink_to(os.path.relpath(scoped, modules / '@demo'), target_is_directory=True)
        before = ns.inventory(nexus)
        self.assertEqual(before['projects']['app']['local']['node_packages'], ['@demo/other@1.2.3', 'mock-pkg@1.0.0'])
        updated = store / 'mock-pkg@2.0.0' / 'node_modules' / 'mock-pkg'
        updated.mkdir(parents=True)
        (updated / 'package.json').write_text(json.dumps({'name': 'mock-pkg', 'version': '2.0.0'}))
        (modules / 'mock-pkg').unlink()
        (modules / 'mock-pkg').symlink_to(os.path.relpath(updated, modules), target_is_directory=True)
        after = ns.inventory(nexus)
        self.assertIn('Обнаружен Node.js-пакет: app — mock-pkg@2.0.0', ns.compare(before, after))
        self.assertIn('Не найден Node.js-пакет: app — mock-pkg@1.0.0', ns.compare(before, after))
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'package.json').write_text(json.dumps({'name': 'external', 'version': '1.0.0'}))
        (modules / 'external').symlink_to(outside, target_is_directory=True)
        unavailable = ns.inventory(nexus)
        self.assertIsNone(unavailable['projects']['app']['local']['node_packages'])
        self.assertFalse(any('Node.js-пакет:' in row for row in ns.compare(before, unavailable)))
        (modules / 'external').unlink()
        (modules / 'broken').symlink_to(store / 'missing', target_is_directory=True)
        self.assertIsNone(ns.inventory(nexus)['projects']['app']['local']['node_packages'])

    def test_project_baseline_and_dependency_change(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('a==1\n')
        (project / '.venv').mkdir()
        (project / '.venv' / 'package.json').write_text('{}')
        first = ns.inventory(project.parent)
        self.assertEqual(set(first['projects']), {'app'})
        self.assertEqual(ns.project_actions(first), [])
        (project / 'requirements.txt').write_text('a==2\n')
        self.assertEqual(ns.compare(first, ns.inventory(project.parent)), ['Изменён манифест: app/requirements.txt'])

    def test_report_distinguishes_added_and_removed_lock_files(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        (nexus / 'package.json').write_text('{}')
        (nexus / 'node_modules').mkdir()
        state = self.root / 'state'
        state.mkdir()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        lock = nexus / 'package-lock.json'
        lock.write_text('{}')
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'),
                         ['Добавлен манифест: ./package-lock.json'])
        lock.unlink()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'),
                         ['Удалён манифест: ./package-lock.json'])
        report = self.write_project_report(self.shared, state, 'linux', ns.inventory(nexus), include_peers=False)
        self.assertIn('.: добавлен package-lock.json', report.read_text())
        self.assertIn('.: удалён package-lock.json', report.read_text())

    def test_rust_target_is_reported_only_for_a_cargo_project(self):
        nexus = self.root / 'Nexus'
        rust, other = nexus / 'rust-app', nexus / 'other'
        rust.mkdir(parents=True)
        (other / 'target').mkdir(parents=True)
        (rust / 'Cargo.toml').write_text('[package]\nname = "mock"\nversion = "0.1.0"\n')
        (other / 'requirements.txt').write_text('synthetic==1\n')
        before = ns.inventory(nexus)
        state = self.root / 'state-rust'
        state.mkdir()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux'), [])
        (rust / 'target').mkdir()
        (rust / 'target' / 'mock-artifact').write_text('synthetic')
        after = ns.inventory(nexus)
        self.assertIn('Локальное Rust target: rust-app — обнаружено', ns.compare(before, after))
        self.assertIn('Локальное Rust target: rust-app — обнаружено',
                      self.record_projects(nexus, self.shared, state, 'linux'))
        self.assertIn('Локальное Rust target: rust-app — обнаружено', self.project_log(self.shared)[-1]['changes'])
        report = self.write_project_report(self.shared, state, 'linux', after, include_peers=False).read_text()
        self.assertIn('rust-app', report)
        self.assertIn('Rust target', report)
        self.assertNotIn('other/target', report)
        self.assertTrue((other / 'target').is_dir())

    @unittest.skipIf(os.name == 'nt', 'requires symlink privileges')
    def test_rust_target_symlink_does_not_claim_artifacts_removed(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'rust-app'
        (project / 'target').mkdir(parents=True)
        (project / 'Cargo.toml').write_text('[package]\nname = "mock"\nversion = "0.1.0"\n')
        before = ns.inventory(nexus)
        (project / 'target').rmdir()
        (project / 'target').symlink_to(self.root / 'unavailable')
        after = ns.inventory(nexus)
        self.assertFalse(any('Rust target' in change for change in ns.compare(before, after)))
        self.assertTrue(any(action['reason'] == 'Rust target является ссылкой' for action in ns.project_actions(after)))

    def test_inventory_finds_projects_in_generic_build_named_directories(self):
        nexus = self.root / 'Nexus'
        for name in ('build', 'dist', 'target'):
            folder = nexus / name / 'project'
            folder.mkdir(parents=True)
            (folder / 'requirements.txt').write_text('example==1\n')
        self.assertEqual(set(ns.inventory(nexus)['projects']), {
            'build/project', 'dist/project', 'target/project'})

    def test_detects_python_and_node_package_managers_conservatively(self):
        nexus = self.root / 'Nexus'
        python = nexus / 'python-app'
        node = nexus / 'node-app'
        python.mkdir(parents=True)
        node.mkdir()
        (python / 'Pipfile').write_text('[packages]\n')
        (python / 'poetry.lock').write_text('mock lock\n')
        (node / 'package.json').write_text('{}')
        (node / 'package-lock.json').write_text('{}')
        (node / 'bun.lock').write_text('mock lock')
        before = ns.inventory(nexus)
        self.assertIn('poetry.lock', before['projects']['python-app']['files'])
        actions = {action['project']: action for action in ns.project_actions(before)}
        self.assertIn('Python-окружение', actions['python-app']['reason'])
        self.assertIsNone(actions['node-app']['command'])
        (python / 'poetry.lock').write_text('updated lock\n')
        self.assertIn('Изменён манифест: python-app/poetry.lock', ns.compare(before, ns.inventory(nexus)))
        (node / 'bun.lock').unlink()
        current = ns.inventory(nexus)
        current['executables'].update({'node': True, 'npm': True})
        actions = {action['project']: action for action in ns.project_actions(current)}
        self.assertEqual(actions['node-app']['command'], 'npm ci')

    def test_project_report_detects_missing_toolchains_without_running_them(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('mock==1\n')
        (project / 'package.json').write_text('{}')
        (project / 'pom.xml').write_text('<project/>')
        (project / '.venv').mkdir()
        (project / 'node_modules').mkdir()
        def lookup(installed):
            return lambda command: '/mock/bin/' + command if command in installed else None
        with mock.patch.object(ns.shutil, 'which', side_effect=lookup({'python3'})), \
             mock.patch.object(ns.subprocess, 'run', side_effect=AssertionError('tool must not be run')):
            first = ns.inventory(nexus)
            actions = ns.project_actions(first)
        self.assertEqual(first['toolchains'], {'Python': True, 'Node.js': False, 'Java/JDK': False})
        self.assertTrue(any('Node.js не найден в PATH' in action['reason'] for action in actions))
        self.assertTrue(any('Java/JDK не найден в PATH' in action['reason'] for action in actions))
        with mock.patch.object(ns.shutil, 'which', side_effect=lookup({'python3', 'node', 'javac'})):
            second = ns.inventory(nexus)
        self.assertEqual(ns.compare(first, second), ['Инструмент Java/JDK: найден в PATH', 'Инструмент Node.js: найден в PATH'])
        old_format = dict(first)
        old_format.pop('toolchains')
        self.assertEqual(ns.compare(old_format, second), [])
        state = self.root / 'state'
        state.mkdir()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=first), [])
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=second),
                         ['Инструмент Java/JDK: найден в PATH', 'Инструмент Node.js: найден в PATH'])
        self.assertNotIn('/mock/bin', json.dumps(self.project_log(self.shared), ensure_ascii=False))

    def test_env_example_is_reported_without_reading_contents(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('example==1\n')
        before = ns.inventory(project.parent)
        example = project / '.env.example'
        example.write_text('FAKE_TOKEN=should-not-enter-report\n')
        with mock.patch.object(ns, '_digest', wraps=ns._digest) as digest:
            after = ns.inventory(project.parent)
        self.assertNotIn(example, [call.args[0] for call in digest.call_args_list])
        self.assertIn('Образец .env: app — обнаружен', ns.compare(before, after))
        actions = ns.project_actions(after)
        self.assertTrue(any('Образец .env' in action['reason'] for action in actions))
        self.assertNotIn('should-not-enter-report', json.dumps(actions, ensure_ascii=False))

    def test_uppercase_env_template_is_reported_without_reading_contents(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('example==1\n')
        before = ns.inventory(project.parent)
        example = project / '.ENV.EXAMPLE'
        example.write_text('FAKE_TOKEN=not-in-report\n')
        with mock.patch.object(ns, '_digest', wraps=ns._digest) as digest:
            after = ns.inventory(project.parent)
        self.assertNotIn(example, [call.args[0] for call in digest.call_args_list])
        self.assertIn('Образец .env: app — обнаружен', ns.compare(before, after))
        self.assertTrue(any('Образец .env' in action['reason'] for action in ns.project_actions(after)))
        self.assertNotIn('not-in-report', json.dumps(after, ensure_ascii=False))

    def test_example_ignore_warnings_do_not_assume_excluded_directories_are_disposable(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('mock==1\n')
        (project / '.venv').mkdir()
        (project / '.git').mkdir()
        (project / 'node_modules').mkdir()
        misleading = nexus / 'venv'
        misleading.mkdir()
        (misleading / 'requirements.txt').write_text('MOCK_PRIVATE_CONTENT=not-for-report\n')
        inventory = ns.inventory(nexus)
        candidates = {item['path']: item for item in inventory['sample_exclusions']}
        self.assertEqual(set(candidates), {'Bot/.venv', 'Bot/.git', 'Bot/node_modules', 'venv'})
        self.assertTrue(candidates['venv']['project_marker'])
        self.assertEqual(candidates['Bot/.git']['kind'], 'git')
        self.assertNotIn('venv', inventory['projects'])
        self.assertNotIn('MOCK_PRIVATE_CONTENT', json.dumps(inventory, ensure_ascii=False))
        sample = set(Path(ns.__file__).with_name('stignore.example').read_text().splitlines())
        for item in candidates:
            self.assertIn(Path(item).name, sample)
        state = self.root / 'state'
        state.mkdir()
        report = ns.write_project_report(self.shared, state, 'linux', inventory, key=self.key, include_peers=False)
        text = report.read_text()
        self.assertIn('действующие правила Syncthing не проверены', text)
        self.assertIn('"venv": есть признак проекта', text)
        self.assertIn('"Bot/.venv": локальное окружение или данные', text)
        self.assertIn('"Bot/.git": Git исключён образцом', text)
        self.assertNotIn('MOCK_PRIVATE_CONTENT', text)

    def test_logs_local_environment_presence_without_claiming_package_contents(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'pyproject.toml').write_text('[project]\nname = "example"\n')
        first = ns.inventory(project.parent)
        self.assertEqual(len(ns.project_actions(first)), 1)
        (project / '.venv').mkdir()
        second = ns.inventory(project.parent)
        self.assertEqual(ns.compare(first, second), ['Локальное Python-окружение: app — обнаружено'])
        self.assertEqual(ns.compare(second, first), ['Локальное Python-окружение: app — не найдено'])
        self.assertEqual(ns.project_actions(second)[0]['reason'], 'Каталог Python-окружения пуст')
        state = self.root / 'state'
        state.mkdir()
        self.assertEqual(self.record_projects(project.parent, self.shared, state, 'linux'), [])
        (project / '.venv').rmdir()
        self.assertEqual(self.record_projects(project.parent, self.shared, state, 'linux'),
                         ['Локальное Python-окружение: app — не найдено'])
        self.assertEqual(self.project_log(self.shared)[0]['changes'],
                         ['Локальное Python-окружение: app — не найдено'])

    def test_python_restore_suggestions_follow_platform_and_declared_manager(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('synthetic-package==1\n')
        current = ns.inventory(project.parent)
        current['executables'].update({'python3': True, 'py': True})
        linux = ns.project_actions(current, platform='linux')[0]
        windows = ns.project_actions(current, platform='win32')[0]
        mac = ns.project_actions(current, platform='darwin')[0]
        self.assertEqual(linux['steps'], ['python3 -m venv .venv',
                                          '.venv/bin/python -m pip install -r requirements.txt'])
        self.assertEqual(mac['steps'], linux['steps'])
        self.assertEqual(windows['steps'], ['py -3 -m venv .venv',
                                           r'.venv\Scripts\python.exe -m pip install -r requirements.txt'])
        self.assertEqual(ns.project_actions(current, platform='unknown')[0]['steps'], [])
        (project / 'poetry.lock').write_text('synthetic lock\n')
        self.assertEqual(ns.project_actions(ns.inventory(project.parent), platform='linux')[0]['steps'], [])
        self.assertFalse((project / '.venv').exists())

    def test_project_report_uses_target_platform_for_restore_suggestions(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('synthetic-package==1\n')
        current = ns.inventory(project.parent)
        current['executables'].update({'python3': True, 'py': True})
        state = self.root / 'state'
        state.mkdir()
        report = self.write_project_report(self.shared, state, 'windows', current,
                                           include_peers=False, platform='win32')
        content = report.read_text(encoding='utf-8')
        self.assertIn('py -3 -m venv .venv', content)
        self.assertIn(r'.venv\Scripts\python.exe -m pip install -r requirements.txt', content)
        self.assertNotIn('.venv/bin/python', content)

    def test_restore_suggestions_require_corresponding_executable(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('mock==1\n')
        (project / 'package.json').write_text('{}')
        (project / 'package-lock.json').write_text('{}')
        def lookup(installed):
            return lambda name: '/mock/bin/' + name if name in installed else None
        with mock.patch.object(ns.shutil, 'which', side_effect=lookup({'node'})):
            current = ns.inventory(nexus)
        previous = current
        actions = ns.project_actions(current, platform='linux')
        python = next(action for action in actions if 'Python-окружение' in action['reason'])
        node = next(action for action in actions if action['reason'] == 'node_modules не найден')
        self.assertEqual(python['steps'], [])
        self.assertIsNone(node['command'])
        state = self.root / 'report-state'
        state.mkdir()
        self.assertEqual(self.record_projects(nexus, self.shared, state, 'linux', current=previous), [])
        report = self.write_project_report(self.shared, state, 'linux', current, include_peers=False).read_text()
        self.assertNotIn('`npm ci`', report)
        self.assertNotIn('`python3 -m venv', report)
        self.assertIn('Python не найден в PATH', report)
        self.assertIn('npm не найден в PATH', report)
        with mock.patch.object(ns.shutil, 'which', side_effect=lookup({'python3', 'node', 'npm'})):
            current = ns.inventory(nexus)
        self.assertIn('Команда npm: найдена в PATH', ns.compare(previous, current))
        self.assertIn('Команда python3: найдена в PATH', ns.compare(previous, current))
        changes = self.record_projects(nexus, self.shared, state, 'linux', current=current)
        self.assertIn('Команда npm: найдена в PATH', changes)
        events = self.project_log(self.shared)
        self.assertTrue(any('Команда npm: найдена в PATH' in event['changes'] for event in events))
        self.assertNotIn('/mock/bin', json.dumps(events))
        actions = ns.project_actions(current, platform='linux')
        self.assertTrue(next(action for action in actions if 'Python-окружение' in action['reason'])['steps'])
        self.assertEqual(next(action for action in actions if action['reason'] == 'node_modules не найден')['command'], 'npm ci')

    def test_python_wheel_platform_observations_follow_encrypted_changes(self):
        source = self.root / 'source' / 'app'
        target = self.root / 'target' / 'app'
        wheel = source / '.venv' / 'lib' / 'python3.12' / 'site-packages' / 'mock_native-1.0.dist-info' / 'WHEEL'
        wheel.parent.mkdir(parents=True)
        target.mkdir(parents=True)
        for project in (source, target):
            (project / 'requirements.txt').write_text('mock-native==1.0\n')
        wheel.write_text('Wheel-Version: 1.0\nTag: cp312-cp312-manylinux_2_17_x86_64\n')
        source_state, target_state = self.root / 'source-state', self.root / 'target-state'
        source_state.mkdir()
        target_state.mkdir()
        self.record_projects(source.parent, self.shared, source_state, 'linux')
        event = next((self.shared / 'nexus-sync' / 'project-events' / 'linux').glob('*.nxe'))
        self.assertNotIn(b'manylinux', event.read_bytes())
        report = self.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                           include_peers=True, platform='win32')
        line = next(row for row in report.read_text().splitlines() if 'mock_native-1.0 — отмечен' in row)
        self.assertIn('manylinux_2_17_x86_64', line)
        self.assertIn('сборка для другой ОС', line)
        wheel.write_text('Wheel-Version: 1.0\nTag: py3-none-any\n')
        self.record_projects(source.parent, self.shared, source_state, 'linux')
        self.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                  include_peers=True, platform='win32')
        line = next(row for row in report.read_text().splitlines() if 'mock_native-1.0 — отмечен' in row)
        self.assertIn('py3-none-any', line)
        self.assertNotIn('сборка для другой ОС', line)
        wheel.write_text('Wheel-Version: 1.0\nGenerator: SYNTHETIC_PRIVATE\n'
                         'Tag: cp312-cp312-manylinux_2_17_x86_64\nTag: cp312-cp312-win_amd64\n')
        self.record_projects(source.parent, self.shared, source_state, 'linux')
        self.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                  include_peers=True, platform='win32')
        line = next(row for row in report.read_text().splitlines() if 'mock_native-1.0 — отмечен' in row)
        self.assertIn('win_amd64', line)
        self.assertNotIn('сборка для другой ОС', line)
        self.assertNotIn('SYNTHETIC_PRIVATE', json.dumps(self.project_log(self.shared)))
        wheel.write_text('Wheel-Version: 1.0\nTag: cp312-cp312-manylinux_2_17_x86_64.win_amd64\n')
        self.record_projects(source.parent, self.shared, source_state, 'linux')
        self.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                  include_peers=True, platform='win32')
        line = next(row for row in report.read_text().splitlines() if 'mock_native-1.0 — отмечен' in row)
        self.assertNotIn('сборка для другой ОС', line)
        wheel.unlink()
        self.record_projects(source.parent, self.shared, source_state, 'linux')
        self.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                  include_peers=True, platform='win32')
        line = next(row for row in report.read_text().splitlines() if 'mock_native-1.0 — отмечен' in row)
        self.assertNotIn('wheel:', line)
        self.assertNotIn('сборка для другой ОС', line)

    def test_node_platform_restrictions_follow_encrypted_observations(self):
        source = self.root / 'source' / 'app'
        source.mkdir(parents=True)
        (source / 'package.json').write_text('{"name":"app"}')
        package = source / 'node_modules' / 'mock-native'
        package.mkdir(parents=True)
        metadata = package / 'package.json'
        metadata.write_text(json.dumps({'name': 'mock-native', 'version': '1.0.0',
                                        'os': ['linux'], 'cpu': ['x64']}))
        mixed = source / 'node_modules' / 'mock-mixed'
        mixed.mkdir()
        (mixed / 'package.json').write_text(json.dumps({'name': 'mock-mixed', 'version': '1.0.0',
                                                       'os': ['linux', '!win32']}))
        source_state = self.root / 'source-state'
        target_state = self.root / 'target-state'
        source_state.mkdir()
        target_state.mkdir()
        target = self.root / 'target' / 'app'
        target.mkdir(parents=True)
        (target / 'package.json').write_text('{"name":"app"}')
        key = b'v' * 32
        ns.record_projects(source.parent, self.shared, source_state, 'linux', key=key)
        events = ns.project_log(self.shared, key)
        self.assertTrue(any('Ограничения Node.js-пакета' in item and 'mock-native' in item
                            for event in events for item in event['changes']))
        event_path = next((self.shared / 'nexus-sync' / 'project-events' / 'linux').glob('*.nxe'))
        self.assertNotIn(b'mock-native', event_path.read_bytes())
        report = ns.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                         key=key, include_peers=True, platform='win32')
        lines = report.read_text().splitlines()
        native = next(line for line in lines if 'mock-native@1.0.0 — отмечен' in line)
        self.assertIn('OS в метаданных: linux', native)
        self.assertIn('CPU в метаданных: x64', native)
        self.assertIn('может исключать текущую ОС', native)
        mixed_line = next(line for line in lines if 'mock-mixed@1.0.0 — отмечен' in line)
        self.assertNotIn('может исключать текущую ОС', mixed_line)
        metadata.write_text(json.dumps({'name': 'mock-native', 'version': '1.0.0', 'os': 'linux'}))
        incomplete = ns.inventory(source.parent)
        self.assertIsNone(incomplete['projects']['app']['local']['node_constraints'])
        changes = ns.record_projects(source.parent, self.shared, source_state, 'linux', key=key, current=incomplete)
        self.assertFalse(any('Ограничения Node.js-пакета' in item for item in changes))
        metadata.write_text(json.dumps({'name': 'mock-native', 'version': '1.0.0', 'os': ['win32']}))
        ns.record_projects(source.parent, self.shared, source_state, 'linux', key=key)
        text = ns.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                       key=key, include_peers=True, platform='win32').read_text()
        latest = next(line for line in text.splitlines() if 'mock-native@1.0.0 — отмечен' in line)
        self.assertIn('OS в метаданных: win32', latest)
        self.assertNotIn('может исключать текущую ОС', latest)
        metadata.write_text(json.dumps({'name': 'mock-native', 'version': '1.0.0'}))
        ns.record_projects(source.parent, self.shared, source_state, 'linux', key=key)
        text = ns.write_project_report(self.shared, target_state, 'windows', ns.inventory(target.parent),
                                       key=key, include_peers=True, platform='win32').read_text()
        latest = next(line for line in text.splitlines() if 'mock-native@1.0.0 — отмечен' in line)
        self.assertNotIn('OS в метаданных', latest)

    def test_project_report_includes_manual_node_restore_suggestion(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'package.json').write_text('{"name":"mock"}')
        (project / 'package-lock.json').write_text('{}')
        state = self.root / 'state'
        state.mkdir()
        current = ns.inventory(project.parent)
        current['executables'].update({'node': True, 'npm': True})
        report = ns.write_project_report(self.shared, state, 'mock', current,
                                         key=b'm' * 32, include_peers=True)
        text = report.read_text()
        self.assertIn('Предложения для проверки вручную', text)
        self.assertIn('`npm ci`', text)
        self.assertFalse((project / 'node_modules').exists())

    def test_empty_environments_appear_in_local_report_and_disappear_when_populated(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        (project / 'requirements.txt').write_text('mock==1\n')
        (project / 'package.json').write_text('{}')
        (project / '.venv').mkdir()
        (project / 'node_modules').mkdir()
        state = self.root / 'state'
        state.mkdir()
        current = ns.inventory(nexus)
        report = self.write_project_report(self.shared, state, 'linux', current, include_peers=False)
        self.assertIn('Каталог Python-окружения пуст', report.read_text())
        self.assertIn('Каталог node_modules пуст', report.read_text())
        (project / '.venv' / 'pyvenv.cfg').write_text('mock\n')
        (project / 'node_modules' / 'mock-package').mkdir()
        reasons = {item['reason'] for item in ns.project_actions(ns.inventory(nexus))}
        self.assertNotIn('Каталог Python-окружения пуст', reasons)
        self.assertNotIn('Каталог node_modules пуст', reasons)

    def test_maps_project_paths_between_operating_systems_without_prefix_guessing(self):
        self.assertEqual(ns.map_path('/home/user/Nexus', r'D:\Nexus', '/home/user/Nexus/Bot/src'), r'D:\Nexus\Bot\src')
        self.assertEqual(ns.map_path(r'C:\Users\me\Nexus', '/Users/me/Nexus', r'c:\users\me\nexus\Bot'), '/Users/me/Nexus/Bot')
        with self.assertRaises(ns.SyncError):
            ns.map_path('/home/user/Nexus', '/Users/me/Nexus', '/home/user/Nexus-old/Bot')
        with self.assertRaises(ns.SyncError):
            ns.map_path('/home/user/Nexus', '/Users/me/Nexus', '/home/user/Nexus/../secret')
        for path in ('/home/user/Nexus/C:/outside', '/home/user/Nexus/CON/file',
                     '/home/user/Nexus/a\\b/file', '/home/user/Nexus/trailing./file',
                     '/home/user/Nexus/bad\x00name'):
            with self.subTest(path=path), self.assertRaises(ns.SyncError):
                ns.map_path('/home/user/Nexus', r'D:\Nexus', path)
        self.assertEqual(ns.map_path('/home/user/Nexus', r'D:\Nexus', '/home/user/Nexus/valid/Bot'),
                         r'D:\Nexus\valid\Bot')

    @unittest.skipIf(os.name == 'nt', 'requires case-sensitive test filesystem')
    def test_windows_chat_path_uses_unique_local_directory_case(self):
        nexus = self.root / 'Nexus'
        (nexus / 'Bot' / 'Source').mkdir(parents=True)
        incoming = self.root / 'incoming-windows'
        sessions = incoming / 'sessions'
        sessions.mkdir(parents=True)
        history = sessions / 'rollout-case.jsonl'
        old = r'C:\Users\Mock\Nexus\bot\source'
        rows = [{'type': 'session_meta', 'payload': {'id': 'mock-case', 'cwd': old}},
                {'type': 'event_msg', 'payload': {'message': old}}]
        history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        original = history.read_bytes()
        source = r'C:\Users\Mock\Nexus'
        local_rows = json.loads(json.dumps(rows))
        local_rows[0]['payload']['cwd'] = str(nexus / 'Bot' / 'Source')
        self.chat.write_text(''.join(json.dumps(row) + '\n' for row in local_rows))
        local_before = self.chat.read_bytes()
        plan = ns.plan_chats(incoming, self.codex, source, str(nexus))[0]
        self.assertEqual(plan['cwd'], 'AVAILABLE')
        self.assertEqual(plan['relation'], 'EQUIVALENT_AFTER_PATH_MAPPING')
        output = self.root / 'mapped-windows'
        self.assertEqual(ns.prepare_chats(incoming, output, source, str(nexus)), 1)
        mapped = [json.loads(line) for line in (output / 'sessions' / history.name).read_text().splitlines()]
        self.assertEqual(mapped[0]['payload']['cwd'], str(nexus / 'Bot' / 'Source'))
        self.assertEqual(mapped[1]['payload']['message'], old)
        self.assertEqual(history.read_bytes(), original)
        self.assertEqual(self.chat.read_bytes(), local_before)
        (nexus / 'bot').mkdir()
        plan = ns.plan_chats(incoming, self.codex, source, str(nexus))[0]
        self.assertEqual(plan['cwd'], 'MAPPED_AMBIGUOUS')
        self.assertEqual(plan['relation'], 'DIVERGED')
        with self.assertRaises(ns.SyncError):
            ns.prepare_chats(incoming, self.root / 'ambiguous-windows', source, str(nexus))
        self.assertFalse((self.root / 'ambiguous-windows').exists())
        (nexus / 'bot').rmdir()
        outside = self.root / 'outside'
        outside.mkdir()
        (nexus / 'escape').symlink_to(outside, target_is_directory=True)
        rows[0]['payload']['cwd'] = r'C:\Users\Mock\Nexus\escape'
        history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        self.assertEqual(ns.plan_chats(incoming, self.codex, source, str(nexus))[0]['cwd'], 'MAPPED_OUTSIDE_NEXUS')
        with self.assertRaises(ns.SyncError):
            ns.prepare_chats(incoming, self.root / 'outside-windows', source, str(nexus))
        rows[0]['payload']['cwd'] = '/linux/Nexus/bot/source'
        history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        self.assertEqual(ns.plan_chats(incoming, self.codex, '/linux/Nexus', str(nexus))[0]['cwd'], 'MAPPED_BUT_MISSING')


    def test_read_only_chat_plan_never_merges_divergent_history(self):
        incoming = self.root / 'incoming'
        incoming_folder = incoming / 'sessions'
        incoming_folder.mkdir(parents=True)
        source = incoming_folder / 'rollout-source.jsonl'
        base = b'{"type":"session_meta","payload":{"id":"test","cwd":"/linux/Nexus/Bot"}}\n'
        source.write_bytes(base + b'{"message":"remote"}\n')
        self.chat.write_bytes(base + b'{"message":"local"}\n')
        local_project = self.root / 'Nexus' / 'Bot'
        local_project.mkdir(parents=True)
        result = ns.plan_chats(incoming, self.codex, '/linux/Nexus', str(local_project.parent))
        self.assertEqual(result[0]['relation'], 'DIVERGED')
        self.assertEqual(result[0]['cwd'], 'AVAILABLE')
        self.assertEqual(self.chat.read_bytes(), base + b'{"message":"local"}\n')
        self.chat.write_bytes(base)
        self.assertEqual(ns.plan_chats(incoming, self.codex)[0]['relation'], 'INCOMING_EXTENDS_LOCAL')
        self.chat.unlink()
        self.assertEqual(ns.plan_chats(incoming, self.codex)[0]['relation'], 'NO_LOCAL_FILE')

    def test_chat_plan_checks_each_turn_directory_without_rewriting_history(self):
        incoming = self.root / 'incoming'
        sessions = incoming / 'sessions'
        sessions.mkdir(parents=True)
        history = sessions / 'rollout-mock.jsonl'
        nexus = self.root / 'Nexus'
        (nexus / 'Bot').mkdir(parents=True)
        source_root = r'C:\Mock\Nexus'
        rows = [{'type': 'session_meta', 'payload': {'id': 'test', 'cwd': r'C:\Mock\Nexus\Bot'}},
                {'type': 'turn_context', 'payload': {'cwd': r'C:\Mock\Nexus\Bot\src'}},
                {'type': 'response_item', 'payload': {'text': r'C:\Mock\Nexus\Bot\src'}}]
        def write():
            history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        write()
        before = history.read_bytes()
        result = ns.plan_chats(incoming, self.codex, source_root, str(nexus))
        self.assertEqual(result[0]['cwd'], 'MAPPED_BUT_MISSING')
        self.assertEqual(history.read_bytes(), before)
        (nexus / 'Bot' / 'src').mkdir()
        self.assertEqual(ns.plan_chats(incoming, self.codex, source_root, str(nexus))[0]['cwd'], 'AVAILABLE')
        rows[1]['payload']['cwd'] = r'C:\Elsewhere\project'
        write()
        self.assertEqual(ns.plan_chats(incoming, self.codex, source_root, str(nexus))[0]['cwd'], 'UNMAPPED')
        if os.name == 'posix':
            outside = self.root / 'outside-turn'
            outside.mkdir()
            (nexus / 'Bot' / 'external').symlink_to(outside, target_is_directory=True)
            rows[1]['payload']['cwd'] = r'C:\Mock\Nexus\Bot\external'
            write()
            self.assertEqual(ns.plan_chats(incoming, self.codex, source_root, str(nexus))[0]['cwd'], 'MAPPED_OUTSIDE_NEXUS')
        rows[1]['payload']['cwd'] = r'C:\Mock\Nexus\Bot\src'
        write()
        with history.open('ab') as out:
            out.write(b'{"partial":')
        self.assertEqual(ns.plan_chats(incoming, self.codex, source_root, str(nexus))[0]['cwd'], 'HISTORY_UNREADABLE')

    @unittest.skipIf(os.name == 'nt', 'requires symlink privileges')
    def test_chat_plan_rejects_mapped_directory_outside_nexus(self):
        incoming = self.root / 'incoming'
        sessions = incoming / 'sessions'
        sessions.mkdir(parents=True)
        (sessions / 'rollout-mock.jsonl').write_text(json.dumps({'type': 'session_meta', 'payload':
            {'id': 'test', 'cwd': '/old/Nexus/Bot'}}) + '\n')
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        outside = self.root / 'outside'
        outside.mkdir()
        (nexus / 'Bot').symlink_to(outside, target_is_directory=True)
        result = ns.plan_chats(incoming, self.codex, '/old/Nexus', str(nexus))
        self.assertEqual(result[0]['cwd'], 'MAPPED_OUTSIDE_NEXUS')
        with self.assertRaises(ns.SyncError):
            ns.prepare_chats(incoming, self.root / 'prepared', '/old/Nexus', str(nexus))
        self.assertFalse((self.root / 'prepared').exists())
        (nexus / 'Bot').unlink()
        (nexus / 'RealBot').mkdir()
        (nexus / 'Bot').symlink_to(nexus / 'RealBot', target_is_directory=True)
        self.assertEqual(ns.plan_chats(incoming, self.codex, '/old/Nexus', str(nexus))[0]['cwd'], 'AVAILABLE')
        self.assertEqual(ns.prepare_chats(incoming, self.root / 'prepared', '/old/Nexus', str(nexus)), 1)

    def test_chat_plan_flags_duplicate_session_ids(self):
        incoming = self.root / 'incoming'
        folder = incoming / 'sessions'
        folder.mkdir(parents=True)
        first = '{"type":"session_meta","payload":{"id":"same"}}\n'
        (folder / 'rollout-a.jsonl').write_text(first)
        (folder / 'rollout-b.jsonl').write_text(first)
        self.assertEqual(ns.plan_chats(incoming, self.codex)[0]['relation'], 'AMBIGUOUS')

    def test_chat_plan_compares_only_structured_paths_across_systems(self):
        incoming = self.root / 'incoming'
        folder = incoming / 'sessions'
        folder.mkdir(parents=True)
        source = folder / 'rollout-source.jsonl'
        def records(cwd):
            return [{'type': 'session_meta', 'payload': {'id': 'test', 'cwd': cwd}},
                    {'type': 'turn_context', 'payload': {'cwd': cwd}},
                    {'type': 'response_item', 'payload': {'text': 'unchanged'}}]
        def write(path, rows):
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        linux = records('/linux/Nexus/Bot')
        windows = records(r'D:\Nexus\Bot')
        write(source, linux)
        write(self.chat, windows)
        def relation():
            return ns.plan_chats(incoming, self.codex, '/linux/Nexus', r'D:\Nexus')[0]['relation']
        self.assertEqual(relation(), 'EQUIVALENT_AFTER_PATH_MAPPING')
        write(source, linux + [{'type': 'event_msg', 'payload': {'message': 'new'}}])
        self.assertEqual(relation(), 'INCOMING_EXTENDS_AFTER_PATH_MAPPING')
        with source.open('ab') as handle:
            handle.write(b'{"incomplete":')
        with self.assertRaises(ns.SyncError):
            relation()
        write(source, linux)
        write(self.chat, windows + [{'type': 'event_msg', 'payload': {'message': 'new'}}])
        self.assertEqual(relation(), 'LOCAL_EXTENDS_AFTER_PATH_MAPPING')
        windows[2]['payload']['text'] = '/linux/Nexus/Bot'
        write(self.chat, windows)
        self.assertEqual(relation(), 'DIVERGED')
        windows[0]['payload']['cwd'] = r'D:\Other\Bot'
        write(self.chat, windows)
        self.assertEqual(relation(), 'DIVERGED')

    def test_local_environments_are_not_copied_or_installed(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        (project / 'package.json').write_text('{}')
        (project / 'package-lock.json').write_text('{}')
        current = ns.inventory(project.parent)
        current['executables'].update({'node': True, 'npm': True})
        actions = ns.project_actions(current)
        self.assertEqual(actions[0]['command'], 'npm ci')
        self.assertFalse((project / 'node_modules').exists())

    def test_project_change_events_do_not_include_manifest_contents(self):
        project = self.root / 'Nexus' / 'app'
        project.mkdir(parents=True)
        req = project / 'requirements.txt'
        req.write_text('private-internal-package==1')
        state = self.root / 'local-state'
        state.mkdir()
        self.assertEqual(self.record_projects(project.parent, self.shared, state, 'linux'), [])
        req.write_text('private-internal-package==2')
        changes = self.record_projects(project.parent, self.shared, state, 'linux')
        self.assertEqual(changes, ['Изменён манифест: app/requirements.txt'])
        event = next((self.shared / 'nexus-sync' / 'project-events' / 'linux').iterdir())
        self.assertNotIn(b'private-internal-package', event.read_bytes())
        self.assertEqual(self.project_log(self.shared)[0]['changes'], changes)
        self.assertEqual(self.record_projects(project.parent, self.shared, state, 'linux'), [])

    def test_project_events_are_encrypted_and_tampering_is_detected(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'private-project'
        project.mkdir(parents=True)
        manifest = project / 'requirements.txt'
        manifest.write_text('one')
        state = self.root / 'state'
        state.mkdir()
        self.record_projects(nexus, self.shared, state, 'linux')
        manifest.write_text('two')
        baseline = state / 'projects-linux.json'
        baseline_before = baseline.read_bytes()
        with self.assertRaises(ValueError):
            ns.record_projects(nexus, self.shared, state, 'linux', key=b'bad')
        self.assertEqual(baseline.read_bytes(), baseline_before)
        changes = self.record_projects(nexus, self.shared, state, 'linux')
        directory = self.shared / 'nexus-sync' / 'project-events' / 'linux'
        event = next(directory.glob('*.nxe'))
        self.assertNotIn(b'private-project', event.read_bytes())
        self.assertEqual(self.project_log(self.shared)[0]['changes'], changes)
        with self.assertRaises(ns.SyncError):
            ns.project_log(self.shared, b'b' * 32)
        original = event.read_bytes()
        event.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        with self.assertRaises(ns.SyncError):
            self.project_log(self.shared)
        event.write_bytes(original)
        legacy = directory / 'legacy.json'
        legacy.write_text(json.dumps({'format': 'nexus-project-event-v1', 'machine': 'linux',
                                      'created': '2026-01-01', 'changes': ['old']}))
        self.assertEqual(len(self.project_log(self.shared)), 1)
        self.assertEqual(len(self.project_log(self.shared, include_legacy=True)), 2)

    def test_project_event_cli_uses_key(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'app'
        project.mkdir(parents=True)
        manifest = project / 'requirements.txt'
        manifest.write_text('one')
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        script = str(Path(ns.__file__))
        record = [sys.executable, script, 'projects-record', '--nexus', str(nexus),
                  '--shared', str(self.shared), '--state-dir', str(state), '--machine', 'linux',
                  '--key-file', str(key)]
        subprocess.run(record, check=True, capture_output=True)
        manifest.write_text('two')
        subprocess.run(record, check=True, capture_output=True)
        log = subprocess.run([sys.executable, script, 'projects-log', '--shared', str(self.shared),
                              '--key-file', str(key)], check=True, capture_output=True, text=True)
        self.assertIn('app/requirements.txt', log.stdout)

    def test_secret_bundle_encrypts_and_recovers_without_overwrite(self):
        secret = self.root / '.env'
        secret.write_bytes(b'PASSWORD=do-not-leak')
        bundle = ns.secrets_pack([secret], self.shared, 'linux', self.key)
        self.assertNotIn(b'do-not-leak', bundle.read_bytes())
        target = self.root / 'secrets-recovered'
        self.assertEqual(ns.secrets_extract(bundle, target, self.key, self.shared), 1)
        self.assertEqual((target / '.env').read_bytes(), secret.read_bytes())
        with self.assertRaises(FileExistsError):
            ns.secrets_extract(bundle, target, self.key, self.shared)
        with self.assertRaises(ns.SyncError):
            ns.secrets_extract(bundle, self.root / 'wrong-key', b'b' * 32, self.shared)
        with self.assertRaises(ns.SyncError):
            ns.secrets_extract(bundle, self.shared / 'unsafe', self.key, self.shared)

    def test_secret_quarantine_integrity_includes_excluded_and_empty_directories(self):
        root = self.root / 'quarantine'
        root.mkdir()
        (root / '.env').write_text('mock')
        original = ns._secret_tree_tag(root, self.key)
        (root / '.venv').mkdir()
        with_empty_directory = ns._secret_tree_tag(root, self.key)
        self.assertNotEqual(original, with_empty_directory)
        (root / '.venv' / 'hidden').write_text('mock')
        self.assertNotEqual(with_empty_directory, ns._secret_tree_tag(root, self.key))
        if os.name != 'nt':
            (root / 'linked').symlink_to(root / '.env')
            with self.assertRaises(ns.SyncError):
                ns._secret_tree_tag(root, self.key)

    def test_secret_plan_compares_without_writing_or_exposing_contents(self):
        sender = self.root / 'sender'
        incoming = sender / 'app'
        incoming.mkdir(parents=True)
        for name in ('same.env', 'conflict.env', 'missing.env', 'ambiguous.env', 'linked.env'):
            (incoming / name).write_text('MOCK_PRIVATE_VALUE')
        package = ns.secrets_pack(list(incoming.iterdir()), self.shared, 'linux', self.key, root=sender)
        nexus = self.root / 'Nexus'
        target = nexus / 'app'
        target.mkdir(parents=True)
        (target / 'same.env').write_text('MOCK_PRIVATE_VALUE')
        (target / 'conflict.env').write_text('MOCK_OTHER_VALUE')
        (target / 'AMBIGUOUS.env').write_text('mock')
        if os.name != 'nt':
            (target / 'linked.env').symlink_to(target / 'same.env')
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'direction': 'send-receive', 'peer_roots': {'linux': '/old/Nexus'},
               'prepare_secrets': True}
        cfg = ns.validate_settings(raw)
        self.assertEqual(ns.prepare_received_secrets(cfg, self.key), 1)
        plan = ns.plan_prepared_secrets(cfg, self.key, 'linux', package.stem)
        states = {item['path']: item['state'] for item in plan['files']}
        self.assertEqual(states['app/same.env'], 'SAME')
        self.assertEqual(states['app/conflict.env'], 'CONFLICT')
        self.assertEqual(states['app/missing.env'], 'MISSING')
        self.assertEqual(states['app/ambiguous.env'], 'AMBIGUOUS_NAME')
        if os.name != 'nt':
            self.assertEqual(states['app/linked.env'], 'UNSAFE_PATH')
        self.assertFalse(plan['applied'])
        self.assertNotIn('MOCK_PRIVATE_VALUE', json.dumps(plan))
        self.assertEqual((target / 'conflict.env').read_text(), 'MOCK_OTHER_VALUE')
        self.assertFalse((target / 'missing.env').exists())
        config = self.root / 'plan-config.json'
        config.write_text(json.dumps(raw))
        cli = subprocess.run([sys.executable, str(Path(ns.__file__)), 'plan-secrets', '--config', str(config),
                              '--source', 'linux', '--package', package.stem], capture_output=True, text=True)
        self.assertEqual(cli.returncode, 0, cli.stderr)
        self.assertNotIn('MOCK_PRIVATE_VALUE', cli.stdout + cli.stderr)
        self.assertEqual(json.loads(cli.stdout)['files'], plan['files'])
        with self.assertRaises(ns.SyncError):
            ns.plan_prepared_secrets(dict(cfg, direction='send-only'), self.key, 'linux', package.stem)
        encrypted = package.read_bytes()
        package.write_bytes(encrypted[:-1] + bytes([encrypted[-1] ^ 1]))
        with self.assertRaises(ns.SyncError):
            ns.plan_prepared_secrets(cfg, self.key, 'linux', package.stem)
        package.write_bytes(encrypted)
        quarantine = state / 'prepared-secrets' / 'linux' / package.stem / 'files'
        (quarantine / '.venv').mkdir()
        (quarantine / '.venv' / 'unexpected').write_text('mock')
        with self.assertRaises(ns.SyncError):
            ns.plan_prepared_secrets(cfg, self.key, 'linux', package.stem)

    def test_opt_in_automatic_secret_quarantine_without_project_writes(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        sender = self.root / 'sender'
        (sender / 'app').mkdir(parents=True)
        secret = sender / 'app' / '.env'
        secret.write_bytes(b'MOCK_SECRET=one')
        package = ns.secrets_pack([secret], self.shared, 'linux', self.key, root=sender)
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'mac', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'direction': 'send-receive', 'peer_roots': {'linux': '/old/Nexus'}}
        cfg = ns.validate_settings(raw)
        self.assertEqual(ns.prepare_received_secrets(cfg, self.key), 0)
        raw['prepare_secrets'] = True
        cfg = ns.validate_settings(raw)
        send_only = dict(cfg, direction='send-only')
        self.assertEqual(ns.prepare_received_secrets(send_only, self.key), 0)
        _, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('Подготовлено пакетов выбранных файлов: 1' in note for note in notes))
        self.assertNotIn('MOCK_SECRET', '\n'.join(notes))
        prepared = state / 'prepared-secrets' / 'linux' / package.stem
        self.assertEqual((prepared / 'files' / 'app' / '.env').read_bytes(), secret.read_bytes())
        self.assertFalse((nexus / 'app').exists())
        self.assertNotIn('MOCK_SECRET', (prepared / 'prepared.json').read_text())
        status = ns.configured_status(cfg, self.key)
        self.assertEqual(status['prepared_secret_records'], 1)
        self.assertEqual(status['prepared_secret_invalid'], 0)
        self.assertIn('содержимое не проверено', status['message'])
        self.assertEqual(ns.prepare_received_secrets(cfg, self.key), 0)
        flat = ns.secrets_pack([secret], self.shared, 'linux', self.key)
        self.assertEqual(ns.prepare_received_secrets(cfg, self.key), 0)
        self.assertFalse((state / 'prepared-secrets' / 'linux' / flat.stem).exists())
        (prepared / 'files' / 'app' / '.env').write_bytes(b'modified')
        secret.write_bytes(b'MOCK_SECRET=two')
        later = ns.secrets_pack([secret], self.shared, 'linux', self.key, root=sender)
        with self.assertRaises(ns.SyncError):
            ns.prepare_received_secrets(cfg, self.key)
        self.assertEqual((state / 'prepared-secrets' / 'linux' / later.stem / 'files' / 'app' / '.env').read_bytes(), b'MOCK_SECRET=two')
        (prepared / 'prepared.json').write_text('{}')
        status = ns.configured_status(cfg, self.key)
        self.assertEqual(status['prepared_secret_records'], 1)
        self.assertEqual(status['prepared_secret_invalid'], 1)
        self.assertNotIn('MOCK_SECRET', json.dumps(status, ensure_ascii=False))

    def test_opt_in_secret_discovery_publishes_new_matching_files(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        project.mkdir(parents=True)
        selected = project / '.env'
        selected.write_text('MOCK_PRIVATE=first')
        (project / '.env.example').write_text('MOCK_EXAMPLE=first')
        named_credentials = ('.npmrc', '.pypirc', '.netrc', '.git-credentials', '.pgpass', '.htpasswd')
        for name in named_credentials:
            (project / name).write_text('MOCK_NAMED_CREDENTIAL')
        (project / 'requirements.txt').write_text('example==1\n')
        excluded = project / 'node_modules'
        excluded.mkdir()
        (excluded / '.env').write_text('NOT_SELECTED=mock')
        key = self.root / 'key'
        key.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        report = self.write_project_report(self.shared, state, 'linux', ns.inventory(nexus), include_peers=False)
        self.assertNotIn('MOCK_NAMED_CREDENTIAL', report.read_text())
        self.assertNotIn('MOCK_PRIVATE', report.read_text())
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'secret_files': [str(selected)]}
        cfg = ns.validate_settings(raw)
        self.assertFalse(cfg['discover_secrets'])
        raw['discover_secrets'] = True
        cfg = ns.validate_settings(raw)
        self.assertEqual({path.relative_to(nexus).as_posix() for path in
                          ns.discover_secret_files(nexus, (self.shared, self.codex))},
                         {'Bot/.env', 'Bot/.env.example', *(f'Bot/{name}' for name in named_credentials)})
        self.assertTrue(ns.publish_secrets_once(cfg, self.key))
        self.assertFalse(ns.publish_secrets_once(cfg, self.key))
        added = project / 'mock.pem'
        added.write_text('MOCK_PRIVATE=second')
        self.assertTrue(ns.publish_secrets_once(cfg, self.key))
        folder = self.shared / 'nexus-sync' / 'secrets' / 'linux'
        package = folder / json.loads((state / 'last-secrets-linux.json').read_text())['package']
        self.assertNotIn(b'MOCK_PRIVATE', package.read_bytes())
        quarantine = self.root / 'discovered-quarantine'
        self.assertNotIn(b'MOCK_NAMED_CREDENTIAL', package.read_bytes())
        self.assertEqual(ns.secrets_extract(package, quarantine, self.key, self.shared), 3 + len(named_credentials))
        for name in named_credentials:
            self.assertEqual((quarantine / 'Bot' / name).read_text(), 'MOCK_NAMED_CREDENTIAL')
        self.assertEqual((quarantine / 'Bot' / 'mock.pem').read_text(), 'MOCK_PRIVATE=second')
        self.assertFalse((quarantine / 'Bot' / 'node_modules').exists())
        raw['discover_secrets'] = 'yes'
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)

    def test_secret_discovery_retries_when_file_list_changes(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        first, second = nexus / '.env', nexus / 'new.key'
        first.write_text('mock one')
        second.write_text('mock two')
        key = self.root / 'key'
        key.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'discover_secrets': True})
        with mock.patch.object(ns, 'discover_secret_files', side_effect=[[first], [first, second]]):
            with self.assertRaises(ns.SyncError):
                ns.publish_secrets_once(cfg, self.key)
        self.assertFalse((state / 'last-secrets-linux.json').exists())
        self.assertTrue(ns.publish_secrets_once(cfg, self.key))
        if os.name != 'nt':
            (nexus / 'bad.pem').symlink_to(first)
            with self.assertRaises(ns.SyncError):
                ns.discover_secret_files(nexus, (self.shared, self.codex))

    def test_watcher_publishes_only_explicit_secrets_and_retries_damage(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        secret = nexus / '.env'
        secret.write_bytes(b'FAKE_PRIVATE=first-value')
        other_project = nexus / 'Bot'
        other_project.mkdir()
        other_secret = other_project / '.env'
        other_secret.write_bytes(b'OTHER_PRIVATE=mock-value')
        key = self.root / 'key'
        key.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        raw = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
               'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
               'key_file': str(key), 'secret_files': [str(secret), str(other_secret)]}
        cfg = ns.validate_settings(raw)
        previous, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('пакет выбранных файлов' in note for note in notes))
        folder = self.shared / 'nexus-sync' / 'secrets' / 'linux'
        packages = list(folder.glob('*.nxs'))
        self.assertEqual(len(packages), 1)
        self.assertNotIn(b'first-value', packages[0].read_bytes())
        self.assertNotIn(b'Bot', packages[0].read_bytes())
        marker = (state / 'last-secrets-linux.json').read_text()
        self.assertNotIn('first-value', marker)
        self.assertNotIn('.env', marker)
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertFalse(any('пакет выбранных файлов' in note for note in notes))
        self.assertEqual(len(list(folder.glob('*.nxs'))), 1)
        secret.write_bytes(b'FAKE_PRIVATE=second-value')
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertTrue(any('пакет выбранных файлов' in note for note in notes))
        self.assertEqual(len(list(folder.glob('*.nxs'))), 2)
        current = json.loads((state / 'last-secrets-linux.json').read_text())['package']
        (folder / current).write_bytes(b'broken')
        self.assertTrue(ns.publish_secrets_once(cfg, self.key))
        self.assertEqual(len(list(folder.glob('*.nxs'))), 3)
        final_package = json.loads((state / 'last-secrets-linux.json').read_text())['package']
        restored = self.root / 'restored-secrets'
        ns.secrets_extract(folder / final_package, restored, self.key, self.shared)
        self.assertEqual((restored / '.env').read_bytes(), b'FAKE_PRIVATE=second-value')
        self.assertEqual((restored / 'Bot' / '.env').read_bytes(), b'OTHER_PRIVATE=mock-value')
        raw['secret_files'] = [str(key)]
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)
        unencrypted = self.shared / 'already-shared'
        unencrypted.write_bytes(b'fake')
        raw['secret_files'] = [str(unencrypted)]
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(raw)

    def test_missing_selected_secret_does_not_block_chat_and_retries(self):
        nexus = self.root / 'Nexus'
        project = nexus / 'Bot'
        project.mkdir(parents=True)
        secret = project / '.env'
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        config = self.root / 'config.json'
        config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'secret_files': [str(secret)]}))
        with mock.patch.object(ns, 'secret_fingerprint', side_effect=AssertionError('config must not read secrets')):
            cfg = ns.load_settings(config)
        previous, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('Пакет выбранных файлов отложен' in note for note in notes))
        self.assertTrue(any('снимок' in note for note in notes))
        self.assertNotIn(str(secret), '\n'.join(notes))
        self.assertTrue((state / 'project-changelog-linux.md').is_file())
        self.assertEqual(len(list((self.shared / 'nexus-sync' / 'snapshots' / 'linux').iterdir())), 1)
        self.assertEqual(ns.configured_status(cfg, self.key)['state'], 'snapshot_ready')
        self.assertEqual(ns.configured_status(cfg, self.key)['outgoing_packages']['secret_state'], 'pending')
        secret.write_bytes(b'FAKE_SECRET=arrived')
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertTrue(any('зашифрованный пакет' in note for note in notes))
        packages = list((self.shared / 'nexus-sync' / 'secrets' / 'linux').glob('*.nxs'))
        self.assertEqual(len(packages), 1)
        self.assertNotIn(b'FAKE_SECRET', packages[0].read_bytes())
        self.assertNotIn('arrived', '\n'.join(notes))
        outgoing = ns.configured_status(cfg, self.key)
        self.assertEqual(outgoing['outgoing_packages']['secret_state'], 'marker_verified')
        self.assertIn('содержимое и доставка не проверены', outgoing['message'])
        self.assertNotIn('FAKE_SECRET', json.dumps(outgoing, ensure_ascii=False))
        marker = state / 'last-secrets-linux.json'
        original_marker = marker.read_bytes()
        tampered = json.loads(original_marker)
        tampered['fingerprint'] = '0' * 64
        marker.write_text(json.dumps(tampered))
        self.assertEqual(ns.configured_status(cfg, self.key)['outgoing_package_invalid'], 1)
        marker.write_bytes(original_marker)
        original_package = packages[0].read_bytes()
        packages[0].write_bytes(b'broken')
        self.assertEqual(ns.configured_status(cfg, self.key)['outgoing_package_invalid'], 1)
        packages[0].write_bytes(original_package)

    def test_nested_secret_bundle_refuses_escaping_and_colliding_paths(self):
        AESGCM, _ = ns._aead()
        nonce = b'x' * 12
        for index, paths in enumerate((['../escape'], ['/absolute'], ['a/../../escape'],
                                       ['CON'], ['COM1.env'], ['file', 'file/nested'], ['A/key', 'a/key'],
                                       ['caf\u00e9/key', 'cafe\u0301/key'], ['CAF\u00c9', 'cafe\u0301/nested'])):
            payload = json.dumps({'version': 2, 'files': [{'name': name, 'data': 'ZmFrZQ=='} for name in paths]}).encode()
            encrypted = ns.SECRET_MAGIC + nonce + AESGCM(self.key).encrypt(nonce, payload, ns.SECRET_MAGIC)
            bundle = self.shared / f'bad-{index}.nxs'
            bundle.write_bytes(encrypted)
            target = self.root / f'unsafe-{index}'
            with self.assertRaises(ns.SyncError):
                ns.secrets_extract(bundle, target, self.key, self.shared)
            self.assertFalse(target.exists())

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_git_history_is_encrypted_and_bundle_can_be_read(self):
        repo = self.root / 'repo'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'file.txt').write_text('example', encoding='utf-8')
        subprocess.run(['git', '-C', str(repo), 'add', 'file.txt'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
        (repo / 'file.txt').write_text('staged text', encoding='utf-8')
        (repo / 'image.bin').write_bytes(b'\x00staged-binary\xff')
        subprocess.run(['git', '-C', str(repo), 'add', 'file.txt', 'image.bin'], check=True)
        (repo / 'file.txt').write_text('unstaged text', encoding='utf-8')
        packed = ns.git_pack(repo, self.shared, 'linux', self.key)
        self.assertNotIn(b'staged text', packed.read_bytes())
        restored = self.root / 'git-quarantine'
        ns.secrets_extract(packed, restored, self.key, self.shared)
        result = subprocess.run(['git', 'bundle', 'list-heads', str(restored / 'repository.bundle')], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.strip())
        metadata = json.loads((restored / 'repository.json').read_text())
        self.assertFalse(metadata['working_tree_included'])
        self.assertEqual(metadata['head_ref'], subprocess.check_output(
            ['git', '-C', str(repo), 'symbolic-ref', 'HEAD'], text=True).strip())
        clone = self.root / 'disposable-repo'
        subprocess.run(['git', 'clone', '-q', str(restored / 'repository.bundle'), str(clone)], check=True)
        apply = subprocess.run(['git', '-C', str(clone), 'apply', '--cached', '--binary', str(restored / 'staged.patch')], capture_output=True)
        self.assertEqual(apply.returncode, 0, apply.stderr.decode(errors='replace'))
        tracked = subprocess.check_output(['git', '-C', str(clone), 'show', ':file.txt'])
        binary = subprocess.check_output(['git', '-C', str(clone), 'show', ':image.bin'])
        self.assertEqual(tracked, b'staged text')
        self.assertEqual(binary, b'\x00staged-binary\xff')
        self.assertEqual((repo / 'file.txt').read_text(), 'unstaged text')
        self.assertEqual(ns.git_status(repo)['modified'], 1)
        real_run = subprocess.run
        def change_index_after_bundle(args, **kwargs):
            result = real_run(args, **kwargs)
            if 'bundle' in args and 'create' in args:
                (repo / 'file.txt').write_text('concurrent staged change', encoding='utf-8')
                real_run(['git', '-C', str(repo), 'add', 'file.txt'], check=True)
            return result
        with mock.patch.object(ns.subprocess, 'run', side_effect=change_index_after_bundle):
            with self.assertRaisesRegex(ns.SyncError, 'индекс изменился'):
                ns.git_pack(repo, self.shared, 'linux', self.key)
        self.assertEqual(len(list((self.shared / 'nexus-sync' / 'secrets' / 'linux').glob('*.nxs'))), 1)
        (repo / '.git' / 'index.lock').write_text('test lock')
        with self.assertRaisesRegex(ns.SyncError, 'индекс занят'):
            ns.git_pack(repo, self.shared, 'linux', self.key)

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_git_preview_restores_branch_and_index_in_separate_directory(self):
        nexus = self.root / 'Nexus'
        repo = nexus / 'app'
        repo.mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        subprocess.run(['git', '-C', str(repo), 'checkout', '-qb', 'feature/mock'], check=True)
        source = repo / 'file'
        source.write_text('base')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
        source.write_text('base second')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'second'], check=True)
        source.write_text('staged')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        source.write_text('unstaged')
        remote = 'https://mock-user:mock-secret@example.invalid/repo.git'
        subprocess.run(['git', '-C', str(repo), 'remote', 'add', 'origin', remote], check=True)
        before_remote = ns.git_fingerprint(repo, nexus, self.key)
        subprocess.run(['git', '-C', str(repo), 'remote', 'set-url', '--push', 'origin',
                        'https://example.invalid/push.git'], check=True)
        self.assertNotEqual(before_remote, ns.git_fingerprint(repo, nexus, self.key))
        subprocess.run(['git', '-C', str(repo), 'update-ref', 'refs/remotes/origin/feature/mock', 'HEAD^'], check=True)
        subprocess.run(['git', '-C', str(repo), 'branch', 'other-local', 'HEAD^'], check=True)
        before_upstream = ns.git_fingerprint(repo, nexus, self.key)
        subprocess.run(['git', '-C', str(repo), 'config', 'branch.feature/mock.remote', 'origin'], check=True)
        subprocess.run(['git', '-C', str(repo), 'config', 'branch.feature/mock.merge', 'refs/heads/feature/mock'], check=True)
        self.assertNotEqual(before_upstream, ns.git_fingerprint(repo, nexus, self.key))
        package = ns.git_pack(repo, self.shared, 'linux', self.key, nexus=nexus)
        self.assertNotIn(b'mock-secret', package.read_bytes())
        config = self.root / 'config.json'
        config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'peer_roots': {'linux': '/mock/Nexus'}}))
        secret = repo / '.env'
        secret.write_text('MOCK_PRIVATE=do-not-print')
        selected = ns.secrets_pack([secret], self.shared, 'linux', self.key, root=nexus)
        bad = package.parent / ('0' * 32 + '.nxs')
        bad.write_bytes(b'incomplete')
        listed = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'packages',
                                 '--config', str(config)], capture_output=True, text=True)
        self.assertEqual(listed.returncode, 0, listed.stderr)
        entries = {item['id']: item for item in json.loads(listed.stdout)}
        self.assertEqual(entries[package.stem]['type'], 'git')
        self.assertEqual(entries[package.stem]['project'], 'app')
        self.assertEqual(entries[selected.stem]['type'], 'selected-files')
        self.assertEqual(entries[bad.stem]['type'], 'unverified')
        self.assertNotIn('do-not-print', listed.stdout)
        self.assertNotIn('staged', listed.stdout)
        self.assertNotIn('mock-secret', listed.stdout + listed.stderr)
        self.assertTrue(all(item['type'] == 'unverified' for item in
                            ns.list_encrypted_packages(ns.load_settings(config), b'z' * 32)))
        preview = self.root / 'git-preview'
        result = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'git-preview', str(package),
                                 '--config', str(config), '--destination', str(preview)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('mock-secret', result.stdout + result.stderr)
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'remote', 'get-url', 'origin'],
                                                 text=True).strip(), remote)
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'remote', 'get-url', '--push', 'origin'],
                                                 text=True).strip(), 'https://example.invalid/push.git')
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'symbolic-ref', '--short', 'HEAD'], text=True).strip(), 'feature/mock')
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'rev-parse', '--symbolic-full-name', '@{upstream}'],
                                                 text=True).strip(), 'refs/remotes/origin/feature/mock')
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'rev-parse', '@{upstream}']),
                         subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD^']))
        self.assertNotEqual(subprocess.run(['git', '-C', str(preview), 'show-ref', '--verify', '--quiet',
                                            'refs/remotes/origin/other-local']).returncode, 0)
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'rev-parse', 'refs/heads/other-local']),
                         subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'refs/heads/other-local']))
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'show', ':file']), b'staged')
        self.assertEqual((preview / 'file').read_text(), 'base second')
        self.assertEqual(source.read_text(), 'unstaged')
        with self.assertRaises(ns.SyncError):
            ns.git_preview(package, nexus / 'forbidden', ns.load_settings(config), self.key)

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_git_bundle_preserves_detached_head_without_branch(self):
        repo = self.root / 'detached-repo'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'file').write_text('first')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        commit = ['git', '-C', str(repo), '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm']
        subprocess.run(commit + ['first'], check=True)
        subprocess.run(['git', '-C', str(repo), 'checkout', '-q', '--detach'], check=True)
        (repo / 'file').write_text('detached commit')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(commit + ['detached'], check=True)
        expected = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
        packed = ns.git_pack(repo, self.shared, 'linux', self.key)
        restored = self.root / 'git-quarantine'
        ns.secrets_extract(packed, restored, self.key, self.shared)
        self.assertIsNone(json.loads((restored / 'repository.json').read_text())['head_ref'])
        heads = subprocess.check_output(['git', 'bundle', 'list-heads', str(restored / 'repository.bundle')], text=True)
        self.assertIn(expected + ' HEAD', heads)
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key)})
        preview = self.root / 'detached-preview'
        ns.git_preview(packed, preview, cfg, self.key)
        self.assertEqual(subprocess.check_output(['git', '-C', str(preview), 'rev-parse', 'HEAD'], text=True).strip(), expected)
        self.assertNotEqual(subprocess.run(['git', '-C', str(preview), 'symbolic-ref', '-q', 'HEAD'], capture_output=True).returncode, 0)

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_git_pack_rejects_branch_switch_at_same_commit(self):
        repo = self.root / 'same-commit-repo'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'file').write_text('mock')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
        subprocess.run(['git', '-C', str(repo), 'branch', 'other'], check=True)
        real_run = subprocess.run
        def switch_after_bundle(args, **kwargs):
            result = real_run(args, **kwargs)
            if 'bundle' in args and 'create' in args:
                real_run(['git', '-C', str(repo), 'checkout', '-q', 'other'], check=True)
            return result
        with mock.patch.object(ns.subprocess, 'run', side_effect=switch_after_bundle):
            with self.assertRaisesRegex(ns.SyncError, 'Git-ссылки или индекс'):
                ns.git_pack(repo, self.shared, 'linux', self.key)
        self.assertFalse((self.shared / 'nexus-sync' / 'secrets').exists())

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_explicit_git_watcher_sends_changes_and_retries_missing_repo(self):
        nexus = self.root / 'Nexus'
        repo = nexus / 'Bot'
        repo.mkdir(parents=True)
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'file').write_text('first')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
        missing = nexus / 'Later'
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'git_repos': [str(repo), str(missing)]})
        previous, notes = ns.supervisor_once(cfg, None)
        self.assertTrue(any('Git-пакет' in note and 'Создан' in note for note in notes))
        self.assertTrue(any('Git-пакет отложен' in note for note in notes))
        local = ns.configured_status(cfg, self.key)
        self.assertEqual(local['outgoing_packages']['git_markers'], 1)
        self.assertEqual(local['outgoing_packages']['git_invalid'], 0)
        self.assertIn('содержимое и доставка не проверены', local['message'])
        git_marker = next(state.glob('last-git-linux-*.json'))
        original_marker = git_marker.read_bytes()
        git_marker.write_text('{}')
        self.assertEqual(ns.configured_status(cfg, self.key)['outgoing_package_invalid'], 1)
        git_marker.write_bytes(original_marker)
        self.assertTrue(any('снимок' in note for note in notes))
        folder = self.shared / 'nexus-sync' / 'secrets' / 'linux'
        packages = list(folder.glob('*.nxs'))
        self.assertEqual(len(packages), 1)
        quarantine = self.root / 'git-quarantine'
        ns.secrets_extract(packages[0], quarantine, self.key, self.shared)
        self.assertEqual(json.loads((quarantine / 'repository.json').read_text())['project'], 'Bot')
        self.assertNotIn(str(repo), '\n'.join(notes))
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertFalse(any('Git-пакет' in note and 'Создан' in note for note in notes))
        self.assertEqual(len(list(folder.glob('*.nxs'))), 1)
        (repo / 'file').write_text('unstaged')
        ns.supervisor_once(cfg, previous)
        self.assertEqual(len(list(folder.glob('*.nxs'))), 1)
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertTrue(any('Git-пакет' in note and 'Создан' in note for note in notes))
        self.assertEqual(len(list(folder.glob('*.nxs'))), 2)
        before = set(folder.glob('*.nxs'))
        missing.mkdir()
        subprocess.run(['git', 'init', '-q', str(missing)], check=True)
        (missing / 'file').write_text('later')
        subprocess.run(['git', '-C', str(missing), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(missing), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'later'], check=True)
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertTrue(any('Git-пакет' in note and 'Создан' in note for note in notes))
        added = set(folder.glob('*.nxs')) - before
        self.assertEqual(len(added), 1)
        later_quarantine = self.root / 'later-git-quarantine'
        ns.secrets_extract(added.pop(), later_quarantine, self.key, self.shared)
        self.assertEqual(json.loads((later_quarantine / 'repository.json').read_text())['project'], 'Later')
        bad = {'format': 'nexus-sync-config-v1', 'machine': 'linux', 'nexus': str(nexus),
            'codex_home': str(self.codex), 'shared': str(self.shared), 'state_dir': str(state),
            'key_file': str(key), 'git_repos': [str(self.root / 'outside')]}
        with self.assertRaises(ns.SyncError):
            ns.validate_settings(bad)

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_discover_git_handles_nested_repositories_and_worktrees(self):
        nexus = self.root / 'Nexus'
        repo = nexus / 'Bot'
        nested = repo / 'Nested'
        nested.mkdir(parents=True)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        (repo / 'file').write_text('example')
        subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
        subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
        subprocess.run(['git', 'init', '-q', str(nested)], check=True)
        linked = nexus / 'Linked'
        subprocess.run(['git', '-C', str(repo), 'worktree', 'add', '-q', '--detach', str(linked)], check=True)
        ignored = nexus / 'node_modules' / 'dependency'
        ignored.mkdir(parents=True)
        subprocess.run(['git', 'init', '-q', str(ignored)], check=True)
        for name in ('.pytest_cache', '.mypy_cache', '.ruff_cache', '.tox', '.nox'):
            nested_cache = nexus / name / 'dependency'
            nested_cache.mkdir(parents=True)
            subprocess.run(['git', 'init', '-q', str(nested_cache)], check=True)
        exchange = nexus / 'exchange'
        exchange.mkdir()
        subprocess.run(['git', 'init', '-q', str(exchange)], check=True)
        found = ns.discover_git_repos(nexus, exchange)
        self.assertEqual({path.relative_to(nexus).as_posix() for path in found}, {'Bot', 'Bot/Nested', 'Linked'})
        with self.assertRaises(ns.SyncError):
            ns.discover_git_repos(nexus, self.root)
        result = subprocess.run([sys.executable, str(Path(ns.__file__).resolve()), 'discover-git',
                                 '--nexus', str(nexus), '--shared', str(exchange)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(set(json.loads(result.stdout)), {'Bot', 'Bot/Nested', 'Linked'})

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_discovery_packages_new_repositories_without_duplicates(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        def create_repo(name):
            repo = nexus / name
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            (repo / 'file').write_text('mock')
            subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                            'user.email=test@example.invalid', 'commit', '-qm', 'first'], check=True)
            return repo
        first = create_repo('First')
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'git_repos': [str(first)], 'git_discovery': True})
        self.assertEqual(sum('Создан зашифрованный Git-пакет' in note for note in ns.git_cycle(cfg)), 1)
        create_repo('Second')
        self.assertEqual(sum('Создан зашифрованный Git-пакет' in note for note in ns.git_cycle(cfg)), 1)
        self.assertEqual(len(list((self.shared / 'nexus-sync' / 'secrets' / 'linux').glob('*.nxs'))), 2)

    @unittest.skipUnless(shutil.which('git'), 'Git is required')
    def test_git_discovery_preserves_distinct_case_sensitive_repositories(self):
        nexus = self.root / 'Nexus'
        nexus.mkdir()
        state = self.root / 'state'
        state.mkdir()
        key = self.root / 'key'
        key.write_bytes(self.key)
        for name in ('Project', 'project'):
            repo = nexus / name
            if repo.exists():
                self.skipTest('filesystem does not support distinct case-sensitive names')
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            (repo / 'file').write_text(name)
            subprocess.run(['git', '-C', str(repo), 'add', 'file'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c',
                            'user.email=test@example.invalid', 'commit', '-qm', 'mock'], check=True)
        cfg = ns.validate_settings({'format': 'nexus-sync-config-v1', 'machine': 'linux',
            'nexus': str(nexus), 'codex_home': str(self.codex), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key), 'git_discovery': True})
        notes = ns.git_cycle(cfg)
        self.assertEqual(sum('Создан зашифрованный Git-пакет' in note for note in notes), 2)
        self.assertTrue(any('различаются регистром' in note for note in notes))
        packages = ns.list_encrypted_packages(cfg, self.key)
        self.assertEqual({item['project'] for item in packages}, {'Project', 'project'})
        notes = ns.git_cycle(cfg)
        self.assertFalse(any('Создан зашифрованный Git-пакет' in note for note in notes))
        self.assertTrue(any('различаются регистром' in note for note in notes))

    def test_shared_folder_must_not_overlap_codex(self):
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.codex, 'linux', self.key)

    def test_status_never_calls_a_local_snapshot_delivered(self):
        self.assertEqual(ns.status(self.shared, 'linux')['state'], 'no_snapshot')
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        state = ns.status(self.shared, 'linux')
        self.assertEqual(state['state'], 'snapshot_ready')
        self.assertIn('не подтверждена', state['message'])
        (result / 'manifest.json').write_text('{broken', encoding='utf-8')
        self.assertEqual(ns.status(self.shared, 'linux')['state'], 'invalid')

    def test_latest_snapshot_uses_authenticated_sender_time_not_directory_mtime(self):
        older = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        newer = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        def set_created(path, timestamp):
            manifest_path = path / 'manifest.json'
            data = json.loads(manifest_path.read_text())
            data.pop('previous_snapshot', None)  # Model snapshots created before chain support.
            data['created'] = timestamp
            data['authentication'] = ns._manifest_tag(data, self.key)
            manifest_path.write_text(json.dumps(data))
        set_created(older, '2026-01-01T00:00:00+00:00')
        set_created(newer, '2026-01-02T00:00:00+00:00')
        os.utime(older, ns=(older.stat().st_atime_ns, newer.stat().st_mtime_ns + 2_000_000_000))
        selected, _ = ns.latest_authenticated_snapshot(older.parent, 'linux', self.key)
        self.assertEqual(selected, newer)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['created'], '2026-01-02T00:00:00+00:00')
        set_created(older, '2026-01-02T00:00:00+00:00')
        with self.assertRaises(ns.SyncError):
            ns.latest_authenticated_snapshot(older.parent, 'linux', self.key)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'invalid')
        set_created(older, '2026-01-01T00:00:00+00:00')
        old_manifest = older / 'manifest.json'
        tampered = json.loads(old_manifest.read_text())
        tampered['created'] = '2027-01-01T00:00:00+00:00'
        old_manifest.write_text(json.dumps(tampered))
        with self.assertRaises(ns.SyncError):
            ns.latest_authenticated_snapshot(older.parent, 'linux', self.key)

    def test_signed_snapshot_chain_handles_clock_rollback_and_detects_forks(self):
        first = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        second = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        first_data = ns.verify(first, self.key)
        second_path = second / 'manifest.json'
        second_data = json.loads(second_path.read_text())
        self.assertIsNone(first_data['previous_snapshot'])
        self.assertEqual(second_data['previous_snapshot'], first.name)
        second_data['created'] = '2000-01-01T00:00:00+00:00'
        second_data['authentication'] = ns._manifest_tag(second_data, self.key)
        second_path.write_text(json.dumps(second_data))
        selected, _ = ns.latest_authenticated_snapshot(first.parent, 'linux', self.key)
        self.assertEqual(selected, second)
        third = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        third_path = third / 'manifest.json'
        third_data = json.loads(third_path.read_text())
        self.assertEqual(third_data['previous_snapshot'], second.name)
        third_data['previous_snapshot'] = 'f' * 32
        third_data['authentication'] = ns._manifest_tag(third_data, self.key)
        third_path.write_text(json.dumps(third_data))
        with self.assertRaises(ns.SyncError):
            ns.latest_authenticated_snapshot(first.parent, 'linux', self.key)
        third_data['previous_snapshot'] = first.name
        third_data['authentication'] = ns._manifest_tag(third_data, self.key)
        third_path.write_text(json.dumps(third_data))
        with self.assertRaises(ns.SyncError):
            ns.latest_authenticated_snapshot(first.parent, 'linux', self.key)
        with self.assertRaises(ns.SyncError):
            ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'invalid')

    def test_snapshot_chain_can_follow_multiple_legacy_snapshots(self):
        first = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        second = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        for path, timestamp in ((first, '2026-01-01T00:00:00+00:00'),
                                (second, '2026-01-02T00:00:00+00:00')):
            manifest_path = path / 'manifest.json'
            data = json.loads(manifest_path.read_text())
            data.pop('previous_snapshot')
            data['created'] = timestamp
            data['authentication'] = ns._manifest_tag(data, self.key)
            manifest_path.write_text(json.dumps(data))
        third = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        self.assertEqual(ns.verify(third, self.key)['previous_snapshot'], second.name)
        self.assertEqual(ns.latest_authenticated_snapshot(first.parent, 'linux', self.key)[0], third)

    def test_old_snapshot_with_unknown_history_mode_shows_warning(self):
        snapshot = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        path = snapshot / 'manifest.json'
        manifest = json.loads(path.read_text())
        del manifest['paginated_files']
        manifest['authentication'] = ns._manifest_tag(manifest, self.key)
        path.write_text(json.dumps(manifest))
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'snapshot_unverified')
        manifest['paginated_files'] = '0'
        manifest['authentication'] = ns._manifest_tag(manifest, self.key)
        path.write_text(json.dumps(manifest))
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['state'], 'invalid')

    def test_modified_receipt_never_claims_delivery(self):
        result = ns.snapshot(self.codex, self.shared, 'linux', self.key)
        receipt = ns.acknowledge(result, self.shared, 'mac', self.key)
        data = json.loads(receipt.read_text(encoding='utf-8'))
        data['receiver'] = 'vps'
        receipt.write_text(json.dumps(data), encoding='utf-8')
        self.assertEqual(ns.status(self.shared, 'linux', self.key)['received_by'], [])


if __name__ == '__main__':
    unittest.main()
