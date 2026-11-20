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
        self.root = Path(self.tmp.name).resolve()
        self.codex = self.root / 'codex'
        self.shared = self.root / 'shared'
        self.sessions = self.codex / 'sessions' / '2026' / '01' / '01'
        self.sessions.mkdir(parents=True)
        self.shared.mkdir()
        self.chat = self.sessions / 'rollout-test.jsonl'
        self.key = b'a' * 32
        self.chat.write_text('{"type":"session_meta","payload":{"id":"test"}}\n', encoding='utf-8')

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

    def test_plan_reports_unsupported_incoming_without_blocking_other_chats(self):
        incoming = self.root / 'incoming'
        sessions = incoming / 'sessions'
        sessions.mkdir(parents=True)
        for identifier, mode in [('ordinary', 'legacy'), ('paged', 'paginated'), ('unknown', 'future')]:
            (sessions / f'rollout-{identifier}.jsonl').write_text(json.dumps({
                'type': 'session_meta', 'payload': {'id': identifier, 'history_mode': mode}
            }) + '\n')
        before = {p.name: p.read_bytes() for p in sessions.iterdir()}
        plan = {item['chat_hash']: item for item in ns.plan_chats(incoming, self.codex)}
        def item(name):
            return plan[hashlib.sha256(name.encode()).hexdigest()]
        self.assertEqual(item('ordinary')['relation'], 'NO_LOCAL_FILE')
        self.assertEqual(item('paged')['relation'], 'INCOMING_UNSUPPORTED')
        self.assertEqual(item('paged')['incoming_format_warning'], 'PAGINATED')
        self.assertEqual(item('unknown')['incoming_format_warning'], 'UNKNOWN_FORMAT')
        self.assertEqual(before, {p.name: p.read_bytes() for p in sessions.iterdir()})

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
        ns.write_heartbeat(cfg, self.key, second)
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

    def test_supervisor_sends_once_without_project_scanning(self):
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
        self.assertEqual(ns.previous_supervisor_snapshot(cfg), previous)
        _, notes = ns.supervisor_once(cfg, previous)
        self.assertEqual(notes, [])
    def test_windows_content_scan_does_not_block_other_supervisor_cycles(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        observations = []
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'codex_home': self.codex}
        def fingerprint_work(_cfg):
            started.set()
            if not release.wait(timeout=3):
                raise AssertionError('test did not release content scan')
            finished.set()
            return 'a' * 64
        def cycle(_cfg, previous, *, fingerprint_override):
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
        self.assertFalse((self.shared / 'nexus-sync' / 'snapshots' / 'windows').exists())

    def test_windows_content_scan_failure_is_reported_without_reusing_old_result(self):
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'fake-key',
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
        def supervisor(_cfg, previous, *, fingerprint_override):
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
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'fake-key',
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
        def supervisor(_cfg, previous, *, fingerprint_override):
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
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'fake-key',
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
        def supervisor(_cfg, previous, *, fingerprint_override):
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
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'fake-key',
               'shared': self.shared, 'auto_index_preview_dir': self.root / 'preview'}
        def index_work(_cfg):
            started.set()
            self.assertTrue(release.wait(timeout=3))
            return []
        def chat_work(_cfg, previous, **_kwargs):
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
        cfg = {'interval_seconds': 10, 'key_file': self.root / 'key',
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
        self.assertFalse(heartbeat.call_args.args[3])

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
            ns.write_heartbeat(cfg, self.key, [], True, fingerprint_running=True)
        with mock.patch.object(ns.time, 'time', return_value=1001):
            observed = ns.configured_status(cfg, self.key)['observer']
            self.assertEqual(observed['state'], 'recent')
            self.assertTrue(observed['index_running'])
            self.assertTrue(observed['fingerprint_running'])
            status = ns.configured_status(dict(cfg, auto_index_preview_dir=self.root / 'preview'), self.key)
            self.assertEqual(status['index_activity'], 'running')
            self.assertEqual(status['fingerprint_activity'], 'running')
        with mock.patch.object(ns.time, 'time', return_value=1006):
            ns.write_heartbeat(cfg, self.key, ['Снимок отложен: FAKE_PRIVATE'])
            observed = ns.configured_status(cfg, self.key)['observer']
            self.assertEqual(observed['state'], 'error')
            self.assertEqual(set(observed['errors']), {'snapshot'})
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
            ns.write_heartbeat(cfg, self.key, [], False)

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
        def two_cycles_on_windows():
            calls = 0
            def tick(_seconds):
                nonlocal calls
                calls += 1
                if sys.platform == 'win32' and calls == 1:
                    threading.Event().wait(0.2)
                    return
                raise KeyboardInterrupt
            return tick
        with mock.patch.object(ns.time, 'sleep', side_effect=two_cycles_on_windows()):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(config)
        cfg = ns.load_settings(config)
        self.assertEqual(ns.heartbeat_status(cfg, self.key)['state'], 'error')
        self.assertIn('snapshot', ns.heartbeat_status(cfg, self.key)['errors'])
        self.chat.write_text('{"type":"session_meta","payload":{"id":"test"}}\n')
        with mock.patch.object(ns.time, 'sleep', side_effect=two_cycles_on_windows()):
            with self.assertRaises(KeyboardInterrupt):
                ns.run_supervisor(config)
        self.assertEqual(ns.heartbeat_status(cfg, self.key)['state'], 'recent')



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
        try:
            (nexus / 'bot').mkdir()
        except FileExistsError:
            # The volume itself cannot represent two case-distinct names.
            pass
        else:
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
        expected = 'AVAILABLE' if (nexus / 'bot' / 'source').is_dir() else 'MAPPED_BUT_MISSING'
        self.assertEqual(ns.plan_chats(incoming, self.codex, '/linux/Nexus', str(nexus))[0]['cwd'], expected)


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

    def test_catalog_uses_thread_id_when_session_is_shared(self):
        incoming = self.root / 'incoming'
        folder = incoming / 'sessions'
        folder.mkdir(parents=True)
        for thread in ('thread-a', 'thread-b'):
            (folder / f'rollout-{thread}.jsonl').write_text(json.dumps({
                'type': 'session_meta', 'payload': {'id': thread, 'session_id': 'shared', 'history_mode': 'paginated'}
            }) + '\n')
        (folder / 'rollout-legacy.jsonl').write_text(json.dumps({
            'type': 'session_meta', 'payload': {'session_id': 'legacy'}
        }) + '\n')
        catalog = ns._chat_catalog(incoming, include_unsupported=True)
        self.assertEqual(set(catalog), {'thread-a', 'thread-b', 'legacy'})
        self.assertTrue(all(len(entries) == 1 for entries in catalog.values()))

    def test_plan_reports_history_base_without_assuming_it_is_complete(self):
        incoming = self.root / 'incoming'
        folder = incoming / 'sessions'
        folder.mkdir(parents=True)
        for identity, base in [('present', None), ('child', {'thread_id': 'present'}),
                               ('missing', {'thread_id': 'absent'}), ('bad', {'unexpected': 1})]:
            payload = {'id': identity, 'history_mode': 'paginated'}
            if base is not None:
                payload['history_base'] = base
            (folder / f'rollout-{identity}.jsonl').write_text(json.dumps({'type': 'session_meta', 'payload': payload}) + '\n')
        plan = {item['chat_hash']: item for item in ns.plan_chats(incoming, self.codex)}
        def state(identity):
            return plan[hashlib.sha256(identity.encode()).hexdigest()]['history_base']
        self.assertEqual(state('present'), 'NOT_REQUIRED')
        self.assertEqual(state('child'), 'PRESENT_UNVERIFIED')
        self.assertEqual(state('missing'), 'MISSING')
        self.assertEqual(state('bad'), 'INVALID')

    def test_plan_reports_missing_parent_without_importing_archives(self):
        incoming = self.root / 'incoming'
        folder = incoming / 'sessions'
        folder.mkdir(parents=True)
        for identity, parent in [('parent', None), ('child', 'parent'), ('orphan', 'absent'), ('self', 'self')]:
            payload = {'id': identity, 'history_mode': 'paginated'}
            if parent is not None:
                payload['parent_thread_id'] = parent
            (folder / f'rollout-{identity}.jsonl').write_text(json.dumps({'type': 'session_meta', 'payload': payload}) + '\n')
        plan = {item['chat_hash']: item for item in ns.plan_chats(incoming, self.codex)}
        def state(identity):
            return plan[hashlib.sha256(identity.encode()).hexdigest()]['parent_thread']
        self.assertEqual(state('parent'), 'NOT_REQUIRED')
        self.assertEqual(state('child'), 'PRESENT_UNVERIFIED')
        self.assertEqual(state('orphan'), 'MISSING')
        self.assertEqual(state('self'), 'INVALID')

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
