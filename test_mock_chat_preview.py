import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import mock_chat_preview as preview
import nexus_sync as ns


@unittest.skipUnless(sys.platform == 'linux', 'requires Linux mock profile locks')
class MockChatPreviewTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='nexus-chat-preview-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.shared = self.root / 'shared'
        self.shared.mkdir()
        self.target = self.root / 'target'
        self.target.mkdir()
        self.source = self.root / 'source'
        (self.source / 'sessions').mkdir(parents=True)
        self.chat = self.source / 'sessions' / 'rollout-mock.jsonl'
        self.chat.write_text(json.dumps({'type': 'session_meta', 'payload': {
            'id': '019b0000-0000-7000-8000-000000000075', 'cwd': '/source/Nexus/Bot'}}) + '\n')
        self.key = b'p' * 32
        self.output = self.root / 'previews'
        preview.init(self.output, self.shared, self.target, 'linux', '/source/Nexus', self.key)

    def test_prepares_a_new_profile_for_each_snapshot_and_rejects_rollback(self):
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        def simulate(profile, *args, **kwargs):
            (profile / 'synthetic-index').write_text('indexed')
            return {'state': 'COMPLETE', 'processed': 1}
        with mock.patch.object(preview.mock, 'sync_all_mock', side_effect=simulate) as sync:
            first = preview.prepare(self.output, self.key, Path(sys.executable))
            self.assertEqual(first['state'], 'PREPARED')
            self.assertFalse(first['verified_in_cursor'])
            self.assertEqual(preview.prepare(self.output, self.key, Path(sys.executable))['state'], 'UNCHANGED')
            self.chat.write_text(self.chat.read_text() +
                                 json.dumps({'type': 'event_msg', 'payload': {'message': 'mock'}}) + '\n')
            second_snapshot = ns.snapshot(self.source, self.shared, 'linux', self.key)
            second = preview.prepare(self.output, self.key, Path(sys.executable))
            self.assertEqual(second['state'], 'PREPARED')
            self.assertNotEqual(first['profile'], second['profile'])
            self.assertTrue((Path(first['profile']) / 'synthetic-index').is_file())
            self.assertEqual(sync.call_count, 2)
            shutil.move(second_snapshot, self.root / 'held-snapshot')
            with self.assertRaises(ns.SyncError):
                preview.prepare(self.output, self.key, Path(sys.executable))
            self.assertEqual(sync.call_count, 2)

    def test_failed_preview_keeps_profile_and_preserves_checkpoint(self):
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(preview.mock, 'sync_all_mock', return_value={'state': 'PENDING', 'processed': 0}) as sync:
            self.assertEqual(preview.prepare(self.output, self.key, Path(sys.executable))['state'], 'ATTENTION')
            self.assertEqual(preview.prepare(self.output, self.key, Path(sys.executable))['state'], 'ATTENTION')
            self.assertEqual(sync.call_count, 1)
        self.assertEqual(len(list(self.output.glob('profile-*'))), 1)
        self.assertFalse((self.output / preview.CHECKPOINT).exists())

    def test_forged_or_missing_checkpoint_is_not_accepted(self):
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(preview.mock, 'sync_all_mock', return_value={'state': 'COMPLETE', 'processed': 1}):
            result = preview.prepare(self.output, self.key, Path(sys.executable))
            shutil.rmtree(result['profile'])
            with self.assertRaises(ns.SyncError):
                preview.prepare(self.output, self.key, Path(sys.executable))
            (self.output / preview.CHECKPOINT).write_text('{}')
            with self.assertRaises(ns.SyncError):
                preview.prepare(self.output, self.key, Path(sys.executable))

    def test_failure_does_not_start_a_second_indexing_process(self):
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(preview.mock, 'sync_all_mock', side_effect=ns.SyncError('synthetic failure')) as sync:
            with self.assertRaises(ns.SyncError):
                preview.prepare(self.output, self.key, Path(sys.executable))
            self.assertEqual(preview.prepare(self.output, self.key, Path(sys.executable))['state'], 'ATTENTION')
            self.assertEqual(sync.call_count, 1)

    def test_read_only_status_reports_preview_and_unfinished_attempt(self):
        self.assertEqual(preview.status(self.output, self.key, self.shared)['state'], 'NOT_PREPARED')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        with mock.patch.object(preview.mock, 'sync_all_mock', return_value={'state': 'COMPLETE', 'processed': 1}):
            result = preview.prepare(self.output, self.key, Path(sys.executable))
        before = {path.name for path in self.output.iterdir()}
        status = preview.status(self.output, self.key, self.shared)
        self.assertEqual(status['state'], 'READY_LOCAL')
        self.assertFalse(status['verified_in_cursor'])
        self.assertFalse(status['index_current_verified'])
        self.assertFalse(status['remote_freshness_confirmed'])
        self.assertEqual(before, {path.name for path in self.output.iterdir()})
        self.chat.write_text(self.chat.read_text() + json.dumps({'type': 'event_msg', 'payload': {'message': 'mock'}}) + '\n')
        ns.snapshot(self.source, self.shared, 'linux', self.key)
        self.assertEqual(preview.status(self.output, self.key, self.shared)['state'], 'OUTDATED')
        (self.output / preview.ATTEMPT).write_text('{}')
        self.assertEqual(preview.status(self.output, self.key, self.shared)['state'], 'ATTENTION')
        (self.output / preview.ATTEMPT).unlink()
        shutil.rmtree(result['profile'])
        self.assertEqual(preview.status(self.output, self.key, self.shared)['state'], 'INVALID')

    def test_summary_exposes_mock_status_only_when_requested(self):
        key_file = self.root / 'key'
        key_file.write_bytes(self.key)
        state = self.root / 'state'
        state.mkdir()
        config = self.root / 'config.json'
        config.write_text(json.dumps({'format': 'nexus-sync-config-v1', 'machine': 'mac',
            'nexus': str(self.target), 'codex_home': str(self.source), 'shared': str(self.shared),
            'state_dir': str(state), 'key_file': str(key_file)}))
        command = [sys.executable, ns.__file__, 'summary', '--config', str(config)]
        normal = json.loads(subprocess.check_output(command))
        self.assertNotIn('mock_chat_preview', normal)
        mock_status = json.loads(subprocess.check_output(command + ['--mock-preview-root', str(self.output)]))
        self.assertEqual(mock_status['mock_chat_preview']['state'], 'NOT_PREPARED')
        self.assertFalse(mock_status['mock_chat_preview']['verified_in_cursor'])


if __name__ == '__main__':
    unittest.main()
