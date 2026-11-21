import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import mock_chat_sync as mock
import mock_multigroup_restore as groups
import nexus_sync as ns


class GroupReportRecoveryTests(unittest.TestCase):
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
