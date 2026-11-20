from pathlib import Path
import sys
import tempfile
import unittest

import mock_git_sync
import mock_secret_sync
import nexus_sync as ns


@unittest.skipUnless(sys.platform == 'linux', 'mock restore requires Linux')
class MockSecretTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='nexus-mock-secret-test-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.shared = self.root / 'shared'
        self.shared.mkdir()
        self.source = self.root / 'source'
        (self.source / 'Bot').mkdir(parents=True)
        (self.source / 'Bot' / '.env').write_text('SYNTHETIC=one\n')
        self.key = b'k' * 32
        self.package = ns.secrets_pack([self.source / 'Bot' / '.env'], self.shared, 'linux', self.key, root=self.source)
        self.target = self.root / 'target'
        mock_git_sync.init_workspace(self.target, self.shared, self.key)
        (self.target / 'Bot').mkdir()

    def apply(self):
        return mock_secret_sync.apply_secret(self.target, self.shared, self.key, self.package, 'Bot/.env')

    def test_installs_once_and_never_overwrites_conflicting_local_file(self):
        self.assertEqual(self.apply()['state'], 'INSTALLED')
        result = self.target / 'Bot' / '.env'
        self.assertEqual(result.read_text(), 'SYNTHETIC=one\n')
        self.assertEqual(result.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.apply()['state'], 'UNCHANGED')
        result.write_text('SYNTHETIC=local\n')
        with self.assertRaises(ns.SyncError):
            self.apply()
        self.assertEqual(result.read_text(), 'SYNTHETIC=local\n')

    def test_rejects_symlinked_target_and_wrong_key(self):
        target = self.target / 'Bot' / '.env'
        outside = self.root / 'outside'
        outside.write_text('unchanged\n')
        target.symlink_to(outside)
        with self.assertRaises(OSError):
            self.apply()
        self.assertEqual(outside.read_text(), 'unchanged\n')
        with self.assertRaises(ns.SyncError):
            mock_secret_sync.apply_secret(self.target, self.shared, b'z' * 32, self.package, 'Bot/.env')

    def test_rejects_non_matching_and_reserved_paths(self):
        for path in ('Bot/missing', 'Bot/.git/config', '../outside', '.STIGNORE',
                     mock_git_sync.MARKER.upper()):
            with self.assertRaises((ns.SyncError, OSError)):
                mock_secret_sync.apply_secret(self.target, self.shared, self.key, self.package, path)
        self.assertFalse((self.target / 'Bot' / '.env').exists())


if __name__ == '__main__':
    unittest.main()
