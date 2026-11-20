import json
from pathlib import Path
import tempfile
import unittest

import cosync_extensions as ext
import nexus_sync as ns


class ExtensionBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.codex = self.root / 'source'
        self.codex.mkdir()
        self.exchange = self.root / 'exchange'
        self.exchange.mkdir()
        self.key = b'k' * 32
        for name in ('skills/custom/SKILL.md', 'skills/.system/builtin/SKILL.md',
                     'plugins/cache/vendor/plugin/1/file', 'plugins/custom/plugin.json',
                     'hooks/check.sh', 'config.toml', 'auth.json'):
            path = self.codex / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('private synthetic ' + name)
        (self.codex / 'hooks/check.sh').chmod(0o700)
        (self.codex / 'hooks/platform-tool').write_bytes(b'\x7fELFsynthetic')

    def test_encrypted_bundle_excludes_system_cache_and_auth(self):
        bundle = self.exchange / 'bundle'
        count = ext.export_bundle(self.codex, bundle, self.key)
        self.assertEqual(count, 4)
        self.assertNotIn(b'private synthetic', bundle.read_bytes())
        output = self.root / 'quarantine'
        self.assertEqual(ext.extract_bundle(bundle, output, self.key), count)
        self.assertEqual((output / 'skills/custom/SKILL.md').read_text(), 'private synthetic skills/custom/SKILL.md')
        self.assertFalse((output / 'auth.json').exists())
        self.assertFalse((output / 'skills/.system').exists())
        self.assertFalse((output / 'plugins/cache').exists())
        self.assertFalse((output / 'hooks/platform-tool').exists())
        excluded = json.loads((output / 'excluded-files.json').read_text())['excluded']
        self.assertTrue(any(x['path'] == 'hooks/platform-tool' for x in excluded))
        inventory = json.loads((output / 'cached-plugin-inventory.json').read_text())
        self.assertTrue(inventory['installed_not_verified'])
        self.assertEqual(inventory['cached_plugins'], [{'registry': 'vendor', 'plugin': 'plugin', 'version': '1'}])
        self.assertTrue((output / 'hooks/check.sh').stat().st_mode & 0o100)

    def test_wrong_key_and_tampering_do_not_extract(self):
        bundle = self.exchange / 'bundle'
        ext.export_bundle(self.codex, bundle, self.key)
        dest = self.root / 'quarantine'
        with self.assertRaises(ns.SyncError): ext.extract_bundle(bundle, dest, b'x' * 32)
        data = bytearray(bundle.read_bytes()); data[-1] ^= 1; bundle.write_bytes(data)
        with self.assertRaises(ns.SyncError): ext.extract_bundle(bundle, dest, self.key)
        self.assertFalse(dest.exists())

    def test_rejects_symlinks_existing_targets_and_unsafe_names(self):
        (self.codex / 'hooks/link').symlink_to(self.root)
        with self.assertRaises(ns.SyncError): ext.export_bundle(self.codex, self.exchange / 'bundle', self.key)
        (self.codex / 'hooks/link').unlink()
        ext.export_bundle(self.codex, self.exchange / 'bundle', self.key)
        with self.assertRaises(ns.SyncError): ext.extract_bundle(self.exchange / 'bundle', self.codex, self.key)
        for path in ('../x', 'plugins/cache/a', 'hooks/../../x', 'auth.json', 'hooks/C:\\test'):
            with self.subTest(path=path), self.assertRaises(ns.SyncError): ext._safe_name(path)


if __name__ == '__main__': unittest.main()
