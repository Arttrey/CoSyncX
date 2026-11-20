import os
from pathlib import Path
import shutil
import tempfile
import unittest

import mock_ignore_probe as probe
import nexus_sync as ns


class MockIgnoreProbeTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix='nexus-ignore-test-')
        self.addCleanup(self.workspace.cleanup)
        self.rules = Path(self.workspace.name) / 'rules'
        self.rules.write_text('venv\nnode_modules\n', encoding='utf-8')

    def test_validates_synthetic_input(self):
        data, paths = probe.validate_inputs(self.rules, ['venv/lib.py', 'src/main.py'])
        self.assertEqual(data, b'venv\nnode_modules\n')
        self.assertEqual(paths, ['venv/lib.py', 'src/main.py'])

    def test_rejects_external_includes_and_ambiguous_paths(self):
        self.rules.write_text('\ufeff #include other\n', encoding='utf-8')
        with self.assertRaises(ns.SyncError):
            probe.validate_inputs(self.rules, ['src/main.py'])
        self.rules.write_text('venv\n', encoding='utf-8')
        for paths in (['../outside'], ['.stignore'], ['file', 'file/child'], ['A/x', 'a/X']):
            with self.subTest(paths=paths), self.assertRaises(ns.SyncError):
                probe.validate_inputs(self.rules, paths)

    def test_secret_discovery_matches_uppercase_names(self):
        project = Path(self.workspace.name) / 'project'
        project.mkdir()
        names = ('.ENV', '.NPMRC', 'server.PEM')
        for name in names:
            (project / name).write_text('synthetic', encoding='utf-8')
        self.assertEqual({path.name for path in ns.discover_secret_files(project, ())}, set(names))

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'),
                         'requires opt-in local Syncthing')
    def test_sample_ignores_uppercase_secret_names(self):
        self.rules.write_bytes((Path(__file__).resolve().parents[1] / 'src' / 'stignore.example').read_bytes())
        result = probe.probe(self.rules, ['Bot/.ENV', 'Bot/.NPMRC', 'Bot/server.PEM', 'Bot/source.py'], timeout=25)
        self.assertEqual(result['state'], 'CHECKED', result)
        self.assertEqual([item['state'] for item in result['files']],
                         ['IGNORED_IN_MOCK', 'IGNORED_IN_MOCK', 'IGNORED_IN_MOCK', 'INDEXED_IN_MOCK'])

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'),
                         'requires opt-in local Syncthing')
    def test_real_syncthing_scans_only_synthetic_files(self):
        result = probe.probe(self.rules, ['venv/lib.py', 'node_modules/x.js', 'src/main.py'], timeout=25)
        self.assertEqual(result['state'], 'CHECKED', result)
        self.assertFalse(result['real_folder_checked'])
        self.assertEqual([item['state'] for item in result['files']],
                         ['IGNORED_IN_MOCK', 'IGNORED_IN_MOCK', 'INDEXED_IN_MOCK'])

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'),
                         'requires opt-in local Syncthing')
    def test_unindexed_control_does_not_claim_results(self):
        self.rules.write_text('*\n', encoding='utf-8')
        result = probe.probe(self.rules, ['src/main.py'], timeout=3)
        self.assertEqual(result['state'], 'UNKNOWN')
        self.assertNotIn('files', result)


if __name__ == '__main__':
    unittest.main()
