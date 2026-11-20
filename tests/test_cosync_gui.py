from pathlib import Path
import unittest

import cosync_engine


class SyncthingGuiTests(unittest.TestCase):
    def test_modified_upstream_interface_preserves_sync_controls_and_license(self):
        root = cosync_engine.gui_assets_dir()
        html = (root / 'default' / 'index.html').read_text(encoding='utf-8')
        self.assertIn('CoSyncX', html)
        self.assertIn('SyncthingController', html)
        self.assertIn('addFolder()', html)
        self.assertIn('addDevice()', html)
        self.assertIn('Codex', html)
        self.assertNotIn('ng-click="advanced()"', html)
        self.assertNotIn('ng-click="shutdown()"', html)
        self.assertIn('Mozilla Public License', (root / 'LICENSE').read_text(encoding='utf-8'))
        self.assertTrue((root / 'default' / 'assets' / 'css' / 'cosyncx.css').is_file())


if __name__ == '__main__':
    unittest.main()
