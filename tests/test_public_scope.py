import contextlib
import io
from pathlib import Path
import tempfile
import unittest

import nexus_sync as sync


class PublicScopeTests(unittest.TestCase):
    def test_chat_cli_does_not_offer_project_git_or_secret_transfer(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as stop:
            sync.main(['--help'])
        self.assertEqual(stop.exception.code, 0)
        text = output.getvalue()
        for command in ('git-pack', 'git-preview', 'discover-git', 'projects-record',
                        'projects-log', 'secrets-pack', 'secrets-extract'):
            self.assertNotIn(command, text)
        for command in ('snapshot', 'verify', 'prepare-chats'):
            self.assertIn(command, text)

    def test_public_source_has_no_git_or_dependency_features(self):
        source = Path(sync.__file__).read_text(encoding='utf-8')
        for symbol in ('def git_pack(', 'def git_preview(', 'def record_projects(',
                       'def write_project_report(', 'def secrets_pack('):
            self.assertNotIn(symbol, source)

    def test_legacy_configuration_with_enabled_removed_feature_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ('nexus', 'codex', 'shared', 'state'):
                (root / name).mkdir()
            key = root / 'key'
            key.write_bytes(b'x' * 32)
            config = {'format': 'nexus-sync-config-v1', 'machine': 'test',
                      'nexus': str(root / 'nexus'), 'codex_home': str(root / 'codex'),
                      'shared': str(root / 'shared'), 'state_dir': str(root / 'state'),
                      'key_file': str(key), 'git_discovery': True}
            with self.assertRaises(sync.SyncError):
                sync.validate_settings(config)
