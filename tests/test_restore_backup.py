import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import cosync_restore_backup as backup


class BackupRotationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        self.profile = root / 'profile'
        self.backups = root / 'backups'
        self.profile.mkdir()
        self.backups.mkdir()
        (self.profile / 'sessions').mkdir()
        self.chat = self.profile / 'sessions' / 'chat.jsonl'
        self.chat.write_text('first\n')
        self.db = self.profile / 'state.sqlite'
        with sqlite3.connect(self.db) as conn:
            conn.execute('create table test (value text)')
            conn.execute("insert into test values ('first')")

    def test_successful_rotation_keeps_only_latest_verified_generation(self):
        names = ['sessions/chat.jsonl', 'state.sqlite']
        latest = backup.replace_latest(self.profile, self.backups, names)
        self.assertEqual((latest / names[0]).read_text(), 'first\n')
        self.chat.write_text('second\n')
        with sqlite3.connect(self.db) as conn:
            conn.execute("update test set value='second'")
        backup.replace_latest(self.profile, self.backups, names)
        self.assertEqual((latest / names[0]).read_text(), 'second\n')
        with sqlite3.connect(latest / 'state.sqlite') as conn:
            self.assertEqual(conn.execute('select value from test').fetchone(), ('second',))
        self.assertEqual(set(json.loads((latest / 'manifest.json').read_text())['files']), set(names))
        self.assertEqual([p.name for p in self.backups.iterdir()], ['latest'])

    def test_failure_keeps_previous_backup(self):
        backup.replace_latest(self.profile, self.backups, ['sessions/chat.jsonl'])
        self.chat.write_text('second\n')
        with mock.patch.object(backup, '_digest', side_effect=OSError('read failed')):
            with self.assertRaises(OSError):
                backup.replace_latest(self.profile, self.backups, ['sessions/chat.jsonl'])
        self.assertEqual((self.backups / 'latest/sessions/chat.jsonl').read_text(), 'first\n')
        self.assertEqual([p.name for p in self.backups.iterdir()], ['latest'])

    def test_rejects_symlinks_and_path_traversal(self):
        (self.profile / 'link').symlink_to(self.chat)
        for name in ('../outside', '/absolute', 'link', 'manifest.json'):
            with self.subTest(name=name), self.assertRaises(backup.BackupError):
                backup.replace_latest(self.profile, self.backups, [name])


if __name__ == '__main__':
    unittest.main()
