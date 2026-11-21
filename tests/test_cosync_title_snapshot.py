import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from cosync_title_snapshot import TitleSnapshotError, write_active_titles, main


class TitleSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cosyncx-titles-')
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        self.profile, self.output = root / 'profile', root / 'output'
        self.profile.mkdir()
        self.output.mkdir(mode=0o700)
        (self.profile / 'sessions').mkdir()
        self.ids = [f'019b0000-0000-7000-8000-{n:012d}' for n in range(1, 5)]
        self.database = self.profile / 'state_5.sqlite'
        with sqlite3.connect(self.database) as db:
            db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
            db.executemany('INSERT INTO threads VALUES (?,?)',
                           [(self.ids[0], 0), (self.ids[1], 1), (self.ids[2], 0)])
        for identity in (self.ids[0], self.ids[1], self.ids[3]):
            (self.profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text('synthetic')
        self.index = self.profile / 'session_index.jsonl'
        self.index.write_text(''.join(json.dumps({'id': identity, 'thread_name': 'synthetic'}) + '\n'
                                      for identity in self.ids))
        self.target = self.output / 'session_index.jsonl'

    def test_excludes_archived_unknown_and_missing_sessions(self):
        self.assertEqual(write_active_titles(self.profile, self.target), 1)
        rows = [json.loads(line) for line in self.target.read_text().splitlines()]
        self.assertEqual([row['id'] for row in rows], [self.ids[0]])
        with sqlite3.connect(self.database) as db:
            db.execute('UPDATE threads SET archived=1 WHERE id=?', (self.ids[0],))
        self.assertEqual(write_active_titles(self.profile, self.target), 0)
        self.assertEqual(self.target.read_bytes(), b'')

    def test_incomplete_index_fails_without_replacing_previous_snapshot(self):
        write_active_titles(self.profile, self.target)
        previous = self.target.read_bytes()
        self.index.write_bytes(self.index.read_bytes() + b'{')
        with self.assertRaises(TitleSnapshotError):
            write_active_titles(self.profile, self.target)
        self.assertEqual(self.target.read_bytes(), previous)

    def test_rejects_session_symlink_and_profile_destination(self):
        (self.profile / 'sessions' / 'symlink').symlink_to(self.output, target_is_directory=True)
        with self.assertRaises(TitleSnapshotError):
            write_active_titles(self.profile, self.target)
        with self.assertRaises(TitleSnapshotError):
            write_active_titles(self.profile, self.profile / 'unsafe')

    def test_cli_removes_stale_snapshot_when_export_fails(self):
        write_active_titles(self.profile, self.target)
        self.index.write_bytes(b'{')
        with patch('sys.stderr'):
            self.assertEqual(main(['--profile', str(self.profile), '--output', str(self.target)]), 1)
        self.assertFalse(self.target.exists())


if __name__ == '__main__': unittest.main()
