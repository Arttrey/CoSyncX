from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import cosync_archive_plan as plan
import nexus_sync as ns


class ArchivePlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='cosyncx-archive-plan-')
        self.addCleanup(self.tmp.cleanup)
        self.profile = Path(self.tmp.name).resolve()
        self.ids = [f'019b0000-0000-7000-8000-{n:012d}' for n in range(1, 4)]
        self.database = self.profile / 'state_5.sqlite'
        with sqlite3.connect(self.database) as db:
            db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
            db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT, status TEXT)')
            db.executemany('INSERT INTO threads VALUES (?,?)', [(self.ids[0], 1), (self.ids[1], 1), (self.ids[2], 0)])
            db.execute('INSERT INTO thread_spawn_edges VALUES (?,?,?)', (self.ids[0], self.ids[1], 'closed'))
        self.catalog = {identity: [{'path': self.profile / ('file' + str(index) + '.jsonl')}]
                        for index, identity in enumerate(self.ids)}

    def test_includes_descendants_and_all_local_versions(self):
        self.catalog[self.ids[1]].append({'path': self.profile / 'another-version.jsonl'})
        with patch.object(plan.ns, '_chat_catalog', return_value=self.catalog):
            result = plan.archive_impact(self.profile, [self.ids[0]])
        self.assertEqual(result['threads'], set(self.ids[:2]))
        self.assertEqual(result['archived'], 2)
        self.assertEqual(len(result['files']), 3)

    def test_missing_history_or_cycle_fails_closed(self):
        with patch.object(plan.ns, '_chat_catalog', return_value={self.ids[0]: self.catalog[self.ids[0]]}):
            with self.assertRaises(ns.SyncError): plan.archive_impact(self.profile, [self.ids[0]])
        with sqlite3.connect(self.database) as db:
            db.execute('INSERT INTO thread_spawn_edges VALUES (?,?,?)', (self.ids[1], self.ids[0], 'closed'))
        with patch.object(plan.ns, '_chat_catalog', return_value=self.catalog):
            with self.assertRaises(ns.SyncError): plan.archive_impact(self.profile, [self.ids[0]])


if __name__ == '__main__': unittest.main()
