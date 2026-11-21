import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from cosync_graph_snapshot import GraphError, read_graph, write_graph


class GraphSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cosyncx-graph-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.profile, self.output = self.root / 'profile', self.root / 'output'
        self.profile.mkdir()
        self.output.mkdir(mode=0o700)
        self.database = self.profile / 'state_5.sqlite'
        self.ids = [f'019b0000-0000-7000-8000-{number:012d}' for number in range(1, 5)]
        self.db = sqlite3.connect(self.database)
        self.addCleanup(self.db.close)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER, title TEXT, cwd TEXT)')
        self.db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT PRIMARY KEY, status TEXT)')
        self.db.executemany('INSERT INTO threads VALUES (?, ?, ?, ?)', [
            (self.ids[0], 0, 'synthetic private title', '/synthetic/private/path'),
            (self.ids[1], 0, '', ''), (self.ids[2], 1, '', '')])
        self.db.executemany('INSERT INTO thread_spawn_edges VALUES (?, ?, ?)', [
            (self.ids[0], self.ids[1], 'closed'), (self.ids[0], self.ids[2], 'open'),
            (self.ids[0], self.ids[3], 'closed')])
        self.db.commit()

    def test_wal_consistent_minimal_and_no_unnecessary_rewrite(self):
        dest = self.output / 'graph.json'
        self.assertEqual(write_graph(self.database, dest), 1)
        value = json.loads(dest.read_text())
        self.assertEqual(value['edges'], [{'parent': self.ids[0], 'child': self.ids[1], 'status': 'closed'}])
        self.assertEqual(value['omitted_archived'], 1)
        self.assertEqual(value['omitted_missing'], 1)
        self.assertNotIn('synthetic private', dest.read_text())
        timestamp = dest.stat().st_mtime_ns
        self.assertEqual(write_graph(self.database, dest), 1)
        self.assertEqual(dest.stat().st_mtime_ns, timestamp)
        self.db.execute("UPDATE thread_spawn_edges SET status='open' WHERE child_thread_id=?", (self.ids[1],))
        self.db.commit()
        write_graph(self.database, dest)
        self.assertEqual(json.loads(dest.read_text())['edges'][0]['status'], 'open')

    def test_bad_status_and_cycles_are_rejected(self):
        self.db.execute("UPDATE thread_spawn_edges SET status='unknown' WHERE child_thread_id=?", (self.ids[1],))
        self.db.commit()
        with self.assertRaises(GraphError): read_graph(self.database)
        self.db.execute("UPDATE thread_spawn_edges SET status='open' WHERE child_thread_id=?", (self.ids[1],))
        self.db.execute('INSERT INTO thread_spawn_edges VALUES (?, ?, ?)', (self.ids[1], self.ids[0], 'closed'))
        self.db.commit()
        with self.assertRaises(GraphError): read_graph(self.database)


if __name__ == '__main__': unittest.main()
