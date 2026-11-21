import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from cosync_graph_snapshot import GraphError, read_graph, read_shared_graph, write_graph


class GraphSnapshotTests(unittest.TestCase):
    def test_device_graphs_merge_and_conflicting_child_is_deferred(self):
        base = {'format':'cosyncx-active-graph-v1','omitted_missing':0,'omitted_archived':0,
                'edges':[{'parent':self.ids[0],'child':self.ids[1],'status':'open'}]}
        (self.output/'graph.json').write_text(json.dumps(base))
        devices=self.output/'devices';devices.mkdir()
        peer={'format':'cosyncx-active-graph-v1','omitted_missing':0,'omitted_archived':0,
              'edges':[{'parent':self.ids[0],'child':self.ids[2],'status':'closed'}]}
        (devices/('a'*32+'.json')).write_text(json.dumps(peer))
        _,combined,blocked=read_shared_graph(self.output)
        self.assertEqual(len(combined['edges']),2);self.assertEqual(blocked,set())
        peer['edges'].append({'parent':self.ids[3],'child':self.ids[1],'status':'open'})
        (devices/('a'*32+'.json')).write_text(json.dumps(peer))
        _,combined,blocked=read_shared_graph(self.output)
        self.assertEqual(blocked,{self.ids[1]})
        self.assertEqual([x['child'] for x in combined['edges']],[self.ids[2]])
        (devices/'unknown.json').write_text('{}')
        self.assertEqual(read_shared_graph(self.output)[2],{self.ids[1]})

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
