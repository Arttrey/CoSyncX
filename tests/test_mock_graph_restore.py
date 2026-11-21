import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import mock_chat_sync as mock
import mock_graph_restore as restore
import cosync_restore_backup as backup
import nexus_sync as ns


class MockGraphRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cosyncx-graph-restore-')
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        self.profile, self.shared, self.backup, self.exchange = (root / x for x in ('profile', 'shared', 'backup', 'exchange'))
        for folder in (self.shared, self.backup, self.exchange): folder.mkdir()
        self.key = b'g' * 32
        mock.init_mock_profile(self.profile, self.key, self.shared)
        self.ids = [f'019b0000-0000-7000-8000-{n:012d}' for n in range(1, 6)]
        self.db = self.profile / 'state_5.sqlite'
        with sqlite3.connect(self.db) as db:
            db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
            db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT PRIMARY KEY, status TEXT)')
            db.executemany('INSERT INTO threads VALUES (?,0)', [(identity,) for identity in self.ids])
            db.executemany('INSERT INTO thread_spawn_edges VALUES (?,?,?)', [
                (self.ids[2], self.ids[1], 'open'), (self.ids[3], self.ids[4], 'closed')])
        self.graph = self.exchange / 'graph.json'
        self.data = {'format': 'cosyncx-active-graph-v1', 'created_at_ms': 1, 'omitted_archived': 0,
                     'omitted_missing': 0, 'edges': [{'parent': self.ids[0], 'child': self.ids[1], 'status': 'closed'}]}
        self.graph.write_text(json.dumps(self.data))

    def test_updates_only_selected_edges_and_saves_previous_database(self):
        self.assertEqual(restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key), 1)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT parent_thread_id,status FROM thread_spawn_edges WHERE child_thread_id=?',
                                        (self.ids[1],)).fetchone(), (self.ids[0], 'closed'))
            self.assertEqual(db.execute('SELECT parent_thread_id,status FROM thread_spawn_edges WHERE child_thread_id=?',
                                        (self.ids[4],)).fetchone(), (self.ids[3], 'closed'))
        with sqlite3.connect(self.backup / 'latest' / 'state_5.sqlite') as db:
            self.assertEqual(db.execute('SELECT parent_thread_id,status FROM thread_spawn_edges WHERE child_thread_id=?',
                                        (self.ids[1],)).fetchone(), (self.ids[2], 'open'))
        self.assertEqual(restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key), 0)

    def test_incomplete_or_missing_chat_is_never_applied(self):
        self.data['omitted_missing'] = 1
        self.graph.write_text(json.dumps(self.data))
        with self.assertRaises(ns.SyncError): restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key)
        self.data['omitted_missing'] = 0
        self.graph.write_text(json.dumps(self.data))
        with sqlite3.connect(self.db) as db:
            db.execute('UPDATE threads SET archived=1 WHERE id=?', (self.ids[1],))
        with self.assertRaises(ns.SyncError): restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key)
        self.assertFalse((self.backup / 'latest').exists())

    def test_existing_transaction_backup_is_preserved(self):
        chat = self.profile / 'sentinel.jsonl'
        chat.write_text('original history')
        backup.replace_latest(self.profile, self.backup, [self.db.name, chat.name])
        self.assertEqual(restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key,
                                            use_existing_backup=True), 1)
        self.assertEqual((self.backup / 'latest' / chat.name).read_text(), 'original history')
        with sqlite3.connect(self.backup / 'latest' / self.db.name) as db:
            self.assertEqual(db.execute('SELECT parent_thread_id FROM thread_spawn_edges WHERE child_thread_id=?',
                                        (self.ids[1],)).fetchone(), (self.ids[2],))

    def test_existing_transaction_backup_must_be_valid(self):
        backup.replace_latest(self.profile, self.backup, [self.db.name])
        (self.backup / 'latest' / self.db.name).write_bytes(b'corrupted')
        with self.assertRaises(ns.SyncError):
            restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key,
                                use_existing_backup=True)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT parent_thread_id FROM thread_spawn_edges WHERE child_thread_id=?',
                                        (self.ids[1],)).fetchone(), (self.ids[2],))

    def test_rejects_cycle_created_by_merging_with_local_edges(self):
        with sqlite3.connect(self.db) as db:
            db.execute('INSERT INTO thread_spawn_edges VALUES (?,?,?)', (self.ids[1], self.ids[0], 'closed'))
        with self.assertRaises(ns.SyncError):
            restore.apply_graph(self.profile, self.shared, self.backup, self.graph, self.key)
        self.assertFalse((self.backup / 'latest').exists())


if __name__ == '__main__': unittest.main()
