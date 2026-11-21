from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

import cosync_sqlite_scope as scope
import nexus_sync as ns


class DatabaseScopeTests(unittest.TestCase):
    def test_only_selected_chats_and_their_relationships_may_change(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-scope-') as tmp:
            old, new = Path(tmp) / 'old.sqlite', Path(tmp) / 'new.sqlite'
            selected = '019b0000-0000-7000-8000-000000000001'
            protected = '019b0000-0000-7000-8000-000000000002'
            other = '019b0000-0000-7000-8000-000000000003'
            with sqlite3.connect(old) as db:
                db.execute('CREATE TABLE threads (id TEXT, title TEXT)')
                db.execute('CREATE TABLE thread_items (thread_id TEXT, item TEXT)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT, status TEXT)')
                db.execute('CREATE TABLE projects (id TEXT, name TEXT COLLATE NOCASE)')
                db.executemany('INSERT INTO threads VALUES (?, ?)', [(selected, 'old'), (protected, 'keep')])
                db.executemany('INSERT INTO thread_items VALUES (?, ?)', [(selected, 'old'), (protected, 'keep'), (None, 'global')])
                db.execute('INSERT INTO projects VALUES (?, ?)', ('project', 'keep'))
                db.execute('INSERT INTO projects VALUES (?, ?)', ('project', 'keep'))
            shutil.copyfile(old, new)
            with sqlite3.connect(new) as db:
                db.execute('UPDATE threads SET title=? WHERE id=?', ('new', selected))
                db.execute('UPDATE thread_items SET item=? WHERE thread_id=?', ('new', selected))
                db.execute('INSERT INTO thread_spawn_edges VALUES (?, ?, ?)', (selected, other, 'closed'))
            scope.verify_unaffected(old, new, {selected}, protected={protected})
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected, protected}, protected={protected})
            with sqlite3.connect(new) as db:
                db.execute('INSERT INTO thread_spawn_edges VALUES (?, ?, ?)', (selected, protected, 'closed'))
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected}, protected={protected})
            with sqlite3.connect(new) as db:
                db.execute('DELETE FROM thread_spawn_edges WHERE child_thread_id=?', (protected,))
                db.execute('UPDATE thread_items SET item=? WHERE thread_id IS NULL', ('changed',))
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected}, protected={protected})
            with sqlite3.connect(new) as db:
                db.execute('UPDATE thread_items SET item=? WHERE thread_id IS NULL', ('global',))
                db.execute('UPDATE threads SET title=? WHERE id=?', ('changed', protected))
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected}, protected={protected})
            with sqlite3.connect(new) as db:
                db.execute('UPDATE threads SET title=? WHERE id=?', ('keep', protected))
                db.execute('DELETE FROM projects WHERE rowid = (SELECT min(rowid) FROM projects)')
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected}, protected={protected})
            with sqlite3.connect(new) as db:
                db.execute('INSERT INTO projects VALUES (?, ?)', ('project', 'keep'))
                db.execute('UPDATE projects SET name=?', ('KEEP',))
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {selected}, protected={protected})

    def test_unknown_schema_change_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-scope-schema-') as tmp:
            old, new = Path(tmp) / 'old.sqlite', Path(tmp) / 'new.sqlite'
            with sqlite3.connect(old) as db: db.execute('CREATE TABLE threads (id TEXT)')
            shutil.copyfile(old, new)
            with sqlite3.connect(new) as db: db.execute('CREATE TABLE unexpected (id TEXT)')
            with self.assertRaises(ns.SyncError):
                scope.verify_unaffected(old, new, {'019b0000-0000-7000-8000-000000000001'})


if __name__ == '__main__':
    unittest.main()
