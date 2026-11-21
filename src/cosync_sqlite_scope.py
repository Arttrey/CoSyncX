"""Verify a staged Codex database does not change unrelated conversations."""
from contextlib import closing
from pathlib import Path
import re
import sqlite3
import uuid

import nexus_sync as ns


def verify_unaffected(original: Path, candidate: Path, impacted: set[str], *, protected: set[str] = frozenset()) -> None:
    """Fail closed on schema differences or changes outside selected thread IDs.

    Both databases must be closed snapshots; this routine does not edit them.
    """
    if (not isinstance(impacted, (set, frozenset)) or not impacted
            or not isinstance(protected, (set, frozenset)) or impacted & protected):
        raise ns.SyncError('Invalid protected or impacted chat IDs')
    try:
        if any(not isinstance(value, str) or str(uuid.UUID(value)) != value for value in impacted | protected):
            raise ns.SyncError('Invalid protected or impacted chat IDs')
    except (ValueError, AttributeError) as exc:
        raise ns.SyncError('Invalid protected or impacted chat IDs') from exc
    original, candidate = Path(original), Path(candidate)
    if any(path.is_symlink() or not path.is_file() for path in (original, candidate)):
        raise ns.SyncError('Database snapshot unavailable')
    if len(impacted) > 100000 or len(protected) > 100000:
        raise ns.SyncError('Too many impacted chats to verify')
    try:
        with closing(sqlite3.connect(original.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('ATTACH DATABASE ? AS candidate', (candidate.resolve().as_uri() + '?mode=ro',))
            db.execute('PRAGMA temp_store = MEMORY')
            db.execute('CREATE TEMP TABLE impacted_ids (id TEXT PRIMARY KEY)')
            db.execute('CREATE TEMP TABLE protected_ids (id TEXT PRIMARY KEY)')
            db.executemany('INSERT INTO impacted_ids VALUES (?)', ((value,) for value in impacted))
            db.executemany('INSERT INTO protected_ids VALUES (?)', ((value,) for value in protected))
            db.execute('PRAGMA query_only = ON')
            def objects(schema):
                return db.execute(f"SELECT type, name, tbl_name, sql FROM {schema}.sqlite_master "
                                  "WHERE type IN ('table', 'index', 'trigger', 'view') ORDER BY type, name").fetchall()
            if objects('main') != objects('candidate'):
                raise ns.SyncError('Database schema changed')
            tables = [row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type='table'")]
            for name in tables:
                if not re.fullmatch(r'[A-Za-z0-9_]+', name):
                    raise ns.SyncError('Unsupported database table')
                columns = [row[1] for row in db.execute(f'PRAGMA main.table_info("{name}")')]
                if name == 'threads':
                    if 'id' not in columns: raise ns.SyncError('Unsupported threads schema')
                    where = ' WHERE (id IS NULL OR id NOT IN (SELECT id FROM temp.impacted_ids))'
                elif name == 'thread_spawn_edges':
                    if not {'parent_thread_id', 'child_thread_id'} <= set(columns):
                        raise ns.SyncError('Unsupported relationships schema')
                    where = (' WHERE (parent_thread_id IS NULL OR parent_thread_id NOT IN (SELECT id FROM temp.impacted_ids))'
                             ' AND (child_thread_id IS NULL OR child_thread_id NOT IN (SELECT id FROM temp.impacted_ids))')
                elif 'thread_id' in columns:
                    where = ' WHERE (thread_id IS NULL OR thread_id NOT IN (SELECT id FROM temp.impacted_ids))'
                else:
                    where = ''
                if not columns:
                    raise ns.SyncError('Unsupported empty database table')
                quoted_columns = ['"' + column.replace('"', '""') + '"' for column in columns]
                selected_columns = ','.join(part for column in quoted_columns
                                            for part in (column + ' COLLATE BINARY', 'typeof(' + column + ')'))
                def differences(first, second, clause):
                    return (f'SELECT {selected_columns}, count(*) FROM {first}."{name}"{clause} '
                            f'GROUP BY {selected_columns} EXCEPT '
                            f'SELECT {selected_columns}, count(*) FROM {second}."{name}"{clause} '
                            f'GROUP BY {selected_columns} LIMIT 1')
                for first, second in (('main', 'candidate'), ('candidate', 'main')):
                    sql = differences(first, second, where)
                    if db.execute(sql).fetchone() is not None:
                        raise ns.SyncError('Database contains changes outside restored chats')
                    if name == 'thread_spawn_edges' and protected:
                        clause = (' WHERE parent_thread_id IN (SELECT id FROM temp.protected_ids)'
                                  ' OR child_thread_id IN (SELECT id FROM temp.protected_ids)')
                        sql = differences(first, second, clause)
                        if db.execute(sql).fetchone() is not None:
                            raise ns.SyncError('Protected chat relationship changed')
    except sqlite3.Error as exc:
        raise ns.SyncError('Could not verify database changes') from exc
