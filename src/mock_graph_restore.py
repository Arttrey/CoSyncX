"""Experimental relationship restore, restricted to authenticated mock profiles."""
import json
import hashlib
from pathlib import Path
import sqlite3

import cosync_graph_snapshot as graph
import cosync_restore_backup as backup
import mock_chat_sync as mock
import mock_journal
import nexus_sync as ns


def apply_graph(profile: Path, shared: Path, backup_root: Path, graph_file: Path, key: bytes,
                *, use_existing_backup: bool = False) -> int:
    profile = mock._validate_profile(profile, key)
    shared, backup_root, graph_file = (mock._mock_path(path) for path in (shared, backup_root, graph_file))
    if (not shared.is_dir() or not backup_root.is_dir() or graph_file.is_symlink() or not graph_file.is_file()
            or graph_file.stat().st_size > 32 * 1024 * 1024
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock graph paths')
    mock_journal.ensure_clear(backup_root)
    return _apply_graph(profile, backup_root, graph_file, use_existing_backup=use_existing_backup)


def _apply_graph(profile: Path, backup_root: Path, graph_file: Path, *, use_existing_backup: bool = False,
                 ignore_busy: frozenset[str] = frozenset()) -> int:
    """Apply validated links after the caller has guarded and backed up the profile."""
    before = graph_file.stat()
    raw = graph_file.read_bytes()
    after = graph_file.stat()
    if ((before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino)
            != (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)
            or len(raw) != before.st_size):
        raise ns.SyncError('Mock graph changed during reading')
    value = json.loads(raw)
    if (not isinstance(value, dict) or value.get('format') != 'cosyncx-active-graph-v1'
            or type(value.get('omitted_missing')) is not int or value['omitted_missing'] != 0
            or type(value.get('omitted_archived')) is not int or value['omitted_archived'] < 0
            or not isinstance(value.get('edges'), list)):
        raise ns.SyncError('Mock graph is incomplete')
    edges, seen = [], set()
    for item in value['edges']:
        if (not isinstance(item, dict) or set(item) != {'parent', 'child', 'status'}
                or not graph._uuid(item['parent']) or not graph._uuid(item['child'])
                or item['parent'] == item['child'] or item['child'] in seen
                or item['status'] not in ('open', 'closed')):
            raise ns.SyncError('Invalid mock graph edge')
        seen.add(item['child'])
        edges.append((item['parent'], item['child'], item['status']))
    parents = {child: parent for parent, child, _ in edges}
    resolved = set()
    for child in parents:
        visited, node = set(), child
        while node in parents and node not in resolved:
            if node in visited:
                raise ns.SyncError('Cyclic mock graph')
            visited.add(node)
            node = parents[node]
        resolved.update(visited)
    if not edges:
        return 0
    database = profile / 'state_5.sqlite'
    if database.is_symlink() or not database.is_file():
        raise ns.SyncError('Mock state database is unavailable')
    with mock._mock_thread_lock(profile, edges[0][0], ignore_busy=ignore_busy):
        with sqlite3.connect(database, timeout=1) as db:
            db.execute('BEGIN IMMEDIATE')
            cols = {row[1] for row in db.execute('PRAGMA table_info(thread_spawn_edges)')}
            thread_cols = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
            if cols != {'parent_thread_id', 'child_thread_id', 'status'} or not {'id', 'archived'} <= thread_cols:
                raise ns.SyncError('Unsupported mock state schema')
            for parent, child, _ in edges:
                for identity in (parent, child):
                    row = db.execute('SELECT archived FROM threads WHERE id = ?', (identity,)).fetchone()
                    if row != (0,):
                        raise ns.SyncError('Mock graph references a missing or archived chat')
            changes = []
            combined = {child: parent for parent, child in db.execute(
                'SELECT parent_thread_id, child_thread_id FROM thread_spawn_edges')}
            for parent, child, status in edges:
                row = db.execute('SELECT parent_thread_id, status FROM thread_spawn_edges WHERE child_thread_id = ?',
                                 (child,)).fetchone()
                if row != (parent, status):
                    changes.append((parent, child, status))
                combined[child] = parent
            resolved = set()
            for child in combined:
                visited, node = set(), child
                while node in combined and node not in resolved:
                    if node in visited:
                        raise ns.SyncError('Mock graph would introduce a cycle')
                    visited.add(node)
                    node = combined[node]
                resolved.update(visited)
            if not changes:
                return 0
            if use_existing_backup:
                latest = backup_root / 'latest'
                manifest_path = latest / 'manifest.json'
                if (latest.is_symlink() or not latest.is_dir() or manifest_path.is_symlink()
                        or not manifest_path.is_file() or manifest_path.stat().st_size > 1024 * 1024):
                    raise ns.SyncError('Mock transaction backup unavailable')
                manifest = json.loads(manifest_path.read_bytes())
                try:
                    saved = backup._resolve_file(latest, database.name)
                except backup.BackupError as exc:
                    raise ns.SyncError('Mock transaction database backup unavailable') from exc
                if (not isinstance(manifest, dict) or manifest.get('version') != 1
                        or not isinstance(manifest.get('files'), dict)
                        or manifest['files'].get(database.name) != ns._digest(saved)):
                    raise ns.SyncError('Mock transaction database backup mismatch')
            else:
                backup.replace_latest(profile, backup_root, [database.name])
            if (graph_file.stat().st_size != before.st_size or ns._digest(graph_file) != hashlib.sha256(raw).hexdigest()):
                raise ns.SyncError('Mock graph changed during backup')
            db.executemany('INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) VALUES (?, ?, ?) '
                           'ON CONFLICT(child_thread_id) DO UPDATE SET parent_thread_id=excluded.parent_thread_id, '
                           'status=excluded.status', changes)
            return len(changes)
