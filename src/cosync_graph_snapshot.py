"""Create a minimal read-only snapshot of active Codex spawn relationships.

The snapshot contains thread IDs and the source's open/closed edge status,
not chat contents, names, paths, credentials, or the SQLite database itself.
"""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import time
import uuid


class GraphError(Exception):
    pass


def _uuid(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def read_graph(database: Path) -> dict:
    if (not database.is_absolute() or database.name != 'state_5.sqlite'
            or database.is_symlink() or not database.is_file()):
        raise GraphError('Unsupported Codex state database')
    try:
        db = sqlite3.connect(database.resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5)
        try:
            db.execute('PRAGMA query_only = ON')
            db.execute('BEGIN')
            columns = {row[1] for row in db.execute('PRAGMA table_info(thread_spawn_edges)')}
            threads = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
            if columns != {'parent_thread_id', 'child_thread_id', 'status'} or not {'id', 'archived'} <= threads:
                raise GraphError('Unsupported Codex relationship schema')
            rows = db.execute('SELECT e.parent_thread_id, e.child_thread_id, e.status, '
                'p.id, p.archived, c.id, c.archived '
                'FROM thread_spawn_edges e LEFT JOIN threads p ON p.id = e.parent_thread_id '
                'LEFT JOIN threads c ON c.id = e.child_thread_id '
                'ORDER BY e.parent_thread_id, e.child_thread_id LIMIT 100001').fetchall()
            if len(rows) > 100000:
                raise GraphError('Codex relationship snapshot is too large')
        finally:
            db.close()
    except (sqlite3.Error, OSError) as exc:
        raise GraphError('Could not read a consistent Codex relationship snapshot') from exc
    seen = set()
    edges = []
    omitted_archived = omitted_missing = 0
    for parent, child, status, parent_id, parent_archived, child_id, child_archived in rows:
        if (not _uuid(parent) or not _uuid(child) or parent == child or child in seen
                or status not in ('open', 'closed')):
            raise GraphError('Unsupported Codex relationship data')
        seen.add(child)
        if parent_id is None or child_id is None:
            omitted_missing += 1
            continue
        if parent_archived not in (0, 1) or child_archived not in (0, 1):
            raise GraphError('Unsupported Codex archive state')
        if parent_archived or child_archived:
            omitted_archived += 1
            continue
        edges.append({'parent': parent, 'child': child, 'status': status})
    parents = {edge['child']: edge['parent'] for edge in edges}
    resolved = set()
    for child in parents:
        visited, node = set(), child
        while node in parents and node not in resolved:
            if node in visited:
                raise GraphError('Cyclic Codex relationships')
            visited.add(node)
            node = parents[node]
        resolved.update(visited)
    return {'format': 'cosyncx-active-graph-v1', 'created_at_ms': int(time.time() * 1000), 'edges': edges,
            'omitted_archived': omitted_archived, 'omitted_missing': omitted_missing}


def write_graph(database: Path, destination: Path) -> int:
    if (not destination.is_absolute() or destination.is_symlink() or destination.parent.is_symlink()
            or not destination.parent.is_dir()):
        raise GraphError('Graph destination is unavailable')
    profile = database.parent.resolve(strict=True)
    parent = destination.parent.resolve(strict=True)
    if parent == profile or profile in parent.parents or parent in profile.parents:
        raise GraphError('Graph destination must be outside the Codex profile')
    info = parent.stat()
    if os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise GraphError('Graph destination directory is not private')
    graph = read_graph(database)
    if destination.is_file() and not destination.is_symlink() and destination.stat().st_size <= 32 * 1024 * 1024:
        try:
            previous = json.loads(destination.read_bytes())
            if (isinstance(previous, dict) and previous.get('format') == graph['format']
                    and all(previous.get(key) == graph[key] for key in ('edges', 'omitted_archived', 'omitted_missing'))):
                return len(graph['edges'])
        except (OSError, ValueError, TypeError):
            pass
    fd, temp = tempfile.mkstemp(prefix='.cosyncx-graph-', dir=parent)
    temp = Path(temp)
    try:
        if hasattr(os, 'fchmod'):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            json.dump(graph, output, ensure_ascii=True, separators=(',', ':'))
            output.flush()
            os.fsync(output.fileno())
        if destination.is_symlink():
            raise GraphError('Graph destination is a symlink')
        os.replace(temp, destination)
    finally:
        temp.unlink(missing_ok=True)
    return len(graph['edges'])


def main(argv=None):
    parser = argparse.ArgumentParser(description='Export active Codex thread relationships without chat contents')
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps({'edges': write_graph(args.database, args.output)}))
        return 0
    except (GraphError, OSError, ValueError):
        print('CoSyncX: could not export Codex relationships', file=sys.stderr)
        return 1
