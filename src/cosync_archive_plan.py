"""Read-only impact analysis before unarchiving a Codex conversation.

Codex can unarchive descendants as well as the requested thread. The caller
must re-run this under its offline coordination before creating one complete
backup; this module does not lock or change a profile.
"""
from collections import defaultdict
import argparse
import json
from pathlib import Path
import sqlite3
import sys

import cosync_graph_snapshot as graph
import nexus_sync as ns


def archive_impact(profile: Path, roots: list[str]) -> dict:
    if (not profile.is_absolute() or profile.is_symlink() or not profile.is_dir()
            or not roots or len(set(roots)) != len(roots) or any(not graph._uuid(x) for x in roots)):
        raise ns.SyncError('Invalid archive impact request')
    profile = profile.resolve(strict=True)
    database = profile / 'state_5.sqlite'
    if database.is_symlink() or not database.is_file():
        raise ns.SyncError('Codex state database unavailable')
    try:
        with sqlite3.connect(database.resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5) as db:
            db.execute('PRAGMA query_only = ON')
            db.execute('BEGIN')
            edge_columns = {row[1] for row in db.execute('PRAGMA table_info(thread_spawn_edges)')}
            thread_columns = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
            if edge_columns != {'parent_thread_id', 'child_thread_id', 'status'} or not {'id', 'archived'} <= thread_columns:
                raise ns.SyncError('Unsupported archive database schema')
            edges = db.execute('SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges LIMIT 100001').fetchall()
            threads = db.execute('SELECT id, archived FROM threads LIMIT 100001').fetchall()
    except sqlite3.Error as exc:
        raise ns.SyncError('Cannot read archive impact') from exc
    if len(edges) > 100000 or len(threads) > 100000:
        raise ns.SyncError('Archive impact exceeds supported limits')
    states = {}
    for identity, archived in threads:
        if not graph._uuid(identity) or archived not in (0, 1) or identity in states:
            raise ns.SyncError('Invalid archive thread data')
        states[identity] = archived
    children = defaultdict(list)
    parent_of = {}
    for parent, child, status in edges:
        if (not graph._uuid(parent) or not graph._uuid(child) or parent == child or child in parent_of
                or status not in ('open', 'closed') or parent not in states or child not in states):
            raise ns.SyncError('Invalid or incomplete archive relationships')
        parent_of[child] = parent
        children[parent].append(child)
    selected, pending = set(), list(roots)
    if any(root not in states for root in roots):
        raise ns.SyncError('Archive thread missing from state')
    while pending:
        identity = pending.pop()
        if identity in selected:
            continue
        selected.add(identity)
        pending.extend(children[identity])
    resolved = set()
    for identity in selected:
        chain, current = set(), identity
        while current in parent_of and current not in resolved:
            if current in chain:
                raise ns.SyncError('Cyclic archive relationships')
            chain.add(current)
            current = parent_of[current]
        resolved.update(chain)
    catalog = ns._chat_catalog(profile, include_unsupported=True)
    if any(identity not in catalog for identity in selected):
        raise ns.SyncError('Archive history unavailable')
    files = sorted({item['path'].relative_to(profile).as_posix()
                    for identity in selected for item in catalog[identity]})
    return {'threads': selected, 'files': files, 'archived': sum(states[x] for x in selected)}


def impact_counts(profile: Path, roots: list[str]) -> dict:
    impact = archive_impact(profile, roots)
    return {'threads': len(impact['threads']), 'history_files': len(impact['files']),
            'archived_threads': impact['archived'], 'read_only': True}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Count chats and files affected by unarchiving')
    parser.add_argument('--profile', required=True, type=Path)
    parser.add_argument('--thread-id', required=True, action='append')
    args = parser.parse_args(argv)
    try:
        print(json.dumps(impact_counts(args.profile, args.thread_id), sort_keys=True))
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError):
        print('CoSyncX: archive impact could not be safely determined', file=sys.stderr)
        return 1
