"""Publish a configured set of existing single-page chats sequentially."""
import argparse
from collections import Counter
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time
import uuid

import cosync_lineage as lineage
import cosync_publish_chat as publisher
import cosync_publish_title as titles
import cosync_receive_plan as receive_plan
import nexus_sync as ns


def _fingerprint(path: Path):
    value = path.stat()
    return (value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _add_discovered_file(source, exchange, relative, state):
    if not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_DIRECTORY'):
        raise ns.SyncError('Safe discovery is unavailable on this platform')
    opened = []
    try:
        fd = os.open(exchange, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        opened.append(fd)
        for part in relative.parts[:-1]:
            try:
                os.mkdir(part, 0o700, dir_fd=fd)
            except FileExistsError:
                pass
            fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            opened.append(fd)
        with tempfile.TemporaryDirectory(prefix='.new-chat-', dir=state) as tmp:
            snapshot = Path(tmp) / source.name
            ns._copy_stable_chat(source, snapshot)
            os.link(snapshot, relative.name, dst_dir_fd=fd, follow_symlinks=False)
    finally:
        for fd in reversed(opened):
            os.close(fd)


def discover_slots(config, local_catalog, remote_catalog):
    """Publish new standalone active chats without overwriting exchange files.

    Histories with child sessions require graph publication and remain pending.
    Existing discovered slots continue through the regular conflict policy.
    """
    root = Path(config['discovery_state'])
    profile, exchange = Path(config['profile']), Path(config['exchange'])
    if (not all(p.is_absolute() and p.is_dir() and not p.is_symlink() for p in (root, profile, exchange))
            or root == exchange or exchange in root.parents or root == profile or profile in root.parents):
        raise ns.SyncError('Invalid discovery directories')
    if os.name != 'nt' and (root.stat().st_uid != os.getuid() or stat.S_IMODE(root.stat().st_mode) & 0o077):
        raise ns.SyncError('Discovery state is not private')
    database = profile / 'state_5.sqlite'
    if database.is_symlink() or not database.is_file():
        raise ns.SyncError('Codex state unavailable')
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        active = {r[0] for r in db.execute('SELECT id FROM threads WHERE archived=0')}
        graph = list(db.execute('SELECT parent_thread_id, child_thread_id FROM thread_spawn_edges'))
    dependent = {parent for parent, _ in graph}
    dependent.update(item.get('parent_thread_id') for items in local_catalog.values() for item in items
                     if item.get('parent_thread_id'))
    titles_data = ns._read_title_index(Path(config['title_source']).parent)
    named = {r['id'] for r in ns._parse_title_index(titles_data)} if titles_data is not None else set()
    fixed = {slot['id'] for slot in config['slots']}
    slots, counts = [], Counter()
    for identity, entries in local_catalog.items():
        try:
            if (identity in fixed or identity not in active or identity not in named
                    or receive_plan.source_kind(entries) != 'chat'):
                continue
            if str(uuid.UUID(identity)) != identity or len(entries) != 1 or entries[0]['folder'] != 'sessions':
                counts['DISCOVERY_UNSUPPORTED'] += 1
                continue
            if (entries[0].get('unsupported') == 'PAGINATED'
                    and ns._paged_lineage(entries, identity) != 'PRESENT_UNVERIFIED') or (
                    entries[0].get('unsupported') not in (None, 'PAGINATED') or
                    (entries[0].get('unsupported') is None and entries[0].get('history_base') is not None)):
                counts['DISCOVERY_LINEAGE_PENDING'] += 1
                continue
            state = root / identity
            marker = state / 'slot.json'
            known = marker.is_file() and not marker.is_symlink()
            if identity in remote_catalog and not known:
                continue
            if identity in dependent:
                counts['DISCOVERY_GRAPH_PENDING'] += 1
                if not known:
                    continue
            source = entries[0]['path']
            relative = source.relative_to(profile)
            if relative.parts[0] != 'sessions' or '..' in relative.parts:
                raise ns.SyncError('Invalid discovered chat path')
            destination = exchange / relative
            expected = {'id': identity, 'source': str(source), 'destination': str(destination), 'state_dir': str(state)}
            if state.is_symlink() or (state.exists() and not state.is_dir()):
                raise ns.SyncError('Invalid discovery state')
            state.mkdir(mode=0o700, exist_ok=True)
            if known:
                if marker.stat().st_size > 65536:
                    raise ns.SyncError('Discovery marker too large')
                if json.loads(marker.read_text()) != expected:
                    raise ns.SyncError('Discovered chat changed paths')
            else:
                fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'w') as out:
                    json.dump(expected, out); out.flush(); os.fsync(out.fileno())
            if identity not in remote_catalog:
                _add_discovered_file(source, exchange, relative, state)
                counts['DISCOVERED'] += 1
            slots.append(expected)
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error, ns.SyncError):
            counts['DISCOVERY_RETRY'] += 1
    return slots, dict(counts)


def check_paths(slot, profile, exchange, local_catalog, remote_catalog):
    identity, source, dest = slot['id'], Path(slot['source']), Path(slot['destination'])
    local = local_catalog.get(identity, [])
    remote = remote_catalog.get(identity, [])
    if (len(local) != 1 or len(remote) != 1 or local[0]['folder'] != 'sessions'
            or remote[0]['folder'] != 'sessions' or local[0]['path'] != source or remote[0]['path'] != dest
            or profile not in source.parents or exchange not in dest.parents):
        return 'MULTIPAGE_OR_CHANGED'
    page = lineage._page_id(dest, identity)
    for other, items in remote_catalog.items():
        if other != identity and any(isinstance(item.get('history_base'), dict)
                                     and item['history_base'].get('thread_id') == page for item in items):
            return 'REFERENCED_PAGE'
    return None


def run_once(config, cache):
    profile, exchange = Path(config['profile']), Path(config['exchange'])
    local_catalog = ns._chat_catalog(profile, include_unsupported=True)
    remote_catalog = ns._chat_catalog(exchange, include_unsupported=True)
    counts = Counter()
    for slot in config['slots']:
        source, dest = Path(slot['source']), Path(slot['destination'])
        try:
            blocked = check_paths(slot, profile, exchange, local_catalog, remote_catalog)
            if blocked:
                cache.pop(slot['id'], None)
                counts[blocked] += 1
                continue
            before = (_fingerprint(source), _fingerprint(dest))
            cached = cache.get(slot['id'])
            if cached and cached[0] == before:
                result = cached[1]
            else:
                result = publisher.publish(source, dest, Path(slot['state_dir']), replace_initial=True,
                    prefer_more=True, path_roots=(config['local_root'], config['remote_root']))
                after = (_fingerprint(source), _fingerprint(dest))
                if before[0] == after[0] and result in ('PUBLISHED', 'UNCHANGED', 'WAITING_REMOTE', 'CONFLICT', 'TIE'):
                    cache[slot['id']] = (after, 'UNCHANGED' if result == 'PUBLISHED' else result, result == 'PUBLISHED')
                else:
                    cache.pop(slot['id'], None)
            counts[result] += 1
        except (OSError, ValueError, ns.SyncError):
            cache.pop(slot['id'], None)
            counts['RETRY'] += 1
    return dict(counts)


def run_titles(config, cache=None):
    counts = Counter()
    source, destination, lock = (Path(config[key]) for key in ('title_source', 'title_destination', 'title_lock'))
    for slot in config['slots']:
        try:
            cached = cache.get(slot['id']) if cache is not None else None
            if cache is not None and (not cached or cached[1] in ('WAITING_REMOTE', 'CONFLICT', 'TIE')):
                counts['WAITING_HISTORY'] += 1
                continue
            state = Path(slot['state_dir']) / 'title'
            state.mkdir(mode=0o700, exist_ok=True)
            result = titles.publish_title(source, destination, state, slot['id'], replace_initial=True,
                                          prefer_local=bool(cached and len(cached) > 2 and cached[2]), lock_file=lock)
            if cached and len(cached) > 2 and result in ('PUBLISHED', 'UNCHANGED'):
                cache[slot['id']] = (cached[0], cached[1], False)
            counts[result] += 1
        except (OSError, ValueError, ns.SyncError):
            counts['RETRY'] += 1
    return dict(counts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    if not args.config.is_absolute() or not 10 <= args.interval <= 3600: parser.error('Invalid configuration')
    cache = {}
    while True:
        try:
            info = args.config.lstat()
            if (not stat.S_ISREG(info.st_mode) or args.config.is_symlink() or info.st_size > 1024 * 1024
                    or (os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077))):
                raise ns.SyncError('Invalid publisher configuration')
            config = json.loads(args.config.read_text())
            discovery = {}
            if config.get('discover_new') is True:
                extra, discovery = discover_slots(config,
                    ns._chat_catalog(Path(config['profile']), include_unsupported=True),
                    ns._chat_catalog(Path(config['exchange']), include_unsupported=True))
                config = {**config, 'slots': [*config['slots'], *extra]}
            counts = run_once(config, cache)
            title_counts = (run_titles(config, cache) if all(key in config for key in
                            ('title_source', 'title_destination', 'title_lock')) else {})
            fd, temp = tempfile.mkstemp(prefix='.health-', dir=args.config.parent)
            try:
                with os.fdopen(fd, 'w') as stream:
                    json.dump({'counts': counts, 'titles': title_counts, 'discovery': discovery,
                               'updated_at_ms': int(time.time() * 1000)}, stream)
                os.replace(temp, args.config.parent / 'health.json')
            finally:
                if os.path.exists(temp): os.unlink(temp)
        except (OSError, ValueError, TypeError, KeyError, ns.SyncError):
            pass
        time.sleep(args.interval)


if __name__ == '__main__': main()
