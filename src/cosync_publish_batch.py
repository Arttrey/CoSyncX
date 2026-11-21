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
from urllib.parse import quote

import cosync_engine
import cosync_lineage as lineage
import cosync_graph_snapshot as graph_schema
import cosync_publish_chat as publisher
import cosync_publish_title as titles
import cosync_receive_plan as receive_plan
import nexus_sync as ns


def _fingerprint(path: Path):
    value = path.stat()
    return (value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _graph_ready(config):
    """Confirm the configured graph is shared in the intended direction."""
    try:
        client = cosync_engine.ExistingEngine(config['discovery_syncthing_url'],
            Path(config['discovery_syncthing_config']), config_xml=True)
        folder = config['discovery_graph_folder']
        peer = config['discovery_peer']
        if not isinstance(folder, str) or not folder or not isinstance(peer, str) or not peer:
            return False
        data = client._get('config/folders/' + quote(folder, safe=''))
        if (not isinstance(data, dict) or data.get('id') != folder or data.get('type') != 'sendreceive'
                or not isinstance(data.get('path'), str)
                or Path(data['path']).resolve(strict=True) != Path(config['discovery_graph']).resolve(strict=True)
                or not isinstance(data.get('devices'), list)):
            return False
        devices = [x for x in data['devices'] if isinstance(x, dict) and x.get('deviceID') == peer]
        if len(devices) != 1 or (config.get('discovery_require_encryption') is True and not devices[0].get('encryptionPassword')):
            return False
        status = client._get('db/status?folder=' + quote(folder, safe=''))
        return (isinstance(status, dict) and not status.get('error') and not status.get('invalid')
                and status.get('errors') == 0 and status.get('pullErrors') == 0)
    except (cosync_engine.EngineError, OSError, ValueError, TypeError, KeyError):
        return False


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


def _add_discovered_slot(root, profile, exchange, identity, entries, remote_catalog, *, kind='chat', adopt=False):
    if str(uuid.UUID(identity)) != identity or len(entries) != 1 or entries[0]['folder'] != 'sessions':
        raise ns.SyncError('Unsupported discovered history')
    if (entries[0].get('unsupported') == 'PAGINATED'
            and ns._paged_lineage(entries, identity) != 'PRESENT_UNVERIFIED') or (
            entries[0].get('unsupported') not in (None, 'PAGINATED') or
            (entries[0].get('unsupported') is None and entries[0].get('history_base') is not None)):
        raise ns.SyncError('Incomplete discovered lineage')
    state, source = root / identity, entries[0]['path']
    marker = state / 'slot.json'
    known = marker.is_file() and not marker.is_symlink()
    relative = source.relative_to(profile)
    if relative.parts[0] != 'sessions' or '..' in relative.parts:
        raise ns.SyncError('Invalid discovered chat path')
    destination = exchange / relative
    if identity in remote_catalog and not known:
        remote = remote_catalog[identity]
        if not adopt or len(remote) != 1 or remote[0]['folder'] != 'sessions' or remote[0]['path'] != destination:
            raise ns.SyncError('Received history already exists')
    expected = {'id': identity, 'source': str(source), 'destination': str(destination), 'state_dir': str(state)}
    if kind == 'subagent': expected['kind'] = kind
    if state.is_symlink() or (state.exists() and not state.is_dir()):
        raise ns.SyncError('Invalid discovery state')
    state.mkdir(mode=0o700, exist_ok=True)
    if known:
        if marker.stat().st_size > 65536 or json.loads(marker.read_text()) != expected:
            raise ns.SyncError('Discovered chat changed paths')
    else:
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'w') as out:
            json.dump(expected, out); out.flush(); os.fsync(out.fileno())
    created = identity not in remote_catalog
    if created:
        _add_discovered_file(source, exchange, relative, state)
    return expected, created


def _write_device_graph(directory, device_id, edges):
    """Write this device's graph separately from other devices' graphs."""
    if not isinstance(device_id, str) or len(device_id) != 32 or any(x not in '0123456789abcdef' for x in device_id):
        raise ns.SyncError('Invalid discovery device ID')
    directory = Path(directory)
    if not directory.is_absolute() or directory.is_symlink() or not directory.is_dir():
        raise ns.SyncError('Invalid shared graph directory')
    devices = directory / 'devices'
    if devices.is_symlink() or (devices.exists() and not devices.is_dir()):
        raise ns.SyncError('Invalid device graph directory')
    devices.mkdir(mode=0o700, exist_ok=True)
    path = devices / (device_id + '.json')
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ns.SyncError('Invalid device graph')
    existing = {}
    if path.is_file():
        if path.stat().st_size > 32 * 1024 * 1024: raise ns.SyncError('Device graph too large')
        old = json.loads(path.read_bytes())
        if not isinstance(old, dict) or old.get('format') != 'cosyncx-active-graph-v1' or not isinstance(old.get('edges'), list):
            raise ns.SyncError('Invalid device graph')
        for edge in old['edges']:
            if (not isinstance(edge, dict) or set(edge) != {'parent','child','status'}
                    or not graph_schema._uuid(edge['parent']) or not graph_schema._uuid(edge['child'])
                    or edge['parent'] == edge['child'] or edge['status'] not in ('open','closed')
                    or edge['child'] in existing):
                raise ns.SyncError('Invalid device graph edge')
            existing[edge['child']] = edge
    before = dict(existing)
    for edge in edges:
        previous = existing.get(edge['child'])
        if previous is not None and previous['parent'] != edge['parent']:
            raise ns.SyncError('Conflicting device graph parent')
        existing[edge['child']] = edge
    if existing == before:
        return
    value = {'format':'cosyncx-active-graph-v1','edges':[existing[x] for x in sorted(existing)],
             'omitted_missing':0,'omitted_archived':0}
    fd, temporary = tempfile.mkstemp(prefix='.graph-', dir=devices)
    try:
        with os.fdopen(fd,'w') as out:
            json.dump(value,out,sort_keys=True,separators=(',',':'));out.flush();os.fsync(out.fileno())
        if path.is_symlink(): raise ns.SyncError('Device graph changed')
        os.replace(temporary,path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def discover_slots(config, local_catalog, remote_catalog):
    """Publish newly discovered active chats and eligible child sessions."""
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
        thread_states = {r[0]: r[1] for r in db.execute('SELECT id, archived FROM threads')}
        active = {identity for identity, archived in thread_states.items() if archived == 0}
        graph_columns = {r[1] for r in db.execute('PRAGMA table_info(thread_spawn_edges)')}
        graph = (list(db.execute('SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges'))
                 if 'status' in graph_columns else
                 [(p, c, None) for p, c in db.execute('SELECT parent_thread_id, child_thread_id FROM thread_spawn_edges')])
    children = {}
    for parent, child, _ in graph:
        children.setdefault(parent, set()).add(child)
    for child, items in local_catalog.items():
        for item in items:
            parent = item.get('parent_thread_id')
            if parent: children.setdefault(parent, set()).add(child)
    titles_data = ns._read_title_index(Path(config['title_source']).parent)
    named = {r['id'] for r in ns._parse_title_index(titles_data)} if titles_data is not None else set()
    fixed = {slot['id'] for slot in config['slots']}
    excluded_roots = set(config.get('discovery_excluded_roots', ()))
    adopt_existing = config.get('discovery_adopt_existing') is True
    slots, counts = [], Counter()
    for identity, entries in local_catalog.items():
        try:
            if (identity not in active or identity not in named
                    or receive_plan.source_kind(entries) != 'chat'):
                continue
            state = root / identity
            marker = state / 'slot.json'
            known = marker.is_file() and not marker.is_symlink()
            separately_managed = identity in fixed or identity in excluded_roots
            adopt = (adopt_existing and identity in remote_catalog and not known
                     and not separately_managed)
            existing_root = separately_managed or (identity in remote_catalog and not known and not adopt)
            if existing_root and not children.get(identity):
                continue
            descendants, pending, queue = [], False, [identity]
            seen = {identity}
            while queue:
                parent = queue.pop()
                for child in sorted(children.get(parent, set())):
                    if child in seen:
                        pending = True; continue
                    seen.add(child)
                    if thread_states.get(child) == 1:
                        continue
                    child_entries = local_catalog.get(child, [])
                    if (child not in active or not child_entries or receive_plan.source_kind(child_entries) != 'subagent'
                            or any(item.get('parent_thread_id') != parent for item in child_entries)):
                        pending = True; continue
                    queue.append(child)
                    descendants.append((child, child_entries))
            graph_enabled = bool(config.get('discovery_graph') and config.get('discovery_device'))
            if descendants and not graph_enabled: pending = True
            if any(status not in ('open', 'closed') for parent, child, status in graph if parent in seen and child in seen):
                pending = True
            if pending:
                counts['DISCOVERY_GRAPH_PENDING'] += 1
                if not known and not existing_root:
                    continue
            published = {identity}
            if graph_enabled:
                child_slots = []
                for child, child_entries in descendants:
                    parent = child_entries[0].get('parent_thread_id')
                    if parent and parent not in published and parent not in remote_catalog:
                        counts['DISCOVERY_GRAPH_PENDING'] += 1
                        continue
                    try:
                        child_slot, created = _add_discovered_slot(root, profile, exchange, child, child_entries,
                                                                    remote_catalog, kind='subagent', adopt=True)
                    except (OSError, ValueError, TypeError, KeyError, ns.SyncError):
                        pending = True
                        counts['DISCOVERY_GRAPH_PENDING'] += 1
                        continue
                    child_slots.append(child_slot)
                    published.add(child)
                    if created: counts['DISCOVERED_CHILD'] += 1
                slots.extend(child_slots)
                if pending and not known and not existing_root:
                    continue
            elif descendants and not known and not existing_root:
                continue
            if not existing_root:
                expected, created = _add_discovered_slot(root, profile, exchange, identity, entries, remote_catalog,
                                                         adopt=adopt)
                if created: counts['DISCOVERED'] += 1
                slots.append(expected)
            if descendants and graph_enabled:
                edges = [{'parent':p,'child':c,'status':status} for p,c,status in graph
                         if p in published and c in published and status in ('open','closed')]
                with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
                    latest = set(db.execute('SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges'))
                if any((e['parent'],e['child'],e['status']) not in latest for e in edges):
                    raise ns.SyncError('Relationships changed during discovery')
                _write_device_graph(config['discovery_graph'], config['discovery_device'], edges)
        except ns.SyncError as exc:
            counts['DISCOVERY_LINEAGE_PENDING' if str(exc) == 'Incomplete discovered lineage' else 'DISCOVERY_RETRY'] += 1
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
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
                    prefer_more=True, path_roots=(config['local_root'], config['remote_root']),
                    subagent=slot.get('kind') == 'subagent',
                    prefer_local_on_tie=config.get('prefer_local_on_tie') is True)
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
        if slot.get('kind') == 'subagent':
            continue
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
                graph_unavailable = bool(config.get('discovery_graph')) and not _graph_ready(config)
                if graph_unavailable:
                    config = {key:value for key,value in config.items() if key not in ('discovery_graph','discovery_device')}
                extra, discovery = discover_slots(config,
                    ns._chat_catalog(Path(config['profile']), include_unsupported=True),
                    ns._chat_catalog(Path(config['exchange']), include_unsupported=True))
                if graph_unavailable: discovery['GRAPH_UNAVAILABLE'] = 1
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
