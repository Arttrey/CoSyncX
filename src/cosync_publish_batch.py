"""Publish a configured set of existing single-page chats sequentially."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import stat
import tempfile
import time

import cosync_lineage as lineage
import cosync_publish_chat as publisher
import cosync_publish_title as titles
import nexus_sync as ns


def _fingerprint(path: Path):
    value = path.stat()
    return (value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


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
                counts[blocked] += 1
                continue
            before = (_fingerprint(source), _fingerprint(dest))
            cached = cache.get(slot['id'])
            if cached and cached[0] == before:
                result = cached[1]
            else:
                result = publisher.publish(source, dest, Path(slot['state_dir']), replace_initial=True,
                    prefer_local=True, path_roots=(config['local_root'], config['remote_root']))
                after = (_fingerprint(source), _fingerprint(dest))
                if before[0] == after[0] and result in ('PUBLISHED', 'UNCHANGED', 'WAITING_REMOTE', 'CONFLICT'):
                    cache[slot['id']] = (after, 'UNCHANGED' if result == 'PUBLISHED' else result)
                else:
                    cache.pop(slot['id'], None)
            counts[result] += 1
        except (OSError, ValueError, ns.SyncError):
            cache.pop(slot['id'], None)
            counts['RETRY'] += 1
    return dict(counts)


def run_titles(config):
    counts = Counter()
    source, destination, lock = (Path(config[key]) for key in ('title_source', 'title_destination', 'title_lock'))
    for slot in config['slots']:
        try:
            state = Path(slot['state_dir']) / 'title'
            state.mkdir(mode=0o700, exist_ok=True)
            result = titles.publish_title(source, destination, state, slot['id'], replace_initial=True,
                                          prefer_local=True, lock_file=lock)
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
            counts = run_once(config, cache)
            title_counts = (run_titles(config) if all(key in config for key in
                            ('title_source', 'title_destination', 'title_lock')) else {})
            fd, temp = tempfile.mkstemp(prefix='.health-', dir=args.config.parent)
            try:
                with os.fdopen(fd, 'w') as stream:
                    json.dump({'counts': counts, 'titles': title_counts,
                               'updated_at_ms': int(time.time() * 1000)}, stream)
                os.replace(temp, args.config.parent / 'health.json')
            finally:
                if os.path.exists(temp): os.unlink(temp)
        except (OSError, ValueError, TypeError, KeyError, ns.SyncError):
            pass
        time.sleep(args.interval)


if __name__ == '__main__': main()
