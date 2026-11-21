"""Publish one selected chat title without replacing unrelated titles."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time
import uuid

import nexus_sync as ns


def _atomic(path: Path, data: bytes, temporary_dir: Path) -> None:
    fd, name = tempfile.mkstemp(prefix='.title-', dir=temporary_dir)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)


@contextmanager
def _title_lock(path: Path):
    if not path.is_absolute() or path.is_symlink() or not path.parent.is_dir() or path.parent.is_symlink():
        raise ns.SyncError('Invalid title coordination path')
    if os.name != 'nt' and (path.parent.stat().st_uid != os.getuid() or stat.S_IMODE(path.parent.stat().st_mode) & 0o022):
        raise ns.SyncError('Title coordination directory is not private')
    import fcntl
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode): raise ns.SyncError('Invalid title coordination file')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def publish_title(source: Path, destination: Path, state_dir: Path, thread_id: str, *, replace_initial=False,
                  prefer_local=False, lock_file: Path | None = None) -> str:
    source, destination, state_dir = map(Path, (source, destination, state_dir))
    try:
        if str(uuid.UUID(thread_id)) != thread_id: raise ValueError()
    except (ValueError, TypeError, AttributeError) as exc:
        raise ns.SyncError('Invalid chat identifier') from exc
    if (not all(p.is_absolute() for p in (source, destination, state_dir))
            or source.name != 'session_index.jsonl' or destination.name != source.name
            or source.is_symlink() or destination.is_symlink() or state_dir.is_symlink()
            or not source.is_file() or not destination.is_file() or not state_dir.is_dir()
            or source.resolve() == destination.resolve()
            or state_dir.resolve() in (source.parent.resolve(), destination.parent.resolve())
            or destination.parent.resolve() in state_dir.resolve().parents):
        raise ns.SyncError('Invalid title publication paths')
    if lock_file is not None:
        lock_file = Path(lock_file)
        if lock_file.parent.resolve() == destination.parent.resolve():
            raise ns.SyncError('Title lock cannot be in the exchange folder')
        with _title_lock(lock_file):
            return publish_title(source, destination, state_dir, thread_id, replace_initial=replace_initial,
                                 prefer_local=prefer_local)
    if os.name != 'nt' and (state_dir.stat().st_uid != os.getuid() or stat.S_IMODE(state_dir.stat().st_mode) & 0o077):
        raise ns.SyncError('Title state directory must be private')
    state_file, backup = state_dir / 'state.json', state_dir / 'latest.jsonl'
    if state_file.is_symlink() or backup.is_symlink(): raise ns.SyncError('Invalid title state')
    previous = None
    if state_file.exists():
        previous = json.loads(state_file.read_text())
        if (not isinstance(previous, dict) or set(previous) != {'version', 'name'}
                or previous['version'] != 1 or not isinstance(previous['name'], str)):
            raise ns.SyncError('Invalid title state')
    raw_source = ns._read_title_index(source.parent)
    raw_destination = ns._read_title_index(destination.parent)
    source_records = [r for r in ns._parse_title_index(raw_source) if r['id'] == thread_id]
    dest_records = [r for r in ns._parse_title_index(raw_destination) if r['id'] == thread_id]
    if not source_records: return 'WAITING'
    remote_name = dest_records[-1]['thread_name'] if dest_records else None
    local_record = source_records[-1]
    if previous and remote_name != previous['name']:
        if not prefer_local: return 'CONFLICT'
        if local_record['thread_name'] == previous['name']: return 'WAITING_REMOTE'
    if previous is None and not replace_initial: return 'NEEDS_INITIAL_APPROVAL'
    changed = remote_name != local_record['thread_name']
    if changed:
        lines = [line for line in raw_destination.splitlines(keepends=True)
                 if json.loads(line).get('id') != thread_id]
        lines.append((json.dumps(local_record, ensure_ascii=False, separators=(',', ':')) + '\n').encode())
        if hashlib.sha256(ns._read_title_index(destination.parent)).digest() != hashlib.sha256(raw_destination).digest():
            return 'CONFLICT'
        _atomic(backup, raw_destination, state_dir)
        _atomic(destination, b''.join(lines), state_dir)
    _atomic(state_file, json.dumps({'version': 1, 'name': local_record['thread_name']}).encode(), state_dir)
    return 'PUBLISHED' if changed else 'UNCHANGED'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--destination', required=True, type=Path)
    parser.add_argument('--state-dir', required=True, type=Path)
    parser.add_argument('--thread-id', required=True)
    parser.add_argument('--replace-initial', action='store_true')
    parser.add_argument('--prefer-local', action='store_true')
    parser.add_argument('--lock-file', type=Path)
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    if not 10 <= args.interval <= 3600: parser.error('Invalid interval')
    while True:
        try:
            result = publish_title(args.source, args.destination, args.state_dir, args.thread_id,
                                   replace_initial=args.replace_initial, prefer_local=args.prefer_local,
                                   lock_file=args.lock_file)
        except (OSError, ValueError, ns.SyncError):
            result = 'RETRY'
        try:
            health = args.state_dir / 'health.json'
            if health.is_symlink(): raise ns.SyncError('Invalid publisher status')
            _atomic(health, json.dumps({'state': result, 'updated_at_ms': int(time.time() * 1000)}).encode(), args.state_dir)
        except (OSError, ns.SyncError):
            pass
        time.sleep(args.interval)


if __name__ == '__main__': main()
