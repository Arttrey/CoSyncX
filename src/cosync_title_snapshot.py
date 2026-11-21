"""Export only titles of active chats with locally present session files."""
import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import tempfile
import uuid


class TitleSnapshotError(Exception):
    pass


_FILE = re.compile(r'-([0-9a-fA-F-]{36})(?:_[0-9a-fA-F-]{36})?\.jsonl$')
_MAX_BYTES = 32 * 1024 * 1024


def _active_ids(database: Path) -> set[str]:
    if database.is_symlink() or not database.is_file():
        raise TitleSnapshotError('Codex state unavailable')
    try:
        with sqlite3.connect(database.resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5) as db:
            db.execute('PRAGMA query_only = ON')
            columns = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
            if not {'id', 'archived'} <= columns:
                raise TitleSnapshotError('Unsupported state schema')
            rows = db.execute('SELECT id FROM threads WHERE archived=0 LIMIT 100001').fetchall()
    except sqlite3.Error as exc:
        raise TitleSnapshotError('Cannot read active chats') from exc
    if len(rows) > 100000:
        raise TitleSnapshotError('Too many active chats')
    result = set()
    for (identity,) in rows:
        try:
            if not isinstance(identity, str) or str(uuid.UUID(identity)) != identity:
                raise ValueError()
        except (ValueError, AttributeError) as exc:
            raise TitleSnapshotError('Invalid active chat ID') from exc
        result.add(identity)
    return result


def _present_ids(sessions: Path) -> set[str]:
    if sessions.is_symlink() or not sessions.is_dir():
        raise TitleSnapshotError('Active sessions unavailable')
    result = set()
    for root, dirs, files in os.walk(sessions, followlinks=False):
        base = Path(root)
        if any((base / name).is_symlink() for name in dirs):
            raise TitleSnapshotError('Session directory contains a link')
        for name in files:
            match = _FILE.search(name)
            if match:
                path = base / name
                if path.is_symlink() or not path.is_file():
                    raise TitleSnapshotError('Session file is unavailable')
                try:
                    result.add(str(uuid.UUID(match.group(1))))
                except ValueError as exc:
                    raise TitleSnapshotError('Invalid session file ID') from exc
        if len(result) > 100000:
            raise TitleSnapshotError('Too many session files')
    return result


def write_active_titles(profile: Path, destination: Path) -> int:
    if (not profile.is_absolute() or profile.is_symlink() or not profile.is_dir()
            or not destination.is_absolute() or destination.is_symlink()
            or destination.parent.is_symlink() or not destination.parent.is_dir()):
        raise TitleSnapshotError('Invalid profile or destination')
    profile, parent = profile.resolve(strict=True), destination.parent.resolve(strict=True)
    if parent == profile or profile in parent.parents or parent in profile.parents:
        raise TitleSnapshotError('Destination must be outside the profile')
    info = parent.stat()
    if os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise TitleSnapshotError('Destination directory is not private')
    database, index = profile / 'state_5.sqlite', profile / 'session_index.jsonl'
    if index.is_symlink() or not index.is_file() or index.stat().st_size > _MAX_BYTES:
        raise TitleSnapshotError('Title index unavailable')
    active = _active_ids(database)
    present = _present_ids(profile / 'sessions')
    before = index.lstat()
    fd = os.open(index, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise TitleSnapshotError('Title index changed')
        raw = stream.read(_MAX_BYTES + 1)
        after = os.fstat(stream.fileno())
    if (len(raw) != before.st_size or (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
        raise TitleSnapshotError('Title index changed')
    selected, ids = [], set()
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b'\n'):
            raise TitleSnapshotError('Title index has an incomplete record')
        try:
            row = json.loads(line)
            identity = row.get('id') if isinstance(row, dict) else None
            if not isinstance(identity, str) or str(uuid.UUID(identity)) != identity or not isinstance(row.get('thread_name'), str):
                raise ValueError()
        except (ValueError, TypeError, AttributeError) as exc:
            raise TitleSnapshotError('Invalid title record') from exc
        if identity in active and identity in present:
            selected.append(line)
            ids.add(identity)
    if _active_ids(database) != active or _present_ids(profile / 'sessions') != present:
        raise TitleSnapshotError('Active sessions changed during export')
    data = b''.join(selected)
    if destination.is_file() and not destination.is_symlink() and destination.stat().st_size <= _MAX_BYTES:
        if destination.read_bytes() == data:
            return len(ids)
    fd, temp = tempfile.mkstemp(prefix='.cosyncx-titles-', dir=parent)
    temp = Path(temp)
    try:
        if hasattr(os, 'fchmod'): os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        if destination.is_symlink(): raise TitleSnapshotError('Destination is a link')
        os.replace(temp, destination)
    finally:
        temp.unlink(missing_ok=True)
    return len(ids)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Export titles for active sessions only')
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps({'active_titles': write_active_titles(args.profile, args.output)}))
        return 0
    except (TitleSnapshotError, OSError, ValueError, TypeError):
        # A stale snapshot might contain a title that became archived. Remove
        # only a regular file in a private, separate export directory.
        try:
            parent = args.output.parent
            if (args.profile.is_absolute() and args.profile.is_dir() and not args.profile.is_symlink()
                    and args.output.is_absolute() and args.output.name == 'session_index.jsonl'
                    and not parent.is_symlink() and parent.is_dir()
                    and not args.output.is_symlink() and args.output.is_file()):
                profile = args.profile.resolve(strict=True)
                root = parent.resolve(strict=True)
                info = root.stat()
                if (root != profile and profile not in root.parents and root not in profile.parents
                        and (os.name == 'nt' or (info.st_uid == os.getuid() and not stat.S_IMODE(info.st_mode) & 0o077))):
                    args.output.unlink()
        except OSError:
            pass
        print('CoSyncX: active title snapshot unavailable', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
