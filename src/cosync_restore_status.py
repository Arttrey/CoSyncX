"""Publish a private, atomic status for the local Cursor extension."""
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time


class StatusError(Exception):
    pass


def _valid_event(event, mock_only):
    return (isinstance(event, dict) and event.get('state') == 'RESTORED' and event.get('indexed') is True
            and type(event.get('changed_chats')) is int and event['changed_chats'] >= 1
            and event.get('mock_only') is mock_only and isinstance(event.get('id'), str)
            and re.fullmatch(r'[0-9a-f]{32}', event['id']) is not None)


def read_status(path: Path, *, mock_only: bool, max_age_ms: int | None = 90000) -> dict | None:
    if not path.is_absolute() or type(mock_only) is not bool:
        return None
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > 65536:
            return None
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (not stat.S_ISREG(opened.st_mode) or opened.st_size > 65536
                    or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)):
                return None
            if os.name != 'nt' and (opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) & 0o077):
                return None
            raw = stream.read(65537)
            after = os.fstat(stream.fileno())
        if (len(raw) != opened.st_size or (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            return None
        value = json.loads(raw)
        now = int(time.time() * 1000)
        if (not isinstance(value, dict) or value.get('format') != 'cosyncx-restore-status-v1'
                or value.get('mock_only') is not mock_only or type(value.get('updated_at_ms')) is not int
                or value['updated_at_ms'] > now + 5000
                or (max_age_ms is not None and now - value['updated_at_ms'] > max_age_ms)
                or value.get('state') not in {'TRANSFERRING', 'RESTORING', 'PENDING', 'COMPLETE', 'ERROR'}):
            return None
        event = value.get('restore_event')
        if event is not None and not _valid_event(event, mock_only):
            return None
        return value
    except (OSError, ValueError, TypeError):
        return None


def write_status(path: Path, state: str, *, mock_only: bool, event: dict | None = None) -> None:
    if (not path.is_absolute() or state not in {'TRANSFERRING', 'RESTORING', 'PENDING', 'COMPLETE', 'ERROR'}
            or type(mock_only) is not bool):
        raise StatusError('Invalid status')
    if event is not None:
        if not _valid_event(event, mock_only):
            raise StatusError('Invalid restore event')
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir() or path.is_symlink():
        raise StatusError('Status directory is unavailable')
    info = parent.stat()
    if os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise StatusError('Status directory is not private')
    safe_event = ({name: event[name] for name in ('id', 'state', 'indexed', 'changed_chats', 'mock_only')}
                  if event is not None else None)
    status = {'format': 'cosyncx-restore-status-v1', 'state': state, 'mock_only': mock_only,
              'updated_at_ms': int(time.time() * 1000), 'restore_event': safe_event}
    fd, temporary = tempfile.mkstemp(prefix='.cosyncx-status-', dir=parent)
    temporary = Path(temporary)
    try:
        if hasattr(os, 'fchmod'):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(status, stream, ensure_ascii=True, separators=(',', ':'))
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise StatusError('Status destination is a symlink')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
