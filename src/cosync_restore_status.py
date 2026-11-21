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


def write_status(path: Path, state: str, *, mock_only: bool, event: dict | None = None) -> None:
    if (not path.is_absolute() or state not in {'TRANSFERRING', 'RESTORING', 'PENDING', 'COMPLETE', 'ERROR'}
            or type(mock_only) is not bool or (event is not None and state != 'COMPLETE')):
        raise StatusError('Invalid status')
    if event is not None:
        if (not isinstance(event, dict) or event.get('state') != 'RESTORED' or event.get('indexed') is not True
                or type(event.get('changed_chats')) is not int or event['changed_chats'] < 1
                or event.get('mock_only') is not mock_only or not isinstance(event.get('id'), str)
                or re.fullmatch(r'[0-9a-f]{32}', event['id']) is None):
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
