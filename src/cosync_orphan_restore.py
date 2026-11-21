"""Repair one indexed subagent whose received history is missing locally.

The authenticated journal keeps an interrupted repair retryable without
replacing a user's chat or rolling back unrelated database changes.
"""
from contextlib import closing, contextmanager
import hmac
import json
import os
from pathlib import Path, PureWindowsPath
import re
import sqlite3
import stat
import sys
import tempfile

import cosync_lineage as lineage
import cosync_graph_snapshot as graph
import cosync_restore_backup as backup
import mock_chat_sync as locking
import mock_paginated_import as indexer
import nexus_sync as ns


@contextmanager
def _repair_lock(receiver):
    if sys.platform not in ('darwin', 'linux'):
        raise ns.SyncError('Orphan repair lock unavailable on this platform')
    import fcntl
    path = receiver.backup_root / 'direct-orphan.lock'
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ns.SyncError('Invalid orphan repair lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise locking.MockChatBusy('Orphan repair is already running') from exc
        yield
    finally:
        os.close(fd)


def _read(receiver):
    path = receiver.backup_root / 'direct-orphan-pending.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        return None
    try:
        value = json.loads(path.read_bytes())
        tag = value.get('authentication')
        if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(value, receiver.key)):
            return None
        return value
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def _busy(receiver, identity):
    busy = frozenset(locking._busy_thread_ids(receiver.profile))
    if identity in busy:
        raise locking.MockChatBusy('Orphan chat is currently open')
    return busy


def _save_indexed(receiver, record):
    journal = receiver.backup_root / 'direct-orphan-pending.json'
    if _read(receiver) != record:
        raise ns.SyncError('Orphan journal changed')
    updated = {**record, 'phase': 'indexed'}
    updated['authentication'] = ns._manifest_tag(updated, receiver.key)
    fd, temporary = tempfile.mkstemp(prefix='.orphan-journal-', dir=receiver.backup_root)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as out:
            json.dump(updated, out); out.flush(); os.fsync(out.fileno())
        if journal.is_symlink() or _read(receiver) != record:
            raise ns.SyncError('Orphan journal changed')
        os.replace(temporary, journal)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def prepare(receiver, identity, entries, old_path, old_cwd):
    """Stage one unambiguous missing subagent and back up its databases."""
    with _repair_lock(receiver):
        return _prepare_locked(receiver, identity, entries, old_path, old_cwd)


def _prepare_locked(receiver, identity, entries, old_path, old_cwd):
    if (len(entries) != 1 or entries[0].get('folder') != 'sessions'
            or not isinstance(old_path, str) or not isinstance(old_cwd, str)
            or not (Path(old_path).is_absolute() or PureWindowsPath(old_path).is_absolute())
            or Path(old_path).exists()):
        return False
    journal = receiver.backup_root / 'direct-orphan-pending.json'
    stage = receiver.backup_root / 'direct-orphan-stage.jsonl'
    if journal.exists() or journal.is_symlink() or stage.is_symlink():
        return False
    if stage.exists():
        info = stage.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            return False
        stage.unlink()
    projection = receiver.profile / 'thread_history_1.sqlite'
    if projection.is_symlink() or not projection.is_file():
        return False
    with closing(sqlite3.connect(projection.as_uri() + '?mode=ro', uri=True)) as db:
        indexer._validate_projection_schema(db)
    with tempfile.TemporaryDirectory(prefix='cosyncx-orphan-prepare-') as tmp:
        prepared = Path(tmp).resolve() / 'prepared'
        lineage.prepare_lineage(receiver.source, prepared, identity, receiver.mappings,
                                preserve_unmapped_history=True)
        items = ns._chat_catalog(prepared, include_unsupported=True).get(identity, [])
        if len(items) != 1 or not isinstance(items[0].get('cwd'), str):
            return False
        relative = items[0]['path'].relative_to(prepared)
        if len(relative.parts) < 2 or relative.parts[0] != 'sessions' or '..' in relative.parts:
            return False
        destination = receiver.profile / relative
        if destination.exists() or destination.is_symlink():
            return False
        try:
            ns._copy_stable_chat(items[0]['path'], stage)
            os.chmod(stage, 0o600)
            with locking._mock_thread_lock(receiver.profile, identity, ignore_busy=_busy(receiver, identity)):
                if not receiver._write_allowed():
                    return False
                with closing(sqlite3.connect((receiver.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
                    if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                        return False
                    row = db.execute('SELECT rollout_path,cwd,archived FROM threads WHERE id=?', (identity,)).fetchone()
                if row != (old_path, old_cwd, 0) or Path(old_path).exists():
                    return False
                names = [x for x in ('state_5.sqlite', 'thread_history_1.sqlite', 'session_index.jsonl')
                         if (receiver.profile / x).is_file()]
                if 'state_5.sqlite' not in names or 'thread_history_1.sqlite' not in names:
                    return False
                latest = backup.replace_latest(receiver.profile, receiver.backup_root, names)
                record = {'version': 1, 'phase': 'prepared', 'id': identity, 'relative': relative.as_posix(),
                          'old_path': old_path, 'old_cwd': old_cwd, 'new_cwd': items[0]['cwd'],
                          'digest': ns._digest(stage), 'backup_digest': ns._digest(latest / 'manifest.json'),
                          'event_id': os.urandom(16).hex()}
                record['authentication'] = ns._manifest_tag(record, receiver.key)
                fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'w') as out:
                    json.dump(record, out); out.flush(); os.fsync(out.fileno())
                return True
        finally:
            if not journal.exists():
                stage.unlink(missing_ok=True)


def resume(receiver, on_restore=None):
    """Resume a journalled repair; do not overwrite another writer's data."""
    with _repair_lock(receiver):
        return _resume_locked(receiver, on_restore=on_restore)


def _resume_locked(receiver, on_restore=None):
    record = _read(receiver)
    if not record or record.get('version') != 1 or record.get('phase') not in ('prepared', 'indexed'):
        return False
    identity, relative = record.get('id'), record.get('relative')
    if (not graph._uuid(identity) or not isinstance(record.get('event_id'), str)
            or re.fullmatch(r'[0-9a-f]{32}', record['event_id']) is None
            or not isinstance(record.get('digest'), str) or re.fullmatch(r'[0-9a-f]{64}', record['digest']) is None
            or not all(isinstance(record.get(k), str) for k in ('old_path', 'old_cwd', 'new_cwd', 'backup_digest'))):
        return False
    if not isinstance(relative, str):
        return False
    parts = relative.split('/')
    if len(parts) < 2 or parts[0] != 'sessions' or any(x in ('', '.', '..') for x in parts):
        return False
    stage = receiver.backup_root / 'direct-orphan-stage.jsonl'
    manifest = receiver.backup_root / 'latest' / 'manifest.json'
    if (stage.is_symlink() or manifest.is_symlink() or not manifest.is_file()
            or ns._digest(manifest) != record.get('backup_digest')):
        return False
    destination = receiver.profile.joinpath(*parts)
    if record['phase'] == 'indexed':
        if (destination.is_symlink() or not destination.is_file()
                or not any(item['id'] == record['event_id'] for item in receiver._read_carry())):
            return False
        with closing(sqlite3.connect((receiver.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            row = db.execute('SELECT rollout_path,cwd,archived FROM threads WHERE id=?', (identity,)).fetchone()
        if row != (str(destination), record['new_cwd'], 0):
            return False
        stage.unlink(missing_ok=True)
        (receiver.backup_root / 'direct-orphan-pending.json').unlink()
        return True
    if not stage.is_file() or ns._digest(stage) != record.get('digest'):
        return False
    if on_restore is not None:
        on_restore()
    with locking._mock_thread_lock(receiver.profile, identity, ignore_busy=_busy(receiver, identity)):
        if not receiver._write_allowed():
            return False
        old = (record['old_path'], record['old_cwd'], 0)
        new = (str(destination), record['new_cwd'], 0)
        with closing(sqlite3.connect((receiver.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            row = db.execute('SELECT rollout_path,cwd,archived FROM threads WHERE id=?', (identity,)).fetchone()
        if row not in (old, new) or (row == old and Path(record['old_path']).exists()):
            return False
        parent = receiver.profile
        for part in parts[:-1]:
            parent = parent / part
            if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
                return False
            parent.mkdir(mode=0o700, exist_ok=True)
        if destination.is_symlink():
            return False
        if not destination.exists():
            temporary = parent / ('.cosyncx-orphan-' + os.urandom(12).hex())
            try:
                ns._copy_stable_chat(stage, temporary)
                os.link(temporary, destination, follow_symlinks=False)
            finally:
                temporary.unlink(missing_ok=True)
        if not destination.is_file() or ns._digest(destination) != record['digest']:
            return False
        with closing(sqlite3.connect(receiver.profile / 'state_5.sqlite', timeout=1)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT rollout_path,cwd,archived FROM threads WHERE id=?', (identity,)).fetchone()
            if row == old:
                changed = db.execute('UPDATE threads SET rollout_path=?,cwd=? WHERE id=? AND rollout_path=? AND cwd=? AND archived=0',
                    (str(destination), record['new_cwd'], identity, record['old_path'], record['old_cwd'])).rowcount
                if changed != 1:
                    return False
            elif row != new:
                return False
    ignored = _busy(receiver, identity)
    indexer._reset_profile_projection(receiver.profile, receiver.backup_root, identity, ignore_busy=ignored)
    indexer._index_profile(receiver.profile, identity, receiver.codex, title=None, ignore_busy=_busy(receiver, identity))
    events = receiver._read_carry()
    if all(item['id'] != record['event_id'] for item in events):
        events.append({'id': record['event_id'], 'count': 1})
        receiver._write_carry(events)
    _save_indexed(receiver, record)
    stage.unlink(missing_ok=True)
    (receiver.backup_root / 'direct-orphan-pending.json').unlink()
    return True
