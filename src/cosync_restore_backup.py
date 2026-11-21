"""Create a verified single-generation backup for a guarded restore.

The caller must exclude concurrent writers before invoking this module. This
module does not restore data, start services or inspect a real Codex profile.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import tempfile


class BackupError(Exception):
    pass


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _resolve_file(root: Path, name: str) -> Path:
    if not isinstance(name, str) or '\\' in name or ':' in name or any(part in ('', '.', '..') for part in name.split('/')):
        raise BackupError('Invalid backup path')
    rel = PurePosixPath(name)
    if rel.is_absolute() or not rel.parts or any(part in ('', '.', '..') for part in rel.parts):
        raise BackupError('Invalid backup path')
    current = root
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise BackupError('Backup source contains a symlink')
    if not current.is_file() or current.resolve() != current:
        raise BackupError('Backup source is not a regular file')
    return current


def replace_latest(profile: Path, backup_root: Path, names: list[str], *, pre_commit=None) -> Path:
    """Replace latest only after every new file is copied and verified.

    Paths must identify existing regular files beneath profile. The caller is
    responsible for a process/lock gate and for selecting all state needed to
    undo its transaction. No file in the profile is modified.
    """
    profile = profile.resolve(strict=True)
    backup_root = backup_root.resolve(strict=True)
    if (not profile.is_dir() or not backup_root.is_dir() or backup_root == profile
            or profile in backup_root.parents or backup_root in profile.parents):
        raise BackupError('Invalid or overlapping backup directories')
    if not names or len(set(names)) != len(names) or 'manifest.json' in names:
        raise BackupError('Invalid backup selection')
    latest = backup_root / 'latest'
    previous = backup_root / '.previous'
    if latest.is_symlink() or previous.exists() or previous.is_symlink():
        raise BackupError('Previous backup transaction needs review')
    if latest.exists() and not latest.is_dir():
        raise BackupError('Invalid latest backup')
    stage = Path(tempfile.mkdtemp(prefix='.pending-', dir=backup_root))
    moved = False
    try:
        manifest = {}
        for name in names:
            source = _resolve_file(profile, name)
            target = stage.joinpath(*PurePosixPath(name).parts)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            before = source.stat()
            if source.suffix == '.sqlite':
                with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as src, sqlite3.connect(target) as dst:
                    src.backup(dst)
                    if dst.execute('PRAGMA quick_check').fetchone() != ('ok',):
                        raise BackupError('Database integrity check failed')
            else:
                with source.open('rb') as inp, target.open('xb') as out:
                    shutil.copyfileobj(inp, out, 1024 * 1024)
                after = source.stat()
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                    raise BackupError('Source changed during backup')
                if _digest(source) != _digest(target):
                    raise BackupError('Backup verification failed')
            os.chmod(target, 0o600)
            manifest[name] = _digest(target)
        manifest_path = stage / 'manifest.json'
        manifest_text = json.dumps({'version': 1, 'files': manifest}, sort_keys=True)
        manifest_path.write_text(manifest_text, encoding='utf-8')
        os.chmod(manifest_path, 0o600)
        for name, digest in manifest.items():
            if _digest(stage.joinpath(*PurePosixPath(name).parts)) != digest:
                raise BackupError('Backup changed before commit')
        if pre_commit is not None:
            if not callable(pre_commit):
                raise BackupError('Invalid backup verification callback')
            pre_commit(stage, dict(manifest))
            for name, digest in manifest.items():
                if _digest(stage.joinpath(*PurePosixPath(name).parts)) != digest:
                    raise BackupError('Backup changed during final verification')
            if manifest_path.is_symlink() or manifest_path.read_text(encoding='utf-8') != manifest_text:
                raise BackupError('Backup manifest changed during final verification')
        if latest.exists():
            os.rename(latest, previous)
            moved = True
        try:
            os.rename(stage, latest)
        except OSError:
            if moved:
                os.rename(previous, latest)
            raise
        if moved:
            shutil.rmtree(previous)
        return latest
    finally:
        if stage.exists():
            shutil.rmtree(stage)
