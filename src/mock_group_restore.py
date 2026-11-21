"""Experimental parent/child transaction, restricted to disposable profiles.

The permanent latest backup is made before either step. Per-step backups and
authenticated journals remain until the complete group has been indexed.
"""
import hmac
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import cosync_graph_snapshot as graph
import cosync_restore_backup as backup
import mock_archive_backup
import mock_archive_restore as archive
import mock_chat_sync as mock
import mock_child_batch as child_batch
import mock_journal
import nexus_sync as ns


def _verify_backup(latest: Path, expected: str) -> None:
    manifest = latest / 'manifest.json'
    if (latest.is_symlink() or not latest.is_dir() or archive._stable_digest(manifest) != expected):
        raise ns.SyncError('Mock group backup changed')
    data = json.loads(manifest.read_bytes())
    if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('files'), dict):
        raise ns.SyncError('Invalid mock group backup')
    for name, digest in data['files'].items():
        try:
            saved = backup._resolve_file(latest, name)
        except backup.BackupError as exc:
            raise ns.SyncError('Mock group backup unavailable') from exc
        if not isinstance(digest, str) or archive._stable_digest(saved) != digest:
            raise ns.SyncError('Mock group backup changed')


def restore(source: Path, profile: Path, shared: Path, backup_root: Path, key: bytes,
            parent: str, mappings: list[tuple[str, str]], executable: Path, *, children: list[str],
            title: str | None = None, flatten_multi: bool = False, fallback_root: Path | None = None) -> dict:
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    executable = executable.resolve(strict=True)
    if (not source.is_dir() or not shared.is_dir() or not backup_root.is_dir() or not executable.is_file()
            or not graph._uuid(parent) or not isinstance(children, list) or not children
            or any(not graph._uuid(child) or child == parent for child in children)
            or len(set(children)) != len(children) or any(a == b or a in b.parents or b in a.parents for a, b in
                ((source, profile), (source, shared), (source, backup_root), (profile, shared),
                 (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock group')
    mock_journal.ensure_clear(backup_root)
    journal = backup_root / 'mock-group-pending.json'
    parent_step, child_step = backup_root / 'group-parent', backup_root / 'group-children'
    if any(path.exists() or path.is_symlink() for path in (parent_step, child_step)):
        raise ns.SyncError('Mock group recovery directories exist')
    incoming = ns._chat_catalog(source, include_unsupported=True)
    for identity in children:
        entries = incoming.get(identity, [])
        if len(entries) != 1 or entries[0].get('parent_thread_id') != parent:
            raise ns.SyncError('Mock group child relationship is invalid')
    mock_archive_backup.prepare(profile, shared, backup_root, key, [parent], extra_threads=children)
    manifest = backup_root / 'latest' / 'manifest.json'
    data = {'version': 1, 'parent': parent, 'children': list(children), 'complete': False,
            'backup_manifest_digest': archive._stable_digest(manifest)}
    _verify_backup(backup_root / 'latest', data['backup_manifest_digest'])
    data['authentication'] = ns._manifest_tag(data, key)
    fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as out:
        json.dump(data, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    try:
        parent_step.mkdir(mode=0o700)
        child_step.mkdir(mode=0o700)
        result = archive.restore(source, profile, shared, parent_step, key, parent, mappings, executable,
                                 title=title, flatten_multi=flatten_multi, fallback_root=fallback_root,
                                 retain_journal=True)
        if result.get('path_report') is not None:
            raise ns.SyncError('Mock group path report needs durable delivery')
        child_batch.restore(source, profile, shared, child_step, key, parent, mappings, executable,
                            children=children, retain_journal=True, defer_rollback=True)
        _verify_backup(backup_root / 'latest', data['backup_manifest_digest'])
        data['complete'] = True
        archive._replace_journal(journal, data, key)
        recover(profile, shared, backup_root, key)
        return {'mock_only': True, 'indexed': True, 'changed_chats': 1 + len(children)}
    except Exception as exc:
        if not isinstance(exc, subprocess.SubprocessError):
            try:
                recover(profile, shared, backup_root, key)
            except Exception:
                pass
        raise


def recover(profile: Path, shared: Path, backup_root: Path, key: bytes) -> None:
    """Undo unfinished steps, or finish cleanup after a committed group."""
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir() or any(a == b or a in b.parents or b in a.parents
            for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock group recovery paths')
    journal = backup_root / 'mock-group-pending.json'
    mock_journal.ensure_clear(backup_root, except_name=journal.name)
    if journal.is_symlink() or not journal.is_file() or journal.stat().st_size > 1024 * 1024:
        raise ns.SyncError('Mock group journal unavailable')
    data = json.loads(journal.read_bytes())
    if (not isinstance(data, dict) or data.get('version') != 1 or type(data.get('complete')) is not bool
            or not graph._uuid(data.get('parent')) or not isinstance(data.get('children'), list)
            or not data['children'] or any(not graph._uuid(x) or x == data['parent'] for x in data['children'])
            or len(set(data['children'])) != len(data['children'])):
        raise ns.SyncError('Invalid mock group journal')
    tag = data.get('authentication')
    if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key)):
        raise ns.SyncError('Mock group journal is not authenticated')
    latest = backup_root / 'latest'
    _verify_backup(latest, data.get('backup_manifest_digest'))
    parent_step, child_step = backup_root / 'group-parent', backup_root / 'group-children'
    for path in (parent_step, child_step):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise ns.SyncError('Mock group recovery directory is unsafe')
    if not data['complete']:
        child_journal = child_step / 'mock-child-pending.json'
        parent_journal = parent_step / 'mock-archive-pending.json'
        if child_journal.exists() or child_journal.is_symlink():
            child_batch.rollback(profile, shared, child_step, key)
            # The child rollback restored its verified pre-step SQLite backup.
            # SQLite may change file metadata while copying those same pages;
            # refresh the parent's authenticated observation before its rollback.
            if parent_journal.is_symlink() or not parent_journal.is_file() or parent_journal.stat().st_size > 1024 * 1024:
                raise ns.SyncError('Mock group parent journal unavailable')
            parent_data = json.loads(parent_journal.read_bytes())
            if (not isinstance(parent_data, dict) or parent_data.get('thread') != data['parent']
                    or not isinstance(parent_data.get('authentication'), str)
                    or not hmac.compare_digest(parent_data['authentication'], ns._manifest_tag(parent_data, key))
                    or not isinstance(parent_data.get('verified_db_signatures'), dict)):
                raise ns.SyncError('Mock group parent journal invalid')
            parent_manifest = parent_step / 'latest' / 'manifest.json'
            if archive._stable_digest(parent_manifest) != parent_data.get('backup_manifest_digest'):
                raise ns.SyncError('Mock group parent backup changed')
            manifest = json.loads(parent_manifest.read_bytes())
            if not isinstance(manifest, dict) or not isinstance(manifest.get('files'), dict):
                raise ns.SyncError('Mock group parent backup invalid')
            db_names = {name for name in manifest['files'] if re.fullmatch(r'(?:state|thread_history)_\d+\.sqlite', name)}
            if not db_names:
                raise ns.SyncError('Mock group parent databases unavailable')
            parent_data['verified_db_signatures'] = archive._db_signatures(profile, db_names)
            archive._replace_journal(parent_journal, parent_data, key)
        if parent_journal.exists() or parent_journal.is_symlink():
            archive.rollback(profile, shared, parent_step, key)
    for path in (child_step, parent_step):
        if path.is_dir():
            shutil.rmtree(path)
    journal.unlink()
