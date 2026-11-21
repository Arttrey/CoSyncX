"""Isolated batch experiment for active subagent histories; never a live receiver."""
import hmac
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tempfile

import cosync_graph_snapshot as graph
import cosync_lineage as lineage
import cosync_receive_plan
import cosync_restore_backup as backup
import mock_archive_restore as archive
import mock_chat_sync as mock
import mock_journal
import mock_paginated_import as pages
import nexus_sync as ns


def restore(source: Path, profile: Path, shared: Path, backup_root: Path, key: bytes,
            parent: str, mappings: list[tuple[str, str]], executable: Path, *, children: list[str]) -> int:
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    executable = executable.resolve(strict=True)
    if (not source.is_dir() or not shared.is_dir() or not backup_root.is_dir() or not executable.is_file()
            or not graph._uuid(parent) or not isinstance(children, list) or not children
            or len(set(children)) != len(children) or any(not graph._uuid(x) or x == parent for x in children)
            or any(a == b or a in b.parents or b in a.parents for a, b in
                   ((source, profile), (source, shared), (source, backup_root), (profile, shared),
                    (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock child batch')
    mock_journal.ensure_clear(backup_root)
    source_catalog = ns._chat_catalog(source, include_unsupported=True)
    local_catalog = ns._chat_catalog(profile, include_unsupported=True)
    if parent not in local_catalog or any(item['folder'] != 'sessions' for item in local_catalog[parent]):
        raise ns.SyncError('Mock parent is not active')
    with sqlite3.connect((profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        states = dict(db.execute('SELECT id, archived FROM threads'))
    if states.get(parent) != 0:
        raise ns.SyncError('Mock parent is not indexed')
    before = {}
    for identity in children:
        incoming, current = source_catalog.get(identity, []), local_catalog.get(identity, [])
        if (len(incoming) != 1 or incoming[0].get('parent_thread_id') != parent
                or cosync_receive_plan.source_kind(incoming) != 'subagent'):
            raise ns.SyncError('Unsupported mock child source')
        if current:
            if len(current) != 1 or current[0]['folder'] != 'sessions' or states.get(identity) != 0:
                raise ns.SyncError('Mock child versions are ambiguous')
            before[identity] = current[0]['path'].relative_to(profile).as_posix()
        elif identity in states:
            raise ns.SyncError('Mock missing child has stale state')
        else:
            before[identity] = None
    journal = backup_root / 'mock-child-pending.json'
    with tempfile.TemporaryDirectory(prefix='cosyncx-child-batch-') as tmp:
        staged, targets, source_digests = {}, {}, {}
        for number, identity in enumerate(children):
            dest = Path(tmp).resolve() / str(number)
            lineage.prepare_lineage(source, dest, identity, mappings)
            if (dest / 'cosyncx-path-report.json').exists():
                raise ns.SyncError('Mock child path report needs delivery')
            incoming = ns._chat_catalog(dest, include_unsupported=True).get(identity, [])
            if len(incoming) != 1 or incoming[0]['folder'] != 'sessions':
                raise ns.SyncError('Mock child preparation failed')
            staged[identity] = incoming[0]['path']
            source_digests[identity] = archive._stable_digest(incoming[0]['path'])
            targets[identity] = (before[identity] if before[identity] is not None
                                 else incoming[0]['path'].relative_to(dest).as_posix())
        if len(set(targets.values())) != len(targets):
            raise ns.SyncError('Mock child destinations conflict')
        dbs = sorted(set(profile.glob('state_*.sqlite')) | set(profile.glob('thread_history_*.sqlite')))
        if not dbs or any(p.is_symlink() or not p.is_file() for p in dbs) or profile / 'state_5.sqlite' not in dbs:
            raise ns.SyncError('Mock child databases unavailable')
        title = profile / 'session_index.jsonl'
        if title.is_symlink() or (title.exists() and not title.is_file()):
            raise ns.SyncError('Mock child title index unavailable')
        title_existed = title.is_file()
        names = [name for name in before.values() if name is not None] + [p.name for p in dbs]
        if title_existed: names.append(title.name)
        db_names = {p.name for p in dbs}
        with mock._mock_thread_lock(profile, parent):
            initial_db = archive._db_signatures(profile, db_names)
            def verify(_stage, manifest):
                now = ns._chat_catalog(profile, include_unsupported=True)
                for identity in children:
                    paths = [x['path'].relative_to(profile).as_posix() for x in now.get(identity, [])]
                    if paths != ([before[identity]] if before[identity] is not None else []):
                        raise ns.SyncError('Mock child changed during backup')
                    if before[identity] is not None and ns._digest(profile / before[identity]) != manifest.get(before[identity]):
                        raise ns.SyncError('Mock child history changed during backup')
                if archive._db_signatures(profile, db_names) != initial_db or title.is_file() != title_existed:
                    raise ns.SyncError('Mock child state changed during backup')
                if title_existed and ns._digest(title) != manifest.get(title.name):
                    raise ns.SyncError('Mock child title state changed during backup')
            latest = backup.replace_latest(profile, backup_root, names, pre_commit=verify)
        data = {'version': 1, 'parent': parent, 'children': children, 'before': before, 'targets': targets,
                'source_digests': source_digests, 'verified_digests': {}, 'title_existed': title_existed,
                'backup_manifest_digest': archive._stable_digest(latest / 'manifest.json')}
        data['authentication'] = ns._manifest_tag(data, key)
        fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            json.dump(data, out, sort_keys=True); out.flush(); os.fsync(out.fileno())
        try:
            for identity in children:
                target = profile.joinpath(*targets[identity].split('/'))
                parent_dir = profile
                for component in targets[identity].split('/')[:-1]:
                    parent_dir = parent_dir / component
                    if parent_dir.is_symlink(): raise ns.SyncError('Mock child path is a link')
                    parent_dir.mkdir(mode=0o700, exist_ok=True)
                    if not parent_dir.is_dir(): raise ns.SyncError('Mock child directory unavailable')
                if target.is_symlink(): raise ns.SyncError('Mock child target is a link')
                if before[identity] is None:
                    if target.exists(): raise ns.SyncError('Mock child destination occupied')
                elif not target.is_file() or ns._digest(target) != json.loads((latest/'manifest.json').read_bytes())['files'][before[identity]]:
                    raise ns.SyncError('Mock child changed after backup')
                temporary = target.with_name('.cosyncx-child-' + os.urandom(12).hex())
                try:
                    ns._copy_stable_chat(staged[identity], temporary)
                    if ns._digest(temporary) != source_digests[identity]: raise ns.SyncError('Mock child source changed')
                    if before[identity] is None:
                        os.link(temporary, target, follow_symlinks=False)
                        temporary.unlink()
                    else:
                        os.replace(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
                if before[identity] is not None:
                    pages.reset_mock_projection(profile, shared, backup_root, key, identity)
                def mark_verified(identity=identity, target=target):
                    data['verified_digests'][identity] = archive._stable_digest(target)
                    data['verified_title_existed'] = title.is_file()
                    data['verified_title_digest'] = archive._stable_digest(title) if title.is_file() else None
                    data['verified_db_signatures'] = archive._db_signatures(profile, db_names)
                    archive._replace_journal(journal, data, key)
                pages.index_thread(profile, shared, key, identity, executable, on_verified=mark_verified)
            journal.unlink()
            return len(children)
        except Exception as exc:
            if not isinstance(exc, subprocess.SubprocessError):
                try:
                    rollback(profile, shared, backup_root, key)
                except Exception:
                    pass
            raise


def rollback(profile: Path, shared: Path, backup_root: Path, key: bytes) -> None:
    """Roll back only when every affected mock file and journal is verified."""
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir() or any(
            a == b or a in b.parents or b in a.parents for a, b in
            ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Mock child recovery unavailable')
    journal, latest = backup_root / 'mock-child-pending.json', backup_root / 'latest'
    if (journal.is_symlink() or not journal.is_file() or journal.stat().st_size > 1024 * 1024
            or latest.is_symlink() or not latest.is_dir()):
        raise ns.SyncError('Mock child recovery unavailable')
    for name in mock_journal.PENDING:
        if name != journal.name and ((backup_root / name).exists() or (backup_root / name).is_symlink()):
            raise ns.SyncError('Another mock transaction needs recovery')
    data = json.loads(journal.read_bytes())
    if not isinstance(data, dict) or data.get('version') != 1:
        raise ns.SyncError('Invalid mock child journal')
    tag = data.get('authentication')
    if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key)):
        raise ns.SyncError('Mock child journal is not authenticated')
    parent, children = data.get('parent'), data.get('children')
    if (not graph._uuid(parent) or not isinstance(children, list) or not children
            or any(not graph._uuid(x) or x == parent for x in children) or len(set(children)) != len(children)):
        raise ns.SyncError('Invalid mock child journal identities')
    for field in ('before', 'targets', 'source_digests', 'verified_digests'):
        if not isinstance(data.get(field), dict): raise ns.SyncError('Invalid mock child journal data')
    if (set(data['before']) != set(children) or set(data['targets']) != set(children)
            or set(data['source_digests']) != set(children) or not set(data['verified_digests']) <= set(children)
            or type(data.get('title_existed')) is not bool):
        raise ns.SyncError('Invalid mock child journal data')
    for identity in children:
        if not isinstance(data['targets'][identity], str) or (data['before'][identity] is not None and
                data['before'][identity] != data['targets'][identity]):
            raise ns.SyncError('Invalid mock child recovery target')
        for name in (data['targets'][identity], data['before'][identity]):
            if name is not None and (not isinstance(name, str) or not name.startswith('sessions/')
                    or '\\' in name or any(part in ('', '.', '..') for part in name.split('/'))):
                raise ns.SyncError('Invalid mock child recovery path')
        for digest in [data['source_digests'][identity]] + ([data['verified_digests'][identity]] if identity in data['verified_digests'] else []):
            if not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None:
                raise ns.SyncError('Invalid mock child recovery digest')
    if len(set(data['targets'].values())) != len(children):
        raise ns.SyncError('Duplicate mock child recovery targets')
    manifest_path = latest / 'manifest.json'
    if (manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 1024 * 1024
            or archive._stable_digest(manifest_path) != data.get('backup_manifest_digest')):
        raise ns.SyncError('Mock child backup manifest changed')
    manifest = json.loads(manifest_path.read_bytes())
    if not isinstance(manifest, dict) or manifest.get('version') != 1 or not isinstance(manifest.get('files'), dict):
        raise ns.SyncError('Invalid mock child backup')
    entries = manifest['files']
    with mock._mock_thread_lock(profile, parent):
        saved = {}
        for name, digest in entries.items():
            try: path = backup._resolve_file(latest, name)
            except backup.BackupError as exc: raise ns.SyncError('Mock child backup unavailable') from exc
            if not isinstance(digest, str) or ns._digest(path) != digest: raise ns.SyncError('Mock child backup changed')
            saved[name] = path
        old_paths = {x for x in data['before'].values() if x is not None}
        if not old_paths <= set(entries): raise ns.SyncError('Mock child backup incomplete')
        saved_catalog = ns._chat_catalog(latest, include_unsupported=True)
        if set(saved_catalog) != {identity for identity in children if data['before'][identity] is not None}:
            raise ns.SyncError('Mock child backup identities changed')
        for identity, name in data['before'].items():
            if name is not None and [item['path'].relative_to(latest).as_posix() for item in saved_catalog[identity]] != [name]:
                raise ns.SyncError('Mock child backup paths changed')
        current_catalog = ns._chat_catalog(profile, include_unsupported=True)
        current = []
        for identity in children:
            old = data['before'][identity]
            allowed = {data['source_digests'][identity]}
            if old is not None: allowed.add(entries[old])
            if identity in data['verified_digests']: allowed.add(data['verified_digests'][identity])
            found = current_catalog.get(identity, [])
            target = profile.joinpath(*data['targets'][identity].split('/'))
            if target.is_symlink() or (target.exists() and all(item['path'] != target for item in found)):
                raise ns.SyncError('Mock child recovery target occupied')
            if any(ns._digest(item['path']) not in allowed for item in found):
                raise ns.SyncError('Mock child changed after interruption')
            current.extend(found)
        current_names = {item['path'].relative_to(profile).as_posix() for item in current}
        for name in old_paths:
            path = profile
            for part in name.split('/'):
                path /= part
                if path.is_symlink(): raise ns.SyncError('Mock child recovery path is a link')
            if path.exists() and (not path.is_file() or name not in current_names):
                raise ns.SyncError('Mock child recovery target occupied')
        db_names = {name for name in entries if re.fullmatch(r'(?:state|thread_history)_\d+\.sqlite', name)}
        live_db = {p.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite') for p in profile.glob(pattern)}
        if 'state_5.sqlite' not in db_names or db_names != live_db: raise ns.SyncError('Mock child databases changed')
        if data['verified_digests'] and archive._db_signatures(profile, db_names) != data.get('verified_db_signatures'):
            raise ns.SyncError('Mock child databases changed after verification')
        title = profile / 'session_index.jsonl'
        if title.is_symlink() or (title.exists() and not title.is_file()) or data['title_existed'] != ('session_index.jsonl' in entries):
            raise ns.SyncError('Mock child title state changed')
        if data['verified_digests']:
            if (title.is_file() != data.get('verified_title_existed') or
                    (title.is_file() and archive._stable_digest(title) != data.get('verified_title_digest'))):
                raise ns.SyncError('Mock child title changed after verification')
        elif (title.is_file() != data['title_existed'] or
              (title.is_file() and ns._digest(title) != entries.get('session_index.jsonl'))):
            raise ns.SyncError('Mock child title changed')
        for name in db_names:
            path = profile / name
            if path.is_symlink() or not path.is_file(): raise ns.SyncError('Mock child database unavailable')
            with sqlite3.connect(saved[name].as_uri() + '?mode=ro', uri=True) as db:
                if db.execute('PRAGMA quick_check').fetchone() != ('ok',): raise ns.SyncError('Mock child backup database invalid')
        def copy_saved(name, target):
            tmp = target.with_name('.cosyncx-child-recover-' + os.urandom(12).hex())
            try:
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'wb') as out, saved[name].open('rb') as inp:
                    shutil.copyfileobj(inp, out, 1024 * 1024); out.flush(); os.fsync(out.fileno())
                if ns._digest(tmp) != entries[name] or ns._digest(saved[name]) != entries[name]:
                    raise ns.SyncError('Mock child backup changed during recovery')
                os.replace(tmp, target)
            finally:
                tmp.unlink(missing_ok=True)
        for item in current:
            if item['path'].relative_to(profile).as_posix() not in old_paths: item['path'].unlink()
        for name in old_paths:
            target = profile.joinpath(*name.split('/'))
            parent_dir = profile
            for part in name.split('/')[:-1]:
                parent_dir /= part
                if parent_dir.is_symlink(): raise ns.SyncError('Mock child recovery path is a link')
                parent_dir.mkdir(mode=0o700, exist_ok=True)
            copy_saved(name, target)
        if data['title_existed']:
            copy_saved('session_index.jsonl', title)
        elif title.is_file():
            title.unlink()
        for name in sorted(db_names):
            with sqlite3.connect(saved[name].as_uri() + '?mode=ro', uri=True) as src:
                with sqlite3.connect(profile / name, timeout=1) as dst:
                    src.backup(dst)
                    if dst.execute('PRAGMA quick_check').fetchone() != ('ok',): raise ns.SyncError('Mock child recovery database invalid')
        journal.unlink()
