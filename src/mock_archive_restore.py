"""One-page archived-chat experiment, restricted to disposable mock profiles.

An interruption leaves a journal and requires review. This is not a live
receiver; multi-page flattening is an explicit, isolated experiment.
"""
import json
import hmac
import hashlib
import os
from pathlib import Path
import queue
import re
import sqlite3
import stat
import shutil
import subprocess
import tempfile
import threading
import time

import cosync_archive_plan
import cosync_graph_snapshot as graph
import cosync_lineage as lineage
import cosync_restore_backup as backup
import mock_archive_backup
import mock_chat_sync as mock
import mock_journal
import mock_paginated_import as pages
import nexus_sync as ns


def _db_signatures(profile: Path, names: set[str]) -> dict:
    result = {}
    for name in sorted(names):
        for path in (profile / name, profile / (name + '-wal')):
            if path.is_symlink():
                raise ns.SyncError('Mock database is a link')
            if path.exists():
                info = path.stat()
                result[path.name] = (None if path.name.endswith('-wal') and info.st_size == 0
                                     else [info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns])
            else:
                result[path.name] = None
    return result


def _stable_digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ns.SyncError('Mock verified file unavailable')
    before = path.stat()
    digest = ns._digest(path)
    after = path.stat()
    if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or ns._digest(path) != digest):
        raise ns.SyncError('Mock verified file changed')
    return digest


def _replace_journal(journal: Path, data: dict, key: bytes) -> None:
    data['authentication'] = ns._manifest_tag(data, key)
    fd, name = tempfile.mkstemp(prefix='.mock-archive-journal-', dir=journal.parent)
    temporary = Path(name)
    try:
        if hasattr(os, 'fchmod'):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            json.dump(data, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        if journal.is_symlink() or not journal.is_file():
            raise ns.SyncError('Mock archive journal unavailable')
        os.replace(temporary, journal)
    finally:
        temporary.unlink(missing_ok=True)


def _change_archive(profile: Path, executable: Path, thread: str, method: str) -> None:
    if method not in ('thread/archive', 'thread/unarchive'):
        raise ns.SyncError('Unsupported mock archive action')
    env = {key: value for key, value in os.environ.items()
           if key not in ('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_ADMIN_KEY', 'CHATGPT_ACCESS_TOKEN')}
    env['CODEX_HOME'] = str(profile)
    process = subprocess.Popen([str(executable), 'app-server', '--listen', 'stdio://'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    output = queue.Queue()
    def pump():
        try:
            while line := process.stdout.readline(4 * 1024 * 1024 + 1):
                if len(line) > 4 * 1024 * 1024 or not line.endswith(b'\n'):
                    break
                output.put(line)
        finally:
            output.put(None)
    reader = threading.Thread(target=pump, daemon=True)
    reader.start()
    def call(number, method, params):
        process.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
        process.stdin.flush()
        deadline = time.monotonic() + 20
        while True:
            try:
                line = output.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty as exc:
                raise ns.SyncError('Mock Codex request timed out') from exc
            if line is None:
                raise ns.SyncError('Mock Codex server ended')
            result = json.loads(line)
            if isinstance(result, dict) and result.get('id') == number:
                if 'error' in result or 'result' not in result:
                    raise ns.SyncError('Mock Codex rejected unarchive')
                return
    try:
        call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-mock-archive', 'version': '0.1'},
                               'capabilities': {'experimentalApi': True}})
        process.stdin.write(b'{"method":"initialized","params":{}}\n')
        process.stdin.flush()
        call(2, method, {'threadId': thread})
    finally:
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
        reader.join(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


def _unarchive(profile: Path, executable: Path, thread: str) -> None:
    _change_archive(profile, executable, thread, 'thread/unarchive')


def _archive(profile: Path, executable: Path, thread: str) -> None:
    _change_archive(profile, executable, thread, 'thread/archive')


def restore(source: Path, profile: Path, shared: Path, backup_root: Path, key: bytes,
            thread: str, mappings: list[tuple[str, str]], executable: Path, *, title: str | None = None,
            flatten_multi: bool = False, fallback_root: Path | None = None,
            retain_journal: bool = False, defer_rollback: bool = False) -> None:
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    executable = executable.resolve(strict=True)
    if (not source.is_dir() or not shared.is_dir() or not backup_root.is_dir() or not executable.is_file()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((source, profile), (source, shared), (source, backup_root),
                                (profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock archived restore paths')
    journal = backup_root / 'mock-archive-pending.json'
    mock_journal.ensure_clear(backup_root)
    impact = cosync_archive_plan.archive_impact(profile, [thread])
    descendants = impact['threads'] - {thread}
    source_catalog = ns._chat_catalog(source, include_unsupported=True)
    if descendants & set(source_catalog) or descendants - impact['archived_ids']:
        raise ns.SyncError('Mock descendants need a full group transaction')
    with sqlite3.connect((profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        direct_children = [row[0] for row in db.execute(
            'SELECT child_thread_id FROM thread_spawn_edges WHERE parent_thread_id=? ORDER BY child_thread_id', (thread,))]
    if not set(direct_children) <= descendants:
        raise ns.SyncError('Mock child relationships changed')
    local = ns._chat_catalog(profile, include_unsupported=True).get(thread, [])
    if not local or any(item['folder'] != 'archived_sessions' for item in local):
        raise ns.SyncError('Mock archived versions are ambiguous')
    with tempfile.TemporaryDirectory(prefix='cosyncx-archive-prepare-') as tmp:
        prepared = Path(tmp).resolve() / 'prepared'
        source_pages = source_catalog.get(thread, [])
        if len(source_pages) > 1:
            if flatten_multi is not True:
                raise ns.SyncError('Mock multi-page archive needs explicit experimental flattening')
            page_ids = {lineage._page_id(item['path'], thread) for item in source_pages}
            for identity, items in source_catalog.items():
                if identity == thread:
                    continue
                if any(isinstance(item.get('history_base'), dict)
                       and item['history_base'].get('thread_id') in page_ids for item in items):
                    raise ns.SyncError('Mock external page references prevent flattening')
            pages.prepare_flattened_mock(source, prepared, thread, mappings, fallback_root=fallback_root)
        else:
            lineage.prepare_lineage(source, prepared, thread, mappings, fallback_root=fallback_root)
        report_path = prepared / 'cosyncx-path-report.json'
        report = None
        if report_path.exists():
            info = backup_root.stat()
            if (report_path.is_symlink() or not report_path.is_file() or report_path.stat().st_size > 4 * 1024 * 1024
                    or (os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077))):
                raise ns.SyncError('Mock missing-path report directory is not private')
            report = json.loads(report_path.read_bytes())
            if (not isinstance(report, dict) or report.get('version') != 1
                    or type(report.get('fallback_used')) is not bool
                    or not isinstance(report.get('unmapped_working_directories'), list)
                    or not isinstance(report.get('unavailable_local_files'), list)
                    or not (report['unmapped_working_directories'] or report['unavailable_local_files'])
                    or any(not isinstance(value, str) for value in
                           report['unmapped_working_directories'] + report['unavailable_local_files'])):
                raise ns.SyncError('Invalid mock missing-path report')
        incoming = ns._chat_catalog(prepared, include_unsupported=True).get(thread, [])
        if len(incoming) != 1 or incoming[0]['folder'] != 'sessions':
            raise ns.SyncError('Mock archive requires one incoming page')
        saved = mock_archive_backup.prepare(profile, shared, backup_root, key, [thread])
        data = {'version': 1, 'thread': thread, 'source_digest': ns._digest(incoming[0]['path']),
                'previous_files': impact['files'], 'previous_threads': sorted(impact['threads']),
                'title_existed': saved['title_existed'],
                'backup_manifest_digest': _stable_digest(backup_root / 'latest' / 'manifest.json')}
        data['authentication'] = ns._manifest_tag(data, key)
        fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            json.dump(data, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        try:
            _unarchive(profile, executable, thread)
            for child in direct_children:
                _archive(profile, executable, child)
            after = cosync_archive_plan.archive_impact(profile, [thread])
            if after['threads'] != impact['threads'] or not descendants <= after['archived_ids']:
                raise ns.SyncError('Mock descendants did not return to archive')
            manifest_path = backup_root / 'latest' / 'manifest.json'
            if _stable_digest(manifest_path) != data['backup_manifest_digest']:
                raise ns.SyncError('Mock backup changed after preparation')
            manifest = json.loads(manifest_path.read_bytes())['files']
            known = {}
            for item in local:
                name = item['path'].relative_to(profile).as_posix()
                if name not in manifest:
                    raise ns.SyncError('Mock archived version is absent from backup')
                known.setdefault(item['path'].name, set()).add(manifest[name])
            current = ns._chat_catalog(profile, include_unsupported=True).get(thread, [])
            active = [item for item in current if item['folder'] == 'sessions']
            remaining = [item for item in current if item['folder'] == 'archived_sessions']
            canonical = [item for item in active if lineage._page_id(item['path'], thread) == thread]
            if len(canonical) != 1 or len(active) + len(remaining) != len(current):
                raise ns.SyncError('Mock unarchive produced an unexpected state')
            for item in current:
                if ns._digest(item['path']) not in known.get(item['path'].name, set()):
                    raise ns.SyncError('Mock archived version changed after backup')
            for item in current:
                if item['path'] == canonical[0]['path']:
                    continue
                item['path'].unlink()
            target = canonical[0]['path']
            temporary = target.with_name('.cosyncx-archive-' + os.urandom(12).hex())
            try:
                ns._copy_stable_chat(incoming[0]['path'], temporary)
                if ns._digest(temporary) != data['source_digest']:
                    raise ns.SyncError('Mock incoming history changed after preparation')
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
            pages.reset_mock_projection(profile, shared, backup_root, key, thread)
            def mark_verified():
                title_path = profile / 'session_index.jsonl'
                if title_path.is_symlink() or (title_path.exists() and not title_path.is_file()):
                    raise ns.SyncError('Mock verified title index unavailable')
                manifest_path = backup_root / 'latest' / 'manifest.json'
                if _stable_digest(manifest_path) != data['backup_manifest_digest']:
                    raise ns.SyncError('Mock backup changed during indexing')
                manifest = json.loads(manifest_path.read_bytes())
                db_names = {name for name in manifest['files'] if re.fullmatch(r'(?:state|thread_history)_\d+\.sqlite', name)}
                data.update({'verified_digest': _stable_digest(target),
                             'verified_title_existed': title_path.is_file(),
                             'verified_title_digest': _stable_digest(title_path) if title_path.is_file() else None,
                             'verified_db_signatures': _db_signatures(profile, db_names)})
                _replace_journal(journal, data, key)
            pages.index_thread(profile, shared, key, thread, executable, title=title,
                               allow_title_change=True, on_verified=mark_verified)
            output_report = None
            if report is not None:
                output_report = backup_root / ('mock-path-report-' + os.urandom(16).hex() + '.json')
                payload = {'format': 'cosyncx-mock-path-report-v1', 'thread': thread, **report}
                payload['authentication'] = ns._manifest_tag(payload, key)
                content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode('utf-8')
                data.update({'report_name': output_report.name, 'report_digest': hashlib.sha256(content).hexdigest()})
                _replace_journal(journal, data, key)
                fd = os.open(output_report, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'wb') as out:
                    out.write(content)
                    out.flush()
                    os.fsync(out.fileno())
            if not retain_journal:
                journal.unlink()
            return {'mock_only': True, 'path_report': output_report}
        except Exception as exc:
            # A subprocess timeout may mean our own Codex process still uses
            # the mock profile; do not attempt rollback until it is stopped.
            if not defer_rollback and not isinstance(exc, subprocess.SubprocessError):
                try:
                    rollback(profile, shared, backup_root, key)
                except Exception:
                    pass  # The authenticated journal remains for review.
            raise


def rollback(profile: Path, shared: Path, backup_root: Path, key: bytes) -> None:
    """Restore a verified interrupted mock transaction, or refuse unknown data."""
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock recovery paths')
    journal, latest = backup_root / 'mock-archive-pending.json', backup_root / 'latest'
    if (journal.is_symlink() or not journal.is_file() or journal.stat().st_size > 1024 * 1024
            or latest.is_symlink() or not latest.is_dir()):
        raise ns.SyncError('Mock archive recovery unavailable')
    mock_journal.ensure_clear(backup_root, except_name=journal.name)
    data = json.loads(journal.read_bytes())
    manifest_path = latest / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 1024 * 1024:
        raise ns.SyncError('Mock archive backup manifest unavailable')
    manifest = json.loads(manifest_path.read_bytes())
    if (not isinstance(data, dict) or data.get('version') != 1
            or not isinstance(data.get('thread'), str)
            or not isinstance(data.get('source_digest'), str)
            or re.fullmatch(r'[0-9a-f]{64}', data['source_digest']) is None
            or not isinstance(data.get('backup_manifest_digest'), str)
            or re.fullmatch(r'[0-9a-f]{64}', data['backup_manifest_digest']) is None
            or type(data.get('title_existed')) is not bool
            or not isinstance(data.get('previous_files'), list) or not data['previous_files']
            or not isinstance(data.get('previous_threads'), list) or not data['previous_threads']
            or any(not graph._uuid(x) for x in data['previous_threads'])
            or not isinstance(manifest, dict) or manifest.get('version') != 1
            or not isinstance(manifest.get('files'), dict)):
        raise ns.SyncError('Invalid mock archive recovery data')
    tag = data.get('authentication')
    if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key)):
        raise ns.SyncError('Mock archive journal is not authenticated')
    if _stable_digest(manifest_path) != data['backup_manifest_digest']:
        raise ns.SyncError('Mock archive backup manifest changed')
    report_file = None
    if 'report_name' in data or 'report_digest' in data:
        if (not isinstance(data.get('report_name'), str)
                or re.fullmatch(r'mock-path-report-[0-9a-f]{32}\.json', data['report_name']) is None
                or not isinstance(data.get('report_digest'), str)
                or re.fullmatch(r'[0-9a-f]{64}', data['report_digest']) is None):
            raise ns.SyncError('Invalid mock path report journal')
        report_file = backup_root / data['report_name']
        if report_file.is_symlink() or (report_file.exists() and
                (not report_file.is_file() or _stable_digest(report_file) != data['report_digest'])):
            raise ns.SyncError('Mock path report changed after interruption')
    verified_keys = {'verified_digest', 'verified_title_existed', 'verified_title_digest', 'verified_db_signatures'}
    has_verified = bool(verified_keys & set(data))
    if has_verified and (not verified_keys <= set(data)
            or not isinstance(data['verified_digest'], str) or re.fullmatch(r'[0-9a-f]{64}', data['verified_digest']) is None
            or type(data['verified_title_existed']) is not bool
            or (data['verified_title_existed'] and (not isinstance(data['verified_title_digest'], str)
                or re.fullmatch(r'[0-9a-f]{64}', data['verified_title_digest']) is None))
            or (not data['verified_title_existed'] and data['verified_title_digest'] is not None)
            or not isinstance(data['verified_db_signatures'], dict)):
        raise ns.SyncError('Invalid verified mock state')
    thread, names, entries = data['thread'], data['previous_files'], manifest['files']
    if (any(not isinstance(x, str) or not x.startswith('archived_sessions/') for x in names)
            or len(set(names)) != len(names) or not set(names) <= set(entries)):
        raise ns.SyncError('Invalid mock archived history selection')
    affected = set(data['previous_threads'])
    if len(affected) != len(data['previous_threads']) or thread not in affected:
        raise ns.SyncError('Invalid mock archived thread selection')
    with mock._mock_thread_lock(profile, thread):
        saved = {}
        for name, expected in entries.items():
            try:
                path = backup._resolve_file(latest, name)
            except backup.BackupError as exc:
                raise ns.SyncError('Mock archive backup file unavailable') from exc
            if not isinstance(expected, str) or ns._digest(path) != expected:
                raise ns.SyncError('Mock archive backup integrity failed')
            saved[name] = path
        catalog = ns._chat_catalog(latest, include_unsupported=True)
        backed_names = {item['path'].relative_to(latest).as_posix() for items in catalog.values() for item in items}
        if set(catalog) != affected or backed_names != set(names):
            raise ns.SyncError('Mock backup does not match archived chat')
        live_catalog = ns._chat_catalog(profile, include_unsupported=True)
        current = []
        for identity in affected:
            allowed = {entries[x['path'].relative_to(latest).as_posix()] for x in catalog[identity]}
            if identity == thread:
                allowed.add(data['source_digest'])
                if has_verified:
                    allowed.add(data['verified_digest'])
            items = live_catalog.get(identity, [])
            if any(ns._digest(item['path']) not in allowed for item in items):
                raise ns.SyncError('Mock history changed after interruption')
            current.extend(items)
        current_names = {item['path'].relative_to(profile).as_posix() for item in current}
        for name in names:
            path = profile
            for component in name.split('/'):
                path = path / component
                if path.is_symlink():
                    raise ns.SyncError('Mock recovery path is a link')
            if path.exists() and (not path.is_file() or name not in current_names):
                raise ns.SyncError('Mock recovery target was occupied')
        db_names = {name for name in entries if re.fullmatch(r'(?:state|thread_history)_\d+\.sqlite', name)}
        live_db = {path.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite') for path in profile.glob(pattern)}
        if 'state_5.sqlite' not in db_names or db_names != live_db:
            raise ns.SyncError('Mock database set changed after interruption')
        if has_verified and _db_signatures(profile, db_names) != data['verified_db_signatures']:
            raise ns.SyncError('Mock databases changed after verified indexing')
        title = profile / 'session_index.jsonl'
        if (title.is_symlink() or (title.exists() and not title.is_file())
                or data['title_existed'] != ('session_index.jsonl' in entries)):
            raise ns.SyncError('Mock title state changed after interruption')
        if has_verified:
            if (title.is_file() != data['verified_title_existed']
                    or (title.is_file() and _stable_digest(title) != data['verified_title_digest'])):
                raise ns.SyncError('Mock title changed after verified indexing')
        elif (not data['title_existed'] and title.exists()) or (data['title_existed'] and
                (not title.is_file() or ns._digest(title) != entries['session_index.jsonl'])):
            raise ns.SyncError('Mock title changed after interruption')
        for name in db_names:
            target = profile / name
            if target.is_symlink() or not target.is_file():
                raise ns.SyncError('Mock database unavailable')
            with sqlite3.connect(saved[name].as_uri() + '?mode=ro', uri=True) as source_db:
                if source_db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise ns.SyncError('Mock backup database is invalid')
        def copy_saved(name, destination):
            temporary = destination.with_name('.cosyncx-recover-' + os.urandom(12).hex())
            try:
                fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'wb') as out, saved[name].open('rb') as inp:
                    shutil.copyfileobj(inp, out, 1024 * 1024)
                    out.flush()
                    os.fsync(out.fileno())
                if ns._digest(temporary) != entries[name] or ns._digest(saved[name]) != entries[name]:
                    raise ns.SyncError('Mock backup changed during recovery')
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
        for item in current:
            if item['path'].relative_to(profile).as_posix() not in names:
                item['path'].unlink()
        for name in names:
            target = profile.joinpath(*name.split('/'))
            parent = profile
            for component in name.split('/')[:-1]:
                parent = parent / component
                if parent.is_symlink(): raise ns.SyncError('Mock recovery path is a link')
                parent.mkdir(mode=0o700, exist_ok=True)
                if not parent.is_dir(): raise ns.SyncError('Mock recovery directory unavailable')
            if target.is_symlink(): raise ns.SyncError('Mock recovery target is a link')
            copy_saved(name, target)
        if data['title_existed']:
            copy_saved('session_index.jsonl', title)
        elif has_verified and title.is_file():
            title.unlink()
        for name in sorted(db_names):
            with sqlite3.connect(saved[name].as_uri() + '?mode=ro', uri=True) as source_db:
                with sqlite3.connect(profile / name, timeout=1) as destination_db:
                    source_db.backup(destination_db)
                    if destination_db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                        raise ns.SyncError('Recovered mock database is invalid')
        if report_file is not None and report_file.is_file():
            report_file.unlink()
        journal.unlink()
