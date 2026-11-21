"""Add complete paginated threads to an authenticated disposable profile.

This is not a production receiver: it only accepts mock profiles under the
system temporary directory and deliberately refuses existing thread IDs.
"""
from pathlib import Path
from collections import Counter
from contextlib import closing
import hashlib
import hmac
import json
import os
import queue
import sqlite3
import subprocess
import tempfile
import threading
import time

import cosync_lineage as lineage
import cosync_receive_plan
import cosync_restore_backup as backup
import mock_chat_sync as mock
import mock_journal
import nexus_sync as ns


def _text_tag(role: str, text: str) -> str:
    return role + ':' + hashlib.sha256(text.encode('utf-8')).hexdigest()


def _expected_visible_messages(path: Path, *, subagent: bool = False) -> Counter:
    """Compare message hashes, never print or persist the message text."""
    responses, events, completed = Counter(), Counter(), Counter()
    start_ordinal = None
    with path.open('rb') as stream:
        number = -1
        while line := stream.readline(lineage.MAX_RECORD_BYTES + 1):
            number += 1
            if len(line) > lineage.MAX_RECORD_BYTES:
                raise ns.SyncError('Mock record is too large')
            if not line.endswith(b'\n'):
                raise ns.SyncError('Incomplete mock record')
            row = json.loads(line)
            payload = row.get('payload') if isinstance(row, dict) else None
            if not isinstance(payload, dict):
                continue
            if number == 0 and subagent:
                start_ordinal = payload.get('subagent_history_start_ordinal')
                # Some standalone subagent rollouts have no inherited-history
                # boundary. Compare their actual records from the beginning;
                # the Codex response still has to contain the expected items.
                if start_ordinal is None:
                    start_ordinal = 0
                if type(start_ordinal) is not int or start_ordinal < 0:
                    raise ns.SyncError('Mock subagent history boundary is unavailable')
            if subagent and number > 0:
                if type(row.get('ordinal')) is not int:
                    raise ns.SyncError('Mock subagent ordinal is unavailable')
                if row['ordinal'] < start_ordinal:
                    continue
            kind = payload.get('type')
            if row.get('type') == 'response_item' and kind == 'message' and payload.get('role') == 'user':
                parts = payload.get('content')
                if not isinstance(parts, list):
                    raise ns.SyncError('Unsupported mock user content')
                texts = []
                for part in parts:
                    if not isinstance(part, dict):
                        raise ns.SyncError('Unsupported mock user content')
                    if part.get('type') == 'input_text' and isinstance(part.get('text'), str):
                        texts.append(part['text'])
                    elif (part.get('type') == 'input_image' and isinstance(part.get('image_url'), str)
                          and part['image_url'].startswith('data:')):
                        responses[_text_tag('image', part['image_url'])] += 1
                    else:
                        raise ns.SyncError('Unsupported mock user content')
                responses[_text_tag('user', ''.join(texts))] += 1
            elif row.get('type') == 'response_item' and kind == 'message' and payload.get('role') == 'assistant':
                parts = payload.get('content')
                if not isinstance(parts, list) or any(not isinstance(part, dict) or part.get('type') != 'output_text'
                                                      or not isinstance(part.get('text'), str) for part in parts):
                    raise ns.SyncError('Unsupported mock assistant content')
                responses[_text_tag('assistant', ''.join(part['text'] for part in parts))] += 1
            elif row.get('type') == 'event_msg' and kind == 'user_message':
                if payload.get('text_elements'):
                    raise ns.SyncError('Unsupported mock user attachments')
                images = payload.get('images') or []
                if not isinstance(images, list) or any(not isinstance(image, str) or not image.startswith('data:') for image in images):
                    raise ns.SyncError('Unsupported mock user attachments')
                for image in images:
                    events[_text_tag('image', image)] += 1
                local_images = payload.get('local_images') or []
                if not isinstance(local_images, list) or any(not isinstance(image, str) for image in local_images):
                    raise ns.SyncError('Unsupported mock user attachments')
                for image in local_images:
                    events[_text_tag('local_image', image)] += 1
                if isinstance(payload.get('message'), str):
                    events[_text_tag('user', payload['message'])] += 1
            elif row.get('type') == 'event_msg' and kind == 'agent_message' and isinstance(payload.get('message'), str):
                events[_text_tag('assistant', payload['message'])] += 1
            elif row.get('type') == 'event_msg' and kind == 'item_completed':
                item = payload.get('item')
                if not isinstance(item, dict) or item.get('type') not in ('UserMessage', 'AgentMessage'):
                    continue
                role = 'user' if item['type'] == 'UserMessage' else 'assistant'
                expected_type = 'text' if role == 'user' else 'Text'
                parts = item.get('content')
                if not isinstance(parts, list):
                    raise ns.SyncError('Unsupported mock completed message')
                texts = []
                for part in parts:
                    if not isinstance(part, dict):
                        raise ns.SyncError('Unsupported mock completed message')
                    if part.get('type') == expected_type and isinstance(part.get('text'), str) and not part.get('text_elements'):
                        texts.append(part['text'])
                    elif (role == 'user' and part.get('type') == 'image' and isinstance(part.get('image_url'), str)
                          and part['image_url'].startswith('data:')):
                        completed[_text_tag('image', part['image_url'])] += 1
                    elif role == 'user' and part.get('type') == 'local_image' and isinstance(part.get('path'), str):
                        completed[_text_tag('local_image', part['path'])] += 1
                    else:
                        raise ns.SyncError('Unsupported mock completed message')
                completed[_text_tag(role, ''.join(texts))] += 1
    # Paginated rollouts can contain model-only user input that never appears
    # in the UI. Completed message items are the visible conversation; older
    # histories without them fall back to response/event records.
    expected = Counter()
    for role in ('user', 'assistant', 'image', 'local_image'):
        prefix = role + ':'
        completed_role = Counter({key: value for key, value in completed.items() if key.startswith(prefix)})
        response_role = Counter({key: value for key, value in responses.items() if key.startswith(prefix)})
        event_role = Counter({key: value for key, value in events.items() if key.startswith(prefix)})
        has_completed_user = any(key.startswith('user:') for key in completed)
        if completed_role or (role in ('image', 'local_image') and has_completed_user):
            expected.update(completed_role | event_role)
        elif not (subagent and role in ('user', 'image', 'local_image')):
            expected.update(completed_role | event_role | response_role)
    return expected


def prepare_flattened_mock(source: Path, destination: Path, thread_id: str,
                           mappings: list[tuple[str, str]], *, fallback_root: Path | None = None) -> int:
    """Experimentally flatten one linear mock lineage into a new isolated copy.

    Original files are untouched. This is not enabled in a real receiver:
    external references to physical pages and future updates need validation.
    """
    source, destination = mock._mock_path(source), mock._mock_path(destination)
    return _prepare_flattened(source, destination, thread_id, mappings, fallback_root=fallback_root)


def _prepare_flattened(source: Path, destination: Path, thread_id: str,
                       mappings: list[tuple[str, str]], *, fallback_root: Path | None = None,
                       preserve_unmapped_history: bool = False) -> int:
    """Prepare a lineage only in a new temporary directory; source is read-only."""
    source = source.resolve(strict=True)
    destination = destination.parent.resolve(strict=True) / destination.name
    temporary = Path(tempfile.gettempdir()).resolve(strict=True)
    if (not source.is_dir() or destination.exists() or destination.is_symlink()
            or temporary not in destination.parents or '.codex' in {p.casefold() for p in destination.parts}
            or source in destination.parents or destination in source.parents):
        raise ns.SyncError('Mock flatten directories overlap')
    with tempfile.TemporaryDirectory(prefix='cosyncx-flat-stage-') as temp:
        prepared = Path(temp).resolve() / 'prepared'
        lineage.prepare_lineage(source, prepared, thread_id, mappings, fallback_root=fallback_root,
                                preserve_unmapped_history=preserve_unmapped_history)
        items = ns._chat_catalog(prepared, include_unsupported=True)[thread_id]
        pages = {lineage._page_id(item['path'], thread_id): item for item in items}
        parents = {page: str(item['history_base']['thread_id']) for page, item in pages.items()
                   if item.get('history_base')}
        leaves = set(pages) - set(parents.values())
        if len(leaves) != 1:
            raise ns.SyncError('Branched mock histories cannot be flattened')
        chain = [leaves.pop()]
        while chain[-1] in parents:
            chain.append(parents[chain[-1]])
        chain.reverse()
        if len(chain) != len(pages):
            raise ns.SyncError('Mock lineage is not a single chain')
        def nested_ordinal(value):
            if isinstance(value, dict):
                return (any('ordinal' in str(key).lower() or 'byte_offset' in str(key).lower()
                            or key == 'history_base' for key in value)
                        or any(nested_ordinal(child) for child in value.values()))
            if isinstance(value, list):
                return any(nested_ordinal(child) for child in value)
            return False
        with pages[chain[-1]]['path'].open('rb') as stream:
            header = stream.readline(lineage.MAX_RECORD_BYTES + 1)
        if len(header) > lineage.MAX_RECORD_BYTES or not header.endswith(b'\n'):
            raise ns.SyncError('Invalid mock session metadata')
        leaf_meta = json.loads(header)
        if not isinstance(leaf_meta, dict) or not isinstance(leaf_meta.get('payload'), dict):
            raise ns.SyncError('Invalid mock session metadata')
        leaf_meta['payload'].pop('history_base', None)
        leaf_meta['ordinal'] = 0
        root_path = pages[chain[0]]['path'].relative_to(prepared)
        with tempfile.TemporaryDirectory(prefix='cosyncx-flat-output-', dir=destination.parent) as staged:
            staged = Path(staged)
            path = staged / root_path
            path.parent.mkdir(mode=0o700, parents=True)
            last_ordinal, count = -1, 1
            with path.open('x', encoding='utf-8') as output:
                output.write(json.dumps(leaf_meta, ensure_ascii=False, separators=(',', ':')) + '\n')
                for index, page in enumerate(chain):
                    source_path = pages[page]['path']
                    limit = (pages[chain[index + 1]]['history_base']['end_byte_offset']
                             if index + 1 < len(chain) else source_path.stat().st_size)
                    position, number = 0, 0
                    with source_path.open('rb') as stream:
                        while position < limit:
                            line = stream.readline(lineage.MAX_RECORD_BYTES + 1)
                            if not line or len(line) > lineage.MAX_RECORD_BYTES or not line.endswith(b'\n'):
                                raise ns.SyncError('Incomplete or oversized mock page record')
                            position += len(line)
                            if position > limit:
                                raise ns.SyncError('Mock page boundary is not a record boundary')
                            row = json.loads(line)
                            if not isinstance(row, dict) or type(row.get('ordinal')) is not int or row['ordinal'] <= last_ordinal:
                                raise ns.SyncError('Mock ordinals are ambiguous')
                            last_ordinal = row['ordinal']
                            if number:
                                if nested_ordinal(row.get('payload')):
                                    raise ns.SyncError('Nested ordinal is unsupported')
                                row['ordinal'] = count
                                output.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
                                count += 1
                            number += 1
            destination.mkdir(mode=0o700)
            for child in staged.iterdir():
                os.replace(child, destination / child.name)
            report = prepared / 'cosyncx-path-report.json'
            if report.is_file() and not report.is_symlink():
                os.replace(report, destination / report.name)
        return count


def add_thread(source: Path, profile: Path, shared: Path, key: bytes,
               thread_id: str, mappings: list[tuple[str, str]]) -> int:
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared = mock._mock_path(shared)
    if (not source.is_dir() or not shared.is_dir() or source == profile or source == shared
            or source in profile.parents or profile in source.parents
            or source in shared.parents or shared in source.parents):
        raise ns.SyncError('Mock folders overlap')
    with tempfile.TemporaryDirectory(prefix='cosyncx-pages-') as temporary:
        prepared = Path(temporary).resolve() / 'prepared'
        count = lineage.prepare_lineage(source, prepared, thread_id, mappings)
        if (prepared / 'cosyncx-path-report.json').exists():
            raise ns.SyncError('Mock missing-path report needs delivery before adding a thread')
        files = [item['path'] for item in ns._chat_catalog(prepared, include_unsupported=True)[thread_id]]
        # The mock lock follows the Codex thread-writer protocol. No destination
        # is replaced; an interrupted batch is reported, not blindly retried.
        with mock._mock_thread_lock(profile, thread_id):
            if thread_id in ns._chat_catalog(profile, include_unsupported=True):
                raise ns.SyncError('Mock thread already exists')
            created = []
            try:
                for source_file in sorted(files):
                    relative = source_file.relative_to(prepared)
                    if not relative.parts or relative.parts[0] != 'sessions':
                        raise ns.SyncError('Expected active mock pages')
                    parent = profile
                    for component in relative.parts[:-1]:
                        parent = parent / component
                        if parent.is_symlink():
                            raise ns.SyncError('Mock destination contains a symlink')
                        parent.mkdir(mode=0o700, exist_ok=True)
                        if not parent.is_dir():
                            raise ns.SyncError('Mock destination is not a directory')
                    destination = parent / relative.name
                    if destination.exists() or destination.is_symlink():
                        raise ns.SyncError('Mock destination already exists')
                    os.link(source_file, destination, follow_symlinks=False)
                    created.append(destination)
            except (OSError, ns.SyncError):
                for destination in reversed(created):
                    destination.unlink(missing_ok=True)
                raise
        return count


def update_thread(source: Path, profile: Path, shared: Path, backup_root: Path, key: bytes,
                  thread_id: str, mappings: list[tuple[str, str]]) -> bool:
    """Apply only a proven continuation in a disposable profile, with backup.

    A pre-existing journal means a previous operation needs manual recovery.
    Concurrent writer locks are held across comparison, backup and replacement.
    """
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not source.is_dir() or not shared.is_dir() or not backup_root.is_dir()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((source, profile), (source, shared), (source, backup_root),
                                (profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Mock folders overlap')
    journal = backup_root / 'mock-update-pending.json'
    mock_journal.ensure_clear(backup_root)
    with tempfile.TemporaryDirectory(prefix='cosyncx-update-') as temp:
        prepared = Path(temp).resolve() / 'prepared'
        lineage.prepare_lineage(source, prepared, thread_id, mappings)
        incoming = ns._chat_catalog(prepared, include_unsupported=True)[thread_id]
        with mock._mock_thread_lock(profile, thread_id):
            local = ns._chat_catalog(profile, include_unsupported=True).get(thread_id, [])
            if not local or any(item['folder'] != 'sessions' for item in local):
                raise ns.SyncError('Mock thread is missing or archived')
            old = {item['path'].name: item['path'] for item in local}
            new = {item['path'].name: item['path'] for item in incoming}
            if not set(old) <= set(new):
                raise ns.SyncError('Mock update would remove history pages')
            changed = []
            local_ahead = False
            for name, old_path in old.items():
                relation = ns._path_adjusted_relation(new[name], old_path, str(profile), str(profile))
                if relation == 'INCOMING_EXTENDS_AFTER_PATH_MAPPING':
                    changed.append(name)
                elif relation == 'LOCAL_EXTENDS_AFTER_PATH_MAPPING':
                    local_ahead = True
                elif relation not in ('EQUIVALENT_AFTER_PATH_MAPPING', 'LOCAL_EXTENDS_AFTER_PATH_MAPPING'):
                    raise ns.SyncError('Mock history diverged')
            added = set(new) - set(old)
            if local_ahead and (changed or added):
                raise ns.SyncError('Mock local continuation must be preserved')
            if not changed and not added:
                return False
            backup_names = [old[name].relative_to(profile).as_posix() for name in old]
            backup_names += [path.name for path in profile.glob('state_*.sqlite') if path.is_file() and not path.is_symlink()]
            backup_names += [path.name for path in profile.glob('thread_history_*.sqlite') if path.is_file() and not path.is_symlink()]
            title_index = profile / 'session_index.jsonl'
            if title_index.is_file() and not title_index.is_symlink():
                backup_names.append(title_index.name)
            expected = {name: ns._digest(path) for name, path in old.items()}
            def verify_backup(_stage, manifest):
                for name, path in old.items():
                    relative = path.relative_to(profile).as_posix()
                    if ns._digest(path) != expected[name] or manifest.get(relative) != expected[name]:
                        raise ns.SyncError('Mock history changed during backup')
            latest = backup.replace_latest(profile, backup_root, backup_names, pre_commit=verify_backup)
            pending = []
            try:
                for name in changed:
                    destination = old[name]
                    temporary = destination.with_name('.cosyncx-mock-' + os.urandom(12).hex())
                    ns._copy_stable_chat(new[name], temporary)
                    pending.append((temporary, destination))
                operations = []
                for name in changed:
                    operations.append({'path': old[name].relative_to(profile).as_posix(),
                                       'before': expected[name], 'after': ns._digest(new[name])})
                for name in sorted(added):
                    item = next(item for item in incoming if item['path'].name == name)
                    operations.append({'path': item['path'].relative_to(prepared).as_posix(),
                                       'before': None, 'after': ns._digest(item['path'])})
                recovery = {'version': 1, 'thread': thread_id, 'operations': operations,
                            'backup_manifest_digest': ns._digest(latest / 'manifest.json')}
                recovery['authentication'] = ns._manifest_tag(recovery, key)
                fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    json.dump(recovery, stream, sort_keys=True)
                    stream.flush()
                    os.fsync(stream.fileno())
                if any(ns._digest(path) != expected[name] for name, path in old.items()):
                    raise ns.SyncError('Mock history changed during backup')
                for temporary, destination in pending:
                    os.replace(temporary, destination)
                for name in sorted(added):
                    item = next(item for item in incoming if item['path'].name == name)
                    relative = item['path'].relative_to(prepared)
                    if relative.parts[0] != 'sessions':
                        raise ns.SyncError('Invalid mock destination')
                    # New pages must remain beside the existing lineage; no
                    # implicit directory creation or overwrite is permitted.
                    destination = profile / relative
                    if destination.parent.is_symlink() or not destination.parent.is_dir():
                        raise ns.SyncError('Mock page directory is unavailable')
                    os.link(item['path'], destination, follow_symlinks=False)
                journal.unlink()
                return True
            finally:
                for temporary, _ in pending:
                    temporary.unlink(missing_ok=True)


def recover_pending(profile: Path, shared: Path, backup_root: Path, key: bytes) -> None:
    """Roll back an interrupted mock update if all files match its journal."""
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir() or profile == backup_root
            or profile in backup_root.parents or backup_root in profile.parents):
        raise ns.SyncError('Invalid mock recovery folders')
    journal, latest = backup_root / 'mock-update-pending.json', backup_root / 'latest'
    if journal.is_symlink() or not journal.is_file() or journal.stat().st_size > 1024 * 1024 or latest.is_symlink():
        raise ns.SyncError('Mock recovery journal unavailable')
    mock_journal.ensure_clear(backup_root, except_name=journal.name)
    data = json.loads(journal.read_text(encoding='utf-8'))
    manifest_path = latest / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 1024 * 1024:
        raise ns.SyncError('Mock backup manifest unavailable')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if (not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('thread'), str)
            or not isinstance(data.get('operations'), list) or not data['operations']
            or not isinstance(manifest, dict) or not isinstance(manifest.get('files'), dict)):
        raise ns.SyncError('Invalid mock recovery data')
    tag = data.get('authentication')
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key))
            or data.get('backup_manifest_digest') != ns._digest(manifest_path)):
        raise ns.SyncError('Mock recovery journal or backup is not authenticated')
    with mock._mock_thread_lock(profile, data['thread']):
        checked = []
        for item in data['operations']:
            if not isinstance(item, dict) or set(item) != {'path', 'before', 'after'}:
                raise ns.SyncError('Invalid mock recovery entry')
            name, before, after = item['path'], item['before'], item['after']
            if (not isinstance(name, str) or not name.startswith('sessions/') or '\\' in name
                    or any(part in ('', '.', '..') for part in name.split('/'))
                    or not isinstance(after, str) or len(after) != 64
                    or (before is not None and (not isinstance(before, str) or len(before) != 64))):
                raise ns.SyncError('Invalid mock recovery path')
            destination = profile.joinpath(*name.split('/'))
            parent = profile
            for part in name.split('/')[:-1]:
                parent /= part
                if parent.is_symlink() or not parent.is_dir():
                    raise ns.SyncError('Mock recovery parent unavailable')
            if destination.is_symlink() or (destination.exists() and not destination.is_file()):
                raise ns.SyncError('Mock recovery destination unsafe')
            current = ns._digest(destination) if destination.exists() else None
            if current not in (before, after):
                raise ns.SyncError('Mock destination changed after interruption')
            saved = None
            if before is not None:
                try:
                    saved = backup._resolve_file(latest, name)
                except backup.BackupError as exc:
                    raise ns.SyncError('Mock backup file unavailable') from exc
                if manifest['files'].get(name) != before or ns._digest(saved) != before:
                    raise ns.SyncError('Mock backup does not match journal')
            checked.append((destination, saved, current, before))
        for destination, saved, current, before in checked:
            if saved is None:
                if destination.exists():
                    destination.unlink()
            elif current != before:
                temporary = destination.with_name('.cosyncx-recover-' + os.urandom(12).hex())
                try:
                    ns._copy_stable_chat(saved, temporary)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
        journal.unlink()


def reset_mock_projection(profile: Path, shared: Path, backup_root: Path, key: bytes, thread_id: str) -> None:
    """Experimental scoped index reset, allowed only for an authenticated mock.

    This uses an internal Codex schema, not a stable Codex API. It refuses any
    schema other than the locally tested version and needs a verified backup.
    """
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if not shared.is_dir() or not backup_root.is_dir():
        raise ns.SyncError('Mock projection folders unavailable')
    _reset_profile_projection(profile, backup_root, thread_id)


def _reset_profile_projection(profile: Path, backup_root: Path, thread_id: str, *,
                              ignore_busy: frozenset[str] = frozenset()) -> None:
    """Reset one verified projection after the caller has guarded and backed up the profile."""
    database = profile / 'thread_history_1.sqlite'
    latest = backup_root / 'latest'
    if database.is_symlink() or not database.is_file() or latest.is_symlink():
        raise ns.SyncError('Mock projection database unavailable')
    manifest_path = latest / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ns.SyncError('Mock backup unavailable')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    try:
        saved = backup._resolve_file(latest, database.name)
    except backup.BackupError as exc:
        raise ns.SyncError('Mock projection backup unavailable') from exc
    if (not isinstance(manifest, dict) or not isinstance(manifest.get('files'), dict)
            or manifest['files'].get(database.name) != ns._digest(saved)):
        raise ns.SyncError('Mock projection backup mismatch')
    expected = {
        'thread_turns': {'thread_id', 'turn_id', 'rollout_ordinal', 'status', 'error_json', 'started_at',
                         'completed_at', 'duration_ms', 'first_user_item_id', 'final_agent_item_id',
                         'rollout_byte_offset', 'rollout_end_ordinal', 'rollout_end_byte_offset', 'root_turn_id'},
        'thread_items': {'thread_id', 'turn_id', 'item_id', 'rollout_ordinal', 'created_at_ms', 'item_json',
                         'item_type', 'updated_at_ordinal', 'started_at_ms', 'completed_at_ms'},
        'thread_history_projection_state': {'thread_id', 'next_rollout_byte_offset', 'next_rollout_ordinal'},
        'thread_realtime_items': {'thread_id', 'item_id', 'rollout_ordinal', 'created_at_ms', 'item_type', 'item_json'},
    }
    with mock._mock_thread_lock(profile, thread_id, ignore_busy=ignore_busy):
        with closing(sqlite3.connect(database, timeout=1)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if tables != set(expected) | {'_sqlx_migrations'}:
                raise ns.SyncError('Unknown mock projection tables')
            migrations = list(db.execute('SELECT version, success FROM _sqlx_migrations ORDER BY version'))
            if migrations != [(number, 1) for number in range(1, 9)]:
                raise ns.SyncError('Unknown mock projection migrations')
            for table, columns in expected.items():
                if {row[1] for row in db.execute('PRAGMA table_info(' + table + ')')} != columns:
                    raise ns.SyncError('Unknown mock projection schema')
            extras = list(db.execute("SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'view')"))
            if extras != [('trigger', 'thread_realtime_items_projection_cleanup')]:
                raise ns.SyncError('Unknown mock projection triggers')
            if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                raise ns.SyncError('Mock projection database failed integrity check')
            for table in ('thread_items', 'thread_turns', 'thread_realtime_items', 'thread_history_projection_state'):
                db.execute('DELETE FROM ' + table + ' WHERE thread_id = ?', (thread_id,))


def index_thread(profile: Path, shared: Path, key: bytes, thread_id: str,
                 executable: Path, *, title: str | None = None, allow_title_change: bool = False,
                 on_verified=None) -> None:
    """Verify a complete mock thread through an isolated real Codex server."""
    profile = mock._validate_profile(profile, key)
    shared = mock._mock_path(shared)
    executable = executable.resolve(strict=True)
    if not shared.is_dir() or not executable.is_file():
        raise ns.SyncError('Invalid mock index configuration')
    return _index_profile(profile, thread_id, executable, title=title,
                          allow_title_change=allow_title_change, on_verified=on_verified)


def _index_profile(profile: Path, thread_id: str, executable: Path, *, title: str | None = None,
                   allow_title_change: bool = False, on_verified=None,
                   ignore_busy: frozenset[str] = frozenset()) -> None:
    """Codex index operation; caller must validate and coordinate the profile."""
    if title is not None:
        if not isinstance(title, str) or not title:
            raise ns.SyncError('Invalid mock chat title')
        title_data = ns._read_title_index(profile)
        if title_data is not None:
            names = {row['id']: row['thread_name'] for row in ns._parse_title_index(title_data)}
            if thread_id in names and names[thread_id] != title and not allow_title_change:
                raise ns.SyncError('Mock title conflict')
    pages = ns._chat_catalog(profile, include_unsupported=True).get(thread_id, [])
    if not pages or any(item['folder'] != 'sessions' for item in pages):
        raise ns.SyncError('Mock thread is missing or not active')
    if ns._paged_lineage(pages, thread_id) != 'PRESENT_UNVERIFIED':
        raise ns.SyncError('Mock lineage is incomplete')
    if len(pages) > 1:
        raise ns.SyncError('Multi-page mock projection is not verified')
    kind = cosync_receive_plan.source_kind(pages)
    if kind not in ('chat', 'subagent') or (kind == 'subagent' and title is not None):
        raise ns.SyncError('Unsupported mock thread source')
    page_ids = {lineage._page_id(item['path'], thread_id): item for item in pages}
    parents = {item['history_base']['thread_id'] for item in pages if item.get('history_base')}
    leaves = set(page_ids) - parents
    if len(leaves) != 1:
        raise ns.SyncError('Branched mock lineage is unsupported')
    leaf = page_ids[leaves.pop()]
    expected_messages = _expected_visible_messages(leaf['path'], subagent=kind == 'subagent')
    if kind == 'chat' and not any(key.startswith('user:') for key in expected_messages):
        raise ns.SyncError('Mock conversation has no verifiable user messages')
    with mock._mock_thread_lock(profile, thread_id, ignore_busy=ignore_busy):
        pass
    env = {name: value for name, value in os.environ.items()
           if name not in ('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_ADMIN_KEY', 'CHATGPT_ACCESS_TOKEN')}
    env['CODEX_HOME'] = str(profile)
    process = subprocess.Popen([str(executable), 'app-server', '--listen', 'stdio://'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    output = queue.Queue()
    def pump():
        try:
            while line := process.stdout.readline(32 * 1024 * 1024 + 1):
                if len(line) > 32 * 1024 * 1024 or not line.endswith(b'\n'):
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    break
                output.put(line)
        finally:
            output.put(None)
    threading.Thread(target=pump, daemon=True).start()
    def call(number, method, params):
        process.stdin.write((json.dumps({'id': number, 'method': method, 'params': params}) + '\n').encode())
        process.stdin.flush()
        deadline = time.monotonic() + 20
        while True:
            try:
                line = output.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty as exc:
                raise ns.SyncError('Mock Codex index timed out') from exc
            if line is None:
                raise ns.SyncError('Mock Codex index terminated')
            response = json.loads(line)
            if isinstance(response, dict) and response.get('id') == number:
                if 'error' in response or 'result' not in response:
                    raise ns.SyncError('Mock Codex rejected index request')
                return response['result']
    verified = False
    try:
        call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-mock', 'version': '0.1'},
                                'capabilities': {'experimentalApi': True}})
        process.stdin.write(b'{"method":"initialized","params":{}}\n')
        process.stdin.flush()
        call(2, 'thread/resume', {'threadId': thread_id, 'path': str(leaf['path']),
                                  'cwd': leaf['cwd'], 'excludeTurns': True})
        number = 3
        if title is not None:
            call(number, 'thread/name/set', {'threadId': thread_id, 'name': title})
            number += 1
        cursor, seen, actual_messages = None, set(), Counter()
        while True:
            # Listing individual items keeps image-heavy conversations from
            # returning the entire history or many large turns in one response.
            params = {'threadId': thread_id, 'limit': 1 if leaf['path'].stat().st_size > ns.MAX_CHAT_BYTES else 10}
            if cursor is not None:
                params['cursor'] = cursor
            result = call(number, 'thread/items/list', params)
            number += 1
            if not isinstance(result, dict) or not isinstance(result.get('data'), list) or 'nextCursor' not in result:
                raise ns.SyncError('Invalid mock turns result')
            for entry in result['data']:
                if not isinstance(entry, dict) or not isinstance(entry.get('item'), dict):
                    raise ns.SyncError('Invalid mock turn items')
                for item in (entry['item'],):
                    if not isinstance(item, dict):
                        continue
                    if item.get('type') == 'agentMessage':
                        if not isinstance(item.get('text'), str):
                            raise ns.SyncError('Invalid indexed mock assistant content')
                        actual_messages[_text_tag('assistant', item['text'])] += 1
                        continue
                    if item.get('type') != 'userMessage':
                        continue
                    parts = item.get('content')
                    if not isinstance(parts, list):
                        raise ns.SyncError('Unsupported indexed mock user content')
                    texts = []
                    for part in parts:
                        if not isinstance(part, dict):
                            raise ns.SyncError('Unsupported indexed mock user content')
                        if part.get('type') == 'text' and isinstance(part.get('text'), str) and not part.get('text_elements'):
                            texts.append(part['text'])
                        elif (part.get('type') == 'image' and isinstance(part.get('url'), str)
                              and part['url'].startswith('data:')):
                            actual_messages[_text_tag('image', part['url'])] += 1
                        elif part.get('type') == 'localImage' and isinstance(part.get('path'), str):
                            actual_messages[_text_tag('local_image', part['path'])] += 1
                        else:
                            raise ns.SyncError('Unsupported indexed mock user content')
                    actual_messages[_text_tag('user', ''.join(texts))] += 1
            cursor = result['nextCursor']
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in seen or len(seen) >= 100000:
                raise ns.SyncError('Invalid mock turns cursor')
            seen.add(cursor)
        if expected_messages - actual_messages:
            raise ns.SyncError('Mock messages are missing from the index')
        if kind == 'subagent':
            result = call(number, 'thread/read', {'threadId': thread_id, 'includeTurns': False})
            if not isinstance(result, dict) or not isinstance(result.get('thread'), dict) or result['thread'].get('id') != thread_id:
                raise ns.SyncError('Mock subagent cannot be read')
            verified = True
            return
        cursor, seen = None, set()
        while True:
            params = {'limit': 100, 'useStateDbOnly': True}
            if cursor is not None:
                params['cursor'] = cursor
            result = call(number, 'thread/list', params)
            number += 1
            if not isinstance(result, dict) or not isinstance(result.get('data'), list) or 'nextCursor' not in result:
                raise ns.SyncError('Invalid mock index result')
            for item in result['data']:
                if isinstance(item, dict) and item.get('id') == thread_id:
                    if title is not None and item.get('name') != title:
                        raise ns.SyncError('Mock title was not indexed')
                    if item.get('cwd') != leaf['cwd']:
                        raise ns.SyncError('Mock workspace was not indexed')
                    verified = True
                    return
            cursor = result['nextCursor']
            if cursor is None:
                raise ns.SyncError('Mock thread not found in index')
            if not isinstance(cursor, str) or not cursor or cursor in seen or len(seen) >= 1000:
                raise ns.SyncError('Invalid mock index cursor')
            seen.add(cursor)
    finally:
        try:
            process.stdin.close()
        except OSError:
            pass
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
            raise ns.SyncError('Mock Codex server did not stop')
        process.stdout.close()
        if verified and on_verified is not None:
            on_verified()


def replace_mock_titles(profile: Path, shared: Path, backup_root: Path, key: bytes,
                        titles: dict[str, str], executable: Path) -> int:
    """Replace local mock names after one verified backup of all title state."""
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Mock title folders overlap')
    if not isinstance(titles, dict) or not titles or any(not isinstance(k, str) or not isinstance(v, str) or not v
                                                         for k, v in titles.items()):
        raise ns.SyncError('Invalid mock titles')
    journal = backup_root / 'mock-title-pending.json'
    mock_journal.ensure_clear(backup_root)
    catalog = ns._chat_catalog(profile, include_unsupported=True)
    for thread in titles:
        entries = catalog.get(thread, [])
        if len(entries) != 1 or entries[0]['folder'] != 'sessions' or entries[0]['unsupported'] != 'PAGINATED':
            raise ns.SyncError('Mock title target is not a supported active chat')
    backup_names = [path.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite')
                    for path in profile.glob(pattern) if path.is_file() and not path.is_symlink()]
    if (profile / 'session_index.jsonl').is_file() and not (profile / 'session_index.jsonl').is_symlink():
        backup_names.append('session_index.jsonl')
    if not backup_names:
        raise ns.SyncError('Mock title state is unavailable')
    with mock._mock_thread_lock(profile, next(iter(titles))):
        backup.replace_latest(profile, backup_root, backup_names)
        fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({'version': 1, 'threads': sorted(titles)}, stream)
            stream.flush()
            os.fsync(stream.fileno())
    for thread, title in sorted(titles.items()):
        index_thread(profile, shared, key, thread, executable, title=title, allow_title_change=True)
    journal.unlink()
    return len(titles)
