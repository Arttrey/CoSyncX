"""Add complete paginated threads to an authenticated disposable profile.

This is not a production receiver: it only accepts mock profiles under the
system temporary directory and deliberately refuses existing thread IDs.
"""
from pathlib import Path
from collections import Counter
import hashlib
import json
import os
import queue
import sqlite3
import subprocess
import tempfile
import threading
import time

import cosync_lineage as lineage
import cosync_restore_backup as backup
import mock_chat_sync as mock
import nexus_sync as ns


def _text_tag(role: str, text: str) -> str:
    return role + ':' + hashlib.sha256(text.encode('utf-8')).hexdigest()


def _expected_visible_messages(path: Path) -> Counter:
    """Compare message hashes, never print or persist the message text."""
    responses, events, completed = Counter(), Counter(), Counter()
    with path.open('rb') as stream:
        for line in stream:
            if not line.endswith(b'\n'):
                raise ns.SyncError('Incomplete mock record')
            row = json.loads(line)
            payload = row.get('payload') if isinstance(row, dict) else None
            if not isinstance(payload, dict):
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
                if payload.get('local_images') or payload.get('text_elements'):
                    raise ns.SyncError('Unsupported mock user attachments')
                images = payload.get('images') or []
                if not isinstance(images, list) or any(not isinstance(image, str) or not image.startswith('data:') for image in images):
                    raise ns.SyncError('Unsupported mock user attachments')
                for image in images:
                    events[_text_tag('image', image)] += 1
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
                    else:
                        raise ns.SyncError('Unsupported mock completed message')
                completed[_text_tag(role, ''.join(texts))] += 1
    # Paginated rollouts can contain model-only user input that never appears
    # in the UI. Completed message items are the visible conversation; older
    # histories without them fall back to response/event records.
    expected = Counter()
    for role in ('user', 'assistant', 'image'):
        prefix = role + ':'
        completed_role = Counter({key: value for key, value in completed.items() if key.startswith(prefix)})
        response_role = Counter({key: value for key, value in responses.items() if key.startswith(prefix)})
        event_role = Counter({key: value for key, value in events.items() if key.startswith(prefix)})
        has_completed_user = any(key.startswith('user:') for key in completed)
        if completed_role or (role == 'image' and has_completed_user):
            expected.update(completed_role | event_role)
        else:
            expected.update(completed_role | event_role | response_role)
    return expected


def prepare_flattened_mock(source: Path, destination: Path, thread_id: str,
                           mappings: list[tuple[str, str]]) -> int:
    """Experimentally flatten one linear mock lineage into a new isolated copy.

    Original files are untouched. This is not enabled in a real receiver:
    external references to physical pages and future updates need validation.
    """
    source, destination = mock._mock_path(source), mock._mock_path(destination)
    if (not source.is_dir() or destination.exists() or destination.is_symlink()
            or source in destination.parents or destination in source.parents):
        raise ns.SyncError('Mock flatten directories overlap')
    with tempfile.TemporaryDirectory(prefix='cosyncx-flat-stage-') as temp:
        prepared = Path(temp).resolve() / 'prepared'
        lineage.prepare_lineage(source, prepared, thread_id, mappings)
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
        records = []
        leaf_meta = None
        last_ordinal = -1
        for index, page in enumerate(chain):
            path = pages[page]['path']
            limit = (pages[chain[index + 1]]['history_base']['end_byte_offset']
                     if index + 1 < len(chain) else path.stat().st_size)
            position = 0
            with path.open('rb') as stream:
                for number, line in enumerate(stream):
                    position += len(line)
                    if position > limit:
                        break
                    if not line.endswith(b'\n'):
                        raise ns.SyncError('Incomplete mock page')
                    row = json.loads(line)
                    if not isinstance(row, dict) or type(row.get('ordinal')) is not int or row['ordinal'] <= last_ordinal:
                        raise ns.SyncError('Mock ordinals are ambiguous')
                    last_ordinal = row['ordinal']
                    if number == 0:
                        if page == chain[-1]:
                            leaf_meta = row
                        continue
                    if nested_ordinal(row.get('payload')):
                        raise ns.SyncError('Nested ordinal is unsupported')
                    records.append(row)
            if position < limit:
                raise ns.SyncError('Incomplete mock page prefix')
        if not isinstance(leaf_meta, dict) or not isinstance(leaf_meta.get('payload'), dict):
            raise ns.SyncError('Invalid mock session metadata')
        leaf_meta['payload'].pop('history_base', None)
        output = [leaf_meta, *records]
        for ordinal, row in enumerate(output):
            row['ordinal'] = ordinal
        root_path = pages[chain[0]]['path'].relative_to(prepared)
        with tempfile.TemporaryDirectory(prefix='cosyncx-flat-output-', dir=destination.parent) as staged:
            staged = Path(staged)
            path = staged / root_path
            path.parent.mkdir(mode=0o700, parents=True)
            with path.open('x', encoding='utf-8') as stream:
                for row in output:
                    stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
            destination.mkdir(mode=0o700)
            for child in staged.iterdir():
                os.replace(child, destination / child.name)
        return len(output)


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
    if journal.exists() or journal.is_symlink():
        raise ns.SyncError('Mock update requires recovery')
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
            latest = backup.replace_latest(profile, backup_root, backup_names)
            expected = {name: ns._digest(path) for name, path in old.items()}
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
                fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    json.dump({'version': 1, 'thread': thread_id, 'operations': operations}, stream)
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
    data = json.loads(journal.read_text(encoding='utf-8'))
    manifest_path = latest / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 1024 * 1024:
        raise ns.SyncError('Mock backup manifest unavailable')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if (not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('thread'), str)
            or not isinstance(data.get('operations'), list) or not data['operations']
            or not isinstance(manifest, dict) or not isinstance(manifest.get('files'), dict)):
        raise ns.SyncError('Invalid mock recovery data')
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
    with mock._mock_thread_lock(profile, thread_id):
        with sqlite3.connect(database, timeout=1) as db:
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
                 executable: Path, *, title: str | None = None, allow_title_change: bool = False) -> None:
    """Verify a complete mock thread through an isolated real Codex server."""
    profile = mock._validate_profile(profile, key)
    shared = mock._mock_path(shared)
    executable = executable.resolve(strict=True)
    if not shared.is_dir() or not executable.is_file():
        raise ns.SyncError('Invalid mock index configuration')
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
    page_ids = {lineage._page_id(item['path'], thread_id): item for item in pages}
    parents = {item['history_base']['thread_id'] for item in pages if item.get('history_base')}
    leaves = set(page_ids) - parents
    if len(leaves) != 1:
        raise ns.SyncError('Branched mock lineage is unsupported')
    leaf = page_ids[leaves.pop()]
    expected_messages = _expected_visible_messages(leaf['path'])
    if not any(key.startswith('user:') for key in expected_messages):
        raise ns.SyncError('Mock conversation has no verifiable user messages')
    with mock._mock_thread_lock(profile, thread_id):
        pass
    env = {name: value for name, value in os.environ.items()
           if name not in ('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_ADMIN_KEY', 'CHATGPT_ACCESS_TOKEN')}
    env['CODEX_HOME'] = str(profile)
    process = subprocess.Popen([str(executable), 'app-server', '--listen', 'stdio://'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    output = queue.Queue()
    def pump():
        try:
            for line in process.stdout:
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
    try:
        call(1, 'initialize', {'clientInfo': {'name': 'cosyncx-mock', 'version': '0.1'},
                                'capabilities': {'experimentalApi': True}})
        process.stdin.write(b'{"method":"initialized","params":{}}\n')
        process.stdin.flush()
        call(2, 'thread/resume', {'threadId': thread_id, 'path': str(leaf['path']), 'cwd': leaf['cwd']})
        number = 3
        if title is not None:
            call(number, 'thread/name/set', {'threadId': thread_id, 'name': title})
            number += 1
        cursor, seen, actual_messages = None, set(), Counter()
        while True:
            params = {'threadId': thread_id, 'itemsView': 'full', 'limit': 100}
            if cursor is not None:
                params['cursor'] = cursor
            result = call(number, 'thread/turns/list', params)
            number += 1
            if not isinstance(result, dict) or not isinstance(result.get('data'), list) or 'nextCursor' not in result:
                raise ns.SyncError('Invalid mock turns result')
            for turn in result['data']:
                if not isinstance(turn, dict) or not isinstance(turn.get('items'), list):
                    raise ns.SyncError('Invalid mock turn items')
                for item in turn['items']:
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
                        else:
                            raise ns.SyncError('Unsupported indexed mock user content')
                    actual_messages[_text_tag('user', ''.join(texts))] += 1
            cursor = result['nextCursor']
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in seen or len(seen) >= 1000:
                raise ns.SyncError('Invalid mock turns cursor')
            seen.add(cursor)
        if expected_messages - actual_messages:
            raise ns.SyncError('Mock messages are missing from the index')
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
                    return
            cursor = result['nextCursor']
            if cursor is None:
                raise ns.SyncError('Mock thread not found in index')
            if not isinstance(cursor, str) or not cursor or cursor in seen or len(seen) >= 1000:
                raise ns.SyncError('Invalid mock index cursor')
            seen.add(cursor)
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
            raise ns.SyncError('Mock Codex server did not stop')
        process.stdout.close()


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
    if journal.exists() or journal.is_symlink():
        raise ns.SyncError('Mock title update requires recovery')
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
