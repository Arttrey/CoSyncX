#!/usr/bin/env python3
"""Explicit Linux-only experiment for disposable synthetic Codex profiles."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time
import unicodedata

import nexus_sync as ns

MARKER = '.nexus-sync-mock-profile.json'
SYNC_MARKER = '.nexus-mock-sync'
WATCH_STATUS = '.nexus-mock-watch-status.json'


class MockChatConflict(ns.SyncError):
    """A specific mock chat has divergent histories."""


def _mock_path(path: Path) -> Path:
    parent = path.parent.resolve(strict=True)
    result = parent / path.name
    temporary = Path(tempfile.gettempdir()).resolve(strict=True)
    if (temporary not in result.parents or result.is_symlink()
            or '.codex' in {part.casefold() for part in result.parts}):
        raise ns.SyncError('Разрешён только отдельный временный моковый профиль')
    return result


def init_mock_profile(path: Path, key: bytes, shared: Path) -> None:
    if len(key) != 32:
        raise ns.SyncError('Неверная длина ключа')
    profile = _mock_path(path)
    exchange = _mock_path(shared)
    if not exchange.is_dir() or profile == exchange or exchange in profile.parents or profile in exchange.parents:
        raise ns.SyncError('Моковый профиль должен быть вне общей папки')
    profile.mkdir(mode=0o700)
    marker = {'format': 'nexus-mock-profile-v1', 'path': str(profile)}
    marker['authentication'] = ns._manifest_tag(marker, key)
    with (profile / MARKER).open('x', encoding='utf-8') as stream:
        json.dump(marker, stream, ensure_ascii=False, sort_keys=True)
        stream.write('\n')


def _validate_profile(path: Path, key: bytes) -> Path:
    if len(key) != 32:
        raise ns.SyncError('Неверная длина ключа')
    profile = _mock_path(path)
    marker_path = profile / MARKER
    if not profile.is_dir() or marker_path.is_symlink() or not marker_path.is_file() or marker_path.stat().st_size > 65536:
        raise ns.SyncError('Не найдена отметка мокового профиля')
    marker = json.loads(marker_path.read_text(encoding='utf-8'))
    tag = marker.get('authentication') if isinstance(marker, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
            or marker.get('format') != 'nexus-mock-profile-v1' or marker.get('path') != str(profile)):
        raise ns.SyncError('Отметка мокового профиля не подтверждена')
    return profile


@contextmanager
def _mock_thread_lock(profile: Path, thread_id: str):
    if sys.platform != 'linux':
        raise ns.SyncError('Опыт с блокировкой доступен только на Linux')
    import fcntl
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', thread_id):
        raise ns.SyncError('Некорректный ID чата')
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK
    opened = []
    root_fd = directory_fd = None
    try:
        root_fd = os.open(profile, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.mkdir('thread-writer-locks', 0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        directory_fd = os.open('thread-writer-locks', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        for name in ('.coordination.lock',):
            fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
            opened.append(fd)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ns.SyncError('Некорректный файл блокировки')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ns.SyncError('Моковый чат занят; применение отложено') from exc
        names = set(os.listdir(directory_fd))
        other_locks = {name for name in names if re.fullmatch(r'[0-9a-fA-F-]{36}\.lock', name)}
        if any(name.endswith('.lock') and name != '.coordination.lock' and name not in other_locks for name in names):
            raise ns.SyncError('Неизвестная блокировка мокового профиля')
        for name in sorted(other_locks | {thread_id + '.lock'}):
            fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
            opened.append(fd)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ns.SyncError('Некорректный файл блокировки')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ns.SyncError('Моковый профиль занят; применение отложено') from exc
        yield
    finally:
        for fd in reversed(opened):
            os.close(fd)
        if directory_fd is not None:
            os.close(directory_fd)
        if root_fd is not None:
            os.close(root_fd)


def _check_mock_ordinals(path: Path) -> None:
    previous = -1
    with path.open('rb') as stream:
        for line in stream:
            if not line.endswith(b'\n'):
                raise ns.SyncError('Незавершённая запись пагинированного мокового чата')
            record = json.loads(line)
            ordinal = record.get('ordinal') if isinstance(record, dict) else None
            if type(ordinal) is not int or ordinal < 0 or ordinal <= previous:
                raise ns.SyncError('Неподтверждённый порядок записей пагинированного мокового чата')
            previous = ordinal


def _add_mock_chat(profile: Path, relative: Path, incoming: Path, thread_id: str,
                   snapshot: Path, key: bytes, manifest_tag: str) -> None:
    """Install a new mock file without replacing any existing path."""
    parts = relative.parts
    if len(parts) < 2 or parts[0] != 'sessions' or any(part in ('', '.', '..') for part in parts):
        raise ns.SyncError('Некорректный путь мокового чата')
    opened = []
    temp_name = '.nexus-mock-new-' + os.urandom(12).hex()
    temp_created = False
    parent_fd = None
    try:
        fd = os.open(profile, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        opened.append(fd)
        for name in parts[:-1]:
            folded = unicodedata.normalize('NFD', name.casefold())
            matches = [existing for existing in os.listdir(fd)
                       if unicodedata.normalize('NFD', existing.casefold()) == folded]
            if any(existing != name for existing in matches):
                raise ns.SyncError('Неоднозначный путь мокового чата')
            if not matches:
                os.mkdir(name, 0o700, dir_fd=fd)
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            opened.append(fd)
        parent_fd = fd
        folded = unicodedata.normalize('NFD', parts[-1].casefold())
        if any(unicodedata.normalize('NFD', name.casefold()) == folded for name in os.listdir(parent_fd)):
            raise ns.SyncError('Путь мокового чата уже занят')
        before = incoming.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ns.SyncError('Моковая история недоступна')
        digest = hashlib.sha256()
        temp_fd = os.open(temp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent_fd)
        temp_created = True
        with os.fdopen(temp_fd, 'wb') as output:
            with os.fdopen(os.open(incoming, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
                actual = os.fstat(stream.fileno())
                if (actual.st_dev, actual.st_ino) != (before.st_dev, before.st_ino):
                    raise ns.SyncError('Моковая история изменилась')
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
                    output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        after = incoming.lstat()
        if ((before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) !=
                (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)
                or digest.hexdigest() != ns._digest(incoming)
                or ns.verify(snapshot, key).get('authentication') != manifest_tag
                or thread_id in ns._chat_catalog(profile, include_unsupported=True)):
            raise ns.SyncError('Моковая история или снимок изменились')
        os.link(temp_name, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
    finally:
        if parent_fd is not None and temp_created:
            try:
                os.unlink(temp_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
        for fd in reversed(opened):
            os.close(fd)


def _mock_legacy_settings_rebase(local: Path, incoming: Path) -> list[bytes] | None:
    """Keep mock-only local settings while appending an exact remote continuation."""
    if local.stat().st_size > ns.MAX_CHAT_BYTES or incoming.stat().st_size > ns.MAX_CHAT_BYTES:
        raise ns.SyncError('Моковая история слишком велика для экспериментального объединения')
    local_lines = local.read_bytes().splitlines(keepends=True)
    incoming_lines = incoming.read_bytes().splitlines(keepends=True)
    if any(not line.endswith(b'\n') for line in local_lines + incoming_lines):
        raise ns.SyncError('Незавершённая запись мокового чата')
    left, right = [json.loads(line) for line in local_lines], [json.loads(line) for line in incoming_lines]
    if any(not isinstance(row, dict) or 'ordinal' in row for row in left + right):
        raise ns.SyncError('Неподдерживаемая моковая история')
    common = 0
    while common < min(len(left), len(right)) and left[common] == right[common]:
        common += 1
    def is_setting(row):
        return (row.get('type') == 'event_msg' and isinstance(row.get('payload'), dict)
                and row['payload'].get('type') == 'thread_settings_applied')
    if not common or common >= len(right):
        raise ns.SyncError('Нет подтверждённого расхождения моковых настроек')
    matches = []
    for skipped in range(1, len(left) - common + 1):
        if not is_setting(left[common + skipped - 1]):
            break
        remainder = left[common + skipped:]
        if right[common:common + len(remainder)] == remainder:
            matches.append(common + len(remainder))
    if len(matches) != 1:
        raise ns.SyncError('Моковое продолжение неоднозначно')
    if matches[0] == len(right):
        return None
    return local_lines + incoming_lines[matches[0]:]


def _mock_settings_rebase(local: Path, incoming: Path) -> list[bytes]:
    """Experimental, narrowly checked merge of a mock settings-only suffix."""
    if local.stat().st_size > ns.MAX_CHAT_BYTES or incoming.stat().st_size > ns.MAX_CHAT_BYTES:
        raise ns.SyncError('Моковая история слишком велика для экспериментального объединения')
    local_lines, incoming_lines = local.read_bytes().splitlines(keepends=True), incoming.read_bytes().splitlines(keepends=True)
    left, right = [json.loads(line) for line in local_lines], [json.loads(line) for line in incoming_lines]
    common = 0
    while common < min(len(left), len(right)) and left[common] == right[common]:
        common += 1
    settings, extra = left[common:], right[common:]
    if not common or not settings or not extra:
        raise ns.SyncError('Нет подтверждённой общей части моковых историй')
    if any(not isinstance(row, dict) or row.get('type') != 'event_msg'
           or not isinstance(row.get('payload'), dict) or row['payload'].get('type') != 'thread_settings_applied'
           for row in settings):
        raise ns.SyncError('Локальное расхождение содержит неподдерживаемые записи')
    def nested_ordinal(value):
        if isinstance(value, dict):
            return 'ordinal' in value or any(nested_ordinal(child) for child in value.values())
        if isinstance(value, list):
            return any(nested_ordinal(child) for child in value)
        return False
    checked_extra = extra
    if len(extra) == 5:
        # A migrated rollout can place the new user message before the closing
        # event of the preceding turn. Only accept a single, known open turn.
        closing = extra[1]
        payload = closing.get('payload') if isinstance(closing, dict) else None
        old_events = [row.get('payload') for row in left[:common]
                      if isinstance(row, dict) and row.get('type') == 'event_msg'
                      and isinstance(row.get('payload'), dict)]
        turn_id = payload.get('turn_id') if isinstance(payload, dict) else None
        started = [item for item in old_events if item.get('type') == 'task_started' and item.get('turn_id') == turn_id]
        finished = [item for item in old_events if item.get('type') == 'task_complete' and item.get('turn_id') == turn_id]
        if (not isinstance(closing, dict) or set(closing) - {'timestamp', 'ordinal', 'type', 'payload'}
                or closing.get('type') != 'event_msg' or not isinstance(payload, dict)
                or payload.get('type') != 'task_complete'
                or set(payload) - {'type', 'turn_id', 'last_agent_message', 'completed_at'}
                or payload.get('last_agent_message') is not None or nested_ordinal(payload)
                or not isinstance(turn_id, str) or not turn_id or len(started) != 1 or finished):
            raise ns.SyncError('Завершение предыдущего мокового хода неоднозначно')
        checked_extra = [extra[0], *extra[2:]]
    if len(checked_extra) % 4:
        raise ns.SyncError('Входящее моковое продолжение не поддерживается')
    seen_turns = {row['payload']['turn_id'] for row in left if isinstance(row, dict)
                  and isinstance(row.get('payload'), dict) and isinstance(row['payload'].get('turn_id'), str)}
    seen_items = {row['payload']['item'].get('id') for row in left if isinstance(row, dict)
                  and isinstance(row.get('payload'), dict) and isinstance(row['payload'].get('item'), dict)
                  and isinstance(row['payload']['item'].get('id'), str)}
    session = left[0].get('payload', {}) if isinstance(left[0], dict) else {}
    session_id = (session.get('session_id') or session.get('id')) if isinstance(session, dict) else None
    for index in range(0, len(checked_extra), 4):
        group = checked_extra[index:index + 4]
        if any(not isinstance(row, dict) or set(row) - {'timestamp', 'ordinal', 'type', 'payload'}
               or not isinstance(row.get('type'), str)
               or not isinstance(row.get('payload'), dict) or nested_ordinal(row['payload']) for row in group):
            raise ns.SyncError('Входящее моковое продолжение не поддерживается')
        message, started, completed, finished = [row['payload'] for row in group]
        item = completed.get('item')
        text_items = message.get('content')
        item_content = item.get('content') if isinstance(item, dict) else None
        turn = started.get('turn_id')
        if (group[0]['type'] != 'response_item' or message.get('type') != 'message' or message.get('role') != 'user'
                or set(message) != {'type', 'role', 'content'} or not isinstance(text_items, list)
                or not text_items or any(not isinstance(part, dict) or set(part) != {'type', 'text'}
                                         or part.get('type') != 'input_text' or not isinstance(part.get('text'), str)
                                         for part in text_items)
                or [row['type'] for row in group[1:]] != ['event_msg'] * 3
                or started.get('type') != 'task_started' or set(started) - {'type', 'turn_id', 'started_at', 'model_context_window', 'collaboration_mode_kind'}
                or completed.get('type') != 'item_completed' or set(completed) != {'type', 'thread_id', 'turn_id', 'item', 'completed_at_ms'}
                or not isinstance(item, dict) or set(item) != {'type', 'id', 'content'} or item.get('type') != 'UserMessage'
                or completed.get('thread_id') != session_id or not isinstance(item.get('id'), str)
                or not item['id'] or item['id'] in seen_items
                or not isinstance(item_content, list) or len(item_content) != len(text_items)
                or any(not isinstance(part, dict) or set(part) != {'type', 'text', 'text_elements'}
                       or part.get('type') != 'text' or part.get('text') != expected['text']
                       or part.get('text_elements') != [] for part, expected in zip(item_content, text_items))
                or finished.get('type') != 'task_complete' or set(finished) - {'type', 'turn_id', 'last_agent_message', 'completed_at'}
                or finished.get('last_agent_message') is not None or not isinstance(turn, str) or not turn
                or completed.get('turn_id') != turn or finished.get('turn_id') != turn or turn in seen_turns):
            raise ns.SyncError('Входящее моковое продолжение не поддерживается')
        seen_turns.add(turn)
        seen_items.add(item['id'])
    last = left[-1]['ordinal']
    result = list(local_lines)
    for offset, row in enumerate(extra, 1):
        updated = dict(row)
        updated['ordinal'] = last + offset
        result.append((json.dumps(updated, ensure_ascii=True, separators=(',', ':')) + '\n').encode('utf-8'))
    return result


def apply_mock_snapshot(profile: Path, snapshot: Path, shared: Path, key: bytes,
                        thread_id: str, source_root: str, target_root: Path, *,
                        allow_mock_settings_rebase: bool = False) -> dict:
    """Add or update one mock JSONL; index rebuilding is a separate operation."""
    profile = _validate_profile(profile, key)
    exchange = _mock_path(shared)
    target_nexus = _mock_path(target_root)
    if (not exchange.is_dir() or not target_nexus.is_dir() or profile == exchange or profile == target_nexus
            or exchange in profile.parents or profile in exchange.parents
            or target_nexus in profile.parents or profile in target_nexus.parents):
        raise ns.SyncError('Моковые папки должны быть раздельными')
    source = snapshot.resolve(strict=True)
    root = exchange / 'nexus-sync' / 'snapshots'
    if snapshot.is_symlink() or root.is_symlink() or root not in source.parents:
        raise ns.SyncError('Нужен проверяемый снимок из папки обмена')
    manifest = ns.verify(source, key)
    if manifest.get('omitted_files') != 0 or manifest.get('unknown_history_files') != 0:
        raise ns.SyncError('Неполный или неподдерживаемый снимок')
    with tempfile.TemporaryDirectory(prefix='nexus-mock-apply-') as tmp:
        quarantine = Path(tmp) / 'quarantine'
        mapped = Path(tmp) / 'mapped'
        ns.extract(source, quarantine, key)
        ns.prepare_chats(quarantine, mapped, source_root, str(target_nexus),
                         allow_paginated_preview=True)
        incoming = ns._chat_catalog(mapped, include_unsupported=True).get(thread_id, [])
        if len(incoming) != 1 or incoming[0]['folder'] != 'sessions' or incoming[0]['unsupported'] not in (None, 'PAGINATED'):
            raise ns.SyncError('В снимке нет однозначного активного тестового чата')
        with _mock_thread_lock(profile, thread_id):
            local = ns._chat_catalog(profile, include_unsupported=True).get(thread_id, [])
            if not local:
                if incoming[0]['unsupported'] == 'PAGINATED':
                    _check_mock_ordinals(incoming[0]['path'])
                _add_mock_chat(profile, incoming[0]['path'].relative_to(mapped), incoming[0]['path'],
                               thread_id, source, key, manifest['authentication'])
                return {'state': 'ADDED', 'index_updated': False, 'titles_updated': False,
                        'backup_created': False, 'mock_only': True}
            if (len(local) != 1 or local[0]['folder'] != 'sessions'
                    or local[0]['unsupported'] != incoming[0]['unsupported']):
                raise ns.SyncError('Моковый чат отсутствует или имеет несовместимый формат')
            path = local[0]['path']
            if local[0]['unsupported'] == 'PAGINATED':
                _check_mock_ordinals(path)
                _check_mock_ordinals(incoming[0]['path'])
            before = ns._digest(path)
            incoming_before = ns._digest(incoming[0]['path'])
            relation = ns._path_adjusted_relation(incoming[0]['path'], path,
                                                   str(target_nexus), str(target_nexus))
            if relation == 'EQUIVALENT_AFTER_PATH_MAPPING':
                return {'state': 'UNCHANGED', 'index_updated': False, 'titles_updated': False,
                        'backup_created': False, 'mock_only': True}
            if relation == 'LOCAL_EXTENDS_AFTER_PATH_MAPPING':
                return {'state': 'LOCAL_AHEAD', 'index_updated': False, 'titles_updated': False,
                        'backup_created': False, 'mock_only': True}
            rebased = None
            if relation == 'DIVERGED' and allow_mock_settings_rebase:
                rebased = (_mock_settings_rebase(path, incoming[0]['path'])
                           if local[0]['unsupported'] == 'PAGINATED'
                           else _mock_legacy_settings_rebase(path, incoming[0]['path']))
                if rebased is None:
                    return {'state': 'LOCAL_AHEAD', 'index_updated': False, 'titles_updated': False,
                            'backup_created': False, 'mock_only': True}
            elif relation == 'DIVERGED':
                raise MockChatConflict('Моковые истории расходятся')
            elif relation != 'INCOMING_EXTENDS_AFTER_PATH_MAPPING':
                raise ns.SyncError('Моковые истории расходятся или локальная история длиннее')
            backups = profile / '.nexus-mock-backups'
            backup_dir = backups / thread_id
            if backups.is_symlink() or backup_dir.is_symlink():
                raise ns.SyncError('Папка моковых резервных копий является ссылкой')
            backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            backup = backup_dir / (os.urandom(16).hex() + '.jsonl')
            temporary = path.parent / ('.nexus-mock-' + os.urandom(12).hex())
            try:
                ns._copy_stable_chat(path, backup)
                if ns._digest(backup) != before:
                    raise ns.SyncError('Моковая резервная копия не совпала')
                if rebased is None:
                    ns._copy_stable_chat(incoming[0]['path'], temporary)
                else:
                    with temporary.open('xb') as stream:
                        stream.writelines(rebased)
                    if local[0]['unsupported'] == 'PAGINATED':
                        _check_mock_ordinals(temporary)
                if (ns._digest(path) != before or ns._digest(incoming[0]['path']) != incoming_before
                        or ns.verify(source, key).get('authentication') != manifest.get('authentication')):
                    raise ns.SyncError('Моковая история или снимок изменились')
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
    return {'state': 'REBASED_MOCK' if rebased is not None else 'UPDATED', 'index_updated': False, 'titles_updated': False,
            'backup_created': True, 'mock_only': True}


def reindex_mock_profile(profile: Path, shared: Path, key: bytes, target_root: Path,
                         executable: Path, thread_id: str, *, title_override: str | None = None) -> dict:
    """Explicitly index a mock profile after its test server is stopped."""
    profile = _validate_profile(profile, key)
    exchange, target_nexus = _mock_path(shared), _mock_path(target_root)
    if (not exchange.is_dir() or not target_nexus.is_dir() or profile == exchange or profile == target_nexus
            or exchange in profile.parents or profile in exchange.parents
            or target_nexus in profile.parents or profile in target_nexus.parents):
        raise ns.SyncError('Моковые папки недоступны')
    catalogue = ns._chat_catalog(profile, include_unsupported=True)
    if (thread_id not in catalogue or any(len(items) != 1 or items[0]['folder'] != 'sessions'
            or items[0]['unsupported'] not in (None, 'PAGINATED') for items in catalogue.values())):
        raise ns.SyncError('Активный моковый чат отсутствует или неоднозначен')
    with _mock_thread_lock(profile, thread_id):
        pass  # A busy writer must be detected before starting the test server.
    marker = profile / 'index-preview.json'
    if marker.is_symlink():
        raise ns.SyncError('Отметка индекса является ссылкой')
    if not marker.exists():
        with marker.open('x', encoding='utf-8') as stream:
            json.dump({'format': 'nexus-index-preview-v1', 'applied': False, 'partial': True}, stream)
    with tempfile.TemporaryDirectory(prefix='nexus-mock-index-') as tmp:
        dummy = Path(tmp) / 'empty'
        dummy.mkdir()
        cfg = {'nexus': target_nexus, 'codex_home': dummy, 'shared': exchange, 'state_dir': dummy}
        options = {'title_overrides': {thread_id: title_override}} if title_override is not None else {}
        report = ns.index_preview(profile, profile, str(target_nexus), cfg, executable, reindex_mock=True, **options)
    return {'mock_only': True, 'index_updated': not report['partial'],
            'title_verified': title_override is not None and not report['partial']
                              and hashlib.sha256(thread_id.encode()).hexdigest() not in report['title_unmatched'],
            'contents_verified': report['contents_verified'], 'verified_in_cursor': False}


def _mock_chat_tag(profile: Path, thread_id: str, key: bytes, *, catalogue: dict | None = None) -> str:
    if catalogue is None:
        catalogue = ns._chat_catalog(profile, include_unsupported=True)
    matches = catalogue.get(thread_id, [])
    if len(matches) != 1 or matches[0]['folder'] != 'sessions':
        raise ns.SyncError('Моковый чат отсутствует или неоднозначен')
    path = matches[0]['path']
    before = path.stat()
    digest = hmac.new(key, b'nexus-mock-chat-state-v1\0', hashlib.sha256)
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (
            after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino):
        raise ns.SyncError('Моковый чат изменился во время проверки')
    return digest.hexdigest()


def _mock_title_tag(data: bytes | None, thread_id: str, key: bytes, *, names: dict | None = None) -> str:
    if names is None:
        names = {row['id']: row['thread_name'] for row in ns._parse_title_index(data)} if data is not None else {}
    title = names.get(thread_id)
    value = b'\0' if title is None else b'\1' + title.encode('utf-8')
    return hmac.new(key, b'nexus-mock-title-state-v2\0' + value, hashlib.sha256).hexdigest()


def _require_snapshot_ancestor(folder: Path, peer: str, key: bytes, newest: str,
                               previous: object, previous_tag: object) -> None:
    """Reject rollback or a rewritten previously accepted mock snapshot."""
    if (not isinstance(previous, str) or not re.fullmatch(r'[0-9a-f]{32}', previous)
            or not isinstance(previous_tag, str) or not re.fullmatch(r'[0-9a-f]{64}', previous_tag)):
        raise ns.SyncError('Некорректная отметка мокового снимка')
    current = newest
    seen = set()
    while isinstance(current, str) and re.fullmatch(r'[0-9a-f]{32}', current) and len(seen) < 10000:
        if current in seen:
            break
        seen.add(current)
        directory = folder / current
        path = directory / 'manifest.json'
        if directory.is_symlink() or path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
            raise ns.SyncError('Цепочка моковых снимков неполна')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        tag = manifest.get('authentication') if isinstance(manifest, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(manifest, key))
                or manifest.get('id') != current or manifest.get('machine') != peer):
            raise ns.SyncError('Цепочка моковых снимков не подтверждена')
        if current == previous:
            if not hmac.compare_digest(tag, previous_tag):
                raise ns.SyncError('Ранее принятый моковый снимок изменился')
            return
        current = manifest.get('previous_snapshot')
    raise ns.SyncError('Ранее принятый моковый снимок отсутствует в цепочке')


def sync_latest_mock(profile: Path, shared: Path, key: bytes, peer: str, thread_id: str,
                     source_root: str, target_root: Path, executable: Path, *,
                     allow_mock_settings_rebase: bool = False, expected_snapshot: str | None = None,
                     expected_manifest_tag: str | None = None) -> dict:
    """One explicit, offline mock-only receive and index cycle."""
    profile = _validate_profile(profile, key)
    exchange = _mock_path(shared)
    target_nexus = _mock_path(target_root)
    executable = executable.resolve(strict=True)
    if (not target_nexus.is_dir() or not executable.is_file() or profile == target_nexus
            or target_nexus in profile.parents or profile in target_nexus.parents
            or profile == exchange or exchange in profile.parents or profile in exchange.parents):
        raise ns.SyncError('Некорректные моковые папки или исполняемый файл')
    ns.map_path(source_root, '/__nexus_sync_root__', source_root)
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', thread_id):
        raise ns.SyncError('Некорректный ID мокового чата')
    ns._safe_machine(peer)
    root = exchange / 'nexus-sync' / 'snapshots'
    folder = root / peer
    if not exchange.is_dir() or (exchange / 'nexus-sync').is_symlink() or root.is_symlink() or folder.is_symlink():
        raise ns.SyncError('Некорректная папка снимков')
    latest = ns.latest_authenticated_snapshot(folder, peer, key)
    if latest is None:
        raise ns.SyncError('Подтверждённый снимок не найден')
    snapshot, manifest = latest
    if ((expected_snapshot is not None and snapshot.name != expected_snapshot)
            or (expected_manifest_tag is not None and manifest.get('authentication') != expected_manifest_tag)):
        raise ns.SyncError('Снимок изменился во время пакетной синхронизации')
    if manifest.get('omitted_files') != 0 or manifest.get('unknown_history_files') != 0:
        raise ns.SyncError('Неполный или неподдерживаемый снимок')
    incoming_data = ns._snapshot_title_data(snapshot, manifest, key)
    incoming_names = {row['id']: row['thread_name'] for row in ns._parse_title_index(incoming_data)} if incoming_data is not None else {}
    incoming_title = incoming_names.get(thread_id)
    local_title_data = ns._read_title_index(profile)
    local_names = {row['id']: row['thread_name'] for row in ns._parse_title_index(local_title_data)} if local_title_data is not None else {}
    local_title = local_names.get(thread_id)
    if incoming_title is not None and not incoming_title:
        return {'state': 'TITLE_UNSUPPORTED', 'mock_only': True, 'chat_updated': False,
                'index_updated': False, 'titles_updated': False, 'verified_in_cursor': False}
    if incoming_title is not None and local_title is not None and incoming_title != local_title:
        return {'state': 'TITLE_CONFLICT', 'mock_only': True, 'chat_updated': False,
                'index_updated': False, 'titles_updated': False, 'verified_in_cursor': False}
    marker_dir = profile / SYNC_MARKER
    if marker_dir.is_symlink() or (marker_dir.exists() and not marker_dir.is_dir()):
        raise ns.SyncError('Каталог отметок является ссылкой')
    marker_path = marker_dir / (peer + '-' + thread_id + '.json')
    if marker_path.is_symlink():
        raise ns.SyncError('Отметка синхронизации является ссылкой')
    if marker_path.exists():
        if not marker_path.is_file() or marker_path.stat().st_size > 65536:
            raise ns.SyncError('Некорректная отметка синхронизации')
        marker = json.loads(marker_path.read_text(encoding='utf-8'))
        tag = marker.get('authentication') if isinstance(marker, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                or marker.get('format') != 'nexus-mock-sync-v2'
                or marker.get('source') != peer or marker.get('thread') != thread_id):
            raise ns.SyncError('Отметка синхронизации не подтверждена')
        _require_snapshot_ancestor(folder, peer, key, snapshot.name, marker.get('snapshot'), marker.get('manifest_tag'))
        if (marker.get('source') == peer and marker.get('snapshot') == snapshot.name
                and marker.get('manifest_tag') == manifest.get('authentication') and marker.get('thread') == thread_id
                and marker.get('source_root') == source_root and marker.get('target_root') == str(target_nexus)
                and marker.get('executable') == str(executable)
                and marker.get('title_tag') == _mock_title_tag(local_title_data, thread_id, key)
                and marker.get('local_tag') == _mock_chat_tag(profile, thread_id, key)):
            return {'state': 'UNCHANGED', 'mock_only': True, 'index_current_verified': False, 'titles_updated': False,
                    'contents_verified': False, 'verified_in_cursor': False}
    applied = apply_mock_snapshot(profile, snapshot, exchange, key, thread_id, source_root, target_nexus,
                                  allow_mock_settings_rebase=allow_mock_settings_rebase)
    if applied['state'] == 'LOCAL_AHEAD':
        return {'state': 'LOCAL_AHEAD', 'mock_only': True, 'chat_updated': False,
                'backup_created': False, 'index_updated': False, 'titles_updated': False,
                'contents_verified': False, 'verified_in_cursor': False}
    if ns._read_title_index(profile) != local_title_data:
        return {'state': 'TITLE_PENDING', 'mock_only': True, 'chat_updated': applied['state'] != 'UNCHANGED',
                'backup_created': applied['backup_created'], 'index_updated': False,
                'titles_updated': False, 'verified_in_cursor': False}
    try:
        options = {'title_override': incoming_title} if incoming_title is not None else {}
        indexed = reindex_mock_profile(profile, exchange, key, target_nexus, executable, thread_id, **options)
    except (ns.SyncError, OSError, ValueError, TypeError):
        return {'state': 'INDEX_PENDING', 'mock_only': True, 'chat_updated': applied['state'] != 'UNCHANGED', 'titles_updated': False,
                'backup_created': applied['backup_created'], 'index_updated': False, 'verified_in_cursor': False}
    if not indexed['index_updated']:
        return {'state': 'INDEX_PENDING', 'mock_only': True, 'chat_updated': applied['state'] != 'UNCHANGED', 'titles_updated': False,
                'backup_created': applied['backup_created'], 'index_updated': False, 'verified_in_cursor': False}
    if incoming_title is not None and not indexed.get('title_verified', False):
        return {'state': 'TITLE_PENDING', 'mock_only': True, 'chat_updated': applied['state'] != 'UNCHANGED',
                'backup_created': applied['backup_created'], 'index_updated': True,
                'titles_updated': False, 'verified_in_cursor': False}
    if ns.verify(snapshot, key).get('authentication') != manifest.get('authentication'):
        raise ns.SyncError('Снимок изменился во время моковой синхронизации')
    latest_after = ns.latest_authenticated_snapshot(folder, peer, key)
    if latest_after is None or latest_after[0].name != snapshot.name or latest_after[1].get('authentication') != manifest.get('authentication'):
        return {'state': 'NEWER_SNAPSHOT_PENDING', 'mock_only': True, 'index_updated': True, 'titles_updated': False,
                'contents_verified': False, 'verified_in_cursor': False}
    marker = {'format': 'nexus-mock-sync-v2', 'source': peer, 'snapshot': snapshot.name,
              'manifest_tag': manifest['authentication'], 'thread': thread_id,
              'source_root': source_root, 'target_root': str(target_nexus), 'executable': str(executable),
              'title_tag': _mock_title_tag(ns._read_title_index(profile), thread_id, key),
              'local_tag': _mock_chat_tag(profile, thread_id, key)}
    marker['authentication'] = ns._manifest_tag(marker, key)
    if marker_dir.is_symlink():
        raise ns.SyncError('Каталог отметок является ссылкой')
    marker_dir.mkdir(mode=0o700, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-mock-sync-', dir=marker_dir, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(marker, stream, ensure_ascii=False, sort_keys=True)
            stream.write('\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, marker_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {'state': applied['state'], 'mock_only': True, 'backup_created': applied['backup_created'], 'titles_updated': incoming_title is not None,
            'index_updated': True, 'contents_verified': False, 'verified_in_cursor': False}


def sync_all_mock(profile: Path, shared: Path, key: bytes, peer: str, source_root: str,
                  target_root: Path, executable: Path, *, allow_mock_settings_rebase: bool = False) -> dict:
    """Explicitly receive all supported active chats from one mock snapshot, sequentially."""
    _validate_profile(profile, key)
    exchange = _mock_path(shared)
    ns._safe_machine(peer)
    root = exchange / 'nexus-sync' / 'snapshots'
    folder = root / peer
    if not exchange.is_dir() or (exchange / 'nexus-sync').is_symlink() or root.is_symlink() or folder.is_symlink():
        raise ns.SyncError('Некорректная папка снимков')
    latest = ns.latest_authenticated_snapshot(folder, peer, key)
    if latest is None:
        raise ns.SyncError('Подтверждённый снимок не найден')
    snapshot, manifest = latest
    if manifest.get('omitted_files') != 0 or manifest.get('unknown_history_files') != 0:
        raise ns.SyncError('Неполный или неподдерживаемый снимок')
    with tempfile.TemporaryDirectory(prefix='nexus-mock-batch-') as tmp:
        quarantine = Path(tmp) / 'quarantine'
        ns.extract(snapshot, quarantine, key)
        catalogue = ns._chat_catalog(quarantine, include_unsupported=True)
        if any(len(items) != 1 or items[0]['folder'] != 'sessions'
               or items[0]['unsupported'] not in (None, 'PAGINATED') for items in catalogue.values()):
            raise ns.SyncError('Снимок содержит неоднозначные или неподдерживаемые чаты')
        identifiers = sorted(catalogue)
        if ns.verify(snapshot, key).get('authentication') != manifest.get('authentication'):
            raise ns.SyncError('Снимок изменился во время подготовки пакета')
    counts: dict[str, int] = {}
    has_pending_chat = False
    indexes_updated = 0
    for identifier in identifiers:
        try:
            result = sync_latest_mock(profile, shared, key, peer, identifier, source_root, target_root,
                                      executable, expected_snapshot=snapshot.name,
                                      expected_manifest_tag=manifest['authentication'],
                                      allow_mock_settings_rebase=allow_mock_settings_rebase)
        except MockChatConflict:
            counts['CONFLICT'] = counts.get('CONFLICT', 0) + 1
            has_pending_chat = True
            continue
        except (ns.SyncError, OSError, ValueError, TypeError):
            return {'state': 'PENDING', 'mock_only': True, 'processed': sum(counts.values()),
                    'total': len(identifiers), 'results': counts, 'verified_in_cursor': False}
        state = result['state']
        indexes_updated += result.get('index_updated') is True
        counts[state] = counts.get(state, 0) + 1
        if state in ('LOCAL_AHEAD', 'TITLE_CONFLICT'):
            has_pending_chat = True
            continue
        if state not in ('ADDED', 'UPDATED', 'UNCHANGED', 'REBASED_MOCK'):
            return {'state': 'PENDING', 'mock_only': True, 'processed': sum(counts.values()),
                    'total': len(identifiers), 'results': counts, 'verified_in_cursor': False}
    return {'state': 'PENDING' if has_pending_chat else 'COMPLETE', 'mock_only': True, 'processed': len(identifiers),
            'total': len(identifiers), 'results': counts, 'index_updated_count': indexes_updated,
            'snapshot': {'id': snapshot.name, 'authentication': manifest['authentication']},
            'verified_in_cursor': False}


def sync_sources_mock(profile: Path, shared: Path, key: bytes, sources: list[tuple[str, str]],
                      target_root: Path, executable: Path, *, allow_mock_settings_rebase: bool = False) -> dict:
    """Receive from multiple explicitly configured test sources, without merging conflicts."""
    if not 1 <= len(sources) <= 16:
        raise ns.SyncError('Некорректное число моковых источников')
    names = []
    for peer, source_root in sources:
        names.append(ns._safe_machine(peer).casefold())
        ns.map_path(source_root, '/__nexus_sync_root__', source_root)
    if len(set(names)) != len(names):
        raise ns.SyncError('Повторяется моковый источник')
    def run_pass():
        result = []
        for peer, source_root in sources:
            try:
                result.append(sync_all_mock(profile, shared, key, peer, source_root, target_root, executable,
                                            allow_mock_settings_rebase=allow_mock_settings_rebase))
            except (ns.SyncError, OSError, ValueError, TypeError):
                result.append({'state': 'PENDING', 'results': {}})
        return result
    first = run_pass()
    if len(sources) == 1:
        return first[0]
    checked = run_pass()
    ready = sum(item['state'] == 'COMPLETE' for item in checked)
    changed = any(item.get('index_updated_count', 0) or
                  any(item.get('results', {}).get(state, 0) for state in ('ADDED', 'UPDATED', 'REBASED_MOCK'))
                  for item in checked)
    state = 'COMPLETE' if ready == len(sources) and not changed else 'PENDING'
    snapshots = {peer: item['snapshot'] for (peer, _), item in zip(sources, checked)
                 if item['state'] == 'COMPLETE' and isinstance(item.get('snapshot'), dict)}
    if state == 'COMPLETE' and len(snapshots) != len(sources):
        state = 'PENDING'
    return {'state': state,
            'mock_only': True, 'sources_ready': ready, 'sources_expected': len(sources),
            'snapshots': snapshots,
            'verified_in_cursor': False, 'remote_freshness_confirmed': False}


def write_watch_status(profile: Path, shared: Path, key: bytes, source: str, result: dict, interval: float) -> None:
    profile = _validate_profile(profile, key)
    shared = _mock_path(shared)
    ns._safe_machine(source)
    if (not shared.is_dir() or not 0.5 <= interval <= 60
            or result.get('state') not in ('COMPLETE', 'PENDING', 'STOPPED')):
        raise ns.SyncError('Некорректное состояние мокового наблюдателя')
    marker = {'format': 'nexus-mock-watch-status-v2', 'profile': str(profile), 'shared': str(shared),
              'source': source, 'state': result['state'], 'time': time.time(), 'interval': interval}
    if result['state'] == 'COMPLETE':
        snapshots = result.get('snapshots') or {source: result.get('snapshot')}
        if (not isinstance(snapshots, dict) or source not in snapshots or not 1 <= len(snapshots) <= 16
                or any(not isinstance(peer, str) or ns._safe_machine(peer) != peer
                       or not isinstance(item, dict) or not re.fullmatch(r'[0-9a-f]{32}', str(item.get('id', '')))
                       or not re.fullmatch(r'[0-9a-f]{64}', str(item.get('authentication', '')))
                       for peer, item in snapshots.items())):
            raise ns.SyncError('Не подтверждены моковые снимки наблюдателя')
        marker['snapshots'] = snapshots
    marker['authentication'] = ns._manifest_tag(marker, key)
    destination = profile / WATCH_STATUS
    if destination.is_symlink():
        raise ns.SyncError('Отметка наблюдателя является ссылкой')
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=profile, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(marker, stream, sort_keys=True)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def watch_status(profile: Path, shared: Path, key: bytes) -> dict:
    result = {'state': 'INVALID', 'mock_only': True, 'verified_in_cursor': False,
              'remote_freshness_confirmed': False}
    try:
        profile, shared = _validate_profile(profile, key), _mock_path(shared)
        path = profile / WATCH_STATUS
        if not shared.is_dir() or path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
            return result
        marker = json.loads(path.read_text(encoding='utf-8'))
        tag = marker.get('authentication') if isinstance(marker, dict) else None
        timestamp, interval = marker.get('time'), marker.get('interval')
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                or marker.get('format') != 'nexus-mock-watch-status-v2' or marker.get('profile') != str(profile)
                or marker.get('shared') != str(shared) or marker.get('state') not in ('COMPLETE', 'PENDING', 'STOPPED')
                or type(timestamp) not in (int, float) or not math.isfinite(timestamp)
                or type(interval) not in (int, float) or not 0.5 <= interval <= 60):
            return result
        ns._safe_machine(marker.get('source'))
        age = time.time() - timestamp
        if age < -5 or age > max(30, interval * 3 + 10):
            result['state'] = 'STALE'
        else:
            result['state'] = marker['state']
        if result['state'] == 'COMPLETE':
            snapshots = marker.get('snapshots')
            if not isinstance(snapshots, dict) or marker['source'] not in snapshots or not 1 <= len(snapshots) <= 16:
                result['state'] = 'INVALID'
                return result
            root = shared / 'nexus-sync' / 'snapshots'
            if (shared / 'nexus-sync').is_symlink() or root.is_symlink():
                result['state'] = 'INVALID'
                return result
            local_catalogue = ns._chat_catalog(profile, include_unsupported=True)
            title_data = ns._read_title_index(profile)
            title_names = ({row['id']: row['thread_name'] for row in ns._parse_title_index(title_data)}
                           if title_data is not None else {})
            for peer, item in snapshots.items():
                ns._safe_machine(peer)
                if (not isinstance(item, dict) or not re.fullmatch(r'[0-9a-f]{32}', str(item.get('id', '')))
                        or not re.fullmatch(r'[0-9a-f]{64}', str(item.get('authentication', '')))
                        or (root / peer).is_symlink()):
                    result['state'] = 'INVALID'
                    return result
                latest = ns.latest_authenticated_snapshot(root / peer, peer, key)
                if (latest is None or latest[0].name != item['id']
                        or not hmac.compare_digest(latest[1]['authentication'], item['authentication'])):
                    result['state'] = 'PENDING'
                    return result
                with tempfile.TemporaryDirectory(prefix='nexus-mock-status-') as tmp:
                    extracted = Path(tmp) / 'snapshot'
                    ns.extract(latest[0], extracted, key)
                    catalogue = ns._chat_catalog(extracted, include_unsupported=True)
                    receipt_dir = profile / SYNC_MARKER
                    if receipt_dir.is_symlink():
                        result['state'] = 'INVALID'
                        return result
                    for identity, entries in catalogue.items():
                        if (not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', identity)
                                or len(entries) != 1 or entries[0]['folder'] != 'sessions'
                                or entries[0]['unsupported'] not in (None, 'PAGINATED')):
                            result['state'] = 'INVALID'
                            return result
                        receipt_path = receipt_dir / (peer + '-' + identity + '.json')
                        if receipt_path.is_symlink():
                            result['state'] = 'INVALID'
                            return result
                        if not receipt_path.is_file() or receipt_path.stat().st_size > 65536:
                            result['state'] = 'PENDING'
                            return result
                        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
                        receipt_tag = receipt.get('authentication') if isinstance(receipt, dict) else None
                        if (not isinstance(receipt_tag, str)
                                or not hmac.compare_digest(receipt_tag, ns._manifest_tag(receipt, key))
                                or receipt.get('format') != 'nexus-mock-sync-v2'
                                or receipt.get('source') != peer or receipt.get('thread') != identity):
                            result['state'] = 'INVALID'
                            return result
                        if (receipt.get('snapshot') != item['id'] or receipt.get('manifest_tag') != item['authentication']
                                or receipt.get('title_tag') != _mock_title_tag(title_data, identity, key, names=title_names)
                                or receipt.get('local_tag') != _mock_chat_tag(profile, identity, key, catalogue=local_catalogue)):
                            result['state'] = 'PENDING'
                            return result
                    if ns._read_title_index(profile) != title_data:
                        result['state'] = 'PENDING'
                        return result
        return result
    except (ns.SyncError, OSError, ValueError, TypeError, KeyError, AttributeError):
        result['state'] = 'INVALID'
        return result


def watch_cycle(profile: Path, shared: Path, key: bytes, sources: list[tuple[str, str]],
                target_root: Path, executable: Path, previous: dict | None, *,
                allow_mock_settings_rebase: bool = False) -> dict:
    """Reuse a fully checked unchanged mock cycle only within the same configuration."""
    context = {'sources': sources, 'target': str(target_root.resolve(strict=True)),
               'executable': str(executable.resolve(strict=True)), 'rebase': allow_mock_settings_rebase}
    context_tag = hmac.new(key, b'nexus-mock-watch-context-v1\0' +
                           json.dumps(context, sort_keys=True, separators=(',', ':')).encode(), hashlib.sha256).hexdigest()
    if isinstance(previous, dict) and previous.get('state') == 'COMPLETE' and previous.get('_watch_context') == context_tag:
        try:
            expected = previous.get('snapshots') or {sources[0][0]: previous.get('snapshot')}
            marker_path = _validate_profile(profile, key) / WATCH_STATUS
            if not marker_path.is_symlink() and marker_path.is_file() and marker_path.stat().st_size <= 65536:
                marker = json.loads(marker_path.read_text(encoding='utf-8'))
                tag = marker.get('authentication') if isinstance(marker, dict) else None
                if (isinstance(tag, str) and hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                        and marker.get('snapshots') == expected
                        and watch_status(profile, shared, key)['state'] == 'COMPLETE'):
                    return previous
        except (ns.SyncError, OSError, ValueError, TypeError, KeyError, AttributeError):
            pass
    result = sync_sources_mock(profile, shared, key, sources, target_root, executable,
                               allow_mock_settings_rebase=allow_mock_settings_rebase)
    return {**result, '_watch_context': context_tag}


@contextmanager
def _mock_watcher_lock(profile: Path, key: bytes):
    if sys.platform != 'linux':
        raise ns.SyncError('Моковый наблюдатель доступен только на Linux')
    import fcntl
    profile = _validate_profile(profile, key)
    fd = os.open(profile / '.nexus-mock-watch.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ns.SyncError('Некорректная блокировка наблюдателя')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ns.SyncError('Другой моковый наблюдатель уже работает') from exc
        yield
    finally:
        os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(description='Работа только с отдельным временным моковым профилем')
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init')
    init.add_argument('--profile', type=Path, required=True)
    init.add_argument('--shared', type=Path, required=True)
    init.add_argument('--key-file', type=Path, required=True)
    apply = sub.add_parser('apply')
    apply.add_argument('--profile', type=Path, required=True)
    apply.add_argument('--shared', type=Path, required=True)
    apply.add_argument('--key-file', type=Path, required=True)
    apply.add_argument('--snapshot', type=Path, required=True)
    apply.add_argument('--thread-id', required=True)
    apply.add_argument('--source-root', required=True)
    apply.add_argument('--target-root', type=Path, required=True)
    apply.add_argument('--allow-mock-settings-rebase', action='store_true',
                       help='экспериментально сохранить только моковые локальные записи настроек')
    reindex = sub.add_parser('reindex')
    reindex.add_argument('--profile', type=Path, required=True)
    reindex.add_argument('--shared', type=Path, required=True)
    reindex.add_argument('--key-file', type=Path, required=True)
    reindex.add_argument('--target-root', type=Path, required=True)
    reindex.add_argument('--codex-executable', type=Path, required=True)
    reindex.add_argument('--thread-id', required=True)
    sync = sub.add_parser('sync-latest')
    sync.add_argument('--profile', type=Path, required=True)
    sync.add_argument('--shared', type=Path, required=True)
    sync.add_argument('--key-file', type=Path, required=True)
    sync.add_argument('--source', required=True)
    sync.add_argument('--thread-id', required=True)
    sync.add_argument('--source-root', required=True)
    sync.add_argument('--target-root', type=Path, required=True)
    sync.add_argument('--codex-executable', type=Path, required=True)
    sync.add_argument('--allow-mock-settings-rebase', action='store_true')
    batch = sub.add_parser('sync-all')
    batch.add_argument('--profile', type=Path, required=True)
    batch.add_argument('--shared', type=Path, required=True)
    batch.add_argument('--key-file', type=Path, required=True)
    batch.add_argument('--source', required=True)
    batch.add_argument('--source-root', required=True)
    batch.add_argument('--target-root', type=Path, required=True)
    batch.add_argument('--codex-executable', type=Path, required=True)
    batch.add_argument('--allow-mock-settings-rebase', action='store_true')
    watch = sub.add_parser('watch-all', parents=[], help='наблюдение только для отдельного мокового профиля')
    watch.add_argument('--profile', type=Path, required=True)
    watch.add_argument('--shared', type=Path, required=True)
    watch.add_argument('--key-file', type=Path, required=True)
    watch.add_argument('--source', required=True)
    watch.add_argument('--source-root', required=True)
    watch.add_argument('--also-source', nargs=2, action='append', default=[], metavar=('ID', 'ROOT'),
                       help='дополнительный моковый источник и его исходный путь Nexus')
    watch.add_argument('--target-root', type=Path, required=True)
    watch.add_argument('--codex-executable', type=Path, required=True)
    watch.add_argument('--allow-mock-settings-rebase', action='store_true')
    watch.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    try:
        _mock_path(args.shared)
        _mock_path(args.key_file)
        key = ns.read_key(args.key_file, args.shared)
        if args.command == 'init':
            init_mock_profile(args.profile, key, args.shared)
            print('Создан отдельный пустой моковый профиль.')
        elif args.command == 'apply':
            result = apply_mock_snapshot(args.profile, args.snapshot, args.shared, key, args.thread_id,
                                         args.source_root, args.target_root,
                                         allow_mock_settings_rebase=args.allow_mock_settings_rebase)
            print(json.dumps(result, ensure_ascii=False))
            if result['state'] == 'LOCAL_AHEAD':
                return 1
        elif args.command == 'reindex':
            result = reindex_mock_profile(args.profile, args.shared, key, args.target_root,
                                          args.codex_executable, args.thread_id)
            print(json.dumps(result, ensure_ascii=False))
        elif args.command == 'sync-latest':
            result = sync_latest_mock(args.profile, args.shared, key, args.source, args.thread_id,
                                      args.source_root, args.target_root, args.codex_executable,
                                      allow_mock_settings_rebase=args.allow_mock_settings_rebase)
            print(json.dumps(result, ensure_ascii=False))
            if result['state'] in ('LOCAL_AHEAD', 'INDEX_PENDING', 'NEWER_SNAPSHOT_PENDING', 'TITLE_PENDING', 'TITLE_CONFLICT', 'TITLE_UNSUPPORTED'):
                return 1
        elif args.command == 'watch-all':
            if not 0.5 <= args.interval <= 60:
                raise ns.SyncError('Некорректный интервал')
            sources = [(args.source, args.source_root), *(tuple(item) for item in args.also_source)]
            if (len(sources) > 16 or len({ns._safe_machine(peer).casefold() for peer, _ in sources}) != len(sources)):
                raise ns.SyncError('Некорректный список моковых источников')
            for _, source_root in sources:
                ns.map_path(source_root, '/__nexus_sync_root__', source_root)
            with _mock_watcher_lock(args.profile, key):
                previous = None
                try:
                    while True:
                        try:
                            result = watch_cycle(args.profile, args.shared, key, sources,
                                                 args.target_root, args.codex_executable, previous,
                                                 allow_mock_settings_rebase=args.allow_mock_settings_rebase)
                        except (ns.SyncError, OSError, ValueError, TypeError):
                            result = {'state': 'PENDING', 'mock_only': True, 'verified_in_cursor': False}
                        write_watch_status(args.profile, args.shared, key, args.source, result, args.interval)
                        if result != previous:
                            public = {name: value for name, value in result.items()
                                      if name not in ('_watch_context', 'snapshot', 'snapshots')}
                            print(json.dumps(public, ensure_ascii=False), flush=True)
                            previous = result
                        time.sleep(args.interval)
                finally:
                    write_watch_status(args.profile, args.shared, key, args.source, {'state': 'STOPPED'}, args.interval)
        else:
            result = sync_all_mock(args.profile, args.shared, key, args.source, args.source_root,
                                   args.target_root, args.codex_executable,
                                   allow_mock_settings_rebase=args.allow_mock_settings_rebase)
            print(json.dumps(result, ensure_ascii=False))
            if result['state'] != 'COMPLETE':
                return 1
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError) as exc:
        print('Моковая операция отложена: ' + type(exc).__name__, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
