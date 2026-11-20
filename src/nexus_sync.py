#!/usr/bin/env python3
"""Local-first prototype; optional loopback-only mock status, no Codex writes."""
from __future__ import annotations

import argparse
import base64
import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import plistlib
import queue
import re
import shutil
import stat as statmod
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

VERSION = 1
TITLE_INDEX_AAD = b'nexus-session-index-v1'
MAX_CHAT_BYTES = 16 * 1024 * 1024


class SyncError(Exception):
    pass


class UnsupportedHistoryError(SyncError):
    pass


class UnstableChatError(SyncError):
    pass


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _parse_title_index(data: bytes) -> list[dict]:
    if len(data) > MAX_CHAT_BYTES:
        raise SyncError('Индекс названий слишком велик')
    result = []
    for line in data.splitlines(keepends=True):
        if not line.endswith(b'\n'):
            raise SyncError('Индекс названий содержит незавершённую запись')
        try:
            record = json.loads(line)
        except (ValueError, UnicodeError) as exc:
            raise SyncError('Индекс названий содержит некорректную запись') from exc
        if (not isinstance(record, dict) or not isinstance(record.get('id'), str) or not record['id']
                or not isinstance(record.get('thread_name'), str)):
            raise SyncError('Неизвестный формат индекса названий')
        result.append(record)
    return result


def _read_title_index(root: Path) -> bytes | None:
    path = root / 'session_index.jsonl'
    if path.is_symlink():
        raise SyncError('Индекс названий является ссылкой')
    if not path.exists():
        return None
    if not path.is_file():
        raise SyncError('Индекс названий недоступен')
    before = path.stat()
    if before.st_size > MAX_CHAT_BYTES:
        raise SyncError('Индекс названий слишком велик')
    data = path.read_bytes()
    after = path.stat()
    if ((before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) !=
            (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)
            or len(data) != before.st_size or _digest(path) != hashlib.sha256(data).hexdigest()):
        raise SyncError('Индекс названий изменился во время чтения')
    _parse_title_index(data)
    return data


def _merge_title_indices(sources: list[tuple[Path, set[str]]]) -> tuple[bytes | None, list[str]]:
    """Keep the first source's name on disagreement and report hashed IDs."""
    selected: dict[str, dict] = {}
    conflicts = set()
    found = False
    for root, allowed in sources:
        data = _read_title_index(root)
        if data is None:
            continue
        found = True
        latest = {record['id']: record for record in _parse_title_index(data) if record['id'] in allowed}
        for identifier, record in latest.items():
            if identifier not in selected:
                selected[identifier] = record
            elif selected[identifier]['thread_name'] != record['thread_name']:
                conflicts.add(identifier)
    payload = ''.join(json.dumps(selected[identifier], ensure_ascii=False, separators=(',', ':')) + '\n'
                      for identifier in sorted(selected)).encode('utf-8') if found else None
    return payload, [hashlib.sha256(identifier.encode('utf-8')).hexdigest() for identifier in sorted(conflicts)]


def _safe_machine(value: str) -> str:
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', value) or value in {'.', '..'}:
        raise SyncError('Недопустимый идентификатор устройства')
    return value


def map_path(source_root: str, target_root: str, value: str) -> str:
    """Translate a known project path without changing chat history."""
    def parse(text: str):
        if not isinstance(text, str) or any(ord(char) < 32 or ord(char) == 127 for char in text):
            raise SyncError('Путь содержит недопустимые символы')
        if re.match(r'^[A-Za-z]:[\\/]', text) or text.startswith('\\\\'):
            obj = PureWindowsPath(text)
        else:
            obj = PurePosixPath(text)
        if not obj.is_absolute() or '..' in obj.parts:
            raise SyncError('Нужен абсолютный путь без переходов ..')
        return obj
    source, target, original = parse(source_root), parse(target_root), parse(value)
    if type(source) is not type(original):
        raise SyncError('Путь не соответствует исходной системе')
    try:
        suffix = original.relative_to(source)
    except ValueError as exc:
        raise SyncError('Путь находится вне исходного Nexus') from exc
    if isinstance(target, PureWindowsPath):
        for part in suffix.parts:
            _portable_parts(part, nested=False)
    mapped = target.joinpath(*suffix.parts)
    try:
        mapped.relative_to(target)
    except ValueError as exc:
        raise SyncError('Путь выходит за целевой Nexus') from exc
    if not mapped.is_absolute():
        raise SyncError('Целевой путь не является абсолютным')
    return str(mapped)


def _walk(root: Path, *, reject_symlinks: bool = False, observe_directory=None):
    def failed(error):
        raise SyncError(f'Ошибка чтения каталога: {error}')
    for current, directories, files in os.walk(root, followlinks=False, onerror=failed):
        directory = Path(current)
        if observe_directory is not None:
            observe_directory(directory, directories, files)
        kept = []
        for name in directories:
            path = directory / name
            try:
                linked = path.is_symlink()
            except OSError as exc:
                raise SyncError('Не удалось проверить каталог чатов') from exc
            if linked and reject_symlinks:
                raise SyncError('Символьная ссылка в каталоге чатов: сравнение остановлено')
            if not linked:
                kept.append(name)
        directories[:] = sorted(kept)
        for name in sorted(files):
            path = directory / name
            try:
                mode = path.lstat().st_mode
            except OSError:
                # Keep an inaccessible directory entry visible to callers so
                # they can report it instead of silently claiming completeness.
                yield path
                continue
            if statmod.S_ISLNK(mode) and reject_symlinks:
                raise SyncError('Символьная ссылка в каталоге чатов: сравнение остановлено')
            if statmod.S_ISREG(mode):
                yield path


def _history_roots(codex: Path) -> list[Path]:
    roots = []
    for name in ('sessions', 'archived_sessions'):
        directory = codex / name
        if directory.is_symlink():
            raise SyncError('Каталог чатов является символьной ссылкой')
        if directory.is_dir():
            roots.append(directory)
        elif directory.exists():
            raise SyncError('Путь каталога чатов занят файлом')
    return roots


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2) + '\n', encoding='utf-8')


def _manifest_tag(manifest: dict, key: bytes) -> str:
    encoded = json.dumps({k: v for k, v in manifest.items() if k != 'authentication'}, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    derived = hmac.new(key, b'nexus-sync-manifest-v1', hashlib.sha256).digest()
    return hmac.new(derived, encoded, hashlib.sha256).hexdigest()


def _crypto():
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.exceptions import InvalidTag
    except ImportError as exc:
        raise SyncError('Нужен пакет cryptography: установите его в отдельное окружение') from exc
    return Cipher, algorithms, modes, InvalidTag


def _aead():
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.exceptions import InvalidTag
    except ImportError as exc:
        raise SyncError('Нужен пакет cryptography: установите его в отдельное окружение') from exc
    return AESGCM, InvalidTag


def read_key(path: Path, shared: Path) -> bytes:
    resolved = path.resolve(strict=True)
    share = shared.resolve(strict=True)
    if resolved == share or share in resolved.parents:
        raise SyncError('Ключ должен храниться вне папки Syncthing')
    key = resolved.read_bytes()
    if len(key) != 32:
        raise SyncError('Ключ должен содержать ровно 32 байта')
    return key


def create_key(path: Path) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(os.urandom(32))


def _portable_parts(name: str, *, nested: bool) -> tuple[str, ...]:
    parts = name.split('/')
    if not nested and len(parts) != 1:
        raise SyncError('Некорректный путь')
    for part in parts:
        device_name = part.split('.', 1)[0].rstrip(' ')
        if (not part or part in ('.', '..') or part[-1] in (' ', '.')
                or any(c in '<>:"\\|?*' or ord(c) < 32 for c in part)
                or re.fullmatch(r'(?i:CON|PRN|AUX|NUL|CONIN\$|CONOUT\$|COM[1-9¹²³]|LPT[1-9¹²³])', device_name)):
            raise SyncError('Непереносимый путь')
    return tuple(parts)


def _omitted_token(key: bytes, relative: str) -> str:
    derived = hmac.new(key, b'nexus-omitted-path-v1', hashlib.sha256).digest()
    return hmac.new(derived, relative.encode('utf-8'), hashlib.sha256).hexdigest()


def snapshot(codex: Path, shared: Path, machine: str, key: bytes, *, omitted_tokens: list | None = None) -> Path:
    """Publish individually stable, valid JSONL; absence never means deletion."""
    machine = _safe_machine(machine)
    codex = codex.resolve(strict=True)
    shared = shared.resolve(strict=True)
    if shared == codex or shared in codex.parents or codex in shared.parents:
        raise SyncError('Папка обмена и папка Codex должны быть раздельными')
    if len(key) != 32:
        raise SyncError('Неверная длина ключа')
    Cipher, algorithms, modes, _ = _crypto()
    roots = _history_roots(codex)
    if not roots:
        raise SyncError('Каталоги sessions и archived_sessions не найдены')
    parent = shared / 'nexus-sync' / 'snapshots' / machine
    parent.mkdir(parents=True, exist_ok=True)
    previous = latest_authenticated_snapshot(parent, machine, key, verify_payload=False)
    with tempfile.TemporaryDirectory(prefix='.staging-', dir=parent) as tmp:
        stage = Path(tmp)
        entries = []
        paginated_files = 0
        unknown_history_files = 0
        omitted_files = 0
        local_omissions = []
        for root in roots:
            for source in _walk(root, reject_symlinks=True):
                if not source.name.startswith('rollout-') or source.suffix != '.jsonl':
                    continue
                rel = source.relative_to(codex)
                dest = stage / 'files' / (rel.as_posix() + '.enc')
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    if source.is_symlink():
                        raise OSError('source became a symlink')
                    before = source.stat()
                except OSError:
                    omitted_files += 1
                    local_omissions.append(_omitted_token(key, rel.as_posix()))
                    continue
                nonce = os.urandom(12)
                encryptor = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
                encryptor.authenticate_additional_data(rel.as_posix().encode('utf-8'))
                original = hashlib.sha256()
                classified = False
                paginated = False
                try:
                    if before.st_size == 0:
                        raise UnstableChatError('Пустая история')
                    try:
                        handle = source.open('rb')
                    except OSError as exc:
                        raise UnstableChatError('История недоступна') from exc
                    with handle, dest.open('xb') as output:
                        output.write(nonce)
                        for number, line in enumerate(handle, 1):
                            if not line.endswith(b'\n'):
                                raise UnstableChatError('Незавершённая запись')
                            try:
                                record = json.loads(line)
                            except (ValueError, UnicodeError) as exc:
                                raise UnstableChatError('Некорректная запись') from exc
                            if not isinstance(record, dict):
                                raise UnstableChatError('Некорректная запись')
                            if number == 1 and record.get('type') == 'session_meta' and isinstance(record.get('payload'), dict):
                                mode = record['payload'].get('history_mode')
                                if mode == 'paginated':
                                    paginated = True
                                    classified = True
                                elif mode in (None, 'legacy'):
                                    classified = True
                            original.update(line)
                            output.write(encryptor.update(line))
                        output.write(encryptor.finalize())
                        output.write(encryptor.tag)
                    try:
                        if source.is_symlink():
                            raise OSError('source became a symlink')
                        after = source.stat()
                        source_digest = _digest(source)
                    except OSError as exc:
                        raise UnstableChatError('История недоступна') from exc
                    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                        raise UnstableChatError('История менялась во время копирования')
                    if source_digest != original.hexdigest():
                        raise UnstableChatError('История изменилась во время проверки')
                except UnstableChatError:
                    dest.unlink(missing_ok=True)
                    omitted_files += 1
                    local_omissions.append(_omitted_token(key, rel.as_posix()))
                    continue
                paginated_files += int(paginated)
                unknown_history_files += int(not classified)
                entries.append({'path': rel.as_posix(), 'size': dest.stat().st_size, 'sha256': _digest(dest)})
        if not entries:
            raise SyncError('Нет завершённых историй для снимка')
        try:
            title_data = _read_title_index(codex)
            if title_data is None:
                title_index = {'state': 'absent'}
            else:
                AESGCM, _ = _aead()
                nonce = os.urandom(12)
                encrypted_titles = nonce + AESGCM(key).encrypt(nonce, title_data, TITLE_INDEX_AAD)
                (stage / 'title-index.enc').write_bytes(encrypted_titles)
                title_index = {'state': 'captured', 'size': len(encrypted_titles),
                               'sha256': hashlib.sha256(encrypted_titles).hexdigest()}
        except (SyncError, OSError, ValueError):
            (stage / 'title-index.enc').unlink(missing_ok=True)
            title_index = {'state': 'omitted'}
            omitted_files += 1
            local_omissions.append(_omitted_token(key, 'session_index.jsonl'))
        identity = uuid.uuid4().hex
        manifest = {'format': 'nexus-chat-snapshot', 'version': VERSION, 'encryption': 'aes-256-gcm-v1', 'machine': machine, 'id': identity, 'created': datetime.now(timezone.utc).isoformat(), 'previous_snapshot': previous[0].name if previous else None, 'files': entries, 'paginated_files': paginated_files, 'unknown_history_files': unknown_history_files, 'omitted_files': omitted_files, 'title_index': title_index}
        manifest['authentication'] = _manifest_tag(manifest, key)
        _write_json(stage / 'manifest.json', manifest)
        destination = parent / identity
        os.replace(stage, destination)
    if omitted_tokens is not None:
        omitted_tokens.extend(local_omissions)
    return destination


def _snapshot_title_data(root: Path, manifest: dict, key: bytes | None) -> bytes | None:
    info = manifest.get('title_index')
    if info is None:
        return None  # Snapshot created before title-index support.
    if not isinstance(info, dict) or info.get('state') not in ('captured', 'absent', 'omitted'):
        raise SyncError('Некорректные сведения об индексе названий')
    if info['state'] != 'captured':
        return None
    path = root / 'title-index.enc'
    if (path.is_symlink() or not path.is_file() or type(info.get('size')) is not int
            or not 28 <= info['size'] <= MAX_CHAT_BYTES + 28
            or not isinstance(info.get('sha256'), str) or not re.fullmatch(r'[0-9a-f]{64}', info['sha256'])):
        raise SyncError('Зашифрованный индекс названий недоступен')
    encoded = path.read_bytes()
    if len(encoded) != info['size'] or hashlib.sha256(encoded).hexdigest() != info['sha256']:
        raise SyncError('Зашифрованный индекс названий повреждён')
    if key is None:
        return None
    AESGCM, InvalidTag = _aead()
    try:
        data = AESGCM(key).decrypt(encoded[:12], encoded[12:], TITLE_INDEX_AAD)
    except (InvalidTag, ValueError) as exc:
        raise SyncError('Индекс названий не прошёл проверку подлинности') from exc
    _parse_title_index(data)
    return data


def verify(snapshot_dir: Path, key: bytes | None = None) -> dict:
    root = snapshot_dir.resolve(strict=True)
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    if not isinstance(manifest, dict) or manifest.get('format') != 'nexus-chat-snapshot' or manifest.get('version') != VERSION or manifest.get('encryption') != 'aes-256-gcm-v1' or not isinstance(manifest.get('files'), list) or not manifest['files']:
        raise SyncError('Неизвестный или пустой манифест')
    for field in ('paginated_files', 'unknown_history_files'):
        if field in manifest and (type(manifest[field]) is not int or not 0 <= manifest[field] <= len(manifest['files'])):
            raise SyncError('Некорректное число историй указанного формата')
    if 'omitted_files' in manifest and (type(manifest['omitted_files']) is not int or manifest['omitted_files'] < 0):
        raise SyncError('Некорректное число пропущенных историй')
    if ('previous_snapshot' in manifest and manifest['previous_snapshot'] is not None
            and (not isinstance(manifest['previous_snapshot'], str) or not re.fullmatch(r'[0-9a-f]{32}', manifest['previous_snapshot']))):
        raise SyncError('Некорректная ссылка на предыдущий снимок')
    if manifest.get('paginated_files', 0) + manifest.get('unknown_history_files', 0) > len(manifest['files']):
        raise SyncError('Некорректное число историй указанного формата')
    if key is not None and len(key) != 32:
        raise SyncError('Неверная длина ключа')
    if key is not None:
        tag = manifest.get('authentication')
        if not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(manifest, key)):
            raise SyncError('Ключ не подошёл или манифест изменён')
    seen = set()
    for item in manifest['files']:
        if not isinstance(item, dict) or not isinstance(item.get('path'), str):
            raise SyncError('Некорректная запись манифеста')
        rel = item['path']
        parts = rel.split('/')
        if len(parts) < 2 or parts[0] not in ('sessions', 'archived_sessions') or any(p in ('', '.', '..') or '\\' in p or ':' in p for p in parts) or rel in seen:
            raise SyncError('Небезопасный путь или дубликат в манифесте')
        seen.add(rel)
        path = root / 'files'
        parts[-1] += '.enc'
        for part in parts:
            path = path / part
            if path.is_symlink():
                raise SyncError('Символьные ссылки в снимке запрещены')
        if not path.is_file() or path.stat().st_size != item.get('size') or _digest(path) != item.get('sha256'):
            raise SyncError(f'Файл отсутствует или повреждён: {rel}')
        if key is not None:
            Cipher, algorithms, modes, InvalidTag = _crypto()
            if path.stat().st_size < 28:
                raise SyncError(f'Неполный шифротекст: {rel}')
            try:
                with path.open('rb') as handle:
                    nonce = handle.read(12)
                    handle.seek(-16, os.SEEK_END)
                    tag = handle.read(16)
                    handle.seek(12)
                    decryptor = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
                    decryptor.authenticate_additional_data(rel.encode('utf-8'))
                    remaining = path.stat().st_size - 28
                    while remaining:
                        chunk = handle.read(min(1024 * 1024, remaining))
                        if not chunk:
                            raise SyncError(f'Неполный шифротекст: {rel}')
                        decryptor.update(chunk)
                        remaining -= len(chunk)
                    decryptor.finalize()
            except InvalidTag as exc:
                raise SyncError(f'Не подошёл ключ или нарушена аутентичность: {rel}') from exc
    _snapshot_title_data(root, manifest, key)
    return manifest


def latest_authenticated_snapshot(folder: Path, peer: str, key: bytes, *, verify_payload: bool = True) -> tuple[Path, dict] | None:
    """Follow signed links; the sender may defer verification of old payloads."""
    if folder.is_symlink():
        raise SyncError('Каталог снимков является ссылкой')
    if not folder.is_dir():
        return None
    candidates = []
    for entry in folder.iterdir():
        if not re.fullmatch(r'[0-9a-f]{32}', entry.name):
            continue
        if entry.is_symlink() or not entry.is_dir():
            raise SyncError('Некорректный каталог снимка')
        manifest_path = entry / 'manifest.json'
        if manifest_path.is_symlink():
            raise SyncError('Манифест снимка является ссылкой')
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        tag = manifest.get('authentication') if isinstance(manifest, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(manifest, key))
                or manifest.get('format') != 'nexus-chat-snapshot' or manifest.get('version') != VERSION
                or manifest.get('encryption') != 'aes-256-gcm-v1'
                or not isinstance(manifest.get('files'), list) or not manifest['files']
                or manifest.get('machine') != peer or manifest.get('id') != entry.name
                or not isinstance(manifest.get('created'), str)):
            raise SyncError('Манифест снимка не подтверждён')
        try:
            timestamp = datetime.fromisoformat(manifest['created'])
        except ValueError as exc:
            raise SyncError('Время снимка не распознано') from exc
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise SyncError('Время снимка не содержит часовой пояс')
        linked = 'previous_snapshot' in manifest
        predecessor = manifest.get('previous_snapshot')
        if linked and predecessor is not None and (not isinstance(predecessor, str) or not re.fullmatch(r'[0-9a-f]{32}', predecessor)):
            raise SyncError('Некорректная ссылка на предыдущий снимок')
        candidates.append((timestamp.astimezone(timezone.utc), entry, tag, linked, predecessor))
    if not candidates:
        return None
    legacy = sorted((item for item in candidates if not item[3]), key=lambda item: item[0], reverse=True)
    linked = [item for item in candidates if item[3]]
    if len(legacy) > 1 and legacy[0][0] == legacy[1][0]:
        raise SyncError('Порядок старых снимков неоднозначен')
    if not linked:
        selected = legacy[0]
    else:
        active = {item[1].name: item for item in linked}
        if legacy:
            anchor = legacy[0]
            active[anchor[1].name] = anchor
            roots = [anchor[1].name]
            if any(item[4] is None for item in linked):
                raise SyncError('Обнаружено разветвление цепочки снимков')
        else:
            roots = [item[1].name for item in linked if item[4] is None]
        if len(roots) != 1:
            raise SyncError('Обнаружено разветвление цепочки снимков')
        children = {}
        for item in linked:
            previous = item[4]
            if previous is not None:
                if previous not in active or previous in children:
                    raise SyncError('Цепочка снимков неполна или разветвлена')
                children[previous] = item[1].name
        visited = set()
        current = roots[0]
        while current not in visited:
            visited.add(current)
            if current not in children:
                break
            current = children[current]
        if len(visited) != len(active) or current in children:
            raise SyncError('Цепочка снимков неполна или разветвлена')
        selected = active[current]
    _, chosen, expected_tag, _, _ = selected
    if verify_payload:
        manifest = verify(chosen, key)
    else:
        manifest_path = chosen / 'manifest.json'
        if manifest_path.is_symlink():
            raise SyncError('Манифест снимка является ссылкой')
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    tag = manifest.get('authentication') if isinstance(manifest, dict) else None
    if not isinstance(tag, str) or not hmac.compare_digest(tag, expected_tag) or not hmac.compare_digest(tag, _manifest_tag(manifest, key)):
        raise SyncError('Снимок изменился во время проверки')
    return chosen, manifest


def extract(snapshot_dir: Path, destination: Path, key: bytes) -> int:
    """Extract to a new quarantine directory, never into Codex's live state."""
    manifest = verify(snapshot_dir, key)
    root = snapshot_dir.resolve(strict=True)
    parent = destination.parent.resolve(strict=True)
    if '.codex' in {part.casefold() for part in (*parent.parts, destination.name)}:
        raise SyncError('Распаковка в каталог Codex запрещена')
    if destination.exists() or destination.is_symlink():
        raise SyncError('Каталог назначения уже существует')
    Cipher, algorithms, modes, InvalidTag = _crypto()
    with tempfile.TemporaryDirectory(prefix='.nexus-extract-', dir=parent) as tmp:
        stage = Path(tmp)
        for item in manifest['files']:
            rel = item['path']
            source = root / 'files' / (rel + '.enc')
            target = stage.joinpath(*rel.split('/'))
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                with source.open('rb') as inp, target.open('xb') as out:
                    size = source.stat().st_size
                    nonce = inp.read(12)
                    inp.seek(-16, os.SEEK_END)
                    tag = inp.read(16)
                    inp.seek(12)
                    dec = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
                    dec.authenticate_additional_data(rel.encode('utf-8'))
                    remaining = size - 28
                    while remaining > 0:
                        chunk = inp.read(min(1024 * 1024, remaining))
                        if not chunk:
                            raise SyncError('Шифротекст изменился во время распаковки')
                        out.write(dec.update(chunk))
                        remaining -= len(chunk)
                    out.write(dec.finalize())
            except InvalidTag as exc:
                raise SyncError('Шифротекст изменился во время распаковки') from exc
        title_data = _snapshot_title_data(root, manifest, key)
        if title_data is not None:
            (stage / 'session_index.jsonl').write_bytes(title_data)
        # Never overwrite an existing directory, even one created after preview.
        destination.mkdir(mode=0o700)
        try:
            for child in stage.iterdir():
                os.replace(child, destination / child.name)
        except BaseException:
            # A partial quarantine is kept, rather than deleting anything that
            # could have been added by another process.
            raise SyncError(f'Распаковка прервалась; проверьте каталог {destination}')
    return len(manifest['files'])


def acknowledge(snapshot_dir: Path, shared: Path, receiver: str, key: bytes) -> Path:
    """A receiver signs its observation; this does not mean chats were imported."""
    receiver = _safe_machine(receiver)
    root = shared.resolve(strict=True)
    snapshot_path = snapshot_dir.resolve(strict=True)
    snapshots_root = root / 'nexus-sync' / 'snapshots'
    if snapshots_root.resolve() not in snapshot_path.parents:
        raise SyncError('Снимок должен находиться в папке обмена')
    manifest = verify(snapshot_path, key)
    snapshot_id = manifest.get('id')
    source = manifest.get('machine')
    if not isinstance(snapshot_id, str) or not re.fullmatch(r'[0-9a-f]{32}', snapshot_id):
        raise SyncError('Некорректный идентификатор снимка')
    _safe_machine(source if isinstance(source, str) else '')
    if snapshot_path.name != snapshot_id or snapshot_path.parent.name != source:
        raise SyncError('Расположение снимка не совпадает с манифестом')
    receipt = {'format': 'nexus-receipt-v1', 'source': source, 'receiver': receiver, 'id': snapshot_id,
               'manifest_tag': manifest['authentication'], 'created': datetime.now(timezone.utc).isoformat(), 'applied': False}
    receipt['authentication'] = _manifest_tag(receipt, key)
    parent = root / 'nexus-sync' / 'receipts' / source / snapshot_id
    parent.mkdir(parents=True, exist_ok=True)
    result = parent / (receiver + '.json')
    # Do not replace another run's receipt or claim the imported state.
    with result.open('x', encoding='utf-8') as output:
        json.dump(receipt, output, ensure_ascii=False, sort_keys=True, indent=2)
        output.write('\n')
    return result


def _chat_catalog(root: Path, *, include_unsupported: bool = False) -> dict[str, list[dict]]:
    root = root.resolve(strict=True)
    catalogue: dict[str, list[dict]] = {}
    for folder in ('sessions', 'archived_sessions'):
        directory = root / folder
        if directory.is_symlink():
            raise SyncError('Каталог чатов является символьной ссылкой; сравнение остановлено')
        if not directory.is_dir():
            if directory.exists():
                raise SyncError('Путь каталога чатов занят файлом; сравнение остановлено')
            continue
        for path in _walk(directory, reject_symlinks=True):
            if not path.name.startswith('rollout-') or path.suffix != '.jsonl':
                continue
            try:
                with path.open('rb') as stream:
                    first = stream.readline(1024 * 1024 + 1)
                if len(first) > 1024 * 1024:
                    raise SyncError('Слишком большая строка метаданных')
                record = json.loads(first)
                payload = record.get('payload') if isinstance(record, dict) and record.get('type') == 'session_meta' else None
                # Newer Codex histories use id for the thread and session_id
                # for the shared session; several threads can share the latter.
                identifier = (payload.get('id') or payload.get('session_id')) if isinstance(payload, dict) else None
                if not isinstance(identifier, str) or not identifier:
                    raise SyncError('Чат без идентификатора; сравнение остановлено')
                mode = payload.get('history_mode')
                unsupported = 'PAGINATED' if mode == 'paginated' else 'UNKNOWN_FORMAT' if mode not in (None, 'legacy') else None
                if unsupported and not include_unsupported:
                    raise UnsupportedHistoryError('Пагинированная история пока не поддерживается' if unsupported == 'PAGINATED' else 'Неизвестный формат истории пока не поддерживается')
                catalogue.setdefault(identifier, []).append({'path': path, 'cwd': payload.get('cwd'), 'folder': folder, 'unsupported': unsupported,
                                                            'history_base': payload.get('history_base'),
                                                            'parent_thread_id': payload.get('parent_thread_id'),
                                                            'thread_source': payload.get('thread_source')})
            except (ValueError, UnicodeError) as exc:
                raise SyncError('Неподдерживаемые метаданные чата; сравнение остановлено') from exc
    return catalogue


def _is_prefix(left: Path, right: Path) -> bool:
    if left.stat().st_size > right.stat().st_size:
        return False
    with left.open('rb') as first, right.open('rb') as second:
        while True:
            block = first.read(1024 * 1024)
            if not block:
                return True
            if second.read(len(block)) != block:
                return False


def _path_adjusted_relation(incoming: Path, local: Path, source_root: str, target_root: str) -> str:
    """Compare JSON records after normalizing only known structured cwd fields."""
    def normalized(line: bytes, root: str) -> str:
        if not line.endswith(b'\n'):
            raise SyncError('Незавершённая запись чата; повторите сравнение')
        try:
            record = json.loads(line)
        except (ValueError, UnicodeError) as exc:
            raise SyncError('Некорректная запись чата; сравнение остановлено') from exc
        if not isinstance(record, dict):
            raise SyncError('Некорректная запись чата; сравнение остановлено')
        if record.get('type') in ('session_meta', 'turn_context'):
            payload = record.get('payload')
            if isinstance(payload, dict) and 'cwd' in payload and not isinstance(payload['cwd'], str):
                raise SyncError('Некорректный путь в метаданных чата')
            if isinstance(payload, dict) and isinstance(payload.get('cwd'), str):
                try:
                    mapped = map_path(root, '/__nexus_sync_root__', payload['cwd'])
                except SyncError:
                    pass  # Unknown paths remain literal and cannot be guessed.
                else:
                    windows_source = bool(re.match(r'^[A-Za-z]:[\\/]', root) or root.startswith('\\\\'))
                    local_root = Path(target_root).expanduser()
                    if (root == source_root and windows_source and os.name != 'nt'
                            and local_root.is_absolute() and local_root.is_dir()):
                        local_root = local_root.resolve(strict=True)
                        candidate = Path(map_path(root, str(local_root), payload['cwd']))
                        state, resolved = _resolve_local_project(candidate, local_root, root)
                        if state != 'AVAILABLE' or resolved is None:
                            payload['cwd'] = {'nexus_sync_unresolved_windows_path': payload['cwd']}
                            return json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
                        mapped = map_path(str(local_root), '/__nexus_sync_root__', str(resolved))
                    payload['cwd'] = {'nexus_sync_normalized_path': mapped}
        return json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(',', ':'))

    with incoming.open('rb') as source, local.open('rb') as target:
        while True:
            left, right = source.readline(), target.readline()
            if not left and not right:
                return 'EQUIVALENT_AFTER_PATH_MAPPING'
            if not left:
                normalized(right, target_root)
                for line in target:
                    normalized(line, target_root)
                return 'LOCAL_EXTENDS_AFTER_PATH_MAPPING'
            if not right:
                normalized(left, source_root)
                for line in source:
                    normalized(line, source_root)
                return 'INCOMING_EXTENDS_AFTER_PATH_MAPPING'
            if normalized(left, source_root) != normalized(right, target_root):
                return 'DIVERGED'


def _resolve_local_project(mapped: Path, root: Path, source_root: str) -> tuple[str, Path | None]:
    """Resolve Windows-origin case differences only when every local match is unique."""
    try:
        root = root.resolve(strict=True)
        if not root.is_dir():
            return 'MAPPED_BUT_MISSING', None
        relative = mapped.relative_to(root)
        windows_source = bool(re.match(r'^[A-Za-z]:[\\/]', source_root) or source_root.startswith('\\\\'))
        if windows_source and os.name != 'nt':
            current = root
            for part in relative.parts:
                candidates = [child for child in current.iterdir() if child.name.lower() == part.lower()]
                if len(candidates) > 1:
                    return 'MAPPED_AMBIGUOUS', None
                if not candidates:
                    return 'MAPPED_BUT_MISSING', None
                current = candidates[0].resolve(strict=True)
                if current != root and root not in current.parents:
                    return 'MAPPED_OUTSIDE_NEXUS', None
                if not current.is_dir():
                    return 'MAPPED_BUT_MISSING', None
            resolved = current
        else:
            resolved = mapped.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            return 'MAPPED_OUTSIDE_NEXUS', None
        if not resolved.is_dir():
            return 'MAPPED_BUT_MISSING', None
        return 'AVAILABLE', resolved
    except (OSError, RuntimeError, ValueError):
        return 'MAPPED_BUT_MISSING', None


def _history_cwd_status(path: Path, source_root: str, target_root: str) -> str:
    """Check each structured cwd without exposing history or rewriting it."""
    try:
        before = path.stat()
        if path.is_symlink():
            return 'HISTORY_UNREADABLE'
        local_root = Path(target_root).expanduser()
        if not local_root.is_absolute():
            return 'MAPPED_BUT_MISSING'
        try:
            resolved_root = local_root.resolve(strict=True)
        except (OSError, RuntimeError):
            return 'MAPPED_BUT_MISSING'
        if not resolved_root.is_dir():
            return 'MAPPED_BUT_MISSING'
        result = 'AVAILABLE'
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                return 'HISTORY_UNREADABLE'
            for number, line in enumerate(stream, 1):
                if len(line) > 16 * 1024 * 1024 or not line.endswith(b'\n'):
                    return 'HISTORY_UNREADABLE'
                record = json.loads(line)
                if not isinstance(record, dict):
                    return 'HISTORY_UNREADABLE'
                if record.get('type') not in ('session_meta', 'turn_context'):
                    continue
                payload = record.get('payload')
                if not isinstance(payload, dict):
                    return 'HISTORY_UNREADABLE'
                cwd = payload.get('cwd')
                if number != 1 and cwd is None:
                    continue
                if not isinstance(cwd, str):
                    return 'MISSING_SOURCE_CWD'
                try:
                    mapped = Path(map_path(source_root, str(resolved_root), cwd)).expanduser()
                except SyncError:
                    return 'UNMAPPED'
                if not mapped.is_absolute():
                    return 'MAPPED_BUT_MISSING'
                state, _ = _resolve_local_project(mapped, resolved_root, source_root)
                if state != 'AVAILABLE':
                    return state
        after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino):
            return 'HISTORY_UNREADABLE'
        return result
    except (OSError, ValueError, UnicodeError, RuntimeError):
        return 'HISTORY_UNREADABLE'


def plan_chats(incoming: Path, target: Path, source_root: str | None = None, target_root: str | None = None) -> list[dict]:
    """Read-only classification; never decide that an unknown chat is absent."""
    if bool(source_root) != bool(target_root):
        raise SyncError('Укажите оба корня Nexus для сопоставления путей')
    incoming = incoming.resolve(strict=True)
    target = target.resolve(strict=True)
    if incoming == target or incoming in target.parents or target in incoming.parents:
        raise SyncError('Входящая копия и локальный Codex должны быть раздельными')
    # Inspect unsupported histories without declaring them safe to import. One
    # paginated chat must not prevent planning all other independent chats.
    sources = _chat_catalog(incoming, include_unsupported=True)
    if not sources:
        raise SyncError('Во входящей копии не найдено чатов')
    targets = _chat_catalog(target, include_unsupported=True)
    plan = []
    for identifier, versions in sorted(sources.items()):
        local = targets.get(identifier, [])
        thread_source = versions[0].get('thread_source') if len(versions) == 1 else None
        thread_kind = 'USER' if thread_source == 'user' else 'SERVICE' if thread_source in ('subagent', 'guardian_review') else 'UNKNOWN'
        parent_status = 'AMBIGUOUS' if len(versions) != 1 else 'NOT_REQUIRED'
        if len(versions) == 1 and versions[0].get('parent_thread_id') is not None:
            parent = versions[0]['parent_thread_id']
            if not isinstance(parent, str) or not parent or parent == identifier:
                parent_status = 'INVALID'
            elif len(sources.get(parent, [])) == 1 or len(targets.get(parent, [])) == 1:
                parent_status = 'PRESENT_UNVERIFIED'
            elif sources.get(parent) or targets.get(parent):
                parent_status = 'AMBIGUOUS'
            else:
                parent_status = 'MISSING'
        base_status = 'AMBIGUOUS' if len(versions) != 1 else 'NOT_REQUIRED'
        if len(versions) == 1 and versions[0].get('history_base') is not None:
            base = versions[0]['history_base']
            base_id = base.get('thread_id') if isinstance(base, dict) else None
            if not isinstance(base_id, str) or not base_id:
                base_status = 'INVALID'
            elif len(sources.get(base_id, [])) == 1 or len(targets.get(base_id, [])) == 1:
                base_status = 'PRESENT_UNVERIFIED'
            elif sources.get(base_id) or targets.get(base_id):
                base_status = 'AMBIGUOUS'
            else:
                base_status = 'MISSING'
        if len(versions) != 1 or len(local) > 1:
            relation = 'AMBIGUOUS'
        elif versions[0]['unsupported']:
            relation = 'INCOMING_UNSUPPORTED'
        elif not local:
            relation = 'NO_LOCAL_FILE'
        elif local[0]['unsupported']:
            relation = 'LOCAL_UNSUPPORTED'
        elif local[0]['folder'] != versions[0]['folder']:
            relation = 'ARCHIVE_CONFLICT'
        elif _digest(local[0]['path']) == _digest(versions[0]['path']):
            relation = 'IDENTICAL'
        elif _is_prefix(local[0]['path'], versions[0]['path']):
            relation = 'INCOMING_EXTENDS_LOCAL'
        elif _is_prefix(versions[0]['path'], local[0]['path']):
            relation = 'LOCAL_EXTENDS_INCOMING'
        elif source_root and target_root:
            relation = _path_adjusted_relation(versions[0]['path'], local[0]['path'], source_root, target_root)
        else:
            relation = 'DIVERGED'
        # A read-only comparison is useful for newer histories, but does not
        # certify that their index or inherited pages can be restored safely.
        paginated_relation = None
        if len(versions) == 1 and len(local) <= 1 and versions[0]['unsupported'] == 'PAGINATED':
            try:
                if not local:
                    paginated_relation = 'NO_LOCAL_FILE'
                elif local[0]['unsupported'] != 'PAGINATED' or local[0]['folder'] != versions[0]['folder']:
                    paginated_relation = 'INCOMPATIBLE'
                elif _digest(local[0]['path']) == _digest(versions[0]['path']):
                    paginated_relation = 'IDENTICAL'
                elif source_root and target_root:
                    paginated_relation = _path_adjusted_relation(versions[0]['path'], local[0]['path'], source_root, target_root)
                else:
                    paginated_relation = 'DIFFERENT_UNCLASSIFIED'
            except (OSError, SyncError):
                paginated_relation = 'UNREADABLE'
        cwd_status = 'NOT_CHECKED'
        if source_root and target_root:
            cwd_status = _history_cwd_status(versions[0]['path'], source_root, target_root)
        plan.append({'chat_hash': hashlib.sha256(identifier.encode('utf-8')).hexdigest(), 'relation': relation, 'cwd': cwd_status,
                     'incoming_format_warning': versions[0]['unsupported'] if len(versions) == 1 else None,
                     'history_base': base_status, 'parent_thread': parent_status, 'thread_kind': thread_kind,
                     'paginated_relation_unverified': paginated_relation})
    return plan


def prepare_chats(incoming: Path, destination: Path, source_root: str, target_root: str, *, skip_unsupported: bool = False, skipped: list | None = None, allow_paginated_preview: bool = False, copy_title_index: bool = False) -> int:
    """Prepare a separate test copy; never merge into an existing Codex state."""
    incoming = incoming.resolve(strict=True)
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if '.codex' in {part.casefold() for part in destination.parts}:
        raise SyncError('Подготовка внутри .codex запрещена')
    if destination.exists() or destination.is_symlink():
        raise SyncError('Каталог назначения уже существует')
    if incoming == parent or incoming in parent.parents:
        raise SyncError('Подготовленная копия должна быть вне входящего каталога')
    target = Path(target_root).expanduser().resolve(strict=True)
    if not target.is_dir():
        raise SyncError('Локальный корень Nexus должен быть каталогом')
    catalog = _chat_catalog(incoming, include_unsupported=skip_unsupported or allow_paginated_preview)
    if not catalog or any(len(versions) != 1 for versions in catalog.values()):
        raise SyncError('Нет чатов или обнаружены повторяющиеся идентификаторы')
    unsupported = [{'chat_hash': hashlib.sha256(identifier.encode('utf-8')).hexdigest(), 'reason': versions[0]['unsupported']}
                   for identifier, versions in sorted(catalog.items()) if versions[0]['unsupported']
                   and not (allow_paginated_preview and versions[0]['unsupported'] == 'PAGINATED')]
    if unsupported and not skip_unsupported:
        raise UnsupportedHistoryError('Неизвестный формат истории пока не поддерживается')
    if skipped is not None:
        skipped.extend(unsupported)
    catalog = {identifier: versions for identifier, versions in catalog.items()
               if not versions[0]['unsupported'] or (allow_paginated_preview and versions[0]['unsupported'] == 'PAGINATED')}
    if not catalog:
        raise UnsupportedHistoryError('Нет чатов с поддерживаемым форматом')
    with tempfile.TemporaryDirectory(prefix='.nexus-prepare-', dir=parent) as tmp:
        stage = Path(tmp)
        for versions in catalog.values():
            source = versions[0]['path']
            output = stage / source.relative_to(incoming)
            output.parent.mkdir(parents=True, exist_ok=True)
            before = source.stat()
            with source.open('rb') as inp, output.open('xb') as out:
                for number, line in enumerate(inp, 1):
                    if not line.endswith(b'\n'):
                        raise SyncError(f'Незавершённая строка истории: {number}')
                    try:
                        record = json.loads(line)
                    except (ValueError, UnicodeError) as exc:
                        raise SyncError(f'Некорректная строка истории: {number}') from exc
                    if not isinstance(record, dict):
                        raise SyncError(f'Некорректная строка истории: {number}')
                    if number == 1 and record.get('type') != 'session_meta':
                        raise SyncError('Первая запись не является session_meta')
                    if record.get('type') in ('session_meta', 'turn_context'):
                        payload = record.get('payload')
                        if not isinstance(payload, dict):
                            raise SyncError('Некорректные метаданные истории')
                        cwd = payload.get('cwd')
                        if number == 1 or cwd is not None:
                            if not isinstance(cwd, str):
                                raise SyncError('Не указан путь проекта в метаданных')
                            mapped = Path(map_path(source_root, str(target), cwd))
                            state, resolved = _resolve_local_project(mapped, target, source_root)
                            if state != 'AVAILABLE' or resolved is None:
                                raise SyncError('Локальная папка проекта недоступна или неоднозначна')
                            payload['cwd'] = str(resolved)
                            line = (json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8')
                    out.write(line)
            after = source.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise SyncError('История изменилась во время подготовки')
        if copy_title_index:
            title_data = _read_title_index(incoming)
            if title_data is not None:
                records = [record for record in _parse_title_index(title_data) if record['id'] in catalog]
                output = ''.join(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n' for record in records)
                (stage / 'session_index.jsonl').write_text(output, encoding='utf-8')
        destination.mkdir(mode=0o700)
        for child in stage.iterdir():
            os.replace(child, destination / child.name)
    return len(catalog)


def index_preview(incoming: Path, destination: Path, source_root: str, cfg: dict, executable: Path, *, reindex_mock: bool = False, replay_paginated_archives: bool = False, title_overrides: dict[str, str] | None = None) -> dict:
    """Experiment on a new private profile; never open or write the live Codex home."""
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    incoming = incoming.resolve(strict=True)
    for root in (cfg['nexus'], cfg['codex_home'], cfg['shared'], cfg['state_dir']):
        if parent == root or root in parent.parents or incoming == root or root in incoming.parents:
            raise SyncError('Тестовые истории и результат должны быть вне рабочих и общих папок')
    executable = executable.resolve(strict=True)
    if not executable.is_file():
        raise SyncError('Исполняемый файл Codex не найден')
    if reindex_mock and replay_paginated_archives:
        raise SyncError('Восстановление пагинированных архивов разрешено только в новой тестовой копии')
    if title_overrides is not None and not reindex_mock:
        raise SyncError('Названия можно задать только для повторной индексации моковой копии')
    if reindex_mock:
        temporary_root = Path(tempfile.gettempdir()).resolve(strict=True)
        if (destination.is_symlink() or not destination.is_dir() or destination.resolve(strict=True) != incoming
                or temporary_root not in incoming.parents or '.codex' in {part.casefold() for part in incoming.parts}):
            raise SyncError('Повторная индексация разрешена только в отдельном временном тестовом профиле')
        marker = destination / 'index-preview.json'
        if marker.is_symlink() or not marker.is_file():
            raise SyncError('Не найдена отметка тестового профиля')
        previous_report = json.loads(marker.read_text(encoding='utf-8'))
        if not isinstance(previous_report, dict) or previous_report.get('format') != 'nexus-index-preview-v1' or previous_report.get('applied') is not False:
            raise SyncError('Неизвестная отметка тестового профиля')
        if previous_report.get('reindex_in_progress_or_failed') is True:
            raise SyncError('Предыдущая индексация не завершена; создайте новую тестовую копию')
        previous_report.update({'partial': True, 'reindex_in_progress_or_failed': True})
        _write_json(marker, previous_report)
    else:
        prepare_chats(incoming, destination, source_root, str(cfg['nexus']), allow_paginated_preview=True, copy_title_index=True)
    catalog = _chat_catalog(destination, include_unsupported=True)
    if any(len(versions) != 1 or versions[0]['unsupported'] not in (None, 'PAGINATED') for versions in catalog.values()):
        raise SyncError('Повторяющиеся или неподдерживаемые истории в тестовом профиле')
    title_data = _read_title_index(destination)
    expected_titles = {record['id']: record['thread_name'] for record in _parse_title_index(title_data)} if title_data is not None else {}
    if title_overrides is not None:
        if not isinstance(title_overrides, dict):
            raise SyncError('Некорректные названия моковых чатов')
        for identifier, name in title_overrides.items():
            if (not isinstance(identifier, str) or identifier not in catalog or not isinstance(name, str) or not name
                    or (identifier in expected_titles and expected_titles[identifier] != name)):
                raise SyncError('Конфликт названий моковых чатов')
            expected_titles[identifier] = name
    pending = [(identifier, versions[0]) for identifier, versions in sorted(catalog.items())]
    if not pending:
        raise SyncError('В тестовой копии нет чатов; каталог сохранён')
    if any(not re.fullmatch(r'[0-9a-fA-F-]{36}', identifier) for identifier, _ in pending):
        raise SyncError('Некорректный идентификатор чата; тестовый каталог сохранён')
    env = {name: value for name, value in os.environ.items()
           if name not in ('CODEX_HOME', 'OPENAI_API_KEY', 'OPENAI_ADMIN_KEY', 'CHATGPT_ACCESS_TOKEN')}
    env['CODEX_HOME'] = str(destination)
    try:
        process = subprocess.Popen([str(executable), 'app-server', '--listen', 'stdio://'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    except OSError as exc:
        raise SyncError('Не удалось запустить тестовый app-server; каталог сохранён') from exc
    lines = queue.Queue()
    def pump():
        try:
            for line in process.stdout:
                lines.put(line)
        finally:
            lines.put(None)
    threading.Thread(target=pump, daemon=True).start()
    def send(value):
        process.stdin.write((json.dumps(value) + '\n').encode('utf-8'))
        process.stdin.flush()
    def call(identifier, method, params):
        send({'id': identifier, 'method': method, 'params': params})
        deadline = time.monotonic() + 20
        while True:
            try:
                line = lines.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty as exc:
                raise SyncError('Истекло время ожидания app-server') from exc
            if line is None:
                raise SyncError('Тестовый app-server завершился')
            response = json.loads(line)
            if isinstance(response, dict) and response.get('id') == identifier:
                if 'error' in response or 'result' not in response:
                    raise SyncError('Тестовый app-server отклонил запрос')
                return response['result']
    request_id = 2
    def request(method, params):
        nonlocal request_id
        result = call(request_id, method, params)
        request_id += 1
        return result
    def list_ids(archived: bool, state_only: bool) -> dict[str, str | None]:
        found, seen_cursors, cursor = {}, set(), None
        while True:
            params = {'limit': 100, 'archived': archived, 'useStateDbOnly': state_only}
            if cursor is not None:
                params['cursor'] = cursor
            listing = request('thread/list', params)
            if not isinstance(listing, dict) or not isinstance(listing.get('data'), list):
                raise SyncError('Тестовый app-server вернул неподдерживаемый список чатов')
            found.update({item['id']: item.get('name') for item in listing['data'] if isinstance(item, dict) and isinstance(item.get('id'), str)})
            cursor = listing.get('nextCursor')
            if cursor is None:
                return found
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors or len(seen_cursors) >= 1000:
                raise SyncError('Невозможно проверить все страницы списка чатов')
            seen_cursors.add(cursor)
    turn_pages_checked = 0
    turn_items_returned = 0
    def check_turn_pages(identifier: str) -> None:
        nonlocal turn_pages_checked, turn_items_returned
        cursor, seen_cursors = None, set()
        while True:
            params = {'threadId': identifier, 'itemsView': 'full', 'limit': 100}
            if cursor is not None:
                params['cursor'] = cursor
            page = request('thread/turns/list', params)
            if not isinstance(page, dict) or not isinstance(page.get('data'), list) or 'nextCursor' not in page:
                raise SyncError('Тестовый app-server вернул неподдерживаемый формат страниц истории')
            turn_pages_checked += 1
            turn_items_returned += len(page['data'])
            cursor = page['nextCursor']
            if cursor is None:
                return
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors or len(seen_cursors) >= 1000:
                raise SyncError('Невозможно проверить все страницы истории')
            seen_cursors.add(cursor)
    unlisted = []
    title_unmatched = []
    archive_unverified = []
    archive_replayed = []
    try:
        call(1, 'initialize', {'clientInfo': {'name': 'nexus_sync_preview', 'version': '0.1'},
                               'capabilities': {'experimentalApi': True}})
        send({'method': 'initialized', 'params': {}})
        if reindex_mock and expected_titles:
            current_names = list_ids(False, True)
            for identifier, name in expected_titles.items():
                existing_name = current_names.get(identifier)
                if name and existing_name and existing_name != name:
                    raise SyncError('Конфликт названия в тестовой базе Codex')
        if any(item['folder'] == 'archived_sessions' for _, item in pending):
            list_ids(True, False)  # Let Codex discover archived JSONL in this disposable profile.
        if reindex_mock:
            # A moved profile can retain stale paths in the database. For an
            # ordinary archived mock thread, Codex updates that path when it is
            # unarchived and archived again. Paginated archives remain unverified.
            archived_found = list_ids(True, True)
            for identifier, item in pending:
                if item['folder'] == 'archived_sessions' and item['unsupported'] is None and identifier not in archived_found:
                    request('thread/unarchive', {'threadId': identifier})
                    request('thread/archive', {'threadId': identifier})
        for identifier, item in pending:
            archived = item['folder'] == 'archived_sessions'
            if not archived:
                params = {'threadId': identifier, 'cwd': item['cwd']}
                if reindex_mock:
                    params['path'] = str(item['path'])
                request('thread/resume', params)
            if not archived and expected_titles.get(identifier):
                request('thread/name/set', {'threadId': identifier, 'name': expected_titles[identifier]})
            if item['unsupported'] == 'PAGINATED' and not archived:
                check_turn_pages(identifier)
            elif archived and item['unsupported'] == 'PAGINATED':
                # Codex may reject thread/read when the archived database still
                # points at the old profile path. Preserve the partial report.
                archive_unverified.append(identifier)
                if replay_paginated_archives:
                    request('thread/unarchive', {'threadId': identifier})
                    active = _chat_catalog(destination, include_unsupported=True).get(identifier, [])
                    if len(active) != 1 or active[0]['folder'] != 'sessions' or active[0]['unsupported'] != 'PAGINATED':
                        raise SyncError('Не удалось проверить извлечённый тестовый архив')
                    request('thread/resume', {'threadId': identifier, 'cwd': active[0]['cwd'], 'path': str(active[0]['path'])})
                    if expected_titles.get(identifier):
                        request('thread/name/set', {'threadId': identifier, 'name': expected_titles[identifier]})
                    check_turn_pages(identifier)
                    request('thread/archive', {'threadId': identifier})
                    archive_replayed.append(identifier)
            else:
                thread = request('thread/read', {'threadId': identifier, 'includeTurns': True})
                if not isinstance(thread, dict):
                    raise SyncError('Тестовый app-server вернул неподдерживаемый формат истории')
        for folder in ('sessions', 'archived_sessions'):
            expected = {identifier for identifier, item in pending if item['folder'] == folder}
            if not expected:
                continue
            found = list_ids(folder == 'archived_sessions', True)
            unlisted.extend(sorted(expected - set(found)))
            title_unmatched.extend(sorted(identifier for identifier in expected & expected_titles.keys()
                                          if found.get(identifier) != expected_titles[identifier]))
    except (OSError, ValueError, TypeError) as exc:
        raise SyncError('Ошибка тестового app-server; каталог сохранён') from exc
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired as exc:
            raise SyncError('App-server не завершился после закрытия канала; каталог сохранён') from exc
        process.stdout.close()
    if process.returncode:
        raise SyncError('Тестовый app-server завершился с ошибкой; каталог сохранён')
    result = {'format': 'nexus-index-preview-v1', 'resumed': sum(item['folder'] == 'sessions' for _, item in pending),
              'pagination_responded': sum(item['unsupported'] == 'PAGINATED' and item['folder'] == 'sessions' for _, item in pending),
              'turn_pages_checked': turn_pages_checked,
              'turn_items_returned': turn_items_returned,
              'legacy_responded': sum(item['unsupported'] is None for _, item in pending), 'applied': False,
              'db_unlisted': [hashlib.sha256(identifier.encode('utf-8')).hexdigest() for identifier in unlisted],
              'title_unmatched': [hashlib.sha256(identifier.encode('utf-8')).hexdigest() for identifier in title_unmatched],
              'archived_paginated_unverified': [hashlib.sha256(identifier.encode('utf-8')).hexdigest() for identifier in archive_unverified],
              'archived_paginated_replayed': [hashlib.sha256(identifier.encode('utf-8')).hexdigest() for identifier in archive_replayed],
              'partial': bool(unlisted or title_unmatched or archive_unverified), 'verified_in_cursor': False, 'contents_verified': False}
    if reindex_mock:
        result['mock_reindexed'] = True
    _write_json(destination / 'index-preview.json', result)
    return result


def index_snapshot_preview(cfg: dict, key: bytes, peer: str, snapshot_id: str,
                           destination: Path, executable: Path, *, include_local: bool = False) -> dict:
    """Verify a received snapshot and index only an isolated test copy."""
    _safe_machine(peer)
    if cfg['direction'] != 'send-receive' or peer not in cfg['peer_roots']:
        raise SyncError('Получение снимка для этого устройства не включено')
    if not re.fullmatch(r'[0-9a-f]{32}', snapshot_id):
        raise SyncError('Некорректный идентификатор снимка')
    root = cfg['shared'] / 'nexus-sync' / 'snapshots'
    source = root / peer / snapshot_id
    if root.is_symlink() or (root / peer).is_symlink() or source.is_symlink():
        raise SyncError('Символьные ссылки в пути снимка запрещены')
    manifest = verify(source, key)
    if manifest.get('machine') != peer or manifest.get('id') != snapshot_id:
        raise SyncError('Снимок не соответствует расположению')
    parent = destination.parent.resolve(strict=True)
    for forbidden in (cfg['nexus'], cfg['codex_home'], cfg['shared'], cfg['state_dir']):
        if parent == forbidden or forbidden in parent.parents:
            raise SyncError('Тестовый профиль должен быть вне рабочих и общих папок')
    with tempfile.TemporaryDirectory(prefix='.nexus-index-source-', dir=parent) as tmp:
        quarantine = Path(tmp) / 'quarantine'
        extract(source, quarantine, key)
        if verify(source, key).get('authentication') != manifest.get('authentication'):
            raise SyncError('Снимок изменился во время подготовки')
        actions = []
        title_conflicts = []
        if include_local:
            mapped = Path(tmp) / 'mapped'
            prepare_chats(quarantine, mapped, cfg['peer_roots'][peer], str(cfg['nexus']), allow_paginated_preview=True)
            local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
            incoming = _chat_catalog(mapped, include_unsupported=True)
            if any(len(versions) != 1 for versions in list(local.values()) + list(incoming.values())):
                raise SyncError('Повторяющиеся идентификаторы чатов; объединение остановлено')
            merged = Path(tmp) / 'merged'
            merged.mkdir(mode=0o700)
            baseline = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                        for versions in local.values() for item in versions}
            local_titles = _read_title_index(cfg['codex_home'])
            for versions in local.values():
                item = versions[0]
                _copy_stable_chat(item['path'], merged / item['path'].relative_to(cfg['codex_home']))
            accepted_titles = set()
            for identifier, versions in sorted(incoming.items()):
                item = versions[0]
                local_item = local.get(identifier)
                action = 'CONFLICT'
                if local_item:
                    existing = local_item[0]
                    local_copy = merged / existing['path'].relative_to(cfg['codex_home'])
                    if existing['folder'] == item['folder'] and existing['unsupported'] == item['unsupported']:
                        relation = _path_adjusted_relation(item['path'], local_copy,
                                                           str(cfg['nexus']), str(cfg['nexus']))
                        if relation == 'EQUIVALENT_AFTER_PATH_MAPPING':
                            action = 'KEEP_IDENTICAL'
                        elif relation == 'LOCAL_EXTENDS_AFTER_PATH_MAPPING':
                            action = 'KEEP_LOCAL_EXTENDS'
                        elif relation == 'INCOMING_EXTENDS_AFTER_PATH_MAPPING':
                            candidate = Path(tmp) / ('candidate-' + uuid.uuid4().hex)
                            _copy_stable_chat(item['path'], candidate)
                            os.replace(candidate, local_copy)
                            action = 'EXTEND_COPY'
                else:
                    target = merged / item['path'].relative_to(mapped)
                    if not target.exists() and not target.is_symlink():
                        _copy_stable_chat(item['path'], target)
                        action = 'ADD_COPY'
                actions.append({'chat_hash': hashlib.sha256(identifier.encode('utf-8')).hexdigest(), 'action': action})
                if action != 'CONFLICT':
                    accepted_titles.add(identifier)
            titles, title_conflicts = _merge_title_indices([(cfg['codex_home'], set(local)), (quarantine, accepted_titles)])
            if titles is not None:
                (merged / 'session_index.jsonl').write_bytes(titles)
            current = _chat_catalog(cfg['codex_home'], include_unsupported=True)
            after = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                     for versions in current.values() for item in versions}
            if baseline != after or local_titles != _read_title_index(cfg['codex_home']):
                raise SyncError('Локальные чаты изменились во время копирования')
            result = index_preview(merged, destination, str(cfg['nexus']), cfg, executable)
            current = _chat_catalog(cfg['codex_home'], include_unsupported=True)
            after = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                     for versions in current.values() for item in versions}
            local_changed = baseline != after or local_titles != _read_title_index(cfg['codex_home'])
        else:
            result = index_preview(quarantine, destination, cfg['peer_roots'][peer], cfg, executable)
            local_changed = False
    omitted = manifest.get('omitted_files')
    unresolved = sum(action['action'] == 'CONFLICT' for action in actions)
    result.update({'source': peer, 'snapshot': snapshot_id, 'omitted_files': omitted,
                   'local_included': include_local, 'local_changed': local_changed,
                   'actions': actions, 'unresolved_chats': unresolved, 'title_conflicts': title_conflicts,
                   'partial': result.get('partial', False) or omitted is None or omitted > 0 or unresolved > 0 or bool(title_conflicts) or local_changed})
    _write_json(destination / 'index-preview.json', result)
    return result


def index_multi_snapshot_preview(cfg: dict, key: bytes, selections: list[tuple[str, str]],
                                 destination: Path, executable: Path, *, expected_tags: dict | None = None) -> dict:
    """Merge selected peer snapshots and local chats into an isolated preview."""
    if cfg['direction'] != 'send-receive' or not selections:
        raise SyncError('Получение не включено или снимки не выбраны')
    if len({peer for peer, _ in selections}) != len(selections):
        raise SyncError('Выберите не более одного снимка каждого устройства')
    parent = destination.parent.resolve(strict=True)
    for forbidden in (cfg['nexus'], cfg['codex_home'], cfg['shared'], cfg['state_dir']):
        if parent == forbidden or forbidden in parent.parents:
            raise SyncError('Тестовый профиль должен быть вне рабочих и общих папок')
    root = cfg['shared'] / 'nexus-sync' / 'snapshots'
    if root.is_symlink():
        raise SyncError('Каталог снимков является ссылкой')
    selected = []
    for peer, identity in sorted(selections):
        _safe_machine(peer)
        if peer not in cfg['peer_roots'] or not re.fullmatch(r'[0-9a-f]{32}', identity):
            raise SyncError('Неизвестное устройство или идентификатор снимка')
        source = root / peer / identity
        if (root / peer).is_symlink() or source.is_symlink():
            raise SyncError('Символьные ссылки в пути снимка запрещены')
        manifest = verify(source, key)
        if manifest.get('machine') != peer or manifest.get('id') != identity:
            raise SyncError('Снимок не соответствует расположению')
        if expected_tags is not None and manifest.get('authentication') != expected_tags.get(peer):
            raise SyncError('Снимок изменился после выбора')
        selected.append((peer, identity, source, manifest))
    local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
    if any(len(items) != 1 for items in local.values()):
        raise SyncError('Повторяющиеся локальные идентификаторы чатов')
    baseline = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                for items in local.values() for item in items}
    local_titles = _read_title_index(cfg['codex_home'])
    title_sources = [(cfg['codex_home'], set(local))]
    actions = []
    with tempfile.TemporaryDirectory(prefix='.nexus-multi-preview-', dir=parent) as tmp:
        temporary = Path(tmp)
        merged = temporary / 'merged'
        merged.mkdir(mode=0o700)
        for items in local.values():
            item = items[0]
            _copy_stable_chat(item['path'], merged / item['path'].relative_to(cfg['codex_home']))
        for number, (peer, identity, source, manifest) in enumerate(selected):
            quarantine, mapped = temporary / f'quarantine-{number}', temporary / f'mapped-{number}'
            extract(source, quarantine, key)
            if verify(source, key).get('authentication') != manifest.get('authentication'):
                raise SyncError('Снимок изменился во время подготовки')
            prepare_chats(quarantine, mapped, cfg['peer_roots'][peer], str(cfg['nexus']), allow_paginated_preview=True)
            incoming = _chat_catalog(mapped, include_unsupported=True)
            if any(len(items) != 1 for items in incoming.values()):
                raise SyncError('Повторяющиеся идентификаторы во входящем снимке')
            current = _chat_catalog(merged, include_unsupported=True)
            accepted_titles = set()
            for identifier, items in sorted(incoming.items()):
                item = items[0]
                existing = current.get(identifier)
                action = 'CONFLICT'
                if existing and len(existing) == 1:
                    prior = existing[0]
                    if prior['folder'] == item['folder'] and prior['unsupported'] == item['unsupported']:
                        relation = _path_adjusted_relation(item['path'], prior['path'], str(cfg['nexus']), str(cfg['nexus']))
                        if relation == 'EQUIVALENT_AFTER_PATH_MAPPING':
                            action = 'KEEP_IDENTICAL'
                        elif relation == 'LOCAL_EXTENDS_AFTER_PATH_MAPPING':
                            action = 'KEEP_LOCAL_EXTENDS'
                        elif relation == 'INCOMING_EXTENDS_AFTER_PATH_MAPPING':
                            candidate = temporary / ('candidate-' + uuid.uuid4().hex)
                            _copy_stable_chat(item['path'], candidate)
                            os.replace(candidate, prior['path'])
                            action = 'EXTEND_COPY'
                elif not existing:
                    target = merged / item['path'].relative_to(mapped)
                    if not target.exists() and not target.is_symlink():
                        _copy_stable_chat(item['path'], target)
                        action = 'ADD_COPY'
                actions.append({'source': peer, 'chat_hash': hashlib.sha256(identifier.encode('utf-8')).hexdigest(), 'action': action})
                if action != 'CONFLICT':
                    accepted_titles.add(identifier)
            title_sources.append((quarantine, accepted_titles))
            # Re-read on the next source so newly added chats participate in comparisons.
        titles, title_conflicts = _merge_title_indices(title_sources)
        if titles is not None:
            (merged / 'session_index.jsonl').write_bytes(titles)
        def local_hashes():
            catalog = _chat_catalog(cfg['codex_home'], include_unsupported=True)
            return {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                    for items in catalog.values() for item in items}
        if baseline != local_hashes() or local_titles != _read_title_index(cfg['codex_home']):
            raise SyncError('Локальные чаты изменились во время копирования')
        result = index_preview(merged, destination, str(cfg['nexus']), cfg, executable)
        local_changed = baseline != local_hashes() or local_titles != _read_title_index(cfg['codex_home'])
    unresolved = sum(item['action'] == 'CONFLICT' for item in actions)
    sources = [{'source': peer, 'snapshot': identity, 'omitted_files': manifest.get('omitted_files')}
               for peer, identity, _, manifest in selected]
    result.update({'sources': sources, 'local_included': True, 'local_changed': local_changed,
                   'actions': actions, 'unresolved_chats': unresolved, 'title_conflicts': title_conflicts,
                   'partial': result.get('partial', False) or local_changed or unresolved > 0 or bool(title_conflicts) or any(item['omitted_files'] is None or item['omitted_files'] > 0 for item in sources)})
    _write_json(destination / 'index-preview.json', result)
    return result


def auto_multi_index_cycle(cfg: dict, key: bytes) -> list[str]:
    """Build an opt-in combined preview from currently verifiable peers."""
    base = cfg['auto_index_preview_dir']
    root = cfg['shared'] / 'nexus-sync' / 'snapshots'
    if base.is_symlink() or root.is_symlink():
        raise SyncError('Каталог снимков или копий является ссылкой')
    selected, missing = [], []
    for peer in sorted(cfg['peer_roots']):
        folder = root / peer
        try:
            if folder.is_symlink() or not folder.is_dir():
                raise SyncError('Снимок недоступен')
            latest = latest_authenticated_snapshot(folder, peer, key)
            if latest is None:
                raise SyncError('Снимок недоступен')
            candidate, manifest = latest
            selected.append((peer, candidate.name, manifest['authentication']))
        except (SyncError, OSError, ValueError, TypeError, KeyError):
            missing.append(peer)
    if not selected:
        return ['Индексация отложена: нет доступных снимков'] if missing else []
    local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
    fingerprint = source_fingerprint(cfg['codex_home'], key=key) if local else 'empty'
    identity_data = {'sources': selected, 'missing': missing, 'local': fingerprint,
                     'peer_roots': cfg['peer_roots']}
    identity = hmac.new(key, json.dumps(identity_data, sort_keys=True, ensure_ascii=False).encode('utf-8'), hashlib.sha256).hexdigest()
    prefix = 'combined-' + identity[:32]
    attempts = [path for path in sorted(base.iterdir())
                if path.name == prefix or re.fullmatch(re.escape(prefix) + r'-[0-9a-f]{12}', path.name)]
    for candidate_path in attempts:
        try:
            marker_path = candidate_path / 'auto-preview.json'
            if (candidate_path.is_symlink() or not candidate_path.is_dir() or marker_path.is_symlink()
                    or not marker_path.is_file() or marker_path.stat().st_size > 65536):
                continue
            marker = json.loads(marker_path.read_text(encoding='utf-8'))
            tag = marker.get('authentication') if isinstance(marker, dict) else None
            if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(marker, key))
                    and marker.get('format') == 'nexus-auto-multi-preview-v1'
                    and marker.get('identity') == identity and isinstance(marker.get('partial'), bool)):
                return ['Индексация отложена: есть недоступные снимки, конфликты или пропуски'] if marker['partial'] else []
        except (OSError, ValueError, TypeError):
            continue
    if len(attempts) >= 3:
        raise SyncError('Неудачные тестовые копии требуют проверки перед повторной индексацией')
    destination = base / (prefix + '-' + uuid.uuid4().hex[:12])
    report = index_multi_snapshot_preview(cfg, key, [(peer, snapshot) for peer, snapshot, _ in selected],
                                          destination, cfg['auto_index_codex_executable'],
                                          expected_tags={peer: tag for peer, _, tag in selected})
    for peer, snapshot, tag in selected:
        if verify(root / peer / snapshot, key).get('authentication') != tag:
            raise SyncError('Снимок изменился во время подготовки')
    report['missing_sources'] = missing
    report['partial'] = report['partial'] or bool(missing)
    _write_json(destination / 'index-preview.json', report)
    marker = {'format': 'nexus-auto-multi-preview-v1', 'identity': identity, 'partial': report['partial']}
    marker['authentication'] = _manifest_tag(marker, key)
    with (destination / 'auto-preview.json').open('x', encoding='utf-8') as stream:
        json.dump(marker, stream, ensure_ascii=False, sort_keys=True)
        stream.write('\n')
    notes = ['Подготовлена объединённая тестовая копия; в Codex не импортирована']
    if report['partial']:
        notes.append('Индексация отложена: есть недоступные снимки, конфликты или пропуски')
    return notes


def auto_index_cycle(cfg: dict) -> list[str]:
    """At most one isolated preview per cycle; never import into Codex."""
    base = cfg.get('auto_index_preview_dir')
    if cfg.get('direction') != 'send-receive' or base is None:
        return []
    key = read_key(cfg['key_file'], cfg['shared'])
    if cfg.get('auto_index_combine_peers', False):
        return auto_multi_index_cycle(cfg, key)
    snapshots = cfg['shared'] / 'nexus-sync' / 'snapshots'
    if snapshots.is_symlink():
        raise SyncError('Каталог снимков является ссылкой')
    if not snapshots.is_dir():
        return []
    local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
    fingerprint = source_fingerprint(cfg['codex_home'], key=key) if local else 'empty'
    notes = []
    created = False
    peers = sorted(cfg['peer_roots'])
    cursor_path = cfg['state_dir'] / f"auto-index-cursor-{cfg['machine']}.json"
    if cursor_path.is_symlink():
        raise SyncError('Отметка очереди является ссылкой')
    if cursor_path.exists():
        cursor = json.loads(cursor_path.read_text(encoding='utf-8'))
        tag = cursor.get('authentication') if isinstance(cursor, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(cursor, key))
                or cursor.get('format') != 'nexus-index-cursor-v1' or cursor.get('machine') != cfg['machine']
                or not isinstance(cursor.get('next_peer'), str)):
            raise SyncError('Отметка очереди не подтверждена')
        if cursor['next_peer'] in peers:
            offset = peers.index(cursor['next_peer'])
            peers = peers[offset:] + peers[:offset]
    def process_peer(peer: str, already_created: bool) -> tuple[list[str], bool]:
        source = snapshots / peer
        if source.is_symlink():
            raise SyncError('Каталог снимков является ссылкой')
        if not source.is_dir():
            return [], False
        latest = latest_authenticated_snapshot(source, peer, key)
        if latest is None:
            return [], False
        candidate, manifest = latest
        identity = hmac.new(key, (fingerprint + '\0' + cfg['peer_roots'][peer]).encode('utf-8'), hashlib.sha256).hexdigest()[:24]
        parent = base / peer
        if base.is_symlink() or parent.is_symlink():
            raise SyncError('Каталог тестовых копий является ссылкой')
        parent.mkdir(mode=0o700, exist_ok=True)
        prefix = candidate.name + '-' + identity
        attempts = [path for path in sorted(parent.iterdir())
                    if path.name == prefix or re.fullmatch(re.escape(prefix) + r'-[0-9a-f]{12}', path.name)]
        for candidate_path in attempts:
            try:
                marker_path = candidate_path / 'auto-preview.json'
                if (candidate_path.is_symlink() or not candidate_path.is_dir() or marker_path.is_symlink()
                        or not marker_path.is_file() or marker_path.stat().st_size > 65536):
                    continue
                marker = json.loads(marker_path.read_text(encoding='utf-8'))
                tag = marker.get('authentication') if isinstance(marker, dict) else None
                if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(marker, key))
                        and marker.get('format') == 'nexus-auto-preview-v1' and marker.get('source') == peer
                        and marker.get('snapshot') == candidate.name and marker.get('manifest_tag') == manifest.get('authentication')
                        and marker.get('identity') == identity and isinstance(marker.get('partial'), bool)):
                    if marker['partial']:
                        return ['Индексация отложена: есть конфликты или пропуски в тестовой копии'], False
                    return [], False
            except (OSError, ValueError, TypeError):
                continue
        if len(attempts) >= 3:
            raise SyncError('Неудачные тестовые копии требуют проверки перед повторной индексацией')
        if already_created:
            return [], False
        destination = parent / (prefix + '-' + uuid.uuid4().hex[:12])
        report = index_snapshot_preview(cfg, key, peer, candidate.name, destination,
                                        cfg['auto_index_codex_executable'], include_local=True)
        marker = {'format': 'nexus-auto-preview-v1', 'source': peer, 'snapshot': candidate.name,
                  'manifest_tag': manifest['authentication'], 'identity': identity, 'partial': report['partial']}
        marker['authentication'] = _manifest_tag(marker, key)
        with (destination / 'auto-preview.json').open('x', encoding='utf-8') as stream:
            json.dump(marker, stream, ensure_ascii=False, sort_keys=True)
            stream.write('\n')
        result = ['Подготовлена тестовая индексированная копия; в Codex не импортирована']
        if report['partial']:
            result.append('Индексация отложена: есть конфликты или пропуски в тестовой копии')
        return result, True
    for peer in peers:
        try:
            result, did_create = process_peer(peer, created)
            notes.extend(result)
            created = created or did_create
            if did_create:
                ordered = sorted(cfg['peer_roots'])
                marker = {'format': 'nexus-index-cursor-v1', 'machine': cfg['machine'],
                          'next_peer': ordered[(ordered.index(peer) + 1) % len(ordered)]}
                marker['authentication'] = _manifest_tag(marker, key)
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-index-cursor-',
                                                 dir=cfg['state_dir'], delete=False) as handle:
                    temporary = Path(handle.name)
                    json.dump(marker, handle, ensure_ascii=False, sort_keys=True)
                    handle.write('\n')
                try:
                    os.replace(temporary, cursor_path)
                finally:
                    temporary.unlink(missing_ok=True)
        except (SyncError, OSError, ValueError, TypeError, KeyError) as exc:
            notes.append('Индексация отложена: ' + type(exc).__name__)
    return notes


def status(shared: Path, machine: str, key: bytes | None = None) -> dict:
    """Report only locally verified observations, not delivery claims."""
    machine = _safe_machine(machine)
    parent = shared.resolve(strict=True) / 'nexus-sync' / 'snapshots' / machine
    if not parent.is_dir():
        return {'state': 'no_snapshot', 'message': 'Снимков пока нет'}
    try:
        if key is not None:
            latest = latest_authenticated_snapshot(parent, machine, key)
            if latest is None:
                return {'state': 'no_snapshot', 'message': 'Снимков пока нет'}
            _, manifest = latest
        else:
            candidates = sorted(
                (path for path in parent.iterdir() if path.is_dir() and not path.is_symlink() and not path.name.startswith('.')),
                key=lambda path: path.stat().st_mtime_ns, reverse=True,
            )
            if not candidates:
                return {'state': 'no_snapshot', 'message': 'Снимков пока нет'}
            manifest = verify(candidates[0], None)
    except (SyncError, OSError, ValueError, TypeError, KeyError):
        return {'state': 'invalid', 'message': 'Последний снимок не прошёл проверку'}
    received = []
    if key is not None and re.fullmatch(r'[0-9a-f]{32}', str(manifest.get('id', ''))):
        directory = shared / 'nexus-sync' / 'receipts' / machine / manifest['id']
        if directory.is_dir() and not directory.is_symlink():
            for path in sorted(directory.glob('*.json')):
                try:
                    if path.is_symlink():
                        continue
                    receipt = json.loads(path.read_text(encoding='utf-8'))
                    tag = receipt.get('authentication')
                    if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(receipt, key))
                            and receipt.get('id') == manifest['id'] and receipt.get('source') == machine
                            and receipt.get('manifest_tag') == manifest.get('authentication')
                            and receipt.get('receiver') == path.stem and receipt.get('applied') is False):
                        received.append(path.stem)
                except (OSError, ValueError, TypeError, AttributeError):
                    continue
    message = (('Проверено получателями: ' + ', '.join(received) + '; в Codex не применено') if received else
               'Локальный снимок проверен; доставка не подтверждена' if key is not None else
               'Обнаружен снимок; подлинность не подтверждена; доставка не подтверждена')
    paginated = manifest.get('paginated_files')
    unknown = manifest.get('unknown_history_files')
    omitted = manifest.get('omitted_files')
    if paginated is None or unknown is None or omitted is None or paginated > 0 or unknown > 0 or omitted > 0:
        warning = 'Полнота старого снимка не проверена' if paginated is None or unknown is None or omitted is None else f'Пагинированных историй: {paginated}; неизвестных форматов: {unknown}; пропущено файлов: {omitted}'
        message += '; ' + warning + '; восстановление не подтверждено'
        state = 'snapshot_unverified'
    else:
        state = 'snapshot_received' if received else 'snapshot_ready'
    return {'state': state, 'message': message, 'received_by': received, 'authenticity_verified': key is not None,
            'files': len(manifest['files']), 'created': manifest.get('created'), 'paginated_files': paginated, 'unknown_history_files': unknown, 'omitted_files': omitted}


def source_fingerprint(codex: Path, *, key: bytes | None = None, platform: str | None = None) -> str:
    """Detect changes; on Windows also stream keyed content due to timestamp semantics."""
    root = codex.resolve(strict=True)
    platform = sys.platform if platform is None else platform
    if platform == 'win32':
        if not isinstance(key, bytes) or len(key) != 32:
            raise SyncError('Для проверки содержимого нужен ключ')
        digest = hmac.new(key, b'nexus-windows-chat-fingerprint-v1\0', hashlib.sha256)
    else:
        digest = hashlib.sha256()
    found = False
    for directory in _history_roots(root):
        for path in _walk(directory, reject_symlinks=True):
            if not path.name.startswith('rollout-') or path.suffix != '.jsonl':
                continue
            found = True
            try:
                details = path.stat()
                state = [details.st_size, details.st_mtime_ns, details.st_ctime_ns, details.st_ino, details.st_mode]
            except OSError:
                details = None
                state = ['unavailable']
            digest.update(json.dumps([path.relative_to(root).as_posix(), *state], ensure_ascii=False).encode('utf-8'))
            digest.update(b'\0')
            if platform == 'win32':
                try:
                    if details is None or path.is_symlink():
                        raise OSError('file unavailable')
                    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0))
                    with os.fdopen(fd, 'rb') as stream:
                        opened = os.fstat(stream.fileno())
                        if (not statmod.S_ISREG(opened.st_mode) or
                                (details.st_dev, details.st_ino) != (opened.st_dev, opened.st_ino)):
                            raise OSError('not a regular file')
                        while chunk := stream.read(1024 * 1024):
                            digest.update(chunk)
                    digest.update(b'\0content-end\0')
                except OSError:
                    digest.update(b'\0content-unavailable\0')
    if not found:
        raise SyncError('Не найдено ни одного rollout-*.jsonl')
    index = root / 'session_index.jsonl'
    if index.is_symlink():
        raise SyncError('Индекс названий является ссылкой')
    try:
        details = index.stat()
        state = [details.st_size, details.st_mtime_ns, details.st_ctime_ns, details.st_ino, details.st_mode]
    except FileNotFoundError:
        details = None
        state = ['absent']
    except OSError:
        details = None
        state = ['unavailable']
    digest.update(json.dumps(['session_index.jsonl', *state], ensure_ascii=False).encode('utf-8'))
    if platform == 'win32' and details is not None:
        try:
            fd = os.open(index, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0))
            with os.fdopen(fd, 'rb') as stream:
                opened = os.fstat(stream.fileno())
                if (not statmod.S_ISREG(opened.st_mode) or
                        (details.st_dev, details.st_ino) != (opened.st_dev, opened.st_ino)):
                    raise OSError('not a regular file')
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
        except OSError:
            digest.update(b'\0title-unavailable\0')
    return digest.hexdigest()


def omitted_chat_ready(codex: Path, key: bytes, tokens: list[str]) -> bool:
    """Check omitted candidates locally; no paths or contents are logged."""
    wanted = set(tokens)
    if not wanted:
        return False
    root = codex.resolve(strict=True)
    if _omitted_token(key, 'session_index.jsonl') in wanted:
        try:
            if _read_title_index(root) is not None:
                return True
        except (SyncError, OSError, ValueError):
            pass
    for directory in _history_roots(root):
        for path in _walk(directory, reject_symlinks=True):
            if not path.name.startswith('rollout-') or path.suffix != '.jsonl':
                continue
            if _omitted_token(key, path.relative_to(root).as_posix()) not in wanted:
                continue
            try:
                before = path.stat()
                if before.st_size == 0 or path.is_symlink():
                    continue
                with path.open('rb') as stream:
                    for line in stream:
                        if not line.endswith(b'\n') or not isinstance(json.loads(line), dict):
                            raise ValueError('incomplete record')
                after = path.stat()
                if (before.st_size, before.st_mtime_ns, before.st_ino) == (after.st_size, after.st_mtime_ns, after.st_ino):
                    return True
            except (OSError, ValueError, UnicodeError):
                continue
    return False


def retry_omitted(cfg: dict, key: bytes, fingerprint: str) -> bool:
    """Recheck a previously partial snapshot even when source metadata is unchanged."""
    marker = cfg['state_dir'] / f"last-snapshot-{cfg['machine']}.json"
    try:
        data = json.loads(marker.read_text(encoding='utf-8'))
        tag, tokens = data.get('authentication'), data.get('omitted_tokens')
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(data, key))
                or data.get('fingerprint') != fingerprint or not isinstance(tokens, list)
                or any(not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{64}', token) for token in tokens)):
            return True  # An old or invalid marker is refreshed once.
        return omitted_chat_ready(cfg['codex_home'], key, tokens)
    except (OSError, ValueError, TypeError, AttributeError):
        return True


def watch(codex: Path, shared: Path, machine: str, interval: float, key: bytes) -> None:
    if interval < 10:
        raise SyncError('Интервал должен быть не меньше 10 секунд')
    previous = None
    omitted = []
    while True:
        try:
            current = source_fingerprint(codex, key=key)
            if current != previous or omitted_chat_ready(codex, key, omitted):
                new_omitted = []
                destination = snapshot(codex, shared, machine, key, omitted_tokens=new_omitted)
                omitted = new_omitted
                previous = current
                metadata = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
                warning = '; есть пропуски или неподтверждённые форматы' if metadata.get('paginated_files') or metadata.get('unknown_history_files') or metadata.get('omitted_files') else ''
                print(f'Локальный снимок создан: {destination.name}; доставка не подтверждена{warning}', flush=True)
        except (SyncError, OSError, ValueError) as exc:
            print(f'Снимок отложен: {exc}', file=sys.stderr, flush=True)
        time.sleep(interval)


def receive_once(shared: Path, receiver: str, key: bytes) -> int:
    receiver = _safe_machine(receiver)
    root = shared.resolve(strict=True) / 'nexus-sync' / 'snapshots'
    if not root.is_dir():
        return 0
    count = 0
    for source in sorted(root.iterdir()):
        if not source.is_dir() or source.is_symlink() or source.name.startswith('.') or source.name == receiver:
            continue
        for candidate in sorted(source.iterdir()):
            if not candidate.is_dir() or candidate.is_symlink() or candidate.name.startswith('.'):
                continue
            try:
                receipt_path = shared / 'nexus-sync' / 'receipts' / source.name / candidate.name / (receiver + '.json')
                if receipt_path.is_file() and not receipt_path.is_symlink():
                    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
                    tag = receipt.get('authentication') if isinstance(receipt, dict) else None
                    if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(receipt, key))
                            and receipt.get('source') == source.name and receipt.get('id') == candidate.name
                            and receipt.get('receiver') == receiver and receipt.get('applied') is False):
                        continue
                    raise SyncError('Некорректное подтверждение получения')
                acknowledge(candidate, shared, receiver, key)
                count += 1
            except FileExistsError:
                continue
            except (SyncError, OSError, ValueError, KeyError, TypeError) as exc:
                # A partially delivered snapshot remains available for retry.
                print(f'Получение ещё не подтверждено: {type(exc).__name__}', file=sys.stderr, flush=True)
    return count


def watch_receive(shared: Path, machine: str, interval: float, key: bytes) -> None:
    if interval < 10:
        raise SyncError('Интервал должен быть не меньше 10 секунд')
    while True:
        try:
            count = receive_once(shared, machine, key)
            if count:
                print(f'Проверено полученных снимков: {count}; в Codex не применены.', flush=True)
        except (SyncError, OSError, ValueError) as exc:
            print(f'Получение отложено: {type(exc).__name__}', file=sys.stderr, flush=True)
        time.sleep(interval)


def _prepared_inventory(root: Path, key: bytes, *, allow_empty: bool = False) -> list[dict]:
    """Authenticate the complete prepared file set without storing plain hashes."""
    derived = hmac.new(key, b'nexus-prepared-files-v2', hashlib.sha256).digest()
    entries = []
    def failed(error):
        raise SyncError('Не удалось прочитать подготовленные файлы') from error
    for current, directories, files in os.walk(root, followlinks=False, onerror=failed):
        directory = Path(current)
        directories.sort()
        for name in directories:
            child = directory / name
            if child.is_symlink():
                raise SyncError('Символьные ссылки в подготовленной копии запрещены')
            if directory == root and name not in ('sessions', 'archived_sessions'):
                raise SyncError('Неожиданный каталог в подготовленной копии')
        for name in sorted(files):
            path = directory / name
            if directory == root and name == 'prepared.json' and not path.is_symlink():
                continue
            relative = path.relative_to(root)
            if (path.is_symlink() or not path.is_file() or len(relative.parts) < 2
                    or relative.parts[0] not in ('sessions', 'archived_sessions')
                    or not name.startswith('rollout-') or not name.endswith('.jsonl')):
                raise SyncError('Неожиданный файл в подготовленной копии')
            before = path.stat()
            digest = hmac.new(derived, digestmod=hashlib.sha256)
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise SyncError('Подготовленный файл изменился во время проверки')
            entries.append({'path': relative.as_posix(), 'size': before.st_size, 'tag': digest.hexdigest()})
    if not entries and not allow_empty:
        raise SyncError('Подготовленные чаты не найдены')
    return sorted(entries, key=lambda entry: entry['path'])


def prepare_received_once(cfg: dict, key: bytes) -> int:
    """Prepare opted-in peer snapshots locally; never write into Codex."""
    if cfg['direction'] != 'send-receive':
        return 0
    snapshots = cfg['shared'] / 'nexus-sync' / 'snapshots'
    if not snapshots.is_dir() or snapshots.is_symlink():
        return 0
    count = 0
    for peer, peer_root in sorted(cfg['peer_roots'].items()):
        source = snapshots / peer
        if source.is_symlink() or not source.is_dir():
            continue
        for candidate in sorted(source.iterdir()):
            if candidate.is_symlink() or not candidate.is_dir() or not re.fullmatch(r'[0-9a-f]{32}', candidate.name):
                continue
            try:
                manifest = verify(candidate, key)
                if manifest.get('machine') != peer or manifest.get('id') != candidate.name:
                    raise SyncError('Расположение снимка не совпадает с манифестом')
                base = cfg['state_dir'] / 'prepared-chats'
                parent = base / peer
                if base.is_symlink() or parent.is_symlink():
                    raise SyncError('Символьные ссылки в папке подготовки запрещены')
                base.mkdir(mode=0o700, exist_ok=True)
                parent.mkdir(mode=0o700, exist_ok=True)
                destination = parent / candidate.name
                expected = {'id': candidate.name, 'source': peer, 'manifest_tag': manifest['authentication'],
                            'source_root': peer_root, 'target_root': str(cfg['nexus'])}
                if destination.exists() or destination.is_symlink():
                    if destination.is_symlink():
                        raise SyncError('Символьные ссылки в папке подготовки запрещены')
                    marker = destination / 'prepared.json'
                    if marker.is_symlink():
                        raise SyncError('Некорректная отметка подготовки')
                    metadata = json.loads(marker.read_text(encoding='utf-8'))
                    tag = metadata.get('authentication') if isinstance(metadata, dict) else None
                    if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(metadata, key))
                            or metadata.get('format') not in ('nexus-prepared-v1', 'nexus-prepared-v2')
                            or any(metadata.get(name) != value for name, value in expected.items())):
                        raise SyncError('Подготовка уже существует или не завершена')
                    if metadata['format'] == 'nexus-prepared-v2' and metadata.get('files') != _prepared_inventory(destination, key, allow_empty=bool(metadata.get('skipped'))):
                        raise SyncError('Подготовленная копия изменилась')
                    continue
                with tempfile.TemporaryDirectory(prefix='.nexus-receive-', dir=cfg['state_dir']) as tmp:
                    temporary = Path(tmp)
                    quarantine = temporary / 'quarantine'
                    prepared = temporary / 'prepared'
                    extract(candidate, quarantine, key)
                    skipped = []
                    try:
                        prepare_chats(quarantine, prepared, peer_root, str(cfg['nexus']), skip_unsupported=True, skipped=skipped)
                    except UnsupportedHistoryError:
                        if not skipped:
                            raise
                        prepared.mkdir(mode=0o700)
                    metadata = {'format': 'nexus-prepared-v2', **expected, 'files': _prepared_inventory(prepared, key, allow_empty=bool(skipped)), 'skipped': skipped, 'omitted_files': manifest.get('omitted_files')}
                    metadata['authentication'] = _manifest_tag(metadata, key)
                    # Reserve the final path exclusively; keep partial output on
                    # interruption for inspection, never replace another run.
                    destination.mkdir(mode=0o700)
                    for child in prepared.iterdir():
                        os.replace(child, destination / child.name)
                    with (destination / 'prepared.json').open('x', encoding='utf-8') as marker:
                        json.dump(metadata, marker, ensure_ascii=False, sort_keys=True)
                        marker.write('\n')
                count += 1
            except (SyncError, OSError, ValueError, KeyError, TypeError) as exc:
                print(f'Подготовка полученного снимка отложена: {type(exc).__name__}', file=sys.stderr, flush=True)
    return count


def load_settings(path: Path) -> dict:
    """Device-local configuration; never write it to the shared workspace."""
    raw = json.loads(path.read_text(encoding='utf-8'))
    result = validate_settings(raw)
    resolved = path.expanduser().resolve(strict=True)
    for root in (result['shared'], result['nexus'], result['codex_home']):
        if resolved == root or root in resolved.parents:
            raise SyncError('Конфигурацию устройства храните вне Nexus, Codex и общей папки')
    return result


def prepared_records(cfg: dict, key: bytes):
    """Yield authenticated preparation markers, without attesting file bytes."""
    base = cfg['state_dir'] / 'prepared-chats'
    if cfg['direction'] == 'send-receive' and base.is_dir() and not base.is_symlink():
        for peer, peer_root in cfg['peer_roots'].items():
            parent = base / peer
            if not parent.is_dir() or parent.is_symlink():
                continue
            for entry in sorted(parent.iterdir()):
                if entry.is_symlink() or not entry.is_dir() or not re.fullmatch(r'[0-9a-f]{32}', entry.name):
                    continue
                marker = entry / 'prepared.json'
                try:
                    if marker.is_symlink():
                        continue
                    data = json.loads(marker.read_text(encoding='utf-8'))
                    tag = data.get('authentication') if isinstance(data, dict) else None
                    if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(data, key))
                            and data.get('format') in ('nexus-prepared-v1', 'nexus-prepared-v2') and data.get('source') == peer
                            and data.get('id') == entry.name and data.get('source_root') == peer_root
                            and data.get('target_root') == str(cfg['nexus'])):
                        yield peer, entry, data
                except (OSError, ValueError, TypeError, AttributeError):
                    continue


def plan_prepared(cfg: dict, key: bytes) -> dict:
    """Read-only, point-in-time comparison, never an instruction to import."""
    snapshots = []
    for peer, entry, metadata in prepared_records(cfg, key):
        try:
            if metadata['format'] != 'nexus-prepared-v2':
                snapshots.append({'source': peer, 'snapshot': entry.name, 'state': 'UNVERIFIED', 'chats': []})
                continue
            skipped = metadata.get('skipped', [])
            omitted = metadata.get('omitted_files')
            if omitted is not None and (type(omitted) is not int or omitted < 0):
                raise SyncError('Некорректное число пропущенных историй')
            if (not isinstance(skipped, list) or any(not isinstance(item, dict)
                    or not isinstance(item.get('chat_hash'), str) or not re.fullmatch(r'[0-9a-f]{64}', item['chat_hash'])
                    or item.get('reason') not in ('PAGINATED', 'UNKNOWN_FORMAT') for item in skipped)):
                raise SyncError('Некорректный список пропущенных чатов')
            if metadata.get('files') != _prepared_inventory(entry, key, allow_empty=bool(skipped)):
                raise SyncError('Подготовленная копия изменилась')
            chats = plan_chats(entry, cfg['codex_home'], str(cfg['nexus']), str(cfg['nexus'])) if metadata['files'] else []
            if metadata.get('files') != _prepared_inventory(entry, key, allow_empty=bool(skipped)):
                raise SyncError('Подготовленная копия изменилась во время сравнения')
            snapshots.append({'source': peer, 'snapshot': entry.name, 'state': 'PARTIAL' if skipped or omitted is None or omitted > 0 else 'COMPARED', 'prepared_files': len(metadata['files']), 'chats': chats, 'skipped': skipped, 'omitted_files': omitted})
        except (SyncError, OSError, ValueError, KeyError, TypeError):
            snapshots.append({'source': peer, 'snapshot': entry.name, 'state': 'UNAVAILABLE', 'chats': []})
    verified = any(item.get('prepared_files', 0) > 0 for item in snapshots) and all(item['state'] in ('COMPARED', 'PARTIAL') for item in snapshots)
    return {'applied': False, 'prepared_files_verified': verified, 'snapshots': snapshots}


def _copy_stable_chat(source: Path, destination: Path) -> None:
    """Copy complete JSONL into a private preview and refuse changing input."""
    if source.is_symlink() or not source.is_file():
        raise SyncError('Исходный файл чата недоступен')
    before = source.stat()
    digest = hashlib.sha256()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with source.open('rb') as inp, destination.open('xb') as out:
        for line in inp:
            if not line.endswith(b'\n'):
                raise SyncError('История содержит незавершённую запись')
            try:
                record = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise SyncError('История содержит некорректную запись') from exc
            if not isinstance(record, dict):
                raise SyncError('История содержит некорректную запись')
            digest.update(line)
            out.write(line)
    after = source.stat()
    if ((before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino)
            or _digest(source) != digest.hexdigest()):
        raise SyncError('История изменилась во время копирования')


def preview_merge(cfg: dict, key: bytes, peer: str, snapshot_id: str, destination: Path) -> dict:
    """Build a separate review copy, never write to the active Codex home."""
    _safe_machine(peer)
    if not re.fullmatch(r'[0-9a-f]{32}', snapshot_id):
        raise SyncError('Некорректный идентификатор снимка')
    matches = [(entry, data) for source, entry, data in prepared_records(cfg, key)
               if source == peer and entry.name == snapshot_id]
    if len(matches) != 1:
        raise SyncError('Подготовленный снимок не найден или не подтверждён')
    incoming, metadata = matches[0]
    if metadata.get('format') != 'nexus-prepared-v2':
        raise SyncError('Для старой копии нет проверки содержимого')
    skipped = metadata.get('skipped', [])
    omitted = metadata.get('omitted_files')
    if omitted is not None and (type(omitted) is not int or omitted < 0):
        raise SyncError('Некорректное число пропущенных историй')
    if (not isinstance(skipped, list) or any(not isinstance(item, dict)
            or not isinstance(item.get('chat_hash'), str) or not re.fullmatch(r'[0-9a-f]{64}', item['chat_hash'])
            or item.get('reason') not in ('PAGINATED', 'UNKNOWN_FORMAT') for item in skipped)):
        raise SyncError('Некорректный список пропущенных чатов')
    if metadata.get('files') != _prepared_inventory(incoming, key, allow_empty=bool(skipped)):
        raise SyncError('Подготовленная копия изменилась')
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if destination.exists() or destination.is_symlink():
        raise SyncError('Каталог результата уже существует')
    for forbidden in (cfg['nexus'], cfg['codex_home'], cfg['shared'], incoming):
        if parent == forbidden or forbidden in parent.parents:
            raise SyncError('Каталог результата должен быть вне Nexus, Codex и папок обмена')
    if '.codex' in {part.casefold() for part in destination.parts}:
        raise SyncError('Результат нельзя сохранять в .codex')
    local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
    sources = _chat_catalog(incoming) if metadata['files'] else {}
    plan = plan_chats(incoming, cfg['codex_home'], str(cfg['nexus']), str(cfg['nexus'])) if sources else []
    by_hash = {hashlib.sha256(identifier.encode('utf-8')).hexdigest(): identifier for identifier in sources}
    baseline = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                for versions in local.values() for item in versions}
    actions = []
    with tempfile.TemporaryDirectory(prefix='.nexus-preview-', dir=parent) as tmp:
        stage = Path(tmp)
        for versions in local.values():
            for item in versions:
                rel = item['path'].relative_to(cfg['codex_home'])
                _copy_stable_chat(item['path'], stage / rel)
        for row in plan:
            identifier = by_hash[row['chat_hash']]
            source = sources[identifier]
            local_versions = local.get(identifier, [])
            relation = row['relation']
            action = 'KEEP_LOCAL' if local_versions else 'SKIP_UNRESOLVED'
            if relation == 'NO_LOCAL_FILE' and len(source) == 1:
                rel = source[0]['path'].relative_to(incoming)
                output = stage / rel
                if output.exists() or output.is_symlink():
                    action = 'PATH_CONFLICT'
                else:
                    _copy_stable_chat(source[0]['path'], output)
                    action = 'ADD_COPY'
            elif relation in ('INCOMING_EXTENDS_LOCAL', 'INCOMING_EXTENDS_AFTER_PATH_MAPPING') and len(source) == len(local_versions) == 1:
                output = stage / local_versions[0]['path'].relative_to(cfg['codex_home'])
                candidate = stage / '.incoming-candidate'
                _copy_stable_chat(source[0]['path'], candidate)
                os.replace(candidate, output)
                action = 'EXTEND_COPY'
            actions.append({'chat_hash': row['chat_hash'], 'relation': relation, 'action': action})
        after_local = _chat_catalog(cfg['codex_home'], include_unsupported=True)
        after_hashes = {item['path'].relative_to(cfg['codex_home']).as_posix(): _digest(item['path'])
                        for versions in after_local.values() for item in versions}
        if baseline != after_hashes:
            raise SyncError('Локальные чаты изменились во время подготовки')
        if metadata.get('files') != _prepared_inventory(incoming, key, allow_empty=bool(skipped)):
            raise SyncError('Входящая копия изменилась во время подготовки')
        # Keeping the local version is resolved only when it already contains
        # all incoming history; divergent and ambiguous versions need review.
        covered_local = {'IDENTICAL', 'EQUIVALENT_AFTER_PATH_MAPPING',
                         'LOCAL_EXTENDS_INCOMING', 'LOCAL_EXTENDS_AFTER_PATH_MAPPING'}
        unresolved = sum(1 for row in actions if not (
            row['action'] in ('ADD_COPY', 'EXTEND_COPY') or
            (row['action'] == 'KEEP_LOCAL' and row['relation'] in covered_local)))
        report = {'format': 'nexus-preview-v1', 'applied': False, 'source': peer,
                  'snapshot': snapshot_id, 'actions': actions, 'skipped': skipped,
                  'omitted_files': omitted, 'unresolved_chats': unresolved,
                  'partial': bool(skipped) or omitted is None or omitted > 0 or unresolved > 0}
        _write_json(stage / 'preview.json', report)
        destination.mkdir(mode=0o700)
        for child in sorted(stage.iterdir(), key=lambda path: path.name == 'preview.json'):
            os.replace(child, destination / child.name)
    return report


def heartbeat_status(cfg: dict, key: bytes) -> dict:
    """Read a signed local cycle marker; it is not proof the process is alive."""
    path = cfg['state_dir'] / f"heartbeat-{cfg['machine']}.json"
    try:
        if path.is_symlink():
            return {'state': 'unverified'}
        data = json.loads(path.read_text(encoding='utf-8'))
        tag = data.get('authentication') if isinstance(data, dict) else None
        allowed = {'snapshot', 'receive', 'index'}
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(data, key))
                or data.get('format') != 'nexus-heartbeat-v1' or data.get('machine') != cfg['machine']
                or type(data.get('timestamp')) is not int or data['timestamp'] < 0
                or not isinstance(data.get('errors'), list) or any(item not in allowed for item in data['errors'])
                or ('index_running' in data and not isinstance(data['index_running'], bool))
                or ('fingerprint_running' in data and not isinstance(data['fingerprint_running'], bool))):
            return {'state': 'unverified'}
        age = time.time() - data['timestamp']
        if age < -30:
            return {'state': 'unverified'}
        if age > max(90, cfg['interval_seconds'] * 3):
            state = 'stale'
        else:
            state = 'error' if data['errors'] else 'recent'
        return {'state': state, 'timestamp': data['timestamp'], 'errors': data['errors'],
                'index_running': data.get('index_running'),
                'fingerprint_running': data.get('fingerprint_running')}
    except FileNotFoundError:
        return {'state': 'unknown'}
    except (OSError, ValueError, TypeError, AttributeError):
        return {'state': 'unverified'}


def write_heartbeat(cfg: dict, key: bytes, notes: list[str], index_running: bool | None = None, *, fingerprint_running: bool | None = None) -> None:
    """Persist only coarse health categories, never notes or private content."""
    categories = {'Снимок отложен:': 'snapshot', 'Получение отложено:': 'receive',
                  'Индексация отложена:': 'index'}
    errors = {category for prefix, category in categories.items() if any(note.startswith(prefix) for note in notes)}
    path = cfg['state_dir'] / f"heartbeat-{cfg['machine']}.json"
    if path.is_symlink():
        raise SyncError('Файл состояния наблюдателя не может быть ссылкой')
    if path.exists():
        previous = json.loads(path.read_text(encoding='utf-8'))
        tag = previous.get('authentication') if isinstance(previous, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(previous, key))
                or previous.get('format') != 'nexus-heartbeat-v1' or previous.get('machine') != cfg['machine']):
            raise SyncError('Существующий файл состояния наблюдателя не подтверждён')
    data = {'format': 'nexus-heartbeat-v1', 'machine': cfg['machine'], 'timestamp': int(time.time()),
            'errors': sorted(errors)}
    if index_running is not None:
        data['index_running'] = bool(index_running)
    if fingerprint_running is not None:
        data['fingerprint_running'] = bool(fingerprint_running)
    data['authentication'] = _manifest_tag(data, key)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-heartbeat-', dir=cfg['state_dir'], delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True)
            handle.write('\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def auto_preview_records(cfg: dict, key: bytes) -> dict:
    """List signed local markers, without asserting preview contents or freshness."""
    base = cfg.get('auto_index_preview_dir')
    if base is None:
        return {'enabled': False, 'available': True, 'records': []}
    if base.is_symlink() or not base.is_dir():
        return {'enabled': True, 'available': False, 'records': []}
    records = []
    def inspect(entry: Path, scope: str, peer: str | None) -> None:
        relative = entry.relative_to(base).as_posix()
        record = {'path': relative, 'scope': scope, 'state': 'unverified', 'applied': False}
        try:
            marker_path = entry / 'auto-preview.json'
            if entry.is_symlink() or not entry.is_dir() or marker_path.is_symlink() or not marker_path.is_file() or marker_path.stat().st_size > 65536:
                raise SyncError('Некорректная отметка')
            marker = json.loads(marker_path.read_text(encoding='utf-8'))
            tag = marker.get('authentication') if isinstance(marker, dict) else None
            if not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(marker, key)) or not isinstance(marker.get('partial'), bool):
                raise SyncError('Некорректная отметка')
            identity = marker.get('identity')
            if scope == 'combined':
                if (marker.get('format') != 'nexus-auto-multi-preview-v1' or not isinstance(identity, str)
                        or not re.fullmatch(r'[0-9a-f]{64}', identity)
                        or not re.fullmatch(re.escape('combined-' + identity[:32]) + r'(?:-[0-9a-f]{12})?', entry.name)):
                    raise SyncError('Некорректная отметка')
            else:
                snapshot = marker.get('snapshot')
                if (marker.get('format') != 'nexus-auto-preview-v1' or marker.get('source') != peer
                        or not isinstance(snapshot, str) or not re.fullmatch(r'[0-9a-f]{32}', snapshot)
                        or not isinstance(identity, str) or not re.fullmatch(r'[0-9a-f]{24}', identity)
                        or not re.fullmatch(re.escape(snapshot + '-' + identity) + r'(?:-[0-9a-f]{12})?', entry.name)):
                    raise SyncError('Некорректная отметка')
            record.update({'state': 'partial' if marker['partial'] else 'marker_verified', 'contents_verified': False})
        except (SyncError, OSError, ValueError, TypeError):
            pass
        records.append(record)
    try:
        for entry in sorted(base.iterdir()):
            if re.fullmatch(r'combined-[0-9a-f]{32}(?:-[0-9a-f]{12})?', entry.name):
                inspect(entry, 'combined', None)
        for peer in sorted(cfg['peer_roots']):
            folder = base / peer
            if folder.is_symlink() or (folder.exists() and not folder.is_dir()):
                records.append({'path': peer, 'scope': 'peer', 'state': 'unverified', 'applied': False})
            elif folder.is_dir():
                for entry in sorted(folder.iterdir()):
                    if re.fullmatch(r'[0-9a-f]{32}-[0-9a-f]{24}(?:-[0-9a-f]{12})?', entry.name):
                        inspect(entry, 'peer', peer)
    except OSError:
        return {'enabled': True, 'available': False, 'records': records}
    return {'enabled': True, 'available': True, 'records': records}


def configured_status(cfg: dict, key: bytes) -> dict:
    """Summarize local records; a preparation marker is not an import proof."""
    result = status(cfg['shared'], cfg['machine'], key)
    expected = sorted(cfg['peer_roots'])
    if expected and result['state'] in ('snapshot_ready', 'snapshot_received', 'snapshot_unverified'):
        received = set(result.get('received_by', []))
        confirmed = sum(peer in received for peer in expected)
        result['expected_receivers'] = len(expected)
        result['confirmed_receivers'] = confirmed
        result['message'] += f'; подтверждено настроенных получателей: {confirmed} из {len(expected)}; импорт в Codex не подтверждён'
    records = list(prepared_records(cfg, key))
    count = len(records)
    skipped_count = sum(len(data.get('skipped', [])) for _, _, data in records if isinstance(data.get('skipped', []), list))
    incomplete = sum(1 for _, _, data in records if type(data.get('omitted_files')) is not int or data['omitted_files'] != 0)
    result['prepared_records'] = count
    result['prepared_skipped'] = skipped_count
    result['prepared_incomplete'] = incomplete
    previews = auto_preview_records(cfg, key)
    result['auto_preview_records'] = sum(record['state'] == 'marker_verified' for record in previews['records'])
    result['auto_preview_partial'] = sum(record['state'] == 'partial' for record in previews['records'])
    result['auto_preview_invalid'] = sum(record['state'] == 'unverified' for record in previews['records']) + int(not previews['available'])
    if previews['enabled']:
        result['message'] += (f"; отметок тестовых индексированных копий: {result['auto_preview_records']}, "
                              f"частичных: {result['auto_preview_partial']}, неподтверждённых: {result['auto_preview_invalid']}; "
                              'содержимое и актуальность не проверены, в Codex не импортированы')
    if count:
        result['message'] += f'; подготовлено копий снимков: {count}, в Codex не импортированы'
    if skipped_count:
        result['message'] += f'; пропущено неподдерживаемых чатов: {skipped_count}'
    if incomplete:
        result['message'] += f'; неполных или старых снимков: {incomplete}'
    observer = heartbeat_status(cfg, key)
    result['observer'] = observer
    if observer['state'] in ('recent', 'error') and observer.get('fingerprint_running') is True:
        result['fingerprint_activity'] = 'running'
        result['message'] += '; выполняется проверка содержимого историй'
    if cfg.get('auto_index_preview_dir') is not None:
        running = observer.get('index_running') if observer['state'] in ('recent', 'error') else None
        result['index_activity'] = 'running' if running is True else 'idle' if running is False else 'unknown'
        if running is True:
            result['message'] += '; подготавливается тестовая индексированная копия'
        elif running is None:
            result['message'] += '; состояние подготовки копии неизвестно'
    if observer['state'] == 'recent':
        result['message'] += '; недавно завершён цикл наблюдателя'
    elif observer['state'] == 'error':
        result['message'] += '; в последнем цикле были ошибки'
    elif observer['state'] == 'stale':
        result['message'] += '; давно нет отчёта наблюдателя'
    else:
        result['message'] += '; состояние наблюдателя не подтверждено'
    return result


def validate_settings(raw: dict) -> dict:
    if not isinstance(raw, dict) or raw.get('format') != 'nexus-sync-config-v1':
        raise SyncError('Неизвестный формат конфигурации')
    retired = ('git_repos', 'git_discovery', 'secret_files', 'discover_secrets', 'prepare_secrets')
    if any(raw.get(name) for name in retired):
        raise SyncError('Устаревшая конфигурация содержит неподдерживаемые настройки; создайте конфигурацию чатов')
    names = ('machine', 'nexus', 'codex_home', 'shared', 'state_dir', 'key_file')
    if any(not isinstance(raw.get(name), str) or not raw[name] for name in names):
        raise SyncError('В конфигурации отсутствуют обязательные пути или ID устройства')
    _safe_machine(raw['machine'])
    paths = {name: Path(raw[name]).expanduser().resolve(strict=True) for name in names if name != 'machine'}
    if any(not paths[name].is_dir() for name in ('nexus', 'codex_home', 'shared', 'state_dir')):
        raise SyncError('Nexus, Codex, общая и локальная папки должны существовать')
    if paths['shared'] == paths['codex_home'] or paths['shared'] in paths['codex_home'].parents or paths['codex_home'] in paths['shared'].parents:
        raise SyncError('Папка Codex и папка обмена должны быть раздельными')
    for root in (paths['shared'], paths['nexus'], paths['codex_home']):
        if paths['state_dir'] == root or root in paths['state_dir'].parents:
            raise SyncError('Локальное состояние должно быть вне Nexus, Codex и папки обмена')
    for root in (paths['nexus'], paths['codex_home']):
        if paths['key_file'] == root or root in paths['key_file'].parents:
            raise SyncError('Ключ должен быть вне Nexus и каталога Codex')
    read_key(paths['key_file'], paths['shared'])
    interval = raw.get('interval_seconds', 60)
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 10 <= interval <= 86400:
        raise SyncError('Интервал должен быть от 10 до 86400 секунд')
    direction = raw.get('direction', 'send-only')
    if direction not in ('send-only', 'send-receive'):
        raise SyncError('Направление должно быть send-only или send-receive')
    peer_roots = raw.get('peer_roots', {})
    if not isinstance(peer_roots, dict):
        raise SyncError('peer_roots должен быть объектом с путями устройств')
    for peer, root in peer_roots.items():
        if not isinstance(peer, str) or _safe_machine(peer) == raw['machine'] or not isinstance(root, str):
            raise SyncError('Некорректное сопоставление устройства')
        map_path(root, '/__nexus_sync_root__', root)
    preview_dir = raw.get('auto_index_preview_dir')
    codex_executable = raw.get('auto_index_codex_executable')
    if bool(preview_dir) != bool(codex_executable) or any(x is not None and not isinstance(x, str) for x in (preview_dir, codex_executable)):
        raise SyncError('Для автоматической тестовой копии укажите каталог и исполняемый Codex')
    if preview_dir:
        preview_path = Path(preview_dir).expanduser()
        executable_path = Path(codex_executable).expanduser()
        if direction != 'send-receive' or not preview_path.is_absolute() or not executable_path.is_absolute() or preview_path.is_symlink():
            raise SyncError('Автоматическая тестовая копия требует send-receive и абсолютные локальные пути')
        preview_path = preview_path.resolve(strict=True)
        executable_path = executable_path.resolve(strict=True)
        if not preview_path.is_dir() or not executable_path.is_file() or '.codex' in {part.casefold() for part in preview_path.parts}:
            raise SyncError('Каталог тестовых копий или Codex недоступен')
        for root in (paths['nexus'], paths['codex_home'], paths['shared'], paths['state_dir']):
            if preview_path == root or root in preview_path.parents or preview_path in root.parents:
                raise SyncError('Каталог тестовых копий должен быть отдельным')
    else:
        preview_path = executable_path = None
    combine_peers = raw.get('auto_index_combine_peers', False)
    if not isinstance(combine_peers, bool) or (combine_peers and preview_path is None):
        raise SyncError('Объединённая тестовая копия требует настроенной автоматической индексации')
    return {'machine': raw['machine'], **paths, 'interval_seconds': interval, 'direction': direction, 'peer_roots': dict(peer_roots), 'auto_index_preview_dir': preview_path, 'auto_index_codex_executable': executable_path, 'auto_index_combine_peers': combine_peers}


_INLINE_FINGERPRINT = object()


def supervisor_once(cfg: dict, previous: str | None, *,
                    fingerprint_override: str | None | object = _INLINE_FINGERPRINT) -> tuple[str | None, list[str]]:
    notes = []
    try:
        key = read_key(cfg['key_file'], cfg['shared'])
        fingerprint = (source_fingerprint(cfg['codex_home'], key=key)
                       if fingerprint_override is _INLINE_FINGERPRINT else fingerprint_override)
        if fingerprint is not None and (not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint)):
            raise SyncError('Некорректная проверка снимка')
        if fingerprint is not None and (fingerprint != previous or retry_omitted(cfg, key, fingerprint)):
            omitted = []
            destination = snapshot(cfg['codex_home'], cfg['shared'], cfg['machine'], key, omitted_tokens=omitted)
            marker = cfg['state_dir'] / f"last-snapshot-{cfg['machine']}.json"
            marker_data = {'fingerprint': fingerprint, 'id': destination.name, 'omitted_tokens': omitted}
            marker_data['authentication'] = _manifest_tag(marker_data, key)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-last-', dir=cfg['state_dir'], delete=False) as handle:
                tmp = Path(handle.name)
                json.dump(marker_data, handle)
            try:
                os.replace(tmp, marker)
            finally:
                tmp.unlink(missing_ok=True)
            previous = fingerprint
            notes.append('Создан локальный зашифрованный снимок; доставка не подтверждена')
            metadata = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
            if metadata.get('paginated_files') or metadata.get('unknown_history_files') or metadata.get('omitted_files'):
                notes.append('Есть пропущенные истории или неподтверждённые форматы; SQLite в снимок не входит')
    except (SyncError, OSError, ValueError) as exc:
        notes.append('Снимок отложен: ' + type(exc).__name__)
    if cfg['direction'] == 'send-receive':
        try:
            count = receive_once(cfg['shared'], cfg['machine'], read_key(cfg['key_file'], cfg['shared']))
            if count:
                notes.append(f'Проверено полученных снимков: {count}; в Codex не применены')
            prepared = prepare_received_once(cfg, read_key(cfg['key_file'], cfg['shared']))
            if prepared:
                notes.append(f'Подготовлено локальных копий снимков: {prepared}; в Codex не применены')
        except (SyncError, OSError, ValueError) as exc:
            notes.append('Получение отложено: ' + type(exc).__name__)
    return previous, notes


def previous_supervisor_snapshot(cfg: dict) -> str | None:
    """Trust a previous fingerprint only while its encrypted snapshot verifies."""
    try:
        marker = json.loads((cfg['state_dir'] / f"last-snapshot-{cfg['machine']}.json").read_text(encoding='utf-8'))
        identity, fingerprint = marker.get('id'), marker.get('fingerprint')
        key = read_key(cfg['key_file'], cfg['shared'])
        tag = marker.get('authentication')
        if not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(marker, key)):
            return None
        if not isinstance(identity, str) or not re.fullmatch(r'[0-9a-f]{32}', identity):
            return None
        if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint):
            return None
        path = cfg['shared'] / 'nexus-sync' / 'snapshots' / cfg['machine'] / identity
        manifest = verify(path, key)
        if manifest.get('id') != identity or manifest.get('machine') != cfg['machine']:
            return None
        return fingerprint
    except (OSError, ValueError, TypeError, KeyError, AttributeError, SyncError):
        return None


def _source_fingerprint_cycle(cfg: dict) -> str:
    return source_fingerprint(cfg['codex_home'], key=read_key(cfg['key_file'], cfg['shared']))


def run_supervisor(settings_path: Path) -> None:
    cfg = load_settings(settings_path)
    previous = previous_supervisor_snapshot(cfg)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='nexus-index') as index_executor, \
         ThreadPoolExecutor(max_workers=1, thread_name_prefix='nexus-fingerprint') as fingerprint_executor:
        index_future = None
        index_errors = False
        fingerprint_future = None
        fingerprint_error = False
        snapshot_error = False
        def collect_index(future, prior_error):
            if future is None or not future.done():
                return future, prior_error
            try:
                index_notes = future.result()
                prior_error = any(note.startswith('Индексация отложена:') for note in index_notes)
                for note in index_notes:
                    print(note, flush=True)
            except Exception as exc:
                prior_error = True
                print('Индексация отложена: ' + type(exc).__name__, file=sys.stderr, flush=True)
            return None, prior_error
        while True:
            if sys.platform == 'win32':
                fingerprint = None
                if fingerprint_future is not None and fingerprint_future.done():
                    try:
                        fingerprint = fingerprint_future.result()
                        if not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint):
                            raise SyncError('Некорректная проверка снимка')
                        fingerprint_error = False
                    except Exception as exc:
                        fingerprint_error = True
                        fingerprint = None
                        print('Проверка снимка отложена: ' + type(exc).__name__, file=sys.stderr, flush=True)
                    fingerprint_future = None
                previous, notes = supervisor_once(cfg, previous, fingerprint_override=fingerprint)
                if fingerprint is not None:
                    snapshot_error = any(note.startswith('Снимок отложен:') for note in notes)
                if (fingerprint_error or snapshot_error) and not any(note.startswith('Снимок отложен:') for note in notes):
                    notes.append('Снимок отложен: SyncError')
                if fingerprint_future is None:
                    fingerprint_future = fingerprint_executor.submit(_source_fingerprint_cycle, cfg)
            else:
                previous, notes = supervisor_once(cfg, previous)
            for note in notes:
                print(note, flush=True)
            index_future, index_errors = collect_index(index_future, index_errors)
            if cfg.get('auto_index_preview_dir') is not None and index_future is None:
                index_future = index_executor.submit(auto_index_cycle, cfg)
            index_future, index_errors = collect_index(index_future, index_errors)
            if index_errors:
                notes.append('Индексация отложена: SyncError')
            try:
                if sys.platform == 'win32':
                    write_heartbeat(cfg, read_key(cfg['key_file'], cfg['shared']), notes, index_future is not None,
                                    fingerprint_running=fingerprint_future is not None and not fingerprint_future.done())
                else:
                    write_heartbeat(cfg, read_key(cfg['key_file'], cfg['shared']), notes, index_future is not None)
            except (SyncError, OSError, ValueError, TypeError) as exc:
                print('Отчёт наблюдателя отложен: ' + type(exc).__name__, file=sys.stderr, flush=True)
            time.sleep(cfg['interval_seconds'])


def render_startup(platform: str, machine: str, python: str, script: str | None, config: str) -> tuple[str, bytes]:
    """Render an inactive per-user startup file without executing it."""
    _safe_machine(machine)
    if platform not in ('linux', 'macos', 'windows'):
        raise SyncError('Неизвестная ОС для файла автозапуска')
    path_type = PureWindowsPath if platform == 'windows' else PurePosixPath
    for value in (python, *([script] if script is not None else []), config):
        if not isinstance(value, str) or any(ord(char) < 32 for char in value):
            raise SyncError('Путь содержит недопустимые символы')
        path = path_type(value)
        if not path.is_absolute() or '..' in path.parts:
            raise SyncError('Для автозапуска нужны абсолютные локальные пути')
    args = [python, *([script] if script is not None else []), 'run', '--config', config]
    if platform == 'linux':
        def quote(value):
            return '"' + value.replace('%', '%%').replace('\\', '\\\\').replace('"', '\\"') + '"'
        command = ' '.join(quote(value) for value in args)
        content = ('[Unit]\nDescription=Nexus Sync\n\n[Service]\nType=simple\n'
                   f'ExecStart=:{command}\nRestart=on-failure\nRestartSec=10s\n'
                   'KillMode=mixed\nKillSignal=SIGINT\nSendSIGKILL=no\nTimeoutStopSec=infinity\n\n'
                   '[Install]\nWantedBy=default.target\n')
        return f'nexus-sync-{machine}.service', content.encode('utf-8')
    if platform == 'macos':
        content = {'Label': f'local.nexussync.{machine}', 'ProgramArguments': args,
                   'RunAtLoad': True, 'KeepAlive': {'SuccessfulExit': False}}
        return f'nexus-sync-{machine}.plist', plistlib.dumps(content, fmt=plistlib.FMT_XML)
    namespace = 'http://schemas.microsoft.com/windows/2004/02/mit/task'
    ET.register_namespace('', namespace)
    tag = lambda name: f'{{{namespace}}}{name}'
    task = ET.Element(tag('Task'), {'version': '1.4'})
    registration = ET.SubElement(task, tag('RegistrationInfo'))
    ET.SubElement(registration, tag('Description')).text = 'Nexus Sync (current user)'
    triggers = ET.SubElement(task, tag('Triggers'))
    trigger = ET.SubElement(triggers, tag('LogonTrigger'))
    ET.SubElement(trigger, tag('Enabled')).text = 'true'
    principals = ET.SubElement(task, tag('Principals'))
    principal = ET.SubElement(principals, tag('Principal'), {'id': 'Author'})
    ET.SubElement(principal, tag('LogonType')).text = 'InteractiveToken'
    ET.SubElement(principal, tag('RunLevel')).text = 'LeastPrivilege'
    settings = ET.SubElement(task, tag('Settings'))
    ET.SubElement(settings, tag('MultipleInstancesPolicy')).text = 'IgnoreNew'
    ET.SubElement(settings, tag('DisallowStartIfOnBatteries')).text = 'false'
    ET.SubElement(settings, tag('StopIfGoingOnBatteries')).text = 'false'
    ET.SubElement(settings, tag('ExecutionTimeLimit')).text = 'PT0S'
    actions = ET.SubElement(task, tag('Actions'), {'Context': 'Author'})
    execute = ET.SubElement(actions, tag('Exec'))
    ET.SubElement(execute, tag('Command')).text = python
    ET.SubElement(execute, tag('Arguments')).text = subprocess.list2cmdline(args[1:])
    return f'nexus-sync-{machine}.xml', ET.tostring(task, encoding='utf-8', xml_declaration=True)


def generate_startup(settings_path: Path, destination: Path) -> Path:
    """Create a new local directory with one inactive startup template."""
    cfg = load_settings(settings_path)
    platform = 'windows' if sys.platform == 'win32' else 'macos' if sys.platform == 'darwin' else 'linux' if sys.platform.startswith('linux') else None
    if platform is None:
        raise SyncError('ОС пока не поддерживается')
    executable = Path(sys.executable).absolute()  # Preserve a virtualenv symlink.
    script = None if getattr(sys, 'frozen', False) else str(Path(__file__).resolve(strict=True))
    config = settings_path.resolve(strict=True)
    if not executable.is_file():
        raise SyncError('Интерпретатор Python не найден')
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if destination.exists() or destination.is_symlink():
        raise SyncError('Каталог назначения уже существует')
    for root in (cfg['nexus'], cfg['codex_home'], cfg['shared']):
        if parent == root or root in parent.parents:
            raise SyncError('Шаблон создаётся вне Nexus, Codex и общей папки')
    name, content = render_startup(platform, cfg['machine'], str(executable), script, str(config))
    destination.mkdir(mode=0o700)
    fd = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(content)
    return destination / name


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='CoSyncX: безопасная подготовка обмена через Syncthing',
                                     epilog='Интерфейс: cosyncx ui --help; движок: cosyncx engine --help; подготовка сборки: cosyncx build-engine --help')
    sub = parser.add_subparsers(dest='command', required=True)
    run = sub.add_parser('run', help='запустить локальный наблюдатель по конфигурации устройства')
    run.add_argument('--config', type=Path, required=True)
    startup = sub.add_parser('generate-startup', help='создать неактивный шаблон автозапуска текущей ОС')
    startup.add_argument('--config', type=Path, required=True)
    startup.add_argument('--destination', type=Path, required=True)
    config = sub.add_parser('init-config', help='создать конфигурацию устройства в новом файле')
    config.add_argument('--output', type=Path, required=True)
    config.add_argument('--nexus', type=Path, required=True)
    config.add_argument('--codex-home', type=Path, required=True)
    config.add_argument('--shared', type=Path, required=True)
    config.add_argument('--state-dir', type=Path, required=True)
    config.add_argument('--key-file', type=Path, required=True)
    config.add_argument('--machine', required=True)
    config.add_argument('--direction', choices=('send-only', 'send-receive'), default='send-only')
    config.add_argument('--peer-root', nargs=2, action='append', metavar=('DEVICE', 'PATH'), default=[],
                        help='ID и проверенный путь Nexus другого устройства; можно повторять')
    config.add_argument('--auto-index-preview-dir', type=Path,
                        help='отдельный локальный каталог для автоматических тестовых копий')
    config.add_argument('--auto-index-codex-executable', type=Path,
                        help='проверенный исполняемый Codex для тестового app-server')
    config.add_argument('--auto-index-combine-peers', action='store_true',
                        help='объединять доступные снимки устройств в одну тестовую копию')
    publish = sub.add_parser('snapshot', help='создать неизменяемую копию историй')
    publish.add_argument('--codex-home', type=Path, required=True)
    publish.add_argument('--shared', type=Path, required=True)
    publish.add_argument('--machine', required=True)
    publish.add_argument('--key-file', type=Path, required=True)
    watcher = sub.add_parser('watch', help='периодически публиковать снимки; экспериментально')
    watcher.add_argument('--codex-home', type=Path, required=True)
    watcher.add_argument('--shared', type=Path, required=True)
    watcher.add_argument('--machine', required=True)
    watcher.add_argument('--key-file', type=Path, required=True)
    watcher.add_argument('--interval', type=float, default=60)
    receiver = sub.add_parser('watch-receive', help='проверять поступившие снимки и подтверждать получение')
    receiver.add_argument('--shared', type=Path, required=True)
    receiver.add_argument('--machine', required=True)
    receiver.add_argument('--key-file', type=Path, required=True)
    receiver.add_argument('--interval', type=float, default=60)
    check = sub.add_parser('verify', help='проверить полученный снимок')
    check.add_argument('snapshot', type=Path)
    check.add_argument('--key-file', type=Path, help='проверить аутентичность AES-GCM')
    unpack = sub.add_parser('extract', help='распаковать в новый карантинный каталог, не в Codex')
    unpack.add_argument('snapshot', type=Path)
    unpack.add_argument('--destination', type=Path, required=True)
    unpack.add_argument('--key-file', type=Path, required=True)
    ack = sub.add_parser('acknowledge', help='подтвердить проверку снимка на устройстве, без импорта')
    ack.add_argument('snapshot', type=Path)
    ack.add_argument('--shared', type=Path, required=True)
    ack.add_argument('--machine', required=True)
    ack.add_argument('--key-file', type=Path, required=True)
    keygen = sub.add_parser('keygen', help='создать новый ключ вне папки обмена')
    keygen.add_argument('path', type=Path)
    state = sub.add_parser('status', help='локальный статус снимков в формате JSON')
    state.add_argument('--shared', type=Path, required=True)
    state.add_argument('--machine', required=True)
    state.add_argument('--key-file', type=Path)
    summary = sub.add_parser('summary', help='локальные статусы по конфигурации устройства')
    summary.add_argument('--config', type=Path, required=True)
    summary.add_argument('--mock-preview-root', type=Path, help='дополнительно проверить отметки моковых чатов в /tmp')
    summary.add_argument('--mock-chat-profile', type=Path, help='проверить состояние мокового приёмника в /tmp')
    summary.add_argument('--mock-syncthing-config', type=Path)
    summary.add_argument('--mock-syncthing-folder', type=Path)
    summary.add_argument('--mock-syncthing-id')
    summary.add_argument('--mock-syncthing-port', type=int)
    previews = sub.add_parser('auto-previews', help='показать отметки локальных тестовых копий')
    previews.add_argument('--config', type=Path, required=True)
    mapping = sub.add_parser('map-path', help='сопоставить путь проекта между ОС без записи файлов')
    mapping.add_argument('--source-root', required=True)
    mapping.add_argument('--target-root', required=True)
    mapping.add_argument('path')
    chatplan = sub.add_parser('plan-chats', help='сравнить полученные чаты с локальными, ничего не записывая')
    chatplan.add_argument('--incoming', type=Path, required=True)
    chatplan.add_argument('--codex-home', type=Path, required=True)
    chatplan.add_argument('--source-root')
    chatplan.add_argument('--target-root')
    preparedplan = sub.add_parser('plan-prepared', help='сравнить подготовленные копии по конфигурации без импорта')
    preparedplan.add_argument('--config', type=Path, required=True)
    preview = sub.add_parser('preview-merge', help='собрать отдельную тестовую копию без изменения Codex')
    preview.add_argument('--config', type=Path, required=True)
    preview.add_argument('--source', required=True)
    preview.add_argument('--snapshot', required=True)
    preview.add_argument('--destination', type=Path, required=True)
    prepare = sub.add_parser('prepare-chats', help='подготовить отдельную тестовую копию с локальными путями')
    prepare.add_argument('--incoming', type=Path, required=True)
    prepare.add_argument('--destination', type=Path, required=True)
    prepare.add_argument('--source-root', required=True)
    prepare.add_argument('--target-root', required=True)
    prepare.add_argument('--allow-paginated-preview', action='store_true',
                         help='экспериментально скопировать пагинированные чаты для теста без индекса')
    index = sub.add_parser('index-preview', help='экспериментально построить индекс в новом тестовом профиле')
    index.add_argument('--incoming', type=Path, required=True)
    index.add_argument('--destination', type=Path, required=True)
    index.add_argument('--source-root', required=True)
    index.add_argument('--config', type=Path, required=True)
    index.add_argument('--codex-executable', type=Path, required=True)
    index.add_argument('--replay-paginated-archives', action='store_true', help='экспериментально перестроить пагинированные архивы в новой копии')
    reindex = sub.add_parser('reindex-mock-preview', help='повторно индексировать перемещённый временный тестовый профиль')
    reindex.add_argument('--profile', type=Path, required=True)
    reindex.add_argument('--config', type=Path, required=True)
    reindex.add_argument('--codex-executable', type=Path, required=True)
    snapshotindex = sub.add_parser('index-snapshot-preview', help='подготовить тестовый индекс полученного снимка')
    snapshotindex.add_argument('--source', required=True)
    snapshotindex.add_argument('--snapshot', required=True)
    snapshotindex.add_argument('--destination', type=Path, required=True)
    snapshotindex.add_argument('--config', type=Path, required=True)
    snapshotindex.add_argument('--codex-executable', type=Path, required=True)
    snapshotindex.add_argument('--include-local', action='store_true',
                               help='добавить локальные чаты в отдельную тестовую копию')
    multiindex = sub.add_parser('index-multi-preview', help='подготовить одну тестовую копию из нескольких снимков')
    multiindex.add_argument('--source-snapshot', nargs=2, action='append', required=True, metavar=('DEVICE', 'ID'))
    multiindex.add_argument('--destination', type=Path, required=True)
    multiindex.add_argument('--config', type=Path, required=True)
    multiindex.add_argument('--codex-executable', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'init-config':
            if len({peer for peer, _ in args.peer_root}) != len(args.peer_root):
                raise SyncError('Путь одного устройства указан несколько раз')
            data = {'format': 'nexus-sync-config-v1', 'machine': _safe_machine(args.machine),
                    'nexus': str(args.nexus.expanduser().resolve(strict=True)),
                    'codex_home': str(args.codex_home.expanduser().resolve(strict=True)),
                    'shared': str(args.shared.expanduser().resolve(strict=True)),
                    'state_dir': str(args.state_dir.expanduser().resolve(strict=True)),
                    'key_file': str(args.key_file.expanduser().resolve(strict=True)), 'interval_seconds': 60,
                    'direction': args.direction, 'peer_roots': dict(args.peer_root),
                    'auto_index_preview_dir': str(args.auto_index_preview_dir.expanduser()) if args.auto_index_preview_dir else None,
                    'auto_index_codex_executable': str(args.auto_index_codex_executable.expanduser()) if args.auto_index_codex_executable else None,
                    'auto_index_combine_peers': args.auto_index_combine_peers}
            validate_settings(data)
            output_path = args.output.expanduser().resolve()
            for private_root in (Path(data['shared']), Path(data['nexus']), Path(data['codex_home'])):
                if output_path == private_root or private_root in output_path.parents:
                    raise SyncError('Конфигурацию устройства храните вне Nexus и общей папки')
            with args.output.open('x', encoding='utf-8') as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2)
                handle.write('\n')
            print(f'Локальная конфигурация создана: {args.output}; автозапуск не настроен.')
        elif args.command == 'run':
            run_supervisor(args.config)
        elif args.command == 'generate-startup':
            created = generate_startup(args.config, args.destination)
            print(f'Шаблон создан: {created}; автозапуск не установлен и не включён.')
        elif args.command == 'snapshot':
            created = snapshot(args.codex_home, args.shared, args.machine, read_key(args.key_file, args.shared))
            print(f'Снимок создан: {created}')
            print('Это копия историй, не импорт в Codex и не подтверждение доставки на VPS.')
            metadata = json.loads((created / 'manifest.json').read_text(encoding='utf-8'))
            if metadata.get('paginated_files') or metadata.get('unknown_history_files') or metadata.get('omitted_files'):
                print('Есть пропущенные истории или неподтверждённые форматы; SQLite в снимок не входит.')
        elif args.command == 'watch':
            watch(args.codex_home, args.shared, args.machine, args.interval, read_key(args.key_file, args.shared))
        elif args.command == 'watch-receive':
            watch_receive(args.shared, args.machine, args.interval, read_key(args.key_file, args.shared))
        elif args.command == 'verify':
            key = args.key_file.read_bytes() if args.key_file else None
            result = verify(args.snapshot, key)
            proof = 'целостность и AES-GCM' if key is not None else 'только хэши; подлинность не проверена'
            print(f"Снимок проверен ({proof}): {len(result['files'])} файлов; импорт в Codex не выполнен.")
            if result.get('paginated_files') is None or result.get('unknown_history_files') is None or result.get('omitted_files') is None or result['paginated_files'] > 0 or result['unknown_history_files'] > 0 or result['omitted_files'] > 0:
                print('Восстановление формата истории не подтверждено.')
        elif args.command == 'extract':
            count = extract(args.snapshot, args.destination, args.key_file.read_bytes())
            print(f'В карантин распаковано файлов: {count}; импорт в Codex не выполнен.')
        elif args.command == 'acknowledge':
            receipt = acknowledge(args.snapshot, args.shared, args.machine, read_key(args.key_file, args.shared))
            print(f'Проверка получения записана: {receipt}; импорт в Codex не выполнен.')
        elif args.command == 'keygen':
            create_key(args.path)
            print('Ключ создан; сохраните его вне Syncthing и передайте на устройства защищённым способом.')
        elif args.command == 'status':
            key = read_key(args.key_file, args.shared) if args.key_file else None
            print(json.dumps(status(args.shared, args.machine, key), ensure_ascii=False))
        elif args.command == 'summary':
            cfg = load_settings(args.config)
            key = read_key(cfg['key_file'], cfg['shared'])
            result = configured_status(cfg, key)
            if args.mock_preview_root is not None:
                import mock_chat_preview
                result['mock_chat_preview'] = mock_chat_preview.status(args.mock_preview_root, key, cfg['shared'])
            if args.mock_chat_profile is not None:
                import mock_chat_sync
                result['mock_chat_watch'] = mock_chat_sync.watch_status(args.mock_chat_profile, cfg['shared'], key)
            mock_syncthing_args = (args.mock_syncthing_config, args.mock_syncthing_folder,
                                   args.mock_syncthing_id, args.mock_syncthing_port)
            if any(value is not None for value in mock_syncthing_args):
                if not all(value is not None for value in mock_syncthing_args):
                    raise SyncError('Нужны все параметры мокового Syncthing')
                import mock_syncthing_status
                result['mock_syncthing_runtime'] = mock_syncthing_status.check(*mock_syncthing_args)
            print(json.dumps(result, ensure_ascii=False))
        elif args.command == 'auto-previews':
            cfg = load_settings(args.config)
            print(json.dumps(auto_preview_records(cfg, read_key(cfg['key_file'], cfg['shared'])), ensure_ascii=False, indent=2))
        elif args.command == 'map-path':
            print(map_path(args.source_root, args.target_root, args.path))
        elif args.command == 'plan-chats':
            print(json.dumps(plan_chats(args.incoming, args.codex_home, args.source_root, args.target_root), ensure_ascii=False, indent=2))
        elif args.command == 'plan-prepared':
            cfg = load_settings(args.config)
            print(json.dumps(plan_prepared(cfg, read_key(cfg['key_file'], cfg['shared'])), ensure_ascii=False, indent=2))
        elif args.command == 'preview-merge':
            cfg = load_settings(args.config)
            report = preview_merge(cfg, read_key(cfg['key_file'], cfg['shared']), args.source, args.snapshot, args.destination)
            print(json.dumps(report, ensure_ascii=False, indent=2))
        elif args.command == 'prepare-chats':
            count = prepare_chats(args.incoming, args.destination, args.source_root, args.target_root,
                                  allow_paginated_preview=args.allow_paginated_preview)
            print(f'Подготовлено тестовых чатов: {count}; импорт в Codex не выполнен.')
            if args.allow_paginated_preview:
                print('Пагинированные истории экспериментальны: индекс не восстановлен, проверка в Cursor не выполнена.')
        elif args.command == 'index-preview':
            cfg = load_settings(args.config)
            result = index_preview(args.incoming, args.destination, args.source_root, cfg, args.codex_executable,
                                   replay_paginated_archives=args.replay_paginated_archives)
            print(json.dumps(result, ensure_ascii=False))
        elif args.command == 'reindex-mock-preview':
            cfg = load_settings(args.config)
            result = index_preview(args.profile, args.profile, str(cfg['nexus']), cfg, args.codex_executable, reindex_mock=True)
            print(json.dumps(result, ensure_ascii=False))
        elif args.command == 'index-snapshot-preview':
            cfg = load_settings(args.config)
            result = index_snapshot_preview(cfg, read_key(cfg['key_file'], cfg['shared']), args.source,
                                            args.snapshot, args.destination, args.codex_executable,
                                            include_local=args.include_local)
            print(json.dumps(result, ensure_ascii=False))
        elif args.command == 'index-multi-preview':
            cfg = load_settings(args.config)
            result = index_multi_snapshot_preview(cfg, read_key(cfg['key_file'], cfg['shared']),
                                                  args.source_snapshot, args.destination, args.codex_executable)
            print(json.dumps(result, ensure_ascii=False))
        return 0
    except KeyboardInterrupt:
        print('\nОстановлено.')
        return 130
    except (SyncError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f'Ошибка: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
