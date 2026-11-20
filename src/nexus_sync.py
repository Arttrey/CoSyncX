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
SECRET_MAGIC = b'NEXUS-SECRETS-1\0'
TITLE_INDEX_AAD = b'nexus-session-index-v1'
PROJECT_EVENT_MAGIC = b'NEXUS-PROJECT-EVENT-1\0'
MAX_SECRET_BYTES = 16 * 1024 * 1024
EXCLUDED = {'.git', '.venv', 'venv', 'node_modules', '__pycache__', '.pytest_cache', '.mypy_cache',
            '.ruff_cache', '.tox', '.nox', '.dart_tool', '.gradle', '.cache', '.pub-cache',
            '.flutter-sdk', '.codexsync-workspace'}
MANIFESTS = {'package.json': 'Node.js', 'package-lock.json': 'npm', 'npm-shrinkwrap.json': 'npm',
             'pnpm-lock.yaml': 'pnpm', 'yarn.lock': 'Yarn', 'bun.lock': 'Bun', 'bun.lockb': 'Bun',
             'requirements.txt': 'Python', 'requirements-dev.txt': 'Python', 'pyproject.toml': 'Python',
             'Pipfile': 'Python', 'Pipfile.lock': 'Python', 'poetry.lock': 'Python', 'uv.lock': 'Python',
             'pubspec.yaml': 'Dart/Flutter', 'pubspec.lock': 'Dart/Flutter', 'Cargo.toml': 'Rust',
             'Cargo.lock': 'Rust', 'go.mod': 'Go', 'go.sum': 'Go', 'pom.xml': 'Java',
             'build.gradle': 'Java/Gradle', 'build.gradle.kts': 'Java/Gradle', 'gradle.lockfile': 'Gradle'}
PYTHON_PROJECT_FILES = {'requirements.txt', 'requirements-dev.txt', 'pyproject.toml', 'Pipfile'}
WINDOWS_RESERVED_STEMS = {'CON', 'PRN', 'AUX', 'NUL', *(f'COM{n}' for n in '123456789¹²³'),
                          *(f'LPT{n}' for n in '123456789¹²³')}
TOOLCHAINS = {'Python': (PYTHON_PROJECT_FILES, ('python3', 'python')),
              'Node.js': ({'package.json'}, ('node',)),
              'Dart/Flutter': ({'pubspec.yaml'}, ('dart', 'flutter')),
              'Rust/Cargo': ({'Cargo.toml'}, ('cargo',)),
              'Go': ({'go.mod'}, ('go',)),
              'Java/JDK': ({'pom.xml', 'build.gradle', 'build.gradle.kts'}, ('javac',))}


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
    if len(data) > MAX_SECRET_BYTES:
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
    if before.st_size > MAX_SECRET_BYTES:
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
            _secret_parts(part, nested=False)
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
            if name not in EXCLUDED and not linked:
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


def _secret_parts(name: str, *, nested: bool) -> tuple[str, ...]:
    parts = name.split('/')
    if not nested and len(parts) != 1:
        raise SyncError('Некорректный путь в пакете секретов')
    for part in parts:
        device_name = part.split('.', 1)[0].rstrip(' ')
        if (not part or part in ('.', '..') or part[-1] in (' ', '.')
                or any(c in '<>:"\\|?*' or ord(c) < 32 for c in part)
                or re.fullmatch(r'(?i:CON|PRN|AUX|NUL|CONIN\$|CONOUT\$|COM[1-9¹²³]|LPT[1-9¹²³])', device_name)):
            raise SyncError('Непереносимый путь в пакете секретов')
    return tuple(parts)


def _add_secret_path(seen: set[tuple[str, ...]], parts: tuple[str, ...]) -> None:
    # Default macOS filesystems can equate canonically equivalent names;
    # case-insensitive Windows/macOS volumes also equate letter case.
    folded = tuple(unicodedata.normalize('NFD', part.casefold()) for part in parts)
    if any(folded[:len(other)] == other or other[:len(folded)] == folded for other in seen):
        raise SyncError('Повторяющиеся или вложенные пути файлов в пакете')
    seen.add(folded)


def secrets_pack(files: list[Path], shared: Path, machine: str, key: bytes, *, root: Path | None = None) -> Path:
    """Encrypt explicitly selected files; never infer or scan for secrets."""
    machine = _safe_machine(machine)
    if not files:
        raise SyncError('Не выбраны файлы')
    AESGCM, _ = _aead()
    items = []
    names: set[tuple[str, ...]] = set()
    root = root.resolve(strict=True) if root is not None else None
    total = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise SyncError('Секрет должен быть обычным файлом, не ссылкой')
        if root is None:
            name = path.name
        else:
            try:
                name = path.resolve(strict=True).relative_to(root).as_posix()
            except ValueError as exc:
                raise SyncError('Выбранный файл находится вне Nexus') from exc
        _add_secret_path(names, _secret_parts(name, nested=root is not None))
        before = path.stat()
        if before.st_size > MAX_SECRET_BYTES or total + before.st_size > MAX_SECRET_BYTES:
            raise SyncError('Суммарный размер секретов превышает 16 МиБ')
        body = path.read_bytes()
        after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino) or len(body) != before.st_size:
            raise SyncError('Файл изменился во время чтения; повторите попытку')
        total += len(body)
        items.append({'name': name, 'data': base64.b64encode(body).decode('ascii')})
    data = json.dumps({'version': 2 if root is not None else 1, 'files': items}, separators=(',', ':')).encode('utf-8')
    nonce = os.urandom(12)
    encrypted = SECRET_MAGIC + nonce + AESGCM(key).encrypt(nonce, data, SECRET_MAGIC)
    folder = shared.resolve(strict=True) / 'nexus-sync' / 'secrets' / machine
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (uuid.uuid4().hex + '.nxs')
    with path.open('xb') as output:
        output.write(encrypted)
    return path


def secret_fingerprint(files: list[Path], key: bytes, key_file: Path, shared: Path, nexus: Path) -> str:
    """Keyed local change detector; never persist secret bytes or plain hashes."""
    digest = hmac.new(key, b'nexus-sync-secret-fingerprint-v2\0', hashlib.sha256)
    total = 0
    names: set[tuple[str, ...]] = set()
    shared = shared.resolve(strict=True)
    nexus = nexus.resolve(strict=True)
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise SyncError('Выбранный файл недоступен или является ссылкой')
        resolved = path.resolve(strict=True)
        if resolved == shared or shared in resolved.parents or os.path.samefile(path, key_file):
            raise SyncError('Ключ и файлы из папки обмена нельзя включать в пакет')
        try:
            name = resolved.relative_to(nexus).as_posix()
        except ValueError as exc:
            raise SyncError('Выбранный файл находится вне Nexus') from exc
        _add_secret_path(names, _secret_parts(name, nested=True))
        before = path.stat()
        total += before.st_size
        if total > MAX_SECRET_BYTES:
            raise SyncError('Суммарный размер секретов превышает 16 МиБ')
        encoded_path = str(resolved).encode('utf-8')
        digest.update(len(encoded_path).to_bytes(8, 'big'))
        digest.update(encoded_path)
        digest.update(before.st_size.to_bytes(8, 'big'))
        size = 0
        with path.open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
        after = path.stat()
        if size != before.st_size or (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise SyncError('Файл изменился во время чтения')
    return digest.hexdigest()


def validate_secret_paths(files: list[Path], key_file: Path, shared: Path, nexus: Path) -> None:
    """Validate declared paths without opening or requiring the selected files."""
    names: set[tuple[str, ...]] = set()
    for path in files:
        if not path.is_absolute() or path.is_symlink():
            raise SyncError('Нужны абсолютные пути к обычным файлам внутри Nexus')
        resolved = path.resolve(strict=False)
        if resolved == shared or shared in resolved.parents or resolved == key_file:
            raise SyncError('Ключ и файлы из папки обмена нельзя включать в пакет')
        try:
            relative = resolved.relative_to(nexus).as_posix()
        except ValueError as exc:
            raise SyncError('Выбранный файл находится вне Nexus') from exc
        _add_secret_path(names, _secret_parts(relative, nested=True))
        if path.exists() and (not path.is_file() or os.path.samefile(path, key_file)):
            raise SyncError('Выбранный путь не является подходящим файлом')


def discover_secret_files(nexus: Path, excluded_roots: tuple[Path, ...]) -> list[Path]:
    """Find regular files matching the sample ignore rules, without reading them."""
    root = nexus.resolve(strict=True)
    excluded = {path.resolve(strict=True) for path in excluded_roots}
    if root in excluded:
        raise SyncError('Корень проектов пересекается с исключённой папкой')
    def matches(name):
        name = name.lower()
        return (name in {'.env', '.npmrc', '.pypirc', '.netrc', '.git-credentials', '.pgpass', '.htpasswd'}
                or name.startswith('.env.') or name.endswith(('.key', '.pem', '.p12')))
    def failed(error):
        raise SyncError('Не удалось завершить поиск выбранных файлов') from error
    found = []
    for current, directories, files in os.walk(root, followlinks=False, onerror=failed):
        folder = Path(current)
        kept = []
        for name in sorted(directories):
            path = folder / name
            if name in EXCLUDED:
                continue
            if path.is_symlink():
                if matches(name):
                    raise SyncError('Обнаруженный файл является ссылкой')
                continue
            if path.resolve(strict=True) in excluded:
                continue
            if matches(name):
                raise SyncError('Путь с именем выбранного файла является каталогом')
            kept.append(name)
        directories[:] = kept
        for name in sorted(files):
            if not matches(name):
                continue
            path = folder / name
            if path.is_symlink() or not path.is_file():
                raise SyncError('Обнаруженный файл недоступен или является ссылкой')
            found.append(path)
            if len(found) > 4096:
                raise SyncError('Слишком много обнаруженных файлов')
    return found


def publish_secrets_once(cfg: dict, key: bytes) -> bool:
    """Publish selected files; name-based discovery is explicitly opt-in."""
    def selected_files():
        files = list(cfg['secret_files'])
        if cfg.get('discover_secrets', False):
            discovered = discover_secret_files(cfg['nexus'], (cfg['shared'], cfg['codex_home']))
            known = {os.path.normcase(str(path.resolve(strict=False))) for path in files}
            files.extend(path for path in discovered if os.path.normcase(str(path.resolve(strict=False))) not in known)
        return sorted(files, key=lambda path: str(path))
    files = selected_files()
    if not files:
        return False
    AESGCM, InvalidTag = _aead()
    fingerprint = secret_fingerprint(files, key, cfg['key_file'], cfg['shared'], cfg['nexus'])
    marker = cfg['state_dir'] / f"last-secrets-{cfg['machine']}.json"
    try:
        previous = json.loads(marker.read_text(encoding='utf-8'))
        tag = previous.get('authentication')
        package = previous.get('package')
        if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(previous, key))
                and previous.get('fingerprint') == fingerprint
                and isinstance(package, str) and re.fullmatch(r'[0-9a-f]{32}\.nxs', package)):
            package_file = cfg['shared'] / 'nexus-sync' / 'secrets' / cfg['machine'] / package
            if package_file.is_symlink() or package_file.stat().st_size > 24 * 1024 * 1024:
                raise ValueError('Некорректный пакет')
            encrypted = package_file.read_bytes()
            if encrypted.startswith(SECRET_MAGIC) and len(encrypted) >= len(SECRET_MAGIC) + 28:
                offset = len(SECRET_MAGIC)
                AESGCM(key).decrypt(encrypted[offset:offset + 12], encrypted[offset + 12:], SECRET_MAGIC)
                return False
    except (OSError, ValueError, TypeError, AttributeError, InvalidTag):
        pass
    package_path = secrets_pack(files, cfg['shared'], cfg['machine'], key, root=cfg['nexus'])
    if selected_files() != files or secret_fingerprint(files, key, cfg['key_file'], cfg['shared'], cfg['nexus']) != fingerprint:
        raise SyncError('Файлы изменились во время подготовки пакета')
    data = {'format': 'nexus-secrets-marker-v1', 'fingerprint': fingerprint, 'package': package_path.name}
    data['authentication'] = _manifest_tag(data, key)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-secret-marker-', dir=cfg['state_dir'], delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True)
            handle.write('\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, marker)
    finally:
        temporary.unlink(missing_ok=True)
    return True


def secrets_extract(bundle: Path, destination: Path, key: bytes, shared: Path) -> int:
    """Decrypt into a newly created local quarantine, never existing files."""
    AESGCM, InvalidTag = _aead()
    if bundle.stat().st_size > 24 * 1024 * 1024:
        raise SyncError('Пакет секретов слишком велик')
    encoded = bundle.read_bytes()
    if not encoded.startswith(SECRET_MAGIC) or len(encoded) < len(SECRET_MAGIC) + 28:
        raise SyncError('Неизвестный формат пакета секретов')
    offset = len(SECRET_MAGIC)
    try:
        raw = AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], SECRET_MAGIC)
        data = json.loads(raw)
    except (InvalidTag, ValueError, UnicodeError) as exc:
        raise SyncError('Ключ не подошёл или пакет секретов повреждён') from exc
    if not isinstance(data, dict) or data.get('version') not in (1, 2) or not isinstance(data.get('files'), list):
        raise SyncError('Некорректный пакет секретов')
    files = []
    names: set[tuple[str, ...]] = set()
    total = 0
    for item in data['files']:
        if not isinstance(item, dict) or not isinstance(item.get('name'), str) or not isinstance(item.get('data'), str):
            raise SyncError('Некорректный пакет секретов')
        name = item['name']
        parts = _secret_parts(name, nested=data['version'] == 2)
        _add_secret_path(names, parts)
        try:
            body = base64.b64decode(item['data'], validate=True)
        except ValueError as exc:
            raise SyncError('Некорректный пакет секретов') from exc
        total += len(body)
        if total > MAX_SECRET_BYTES:
            raise SyncError('Пакет секретов слишком велик')
        files.append((parts, body))
    if not files:
        raise SyncError('Пустой пакет секретов')
    parent = destination.parent.resolve(strict=True)
    shared_root = shared.resolve(strict=True)
    if parent == shared_root or shared_root in parent.parents:
        raise SyncError('Секреты нельзя распаковывать в папку обмена')
    if '.codex' in {part.casefold() for part in (*parent.parts, destination.name)}:
        raise SyncError('Распаковка в каталог Codex запрещена')
    destination.mkdir(mode=0o700)
    for parts, body in files:
        file_path = destination.joinpath(*parts)
        file_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(body)
    return len(files)


def _secret_tree_tag(root: Path, key: bytes) -> str:
    """Authenticate a private quarantine without publishing plain hashes."""
    if root.is_symlink() or not root.is_dir():
        raise SyncError('Карантин секретов недоступен или является ссылкой')
    derived = hmac.new(key, b'nexus-secret-quarantine-v2', hashlib.sha256).digest()
    digest = hmac.new(derived, digestmod=hashlib.sha256)
    total = 0
    def failed(error):
        raise SyncError('Не удалось проверить карантин секретов') from error
    for current, directories, files in os.walk(root, followlinks=False, onerror=failed):
        folder = Path(current)
        directories.sort()
        for name in directories:
            path = folder / name
            if path.is_symlink() or not path.is_dir():
                raise SyncError('Недопустимый каталог в карантине секретов')
            relative = path.relative_to(root).as_posix().encode('utf-8')
            digest.update(b'd' + len(relative).to_bytes(8, 'big') + relative)
        for name in sorted(files):
            path = folder / name
            details = path.lstat()
            if not statmod.S_ISREG(details.st_mode):
                raise SyncError('Недопустимый файл в карантине секретов')
            if details.st_size > MAX_SECRET_BYTES - total:
                raise SyncError('Карантин секретов слишком велик')
            relative = path.relative_to(root).as_posix().encode('utf-8')
            digest.update(b'f' + len(relative).to_bytes(8, 'big') + relative)
            digest.update(details.st_size.to_bytes(8, 'big'))
            read = 0
            with path.open('rb') as stream:
                while chunk := stream.read(1024 * 1024):
                    read += len(chunk)
                    if read > details.st_size:
                        raise SyncError('Файл карантина изменился во время проверки')
                    digest.update(chunk)
            after = path.lstat()
            if (read != details.st_size or (details.st_size, details.st_mtime_ns, details.st_ctime_ns, details.st_ino) !=
                    (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)):
                raise SyncError('Файл карантина изменился во время проверки')
            total += read
    return digest.hexdigest()


def prepare_received_secrets(cfg: dict, key: bytes) -> int:
    """Opt-in preparation of peer packages into private, separate quarantine."""
    if cfg['direction'] != 'send-receive' or not cfg['prepare_secrets']:
        return 0
    shared = cfg['shared'] / 'nexus-sync' / 'secrets'
    if shared.is_symlink() or not shared.is_dir():
        return 0
    base = cfg['state_dir'] / 'prepared-secrets'
    if base.is_symlink():
        raise SyncError('Символьные ссылки в карантине запрещены')
    base.mkdir(mode=0o700, exist_ok=True)
    count = 0
    failures = 0
    def prepare_package(package: Path, parent: Path, peer: str) -> bool:
        destination = parent / package.stem
        if package.stat().st_size > 24 * 1024 * 1024:
            raise SyncError('Пакет секретов слишком велик')
        encoded = package.read_bytes()
        if not encoded.startswith(SECRET_MAGIC) or len(encoded) < len(SECRET_MAGIC) + 28:
            raise SyncError('Некорректный пакет')
        AESGCM, InvalidTag = _aead()
        offset = len(SECRET_MAGIC)
        try:
            data = json.loads(AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], SECRET_MAGIC))
        except (InvalidTag, ValueError, UnicodeError) as exc:
            raise SyncError('Некорректный пакет') from exc
        if not isinstance(data, dict) or type(data.get('version')) is not int or data['version'] not in (1, 2):
            raise SyncError('Некорректный пакет')
        if data['version'] == 1:  # Git and manual flat packages share this folder.
            return False
        package_hash = hashlib.sha256(encoded).hexdigest()
        if destination.exists() or destination.is_symlink():
            if destination.is_symlink() or (destination / 'prepared.json').is_symlink() or (destination / 'files').is_symlink():
                raise SyncError('Символьные ссылки в карантине запрещены')
            metadata = json.loads((destination / 'prepared.json').read_text(encoding='utf-8'))
            tag = metadata.get('authentication') if isinstance(metadata, dict) else None
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(metadata, key))
                    or metadata.get('format') != 'nexus-prepared-secrets-v2'
                    or metadata.get('package_hash') != package_hash or metadata.get('peer') != peer
                    or metadata.get('package') != package.name
                    or metadata.get('files_tag') != _secret_tree_tag(destination / 'files', key)):
                raise SyncError('Подготовленный пакет изменился или неполон')
            return False
        with tempfile.TemporaryDirectory(prefix='.nexus-secret-receive-', dir=cfg['state_dir']) as tmp:
            prepared = Path(tmp) / 'files'
            secrets_extract(package, prepared, key, cfg['shared'])
            if _digest(package) != package_hash:
                raise SyncError('Пакет изменился во время подготовки')
            metadata = {'format': 'nexus-prepared-secrets-v2', 'peer': peer,
                        'package': package.name, 'package_hash': package_hash,
                        'files_tag': _secret_tree_tag(prepared, key)}
            metadata['authentication'] = _manifest_tag(metadata, key)
            destination.mkdir(mode=0o700)
            os.replace(prepared, destination / 'files')
            with (destination / 'prepared.json').open('x', encoding='utf-8') as marker:
                json.dump(metadata, marker, ensure_ascii=False, sort_keys=True)
                marker.write('\n')
        return True

    for peer in sorted(cfg['peer_roots']):
        source = shared / peer
        if source.is_symlink() or not source.is_dir():
            continue
        parent = base / peer
        if parent.is_symlink():
            raise SyncError('Символьные ссылки в карантине запрещены')
        parent.mkdir(mode=0o700, exist_ok=True)
        for package in sorted(source.glob('*.nxs')):
            if package.is_symlink() or not re.fullmatch(r'[0-9a-f]{32}\.nxs', package.name):
                continue
            try:
                count += int(prepare_package(package, parent, peer))
            except (SyncError, OSError, ValueError, TypeError):
                failures += 1
    if failures:
        raise SyncError(f'Не подготовлено пакетов: {failures}')
    return count


def plan_prepared_secrets(cfg: dict, key: bytes, peer: str, identity: str) -> dict:
    """Compare an authenticated quarantine with a workspace, without writing either."""
    _safe_machine(peer)
    if cfg['direction'] != 'send-receive' or peer not in cfg['peer_roots']:
        raise SyncError('Получение от этого устройства не настроено')
    if not re.fullmatch(r'[0-9a-f]{32}', identity):
        raise SyncError('Некорректный идентификатор пакета')
    quarantine_base = cfg['state_dir'] / 'prepared-secrets'
    entry = quarantine_base / peer / identity
    package_base = cfg['shared'] / 'nexus-sync' / 'secrets'
    package = package_base / peer / (identity + '.nxs')
    marker, files = entry / 'prepared.json', entry / 'files'
    for path in (quarantine_base, quarantine_base / peer, entry, marker, files,
                 cfg['shared'] / 'nexus-sync', package_base, package_base / peer, package):
        if path.is_symlink():
            raise SyncError('Символьная ссылка в пути подготовленного пакета запрещена')
    if not marker.is_file() or marker.stat().st_size > 65536 or not package.is_file() or package.stat().st_size > 24 * 1024 * 1024:
        raise SyncError('Подготовленный пакет или его отметка недоступны')
    metadata = json.loads(marker.read_text(encoding='utf-8'))
    tag = metadata.get('authentication') if isinstance(metadata, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(metadata, key))
            or metadata.get('format') != 'nexus-prepared-secrets-v2' or metadata.get('peer') != peer
            or metadata.get('package') != package.name or metadata.get('package_hash') != _digest(package)
            or metadata.get('files_tag') != _secret_tree_tag(files, key)):
        raise SyncError('Подготовленный пакет не прошёл проверку')

    def target_state(parts: tuple[str, ...], source: Path) -> str:
        current = cfg['nexus']
        try:
            for index, name in enumerate(parts):
                if current.is_symlink() or not current.is_dir():
                    return 'UNSAFE_PATH'
                folded = unicodedata.normalize('NFD', name.casefold())
                matching = [entry.name for entry in current.iterdir()
                            if unicodedata.normalize('NFD', entry.name.casefold()) == folded]
                if any(existing != name for existing in matching):
                    return 'AMBIGUOUS_NAME'
                if not matching:
                    return 'MISSING'
                current = current / name
                details = current.lstat()
                if statmod.S_ISLNK(details.st_mode):
                    return 'UNSAFE_PATH'
                if index < len(parts) - 1 and not statmod.S_ISDIR(details.st_mode):
                    return 'UNSAFE_PATH'
                if index == len(parts) - 1 and not statmod.S_ISREG(details.st_mode):
                    return 'UNSAFE_PATH'
            before_local, before_source = current.lstat(), source.lstat()
            if not statmod.S_ISREG(before_local.st_mode) or not statmod.S_ISREG(before_source.st_mode):
                return 'UNSAFE_PATH'
            equal = before_local.st_size == before_source.st_size
            if equal:
                flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
                with os.fdopen(os.open(current, flags), 'rb') as local:
                    with os.fdopen(os.open(source, flags), 'rb') as incoming:
                        local_fd, source_fd = os.fstat(local.fileno()), os.fstat(incoming.fileno())
                        if ((local_fd.st_dev, local_fd.st_ino) != (before_local.st_dev, before_local.st_ino)
                                or (source_fd.st_dev, source_fd.st_ino) != (before_source.st_dev, before_source.st_ino)):
                            return 'UNSTABLE'
                        while True:
                            left, right = local.read(1024 * 1024), incoming.read(1024 * 1024)
                            if left != right:
                                equal = False
                                break
                            if not left:
                                break
            after_local, after_source = current.lstat(), source.lstat()
            stamp = lambda item: (item.st_size, item.st_mtime_ns, item.st_ctime_ns, item.st_dev, item.st_ino, item.st_mode)
            if stamp(before_local) != stamp(after_local) or stamp(before_source) != stamp(after_source):
                return 'UNSTABLE'
            return 'SAME' if equal else 'CONFLICT'
        except OSError:
            return 'UNAVAILABLE'

    results = []
    seen: set[tuple[str, ...]] = set()
    def failed(error):
        raise SyncError('Не удалось проверить карантин') from error
    for current, directories, names in os.walk(files, followlinks=False, onerror=failed):
        directories.sort()
        for name in sorted(names):
            path = Path(current) / name
            relative = path.relative_to(files).as_posix()
            parts = _secret_parts(relative, nested=True)
            _add_secret_path(seen, parts)
            results.append({'path': relative, 'state': target_state(parts, path)})
    if not results or metadata.get('files_tag') != _secret_tree_tag(files, key) or metadata.get('package_hash') != _digest(package):
        raise SyncError('Пакет изменился во время сравнения')
    return {'format': 'nexus-secret-plan-v1', 'source': peer, 'package': identity,
            'files': results, 'applied': False, 'note': 'Результат может устареть; автоматическое применение не выполняется'}


def list_encrypted_packages(cfg: dict, key: bytes) -> list[dict]:
    """Identify authenticated packages without outputting their file data."""
    root = cfg['shared'] / 'nexus-sync' / 'secrets'
    if root.is_symlink():
        raise SyncError('Каталог пакетов является ссылкой')
    if not root.is_dir():
        return []
    AESGCM, InvalidTag = _aead()
    result = []
    for peer in sorted({cfg['machine'], *cfg['peer_roots']}):
        folder = root / peer
        if folder.is_symlink() or not folder.is_dir():
            continue
        for package in sorted(folder.glob('*.nxs')):
            if package.is_symlink() or not re.fullmatch(r'[0-9a-f]{32}\.nxs', package.name):
                continue
            row = {'source': peer, 'id': package.stem, 'type': 'unverified'}
            try:
                if package.stat().st_size > 24 * 1024 * 1024:
                    raise SyncError('Пакет слишком велик')
                encoded = package.read_bytes()
                if not encoded.startswith(SECRET_MAGIC) or len(encoded) < len(SECRET_MAGIC) + 28:
                    raise SyncError('Некорректный пакет')
                offset = len(SECRET_MAGIC)
                payload = json.loads(AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], SECRET_MAGIC))
                if (not isinstance(payload, dict) or type(payload.get('version')) is not int
                        or payload['version'] not in (1, 2) or not isinstance(payload.get('files'), list)):
                    raise SyncError('Некорректный пакет')
                row['type'] = 'selected-files' if payload['version'] == 2 else 'flat-files'
                items = payload['files']
                if payload['version'] == 1 and len(items) == 3 and all(isinstance(item, dict) for item in items):
                    names = [item.get('name') for item in items]
                    if sorted(str(name) for name in names) == ['repository.bundle', 'repository.json', 'staged.patch']:
                        entry = items[names.index('repository.json')]
                        encoded_info = entry.get('data')
                        if not isinstance(encoded_info, str) or len(encoded_info) > 65536:
                            raise SyncError('Некорректные метаданные Git')
                        info = json.loads(base64.b64decode(encoded_info, validate=True))
                        if not isinstance(info, dict) or info.get('format') != 'nexus-git-package-v1':
                            raise SyncError('Некорректные метаданные Git')
                        project = info.get('project')
                        if project is not None:
                            if not isinstance(project, str):
                                raise SyncError('Некорректный путь проекта')
                            if project != '.':
                                _secret_parts(project, nested=True)
                        row.update({'type': 'git', 'project': project})
            except (SyncError, OSError, ValueError, UnicodeError, TypeError, InvalidTag):
                row = {'source': peer, 'id': package.stem, 'type': 'unverified'}
            result.append(row)
    return result


def git_pack(repo: Path, shared: Path, machine: str, key: bytes, *, nexus: Path | None = None) -> Path:
    """Encrypt a Git bundle and a staged patch; never change the source repo."""
    root = repo.resolve(strict=True)
    if not root.is_dir():
        raise SyncError('Путь проекта должен быть каталогом')
    project = None
    if nexus is not None:
        try:
            project = root.relative_to(nexus.resolve(strict=True)).as_posix()
        except ValueError as exc:
            raise SyncError('Репозиторий находится вне Nexus') from exc
    try:
        with tempfile.TemporaryDirectory(prefix='nexus-git-') as tmp:
            temporary = Path(tmp)
            bundle = temporary / 'repository.bundle'
            staged = temporary / 'staged.patch'
            staged_check = temporary / 'staged-check.patch'
            info = temporary / 'repository.json'
            base = ['git', '-C', str(root), '-c', 'maintenance.auto=false', '-c', 'gc.auto=0', '-c', 'core.fsmonitor=false']
            env = {**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}
            def git(*args, output=None, allow_detached=False):
                result = subprocess.run(base + list(args), stdin=subprocess.DEVNULL,
                    stdout=output if output is not None else subprocess.PIPE, stderr=subprocess.PIPE,
                    check=False, env=env)
                if result.returncode and not (allow_detached and result.returncode == 1):
                    raise SyncError('Git не смог подготовить состояние репозитория')
                return result.stdout
            top = Path(os.fsdecode(git('rev-parse', '--show-toplevel').rstrip(b'\n'))).resolve(strict=True)
            if top != root:
                raise SyncError('Укажите корневой каталог Git-репозитория')
            lock_path = Path(os.fsdecode(git('rev-parse', '--git-path', 'index.lock').rstrip(b'\n')))
            if not lock_path.is_absolute():
                lock_path = root / lock_path
            if lock_path.exists():
                raise SyncError('Git-индекс занят; повторите позже')
            if git('ls-files', '--unmerged', '-z'):
                raise SyncError('В индексе есть неразрешённые конфликты')
            head = git('rev-parse', '--verify', 'HEAD').decode('ascii').strip()
            head_ref = git('symbolic-ref', '-q', 'HEAD', allow_detached=True).decode('utf-8').strip() or None
            refs_before = git('show-ref', '--head')
            remotes_before = git('config', '--local', '--null', '--get-regexp',
                                 r'^remote\..*\.(url|pushurl|fetch)$', allow_detached=True)
            remotes = _parse_git_remotes(remotes_before)
            branches_before = git('config', '--local', '--null', '--get-regexp',
                                  r'^branch\..*\.(remote|merge)$', allow_detached=True)
            branches = _parse_git_branches(branches_before)
            diff_args = ('diff', '--cached', '--binary', '--full-index', '--no-color', '--no-ext-diff', '--no-textconv',
                         '--no-renames', '--src-prefix=a/', '--dst-prefix=b/')
            with staged.open('xb') as output:
                git(*diff_args, output=output)
            if staged.stat().st_size > MAX_SECRET_BYTES:
                raise SyncError('Подготовленные изменения превышают 16 МиБ')
            git('bundle', 'create', str(bundle), '--all', 'HEAD')
            with staged_check.open('xb') as output:
                git(*diff_args, output=output)
            if staged_check.stat().st_size > MAX_SECRET_BYTES or _digest(staged) != _digest(staged_check):
                raise SyncError('Git-индекс изменился во время подготовки')
            if (lock_path.exists() or git('rev-parse', '--verify', 'HEAD').decode('ascii').strip() != head
                    or (git('symbolic-ref', '-q', 'HEAD', allow_detached=True).decode('utf-8').strip() or None) != head_ref
                    or git('show-ref', '--head') != refs_before or git('ls-files', '--unmerged', '-z')
                    or git('config', '--local', '--null', '--get-regexp',
                           r'^remote\..*\.(url|pushurl|fetch)$', allow_detached=True) != remotes_before
                    or git('config', '--local', '--null', '--get-regexp',
                           r'^branch\..*\.(remote|merge)$', allow_detached=True) != branches_before):
                raise SyncError('Git-ссылки или индекс изменились во время подготовки')
            _write_json(info, {'format': 'nexus-git-package-v1', 'head': head,
                               'head_ref': head_ref, 'remotes': remotes, 'branches': branches,
                               'staged_patch': 'staged.patch', 'working_tree_included': False,
                               'index_flags_included': False, 'project': project})
            # The generic encrypted envelope protects branch names, patch and content.
            return secrets_pack([bundle, staged, info], shared, machine, key)
    except FileNotFoundError as exc:
        raise SyncError('Git не найден') from exc


def _parse_git_remotes(raw: bytes) -> list[dict[str, str]]:
    if len(raw) > 65536:
        raise SyncError('Настройки Git remote слишком велики')
    result = []
    for entry in raw.split(b'\0'):
        if not entry:
            continue
        try:
            name, value = entry.decode('utf-8').split('\n', 1)
        except (UnicodeError, ValueError) as exc:
            raise SyncError('Неподдерживаемые настройки Git remote') from exc
        match = re.fullmatch(r'remote\.([A-Za-z0-9_.-]+)\.(url|pushurl|fetch)', name)
        if not match or not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise SyncError('Неподдерживаемые настройки Git remote')
        result.append({'name': match.group(1), 'field': match.group(2), 'value': value})
    return result


def _parse_git_branches(raw: bytes) -> list[dict[str, str]]:
    if len(raw) > 65536:
        raise SyncError('Настройки веток Git слишком велики')
    result = []
    for entry in raw.split(b'\0'):
        if not entry:
            continue
        try:
            name, value = entry.decode('utf-8').split('\n', 1)
        except (UnicodeError, ValueError) as exc:
            raise SyncError('Неподдерживаемые настройки веток Git') from exc
        match = re.fullmatch(r'branch\.([A-Za-z0-9_./-]+)\.(remote|merge)', name)
        if not match or not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise SyncError('Неподдерживаемые настройки веток Git')
        result.append({'name': match.group(1), 'field': match.group(2), 'value': value})
    return result


def git_preview(bundle: Path, destination: Path, cfg: dict, key: bytes) -> None:
    """Create a disposable Git preview outside every configured workspace."""
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if destination.exists() or destination.is_symlink():
        raise SyncError('Каталог результата уже существует')
    for root in (cfg['nexus'], cfg['codex_home'], cfg['shared'], cfg['state_dir']):
        if parent == root or root in parent.parents:
            raise SyncError('Тестовый репозиторий должен быть вне рабочих и общих папок')
    if '.codex' in {part.casefold() for part in destination.parts}:
        raise SyncError('Тестовый репозиторий нельзя создавать в .codex')
    with tempfile.TemporaryDirectory(prefix='.nexus-git-preview-', dir=parent) as tmp:
        stage = Path(tmp)
        contents = stage / 'package'
        secrets_extract(bundle, contents, key, cfg['shared'])
        if {p.name for p in contents.iterdir()} != {'repository.bundle', 'staged.patch', 'repository.json'}:
            raise SyncError('Это не поддерживаемый Git-пакет')
        metadata = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
        if (not isinstance(metadata, dict) or metadata.get('format') != 'nexus-git-package-v1'
                or not isinstance(metadata.get('head'), str)
                or not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', metadata['head'])
                or 'head_ref' not in metadata or metadata.get('staged_patch') != 'staged.patch'):
            raise SyncError('Некорректные или устаревшие метаданные Git-пакета')
        ref = metadata['head_ref']
        remotes = metadata.get('remotes', [])
        if not isinstance(remotes, list) or len(remotes) > 1000:
            raise SyncError('Некорректные настройки Git remote')
        for item in remotes:
            if (not isinstance(item, dict) or set(item) != {'name', 'field', 'value'}
                    or not isinstance(item['name'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', item['name'])
                    or item['field'] not in ('url', 'pushurl', 'fetch')
                    or not isinstance(item['value'], str) or not item['value']
                    or len(item['value'].encode('utf-8')) > 65536
                    or any(ord(char) < 32 or ord(char) == 127 for char in item['value'])):
                raise SyncError('Некорректные настройки Git remote')
        branches = metadata.get('branches', [])
        if not isinstance(branches, list) or len(branches) > 1000:
            raise SyncError('Некорректные настройки веток Git')
        for item in branches:
            if (not isinstance(item, dict) or set(item) != {'name', 'field', 'value'}
                    or not isinstance(item['name'], str) or not re.fullmatch(r'[A-Za-z0-9_./-]+', item['name'])
                    or item['field'] not in ('remote', 'merge')
                    or not isinstance(item['value'], str) or not item['value']
                    or len(item['value'].encode('utf-8')) > 65536
                    or any(ord(char) < 32 or ord(char) == 127 for char in item['value'])):
                raise SyncError('Некорректные настройки веток Git')
        if ref is not None and (not isinstance(ref, str) or not ref.startswith('refs/heads/')
                                or not ref[len('refs/heads/'):] or ref[len('refs/heads/'):].startswith('-')):
            raise SyncError('Некорректная ветка Git-пакета')
        template = stage / 'empty-template'
        template.mkdir()
        env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
        env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull,
                    'GIT_TEMPLATE_DIR': str(template), 'GIT_TERMINAL_PROMPT': '0',
                    'GIT_ALLOW_PROTOCOL': 'file', 'GIT_ATTR_NOSYSTEM': '1'})
        def git(*args):
            try:
                result = subprocess.run(['git', '-c', 'core.hooksPath=' + str(template),
                                         '-c', 'core.fsmonitor=false', *args], stdin=subprocess.DEVNULL,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=120)
            except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
                raise SyncError('Git недоступен или не завершил проверку') from exc
            if result.returncode:
                raise SyncError('Не удалось создать тестовый Git-репозиторий')
            return result.stdout
        if ref is not None:
            git('check-ref-format', ref)
        heads = git('bundle', 'list-heads', str(contents / 'repository.bundle')).splitlines()
        expected_head = (metadata['head'] + ' ' + (ref or 'HEAD')).encode('utf-8')
        if expected_head not in heads:
            raise SyncError('Ветка или HEAD не соответствует Git-пакету')
        repository = stage / 'repository'
        git('clone', '--no-checkout', '--', str(contents / 'repository.bundle'), str(repository))
        for line in git('-C', str(repository), 'for-each-ref', '--format=%(refname)', 'refs/remotes/').splitlines():
            ref_name = line.decode('utf-8')
            git('check-ref-format', ref_name)
            git('-C', str(repository), 'update-ref', '-d', ref_name)
        preserved_refs = []
        for line in heads:
            parts = line.decode('utf-8').split(' ', 1)
            if len(parts) == 2 and parts[1].startswith(('refs/remotes/', 'refs/heads/')):
                git('check-ref-format', parts[1])
                preserved_refs.append('+' + parts[1] + ':' + parts[1])
        if preserved_refs:
            git('-C', str(repository), 'fetch', '--no-tags', '--no-write-fetch-head', '--no-recurse-submodules',
                '--update-head-ok', str(contents / 'repository.bundle'), *preserved_refs)
        if ref is None:
            git('-C', str(repository), 'checkout', '--detach', metadata['head'], '--')
        else:
            git('-C', str(repository), 'checkout', '-B', ref[len('refs/heads/'):], metadata['head'], '--')
        patch = contents / 'staged.patch'
        if patch.stat().st_size:
            git('-C', str(repository), 'apply', '--cached', '--binary', '--', str(patch))
        # A bundle clone's origin points to the private temporary directory;
        # avoid leaving an unusable remote while retaining remote-tracking refs.
        git('-C', str(repository), 'config', '--unset-all', 'remote.origin.url')
        git('-C', str(repository), 'config', '--unset-all', 'remote.origin.fetch')
        config_path = repository / '.git' / 'config'
        with config_path.open('a', encoding='utf-8') as config:
            for item in remotes:
                value = item['value'].replace('\\', '\\\\').replace('"', '\\"')
                config.write(f'\n[remote "{item["name"]}"]\n\t{item["field"]} = "{value}"\n')
            for item in branches:
                value = item['value'].replace('\\', '\\\\').replace('"', '\\"')
                config.write(f'\n[branch "{item["name"]}"]\n\t{item["field"]} = "{value}"\n')
        destination.mkdir(mode=0o700)
        for child in repository.iterdir():
            os.replace(child, destination / child.name)


def git_fingerprint(repo: Path, nexus: Path, key: bytes) -> tuple[str, str]:
    """Keyed fingerprint of refs, HEAD and index; excludes working-tree edits."""
    if repo.is_symlink():
        raise SyncError('Репозиторий не может быть символьной ссылкой')
    root = repo.resolve(strict=True)
    try:
        project = root.relative_to(nexus.resolve(strict=True)).as_posix()
    except ValueError as exc:
        raise SyncError('Репозиторий находится вне Nexus') from exc
    base = ['git', '-C', str(root), '-c', 'core.fsmonitor=false']
    env = {**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}
    def git(*args, detached_ok=False):
        result = subprocess.run(base + list(args), stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, env=env)
        if result.returncode and not (detached_ok and result.returncode == 1):
            raise SyncError('Git не смог проверить состояние репозитория')
        return result.stdout
    try:
        top = Path(os.fsdecode(git('rev-parse', '--show-toplevel').rstrip(b'\n'))).resolve(strict=True)
        if top != root:
            raise SyncError('Укажите корень Git-репозитория')
        index = Path(os.fsdecode(git('rev-parse', '--git-path', 'index').rstrip(b'\n')))
        if not index.is_absolute():
            index = root / index
        if Path(str(index) + '.lock').exists() or index.is_symlink() or not index.is_file():
            raise SyncError('Git-индекс недоступен или занят')
        fields = [project.encode('utf-8'), git('show-ref', '--head'), git('symbolic-ref', '-q', 'HEAD', detached_ok=True),
                  git('config', '--local', '--null', '--get-regexp', r'^remote\..*\.(url|pushurl|fetch)$', detached_ok=True),
                  git('config', '--local', '--null', '--get-regexp', r'^branch\..*\.(remote|merge)$', detached_ok=True)]
        digest = hmac.new(key, b'nexus-git-fingerprint-v1\0', hashlib.sha256)
        for field in fields:
            digest.update(len(field).to_bytes(8, 'big'))
            digest.update(field)
        before = index.stat()
        with index.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        after = index.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise SyncError('Git-индекс изменился во время проверки')
        return project, digest.hexdigest()
    except FileNotFoundError as exc:
        raise SyncError('Git или индекс репозитория недоступен') from exc


def publish_git_once(cfg: dict, repo: Path, key: bytes) -> bool:
    project, fingerprint = git_fingerprint(repo, cfg['nexus'], key)
    identity = hmac.new(key, b'nexus-git-project-v1\0' + project.encode('utf-8'), hashlib.sha256).hexdigest()
    marker = cfg['state_dir'] / f"last-git-{cfg['machine']}-{identity}.json"
    AESGCM, InvalidTag = _aead()
    try:
        previous = json.loads(marker.read_text(encoding='utf-8'))
        tag, package = previous.get('authentication'), previous.get('package')
        if (isinstance(tag, str) and hmac.compare_digest(tag, _manifest_tag(previous, key))
                and previous.get('fingerprint') == fingerprint and isinstance(package, str)
                and re.fullmatch(r'[0-9a-f]{32}\.nxs', package)):
            file = cfg['shared'] / 'nexus-sync' / 'secrets' / cfg['machine'] / package
            if file.is_symlink() or file.stat().st_size > 24 * 1024 * 1024:
                raise ValueError('Некорректный пакет')
            encoded = file.read_bytes()
            if encoded.startswith(SECRET_MAGIC) and len(encoded) >= len(SECRET_MAGIC) + 28:
                offset = len(SECRET_MAGIC)
                AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], SECRET_MAGIC)
                return False
    except (OSError, ValueError, TypeError, AttributeError, InvalidTag):
        pass
    package = git_pack(repo, cfg['shared'], cfg['machine'], key, nexus=cfg['nexus'])
    if git_fingerprint(repo, cfg['nexus'], key) != (project, fingerprint):
        raise SyncError('Git-состояние изменилось во время упаковки')
    data = {'format': 'nexus-git-marker-v1', 'fingerprint': fingerprint, 'package': package.name}
    data['authentication'] = _manifest_tag(data, key)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-git-marker-', dir=cfg['state_dir'], delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True)
            handle.write('\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, marker)
    finally:
        temporary.unlink(missing_ok=True)
    return True


def git_status(repo: Path) -> dict:
    try:
        result = subprocess.run(
            ['git', '-C', str(repo.resolve(strict=True)), '-c', 'core.fsmonitor=false',
             'status', '--porcelain=v2', '--branch', '--untracked-files=normal'],
            capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL,
            env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0'},
        )
    except FileNotFoundError as exc:
        raise SyncError('Git или папка проекта не найдены') from exc
    if result.returncode:
        raise SyncError('Git не смог проверить репозиторий')
    status = {'staged': 0, 'modified': 0, 'untracked': 0, 'conflicted': 0, 'ahead': None, 'behind': None}
    for line in result.stdout.splitlines():
        if line.startswith('# branch.ab '):
            match = re.fullmatch(r'# branch\.ab \+(\d+) -(\d+)', line)
            if match:
                status['ahead'], status['behind'] = int(match.group(1)), int(match.group(2))
        elif line.startswith(('1 ', '2 ')) and len(line) >= 4:
            status['staged'] += line[2] != '.'
            status['modified'] += line[3] != '.'
        elif line.startswith('u '):
            status['conflicted'] += 1
        elif line.startswith('? '):
            status['untracked'] += 1
    return status


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
            or not 28 <= info['size'] <= MAX_SECRET_BYTES + 28
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


def _wheel_tags(metadata_dir: Path) -> list[str] | None:
    """Read bounded wheel tags without importing code; missing or malformed means unknown."""
    wheel = metadata_dir / 'WHEEL'
    try:
        if (metadata_dir.is_symlink() or getattr(metadata_dir, 'is_junction', lambda: False)()
                or wheel.is_symlink() or getattr(wheel, 'is_junction', lambda: False)()):
            return None
        before = wheel.lstat()
        if not statmod.S_ISREG(before.st_mode) or before.st_size > 8192:
            return None
        fd = os.open(wheel, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
                     | getattr(os, 'O_BINARY', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                return None
            data = stream.read(8193)
            after = os.fstat(stream.fileno())
        if (len(data) > 8192 or (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns, opened.st_ino)
                != (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)):
            return None
        tags = []
        for line in data.splitlines():
            if line.lower().startswith(b'tag:'):
                tag = line[4:].strip().decode('ascii').lower()
                if len(tag) > 128 or not re.fullmatch(r'[a-z0-9_.]+-[a-z0-9_.]+-[a-z0-9_.]+', tag):
                    return None
                tags.append(tag)
                if len(tags) > 32:
                    return None
        return sorted(set(tags)) if tags else None
    except (OSError, ValueError, UnicodeError):
        return None


def _python_env_packages(project: Path, *, wheels: dict | None = None) -> dict[str, list[str]]:
    """Observe dist-info directory names without executing or importing project code."""
    observed = {}
    wheel_observations = {}
    def linked(path):
        return path.is_symlink() or getattr(path, 'is_junction', lambda: False)()
    for env_name in ('.venv', 'venv'):
        env = project / env_name
        if linked(env) or not env.is_dir():
            continue
        try:
            candidates = []
            lib = env / 'lib'
            if lib.is_dir() and not linked(lib):
                candidates.extend(child / 'site-packages' for child in lib.iterdir()
                                  if re.fullmatch(r'python\d+\.\d+', child.name)
                                  and child.is_dir() and not linked(child))
            windows_lib = env / 'Lib'
            if windows_lib.is_dir() and not linked(windows_lib):
                candidates.append(windows_lib / 'site-packages')
            sites = [site for site in candidates if site.is_dir() and not linked(site)]
            if not sites:
                continue  # Missing or unsupported layout is not evidence of zero packages.
            environment_sites = {}
            for site in sites:
                names = set()
                site_wheels = {}
                before = site.stat()
                for entry in site.iterdir():
                    if entry.name.endswith('.dist-info'):
                        if linked(entry):
                            raise OSError('linked distribution metadata')
                        if entry.is_dir():
                            name = entry.name[:-len('.dist-info')]
                            names.add(name)
                            site_wheels[name] = _wheel_tags(entry)
                        if len(names) > 10000:
                            raise OSError('too many distributions')
                after = site.stat()
                if (before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (after.st_mtime_ns, after.st_ctime_ns, after.st_ino):
                    raise OSError('distribution metadata changed')
                relative_site = env_name + '/' + site.relative_to(env).as_posix()
                environment_sites[relative_site] = sorted(names)
                wheel_observations[relative_site] = site_wheels
            observed.update(environment_sites)
        except OSError:
            continue  # A partial scan must not be interpreted as package removals.
    if wheels is not None:
        wheels['value'] = {site: wheel_observations[site] for site in observed if site in wheel_observations}
    return observed


def _node_packages(project: Path, *, constraints: dict | None = None) -> list[str] | None:
    """Observe top-level npm package names and versions; incomplete scans are unknown."""
    modules = project / 'node_modules'
    if modules.is_symlink() or getattr(modules, 'is_junction', lambda: False)() or not modules.is_dir():
        return None
    try:
        modules_root = modules.resolve(strict=True)
    except OSError:
        return None
    found = []
    restrictions = {}
    restrictions_valid = True
    component = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')
    version = re.compile(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?')
    def scan(folder: Path, scope: str = '') -> None:
        nonlocal restrictions_valid
        before = folder.stat()
        for entry in sorted(folder.iterdir()):
            if entry.name.startswith('.'):
                continue
            if getattr(entry, 'is_junction', lambda: False)():
                raise OSError('junction package')
            link_before = entry.lstat() if entry.is_symlink() else None
            if link_before is not None:
                if not scope and entry.name.startswith('@'):
                    raise OSError('linked package scope')
                resolved = entry.resolve(strict=True)
                try:
                    parts = resolved.relative_to(modules_root).parts
                except ValueError as exc:
                    raise OSError('package link leaves node_modules') from exc
                expected_tail = tuple((scope + entry.name).split('/'))
                if (len(parts) != 3 + len(expected_tail) or parts[0] != '.pnpm'
                        or parts[2] != 'node_modules' or parts[3:] != expected_tail):
                    raise OSError('unsupported package link')
                package_dir = resolved
            else:
                package_dir = entry
            if not entry.is_dir():
                continue
            if not scope and entry.name.startswith('@'):
                if not component.fullmatch(entry.name[1:]):
                    raise OSError('unsupported package scope')
                scan(entry, entry.name + '/')
                continue
            if not component.fullmatch(entry.name):
                raise OSError('unsupported package name')
            metadata = package_dir / 'package.json'
            if metadata.is_symlink() or not metadata.is_file():
                raise OSError('package metadata unavailable')
            first = metadata.stat()
            if first.st_size > 128 * 1024:
                raise OSError('package metadata too large')
            fd = os.open(metadata, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            with os.fdopen(fd, 'rb') as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino) != (first.st_dev, first.st_ino):
                    raise OSError('package metadata changed')
                data = json.loads(stream.read(128 * 1024 + 1))
            last = metadata.stat()
            if ((first.st_size, first.st_mtime_ns, first.st_ctime_ns, first.st_ino) !=
                    (last.st_size, last.st_mtime_ns, last.st_ctime_ns, last.st_ino)):
                raise OSError('package metadata changed')
            if link_before is not None:
                link_after = entry.lstat()
                if ((link_before.st_mtime_ns, link_before.st_ctime_ns, link_before.st_ino) !=
                        (link_after.st_mtime_ns, link_after.st_ctime_ns, link_after.st_ino)
                        or entry.resolve(strict=True) != package_dir):
                    raise OSError('package link changed')
            name = scope + entry.name
            if (not isinstance(data, dict) or data.get('name') != name or not isinstance(data.get('version'), str)
                    or not version.fullmatch(data['version']) or len(data['version']) > 128):
                raise OSError('unsupported package metadata')
            found.append(name + '@' + data['version'])
            declared = {}
            for field in ('os', 'cpu'):
                if field not in data:
                    continue
                values = data[field]
                if (not isinstance(values, list) or len(values) > 32 or
                        any(not isinstance(value, str) or not re.fullmatch(r'!?[a-z0-9][a-z0-9_.-]{0,31}', value)
                            for value in values)):
                    restrictions_valid = False
                    continue
                if values:
                    declared[field] = sorted(set(values))
            if declared:
                restrictions[name + '@' + data['version']] = declared
            if len(found) > 10000:
                raise OSError('too many packages')
        after = folder.stat()
        if ((before.st_mtime_ns, before.st_ctime_ns, before.st_ino) !=
                (after.st_mtime_ns, after.st_ctime_ns, after.st_ino)):
            raise OSError('package directory changed')
    try:
        scan(modules)
    except (OSError, ValueError, UnicodeError, TypeError):
        return None
    if constraints is not None:
        constraints['value'] = restrictions if restrictions_valid else None
    return sorted(found)


def _empty_directory(path: Path) -> bool | None:
    try:
        if path.is_symlink() or not path.is_dir():
            return None
        with os.scandir(path) as entries:
            return next(entries, None) is None
    except OSError:
        return None


def inventory(root: Path) -> dict:
    root = root.resolve(strict=True)
    if not root.is_dir():
        raise SyncError('Корень Nexus должен быть каталогом')
    projects: dict[str, dict] = {}
    portability = []
    sample_exclusions = []
    template_parents = set()
    def inspect_names(directory, directories, files):
        # These are only matches against the bundled example, not the user's
        # actual .stignore; do not descend into excluded environments.
        templates = {'.env.example', '.env.sample', '.env.template', '.env.dist'}
        if any(name.lower() in templates and (directory / name).is_file()
               and not (directory / name).is_symlink() for name in files):
            template_parents.add(directory)
        for name in sorted((set(directories) | ({'.git'} & set(files))) & (EXCLUDED - {'.codexsync-workspace'})):
            entry = directory / name
            relative = entry.relative_to(root).as_posix().encode('utf-8', 'backslashreplace').decode('utf-8')
            linked = entry.is_symlink() or getattr(entry, 'is_junction', lambda: False)()
            marker = False
            can_check = os.name != 'nt' or hasattr(entry, 'is_junction')
            if name != '.git' and name in directories and not linked and can_check:
                marker = any((entry / item).is_file() and not (entry / item).is_symlink()
                             for item in set(MANIFESTS) | {'AGENTS.md'}) or (entry / '.git').exists()
            sample_exclusions.append({'path': relative, 'kind': 'git' if name == '.git' else 'local_name',
                                      'project_marker': marker, 'link': linked})
            if len(sample_exclusions) > 10000:
                raise SyncError('Слишком много совпадений с образцом исключений')
        names = sorted([name for name in directories if name not in EXCLUDED] + files)
        equivalents = {}
        for name in names:
            path = (directory / name).relative_to(root).as_posix().encode('utf-8', 'backslashreplace').decode('utf-8')
            stem = name.split('.', 1)[0].upper()
            if (name.endswith((' ', '.')) or any(ord(char) < 32 or char in '<>:"\\|?*' for char in name)
                    or stem in WINDOWS_RESERVED_STEMS):
                portability.append({'kind': 'windows_name', 'paths': [path]})
            if (directory / name).is_symlink():
                portability.append({'kind': 'symlink', 'paths': [path]})
            equivalent = unicodedata.normalize('NFC', name).rstrip(' .').casefold()
            equivalents.setdefault(equivalent, []).append(path)
        for paths in equivalents.values():
            if len(paths) > 1:
                portability.append({'kind': 'name_collision', 'paths': paths})
        if len(portability) > 10000:
            raise SyncError('Слишком много предупреждений о переносимости путей')
    for path in _walk(root, observe_directory=inspect_names):
        if path.name not in MANIFESTS:
            continue
        relative = path.parent.relative_to(root).as_posix()
        item = projects.setdefault(relative, {'types': set(), 'files': {}, 'local': {}})
        item['types'].add(MANIFESTS[path.name])
        item['files'][path.name] = _digest(path)
        item['local']['env_template_present'] = path.parent in template_parents
        if path.name in PYTHON_PROJECT_FILES:
            envs = [path.parent / name for name in ('.venv', 'venv') if (path.parent / name).is_dir()]
            item['local']['python_env_present'] = bool(envs)
            item['local']['python_env_empty'] = bool(envs) and all(_empty_directory(env) is True for env in envs)
            if 'python_packages' not in item['local']:
                wheels = {}
                item['local']['python_packages'] = _python_env_packages(path.parent, wheels=wheels)
                item['local']['python_wheels'] = wheels.get('value', {})
        elif path.name == 'package.json':
            item['local']['node_modules_present'] = (path.parent / 'node_modules').is_dir()
            item['local']['node_modules_empty'] = _empty_directory(path.parent / 'node_modules') is True
            restrictions = {}
            item['local']['node_packages'] = _node_packages(path.parent, constraints=restrictions)
            item['local']['node_constraints'] = restrictions.get('value')
        elif path.name == 'pubspec.yaml':
            item['local']['dart_tool_present'] = (path.parent / '.dart_tool').is_dir()
        elif path.name == 'Cargo.toml':
            target = path.parent / 'target'
            linked = target.is_symlink() or getattr(target, 'is_junction', lambda: False)()
            item['local']['rust_target_present'] = None if linked else target.is_dir()
    manifest_names = {name for item in projects.values() for name in item['files']}
    toolchains = {name: any(shutil.which(command) is not None for command in commands)
                  for name, (files, commands) in TOOLCHAINS.items() if files & manifest_names}
    commands = set()
    if PYTHON_PROJECT_FILES & manifest_names:
        commands.update(('python3', 'py'))
    if {'package-lock.json', 'npm-shrinkwrap.json'} & manifest_names:
        commands.update(('node', 'npm'))
    executables = {name: shutil.which(name) is not None for name in sorted(commands)}
    return {'format': 'nexus-project-inventory', 'version': VERSION, 'toolchains': toolchains, 'portability': portability,
            'sample_exclusions': sample_exclusions, 'executables': executables,
            'projects': {name: {'types': sorted(item['types']), 'files': item['files'], 'local': item['local']} for name, item in sorted(projects.items())}}


def project_actions(current: dict, *, platform: str | None = None) -> list[dict]:
    """Suggest, never execute, commands where a project clearly declares them."""
    platform = sys.platform if platform is None else platform
    actions = []
    executables = current.get('executables', {})
    for project, item in current['projects'].items():
        files, local = item['files'], item.get('local', {})
        plain_requirements = ('requirements.txt' in files and not (
            {'pyproject.toml', 'Pipfile', 'Pipfile.lock', 'poetry.lock', 'uv.lock'} & set(files)))
        if plain_requirements and platform == 'win32' and executables.get('py') is not False:
            python_steps = ['py -3 -m venv .venv', r'.venv\Scripts\python.exe -m pip install -r requirements.txt']
        elif plain_requirements and platform in ('linux', 'darwin') and executables.get('python3') is not False:
            python_steps = ['python3 -m venv .venv', '.venv/bin/python -m pip install -r requirements.txt']
        else:
            python_steps = []
        if (PYTHON_PROJECT_FILES & set(files)) and not local.get('python_env_present'):
            actions.append({'project': project, 'reason': 'Локальное Python-окружение не найдено', 'command': None,
                            'note': 'Проверить инструкции проекта, Python и совместимость зависимостей', 'steps': python_steps})
        elif (PYTHON_PROJECT_FILES & set(files)) and local.get('python_env_empty'):
            actions.append({'project': project, 'reason': 'Каталог Python-окружения пуст', 'command': None,
                            'note': 'Проверить инструкции проекта, Python и совместимость зависимостей', 'steps': python_steps})
        if plain_requirements:
            executable = 'py' if platform == 'win32' else 'python3' if platform in ('linux', 'darwin') else None
            if executable and executables.get(executable) is False:
                actions.append({'project': project, 'reason': f'{executable} не найден в PATH этого процесса',
                                'command': None, 'note': 'Проверить установку и инструкции проекта'})
        if 'package.json' in files and not local.get('node_modules_present'):
            command = ('npm ci' if ({'package-lock.json', 'npm-shrinkwrap.json'} & set(files))
                       and not ({'pnpm-lock.yaml', 'yarn.lock', 'bun.lock', 'bun.lockb'} & set(files))
                       and executables.get('node') is not False and executables.get('npm') is not False else None)
            actions.append({'project': project, 'reason': 'node_modules не найден', 'command': command, 'note': 'Проверить менеджер пакетов и версию Node.js'})
        elif 'package.json' in files and local.get('node_modules_empty'):
            actions.append({'project': project, 'reason': 'Каталог node_modules пуст', 'command': None, 'note': 'Проверить, нужны ли проекту зависимости и как их установить'})
        elif ('package.json' in files and local.get('node_modules_present')
              and 'node_packages' in local and local['node_packages'] is None):
            actions.append({'project': project, 'reason': 'Состав node_modules не проверен', 'command': None,
                            'note': 'Проверить ссылки и метаданные пакетов; изменений состава могло не попасть в журнал'})
        npm_lock = {'package-lock.json', 'npm-shrinkwrap.json'} & set(files)
        other_locks = {'pnpm-lock.yaml', 'yarn.lock', 'bun.lock', 'bun.lockb'} & set(files)
        if 'package.json' in files and npm_lock and not other_locks and executables.get('npm') is False:
            actions.append({'project': project, 'reason': 'npm не найден в PATH этого процесса', 'command': None,
                            'note': 'Проверить установку Node.js и пакетного менеджера'})
        if 'pubspec.yaml' in files and not local.get('dart_tool_present'):
            actions.append({'project': project, 'reason': '.dart_tool не найден', 'command': None, 'note': 'Определить Flutter или Dart и получить зависимости'})
        if 'Cargo.toml' in files and local.get('rust_target_present') is True:
            actions.append({'project': project, 'reason': 'Обнаружен Rust target', 'command': None,
                            'note': 'Проверить содержимое и правила Syncthing: каталог может содержать локальные артефакты и важные данные'})
        elif 'Cargo.toml' in files and local.get('rust_target_present', False) is None:
            actions.append({'project': project, 'reason': 'Rust target является ссылкой', 'command': None,
                            'note': 'Вручную проверить назначение и правила Syncthing'})
        if local.get('env_template_present'):
            actions.append({'project': project, 'reason': 'Образец .env может быть исключён правилами синхронизации', 'command': None, 'note': 'Вручную проверить содержимое и выбрать безопасный способ передачи'})
        for name, (manifest_files, _) in TOOLCHAINS.items():
            if manifest_files & set(files) and current.get('toolchains', {}).get(name) is False:
                actions.append({'project': project, 'reason': f'{name} не найден в PATH этого процесса', 'command': None,
                                'note': 'Проверить установку и требования проекта; возможна отдельная настройка окружения'})
    return actions


def _node_constraint_event(project: str, package: str, value: dict) -> str:
    return 'Ограничения Node.js-пакета: ' + project + ' — ' + json.dumps(
        {'package': package, 'os': value.get('os', []), 'cpu': value.get('cpu', [])},
        ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _python_wheel_event(project: str, env: str, package: str, tags: list[str] | None) -> str:
    return 'Python wheel: ' + project + ' — ' + json.dumps(
        {'env': env, 'package': package, 'tags': tags}, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def compare(old: dict, new: dict) -> list[str]:
    if old.get('format') != 'nexus-project-inventory' or old.get('version') != VERSION or not isinstance(old.get('projects'), dict):
        raise SyncError('Неподдерживаемый прежний снимок проектов')
    lines = []
    before_tools, after_tools = old.get('toolchains', {}), new.get('toolchains', {})
    if isinstance(before_tools, dict) and isinstance(after_tools, dict):
        for name in sorted(set(before_tools) & set(after_tools) & set(TOOLCHAINS)):
            if type(before_tools[name]) is bool and type(after_tools[name]) is bool and before_tools[name] != after_tools[name]:
                state = 'найден в PATH' if after_tools[name] else 'не найден в PATH'
                lines.append(f'Инструмент {name}: {state}')
    before_commands, after_commands = old.get('executables', {}), new.get('executables', {})
    if isinstance(before_commands, dict) and isinstance(after_commands, dict):
        for name in sorted(set(before_commands) & set(after_commands) & {'python3', 'py', 'npm'}):
            if type(before_commands[name]) is bool and type(after_commands[name]) is bool and before_commands[name] != after_commands[name]:
                state = 'найдена в PATH' if after_commands[name] else 'не найдена в PATH'
                lines.append(f'Команда {name}: {state}')
    left, right = old['projects'], new['projects']
    for project in sorted(set(left) | set(right)):
        if project not in left:
            lines.append(f'Новый проект: {project}')
        elif project not in right:
            lines.append(f'Проект не найден: {project} (проверьте доступность папки)')
        else:
            before, after = left[project]['files'], right[project]['files']
            for file in sorted(set(before) | set(after)):
                if before.get(file) != after.get(file):
                    action = ('Добавлен' if file not in before else
                              'Удалён' if file not in after else 'Изменён')
                    lines.append(f'{action} манифест: {project}/{file}')
            before_local = left[project].get('local', {})
            after_local = right[project].get('local', {})
            labels = {'python_env_present': 'Python-окружение', 'node_modules_present': 'node_modules',
                      'dart_tool_present': '.dart_tool', 'rust_target_present': 'Rust target'}
            for key, label in labels.items():
                # Missing keys can mean the corresponding manifest changed;
                # they are not proof that an environment was added/removed.
                if (type(before_local.get(key)) is bool and type(after_local.get(key)) is bool
                        and before_local[key] != after_local[key]):
                    state = 'обнаружено' if after_local[key] else 'не найдено'
                    lines.append(f'Локальное {label}: {project} — {state}')
            previous_packages = before_local.get('python_packages', {})
            current_packages = after_local.get('python_packages', {})
            if isinstance(previous_packages, dict) and isinstance(current_packages, dict):
                for env in sorted(set(previous_packages) & set(current_packages)):
                    if not isinstance(env, str) or not re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env):
                        continue
                    prior, now = previous_packages[env], current_packages[env]
                    if (not isinstance(prior, list) or not isinstance(now, list)
                            or not all(isinstance(name, str) for name in prior + now)):
                        continue
                    for name in sorted(set(now) - set(prior)):
                        lines.append(f'Обнаружен Python-пакет: {project} ({env}) — {name}')
                    for name in sorted(set(prior) - set(now)):
                        lines.append(f'Не найден Python-пакет: {project} ({env}) — {name}')
            previous_node, current_node = before_local.get('node_packages'), after_local.get('node_packages')
            old_wheels, new_wheels = before_local.get('python_wheels'), after_local.get('python_wheels')
            if isinstance(old_wheels, dict) and isinstance(new_wheels, dict):
                for env in sorted(set(old_wheels) & set(new_wheels)):
                    old_tags, new_tags = old_wheels[env], new_wheels[env]
                    if not isinstance(old_tags, dict) or not isinstance(new_tags, dict):
                        continue
                    for package, tags in sorted(new_tags.items()):
                        if isinstance(package, str) and old_tags.get(package) != tags:
                            lines.append(_python_wheel_event(project, env, package, tags))
            if (isinstance(previous_node, list) and isinstance(current_node, list)
                    and all(isinstance(name, str) for name in previous_node + current_node)):
                for name in sorted(set(current_node) - set(previous_node)):
                    lines.append(f'Обнаружен Node.js-пакет: {project} — {name}')
                for name in sorted(set(previous_node) - set(current_node)):
                    lines.append(f'Не найден Node.js-пакет: {project} — {name}')
            before_constraints = before_local.get('node_constraints')
            after_constraints = after_local.get('node_constraints')
            if isinstance(before_constraints, dict) and isinstance(after_constraints, dict):
                for name in sorted(set(before_constraints) | set(after_constraints)):
                    before_value, after_value = before_constraints.get(name), after_constraints.get(name, {})
                    if isinstance(name, str) and isinstance(after_value, dict) and before_value != after_value:
                        lines.append(_node_constraint_event(project, name, after_value))
            if ('env_template_present' in before_local and 'env_template_present' in after_local
                    and before_local['env_template_present'] != after_local['env_template_present']):
                state = 'обнаружен' if after_local['env_template_present'] else 'не найден'
                lines.append(f'Образец .env: {project} — {state}')
    return lines


def initial_package_observations(previous: dict | None, current: dict) -> list[str]:
    """Describe first successful package scans without presenting them as installs."""
    lines = []
    old_projects = previous.get('projects', {}) if isinstance(previous, dict) else {}
    for project, item in sorted(current['projects'].items()):
        local = item.get('local', {})
        older = old_projects.get(project, {}).get('local', {})
        nodes = local.get('node_packages')
        if isinstance(nodes, list) and not isinstance(older.get('node_packages'), list):
            lines.extend(f'Начально наблюдался Node.js-пакет: {project} — {name}'
                         for name in nodes if isinstance(name, str))
        constraints = local.get('node_constraints')
        if isinstance(constraints, dict) and not isinstance(older.get('node_constraints'), dict):
            lines.extend(_node_constraint_event(project, name, value) for name, value in sorted(constraints.items())
                         if isinstance(name, str) and isinstance(value, dict))
        sites = local.get('python_packages')
        old_sites = older.get('python_packages')
        if isinstance(sites, dict):
            for env, names in sorted(sites.items()):
                if (not isinstance(env, str) or not re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env)
                        or not isinstance(names, list) or (isinstance(old_sites, dict) and env in old_sites)):
                    continue
                lines.extend(f'Начально наблюдался Python-пакет: {project} ({env}) — {name}'
                             for name in names if isinstance(name, str))
        wheels, old_wheels = local.get('python_wheels'), older.get('python_wheels')
        if isinstance(wheels, dict):
            for env, packages in sorted(wheels.items()):
                if not isinstance(packages, dict):
                    continue
                old_packages = old_wheels.get(env) if isinstance(old_wheels, dict) else None
                if not isinstance(old_packages, dict):
                    lines.extend(_python_wheel_event(project, env, package, tags)
                                 for package, tags in sorted(packages.items()) if isinstance(package, str) and tags is not None)
    return lines


def package_observation_resets(current: dict) -> list[str]:
    """Replace older peer observations only for scopes scanned successfully now."""
    lines = []
    for project, item in sorted(current['projects'].items()):
        local = item.get('local', {})
        if isinstance(local.get('node_packages'), list):
            lines.append(f'Обновлён исходный список Node.js: {project}')
        sites = local.get('python_packages')
        if isinstance(sites, dict):
            for env, names in sorted(sites.items()):
                if (isinstance(env, str) and re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env)
                        and isinstance(names, list)):
                    lines.append(f'Обновлён исходный список Python: {project} ({env})')
    return lines


def record_projects(nexus: Path, shared: Path, state_dir: Path, machine: str, *, key: bytes, current: dict | None = None) -> list[str]:
    machine = _safe_machine(machine)
    shared = shared.resolve(strict=True)
    state_dir = state_dir.resolve(strict=True)
    if state_dir == shared or shared in state_dir.parents:
        raise SyncError('Локальная папка состояния должна быть вне папки обмена')
    if not state_dir.is_dir():
        raise SyncError('Локальная папка состояния не найдена')
    current = inventory(nexus) if current is None else current
    baseline = state_dir / f'projects-{machine}.json'
    previous = json.loads(baseline.read_text(encoding='utf-8')) if baseline.exists() else None
    changes = compare(previous, current) if previous is not None else []
    migrating = previous is not None and previous.get('package_baseline_version') != 1
    initial = ((package_observation_resets(current) + initial_package_observations(None, current)) if migrating else
               initial_package_observations(previous, current))
    pending_node = set()
    pending_python = set()
    if previous is not None:
        pending = previous.get('package_baseline_pending', {})
        if isinstance(pending, dict):
            nodes = pending.get('node', [])
            sites = pending.get('python', [])
            pending_node.update(item for item in (nodes if isinstance(nodes, list) else []) if isinstance(item, str))
            pending_python.update(tuple(item) for item in (sites if isinstance(sites, list) else [])
                                  if isinstance(item, list) and len(item) == 2 and all(isinstance(part, str) for part in item))
        if migrating:
            for project, item in current['projects'].items():
                local = item.get('local', {})
                older = previous['projects'].get(project, {}).get('local', {})
                if local.get('node_modules_present') and local.get('node_packages') is None:
                    pending_node.add(project)
                old_sites, sites = older.get('python_packages'), local.get('python_packages')
                if local.get('python_env_present') and isinstance(old_sites, dict) and isinstance(sites, dict):
                    pending_python.update((project, site) for site in old_sites
                                          if isinstance(site, str) and site not in sites
                                          and re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', site))
        for project in sorted(pending_node):
            local = current['projects'].get(project, {}).get('local', {})
            names = local.get('node_packages')
            if isinstance(names, list):
                initial.append(f'Обновлён исходный список Node.js: {project}')
                initial.extend(f'Начально наблюдался Node.js-пакет: {project} — {name}' for name in names if isinstance(name, str))
                constraints = local.get('node_constraints')
                if isinstance(constraints, dict):
                    initial.extend(_node_constraint_event(project, name, value)
                                   for name, value in sorted(constraints.items())
                                   if isinstance(name, str) and isinstance(value, dict))
                pending_node.discard(project)
        for project, env in sorted(pending_python):
            local = current['projects'].get(project, {}).get('local', {})
            sites = local.get('python_packages')
            if (isinstance(sites, dict) and isinstance(sites.get(env), list)
                    and re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env)):
                initial.append(f'Обновлён исходный список Python: {project} ({env})')
                initial.extend(f'Начально наблюдался Python-пакет: {project} ({env}) — {name}'
                               for name in sites[env] if isinstance(name, str))
                pending_python.discard((project, env))
    baseline_current = copy.deepcopy(current)
    baseline_current['package_baseline_version'] = 1
    baseline_current['package_baseline_pending'] = {'node': sorted(pending_node),
                                                    'python': [list(item) for item in sorted(pending_python)]}
    # Preserve the last successful site scan when an existing environment is
    # temporarily unreadable, so its recovery can still reveal real changes.
    if previous is not None:
        for project, item in baseline_current['projects'].items():
            local = item.get('local', {})
            older = previous['projects'].get(project, {}).get('local', {})
            old_packages, packages = older.get('python_packages'), local.get('python_packages')
            if local.get('python_env_present') and isinstance(old_packages, dict) and isinstance(packages, dict):
                for site, names in old_packages.items():
                    if site not in packages:
                        packages[site] = names
            if (local.get('node_modules_present') and local.get('node_packages') is None
                    and isinstance(older.get('node_packages'), list)):
                local['node_packages'] = older['node_packages']
            if (local.get('node_modules_present') and local.get('node_constraints') is None
                    and isinstance(older.get('node_constraints'), dict)):
                local['node_constraints'] = older['node_constraints']
    if changes or initial:
        exchange = shared / 'nexus-sync'
        events_root = exchange / 'project-events'
        directory = events_root / machine
        if any(path.is_symlink() for path in (exchange, events_root, directory)):
            raise SyncError('Ссылки в пути событий проекта запрещены')
        directory.mkdir(parents=True, exist_ok=True)
        event = {'format': 'nexus-project-event-v1', 'machine': machine,
                 'created': datetime.now(timezone.utc).isoformat(), 'changes': initial + changes}
        output = directory / (uuid.uuid4().hex + '.nxe')
        raw = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
        if len(raw) > MAX_SECRET_BYTES:
            raise SyncError('Событие проекта слишком велико')
        AESGCM, _ = _aead()
        nonce = os.urandom(12)
        aad = PROJECT_EVENT_MAGIC + machine.encode('ascii') + b'/' + output.name.encode('ascii')
        encrypted = PROJECT_EVENT_MAGIC + nonce + AESGCM(key).encrypt(nonce, raw, aad)
        with tempfile.NamedTemporaryFile(prefix='.nexus-event-', dir=directory, delete=False) as handle:
            temporary_event = Path(handle.name)
            try:
                handle.write(encrypted)
            except BaseException:
                temporary_event.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary_event, output)
        finally:
            temporary_event.unlink(missing_ok=True)
    # A failed event write leaves the old baseline in place so changes retry.
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-projects-', dir=state_dir, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(baseline_current, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write('\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, baseline)
    finally:
        temporary.unlink(missing_ok=True)
    return changes


def project_log(shared: Path, key: bytes, *, include_legacy: bool = False, machine_filter: str | None = None) -> list[dict]:
    exchange = shared.resolve(strict=True) / 'nexus-sync'
    root = exchange / 'project-events'
    if exchange.is_symlink() or root.is_symlink():
        raise SyncError('Ссылки в пути событий проекта запрещены')
    if not root.is_dir():
        return []
    events = []
    for machine in sorted(root.iterdir()):
        if machine_filter is not None and machine.name != machine_filter:
            continue
        if not machine.is_dir() or machine.is_symlink():
            continue
        paths = list(machine.glob('*.nxe')) + (list(machine.glob('*.json')) if include_legacy else [])
        for path in sorted(paths):
            if path.is_symlink():
                continue
            try:
                if path.suffix == '.nxe':
                    if not re.fullmatch(r'[0-9a-f]{32}\.nxe', path.name):
                        raise SyncError('Некорректное имя события проекта')
                    _safe_machine(machine.name)
                    if path.stat().st_size > MAX_SECRET_BYTES + len(PROJECT_EVENT_MAGIC) + 28:
                        raise SyncError('Событие проекта слишком велико')
                    encoded = path.read_bytes()
                    if not encoded.startswith(PROJECT_EVENT_MAGIC) or len(encoded) < len(PROJECT_EVENT_MAGIC) + 28:
                        raise SyncError('Зашифрованное событие проекта повреждено')
                    offset = len(PROJECT_EVENT_MAGIC)
                    aad = PROJECT_EVENT_MAGIC + machine.name.encode('ascii') + b'/' + path.name.encode('ascii')
                    AESGCM, InvalidTag = _aead()
                    try:
                        raw = AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], aad)
                    except (InvalidTag, ValueError) as exc:
                        raise SyncError('Событие проекта не прошло проверку подлинности') from exc
                    event = json.loads(raw)
                else:
                    event = json.loads(path.read_text(encoding='utf-8'))
                if (isinstance(event, dict) and event.get('format') == 'nexus-project-event-v1'
                        and event.get('machine') == machine.name and isinstance(event.get('created'), str)
                        and isinstance(event.get('changes'), list)
                        and all(isinstance(change, str) for change in event['changes'])):
                    events.append(event)
                elif path.suffix == '.nxe':
                    raise SyncError('Неподдерживаемый формат события проекта')
            except (OSError, ValueError, UnicodeError):
                if path.suffix == '.nxe':
                    raise SyncError('Зашифрованное событие проекта недоступно или повреждено')
                continue
    return sorted(events, key=lambda event: (event['created'], event['machine']))


def write_project_report(shared: Path, state_dir: Path, machine: str, current: dict, *, key: bytes,
                         include_peers: bool, platform: str | None = None) -> Path:
    """Refresh an owned local report; the individual shared events remain intact."""
    machine = _safe_machine(machine)
    state_dir = state_dir.resolve(strict=True)
    shared = shared.resolve(strict=True)
    if state_dir == shared or shared in state_dir.parents:
        raise SyncError('Отчёт должен находиться вне общей папки')
    output = state_dir / f'project-changelog-{machine}.md'
    marker = '<!-- nexus-sync-project-report-v1 -->'
    if output.is_symlink():
        raise SyncError('Отчёт не может быть символьной ссылкой')
    if output.exists():
        with output.open('r', encoding='utf-8') as existing:
            if existing.readline().rstrip('\n') != marker:
                raise SyncError('Существующий файл отчёта не принадлежит Nexus Sync')
    events = project_log(shared, key, machine_filter=None if include_peers else machine)
    actions = project_actions(current, platform=platform)
    lines = [marker, '# Nexus Sync: проекты и зависимости', '',
             f'Устройство: {machine}', '',
             'Окружения проверяются по наличию папок, а не по составу или совместимости пакетов.', '',
             'Инструменты ищутся в PATH процесса без запуска; версии и работоспособность не проверяются.', '',
             '## Что проверить на этом устройстве', '']
    if not actions:
        lines.append('По известным манифестам отсутствующие окружения не обнаружены.')
    for action in actions:
        lines.append(f"- {action['project']}: {action['reason']}. {action['note']}")
        commands = action.get('steps') or ([action['command']] if action.get('command') else [])
        if commands:
            lines.append('  Предложения для проверки вручную, в каталоге проекта: ' +
                         '; '.join(f'`{command}`' for command in commands) + '.')
    candidates = current.get('sample_exclusions', [])
    if isinstance(candidates, list) and candidates:
        lines.extend(['', '## Проверить совпадения с образцом исключений', '',
                      'Это только совпадения имён с stignore.example; действующие правила Syncthing не проверены.',
                      'Содержимое каталогов не проверялось. Нужные исходники и данные могут быть исключены.'])
        for candidate in candidates:
            if not isinstance(candidate, dict) or not isinstance(candidate.get('path'), str):
                continue
            path = json.dumps(candidate['path'], ensure_ascii=False)
            if candidate.get('kind') == 'git':
                note = 'Git исключён образцом; восстановление через Git-пакет требует проверки'
            elif candidate.get('project_marker') is True:
                note = 'есть признак проекта; проверьте до исключения'
            elif candidate.get('link') is True:
                note = 'ссылка; содержимое не проверено'
            else:
                note = 'локальное окружение или данные; требуется проверка'
            lines.append(f'- {path}: {note}')
    portability = current.get('portability', [])
    if portability:
        labels = {'windows_name': 'Имя может быть недопустимо в Windows',
                  'name_collision': 'Имена могут совпасть на другой файловой системе',
                  'symlink': 'Символьная ссылка требует проверки на другом устройстве'}
        lines.extend(['', '## Проверить переносимость путей', '',
                      'Проверка приблизительная и не учитывает конкретные правила Syncthing.'])
        for item in portability:
            if isinstance(item, dict) and item.get('kind') in labels and isinstance(item.get('paths'), list):
                paths = ', '.join(json.dumps(path, ensure_ascii=False) for path in item['paths'] if isinstance(path, str))
                lines.append(f"- {labels[item['kind']]}: {paths}")
    # An existing environment can still predate a manifest change. Events do
    # not prove which version is installed, so retain a conservative reminder.
    changed = set()
    for event in events:
        for change in event['changes']:
            for kind in ('Изменён', 'Добавлен', 'Удалён'):
                prefix = f'{kind} манифест: '
                if change.startswith(prefix):
                    project, separator, name = change[len(prefix):].rpartition('/')
                    if separator and project in current['projects'] and name in MANIFESTS:
                        changed.add((project, name, kind.lower()))
                    break
    if changed:
        lines.extend(['', '## Проверить после изменения зависимостей', '',
                      'События не подтверждают состояние установленных пакетов; проверьте актуальность окружения.'])
        lines.extend(f'- {project}: {kind} {name}' for project, name, kind in sorted(changed))
    peer_node = {}
    peer_node_constraints = {}
    peer_python = {}
    peer_python_wheels = {}
    for event in events:
        if event['machine'] == machine:
            continue
        for change in event['changes']:
            node_reset = 'Обновлён исходный список Node.js: '
            python_reset = 'Обновлён исходный список Python: '
            if change.startswith(node_reset):
                project = change[len(node_reset):]
                if project in current['projects']:
                    peer_node[(event['machine'], project)] = set()
                    peer_node_constraints[(event['machine'], project)] = {}
            if change.startswith(python_reset):
                project, separator, env = change[len(python_reset):].rpartition(' (')
                if (separator and env.endswith(')') and project in current['projects']
                        and re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env[:-1])):
                    peer_python[(event['machine'], project, env[:-1])] = set()
                    for item in list(peer_python_wheels):
                        if item[:3] == (event['machine'], project, env[:-1]):
                            del peer_python_wheels[item]
            for prefix, added in (('Начально наблюдался Node.js-пакет: ', True),
                                  ('Обнаружен Node.js-пакет: ', True), ('Не найден Node.js-пакет: ', False)):
                if change.startswith(prefix):
                    project, separator, name = change[len(prefix):].rpartition(' — ')
                    if separator and project in current['projects'] and name:
                        packages = peer_node.setdefault((event['machine'], project), set())
                        packages.add(name) if added else packages.discard(name)
                        if not added:
                            peer_node_constraints.setdefault((event['machine'], project), {}).pop(name, None)
                    break
            prefix = 'Ограничения Node.js-пакета: '
            if change.startswith(prefix):
                project, separator, encoded = change[len(prefix):].rpartition(' — ')
                if separator and project in current['projects']:
                    try:
                        info = json.loads(encoded)
                        package, os_names, cpus = info['package'], info['os'], info['cpu']
                        valid = (isinstance(package, str) and 0 < len(package) <= 300 and
                                 all(isinstance(values, list) and len(values) <= 32 and all(
                                     isinstance(value, str) and re.fullmatch(r'!?[a-z0-9][a-z0-9_.-]{0,31}', value)
                                     for value in values) for values in (os_names, cpus)))
                        if valid:
                            observed = peer_node_constraints.setdefault((event['machine'], project), {})
                            if os_names or cpus:
                                observed[package] = {'os': os_names, 'cpu': cpus}
                            else:
                                observed.pop(package, None)
                    except (ValueError, KeyError, TypeError):
                        pass
            for prefix, added in (('Начально наблюдался Python-пакет: ', True),
                                  ('Обнаружен Python-пакет: ', True), ('Не найден Python-пакет: ', False)):
                if change.startswith(prefix):
                    owner, separator, name = change[len(prefix):].rpartition(' — ')
                    project, env_separator, env = owner.rpartition(' (')
                    if (separator and env_separator and env.endswith(')') and project in current['projects'] and name
                            and re.fullmatch(r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env[:-1])):
                        packages = peer_python.setdefault((event['machine'], project, env[:-1]), set())
                        packages.add(name) if added else packages.discard(name)
                        if not added:
                            peer_python_wheels.pop((event['machine'], project, env[:-1], name), None)
                    break
            prefix = 'Python wheel: '
            if change.startswith(prefix):
                project, separator, encoded = change[len(prefix):].rpartition(' — ')
                if separator and project in current['projects']:
                    try:
                        info = json.loads(encoded)
                        env, package, tags = info['env'], info['package'], info['tags']
                        valid_env = isinstance(env, str) and re.fullmatch(
                            r'(?:\.venv|venv)/(?:Lib/site-packages|lib/python\d+\.\d+/site-packages)', env)
                        valid_tags = (tags is None or isinstance(tags, list) and 0 < len(tags) <= 32 and all(
                            isinstance(tag, str) and len(tag) <= 128 and re.fullmatch(
                                r'[a-z0-9_.]+-[a-z0-9_.]+-[a-z0-9_.]+', tag) for tag in tags))
                        if valid_env and isinstance(package, str) and 0 < len(package) <= 300 and valid_tags:
                            identity = (event['machine'], project, env, package)
                            if tags is None:
                                peer_python_wheels.pop(identity, None)
                            else:
                                peer_python_wheels[identity] = tags
                    except (ValueError, KeyError, TypeError):
                        pass
    peer_missing = []
    node_details = {}
    python_details = {}
    target_platform = sys.platform if platform is None else platform
    target_os = target_platform if target_platform in ('linux', 'darwin', 'win32') else None
    for (peer, project), packages in sorted(peer_node.items()):
        local = current['projects'][project].get('local', {})
        local_packages = local.get('node_packages')
        for name in sorted(packages):
            if isinstance(local_packages, list) and name in local_packages:
                continue
            state = ('локально не обнаружен' if isinstance(local_packages, list) else
                     'локальный состав не проверен' if local.get('node_modules_present') else
                     'локальный node_modules отсутствует')
            peer_missing.append((peer, project, name, state))
            declared = peer_node_constraints.get((peer, project), {}).get(name)
            if declared:
                os_names, cpus = declared['os'], declared['cpu']
                detail = []
                if os_names:
                    detail.append('OS в метаданных: ' + ', '.join(os_names))
                if cpus:
                    detail.append('CPU в метаданных: ' + ', '.join(cpus))
                positives = [value for value in os_names if not value.startswith('!')]
                negatives = [value[1:] for value in os_names if value.startswith('!')]
                if target_os and ((positives and not negatives and 'any' not in positives and target_os not in positives)
                                  or (negatives and not positives and target_os in negatives)):
                    detail.append('ограничение может исключать текущую ОС')
                node_details[(peer, project, name)] = '; '.join(detail)
    python_union = {}
    for (peer, project, _env), packages in peer_python.items():
        python_union.setdefault((peer, project), set()).update(packages)
    python_wheel_union = {}
    for (peer, project, _env, package), values in peer_python_wheels.items():
        python_wheel_union.setdefault((peer, project, package), set()).update(values)
    for (peer, project), packages in sorted(python_union.items()):
        local = current['projects'][project].get('local', {})
        sites = local.get('python_packages')
        scanned = [names for names in sites.values() if isinstance(names, list)] if isinstance(sites, dict) else []
        local_names = {name for names in scanned for name in names if isinstance(name, str)}
        for name in sorted(packages - local_names):
            state = ('локальное Python-окружение отсутствует' if not local.get('python_env_present') else
                     'локальный состав не проверен' if not scanned else
                     'не обнаружен в проверенных site-packages')
            peer_missing.append((peer, project, name, state))
            tags = sorted(python_wheel_union.get((peer, project, name), ()))
            if tags:
                detail = 'wheel: ' + ', '.join(tags)
                platforms = {tag.rsplit('-', 1)[-1] for tag in tags}
                def wheel_os(value):
                    if value == 'any':
                        return 'any'
                    if value.startswith(('linux_', 'manylinux', 'musllinux')):
                        return 'linux'
                    if value.startswith('macosx_'):
                        return 'darwin'
                    if value == 'win32' or value.startswith('win_'):
                        return 'win32'
                    return None
                systems = {wheel_os(value) for platform in platforms for value in platform.split('.')}
                if target_os and None not in systems and 'any' not in systems and target_os not in systems:
                    detail += '; установленная сборка для другой ОС, другая сборка может существовать'
                python_details[(peer, project, name)] = detail
    if peer_missing:
        lines.extend(['', '## Пакеты с других устройств для проверки', '',
                      'Учтены доступные начальные наблюдения и последующие изменения; состояние и совместимость на другом устройстве не подтверждены.'])
        for peer, project, name, state in peer_missing:
            detail = node_details.get((peer, project, name)) or python_details.get((peer, project, name))
            lines.append(f'- {project}: {name} — отмечен на {peer}; {state}' + (f'; {detail}' if detail else ''))
    lines.extend(['', '## Журнал наблюдений', ''])
    if not events:
        lines.append('Изменений после базовой проверки пока нет.')
    for event in events:
        lines.append(f"- {event['created']} — {event['machine']}")
        lines.extend(f"  - {change}" for change in event['changes'])
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.nexus-report-', dir=state_dir, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write('\n'.join(lines) + '\n')
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


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
                identifier = (payload.get('session_id') or payload.get('id')) if isinstance(payload, dict) else None
                if not isinstance(identifier, str) or not identifier:
                    raise SyncError('Чат без идентификатора; сравнение остановлено')
                mode = payload.get('history_mode')
                unsupported = 'PAGINATED' if mode == 'paginated' else 'UNKNOWN_FORMAT' if mode not in (None, 'legacy') else None
                if unsupported and not include_unsupported:
                    raise UnsupportedHistoryError('Пагинированная история пока не поддерживается' if unsupported == 'PAGINATED' else 'Неизвестный формат истории пока не поддерживается')
                catalogue.setdefault(identifier, []).append({'path': path, 'cwd': payload.get('cwd'), 'folder': folder, 'unsupported': unsupported})
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
    sources = _chat_catalog(incoming)
    if not sources:
        raise SyncError('Во входящей копии не найдено чатов')
    targets = _chat_catalog(target, include_unsupported=True)
    plan = []
    for identifier, versions in sorted(sources.items()):
        local = targets.get(identifier, [])
        if len(versions) != 1 or len(local) > 1:
            relation = 'AMBIGUOUS'
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
        cwd_status = 'NOT_CHECKED'
        if source_root and target_root:
            cwd_status = _history_cwd_status(versions[0]['path'], source_root, target_root)
        plan.append({'chat_hash': hashlib.sha256(identifier.encode('utf-8')).hexdigest(), 'relation': relation, 'cwd': cwd_status})
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
        allowed = {'snapshot', 'secrets', 'receive', 'projects', 'git', 'index'}
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(data, key))
                or data.get('format') != 'nexus-heartbeat-v1' or data.get('machine') != cfg['machine']
                or type(data.get('timestamp')) is not int or data['timestamp'] < 0
                or not isinstance(data.get('errors'), list) or any(item not in allowed for item in data['errors'])
                or not isinstance(data.get('git_running'), bool)
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
                'git_running': data['git_running'], 'index_running': data.get('index_running'),
                'fingerprint_running': data.get('fingerprint_running')}
    except FileNotFoundError:
        return {'state': 'unknown'}
    except (OSError, ValueError, TypeError, AttributeError):
        return {'state': 'unverified'}


def write_heartbeat(cfg: dict, key: bytes, notes: list[str], git_errors: bool, git_running: bool,
                    index_running: bool | None = None, *, fingerprint_running: bool | None = None) -> None:
    """Persist only coarse health categories, never notes or private content."""
    categories = {'Снимок отложен:': 'snapshot', 'Пакет выбранных файлов отложен:': 'secrets',
                  'Получение отложено:': 'receive', 'Проверка проектов отложена:': 'projects',
                  'Индексация отложена:': 'index'}
    errors = {category for prefix, category in categories.items() if any(note.startswith(prefix) for note in notes)}
    if git_errors:
        errors.add('git')
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
            'errors': sorted(errors), 'git_running': bool(git_running)}
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


def secret_preparation_status(cfg: dict, key: bytes) -> tuple[int, int]:
    """Count authenticated markers only, without reading quarantine contents."""
    base = cfg['state_dir'] / 'prepared-secrets'
    if base.is_symlink():
        return 0, 1
    if not base.exists():
        return 0, 0
    if not base.is_dir():
        return 0, 1
    valid = invalid = 0
    try:
        for peer in sorted(cfg['peer_roots']):
            parent = base / peer
            if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
                invalid += 1
                continue
            if not parent.exists():
                continue
            for entry in sorted(parent.iterdir()):
                if not re.fullmatch(r'[0-9a-f]{32}', entry.name):
                    continue
                try:
                    marker = entry / 'prepared.json'
                    if entry.is_symlink() or not entry.is_dir() or marker.is_symlink() or not marker.is_file():
                        raise SyncError('Некорректная отметка')
                    data = json.loads(marker.read_text(encoding='utf-8'))
                    tag = data.get('authentication') if isinstance(data, dict) else None
                    if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(data, key))
                            or data.get('format') != 'nexus-prepared-secrets-v2'
                            or data.get('peer') != peer or data.get('package') != entry.name + '.nxs'):
                        raise SyncError('Некорректная отметка')
                    valid += 1
                except (SyncError, OSError, ValueError, TypeError):
                    invalid += 1
    except OSError:
        invalid += 1
    return valid, invalid


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


def outgoing_package_status(cfg: dict, key: bytes) -> dict:
    """Inspect authenticated local markers without claiming package delivery or contents."""
    machine = cfg['machine']
    shared_parts = (cfg['shared'] / 'nexus-sync', cfg['shared'] / 'nexus-sync' / 'secrets',
                    cfg['shared'] / 'nexus-sync' / 'secrets' / machine)
    def valid(marker: Path, expected_format: str) -> bool:
        try:
            if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 65536:
                return False
            data = json.loads(marker.read_text(encoding='utf-8'))
            if not isinstance(data, dict):
                return False
            tag, package, fingerprint = data.get('authentication'), data.get('package'), data.get('fingerprint')
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, _manifest_tag(data, key))
                    or data.get('format') != expected_format or not isinstance(package, str)
                    or not re.fullmatch(r'[0-9a-f]{32}\.nxs', package)
                    or not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint)
                    or any(path.is_symlink() for path in shared_parts)):
                return False
            path = shared_parts[-1] / package
            if path.is_symlink() or not path.is_file():
                return False
            details = path.stat()
            if not len(SECRET_MAGIC) + 28 <= details.st_size <= 24 * 1024 * 1024:
                return False
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            with os.fdopen(fd, 'rb') as stream:
                opened = os.fstat(stream.fileno())
                return ((details.st_dev, details.st_ino) == (opened.st_dev, opened.st_ino)
                        and stream.read(len(SECRET_MAGIC)) == SECRET_MAGIC)
        except (OSError, ValueError, TypeError, AttributeError):
            return False
    secret_enabled = bool(cfg.get('secret_files') or cfg.get('discover_secrets'))
    secret_marker = cfg['state_dir'] / f'last-secrets-{machine}.json'
    if not secret_enabled:
        secret_state = 'disabled'
    elif not secret_marker.exists() and not secret_marker.is_symlink():
        secret_state = 'pending'
    else:
        secret_state = 'marker_verified' if valid(secret_marker, 'nexus-secrets-marker-v1') else 'invalid'
    git_enabled = bool(cfg.get('git_repos') or cfg.get('git_discovery'))
    git_valid = git_invalid = 0
    if git_enabled:
        pattern = re.compile(r'last-git-' + re.escape(machine) + r'-[0-9a-f]{64}\.json')
        for marker in cfg['state_dir'].iterdir():
            if pattern.fullmatch(marker.name):
                if valid(marker, 'nexus-git-marker-v1'):
                    git_valid += 1
                else:
                    git_invalid += 1
    return {'secret_state': secret_state, 'git_enabled': git_enabled,
            'git_markers': git_valid, 'git_invalid': git_invalid}


def syncthing_source_config_status(cfg: dict, config_file: Path, folder_id: str) -> dict:
    """Read a Syncthing XML file, without proving the running service uses it."""
    result = {'state': 'UNKNOWN', 'runtime_verified': False, 'observer_send_only': cfg.get('direction') == 'send-only',
              'project_folder_send_only': None, 'project_path_matches': None, 'project_folder_paused': None,
              'other_receive_overlap': None}
    try:
        if not isinstance(folder_id, str) or not folder_id or len(folder_id) > 256 or any(ord(c) < 32 for c in folder_id):
            return result
        if config_file.is_symlink() or not config_file.is_file() or config_file.stat().st_size > 4 * 1024 * 1024:
            return result
        before = config_file.stat()
        fd = os.open(config_file, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                return result
            data = stream.read(4 * 1024 * 1024 + 1)
        after = config_file.stat()
        if (len(data) > 4 * 1024 * 1024 or b'<!DOCTYPE' in data or b'<!ENTITY' in data or
                (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) !=
                (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino)):
            return result
        document = ET.fromstring(data)
        folders = document.findall('folder') if document.tag == 'configuration' else []
        matches = [folder for folder in folders if folder.get('id') == folder_id]
        if len(matches) != 1:
            return result
        nexus = cfg['nexus'].resolve(strict=True)
        if not nexus.is_dir():
            return result
        folder = matches[0]
        project_path = Path(folder.get('path', '')).expanduser()
        if not project_path.is_absolute():
            return result
        project_path = project_path.resolve(strict=False)
        paused_values = folder.findall('paused')
        if len(paused_values) > 1:
            return result
        paused_text = (paused_values[0].text or '').strip().lower() if paused_values else 'false'
        if paused_text not in ('true', 'false'):
            return result
        result['project_path_matches'] = project_path == nexus
        result['project_folder_send_only'] = folder.get('type', 'sendreceive') in ('sendonly', 'readonly')
        result['project_folder_paused'] = paused_text == 'true'
        overlap = False
        for other in folders:
            if other is folder:
                continue
            paused_nodes = other.findall('paused')
            if len(paused_nodes) > 1:
                return result
            paused = (paused_nodes[0].text or '').strip().lower() if paused_nodes else 'false'
            if paused not in ('true', 'false'):
                return result
            if paused == 'true' or other.get('type', 'sendreceive') in ('sendonly', 'readonly'):
                continue
            other_path = Path(other.get('path', '')).expanduser()
            if not other_path.is_absolute():
                return result
            other_path = other_path.resolve(strict=False)
            if other_path == nexus or other_path in nexus.parents or nexus in other_path.parents:
                overlap = True
                break
        result['other_receive_overlap'] = overlap
        result['state'] = ('CONFIG_MATCH' if result['observer_send_only'] and result['project_folder_send_only']
                           and result['project_path_matches'] and paused_text == 'false' and not overlap else 'ATTENTION')
    except (OSError, ValueError, TypeError, ET.ParseError, RuntimeError):
        pass
    return result


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
    secret_records, secret_invalid = secret_preparation_status(cfg, key)
    result['prepared_secret_records'] = secret_records
    result['prepared_secret_invalid'] = secret_invalid
    if secret_records:
        result['message'] += f'; отметок пакетов файлов: {secret_records}, содержимое не проверено, в проекты не применены'
    if secret_invalid:
        result['message'] += f'; неподтверждённых отметок пакетов файлов: {secret_invalid}'
    outgoing = outgoing_package_status(cfg, key)
    result['outgoing_packages'] = outgoing
    result['outgoing_package_invalid'] = outgoing['git_invalid'] + int(outgoing['secret_state'] == 'invalid')
    if outgoing['secret_state'] == 'marker_verified':
        result['message'] += '; отметка исходящего пакета выбранных файлов подтверждена, содержимое и доставка не проверены'
    elif outgoing['secret_state'] == 'pending':
        result['message'] += '; отметка исходящего пакета выбранных файлов пока отсутствует'
    elif outgoing['secret_state'] == 'invalid':
        result['message'] += '; отметка исходящего пакета выбранных файлов не подтверждена'
    if outgoing['git_enabled']:
        result['message'] += (f"; подтверждённых отметок исходящих Git-пакетов: {outgoing['git_markers']}, "
                              f"неподтверждённых: {outgoing['git_invalid']}; содержимое и доставка не проверены")
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
    selected = raw.get('secret_files', [])
    if not isinstance(selected, list) or any(not isinstance(item, str) or not item for item in selected):
        raise SyncError('secret_files должен быть списком явно выбранных файлов')
    secret_files = [Path(item).expanduser() for item in selected]
    if any(not item.is_absolute() for item in secret_files):
        raise SyncError('Для выбранных файлов нужны абсолютные локальные пути')
    validate_secret_paths(secret_files, paths['key_file'], paths['shared'], paths['nexus'])
    discover_secrets = raw.get('discover_secrets', False)
    if not isinstance(discover_secrets, bool):
        raise SyncError('discover_secrets должен быть true или false')
    repositories = raw.get('git_repos', [])
    if not isinstance(repositories, list) or any(not isinstance(item, str) or not item for item in repositories):
        raise SyncError('git_repos должен быть списком явно выбранных репозиториев')
    git_repos = [Path(item).expanduser() for item in repositories]
    seen = set()
    for repo in git_repos:
        if not repo.is_absolute() or repo.is_symlink():
            raise SyncError('Для репозитория нужен абсолютный локальный путь')
        resolved = repo.resolve(strict=False)
        if resolved != paths['nexus'] and paths['nexus'] not in resolved.parents:
            raise SyncError('Репозиторий находится вне Nexus')
        if resolved == paths['shared'] or paths['shared'] in resolved.parents:
            raise SyncError('Репозиторий не должен находиться в папке обмена')
        name = str(resolved).casefold()
        if name in seen or (repo.exists() and not repo.is_dir()):
            raise SyncError('Репозиторий указан повторно или не является каталогом')
        seen.add(name)
    git_discovery = raw.get('git_discovery', False)
    if not isinstance(git_discovery, bool):
        raise SyncError('git_discovery должен быть true или false')
    prepare_secrets = raw.get('prepare_secrets', False)
    if not isinstance(prepare_secrets, bool):
        raise SyncError('prepare_secrets должен быть true или false')
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
    return {'machine': raw['machine'], **paths, 'interval_seconds': interval, 'direction': direction, 'peer_roots': dict(peer_roots), 'secret_files': secret_files, 'discover_secrets': discover_secrets, 'git_repos': git_repos, 'git_discovery': git_discovery, 'prepare_secrets': prepare_secrets, 'auto_index_preview_dir': preview_path, 'auto_index_codex_executable': executable_path, 'auto_index_combine_peers': combine_peers}


def discover_git_repos(nexus: Path, shared: Path | None = None) -> list[Path]:
    """Discover real Git worktree roots without following symlinks."""
    root = nexus.resolve(strict=True)
    excluded_share = shared.resolve(strict=True) if shared is not None else None
    if not root.is_dir():
        raise SyncError('Корень Nexus должен быть каталогом')
    if excluded_share is not None and (excluded_share == root or excluded_share in root.parents):
        raise SyncError('Корень Nexus не должен находиться в папке обмена')
    def failed(error):
        raise SyncError('Не удалось прочитать каталог проектов') from error
    repositories = []
    for current, directories, _files in os.walk(root, followlinks=False, onerror=failed):
        folder = Path(current)
        directories[:] = sorted(name for name in directories
            if name not in EXCLUDED and not (folder / name).is_symlink()
            and (excluded_share is None or (folder / name).resolve() != excluded_share))
        marker = folder / '.git'
        if marker.is_symlink() or not (marker.is_dir() or marker.is_file()):
            continue
        try:
            result = subprocess.run(['git', '-C', str(folder), '-c', 'core.fsmonitor=false',
                'rev-parse', '--show-toplevel'], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}, check=False)
        except FileNotFoundError as exc:
            raise SyncError('Git не найден') from exc
        if result.returncode == 0:
            top = Path(os.fsdecode(result.stdout.rstrip(b'\n'))).resolve(strict=False)
            if top == folder.resolve(strict=True):
                repositories.append(folder)
    return repositories


def git_cycle(cfg: dict) -> list[str]:
    notes = []
    repositories = list(cfg['git_repos'])
    if cfg.get('git_discovery', False):
        try:
            repositories.extend(discover_git_repos(cfg['nexus'], cfg['shared']))
        except (SyncError, OSError, ValueError) as exc:
            notes.append('Поиск Git-репозиториев отложен: ' + type(exc).__name__)
    seen = set()
    portable_names = {}
    reported_conflicts = set()
    for repo in repositories:
        try:
            resolved = repo.resolve(strict=False)
            try:
                details = resolved.stat()
                identity = ('inode', details.st_dev, details.st_ino) if details.st_ino else ('path', os.path.normcase(str(resolved)))
            except OSError:
                identity = ('path', os.path.normcase(str(resolved)))
            if identity in seen:
                continue
            seen.add(identity)
            try:
                relative = resolved.relative_to(cfg['nexus']).as_posix()
                portable = unicodedata.normalize('NFC', relative).casefold()
                if portable in portable_names and portable_names[portable] != identity and portable not in reported_conflicts:
                    notes.append('Git-пути различаются регистром или нормализацией Unicode; на другой ОС возможен конфликт')
                    reported_conflicts.add(portable)
                portable_names[portable] = identity
            except ValueError:
                pass  # The normal repository validation reports out-of-root paths.
            if publish_git_once(cfg, repo, read_key(cfg['key_file'], cfg['shared'])):
                notes.append('Создан зашифрованный Git-пакет; доставка не подтверждена')
        except (SyncError, OSError, ValueError, TypeError) as exc:
            notes.append('Git-пакет отложен: ' + type(exc).__name__)
    return notes


_INLINE_FINGERPRINT = object()


def supervisor_once(cfg: dict, previous: str | None, *, include_git: bool = True,
                    fingerprint_override: str | None | object = _INLINE_FINGERPRINT) -> tuple[str | None, list[str]]:
    notes = []
    try:
        if publish_secrets_once(cfg, read_key(cfg['key_file'], cfg['shared'])):
            notes.append('Создан локальный зашифрованный пакет выбранных файлов; доставка не подтверждена')
    except (SyncError, OSError, ValueError, TypeError) as exc:
        notes.append('Пакет выбранных файлов отложен: ' + type(exc).__name__)
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
        try:
            prepared_secrets = prepare_received_secrets(cfg, read_key(cfg['key_file'], cfg['shared']))
            if prepared_secrets:
                notes.append(f'Подготовлено пакетов выбранных файлов: {prepared_secrets}; в проекты не применены')
        except (SyncError, OSError, ValueError, TypeError) as exc:
            notes.append('Получение отложено: ' + type(exc).__name__)
    try:
        current = inventory(cfg['nexus'])
        project_key = read_key(cfg['key_file'], cfg['shared'])
        changes = record_projects(cfg['nexus'], cfg['shared'], cfg['state_dir'], cfg['machine'], key=project_key, current=current)
        write_project_report(cfg['shared'], cfg['state_dir'], cfg['machine'], current,
                             key=project_key, include_peers=cfg['direction'] == 'send-receive')
        if changes:
            notes.append(f'В журнал добавлено изменений манифестов: {len(changes)}')
    except (SyncError, OSError, ValueError, KeyError, TypeError) as exc:
        notes.append('Проверка проектов отложена: ' + type(exc).__name__)
    if include_git:
        notes.extend(git_cycle(cfg))
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
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='nexus-git') as executor, \
         ThreadPoolExecutor(max_workers=1, thread_name_prefix='nexus-index') as index_executor, \
         ThreadPoolExecutor(max_workers=1, thread_name_prefix='nexus-fingerprint') as fingerprint_executor:
        git_future = None
        git_errors = False
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
                previous, notes = supervisor_once(cfg, previous, include_git=False, fingerprint_override=fingerprint)
                if fingerprint is not None:
                    snapshot_error = any(note.startswith('Снимок отложен:') for note in notes)
                if (fingerprint_error or snapshot_error) and not any(note.startswith('Снимок отложен:') for note in notes):
                    notes.append('Снимок отложен: SyncError')
                if fingerprint_future is None:
                    fingerprint_future = fingerprint_executor.submit(_source_fingerprint_cycle, cfg)
            else:
                previous, notes = supervisor_once(cfg, previous, include_git=False)
            for note in notes:
                print(note, flush=True)
            if git_future is not None and git_future.done():
                try:
                    git_notes = git_future.result()
                    git_errors = any('отложен' in note for note in git_notes)
                    for note in git_notes:
                        print(note, flush=True)
                except Exception as exc:
                    git_errors = True
                    print('Проверка Git отложена: ' + type(exc).__name__, file=sys.stderr, flush=True)
                git_future = None
            if (cfg['git_repos'] or cfg.get('git_discovery', False)) and git_future is None:
                git_future = executor.submit(git_cycle, cfg)
            index_future, index_errors = collect_index(index_future, index_errors)
            if cfg.get('auto_index_preview_dir') is not None and index_future is None:
                index_future = index_executor.submit(auto_index_cycle, cfg)
            index_future, index_errors = collect_index(index_future, index_errors)
            if index_errors:
                notes.append('Индексация отложена: SyncError')
            try:
                if sys.platform == 'win32':
                    write_heartbeat(cfg, read_key(cfg['key_file'], cfg['shared']), notes, git_errors,
                                    git_future is not None and not git_future.done(), index_future is not None,
                                    fingerprint_running=fingerprint_future is not None and not fingerprint_future.done())
                else:
                    write_heartbeat(cfg, read_key(cfg['key_file'], cfg['shared']), notes, git_errors,
                                    git_future is not None and not git_future.done(), index_future is not None)
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
    config.add_argument('--secret-file', type=Path, action='append', default=[],
                        help='явно выбранный тестовый файл внутри Nexus; можно повторять')
    config.add_argument('--discover-secrets', action='store_true',
                        help='искать тестовые файлы по правилам образца исключений')
    config.add_argument('--prepare-secrets', action='store_true',
                        help='готовить полученные тестовые пакеты в локальном карантине')
    config.add_argument('--auto-index-preview-dir', type=Path,
                        help='отдельный локальный каталог для автоматических тестовых копий')
    config.add_argument('--auto-index-codex-executable', type=Path,
                        help='проверенный исполняемый Codex для тестового app-server')
    config.add_argument('--auto-index-combine-peers', action='store_true',
                        help='объединять доступные снимки устройств в одну тестовую копию')
    config.add_argument('--git-repo', type=Path, action='append', default=[],
                        help='корень выбранного тестового Git-репозитория внутри Nexus; можно повторять')
    config.add_argument('--discover-git', action='store_true',
                        help='искать тестовые Git-репозитории внутри Nexus при каждом цикле')
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
    secretpack = sub.add_parser('secrets-pack', help='зашифровать явно выбранные файлы')
    secretpack.add_argument('--shared', type=Path, required=True)
    secretpack.add_argument('--machine', required=True)
    secretpack.add_argument('--key-file', type=Path, required=True)
    secretpack.add_argument('files', type=Path, nargs='+')
    secretunpack = sub.add_parser('secrets-extract', help='распаковать секреты в новый локальный каталог')
    secretunpack.add_argument('bundle', type=Path)
    secretunpack.add_argument('--destination', type=Path, required=True)
    secretunpack.add_argument('--key-file', type=Path, required=True)
    secretunpack.add_argument('--shared', type=Path, required=True)
    packages = sub.add_parser('packages', help='список проверенных типов зашифрованных пакетов')
    packages.add_argument('--config', type=Path, required=True)
    discoversecrets = sub.add_parser('discover-secrets', help='показать имена тестовых файлов для проверки перед включением')
    discoversecrets.add_argument('--config', type=Path, required=True)
    secretplan = sub.add_parser('plan-secrets', help='сравнить карантин тестового пакета с локальным проектом')
    secretplan.add_argument('--config', type=Path, required=True)
    secretplan.add_argument('--source', required=True)
    secretplan.add_argument('--package', required=True)
    git = sub.add_parser('git-pack', help='зашифровать Git bundle --all без отправки в remote')
    git.add_argument('--repo', type=Path, required=True)
    git.add_argument('--shared', type=Path, required=True)
    git.add_argument('--machine', required=True)
    git.add_argument('--key-file', type=Path, required=True)
    gitpreview = sub.add_parser('git-preview', help='подготовить новый тестовый Git-репозиторий из пакета')
    gitpreview.add_argument('bundle', type=Path)
    gitpreview.add_argument('--config', type=Path, required=True)
    gitpreview.add_argument('--destination', type=Path, required=True)
    gitcheck = sub.add_parser('git-status', help='число незакоммиченных изменений и состояние относительно upstream')
    gitcheck.add_argument('--repo', type=Path, required=True)
    gitdiscover = sub.add_parser('discover-git', help='показать Git-репозитории внутри Nexus без упаковки')
    gitdiscover.add_argument('--nexus', type=Path, required=True)
    gitdiscover.add_argument('--shared', type=Path, help='папка обмена, исключаемая из поиска')
    state = sub.add_parser('status', help='локальный статус снимков в формате JSON')
    state.add_argument('--shared', type=Path, required=True)
    state.add_argument('--machine', required=True)
    state.add_argument('--key-file', type=Path)
    summary = sub.add_parser('summary', help='локальные статусы по конфигурации устройства')
    summary.add_argument('--config', type=Path, required=True)
    summary.add_argument('--mock-preview-root', type=Path, help='дополнительно проверить отметки моковых чатов в /tmp')
    summary.add_argument('--mock-chat-profile', type=Path, help='проверить состояние мокового приёмника в /tmp')
    summary.add_argument('--syncthing-config', type=Path, help='только читать XML-конфигурацию Syncthing')
    summary.add_argument('--mock-syncthing-config', type=Path)
    summary.add_argument('--mock-syncthing-folder', type=Path)
    summary.add_argument('--mock-syncthing-id')
    summary.add_argument('--mock-syncthing-port', type=int)
    summary.add_argument('--syncthing-project-folder', help='ID папки проектов в Syncthing')
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
    scan = sub.add_parser('projects', help='сравнить манифесты проектов')
    scan.add_argument('--nexus', type=Path, required=True)
    scan.add_argument('--previous', type=Path)
    scan.add_argument('--save', type=Path, help='путь для нового снимка (только новый файл)')
    record = sub.add_parser('projects-record', help='записать изменения манифестов в общую папку')
    record.add_argument('--nexus', type=Path, required=True)
    record.add_argument('--shared', type=Path, required=True)
    record.add_argument('--state-dir', type=Path, required=True)
    record.add_argument('--machine', required=True)
    record.add_argument('--key-file', type=Path, required=True)
    log = sub.add_parser('projects-log', help='показать общий журнал изменений проектов')
    log.add_argument('--shared', type=Path, required=True)
    log.add_argument('--key-file', type=Path, required=True)
    log.add_argument('--include-legacy', action='store_true', help='включить старые открытые и непроверенные события')
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
                    'secret_files': [str(path.expanduser()) for path in args.secret_file],
                    'discover_secrets': args.discover_secrets,
                    'prepare_secrets': args.prepare_secrets,
                    'auto_index_preview_dir': str(args.auto_index_preview_dir.expanduser()) if args.auto_index_preview_dir else None,
                    'auto_index_codex_executable': str(args.auto_index_codex_executable.expanduser()) if args.auto_index_codex_executable else None,
                    'auto_index_combine_peers': args.auto_index_combine_peers,
                    'git_repos': [str(path.expanduser()) for path in args.git_repo],
                    'git_discovery': args.discover_git}
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
        elif args.command == 'secrets-pack':
            key = read_key(args.key_file, args.shared)
            if args.key_file.resolve() in [path.resolve() for path in args.files]:
                raise SyncError('Ключ шифрования нельзя включать в пакет')
            output = secrets_pack(args.files, args.shared, args.machine, key)
            print(f'Зашифрованный пакет создан: {output}; доставка не подтверждена.')
        elif args.command == 'secrets-extract':
            count = secrets_extract(args.bundle, args.destination, read_key(args.key_file, args.shared), args.shared)
            print(f'В новый локальный каталог распаковано файлов: {count}.')
        elif args.command == 'packages':
            cfg = load_settings(args.config)
            print(json.dumps(list_encrypted_packages(cfg, read_key(cfg['key_file'], cfg['shared'])), ensure_ascii=False, indent=2))
        elif args.command == 'discover-secrets':
            cfg = load_settings(args.config)
            found = discover_secret_files(cfg['nexus'], (cfg['shared'], cfg['codex_home']))
            print(json.dumps([path.relative_to(cfg['nexus']).as_posix() for path in found], ensure_ascii=True, indent=2))
        elif args.command == 'plan-secrets':
            cfg = load_settings(args.config)
            print(json.dumps(plan_prepared_secrets(cfg, read_key(cfg['key_file'], cfg['shared']), args.source, args.package), ensure_ascii=True, indent=2))
        elif args.command == 'git-pack':
            output = git_pack(args.repo, args.shared, args.machine, read_key(args.key_file, args.shared))
            print(f'Зашифрованный Git-пакет создан: {output}; индекс сохранён как патч, рабочие изменения передаются отдельно.')
        elif args.command == 'git-preview':
            cfg = load_settings(args.config)
            git_preview(args.bundle, args.destination, cfg, read_key(cfg['key_file'], cfg['shared']))
            print('Тестовый Git-репозиторий подготовлен в новом каталоге; проект не изменён.')
        elif args.command == 'git-status':
            print(json.dumps(git_status(args.repo), ensure_ascii=False))
        elif args.command == 'discover-git':
            root = args.nexus.resolve(strict=True)
            print(json.dumps([path.relative_to(root).as_posix() for path in discover_git_repos(root, args.shared)], ensure_ascii=False, indent=2))
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
            if bool(args.syncthing_config) != bool(args.syncthing_project_folder):
                raise SyncError('Нужны оба параметра проверки Syncthing')
            if args.syncthing_config is not None:
                result['syncthing_source_config'] = syncthing_source_config_status(
                    cfg, args.syncthing_config, args.syncthing_project_folder)
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
        elif args.command == 'projects-record':
            changes = record_projects(args.nexus, args.shared, args.state_dir, args.machine, key=read_key(args.key_file, args.shared))
            print('\n'.join(changes) if changes else 'Новых изменений манифестов нет; базовый снимок сохранён.')
        elif args.command == 'projects-log':
            events = project_log(args.shared, read_key(args.key_file, args.shared), include_legacy=args.include_legacy)
            if args.include_legacy:
                print('Внимание: старые открытые события не аутентифицированы.', file=sys.stderr)
            for event in events:
                print(f"{event['created']} — {event['machine']}")
                for change in event['changes']:
                    print(f'  {change}')
            if not events:
                print('Журнал пока пуст.')
        else:
            current = inventory(args.nexus)
            if args.previous:
                previous = json.loads(args.previous.read_text(encoding='utf-8'))
                print('\n'.join(compare(previous, current)) or 'Изменений манифестов нет.')
            else:
                print(f"Найдено проектов: {len(current['projects'])}. Первый снимок — базовый.")
            for action in project_actions(current):
                print(f"{action['project']}: {action['reason']}. {action['note']}" + (f"; предложение: {action['command']}" if action['command'] else ''))
                for step in action.get('steps', []):
                    print('  Для проверки вручную в каталоге проекта: ' + step)
            for issue in current.get('portability', []):
                print('Проверить переносимость: ' + json.dumps(issue, ensure_ascii=True))
            for issue in current.get('sample_exclusions', []):
                print('Проверить образец исключений: ' + json.dumps(issue, ensure_ascii=True))
            if args.save:
                with args.save.open('x', encoding='utf-8') as output:
                    json.dump(current, output, ensure_ascii=False, sort_keys=True, indent=2)
                    output.write('\n')
                print(f'Сохранён снимок: {args.save}')
        return 0
    except KeyboardInterrupt:
        print('\nОстановлено.')
        return 130
    except (SyncError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f'Ошибка: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
