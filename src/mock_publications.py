#!/usr/bin/env python3
"""Ordered authenticated mock package publications; no real workspace writes."""
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import uuid

import mock_auto_receiver
import mock_chat_sync as mock
import nexus_sync as ns


def _stream(kind: str, name: str, key: bytes) -> str:
    if kind not in ('git', 'secret', 'secret-set') or len(key) != 32:
        raise ns.SyncError('Некорректный поток тестовых пакетов')
    if kind == 'secret-set' and name != 'all':
        raise ns.SyncError('Некорректный поток набора файлов')
    ns._secret_parts(name, nested=True)
    return hmac.new(key, b'nexus-mock-publication-stream-v1\0' + kind.encode() + b'\0' + name.encode('utf-8'),
                    hashlib.sha256).hexdigest()


def _extracted_secret_names(contents: Path, key: bytes) -> list[str]:
    names = []
    for current, dirs, files in os.walk(contents, followlinks=False):
        if any((Path(current) / part).is_symlink() for part in dirs + files):
            raise ns.SyncError('Ссылка в тестовом пакете')
        for part in files:
            relative = (Path(current) / part).relative_to(contents).as_posix()
            _stream('secret', relative, key)
            names.append(relative)
            if len(names) > 16:
                raise ns.SyncError('Слишком много моковых файлов')
    if not names:
        raise ns.SyncError('Пустой тестовый пакет')
    return sorted(names)


def _folder(shared: Path, source: str, stream: str, *, create: bool) -> Path:
    shared = mock._mock_path(shared)
    ns._safe_machine(source)
    if not shared.is_dir() or not re.fullmatch(r'[0-9a-f]{64}', stream):
        raise ns.SyncError('Некорректная тестовая папка обмена')
    current = shared
    for part in ('nexus-sync', 'mock-publications', source, stream):
        current = current / part
        if current.is_symlink():
            raise ns.SyncError('Ссылка в пути публикаций запрещена')
        if create:
            current.mkdir(mode=0o700, exist_ok=True)
        elif not current.is_dir():
            return current
    return current


def _chain(folder: Path, source: str, kind: str, stream: str, key: bytes) -> list[dict]:
    if not folder.exists():
        return []
    if folder.is_symlink() or not folder.is_dir():
        raise ns.SyncError('Некорректная цепочка публикаций')
    entries = {}
    for path in sorted(folder.iterdir()):
        if path.name.startswith('.'):
            continue
        if (path.is_symlink() or not path.is_file() or not re.fullmatch(r'[0-9a-f]{32}\.json', path.name)
                or path.stat().st_size > 65536 or len(entries) >= 10000):
            raise ns.SyncError('Неподдерживаемая запись цепочки')
        item = json.loads(path.read_text(encoding='utf-8'))
        tag = item.get('authentication') if isinstance(item, dict) else None
        empty = isinstance(item, dict) and item.get('empty') is True
        valid_package = (kind == 'secret-set' and empty and item.get('package') is None and item.get('package_tag') is None
                         or not empty and isinstance(item, dict) and item.get('empty') in (None, False)
                         and re.fullmatch(r'[0-9a-f]{32}\.nxs', str(item.get('package', '')))
                         and re.fullmatch(r'[0-9a-f]{64}', str(item.get('package_tag', ''))))
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
                or item.get('format') != 'nexus-mock-publication-v1' or item.get('source') != source
                or item.get('kind') != kind or item.get('stream') != stream or item.get('id') != path.stem
                or type(item.get('ordinal')) is not int or item['ordinal'] < 1
                or 'previous' not in item
                or (item['previous'] is not None and
                    (not isinstance(item['previous'], str) or not re.fullmatch(r'[0-9a-f]{32}', item['previous'])))
                or not valid_package):
            raise ns.SyncError('Запись тестовой публикации не подтверждена')
        entries[item['id']] = item
    if not entries:
        return []
    roots = [item for item in entries.values() if item['previous'] is None]
    children = {}
    for item in entries.values():
        previous = item['previous']
        if previous is not None:
            if previous not in entries or previous in children:
                raise ns.SyncError('Цепочка публикаций неполна или разветвлена')
            children[previous] = item
    if len(roots) != 1:
        raise ns.SyncError('Цепочка публикаций неполна или разветвлена')
    result = []
    seen = set()
    current = roots[0]
    while current is not None:
        if current['id'] in seen or current['ordinal'] != len(result) + 1:
            raise ns.SyncError('Неверный порядок публикаций')
        seen.add(current['id'])
        result.append(current)
        current = children.get(current['id'])
    if len(result) != len(entries):
        raise ns.SyncError('Цепочка публикаций неполна')
    return result


def _validate_package(package: Path, shared: Path, source: str, kind: str, name: str, key: bytes) -> str:
    base = shared / 'nexus-sync' / 'secrets' / source
    if (any(path.is_symlink() for path in (shared / 'nexus-sync', base.parent, base, package))
            or package.parent != base or not re.fullmatch(r'[0-9a-f]{32}\.nxs', package.name)):
        raise ns.SyncError('Тестовый пакет находится вне своего источника')
    tag = mock_auto_receiver._package_tag(package, key)
    with tempfile.TemporaryDirectory(prefix='nexus-mock-publication-') as tmp:
        contents = Path(tmp) / 'contents'
        ns.secrets_extract(package, contents, key, shared)
        if kind == 'git':
            info = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
            if (not isinstance(info, dict) or info.get('format') != 'nexus-git-package-v1'
                    or info.get('project') != name or
                    {p.name for p in contents.iterdir()} != {'repository.bundle', 'repository.json', 'staged.patch'}):
                raise ns.SyncError('Git-пакет относится к другому проекту')
        elif kind == 'secret':
            target = contents.joinpath(*ns._secret_parts(name, nested=True))
            if target.is_symlink() or not target.is_file():
                raise ns.SyncError('Выбранного файла нет в тестовом пакете')
        elif kind == 'secret-set':
            _extracted_secret_names(contents, key)
        else:
            raise ns.SyncError('Некорректный тип тестового пакета')
    if mock_auto_receiver._package_tag(package, key) != tag:
        raise ns.SyncError('Тестовый пакет изменился')
    return tag


def publish(shared: Path, source: str, kind: str, name: str, package: Path, key: bytes) -> dict:
    shared = mock._mock_path(shared)
    stream = _stream(kind, name, key)
    folder = _folder(shared, source, stream, create=True)
    chain = _chain(folder, source, kind, stream, key)
    tag = _validate_package(package, shared, source, kind, name, key)
    if chain and chain[-1]['package'] == package.name and chain[-1]['package_tag'] == tag:
        return {'state': 'UNCHANGED', 'mock_only': True, 'ordinal': chain[-1]['ordinal']}
    identity = uuid.uuid4().hex
    item = {'format': 'nexus-mock-publication-v1', 'source': source, 'kind': kind, 'stream': stream,
            'id': identity, 'ordinal': len(chain) + 1, 'previous': chain[-1]['id'] if chain else None,
            'package': package.name, 'package_tag': tag}
    item['authentication'] = ns._manifest_tag(item, key)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.publication-', dir=folder, delete=False) as out:
        temporary = Path(out.name)
        try:
            json.dump(item, out, sort_keys=True)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.link(temporary, folder / (identity + '.json'))
    finally:
        temporary.unlink(missing_ok=True)
    return {'state': 'PUBLISHED', 'mock_only': True, 'ordinal': item['ordinal']}


def publish_marker(shared: Path, state: Path, source: str, kind: str, name: str, key: bytes) -> dict:
    """Publish the current authenticated sender marker from an isolated mock state."""
    shared, state = mock._mock_path(shared), mock._mock_path(state)
    ns._safe_machine(source)
    _stream(kind, name, key)
    if state.is_symlink() or not state.is_dir():
        raise ns.SyncError('Некорректное тестовое состояние отправителя')
    if kind == 'git':
        identity = hmac.new(key, b'nexus-git-project-v1\0' + name.encode('utf-8'), hashlib.sha256).hexdigest()
        marker = state / f'last-git-{source}-{identity}.json'
        expected_format = 'nexus-git-marker-v1'
    else:
        marker = state / f'last-secrets-{source}.json'
        expected_format = 'nexus-secrets-marker-v1'
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 65536:
        raise ns.SyncError('Тестовая отметка отправителя недоступна')
    encoded = marker.read_bytes()
    item = json.loads(encoded)
    tag = item.get('authentication') if isinstance(item, dict) else None
    package = item.get('package') if isinstance(item, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
            or item.get('format') != expected_format or not isinstance(package, str)
            or not re.fullmatch(r'[0-9a-f]{32}\.nxs', package)):
        raise ns.SyncError('Тестовая отметка отправителя не подтверждена')
    result = publish(shared, source, kind, name, shared / 'nexus-sync' / 'secrets' / source / package, key)
    if marker.is_symlink() or marker.read_bytes() != encoded:
        raise ns.SyncError('Отметка изменилась во время публикации; повторите проверку')
    return result


def publish_all_git_markers(shared: Path, state: Path, source: str, key: bytes) -> dict:
    """Publish authenticated mock Git markers without exposing project names."""
    shared, state = mock._mock_path(shared), mock._mock_path(state)
    ns._safe_machine(source)
    if not shared.is_dir() or not state.is_dir():
        raise ns.SyncError('Тестовая папка недоступна')
    prefix = f'last-git-{source}-'
    markers = sorted(path for path in state.iterdir() if path.name.startswith(prefix) and path.name.endswith('.json'))
    if len(markers) > 1024:
        raise ns.SyncError('Слишком много тестовых Git-отметок')
    result = {'state': 'UNCHANGED', 'mock_only': True, 'published': 0, 'invalid': 0}
    for marker in markers:
        try:
            if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 65536:
                raise ns.SyncError('Некорректная Git-отметка')
            encoded = marker.read_bytes()
            item = json.loads(encoded)
            tag = item.get('authentication') if isinstance(item, dict) else None
            name = item.get('package') if isinstance(item, dict) else None
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
                    or item.get('format') != 'nexus-git-marker-v1' or not isinstance(name, str)
                    or not re.fullmatch(r'[0-9a-f]{32}\.nxs', name)):
                raise ns.SyncError('Git-отметка не подтверждена')
            package = shared / 'nexus-sync' / 'secrets' / source / name
            if any(path.is_symlink() for path in (shared / 'nexus-sync', package.parent.parent, package.parent, package)):
                raise ns.SyncError('Ссылка в пути пакета')
            with tempfile.TemporaryDirectory(prefix='nexus-mock-git-discovery-') as tmp:
                contents = Path(tmp) / 'contents'
                ns.secrets_extract(package, contents, key, shared)
                info = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
                project = info.get('project') if isinstance(info, dict) else None
                if not isinstance(project, str) or info.get('format') != 'nexus-git-package-v1':
                    raise ns.SyncError('Некорректный тестовый Git-пакет')
                _stream('git', project, key)
                identity = hmac.new(key, b'nexus-git-project-v1\0' + project.encode('utf-8'), hashlib.sha256).hexdigest()
                if marker.name != prefix + identity + '.json' or marker.read_bytes() != encoded:
                    raise ns.SyncError('Git-отметка изменилась или относится к другому проекту')
                published = publish_marker(shared, state, source, 'git', project, key)
                result['published'] += published['state'] == 'PUBLISHED'
        except (ns.SyncError, OSError, ValueError, TypeError, AttributeError, KeyError):
            result['invalid'] += 1
    if result['invalid']:
        result['state'] = 'ATTENTION'
    elif result['published']:
        result['state'] = 'PUBLISHED'
    return result


def publish_all_secret_marker(shared: Path, state: Path, source: str, key: bytes) -> dict:
    """Publish each file of one authenticated test package without displaying names."""
    shared, state = mock._mock_path(shared), mock._mock_path(state)
    ns._safe_machine(source)
    if not shared.is_dir() or not state.is_dir():
        raise ns.SyncError('Тестовая папка недоступна')
    marker = state / f'last-secrets-{source}.json'
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 65536:
        raise ns.SyncError('Тестовая отметка отправителя недоступна')
    encoded = marker.read_bytes()
    item = json.loads(encoded)
    tag = item.get('authentication') if isinstance(item, dict) else None
    name = item.get('package') if isinstance(item, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
            or item.get('format') != 'nexus-secrets-marker-v1' or not isinstance(name, str)
            or not re.fullmatch(r'[0-9a-f]{32}\.nxs', name)):
        raise ns.SyncError('Тестовая отметка отправителя не подтверждена')
    package = shared / 'nexus-sync' / 'secrets' / source / name
    package_tag = mock_auto_receiver._package_tag(package, key)
    set_stream = _stream('secret-set', 'all', key)
    set_folder = _folder(shared, source, set_stream, create=False)
    set_chain = _chain(set_folder, source, 'secret-set', set_stream, key)
    if set_chain and set_chain[-1].get('empty') is True:
        previous = next((entry for entry in reversed(set_chain) if entry.get('empty') is not True), None)
        if previous and previous['package'] == name and previous['package_tag'] == package_tag:
            if marker.is_symlink() or marker.read_bytes() != encoded:
                raise ns.SyncError('Отметка изменилась; публикация будет повторена')
            return {'state': 'UNCHANGED', 'mock_only': True, 'published_count': 0,
                    'file_count': 0, 'set_published': False}
    with tempfile.TemporaryDirectory(prefix='nexus-mock-publication-list-') as tmp:
        extracted = Path(tmp) / 'files'
        ns.secrets_extract(package, extracted, key, shared)
        names = _extracted_secret_names(extracted, key)
        if mock_auto_receiver._package_tag(package, key) != package_tag:
            raise ns.SyncError('Тестовый пакет изменился или пуст')
        published = 0
        for relative in sorted(names):
            if marker.is_symlink() or marker.read_bytes() != encoded:
                raise ns.SyncError('Отметка изменилась; публикация будет повторена')
            result = publish(shared, source, 'secret', relative, package, key)
            published += result['state'] == 'PUBLISHED'
        if marker.is_symlink() or marker.read_bytes() != encoded:
            raise ns.SyncError('Отметка изменилась; публикация будет повторена')
        set_result = publish(shared, source, 'secret-set', 'all', package, key)
        if marker.is_symlink() or marker.read_bytes() != encoded or mock_auto_receiver._package_tag(package, key) != package_tag:
            raise ns.SyncError('Пакет или отметка изменились; публикация будет повторена')
    return {'state': 'PUBLISHED' if published or set_result['state'] == 'PUBLISHED' else 'UNCHANGED',
            'mock_only': True, 'published_count': published, 'file_count': len(names),
            'set_published': set_result['state'] == 'PUBLISHED'}


def publish_empty_secret_set(shared: Path, source: str, key: bytes) -> dict:
    """Explicitly publish an empty test set after an existing authenticated set."""
    shared = mock._mock_path(shared)
    stream = _stream('secret-set', 'all', key)
    folder = _folder(shared, source, stream, create=False)
    chain = _chain(folder, source, 'secret-set', stream, key)
    if not chain:
        raise ns.SyncError('Пустой набор допускается только после непустого мокового набора')
    if chain[-1].get('empty') is True:
        return {'state': 'UNCHANGED', 'mock_only': True}
    item = {'format': 'nexus-mock-publication-v1', 'source': source, 'kind': 'secret-set', 'stream': stream,
            'id': uuid.uuid4().hex, 'ordinal': len(chain) + 1, 'previous': chain[-1]['id'],
            'empty': True, 'package': None, 'package_tag': None}
    item['authentication'] = ns._manifest_tag(item, key)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', prefix='.publication-', dir=folder, delete=False) as out:
        temporary = Path(out.name)
        try:
            json.dump(item, out, sort_keys=True)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        if _chain(folder, source, 'secret-set', stream, key)[-1]['authentication'] != chain[-1]['authentication']:
            raise ns.SyncError('Цепочка изменилась; повторите публикацию')
        os.link(temporary, folder / (item['id'] + '.json'))
    finally:
        temporary.unlink(missing_ok=True)
    return {'state': 'PUBLISHED', 'mock_only': True}


def detect_empty_secret_set(shared: Path, state: Path, nexus: Path, source: str, key: bytes) -> dict:
    """Publish an empty test set only after a complete name-based mock scan."""
    shared, state, nexus = mock._mock_path(shared), mock._mock_path(state), mock._mock_path(nexus)
    if not shared.is_dir() or not state.is_dir() or not nexus.is_dir() or nexus == shared or nexus in shared.parents:
        raise ns.SyncError('Некорректные моковые папки')
    ns._safe_machine(source)
    unchanged = {'state': 'UNCHANGED', 'mock_only': True}
    selected = select(shared, source, 'secret-set', 'all', key)
    if not selected or selected[0] is None:
        return unchanged
    marker_path = state / f'last-secrets-{source}.json'
    if marker_path.is_symlink() or not marker_path.is_file() or marker_path.stat().st_size > 65536:
        raise ns.SyncError('Тестовая отметка недоступна')
    encoded = marker_path.read_bytes()
    marker = json.loads(encoded)
    tag = marker.get('authentication') if isinstance(marker, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
            or marker.get('format') != 'nexus-secrets-marker-v1'
            or marker.get('package') != selected[0].name):
        raise ns.SyncError('Тестовый набор не соответствует отметке')
    if ns.discover_secret_files(nexus, (shared,)):
        return unchanged
    with tempfile.TemporaryDirectory(prefix='nexus-mock-empty-check-') as tmp:
        contents = Path(tmp) / 'contents'
        ns.secrets_extract(selected[0], contents, key, shared)
        names = _extracted_secret_names(contents, key)
        for name in names:
            current = nexus
            for part in ns._secret_parts(name, nested=True):
                current = current / part
                if current.is_symlink():
                    raise ns.SyncError('Ссылка в тестовом пути')
            if current.exists():
                return unchanged
    if ns.discover_secret_files(nexus, (shared,)) or marker_path.is_symlink() or marker_path.read_bytes() != encoded:
        return unchanged
    latest = select(shared, source, 'secret-set', 'all', key)
    if not latest or latest[1]['authentication'] != selected[1]['authentication']:
        raise ns.SyncError('Тестовый набор изменился')
    return publish_empty_secret_set(shared, source, key)


def select(shared: Path, source: str, kind: str, name: str, key: bytes, *, previous_id: str | None = None,
           previous_authentication: str | None = None) -> tuple[Path | None, dict] | None:
    shared = mock._mock_path(shared)
    stream = _stream(kind, name, key)
    folder = _folder(shared, source, stream, create=False)
    chain = _chain(folder, source, kind, stream, key)
    if (previous_id is None) != (previous_authentication is None):
        raise ns.SyncError('Некорректная контрольная точка')
    if previous_id is not None and not any(item['id'] == previous_id and
                                           hmac.compare_digest(item['authentication'], previous_authentication)
                                           for item in chain):
        raise ns.SyncError('Отсутствует ранее подтверждённая публикация')
    if not chain:
        return None
    selected = chain[-1]
    package = None if selected.get('empty') is True else shared / 'nexus-sync' / 'secrets' / source / selected['package']
    if package is not None and _validate_package(package, shared, source, kind, name, key) != selected['package_tag']:
        raise ns.SyncError('Тестовый пакет не совпал с публикацией')
    after = _chain(folder, source, kind, stream, key)
    if not after or after[-1]['authentication'] != selected['authentication']:
        raise ns.SyncError('Цепочка изменилась во время проверки')
    return package, selected


def main() -> int:
    parser = argparse.ArgumentParser(description='Порядок публикаций только в тестовом обмене')
    parser.add_argument('operation', choices=('publish', 'publish-marker', 'watch-marker', 'select',
                                              'publish-all-git', 'watch-all-git', 'publish-all-secrets',
                                              'watch-all-secrets', 'publish-empty-secret-set'))
    parser.add_argument('--shared', type=Path, required=True)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--kind', choices=('git', 'secret'))
    parser.add_argument('--name', help='относительный путь проекта или файла')
    parser.add_argument('--package', type=Path)
    parser.add_argument('--state', type=Path, help='тестовый каталог состояния отправителя')
    parser.add_argument('--mock-nexus', type=Path, help='тестовый корень для обнаружения пустого набора')
    parser.add_argument('--interval', type=float, default=1.0)
    args = parser.parse_args()
    try:
        mock._mock_path(args.key_file)
        key = ns.read_key(args.key_file, args.shared)
        if args.operation in ('publish-all-git', 'watch-all-git'):
            if args.state is None or args.kind or args.name or args.package or args.mock_nexus:
                raise ns.SyncError('Нужна только тестовая папка состояния')
            safe_state = mock._mock_path(args.state)
            source = ns._safe_machine(args.source)
            if args.operation == 'watch-all-git':
                if not 0.1 <= args.interval <= 60:
                    raise ns.SyncError('Некорректный интервал')
                last_signature = None
                last_checked = 0.0
                last_warning = False
                while True:
                    try:
                        markers = sorted(path for path in safe_state.iterdir()
                                         if path.name.startswith(f'last-git-{source}-') and path.name.endswith('.json'))
                        if len(markers) > 1024:
                            raise ns.SyncError('Слишком много тестовых Git-отметок')
                        signature = tuple((path.name, path.lstat().st_size, path.lstat().st_mtime_ns,
                                           path.lstat().st_ctime_ns) for path in markers)
                        if signature != last_signature or time.monotonic() - last_checked >= 30:
                            result = publish_all_git_markers(args.shared, safe_state, source, key)
                            if not result['invalid']:
                                last_signature, last_checked = signature, time.monotonic()
                                last_warning = False
                            elif not last_warning:
                                print(json.dumps(result), flush=True)
                                last_warning = True
                            if result['state'] == 'PUBLISHED':
                                print(json.dumps(result), flush=True)
                    except (ns.SyncError, OSError, ValueError, TypeError):
                        pass
                    time.sleep(args.interval)
            print(json.dumps(publish_all_git_markers(args.shared, safe_state, source, key)))
            return 0
        if args.operation == 'publish-empty-secret-set':
            if args.kind or args.name or args.package or args.state or args.mock_nexus:
                raise ns.SyncError('Пустой тестовый набор не принимает дополнительные параметры')
            result = publish_empty_secret_set(args.shared, args.source, key)
            print(json.dumps(result))
            return 0
        if args.operation in ('publish-all-secrets', 'watch-all-secrets'):
            if args.state is None or args.kind or args.name or args.package:
                raise ns.SyncError('Нужна только тестовая папка состояния')
            safe_state = mock._mock_path(args.state)
            if args.mock_nexus is not None and args.operation != 'watch-all-secrets':
                raise ns.SyncError('Тестовый корень поддерживается только при наблюдении')
            if args.operation == 'watch-all-secrets':
                if not 0.1 <= args.interval <= 60:
                    raise ns.SyncError('Некорректный интервал')
                last_marker = None
                last_checked = 0.0
                last_empty_check = 0.0
                while True:
                    try:
                        marker = safe_state / f'last-secrets-{ns._safe_machine(args.source)}.json'
                        if marker.is_symlink() or marker.stat().st_size > 65536:
                            raise ns.SyncError('Некорректная отметка')
                        current = marker.read_bytes()
                        if current != last_marker or time.monotonic() - last_checked >= 30:
                            result = publish_all_secret_marker(args.shared, args.state, args.source, key)
                            if marker.read_bytes() == current:
                                last_marker = current
                                last_checked = time.monotonic()
                            if result['state'] == 'PUBLISHED':
                                print(json.dumps(result), flush=True)
                        if args.mock_nexus is not None and time.monotonic() - last_empty_check >= 1:
                            empty = detect_empty_secret_set(args.shared, safe_state, args.mock_nexus, args.source, key)
                            last_empty_check = time.monotonic()
                            if empty['state'] == 'PUBLISHED':
                                print(json.dumps(empty), flush=True)
                    except (ns.SyncError, OSError, ValueError, TypeError):
                        pass
                    time.sleep(args.interval)
            result = publish_all_secret_marker(args.shared, args.state, args.source, key)
            print(json.dumps(result))
            return 0
        if args.mock_nexus is not None or not args.kind or not args.name:
            raise ns.SyncError('Нужны тип и имя тестового потока')
        if args.operation == 'publish':
            if args.package is None:
                raise ns.SyncError('Нужен тестовый пакет')
            result = publish(args.shared, args.source, args.kind, args.name, args.package, key)
        elif args.operation in ('publish-marker', 'watch-marker'):
            if args.state is None:
                raise ns.SyncError('Нужен тестовый каталог состояния')
            if args.operation == 'watch-marker':
                if not 0.1 <= args.interval <= 60:
                    raise ns.SyncError('Некорректный интервал')
                while True:
                    try:
                        result = publish_marker(args.shared, args.state, args.source, args.kind, args.name, key)
                        if result['state'] == 'PUBLISHED':
                            print(json.dumps(result), flush=True)
                    except (ns.SyncError, OSError, ValueError, TypeError):
                        pass
                    time.sleep(args.interval)
            result = publish_marker(args.shared, args.state, args.source, args.kind, args.name, key)
        else:
            result = select(args.shared, args.source, args.kind, args.name, key)
            result = {'state': 'READY', 'mock_only': True, 'ordinal': result[1]['ordinal']} if result else {'state': 'PENDING', 'mock_only': True}
        print(json.dumps(result))
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError):
        print('Тестовая публикация отложена', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
