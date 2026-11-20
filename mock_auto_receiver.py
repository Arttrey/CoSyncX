#!/usr/bin/env python3
"""Install selected or locally available published mock packages only."""
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

import mock_chat_sync as mock
import mock_git_sync
import mock_secret_sync
import nexus_sync as ns


def _package_tag(package: Path, key: bytes) -> str:
    if package.is_symlink() or not package.is_file() or package.stat().st_size > 24 * 1024 * 1024:
        raise ns.SyncError('Тестовый пакет недоступен')
    before = package.stat()
    digest = hmac.new(key, b'nexus-mock-receiver-package-v1\0', hashlib.sha256)
    fd = os.open(package, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ns.SyncError('Тестовый пакет изменился')
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    after = package.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (
            after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino):
        raise ns.SyncError('Тестовый пакет изменился')
    return digest.hexdigest()


def _receiver_state(root: Path, key: bytes) -> Path:
    root_tag = hmac.new(key, b'nexus-mock-receiver-root-v1\0' + str(root).encode(), hashlib.sha256).hexdigest()
    directory = root.parent / ('.nexus-mock-receiver-' + root_tag[:24])
    if directory.is_symlink():
        raise ns.SyncError('Ссылка в состоянии мокового приёмника')
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory


def _git_receipt(root: Path, key: bytes, project: str) -> Path:
    project_tag = hmac.new(key, b'nexus-mock-receiver-project-v1\0' + project.encode(), hashlib.sha256).hexdigest()
    directory = _receiver_state(root, key)
    return directory / (project_tag + '.json')


def _git_already_installed(root: Path, key: bytes, package: Path, project: str, receipt: Path) -> bool:
    try:
        if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 65536:
            return False
        marker = json.loads(receipt.read_text(encoding='utf-8'))
        tag = marker.get('authentication') if isinstance(marker, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                or marker.get('format') != 'nexus-mock-receiver-git-v1'
                or marker.get('package_tag') != _package_tag(package, key)):
            return False
        _, fingerprint = ns.git_fingerprint(root.joinpath(*ns._secret_parts(project, nested=True)), root, key)
        return marker.get('fingerprint') == fingerprint
    except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
        return False


def _git_receipt_matches_current(root: Path, key: bytes, project: str, receipt: Path) -> bool:
    """Check a managed repository without assuming which peer last updated it."""
    try:
        if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 65536:
            return False
        marker = json.loads(receipt.read_text(encoding='utf-8'))
        tag = marker.get('authentication') if isinstance(marker, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                or marker.get('format') != 'nexus-mock-receiver-git-v1'
                or not re.fullmatch(r'[0-9a-f]{64}', str(marker.get('package_tag', '')))):
            return False
        _, fingerprint = ns.git_fingerprint(root.joinpath(*ns._secret_parts(project, nested=True)), root, key)
        return marker.get('fingerprint') == fingerprint
    except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
        return False


def _write_git_receipt(root: Path, key: bytes, package: Path, project: str, receipt: Path, expected_tag: str) -> None:
    _, fingerprint = ns.git_fingerprint(root.joinpath(*ns._secret_parts(project, nested=True)), root, key)
    if _package_tag(package, key) != expected_tag:
        raise ns.SyncError('Тестовый пакет изменился во время установки')
    marker = {'format': 'nexus-mock-receiver-git-v1', 'package_tag': expected_tag,
              'fingerprint': fingerprint}
    marker['authentication'] = ns._manifest_tag(marker, key)
    if receipt.is_symlink():
        raise ns.SyncError('Ссылка в состоянии мокового приёмника')
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=receipt.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(marker, stream, sort_keys=True)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, receipt)
    finally:
        temporary.unlink(missing_ok=True)


def receive(root: Path, shared: Path, key: bytes, git_package: Path, secret_package: Path,
            project: str, secret_path: str, *, timeout: float = 60, interval: float = 0.5,
            previous_secret_tag: str | None = None, include_secret_tag: bool = False) -> dict:
    if sys.platform != 'linux' or not 1 <= timeout <= 300 or not 0.1 <= interval <= 10:
        raise ns.SyncError('Недопустимые параметры мокового наблюдателя')
    root = mock_git_sync._workspace(root, key)
    mock._mock_path(shared)
    receipt = _git_receipt(root, key, project)
    git_done = secret_done = False
    secret_tag = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not git_done and git_package.is_file():
            try:
                git_done = _git_already_installed(root, key, git_package, project, receipt)
                if not git_done:
                    package_tag = _package_tag(git_package, key)
                    installed = mock_git_sync.apply_git(root, shared, key, git_package, project)
                    if installed['state'] == 'INSTALLED':
                        _write_git_receipt(root, key, git_package, project, receipt, package_tag)
                        git_done = True
            except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
                pass  # A partially delivered package or local conflict remains pending.
        if not secret_done and secret_package.is_file():
            try:
                restored = mock_secret_sync.apply_secret(root, shared, key, secret_package, secret_path,
                                                         previous_tag=previous_secret_tag)
                secret_done = restored['state'] in ('INSTALLED', 'UNCHANGED', 'UPDATED')
                secret_tag = restored['content_tag']
            except (ns.SyncError, OSError, ValueError, TypeError):
                pass
        if git_done and secret_done:
            result = {'state': 'COMPLETE', 'mock_only': True, 'git_installed': True,
                      'secret_ready': True, 'chats_updated': False}
            if include_secret_tag:
                result['secret_content_tag'] = secret_tag
            return result
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return {'state': 'PENDING', 'mock_only': True, 'git_installed': git_done,
            'secret_ready': secret_done, 'chats_updated': False}


def receive_available(root: Path, shared: Path, key: bytes, source: str, project: str, secret_path: str,
                      *, timeout: float = 60, interval: float = 0.5,
                      exclusive_mock_fast_forward: bool = False) -> dict:
    """Apply locally visible publications; this cannot assert remote freshness."""
    import mock_publications as publications
    if sys.platform != 'linux' or not 1 <= timeout <= 300 or not 0.1 <= interval <= 10:
        raise ns.SyncError('Недопустимые параметры мокового наблюдателя')
    root = mock_git_sync._workspace(root, key)
    shared = mock._mock_path(shared)
    ns._safe_machine(source)
    git_stream = publications._stream('git', project, key)
    secret_stream = publications._stream('secret', secret_path, key)
    base = _git_receipt(root, key, project).parent
    identity = hmac.new(key, b'nexus-mock-receiver-chain-v1\0' + source.encode() + b'\0' +
                        git_stream.encode() + b'\0' + secret_stream.encode(), hashlib.sha256).hexdigest()
    checkpoint = base / ('publications-' + identity + '.json')

    def load_checkpoint():
        if checkpoint.is_symlink():
            raise ns.SyncError('Ссылка в контрольной точке запрещена')
        if not checkpoint.exists():
            return None
        if not checkpoint.is_file() or checkpoint.stat().st_size > 65536:
            raise ns.SyncError('Некорректная контрольная точка')
        marker = json.loads(checkpoint.read_text(encoding='utf-8'))
        tag = marker.get('authentication') if isinstance(marker, dict) else None
        if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                or marker.get('format') != 'nexus-mock-receiver-publications-v1'
                or marker.get('identity') != identity):
            raise ns.SyncError('Контрольная точка не подтверждена')
        for kind in ('git', 'secret'):
            entry = marker.get(kind)
            if (not isinstance(entry, dict) or not isinstance(entry.get('id'), str)
                    or not isinstance(entry.get('authentication'), str)):
                raise ns.SyncError('Некорректная контрольная точка')
        return marker

    def choose(kind, name, marker):
        previous = marker[kind] if marker else None
        return publications.select(shared, source, kind, name, key,
            previous_id=previous['id'] if previous else None,
            previous_authentication=previous['authentication'] if previous else None)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            marker = load_checkpoint()
            git = choose('git', project, marker)
            secret = choose('secret', secret_path, marker)
            if git and secret:
                git_mode = marker['git'].get('mode', 'installed') if marker else 'installed'
                if git_mode not in ('installed', 'refs-only', 'fast-forward'):
                    raise ns.SyncError('Некорректная отметка Git')
                if marker and marker['git']['id'] != git[1]['id']:
                    receipt = _git_receipt(root, key, project)
                    # A crash after writing the receipt can be resumed without a second fetch.
                    if not _git_already_installed(root, key, git[0], project, receipt):
                        folder = publications._folder(shared, source, git_stream, create=False)
                        chain = publications._chain(folder, source, 'git', git_stream, key)
                        old = next((item for item in chain if item['id'] == marker['git']['id'] and
                                    item['authentication'] == marker['git']['authentication']), None)
                        if old is None:
                            raise ns.SyncError('Пропущена прежняя Git-публикация')
                        old_package = shared / 'nexus-sync' / 'secrets' / source / old['package']
                        if (publications._validate_package(old_package, shared, source, 'git', project, key)
                                != old['package_tag'] or
                                not _git_already_installed(root, key, old_package, project, receipt)):
                            raise ns.SyncError('Локальный Git изменился')
                        package_tag = _package_tag(git[0], key)
                        mock_git_sync.fetch_git(root, shared, key, git[0], project, source)
                        _write_git_receipt(root, key, git[0], project, receipt, package_tag)
                    git_mode = 'refs-only'
                if git_mode == 'refs-only' and exclusive_mock_fast_forward:
                    receipt = _git_receipt(root, key, project)
                    if not _git_already_installed(root, key, git[0], project, receipt):
                        raise ns.SyncError('Локальный Git изменился')
                    package_tag = _package_tag(git[0], key)
                    advanced = mock_git_sync.fast_forward_git(root, shared, key, git[0], project, source)
                    if advanced['state'] in ('FAST_FORWARDED', 'UNCHANGED'):
                        _write_git_receipt(root, key, git[0], project, receipt, package_tag)
                        git_mode = 'fast-forward'
                previous_secret_tag = None
                if marker:
                    previous_secret_tag = marker['secret'].get('content_tag')
                applied = receive(root, shared, key, git[0], secret[0], project, secret_path,
                                  timeout=1, interval=interval, previous_secret_tag=previous_secret_tag,
                                  include_secret_tag=True)
                if applied['state'] == 'COMPLETE':
                    after_git = choose('git', project, marker)
                    after_secret = choose('secret', secret_path, marker)
                    if (after_git and after_secret and after_git[1]['authentication'] == git[1]['authentication']
                            and after_secret[1]['authentication'] == secret[1]['authentication']):
                        next_marker = {'format': 'nexus-mock-receiver-publications-v1', 'identity': identity,
                            'git': {k: git[1][k] for k in ('id', 'authentication')},
                            'secret': {k: secret[1][k] for k in ('id', 'authentication')}}
                        next_marker['git']['mode'] = git_mode
                        next_marker['secret']['content_tag'] = applied['secret_content_tag']
                        next_marker['authentication'] = ns._manifest_tag(next_marker, key)
                        if checkpoint.is_symlink():
                            raise ns.SyncError('Ссылка в контрольной точке запрещена')
                        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=base, delete=False) as out:
                            temporary = Path(out.name)
                            try:
                                json.dump(next_marker, out, sort_keys=True)
                            except BaseException:
                                temporary.unlink(missing_ok=True)
                                raise
                        try:
                            os.replace(temporary, checkpoint)
                        finally:
                            temporary.unlink(missing_ok=True)
                        return {'state': 'PREPARED_AVAILABLE' if git_mode == 'refs-only' else 'APPLIED_AVAILABLE',
                                'mock_only': True, 'remote_freshness_confirmed': False,
                                'git_installed': True, 'git_update_requires_review': git_mode == 'refs-only',
                                'git_fast_forward_recorded': git_mode == 'fast-forward',
                                'secret_ready': True, 'chats_updated': False}
        except (ns.SyncError, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
            pass
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return {'state': 'PENDING', 'mock_only': True, 'remote_freshness_confirmed': False, 'chats_updated': False}


def receive_secret_available(root: Path, shared: Path, key: bytes, source: str, secret_path: str,
                             *, timeout: float = 60, interval: float = 0.5,
                             expected_package: str | None = None) -> dict:
    """Apply one authenticated mock secret stream without requiring Git."""
    import mock_publications as publications
    if sys.platform != 'linux' or not 1 <= timeout <= 300 or not 0.1 <= interval <= 10:
        raise ns.SyncError('Недопустимые параметры мокового наблюдателя')
    root = mock_git_sync._workspace(root, key)
    shared = mock._mock_path(shared)
    ns._safe_machine(source)
    stream = publications._stream('secret', secret_path, key)
    identity = hmac.new(key, b'nexus-mock-secret-only-v1\0' + source.encode() + b'\0' +
                        stream.encode(), hashlib.sha256).hexdigest()
    checkpoint = _receiver_state(root, key) / ('secret-' + identity + '.json')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if checkpoint.is_symlink():
                raise ns.SyncError('Ссылка в контрольной точке запрещена')
            marker = None
            if checkpoint.exists():
                if not checkpoint.is_file() or checkpoint.stat().st_size > 65536:
                    raise ns.SyncError('Некорректная контрольная точка')
                marker = json.loads(checkpoint.read_text(encoding='utf-8'))
                tag = marker.get('authentication') if isinstance(marker, dict) else None
                if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                        or marker.get('format') != 'nexus-mock-secret-only-v1' or marker.get('identity') != identity
                        or not re.fullmatch(r'[0-9a-f]{32}', str(marker.get('id', '')))
                        or not re.fullmatch(r'[0-9a-f]{64}', str(marker.get('publication_tag', '')))
                        or not re.fullmatch(r'[0-9a-f]{64}', str(marker.get('content_tag', '')))):
                    raise ns.SyncError('Контрольная точка не подтверждена')
            selected = publications.select(shared, source, 'secret', secret_path, key,
                previous_id=marker['id'] if marker else None,
                previous_authentication=marker['publication_tag'] if marker else None)
            if selected:
                if expected_package is not None and selected[0].name != expected_package:
                    raise ns.SyncError('Публикация файла относится к другому набору')
                applied = mock_secret_sync.apply_secret(root, shared, key, selected[0], secret_path,
                    previous_tag=marker['content_tag'] if marker else None)
                after = publications.select(shared, source, 'secret', secret_path, key,
                    previous_id=marker['id'] if marker else None,
                    previous_authentication=marker['publication_tag'] if marker else None)
                if after and after[1]['authentication'] == selected[1]['authentication']:
                    next_marker = {'format': 'nexus-mock-secret-only-v1', 'identity': identity,
                                   'id': selected[1]['id'], 'publication_tag': selected[1]['authentication'],
                                   'content_tag': applied['content_tag']}
                    next_marker['authentication'] = ns._manifest_tag(next_marker, key)
                    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=checkpoint.parent, delete=False) as out:
                        temporary = Path(out.name)
                        try:
                            json.dump(next_marker, out, sort_keys=True)
                        except BaseException:
                            temporary.unlink(missing_ok=True)
                            raise
                    try:
                        if checkpoint.is_symlink():
                            raise ns.SyncError('Ссылка в контрольной точке запрещена')
                        os.replace(temporary, checkpoint)
                    finally:
                        temporary.unlink(missing_ok=True)
                    return {'state': 'APPLIED_AVAILABLE', 'mock_only': True,
                            'remote_freshness_confirmed': False, 'chats_updated': False}
        except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
            pass
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return {'state': 'PENDING', 'mock_only': True, 'remote_freshness_confirmed': False, 'chats_updated': False}


def receive_discovered_git(root: Path, shared: Path, key: bytes, source: str) -> dict:
    """Apply locally visible Git publications to marked mock projects only."""
    import mock_publications as publications
    if sys.platform != 'linux':
        raise ns.SyncError('Моковый приём Git доступен только на Linux')
    root = mock_git_sync._workspace(root, key)
    shared = mock._mock_path(shared)
    ns._safe_machine(source)
    result = {'state': 'PENDING', 'mock_only': True, 'remote_freshness_confirmed': False,
              'git_ready_count': 0, 'git_pending_count': 0, 'chats_updated': False}
    base = shared / 'nexus-sync' / 'mock-publications' / source
    if any(path.is_symlink() for path in (shared / 'nexus-sync', base.parent, base)):
        return result
    if not base.is_dir():
        return result
    folders = sorted(base.iterdir())
    if len(folders) > 1024:
        raise ns.SyncError('Слишком много тестовых публикаций')
    state_dir = _receiver_state(root, key)
    source_tag = hmac.new(key, b'nexus-mock-git-source-v1\0' + source.encode(), hashlib.sha256).hexdigest()
    checkpoints = sorted(state_dir.glob('git-chain-*.json'))
    if len(checkpoints) > 1024:
        raise ns.SyncError('Слишком много контрольных точек Git')
    expected = set()
    observed = set()
    for checkpoint in checkpoints:
        try:
            if checkpoint.is_symlink() or not checkpoint.is_file() or checkpoint.stat().st_size > 65536:
                raise ns.SyncError('Некорректная контрольная точка')
            item = json.loads(checkpoint.read_text(encoding='utf-8'))
            tag = item.get('authentication') if isinstance(item, dict) else None
            identity = item.get('identity') if isinstance(item, dict) else None
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
                    or item.get('format') != 'nexus-mock-git-chain-v1'
                    or not isinstance(identity, str) or not re.fullmatch(r'[0-9a-f]{64}', identity)
                    or checkpoint.name != 'git-chain-' + identity + '.json'
                    or not re.fullmatch(r'[0-9a-f]{64}', str(item.get('source_tag', '')))):
                raise ns.SyncError('Контрольная точка не подтверждена')
            if item['source_tag'] == source_tag:
                expected.add(identity)
        except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
            result['git_pending_count'] += 1
    found = 0
    for folder in folders:
        try:
            if folder.is_symlink() or not folder.is_dir() or not re.fullmatch(r'[0-9a-f]{64}', folder.name):
                raise ns.SyncError('Некорректный поток публикаций')
            entries = sorted(path for path in folder.iterdir() if not path.name.startswith('.'))
            if not entries:
                continue
            first = entries[0]
            if first.is_symlink() or not first.is_file() or first.stat().st_size > 65536:
                raise ns.SyncError('Некорректная публикация')
            sample = json.loads(first.read_text(encoding='utf-8'))
            tag = sample.get('authentication') if isinstance(sample, dict) else None
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(sample, key))
                    or sample.get('source') != source or sample.get('stream') != folder.name):
                raise ns.SyncError('Публикация не подтверждена')
            if sample.get('kind') != 'git':
                continue
            found += 1
            chain = publications._chain(folder, source, 'git', folder.name, key)
            if not chain:
                raise ns.SyncError('Пустая цепочка Git')
            last = chain[-1]
            package = shared / 'nexus-sync' / 'secrets' / source / last['package']
            if (any(path.is_symlink() for path in (shared / 'nexus-sync', package.parent.parent,
                                                   package.parent, package))
                    or _package_tag(package, key) != last['package_tag']):
                raise ns.SyncError('Git-пакет не подтверждён')
            with tempfile.TemporaryDirectory(prefix='nexus-mock-git-receive-') as tmp:
                extracted = Path(tmp) / 'contents'
                ns.secrets_extract(package, extracted, key, shared)
                info = json.loads((extracted / 'repository.json').read_text(encoding='utf-8'))
                project = info.get('project') if isinstance(info, dict) else None
                if (not isinstance(project, str) or info.get('format') != 'nexus-git-package-v1'
                        or publications._stream('git', project, key) != folder.name):
                    raise ns.SyncError('Git-пакет не соответствует потоку')
            identity = hmac.new(key, b'nexus-mock-git-chain-v1\0' + source.encode() + b'\0' +
                                folder.name.encode(), hashlib.sha256).hexdigest()
            observed.add(identity)
            checkpoint = state_dir / ('git-chain-' + identity + '.json')
            previous = None
            if checkpoint.is_symlink():
                raise ns.SyncError('Ссылка в контрольной точке')
            if checkpoint.exists():
                if not checkpoint.is_file() or checkpoint.stat().st_size > 65536:
                    raise ns.SyncError('Некорректная контрольная точка')
                previous = json.loads(checkpoint.read_text(encoding='utf-8'))
                previous_tag = previous.get('authentication') if isinstance(previous, dict) else None
                if (not isinstance(previous_tag, str) or not hmac.compare_digest(previous_tag, ns._manifest_tag(previous, key))
                        or previous.get('format') != 'nexus-mock-git-chain-v1' or previous.get('identity') != identity
                        or previous.get('source_tag') != source_tag
                        or not re.fullmatch(r'[0-9a-f]{32}', str(previous.get('id', '')))
                        or not re.fullmatch(r'[0-9a-f]{64}', str(previous.get('publication_tag', '')))):
                    raise ns.SyncError('Контрольная точка не подтверждена')
            selected = publications.select(shared, source, 'git', project,
                key, previous_id=previous['id'] if previous else None,
                previous_authentication=previous['publication_tag'] if previous else None)
            if not selected:
                raise ns.SyncError('Git-публикация отсутствует')
            package, publication = selected
            receipt = _git_receipt(root, key, project)
            current = _git_receipt_matches_current(root, key, project, receipt)
            if previous and previous['id'] == publication['id']:
                if not current:
                    raise ns.SyncError('Локальный Git изменился')
            elif not _git_already_installed(root, key, package, project, receipt):
                package_tag = _package_tag(package, key)
                if previous:
                    old = next((item for item in chain if item['id'] == previous['id']
                                and item['authentication'] == previous['publication_tag']), None)
                    if old is None:
                        raise ns.SyncError('Предыдущая публикация недоступна')
                    old_package = shared / 'nexus-sync' / 'secrets' / source / old['package']
                    if (publications._validate_package(old_package, shared, source, 'git', project, key) != old['package_tag']
                            or not current):
                        raise ns.SyncError('Локальный Git изменился')
                    mock_git_sync.fetch_git(root, shared, key, package, project, source)
                elif current:
                    mock_git_sync.fetch_git(root, shared, key, package, project, source)
                else:
                    mock_git_sync.apply_git(root, shared, key, package, project)
                _write_git_receipt(root, key, package, project, receipt, package_tag)
            latest = publications.select(shared, source, 'git', project, key,
                previous_id=previous['id'] if previous else None,
                previous_authentication=previous['publication_tag'] if previous else None)
            if not latest or latest[1]['authentication'] != publication['authentication']:
                raise ns.SyncError('Git-публикация изменилась')
            marker = {'format': 'nexus-mock-git-chain-v1', 'identity': identity, 'source_tag': source_tag,
                      'id': publication['id'], 'publication_tag': publication['authentication']}
            marker['authentication'] = ns._manifest_tag(marker, key)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=checkpoint.parent, delete=False) as stream:
                temporary = Path(stream.name)
                try:
                    json.dump(marker, stream, sort_keys=True)
                except BaseException:
                    temporary.unlink(missing_ok=True)
                    raise
            try:
                if checkpoint.is_symlink():
                    raise ns.SyncError('Ссылка в контрольной точке')
                os.replace(temporary, checkpoint)
            finally:
                temporary.unlink(missing_ok=True)
            result['git_ready_count'] += 1
        except (ns.SyncError, OSError, ValueError, TypeError, AttributeError, KeyError, subprocess.SubprocessError):
            result['git_pending_count'] += 1
    result['git_pending_count'] += len(expected - observed)
    if found and not result['git_pending_count']:
        result['state'] = 'APPLIED_AVAILABLE'
    return result


def receive_discovered_secrets(root: Path, shared: Path, key: bytes, source: str,
                               *, interval: float = 0.5) -> dict:
    """Receive an authenticated mock set, without guessing filenames or deleting files."""
    import mock_publications as publications
    if sys.platform != 'linux' or not 0.1 <= interval <= 10:
        raise ns.SyncError('Недопустимые параметры мокового наблюдателя')
    root = mock_git_sync._workspace(root, key)
    shared = mock._mock_path(shared)
    ns._safe_machine(source)
    identity = hmac.new(key, b'nexus-mock-secret-set-receiver-v1\0' + source.encode(), hashlib.sha256).hexdigest()
    checkpoint = _receiver_state(root, key) / ('secret-set-' + identity + '.json')
    result = {'state': 'PENDING', 'mock_only': True, 'remote_freshness_confirmed': False,
              'chats_updated': False}
    try:
        marker = None
        if checkpoint.is_symlink():
            raise ns.SyncError('Ссылка в контрольной точке запрещена')
        if checkpoint.exists():
            if not checkpoint.is_file() or checkpoint.stat().st_size > 65536:
                raise ns.SyncError('Некорректная контрольная точка')
            marker = json.loads(checkpoint.read_text(encoding='utf-8'))
            tag = marker.get('authentication') if isinstance(marker, dict) else None
            if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(marker, key))
                    or marker.get('format') != 'nexus-mock-secret-set-receiver-v1' or marker.get('identity') != identity
                    or not re.fullmatch(r'[0-9a-f]{32}', str(marker.get('id', '')))
                    or not re.fullmatch(r'[0-9a-f]{64}', str(marker.get('publication_tag', '')))):
                raise ns.SyncError('Контрольная точка не подтверждена')
            for field in ('streams', 'missing'):
                values = marker.get(field)
                if (not isinstance(values, list) or len(values) > 1024 or len(set(values)) != len(values)
                        or any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value) for value in values)):
                    raise ns.SyncError('Некорректный список тестовых файлов')
        def choose():
            return publications.select(shared, source, 'secret-set', 'all', key,
                previous_id=marker['id'] if marker else None,
                previous_authentication=marker['publication_tag'] if marker else None)
        selected = choose()
        if not selected:
            return result
        if selected[1].get('empty') is True:
            names = []
        else:
            with tempfile.TemporaryDirectory(prefix='nexus-mock-discover-receiver-') as tmp:
                extracted = Path(tmp) / 'contents'
                ns.secrets_extract(selected[0], extracted, key, shared)
                names = publications._extracted_secret_names(extracted, key)
        streams = {publications._stream('secret', name, key) for name in names}
        for name in names:
            publication = publications.select(shared, source, 'secret', name, key)
            if not publication or publication[0].name != selected[0].name:
                return result
        ready = 0
        for name in names:
            item = receive_secret_available(root, shared, key, source, name, timeout=1, interval=interval,
                                            expected_package=selected[0].name)
            ready += item['state'] == 'APPLIED_AVAILABLE'
        result.update({'secret_ready_count': ready, 'secret_expected_count': len(names)})
        if ready != len(names):
            return result
        latest = choose()
        if not latest or latest[1]['authentication'] != selected[1]['authentication']:
            return result
        missing = ((set(marker['missing']) | set(marker['streams'])) - streams) if marker else set()
        next_marker = {'format': 'nexus-mock-secret-set-receiver-v1', 'identity': identity,
                       'id': selected[1]['id'], 'publication_tag': selected[1]['authentication'],
                       'streams': sorted(streams), 'missing': sorted(missing)}
        next_marker['authentication'] = ns._manifest_tag(next_marker, key)
        if len(missing) > 1024:
            raise ns.SyncError('Слишком много отсутствующих тестовых файлов')
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=checkpoint.parent, delete=False) as out:
            temporary = Path(out.name)
            try:
                json.dump(next_marker, out, sort_keys=True)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        try:
            if checkpoint.is_symlink():
                raise ns.SyncError('Ссылка в контрольной точке запрещена')
            os.replace(temporary, checkpoint)
        finally:
            temporary.unlink(missing_ok=True)
        result.update({'state': 'ATTENTION' if missing else 'APPLIED_AVAILABLE', 'missing_count': len(missing)})
        return result
    except (ns.SyncError, OSError, ValueError, TypeError, AttributeError):
        return result


def receive_many_available(root: Path, shared: Path, key: bytes, source: str, project: str | None,
                           secret_paths: list[str], *, interval: float = 0.5,
                           exclusive_mock_fast_forward: bool = False, secrets_only: bool = False) -> dict:
    """One bounded mock cycle over independently authenticated secret streams."""
    if not 1 <= len(secret_paths) <= 16:
        raise ns.SyncError('Укажите от одного до 16 моковых файлов')
    seen = set()
    for path in secret_paths:
        parts = ns._secret_parts(path, nested=True)
        ns._add_secret_path(seen, parts)
    ready = 0
    git_review = False
    for path in secret_paths:
        if secrets_only:
            result = receive_secret_available(root, shared, key, source, path, timeout=1, interval=interval)
        else:
            if project is None:
                raise ns.SyncError('Не указан моковый проект')
            result = receive_available(root, shared, key, source, project, path, timeout=1, interval=interval,
                                       exclusive_mock_fast_forward=exclusive_mock_fast_forward)
        if result['state'] in ('APPLIED_AVAILABLE', 'PREPARED_AVAILABLE'):
            ready += 1
            git_review |= result['state'] == 'PREPARED_AVAILABLE'
    state = ('PENDING' if ready != len(secret_paths) else
             'PREPARED_AVAILABLE' if git_review else 'APPLIED_AVAILABLE')
    return {'state': state, 'mock_only': True, 'remote_freshness_confirmed': False,
            'secret_ready_count': ready, 'secret_expected_count': len(secret_paths),
            'git_update_requires_review': git_review, 'chats_updated': False}


def main() -> int:
    parser = argparse.ArgumentParser(description='Тестовый приём моковых пакетов')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--shared', type=Path, required=True)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--git-package', type=Path)
    parser.add_argument('--secret-package', type=Path)
    parser.add_argument('--source', help='выбрать локально доступные подтверждённые публикации источника')
    parser.add_argument('--also-source', action='append', default=[], help='дополнительный источник для мокового поиска Git или файлов')
    parser.add_argument('--project')
    parser.add_argument('--secrets-only', action='store_true', help='только моковые файлы без Git')
    parser.add_argument('--discover-secrets', action='store_true', help='получить имена из подтверждённого мокового набора')
    parser.add_argument('--discover-git', action='store_true', help='получить доступные подтверждённые моковые Git-пакеты')
    parser.add_argument('--secret-path', action='append', default=[])
    parser.add_argument('--timeout', type=float, default=60)
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--watch', action='store_true', help='продолжать проверять моковые публикации')
    parser.add_argument('--exclusive-mock-fast-forward', action='store_true',
                        help='разрешить fast-forward только в изолированном моковом проекте без других процессов')
    args = parser.parse_args()
    try:
        if not 1 <= args.timeout <= 300:
            raise ns.SyncError('Недопустимое время ожидания')
        mock._mock_path(args.key_file)
        key = ns.read_key(args.key_file, args.shared)
        if args.also_source and not (args.discover_git or args.discover_secrets):
            raise ns.SyncError('Дополнительные источники доступны только для мокового поиска Git или файлов')
        if args.discover_git:
            if (not args.source or args.project or args.git_package or args.secret_package or args.secret_path
                    or args.secrets_only or args.discover_secrets or args.exclusive_mock_fast_forward
                    or not 0.1 <= args.interval <= 10):
                raise ns.SyncError('Моковый поиск Git требует источник и не принимает отдельные проекты или файлы')
            sources = [ns._safe_machine(value) for value in (args.source, *args.also_source)]
            if len(sources) > 16 or len(set(sources)) != len(sources):
                raise ns.SyncError('Некорректный список тестовых источников')
            last_state = None
            while True:
                results = [receive_discovered_git(args.root, args.shared, key, source) for source in sources]
                if len(results) == 1:
                    result = results[0]
                else:
                    ready = sum(item['state'] == 'APPLIED_AVAILABLE' for item in results)
                    result = {'state': 'APPLIED_AVAILABLE' if ready == len(results) else 'PENDING',
                              'mock_only': True, 'remote_freshness_confirmed': False,
                              'git_sources_ready': ready, 'git_sources_expected': len(results),
                              'git_ready_count': sum(item['git_ready_count'] for item in results),
                              'git_pending_count': sum(item['git_pending_count'] for item in results),
                              'chats_updated': False}
                current = json.dumps(result, sort_keys=True)
                if not args.watch:
                    print(current)
                    return 0 if result['state'] == 'APPLIED_AVAILABLE' else 1
                if current != last_state:
                    print(current, flush=True)
                    last_state = current
                time.sleep(args.interval)
        if args.discover_secrets:
            if not args.secrets_only or args.secret_path or not args.source:
                raise ns.SyncError('Для мокового поиска нужен режим файлов и источник')
            secret_sources = [ns._safe_machine(value) for value in (args.source, *args.also_source)]
            if len(secret_sources) > 16 or len({value.casefold() for value in secret_sources}) != len(secret_sources):
                raise ns.SyncError('Некорректный список тестовых источников')
        elif not args.secret_path:
            raise ns.SyncError('Нужны тестовые пути файлов')
        if args.source and not args.git_package and not args.secret_package:
            if args.secrets_only and (args.project or args.exclusive_mock_fast_forward):
                raise ns.SyncError('Моковый режим файлов не использует Git')
            if not args.secrets_only and not args.project:
                raise ns.SyncError('Нужно указать моковый проект')
            def cycle(timeout):
                if args.secrets_only:
                    if args.discover_secrets:
                        results = [receive_discovered_secrets(args.root, args.shared, key, source, interval=args.interval)
                                   for source in secret_sources]
                        if len(results) == 1:
                            return results[0]
                        # A later source may change a file that an earlier source had
                        # already checked. Recheck each source before reporting readiness.
                        results = [receive_discovered_secrets(args.root, args.shared, key, source, interval=args.interval)
                                   for source in secret_sources]
                        ready = sum(item['state'] == 'APPLIED_AVAILABLE' for item in results)
                        state = ('PENDING' if any(item['state'] == 'PENDING' for item in results) else
                                 'ATTENTION' if any(item['state'] == 'ATTENTION' for item in results) else
                                 'APPLIED_AVAILABLE' if ready == len(results) else 'PENDING')
                        aggregate = {'state': state, 'mock_only': True, 'remote_freshness_confirmed': False,
                                     'secret_sources_ready': ready, 'secret_sources_expected': len(results),
                                     'chats_updated': False}
                        for field in ('secret_ready_count', 'secret_expected_count', 'missing_count'):
                            if all(field in item for item in results):
                                aggregate[field] = sum(item[field] for item in results)
                        return aggregate
                    if len(args.secret_path) == 1:
                        return receive_secret_available(args.root, args.shared, key, args.source, args.secret_path[0],
                                                        timeout=timeout, interval=args.interval)
                    return receive_many_available(args.root, args.shared, key, args.source, None, args.secret_path,
                                                  interval=args.interval, secrets_only=True)
                if len(args.secret_path) == 1:
                    return receive_available(args.root, args.shared, key, args.source, args.project, args.secret_path[0],
                                             timeout=timeout, interval=args.interval,
                                             exclusive_mock_fast_forward=args.exclusive_mock_fast_forward)
                return receive_many_available(args.root, args.shared, key, args.source, args.project, args.secret_path,
                                              interval=args.interval,
                                              exclusive_mock_fast_forward=args.exclusive_mock_fast_forward)
            if args.watch:
                last_state = None
                while True:
                    result = cycle(1)
                    current = json.dumps(result, sort_keys=True)
                    if current != last_state:
                        print(json.dumps(result), flush=True)
                        last_state = current
                    time.sleep(args.interval)
            if args.discover_secrets or len(args.secret_path) > 1:
                deadline = time.monotonic() + args.timeout
                while True:
                    result = cycle(1)
                    if result['state'] != 'PENDING' or time.monotonic() >= deadline:
                        break
            else:
                result = cycle(args.timeout)
        elif not args.source and args.git_package and args.secret_package and args.project and not args.secrets_only and not args.watch and not args.exclusive_mock_fast_forward and len(args.secret_path) == 1:
            result = receive(args.root, args.shared, key, args.git_package, args.secret_package,
                             args.project, args.secret_path[0], timeout=args.timeout, interval=args.interval)
        else:
            raise ns.SyncError('Нужно указать источник или два пакета')
        print(json.dumps(result))
        return 0 if result['state'] in ('COMPLETE', 'APPLIED_AVAILABLE', 'PREPARED_AVAILABLE') else 1
    except (ns.SyncError, OSError, ValueError, TypeError):
        print('Моковый наблюдатель отложен', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
