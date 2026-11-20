#!/usr/bin/env python3
"""Automatically prepare snapshots in new, isolated Linux mock profiles."""
import argparse
from contextlib import contextmanager
import hmac
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time
import uuid

import mock_chat_sync as mock
import nexus_sync as ns

CONFIG = '.nexus-mock-chat-preview.json'
CHECKPOINT = '.nexus-mock-chat-preview-checkpoint.json'
ATTEMPT = '.nexus-mock-chat-preview-attempt.json'


def init(root: Path, shared: Path, target: Path, source: str, source_root: str, key: bytes) -> None:
    root, shared, target = mock._mock_path(root), mock._mock_path(shared), mock._mock_path(target)
    ns._safe_machine(source)
    ns.map_path(source_root, '/__nexus_sync_root__', source_root)
    if (len(key) != 32 or not shared.is_dir() or not target.is_dir() or
            any(a == b or a in b.parents or b in a.parents for a, b in
                ((root, shared), (root, target), (shared, target)))):
        raise ns.SyncError('Некорректные моковые каталоги')
    root.mkdir(mode=0o700)
    marker = {'format': 'nexus-mock-chat-preview-v1', 'root': str(root), 'shared': str(shared),
              'target': str(target), 'source': source, 'source_root': source_root}
    marker['authentication'] = ns._manifest_tag(marker, key)
    with (root / CONFIG).open('x', encoding='utf-8') as out:
        json.dump(marker, out, sort_keys=True)


def _read_signed(path: Path, key: bytes, expected_format: str) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise ns.SyncError('Отметка тестового профиля недоступна')
    item = json.loads(path.read_text(encoding='utf-8'))
    tag = item.get('authentication') if isinstance(item, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(item, key))
            or item.get('format') != expected_format):
        raise ns.SyncError('Отметка тестового профиля не подтверждена')
    return item


def status(root: Path, key: bytes, expected_shared: Path) -> dict:
    """Read only signed mock markers; does not assert running processes or index freshness."""
    result = {'state': 'INVALID', 'mock_only': True, 'index_current_verified': False,
              'verified_in_cursor': False, 'remote_freshness_confirmed': False}
    try:
        root = mock._mock_path(root)
        config = _read_signed(root / CONFIG, key, 'nexus-mock-chat-preview-v1')
        shared = mock._mock_path(Path(config['shared']))
        if config.get('root') != str(root) or shared != mock._mock_path(expected_shared):
            return result
        source = config['source']
        ns._safe_machine(source)
        attempt = root / ATTEMPT
        if attempt.exists() or attempt.is_symlink():
            result['state'] = 'ATTENTION'
            return result
        checkpoint_path = root / CHECKPOINT
        if not checkpoint_path.exists() and not checkpoint_path.is_symlink():
            result['state'] = 'NOT_PREPARED'
            return result
        checkpoint = _read_signed(checkpoint_path, key, 'nexus-mock-chat-preview-checkpoint-v1')
        profile = checkpoint.get('profile')
        if (checkpoint.get('root') != str(root) or checkpoint.get('source') != source or
                not isinstance(profile, str) or not re.fullmatch(r'profile-[0-9a-f]{32}', profile)):
            return result
        mock._validate_profile(root / profile, key)
        folder = shared / 'nexus-sync' / 'snapshots' / source
        if (not shared.is_dir() or (shared / 'nexus-sync').is_symlink() or folder.parent.is_symlink()
                or folder.is_symlink()):
            return result
        latest = ns.latest_authenticated_snapshot(folder, source, key)
        if latest is None:
            return result
        mock._require_snapshot_ancestor(folder, source, key, latest[0].name,
                                        checkpoint.get('snapshot'), checkpoint.get('snapshot_tag'))
        result['state'] = 'READY_LOCAL' if latest[0].name == checkpoint['snapshot'] else 'OUTDATED'
        return result
    except (ns.SyncError, OSError, ValueError, TypeError, KeyError, AttributeError):
        return result


@contextmanager
def _lock(root: Path):
    if sys.platform != 'linux':
        raise ns.SyncError('Эксперимент доступен только на Linux')
    import fcntl
    fd = os.open(root / '.preview.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ns.SyncError('Некорректная тестовая блокировка')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ns.SyncError('Другой тестовый наблюдатель занят') from exc
        yield
    finally:
        os.close(fd)


def prepare(root: Path, key: bytes, executable: Path) -> dict:
    root = mock._mock_path(root)
    if not root.is_dir() or len(key) != 32:
        raise ns.SyncError('Нет тестового каталога')
    executable = executable.resolve(strict=True)
    if not executable.is_file():
        raise ns.SyncError('Тестовый Codex недоступен')
    with _lock(root):
        config = _read_signed(root / CONFIG, key, 'nexus-mock-chat-preview-v1')
        if config.get('root') != str(root):
            raise ns.SyncError('Неверный тестовый каталог')
        attempt_path = root / ATTEMPT
        if attempt_path.is_symlink():
            raise ns.SyncError('Ссылка в отметке попытки')
        if attempt_path.exists():
            attempt = _read_signed(attempt_path, key, 'nexus-mock-chat-preview-attempt-v1')
            if attempt.get('root') != str(root):
                raise ns.SyncError('Некорректная отметка попытки')
            return {'state': 'ATTENTION', 'mock_only': True, 'verified_in_cursor': False}
        shared, target = mock._mock_path(Path(config['shared'])), mock._mock_path(Path(config['target']))
        source, source_root = config['source'], config['source_root']
        ns._safe_machine(source)
        folder = shared / 'nexus-sync' / 'snapshots' / source
        if (not shared.is_dir() or not target.is_dir() or (shared / 'nexus-sync').is_symlink()
                or folder.parent.is_symlink() or folder.is_symlink()):
            raise ns.SyncError('Тестовый обмен недоступен')
        latest = ns.latest_authenticated_snapshot(folder, source, key)
        if latest is None:
            return {'state': 'PENDING', 'mock_only': True, 'verified_in_cursor': False}
        snapshot, manifest = latest
        checkpoint_path = root / CHECKPOINT
        if checkpoint_path.is_symlink():
            raise ns.SyncError('Ссылка в контрольной точке')
        if checkpoint_path.exists():
            checkpoint = _read_signed(checkpoint_path, key, 'nexus-mock-chat-preview-checkpoint-v1')
            if checkpoint.get('root') != str(root) or checkpoint.get('source') != source:
                raise ns.SyncError('Некорректная контрольная точка')
            mock._require_snapshot_ancestor(folder, source, key, snapshot.name,
                                            checkpoint.get('snapshot'), checkpoint.get('snapshot_tag'))
            if checkpoint.get('snapshot') == snapshot.name:
                profile_name = checkpoint.get('profile')
                if not isinstance(profile_name, str) or not re.fullmatch(r'profile-[0-9a-f]{32}', profile_name):
                    raise ns.SyncError('Некорректный тестовый профиль')
                mock._validate_profile(root / profile_name, key)
                return {'state': 'UNCHANGED', 'mock_only': True, 'verified_in_cursor': False,
                        'index_current_verified': False, 'remote_freshness_confirmed': False}
        profile = root / ('profile-' + uuid.uuid4().hex)
        attempt = {'format': 'nexus-mock-chat-preview-attempt-v1', 'root': str(root),
                   'snapshot': snapshot.name, 'profile': profile.name}
        attempt['authentication'] = ns._manifest_tag(attempt, key)
        fd = os.open(attempt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            json.dump(attempt, out, sort_keys=True)
        mock.init_mock_profile(profile, key, shared)
        # Keep failed profiles: a headless process may not have exited after a timeout.
        result = mock.sync_all_mock(profile, shared, key, source, source_root, target, executable)
        after = ns.latest_authenticated_snapshot(folder, source, key)
        if (result['state'] != 'COMPLETE' or after is None or after[0].name != snapshot.name
                or after[1].get('authentication') != manifest.get('authentication')):
            return {'state': 'ATTENTION', 'mock_only': True, 'verified_in_cursor': False}
        checkpoint = {'format': 'nexus-mock-chat-preview-checkpoint-v1', 'root': str(root), 'source': source,
                      'snapshot': snapshot.name, 'snapshot_tag': manifest['authentication'], 'profile': profile.name}
        checkpoint['authentication'] = ns._manifest_tag(checkpoint, key)
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=root, delete=False) as out:
            temporary = Path(out.name)
            try:
                json.dump(checkpoint, out, sort_keys=True)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        try:
            if checkpoint_path.is_symlink():
                raise ns.SyncError('Ссылка в контрольной точке')
            os.replace(temporary, checkpoint_path)
        finally:
            temporary.unlink(missing_ok=True)
        if _read_signed(attempt_path, key, 'nexus-mock-chat-preview-attempt-v1') != attempt:
            raise ns.SyncError('Отметка попытки изменилась')
        attempt_path.unlink()
        return {'state': 'PREPARED', 'mock_only': True, 'profile': str(profile),
                'chats_indexed': result['processed'], 'verified_in_cursor': False,
                'remote_freshness_confirmed': False}


def main() -> int:
    parser = argparse.ArgumentParser(description='Изолированная подготовка моковых чатов')
    parser.add_argument('operation', choices=('init', 'once', 'watch'))
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--shared', type=Path)
    parser.add_argument('--target-root', type=Path)
    parser.add_argument('--source')
    parser.add_argument('--source-root')
    parser.add_argument('--codex-executable', type=Path)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    try:
        mock._mock_path(args.key_file)
        if args.operation == 'init':
            if not (args.shared and args.target_root and args.source and args.source_root):
                raise ns.SyncError('Не хватает параметров тестового профиля')
            key = ns.read_key(args.key_file, args.shared)
            init(args.root, args.shared, args.target_root, args.source, args.source_root, key)
            print(json.dumps({'state': 'INITIALIZED', 'mock_only': True}))
            return 0
        if args.codex_executable is None:
            raise ns.SyncError('Нужен тестовый Codex')
        config = json.loads((mock._mock_path(args.root) / CONFIG).read_text(encoding='utf-8'))
        key = ns.read_key(args.key_file, mock._mock_path(Path(config['shared'])))
        if args.operation == 'once':
            result = prepare(args.root, key, args.codex_executable)
            print(json.dumps(result))
            return 0 if result['state'] in ('PREPARED', 'UNCHANGED') else 1
        if not 0.5 <= args.interval <= 60:
            raise ns.SyncError('Некорректный интервал')
        previous = None
        while True:
            try:
                result = prepare(args.root, key, args.codex_executable)
            except (ns.SyncError, OSError, ValueError, TypeError, KeyError):
                attempt = args.root / ATTEMPT
                result = {'state': 'ATTENTION' if attempt.exists() or attempt.is_symlink() else 'PENDING',
                          'mock_only': True, 'verified_in_cursor': False}
            if result['state'] != previous or result['state'] == 'PREPARED':
                print(json.dumps(result), flush=True)
                previous = result['state']
            if result['state'] == 'ATTENTION':
                return 1
            time.sleep(args.interval)
    except (ns.SyncError, OSError, ValueError, TypeError, KeyError):
        print('Моковая подготовка отложена', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
