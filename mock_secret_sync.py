#!/usr/bin/env python3
"""Manual, no-overwrite secret restore only in a marked temporary workspace."""
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import stat
import sys
import tempfile

import mock_chat_sync as mock
import mock_git_sync
import nexus_sync as ns


def content_tag(key: bytes, body: bytes) -> str:
    return hmac.new(key, b'nexus-mock-secret-content-v1\0' + body, hashlib.sha256).hexdigest()


def apply_secret(root: Path, shared: Path, key: bytes, package: Path, relative: str,
                 *, previous_tag: str | None = None) -> dict:
    if sys.platform != 'linux':
        raise ns.SyncError('Опыт доступен только на Linux')
    root, shared = mock_git_sync._workspace(root, key), mock._mock_path(shared)
    if not shared.is_dir() or root == shared or root in shared.parents or shared in root.parents:
        raise ns.SyncError('Некорректные временные каталоги')
    parts = ns._secret_parts(relative, nested=True)
    reserved = {mock_git_sync.MARKER.casefold(), '.stignore', '.stfolder', '.stversions'}
    if (any(part.casefold() == '.git' for part in parts)
            or any(part.casefold() in reserved for part in parts)):
        raise ns.SyncError('Некорректный путь мокового секрета')
    package_root = shared / 'nexus-sync' / 'secrets'
    if (package.is_symlink() or (shared / 'nexus-sync').is_symlink() or package_root.is_symlink()
            or package.parent.is_symlink() or package_root not in package.resolve(strict=True).parents):
        raise ns.SyncError('Пакет находится вне тестового обмена')
    if package.stat().st_size > 24 * 1024 * 1024:
        raise ns.SyncError('Пакет слишком велик')
    encoded = package.read_bytes()
    if not encoded.startswith(ns.SECRET_MAGIC) or len(encoded) < len(ns.SECRET_MAGIC) + 28:
        raise ns.SyncError('Некорректный пакет')
    AESGCM, InvalidTag = ns._aead()
    offset = len(ns.SECRET_MAGIC)
    try:
        manifest = json.loads(AESGCM(key).decrypt(encoded[offset:offset + 12], encoded[offset + 12:], ns.SECRET_MAGIC))
    except (InvalidTag, ValueError, UnicodeError) as exc:
        raise ns.SyncError('Пакет не подтверждён') from exc
    if not isinstance(manifest, dict) or manifest.get('version') != 2:
        raise ns.SyncError('Нужен пакет с относительными путями')
    digest = hashlib.sha256(encoded).digest()
    with tempfile.TemporaryDirectory(prefix='nexus-mock-secret-') as tmp:
        quarantine = Path(tmp) / 'files'
        ns.secrets_extract(package, quarantine, key, shared)
        source = quarantine.joinpath(*parts)
        if source.is_symlink() or not source.is_file():
            raise ns.SyncError('Моковый файл отсутствует в пакете')
        body = source.read_bytes()
        if hashlib.sha256(package.read_bytes()).digest() != digest:
            raise ns.SyncError('Пакет изменился')
        opened = []
        temporary = '.nexus-secret-' + os.urandom(16).hex()
        created = False
        try:
            opened.append(os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
            for part in parts[:-1]:
                opened.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=opened[-1]))
            directory = opened[-1]
            try:
                existing = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                with os.fdopen(existing, 'rb') as stream:
                    details = os.fstat(stream.fileno())
                    if not stat.S_ISREG(details.st_mode) or details.st_size > 24 * 1024 * 1024:
                        raise ns.SyncError('Некорректный локальный файл')
                    current = stream.read(24 * 1024 * 1024 + 1)
                    after = os.fstat(stream.fileno())
                    if (details.st_ino, details.st_size, details.st_mtime_ns, details.st_ctime_ns) != (
                            after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise ns.SyncError('Локальный файл изменился во время проверки')
                if current == body:
                    return {'state': 'UNCHANGED', 'mock_only': True, 'content_tag': content_tag(key, body)}
                if not isinstance(previous_tag, str) or not hmac.compare_digest(previous_tag, content_tag(key, current)):
                    raise ns.SyncError('Локальный файл отличается; замена запрещена')
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            created = True
            with os.fdopen(fd, 'wb') as output:
                output.write(body)
            if hashlib.sha256(package.read_bytes()).digest() != digest:
                raise ns.SyncError('Пакет изменился')
            if existing is not None:
                current_details = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
                if (details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns, details.st_ctime_ns) != (
                        current_details.st_dev, current_details.st_ino, current_details.st_size,
                        current_details.st_mtime_ns, current_details.st_ctime_ns):
                    raise ns.SyncError('Локальный файл изменился во время восстановления')
                os.replace(temporary, parts[-1], src_dir_fd=directory, dst_dir_fd=directory)
                created = False
                return {'state': 'UPDATED', 'mock_only': True, 'content_tag': content_tag(key, body)}
            try:
                os.link(temporary, parts[-1], src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            except FileExistsError as exc:
                raise ns.SyncError('Локальный файл появился во время восстановления') from exc
            return {'state': 'INSTALLED', 'mock_only': True, 'content_tag': content_tag(key, body)}
        finally:
            if created:
                os.unlink(temporary, dir_fd=opened[-1])
            for fd in reversed(opened):
                os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(description='Восстановление файла только в моковом Nexus')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--shared', type=Path, required=True)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--path', required=True)
    args = parser.parse_args()
    try:
        mock._mock_path(args.key_file)
        result = apply_secret(args.root, args.shared, ns.read_key(args.key_file, args.shared),
                              args.package, args.path)
        result.pop('content_tag', None)
        print(json.dumps(result))
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError):
        print('Моковое восстановление секрета отложено', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
