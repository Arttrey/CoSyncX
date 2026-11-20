#!/usr/bin/env python3
"""Manual Linux-only Git restore into an explicitly marked disposable workspace."""
import argparse
import ctypes
import errno
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import mock_chat_sync as mock
import nexus_sync as ns

MARKER = '.nexus-mock-git-workspace.json'


def init_workspace(root: Path, shared: Path, key: bytes) -> None:
    root, shared = mock._mock_path(root), mock._mock_path(shared)
    if len(key) != 32 or not shared.is_dir() or root == shared or root in shared.parents or shared in root.parents:
        raise ns.SyncError('Некорректные временные каталоги')
    root.mkdir(mode=0o700)
    marker = {'format': 'nexus-mock-git-workspace-v1', 'path': str(root)}
    marker['authentication'] = ns._manifest_tag(marker, key)
    with (root / MARKER).open('x', encoding='utf-8') as stream:
        json.dump(marker, stream, ensure_ascii=False, sort_keys=True)


def _workspace(root: Path, key: bytes) -> Path:
    root = mock._mock_path(root)
    marker = root / MARKER
    if len(key) != 32 or not root.is_dir() or marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 65536:
        raise ns.SyncError('Нет отметки тестового проекта')
    data = json.loads(marker.read_text(encoding='utf-8'))
    tag = data.get('authentication') if isinstance(data, dict) else None
    if (not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key))
            or data.get('format') != 'nexus-mock-git-workspace-v1' or data.get('path') != str(root)):
        raise ns.SyncError('Отметка тестового проекта не подтверждена')
    return root


def apply_git(root: Path, shared: Path, key: bytes, package: Path, project: str) -> dict:
    """Install .git once; never replace existing project files or an existing .git."""
    if sys.platform != 'linux':
        raise ns.SyncError('Восстановление мокового Git пока доступно только на Linux')
    root, shared = _workspace(root, key), mock._mock_path(shared)
    if not shared.is_dir() or root == shared or root in shared.parents or shared in root.parents:
        raise ns.SyncError('Некорректные временные каталоги')
    parts = ns._secret_parts(project, nested=True)
    target = root.joinpath(*parts)
    current = root
    for part in parts:
        if (current / '.git').exists() or (current / '.git').is_symlink():
            raise ns.SyncError('Проект находится внутри другого Git-репозитория')
        current = current / part
        if current.is_symlink() or not current.is_dir():
            raise ns.SyncError('Проект недоступен или содержит ссылку')
    if (target / '.git').exists() or (target / '.git').is_symlink():
        raise ns.SyncError('Git уже существует; восстановление отложено')
    package_root = shared / 'nexus-sync' / 'secrets'
    if (package.is_symlink() or (shared / 'nexus-sync').is_symlink() or package_root.is_symlink()
            or package.parent.is_symlink() or package_root not in package.resolve(strict=True).parents):
        raise ns.SyncError('Пакет находится вне тестовой папки обмена')
    package_hash = ns._digest(package)
    with tempfile.TemporaryDirectory(prefix='nexus-mock-git-') as tmp:
        temporary = Path(tmp)
        contents = temporary / 'contents'
        ns.secrets_extract(package, contents, key, shared)
        info = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
        if not isinstance(info, dict) or info.get('format') != 'nexus-git-package-v1' or info.get('project') != project:
            raise ns.SyncError('Пакет относится к другому проекту')
        preview = temporary / 'preview'
        ns.git_preview(package, preview, {'nexus': root, 'codex_home': root, 'shared': shared, 'state_dir': root}, key)
        if ns._digest(package) != package_hash:
            raise ns.SyncError('Пакет изменился во время восстановления')
        source_git = preview / '.git'
        if source_git.is_symlink() or not source_git.is_dir() or source_git.stat().st_dev != target.stat().st_dev:
            raise ns.SyncError('Нельзя безопасно переместить Git в этот проект')
        source_fd = os.open(preview, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        target_fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            rename = libc.renameat2
            rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            rename.restype = ctypes.c_int
            if rename(source_fd, b'.git', target_fd, b'.git', 1) != 0:  # RENAME_NOREPLACE
                error = ctypes.get_errno()
                if error == errno.EEXIST:
                    raise ns.SyncError('Git появился во время восстановления')
                raise OSError(error, 'Не удалось установить моковый Git')
        finally:
            os.close(target_fd)
            os.close(source_fd)
    return {'state': 'INSTALLED', 'mock_only': True, 'project_files_modified': False}


def fetch_git(root: Path, shared: Path, key: bytes, package: Path, project: str, peer: str) -> dict:
    """Fetch an explicit mock package under immutable per-package refs, without changing HEAD or index."""
    if sys.platform != 'linux':
        raise ns.SyncError('Опыт доступен только на Linux')
    root, shared = _workspace(root, key), mock._mock_path(shared)
    ns._safe_machine(peer)
    if not shared.is_dir() or root == shared or root in shared.parents or shared in root.parents:
        raise ns.SyncError('Некорректные временные каталоги')
    parts = ns._secret_parts(project, nested=True)
    target = root
    for part in parts:
        if (target / '.git').exists() or (target / '.git').is_symlink():
            raise ns.SyncError('Вложенный Git-репозиторий')
        target = target / part
        if target.is_symlink() or not target.is_dir():
            raise ns.SyncError('Некорректный моковый проект')
    if (target / '.git').is_symlink() or not (target / '.git').is_dir():
        raise ns.SyncError('Моковый Git отсутствует')
    package_root = shared / 'nexus-sync' / 'secrets'
    if (not re.fullmatch(r'[0-9a-f]{32}\.nxs', package.name) or package.parent.name != peer
            or package.is_symlink() or (shared / 'nexus-sync').is_symlink() or package_root.is_symlink()
            or package.parent.is_symlink() or package_root not in package.resolve(strict=True).parents):
        raise ns.SyncError('Некорректный тестовый пакет')
    package_hash = ns._digest(package)
    env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
    env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull,
                'GIT_TERMINAL_PROMPT': '0', 'GIT_ALLOW_PROTOCOL': 'file', 'GIT_OPTIONAL_LOCKS': '0'})
    def git(*args):
        result = subprocess.run(['git', '-C', str(target), '-c', 'core.hooksPath=/dev/null',
            '-c', 'core.fsmonitor=false', '-c', 'maintenance.auto=false', *args], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, timeout=120)
        if result.returncode:
            raise ns.SyncError('Не удалось проверить или получить тестовый Git')
        return result.stdout
    with tempfile.TemporaryDirectory(prefix='nexus-mock-git-fetch-') as tmp:
        contents = Path(tmp) / 'contents'
        ns.secrets_extract(package, contents, key, shared)
        info = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
        if not isinstance(info, dict) or info.get('format') != 'nexus-git-package-v1' or info.get('project') != project:
            raise ns.SyncError('Пакет относится к другому проекту')
        bundle = contents / 'repository.bundle'
        heads = git('bundle', 'list-heads', str(bundle)).splitlines()
        prefix = f'refs/nexus-sync/{peer}/{package.stem}/'
        expected = {}
        refspecs = []
        for line in heads:
            pieces = line.decode('utf-8').split(' ', 1)
            if len(pieces) != 2 or not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', pieces[0]):
                raise ns.SyncError('Некорректные ссылки тестового пакета')
            identity, source = pieces
            if not source.startswith(('refs/heads/', 'refs/remotes/', 'refs/tags/')):
                continue
            destination = prefix + source[len('refs/'):]
            git('check-ref-format', source)
            git('check-ref-format', destination)
            expected[destination] = identity
            refspecs.append(source + ':' + destination)
        if not expected:
            raise ns.SyncError('В тестовом пакете нет переносимых ссылок')
        def current_refs():
            lines = git('for-each-ref', '--format=%(refname) %(objectname)', prefix).decode('utf-8').splitlines()
            return dict(line.split(' ', 1) for line in lines)
        existing = current_refs()
        if existing:
            if existing != expected:
                raise ns.SyncError('Ссылки этого пакета уже отличаются')
            return {'state': 'UNCHANGED', 'mock_only': True, 'refs': len(expected), 'index_updated': False}
        if ns._digest(package) != package_hash:
            raise ns.SyncError('Пакет изменился')
        git('fetch', '--atomic', '--no-tags', '--no-write-fetch-head', '--no-recurse-submodules',
            '--no-auto-maintenance', '--', str(bundle), *refspecs)
        if current_refs() != expected:
            raise ns.SyncError('Тестовые ссылки не прошли проверку')
    return {'state': 'FETCHED', 'mock_only': True, 'refs': len(expected), 'index_updated': False}


def fast_forward_git(root: Path, shared: Path, key: bytes, package: Path, project: str, peer: str) -> dict:
    """Advance a clean, exclusively owned mock checkout to a verified same-branch descendant."""
    root, shared = _workspace(root, key), mock._mock_path(shared)
    # Validate the encrypted package and its immutable private refs first.
    fetch_git(root, shared, key, package, project, peer)
    target = root.joinpath(*ns._secret_parts(project, nested=True))
    package_hash = ns._digest(package)
    env = {name: value for name, value in os.environ.items() if not name.upper().startswith('GIT_')}
    env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull,
                'GIT_TERMINAL_PROMPT': '0', 'GIT_OPTIONAL_LOCKS': '0'})
    def git(*args, check=True):
        result = subprocess.run(['git', '-C', str(target), '-c', 'core.hooksPath=/dev/null',
            '-c', 'core.fsmonitor=false', '-c', 'maintenance.auto=false', '-c', 'merge.autoStash=false',
            *args], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=env, timeout=120)
        if check and result.returncode:
            raise ns.SyncError('Ошибка тестового Git')
        return result
    with tempfile.TemporaryDirectory(prefix='nexus-mock-git-advance-') as tmp:
        contents = Path(tmp) / 'contents'
        ns.secrets_extract(package, contents, key, shared)
        info = json.loads((contents / 'repository.json').read_text(encoding='utf-8'))
        branch, head = info.get('head_ref'), info.get('head')
        if (info.get('project') != project or not isinstance(branch, str) or not branch.startswith('refs/heads/')
                or not isinstance(head, str) or not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', head)
                or (contents / 'staged.patch').stat().st_size != 0):
            return {'state': 'BLOCKED', 'mock_only': True}
        if git('symbolic-ref', '-q', 'HEAD', check=False).stdout.decode().strip() != branch:
            return {'state': 'BLOCKED', 'mock_only': True}
        private_ref = f'refs/nexus-sync/{peer}/{package.stem}/' + branch[len('refs/'):]
        if git('rev-parse', '--verify', private_ref).stdout.decode().strip() != head:
            raise ns.SyncError('Тестовая Git-ссылка не соответствует пакету')
        if git('status', '--porcelain=v1', '-z', '--untracked-files=all').stdout:
            return {'state': 'BLOCKED', 'mock_only': True}
        current = git('rev-parse', '--verify', 'HEAD').stdout.decode().strip()
        if current == head:
            return {'state': 'UNCHANGED', 'mock_only': True}
        if git('merge-base', '--is-ancestor', current, head, check=False).returncode != 0:
            return {'state': 'BLOCKED', 'mock_only': True}
        if ns._digest(package) != package_hash:
            raise ns.SyncError('Тестовый пакет изменился')
        # The mock workspace must not be shared with Syncthing or other writers
        # during this explicit experiment; Git has no cross-process transaction
        # covering a status check and the following checkout.
        git('merge', '--ff-only', '--no-edit', '--no-overwrite-ignore', head)
        if (git('rev-parse', '--verify', 'HEAD').stdout.decode().strip() != head
                or git('status', '--porcelain=v1', '-z', '--untracked-files=all').stdout):
            raise ns.SyncError('Тестовый fast-forward не прошёл проверку')
    return {'state': 'FAST_FORWARDED', 'mock_only': True}


def main() -> int:
    parser = argparse.ArgumentParser(description='Восстановление Git только во временном моковом проекте')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('init', 'apply', 'fetch'):
        command = sub.add_parser(name)
        command.add_argument('--root', type=Path, required=True)
        command.add_argument('--shared', type=Path, required=True)
        command.add_argument('--key-file', type=Path, required=True)
        if name in ('apply', 'fetch'):
            command.add_argument('--package', type=Path, required=True)
            command.add_argument('--project', required=True)
        if name == 'fetch':
            command.add_argument('--source', required=True)
    args = parser.parse_args()
    try:
        mock._mock_path(args.shared)
        mock._mock_path(args.key_file)
        key = ns.read_key(args.key_file, args.shared)
        if args.command == 'init':
            init_workspace(args.root, args.shared, key)
            print(json.dumps({'state': 'INITIALIZED', 'mock_only': True}))
        elif args.command == 'apply':
            print(json.dumps(apply_git(args.root, args.shared, key, args.package, args.project)))
        else:
            print(json.dumps(fetch_git(args.root, args.shared, key, args.package, args.project, args.source)))
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
        print('Моковое восстановление Git отложено', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
