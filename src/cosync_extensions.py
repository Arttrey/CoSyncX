"""Encrypted, non-executing export of portable Codex user extensions."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import unicodedata

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import nexus_sync as ns

AAD = b'cosyncx-extension-bundle-v1'
LIMIT = 50 * 1024 * 1024
ROOTS = ('skills', 'hooks', 'plugins', 'rules')
FILES = ('AGENTS.md', 'config.toml', 'hooks.json')
SKIP = {('skills', '.system'), ('plugins', 'cache'), ('plugins', '.remote-plugin-install-staging')}
LOCAL_DIRS = {'node_modules', '.venv', 'venv', '__pycache__', '.cache', 'build', 'dist'}
COMPONENT = re.compile(r'[A-Za-z0-9._-]{1,128}\Z')


def _cached_plugin_inventory(root: Path) -> list[dict]:
    cache = root / 'plugins' / 'cache'
    if cache.is_symlink(): raise ns.SyncError('Plugin cache is a symbolic link')
    if not cache.is_dir(): return []
    result = []
    for registry in sorted(cache.iterdir()):
        if registry.is_symlink() or not registry.is_dir() or not COMPONENT.fullmatch(registry.name): continue
        for plugin in sorted(registry.iterdir()):
            if plugin.is_symlink() or not plugin.is_dir() or not COMPONENT.fullmatch(plugin.name): continue
            for version in sorted(plugin.iterdir()):
                if version.is_symlink() or not version.is_dir() or not COMPONENT.fullmatch(version.name): continue
                result.append({'registry': registry.name, 'plugin': plugin.name, 'version': version.name})
                if len(result) > 10000: raise ns.SyncError('Plugin inventory too large')
    return result


def _items(root: Path, excluded: list[dict]):
    for name in FILES:
        path = root / name
        if path.is_symlink(): raise ns.SyncError('Symbolic links are not supported')
        if path.is_file(): yield path
    for name in ROOTS:
        folder = root / name
        if folder.is_symlink(): raise ns.SyncError('Symbolic links are not supported')
        if not folder.exists(): continue
        if not folder.is_dir(): raise ns.SyncError('Extension root is not a directory')
        for current, dirs, files in os.walk(folder, followlinks=False):
            parent = Path(current)
            for sub in list(dirs):
                p = parent / sub
                if p.is_symlink(): raise ns.SyncError('Symbolic links are not supported')
                if tuple(p.relative_to(root).parts[:2]) in SKIP or sub in LOCAL_DIRS:
                    excluded.append({'path': p.relative_to(root).as_posix(), 'reason': 'system_or_local_directory'})
                    dirs.remove(sub)
            for name in files:
                path = parent / name
                if path.is_symlink(): raise ns.SyncError('Symbolic links are not supported')
                if name in ('.DS_Store', 'Thumbs.db') or path.suffix.lower() in ('.pyc', '.pyo', '.tmp', '.log', '.exe', '.dll', '.so', '.dylib', '.pyd', '.o', '.a'):
                    excluded.append({'path': path.relative_to(root).as_posix(), 'reason': 'local_or_platform_file'})
                    continue
                yield path


def _safe_name(value: str) -> tuple[str, ...]:
    if not isinstance(value, str) or '\\' in value or ':' in value:
        raise ns.SyncError('Invalid extension path')
    parts = value.split('/')
    if any(x in ('', '.', '..') for x in parts): raise ns.SyncError('Invalid extension path')
    if any(x in LOCAL_DIRS for x in parts[1:-1]): raise ns.SyncError('Local dependency directories are not portable')
    if not (value in FILES or (len(parts) >= 2 and parts[0] in ROOTS and tuple(parts[:2]) not in SKIP)):
        raise ns.SyncError('Extension path outside allowed roots')
    return tuple(parts)


def export_bundle(codex: Path, target: Path, key: bytes) -> int:
    root = codex.resolve(strict=True)
    parent = target.parent.resolve(strict=True)
    target = parent / target.name
    if not root.is_dir() or target.is_symlink() or target.exists() or parent == root or root in parent.parents or parent in root.parents:
        raise ns.SyncError('Unsafe extension bundle destination')
    if len(key) != 32: raise ns.SyncError('Invalid key')
    entries, excluded = [], []
    total = 0
    for path in _items(root, excluded):
        relative = path.relative_to(root).as_posix()
        _safe_name(relative)
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode): raise ns.SyncError('Only regular extension files are supported')
        total += before.st_size
        if total > LIMIT: raise ns.SyncError('Extension bundle exceeds 50 MiB')
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(path, flags)
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode) or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                raise ns.SyncError('Extension source changed while opening')
            content = stream.read(LIMIT + 1)
        after = path.lstat()
        if len(content) != before.st_size or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ns.SyncError('Extension file changed while reading')
        if content.startswith((b'\x7fELF', b'MZ', b'\xcf\xfa\xed\xfe', b'\xfe\xed\xfa\xcf', b'\xca\xfe\xba\xbe', b'\xbe\xba\xfe\xca')):
            excluded.append({'path': relative, 'reason': 'local_or_platform_file'})
            continue
        entries.append({'path': relative, 'data': base64.b64encode(content).decode('ascii'), 'sha256': hashlib.sha256(content).hexdigest(), 'executable': bool(before.st_mode & 0o111)})
    payload = json.dumps({'version': 1, 'files': entries, 'excluded': excluded, 'cached_plugins': _cached_plugin_inventory(root)}, separators=(',', ':')).encode()
    nonce = os.urandom(12)
    ciphertext = b'CSXEXT1\0' + nonce + AESGCM(key).encrypt(nonce, payload, AAD)
    with target.open('xb') as stream:
        os.chmod(target, 0o600)
        stream.write(ciphertext)
    return len(entries)


def extract_bundle(bundle: Path, destination: Path, key: bytes) -> int:
    if bundle.is_symlink() or not bundle.is_file() or len(key) != 32: raise ns.SyncError('Invalid extension bundle or key')
    if bundle.stat().st_size > LIMIT * 2: raise ns.SyncError('Extension bundle too large')
    raw = bundle.read_bytes()
    if not raw.startswith(b'CSXEXT1\0') or len(raw) < 36: raise ns.SyncError('Invalid extension bundle')
    try: data = json.loads(AESGCM(key).decrypt(raw[8:20], raw[20:], AAD))
    except (InvalidTag, ValueError, UnicodeError) as exc: raise ns.SyncError('Extension bundle authentication failed') from exc
    if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('files'), list): raise ns.SyncError('Invalid extension bundle')
    cached = data.get('cached_plugins', [])
    if not isinstance(cached, list) or len(cached) > 10000: raise ns.SyncError('Invalid plugin inventory')
    for item in cached:
        if not isinstance(item, dict) or set(item) != {'registry', 'plugin', 'version'} or any(not isinstance(item[k], str) or not COMPONENT.fullmatch(item[k]) for k in item):
            raise ns.SyncError('Invalid plugin inventory')
    excluded = data.get('excluded', [])
    if not isinstance(excluded, list) or len(excluded) > 100000: raise ns.SyncError('Invalid exclusion report')
    for item in excluded:
        if not isinstance(item, dict) or set(item) != {'path', 'reason'} or not isinstance(item['path'], str) or item['reason'] not in ('system_or_local_directory', 'local_or_platform_file'):
            raise ns.SyncError('Invalid exclusion report')
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if '.codex' in {part.casefold() for part in destination.parts} or destination.exists() or destination.is_symlink():
        raise ns.SyncError('Extraction requires a new separate directory')
    entries, seen, total = [], set(), 0
    for item in data['files']:
        if not isinstance(item, dict): raise ns.SyncError('Invalid extension entry')
        parts = _safe_name(item.get('path'))
        folded = unicodedata.normalize('NFD', '/'.join(parts).casefold())
        if folded in seen: raise ns.SyncError('Duplicate or case-conflicting extension paths')
        seen.add(folded)
        try: content = base64.b64decode(item['data'], validate=True)
        except (ValueError, KeyError, TypeError) as exc: raise ns.SyncError('Invalid extension content') from exc
        total += len(content)
        if total > LIMIT or hashlib.sha256(content).hexdigest() != item.get('sha256') or type(item.get('executable')) is not bool:
            raise ns.SyncError('Invalid extension content')
        entries.append((parts, content, item['executable']))
    with tempfile.TemporaryDirectory(prefix='.cosyncx-extensions-', dir=parent) as tmp:
        stage = Path(tmp)
        (stage / 'cached-plugin-inventory.json').write_text(json.dumps({'cached_plugins': cached, 'installed_not_verified': True}, indent=2) + '\n', encoding='utf-8')
        os.chmod(stage / 'cached-plugin-inventory.json', 0o600)
        (stage / 'excluded-files.json').write_text(json.dumps({'excluded': excluded}, indent=2) + '\n', encoding='utf-8')
        os.chmod(stage / 'excluded-files.json', 0o600)
        for parts, content, executable in entries:
            target = stage.joinpath(*parts)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with target.open('xb') as out: out.write(content)
            os.chmod(target, 0o700 if executable else 0o600)
        destination.mkdir(mode=0o700)
        for child in stage.iterdir(): os.replace(child, destination / child.name)
    return len(entries)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Encrypted extension export; extraction is quarantine only')
    sub = parser.add_subparsers(dest='action', required=True)
    exp = sub.add_parser('export'); exp.add_argument('--codex-home', type=Path, required=True); exp.add_argument('--output', type=Path, required=True); exp.add_argument('--key-file', type=Path, required=True)
    ext = sub.add_parser('extract'); ext.add_argument('--bundle', type=Path, required=True); ext.add_argument('--destination', type=Path, required=True); ext.add_argument('--key-file', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        shared = args.output.parent if args.action == 'export' else args.bundle.parent
        key = ns.read_key(args.key_file, shared.resolve(strict=True))
        count = export_bundle(args.codex_home, args.output, key) if args.action == 'export' else extract_bundle(args.bundle, args.destination, key)
        print(json.dumps({'action': args.action, 'files': count, 'applied': False}))
        return 0
    except (ns.SyncError, OSError, ValueError) as exc:
        print('CoSyncX extensions:', str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
