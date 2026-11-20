"""Prepare a verified Syncthing binary for a CoSyncX distribution."""
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile


class VendorError(Exception):
    pass


# Digests published on the official Syncthing v2.1.7 GitHub release.
RELEASES = {
    ('linux', 'amd64'): '2edc416e7ed136b7c858ed5f3f02a3b8316589605e54c380228ceea7acbf0142',
    ('linux', 'arm64'): '48e456690cd2b97e7d26b02b5ac35f62a3b36172267b144cfeaf02c993ab955c',
    ('macos', 'amd64'): 'b8be90176d0f24daebae7b5aaddac52ebfae395df6d99f5e0e54656db00868cb',
    ('macos', 'arm64'): '2df075b463231a307c91da657157beaed06383a941ba7ea04d33d536e3c4af38',
    ('windows', 'amd64'): '3ce7da770f546a8d02b65b2e7ee31f2a3b8b7c3f990ede045590cca4172ecfe1',
    ('windows', 'arm64'): 'd535ffd611b3af223de64bc5dfc60c378881eb13d5c8ecb1431b7407094c15ff',
}


def release_asset(system: str, arch: str) -> tuple[str, str, str]:
    checksum = RELEASES.get((system, arch))
    if checksum is None:
        raise VendorError('Unsupported engine platform')
    extension = 'tar.gz' if system == 'linux' else 'zip'
    return f'syncthing-{system}-{arch}-v2.1.7.{extension}', checksum, 'syncthing.exe' if system == 'windows' else 'syncthing'


def install_archive(archive: Path, destination: Path, expected_sha256: str, *, binary_name: str) -> None:
    """Install only the verified executable and license into a new directory."""
    if binary_name not in ('syncthing', 'syncthing.exe') or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256):
        raise VendorError('Invalid engine archive parameters')
    if archive.is_symlink() or not archive.is_file() or archive.stat().st_size > 100 * 1024 * 1024:
        raise VendorError('Invalid engine archive')
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    if not hmac.compare_digest(digest.hexdigest(), expected_sha256):
        raise VendorError('Engine archive checksum mismatch')
    if destination.exists() or destination.is_symlink() or not destination.parent.is_dir():
        raise VendorError('Engine destination must be a new directory')

    contents = {}
    required = {binary_name, 'LICENSE'}
    def select(name: str) -> str | None:
        parts = PurePosixPath(name).parts
        if len(parts) != 2 or not parts[0].startswith('syncthing-'):
            return None
        name = 'LICENSE' if parts[1] == 'LICENSE.txt' else parts[1]
        return name if name in required else None
    try:
        if zipfile.is_zipfile(archive):
            with zipfile.ZipFile(archive) as bundle:
                for entry in bundle.infolist():
                    name = select(entry.filename)
                    if name is None:
                        continue
                    mode = entry.external_attr >> 16
                    if (name in contents or entry.is_dir() or stat.S_ISLNK(mode)
                            or entry.file_size > (100 * 1024 * 1024 if name == binary_name else 1024 * 1024)):
                        raise VendorError('Invalid engine archive entry')
                    contents[name] = bundle.read(entry)
        else:
            with tarfile.open(archive, 'r:gz') as bundle:
                for entry in bundle:
                    name = select(entry.name)
                    if name is None:
                        continue
                    if (name in contents or not entry.isfile()
                            or entry.size > (100 * 1024 * 1024 if name == binary_name else 1024 * 1024)):
                        raise VendorError('Invalid engine archive entry')
                    stream = bundle.extractfile(entry)
                    if stream is None:
                        raise VendorError('Invalid engine archive entry')
                    with stream:
                        contents[name] = stream.read()
    except (OSError, tarfile.TarError, zipfile.BadZipFile) as exc:
        raise VendorError('Could not read engine archive') from exc
    if set(contents) != required or not all(contents.values()):
        raise VendorError('Engine archive is missing required files')
    created = False
    try:
        destination.mkdir(mode=0o700)
        created = True
        for name, data in contents.items():
            with (destination / name).open('xb') as stream:
                stream.write(data)
        if os.name != 'nt':
            (destination / binary_name).chmod(0o755)
    except OSError as exc:
        if created and destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        raise VendorError('Could not prepare engine directory') from exc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Prepare a verified Syncthing engine for distribution')
    parser.add_argument('--system', choices=('linux', 'windows', 'macos'), required=True)
    parser.add_argument('--arch', choices=('amd64', 'arm64'), required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--archive', type=Path, help='optional previously downloaded official archive')
    args = parser.parse_args(argv)
    try:
        name, checksum, binary = release_asset(args.system, args.arch)
        if args.archive:
            install_archive(args.archive, args.destination, checksum, binary_name=binary)
        else:
            url = 'https://github.com/syncthing/syncthing/releases/download/v2.1.7/' + name
            with tempfile.TemporaryDirectory(prefix='cosyncx-release-') as tmp:
                archive = Path(tmp) / name
                request = urllib.request.Request(url, headers={'User-Agent': 'CoSyncX-build'})
                with urllib.request.urlopen(request, timeout=60) as response, archive.open('xb') as output:
                    size = 0
                    while block := response.read(1024 * 1024):
                        size += len(block)
                        if size > 100 * 1024 * 1024:
                            raise VendorError('Engine archive is too large')
                        output.write(block)
                install_archive(archive, args.destination, checksum, binary_name=binary)
        print('Verified Syncthing engine and license prepared')
        return 0
    except (OSError, VendorError, urllib.error.URLError) as exc:
        print(f'Engine bundle error: {type(exc).__name__}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
