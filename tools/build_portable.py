"""Build a one-file CoSyncX executable on the target operating system."""
import argparse
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import cosync_vendor


def host_target():
    systems = {'Linux': 'linux', 'Windows': 'windows', 'Darwin': 'macos'}
    arches = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}
    system = systems.get(platform.system())
    arch = arches.get(platform.machine().lower())
    if not system or not arch:
        raise cosync_vendor.VendorError('Unsupported build host')
    return system, arch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new distribution directory')
    parser.add_argument('--archive', type=Path, help='previously downloaded official Syncthing archive')
    args = parser.parse_args()
    output = args.output.expanduser().absolute()
    if output.exists() or output.is_symlink() or not output.parent.is_dir():
        parser.error('output must be a new directory with an existing parent')
    system, arch = host_target()
    _, checksum, binary_name = cosync_vendor.release_asset(system, arch)
    with tempfile.TemporaryDirectory(prefix='cosyncx-portable-build-') as tmp:
        tmp = Path(tmp)
        engine = tmp / 'engine'
        if args.archive:
            cosync_vendor.install_archive(args.archive, engine, checksum, binary_name=binary_name)
        else:
            result = cosync_vendor.main(['--system', system, '--arch', arch, '--destination', str(engine)])
            if result:
                return result
        subprocess.run([sys.executable, '-m', 'PyInstaller', '--noconfirm', '--clean', '--onefile',
                        '--name', 'cosyncx', '--distpath', str(tmp / 'dist'),
                        '--workpath', str(tmp / 'work'), '--specpath', str(tmp / 'spec'),
                        '--add-binary', str(engine / binary_name) + ':.',
                        '--add-data', str(ROOT / 'src/cosyncx_gui') + ':cosyncx_gui',
                        str(ROOT / 'src/cosyncx.py')],
                       check=True, cwd=ROOT)
        output.mkdir(mode=0o700)
        executable = 'cosyncx.exe' if system == 'windows' else 'cosyncx'
        shutil.copy2(tmp / 'dist' / executable, output / executable)
        shutil.copy2(ROOT / 'LICENSE', output / 'LICENSE')
        shutil.copy2(engine / 'LICENSE', output / 'SYNCTHING-LICENSE.txt')
    print(f'Built {system}-{arch} distribution: {output}')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, cosync_vendor.VendorError, subprocess.SubprocessError) as exc:
        print(f'Build failed: {type(exc).__name__}', file=sys.stderr)
        raise SystemExit(2)
