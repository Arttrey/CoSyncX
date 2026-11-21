"""Publish a stable copy of one conversation into an existing exchange folder."""
import argparse
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time

import nexus_sync as ns


_NAME = re.compile(r'^rollout-.*-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$')


def publish(source: Path, destination: Path, state_dir: Path, *, replace_initial: bool = False) -> str:
    """Copy a complete stable history; retain one backup and defer conflicts."""
    source, destination, state_dir = map(Path, (source, destination, state_dir))
    if not all(p.is_absolute() for p in (source, destination, state_dir)):
        raise ns.SyncError('Absolute paths required')
    if (source.is_symlink() or destination.is_symlink() or state_dir.is_symlink()
            or not source.is_file() or not destination.is_file() or not state_dir.is_dir()
            or source.resolve() == destination.resolve() or source.name != destination.name
            or not _NAME.fullmatch(source.name)):
        raise ns.SyncError('Invalid chat publication paths')
    if (state_dir.resolve() in (source.parent.resolve(), destination.parent.resolve())
            or destination.parent.resolve() in state_dir.resolve().parents):
        raise ns.SyncError('Publication state overlaps an exchange folder')
    if os.name != 'nt' and (state_dir.stat().st_uid != os.getuid() or stat.S_IMODE(state_dir.stat().st_mode) & 0o077):
        raise ns.SyncError('Publication state directory must be private')
    state_file, backup = state_dir / 'state.json', state_dir / 'latest.jsonl'
    if state_file.is_symlink() or backup.is_symlink():
        raise ns.SyncError('Invalid publication state')
    previous = None
    if state_file.exists():
        previous = json.loads(state_file.read_text())
        if not isinstance(previous, dict) or set(previous) != {'version', 'digest'} or previous['version'] != 1:
            raise ns.SyncError('Invalid publication state')
        if not isinstance(previous['digest'], str) or not re.fullmatch(r'[0-9a-f]{64}', previous['digest']):
            raise ns.SyncError('Invalid publication digest')
    before = ns._digest(destination)
    if previous and before != previous['digest']:
        return 'CONFLICT'
    if previous is None and not replace_initial:
        return 'NEEDS_INITIAL_APPROVAL'
    with tempfile.TemporaryDirectory(prefix='.publish-', dir=state_dir) as tmp:
        snapshot, old = Path(tmp) / 'new.jsonl', Path(tmp) / 'old.jsonl'
        ns._copy_stable_chat(source, snapshot)
        digest = ns._digest(snapshot)
        changed = digest != before
        if changed:
            ns._copy_stable_chat(destination, old)
            if ns._digest(old) != before or ns._digest(destination) != before:
                return 'CONFLICT'
            os.chmod(old, 0o600)
            with old.open('rb') as stream:
                os.fsync(stream.fileno())
            os.replace(old, backup)
            os.chmod(snapshot, 0o600)
            with snapshot.open('rb') as stream:
                os.fsync(stream.fileno())
            os.replace(snapshot, destination)
        fd, temporary = tempfile.mkstemp(prefix='.state-', dir=state_dir)
        try:
            with os.fdopen(fd, 'w') as stream:
                json.dump({'version': 1, 'digest': digest}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, state_file)
        finally:
            if os.path.exists(temporary): os.unlink(temporary)
    return 'PUBLISHED' if changed else 'UNCHANGED'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--destination', required=True, type=Path)
    parser.add_argument('--state-dir', required=True, type=Path)
    parser.add_argument('--replace-initial', action='store_true')
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    if not 10 <= args.interval <= 3600:
        parser.error('Interval must be between 10 and 3600 seconds')
    while True:
        try:
            result = publish(args.source, args.destination, args.state_dir, replace_initial=args.replace_initial)
        except (OSError, ValueError, ns.SyncError):
            result = 'RETRY'
        try:
            path = args.state_dir / 'health.json'
            if path.is_symlink(): raise ns.SyncError('Invalid publisher status')
            fd, temporary = tempfile.mkstemp(prefix='.health-', dir=args.state_dir)
            try:
                with os.fdopen(fd, 'w') as stream:
                    json.dump({'state': result, 'updated_at_ms': int(time.time() * 1000)}, stream)
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary): os.unlink(temporary)
        except (OSError, ns.SyncError):
            pass
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
