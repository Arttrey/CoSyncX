"""Read-only status of one Syncthing folder; never claims chats were imported."""
import argparse
import json
import os
from pathlib import Path
import stat
import time
from urllib.parse import quote

from cosync_engine import ExistingEngine, EngineError


def restore_status(path: Path) -> str | None:
    """Read a fresh private receiver status; never accept mock results."""
    if not path.is_absolute():
        return None
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > 65536:
            return None
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(path, flags)
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (not stat.S_ISREG(opened.st_mode) or opened.st_size > 65536
                    or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)):
                return None
            if os.name != 'nt' and (opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) & 0o077):
                return None
            raw = stream.read(65537)
            after = os.fstat(stream.fileno())
        if (len(raw) != opened.st_size or (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            return None
        value = json.loads(raw)
        now = int(time.time() * 1000)
        if (not isinstance(value, dict) or value.get('format') != 'cosyncx-restore-status-v1'
                or value.get('mock_only') is not False or type(value.get('updated_at_ms')) is not int
                or value['updated_at_ms'] > now + 5000 or now - value['updated_at_ms'] > 90000):
            return None
        states = {'TRANSFERRING': 'TRANSFERRING', 'RESTORING': 'RESTORING',
                  'PENDING': 'PENDING', 'COMPLETE': 'RESTORED', 'ERROR': 'ERROR'}
        return states.get(value.get('state'))
    except (OSError, ValueError, TypeError):
        return None


def transfer_status(client, folder, peer):
    data = client._get('db/status?folder=' + quote(folder, safe=''))
    connections = client._get('system/connections').get('connections', {})
    completion = client._get('db/completion?folder=' + quote(folder, safe='') + '&device=' + quote(peer, safe=''))
    if not connections.get(peer, {}).get('connected') or completion.get('remoteState') != 'valid':
        return {'state': 'DISCONNECTED'}
    if data.get('error') or data.get('invalid') or data.get('errors') or data.get('pullErrors'):
        return {'state': 'ERROR'}
    remote = data.get('remoteSequence')
    if not isinstance(remote, dict) or type(remote.get(peer)) is not int or remote[peer] < 0:
        return {'state': 'TRANSFERRING'}
    needs = ('needFiles', 'needDirectories', 'needSymlinks', 'needDeletes')
    if any(type(data.get(x)) is not int or data[x] < 0 for x in needs):
        return {'state': 'ERROR'}
    if data.get('state') != 'idle' or any(data[x] for x in needs):
        return {'state': 'TRANSFERRING'}
    return {'state': 'TRANSFERRED', 'restore': 'NOT_VERIFIED'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--folder', required=True)
    parser.add_argument('--peer', required=True)
    parser.add_argument('--restore-status', type=Path)
    args = parser.parse_args()
    try:
        value = transfer_status(ExistingEngine(args.url, args.config, config_xml=True), args.folder, args.peer)
        if value.get('state') == 'TRANSFERRED' and args.restore_status:
            received = restore_status(args.restore_status)
            if received is not None:
                value = {'state': received}
    except (EngineError, OSError, ValueError, TypeError, KeyError, AttributeError):
        value = {'state': 'UNAVAILABLE'}
    print(json.dumps(value))


if __name__ == '__main__':
    main()
