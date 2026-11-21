"""Read-only status of one Syncthing folder; never claims chats were imported."""
import argparse
import json
from pathlib import Path
from urllib.parse import quote

from cosync_engine import ExistingEngine, EngineError
from cosync_restore_status import read_status


def restore_status(path: Path) -> str | None:
    """Read a fresh private receiver status; never accept mock results."""
    value = read_status(path, mock_only=False)
    states = {'TRANSFERRING': 'TRANSFERRING', 'RESTORING': 'RESTORING',
              'PENDING': 'PENDING', 'COMPLETE': 'RESTORED', 'ERROR': 'ERROR'}
    return states.get(value.get('state')) if value is not None else None


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
