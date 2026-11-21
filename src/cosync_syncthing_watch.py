"""Read the selected Syncthing peer/folder without modifying configuration."""
from pathlib import Path
from urllib.parse import quote


class WatchError(Exception):
    pass


class SyncthingSource:
    def __init__(self, client, folder_id: str, peer_id: str, sessions: Path):
        if not folder_id or not peer_id:
            raise WatchError('Folder and peer are required')
        self.client, self.folder_id, self.peer_id = client, folder_id, peer_id
        self.sessions = sessions.resolve(strict=True)
        if not self.sessions.is_dir():
            raise WatchError('Receive folder is not a directory')

    def read(self):
        folder = quote(self.folder_id, safe='')
        peer = quote(self.peer_id, safe='')
        config = self.client._get('config/folders/' + folder)
        if not isinstance(config, dict) or config.get('id') != self.folder_id or config.get('type') != 'receiveonly':
            raise WatchError('Expected configured receive-only folder')
        path = config.get('path')
        devices = config.get('devices')
        if (not isinstance(path, str) or not isinstance(devices, list)
                or not any(isinstance(device, dict) and device.get('deviceID') == self.peer_id for device in devices)):
            raise WatchError('Selected peer is not configured for this folder')
        try:
            if Path(path).resolve(strict=True) != self.sessions:
                raise WatchError('Configured folder path changed')
        except OSError as exc:
            raise WatchError('Configured folder is unavailable') from exc
        connections = self.client._get('system/connections')
        if not isinstance(connections, dict) or not isinstance(connections.get('connections'), dict):
            raise WatchError('Peer connection status unavailable')
        connection = connections['connections'].get(self.peer_id)
        connected = isinstance(connection, dict) and connection.get('connected') is True
        status = self.client._get('db/status?folder=' + folder)
        completion = self.client._get('db/completion?folder=' + folder + '&device=' + peer)
        if not isinstance(status, dict) or not isinstance(completion, dict):
            raise WatchError('Folder status unavailable')
        return self.folder_id, status, connected, completion.get('remoteState')
