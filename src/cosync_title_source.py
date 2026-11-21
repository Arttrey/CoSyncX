"""Prepare a title-only Syncthing folder; leave it disconnected until verified."""
import os
from pathlib import Path
import time
import urllib.parse


IGNORE = b'!/session_index.jsonl\n*\n'


class TitleSetupError(Exception):
    pass


def prepare(client, profile: Path, folder_id: str):
    """Register only with this device. No peer is allowed before verification."""
    if not profile.is_absolute() or profile.is_symlink() or not profile.is_dir():
        raise TitleSetupError('Invalid profile')
    index = profile / 'session_index.jsonl'
    if index.is_symlink() or not index.is_file():
        raise TitleSetupError('Title index is unavailable')
    folders = client._get('config/folders')
    if not isinstance(folders, list) or any(f.get('id') == folder_id for f in folders):
        raise TitleSetupError('Folder already exists or configuration is invalid')
    if any(Path(f.get('path', '')).resolve() == profile.resolve() for f in folders):
        raise TitleSetupError('Profile already shared')
    ignore = profile / '.stignore'
    try:
        fd = os.open(ignore, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(IGNORE)
            out.flush()
            os.fsync(out.fileno())
    except FileExistsError as exc:
        raise TitleSetupError('Existing ignore rules require manual review') from exc
    system = client._get('system/status')
    own = system.get('myID')
    if not isinstance(own, str) or not own:
        raise TitleSetupError('Device ID unavailable')
    folder = client._get('config/defaults/folder')
    folder.update({'id': folder_id, 'label': 'CoSyncX titles', 'path': str(profile),
                   'type': 'sendonly', 'markerName': '.', 'devices': [{'deviceID': own}]})
    client._post('config/folders', folder)
    return own


def verify(client, profile: Path, folder_id: str, own: str, timeout=60):
    query = urllib.parse.urlencode({'folder': folder_id})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        folder = client._get('config/folders/' + urllib.parse.quote(folder_id, safe=''))
        if (folder.get('type') != 'sendonly' or folder.get('markerName') != '.'
                or Path(folder.get('path', '')).resolve() != profile.resolve()
                or [x.get('deviceID') for x in folder.get('devices', [])] != [own]
                or (profile / '.stignore').is_symlink()
                or (profile / '.stignore').read_bytes() != IGNORE):
            raise TitleSetupError('Title folder is not isolated')
        status = client._get('db/status?' + query)
        if status.get('state') == 'idle':
            if (status.get('localFiles') != 1 or status.get('localDirectories') != 0
                    or status.get('localSymlinks') != 0 or status.get('errors') != 0):
                raise TitleSetupError('Unexpected indexed items')
            entries = client._get('db/browse?' + query + '&levels=1')
            if (not isinstance(entries, list) or len(entries) != 1
                    or entries[0].get('name') != 'session_index.jsonl'
                    or entries[0].get('type') != 'FILE_INFO_TYPE_FILE'):
                raise TitleSetupError('Unexpected indexed names')
            return
        time.sleep(.5)
    raise TitleSetupError('Title scan did not complete')
