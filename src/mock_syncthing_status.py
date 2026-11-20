#!/usr/bin/env python3
"""Read one disposable Syncthing folder's local and connected-peer state."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import mock_chat_sync as mock
import nexus_sync as ns


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def check(config: Path, folder: Path, folder_id: str, port: int) -> dict:
    """Use only a test XML and directory under the system temporary directory."""
    config, folder = mock._mock_path(config), mock._mock_path(folder)
    if (not config.is_file() or config.stat().st_size > 4 * 1024 * 1024 or not folder.is_dir()
            or not isinstance(folder_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', folder_id)
            or type(port) is not int or not 1 <= port <= 65535):
        raise ns.SyncError('Некорректные параметры мокового Syncthing')
    fd = os.open(config, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024 or b'<!DOCTYPE' in raw or b'<!ENTITY' in raw:
        raise ns.SyncError('Некорректная моковая конфигурация')
    document = ET.fromstring(raw)
    gui = document.find('gui') if document.tag == 'configuration' else None
    key = gui.findtext('apikey') if gui is not None else None
    entries = [item for item in document.findall('folder') if item.get('id') == folder_id]
    if (not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9]{16,128}', key)
            or len(entries) != 1 or Path(entries[0].get('path', '')).resolve(strict=False) != folder):
        raise ns.SyncError('Моковая папка не совпадает с конфигурацией')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    def get(path: str, params=None):
        url = f'http://127.0.0.1:{port}/rest/{path}'
        if params:
            url += '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers={'X-API-Key': key})
        with opener.open(request, timeout=1) as response:
            body = response.read(1024 * 1024 + 1)
        if len(body) > 1024 * 1024:
            raise ns.SyncError('Некорректный ответ мокового Syncthing')
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ns.SyncError('Некорректный ответ мокового Syncthing')
        return value
    try:
        running = get('config/folders/' + urllib.parse.quote(folder_id, safe=''))
        system = get('system/status')
        status = get('db/status', {'folder': folder_id})
        errors = get('folder/errors', {'folder': folder_id})
        connections = get('system/connections').get('connections')
        devices, own = running.get('devices'), system.get('myID')
        if (running.get('id') != folder_id or Path(running.get('path', '')).resolve(strict=False) != folder
                or not isinstance(devices, list) or not isinstance(own, str) or not isinstance(connections, dict)
                or len(devices) > 4 or 'errors' not in errors or errors['errors'] not in (None, [])
                or type(status.get('pullErrors')) is not int or status['pullErrors'] != 0):
            raise ns.SyncError('Состояние моковой папки не подтверждено')
        peers = [device.get('deviceID') for device in devices if isinstance(device, dict) and device.get('deviceID') != own]
        if len(peers) != len(devices) - 1 or len(set(peers)) != len(peers) or any(not isinstance(peer, str) for peer in peers):
            raise ns.SyncError('Не удалось определить моковые устройства')
        connected = current = 0
        for peer in peers:
            item = connections.get(peer)
            if not isinstance(item, dict) or item.get('connected') is not True:
                continue
            connected += 1
            completion = get('db/completion', {'folder': folder_id, 'device': peer})
            if (completion.get('remoteState') == 'valid' and type(completion.get('needItems')) is int
                    and type(completion.get('needBytes')) is int and type(completion.get('needDeletes')) is int
                    and completion['needItems'] == completion['needBytes'] == completion['needDeletes'] == 0):
                current += 1
        local = status.get('state')
        if not isinstance(local, str):
            raise ns.SyncError('Неизвестное состояние моковой папки')
        needed = tuple(status.get(field) for field in ('needTotalItems', 'needBytes', 'needDeletes'))
        if any(type(value) is not int or value < 0 for value in needed):
            raise ValueError('Неполное состояние моковой папки')
        state = ('WAITING' if not peers or connected != len(peers) else
                 'BUSY' if local != 'idle' or current != len(peers) or any(needed) else 'READY')
        return {'state': state, 'mock_only': True, 'local_folder_state': local,
                'connected_peers': connected, 'current_peers': current, 'expected_peers': len(peers),
                'remote_freshness_confirmed': False, 'cursor_chats_confirmed': False}
    except (OSError, ValueError, TypeError, urllib.error.URLError):
        return {'state': 'UNKNOWN', 'mock_only': True, 'remote_freshness_confirmed': False,
                'cursor_chats_confirmed': False}


def main() -> int:
    parser = argparse.ArgumentParser(description='Проверить временный локальный Syncthing')
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--folder', type=Path, required=True)
    parser.add_argument('--folder-id', required=True)
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()
    try:
        result = check(args.config, args.folder, args.folder_id, args.port)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result['state'] != 'UNKNOWN' else 1
    except (ns.SyncError, OSError, ValueError, TypeError, ET.ParseError):
        print('Моковый статус Syncthing недоступен', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
