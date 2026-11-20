"""Isolated Syncthing process setup and read-only local API access."""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


class EngineError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def bundled_binary() -> Path:
    base = Path(getattr(sys, '_MEIPASS', Path(__file__).resolve().parent / '_engine'))
    binary = base / ('syncthing.exe' if os.name == 'nt' else 'syncthing')
    if binary.is_symlink() or not binary.is_file():
        raise EngineError('Bundled engine is unavailable')
    return binary.resolve()


def clean_environment() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if not key.upper().startswith('ST')}


def prepare_managed(binary: Path, home: Path, *, gui_port: int,
                    listen_address: str = 'tcp://127.0.0.1:0') -> None:
    """Create an isolated, loopback-only profile; never alter an existing one."""
    try:
        listener_url = urllib.parse.urlsplit(listen_address)
        listener_port = listener_url.port
        listener_ip = ipaddress.ip_address(listener_url.hostname or '')
    except ValueError as exc:
        raise EngineError('Invalid engine listener') from exc
    if (listener_url.scheme != 'tcp' or listener_port is None or listener_url.path or listener_url.query
            or listener_url.fragment or listener_url.username is not None or listener_url.password is not None
            or (listener_port == 0 and not listener_ip.is_loopback)):
        raise EngineError('Invalid engine listener')
    if (not binary.is_absolute() or binary.is_symlink() or not binary.is_file()
            or not home.is_absolute() or home.exists() or home.is_symlink()
            or not home.parent.is_dir() or type(gui_port) is not int or not 1 <= gui_port <= 65535):
        raise EngineError('Invalid managed engine parameters')
    home.mkdir(mode=0o700)
    try:
        subprocess.run([str(binary), 'generate', '--home', str(home), '--no-port-probing'],
                       check=True, timeout=30, env=clean_environment(),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        path = home / 'config.xml'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
            raise EngineError('Invalid generated engine configuration')
        raw = path.read_bytes()
        if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
            raise EngineError('Invalid generated engine configuration')
        document = ET.fromstring(raw)
        gui, options = document.find('gui'), document.find('options')
        if document.tag != 'configuration' or gui is None or options is None or gui.find('address') is None:
            raise EngineError('Invalid generated engine configuration')
        for folder in document.findall('folder'):
            document.remove(folder)
        gui.find('address').text = f'127.0.0.1:{gui_port}'
        for key in ('globalAnnounceEnabled', 'localAnnounceEnabled', 'relaysEnabled', 'natEnabled', 'startBrowser'):
            entry = options.find(key)
            if entry is None:
                raise EngineError('Missing engine isolation setting')
            entry.text = 'false'
        listener = options.find('listenAddress')
        if listener is None:
            raise EngineError('Missing engine isolation setting')
        for other in options.findall('listenAddress')[1:]:
            options.remove(other)
        listener.text = listen_address
        accepted = options.find('urAccepted')
        if accepted is not None:
            accepted.text = '-1'
        ET.ElementTree(document).write(path, encoding='utf-8', xml_declaration=True)
        (home / '.cosyncx-managed').write_text('cosyncx-managed-v1\n', encoding='ascii')
    except (OSError, subprocess.SubprocessError, ET.ParseError) as exc:
        raise EngineError('Could not prepare isolated engine profile') from exc


def run_managed(binary: Path, home: Path) -> int:
    marker, config = home / '.cosyncx-managed', home / 'config.xml'
    if (not binary.is_absolute() or binary.is_symlink() or not binary.is_file()
            or not home.is_absolute() or home.is_symlink() or not home.is_dir()
            or marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 128
            or marker.read_text(encoding='ascii') != 'cosyncx-managed-v1\n'
            or config.is_symlink() or not config.is_file()):
        raise EngineError('Not an isolated CoSyncX engine profile')
    result = subprocess.run([str(binary), 'serve', '--home', str(home), '--no-browser', '--no-restart',
                             '--no-upgrade', '--log-file=-'], env=clean_environment())
    return result.returncode


class ExistingEngine:
    """Read-only access to a local Syncthing API, without exposing its key."""
    def __init__(self, url: str, key_file: Path, *, config_xml: bool = False):
        parsed = urllib.parse.urlsplit(url)
        try:
            port = parsed.port
        except ValueError as exc:
            raise EngineError('Invalid local API URL') from exc
        if (parsed.scheme != 'http' or parsed.hostname != '127.0.0.1' or not port
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
            raise EngineError('Only a loopback Syncthing API URL is supported')
        limit = 1024 * 1024 if config_xml else 512
        if key_file.is_symlink() or not key_file.is_file() or key_file.stat().st_size > limit:
            raise EngineError('Invalid API key file')
        self._url = f'http://127.0.0.1:{port}'
        self._key_file = key_file
        self._config_xml = config_xml
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def _key(self) -> str:
        limit = 1024 * 1024 if self._config_xml else 512
        before = self._key_file.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise EngineError('Invalid API key source')
        fd = os.open(self._key_file, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (not stat.S_ISREG(opened.st_mode) or
                    (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)):
                raise EngineError('API key source changed')
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise EngineError('Invalid API key source')
        if self._config_xml:
            if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
                raise EngineError('Invalid engine configuration')
            doc = ET.fromstring(raw)
            key = doc.findtext('gui/apikey') if doc.tag == 'configuration' else None
        else:
            key = raw.decode('ascii').strip()
        if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,256}', key):
            raise EngineError('Invalid API key')
        return key

    def _get(self, path: str):
        try:
            request = urllib.request.Request(self._url + '/rest/' + path, headers={'X-API-Key': self._key()})
            with self.opener.open(request, timeout=3) as response:
                raw = response.read(1024 * 1024 + 1)
            data = json.loads(raw)
            if len(raw) > 1024 * 1024 or not isinstance(data, (dict, list)):
                raise EngineError('Invalid engine response')
            return data
        except (OSError, ValueError, UnicodeError, ET.ParseError, urllib.error.URLError) as exc:
            raise EngineError('Local engine is unavailable') from exc

    def ping(self) -> dict[str, str]:
        data = self._get('system/ping')
        if not isinstance(data, dict) or data.get('ping') != 'pong':
            raise EngineError('Invalid engine response')
        return {'state': 'reachable'}

    def folder_status(self, folder_id: str) -> dict:
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', folder_id):
            raise EngineError('Invalid folder ID')
        data = self._get('db/status?' + urllib.parse.urlencode({'folder': folder_id}))
        if not isinstance(data, dict):
            raise EngineError('Invalid engine response')
        state = data.get('state')
        items, size, deleted = (data.get(name) for name in ('needTotalItems', 'needBytes', 'needDeletes'))
        if not isinstance(state, str) or any(type(value) is not int or value < 0 for value in (items, size, deleted)):
            raise EngineError('Incomplete engine response')
        return {'state': state, 'pending_items': items, 'pending_bytes': size, 'pending_deletions': deleted,
                'remote_freshness_confirmed': False}

    def _post(self, path: str, payload: dict) -> None:
        try:
            data = json.dumps(payload, ensure_ascii=True).encode('utf-8')
            request = urllib.request.Request(self._url + '/rest/' + path, data=data, method='POST',
                headers={'X-API-Key': self._key(), 'Content-Type': 'application/json'})
            with self.opener.open(request, timeout=5) as response:
                if not 200 <= response.status < 300:
                    raise EngineError('Engine rejected configuration')
                response.read(1024 * 1024 + 1)
        except (OSError, ValueError, UnicodeError, ET.ParseError, urllib.error.URLError) as exc:
            raise EngineError('Could not update local engine') from exc

    def add_peer(self, device_id: str, address: str) -> None:
        try:
            parsed = urllib.parse.urlsplit(address)
            port = parsed.port
        except ValueError as exc:
            raise EngineError('Invalid peer address') from exc
        if (not re.fullmatch(r'(?:[A-Z2-7]{7}-){7}[A-Z2-7]{7}', device_id)
                or parsed.scheme != 'tcp' or not parsed.hostname or not port or port > 65535
                or parsed.username is not None or parsed.password is not None
                or parsed.path or parsed.query or parsed.fragment
                or any(ord(char) < 33 or ord(char) > 126 for char in address)):
            raise EngineError('Invalid peer parameters')
        devices = self._get('config/devices')
        if not isinstance(devices, list) or any(not isinstance(item, dict) for item in devices):
            raise EngineError('Could not check existing devices')
        if any(item.get('deviceID') == device_id for item in devices):
            raise EngineError('Device already exists')
        own = self._get('system/status')
        template = self._get('config/defaults/device')
        if not isinstance(own, dict) or not isinstance(own.get('myID'), str) or own['myID'] == device_id or not isinstance(template, dict):
            raise EngineError('Device defaults are unavailable')
        template.update({'deviceID': device_id, 'name': 'CoSyncX peer', 'addresses': [address]})
        self._post('config/devices', template)

    def add_folder(self, folder_id: str, folder: Path, peer: str, *, mode: str) -> None:
        if (not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', folder_id)
                or not re.fullmatch(r'(?:[A-Z2-7]{7}-){7}[A-Z2-7]{7}', peer)
                or mode not in ('sendonly', 'sendreceive') or not folder.is_absolute()
                or folder.is_symlink() or not folder.is_dir()):
            raise EngineError('Invalid folder setup')
        target = folder.resolve(strict=True)
        folders = self._get('config/folders')
        if not isinstance(folders, list):
            raise EngineError('Could not check existing folders')
        for item in folders:
            if not isinstance(item, dict) or not isinstance(item.get('path'), str):
                raise EngineError('Could not check existing folders')
            path = Path(item['path']).expanduser()
            if not path.is_absolute():
                raise EngineError('Could not check existing folders')
            other = path.resolve(strict=False)
            if item.get('id') == folder_id or target == other or target in other.parents or other in target.parents:
                raise EngineError('Folder already exists or overlaps an existing folder')
        devices = self._get('config/devices')
        system = self._get('system/status')
        template = self._get('config/defaults/folder')
        if (not isinstance(devices, list) or not any(isinstance(d, dict) and d.get('deviceID') == peer for d in devices)
                or not isinstance(system, dict) or not isinstance(system.get('myID'), str)
                or not isinstance(template, dict) or system['myID'] == peer):
            raise EngineError('Peer or folder defaults are unavailable')
        template.update({'id': folder_id, 'label': folder_id, 'path': str(target), 'type': mode,
                         'devices': [{'deviceID': system['myID']}, {'deviceID': peer}], 'ignorePerms': True})
        self._post('config/folders', template)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='CoSyncX Syncthing engine (experimental)')
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('prepare-managed', help='create an isolated loopback-only profile')
    prepare.add_argument('--binary', type=Path, help='override for an explicitly selected test binary')
    prepare.add_argument('--home', type=Path, required=True)
    prepare.add_argument('--gui-port', type=int, required=True)
    prepare.add_argument('--listen', default='tcp://127.0.0.1:0',
                         help='explicit TCP listener; default is isolated loopback')
    run = commands.add_parser('run-managed', help='run an isolated profile in the foreground')
    run.add_argument('--binary', type=Path, help='override for an explicitly selected test binary')
    run.add_argument('--home', type=Path, required=True)
    ping = commands.add_parser('ping-existing', help='check an existing loopback API without changing it')
    ping.add_argument('--url', required=True)
    secrets = ping.add_mutually_exclusive_group(required=True)
    secrets.add_argument('--api-key-file', type=Path)
    secrets.add_argument('--config', type=Path, help='existing Syncthing config.xml (read only)')
    status = commands.add_parser('status-existing', help='read a local folder state without changing it')
    status.add_argument('--url', required=True)
    status.add_argument('--folder', required=True)
    secrets = status.add_mutually_exclusive_group(required=True)
    secrets.add_argument('--api-key-file', type=Path)
    secrets.add_argument('--config', type=Path, help='existing Syncthing config.xml (read only)')
    peer = commands.add_parser('add-peer', help='explicitly add a new peer to a local engine')
    peer.add_argument('--url', required=True)
    peer.add_argument('--config', type=Path, required=True)
    peer.add_argument('--device-id', required=True)
    peer.add_argument('--address', required=True)
    folder = commands.add_parser('add-folder', help='explicitly add a new nonoverlapping local folder')
    folder.add_argument('--url', required=True)
    folder.add_argument('--config', type=Path, required=True)
    folder.add_argument('--folder', required=True)
    folder.add_argument('--path', type=Path, required=True)
    folder.add_argument('--device-id', required=True)
    folder.add_argument('--mode', choices=('sendonly', 'sendreceive'), required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'prepare-managed':
            prepare_managed(args.binary or bundled_binary(), args.home, gui_port=args.gui_port,
                            listen_address=args.listen)
            loopback = ipaddress.ip_address(urllib.parse.urlsplit(args.listen).hostname).is_loopback
            print(json.dumps({'state': 'prepared', 'external_network': not loopback}))
        elif args.command == 'run-managed':
            return run_managed(args.binary or bundled_binary(), args.home)
        elif args.command in ('add-peer', 'add-folder'):
            client = ExistingEngine(args.url, args.config, config_xml=True)
            if args.command == 'add-peer':
                client.add_peer(args.device_id, args.address)
            else:
                client.add_folder(args.folder, args.path, args.device_id, mode=args.mode)
            print(json.dumps({'state': 'submitted', 'synchronization_confirmed': False}))
        else:
            client = ExistingEngine(args.url, args.config or args.api_key_file, config_xml=bool(args.config))
            result = client.ping() if args.command == 'ping-existing' else client.folder_status(args.folder)
            print(json.dumps(result))
        return 0
    except (EngineError, OSError, UnicodeError, ValueError) as exc:
        print(f'Engine error: {type(exc).__name__}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
