"""Serve CoSyncX's Syncthing GUI over an existing loopback-only instance."""
from __future__ import annotations

import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine

MAX_BODY = 16 * 1024 * 1024
BLOCKED_ACTIONS = {'/rest/system/shutdown', '/rest/system/restart', '/rest/system/reset', '/rest/system/upgrade'}


def override_file(path: str) -> str | None:
    overrides = {'/': 'default/index.html', '/index.html': 'default/index.html',
                 '/assets/css/cosyncx.css': 'default/assets/css/cosyncx.css',
                 '/assets/lang/lang-en.json': 'default/assets/lang/lang-en.json',
                 '/assets/lang/lang-ru.json': 'default/assets/lang/lang-ru.json'}
    return overrides.get(path)


def allowed_request(host: str, origin: str, method: str, raw_path: str, *, port: int) -> bool:
    if host != f'127.0.0.1:{port}' or method not in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
        return False
    try:
        parsed = urllib.parse.urlsplit(raw_path)
        path = urllib.parse.unquote(parsed.path, errors='strict')
    except (ValueError, UnicodeError):
        return False
    if (parsed.scheme or parsed.netloc or not path.startswith('/') or path.startswith('//')
            or '\\' in path or any(part in ('.', '..') for part in path.split('/'))
            or any(ord(c) < 32 for c in path) or path in BLOCKED_ACTIONS):
        return False
    if method != 'GET':
        return origin == f'http://127.0.0.1:{port}' and path.startswith('/rest/')
    return (override_file(path) is not None or path in ('/meta.js', '/themes.json', '/modal.html')
            or path.startswith(('/rest/', '/qr/', '/assets/', '/vendor/', '/syncthing/', '/theme-assets/')))


def make_proxy(client: engine.ExistingEngine, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, status: int, body: bytes, mime: str = 'text/plain; charset=utf-8', cookies=()):
            try:
                self.send_response(status)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('X-Frame-Options', 'DENY')
                for cookie in cookies:
                    self.send_header('Set-Cookie', cookie)
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def forward(self):
            if not allowed_request(self.headers.get('Host', ''), self.headers.get('Origin', ''),
                                   self.command, self.path, port=self.server.server_port):
                return self.respond(403, b'Forbidden')
            path = urllib.parse.urlsplit(self.path).path
            override = override_file(path) if self.command == 'GET' else None
            if override:
                file = engine.gui_assets_dir() / override
                suffix = file.suffix.lower()
                mime = {'.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8',
                        '.json': 'application/json; charset=utf-8'}[suffix]
                return self.respond(200, file.read_bytes(), mime)
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if length < 0 or length > MAX_BODY or (self.command == 'GET' and length):
                    return self.respond(400, b'Invalid request')
                body = self.rfile.read(length) if self.command != 'GET' else None
                headers = {'X-API-Key': client._key()}
                for name in ('Content-Type', 'Accept', 'Cookie', 'Range'):
                    if value := self.headers.get(name):
                        headers[name] = value
                request = urllib.request.Request(client._url + self.path, data=body, headers=headers,
                                                 method=self.command)
                try:
                    response = client.opener.open(request, timeout=75)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    data = response.read(MAX_BODY + 1)
                    if len(data) > MAX_BODY:
                        return self.respond(502, b'Upstream response too large')
                    mime = response.headers.get('Content-Type', 'application/octet-stream')
                    cookies = response.headers.get_all('Set-Cookie', [])
                    return self.respond(response.status, data, mime, cookies)
            except (OSError, ValueError, UnicodeError, http.client.HTTPException, engine.EngineError):
                return self.respond(502, b'Local Syncthing is unavailable')

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = forward

    return ThreadingHTTPServer(('127.0.0.1', port), Handler)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Open the CoSyncX interface using Syncthing')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--existing-url', help='local HTTP API URL, e.g. http://127.0.0.1:8384')
    mode.add_argument('--managed-home', type=Path, help='separate CoSyncX-managed Syncthing profile')
    parser.add_argument('--config', type=Path, help='existing Syncthing config.xml (read only)')
    parser.add_argument('--gui-port', type=int, default=8385, help='local GUI port for a managed profile')
    parser.add_argument('--listen', default='tcp://127.0.0.1:0', help='managed sync listener')
    parser.add_argument('--binary', type=Path, help='explicit Syncthing executable for testing')
    parser.add_argument('--port', type=int, default=0, help='local UI port; 0 selects a free port')
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535 or not 1 <= args.gui_port <= 65535:
        parser.error('invalid port')
    if args.existing_url and (args.config is None or args.binary is not None):
        parser.error('existing mode requires --config and does not use --binary')
    if args.managed_home is not None and args.config is not None:
        parser.error('managed mode does not use --config')
    try:
        if args.managed_home is not None:
            binary = args.binary or engine.bundled_binary()
            home = args.managed_home.expanduser()
            if not home.exists() and not home.is_symlink():
                engine.prepare_managed(binary, home, gui_port=args.gui_port, listen_address=args.listen)
            else:
                config = home / 'config.xml'
                if config.is_symlink() or not config.is_file() or config.stat().st_size > 1024 * 1024:
                    raise engine.EngineError('Invalid managed profile')
                raw = config.read_bytes()
                if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
                    raise engine.EngineError('Invalid managed profile')
                root = ET.fromstring(raw)
                if root.findtext('gui/address') != f'127.0.0.1:{args.gui_port}':
                    raise engine.EngineError('Managed GUI address does not match')
            print(f'CoSyncX: http://127.0.0.1:{args.gui_port}/', flush=True)
            return engine.run_managed(binary, home)
        client = engine.ExistingEngine(args.existing_url, args.config, config_xml=True)
        version = client._get('system/version')
        if not isinstance(version, dict) or str(version.get('version', '')).split('+')[0].split('-')[0] != 'v2.1.7':
            raise engine.EngineError('Existing Syncthing version is not supported')
        server = make_proxy(client, args.port)
        print(f'CoSyncX: http://127.0.0.1:{server.server_port}/', flush=True)
        try:
            server.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0
    except (engine.EngineError, OSError, ValueError, ET.ParseError):
        print('Не удалось запустить интерфейс CoSyncX или подключиться к Syncthing 2.1.7.', file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
