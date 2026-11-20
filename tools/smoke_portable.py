"""Smoke-test a portable build using an isolated local Syncthing profile."""
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


def main():
    app = Path(sys.argv[1]).resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix='cosyncx-release-smoke-') as temporary:
        root = Path(temporary)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        home = root / 'engine'
        setup = subprocess.run([str(app), 'engine', 'prepare-managed', '--home', str(home),
                                '--gui-port', str(port)], capture_output=True, timeout=45)
        if setup.returncode:
            raise RuntimeError('Could not prepare the bundled engine')
        cfg = ET.parse(home / 'config.xml').getroot()
        api_key = cfg.findtext('gui/apikey')
        if not api_key or cfg.findall('folder') or cfg.findtext('options/listenAddress') != 'tcp://127.0.0.1:0':
            raise RuntimeError('Unexpected test profile')
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = f'http://127.0.0.1:{port}'
        process = subprocess.Popen([str(app), 'engine', 'run-managed', '--home', str(home)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ready = False
        try:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError('Bundled engine exited before startup')
                try:
                    check = subprocess.run([str(app), 'engine', 'ping-existing', '--url', url,
                                            '--config', str(home / 'config.xml')], capture_output=True,
                                           timeout=8, text=True)
                    if check.returncode == 0 and json.loads(check.stdout).get('state') == 'reachable':
                        ready = True
                        break
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    pass
                time.sleep(0.25)
            if not ready:
                raise RuntimeError('Bundled engine did not become reachable')
            request = urllib.request.Request(url + '/', headers={'Range': 'bytes=0-2047'})
            with opener.open(request, timeout=8) as response:
                page = response.read()
            if b'CoSyncX' not in page or b'SyncthingController' not in page:
                raise RuntimeError('Modified Syncthing interface was not served')
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                proxy_port = sock.getsockname()[1]
            proxy_url = f'http://127.0.0.1:{proxy_port}'
            proxy = subprocess.Popen([str(app), 'ui', '--existing-url', url, '--config', str(home / 'config.xml'),
                                      '--port', str(proxy_port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            proxy_ready = False
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if proxy.poll() is not None:
                        raise RuntimeError('Existing-engine interface exited before startup')
                    try:
                        with opener.open(proxy_url + '/assets/css/cosyncx.css', timeout=5) as response:
                            if b'cosyncx-brand' in response.read():
                                proxy_ready = True
                                break
                    except OSError:
                        time.sleep(0.2)
                if not proxy_ready:
                    raise RuntimeError('Existing-engine interface did not become reachable')
                with opener.open(proxy_url + '/meta.js', timeout=8) as response:
                    if b'authenticated' not in response.read():
                        raise RuntimeError('Existing engine did not supply GUI metadata')
            finally:
                if proxy_ready and proxy.poll() is None:
                    request = urllib.request.Request(proxy_url + '/cosyncx/close', data=b'', method='POST',
                                                     headers={'Origin': proxy_url})
                    with opener.open(request, timeout=8):
                        pass
                    proxy.wait(timeout=20)
            if proxy.returncode:
                raise RuntimeError('Existing-engine interface did not stop normally')
        finally:
            if ready and process.poll() is None:
                request = urllib.request.Request(url + '/rest/system/shutdown', data=b'', method='POST',
                                                 headers={'X-API-Key': api_key})
                with opener.open(request, timeout=8):
                    pass
                process.wait(timeout=20)
        if process.returncode:
            raise RuntimeError('Bundled engine did not stop normally')
    print('Portable engine: OK')


if __name__ == '__main__':
    main()
