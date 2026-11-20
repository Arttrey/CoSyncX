"""Opt-in test of a temporary isolated Syncthing process on localhost."""
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine
import cosync_proxy
import mock_loopback_proxy


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and sys.platform == 'linux',
                     'requires opt-in local Syncthing on Linux')
class EngineIntegrationTests(unittest.TestCase):
    def test_modified_gui_can_use_existing_syncthing_without_changing_its_config(self):
        binary = Path(os.environ.get('COSYNC_TEST_ENGINE') or shutil.which('syncthing') or '').resolve()
        with tempfile.TemporaryDirectory(prefix='cosyncx-existing-gui-') as tmp:
            root = Path(tmp)
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            home = root / 'existing'
            engine.prepare_managed(binary, home, gui_port=port)
            config = home / 'config.xml'
            key = ET.parse(config).getroot().findtext('gui/apikey')
            client = engine.ExistingEngine(f'http://127.0.0.1:{port}', config, config_xml=True)
            process = subprocess.Popen([str(binary), 'serve', '--home', str(home), '--no-browser', '--no-restart',
                                        '--no-upgrade', '--log-file=-'], stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, start_new_session=True)
            server = thread = upstream_proxy = frontend_proxy = None
            try:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        client.ping()
                        break
                    except engine.EngineError:
                        if time.monotonic() >= deadline:
                            self.fail('existing engine unavailable')
                        time.sleep(0.2)
                before = config.read_bytes()
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    upstream_port = sock.getsockname()[1]
                upstream_proxy = mock_loopback_proxy.LoopbackMSSProxy(upstream_port, port)
                adapted = engine.ExistingEngine(f'http://127.0.0.1:{upstream_port}', config, config_xml=True)
                server = cosync_proxy.make_proxy(adapted, 0)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                ui_port = server.server_port
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    frontend_port = sock.getsockname()[1]
                frontend_proxy = mock_loopback_proxy.LoopbackMSSProxy(frontend_port, ui_port)
                base = f'http://127.0.0.1:{frontend_port}'
                headers = {'Host': f'127.0.0.1:{ui_port}'}
                request = urllib.request.Request(base + '/', headers=headers)
                with client.opener.open(request, timeout=15) as response:
                    interface = response.read().decode('utf-8')
                self.assertIn('CoSyncX', interface)
                self.assertIn('SyncthingController', interface)
                request = urllib.request.Request(base + '/vendor/angular/angular.js', headers=headers)
                with client.opener.open(request, timeout=15) as response:
                    self.assertIn(b'angular', response.read())
                request = urllib.request.Request(base + '/rest/system/status', headers=headers)
                with client.opener.open(request, timeout=10) as response:
                    status = response.read()
                self.assertIn(b'myID', status)
                self.assertNotIn(key.encode(), status)
                request = urllib.request.Request(base + '/rest/config/folders', data=b'{}', method='POST',
                    headers={**headers, 'Origin': 'https://untrusted.example', 'Content-Type': 'application/json'})
                with self.assertRaises(urllib.error.HTTPError) as failure:
                    client.opener.open(request, timeout=5)
                self.assertEqual(failure.exception.code, 403)
                failure.exception.close()
                self.assertEqual(config.read_bytes(), before)
                peer_home = root / 'peer'
                engine.prepare_managed(binary, peer_home, gui_port=port + 1 if port < 65535 else port - 1)
                peer_id = subprocess.check_output([str(binary), 'device-id', '--home', str(peer_home)],
                                                  text=True, timeout=15).strip()
                template = client._get('config/defaults/device')
                template.update({'deviceID': peer_id, 'name': 'synthetic peer', 'addresses': ['tcp://127.0.0.1:29999']})
                request = urllib.request.Request(base + '/rest/config/devices', data=json.dumps(template).encode(),
                    method='POST', headers={**headers, 'Origin': f'http://127.0.0.1:{ui_port}',
                                            'Content-Type': 'application/json'})
                with client.opener.open(request, timeout=10) as response:
                    self.assertEqual(response.status, 200)
                self.assertTrue(any(item.get('deviceID') == peer_id for item in client._get('config/devices')))
            finally:
                if frontend_proxy is not None:
                    frontend_proxy.close()
                if server is not None:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)
                if upstream_proxy is not None:
                    upstream_proxy.close()
                if process.poll() is None:
                    try:
                        request = urllib.request.Request(f'http://127.0.0.1:{port}/rest/system/shutdown',
                            data=b'', headers={'X-API-Key': key}, method='POST')
                        with client.opener.open(request, timeout=5):
                            pass
                    except OSError:
                        os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=20)

    def test_two_managed_nodes_transfer_a_synthetic_file(self):
        binary = Path(os.environ.get('COSYNC_TEST_ENGINE') or shutil.which('syncthing') or '').resolve()
        with tempfile.TemporaryDirectory(prefix='cosyncx-pair-test-') as tmp:
            root = Path(tmp)
            sockets = [socket.socket() for _ in range(4)]
            try:
                for sock in sockets:
                    sock.bind(('127.0.0.1', 0))
                ports = [sock.getsockname()[1] for sock in sockets]
            finally:
                for sock in sockets:
                    sock.close()
            homes = [root / 'source-engine', root / 'target-engine']
            projects = [root / 'source', root / 'target']
            processes = []
            clients = []
            keys = []
            try:
                for index in range(2):
                    projects[index].mkdir()
                    engine.prepare_managed(binary, homes[index], gui_port=ports[index],
                                           listen_address=f'tcp://127.0.0.1:{ports[index + 2]}')
                    key = ET.parse(homes[index] / 'config.xml').getroot().findtext('gui/apikey')
                    keys.append(key)
                    clients.append(engine.ExistingEngine(f'http://127.0.0.1:{ports[index]}',
                                                         homes[index] / 'config.xml', config_xml=True))
                    processes.append(subprocess.Popen([sys.executable, str(Path(engine.__file__)), 'run-managed',
                        '--binary', str(binary), '--home', str(homes[index])], stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL, start_new_session=True))
                ids = [subprocess.check_output([str(binary), 'device-id', '--home', str(home)],
                                               text=True, timeout=15).strip() for home in homes]
                for index, client in enumerate(clients):
                    deadline = time.monotonic() + 30
                    while True:
                        try:
                            client.ping()
                            break
                        except engine.EngineError:
                            if time.monotonic() >= deadline:
                                self.fail('test engine unavailable')
                            time.sleep(0.2)
                    client.add_peer(ids[1-index], f'tcp://127.0.0.1:{ports[3-index]}')
                    client.add_folder('mock-project', projects[index], ids[1-index],
                                      mode='sendonly' if index == 0 else 'sendreceive')
                (projects[0] / 'synthetic.txt').write_text('synthetic transfer\n', encoding='utf-8')
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    target = projects[1] / 'synthetic.txt'
                    if target.is_file() and target.read_text(encoding='utf-8') == 'synthetic transfer\n':
                        break
                    time.sleep(0.25)
                else:
                    self.fail('synthetic transfer did not finish')
                self.assertEqual(clients[0]._get('config/folders')[0]['type'], 'sendonly')
            finally:
                for index, process in enumerate(processes):
                    if process.poll() is None:
                        try:
                            request = urllib.request.Request(f'http://127.0.0.1:{ports[index]}/rest/system/shutdown',
                                data=b'', headers={'X-API-Key': keys[index]}, method='POST')
                            with clients[index].opener.open(request, timeout=5):
                                pass
                        except OSError:
                            os.killpg(process.pid, signal.SIGINT)
                    process.wait(timeout=20)

    def test_isolated_managed_engine_is_reachable_as_existing_engine(self):
        binary = Path(os.environ.get('COSYNC_TEST_ENGINE') or shutil.which('syncthing') or '').resolve()
        self.assertTrue(binary.is_file())
        with tempfile.TemporaryDirectory(prefix='cosyncx-engine-test-') as tmp:
            root = Path(tmp)
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            home = root / 'home'
            engine.prepare_managed(binary, home, gui_port=port)
            key = ET.parse(home / 'config.xml').getroot().findtext('gui/apikey')
            self.assertTrue(key)
            client = engine.ExistingEngine(f'http://127.0.0.1:{port}', home / 'config.xml', config_xml=True)
            process = subprocess.Popen([sys.executable, str(Path(engine.__file__)), 'run-managed',
                                        '--binary', str(binary), '--home', str(home)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        self.fail('managed engine exited before startup')
                    try:
                        state = client.ping()
                        break
                    except engine.EngineError:
                        if time.monotonic() >= deadline:
                            self.fail('managed engine did not become reachable')
                        time.sleep(0.2)
                self.assertEqual(state, {'state': 'reachable'})
                expected = (engine.gui_assets_dir() / 'default' / 'index.html').read_bytes()
                for marker in (b'CoSyncX', b'SyncthingController', b'addFolder()'):
                    offset = expected.index(marker)
                    request = urllib.request.Request(f'http://127.0.0.1:{port}/',
                        headers={'Range': f'bytes={offset}-{offset + len(marker) - 1}'})
                    with client.opener.open(request, timeout=5) as response:
                        self.assertEqual(response.read(), marker)
                with client.opener.open(f'http://127.0.0.1:{port}/assets/css/cosyncx.css', timeout=5) as response:
                    self.assertIn(b'cosyncx-brand', response.read())
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    proxy_port = sock.getsockname()[1]
                proxy = mock_loopback_proxy.LoopbackMSSProxy(proxy_port, port)
                try:
                    with client.opener.open(f'http://127.0.0.1:{proxy_port}/vendor/angular/angular.js', timeout=15) as response:
                        self.assertIn(b'angular', response.read())
                finally:
                    proxy.close()
                peer_home = root / 'peer-home'
                engine.prepare_managed(binary, peer_home, gui_port=port + 1 if port < 65535 else port - 1)
                peer_id = subprocess.check_output([str(binary), 'device-id', '--home', str(peer_home)],
                                                  text=True, timeout=15).strip()
                client.add_peer(peer_id, 'tcp://127.0.0.1:29999')
                project = root / 'project'
                project.mkdir()
                (project / 'synthetic.txt').write_text('synthetic data', encoding='utf-8')
                client.add_folder('mock-project', project, peer_id, mode='sendonly')
                configured = client._get('config/folders')
                entry = next(item for item in configured if item.get('id') == 'mock-project')
                self.assertEqual(entry['type'], 'sendonly')
                self.assertEqual(Path(entry['path']).resolve(), project.resolve())
            finally:
                if process.poll() is None:
                    try:
                        request = urllib.request.Request(f'http://127.0.0.1:{port}/rest/system/shutdown',
                                                         data=b'', headers={'X-API-Key': key}, method='POST')
                        with client.opener.open(request, timeout=5):
                            pass
                    except OSError:
                        os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=20)
            self.assertEqual(process.returncode, 0)

    @unittest.skipUnless(os.environ.get('COSYNC_TEST_APP'), 'requires built portable application')
    def test_portable_app_starts_bundled_engine_without_external_binary(self):
        app = Path(os.environ['COSYNC_TEST_APP']).resolve()
        with tempfile.TemporaryDirectory(prefix='cosyncx-portable-test-') as tmp:
            root = Path(tmp)
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            home = root / 'home'
            result = subprocess.run([str(app), 'engine', 'prepare-managed', '--home', str(home),
                                     '--gui-port', str(port)], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            key = ET.parse(home / 'config.xml').getroot().findtext('gui/apikey')
            self.assertTrue(key)
            client = engine.ExistingEngine(f'http://127.0.0.1:{port}', home / 'config.xml', config_xml=True)
            process = subprocess.Popen([str(app), 'engine', 'run-managed', '--home', str(home)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        self.fail('portable app exited before engine startup')
                    try:
                        self.assertEqual(client.ping(), {'state': 'reachable'})
                        break
                    except engine.EngineError:
                        if time.monotonic() >= deadline:
                            self.fail('portable engine did not become reachable')
                        time.sleep(0.2)
                check = subprocess.run([str(app), 'engine', 'ping-existing', '--url', f'http://127.0.0.1:{port}',
                                        '--config', str(home / 'config.xml')], capture_output=True, text=True, timeout=15)
                self.assertEqual(check.returncode, 0, check.stderr)
                self.assertIn('reachable', check.stdout)
                expected = (engine.gui_assets_dir() / 'default' / 'index.html').read_bytes()
                marker = b'CoSyncX'
                offset = expected.index(marker)
                request = urllib.request.Request(f'http://127.0.0.1:{port}/',
                    headers={'Range': f'bytes={offset}-{offset + len(marker) - 1}'})
                with client.opener.open(request, timeout=5) as response:
                    self.assertEqual(response.read(), marker)
            finally:
                if process.poll() is None:
                    try:
                        request = urllib.request.Request(f'http://127.0.0.1:{port}/rest/system/shutdown',
                                                         data=b'', headers={'X-API-Key': key}, method='POST')
                        with client.opener.open(request, timeout=5):
                            pass
                    except OSError:
                        os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=20)
            self.assertEqual(process.returncode, 0)

    @unittest.skipUnless(os.environ.get('COSYNC_TEST_APP'), 'requires built portable application')
    def test_portable_ui_command_creates_managed_interface(self):
        app = Path(os.environ['COSYNC_TEST_APP']).resolve()
        with tempfile.TemporaryDirectory(prefix='cosyncx-ui-portable-') as tmp:
            home = Path(tmp) / 'home'
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            process = subprocess.Popen([str(app), 'ui', '--managed-home', str(home), '--gui-port', str(port)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            client = None
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        self.fail('portable UI exited before startup')
                    try:
                        client = engine.ExistingEngine(f'http://127.0.0.1:{port}', home / 'config.xml',
                                                       config_xml=True)
                        client.ping()
                        break
                    except (engine.EngineError, OSError):
                        time.sleep(0.2)
                else:
                    self.fail('portable UI did not become reachable')
                expected = (engine.gui_assets_dir() / 'default' / 'index.html').read_bytes()
                marker = b'CoSyncX'
                offset = expected.index(marker)
                request = urllib.request.Request(f'http://127.0.0.1:{port}/',
                    headers={'Range': f'bytes={offset}-{offset + len(marker) - 1}'})
                with client.opener.open(request, timeout=5) as response:
                    self.assertEqual(response.read(), marker)
            finally:
                if process.poll() is None:
                    try:
                        if client is None:
                            raise OSError('UI not ready')
                        key = ET.parse(home / 'config.xml').getroot().findtext('gui/apikey')
                        request = urllib.request.Request(f'http://127.0.0.1:{port}/rest/system/shutdown',
                                                         data=b'', headers={'X-API-Key': key}, method='POST')
                        with client.opener.open(request, timeout=5):
                            pass
                    except OSError:
                        os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=20)
            self.assertEqual(process.returncode, 0)

    @unittest.skipUnless(os.environ.get('COSYNC_TEST_APP'), 'requires built portable application')
    def test_portable_ui_command_connects_to_existing_instance(self):
        app = Path(os.environ['COSYNC_TEST_APP']).resolve()
        binary = Path(os.environ.get('COSYNC_TEST_ENGINE') or shutil.which('syncthing') or '').resolve()
        with tempfile.TemporaryDirectory(prefix='cosyncx-ui-existing-') as tmp:
            root = Path(tmp)
            sockets = [socket.socket() for _ in range(3)]
            try:
                for sock in sockets:
                    sock.bind(('127.0.0.1', 0))
                engine_port, proxy_port, mss_port = [sock.getsockname()[1] for sock in sockets]
            finally:
                for sock in sockets:
                    sock.close()
            home = root / 'existing'
            engine.prepare_managed(binary, home, gui_port=engine_port)
            config = home / 'config.xml'
            key = ET.parse(config).getroot().findtext('gui/apikey')
            client = engine.ExistingEngine(f'http://127.0.0.1:{engine_port}', config, config_xml=True)
            process = subprocess.Popen([str(binary), 'serve', '--home', str(home), '--no-browser', '--no-restart',
                                        '--no-upgrade', '--log-file=-'], stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, start_new_session=True)
            gui_process = proxy = None
            try:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        client.ping()
                        break
                    except engine.EngineError:
                        if time.monotonic() >= deadline:
                            self.fail('existing engine unavailable')
                        time.sleep(0.2)
                before = config.read_bytes()
                gui_process = subprocess.Popen([str(app), 'ui', '--existing-url', f'http://127.0.0.1:{engine_port}',
                    '--config', str(config), '--port', str(proxy_port)], stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, start_new_session=True)
                proxy = mock_loopback_proxy.LoopbackMSSProxy(mss_port, proxy_port)
                request = urllib.request.Request(f'http://127.0.0.1:{mss_port}/',
                                                 headers={'Host': f'127.0.0.1:{proxy_port}'})
                deadline = time.monotonic() + 30
                while True:
                    if gui_process.poll() is not None:
                        self.fail('portable proxy exited before startup')
                    try:
                        with client.opener.open(request, timeout=5) as response:
                            page = response.read()
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            self.fail('portable proxy unavailable')
                        time.sleep(0.2)
                self.assertIn(b'CoSyncX', page)
                self.assertIn(b'SyncthingController', page)
                self.assertEqual(config.read_bytes(), before)
            finally:
                if proxy is not None:
                    proxy.close()
                if gui_process is not None and gui_process.poll() is None:
                    os.killpg(gui_process.pid, signal.SIGINT)
                    gui_process.wait(timeout=20)
                if process.poll() is None:
                    try:
                        request = urllib.request.Request(f'http://127.0.0.1:{engine_port}/rest/system/shutdown',
                                                         data=b'', headers={'X-API-Key': key}, method='POST')
                        with client.opener.open(request, timeout=5):
                            pass
                    except OSError:
                        os.killpg(process.pid, signal.SIGINT)
                process.wait(timeout=20)


if __name__ == '__main__':
    unittest.main()
