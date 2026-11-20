"""Opt-in test of a temporary isolated Syncthing process on localhost."""
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and sys.platform == 'linux',
                     'requires opt-in local Syncthing on Linux')
class EngineIntegrationTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
