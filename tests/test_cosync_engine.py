import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

import cosync_engine as engine
import cosyncx


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_managed_setup_uses_new_isolated_profile_with_no_external_network(self):
        binary = self.root / 'syncthing'
        binary.write_bytes(b'synthetic executable')
        home = self.root / 'managed'

        def generate(command, **kwargs):
            self.assertEqual(command[:2], [str(binary), 'generate'])
            self.assertNotIn('STHOMEDIR', kwargs['env'])
            (home / 'config.xml').write_text(
                '<configuration><folder id="default"/><gui><address>127.0.0.1:8384</address>'
                '<apikey>synthetic-key</apikey></gui><options><listenAddress>default</listenAddress>'
                '<globalAnnounceEnabled>true</globalAnnounceEnabled><localAnnounceEnabled>true</localAnnounceEnabled>'
                '<relaysEnabled>true</relaysEnabled><natEnabled>true</natEnabled><startBrowser>true</startBrowser>'
                '<urAccepted>0</urAccepted></options></configuration>', encoding='utf-8')

        with mock.patch.dict('os.environ', {'STHOMEDIR': '/unrelated'}, clear=False):
            with mock.patch.object(engine.subprocess, 'run', side_effect=generate):
                engine.prepare_managed(binary, home, gui_port=29001)
        cfg = ET.parse(home / 'config.xml').getroot()
        self.assertEqual(cfg.findtext('gui/address'), '127.0.0.1:29001')
        self.assertEqual(cfg.findtext('options/listenAddress'), 'tcp://127.0.0.1:0')
        self.assertEqual(cfg.findall('folder'), [])
        self.assertEqual(cfg.findtext('options/globalAnnounceEnabled'), 'false')
        self.assertEqual(cfg.findtext('options/relaysEnabled'), 'false')
        with self.assertRaises(engine.EngineError):
            engine.prepare_managed(binary, home, gui_port=29001)

    def test_existing_mode_uses_only_loopback_and_does_not_return_api_key(self):
        key = self.root / 'key'
        key.write_text('SYNTHETIC_SECRET_KEY')
        client = engine.ExistingEngine('http://127.0.0.1:29002', key)
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({'ping': 'pong'}).encode()
        with mock.patch.object(client.opener, 'open', return_value=response) as request:
            self.assertEqual(client.ping(), {'state': 'reachable'})
        url_request = request.call_args.args[0]
        self.assertEqual(url_request.full_url, 'http://127.0.0.1:29002/rest/system/ping')
        self.assertEqual(url_request.get_header('X-api-key'), 'SYNTHETIC_SECRET_KEY')
        with self.assertRaises(engine.EngineError):
            engine.ExistingEngine('http://example.com:29002', key)
        with self.assertRaises(engine.EngineError):
            engine.ExistingEngine('http://127.0.0.1:29002/other', key)

    @unittest.skipUnless(engine.sys.platform == 'linux' and hasattr(socket, 'TCP_MAXSEG'), 'Linux TCP option')
    def test_local_api_sets_mss_before_connecting(self):
        with mock.patch.object(engine.socket, 'socket') as make_socket:
            sock = make_socket.return_value
            connection = engine.LocalHTTPConnection('127.0.0.1', 29002, timeout=3)
            connection.connect()
        self.assertIn(mock.call.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200), sock.mock_calls)
        self.assertLess(sock.mock_calls.index(mock.call.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200)),
                        sock.mock_calls.index(mock.call.connect(('127.0.0.1', 29002))))

    def test_managed_start_requires_its_own_marker_and_uses_foreground_process(self):
        binary = self.root / 'syncthing'
        binary.write_bytes(b'synthetic executable')
        home = self.root / 'managed'
        home.mkdir()
        with self.assertRaises(engine.EngineError):
            engine.run_managed(binary, home)
        (home / '.cosyncx-managed').write_text('cosyncx-managed-v1\n')
        (home / 'config.xml').write_text('<configuration/>')
        with mock.patch.object(engine.subprocess, 'run') as run:
            run.return_value.returncode = 0
            self.assertEqual(engine.run_managed(binary, home), 0)
        command = run.call_args.args[0]
        self.assertEqual(command[:2], [str(binary), 'serve'])
        self.assertIn('--no-browser', command)
        self.assertIn('--no-upgrade', command)
        self.assertIn(str(home), command)
        self.assertIn('--debug-gui-assets-dir', command)
        self.assertEqual(Path(command[command.index('--debug-gui-assets-dir') + 1]), engine.gui_assets_dir())

    def test_unified_launcher_routes_engine_commands(self):
        with mock.patch.object(engine, 'main', return_value=7) as command:
            self.assertEqual(cosyncx.main(['engine', 'ping-existing', '--url', 'http://127.0.0.1:1234']), 7)
        command.assert_called_once_with(['ping-existing', '--url', 'http://127.0.0.1:1234'])

    def test_bundled_binary_is_selected_without_system_path(self):
        binary = self.root / ('syncthing.exe' if engine.os.name == 'nt' else 'syncthing')
        binary.write_bytes(b'synthetic executable')
        with mock.patch.object(engine.sys, '_MEIPASS', str(self.root), create=True):
            self.assertEqual(engine.bundled_binary(), binary)
        with mock.patch.object(engine.sys, '_MEIPASS', str(self.root / 'missing'), create=True):
            with self.assertRaises(engine.EngineError):
                engine.bundled_binary()

    def test_existing_engine_reads_key_from_xml_and_reports_folder_state(self):
        config = self.root / 'config.xml'
        config.write_text('<configuration><gui><apikey>SYNTHETIC_SECRET_KEY</apikey></gui></configuration>')
        client = engine.ExistingEngine('http://127.0.0.1:29002', config, config_xml=True)
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            {'state': 'scanning', 'needTotalItems': 2, 'needBytes': 40, 'needDeletes': 0}).encode()
        with mock.patch.object(client.opener, 'open', return_value=response) as request:
            result = client.folder_status('projects')
        self.assertEqual(result['state'], 'scanning')
        self.assertEqual(result['pending_items'], 2)
        self.assertFalse(result['remote_freshness_confirmed'])
        self.assertNotIn('SYNTHETIC_SECRET_KEY', json.dumps(result))
        self.assertIn('folder=projects', request.call_args.args[0].full_url)

    def test_inspect_reports_selected_engine_without_exposing_api_key(self):
        config = self.root / 'config.xml'
        config.write_text('<configuration><gui><apikey>SYNTHETIC_SECRET_KEY</apikey></gui></configuration>')
        client = engine.ExistingEngine('http://127.0.0.1:29002', config, config_xml=True)
        own = 'ABCDEFG-' * 7 + 'ABCDEFG'
        peer = 'BCDEFGH-' * 7 + 'BCDEFGH'
        responses = {'system/status': {'myID': own},
                     'system/connections': {'connections': {peer: {'connected': True}}},
                     'config/devices': [{'deviceID': own, 'name': 'local'},
                                        {'deviceID': peer, 'name': 'vps', 'password': 'SYNTHETIC_SECRET_KEY'}],
                     'config/folders': [{'id': 'projects', 'path': '/projects', 'type': 'sendonly', 'paused': False}]}
        with mock.patch.object(client, '_get', side_effect=responses.__getitem__):
            result = client.inspect()
        self.assertEqual(result['device_id'], own)
        self.assertEqual(result['devices'], [{'id': peer, 'name': 'vps', 'connected': True}])
        self.assertEqual(result['folders'], [{'id': 'projects', 'path': '/projects', 'mode': 'sendonly', 'paused': False}])
        self.assertNotIn('SYNTHETIC_SECRET_KEY', json.dumps(result))
        self.assertFalse(result['synchronization_confirmed'])

    def test_folder_setup_creates_only_new_nonoverlapping_selected_folder(self):
        key = self.root / 'key'
        key.write_text('SYNTHETIC_SECRET_KEY')
        client = engine.ExistingEngine('http://127.0.0.1:29002', key)
        folder = self.root / 'project'
        folder.mkdir()
        device = 'ABCDEFG-' * 7 + 'ABCDEFG'
        def fetch(path):
            return {'config/folders': [], 'config/devices': [{'deviceID': device}],
                    'config/defaults/folder': {'devices': [], 'label': ''},
                    'system/status': {'myID': 'BCDEFGH-' * 7 + 'BCDEFGH'}}[path]
        with mock.patch.object(client, '_get', side_effect=fetch), mock.patch.object(client, '_post') as post:
            client.add_folder('projects', folder, device, mode='sendonly')
        self.assertEqual(post.call_args.args[0], 'config/folders')
        self.assertEqual(post.call_args.args[1]['type'], 'sendonly')
        self.assertEqual(post.call_args.args[1]['path'], str(folder.resolve()))
        with mock.patch.object(client, '_get', return_value=[{'id': 'projects', 'path': str(folder)}]), \
                mock.patch.object(client, '_post') as post:
            with self.assertRaises(engine.EngineError):
                client.add_folder('projects', folder, device, mode='sendonly')
            post.assert_not_called()

    def test_archive_toggle_targets_only_selected_folder(self):
        key = self.root / 'key'
        key.write_text('SYNTHETIC_SECRET_KEY')
        client = engine.ExistingEngine('http://127.0.0.1:29002', key)
        with mock.patch.object(client, '_get', side_effect=[{'id': 'archive-test', 'paused': False},
                                                           {'id': 'archive-test', 'paused': True}]), \
                mock.patch.object(client, '_patch') as patch:
            self.assertEqual(client.set_folder_paused('archive-test', True), {'folder': 'archive-test', 'paused': True})
        patch.assert_called_once_with('config/folders/archive-test', {'paused': True})
        with mock.patch.object(client, '_patch') as patch:
            with self.assertRaises(engine.EngineError):
                client.set_folder_paused('../other', True)
            patch.assert_not_called()

    def test_peer_setup_rejects_duplicates_and_invalid_addresses(self):
        key = self.root / 'key'
        key.write_text('SYNTHETIC_SECRET_KEY')
        client = engine.ExistingEngine('http://127.0.0.1:29002', key)
        device = 'ABCDEFG-' * 7 + 'ABCDEFG'
        def fetch(path):
            return {'config/devices': [], 'config/defaults/device': {'name': '', 'addresses': []},
                    'system/status': {'myID': 'BCDEFGH-' * 7 + 'BCDEFGH'}}[path]
        with mock.patch.object(client, '_get', side_effect=fetch), mock.patch.object(client, '_post') as post:
            client.add_peer(device, 'tcp://127.0.0.1:22000')
        self.assertEqual(post.call_args.args[0], 'config/devices')
        self.assertEqual(post.call_args.args[1]['addresses'], ['tcp://127.0.0.1:22000'])
        with mock.patch.object(client, '_get', return_value=[{'deviceID': device}]), \
                mock.patch.object(client, '_post') as post:
            with self.assertRaises(engine.EngineError):
                client.add_peer(device, 'tcp://127.0.0.1:22000')
            post.assert_not_called()
        with self.assertRaises(engine.EngineError):
            client.add_peer(device, 'https://example.com:443/path')

    def test_managed_setup_accepts_explicit_tcp_listener_only(self):
        binary = self.root / 'syncthing'
        binary.write_bytes(b'synthetic executable')
        home = self.root / 'managed'
        with self.assertRaises(engine.EngineError):
            engine.prepare_managed(binary, home, gui_port=29001, listen_address='https://example.com:443/path')
        self.assertFalse(home.exists())


if __name__ == '__main__':
    unittest.main()
