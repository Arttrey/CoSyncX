import http.client
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request

import cosync_proxy


class ExistingGuiProxyTests(unittest.TestCase):
    def test_rejects_cross_origin_mutations_and_untrusted_hosts(self):
        self.assertTrue(cosync_proxy.allowed_request('127.0.0.1:9876', 'http://127.0.0.1:9876',
                                                      'POST', '/rest/config/folders', port=9876))
        self.assertTrue(cosync_proxy.allowed_request('127.0.0.1:9876', 'http://127.0.0.1:9876',
                                                      'POST', '/cosyncx/close', port=9876))
        for host, origin, path in [
            ('evil.example:9876', 'http://127.0.0.1:9876', '/rest/config/folders'),
            ('127.0.0.1:9876', 'https://evil.example', '/rest/config/folders'),
            ('127.0.0.1:9876', 'http://127.0.0.1:9876', '/rest/system/shutdown'),
            ('127.0.0.1:9876', 'http://127.0.0.1:9876', '/%2e%2e/rest/config'),
        ]:
            self.assertFalse(cosync_proxy.allowed_request(host, origin, 'POST', path, port=9876))

    def test_static_routes_include_modified_syncthing_interface(self):
        self.assertEqual(cosync_proxy.override_file('/'), 'default/index.html')
        self.assertEqual(cosync_proxy.override_file('/assets/css/cosyncx.css'),
                         'default/assets/css/cosyncx.css')
        self.assertIsNone(cosync_proxy.override_file('/vendor/angular/angular.js'))
        self.assertIsNone(cosync_proxy.override_file('/../../secret'))

    def test_managed_mode_creates_and_runs_only_its_own_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary) / 'managed'
            binary = Path(temporary) / 'syncthing'
            binary.write_bytes(b'synthetic')
            with mock.patch.object(cosync_proxy.engine, 'prepare_managed') as prepare, \
                    mock.patch.object(cosync_proxy.engine, 'run_managed', return_value=0) as run:
                result = cosync_proxy.main(['--managed-home', str(home), '--gui-port', '29901',
                                            '--binary', str(binary)])
            self.assertEqual(result, 0)
            prepare.assert_called_once_with(binary, home, gui_port=29901, listen_address='tcp://127.0.0.1:0')
            run.assert_called_once_with(binary, home)

    @unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1', 'requires opt-in localhost')
    def test_truncated_upstream_response_returns_a_safe_gateway_error(self):
        client = mock.Mock()
        client._url = 'http://127.0.0.1:12345'
        client._key.return_value = 'synthetic-key'
        response = mock.MagicMock()
        response.read.side_effect = http.client.IncompleteRead(b'')
        client.opener.open.return_value = response
        server = cosync_proxy.make_proxy(client, 0)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with self.assertRaises(urllib.error.HTTPError) as error:
                opener.open(f'http://127.0.0.1:{server.server_port}/rest/system/status', timeout=5)
            self.assertEqual(error.exception.code, 502)
            error.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
