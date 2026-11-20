import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from urllib.parse import urlparse

import mock_syncthing_status as status


class MockSyncthingStatusTests(unittest.TestCase):
    def test_connection_and_completion_are_both_required(self):
        with tempfile.TemporaryDirectory(prefix='nexus-status-test-') as tmp:
            root = Path(tmp)
            folder = root / 'project'
            folder.mkdir()
            config = root / 'config.xml'
            config.write_text(f'<configuration><gui><apikey>0123456789abcdef</apikey></gui>'
                              f'<folder id="test-folder" path="{folder}"/></configuration>')
            replies = {
                '/rest/config/folders/test-folder': {'id': 'test-folder', 'path': str(folder),
                    'devices': [{'deviceID': 'local'}, {'deviceID': 'peer'}]},
                '/rest/system/status': {'myID': 'local'},
                '/rest/db/status': {'state': 'idle', 'pullErrors': 0,
                                    'needTotalItems': 0, 'needBytes': 0, 'needDeletes': 0},
                '/rest/folder/errors': {'errors': None},
                '/rest/system/connections': {'connections': {'peer': {'connected': False}}},
                '/rest/db/completion': {'remoteState': 'valid', 'needItems': 0, 'needBytes': 0, 'needDeletes': 0},
            }
            class Opener:
                def open(self, request, timeout):
                    self_test.assertEqual(request.get_header('X-api-key'), '0123456789abcdef')
                    return io.BytesIO(json.dumps(replies[urlparse(request.full_url).path]).encode())
            self_test = self
            with mock.patch.object(status.urllib.request, 'build_opener', return_value=Opener()):
                result = status.check(config, folder, 'test-folder', 12345)
                self.assertEqual((result['state'], result['connected_peers']), ('WAITING', 0))
                replies['/rest/system/connections']['connections']['peer']['connected'] = True
                replies['/rest/db/completion']['needItems'] = 1
                self.assertEqual(status.check(config, folder, 'test-folder', 12345)['state'], 'BUSY')
                replies['/rest/db/completion']['needItems'] = 0
                ready = status.check(config, folder, 'test-folder', 12345)
                self.assertEqual((ready['state'], ready['current_peers']), ('READY', 1))
                self.assertNotIn('0123456789abcdef', json.dumps(ready))
                for field in ('needTotalItems', 'needBytes', 'needDeletes'):
                    replies['/rest/db/status'][field] = 1
                    self.assertEqual(status.check(config, folder, 'test-folder', 12345)['state'], 'BUSY')
                    replies['/rest/db/status'][field] = 0
                del replies['/rest/db/status']['needTotalItems']
                self.assertEqual(status.check(config, folder, 'test-folder', 12345)['state'], 'UNKNOWN')
                replies['/rest/db/status']['needTotalItems'] = 0
                replies['/rest/db/completion']['remoteState'] = 'unknown'
                self.assertEqual(status.check(config, folder, 'test-folder', 12345)['state'], 'BUSY')
                replies['/rest/folder/errors']['errors'] = [{'error': 'test'}]
                with self.assertRaises(status.ns.SyncError):
                    status.check(config, folder, 'test-folder', 12345)


if __name__ == '__main__':
    unittest.main()
