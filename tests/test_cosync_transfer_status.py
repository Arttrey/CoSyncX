import unittest
import json
import time
from pathlib import Path
import tempfile

from cosync_transfer_status import transfer_status, restore_status
from cosync_restore_status import write_status


class Client:
    def __init__(self):
        self.status = {'state': 'idle', 'errors': 0, 'pullErrors': 0, 'needFiles': 0,
                       'remoteSequence': {'vps': 2},
                       'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0}
        self.connected = True
        self.remote = 'valid'

    def _get(self, path):
        if path.startswith('db/status?'): return self.status
        if path.startswith('system/connections'): return {'connections': {'vps': {'connected': self.connected}}}
        return {'remoteState': self.remote}


class TransferStatusTests(unittest.TestCase):
    def test_real_fresh_status_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp).resolve() / 'status.json'
            write_status(file, 'COMPLETE', mock_only=False)
            self.assertEqual(restore_status(file), 'RESTORED')
            write_status(file, 'COMPLETE', mock_only=True)
            self.assertIsNone(restore_status(file))
            write_status(file, 'COMPLETE', mock_only=False)
            value = json.loads(file.read_text())
            value['updated_at_ms'] = int(time.time() * 1000) - 100000
            file.write_text(json.dumps(value))
            self.assertIsNone(restore_status(file))

    def test_status_distinguishes_transfer_from_restoration(self):
        client = Client()
        self.assertEqual(transfer_status(client, 'chats', 'vps'),
                         {'state': 'TRANSFERRED', 'restore': 'NOT_VERIFIED'})
        client.status['needFiles'] = 1
        self.assertEqual(transfer_status(client, 'chats', 'vps')['state'], 'TRANSFERRING')
        client.connected = False
        self.assertEqual(transfer_status(client, 'chats', 'vps')['state'], 'DISCONNECTED')
        client.connected = True
        client.status['error'] = 'disk error'
        self.assertEqual(transfer_status(client, 'chats', 'vps')['state'], 'ERROR')


if __name__ == '__main__': unittest.main()
