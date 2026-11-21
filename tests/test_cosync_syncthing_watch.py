from pathlib import Path
import tempfile
import unittest

from cosync_syncthing_watch import SyncthingSource, WatchError


class Client:
    def __init__(self, folder):
        self.folder = {'id': 'chat/folder', 'path': str(folder), 'type': 'receiveonly',
                       'devices': [{'deviceID': 'vps'}]}

    def _get(self, path):
        if path.startswith('config/folders/'): return self.folder
        if path == 'system/connections': return {'connections': {'vps': {'connected': True}}}
        if path.startswith('db/status?'): return {'state': 'idle'}
        if path.startswith('db/completion?'): return {'remoteState': 'valid'}
        raise AssertionError(path)


class SyncthingSourceTests(unittest.TestCase):
    def test_rejects_wrong_peer_folder_and_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp).resolve()
            client = Client(folder)
            source = SyncthingSource(client, 'chat/folder', 'vps', folder)
            self.assertEqual(source.read(), ('chat/folder', {'state': 'idle'}, True, 'valid'))
            for change in ({'type': 'sendreceive'}, {'path': str(folder.parent)},
                           {'devices': [{'deviceID': 'other'}]}, {'id': 'other'}):
                before = client.folder.copy()
                client.folder.update(change)
                with self.assertRaises(WatchError): source.read()
                client.folder = before

    def test_sendreceive_requires_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp).resolve()
            client = Client(folder)
            client.folder['type'] = 'sendreceive'
            source = SyncthingSource(client, 'chat/folder', 'vps', folder, allow_sendreceive=True)
            self.assertEqual(source.read(), ('chat/folder', {'state': 'idle'}, True, 'valid'))
            client.folder['type'] = 'sendonly'
            with self.assertRaises(WatchError): source.read()
            with self.assertRaises(WatchError):
                SyncthingSource(client, 'chat/folder', 'vps', folder, allow_sendreceive='yes')


if __name__ == '__main__': unittest.main()
