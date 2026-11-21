import tempfile
from pathlib import Path
import unittest

import cosync_title_source as source


class Client:
    def __init__(self):
        self.folder = None
        self.names = [{'name': 'session_index.jsonl', 'type': 'FILE_INFO_TYPE_FILE'}]

    def _get(self, path):
        if path == 'config/folders': return []
        if path == 'system/status': return {'myID': 'own'}
        if path == 'config/defaults/folder': return {}
        if path == 'config/folders/titles': return self.folder
        if path.startswith('db/status?'): return {'state': 'idle', 'localFiles': 1,
            'localDirectories': 0, 'localSymlinks': 0, 'errors': 0}
        if path.startswith('db/browse?'): return self.names
        raise AssertionError(path)

    def _post(self, path, data):
        self.folder = data


class TitleSourceTests(unittest.TestCase):
    def test_only_verified_title_and_self_are_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = Path(tmp).resolve()
            (profile / 'session_index.jsonl').write_text('test')
            client = Client()
            own = source.prepare(client, profile, 'titles')
            source.verify(client, profile, 'titles', own)
            client.names.append({'name': 'auth.json', 'type': 'FILE_INFO_TYPE_FILE'})
            with self.assertRaises(source.TitleSetupError):
                source.verify(client, profile, 'titles', own)

    def test_existing_ignore_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = Path(tmp).resolve()
            (profile / 'session_index.jsonl').write_text('test')
            (profile / '.stignore').write_text('keep')
            with self.assertRaises(source.TitleSetupError):
                source.prepare(Client(), profile, 'titles')
            self.assertEqual((profile / '.stignore').read_text(), 'keep')


if __name__ == '__main__': unittest.main()
