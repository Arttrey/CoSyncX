import json
import os
from pathlib import Path
import tempfile
import unittest

from cosync_restore_status import StatusError, write_status


class RestoreStatusTests(unittest.TestCase):
    def test_private_atomic_status_and_confirmed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            status = root / 'status.json'
            event = {'id': 'a' * 32, 'state': 'RESTORED', 'indexed': True,
                     'changed_chats': 1, 'mock_only': False, 'private_extra': 'not included'}
            write_status(status, 'COMPLETE', mock_only=False, event=event)
            saved = json.loads(status.read_text())
            self.assertEqual(saved['format'], 'cosyncx-restore-status-v1')
            self.assertEqual(saved['state'], 'COMPLETE')
            self.assertNotIn('private_extra', saved['restore_event'])
            if os.name != 'nt': self.assertEqual(status.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(StatusError): write_status(status, 'PENDING', mock_only=False, event={**event, 'indexed': False})
            self.assertEqual(json.loads(status.read_text()), saved)
            link = root / 'link'
            link.symlink_to(status)
            with self.assertRaises(StatusError): write_status(link, 'ERROR', mock_only=False)


if __name__ == '__main__': unittest.main()
