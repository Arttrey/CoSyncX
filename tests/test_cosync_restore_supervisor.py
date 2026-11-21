import json
from pathlib import Path
import tempfile
import unittest

from cosync_restore_supervisor import RestoreSupervisor


class RestoreSupervisorTests(unittest.TestCase):
    def test_only_confirmed_real_restore_can_publish_reload_event(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-real-status-') as tmp:
            path = Path(tmp).resolve() / 'status.json'
            class Receiver:
                result = {'state': 'WAITING', 'mock_only': False}
                acknowledged = []
                def acknowledge(self, event):
                    self.acknowledged.append(event)
                def poll(self, _reader, **kwargs):
                    if self.result['state'] == 'COMPLETE':
                        kwargs['on_restore']()
                        self.during = json.loads(path.read_text())['state']
                    return self.result
            receiver = Receiver()
            supervisor = RestoreSupervisor(receiver, lambda: None, path, mock_only=False)
            supervisor.tick()
            self.assertEqual(json.loads(path.read_text())['state'], 'PENDING')
            event = {'id': 'b' * 32, 'state': 'RESTORED', 'indexed': True,
                     'changed_chats': 2, 'mock_only': False}
            receiver.result = {'state': 'COMPLETE', 'mock_only': False,
                               'restore_event': {**event, 'indexed': False}}
            self.assertEqual(supervisor.tick()['state'], 'ERROR')
            self.assertIsNone(json.loads(path.read_text())['restore_event'])
            receiver.result['restore_event'] = {**event, 'mock_only': True}
            self.assertEqual(supervisor.tick()['state'], 'ERROR')
            receiver.result['restore_event'] = event
            self.assertEqual(supervisor.tick()['state'], 'COMPLETE')
            saved = json.loads(path.read_text())
            self.assertEqual(receiver.during, 'RESTORING')
            self.assertEqual(saved['restore_event'], event)
            partial = {**event, 'id': 'c' * 32, 'changed_chats': 1}
            receiver.result = {'state': 'PENDING', 'mock_only': False, 'restore_event': partial}
            supervisor.tick()
            saved = json.loads(path.read_text())
            self.assertEqual(saved['state'], 'PENDING')
            self.assertEqual(saved['restore_event'], partial)
            self.assertEqual(receiver.acknowledged[-1], partial)
            receiver.result = {'state': 'PENDING', 'mock_only': False}
            supervisor.tick()
            saved = json.loads(path.read_text())
            self.assertEqual(saved['state'], 'PENDING')
            self.assertEqual(saved['restore_event'], partial)
            receiver.path_warning_count = 2
            receiver.backup_root = path.parent
            supervisor.tick()
            saved = json.loads(path.read_text())
            self.assertEqual(saved['missing_path_chats'], 2)
            self.assertEqual(saved['missing_path_report'], str(path.parent / 'missing-paths.json'))


if __name__ == '__main__':
    unittest.main()
