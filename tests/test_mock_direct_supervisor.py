import json
from pathlib import Path
import tempfile
import unittest

from cosync_restore_status import write_status
from mock_direct_supervisor import MockSupervisor


class StubReceiver:
    def __init__(self, state): self.state = state
    def poll(self, *args, **kwargs): return {'state': self.state, 'mock_only': True}


class MockSupervisorTests(unittest.TestCase):
    def test_retains_confirmed_event_after_restart_without_accepting_real_status(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-supervisor-') as tmp:
            root = Path(tmp).resolve()
            path = root / 'status.json'
            event = {'id': 'a' * 32, 'state': 'RESTORED', 'indexed': True, 'changed_chats': 1, 'mock_only': True}
            write_status(path, 'COMPLETE', mock_only=True, event=event)
            supervisor = MockSupervisor(StubReceiver('CURRENT'), lambda: None, path)
            self.assertEqual(supervisor.last_event, event)
            supervisor.tick()
            status = json.loads(path.read_text())
            self.assertEqual(status['state'], 'COMPLETE')
            self.assertEqual(status['restore_event'], event)
            supervisor.receiver.state = 'WAITING'
            supervisor.tick()
            self.assertEqual(json.loads(path.read_text())['restore_event'], event)
            write_status(path, 'COMPLETE', mock_only=False, event={**event, 'mock_only': False})
            self.assertIsNone(MockSupervisor(StubReceiver('CURRENT'), lambda: None, path).last_event)


if __name__ == '__main__': unittest.main()
