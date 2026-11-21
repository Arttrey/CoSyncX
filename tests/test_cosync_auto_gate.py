import unittest

from cosync_auto_gate import RestoreGate


def status(sequence=1, **changes):
    value = {'state': 'idle', 'sequence': sequence, 'needFiles': 0,
             'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0}
    value.update(changes)
    return value


class RestoreGateTests(unittest.TestCase):
    def test_waits_for_connection_sync_and_stable_revision(self):
        gate = RestoreGate(quiet_seconds=5)
        self.assertIsNone(gate.observe(status(), connected=False, now=0))
        self.assertIsNone(gate.observe(status(needFiles=1), connected=True, now=1))
        self.assertIsNone(gate.observe(status(), connected=True, now=2))
        self.assertIsNone(gate.observe(status(), connected=True, now=6))
        self.assertEqual(gate.observe(status(), connected=True, now=7), 1)
        self.assertIsNone(gate.observe(status(2), connected=True, now=8))
        self.assertEqual(gate.observe(status(2), connected=True, now=13), 2)

    def test_failed_or_changed_restore_never_emits_reload(self):
        gate = RestoreGate(quiet_seconds=0)
        gate.observe(status(), connected=True, now=0)
        self.assertEqual(gate.observe(status(), connected=True, now=1), 1)
        for result in ({'state': 'SYNCED', 'indexed': True, 'changed_chats': 1},
                       {'state': 'RESTORED', 'indexed': False, 'changed_chats': 1},
                       {'state': 'RESTORED', 'indexed': True, 'changed_chats': -1}):
            with self.assertRaises(ValueError): gate.complete(1, result, after=status(), connected=True)
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': 1}
        with self.assertRaises(ValueError): gate.complete(1, result, after=status(2), connected=True)
        with self.assertRaises(ValueError): gate.complete(1, result, after=status(), connected=False)
        event = gate.complete(1, result, after=status(), connected=True)
        self.assertEqual(event['state'], 'RESTORED')
        self.assertTrue(event['indexed'])
        self.assertEqual(len(event['id']), 32)
        self.assertIsNone(gate.observe(status(), connected=True, now=2))
        with self.assertRaises(ValueError): gate.complete(1, result, after=status(), connected=True)

    def test_unchanged_restoration_is_not_a_reload(self):
        gate = RestoreGate(quiet_seconds=0)
        gate.observe(status(), connected=True, now=0)
        self.assertEqual(gate.observe(status(), connected=True, now=1), 1)
        self.assertIsNone(gate.complete(1, {'state': 'RESTORED', 'indexed': True, 'changed_chats': 0},
                                        after=status(), connected=True))


if __name__ == '__main__': unittest.main()
