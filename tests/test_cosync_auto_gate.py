import unittest

from cosync_auto_gate import RestoreGate, MultiFolderGate


def status(sequence=1, **changes):
    value = {'state': 'idle', 'sequence': sequence, 'needFiles': 0,
             'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0,
             'pullErrors': 0, 'remoteSequence': {'vps': sequence}}
    value.update(changes)
    return value


class RestoreGateTests(unittest.TestCase):
    def test_three_folders_require_graph_to_be_stable_too(self):
        gate = MultiFolderGate(folder_ids=('chats', 'titles', 'graph'), peer_id='vps', quiet_seconds=2)
        def observations(graph):
            return {name: (name, value, True, 'valid') for name, value in
                    (('chats', status()), ('titles', status()), ('graph', graph))}
        self.assertIsNone(gate.observe(observations(status(needFiles=1)), now=0))
        self.assertIsNone(gate.observe(observations(status()), now=1))
        self.assertIsNone(gate.observe(observations(status(2)), now=2))
        revision = gate.observe(observations(status(2)), now=4)
        self.assertEqual(revision, (('chats', 1), ('titles', 1), ('graph', 2)))
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': 1}
        with self.assertRaises(ValueError):
            gate.complete(revision, result, after=observations(status(3)))
        self.assertEqual(gate.complete(revision, result, after=observations(status(2)))['changed_chats'], 1)

    def test_two_folders_wait_for_titles_and_detect_title_only_changes(self):
        gate = MultiFolderGate(folder_ids=('chats', 'titles'), peer_id='vps', quiet_seconds=2)
        def observations(chats=None, titles=None, connected=True):
            return {'chats': ('chats', chats or status(), connected, 'valid'),
                    'titles': ('titles', titles or status(), connected, 'valid')}
        self.assertIsNone(gate.observe(observations(titles=status(needFiles=1)), now=0))
        self.assertIsNone(gate.observe(observations(), now=1))
        self.assertIsNone(gate.observe(observations(), now=2))
        rev = gate.observe(observations(), now=3)
        self.assertEqual(rev, (('chats', 1), ('titles', 1)))
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': 1}
        with self.assertRaises(ValueError): gate.complete(rev, result, after=observations(titles=status(2)))
        self.assertEqual(gate.complete(rev, result, after=observations())['changed_chats'], 1)
        self.assertIsNone(gate.observe(observations(), now=4))
        self.assertIsNone(gate.observe(observations(titles=status(2)), now=5))
        new_rev = gate.observe(observations(titles=status(2)), now=7)
        self.assertEqual(new_rev, (('chats', 1), ('titles', 2)))
        self.assertIsNone(gate.complete(new_rev, {'state': 'RESTORED', 'indexed': True, 'changed_chats': 0},
                                        after=observations(titles=status(2))))
        self.assertIsNone(gate.observe(observations(titles=status(2), connected=False), now=8))
        self.assertIsNone(gate.observe(observations(titles=status(2)), now=9))
        self.assertEqual(gate.observe(observations(titles=status(2)), now=11), new_rev)

    def test_waits_for_connection_sync_and_stable_revision(self):
        gate = RestoreGate(folder_id='chats', peer_id='vps', quiet_seconds=5)
        self.assertIsNone(gate.observe('chats', status(), connected=False, remote_state='valid', now=0))
        self.assertIsNone(gate.observe('chats', status(needFiles=1), connected=True, remote_state='valid', now=1))
        self.assertIsNone(gate.observe('chats', status(), connected=True, remote_state='valid', now=2))
        self.assertIsNone(gate.observe('chats', status(), connected=True, remote_state='valid', now=6))
        self.assertEqual(gate.observe('chats', status(), connected=True, remote_state='valid', now=7), 1)
        self.assertIsNone(gate.observe('chats', status(2), connected=True, remote_state='valid', now=8))
        self.assertEqual(gate.observe('chats', status(2), connected=True, remote_state='valid', now=13), 2)

    def test_failed_or_changed_restore_never_emits_reload(self):
        gate = RestoreGate(folder_id='chats', peer_id='vps', quiet_seconds=0)
        gate.observe('chats', status(), connected=True, remote_state='valid', now=0)
        self.assertEqual(gate.observe('chats', status(), connected=True, remote_state='valid', now=1), 1)
        for result in ({'state': 'SYNCED', 'indexed': True, 'changed_chats': 1},
                       {'state': 'RESTORED', 'indexed': False, 'changed_chats': 1},
                       {'state': 'RESTORED', 'indexed': True, 'changed_chats': -1}):
            with self.assertRaises(ValueError): gate.complete(1, result, folder_id='chats', after=status(), connected=True, remote_state='valid')
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': 1}
        with self.assertRaises(ValueError): gate.complete(1, result, folder_id='chats', after=status(2), connected=True, remote_state='valid')
        with self.assertRaises(ValueError): gate.complete(1, result, folder_id='chats', after=status(), connected=False, remote_state='valid')
        changed_source = status()
        changed_source['sequence'] = 99
        with self.assertRaises(ValueError):
            gate.complete(1, result, folder_id='chats', after=changed_source, connected=True, remote_state='valid')
        event = gate.complete(1, result, folder_id='chats', after=status(), connected=True, remote_state='valid')
        self.assertEqual(event['state'], 'RESTORED')
        self.assertTrue(event['indexed'])
        self.assertEqual(len(event['id']), 32)
        self.assertIsNone(gate.observe('chats', status(), connected=True, remote_state='valid', now=2))
        with self.assertRaises(ValueError): gate.complete(1, result, folder_id='chats', after=status(), connected=True, remote_state='valid')

    def test_only_selected_remote_folder_and_peer_trigger(self):
        gate = RestoreGate(folder_id='chats', peer_id='vps', quiet_seconds=0)
        options = {'connected': True, 'remote_state': 'valid'}
        self.assertIsNone(gate.observe('other', status(), now=0, **options))
        self.assertIsNone(gate.observe('chats', status(remoteSequence={'other': 1}), now=0, **options))
        self.assertIsNone(gate.observe('chats', status(), now=0, **options))
        self.assertEqual(gate.observe('chats', status(sequence=1), now=1, **options), 1)
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': 1}
        gate.complete(1, result, folder_id='chats', after=status(), **options)
        changed_local = status(sequence=1)
        changed_local['sequence'] = 999
        self.assertIsNone(gate.observe('chats', changed_local, now=2, **options))
        self.assertIsNone(gate.observe('chats', status(2), now=3, **options))
        self.assertEqual(gate.observe('chats', status(2), now=4, **options), 2)
        for broken in (status(error='disk failure'), status(pullErrors=1), status(needDeletes=1)):
            with self.assertRaises(ValueError):
                gate.complete(2, result, folder_id='chats', after=broken, **options)
        with self.assertRaises(ValueError):
            gate.complete(2, result, folder_id='other', after=status(2), **options)
        self.assertIsNone(gate.observe('chats', status(2), connected=True, remote_state='notSharing', now=5))

    def test_unchanged_restoration_is_not_a_reload(self):
        gate = RestoreGate(folder_id='chats', peer_id='vps', quiet_seconds=0)
        gate.observe('chats', status(), connected=True, remote_state='valid', now=0)
        self.assertEqual(gate.observe('chats', status(), connected=True, remote_state='valid', now=1), 1)
        self.assertIsNone(gate.complete(1, {'state': 'RESTORED', 'indexed': True, 'changed_chats': 0},
                                        folder_id='chats', after=status(), connected=True, remote_state='valid'))
        # Reconnect can follow a remote index reset even if its numeric
        # sequence happens to match the previous one.
        self.assertIsNone(gate.observe('chats', status(), connected=False, remote_state='unknown', now=2))
        self.assertIsNone(gate.observe('chats', status(), connected=True, remote_state='valid', now=3))
        self.assertEqual(gate.observe('chats', status(), connected=True, remote_state='valid', now=4), 1)


if __name__ == '__main__': unittest.main()
