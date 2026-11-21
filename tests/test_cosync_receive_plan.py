import json
from pathlib import Path
import tempfile
import unittest

from cosync_receive_plan import receive_plan


class ReceivePlanTests(unittest.TestCase):
    def test_user_chat_subagent_and_archived_conflict_are_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            incoming, profile, project = root / 'incoming', root / 'profile', root / 'Nexus'
            (incoming / 'sessions').mkdir(parents=True)
            (profile / 'archived_sessions').mkdir(parents=True)
            project.mkdir()
            def make(where, number, source):
                identity = f'019b0000-0000-7000-8000-{number:012d}'
                record = {'type': 'session_meta', 'payload': {'id': identity, 'source': source,
                          'history_mode': 'paginated', 'cwd': '/source/Nexus'}}
                (where / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(json.dumps(record) + '\n')
            make(incoming / 'sessions', 1, 'vscode')
            make(incoming / 'sessions', 2, {'subagent': {'other': 'guardian'}})
            make(incoming / 'sessions', 3, 'vscode')
            make(profile / 'archived_sessions', 3, 'vscode')
            result = receive_plan(incoming, profile, [('/source/Nexus', str(project))])
            self.assertEqual(result['counts'], {'candidate_new': 1, 'other_subagent': 1, 'archive_conflict': 1})
            self.assertTrue(result['read_only'])
            self.assertFalse(result['index_verified'])


if __name__ == '__main__': unittest.main()
