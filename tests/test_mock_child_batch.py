import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

import mock_chat_sync as mock
import mock_child_batch as batch
import nexus_sync as ns


class ChildBatchTests(unittest.TestCase):
    def test_interruption_restores_previous_database_and_removes_new_child(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-child-test-') as tmp:
            root = Path(tmp).resolve()
            source, profile, shared, backups, project = (root / name for name in
                                                          ('source', 'profile', 'shared', 'backup', 'project'))
            for path in (source, shared, backups, project):
                path.mkdir()
            key = b'c' * 32
            mock.init_mock_profile(profile, key, shared)
            parent = '019b0000-0000-7000-8000-000000000001'
            child = '019b0000-0000-7000-8000-000000000002'
            def history(home, identity, kind):
                folder = home / 'sessions' / '2026' / '01' / '01'
                folder.mkdir(parents=True, exist_ok=True)
                payload = {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                           'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': kind,
                           'model_provider': 'openai', 'history_mode': 'paginated'}
                if identity == child:
                    payload['parent_thread_id'] = parent
                path = folder / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                path.write_text(json.dumps({'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta',
                                            'payload': payload, 'ordinal': 0}) + '\n')
                return path
            history(profile, parent, 'vscode')
            history(source, child, {'subagent': {'other': 'guardian'}})
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?, 0)', (parent,))
            def interrupt(*args, on_verified, **kwargs):
                with sqlite3.connect(profile / 'state_5.sqlite') as db:
                    db.execute('INSERT INTO threads VALUES (?, 0)', (child,))
                on_verified()
                raise RuntimeError('synthetic interruption')
            with patch.object(batch.pages, 'index_thread', side_effect=interrupt):
                with self.assertRaisesRegex(RuntimeError, 'synthetic interruption'):
                    batch.restore(source, profile, shared, backups, key, parent,
                                  [('/source/Nexus', str(project))], Path(sys.executable), children=[child])
            self.assertNotIn(child, ns._chat_catalog(profile, include_unsupported=True))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT id FROM threads').fetchall(), [(parent,)])
            self.assertFalse((backups / 'mock-child-pending.json').exists())


if __name__ == '__main__':
    unittest.main()
