import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver


class DirectMessagePolicyTests(unittest.TestCase):
    def test_larger_remote_is_imported_while_local_larger_and_ties_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / n for n in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for p in (source, titles, graph, profile, backups, project): p.mkdir(mode=0o700)
            (source / 'sessions').mkdir(); (profile / 'sessions').mkdir()
            ids = [f'019b0000-0000-7000-8000-{n:012d}' for n in range(1, 6)]
            def history(identity, cwd, messages):
                meta = {'type': 'session_meta', 'ordinal': 0, 'payload': {'id': identity, 'cwd': cwd, 'source': 'vscode',
                        'timestamp': '2026-01-01T00:00:00.000Z', 'originator': 'codex_vscode',
                        'cli_version': '0.1', 'model_provider': 'openai', 'history_mode': 'paginated'}}
                rows = [meta] + [{'type': 'event_msg', 'ordinal': n + 1,
                                  'payload': {'type': 'user_message', 'message': x}}
                                 for n, x in enumerate(messages)]
                return ''.join(json.dumps(row) + '\n' for row in rows)
            source_messages = [['old'], ['new', 'new2'], ['remote'], ['new', 'new2'], ['missing path']]
            local_messages = [['local', 'local2'], ['old'], ['local']]
            for n, identity in enumerate(ids):
                filename = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                cwd = '/unmapped/project' if n == 4 else '/source/Nexus'
                (source / 'sessions' / filename).write_text(history(identity, cwd, source_messages[n]))
                if n < 3:
                    (profile / 'sessions' / filename).write_text(history(identity, str(project), local_messages[n]))
            original_first = (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[0]}.jsonl').read_bytes()
            original_tie = (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[2]}.jsonl').read_bytes()
            (titles / 'session_index.jsonl').write_text(''.join(json.dumps({'id': i, 'thread_name': 'name'}) + '\n' for i in ids))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.executemany('INSERT INTO threads VALUES (?,0)', [(i,) for i in ids[:3]])
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE test (value TEXT)')
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k' * 32, mappings=[('/source/Nexus', str(project))],
                codex=Path(sys.executable), folder_ids=('sessions', 'titles', 'graph'), peer_id='vps',
                quiet_seconds=0, prefer_more_messages=True)
            st = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                  'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(folder): return lambda: (folder, st, True, 'valid')
            readers = [read(x) for x in ('sessions', 'titles', 'graph')]
            def poll(now): return receiver.poll(readers[0], title_status_reader=readers[1], graph_status_reader=readers[2], now=now)
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                    patch('cosync_direct_restore.indexer._index_profile'), \
                    patch('cosync_direct_restore.indexer._reset_profile_projection'):
                self.assertEqual(poll(0)['state'], 'WAITING')
                result = poll(1)
            self.assertEqual(result['state'], 'PENDING')
            self.assertEqual(result['restore_event']['changed_chats'], 2)
            self.assertEqual((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[0]}.jsonl').read_bytes(), original_first)
            self.assertEqual((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[2]}.jsonl').read_bytes(), original_tie)
            self.assertIn('new2', (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[1]}.jsonl').read_text())
            self.assertTrue((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[3]}.jsonl').exists())
            self.assertFalse((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[4]}.jsonl').exists())


if __name__ == '__main__': unittest.main()
