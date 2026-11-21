import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver


class DirectMessagePolicyTests(unittest.TestCase):
    def test_interrupted_verification_reuses_only_matching_indexed_chats(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            profile, backup = root / 'profile', root / 'backup'
            profile.mkdir(); backup.mkdir(); (backup / 'latest').mkdir()
            manifest = backup / 'latest' / 'manifest.json'; manifest.write_text('{}')
            journal = backup / 'direct-restore-pending.json'; journal.write_text('journal')
            identity = '019b0000-0000-7000-8000-000000000001'
            history = profile / 'history.jsonl'; history.write_text('unchanged')
            digest = hashlib.sha256(history.read_bytes()).hexdigest()
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, title TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?,?,?,0)', (identity, str(history), 'name'))
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE thread_history_projection_state (thread_id TEXT)')
                db.execute('INSERT INTO thread_history_projection_state VALUES (?)', (identity,))
            (profile / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'name'}) + '\n')
            record = {'phase': 'indexing', 'threads': [], 'paths': [], 'operations': [{'before': digest, 'after': digest}],
                      'verified': {identity: {'digest': digest, 'title': 'name'}},
                      'index_snapshot': {'edges': []}, 'backup_manifest_digest': hashlib.sha256(manifest.read_bytes()).hexdigest()}
            receiver = DirectReceiver.__new__(DirectReceiver)
            receiver.profile, receiver.backup_root, receiver.verified = profile, backup, {}
            entries = {identity: [{'path': history, 'folder': 'sessions'}]}
            with patch('cosync_direct_restore.ns._chat_catalog', return_value=entries), \
                 patch('cosync_direct_restore.locking._busy_thread_ids', return_value=set()), \
                 patch.object(receiver, '_read_carry', return_value=[]), \
                 patch.object(receiver, '_read_journal', return_value=record):
                changed = {**record, 'threads': [identity]}
                self.assertFalse(receiver._retire_unchanged_indexing(changed, journal))
                self.assertTrue(journal.exists())
                self.assertTrue(receiver._retire_unchanged_indexing(record, journal))
            self.assertFalse(journal.exists())
            self.assertEqual(receiver.verified[identity], (digest, 'name'))
            self.assertEqual(history.read_text(), 'unchanged')

    def test_busy_queue_waits_before_retrying_unchanged_revision(self):
        class Gate:
            retries = 0
            def observe(self, observations, now=None): return None
            def is_current(self, now=None): return True
            def request_retry(self): self.retries += 1
        receiver = DirectReceiver.__new__(DirectReceiver)
        receiver.pending_result = None
        receiver.pending_busy_ids = frozenset({'busy'})
        receiver._last_pending_retry = 100
        receiver.gate = Gate()
        reader = lambda: ('folder', {}, True, 'valid')
        with tempfile.TemporaryDirectory() as tmp:
            receiver.backup_root = Path(tmp)
            for now in (105, 120, 159):
                result = receiver.poll(reader, title_status_reader=reader, graph_status_reader=reader, now=now)
                self.assertEqual(result['state'], 'PENDING')
                self.assertEqual(receiver.gate.retries, 0)
            receiver.poll(reader, title_status_reader=reader, graph_status_reader=reader, now=160)
            self.assertEqual(receiver.gate.retries, 1)

    def test_larger_remote_is_imported_while_local_larger_and_ties_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / n for n in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for p in (source, titles, graph, profile, backups, project): p.mkdir(mode=0o700)
            (source / 'sessions').mkdir(); (profile / 'sessions').mkdir()
            ids = [f'019b0000-0000-7000-8000-{n:012d}' for n in range(1, 7)]
            def history(identity, cwd, messages):
                meta = {'type': 'session_meta', 'ordinal': 0, 'payload': {'id': identity, 'cwd': cwd, 'source': 'vscode',
                        'timestamp': '2026-01-01T00:00:00.000Z', 'originator': 'codex_vscode',
                        'cli_version': '0.1', 'model_provider': 'openai', 'history_mode': 'paginated'}}
                rows = [meta] + [{'type': 'event_msg', 'ordinal': n + 1,
                                  'payload': {'type': 'user_message', 'message': x}}
                                 for n, x in enumerate(messages)]
                return ''.join(json.dumps(row) + '\n' for row in rows)
            source_messages = [['old'], ['new', 'new2'], ['remote'], ['new', 'new2'], ['missing path'], ['new', 'new2']]
            local_messages = [['local', 'local2'], ['old'], ['local']]
            for n, identity in enumerate(ids):
                filename = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                cwd = '/unmapped/project' if n in (4, 5) else '/source/Nexus'
                (source / 'sessions' / filename).write_text(history(identity, cwd, source_messages[n]))
                if n < 3:
                    (profile / 'sessions' / filename).write_text(history(identity, str(project), local_messages[n]))
                elif n == 5:
                    (profile / 'sessions' / filename).write_text(history(identity, str(project), ['old']))
            original_first = (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[0]}.jsonl').read_bytes()
            original_tie = (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[2]}.jsonl').read_bytes()
            (titles / 'session_index.jsonl').write_text(''.join(json.dumps({'id': i, 'thread_name': 'name'}) + '\n' for i in ids))
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.executemany('INSERT INTO threads VALUES (?,0)', [(i,) for i in ids[:3] + ids[5:]])
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
            self.assertEqual(result['restore_event']['changed_chats'], 3)
            self.assertEqual((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[0]}.jsonl').read_bytes(), original_first)
            self.assertEqual((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[2]}.jsonl').read_bytes(), original_tie)
            self.assertIn('new2', (profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[1]}.jsonl').read_text())
            self.assertTrue((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[3]}.jsonl').exists())
            self.assertFalse((profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[4]}.jsonl').exists())
            mapped = profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{ids[5]}.jsonl'
            self.assertEqual(json.loads(mapped.read_text().splitlines()[0])['payload']['cwd'], str(project))
            self.assertIn('new2', mapped.read_text())


if __name__ == '__main__': unittest.main()
