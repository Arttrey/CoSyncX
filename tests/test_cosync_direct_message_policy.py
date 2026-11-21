import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver
import nexus_sync as ns


class DirectMessagePolicyTests(unittest.TestCase):
    def test_subagent_conflicts_keep_more_messages_and_do_not_block_others(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();source,titles,graph,profile,backups,project=(root/n for n in
                ('source','titles','graph','profile','backups','project'))
            for p in (source,titles,graph,profile,backups,project):p.mkdir(mode=0o700)
            (source/'sessions').mkdir();(profile/'sessions').mkdir()
            parent,a,b,c=[f'019b0000-0000-7000-8000-{n:012d}' for n in (1,2,3,4)]
            def history(identity,cwd,messages,child=False):
                payload={'id':identity,'cwd':cwd,'source':{'subagent':{'other':'guardian'}} if child else 'vscode',
                         'history_mode':'paginated'}
                if child:payload['parent_thread_id']=parent
                rows=[{'type':'session_meta','ordinal':0,'payload':payload}]
                rows += [{'type':'event_msg','ordinal':i+1,'payload':{'type':'agent_message' if child else 'user_message',
                            'message':text}} for i,text in enumerate(messages)]
                return ''.join(json.dumps(x)+'\n' for x in rows)
            source_messages={parent:['parent'],a:['old'],b:['new','newer'],c:['remote']}
            local_messages={parent:['parent'],a:['old','local newer'],b:['old'],c:['local']}
            for identity in (parent,a,b,c):
                name=f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                (source/'sessions'/name).write_text(history(identity,'/source/Nexus',source_messages[identity],identity!=parent))
                (profile/'sessions'/name).write_text(history(identity,str(project),local_messages[identity],identity!=parent))
            (titles/'session_index.jsonl').write_text(json.dumps({'id':parent,'thread_name':'parent'})+'\n')
            (profile/'session_index.jsonl').write_text(json.dumps({'id':parent,'thread_name':'parent'})+'\n')
            (graph/'graph.json').write_text(json.dumps({'format':'cosyncx-active-graph-v1','edges':[],
                                                       'omitted_missing':0,'omitted_archived':0}))
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT,archived INTEGER)')
                db.executemany('INSERT INTO threads VALUES (?,0)',[(i,) for i in (parent,a,b,c)])
            with sqlite3.connect(profile/'thread_history_1.sqlite') as db:db.execute('CREATE TABLE test (value TEXT)')
            receiver=DirectReceiver(source=source,titles=titles,graph=graph,profile=profile,backup_root=backups,
                key=b'k'*32,mappings=[('/source/Nexus',str(project))],codex=Path(sys.executable),
                folder_ids=('sessions','titles','graph'),peer_id='vps',prefer_more_messages=True)
            files={i:profile/'sessions'/f'rollout-2026-01-01T00-00-00-{i}.jsonl' for i in (a,b,c)}
            before_a,before_c=files[a].read_bytes(),files[c].read_bytes()
            with patch('cosync_direct_restore.quiescence.is_quiet',return_value=True), \
                 patch('cosync_direct_restore.indexer._index_profile'), \
                 patch('cosync_direct_restore.indexer._reset_profile_projection'):
                self.assertEqual(receiver._receive(),1)
            self.assertEqual(files[a].read_bytes(),before_a);self.assertEqual(files[c].read_bytes(),before_c)
            self.assertIn('newer',files[b].read_text());self.assertTrue({a,c} <= receiver.pending_busy_ids)

    def test_conflicting_child_graph_does_not_block_other_chats(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();source,titles,graph,profile,backups,project=(root/n for n in
                ('source','titles','graph','profile','backups','project'))
            for p in (source,titles,graph,profile,backups,project):p.mkdir(mode=0o700)
            (source/'sessions').mkdir();(profile/'sessions').mkdir();(graph/'devices').mkdir()
            a,b,child=[f'019b0000-0000-7000-8000-{n:012d}' for n in (1,2,3)]
            for identity,source_type,parent in ((a,'vscode',None),(b,'vscode',None),
                                               (child,{'subagent':{'other':'guardian'}},a)):
                payload={'id':identity,'cwd':'/source/Nexus','source':source_type,'history_mode':'paginated'}
                if parent:payload['parent_thread_id']=parent
                (source/'sessions'/f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    json.dumps({'type':'session_meta','ordinal':0,'payload':payload})+'\n')
            (titles/'session_index.jsonl').write_text(''.join(json.dumps({'id':i,'thread_name':'name'})+'\n' for i in (a,b)))
            def snapshot(parent):return json.dumps({'format':'cosyncx-active-graph-v1','omitted_missing':0,
                'omitted_archived':0,'edges':[{'parent':parent,'child':child,'status':'closed'}]})
            (graph/'graph.json').write_text(snapshot(a));(graph/'devices'/('a'*32+'.json')).write_text(snapshot(b))
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT,archived INTEGER)')
            receiver=DirectReceiver(source=source,titles=titles,graph=graph,profile=profile,backup_root=backups,
                key=b'k'*32,mappings=[('/source/Nexus',str(project))],codex=Path(sys.executable),
                folder_ids=('sessions','titles','graph'),peer_id='vps')
            with patch('cosync_direct_restore.quiescence.is_quiet',return_value=True), \
                 patch('cosync_direct_restore.indexer._index_profile'):
                self.assertEqual(receiver._receive(),2)
            self.assertEqual(receiver.pending_busy_ids,{child})
            local=ns._chat_catalog(profile,include_unsupported=True)
            self.assertIn(a,local);self.assertIn(b,local);self.assertNotIn(child,local)

    def test_receive_does_not_reprepare_an_unchanged_verified_history(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / n for n in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for p in (source, titles, graph, profile, backups, project): p.mkdir(mode=0o700)
            (source / 'sessions').mkdir(); (profile / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'
            filename = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            def history(cwd):
                return json.dumps({'type': 'session_meta', 'ordinal': 0, 'payload': {'id': identity,
                    'cwd': cwd, 'source': 'vscode', 'history_mode': 'paginated'}}) + '\n'
            remote, local = source / 'sessions' / filename, profile / 'sessions' / filename
            remote.write_text(history('/source/Nexus')); local.write_text(history(str(project)))
            title = json.dumps({'id': identity, 'thread_name': 'name'}) + '\n'
            (titles / 'session_index.jsonl').write_text(title); (profile / 'session_index.jsonl').write_text(title)
            (graph / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1', 'edges': [],
                'omitted_missing': 0, 'omitted_archived': 0}))
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, title TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?,?,?,0)', (identity, str(local), 'name'))
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE thread_history_projection_state (thread_id TEXT, next_rollout_byte_offset INTEGER, next_rollout_ordinal INTEGER)')
                db.execute('INSERT INTO thread_history_projection_state VALUES (?,1,1)', (identity,))
            receiver = DirectReceiver(source=source, titles=titles, graph=graph, profile=profile,
                backup_root=backups, key=b'k'*32, mappings=[('/source/Nexus', str(project))], codex=Path(sys.executable),
                folder_ids=('sessions','titles','graph'), peer_id='vps', prefer_more_messages=True)
            receiver.verified[identity] = (hashlib.sha256(local.read_bytes()).hexdigest(), 'name')
            receiver._cache_verified_sources({identity: hashlib.sha256(remote.read_bytes()).hexdigest()})
            with patch('cosync_direct_restore.quiescence.is_quiet', return_value=True), \
                 patch('cosync_direct_restore.lineage.prepare_lineage', side_effect=AssertionError('unnecessary preparation')):
                self.assertEqual(receiver._receive(), 0)

    def test_cached_history_requires_unchanged_source_local_and_index(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); source = root / 'source'; profile = root / 'profile'
            source.mkdir(); profile.mkdir(); remote = source / 'chat.jsonl'; local = profile / 'chat.jsonl'
            remote.write_text('received'); local.write_text('local')
            identity = '019b0000-0000-7000-8000-000000000001'
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, title TEXT, archived INTEGER)')
                db.execute('INSERT INTO threads VALUES (?,?,?,0)', (identity, str(local), 'older database title'))
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('CREATE TABLE thread_history_projection_state (thread_id TEXT, next_rollout_byte_offset INTEGER, next_rollout_ordinal INTEGER)')
                db.execute('INSERT INTO thread_history_projection_state VALUES (?,10,2)', (identity,))
            (profile / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'name'}) + '\n')
            receiver = DirectReceiver.__new__(DirectReceiver)
            receiver.source, receiver.profile, receiver._verified_sources = source, profile, {}
            receiver.verified = {identity: (hashlib.sha256(local.read_bytes()).hexdigest(), 'name')}
            fingerprint = hashlib.sha256(remote.read_bytes()).hexdigest()
            receiver._cache_verified_sources({identity: fingerprint})
            incoming = {identity: [{'path': remote}]}; current = {identity: [{'path': local, 'folder': 'sessions'}]}
            def cached(): return receiver._cached_unchanged(incoming, current, {identity: 'name'}, {identity: 'chat'}, set())[0]
            self.assertEqual(cached(), {identity})
            remote.write_text('new received')
            self.assertEqual(cached(), set())
            remote.write_text('received')
            local.write_text('new local')
            self.assertEqual(cached(), set())
            local.write_text('local')
            with sqlite3.connect(profile / 'thread_history_1.sqlite') as db:
                db.execute('UPDATE thread_history_projection_state SET next_rollout_ordinal=3')
            self.assertEqual(cached(), set())

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
            carry = [{'id': 'a' * 32, 'count': 1}]
            with patch('cosync_direct_restore.ns._chat_catalog', return_value=entries), \
                 patch('cosync_direct_restore.locking._busy_thread_ids', return_value=set()), \
                 patch.object(receiver, '_read_carry', return_value=carry), \
                 patch.object(receiver, '_read_journal', return_value=record):
                changed = {**record, 'threads': [identity]}
                self.assertFalse(receiver._retire_unchanged_indexing(changed, journal))
                self.assertTrue(journal.exists())
                self.assertTrue(receiver._retire_unchanged_indexing(record, journal))
                journal.write_text('journal')
                indexed = {**record, 'phase': 'indexed'}
                receiver._read_journal.return_value = indexed
                self.assertTrue(receiver._retire_unchanged_indexing(indexed, journal))
                self.assertEqual(receiver._read_carry(), carry)
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
