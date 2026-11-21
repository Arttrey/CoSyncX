import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from cosync_publish_batch import _graph_ready, discover_slots, run_once, run_titles
import nexus_sync as ns
from cosync_graph_snapshot import read_shared_graph


class PublishBatchTests(unittest.TestCase):
    def test_graph_discovery_requires_correct_folder_and_encrypted_peer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();config={'discovery_syncthing_url':'http://127.0.0.1:8384',
                'discovery_syncthing_config':str(root/'config.xml'),'discovery_graph_folder':'graph',
                'discovery_peer':'peer','discovery_graph':str(root),'discovery_require_encryption':True}
            folder={'id':'graph','type':'sendreceive','path':str(root),'devices':[{'deviceID':'peer','encryptionPassword':'set'}]}
            class Client:
                def _get(self,path):return folder if path.startswith('config/') else {'errors':0,'pullErrors':0}
            with patch('cosync_publish_batch.cosync_engine.ExistingEngine',return_value=Client()):
                self.assertTrue(_graph_ready(config))
                folder['type']='receiveonly';self.assertFalse(_graph_ready(config))
                folder['type']='sendreceive';folder['devices'][0]['encryptionPassword']=''
                self.assertFalse(_graph_ready(config))

    def test_discovers_chat_with_child_and_device_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();profile,exchange,states,titles_dir,graphdir=(root/x for x in ('p','e','s','t','g'))
            for p in (profile,exchange,states,titles_dir,graphdir):p.mkdir(mode=0o700)
            (profile/'sessions').mkdir();(exchange/'sessions').mkdir()
            parent='019b0000-0000-7000-8000-000000000001';child='019b0000-0000-7000-8000-000000000002'
            def path(identity):return profile/'sessions'/f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            def meta(identity,source,**extra):return json.dumps({'type':'session_meta','ordinal':0,'payload':{
                'id':identity,'cwd':'/local','source':source,'history_mode':'paginated',**extra}})+'\n'
            path(parent).write_text(meta(parent,'vscode')+json.dumps({'type':'event_msg','ordinal':1,
                'payload':{'type':'user_message','message':'hello'}})+'\n')
            path(child).write_text(meta(child,{'subagent':{'other':'guardian'}},parent_thread_id=parent)+
                json.dumps({'type':'event_msg','ordinal':1,'payload':{'type':'agent_message','message':'first'}})+'\n')
            (profile/'session_index.jsonl').write_text(json.dumps({'id':parent,'thread_name':'New'})+'\n')
            (titles_dir/'session_index.jsonl').write_text('')
            (graphdir/'graph.json').write_text(json.dumps({'format':'cosyncx-active-graph-v1','edges':[],
                'omitted_missing':0,'omitted_archived':0}))
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT, status TEXT)')
                db.executemany('INSERT INTO threads VALUES (?,0)',[(parent,),(child,)])
                db.execute('INSERT INTO thread_spawn_edges VALUES (?,?,?)',(parent,child,'closed'))
            config={'profile':str(profile),'exchange':str(exchange),'discovery_state':str(states),'slots':[],
                'title_source':str(profile/'session_index.jsonl'),'title_destination':str(titles_dir/'session_index.jsonl'),
                'title_lock':str(root/'titles.lock'),'local_root':'/local','remote_root':'/remote',
                'discovery_graph':str(graphdir),'discovery_device':'a'*32}
            def discover():return discover_slots(config,ns._chat_catalog(profile,include_unsupported=True),
                                                  ns._chat_catalog(exchange,include_unsupported=True))
            slots,counts=discover()
            self.assertEqual(counts,{'DISCOVERED_CHILD':1,'DISCOVERED':1})
            self.assertEqual({x['id'] for x in slots},{parent,child})
            self.assertTrue((exchange/'sessions'/path(child).name).is_file())
            _,combined,blocked=read_shared_graph(graphdir)
            self.assertEqual(blocked,set());self.assertEqual(combined['edges'],[{'parent':parent,'child':child,'status':'closed'}])
            cache={};extended={**config,'slots':slots}
            self.assertEqual(run_once(extended,cache),{'UNCHANGED':2})
            self.assertEqual(run_titles(extended,cache),{'PUBLISHED':1})
            with path(child).open('a') as f:f.write(json.dumps({'type':'event_msg','ordinal':2,
                'payload':{'type':'agent_message','message':'second'}})+'\n')
            slots,_=discover();self.assertEqual(run_once({**config,'slots':slots},cache),{'PUBLISHED':1,'UNCHANGED':1})
            self.assertIn('second',(exchange/'sessions'/path(child).name).read_text())
            grandchild='019b0000-0000-7000-8000-000000000003'
            path(grandchild).write_text(meta(grandchild,{'subagent':{'other':'guardian'}},parent_thread_id=child)+
                json.dumps({'type':'event_msg','ordinal':1,'payload':{'type':'agent_message','message':'nested'}})+'\n')
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('INSERT INTO threads VALUES (?,0)',(grandchild,))
                db.execute('INSERT INTO thread_spawn_edges VALUES (?,?,?)',(child,grandchild,'closed'))
            config['slots']=[next(s for s in slots if s['id']==parent)]
            more,counts=discover()
            self.assertEqual({x['id'] for x in more},{child,grandchild})
            self.assertEqual(counts,{'DISCOVERED_CHILD':1})
            self.assertTrue((exchange/'sessions'/path(grandchild).name).is_file())
            _,combined,blocked=read_shared_graph(graphdir)
            self.assertEqual(blocked,set());self.assertEqual(len(combined['edges']),2)

    def test_discovers_new_standalone_chat_then_publishes_later_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            profile, exchange, states, shared_titles = (root / x for x in ('profile', 'exchange', 'states', 'titles'))
            for p in (profile, exchange, states, shared_titles): p.mkdir(mode=0o700)
            (profile / 'sessions').mkdir(); (exchange / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'
            filename = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            source, dest = profile / 'sessions' / filename, exchange / 'sessions' / filename
            meta = json.dumps({'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/local/Nexus', 'source': 'vscode'}}) + '\n'
            def msg(text): return json.dumps({'type': 'event_msg', 'payload': {'type': 'user_message', 'message': text}}) + '\n'
            source.write_text(meta + msg('first'))
            (profile / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': 'New'}) + '\n')
            (shared_titles / 'session_index.jsonl').write_text('')
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT)')
                db.execute('INSERT INTO threads VALUES (?,0)', (identity,))
            config = {'profile': str(profile), 'exchange': str(exchange), 'local_root': '/local/Nexus',
                      'remote_root': '/remote/Nexus', 'title_source': str(profile / 'session_index.jsonl'),
                      'title_destination': str(shared_titles / 'session_index.jsonl'), 'title_lock': str(root / 'title.lock'),
                      'discovery_state': str(states), 'slots': []}
            def discover(): return discover_slots(config, ns._chat_catalog(profile, include_unsupported=True),
                                                  ns._chat_catalog(exchange, include_unsupported=True))
            slots, counts = discover()
            self.assertEqual(counts, {'DISCOVERED': 1})
            self.assertEqual(dest.read_text(), source.read_text())
            extended = {**config, 'slots': slots}; cache = {}
            self.assertEqual(run_once(extended, cache), {'UNCHANGED': 1})
            self.assertEqual(run_titles(extended, cache), {'PUBLISHED': 1})
            self.assertEqual(json.loads((shared_titles / 'session_index.jsonl').read_text())['thread_name'], 'New')
            source.write_text(source.read_text() + msg('second'))
            slots, counts = discover()
            self.assertEqual(counts, {})
            self.assertEqual(run_once({**config, 'slots': slots}, cache), {'PUBLISHED': 1})
            self.assertIn('second', dest.read_text())
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('UPDATE threads SET archived=1')
            self.assertEqual(discover()[0], [])

    def test_adopts_existing_chat_only_when_enabled_and_not_separately_managed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            profile, exchange, states = (root / x for x in ('profile', 'exchange', 'states'))
            for p in (profile, exchange, states): p.mkdir(mode=0o700)
            (profile / 'sessions').mkdir(); (exchange / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'
            name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            meta = json.dumps({'type':'session_meta','payload':{'id':identity,'cwd':'/local','source':'vscode'}}) + '\n'
            message = lambda text: json.dumps({'type':'event_msg','payload':{'type':'user_message','message':text}}) + '\n'
            (profile/'sessions'/name).write_text(meta + message('first') + message('second'))
            (exchange/'sessions'/name).write_text(meta + message('first'))
            (profile/'session_index.jsonl').write_text(json.dumps({'id':identity,'thread_name':'Existing'})+'\n')
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT)')
                db.execute('INSERT INTO threads VALUES (?,0)', (identity,))
            config = {'profile':str(profile),'exchange':str(exchange),'discovery_state':str(states),
                      'title_source':str(profile/'session_index.jsonl'),'slots':[],
                      'local_root':'/local','remote_root':'/local'}
            def discover(): return discover_slots(config, ns._chat_catalog(profile,include_unsupported=True),
                                                  ns._chat_catalog(exchange,include_unsupported=True))
            self.assertEqual(discover()[0], [])
            config['discovery_adopt_existing'] = True
            config['discovery_excluded_roots'] = [identity]
            self.assertEqual(discover()[0], [])
            config['discovery_excluded_roots'] = []
            slots, counts = discover()
            self.assertEqual([s['id'] for s in slots], [identity])
            self.assertEqual(counts, {})
            self.assertEqual(run_once({**config,'slots':slots}, {}), {'PUBLISHED':1})
            self.assertIn('second', (exchange/'sessions'/name).read_text())

    def test_discovery_defers_chat_with_child_relationship(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); profile, exchange, states, titles_dir = (root / x for x in ('p','e','s','t'))
            for p in (profile, exchange, states, titles_dir): p.mkdir(mode=0o700)
            (profile / 'sessions').mkdir(); (exchange / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'; child = '019b0000-0000-7000-8000-000000000002'
            source = profile / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            source.write_text(json.dumps({'type':'session_meta','payload':{'id':identity,'cwd':'/local','source':'vscode'}})+'\n')
            (profile / 'session_index.jsonl').write_text(json.dumps({'id':identity,'thread_name':'New'})+'\n')
            (titles_dir / 'session_index.jsonl').write_text('')
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT)')
                db.execute('INSERT INTO threads VALUES (?,0)', (identity,))
                db.execute('INSERT INTO thread_spawn_edges VALUES (?,?)', (identity,child))
            config={'profile':str(profile),'exchange':str(exchange),'discovery_state':str(states),'slots':[],
                    'title_source':str(profile/'session_index.jsonl')}
            slots, counts=discover_slots(config,ns._chat_catalog(profile,include_unsupported=True),{})
            self.assertEqual(slots, []); self.assertEqual(counts, {'DISCOVERY_GRAPH_PENDING':1})
            self.assertEqual(list((exchange/'sessions').iterdir()), [])
            with sqlite3.connect(profile / 'state_5.sqlite') as db:
                db.execute('DELETE FROM thread_spawn_edges')
            source.write_text(json.dumps({'type':'session_meta','payload':{'id':identity,'cwd':'/local','source':'vscode',
                'history_mode':'paginated','history_base':{'thread_id':child,'end_byte_offset':10,'end_ordinal_exclusive':1}}})+'\n')
            slots, counts=discover_slots(config,ns._chat_catalog(profile,include_unsupported=True),{})
            self.assertEqual(slots, []); self.assertEqual(counts, {'DISCOVERY_LINEAGE_PENDING':1})
            self.assertEqual(list((exchange/'sessions').iterdir()), [])

    def test_discovery_does_not_overwrite_existing_unrelated_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); profile, exchange, states = (root / x for x in ('p','e','s'))
            for p in (profile, exchange, states): p.mkdir(mode=0o700)
            (profile/'sessions').mkdir(); (exchange/'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'
            filename = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            source, occupied = profile/'sessions'/filename, exchange/'sessions'/filename
            source.write_text(json.dumps({'type':'session_meta','payload':{'id':identity,'cwd':'/local','source':'vscode'}})+'\n')
            occupied.write_text('keep this unrelated file')
            (profile/'session_index.jsonl').write_text(json.dumps({'id':identity,'thread_name':'New'})+'\n')
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT, archived INTEGER)')
                db.execute('CREATE TABLE thread_spawn_edges (parent_thread_id TEXT, child_thread_id TEXT)')
                db.execute('INSERT INTO threads VALUES (?,0)',(identity,))
            config={'profile':str(profile),'exchange':str(exchange),'discovery_state':str(states),'slots':[],
                    'title_source':str(profile/'session_index.jsonl')}
            slots, counts=discover_slots(config,ns._chat_catalog(profile,include_unsupported=True),{})
            self.assertEqual(slots, []);self.assertEqual(counts, {'DISCOVERY_RETRY':1})
            self.assertEqual(occupied.read_text(), 'keep this unrelated file')

    def test_title_batch_keeps_unrelated_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, states = (root / name for name in ('source', 'exchange', 'states'))
            for p in (source, exchange, states): p.mkdir(mode=0o700)
            ids = ['019b0000-0000-7000-8000-000000000001', '019b0000-0000-7000-8000-000000000002']
            other = '019b0000-0000-7000-8000-000000000003'
            def lines(items):
                return ''.join(json.dumps({'id': identity, 'thread_name': name}) + '\n' for identity, name in items)
            src, dst = source / 'session_index.jsonl', exchange / 'session_index.jsonl'
            src.write_text(lines([(ids[0], 'new one'), (ids[1], 'new two')]))
            dst.write_text(lines([(ids[0], 'old one'), (ids[1], 'old two'), (other, 'keep')]))
            slots = []
            for identity in ids:
                state = states / identity; state.mkdir(mode=0o700)
                slots.append({'id': identity, 'state_dir': str(state)})
            cfg = {'title_source': str(src), 'title_destination': str(dst), 'title_lock': str(root / 'titles.lock'),
                   'slots': slots}
            self.assertEqual(run_titles(cfg), {'PUBLISHED': 2})
            actual = {row['id']: row['thread_name'] for row in map(json.loads, dst.read_text().splitlines())}
            self.assertEqual(actual, {ids[0]: 'new one', ids[1]: 'new two', other: 'keep'})
            src.write_text(lines([(ids[0], 'changed locally'), (ids[1], 'changed locally')]))
            cache = {ids[0]: (None, 'WAITING_REMOTE', False), ids[1]: (None, 'TIE', False)}
            self.assertEqual(run_titles(cfg, cache), {'WAITING_HISTORY': 2})
            self.assertNotIn('changed locally', dst.read_text())

    def test_publishes_selected_chat_and_waits_for_remote_only_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            profile, exchange, state = (root / name for name in ('profile', 'exchange', 'state'))
            for p in (profile, exchange, state): p.mkdir(mode=0o700)
            (profile / 'sessions').mkdir(); (exchange / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-000000000001'
            name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            src, dst = profile / 'sessions' / name, exchange / 'sessions' / name
            metadata = json.dumps({'type': 'session_meta', 'payload': {'id': identity, 'cwd': '/local/Nexus',
                                                                      'source': 'vscode'}}) + '\n'
            def msg(text): return json.dumps({'type': 'event_msg', 'payload': {'type': 'user_message', 'message': text}}) + '\n'
            src.write_text(metadata + msg('local') + msg('local 2'))
            dst.write_text(metadata + msg('remote'))
            config = {'profile': str(profile), 'exchange': str(exchange), 'local_root': '/local/Nexus',
                      'remote_root': '/remote/Nexus', 'slots': [{'id': identity, 'source': str(src),
                      'destination': str(dst), 'state_dir': str(state)}]}
            cache = {}
            self.assertEqual(run_once(config, cache), {'PUBLISHED': 1})
            self.assertEqual(dst.read_text(), src.read_text())
            self.assertEqual(run_once(config, cache), {'UNCHANGED': 1})
            dst.write_text(src.read_text() + msg('remote update'))
            self.assertEqual(run_once(config, cache), {'WAITING_REMOTE': 1})
            self.assertIn('remote update', dst.read_text())


if __name__ == '__main__': unittest.main()
