import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from cosync_publish_batch import discover_slots, run_once, run_titles
import nexus_sync as ns


class PublishBatchTests(unittest.TestCase):
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
