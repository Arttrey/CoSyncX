import json
from pathlib import Path
import tempfile
import unittest

from cosync_publish_batch import run_once, run_titles


class PublishBatchTests(unittest.TestCase):
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
            src.write_text(metadata + '{"msg":"local"}\n')
            dst.write_text(metadata + '{"msg":"remote"}\n')
            config = {'profile': str(profile), 'exchange': str(exchange), 'local_root': '/local/Nexus',
                      'remote_root': '/remote/Nexus', 'slots': [{'id': identity, 'source': str(src),
                      'destination': str(dst), 'state_dir': str(state)}]}
            cache = {}
            self.assertEqual(run_once(config, cache), {'PUBLISHED': 1})
            self.assertEqual(dst.read_text(), src.read_text())
            self.assertEqual(run_once(config, cache), {'UNCHANGED': 1})
            dst.write_text(src.read_text() + '{"msg":"remote update"}\n')
            self.assertEqual(run_once(config, cache), {'WAITING_REMOTE': 1})
            self.assertIn('remote update', dst.read_text())


if __name__ == '__main__': unittest.main()
