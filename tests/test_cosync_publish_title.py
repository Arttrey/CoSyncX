import json
from pathlib import Path
import tempfile
import unittest

from cosync_publish_title import publish_title


class PublishTitleTests(unittest.TestCase):
    def test_merges_one_title_and_defers_conflicts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, dest, state = (root / x for x in ('source', 'dest', 'state'))
            for p in (source, dest, state): p.mkdir(mode=0o700)
            identity = '019b0000-0000-7000-8000-000000000001'
            other = '019b0000-0000-7000-8000-000000000002'
            def row(i, name): return json.dumps({'id': i, 'thread_name': name}) + '\n'
            src, dst = source / 'session_index.jsonl', dest / 'session_index.jsonl'
            src.write_text(row(identity, 'new') + row(other, 'local other'))
            old = row(identity, 'old') + row(other, 'remote other')
            dst.write_text(old)
            self.assertEqual(publish_title(src, dst, state, identity), 'NEEDS_INITIAL_APPROVAL')
            self.assertEqual(publish_title(src, dst, state, identity, replace_initial=True), 'PUBLISHED')
            self.assertEqual((state / 'latest.jsonl').read_text(), old)
            self.assertEqual([x['thread_name'] for x in map(json.loads, dst.read_text().splitlines())],
                             ['remote other', 'new'])
            dst.write_text(row(other, 'new remote other') + row(identity, 'new'))
            src.write_text(row(identity, 'newer'))
            self.assertEqual(publish_title(src, dst, state, identity), 'PUBLISHED')
            self.assertIn('new remote other', dst.read_text())
            dst.write_text(row(identity, 'remote conflict') + row(other, 'new remote other'))
            self.assertEqual(publish_title(src, dst, state, identity), 'CONFLICT')
            self.assertIn('remote conflict', dst.read_text())
            src.write_text(row(identity, 'newer'))
            self.assertEqual(publish_title(src, dst, state, identity, prefer_local=True), 'WAITING_REMOTE')
            src.write_text(row(identity, 'newer local'))
            self.assertEqual(publish_title(src, dst, state, identity, prefer_local=True), 'PUBLISHED')
            self.assertIn('newer local', dst.read_text())

    def test_title_coordination_lock_prevents_parallel_writes(self):
        import fcntl
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, dest, state = (root / x for x in ('source', 'dest', 'state'))
            for p in (source, dest, state): p.mkdir(mode=0o700)
            identity = '019b0000-0000-7000-8000-000000000001'
            src, dst = source / 'session_index.jsonl', dest / 'session_index.jsonl'
            src.write_text(json.dumps({'id': identity, 'thread_name': 'local'}) + '\n')
            dst.write_text(json.dumps({'id': identity, 'thread_name': 'remote'}) + '\n')
            lock = root / 'titles.lock'
            with lock.open('w') as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(BlockingIOError):
                    publish_title(src, dst, state, identity, replace_initial=True, lock_file=lock)
            self.assertEqual(publish_title(src, dst, state, identity, replace_initial=True, lock_file=lock), 'PUBLISHED')


if __name__ == '__main__': unittest.main()
