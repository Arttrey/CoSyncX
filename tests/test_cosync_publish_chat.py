import json
from pathlib import Path
import tempfile
import unittest

from cosync_publish_chat import publish
import nexus_sync as ns


class PublishChatTests(unittest.TestCase):
    def test_preserves_backup_and_defers_external_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, state = (root / x for x in ('source', 'exchange', 'state'))
            for directory in (source, exchange, state): directory.mkdir(mode=0o700)
            name = 'rollout-2026-01-01T00-00-00-019b0000-0000-7000-8000-000000000001.jsonl'
            src, dst = source / name, exchange / name
            old, new, later = (json.dumps({'msg': x}) + '\n' for x in ('old', 'new', 'later'))
            src.write_text(new); dst.write_text(old)
            self.assertEqual(publish(src, dst, state), 'NEEDS_INITIAL_APPROVAL')
            self.assertEqual(publish(src, dst, state, replace_initial=True), 'PUBLISHED')
            self.assertEqual(dst.read_text(), new)
            self.assertEqual((state / 'latest.jsonl').read_text(), old)
            self.assertEqual(publish(src, dst, state), 'UNCHANGED')
            src.write_text(later)
            self.assertEqual(publish(src, dst, state), 'PUBLISHED')
            self.assertEqual((state / 'latest.jsonl').read_text(), new)
            dst.write_text(old)
            self.assertEqual(publish(src, dst, state), 'CONFLICT')
            self.assertEqual(dst.read_text(), old)

    def test_incomplete_chat_does_not_replace_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, state = (root / x for x in ('source', 'exchange', 'state'))
            for directory in (source, exchange, state): directory.mkdir(mode=0o700)
            name = 'rollout-2026-01-01T00-00-00-019b0000-0000-7000-8000-000000000001.jsonl'
            src, dst = source / name, exchange / name
            src.write_text('{"partial":'); dst.write_text('{"complete":true}\n')
            with self.assertRaises(ns.SyncError): publish(src, dst, state, replace_initial=True)
            self.assertEqual(dst.read_text(), '{"complete":true}\n')

    def test_identical_first_snapshot_records_baseline_before_later_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, state = (root / x for x in ('source', 'exchange', 'state'))
            for directory in (source, exchange, state): directory.mkdir(mode=0o700)
            name = 'rollout-2026-01-01T00-00-00-019b0000-0000-7000-8000-000000000001.jsonl'
            src, dst = source / name, exchange / name
            src.write_text('{"msg":"same"}\n'); dst.write_text(src.read_text())
            self.assertEqual(publish(src, dst, state, replace_initial=True), 'UNCHANGED')
            dst.write_text('{"msg":"remote"}\n'); src.write_text('{"msg":"local"}\n')
            self.assertEqual(publish(src, dst, state, replace_initial=True), 'CONFLICT')
            self.assertEqual(dst.read_text(), '{"msg":"remote"}\n')

    def test_prefer_local_preserves_remote_only_changes_and_resolves_divergence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, state = (root / x for x in ('source', 'exchange', 'state'))
            for directory in (source, exchange, state): directory.mkdir(mode=0o700)
            name = 'rollout-2026-01-01T00-00-00-019b0000-0000-7000-8000-000000000001.jsonl'
            src, dst = source / name, exchange / name
            common = '{"msg":"common"}\n'
            src.write_text(common); dst.write_text(common)
            args = {'prefer_local': True, 'path_roots': ('/local/Nexus', '/remote/Nexus')}
            self.assertEqual(publish(src, dst, state, replace_initial=True, **args), 'UNCHANGED')
            dst.write_text(common + '{"msg":"remote"}\n')
            self.assertEqual(publish(src, dst, state, **args), 'WAITING_REMOTE')
            src.write_text(common + '{"msg":"local"}\n')
            self.assertEqual(publish(src, dst, state, **args), 'PUBLISHED')
            self.assertIn('local', dst.read_text())
            self.assertIn('remote', (state / 'latest.jsonl').read_text())

    def test_prefer_local_does_not_republish_equivalent_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, exchange, state = (root / x for x in ('source', 'exchange', 'state'))
            for directory in (source, exchange, state): directory.mkdir(mode=0o700)
            name = 'rollout-2026-01-01T00-00-00-019b0000-0000-7000-8000-000000000001.jsonl'
            src, dst = source / name, exchange / name
            src.write_text(json.dumps({'type': 'session_meta', 'payload': {'cwd': '/local/Nexus'}}) + '\n')
            dst.write_text(json.dumps({'type': 'session_meta', 'payload': {'cwd': '/remote/Nexus'}}) + '\n')
            args = {'prefer_local': True, 'path_roots': ('/local/Nexus', '/remote/Nexus')}
            self.assertEqual(publish(src, dst, state, replace_initial=True, **args), 'UNCHANGED')
            self.assertEqual(publish(src, dst, state, **args), 'UNCHANGED')
            self.assertIn('/remote/Nexus', dst.read_text())


if __name__ == '__main__': unittest.main()
