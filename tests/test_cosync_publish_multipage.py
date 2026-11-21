import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cosync_publish_multipage import replace_pages
import nexus_sync as ns


class PublishMultipageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name).resolve()
        self.profile, self.exchange, self.state = (root / x for x in ('profile', 'exchange', 'state'))
        for p in (self.profile, self.exchange, self.state): p.mkdir(mode=0o700)
        (self.profile / 'sessions').mkdir(); (self.exchange / 'sessions').mkdir()
        self.id = '019b0000-0000-7000-8000-000000000001'
        self.page_id = '019b0000-0000-7000-8000-000000000002'
        self.name = f'rollout-2026-01-01T00-00-00-{self.id}.jsonl'
        self.local = self.profile / 'sessions' / self.name
        self.root = self.exchange / 'sessions' / self.name
        self.page = self.exchange / 'sessions' / self.name.replace('.jsonl', '_' + self.page_id + '.jsonl')
        def record(base=None):
            payload = {'id': self.id, 'cwd': '/Nexus', 'source': 'vscode'}
            if base: payload['history_base'] = {'thread_id': base}
            return json.dumps({'type': 'session_meta', 'payload': payload}) + '\n'
        self.local.write_text(record() + '{"text":"local"}\n')
        self.root.write_text(record() + '{"text":"old"}\n')
        self.page.write_text(record(self.id) + '{"text":"old page"}\n')

    def tearDown(self): self.temp.cleanup()

    def test_recovers_interrupted_page_removal(self):
        original_unlink = Path.unlink
        def fail_page(path, *args, **kwargs):
            if path == self.page: raise OSError('simulated interruption')
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', fail_page):
            with self.assertRaises(OSError):
                replace_pages(self.profile, self.exchange, self.state, self.id, lambda: True)
        self.assertTrue((self.state / 'pending.json').exists())
        self.assertEqual(self.root.read_text(), self.local.read_text())
        self.assertEqual(replace_pages(self.profile, self.exchange, self.state, self.id, lambda: True), 2)
        self.assertFalse(self.page.exists())
        self.assertFalse((self.state / 'pending.json').exists())
        self.assertEqual(len(list((self.state / 'latest').glob('page-*.jsonl'))), 2)

    def test_rejects_unpaused_folder_and_external_reference(self):
        with self.assertRaises(ns.SyncError):
            replace_pages(self.profile, self.exchange, self.state, self.id, lambda: False)
        other = '019b0000-0000-7000-8000-000000000003'
        path = self.exchange / 'sessions' / f'rollout-2026-01-01T00-00-00-{other}.jsonl'
        path.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': other, 'cwd': '/Nexus',
            'source': 'vscode', 'history_base': {'thread_id': self.page_id}}}) + '\n')
        with self.assertRaises(ns.SyncError):
            replace_pages(self.profile, self.exchange, self.state, self.id, lambda: True)
        self.assertTrue(self.page.exists())

    def test_folder_unpaused_during_publication_is_deferred(self):
        checks = iter((True, True, False))
        with self.assertRaises(ns.SyncError):
            replace_pages(self.profile, self.exchange, self.state, self.id, lambda: next(checks, False))
        self.assertTrue((self.state / 'pending.json').exists())
        self.assertEqual(replace_pages(self.profile, self.exchange, self.state, self.id, lambda: True), 2)


if __name__ == '__main__': unittest.main()
