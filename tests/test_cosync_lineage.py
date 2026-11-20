import json
from pathlib import Path
import tempfile
import unittest

import cosync_lineage as lineage
import nexus_sync as ns


class LineageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.source = self.root / 'source'
        self.folder = self.source / 'sessions' / '2026' / '01' / '01'
        self.folder.mkdir(parents=True)
        self.nexus = self.root / 'Local Nexus'
        self.nexus.mkdir()
        self.thread = '019b0000-0000-7000-8000-000000000001'
        self.page = '019b0000-0000-7000-8000-000000000002'

    def _write(self, page=None, base=None, cwd='c:\\Neurocity'):
        name = f'rollout-2026-01-01T00-00-00-{self.thread}' + (f'_{page}' if page else '') + '.jsonl'
        payload = {'id': self.thread, 'history_mode': 'paginated', 'cwd': cwd}
        if base is not None: payload['history_base'] = {'thread_id': base[0], 'end_byte_offset': base[1], 'end_ordinal_exclusive': 2}
        records = [{'type': 'session_meta', 'payload': payload, 'ordinal': 0},
                   {'type': 'response_item', 'payload': {'text': 'c:\\Neurocity is historical text'}, 'ordinal': 1}]
        path = self.folder / name
        path.write_text(''.join(json.dumps(x) + '\n' for x in records))
        return path

    def test_rewrites_cwd_and_updates_parent_boundary_without_touching_text(self):
        root_page = self._write()
        child = self._write(self.page, (self.thread, root_page.stat().st_size), '/home/arttrey/c:\\Neurocity')
        before = [root_page.read_bytes(), child.read_bytes()]
        target = self.root / 'prepared'
        mappings = [('c:\\Neurocity', str(self.nexus)), ('/home/arttrey/c:\\Neurocity', str(self.nexus))]
        self.assertEqual(lineage.prepare_lineage(self.source, target, self.thread, mappings), 2)
        out_root = target / root_page.relative_to(self.source)
        out_child = target / child.relative_to(self.source)
        records = [json.loads(x) for x in out_child.read_text().splitlines()]
        self.assertEqual(records[0]['payload']['cwd'], str(self.nexus))
        self.assertEqual(records[0]['payload']['history_base']['end_byte_offset'], out_root.stat().st_size)
        self.assertEqual(records[1]['payload']['text'], 'c:\\Neurocity is historical text')
        self.assertEqual([root_page.read_bytes(), child.read_bytes()], before)

    def test_missing_page_and_unmapped_cwd_fail_without_output(self):
        self._write(self.page, (self.thread, 20))
        target = self.root / 'prepared'
        with self.assertRaises(ns.SyncError):
            lineage.prepare_lineage(self.source, target, self.thread, [('c:\\Neurocity', str(self.nexus))])
        self.assertFalse(target.exists())
        root_page = self._write(cwd='/unknown/path')
        child = next(p for p in self.folder.iterdir() if p != root_page)
        row = json.loads(child.read_text().splitlines()[0])
        row['payload']['history_base']['end_byte_offset'] = root_page.stat().st_size
        child.write_text(json.dumps(row) + '\n')
        with self.assertRaises(ns.SyncError):
            lineage.prepare_lineage(self.source, target, self.thread, [('c:\\Neurocity', str(self.nexus))])
        self.assertFalse(target.exists())


if __name__ == '__main__': unittest.main()
