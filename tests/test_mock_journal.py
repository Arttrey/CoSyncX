from pathlib import Path
import tempfile
import unittest

import mock_journal
import nexus_sync as ns


class MockJournalTests(unittest.TestCase):
    def test_any_interrupted_transaction_blocks_other_backup(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-journal-') as tmp:
            root = Path(tmp)
            mock_journal.ensure_clear(root)
            for name in mock_journal.PENDING:
                path = root / name
                path.write_text('{}')
                with self.assertRaises(ns.SyncError):
                    mock_journal.ensure_clear(root)
                mock_journal.ensure_clear(root, except_name=name)
                path.unlink()
            with self.assertRaises(ns.SyncError):
                mock_journal.ensure_clear(root, except_name='unknown')


if __name__ == '__main__': unittest.main()
