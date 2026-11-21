from pathlib import Path
import sys
import tempfile
import unittest

import mock_chat_sync as locking


@unittest.skipUnless(sys.platform in ('linux', 'darwin'), 'requires Unix writer locks')
class WriterQueueTests(unittest.TestCase):
    def test_busy_chat_can_be_deferred_without_blocking_another(self):
        import fcntl
        with tempfile.TemporaryDirectory(prefix='cosyncx-writer-queue-') as tmp:
            profile = Path(tmp).resolve()
            directory = profile / 'thread-writer-locks'; directory.mkdir()
            (directory / '.coordination.lock').touch()
            busy = '019b0000-0000-7000-8000-000000000001'
            free = '019b0000-0000-7000-8000-000000000002'
            with (directory / (busy + '.lock')).open('a+b') as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                found = locking._busy_thread_ids(profile)
                self.assertEqual(found, {busy})
                with self.assertRaises(locking.MockChatBusy):
                    with locking._mock_thread_lock(profile, free): pass
                with locking._mock_thread_lock(profile, free, ignore_busy=frozenset(found)):
                    with (directory / (free + '.lock')).open('a+b') as free_stream:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(free_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(locking._busy_thread_ids(profile), set())
            with locking._mock_thread_lock(profile, busy): pass


if __name__ == '__main__':
    unittest.main()
