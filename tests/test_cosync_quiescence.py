import unittest
from unittest import mock

import cosync_quiescence as q


class QuiescenceTests(unittest.TestCase):
    def test_detects_cursor_and_codex_without_relying_on_window_state(self):
        self.assertTrue(q.parse_processes('  10 /Applications/Cursor.app/Contents/MacOS/Cursor\n'))
        self.assertTrue(q.parse_processes('  11 /usr/local/bin/codex\n'))
        self.assertTrue(q.parse_processes('  12 codex-code-mode-host\n'))
        self.assertFalse(q.parse_processes('  1 /sbin/init\n  2 /usr/bin/python3\n'))
        for malformed in ('', 'not-a-process', '4'):
            with self.subTest(value=malformed), self.assertRaises(q.QuiescenceError): q.parse_processes(malformed)

    def test_unknown_platform_and_process_failure_fail_closed(self):
        with mock.patch.object(q.sys, 'platform', 'win32'):
            with self.assertRaises(q.QuiescenceError): q.is_quiet()
        with mock.patch.object(q.sys, 'platform', 'darwin'), mock.patch.object(q.subprocess, 'run', side_effect=OSError):
            with self.assertRaises(q.QuiescenceError): q.is_quiet()


if __name__ == '__main__': unittest.main()
