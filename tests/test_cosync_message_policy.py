import json
from pathlib import Path
import tempfile
import unittest

from cosync_message_policy import choose, message_count
import nexus_sync as ns


def record(message):
    return json.dumps({'type': 'event_msg', 'payload': {'type': 'user_message', 'message': message}}) + '\n'


class MessagePolicyTests(unittest.TestCase):
    def test_message_count_not_file_size_or_time_selects_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = Path(tmp) / 'a.jsonl', Path(tmp) / 'b.jsonl'
            a.write_text(record('a') + record('b'))
            b.write_text(record('much longer text ' * 1000))
            self.assertGreater(b.stat().st_size, a.stat().st_size)
            self.assertEqual(message_count(a), 2)
            self.assertEqual(choose(a, b, '/source', '/target'), 'SOURCE_MORE')
            self.assertEqual(choose(b, a, '/target', '/source'), 'TARGET_MORE')
            b.write_text(record('a') + record('different'))
            self.assertEqual(choose(a, b, '/source', '/target'), 'TIE')
            b.write_text(a.read_text())
            self.assertEqual(choose(a, b, '/source', '/target'), 'EQUAL')

    def test_unknown_or_empty_history_does_not_win(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = Path(tmp) / 'a', Path(tmp) / 'b'
            a.write_text('{"type":"unknown"}\n'); b.write_text(a.read_text())
            with self.assertRaises(ns.SyncError): choose(a, b, '/source', '/target')


if __name__ == '__main__': unittest.main()
