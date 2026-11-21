"""Opt-in test: title-only source via an encrypted localhost relay."""
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine
import cosync_title_snapshot


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'),
                     'requires isolated Syncthing')
class TitleRelayTests(unittest.TestCase):
    def test_active_title_export_via_untrusted_relay(self):
        self.exercise_relay(reverse=False)

    def test_bidirectional_data_via_untrusted_relay(self):
        self.exercise_relay(reverse=True)

    def exercise_relay(self, reverse):
        with tempfile.TemporaryDirectory(prefix='cosyncx-title-relay-') as tmp:
            root = Path(tmp).resolve()
            sockets = [socket.socket() for _ in range(6)]
            try:
                for sock in sockets: sock.bind(('127.0.0.1', 0))
                ports = [sock.getsockname()[1] for sock in sockets]
            finally:
                for sock in sockets: sock.close()
            source, relay, received = root / 'codex', root / 'relay', root / 'received'
            for directory in (source, relay, received): directory.mkdir()
            (source / 'sessions').mkdir()
            (source / 'auth.json').write_text('SYNTHETIC_SECRET_MUST_NOT_TRANSFER')
            record = {'id': '019b0000-0000-7000-8000-000000000001', 'thread_name': 'synthetic private title',
                      'updated_at': '2026-01-01T00:00:00Z'}
            archived = {'id': '019b0000-0000-7000-8000-000000000002', 'thread_name': 'synthetic archived title',
                        'updated_at': '2026-01-01T00:00:00Z'}
            for item in (record, archived):
                (source / 'sessions' / f"rollout-2026-01-01T00-00-00-{item['id']}.jsonl").write_text('SYNTHETIC_CHAT_MUST_NOT_TRANSFER')
            with sqlite3.connect(source / 'state_5.sqlite') as db:
                db.execute('CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)')
                db.executemany('INSERT INTO threads VALUES (?,?)', [(record['id'], 0), (archived['id'], 1)])
            # Exercise a file spanning many encryption chunks, not just a tiny record.
            record['thread_name'] *= 1500
            (source / 'session_index.jsonl').write_text(json.dumps(record) + '\n' + json.dumps(archived) + '\n')
            export = root / 'active-titles'
            export.mkdir(mode=0o700)
            self.assertEqual(cosync_title_snapshot.write_active_titles(source, export / 'session_index.jsonl'), 1)
            homes = [root / 'sender-engine', root / 'relay-engine', root / 'receiver-engine']
            clients, processes, keys = [], [], []
            binary = Path(shutil.which('syncthing')).resolve(strict=True)
            try:
                for index in range(3):
                    engine.prepare_managed(binary, homes[index], gui_port=ports[index],
                                           listen_address=f'tcp://127.0.0.1:{ports[index + 3]}')
                    keys.append(ET.parse(homes[index] / 'config.xml').getroot().findtext('gui/apikey'))
                    clients.append(engine.ExistingEngine(f'http://127.0.0.1:{ports[index]}',
                                                         homes[index] / 'config.xml', config_xml=True))
                    processes.append(subprocess.Popen([sys.executable, str(Path(engine.__file__)), 'run-managed',
                        '--binary', str(binary), '--home', str(homes[index])], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                ids = [subprocess.check_output([str(binary), 'device-id', '--home', str(home)], text=True, timeout=15).strip() for home in homes]
                for client in clients:
                    deadline = time.monotonic() + 60
                    while True:
                        try: client.ping(); break
                        except engine.EngineError:
                            if time.monotonic() > deadline: raise
                            time.sleep(0.2)
                for left, right in ((0, 1), (1, 0), (1, 2), (2, 1)):
                    clients[left].add_peer(ids[right], f'tcp://127.0.0.1:{ports[right + 3]}')
                password = 'synthetic-relay-test-password'
                devices = [
                    [{'deviceID': ids[0]}, {'deviceID': ids[1], 'encryptionPassword': password}],
                    [{'deviceID': ids[1]}, {'deviceID': ids[0]}, {'deviceID': ids[2]}],
                    [{'deviceID': ids[2]}, {'deviceID': ids[1], 'encryptionPassword': password}],
                ]
                for index, folder_path in enumerate((export, relay, received)):
                    folder = clients[index]._get('config/defaults/folder')
                    folder.update({'id': 'mock-titles-encrypted', 'path': str(folder_path),
                                   'type': (('sendreceive', 'receiveencrypted', 'sendreceive') if reverse else
                                            ('sendonly', 'receiveencrypted', 'receiveonly'))[index],
                                   'devices': devices[index]})
                    clients[index]._post('config/folders', folder)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    file = received / 'session_index.jsonl'
                    if file.is_file(): break
                    time.sleep(0.2)
                else: self.fail('Encrypted synthetic title did not arrive through relay')
                self.assertEqual(json.loads(file.read_text()), record)
                self.assertNotIn('synthetic archived title', file.read_text())
                relay_status = clients[1]._get('db/status?folder=mock-titles-encrypted')
                self.assertEqual(relay_status.get('errors'), 0)
                self.assertEqual({p.name for p in received.iterdir()} - {'.stfolder'}, {'session_index.jsonl'})
                relay_files = [path for path in relay.rglob('*') if path.is_file() and '.stfolder' not in path.parts]
                self.assertTrue(relay_files)
                for path in relay_files:
                    self.assertNotIn(b'synthetic private title', path.read_bytes())
                    self.assertNotIn(b'SYNTHETIC_SECRET_MUST_NOT_TRANSFER', path.read_bytes())
                self.assertFalse((source / '.stfolder').exists())
                if reverse:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        relay_connections = clients[1]._get('system/connections').get('connections', {})
                        if all(relay_connections.get(ids[i], {}).get('connected') for i in (0, 2)):
                            break
                        time.sleep(0.25)
                    else:
                        self.fail('Both relay peers did not connect')
                    outgoing = received / 'reverse.txt'
                    outgoing.write_text('SYNTHETIC REVERSE CHAT')
                    clients[2]._post('db/scan?folder=mock-titles-encrypted', {})
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        back = export / 'reverse.txt'
                        if back.is_file() and back.read_text() == 'SYNTHETIC REVERSE CHAT':
                            break
                        time.sleep(0.25)
                    else:
                        states = [client._get('db/status?folder=mock-titles-encrypted') for client in clients]
                        summary = [{k: state.get(k) for k in ('state', 'needTotalItems', 'errors', 'pullErrors')}
                                   for state in states]
                        errors = clients[0]._get('folder/errors?folder=mock-titles-encrypted')
                        self.fail('Reverse encrypted transfer did not reach sender: ' + repr(summary) + ' ' + repr(errors))
                    relay_files = [p for p in relay.rglob('*') if p.is_file() and '.stfolder' not in p.parts]
                    self.assertTrue(all(b'SYNTHETIC REVERSE CHAT' not in p.read_bytes() for p in relay_files))
                    self.assertFalse(clients[0]._get('db/status?folder=mock-titles-encrypted').get('pullErrors'))
            finally:
                for index, process in enumerate(processes):
                    if process.poll() is None:
                        request = urllib.request.Request(f'http://127.0.0.1:{ports[index]}/rest/system/shutdown',
                            data=b'', method='POST', headers={'X-API-Key': keys[index]})
                        try:
                            with clients[index].opener.open(request, timeout=5): pass
                        except OSError: pass
                    try: process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        process.wait(timeout=5)


if __name__ == '__main__': unittest.main()
