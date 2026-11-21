"""Opt-in test: title-only source via an encrypted localhost relay."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine
import cosync_title_source


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and shutil.which('syncthing'),
                     'requires isolated Syncthing')
class TitleRelayTests(unittest.TestCase):
    def test_direct_title_source_only_via_untrusted_relay(self):
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
            (source / 'sessions' / 'rollout.jsonl').write_text('SYNTHETIC_CHAT_MUST_NOT_TRANSFER')
            record = {'id': '019b0000-0000-7000-8000-000000000001', 'thread_name': 'synthetic private title',
                      'updated_at': '2026-01-01T00:00:00Z'}
            (source / 'session_index.jsonl').write_text(json.dumps(record) + '\n')
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
                    deadline = time.monotonic() + 30
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
                own = cosync_title_source.prepare(clients[0], source, 'mock-titles-encrypted')
                cosync_title_source.verify(clients[0], source, 'mock-titles-encrypted', own)
                for index, folder_path in enumerate((relay, received), start=1):
                    folder = clients[index]._get('config/defaults/folder')
                    folder.update({'id': 'mock-titles-encrypted', 'path': str(folder_path),
                                   'type': ('sendonly', 'receiveencrypted', 'receiveonly')[index],
                                   'devices': devices[index]})
                    clients[index]._post('config/folders', folder)
                sender = clients[0]._get('config/folders/mock-titles-encrypted')
                sender['devices'] = devices[0]
                request = urllib.request.Request(f'http://127.0.0.1:{ports[0]}/rest/config/folders/mock-titles-encrypted',
                    data=json.dumps(sender).encode(), method='PUT',
                    headers={'X-API-Key': keys[0], 'Content-Type': 'application/json'})
                with clients[0].opener.open(request, timeout=5): pass
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    file = received / 'session_index.jsonl'
                    if file.is_file(): break
                    time.sleep(0.2)
                else: self.fail('Encrypted synthetic title did not arrive through relay')
                self.assertEqual(json.loads(file.read_text()), record)
                relay_status = clients[1]._get('db/status?folder=mock-titles-encrypted')
                self.assertEqual(relay_status.get('errors'), 0)
                self.assertEqual({p.name for p in received.iterdir()} - {'.stfolder'}, {'session_index.jsonl'})
                relay_files = [path for path in relay.rglob('*') if path.is_file() and '.stfolder' not in path.parts]
                self.assertTrue(relay_files)
                for path in relay_files:
                    self.assertNotIn(b'synthetic private title', path.read_bytes())
                    self.assertNotIn(b'SYNTHETIC_SECRET_MUST_NOT_TRANSFER', path.read_bytes())
                self.assertFalse((source / '.stfolder').exists())
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
