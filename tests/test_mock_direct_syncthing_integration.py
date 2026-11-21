"""Opt-in localhost Syncthing + disposable Codex integration."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import xml.etree.ElementTree as ET

import cosync_engine as engine
from cosync_syncthing_watch import SyncthingSource
from mock_direct_receiver import MockDirectReceiver
from mock_direct_supervisor import MockSupervisor
import mock_chat_sync as mock


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1' and os.environ.get('NEXUS_TEST_CODEX') == '1'
                     and shutil.which('syncthing') and shutil.which('codex'), 'requires isolated Syncthing and Codex')
class DirectSyncthingIntegrationTests(unittest.TestCase):
    def test_received_folder_triggers_indexed_mock_restore(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-local-integration-') as tmp:
            root = Path(tmp).resolve()
            socks = [socket.socket() for _ in range(4)]
            try:
                for sock in socks: sock.bind(('127.0.0.1', 0))
                ports = [sock.getsockname()[1] for sock in socks]
            finally:
                for sock in socks: sock.close()
            sender_home = root / 'sender-home'
            source, received = sender_home / 'sessions', root / 'received' / 'sessions'
            source.mkdir(parents=True)
            received.mkdir(parents=True)
            titles_received = root / 'received-titles'
            titles_received.mkdir()
            graph_source, graph_received = root / 'sender-graph', root / 'received-graph'
            graph_source.mkdir()
            graph_received.mkdir()
            (graph_source / 'graph.json').write_text(json.dumps({'format': 'cosyncx-active-graph-v1',
                'created_at_ms': 1, 'omitted_missing': 0, 'omitted_archived': 0, 'edges': []}))
            (sender_home / '.stignore').write_text('!/session_index.jsonl\n*\n')
            for name in ('auth.json', 'config.toml', 'state_5.sqlite'):
                (sender_home / name).write_text('synthetic private data')
            homes = [root / 'sender-engine', root / 'receiver-engine']
            clients, processes, keys = [], [], []
            stop = threading.Event()
            worker = None
            binary = Path(shutil.which('syncthing')).resolve(strict=True)
            try:
                for index in range(2):
                    engine.prepare_managed(binary, homes[index], gui_port=ports[index],
                                           listen_address=f'tcp://127.0.0.1:{ports[index + 2]}')
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
                for index, other in ((0, 1), (1, 0)):
                    clients[index].add_peer(ids[other], f'tcp://127.0.0.1:{ports[other + 2]}')
                for index, folder_path in enumerate((source, received)):
                    folder = clients[index]._get('config/defaults/folder')
                    folder.update({'id': 'mock-chats', 'path': str(folder_path),
                                   'type': 'sendonly' if index == 0 else 'receiveonly',
                                   'devices': [{'deviceID': ids[index]}, {'deviceID': ids[1-index]}]})
                    clients[index]._post('config/folders', folder)
                # A separate folder rooted at the Codex home can transfer the
                # title index directly while excluding every other entry.
                for index, folder_path in enumerate((sender_home, titles_received)):
                    folder = clients[index]._get('config/defaults/folder')
                    folder.update({'id': 'mock-titles', 'path': str(folder_path),
                                   'type': 'sendonly' if index == 0 else 'receiveonly',
                                   'devices': [{'deviceID': ids[index]}, {'deviceID': ids[1-index]}]})
                    clients[index]._post('config/folders', folder)
                for index, folder_path in enumerate((graph_source, graph_received)):
                    folder = clients[index]._get('config/defaults/folder')
                    folder.update({'id': 'mock-graph', 'path': str(folder_path),
                                   'type': 'sendonly' if index == 0 else 'receiveonly',
                                   'devices': [{'deviceID': ids[index]}, {'deviceID': ids[1-index]}]})
                    clients[index]._post('config/folders', folder)
                shared, nexus, profile = root / 'shared', root / 'Nexus', root / 'profile'
                shared.mkdir()
                nexus.mkdir()
                key = b'i' * 32
                mock.init_mock_profile(profile, key, shared)
                receiver = MockDirectReceiver(source=received.parent, profile=profile, shared=shared, key=key,
                    source_root='/source/Nexus', target_root=nexus, codex=shutil.which('codex'),
                    folder_id='mock-chats', peer_id=ids[0], quiet_seconds=0,
                    titles=titles_received, titles_folder_id='mock-titles',
                    graph=graph_received, graph_folder_id='mock-graph')
                watch = SyncthingSource(clients[1], 'mock-chats', ids[0], received)
                watch_titles = SyncthingSource(clients[1], 'mock-titles', ids[0], titles_received)
                watch_graph = SyncthingSource(clients[1], 'mock-graph', ids[0], graph_received)
                status_dir = root / 'status'
                status_dir.mkdir(mode=0o700)
                status_file = status_dir / 'status.json'
                supervisor = MockSupervisor(receiver, watch.read, status_file, title_reader=watch_titles.read,
                                            graph_reader=watch_graph.read)
                worker = threading.Thread(target=supervisor.run, args=(stop, 0.2), daemon=True)
                worker.start()
                def add(number):
                    identity = f'019b0000-0000-7000-8000-{number:012d}'
                    rows = [
                        {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                         {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                          'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                          'model_provider': 'openai'}},
                        {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                         {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic'}]}},
                        {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                         {'type': 'user_message', 'message': 'synthetic', 'images': [], 'local_images': [], 'text_elements': []}}]
                    with tempfile.TemporaryDirectory(prefix='cosyncx-migrate-') as temp:
                        home = Path(temp).resolve()
                        (home / 'sessions').mkdir()
                        chat = home / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
                        chat.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                        migrated = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                            env={**os.environ, 'CODEX_HOME': str(home)}, capture_output=True, text=True, timeout=30)
                        self.assertEqual(migrated.returncode, 0, migrated.stderr)
                        shutil.copyfile(chat, source / chat.name)
                    clients[0]._post('db/scan?folder=mock-chats', {})
                last_event = None
                def wait_restore(expected_count):
                    nonlocal last_event
                    deadline = time.monotonic() + 45
                    while time.monotonic() < deadline:
                        try:
                            result = json.loads(status_file.read_text())
                            event = result.get('restore_event')
                            if result.get('state') == 'COMPLETE' and event and event['id'] != last_event:
                                self.assertEqual(event['changed_chats'], expected_count)
                                self.assertTrue(event['mock_only'])
                                self.assertTrue(result['mock_only'])
                                last_event = event['id']
                                return
                        except (OSError, ValueError, KeyError):
                            pass
                        time.sleep(0.2)
                    self.fail('Isolated transfer did not produce an indexed mock restore')
                index_record = {'id': '019b0000-0000-7000-8000-000000000001',
                                'thread_name': 'synthetic title', 'updated_at': '2026-01-01T00:00:00Z'}
                (sender_home / 'session_index.jsonl').write_text(json.dumps(index_record) + '\n')
                clients[0]._post('db/scan?folder=mock-titles', {})
                add(1)
                wait_restore(1)
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    if (titles_received / 'session_index.jsonl').is_file(): break
                    time.sleep(0.2)
                else: self.fail('Synthetic title index was not transferred')
                self.assertEqual(json.loads((titles_received / 'session_index.jsonl').read_text()), index_record)
                self.assertEqual({p.name for p in titles_received.iterdir()} - {'.stfolder'}, {'session_index.jsonl'})
                self.assertIn('synthetic title', (profile / 'session_index.jsonl').read_text())
                second_title = {'id': '019b0000-0000-7000-8000-000000000002',
                                'thread_name': 'second synthetic title', 'updated_at': '2026-01-01T00:00:00Z'}
                (sender_home / 'session_index.jsonl').write_text(json.dumps(index_record) + '\n' + json.dumps(second_title) + '\n')
                clients[0]._post('db/scan?folder=mock-titles', {})
                add(2)
                wait_restore(1)
                self.assertIn('second synthetic title', (profile / 'session_index.jsonl').read_text())
            finally:
                stop.set()
                if worker is not None:
                    worker.join(timeout=20)
                for index, process in enumerate(processes):
                    if process.poll() is None:
                        req = urllib.request.Request(f'http://127.0.0.1:{ports[index]}/rest/system/shutdown',
                            data=b'', method='POST', headers={'X-API-Key': keys[index]})
                        try:
                            with clients[index].opener.open(req, timeout=5): pass
                        except OSError: pass
                    try: process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        process.wait(timeout=5)


if __name__ == '__main__': unittest.main()
