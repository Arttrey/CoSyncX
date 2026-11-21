"""Opt-in experiment using a disposable profile and the installed Codex."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import mock_chat_sync as mock
from mock_direct_receiver import MockDirectReceiver


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'),
                     'requires disposable Codex experiment')
class DirectReceiverTests(unittest.TestCase):
    def test_linked_subagent_prevents_incomplete_success(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-child-') as tmp:
            root = Path(tmp).resolve()
            source, shared, target, profile = (root / x for x in ('source', 'shared', 'Nexus', 'profile'))
            (source / 'sessions').mkdir(parents=True)
            shared.mkdir()
            target.mkdir()
            key = b'z' * 32
            mock.init_mock_profile(profile, key, shared)
            parent = '019b0000-0000-7000-8000-000000000001'
            child = '019b0000-0000-7000-8000-000000000002'
            for identity, source_kind, parent_id in ((parent, 'vscode', None),
                    (child, {'subagent': {'other': 'guardian'}}, parent)):
                meta = {'id': identity, 'source': source_kind, 'cwd': '/source/Nexus',
                        'history_mode': 'paginated', 'parent_thread_id': parent_id}
                (source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                    json.dumps({'type': 'session_meta', 'payload': meta, 'ordinal': 0}) + '\n')
            receiver = MockDirectReceiver(source=source, profile=profile, shared=shared, key=key,
                source_root='/source/Nexus', target_root=target, codex=shutil.which('codex'),
                folder_id='chats', peer_id='vps', quiet_seconds=0)
            status = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                      'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(): return 'chats', status, True, 'valid'
            receiver.poll(read, now=0)
            result = receiver.poll(read, now=1)
            self.assertEqual(result['state'], 'PENDING')
            self.assertNotIn('restore_event', result)
            self.assertFalse((profile / 'sessions').exists())

    def test_title_only_change_waits_for_title_folder_and_keeps_previous_title(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-title-update-') as tmp:
            root = Path(tmp).resolve()
            source, shared, target, profile, titles, backups = (root / x for x in
                ('source', 'shared', 'Nexus', 'profile', 'titles', 'backups'))
            (source / 'sessions').mkdir(parents=True)
            for path in (shared, target, titles, backups): path.mkdir()
            key = b't' * 32
            mock.init_mock_profile(profile, key, shared)
            identity = '019b0000-0000-7000-8000-000000000001'
            rows = [{'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                     {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                      'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic message'}]}},
                    {'timestamp': '2026-01-01T00:00:01.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'synthetic message', 'images': [], 'local_images': [], 'text_elements': []}}]
            history = source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                env={**os.environ, 'CODEX_HOME': str(source)}, capture_output=True, text=True, timeout=30)
            self.assertEqual(migration.returncode, 0, migration.stderr)
            def set_title(name):
                (titles / 'session_index.jsonl').write_text(json.dumps({'id': identity, 'thread_name': name,
                    'updated_at': '2026-01-01T00:00:00Z'}) + '\n')
            set_title('original title')
            chat = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                    'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            title_state = dict(chat, remoteSequence={'vps': 1})
            def read_chat(): return 'chats', chat, True, 'valid'
            def read_title(): return 'titles', title_state, True, 'valid'
            receiver = MockDirectReceiver(source=source, profile=profile, shared=shared, key=key,
                source_root='/source/Nexus', target_root=target, codex=shutil.which('codex'),
                folder_id='chats', peer_id='vps', quiet_seconds=0, titles=titles,
                titles_folder_id='titles', backup_root=backups)
            self.assertEqual(receiver.poll(read_chat, title_status_reader=read_title, now=0)['state'], 'WAITING')
            self.assertEqual(receiver.poll(read_chat, title_status_reader=read_title, now=1)['state'], 'COMPLETE')
            set_title('updated title')
            title_state['sequence'] = 2
            title_state['remoteSequence']['vps'] = 2
            title_state['needFiles'] = 1
            self.assertEqual(receiver.poll(read_chat, title_status_reader=read_title, now=2)['state'], 'WAITING')
            title_state['needFiles'] = 0
            self.assertEqual(receiver.poll(read_chat, title_status_reader=read_title, now=3)['state'], 'WAITING')
            result = receiver.poll(read_chat, title_status_reader=read_title, now=4)
            self.assertEqual(result['state'], 'COMPLETE')
            self.assertEqual(result['restore_event']['changed_chats'], 1)
            self.assertIn('updated title', (profile / 'session_index.jsonl').read_text())
            self.assertIn('original title', (backups / 'latest' / 'session_index.jsonl').read_text())
            self.assertEqual([p.name for p in backups.iterdir()], ['latest'])

    def test_incomplete_projection_never_emits_restore_event(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-incomplete-index-') as tmp:
            root = Path(tmp).resolve()
            source, shared, target, profile = (root / x for x in ('source', 'shared', 'Nexus', 'profile'))
            (source / 'sessions').mkdir(parents=True)
            shared.mkdir()
            target.mkdir()
            key = b'z' * 32
            mock.init_mock_profile(profile, key, shared)
            identity = '019b0000-0000-7000-8000-000000000001'
            rows = [
                {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'ordinal': 0, 'payload':
                 {'id': identity, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus',
                  'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode',
                  'model_provider': 'openai', 'history_mode': 'paginated'}},
                {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'ordinal': 1, 'payload':
                 {'type': 'user_message', 'message': 'synthetic missing message', 'images': [], 'local_images': [], 'text_elements': []}}]
            (source / 'sessions' / f'rollout-2026-01-01T00-00-00-{identity}.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in rows))
            receiver = MockDirectReceiver(source=source, profile=profile, shared=shared, key=key,
                source_root='/source/Nexus', target_root=target, codex=shutil.which('codex'),
                folder_id='chats', peer_id='vps', quiet_seconds=0)
            status = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1}, 'needFiles': 0,
                      'needDirectories': 0, 'needSymlinks': 0, 'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(): return 'chats', status, True, 'valid'
            self.assertEqual(receiver.poll(read, now=0)['state'], 'WAITING')
            result = receiver.poll(read, now=1)
            self.assertEqual(result['state'], 'PENDING')
            self.assertNotIn('restore_event', result)

    def test_transfer_then_index_and_emit_only_for_new_source_revision(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source, shared, target, profile = (root / x for x in ('source', 'shared', 'Nexus', 'profile'))
            (source / 'sessions').mkdir(parents=True)
            shared.mkdir()
            (target / 'Bot').mkdir(parents=True)
            key = b'x' * 32
            mock.init_mock_profile(profile, key, shared)
            def add(number):
                chat = f'019b0000-0000-7000-8000-{number:012d}'
                image = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLttAAAAABJRU5ErkJggg=='
                rows = [
                    {'timestamp': '2026-01-01T00:00:00.000Z', 'type': 'session_meta', 'payload':
                     {'id': chat, 'timestamp': '2026-01-01T00:00:00.000Z', 'cwd': '/source/Nexus/Bot',
                      'originator': 'codex_vscode', 'cli_version': '0.1.0', 'source': 'vscode', 'model_provider': 'openai'}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'synthetic chat'},
                                                                    {'type': 'input_image', 'image_url': image}]}},
                    {'timestamp': '2026-01-01T00:00:01.000Z', 'type': 'event_msg', 'payload':
                     {'type': 'user_message', 'message': 'synthetic chat', 'images': [image], 'local_images': [], 'text_elements': []}},
                    {'timestamp': '2026-01-01T00:00:02.000Z', 'type': 'response_item', 'payload':
                     {'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'synthetic answer'}]}},
                    {'timestamp': '2026-01-01T00:00:02.001Z', 'type': 'event_msg', 'payload':
                     {'type': 'agent_message', 'message': 'synthetic answer'}}]
                with tempfile.TemporaryDirectory(prefix='cosyncx-migrate-') as tmp_source:
                    build_home = Path(tmp_source).resolve()
                    (build_home / 'sessions').mkdir()
                    history = build_home / 'sessions' / f'rollout-2026-01-01T00-00-00-{chat}.jsonl'
                    history.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                    migration = subprocess.run([shutil.which('codex'), 'migrate-rollouts', '--apply', '--json'],
                        env={**os.environ, 'CODEX_HOME': str(build_home)}, capture_output=True, text=True, timeout=30)
                    self.assertEqual(migration.returncode, 0, migration.stderr)
                    shutil.copyfile(history, source / 'sessions' / history.name)
            add(1)
            state = {'state': 'idle', 'sequence': 1, 'remoteSequence': {'vps': 1},
                     'needFiles': 0, 'needDirectories': 0, 'needSymlinks': 0,
                     'needDeletes': 0, 'errors': 0, 'pullErrors': 0}
            def read(): return 'chats', state, True, 'valid'
            receiver = MockDirectReceiver(source=source, profile=profile, shared=shared, key=key,
                source_root='/source/Nexus', target_root=target, codex=shutil.which('codex'),
                folder_id='chats', peer_id='vps', quiet_seconds=0)
            self.assertEqual(receiver.poll(read, now=0)['state'], 'WAITING')
            first = receiver.poll(read, now=1)
            self.assertEqual(first['state'], 'COMPLETE')
            self.assertEqual(first['restore_event']['changed_chats'], 1)
            self.assertEqual(receiver.poll(read, now=2)['state'], 'CURRENT')
            # A new remote index sequence alone is not proof of a chat change.
            state['remoteSequence']['vps'] = 2
            state['sequence'] = 2
            self.assertEqual(receiver.poll(read, now=3)['state'], 'WAITING')
            unchanged = receiver.poll(read, now=4)
            self.assertEqual(unchanged['state'], 'COMPLETE')
            self.assertIsNone(unchanged['restore_event'])
            add(2)
            state['needFiles'] = 1
            state['remoteSequence']['vps'] = 3
            self.assertEqual(receiver.poll(read, now=5)['state'], 'WAITING')
            state['needFiles'] = 0
            state['sequence'] = 3
            self.assertEqual(receiver.poll(read, now=6)['state'], 'WAITING')
            second = receiver.poll(read, now=7)
            self.assertEqual(second['state'], 'COMPLETE')
            self.assertEqual(second['restore_event']['changed_chats'], 1)
            self.assertEqual(len(list((profile / 'sessions').rglob('*.jsonl'))), 2)
            # A multi-page thread is not yet handled by the mock batch importer.
            # It must remain pending rather than announcing a successful reload.
            first_id = '019b0000-0000-7000-8000-000000000001'
            page_id = '019b0000-0000-7000-8000-000000000099'
            original = next((source / 'sessions').glob(f'*{first_id}.jsonl'))
            meta = json.loads(original.read_text().splitlines()[0])
            meta['ordinal'] = 3
            meta['payload']['history_base'] = {'thread_id': first_id,
                'end_byte_offset': original.stat().st_size, 'end_ordinal_exclusive': 3}
            original.with_name(original.stem + '_' + page_id + '.jsonl').write_text(json.dumps(meta) + '\n')
            state['remoteSequence']['vps'] = 4
            state['sequence'] = 4
            self.assertEqual(receiver.poll(read, now=8)['state'], 'WAITING')
            pending = receiver.poll(read, now=9)
            self.assertEqual(pending['state'], 'PENDING')
            self.assertNotIn('restore_event', pending)


if __name__ == '__main__': unittest.main()
