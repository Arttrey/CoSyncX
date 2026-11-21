"""Experimental direct-folder receive cycle for disposable mock profiles only."""
from pathlib import Path
import tempfile

import cosync_auto_gate
import cosync_receive_plan
import cosync_restore_backup
import mock_chat_sync as mock
import mock_paginated_import as pages_import
import nexus_sync as ns


class MockDirectReceiver:
    """Exercise the receive pipeline without ever writing a real Codex profile.

    status_reader returns (folder_id, status, connected, remote_state) from the
    selected Syncthing folder and peer. It must be called again after indexing.
    """

    def __init__(self, *, source, profile, shared, key, source_root, target_root,
                 codex, folder_id, peer_id, quiet_seconds=5, backup_root=None,
                 titles=None, titles_folder_id=None):
        self.source = mock._mock_path(Path(source))
        self.profile = mock._validate_profile(Path(profile), key)
        self.shared = mock._mock_path(Path(shared))
        self.target = mock._mock_path(Path(target_root))
        self.backup_root = mock._mock_path(Path(backup_root)) if backup_root is not None else None
        self.titles = mock._mock_path(Path(titles)) if titles is not None else None
        if (self.titles is None) != (titles_folder_id is None):
            raise ns.SyncError('Mock title folder and ID must be configured together')
        if self.titles is not None and (not self.titles.is_dir() or self.titles == self.source
                or self.titles in self.source.parents or self.source in self.titles.parents):
            raise ns.SyncError('Mock title folder must be separate')
        if (not (self.source / 'sessions').is_dir()
                or (self.source / 'archived_sessions').exists()):
            raise ns.SyncError('Expected an active-only temporary source')
        self.key, self.source_root, self.codex = key, source_root, Path(codex)
        self.gate = (cosync_auto_gate.RestoreGate(folder_id=folder_id, peer_id=peer_id, quiet_seconds=quiet_seconds)
                     if self.titles is None else cosync_auto_gate.MultiFolderGate(
                         folder_ids=(folder_id, titles_folder_id), peer_id=peer_id, quiet_seconds=quiet_seconds))
        self.pending_threads = set()

    def _chat_digests(self):
        catalog = ns._chat_catalog(self.profile, include_unsupported=True)
        return {(chat, item['path'].name): ns._digest(item['path'])
                for chat, entries in catalog.items() for item in entries}

    def _title_names(self):
        data = ns._read_title_index(self.profile)
        return {row['id']: row['thread_name'] for row in ns._parse_title_index(data)} if data is not None else {}

    def _receive(self):
        if self.backup_root is not None and any((self.backup_root / name).exists()
                or (self.backup_root / name).is_symlink()
                for name in ('mock-title-pending.json', 'mock-update-pending.json')):
            return False
        incoming = ns._chat_catalog(self.source, include_unsupported=True)
        title_data = ns._read_title_index(self.titles) if self.titles is not None else None
        if self.titles is not None and title_data is None:
            return False
        titles = {row['id']: row['thread_name'] for row in ns._parse_title_index(title_data)} if title_data is not None else {}
        kinds = {thread: cosync_receive_plan.source_kind(entries) for thread, entries in incoming.items()}
        if 'unknown' in kinds.values():
            return False
        # A subagent history can be a dependency of a visible conversation.
        # Do not claim a complete restore while that relationship is unsupported.
        if any(kinds[thread] == 'subagent' and kinds.get(entries[0].get('parent_thread_id')) == 'chat'
               for thread, entries in incoming.items()):
            return False
        incoming = {thread: entries for thread, entries in incoming.items() if kinds[thread] == 'chat'}
        if not incoming:
            return True
        multi = {thread: entries for thread, entries in incoming.items() if len(entries) > 1}
        local = ns._chat_catalog(self.profile, include_unsupported=True)
        local_titles = self._title_names()
        changed_titles = {thread: title for thread, title in titles.items()
                          if thread in incoming and thread in local and local_titles.get(thread) != title}
        if changed_titles:
            if self.backup_root is None:
                return False
            pages_import.replace_mock_titles(self.profile, self.shared, self.backup_root, self.key,
                                             changed_titles, self.codex)
        for thread in multi:
            # Flattening remains a mock-only experiment until references from
            # other threads and incremental updates have been verified.
            with tempfile.TemporaryDirectory(prefix='cosyncx-mock-compare-') as temp:
                prepared = Path(temp).resolve() / 'prepared'
                pages_import.prepare_flattened_mock(self.source, prepared, thread,
                                                   [(self.source_root, str(self.target))])
                checked = ns._chat_catalog(prepared, include_unsupported=True).get(thread, [])
                if len(checked) != 1:
                    return False
                if thread in local:
                    current = local[thread]
                    if len(current) != 1 or current[0]['folder'] != 'sessions':
                        return False
                    relation = ns._path_adjusted_relation(checked[0]['path'], current[0]['path'],
                                                          str(self.target), str(self.target))
                    if relation not in ('EQUIVALENT_AFTER_PATH_MAPPING', 'LOCAL_EXTENDS_AFTER_PATH_MAPPING'):
                        return False
                else:
                    pages_import.add_thread(prepared, self.profile, self.shared, self.key, thread,
                                            [(str(self.target), str(self.target))])
                pages_import.index_thread(self.profile, self.shared, self.key, thread, self.codex,
                                          title=titles.get(thread))
        single = [entries[0]['path'] for entries in incoming.values() if len(entries) == 1]
        if not single:
            return bool(multi)
        # The existing single-page mock receiver expects a snapshot. Keep this
        # temporary adapter isolated; the production transport is direct.
        with tempfile.TemporaryDirectory(prefix='cosyncx-mock-single-') as temp:
            staged = Path(temp).resolve() / 'source'
            staged.mkdir()
            for path in single:
                destination = staged / path.relative_to(self.source.resolve(strict=True))
                destination.parent.mkdir(parents=True, exist_ok=True)
                ns._copy_stable_chat(path, destination)
            if title_data is not None:
                (staged / 'session_index.jsonl').write_bytes(title_data)
            ns.snapshot(staged, self.shared, 'source', self.key)
            result = mock.sync_all_mock(self.profile, self.shared, self.key, 'source',
                                        self.source_root, self.target, self.codex,
                                        accept_local_ahead=True)
            if result.get('state') != 'COMPLETE':
                return False
            for thread, entries in incoming.items():
                if len(entries) == 1:
                    pages_import.index_thread(self.profile, self.shared, self.key, thread, self.codex)
            return True

    def poll(self, status_reader, *, title_status_reader=None, on_restore=None, now=None):
        first = status_reader()
        if self.titles is None:
            folder, status, connected, remote_state = first
            revision = self.gate.observe(folder, status, connected=connected, remote_state=remote_state, now=now)
        else:
            if title_status_reader is None:
                return {'state': 'WAITING', 'mock_only': True}
            second = title_status_reader()
            observations = {first[0]: first, second[0]: second}
            revision = self.gate.observe(observations, now=now)
        if revision is None:
            return {'state': 'CURRENT' if self.gate.is_current(now=now) else 'WAITING', 'mock_only': True}
        try:
            if on_restore is not None:
                on_restore()
            before = self._chat_digests()
            before_titles = self._title_names()
            try:
                complete = self._receive()
            finally:
                after_chats = self._chat_digests()
                after_titles = self._title_names()
                changed = {key[0] for key, digest in after_chats.items() if before.get(key) != digest}
                changed.update(thread for thread, name in after_titles.items() if before_titles.get(thread) != name)
                self.pending_threads.update(changed)
            if not complete:
                return {'state': 'PENDING', 'mock_only': True}
            result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': len(self.pending_threads)}
            first = status_reader()
            if self.titles is None:
                folder, after, connected, remote_state = first
                event = self.gate.complete(revision, result, folder_id=folder, after=after,
                                           connected=connected, remote_state=remote_state)
            else:
                second = title_status_reader()
                event = self.gate.complete(revision, result, after={first[0]: first, second[0]: second})
            if event is not None:
                event['mock_only'] = True
            self.pending_threads.clear()
            return {'state': 'COMPLETE', 'mock_only': True, 'restore_event': event,
                    'verified_in_cursor': False}
        except (ns.SyncError, cosync_restore_backup.BackupError, OSError, ValueError, TypeError):
            return {'state': 'PENDING', 'mock_only': True}
