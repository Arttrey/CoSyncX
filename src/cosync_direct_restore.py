"""Guarded direct-folder receiver for active chats.

Archived IDs stay archived. Existing active versions are backed up before
replacement. Concurrent mode must be explicitly enabled.
"""
import argparse
from contextlib import closing
import json
import hashlib
import hmac
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import threading
import shutil
import time

import cosync_auto_gate
import cosync_engine
import cosync_graph_snapshot as graph_schema
import cosync_lineage as lineage
import cosync_message_policy as message_policy
import cosync_orphan_restore as orphan_restore
import cosync_quiescence as quiescence
import cosync_receive_plan
import cosync_restore_backup as backup
import cosync_restore_supervisor
import cosync_syncthing_watch
import mock_chat_sync as locking
import mock_graph_restore
import mock_journal
import mock_paginated_import as indexer
import nexus_sync as ns


class DirectReceiver:
    def __init__(self, *, source: Path, titles: Path, graph: Path, profile: Path, backup_root: Path,
                 key: bytes, mappings: list[tuple[str, str]], codex: Path, folder_ids: tuple[str, str, str],
                 peer_id: str, quiet_seconds: float = 5, excluded_thread_ids=(), allow_active: bool = False,
                 prefer_more_messages: bool = False):
        paths = [Path(x).resolve(strict=True) for x in (source, titles, graph, profile, backup_root)]
        if any(not p.is_dir() for p in paths) or len(key) != 32 or not mappings:
            raise ns.SyncError('Invalid direct receiver configuration')
        self.source, self.titles, self.graph, self.profile, self.backup_root = paths
        sessions = (self.source / 'sessions').resolve(strict=True)
        independent = (sessions, self.titles, self.graph, self.profile, self.backup_root)
        if (any(a == b or a in b.parents or b in a.parents for i, a in enumerate(independent)
                for b in independent[i + 1:]) or any(
                item == self.source or item in self.source.parents or self.source in item.parents
                for item in (self.profile, self.backup_root))):
            raise ns.SyncError('Overlapping direct receiver directories')
        info = self.backup_root.stat()
        if os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise ns.SyncError('Backup directory must be private')
        if not (self.source / 'sessions').is_dir() or (self.source / 'archived_sessions').exists():
            raise ns.SyncError('Expected an active-only source')
        self.key, self.mappings = key, list(mappings)
        if (not isinstance(excluded_thread_ids, (tuple, list, set))
                or any(not graph_schema._uuid(identity) for identity in excluded_thread_ids)):
            raise ns.SyncError('Invalid excluded thread IDs')
        self.excluded_thread_ids = frozenset(excluded_thread_ids)
        if type(allow_active) is not bool or (allow_active and sys.platform not in ('darwin', 'linux')):
            raise ns.SyncError('Concurrent restore is unavailable on this platform')
        self.allow_active = allow_active
        if type(prefer_more_messages) is not bool:
            raise ns.SyncError('Invalid message comparison policy')
        self.prefer_more_messages = prefer_more_messages
        self.codex = Path(codex).resolve(strict=True)
        if not self.codex.is_file():
            raise ns.SyncError('Codex executable unavailable')
        self.gate = cosync_auto_gate.MultiFolderGate(folder_ids=folder_ids, peer_id=peer_id,
                                                     quiet_seconds=quiet_seconds)
        self.verified = {}
        self._verified_sources = {}
        self._last_fingerprints = {}
        self.pending_result = None
        self.unresolved_archived_count = 0
        self.pending_busy_ids = frozenset()
        self._last_pending_retry = None
        self._ignored_busy = frozenset()
        self.path_warning_count = 0
        report = self.backup_root / 'missing-paths.json'
        if report.is_file() and not report.is_symlink():
            info = report.stat()
            if (info.st_size <= 16 * 1024 * 1024 and
                    (os.name == 'nt' or (info.st_uid == os.getuid() and not stat.S_IMODE(info.st_mode) & 0o077))):
                try:
                    value = json.loads(report.read_bytes())
                    if value.get('version') == 1 and isinstance(value.get('chats'), dict):
                        self.path_warning_count = len(value['chats'])
                except (ValueError, TypeError, AttributeError):
                    pass

    def _write_allowed(self):
        return self.allow_active or quiescence.is_quiet()

    def _existing_workspace(self, entries):
        """Reuse only this chat's already configured, available local workspace."""
        if not self.prefer_more_messages or len(entries) != 1 or entries[0].get('folder') != 'sessions':
            return None
        cwd = entries[0].get('cwd')
        if not isinstance(cwd, str):
            return None
        try:
            return Path(lineage._map_cwd(cwd, self.mappings))
        except (ns.SyncError, OSError, ValueError):
            return None

    def _index_evidence(self):
        """Read only the per-thread index fields used to validate cached work."""
        try:
            state, history = self.profile / 'state_5.sqlite', self.profile / 'thread_history_1.sqlite'
            if any(p.is_symlink() or not p.is_file() for p in (state, history)):
                return {}
            with closing(sqlite3.connect(state.as_uri() + '?mode=ro', uri=True)) as db:
                rows = {r[0]: tuple(r[1:]) for r in db.execute('SELECT id, rollout_path, title, archived FROM threads')}
            with closing(sqlite3.connect(history.as_uri() + '?mode=ro', uri=True)) as db:
                projections = {r[0]: tuple(r[1:]) for r in db.execute(
                    'SELECT thread_id, next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state')}
            return {identity: (row, projections[identity]) for identity, row in rows.items() if identity in projections}
        except (sqlite3.Error, OSError):
            return {}

    def _cache_verified_sources(self, fingerprints):
        if not isinstance(fingerprints, dict):
            return
        evidence = self._index_evidence()
        for identity, fingerprint in fingerprints.items():
            if identity in self.verified and identity in evidence:
                self._verified_sources[identity] = (fingerprint, self.verified[identity], evidence[identity])

    def _cached_unchanged(self, incoming, local, names, kinds, skipped):
        """Reuse only verified histories whose files and per-thread index match."""
        evidence = self._index_evidence()
        title_data = ns._read_title_index(self.profile)
        local_names = ({row['id']: row['thread_name'] for row in ns._parse_title_index(title_data)}
                       if title_data is not None else {})
        unchanged, fingerprints = set(), {}
        for identity, entries in incoming.items():
            cached, current = self._verified_sources.get(identity), local.get(identity, [])
            expected_title = names.get(identity) if kinds[identity] == 'chat' else None
            if (identity in skipped or not cached or len(current) != 1 or current[0]['folder'] != 'sessions'
                    or self.verified.get(identity) != cached[1] or evidence.get(identity) != cached[2]
                    or cached[2][0][0] != str(current[0]['path']) or cached[2][0][2] != 0
                    or cached[1][1] != expected_title or (expected_title is not None and
                        local_names.get(identity) != expected_title)):
                continue
            try:
                if len(entries) > 1:
                    pages = {lineage._page_id(item['path'], identity) for item in entries}
                    if any(isinstance(item.get('history_base'), dict)
                           and item['history_base'].get('thread_id') in pages
                           for other, others in incoming.items() if other != identity for item in others):
                        continue
                paths = sorted((item['path'] for item in entries), key=str)
                target = current[0]['path']
                all_paths = paths + [target]
                before = [(p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in all_paths]
                fingerprint = (ns._digest(paths[0]) if len(paths) == 1 else
                               [[p.relative_to(self.source).as_posix(), ns._digest(p)] for p in paths])
                target_digest = ns._digest(target)
                after = [(p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in all_paths]
                if before == after and fingerprint == cached[0] and target_digest == cached[1][0]:
                    unchanged.add(identity)
                    fingerprints[identity] = fingerprint
            except (OSError, ValueError, ns.SyncError):
                continue
        return unchanged, fingerprints

    def _write_path_report(self, reports):
        """Keep missing local paths private and available across restarts."""
        path = self.backup_root / 'missing-paths.json'
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ns.SyncError('Missing-path report destination unavailable')
        previous = {}
        if path.is_file():
            info = path.stat()
            if (info.st_size > 16 * 1024 * 1024 or
                    (os.name != 'nt' and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077))):
                raise ns.SyncError('Missing-path report is not private or is too large')
            value = json.loads(path.read_bytes())
            if not isinstance(value, dict) or value.get('version') != 1 or not isinstance(value.get('chats'), dict):
                raise ns.SyncError('Invalid missing-path report')
            previous = value['chats']
        previous.update(reports)
        value = {'version': 1, 'chats': previous}
        encoded = json.dumps(value, ensure_ascii=False).encode('utf-8')
        if len(encoded) > 16 * 1024 * 1024:
            raise ns.SyncError('Missing-path report too large')
        fd, temporary = tempfile.mkstemp(prefix='.cosyncx-paths-', dir=self.backup_root)
        temporary = Path(temporary)
        try:
            if hasattr(os, 'fchmod'):
                os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(encoded)
                stream.flush(); os.fsync(stream.fileno())
            if path.is_symlink():
                raise ns.SyncError('Missing-path report destination changed')
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        self.path_warning_count = len(previous)

    def _read_journal(self):
        path = self.backup_root / 'direct-restore-pending.json'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
            return None
        record = json.loads(path.read_bytes())
        if (not isinstance(record, dict) or record.get('version') != 1
                or not isinstance(record.get('authentication'), str)
                or not hmac.compare_digest(record['authentication'], ns._manifest_tag(record, self.key))):
            return None
        return record

    def _write_journal(self, record):
        path = self.backup_root / 'direct-restore-pending.json'
        if path.is_symlink() or not path.is_file():
            raise ns.SyncError('Direct restore journal unavailable')
        record['authentication'] = ns._manifest_tag(record, self.key)
        fd, temporary = tempfile.mkstemp(prefix='.cosyncx-direct-journal-', dir=self.backup_root)
        temporary = Path(temporary)
        try:
            if hasattr(os, 'fchmod'):
                os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                json.dump(record, stream, sort_keys=True)
                stream.flush(); os.fsync(stream.fileno())
            if path.is_symlink() or not path.is_file():
                raise ns.SyncError('Direct restore journal unavailable')
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _read_carry(self):
        path = self.backup_root / 'direct-restore-carry.json'
        if not path.exists() and not path.is_symlink():
            return []
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
            raise ns.SyncError('Direct restore carry is unavailable')
        value = json.loads(path.read_bytes())
        if (not isinstance(value, dict) or value.get('version') != 1 or not isinstance(value.get('events'), list)
                or not isinstance(value.get('authentication'), str)
                or not hmac.compare_digest(value['authentication'], ns._manifest_tag(value, self.key))):
            raise ns.SyncError('Direct restore carry is invalid')
        events = value['events']
        if (any(not isinstance(event, dict) or set(event) != {'id', 'count'}
                or not isinstance(event['id'], str) or len(event['id']) != 32
                or type(event['count']) is not int or event['count'] < 1 for event in events)
                or len({event['id'] for event in events}) != len(events)):
            raise ns.SyncError('Direct restore carry events are invalid')
        return events

    def _write_carry(self, events):
        path = self.backup_root / 'direct-restore-carry.json'
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ns.SyncError('Direct restore carry is unavailable')
        value = {'version': 1, 'events': events}
        value['authentication'] = ns._manifest_tag(value, self.key)
        fd, temporary = tempfile.mkstemp(prefix='.cosyncx-direct-carry-', dir=self.backup_root)
        temporary = Path(temporary)
        try:
            if hasattr(os, 'fchmod'):
                os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                json.dump(value, stream, sort_keys=True)
                stream.flush(); os.fsync(stream.fileno())
            if path.is_symlink() or (path.exists() and not path.is_file()):
                raise ns.SyncError('Direct restore carry is unavailable')
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _database_state(self, names):
        result = {}
        for name in sorted(names):
            for path in (self.profile / name, self.profile / (name + '-wal')):
                if path.is_symlink():
                    raise ns.SyncError('Direct database is a link')
                if not path.exists():
                    result[path.name] = None
                    continue
                info = path.stat()
                result[path.name] = (None if path.name.endswith('-wal') and info.st_size == 0 else
                                     [info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns])
        return result

    def _title_state(self):
        raw = ns._read_title_index(self.profile)
        return hashlib.sha256(raw).hexdigest() if raw is not None else None

    def _retire_unchanged_indexing(self, record, journal):
        """Retire a verification-only pass without touching chats.

        Only entries that are still present and indexed can be reused. A pass
        that changed a chat or its relationships always follows normal recovery.
        """
        operations, verified = record.get('operations'), record.get('verified')
        snapshot = record.get('index_snapshot')
        if (record.get('phase') not in ('indexing', 'indexed') or record.get('threads') != [] or record.get('paths') != []
                or not isinstance(operations, list) or not operations
                or any(not isinstance(op, dict) or op.get('before') is None or op.get('before') != op.get('after')
                       for op in operations)
                or not isinstance(verified, dict) or not isinstance(snapshot, dict)
                or snapshot.get('edges') != []):
            return False
        manifest = self.backup_root / 'latest' / 'manifest.json'
        if (manifest.is_symlink() or not manifest.is_file()
                or ns._digest(manifest) != record.get('backup_manifest_digest')):
            return False
        try:
            if any((self.profile / name).is_symlink() or not (self.profile / name).is_file()
                   for name in ('state_5.sqlite', 'thread_history_1.sqlite')):
                return False
            local = ns._chat_catalog(self.profile, include_unsupported=True)
            busy = locking._busy_thread_ids(self.profile)
            title_data = ns._read_title_index(self.profile)
            titles = ({row['id']: row['thread_name'] for row in ns._parse_title_index(title_data)}
                      if title_data is not None else {})
            with closing(sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
                rows = {row[0]: row[1:] for row in db.execute('SELECT id, rollout_path, title, archived FROM threads')}
            with closing(sqlite3.connect((self.profile / 'thread_history_1.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
                projected = {row[0] for row in db.execute('SELECT thread_id FROM thread_history_projection_state')}
            reusable = {}
            for identity, item in verified.items():
                entries, row = local.get(identity, []), rows.get(identity)
                if (identity in busy or identity not in projected or len(entries) != 1
                        or entries[0]['folder'] != 'sessions' or not isinstance(item, dict)
                        or not row or row[2] != 0 or row[0] != str(entries[0]['path'])
                        or (item.get('title') is not None and titles.get(identity) != item['title'])):
                    continue
                path = entries[0]['path']
                before = path.stat()
                digest = ns._digest(path)
                after = path.stat()
                if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                        == (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                        and digest == item.get('digest')):
                    reusable[identity] = (digest, item.get('title'))
            if self._read_journal() != record:
                return False
            journal.unlink()
            self.verified.update(reusable)
            self._cache_verified_sources(record.get('source_fingerprints'))
            return True
        except (OSError, ValueError, TypeError, sqlite3.Error, ns.SyncError):
            return False

    def _resume_indexing(self, record, incoming, kinds, names, edges, graph, db_names):
        queue, offset = record.get('index_queue'), record.get('next_index')
        expected, verified = record.get('expected'), record.get('verified')
        if (not isinstance(queue, list) or type(offset) is not int or not 0 <= offset <= len(queue)
                or not isinstance(expected, dict) or set(expected) != set(incoming) or not isinstance(verified, dict)
                or any(not isinstance(item, list) or len(item) != 2 or item[0] not in incoming
                       or item[1] != (names.get(item[0]) if kinds[item[0]] == 'chat' else None) for item in queue)
                or len({item[0] for item in queue}) != len(queue)
                or self._database_state(db_names) != record.get('database_state')
                or self._title_state() != record.get('local_title_state')):
            return None
        catalog = ns._chat_catalog(self.profile, include_unsupported=True)
        for identity, digest in expected.items():
            current = catalog.get(identity, [])
            if (len(current) != 1 or current[0]['folder'] != 'sessions'
                    or not isinstance(digest, str) or ns._digest(current[0]['path']) != digest):
                return None
        for index in range(offset, len(queue)):
            identity, title = queue[index]
            if not self._write_allowed():
                return None
            indexer._index_profile(self.profile, identity, self.codex, title=title, allow_title_change=True,
                                   ignore_busy=self._ignored_busy)
            current = ns._chat_catalog(self.profile, include_unsupported=True).get(identity, [])
            if len(current) != 1 or current[0]['folder'] != 'sessions':
                raise ns.SyncError('Direct indexed history unavailable')
            digest = ns._digest(current[0]['path'])
            expected[identity] = digest
            verified[identity] = {'digest': digest, 'title': title}
            self.verified[identity] = (digest, title)
            record['next_index'] = index + 1
            record['database_state'] = self._database_state(db_names)
            record['local_title_state'] = self._title_state()
            self._write_journal(record)
        if set(verified) != set(incoming):
            return None
        if edges:
            if not self._write_allowed():
                return None
            with tempfile.TemporaryDirectory(prefix='cosyncx-active-graph-') as tmp:
                filtered = Path(tmp) / 'graph.json'
                filtered.write_text(json.dumps({**graph, 'edges': edges}), encoding='utf-8')
                mock_graph_restore._apply_graph(self.profile, self.backup_root, filtered, use_existing_backup=True,
                                                ignore_busy=self._ignored_busy)
        record['phase'] = 'indexed'
        self._write_journal(record)
        return len(record['threads'])

    def acknowledge(self, event):
        record = self._read_journal()
        carry = self._read_carry()
        if record is not None and record.get('phase') != 'indexed':
            raise ns.SyncError('Direct restore acknowledgement mismatch')
        changed = (len(record.get('threads', [])) if record else 0) + sum(item['count'] for item in carry)
        event_id = carry[0]['id'] if carry else (record.get('event_id') if record else None)
        if ((record is None and not carry) or
                (changed == 0 and event is not None) or
                (changed > 0 and (not isinstance(event, dict) or event.get('id') != event_id
                 or event.get('indexed') is not True or event.get('changed_chats') != changed))):
            raise ns.SyncError('Direct restore acknowledgement mismatch')
        if record is not None:
            (self.backup_root / 'direct-restore-pending.json').unlink()
        if carry:
            (self.backup_root / 'direct-restore-carry.json').unlink()
        self.pending_result = None

    def _recover_interrupted(self, record) -> bool:
        """Undo file writes only if SQLite, titles and touched histories are unchanged."""
        if record.get('phase') != 'writing' or not isinstance(record.get('operations'), list):
            return False
        operations = record['operations']
        if not operations or any(not isinstance(item, dict) or set(item) != {'id', 'path', 'before', 'after'}
                                 for item in operations):
            return False
        latest = self.backup_root / 'latest'
        manifest_path = latest / 'manifest.json'
        if (latest.is_symlink() or not latest.is_dir() or manifest_path.is_symlink() or not manifest_path.is_file()
                or ns._digest(manifest_path) != record.get('backup_manifest_digest')):
            return False
        manifest = json.loads(manifest_path.read_bytes())
        if not isinstance(manifest, dict) or manifest.get('version') != 1 or not isinstance(manifest.get('files'), dict):
            return False
        entries = manifest['files']
        checked, names = [], set()
        for item in operations:
            name, before, after = item['path'], item['before'], item['after']
            if (not graph_schema._uuid(item['id']) or not isinstance(name, str) or not name.startswith('sessions/')
                    or '\\' in name or any(part in ('', '.', '..') for part in name.split('/'))
                    or name in names or not isinstance(after, str) or len(after) != 64
                    or (before is not None and (not isinstance(before, str) or len(before) != 64))):
                return False
            names.add(name)
            path = self.profile
            for part in name.split('/'):
                path /= part
                if path.is_symlink():
                    return False
            if path.exists() and not path.is_file():
                return False
            current = ns._digest(path) if path.is_file() else None
            if current not in (before, after):
                return False
            saved = None
            if before is not None:
                try:
                    saved = backup._resolve_file(latest, name)
                except backup.BackupError:
                    return False
                if entries.get(name) != before or ns._digest(saved) != before:
                    return False
            checked.append((path, saved, before, current))
        db_names = {name for name in entries if name.startswith(('state_', 'thread_history_')) and name.endswith('.sqlite')}
        live_names = {p.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite') for p in self.profile.glob(pattern)}
        if 'state_5.sqlite' not in db_names or db_names != live_names:
            return False
        title = self.profile / 'session_index.jsonl'
        expected_title = entries.get('session_index.jsonl')
        if (title.is_symlink() or (title.exists() and not title.is_file())
                or title.is_file() != (expected_title is not None)
                or (title.is_file() and ns._digest(title) != expected_title)):
            return False
        with locking._mock_thread_lock(self.profile, operations[0]['id']):
            if not quiescence.is_quiet():
                return False
            with tempfile.TemporaryDirectory(prefix='cosyncx-db-check-') as tmp:
                for name in db_names:
                    live = self.profile / name
                    try:
                        saved = backup._resolve_file(latest, name)
                        if live.is_symlink() or not live.is_file() or ns._digest(saved) != entries[name]:
                            return False
                        with sqlite3.connect(live.as_uri() + '?mode=ro', uri=True) as src:
                            with sqlite3.connect(Path(tmp) / name) as dst:
                                src.backup(dst)
                        if ns._digest(Path(tmp) / name) != entries[name]:
                            return False
                    except (backup.BackupError, sqlite3.Error, OSError):
                        return False
            for path, saved, before, current in checked:
                if path.is_symlink() or (ns._digest(path) if path.is_file() else None) != current:
                    return False
                if saved is None and current is not None:
                    path.unlink()
                elif saved is not None and current != before:
                    temporary = path.with_name('.cosyncx-recover-' + os.urandom(12).hex())
                    try:
                        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                        with os.fdopen(fd, 'wb') as out, saved.open('rb') as src:
                            shutil.copyfileobj(src, out, 1024 * 1024)
                            out.flush(); os.fsync(out.fileno())
                        if ns._digest(temporary) != before or ns._digest(saved) != before:
                            raise ns.SyncError('Direct backup changed during recovery')
                        os.replace(temporary, path)
                    finally:
                        temporary.unlink(missing_ok=True)
            (self.backup_root / 'direct-restore-pending.json').unlink()
        return True

    def _receive(self, on_restore=None) -> int | None:
        journal = self.backup_root / 'direct-restore-pending.json'
        orphan_journal = self.backup_root / 'direct-orphan-pending.json'
        if orphan_journal.exists() or orphan_journal.is_symlink():
            if journal.exists() or journal.is_symlink():
                return None
            try:
                if not self._write_allowed() or not orphan_restore.resume(self, on_restore=on_restore):
                    return None
            except (ns.SyncError, locking.MockChatBusy, backup.BackupError, OSError, sqlite3.Error, ValueError):
                return None
        self._read_carry()
        prior = self._read_journal() if journal.exists() or journal.is_symlink() else None
        if (journal.exists() or journal.is_symlink()) and not prior:
            return None
        if prior is not None and prior.get('phase') in ('indexing', 'indexed'):
            db_names = {name.removesuffix('-wal') for name in prior.get('database_state', {})}
            if (prior['phase'] == 'indexed' or self._database_state(db_names) != prior.get('database_state')
                    or self._title_state() != prior.get('local_title_state')):
                if self._retire_unchanged_indexing(prior, journal):
                    prior = None
        try:
            mock_journal.ensure_clear(self.backup_root)
        except ns.SyncError:
            return None
        if prior is not None and prior.get('phase') == 'writing':
            try:
                if not quiescence.is_quiet():
                    return None
            except quiescence.QuiescenceError:
                return None
            if not self._recover_interrupted(prior):
                return None
            prior = None
        if prior is not None and prior.get('phase') not in ('indexing', 'indexed'):
            return None
        try:
            graph_bytes, graph, graph_conflicts = graph_schema.read_shared_graph(self.graph)
        except (graph_schema.GraphError, OSError, ValueError, TypeError):
            return None
        if (not isinstance(graph, dict) or graph.get('format') != 'cosyncx-active-graph-v1'
                or not isinstance(graph.get('edges'), list) or type(graph.get('omitted_missing')) is not int
                or graph['omitted_missing'] != 0 or type(graph.get('omitted_archived')) is not int
                or graph['omitted_archived'] < 0):
            return None
        title_bytes = ns._read_title_index(self.titles)
        if title_bytes is None:
            return None
        names = {entry['id']: entry['thread_name'] for entry in ns._parse_title_index(title_bytes)}
        incoming = ns._chat_catalog(self.source, include_unsupported=True)
        all_incoming = dict(incoming)
        local = ns._chat_catalog(self.profile, include_unsupported=True)
        source_kinds = {identity: cosync_receive_plan.source_kind(entries) for identity, entries in incoming.items()}
        # A locally archived conversation stays archived even when the source
        # has an active copy. It and its dependent children must not block
        # unrelated active chats from being imported.
        skipped = set(self.excluded_thread_ids & set(incoming))
        for identity in incoming:
            current = local.get(identity, [])
            folders = {item['folder'] for item in current}
            if 'archived_sessions' in folders:
                if folders != {'archived_sessions'}:
                    return None
                skipped.add(identity)
        # Codex can mark a thread archived in SQLite before or without moving
        # its history file. Such a thread and its children must stay local.
        database = self.profile / 'state_5.sqlite'
        indexed_ids = set()
        indexed_paths = {}
        if database.is_symlink():
            return None
        if database.is_file() and set(incoming) - skipped:
            wal, shm = self.profile / 'state_5.sqlite-wal', self.profile / 'state_5.sqlite-shm'
            if wal.is_symlink() or shm.is_symlink() or (wal.exists() and not shm.exists()):
                return None
            try:
                with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as db:
                    rows = db.execute('SELECT id, archived FROM threads').fetchall()
                    indexed_ids = {row[0] for row in rows}
                    archived_ids = {row[0] for row in rows if row[1] == 1}
                    columns = {row[1] for row in db.execute('PRAGMA table_info(threads)')}
                    if {'rollout_path', 'cwd'} <= columns:
                        indexed_paths = {row[0]: (row[1], row[2]) for row in
                                         db.execute('SELECT id, rollout_path, cwd FROM threads WHERE archived=0')}
            except sqlite3.Error:
                return None
            skipped.update(set(incoming) & archived_ids)
        self.unresolved_archived_count = sum(identity in skipped and identity not in self.excluded_thread_ids
                                             and source_kinds[identity] == 'chat' for identity in incoming)
        skipped.update(graph_conflicts & set(incoming))
        # An indexed thread without its local history cannot safely be treated
        # as a new thread. Defer it and its descendants, not the whole batch.
        orphan_ids = (set(incoming) - skipped) & indexed_ids - set(local)
        # A stale foreign path can be repaired only for an unambiguous child
        # history whose parent is present and neither writer is active.
        if prior is None and orphan_ids and indexed_paths:
            try:
                busy = locking._busy_thread_ids(self.profile) if self._write_allowed() else set(orphan_ids)
                for identity in sorted(orphan_ids):
                    entries = incoming[identity]
                    parent = entries[0].get('parent_thread_id') if len(entries) == 1 else None
                    if (source_kinds[identity] != 'subagent' or identity in busy or parent in busy
                            or parent not in incoming or parent not in local or identity not in indexed_paths):
                        continue
                    old_path, old_cwd = indexed_paths[identity]
                    try:
                        if orphan_restore.prepare(self, identity, entries, old_path, old_cwd):
                            if not orphan_restore.resume(self, on_restore=on_restore):
                                return None
                            local = ns._chat_catalog(self.profile, include_unsupported=True)
                    except (ns.SyncError, locking.MockChatBusy, backup.BackupError, OSError, sqlite3.Error, ValueError):
                        if orphan_journal.exists() or orphan_journal.is_symlink():
                            return None
                orphan_ids = (set(incoming) - skipped) & indexed_ids - set(local)
            except (ns.SyncError, locking.MockChatBusy, OSError, ValueError):
                pass
        skipped.update(orphan_ids)
        self.pending_busy_ids = frozenset(orphan_ids | (graph_conflicts & set(incoming)))
        self._ignored_busy = frozenset()
        if set(incoming) - skipped:
            try:
                if self._write_allowed():
                    self._ignored_busy = frozenset(locking._busy_thread_ids(self.profile))
                    self.pending_busy_ids |= frozenset((set(incoming) - skipped) & self._ignored_busy)
                    skipped.update(self.pending_busy_ids)
            except (quiescence.QuiescenceError, locking.MockChatBusy, FileNotFoundError):
                return None
        cached_ids, cached_fingerprints = (self._cached_unchanged(incoming, local, names, source_kinds, skipped)
                                            if self.prefer_more_messages and prior is None and self._verified_sources
                                            else (set(), {}))
        if self.prefer_more_messages and prior is None:
            with tempfile.TemporaryDirectory(prefix='cosyncx-message-policy-') as tmp:
                for number, (identity, entries) in enumerate(incoming.items()):
                    current = local.get(identity, [])
                    if identity in skipped or identity in cached_ids:
                        continue
                    if current and (len(current) != 1 or current[0]['folder'] != 'sessions'):
                        skipped.add(identity)
                        self.pending_busy_ids |= frozenset((identity,))
                        continue
                    try:
                        fallback = self._existing_workspace(current)
                        if not current and len(entries) == 1:
                            cwd = entries[0].get('cwd')
                            if not isinstance(cwd, str): raise ns.SyncError('Working directory unavailable')
                            lineage._map_cwd(cwd, self.mappings)
                            continue
                        stage = Path(tmp) / ('source-' + str(number))
                        if len(entries) == 1:
                            lineage.prepare_lineage(self.source, stage, identity, self.mappings, fallback_root=fallback,
                                                    preserve_unmapped_history=True)
                        else:
                            if source_kinds[identity] != 'chat': raise ns.SyncError('Unsupported multi-page history')
                            pages = {lineage._page_id(item['path'], identity) for item in entries}
                            if any(isinstance(item.get('history_base'), dict)
                                   and item['history_base'].get('thread_id') in pages
                                   for other, others in all_incoming.items() if other != identity for item in others):
                                raise ns.SyncError('Referenced multi-page history')
                            indexer._prepare_flattened(self.source, stage, identity, self.mappings, fallback_root=fallback,
                                                       preserve_unmapped_history=True)
                        staged = ns._chat_catalog(stage, include_unsupported=True).get(identity, [])
                        if len(staged) != 1: raise ns.SyncError('History comparison unavailable')
                        if not current:
                            continue
                        snapshot = Path(tmp) / ('local-' + str(number) + '.jsonl')
                        ns._copy_stable_chat(current[0]['path'], snapshot)
                        choice = message_policy.choose(staged[0]['path'], snapshot,
                                                       self.mappings[0][1], self.mappings[0][1],
                                                       subagent=source_kinds[identity] == 'subagent')
                        if choice in ('TARGET_MORE', 'TIE'):
                            skipped.add(identity)
                            self.pending_busy_ids |= frozenset((identity,))
                    except (ns.SyncError, OSError, ValueError):
                        skipped.add(identity)
                        self.pending_busy_ids |= frozenset((identity,))
        for identity, entries in incoming.items():
            if source_kinds[identity] == 'subagent' and entries[0].get('parent_thread_id') not in incoming:
                skipped.add(identity)
        while True:
            dependent = {identity for identity, entries in incoming.items()
                         if identity not in skipped and any(item.get('parent_thread_id') in skipped for item in entries)}
            if not dependent:
                break
            skipped.update(dependent)
        incoming = {identity: entries for identity, entries in incoming.items() if identity not in skipped}
        kinds = {identity: source_kinds[identity] for identity in incoming}
        if any(kind not in ('chat', 'subagent') for kind in kinds.values()):
            return None
        edges, seen_children = [], set()
        for edge in graph['edges']:
            if (not isinstance(edge, dict) or set(edge) != {'parent', 'child', 'status'}
                    or not graph_schema._uuid(edge['parent']) or not graph_schema._uuid(edge['child'])
                    or edge['parent'] == edge['child'] or edge['child'] in seen_children
                    or edge['status'] not in ('open', 'closed')):
                return None
            seen_children.add(edge['child'])
            if edge['parent'] in skipped or edge['child'] in skipped:
                continue
            if edge['parent'] not in incoming or edge['child'] not in incoming:
                return None
            edges.append(edge)
        for identity, entries in incoming.items():
            if (kinds[identity] == 'subagent' and
                    kinds.get(entries[0].get('parent_thread_id')) not in ('chat', 'subagent')):
                return None
            if kinds[identity] == 'chat' and not names.get(identity):
                return None
        depth = {}
        for identity in incoming:
            chain, visited, node = [], set(), identity
            while node not in depth:
                if node in visited:
                    raise ns.SyncError('Cyclic active subagent relationship')
                visited.add(node)
                chain.append(node)
                if kinds[node] == 'chat':
                    break
                node = incoming[node][0].get('parent_thread_id')
            base = depth[node] if node in depth else -1
            for item in reversed(chain):
                base += 1
                depth[item] = base
        fingerprints = {}
        for identity, entries in incoming.items():
            if identity in cached_fingerprints:
                fingerprints[identity] = cached_fingerprints[identity]
            elif len(entries) == 1:
                fingerprints[identity] = ns._digest(entries[0]['path'])
            else:
                fingerprints[identity] = [[item['path'].relative_to(self.source).as_posix(), ns._digest(item['path'])]
                                          for item in sorted(entries, key=lambda item: str(item['path']))]
        self._last_fingerprints = fingerprints
        if prior is not None:
            latest = self.backup_root / 'latest'
            manifest = latest / 'manifest.json'
            if (latest.is_symlink() or not latest.is_dir() or manifest.is_symlink() or not manifest.is_file()
                    or ns._digest(manifest) != prior.get('backup_manifest_digest')):
                return None
            saved = json.loads(manifest.read_bytes())
            if not isinstance(saved, dict) or saved.get('version') != 1 or not isinstance(saved.get('files'), dict):
                return None
            for name, digest in saved['files'].items():
                try:
                    saved_file = backup._resolve_file(latest, name)
                except backup.BackupError:
                    return None
                if not isinstance(digest, str) or ns._digest(saved_file) != digest:
                    return None
            source_same = (len(fingerprints) == len(incoming) and prior.get('source_fingerprints') == fingerprints
                           and prior.get('title_digest') == hashlib.sha256(title_bytes).hexdigest()
                           and prior.get('graph_digest') == hashlib.sha256(graph_bytes).hexdigest())
            if (not isinstance(prior.get('verified'), dict)
                    or not isinstance(prior.get('threads'), list)
                    or not isinstance(prior.get('event_id'), str) or len(prior['event_id']) != 32):
                return None
            if self.excluded_thread_ids & set(prior['verified']):
                return None
            if not source_same:
                if prior.get('phase') == 'indexing':
                    snapshot = prior.get('index_snapshot')
                    if (not isinstance(snapshot, dict) or set(snapshot) != {'kinds', 'names', 'edges', 'graph'}
                            or not isinstance(snapshot['kinds'], dict) or not isinstance(snapshot['names'], dict)
                            or not isinstance(snapshot['edges'], list) or not isinstance(snapshot['graph'], dict)
                            or set(snapshot['kinds']) != set(prior.get('expected', {}))
                            or self.excluded_thread_ids & set(snapshot['kinds'])):
                        return None
                    # Do not unarchive a thread just because the interrupted
                    # operation was started before its local state changed.
                    if any(any(item['folder'] != 'sessions' for item in local.get(identity, []))
                           for identity in snapshot['kinds']):
                        return None
                    db_names = {name for name in saved['files'] if name.startswith(('state_', 'thread_history_'))
                                and name.endswith('.sqlite')}
                    current_db_names = {path.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite')
                                        for path in self.profile.glob(pattern)}
                    if not db_names or db_names != current_db_names:
                        return None
                    if self._resume_indexing(prior, snapshot['kinds'], snapshot['kinds'], snapshot['names'],
                                             snapshot['edges'], snapshot['graph'], db_names) is None:
                        return None
                if (prior.get('phase') != 'indexed' or not isinstance(prior.get('source_fingerprints'), dict)
                        or set(prior['verified']) != set(prior['source_fingerprints'])
                        or not set(prior['threads']) <= set(prior['verified'])):
                    return None
                # Do not retire a confirmed receipt while Syncthing is still
                # exposing an incomplete or changing new history.
                try:
                    with tempfile.TemporaryDirectory(prefix='cosyncx-source-check-') as tmp:
                        for number, (identity, entries) in enumerate(sorted(incoming.items())):
                            paths = sorted(entries, key=lambda item: str(item['path']))
                            for index, item in enumerate(paths):
                                ns._copy_stable_chat(item['path'], Path(tmp) / f'{number}-{index}.jsonl')
                            actual = (ns._digest(paths[0]['path']) if len(paths) == 1 else
                                      [[item['path'].relative_to(self.source).as_posix(), ns._digest(item['path'])]
                                       for item in paths])
                            if actual != fingerprints.get(identity):
                                return None
                except (ns.SyncError, OSError, ValueError):
                    return None
                for identity, item in prior['verified'].items():
                    current = local.get(identity, [])
                    if (len(current) != 1 or current[0]['folder'] != 'sessions' or not isinstance(item, dict)
                            or item.get('digest') != ns._digest(current[0]['path'])):
                        return None
                    if not self._write_allowed():
                        return None
                    indexer._index_profile(self.profile, identity, self.codex, title=item.get('title'),
                                           ignore_busy=self._ignored_busy)
                    self.verified[identity] = (ns._digest(current[0]['path']), item.get('title'))
                events = self._read_carry()
                if prior['threads'] and all(event['id'] != prior['event_id'] for event in events):
                    events.append({'id': prior['event_id'], 'count': len(prior['threads'])})
                    self._write_carry(events)
                journal.unlink()
                return self._receive(on_restore=on_restore)
            if not set(prior['threads']) <= set(fingerprints):
                return None
            db_names = {name for name in saved['files'] if name.startswith(('state_', 'thread_history_'))
                        and name.endswith('.sqlite')}
            current_db_names = {path.name for pattern in ('state_*.sqlite', 'thread_history_*.sqlite')
                                for path in self.profile.glob(pattern)}
            if not db_names or db_names != current_db_names:
                return None
            if prior['phase'] == 'indexing':
                if on_restore is not None: on_restore()
                return self._resume_indexing(prior, incoming, kinds, names, edges, graph, db_names)
            if set(prior['verified']) != set(fingerprints):
                return None
            ordered = sorted(fingerprints, key=lambda identity: (depth[identity], identity))
            if on_restore is not None: on_restore()
            for identity in ordered:
                current = local.get(identity, [])
                item = prior['verified'].get(identity)
                if (len(current) != 1 or current[0]['folder'] != 'sessions' or not isinstance(item, dict)
                        or item.get('digest') != ns._digest(current[0]['path'])
                        or item.get('title') != (names.get(identity) if kinds.get(identity) == 'chat' else None)):
                    return None
                if not self._write_allowed():
                    return None
                indexer._index_profile(self.profile, identity, self.codex, title=item['title'],
                                       ignore_busy=self._ignored_busy)
                item['digest'] = ns._digest(current[0]['path'])
                self.verified[identity] = (item['digest'], item['title'])
            if edges:
                with sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
                    for edge in edges:
                        actual = db.execute('SELECT parent_thread_id, status FROM thread_spawn_edges WHERE child_thread_id=?',
                                            (edge['child'],)).fetchone()
                        if actual != (edge['parent'], edge['status']):
                            return None
            self._write_journal(prior)
            return len(prior['threads'])
        with tempfile.TemporaryDirectory(prefix='cosyncx-direct-prepare-') as temp:
            prepared = []
            recheck = []
            path_reports = {}
            local_titles_data = ns._read_title_index(self.profile)
            local_titles = ({entry['id']: entry['thread_name'] for entry in ns._parse_title_index(local_titles_data)}
                            if local_titles_data is not None else {})
            for number, (identity, entries) in enumerate(sorted(incoming.items())):
                if not entries or kinds[identity] not in ('chat', 'subagent'):
                    return None
                if identity in cached_ids:
                    continue
                stage = Path(temp).resolve() / str(number)
                fallback = (self._existing_workspace(local.get(identity, [])) if self.prefer_more_messages
                            else Path(self.mappings[0][1]))
                if len(entries) == 1:
                    lineage.prepare_lineage(self.source, stage, identity, self.mappings,
                                            fallback_root=fallback,
                                            preserve_unmapped_history=self.prefer_more_messages)
                else:
                    if kinds[identity] != 'chat':
                        return None
                    page_ids = {lineage._page_id(item['path'], identity) for item in entries}
                    for other, other_entries in all_incoming.items():
                        if other == identity:
                            continue
                        if any(isinstance(item.get('history_base'), dict) and
                               item['history_base'].get('thread_id') in page_ids for item in other_entries):
                            return None
                    indexer._prepare_flattened(self.source, stage, identity, self.mappings,
                                               fallback_root=fallback,
                                               preserve_unmapped_history=self.prefer_more_messages)
                report = stage / 'cosyncx-path-report.json'
                if report.is_file() and not report.is_symlink():
                    path_reports[identity] = json.loads(report.read_bytes())
                staged = ns._chat_catalog(stage, include_unsupported=True).get(identity, [])
                if len(staged) != 1:
                    return None
                current = local.get(identity, [])
                if current:
                    if len(current) != 1 or current[0]['folder'] != 'sessions':
                        return None
                    expected_title = names.get(identity) if kinds[identity] == 'chat' else None
                    target = self.mappings[0][1]
                    relation = ns._path_adjusted_relation(staged[0]['path'], current[0]['path'], target, target)
                    if relation not in ('EQUIVALENT_AFTER_PATH_MAPPING', 'LOCAL_EXTENDS_AFTER_PATH_MAPPING',
                                        'INCOMING_EXTENDS_AFTER_PATH_MAPPING', 'DIVERGED'):
                        return None
                    if self.prefer_more_messages:
                        choice = message_policy.choose(staged[0]['path'], current[0]['path'], target, target,
                                                       subagent=kinds[identity] == 'subagent')
                        if choice not in ('SOURCE_MORE', 'EQUAL'): return None
                        replacement = staged[0]['path'] if choice == 'SOURCE_MORE' else None
                    else:
                        replacement = staged[0]['path'] if relation in ('INCOMING_EXTENDS_AFTER_PATH_MAPPING', 'DIVERGED') else None
                    title_changed = expected_title is not None and local_titles.get(identity) != expected_title
                    current_digest = ns._digest(current[0]['path'])
                    if replacement is not None or title_changed or self.verified.get(identity) != (current_digest, expected_title):
                        recheck.append((identity, current[0]['path'], expected_title, current_digest, replacement, title_changed))
                    continue
                relative = staged[0]['path'].relative_to(stage)
                prepared.append((identity, staged[0]['path'], relative,
                                 names.get(identity) if kinds[identity] == 'chat' else None))
            graph_changed = set()
            if edges:
                try:
                    with sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
                        columns = {row[1] for row in db.execute('PRAGMA table_info(thread_spawn_edges)')}
                        if columns != {'parent_thread_id', 'child_thread_id', 'status'}:
                            return None
                        for edge in edges:
                            actual = db.execute('SELECT parent_thread_id, status FROM thread_spawn_edges WHERE child_thread_id=?',
                                                (edge['child'],)).fetchone()
                            if actual != (edge['parent'], edge['status']):
                                graph_changed.add(edge['parent'])
                except sqlite3.Error:
                    return None
            if not prepared and not recheck and not graph_changed:
                return 0
            if graph_changed and not prepared and not recheck:
                identity = sorted(graph_changed)[0]
                current = local.get(identity, [])
                if len(current) != 1 or current[0]['folder'] != 'sessions':
                    return None
                title = names.get(identity) if kinds[identity] == 'chat' else None
                recheck.append((identity, current[0]['path'], title, ns._digest(current[0]['path']), None, False))
            if on_restore is not None: on_restore()
            prepared.sort(key=lambda entry: (depth[entry[0]], entry[0]))
            db_paths = {p for pattern in ('state_*.sqlite', 'thread_history_*.sqlite')
                        for p in self.profile.glob(pattern)}
            if any(p.is_symlink() or not p.is_file() for p in db_paths):
                return None
            dbs = sorted(p.name for p in db_paths)
            if 'state_5.sqlite' not in dbs:
                return None
            if any(replacement is not None for _, _, _, _, replacement, _ in recheck) and 'thread_history_1.sqlite' not in dbs:
                return None
            try:
                with sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
                    existing_ids = {row[0] for row in db.execute('SELECT id FROM threads')}
            except sqlite3.Error:
                return None
            if any(identity in existing_ids for identity, _, _, _ in prepared):
                return None
            index = self.profile / 'session_index.jsonl'
            if index.is_symlink() or (index.exists() and not index.is_file()):
                return None
            selection = dbs + ([index.name] if index.is_file() else [])
            selection += [path.relative_to(self.profile).as_posix() for _, path, _, _, _, _ in recheck]
            lock_id = prepared[0][0] if prepared else (recheck[0][0] if recheck else sorted(graph_changed)[0])
            try:
                if not self._write_allowed():
                    return None
            except quiescence.QuiescenceError:
                return None
            with locking._mock_thread_lock(self.profile, lock_id, ignore_busy=self._ignored_busy):
                if not self._write_allowed():
                    return None
                current_ids = ns._chat_catalog(self.profile, include_unsupported=True)
                if any(identity in current_ids for identity, _, _, _ in prepared):
                    return None
                with sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
                    for identity, _, _, _ in prepared:
                        if db.execute('SELECT 1 FROM threads WHERE id=? LIMIT 1', (identity,)).fetchone():
                            return None
                    for identity, _, _, _, _, _ in recheck:
                        state = db.execute('SELECT archived FROM threads WHERE id=?', (identity,)).fetchone()
                        if state is not None and state != (0,):
                            return None
                def verify_backup(_stage, manifest):
                    for _, path, _, digest, _, _ in recheck:
                        relative = path.relative_to(self.profile).as_posix()
                        if ns._digest(path) != digest or manifest.get(relative) != digest:
                            raise ns.SyncError('Direct history changed during backup')
                latest = backup.replace_latest(self.profile, self.backup_root, selection, pre_commit=verify_backup)
                if not self._write_allowed():
                    return None
                if path_reports:
                    self._write_path_report(path_reports)
                changed = sorted(set([identity for identity, _, _, _ in prepared] +
                           [identity for identity, _, _, _, replacement, title_changed in recheck
                            if replacement is not None or title_changed]) | graph_changed)
                operations = [{'id': identity, 'path': relative.as_posix(), 'before': None, 'after': ns._digest(staged)}
                              for identity, staged, relative, _ in prepared]
                operations += [{'id': identity, 'path': path.relative_to(self.profile).as_posix(),
                                'before': digest, 'after': ns._digest(replacement) if replacement else digest}
                               for identity, path, _, digest, replacement, _ in recheck]
                record = {'version': 1, 'phase': 'writing', 'event_id': os.urandom(16).hex(),
                          'operations': operations,
                          'threads': changed,
                          'paths': ([relative.as_posix() for _, _, relative, _ in prepared] +
                                    [path.relative_to(self.profile).as_posix() for _, path, _, _, replacement, _ in recheck
                                     if replacement is not None]),
                          'backup_manifest_digest': ns._digest(latest / 'manifest.json'),
                          'source_fingerprints': fingerprints,
                          'title_digest': hashlib.sha256(title_bytes).hexdigest(),
                          'graph_digest': hashlib.sha256(graph_bytes).hexdigest()}
                record['authentication'] = ns._manifest_tag(record, self.key)
                fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    json.dump(record, stream, sort_keys=True)
                    stream.flush(); os.fsync(stream.fileno())
                for _, staged, relative, _ in prepared:
                    if not self._write_allowed():
                        raise ns.SyncError('Codex started during direct restore')
                    parent = self.profile
                    for part in relative.parts[:-1]:
                        parent = parent / part
                        if parent.is_symlink():
                            raise ns.SyncError('Direct destination contains a link')
                        parent.mkdir(mode=0o700, exist_ok=True)
                    target = self.profile / relative
                    if target.exists() or target.is_symlink():
                        raise ns.SyncError('Direct destination occupied')
                    temporary = parent / ('.cosyncx-' + os.urandom(12).hex())
                    try:
                        ns._copy_stable_chat(staged, temporary)
                        if not self._write_allowed():
                            raise ns.SyncError('Codex started during direct restore')
                        os.link(temporary, target, follow_symlinks=False)
                    finally:
                        temporary.unlink(missing_ok=True)
                for _, target, _, original_digest, replacement, _ in recheck:
                    if replacement is None:
                        continue
                    if not self._write_allowed():
                        raise ns.SyncError('Codex started during direct restore')
                    if target.is_symlink() or not target.is_file() or ns._digest(target) != original_digest:
                        raise ns.SyncError('Direct history changed after backup')
                    temporary = target.with_name('.cosyncx-' + os.urandom(12).hex())
                    try:
                        ns._copy_stable_chat(replacement, temporary)
                        if not self._write_allowed():
                            raise ns.SyncError('Codex started during direct restore')
                        os.replace(temporary, target)
                    finally:
                        temporary.unlink(missing_ok=True)
            for identity, _, _, _, replacement, _ in recheck:
                if replacement is not None:
                    if not self._write_allowed():
                        raise ns.SyncError('Codex started during direct restore')
                    indexer._reset_profile_projection(self.profile, self.backup_root, identity,
                                                      ignore_busy=self._ignored_busy)
            verified = {identity: {'digest': item[0], 'title': item[1]}
                        for identity, item in self.verified.items() if identity in incoming}
            to_index = [(identity, title) for identity, _, _, title in prepared]
            to_index += [(identity, title) for identity, _, title, _, _, _ in recheck]
            to_index.sort(key=lambda item: (depth[item[0]], item[0]))
            current = ns._chat_catalog(self.profile, include_unsupported=True)
            expected = {}
            for identity in incoming:
                items = current.get(identity, [])
                if len(items) != 1 or items[0]['folder'] != 'sessions':
                    raise ns.SyncError('Direct history unavailable after writing')
                expected[identity] = ns._digest(items[0]['path'])
            record.update({'verified': verified, 'expected': expected, 'index_queue': [list(item) for item in to_index],
                           'next_index': 0, 'database_state': self._database_state(dbs),
                           'local_title_state': self._title_state(), 'phase': 'indexing',
                           'index_snapshot': {'kinds': kinds, 'names': {identity: names[identity] for identity in incoming
                                                                        if identity in names},
                                              'edges': edges, 'graph': graph}})
            self._write_journal(record)
            return self._resume_indexing(record, incoming, kinds, names, edges, graph, set(dbs))

    def poll(self, chat_reader, *, title_status_reader=None, graph_status_reader=None, on_check=None, on_restore=None, now=None):
        if self.pending_result is not None:
            return self.pending_result
        if title_status_reader is None or graph_status_reader is None:
            return {'state': 'WAITING', 'mock_only': False}
        def observe():
            values = [chat_reader(), title_status_reader(), graph_status_reader()]
            return {value[0]: value for value in values}
        observations = observe()
        revision = self.gate.observe(observations, now=now)
        if revision is None:
            if self.gate.is_current(now=now) and (self.backup_root / 'direct-restore-pending.json').exists():
                self.gate.request_retry()
                return {'state': 'PENDING', 'mock_only': False}
            if self.gate.is_current(now=now) and self.pending_busy_ids:
                current_time = time.monotonic() if now is None else now
                if self._last_pending_retry is None or current_time - self._last_pending_retry >= 60:
                    self._last_pending_retry = current_time
                    self.gate.request_retry()
                return {'state': 'PENDING', 'mock_only': False}
            state = ('PENDING' if self.unresolved_archived_count else 'CURRENT') if self.gate.is_current(now=now) else 'WAITING'
            return {'state': state, 'mock_only': False}
        if on_check is not None:
            on_check()
        try:
            count = self._receive(on_restore=on_restore)
        except locking.MockChatBusy:
            return {'state': 'PENDING', 'mock_only': False}
        if count is None:
            return {'state': 'PENDING', 'mock_only': False}
        record = self._read_journal()
        carry = self._read_carry()
        total = count + sum(item['count'] for item in carry)
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': total}
        try:
            event = self.gate.complete(revision, result, after=observe())
        except ValueError:
            # A new transfer can arrive while this batch is being indexed.
            # Its revision still needs another pass, but an authenticated,
            # fully indexed journal already proves this batch was applied.
            self._cache_verified_sources(self._last_fingerprints)
            self.gate.request_retry()
            if (record is not None and record.get('phase') == 'indexed'
                    and isinstance(record.get('threads'), list)
                    and count == len(record['threads'])):
                event_id = carry[0]['id'] if carry else record.get('event_id')
                if total and isinstance(event_id, str) and len(event_id) == 32:
                    event = {'id': event_id, 'state': 'RESTORED', 'indexed': True,
                             'changed_chats': total, 'mock_only': False}
                    self.pending_result = {'state': 'PENDING', 'mock_only': False,
                                           'restore_event': event, 'acknowledge_required': True}
                    return self.pending_result
            return {'state': 'PENDING', 'mock_only': False}
        self._cache_verified_sources(self._last_fingerprints)
        if event is not None:
            event['mock_only'] = False
            if not carry and (not record or record.get('phase') != 'indexed'):
                raise ns.SyncError('Direct restore confirmation unavailable')
            event['id'] = carry[0]['id'] if carry else record['event_id']
        state = 'PENDING' if self.unresolved_archived_count or self.pending_busy_ids else 'COMPLETE'
        if self.pending_busy_ids:
            self._last_pending_retry = time.monotonic() if now is None else now
        result = {'state': state, 'mock_only': False, 'restore_event': event}
        if carry or (record and record.get('phase') == 'indexed'):
            result['acknowledge_required'] = True
            self.pending_result = result
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description='Experimental direct-folder restore watcher')
    for name in ('source', 'titles', 'graph', 'profile', 'backup-root', 'key-file', 'codex',
                 'syncthing-config', 'status-file'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--map-root', required=True, nargs=2, action='append', metavar=('SOURCE', 'TARGET'))
    for name in ('url', 'sessions-folder-id', 'titles-folder-id', 'graph-folder-id', 'peer-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--exclude-thread-id', action='append', default=[],
                        help='keep this local chat and its dependent sessions unchanged')
    parser.add_argument('--allow-active', action='store_true',
                        help='allow experimental restores while Cursor or Codex is running')
    parser.add_argument('--allow-sendreceive-source', action='store_true',
                        help='accept explicitly configured send-and-receive Syncthing source folders')
    parser.add_argument('--prefer-more-messages', action='store_true',
                        help='select the chat history with more visible messages')
    parser.add_argument('--experimental', action='store_true', required=True,
                        help='enable the incomplete prototype on a disposable profile')
    args = parser.parse_args(argv)
    try:
        key = ns.read_key(args.key_file, args.source)
        client = cosync_engine.ExistingEngine(args.url, args.syncthing_config, config_xml=True)
        receiver = DirectReceiver(source=args.source, titles=args.titles, graph=args.graph, profile=args.profile,
            backup_root=args.backup_root, key=key, mappings=args.map_root, codex=args.codex,
            folder_ids=(args.sessions_folder_id, args.titles_folder_id, args.graph_folder_id), peer_id=args.peer_id,
            excluded_thread_ids=args.exclude_thread_id, allow_active=args.allow_active,
            prefer_more_messages=args.prefer_more_messages)
        sources = [cosync_syncthing_watch.SyncthingSource(client, folder, args.peer_id, path,
                    allow_sendreceive=args.allow_sendreceive_source) for folder, path in (
            (args.sessions_folder_id, args.source / 'sessions'), (args.titles_folder_id, args.titles),
            (args.graph_folder_id, args.graph))]
        supervisor = cosync_restore_supervisor.RestoreSupervisor(receiver, sources[0].read, args.status_file,
            mock_only=False, title_reader=sources[1].read, graph_reader=sources[2].read)
        supervisor.run(threading.Event(), args.interval)
        return 0
    except KeyboardInterrupt:
        return 0
    except (ns.SyncError, cosync_engine.EngineError, cosync_syncthing_watch.WatchError,
            OSError, ValueError, TypeError):
        print('CoSyncX: direct restore watcher unavailable', file=sys.stderr)
        return 1
