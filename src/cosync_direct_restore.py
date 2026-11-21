"""Guarded direct-folder receiver for active chats.

Archived IDs, graphs and unavailable paths are left pending. Existing active
versions are backed up before replacement. No profile write is attempted
while Codex or Cursor runs.
"""
import argparse
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

import cosync_auto_gate
import cosync_engine
import cosync_graph_snapshot as graph_schema
import cosync_lineage as lineage
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
                 peer_id: str, quiet_seconds: float = 5):
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
        self.codex = Path(codex).resolve(strict=True)
        if not self.codex.is_file():
            raise ns.SyncError('Codex executable unavailable')
        self.gate = cosync_auto_gate.MultiFolderGate(folder_ids=folder_ids, peer_id=peer_id,
                                                     quiet_seconds=quiet_seconds)
        self.verified = {}
        self.pending_result = None

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

    def acknowledge(self, event):
        record = self._read_journal()
        changed = len(record.get('threads', [])) if record else -1
        if (not record or record.get('phase') != 'indexed' or
                (changed == 0 and event is not None) or
                (changed > 0 and (not isinstance(event, dict) or event.get('id') != record.get('event_id')
                 or event.get('indexed') is not True or event.get('changed_chats') != changed))):
            raise ns.SyncError('Direct restore acknowledgement mismatch')
        (self.backup_root / 'direct-restore-pending.json').unlink()
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

    def _receive(self) -> int | None:
        journal = self.backup_root / 'direct-restore-pending.json'
        prior = self._read_journal() if journal.exists() or journal.is_symlink() else None
        if (journal.exists() or journal.is_symlink()) and not prior:
            return None
        try:
            mock_journal.ensure_clear(self.backup_root)
        except ns.SyncError:
            return None
        try:
            if not quiescence.is_quiet():
                return None
        except quiescence.QuiescenceError:
            return None
        if prior is not None and prior.get('phase') == 'writing':
            if not self._recover_interrupted(prior):
                return None
            prior = None
        if prior is not None and prior.get('phase') != 'indexed':
            return None
        graph_file = self.graph / 'graph.json'
        if graph_file.is_symlink() or not graph_file.is_file() or graph_file.stat().st_size > 32 * 1024 * 1024:
            return None
        graph_bytes = graph_file.read_bytes()
        if ns._digest(graph_file) != hashlib.sha256(graph_bytes).hexdigest():
            return None
        graph = json.loads(graph_bytes)
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
        # A locally archived conversation stays archived even when the source
        # has an active copy. It and its dependent children must not block
        # unrelated active chats from being imported.
        skipped = set()
        for identity in incoming:
            current = local.get(identity, [])
            folders = {item['folder'] for item in current}
            if 'archived_sessions' in folders:
                if folders != {'archived_sessions'}:
                    return None
                skipped.add(identity)
        source_kinds = {identity: cosync_receive_plan.source_kind(entries) for identity, entries in incoming.items()}
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
            if len(entries) == 1:
                fingerprints[identity] = ns._digest(entries[0]['path'])
            else:
                fingerprints[identity] = [[item['path'].relative_to(self.source).as_posix(), ns._digest(item['path'])]
                                          for item in sorted(entries, key=lambda item: str(item['path']))]
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
            if (len(fingerprints) != len(incoming) or prior.get('source_fingerprints') != fingerprints
                    or prior.get('title_digest') != hashlib.sha256(title_bytes).hexdigest()
                    or prior.get('graph_digest') != hashlib.sha256(graph_bytes).hexdigest()
                    or not isinstance(prior.get('verified'), dict)
                    or set(prior['verified']) != set(fingerprints)
                    or not isinstance(prior.get('threads'), list)
                    or not set(prior['threads']) <= set(fingerprints)
                    or not isinstance(prior.get('event_id'), str) or len(prior['event_id']) != 32):
                return None
            ordered = sorted(fingerprints, key=lambda identity: (depth[identity], identity))
            for identity in ordered:
                current = local.get(identity, [])
                item = prior['verified'].get(identity)
                if (len(current) != 1 or current[0]['folder'] != 'sessions' or not isinstance(item, dict)
                        or item.get('digest') != ns._digest(current[0]['path'])
                        or item.get('title') != (names.get(identity) if kinds.get(identity) == 'chat' else None)):
                    return None
                if not quiescence.is_quiet():
                    return None
                indexer._index_profile(self.profile, identity, self.codex, title=item['title'])
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
            local_titles_data = ns._read_title_index(self.profile)
            local_titles = ({entry['id']: entry['thread_name'] for entry in ns._parse_title_index(local_titles_data)}
                            if local_titles_data is not None else {})
            for number, (identity, entries) in enumerate(sorted(incoming.items())):
                if not entries or kinds[identity] not in ('chat', 'subagent'):
                    return None
                stage = Path(temp).resolve() / str(number)
                if len(entries) == 1:
                    lineage.prepare_lineage(self.source, stage, identity, self.mappings)
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
                    indexer._prepare_flattened(self.source, stage, identity, self.mappings)
                if (stage / 'cosyncx-path-report.json').exists():
                    return None
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
            with locking._mock_thread_lock(self.profile, lock_id):
                if not quiescence.is_quiet():
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
                        os.link(temporary, target, follow_symlinks=False)
                    finally:
                        temporary.unlink(missing_ok=True)
                for _, target, _, original_digest, replacement, _ in recheck:
                    if replacement is None:
                        continue
                    if target.is_symlink() or not target.is_file() or ns._digest(target) != original_digest:
                        raise ns.SyncError('Direct history changed after backup')
                    temporary = target.with_name('.cosyncx-' + os.urandom(12).hex())
                    try:
                        ns._copy_stable_chat(replacement, temporary)
                        os.replace(temporary, target)
                    finally:
                        temporary.unlink(missing_ok=True)
            for identity, _, _, _, replacement, _ in recheck:
                if replacement is not None:
                    if not quiescence.is_quiet():
                        raise ns.SyncError('Codex started during direct restore')
                    indexer._reset_profile_projection(self.profile, self.backup_root, identity)
            verified = {identity: {'digest': item[0], 'title': item[1]}
                        for identity, item in self.verified.items() if identity in incoming}
            to_index = [(identity, title) for identity, _, _, title in prepared]
            to_index += [(identity, title) for identity, _, title, _, _, _ in recheck]
            to_index.sort(key=lambda item: (depth[item[0]], item[0]))
            for identity, title in to_index:
                if not quiescence.is_quiet():
                    raise ns.SyncError('Codex started during direct restore')
                indexer._index_profile(self.profile, identity, self.codex, title=title, allow_title_change=True)
                current = ns._chat_catalog(self.profile, include_unsupported=True).get(identity, [])
                if len(current) != 1:
                    raise ns.SyncError('Direct indexed history unavailable')
                self.verified[identity] = (ns._digest(current[0]['path']), title)
                verified[identity] = {'digest': self.verified[identity][0], 'title': title}
            if edges:
                if not quiescence.is_quiet():
                    raise ns.SyncError('Codex started during direct restore')
                filtered = Path(temp).resolve() / 'filtered-graph.json'
                filtered.write_text(json.dumps({**graph, 'edges': edges}), encoding='utf-8')
                mock_graph_restore._apply_graph(self.profile, self.backup_root, filtered, use_existing_backup=True)
            record['verified'] = verified
            record['phase'] = 'indexed'
            self._write_journal(record)
            return len(changed)

    def poll(self, chat_reader, *, title_status_reader=None, graph_status_reader=None, on_restore=None, now=None):
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
            return {'state': 'CURRENT' if self.gate.is_current(now=now) else 'WAITING', 'mock_only': False}
        if on_restore is not None:
            on_restore()
        count = self._receive()
        if count is None:
            return {'state': 'PENDING', 'mock_only': False}
        record = self._read_journal()
        result = {'state': 'RESTORED', 'indexed': True, 'changed_chats': count}
        try:
            event = self.gate.complete(revision, result, after=observe())
        except ValueError:
            return {'state': 'PENDING', 'mock_only': False}
        if event is not None:
            event['mock_only'] = False
            if not record or record.get('phase') != 'indexed':
                raise ns.SyncError('Direct restore confirmation unavailable')
            event['id'] = record['event_id']
        result = {'state': 'COMPLETE', 'mock_only': False, 'restore_event': event}
        if record and record.get('phase') == 'indexed':
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
    parser.add_argument('--experimental', action='store_true', required=True,
                        help='enable the incomplete prototype on a disposable profile')
    args = parser.parse_args(argv)
    try:
        key = ns.read_key(args.key_file, args.source)
        client = cosync_engine.ExistingEngine(args.url, args.syncthing_config, config_xml=True)
        receiver = DirectReceiver(source=args.source, titles=args.titles, graph=args.graph, profile=args.profile,
            backup_root=args.backup_root, key=key, mappings=args.map_root, codex=args.codex,
            folder_ids=(args.sessions_folder_id, args.titles_folder_id, args.graph_folder_id), peer_id=args.peer_id)
        sources = [cosync_syncthing_watch.SyncthingSource(client, folder, args.peer_id, path) for folder, path in (
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
