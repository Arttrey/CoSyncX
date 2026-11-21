"""Guarded direct-folder receiver for previously absent, single-page chats.

Existing or archived IDs, multi-page histories, graphs and unavailable paths
are left pending. No profile write is attempted while Codex or Cursor runs.
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

import cosync_auto_gate
import cosync_engine
import cosync_lineage as lineage
import cosync_quiescence as quiescence
import cosync_receive_plan
import cosync_restore_backup as backup
import cosync_restore_supervisor
import cosync_syncthing_watch
import mock_chat_sync as locking
import mock_paginated_import as indexer
import nexus_sync as ns


class DirectReceiver:
    def __init__(self, *, source: Path, titles: Path, graph: Path, profile: Path, backup_root: Path,
                 key: bytes, mappings: list[tuple[str, str]], codex: Path, folder_ids: tuple[str, str, str],
                 peer_id: str, quiet_seconds: float = 5):
        paths = [Path(x).resolve(strict=True) for x in (source, titles, graph, profile, backup_root)]
        if (any(not p.is_dir() for p in paths) or any(a == b or a in b.parents or b in a.parents
                for i, a in enumerate(paths) for b in paths[i + 1:]) or len(key) != 32 or not mappings):
            raise ns.SyncError('Invalid direct receiver configuration')
        self.source, self.titles, self.graph, self.profile, self.backup_root = paths
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
        self.pending_event = None

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
        if (not record or record.get('phase') != 'indexed' or not isinstance(event, dict)
                or event.get('id') != record.get('event_id') or event.get('indexed') is not True
                or event.get('changed_chats') != len(record.get('threads', []))):
            raise ns.SyncError('Direct restore acknowledgement mismatch')
        (self.backup_root / 'direct-restore-pending.json').unlink()
        self.pending_event = None

    def _receive(self) -> int | None:
        journal = self.backup_root / 'direct-restore-pending.json'
        prior = self._read_journal() if journal.exists() or journal.is_symlink() else None
        if (journal.exists() or journal.is_symlink()) and (not prior or prior.get('phase') != 'indexed'):
            return None
        try:
            if not quiescence.is_quiet():
                return None
        except quiescence.QuiescenceError:
            return None
        graph_file = self.graph / 'graph.json'
        if graph_file.is_symlink() or not graph_file.is_file() or graph_file.stat().st_size > 32 * 1024 * 1024:
            return None
        graph_bytes = graph_file.read_bytes()
        if ns._digest(graph_file) != hashlib.sha256(graph_bytes).hexdigest():
            return None
        graph = json.loads(graph_bytes)
        if (not isinstance(graph, dict) or graph.get('format') != 'cosyncx-active-graph-v1'
                or graph.get('edges') != [] or type(graph.get('omitted_missing')) is not int
                or graph['omitted_missing'] != 0 or type(graph.get('omitted_archived')) is not int
                or graph['omitted_archived'] < 0):
            return None
        title_bytes = ns._read_title_index(self.titles)
        if title_bytes is None:
            return None
        names = {entry['id']: entry['thread_name'] for entry in ns._parse_title_index(title_bytes)}
        incoming = ns._chat_catalog(self.source, include_unsupported=True)
        local = ns._chat_catalog(self.profile, include_unsupported=True)
        kinds = {identity: cosync_receive_plan.source_kind(entries) for identity, entries in incoming.items()}
        for identity, entries in incoming.items():
            if (kinds[identity] == 'subagent' and
                    kinds.get(entries[0].get('parent_thread_id')) != 'chat'):
                return None
            if kinds[identity] == 'chat' and not names.get(identity):
                return None
        fingerprints = {identity: ns._digest(entries[0]['path']) for identity, entries in incoming.items() if len(entries) == 1}
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
                    or not isinstance(prior.get('threads'), list) or not prior['threads']
                    or not set(prior['threads']) <= set(fingerprints)
                    or not isinstance(prior.get('event_id'), str) or len(prior['event_id']) != 32):
                return None
            ordered = sorted(fingerprints, key=lambda identity: (kinds[identity] != 'chat', identity))
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
            self._write_journal(prior)
            return len(prior['threads'])
        with tempfile.TemporaryDirectory(prefix='cosyncx-direct-prepare-') as temp:
            prepared = []
            for number, (identity, entries) in enumerate(sorted(incoming.items())):
                if len(entries) != 1 or kinds[identity] not in ('chat', 'subagent'):
                    return None
                stage = Path(temp).resolve() / str(number)
                lineage.prepare_lineage(self.source, stage, identity, self.mappings)
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
                    if self.verified.get(identity) != (ns._digest(current[0]['path']), expected_title):
                        return None
                    target = self.mappings[0][1]
                    if ns._path_adjusted_relation(staged[0]['path'], current[0]['path'], target, target) not in (
                            'EQUIVALENT_AFTER_PATH_MAPPING', 'LOCAL_EXTENDS_AFTER_PATH_MAPPING'):
                        return None
                    continue
                relative = staged[0]['path'].relative_to(stage)
                prepared.append((identity, staged[0]['path'], relative,
                                 names.get(identity) if kinds[identity] == 'chat' else None))
            if not prepared:
                return 0
            prepared.sort(key=lambda entry: (kinds[entry[0]] != 'chat', entry[0]))
            db_paths = {p for pattern in ('state_*.sqlite', 'thread_history_*.sqlite')
                        for p in self.profile.glob(pattern)}
            if any(p.is_symlink() or not p.is_file() for p in db_paths):
                return None
            dbs = sorted(p.name for p in db_paths)
            if 'state_5.sqlite' not in dbs:
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
            with locking._mock_thread_lock(self.profile, prepared[0][0]):
                if not quiescence.is_quiet():
                    return None
                current_ids = ns._chat_catalog(self.profile, include_unsupported=True)
                if any(identity in current_ids for identity, _, _, _ in prepared):
                    return None
                with sqlite3.connect((self.profile / 'state_5.sqlite').as_uri() + '?mode=ro', uri=True) as db:
                    for identity, _, _, _ in prepared:
                        if db.execute('SELECT 1 FROM threads WHERE id=? LIMIT 1', (identity,)).fetchone():
                            return None
                latest = backup.replace_latest(self.profile, self.backup_root, selection)
                record = {'version': 1, 'phase': 'writing', 'event_id': os.urandom(16).hex(),
                          'threads': [identity for identity, _, _, _ in prepared],
                          'paths': [relative.as_posix() for _, _, relative, _ in prepared],
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
            verified = {identity: {'digest': item[0], 'title': item[1]}
                        for identity, item in self.verified.items() if identity in incoming}
            for identity, _, _, title in prepared:
                if not quiescence.is_quiet():
                    raise ns.SyncError('Codex started during direct restore')
                indexer._index_profile(self.profile, identity, self.codex, title=title)
                current = ns._chat_catalog(self.profile, include_unsupported=True).get(identity, [])
                if len(current) != 1:
                    raise ns.SyncError('Direct indexed history unavailable')
                self.verified[identity] = (ns._digest(current[0]['path']), title)
                verified[identity] = {'digest': self.verified[identity][0], 'title': title}
            record['verified'] = verified
            record['phase'] = 'indexed'
            self._write_journal(record)
            return len(prepared)

    def poll(self, chat_reader, *, title_status_reader=None, graph_status_reader=None, on_restore=None, now=None):
        if self.pending_event is not None:
            return {'state': 'COMPLETE', 'mock_only': False, 'restore_event': self.pending_event}
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
            self.pending_event = event
        return {'state': 'COMPLETE', 'mock_only': False, 'restore_event': event}


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
