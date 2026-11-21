"""Prepare one comprehensive backup of an archived mock thread and descendants."""
from pathlib import Path

import cosync_archive_plan
import cosync_graph_snapshot as graph
import cosync_restore_backup as backup
import mock_chat_sync as mock
import mock_journal
import nexus_sync as ns


def prepare(profile: Path, shared: Path, backup_root: Path, key: bytes, roots: list[str],
            *, extra_threads: list[str] | None = None) -> dict:
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock backup directories')
    if not roots:
        raise ns.SyncError('No mock archive roots')
    if extra_threads is None:
        extra_threads = []
    if (not isinstance(extra_threads, list) or any(not graph._uuid(x) for x in extra_threads)
            or len(set(extra_threads)) != len(extra_threads)):
        raise ns.SyncError('Invalid additional mock threads')
    mock_journal.ensure_clear(backup_root)
    with mock._mock_thread_lock(profile, roots[0]):
        impact = cosync_archive_plan.archive_impact(profile, roots)
        extra = set(extra_threads) - impact['threads']
        catalog = ns._chat_catalog(profile, include_unsupported=True)
        extra_files = {identity: sorted(item['path'].relative_to(profile).as_posix() for item in catalog.get(identity, []))
                       for identity in extra}
        databases = sorted(set(profile.glob('state_*.sqlite')) | set(profile.glob('thread_history_*.sqlite')))
        if not databases or any(p.is_symlink() or not p.is_file() for p in databases):
            raise ns.SyncError('Mock state databases unavailable')
        if profile / 'state_5.sqlite' not in databases:
            raise ns.SyncError('Mock state database unavailable')
        title = profile / 'session_index.jsonl'
        if title.is_symlink() or (title.exists() and not title.is_file()):
            raise ns.SyncError('Mock title index unavailable')
        title_present = title.is_file()
        history_names = sorted(set(impact['files']) | {name for names in extra_files.values() for name in names})
        names = history_names + [p.name for p in databases] + ([title.name] if title_present else [])
        def signatures():
            result = {}
            for database in databases:
                for path in (database, database.with_name(database.name + '-wal')):
                    if path.is_symlink():
                        raise ns.SyncError('Mock database path is a link')
                    if path.exists():
                        s = path.stat()
                        # SQLite may create or remove an empty WAL even during
                        # a read-only backup; it contains no committed pages.
                        result[path.name] = (None if path.name.endswith('-wal') and s.st_size == 0
                                             else (s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns))
                    else:
                        result[path.name] = None
            return result
        before = signatures()
        def verify(_stage, manifest):
            current = cosync_archive_plan.archive_impact(profile, roots)
            current_catalog = ns._chat_catalog(profile, include_unsupported=True)
            current_extra = {identity: sorted(item['path'].relative_to(profile).as_posix()
                                             for item in current_catalog.get(identity, [])) for identity in extra}
            if (current != impact or signatures() != before or title.is_symlink()
                    or title.is_file() != title_present or current_extra != extra_files):
                raise ns.SyncError('Mock archive changed during backup')
            for name in history_names + ([title.name] if title_present else []):
                source = backup._resolve_file(profile, name)
                if ns._digest(source) != manifest.get(name):
                    raise ns.SyncError('Mock history changed during backup')
        backup.replace_latest(profile, backup_root, names, pre_commit=verify)
        return {'mock_only': True, 'threads': len(impact['threads']),
                'history_files': len(impact['files']), 'archived_threads': impact['archived'],
                'title_existed': title_present, 'extra_threads': len(extra),
                'absent_extra_threads': sum(not paths for paths in extra_files.values())}
