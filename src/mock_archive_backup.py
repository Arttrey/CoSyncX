"""Prepare one comprehensive backup of an archived mock thread and descendants."""
from pathlib import Path

import cosync_archive_plan
import cosync_restore_backup as backup
import mock_chat_sync as mock
import nexus_sync as ns


def prepare(profile: Path, shared: Path, backup_root: Path, key: bytes, roots: list[str]) -> dict:
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir()
            or any(a == b or a in b.parents or b in a.parents
                   for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock backup directories')
    if not roots:
        raise ns.SyncError('No mock archive roots')
    with mock._mock_thread_lock(profile, roots[0]):
        impact = cosync_archive_plan.archive_impact(profile, roots)
        databases = sorted(set(profile.glob('state_*.sqlite')) | set(profile.glob('thread_history_*.sqlite')))
        if not databases or any(p.is_symlink() or not p.is_file() for p in databases):
            raise ns.SyncError('Mock state databases unavailable')
        if profile / 'state_5.sqlite' not in databases:
            raise ns.SyncError('Mock state database unavailable')
        title = profile / 'session_index.jsonl'
        if title.is_symlink() or (title.exists() and not title.is_file()):
            raise ns.SyncError('Mock title index unavailable')
        title_present = title.is_file()
        names = impact['files'] + [p.name for p in databases] + ([title.name] if title_present else [])
        def signatures():
            result = {}
            for database in databases:
                for path in (database, database.with_name(database.name + '-wal')):
                    if path.is_symlink():
                        raise ns.SyncError('Mock database path is a link')
                    if path.exists():
                        s = path.stat()
                        result[path.name] = (s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                    else:
                        result[path.name] = None
            return result
        before = signatures()
        def verify(_stage, manifest):
            current = cosync_archive_plan.archive_impact(profile, roots)
            if (current != impact or signatures() != before or title.is_symlink()
                    or title.is_file() != title_present):
                raise ns.SyncError('Mock archive changed during backup')
            for name in impact['files'] + ([title.name] if title_present else []):
                source = backup._resolve_file(profile, name)
                if ns._digest(source) != manifest.get(name):
                    raise ns.SyncError('Mock history changed during backup')
        backup.replace_latest(profile, backup_root, names, pre_commit=verify)
        return {'mock_only': True, 'threads': len(impact['threads']),
                'history_files': len(impact['files']), 'archived_threads': impact['archived'],
                'title_existed': title_present}
