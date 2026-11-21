"""One backup and recoverable staged restores for several mock chat groups."""
import hmac
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import cosync_archive_plan
import cosync_graph_snapshot as graph
import cosync_receive_plan
import mock_archive_backup
import mock_archive_restore as archive
import mock_chat_sync as mock
import mock_child_batch
import mock_group_restore as group
import mock_journal
import nexus_sync as ns


def restore(source: Path, profile: Path, shared: Path, backup_root: Path, key: bytes,
            parents: list[str], children: dict[str, str], mappings: list[tuple[str, str]],
            executable: Path, *, titles: dict[str, str] | None = None,
            flatten_multi: bool = False, fallback_root: Path | None = None) -> dict:
    source = mock._mock_path(source)
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    executable = executable.resolve(strict=True)
    if (not source.is_dir() or not shared.is_dir() or not backup_root.is_dir() or not executable.is_file()
            or not isinstance(parents, list) or not parents or any(not graph._uuid(x) for x in parents)
            or len(set(parents)) != len(parents) or not isinstance(children, dict)
            or any(not graph._uuid(child) or parent not in parents or child in parents
                   for child, parent in children.items())
            or any(a == b or a in b.parents or b in a.parents for a, b in
                   ((source, profile), (source, shared), (source, backup_root), (profile, shared),
                    (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock multiple group request')
    if titles is None:
        titles = {}
    if (not isinstance(titles, dict) or not set(titles) <= set(parents)
            or any(not isinstance(value, str) or not value for value in titles.values())):
        raise ns.SyncError('Invalid mock group titles')
    parents, children, titles = sorted(parents), dict(children), dict(titles)
    mock_journal.ensure_clear(backup_root)
    step_root = backup_root / 'multigroup-steps'
    if step_root.exists() or step_root.is_symlink():
        raise ns.SyncError('Mock multiple group recovery directory exists')
    incoming = ns._chat_catalog(source, include_unsupported=True)
    for parent in parents:
        entries = incoming.get(parent, [])
        if not entries or cosync_receive_plan.source_kind(entries) != 'chat':
            raise ns.SyncError('Mock group parent unavailable')
    for identity, parent in children.items():
        entries = incoming.get(identity, [])
        if (len(entries) != 1 or entries[0].get('parent_thread_id') != parent
                or cosync_receive_plan.source_kind(entries) != 'subagent'):
            raise ns.SyncError('Mock group child relationship invalid')
    impacts = [cosync_archive_plan.archive_impact(profile, [parent]) for parent in parents]
    seen = set()
    for impact in impacts:
        if seen & impact['threads'] or impact['threads'] - impact['archived_ids']:
            raise ns.SyncError('Mock archived groups overlap or contain active descendants')
        if (impact['threads'] - set(parents)) & set(incoming):
            raise ns.SyncError('Mock incoming archived descendants need group support')
        seen.update(impact['threads'])
    mock_archive_backup.prepare(profile, shared, backup_root, key, parents, extra_threads=list(children))
    latest = backup_root / 'latest'
    data = {'version': 1, 'parents': parents, 'children': children, 'complete': False,
            'backup_manifest_digest': archive._stable_digest(latest / 'manifest.json')}
    group._verify_backup(latest, data['backup_manifest_digest'])
    data['authentication'] = ns._manifest_tag(data, key)
    journal = backup_root / 'mock-multigroup-pending.json'
    fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as out:
        json.dump(data, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    reports = {}
    try:
        step_root.mkdir(mode=0o700)
        for parent in parents:
            step = step_root / parent
            step.mkdir(mode=0o700)
            result = archive.restore(source, profile, shared, step, key, parent, mappings, executable,
                                     title=titles.get(parent), flatten_multi=flatten_multi,
                                     fallback_root=fallback_root, retain_journal=True, defer_rollback=True)
            if result.get('path_report') is not None:
                report_path = result['path_report']
                if report_path.is_symlink() or not report_path.is_file() or report_path.stat().st_size > 4 * 1024 * 1024:
                    raise ns.SyncError('Mock group path report unavailable')
                report = json.loads(report_path.read_bytes())
                if (not isinstance(report, dict) or report.get('thread') != parent
                        or not isinstance(report.get('authentication'), str)
                        or not hmac.compare_digest(report['authentication'], ns._manifest_tag(report, key))):
                    raise ns.SyncError('Mock group path report invalid')
                reports[parent] = report
        if children:
            step = step_root / 'children'
            step.mkdir(mode=0o700)
            first_parent = children[sorted(children)[0]]
            mock_child_batch.restore(source, profile, shared, step, key, first_parent, mappings, executable,
                                     children=sorted(children), parents=children, retain_journal=True,
                                     defer_rollback=True)
        group._verify_backup(latest, data['backup_manifest_digest'])
        if reports:
            pending_report = step_root / 'path-report.pending.json'
            payload = {'format': 'cosyncx-mock-multigroup-path-report-v1', 'chats': reports}
            payload['authentication'] = ns._manifest_tag(payload, key)
            fd = os.open(pending_report, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                json.dump(payload, stream, ensure_ascii=False, sort_keys=True)
                stream.flush(); os.fsync(stream.fileno())
            data['report_name'] = 'mock-multigroup-path-report-' + os.urandom(16).hex() + '.json'
            data['report_digest'] = archive._stable_digest(pending_report)
        data['complete'] = True
        archive._replace_journal(journal, data, key)
        recover(profile, shared, backup_root, key)
        return {'mock_only': True, 'indexed': True, 'changed_chats': len(parents) + len(children),
                'path_report': backup_root / data['report_name'] if reports else None}
    except Exception as exc:
        if not isinstance(exc, subprocess.SubprocessError):
            try:
                recover(profile, shared, backup_root, key)
            except Exception:
                pass
        raise


def recover(profile: Path, shared: Path, backup_root: Path, key: bytes) -> None:
    profile = mock._validate_profile(profile, key)
    shared, backup_root = mock._mock_path(shared), mock._mock_path(backup_root)
    if (not shared.is_dir() or not backup_root.is_dir() or any(a == b or a in b.parents or b in a.parents
            for a, b in ((profile, shared), (profile, backup_root), (shared, backup_root)))):
        raise ns.SyncError('Invalid mock multiple group recovery paths')
    journal = backup_root / 'mock-multigroup-pending.json'
    mock_journal.ensure_clear(backup_root, except_name=journal.name)
    if journal.is_symlink() or not journal.is_file() or journal.stat().st_size > 1024 * 1024:
        raise ns.SyncError('Mock multiple group journal unavailable')
    data = json.loads(journal.read_bytes())
    if (not isinstance(data, dict) or data.get('version') != 1 or type(data.get('complete')) is not bool
            or not isinstance(data.get('parents'), list) or not data['parents']
            or any(not graph._uuid(x) for x in data['parents']) or len(set(data['parents'])) != len(data['parents'])
            or not isinstance(data.get('children'), dict)
            or any(not graph._uuid(child) or parent not in data['parents'] or child in data['parents']
                   for child, parent in data['children'].items())):
        raise ns.SyncError('Invalid mock multiple group journal')
    tag = data.get('authentication')
    if not isinstance(tag, str) or not hmac.compare_digest(tag, ns._manifest_tag(data, key)):
        raise ns.SyncError('Mock multiple group journal is not authenticated')
    group._verify_backup(backup_root / 'latest', data.get('backup_manifest_digest'))
    step_root = backup_root / 'multigroup-steps'
    if step_root.is_symlink() or (step_root.exists() and not step_root.is_dir()):
        raise ns.SyncError('Mock multiple group recovery directory unsafe')
    steps = [step_root / parent for parent in data['parents']]
    child_step = step_root / 'children'
    for step in [*steps, child_step]:
        if step.is_symlink() or (step.exists() and not step.is_dir()):
            raise ns.SyncError('Mock multiple group step unsafe')
    if data['complete'] and ('report_name' in data or 'report_digest' in data):
        name, digest = data.get('report_name'), data.get('report_digest')
        if (not isinstance(name, str) or not re.fullmatch(r'mock-multigroup-path-report-[0-9a-f]{32}\.json', name)
                or not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)):
            raise ns.SyncError('Mock group report metadata invalid')
        pending, target = step_root / 'path-report.pending.json', backup_root / name
        if target.is_symlink() or (target.exists() and not target.is_file()):
            raise ns.SyncError('Mock group report destination unsafe')
        if target.is_file():
            if archive._stable_digest(target) != digest:
                raise ns.SyncError('Mock group report destination changed')
        else:
            if pending.is_symlink() or not pending.is_file() or archive._stable_digest(pending) != digest:
                raise ns.SyncError('Mock group report unavailable')
            os.link(pending, target, follow_symlinks=False)
    if not data['complete']:
        prior_reverted = False
        child_journal = child_step / 'mock-child-pending.json'
        if child_journal.exists() or child_journal.is_symlink():
            mock_child_batch.rollback(profile, shared, child_step, key)
            prior_reverted = True
        for parent, step in reversed(list(zip(data['parents'], steps))):
            pending = step / 'mock-archive-pending.json'
            if pending.exists() or pending.is_symlink():
                if prior_reverted:
                    group._refresh_parent(profile, step, parent, key)
                archive.rollback(profile, shared, step, key)
                prior_reverted = True
    if step_root.is_dir():
        shutil.rmtree(step_root)
    journal.unlink()
