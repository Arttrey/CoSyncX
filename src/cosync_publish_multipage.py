"""Replace one received multi-page history with a verified local history."""
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import uuid

import cosync_lineage as lineage
import nexus_sync as ns


def replace_pages(profile: Path, exchange: Path, state_dir: Path, thread_id: str, paused) -> int:
    """Run or resume one replacement. Caller must keep the Syncthing folder paused."""
    profile, exchange, state_dir = map(Path, (profile, exchange, state_dir))
    try:
        if str(uuid.UUID(thread_id)) != thread_id: raise ValueError()
    except (ValueError, TypeError, AttributeError) as exc:
        raise ns.SyncError('Invalid thread identifier') from exc
    if (not all(p.is_absolute() and p.is_dir() and not p.is_symlink() for p in (profile, exchange, state_dir))
            or profile == exchange or profile in exchange.parents or exchange in profile.parents
            or exchange == state_dir or exchange in state_dir.parents or state_dir in exchange.parents
            or not callable(paused) or not paused()):
        raise ns.SyncError('Publication paths or paused state are invalid')
    if os.name != 'nt' and (state_dir.stat().st_uid != os.getuid() or stat.S_IMODE(state_dir.stat().st_mode) & 0o077):
        raise ns.SyncError('Publication state directory must be private')
    journal, latest = state_dir / 'pending.json', state_dir / 'latest'
    if journal.is_symlink() or latest.is_symlink(): raise ns.SyncError('Invalid publication state')
    if not journal.exists():
        local = ns._chat_catalog(profile, include_unsupported=True).get(thread_id, [])
        all_remote = ns._chat_catalog(exchange, include_unsupported=True)
        remote = all_remote.get(thread_id, [])
        if len(local) != 1 or len(remote) < 2 or any(x['folder'] != 'sessions' for x in local + remote):
            raise ns.SyncError('Expected one local history and multiple received pages')
        page_ids = {lineage._page_id(x['path'], thread_id) for x in remote}
        for other, entries in all_remote.items():
            if other != thread_id and any(isinstance(item.get('history_base'), dict)
                    and item['history_base'].get('thread_id') in page_ids for item in entries):
                raise ns.SyncError('Another history refers to an old page')
        root = [x['path'] for x in remote if x['path'].name == local[0]['path'].name]
        if len(root) != 1: raise ns.SyncError('A matching root page is required')
        if latest.exists(): raise ns.SyncError('An earlier backup already exists')
        stage = Path(tempfile.mkdtemp(prefix='.pending-', dir=state_dir))
        try:
            snapshot = stage / 'new.jsonl'
            ns._copy_stable_chat(local[0]['path'], snapshot)
            pages = []
            for number, item in enumerate(remote):
                original = item['path']; saved = stage / f'page-{number}.jsonl'
                ns._copy_stable_chat(original, saved)
                digest = ns._digest(saved)
                if ns._digest(original) != digest: raise ns.SyncError('Received page changed')
                pages.append({'path': original.relative_to(exchange).as_posix(), 'saved': saved.name, 'digest': digest})
            data = {'version': 1, 'id': thread_id, 'root': root[0].relative_to(exchange).as_posix(),
                    'new_digest': ns._digest(snapshot), 'pages': pages}
            (stage / 'manifest.json').write_text(json.dumps(data), encoding='utf-8')
            os.rename(stage, latest)
            fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump(data, stream); stream.flush(); os.fsync(stream.fileno())
        finally:
            if stage.exists(): shutil.rmtree(stage)
    data = json.loads(journal.read_text())
    if (not isinstance(data, dict) or data.get('version') != 1 or data.get('id') != thread_id
            or not isinstance(data.get('pages'), list) or len(data['pages']) < 2
            or json.loads((latest / 'manifest.json').read_text()) != data):
        raise ns.SyncError('Invalid publication journal')
    snapshot = latest / 'new.jsonl'
    if snapshot.is_symlink() or not snapshot.is_file() or ns._digest(snapshot) != data.get('new_digest'):
        raise ns.SyncError('Publication snapshot changed')
    all_remote = ns._chat_catalog(exchange, include_unsupported=True)
    old_page_ids = {lineage._page_id(exchange / Path(entry['path']), thread_id) for entry in data['pages']
                    if isinstance(entry, dict) and isinstance(entry.get('path'), str)}
    for other, entries in all_remote.items():
        if other != thread_id and any(isinstance(item.get('history_base'), dict)
                and item['history_base'].get('thread_id') in old_page_ids for item in entries):
            raise ns.SyncError('Another history refers to an old page')
    paths = []
    for entry in data['pages']:
        if not isinstance(entry, dict) or set(entry) != {'path', 'saved', 'digest'}:
            raise ns.SyncError('Invalid saved page')
        rel = Path(entry['path'])
        if rel.is_absolute() or not rel.parts or rel.parts[0] != 'sessions' or '..' in rel.parts:
            raise ns.SyncError('Invalid received page path')
        path = exchange / rel
        if path.is_symlink() or any((exchange / Path(*rel.parts[:n])).is_symlink() for n in range(1, len(rel.parts))):
            raise ns.SyncError('Received page contains a link')
        saved = latest / entry['saved']
        if Path(entry['saved']).name != entry['saved'] or saved.is_symlink() or ns._digest(saved) != entry['digest']:
            raise ns.SyncError('Saved page changed')
        current = ns._digest(path) if path.is_file() else None
        allowed = (entry['digest'], data['new_digest']) if entry['path'] == data['root'] else (entry['digest'], None)
        if current not in allowed: raise ns.SyncError('Received page changed during publication')
        paths.append((path, entry, current))
    roots = [x for x in paths if x[1]['path'] == data['root']]
    if len(roots) != 1: raise ns.SyncError('Publication root is invalid')
    if not paused(): raise ns.SyncError('Syncthing folder must stay paused')
    root, entry, current = roots[0]
    if current != data['new_digest']:
        temp = state_dir / ('.replacement-' + os.urandom(12).hex())
        try:
            ns._copy_stable_chat(snapshot, temp)
            if not paused() or ns._digest(root) != entry['digest']: raise ns.SyncError('Publication changed')
            os.replace(temp, root)
        finally:
            temp.unlink(missing_ok=True)
    for path, entry, current in paths:
        if path == root or current is None: continue
        if not paused() or ns._digest(path) != entry['digest']: raise ns.SyncError('Publication changed')
        path.unlink()
    journal.unlink()
    return len(paths)
