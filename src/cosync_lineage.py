"""Prepare paginated chat files in an isolated directory; never apply to Codex."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile
import uuid
import argparse
import sys

import nexus_sync as ns

_PAGE = re.compile(r'-([0-9a-fA-F-]{36})(?:_([0-9a-fA-F-]{36}))?\.jsonl$')


def _page_id(path: Path, thread: str) -> str:
    match = _PAGE.search(path.name)
    if not match or str(uuid.UUID(match.group(1))) != thread:
        raise ns.SyncError('Invalid paginated filename')
    return str(uuid.UUID(match.group(2))) if match.group(2) else thread


def _map_cwd(cwd: str, mappings: list[tuple[str, str]]) -> str:
    matches = []
    for source, target in mappings:
        try:
            mapped = ns.map_path(source, target, cwd)
            root = Path(target).resolve(strict=True)
            state, resolved = ns._resolve_local_project(Path(mapped), root, source)
            if state == 'AVAILABLE' and resolved is not None:
                matches.append(str(resolved))
        except (ns.SyncError, OSError, ValueError):
            continue
    if len(set(matches)) != 1:
        raise ns.SyncError('Working directory has no unique local mapping')
    return matches[0]


def prepare_lineage(incoming: Path, destination: Path, thread_id: str,
                    mappings: list[tuple[str, str]]) -> int:
    """Rewrite structured cwd fields and inherited byte offsets in a new copy.

    This is an experiment for isolated profiles only; it never updates the
    user's Codex profile, index or existing destination. It intentionally
    refuses missing pages, unknown paths and symlinks.
    """
    thread = str(uuid.UUID(thread_id))
    incoming = incoming.resolve(strict=True)
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if ('.codex' in {x.casefold() for x in destination.parts} or destination.exists()
            or destination.is_symlink() or incoming == parent or incoming in parent.parents):
        raise ns.SyncError('Destination must be a new isolated directory')
    if not mappings:
        raise ns.SyncError('No path mappings')
    catalog = ns._chat_catalog(incoming, include_unsupported=True)
    entries = catalog.get(thread, [])
    if not entries or ns._paged_lineage(entries, thread) != 'PRESENT_UNVERIFIED':
        raise ns.SyncError('Paginated lineage is incomplete or invalid')
    if len({x['folder'] for x in entries}) != 1:
        raise ns.SyncError('History pages span multiple folders')
    pages = {_page_id(x['path'], thread): x for x in entries}
    dependencies = {page: str(uuid.UUID(item['history_base']['thread_id']))
                    for page, item in pages.items() if item.get('history_base') is not None}
    boundaries = {page: set() for page in pages}
    for page, source in dependencies.items():
        boundaries[source].add(pages[page]['history_base']['end_byte_offset'])
    remaining, translated = set(pages), {}
    with tempfile.TemporaryDirectory(prefix='.cosync-lineage-', dir=parent) as temp:
        stage = Path(temp)
        while remaining:
            ready = sorted(x for x in remaining if x not in dependencies or dependencies[x] in translated)
            if not ready: raise ns.SyncError('Cyclic paginated lineage')
            for page in ready:
                item = pages[page]
                source = item['path']
                output = stage / source.relative_to(incoming)
                output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                before = source.stat()
                old_pos = new_pos = 0
                offsets = {}
                with source.open('rb') as inp, output.open('xb') as out:
                    for line in inp:
                        if not line.endswith(b'\n'): raise ns.SyncError('Incomplete paginated record')
                        original_length = len(line)
                        try: record = json.loads(line)
                        except (ValueError, UnicodeError) as exc: raise ns.SyncError('Invalid paginated record') from exc
                        if not isinstance(record, dict): raise ns.SyncError('Invalid paginated record')
                        payload = record.get('payload')
                        changed = False
                        if record.get('type') in ('session_meta', 'turn_context') and isinstance(payload, dict):
                            cwd = payload.get('cwd')
                            if cwd is not None:
                                if not isinstance(cwd, str): raise ns.SyncError('Invalid working directory')
                                payload['cwd'] = _map_cwd(cwd, mappings)
                                changed = True
                            if record.get('type') == 'session_meta' and page in dependencies:
                                base = payload.get('history_base')
                                old_offset = base.get('end_byte_offset') if isinstance(base, dict) else None
                                if old_offset not in translated[dependencies[page]]:
                                    raise ns.SyncError('Missing translated history boundary')
                                base['end_byte_offset'] = translated[dependencies[page]][old_offset]
                                changed = True
                        if changed:
                            line = (json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8')
                        out.write(line)
                        old_pos += original_length
                        new_pos += len(line)
                        if old_pos in boundaries[page]: offsets[old_pos] = new_pos
                after = source.stat()
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                    raise ns.SyncError('Source history changed during preparation')
                if set(offsets) != boundaries[page]: raise ns.SyncError('History boundary is not a record boundary')
                translated[page] = offsets
                remaining.remove(page)
        staged = ns._chat_catalog(stage, include_unsupported=True).get(thread, [])
        if len(staged) != len(entries) or ns._paged_lineage(staged, thread) != 'PRESENT_UNVERIFIED':
            raise ns.SyncError('Rewritten lineage did not validate')
        destination.mkdir(mode=0o700)
        for child in stage.iterdir(): os.replace(child, destination / child.name)
    return len(entries)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Prepare a paginated history in a new isolated directory')
    parser.add_argument('--incoming', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--thread-id', required=True)
    parser.add_argument('--map-root', nargs=2, action='append', required=True, metavar=('SOURCE', 'TARGET'))
    args = parser.parse_args(argv)
    try:
        pages = prepare_lineage(args.incoming, args.destination, args.thread_id, args.map_root)
        print(json.dumps({'pages': pages, 'applied': False, 'index_verified': False}))
        return 0
    except (ns.SyncError, OSError, ValueError) as exc:
        print('CoSyncX lineage:', str(exc), file=sys.stderr)
        return 1
