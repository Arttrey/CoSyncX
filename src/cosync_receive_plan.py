"""Read-only inventory for deciding which active chats need attention."""
from collections import Counter
import argparse
import json
from pathlib import Path
import sys

import cosync_lineage as lineage
import nexus_sync as ns


def source_kind(pages: list[dict]) -> str:
    kinds = []
    for item in pages:
        with item['path'].open('rb') as stream:
            record = json.loads(stream.readline(1024 * 1024 + 1))
        source = record['payload'].get('source')
        if isinstance(source, dict) and 'subagent' in source:
            kinds.append('subagent')
        elif source in ('vscode', 'cli', 'appServer'):
            kinds.append('chat')
        else:
            kinds.append('unknown')
    return kinds[0] if kinds and len(set(kinds)) == 1 else 'unknown'


def receive_plan(incoming: Path, profile: Path, mappings: list[tuple[str, str]]) -> dict:
    """Return counts only. Never writes or opens the live profile with Codex."""
    received = ns._chat_catalog(incoming, include_unsupported=True)
    existing = ns._chat_catalog(profile, include_unsupported=True)
    counts = Counter()
    kinds = {}
    for thread, pages in received.items():
        try:
            kinds[thread] = source_kind(pages)
        except (KeyError, ValueError, TypeError, OSError):
            kinds[thread] = 'unknown'
    for thread, pages in received.items():
        try:
            kind = kinds[thread]
            if kind == 'unknown':
                counts['unknown_source'] += 1
                continue
            if kind == 'subagent':
                parent = pages[0].get('parent_thread_id')
                counts['linked_subagent' if kinds.get(parent) == 'chat' else 'other_subagent'] += 1
                continue
            local = existing.get(thread, [])
            if local:
                if any(item['folder'] == 'archived_sessions' for item in local):
                    counts['archive_conflict'] += 1
                else:
                    counts['existing_active'] += 1
                continue
            if any(item['folder'] != 'sessions' or item['unsupported'] != 'PAGINATED' for item in pages):
                counts['unsupported'] += 1
                continue
            if ns._paged_lineage(pages, thread) != 'PRESENT_UNVERIFIED':
                counts['incomplete_lineage'] += 1
                continue
            try:
                for item in pages:
                    lineage._map_cwd(item['cwd'], mappings)
            except (ns.SyncError, OSError, TypeError, ValueError):
                counts['missing_project'] += 1
                continue
            counts['candidate_new'] += 1
        except (KeyError, ValueError, TypeError, OSError):
            counts['invalid_metadata'] += 1
    return {'format': 'cosyncx-receive-plan-v1', 'counts': dict(counts),
            'read_only': True, 'index_verified': False, 'transfer_verified': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Read-only inventory of incoming chat histories')
    parser.add_argument('--incoming', type=Path, required=True, help='Parent of the received sessions folder')
    parser.add_argument('--profile', type=Path, required=True, help='Local Codex home')
    parser.add_argument('--map-root', nargs=2, action='append', default=[], metavar=('SOURCE', 'TARGET'))
    args = parser.parse_args(argv)
    try:
        print(json.dumps(receive_plan(args.incoming, args.profile, args.map_root), sort_keys=True))
        return 0
    except (ns.SyncError, OSError, ValueError, TypeError):
        print('CoSyncX: could not inspect the selected folders', file=sys.stderr)
        return 1
