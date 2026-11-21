"""Compare the number of visible messages in two stable chat histories."""
from pathlib import Path

import mock_paginated_import as indexer
import nexus_sync as ns


def message_count(path: Path) -> int:
    entries = indexer._expected_visible_messages(path)
    return sum(count for tag, count in entries.items() if tag.startswith(('user:', 'assistant:')))


def choose(source: Path, target: Path, source_root: str, target_root: str) -> str:
    """Return the side with more messages, or defer a divergent tie."""
    source_messages = indexer._expected_visible_messages(source)
    target_messages = indexer._expected_visible_messages(target)
    left = sum(count for key, count in source_messages.items() if key.startswith(('user:', 'assistant:')))
    right = sum(count for key, count in target_messages.items() if key.startswith(('user:', 'assistant:')))
    if not left and not right:
        raise ns.SyncError('No comparable visible messages')
    if left > right: return 'SOURCE_MORE'
    if right > left: return 'TARGET_MORE'
    if source_messages == target_messages: return 'EQUAL'
    relation = ns._path_adjusted_relation(source, target, source_root, target_root)
    return 'EQUAL' if relation == 'EQUIVALENT_AFTER_PATH_MAPPING' else 'TIE'
