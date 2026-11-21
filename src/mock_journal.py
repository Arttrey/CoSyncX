"""Keep an interrupted mock transaction from losing its only backup."""
import nexus_sync as ns


PENDING = ('mock-title-pending.json', 'mock-update-pending.json', 'mock-archive-pending.json', 'mock-child-pending.json',
           'mock-group-pending.json')


def ensure_clear(backup_root, *, except_name=None):
    if except_name is not None and except_name not in PENDING:
        raise ns.SyncError('Unknown mock transaction')
    if any(name != except_name and ((backup_root / name).exists() or (backup_root / name).is_symlink())
           for name in PENDING):
        raise ns.SyncError('Mock transaction needs recovery before another backup')
