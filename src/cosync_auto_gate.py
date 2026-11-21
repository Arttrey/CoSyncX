"""Fail-closed change gate for a future verified restore receiver.

This module does not perform restoration. A caller must independently prove
that it actually restored and indexed the requested revision before calling
complete(). Transfer status alone can never create a reload event.
"""
from __future__ import annotations

import time
import uuid


class RestoreGate:
    def __init__(self, *, quiet_seconds: float = 5.0, completed_revision: int | None = None):
        if quiet_seconds < 0:
            raise ValueError('quiet_seconds must be nonnegative')
        self.quiet_seconds = quiet_seconds
        self.completed_revision = completed_revision
        self._revision = None
        self._since = None

    def observe(self, status: dict, *, connected: bool, now: float | None = None) -> int | None:
        now = time.monotonic() if now is None else now
        revision = status.get('sequence') if isinstance(status, dict) else None
        ready = (isinstance(status, dict) and connected is True and status.get('state') == 'idle'
                 and type(revision) is int and revision >= 0
                 and all(type(status.get(k)) is int and status[k] == 0
                         for k in ('needFiles', 'needDirectories', 'needSymlinks', 'needDeletes', 'errors')))
        if not ready:
            self._revision = self._since = None
            return None
        if revision != self._revision:
            self._revision, self._since = revision, now
            return None
        if revision == self.completed_revision or now - self._since < self.quiet_seconds:
            return None
        return revision

    def complete(self, revision: int, result: dict, *, after: dict, connected: bool) -> dict | None:
        """Accept only explicit successful indexing, never ordinary sync status."""
        stable = (isinstance(after, dict) and connected is True and after.get('state') == 'idle'
                  and after.get('sequence') == revision and type(after.get('sequence')) is int
                  and all(type(after.get(k)) is int and after[k] == 0
                          for k in ('needFiles', 'needDirectories', 'needSymlinks', 'needDeletes', 'errors')))
        if (type(revision) is not int or revision != self._revision or revision == self.completed_revision
                or not stable or not isinstance(result, dict)
                or result.get('state') != 'RESTORED' or result.get('indexed') is not True
                or type(result.get('changed_chats')) is not int or result['changed_chats'] < 0):
            raise ValueError('restore is not confirmed')
        self.completed_revision = revision
        if result['changed_chats'] == 0:
            return None
        return {'id': uuid.uuid4().hex, 'state': 'RESTORED', 'indexed': True,
                'changed_chats': result['changed_chats']}
