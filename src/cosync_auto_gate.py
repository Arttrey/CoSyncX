"""Fail-closed change gate for a future verified restore receiver.

This module does not perform restoration. A caller must independently prove
that it actually restored and indexed the requested revision before calling
complete(). Transfer status alone can never create a reload event.
"""
from __future__ import annotations

import time
import uuid


class RestoreGate:
    def __init__(self, *, folder_id: str, peer_id: str, quiet_seconds: float = 5.0):
        if not isinstance(folder_id, str) or not folder_id or not isinstance(peer_id, str) or not peer_id:
            raise ValueError('folder_id and peer_id are required')
        if quiet_seconds < 0:
            raise ValueError('quiet_seconds must be nonnegative')
        self.quiet_seconds = quiet_seconds
        self.folder_id, self.peer_id = folder_id, peer_id
        # A bare sequence is not a durable identity: the peer can reset its
        # index. Always reconcile again after a disconnected period.
        self.completed_revision = None
        self._revision = None
        self._local_sequence = None
        self._eligible = False
        self._since = None

    def _ready_revision(self, folder_id: str, status: dict, connected: bool, remote_state: str) -> int | None:
        if folder_id != self.folder_id or not isinstance(status, dict):
            return None
        remote = status.get('remoteSequence')
        revision = remote.get(self.peer_id) if isinstance(remote, dict) else None
        ready = (connected is True and remote_state == 'valid' and status.get('state') == 'idle'
                 and type(revision) is int and revision >= 0
                 and type(status.get('sequence')) is int and status['sequence'] >= 0
                 and not status.get('error') and not status.get('invalid')
                 and all(type(status.get(k)) is int and status[k] == 0
                         for k in ('needFiles', 'needDirectories', 'needSymlinks', 'needDeletes', 'errors', 'pullErrors')))
        return revision if ready else None

    def observe(self, folder_id: str, status: dict, *, connected: bool, remote_state: str,
                now: float | None = None) -> int | None:
        now = time.monotonic() if now is None else now
        revision = self._ready_revision(folder_id, status, connected, remote_state)
        if revision is None:
            if folder_id == self.folder_id and (connected is not True or remote_state != 'valid'):
                self.completed_revision = None
            self._revision = self._local_sequence = self._since = None
            self._eligible = False
            return None
        if revision != self._revision or status['sequence'] != self._local_sequence:
            self._revision, self._local_sequence, self._since = revision, status['sequence'], now
            self._eligible = False
            return None
        if revision == self.completed_revision or now - self._since < self.quiet_seconds:
            return None
        self._eligible = True
        return revision

    def complete(self, revision: int, result: dict, *, folder_id: str, after: dict,
                 connected: bool, remote_state: str) -> dict | None:
        """Accept only explicit successful indexing, never ordinary sync status."""
        stable = (self._ready_revision(folder_id, after, connected, remote_state) == revision
                  and after.get('sequence') == self._local_sequence)
        if (type(revision) is not int or not self._eligible or revision != self._revision or revision == self.completed_revision
                or not stable or not isinstance(result, dict)
                or result.get('state') != 'RESTORED' or result.get('indexed') is not True
                or type(result.get('changed_chats')) is not int or result['changed_chats'] < 0):
            raise ValueError('restore is not confirmed')
        self.completed_revision = revision
        self._eligible = False
        if result['changed_chats'] == 0:
            return None
        return {'id': uuid.uuid4().hex, 'state': 'RESTORED', 'indexed': True,
                'changed_chats': result['changed_chats']}

    def is_current(self, *, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return (self._revision is not None and self._revision == self.completed_revision
                and self._since is not None and now - self._since >= self.quiet_seconds)


class MultiFolderGate:
    """Require both received histories and their title index to be settled."""
    def __init__(self, *, folder_ids: tuple[str, ...], peer_id: str, quiet_seconds: float = 5.0):
        if (not isinstance(folder_ids, tuple) or not folder_ids or len(set(folder_ids)) != len(folder_ids)
                or any(not isinstance(folder, str) or not folder for folder in folder_ids)):
            raise ValueError('Unique folder IDs are required')
        self.gates = {folder: RestoreGate(folder_id=folder, peer_id=peer_id, quiet_seconds=quiet_seconds)
                      for folder in folder_ids}
        self.quiet_seconds = quiet_seconds
        self._current = self._since = self._completed = None
        self._eligible = False

    def _state(self, observations: dict):
        if not isinstance(observations, dict) or set(observations) != set(self.gates):
            self._completed = None
            return None
        for folder in self.gates:
            value = observations[folder]
            if (not isinstance(value, tuple) or len(value) != 4 or value[0] != folder
                    or value[2] is not True or value[3] != 'valid'):
                self._completed = None
                return None
        state = []
        for folder, gate in self.gates.items():
            value = observations[folder]
            _, status, connected, remote_state = value
            revision = gate._ready_revision(folder, status, connected, remote_state)
            if revision is None:
                return None
            state.append((folder, revision, status['sequence']))
        return tuple(state)

    def observe(self, observations: dict, *, now: float | None = None):
        now = time.monotonic() if now is None else now
        state = self._state(observations)
        if state is None:
            self._current = self._since = None
            self._eligible = False
            return None
        revision = tuple((folder, remote) for folder, remote, _ in state)
        if state != self._current:
            self._current, self._since = state, now
            self._eligible = False
            return None
        if revision == self._completed or now - self._since < self.quiet_seconds:
            return None
        self._eligible = True
        return revision

    def complete(self, revision: tuple, result: dict, *, after: dict) -> dict | None:
        state = self._state(after)
        current_revision = tuple((folder, remote) for folder, remote, _ in state) if state is not None else None
        if (not self._eligible or state != self._current or current_revision != revision or revision == self._completed
                or not isinstance(result, dict) or result.get('state') != 'RESTORED' or result.get('indexed') is not True
                or type(result.get('changed_chats')) is not int or result['changed_chats'] < 0):
            raise ValueError('restore is not confirmed')
        self._completed, self._eligible = revision, False
        if result['changed_chats'] == 0:
            return None
        return {'id': uuid.uuid4().hex, 'state': 'RESTORED', 'indexed': True,
                'changed_chats': result['changed_chats']}

    def is_current(self, *, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        if self._current is None or self._since is None or now - self._since < self.quiet_seconds:
            return False
        return tuple((folder, remote) for folder, remote, _ in self._current) == self._completed

    def request_retry(self) -> None:
        """Recheck pending chats even if the received revision did not change."""
        self._completed = None
        self._eligible = False
