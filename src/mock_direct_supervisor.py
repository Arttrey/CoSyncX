"""Automatic Syncthing receive loop for disposable mock profiles only."""
import argparse
from pathlib import Path
import sys
import threading

import cosync_engine
import cosync_restore_status
import cosync_syncthing_watch
import mock_chat_sync as mock
from mock_direct_receiver import MockDirectReceiver
import nexus_sync as ns


class MockSupervisor:
    def __init__(self, receiver, chat_reader, status_file: Path, *, title_reader=None, graph_reader=None):
        self.receiver, self.chat_reader, self.title_reader = receiver, chat_reader, title_reader
        self.graph_reader = graph_reader
        self.status_file = mock._mock_path(status_file)
        previous = cosync_restore_status.read_status(self.status_file, mock_only=True, max_age_ms=None)
        self.last_event = previous.get('restore_event') if previous else None

    def _publish(self, state):
        cosync_restore_status.write_status(self.status_file, state, mock_only=True, event=self.last_event)

    def tick(self):
        try:
            result = self.receiver.poll(self.chat_reader, title_status_reader=self.title_reader,
                                        graph_status_reader=self.graph_reader,
                                        on_restore=lambda: self._publish('RESTORING'))
            state = result.get('state')
            if state == 'COMPLETE':
                if result.get('restore_event') is not None:
                    self.last_event = result['restore_event']
                self._publish('COMPLETE')
            elif state == 'CURRENT':
                self._publish('COMPLETE')
            else:
                self._publish('PENDING')
            return result
        except (ns.SyncError, cosync_engine.EngineError, cosync_syncthing_watch.WatchError,
                cosync_restore_status.StatusError, OSError, ValueError, TypeError):
            try:
                self._publish('ERROR')
            except (cosync_restore_status.StatusError, OSError, ValueError):
                pass
            return {'state': 'ERROR', 'mock_only': True}

    def run(self, stop: threading.Event, interval: float):
        if not 0.2 <= interval <= 60:
            raise ValueError('Invalid mock polling interval')
        while not stop.is_set():
            self.tick()
            stop.wait(interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run only with disposable temporary profiles')
    for name in ('source', 'profile', 'shared', 'key-file', 'target-root', 'codex', 'syncthing-config', 'status-file'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-root', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--folder-id', required=True)
    parser.add_argument('--peer-id', required=True)
    parser.add_argument('--titles', type=Path)
    parser.add_argument('--titles-folder-id')
    parser.add_argument('--graph', type=Path)
    parser.add_argument('--graph-folder-id')
    parser.add_argument('--backup-root', type=Path)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args(argv)
    try:
        for path in (args.source, args.profile, args.shared, args.key_file, args.target_root,
                     args.syncthing_config, args.status_file):
            mock._mock_path(path)
        key = ns.read_key(args.key_file, args.shared)
        client = cosync_engine.ExistingEngine(args.url, args.syncthing_config, config_xml=True)
        receiver = MockDirectReceiver(source=args.source, profile=args.profile, shared=args.shared, key=key,
            source_root=args.source_root, target_root=args.target_root, codex=args.codex,
            folder_id=args.folder_id, peer_id=args.peer_id, backup_root=args.backup_root,
            titles=args.titles, titles_folder_id=args.titles_folder_id,
            graph=args.graph, graph_folder_id=args.graph_folder_id)
        chat = cosync_syncthing_watch.SyncthingSource(client, args.folder_id, args.peer_id, args.source / 'sessions')
        titles = (cosync_syncthing_watch.SyncthingSource(client, args.titles_folder_id, args.peer_id, args.titles)
                  if args.titles is not None else None)
        graph = (cosync_syncthing_watch.SyncthingSource(client, args.graph_folder_id, args.peer_id, args.graph)
                 if args.graph is not None else None)
        supervisor = MockSupervisor(receiver, chat.read, args.status_file, title_reader=titles.read if titles else None,
                                    graph_reader=graph.read if graph else None)
        supervisor.run(threading.Event(), args.interval)
        return 0
    except KeyboardInterrupt:
        return 0
    except (ns.SyncError, cosync_engine.EngineError, cosync_syncthing_watch.WatchError,
            cosync_restore_status.StatusError, OSError, ValueError):
        print('CoSyncX: mock supervisor unavailable', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
