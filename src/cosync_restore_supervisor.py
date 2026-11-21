"""Publish status for a receiver driven by the configured Syncthing folder."""
from pathlib import Path
import threading

import cosync_restore_status as status


class RestoreSupervisor:
    """Turn a receiver's verified result into the status consumed by Cursor.

    A receiver must poll the selected Syncthing source, perform restoration,
    verify the Codex index and recheck the same source before returning an
    event. Transfer completion alone is not a restore event.
    """
    def __init__(self, receiver, chat_reader, status_file: Path, *, mock_only: bool,
                 title_reader=None, graph_reader=None):
        if type(mock_only) is not bool or not Path(status_file).is_absolute():
            raise ValueError('Invalid restore supervisor configuration')
        self.receiver, self.chat_reader = receiver, chat_reader
        self.title_reader, self.graph_reader = title_reader, graph_reader
        self.status_file, self.mock_only = Path(status_file), mock_only
        previous = status.read_status(self.status_file, mock_only=mock_only, max_age_ms=None)
        self.last_event = previous.get('restore_event') if previous else None

    def _publish(self, state):
        count = getattr(self.receiver, 'path_warning_count', 0)
        root = getattr(self.receiver, 'backup_root', None)
        report = str(root / 'missing-paths.json') if count and isinstance(root, Path) else None
        status.write_status(self.status_file, state, mock_only=self.mock_only, event=self.last_event,
                            missing_path_chats=count, missing_path_report=report)

    def tick(self):
        stop_heartbeat = threading.Event()
        state_lock = threading.Lock()
        working_state = [None]
        heartbeat = None
        def update_working(state):
            nonlocal heartbeat
            with state_lock:
                working_state[0] = state
                self._publish(state)
            if heartbeat is None:
                def refresh():
                    while not stop_heartbeat.wait(20):
                        try:
                            with state_lock:
                                if working_state[0] is not None:
                                    self._publish(working_state[0])
                        except (status.StatusError, OSError, ValueError):
                            pass
                heartbeat = threading.Thread(target=refresh, daemon=True)
                heartbeat.start()
        try:
            result = self.receiver.poll(self.chat_reader, title_status_reader=self.title_reader,
                                        graph_status_reader=self.graph_reader,
                                        on_check=lambda: update_working('CHECKING'),
                                        on_restore=lambda: update_working('RESTORING'))
            stop_heartbeat.set()
            if heartbeat is not None:
                heartbeat.join()
            if not isinstance(result, dict) or result.get('mock_only') is not self.mock_only:
                raise ValueError('Receiver mode mismatch')
            state, event = result.get('state'), result.get('restore_event')
            if state in ('COMPLETE', 'PENDING'):
                if event is not None:
                    if not status._valid_event(event, self.mock_only):
                        raise ValueError('Invalid restore confirmation')
                    self.last_event = event
                self._publish(state)
                if ((event is not None or result.get('acknowledge_required') is True)
                        and callable(getattr(self.receiver, 'acknowledge', None))):
                    self.receiver.acknowledge(event)
            elif state == 'CURRENT':
                if event is not None:
                    raise ValueError('Unexpected restore event')
                self._publish('COMPLETE')
            elif state == 'WAITING':
                if event is not None:
                    raise ValueError('Unexpected restore event')
                self._publish('PENDING')
            else:
                raise ValueError('Invalid receiver state')
            return result
        except Exception:
            stop_heartbeat.set()
            if heartbeat is not None:
                heartbeat.join()
            try:
                self._publish('ERROR')
            except (status.StatusError, OSError, ValueError):
                pass
            return {'state': 'ERROR', 'mock_only': self.mock_only}
        finally:
            stop_heartbeat.set()
            if heartbeat is not None:
                heartbeat.join()

    def run(self, stop: threading.Event, interval: float):
        if not 0.2 <= interval <= 60:
            raise ValueError('Invalid polling interval')
        while not stop.is_set():
            self.tick()
            stop.wait(interval)
