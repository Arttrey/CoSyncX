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
        status.write_status(self.status_file, state, mock_only=self.mock_only, event=self.last_event)

    def tick(self):
        try:
            result = self.receiver.poll(self.chat_reader, title_status_reader=self.title_reader,
                                        graph_status_reader=self.graph_reader,
                                        on_restore=lambda: self._publish('RESTORING'))
            if not isinstance(result, dict) or result.get('mock_only') is not self.mock_only:
                raise ValueError('Receiver mode mismatch')
            state, event = result.get('state'), result.get('restore_event')
            if state == 'COMPLETE':
                if event is not None:
                    if not status._valid_event(event, self.mock_only):
                        raise ValueError('Invalid restore confirmation')
                    self.last_event = event
                self._publish('COMPLETE')
                if ((event is not None or result.get('acknowledge_required') is True)
                        and callable(getattr(self.receiver, 'acknowledge', None))):
                    self.receiver.acknowledge(event)
            elif state == 'CURRENT':
                if event is not None:
                    raise ValueError('Unexpected restore event')
                self._publish('COMPLETE')
            elif state in ('WAITING', 'PENDING'):
                if event is not None:
                    raise ValueError('Unexpected restore event')
                self._publish('PENDING')
            else:
                raise ValueError('Invalid receiver state')
            return result
        except Exception:
            try:
                self._publish('ERROR')
            except (status.StatusError, OSError, ValueError):
                pass
            return {'state': 'ERROR', 'mock_only': self.mock_only}

    def run(self, stop: threading.Event, interval: float):
        if not 0.2 <= interval <= 60:
            raise ValueError('Invalid polling interval')
        while not stop.is_set():
            self.tick()
            stop.wait(interval)
