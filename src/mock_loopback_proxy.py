"""TCP-посредник только для временных loopback-испытаний на Linux."""
import socket
import threading


class LoopbackMSSProxy:
    """Forward local TCP streams while limiting MSS, without changing host settings."""

    def __init__(self, listen_port: int, target_port: int, mss: int = 1200):
        if any(type(port) is not int or not 1 <= port <= 65535 for port in (listen_port, target_port)):
            raise ValueError('invalid temporary port')
        if type(mss) is not int or not 536 <= mss <= 4096:
            raise ValueError('invalid MSS')
        self.target_port = target_port
        self.mss = mss
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()
        self.workers = []
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, mss)
            self.listener.bind(('127.0.0.1', listen_port))
            self.listener.listen()
            self.listener.settimeout(0.25)
        except BaseException:
            self.listener.close()
            raise
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while not self.stopped.is_set():
            try:
                incoming, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                outgoing.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, self.mss)
                outgoing.settimeout(2)
                outgoing.connect(('127.0.0.1', self.target_port))
                outgoing.settimeout(None)
            except OSError:
                incoming.close()
                outgoing.close()
                continue
            with self.lock:
                if self.stopped.is_set():
                    incoming.close()
                    outgoing.close()
                    return
                self.connections.update((incoming, outgoing))
                worker = threading.Thread(target=self._forward, args=(incoming, outgoing), daemon=True)
                self.workers.append(worker)
                worker.start()

    def _forward(self, incoming, outgoing):
        def copy(source, target):
            try:
                while not self.stopped.is_set():
                    chunk = source.recv(65536)
                    if not chunk:
                        break
                    target.sendall(chunk)
            except OSError:
                pass
            finally:
                try:
                    target.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        first = threading.Thread(target=copy, args=(incoming, outgoing), daemon=True)
        second = threading.Thread(target=copy, args=(outgoing, incoming), daemon=True)
        first.start()
        second.start()
        first.join()
        second.join()
        with self.lock:
            self.connections.difference_update((incoming, outgoing))
        incoming.close()
        outgoing.close()

    def close(self):
        self.stopped.set()
        self.listener.close()
        self.thread.join(timeout=3)
        with self.lock:
            connections = tuple(self.connections)
            workers = tuple(self.workers)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for worker in workers:
            worker.join(timeout=3)
