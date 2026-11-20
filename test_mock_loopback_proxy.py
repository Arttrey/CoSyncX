import hashlib
import os
import socket
import threading
import unittest

from mock_loopback_proxy import LoopbackMSSProxy


@unittest.skipUnless(os.environ.get('NEXUS_TEST_SYNCTHING') == '1', 'requires opt-in local network')
class LoopbackProxyTests(unittest.TestCase):
    def test_large_stream_and_shutdown(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        listener.settimeout(5)
        target_port = listener.getsockname()[1]
        reservation = socket.socket()
        reservation.bind(('127.0.0.1', 0))
        proxy_port = reservation.getsockname()[1]
        reservation.close()
        body = b'synthetic data\n' * 70000
        result = []

        def receiver():
            try:
                connection, _ = listener.accept()
                connection.settimeout(5)
                with connection:
                    digest = hashlib.sha256()
                    while True:
                        data = connection.recv(65536)
                        if not data:
                            break
                        digest.update(data)
                    result.append(digest.digest())
                    connection.sendall(b'ok')
            finally:
                listener.close()

        server = threading.Thread(target=receiver)
        server.start()
        proxy = LoopbackMSSProxy(proxy_port, target_port)
        try:
            with socket.create_connection(('127.0.0.1', proxy_port), timeout=5) as client:
                client.sendall(body)
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(client.recv(2), b'ok')
            server.join(timeout=5)
            self.assertFalse(server.is_alive())
            self.assertEqual(result, [hashlib.sha256(body).digest()])
        finally:
            proxy.close()
            server.join(timeout=5)
        self.assertFalse(proxy.thread.is_alive())
        self.assertTrue(all(not worker.is_alive() for worker in proxy.workers))


if __name__ == '__main__':
    unittest.main()
