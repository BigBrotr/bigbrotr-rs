"""Single-connection, plaintext PG protocol observer for the isolated lab only.

Forward actual SCRAM/startup/query traffic without recording payloads. The native
client still talks to real PostgreSQL. No socket endpoint is published outside
the disposable runner. A second pooled connection fails this bounded experiment;
this is deliberately not a general PostgreSQL proxy or a product fence.
"""
import select
import socket
import struct
import threading
import time


def exact(stream, count):
    data = bytearray()
    while len(data) < count:
        part = stream.recv(count - len(data))
        if not part:
            raise EOFError('partial protocol frame')
        data.extend(part)
    return bytes(data)


class StartupRelay:
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.listener.settimeout(15)
        self.port = self.listener.getsockname()[1]
        self.authenticated = threading.Event()
        self.proof_sent = threading.Event()
        self.done = threading.Event()
        self.started = time.monotonic()
        self.result = {'queries_before_ready': 0, 'queries_after_ready': 0}
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            with self.listener, self.listener.accept()[0] as client, \
                    socket.create_connection(('pg', 5432), timeout=10) as server:
                client.settimeout(10)
                # sslmode=disable and gssencmode=disable: first frame is startup.
                header = exact(client, 4)
                size, = struct.unpack('!I', header)
                assert 8 <= size <= 16384
                payload = exact(client, size - 4)
                assert payload[:4] == b'\x00\x03\x00\x00'
                server.sendall(header + payload)
                readable = [client, server]
                ready = False
                challenge_seen = False
                client_alive = True
                while readable:
                    assert time.monotonic() - self.started < 30, 'relay deadline'
                    for source in select.select(readable, [], [], .1)[0]:
                        try:
                            tag = source.recv(1)
                        except ConnectionResetError:
                            # An abrupt native exit may reset instead of FIN.
                            assert source is client
                            tag = b''
                        if not tag:
                            readable.remove(source)
                            if source is client:
                                client_alive = False
                                self.result['client_eof_seconds'] = time.monotonic() - self.started
                                server.shutdown(socket.SHUT_WR)
                            else:
                                self.result['server_eof_seconds'] = time.monotonic() - self.started
                                if client_alive:
                                    client.shutdown(socket.SHUT_WR)
                            continue
                        header = exact(source, 4)
                        size, = struct.unpack('!I', header)
                        assert 4 <= size <= 1024 * 1024
                        payload = exact(source, size - 4)
                        if source is server:
                            if tag == b'R' and payload[:4] == b'\0\0\0\x0b':
                                challenge_seen = True
                            if tag == b'R' and payload == b'\0\0\0\0':
                                self.result['authenticated_seconds'] = time.monotonic() - self.started
                                self.authenticated.set()
                            if tag == b'Z' and not ready:
                                ready = True
                                self.result['ready_seconds'] = time.monotonic() - self.started
                            # Drain the real server even after client death. This
                            # observes late startup completion, not a timed guess.
                            if client_alive:
                                client.sendall(tag + header + payload)
                        else:
                            if tag in (b'Q', b'P', b'B', b'D', b'E', b'F'):
                                key = 'queries_after_ready' if ready else 'queries_before_ready'
                                self.result[key] += 1
                            server.sendall(tag + header + payload)
                            if tag == b'p' and challenge_seen:
                                # AuthenticationOk is buffered until startup is
                                # complete on this server. Trigger the injected
                                # delay after the actual SCRAM proof instead.
                                self.result['proof_sent_seconds'] = time.monotonic() - self.started
                                self.proof_sent.set()
        except BaseException as error:
            # Protocol/authentication payloads and diagnostics are never logged.
            self.result['failure'] = type(error).__name__
        finally:
            self.done.set()

    def finish(self):
        self.thread.join(timeout=35)
        assert not self.thread.is_alive(), 'relay join deadline'
        assert 'failure' not in self.result, self.result.get('failure')
        assert self.authenticated.is_set()
        assert 'server_eof_seconds' in self.result
        return dict(self.result)
