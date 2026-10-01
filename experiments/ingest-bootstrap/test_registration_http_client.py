"""Use actual HTTP parsing so the oracle cannot forget a partially delivered status."""
import http.client
import io
import json
import unittest
from registration_http_client import read_outcome


class Wire(io.BytesIO):
    def __init__(self, data, reset_at=None):
        super().__init__(data)
        self.reset_at = reset_at

    def readline(self, *args):
        if self.reset_at == 'headers' and self.tell() > 0:
            raise ConnectionResetError()
        return super().readline(*args)

    def read(self, *args):
        if self.reset_at == 'body':
            raise ConnectionResetError()
        return super().read(*args)

    def makefile(self, mode):
        assert mode == 'rb'
        return self


class OracleTests(unittest.TestCase):
    def read(self, status, reset_at=None, body=b'{"code":"service_unavailable"}', length=None):
        header = (f'HTTP/1.1 {status} Fixture\r\nContent-Length: '
                  f'{len(body) if length is None else length}\r\n\r\n').encode()
        return read_outcome(Wire(header + body, reset_at), 'after')

    def test_success_status_cannot_be_forgotten_after_reset(self):
        for status in (200, 202):
            for stage in ('headers', 'body'):
                with self.subTest(status=status, stage=stage), self.assertRaises(AssertionError):
                    self.read(status, stage)

    def test_unavailable_status_remains_observable_after_reset(self):
        for stage in ('headers', 'body'):
            with self.subTest(stage=stage):
                self.assertEqual(self.read(503, stage), dict(status=503, closed='ConnectionResetError'))

    def test_truncated_unavailable_is_transport_ambiguity(self):
        self.assertEqual(self.read(503, body=b'{', length=10), dict(status=503, closed='IncompleteRead'))

    def test_complete_malformed_unavailable_is_not_transport_ambiguity(self):
        with self.assertRaises(ValueError):
            self.read(503, body=b'{')

    def test_complete_unavailable(self):
        self.assertEqual(self.read(503), dict(status=503, body={'code':'service_unavailable'}))

    def test_empty_wire_has_no_status(self):
        self.assertEqual(read_outcome(Wire(b''), 'after'), dict(closed='RemoteDisconnected'))

    def test_oversize_is_not_transport_ambiguity(self):
        with self.assertRaises(AssertionError):
            self.read(503, body=b' ' * 4097)

    def test_scope_and_endpoint_success_formats_are_distinct(self):
        for scope in (False, True):
            receipt = dict(format='bigbrotr-acquisition-scope-registration/1' if scope
                else 'bigbrotr-relay-registration/1', authority='identity-only',
                command_key='31' * 16, created=True)
            body = json.dumps(receipt).encode()
            wire = f'HTTP/1.1 202 Created\r\nContent-Length: {len(body)}\r\n\r\n'.encode() + body
            with self.subTest(scope=scope):
                self.assertEqual(read_outcome(Wire(wire), 'pass', scope=scope), dict(status=202, body=receipt))
                with self.assertRaises(AssertionError):
                    read_outcome(Wire(wire), 'pass', scope=not scope)


if __name__ == '__main__':
    unittest.main()
