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

    def test_success_formats_and_authorities_are_distinct(self):
        formats = dict(endpoint='bigbrotr-relay-registration/1',
            scope='bigbrotr-acquisition-scope-registration/1', job='bigbrotr-job-acceptance/1')
        for kind, format_name in formats.items():
            receipt = dict(format=format_name, authority='acceptance-only' if kind=='job' else 'identity-only',
                command_key='31'*16, created=True)
            if kind=='job': receipt['execution']='unobserved'
            def wire(value):
                body=json.dumps(value).encode()
                return Wire(f'HTTP/1.1 202 Created\r\nContent-Length: {len(body)}\r\n\r\n'.encode()+body)
            with self.subTest(kind=kind):
                self.assertEqual(read_outcome(wire(receipt), 'pass', kind=kind),dict(status=202,body=receipt))
                for other in formats.keys()-{kind}:
                    with self.assertRaises(AssertionError):
                        read_outcome(wire(receipt), 'pass', kind=other)
                if kind=='job':
                    for patch in ({'authority':'identity-only'},{'execution':'running'},{'created':False}):
                        with self.assertRaises(AssertionError):
                            read_outcome(wire(dict(receipt,**patch)), 'pass', kind=kind)


if __name__ == '__main__':
    unittest.main()
