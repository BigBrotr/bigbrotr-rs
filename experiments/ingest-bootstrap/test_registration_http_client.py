"""HTTP oracle must retain a parsed status when body delivery is interrupted."""
import http.client
import unittest
from unittest.mock import patch
from registration_http_client import read_outcome


class Response:
    def __init__(self, status, body_error=None, body=b'{"code":"service_unavailable"}'):
        self.status = status
        self.body_error = body_error
        self.body = body

    def begin(self):
        pass

    def read(self, limit):
        if self.body_error is not None:
            raise self.body_error
        body, self.body = self.body, b''
        return body


class OracleTests(unittest.TestCase):
    def read(self, status, error=None, body=b'{"code":"service_unavailable"}'):
        with patch('registration_http_client.http.client.HTTPResponse', return_value=Response(status,error,body)):
            return read_outcome(object(), 'after')

    def test_success_headers_cannot_be_forgotten_after_reset(self):
        for status in (200, 202):
            with self.subTest(status=status), self.assertRaises(AssertionError):
                self.read(status, ConnectionResetError())

    def test_unavailable_headers_remain_observable_after_reset(self):
        self.assertEqual(self.read(503, ConnectionResetError()), dict(status=503, closed='ConnectionResetError'))

    def test_truncated_unavailable_is_transport_ambiguity(self):
        self.assertEqual(self.read(503, http.client.IncompleteRead(b'{')), dict(status=503, closed='IncompleteRead'))

    def test_complete_malformed_unavailable_is_not_transport_ambiguity(self):
        with self.assertRaises(ValueError):
            self.read(503, body=b'{')

    def test_complete_unavailable(self):
        self.assertEqual(self.read(503), dict(status=503, body={'code':'service_unavailable'}))


if __name__ == '__main__':
    unittest.main()
