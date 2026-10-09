#!/usr/bin/env python3
"""Bounded smoke protocol tests; no servers, credentials or third-party modules."""
import time
import unittest
from smoke import Nats


class Socket:
    def __init__(self):
        self.timeouts = []
        self.sent = []

    def settimeout(self, value):
        self.timeouts.append(value)

    def sendall(self, value):
        self.sent.append(value)


class PingFile:
    def readline(self):
        time.sleep(0.005)
        return b'PING\r\n'


class ProtocolTests(unittest.TestCase):
    def client(self):
        c = Nats.__new__(Nats)
        c.timeout = 0.025
        c.sock = Socket()
        c.file = PingFile()
        return c

    def test_keepalive_does_not_extend_reply_deadline(self):
        c = self.client()
        with self.assertRaises(TimeoutError):
            c.message()
        self.assertTrue(c.sock.sent)
        self.assertTrue(all(m == b'PONG\r\n' for m in c.sock.sent))
        self.assertGreater(c.sock.timeouts[0], c.sock.timeouts[-1])

    def test_keepalive_does_not_extend_pong_deadline(self):
        c = self.client()
        with self.assertRaises(TimeoutError):
            c.until_pong()
        self.assertGreater(c.sock.timeouts[0], c.sock.timeouts[-1])

    def test_no_responders_is_failure_not_an_empty_success(self):
        c = self.client()
        class NoResponders:
            def readline(self):
                return b'HMSG _INBOX.test 1 16 16\r\n'
            def read(self, size):
                return b'NATS/1.0 503\r\n\r\n'
        c.file = NoResponders()
        with self.assertRaisesRegex(RuntimeError, 'no responders'):
            c.message()


if __name__ == '__main__':
    unittest.main()
