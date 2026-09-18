"""Bounded interactive psql session for catalog lock schedules, lab use only."""
import json
import os
import select
import subprocess
import time


class PgSession:
    def __init__(self, environment, children):
        self.process = subprocess.Popen(
            ['psql', '-X', '-q', '-t', '-A', '--no-password', '-v',
             'ON_ERROR_STOP=off', '-v', 'VERBOSITY=sqlstate'],
            env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, bufsize=0)
        children.append(self.process)
        self.buffer = b''
        self.pending = None
        self.sequence = 0
        self.total_bytes = 0
        self.backend = json.loads(self.execute(
            "SELECT json_build_object('pid',pid,'start',backend_start::text,'user',usename) "
            "FROM pg_stat_activity WHERE pid=pg_backend_pid();")[0])
        assert self.backend['user'] == environment['PGUSER']

    def start(self, query):
        assert self.pending is None
        assert len(query) < 8192
        self.sequence += 1
        self.pending = 'bb_catalog_result_' + str(self.sequence)
        data = (query + '\n\\echo ' + self.pending + ' :SQLSTATE\n').encode()
        assert self.process.stdin.write(data) == len(data)
        self.process.stdin.flush()

    def finish(self, expected='00000'):
        assert self.pending is not None
        until = time.monotonic() + 12
        lines = []
        while True:
            while b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                text = line.decode()
                if text.startswith(self.pending + ' '):
                    assert text == self.pending + ' ' + expected, 'SQLSTATE mismatch'
                    self.pending = None
                    return lines
                lines.append(text)
            remaining = until - time.monotonic()
            assert remaining > 0 and select.select([self.process.stdout], [], [], remaining)[0]
            data = os.read(self.process.stdout.fileno(), 4096)
            self.total_bytes += len(data)
            assert data and self.total_bytes <= 65536
            self.buffer += data

    def execute(self, query, expected='00000'):
        self.start(query)
        return self.finish(expected)

    def close(self, sql):
        # Match the live backend incarnation before signaling. Client EOF alone
        # is not a rollback acknowledgement for a waiting server transaction.
        predicate = (f"pid={self.backend['pid']} AND "
                     f"backend_start::text='{self.backend['start']}' AND "
                     f"usename='{self.backend['user']}'")
        assert sql('SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE ' + predicate) in ('', 't')
        assert sql('SELECT count(*) FROM pg_stat_activity WHERE ' + predicate) == '0'
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=5)
