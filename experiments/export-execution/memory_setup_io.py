"""Finite process/SQL evidence for the root-invoked fresh Export fixture.

No Docker, build, cache, cleanup, retry, or measurement-cgroup ownership.
Credential-bearing SQL output stays in the fresh private evidence directory.
"""
import hashlib
from itertools import chain
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import time


def require(ok, label):
    if not ok:
        raise ValueError(label)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def regular(path, limit=16 * 1024 * 1024):
    path = Path(path)
    require(path.is_absolute() and path.resolve() == path, 'absolute nonsymlink input')
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
            and not info.st_mode & 0o022 and info.st_size <= limit, 'protected finite input')
    return path.read_bytes()


def pinned(spec, executable=False):
    require(set(spec) == {'path', 'bytes', 'sha256'}, 'closed artifact pin')
    raw = regular(spec['path'], 256 * 1024 * 1024)
    require(len(raw) == spec['bytes'] and sha(raw) == spec['sha256'], 'artifact bytes')
    if executable:
        require(Path(spec['path']).stat().st_mode & 0o100, 'executable artifact')
    return raw


def identity(pid):
    base = Path('/proc') / str(pid)
    fields = base.joinpath('stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, start_ticks=fields[19], ppid=int(fields[1]),
                group=int(fields[2]), executable=os.readlink(base / 'exe'))


def group_members(group):
    rows = []
    for path in Path('/proc').iterdir():
        if not path.name.isdecimal():
            continue
        try:
            value = identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if value['group'] == group:
            rows.append(value)
    return sorted(rows, key=lambda value: value['pid'])


def census(root):
    root = Path(root)
    result = {}
    total = 0
    for path in chain([root], root.rglob('*')):
        meta = path.lstat()
        require(stat.S_ISDIR(meta.st_mode) or stat.S_ISREG(meta.st_mode), 'regular retained root')
        total += meta.st_size if path.is_file() else 0
        require(len(result) < 8192 and total <= 256 * 1024 * 1024
                and (not path.is_file() or meta.st_size <= 64 * 1024 * 1024), 'finite one-event retained census')
        result[str(path.relative_to(root))] = dict(device=meta.st_dev, inode=meta.st_ino,
            mode=stat.S_IMODE(meta.st_mode), links=meta.st_nlink, bytes=meta.st_size if path.is_file() else None,
            sha256=sha(path.read_bytes()) if path.is_file() else None)
    return result


class Child:
    def __init__(self, io, label, args, *, stdin=None, env=None):
        self.io, self.label, self.args = io, label, list(map(str, args))
        self.started = time.monotonic()
        self.out = (io.root / (label + '.stdout')).open('xb')
        self.err = (io.root / (label + '.stderr')).open('xb')
        self.identity, self.ended = None, False
        source = None
        try:
            if stdin is not None:
                require(len(stdin) <= 65536, 'finite command input')
                path = io.root / (label + '.stdin')
                with path.open('xb') as stream:
                    stream.write(stdin)
                source = path.open('rb')
            self.child = subprocess.Popen(self.args, stdin=source or subprocess.DEVNULL,
                stdout=self.out, stderr=self.err, env={'LC_ALL': 'C', **(env or {})}, start_new_session=True)
        except BaseException:
            self.out.close()
            self.err.close()
            raise
        finally:
            if source:
                source.close()
        # Register custody before any fallible /proc or evidence operation.
        io.children.append(self)
        self.identity = identity(self.child.pid)
        write(io.root / (label + '.pid.json'), dict(**self.identity, command=self.args))

    def messages(self):
        require(self.out.tell() <= 16 * 1024 * 1024, 'finite startup output')
        return [json.loads(line) for line in (self.io.root / (self.label + '.stdout')).read_text().splitlines(keepends=True)
                if line.endswith('\n')]

    def signal(self, sig):
        require(self.identity is not None and identity(self.child.pid) == self.identity,
                'exact child identity before signal')
        os.kill(self.child.pid, sig)

    def join(self, seconds, *, success=True):
        require(not self.ended, 'single process join')
        forced = False
        try:
            deadline = time.monotonic() + max(.01, min(seconds, self.io.remaining()))
            while True:
                code = self.child.poll()
                bounded = self.out.tell() <= 16 * 1024 * 1024 and self.err.tell() <= 1024 * 1024
                if code is not None and bounded:
                    break
                if not bounded or time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(self.args, seconds)
                time.sleep(.01)
        except subprocess.TimeoutExpired:
            forced = True
            if self.child.poll() is None:
                self.signal(signal.SIGKILL)
            code = self.child.wait(timeout=5)
        finally:
            self.out.close()
            self.err.close()
        self.ended = True
        remaining = group_members(self.child.pid)
        result = dict(identity=self.identity, command=self.args, returncode=code, forced=forced,
            reaped=True, remaining_group=remaining, seconds=time.monotonic()-self.started,
            outcome='UNKNOWN' if forced or remaining else 'PROCESS_EXITED')
        write(self.io.root / (self.label + '.terminal.json'), result)
        require(self.identity is not None and not forced and not remaining,
                'unknown process/native custody; do not reopen')
        if success:
            require(code == 0 and not (self.io.root / (self.label + '.stderr')).stat().st_size,
                    'command failed; preserve original outcome')
        return (self.io.root / (self.label + '.stdout')).read_text()


class IO:
    def __init__(self, root, spec, config):
        self.root, self.spec, self.config = Path(root), spec, config
        self.deadline, self.children, self.serial = time.monotonic() + 120, [], 0

    def remaining(self):
        return self.deadline - time.monotonic()

    def wait(self, predicate, label, seconds=20):
        deadline = min(self.deadline, time.monotonic() + seconds)
        while True:
            value = predicate()
            if value:
                return value
            require(time.monotonic() < deadline, label)
            time.sleep(.05)

    def start(self, label, args, **kwargs):
        require(self.remaining() > 0, 'finite fresh setup deadline')
        return Child(self, label, args, **kwargs)

    def run(self, label, args, seconds=30, **kwargs):
        return self.start(label, args, **kwargs).join(seconds)

    def cli(self, label, *args):
        return json.loads(self.run(label, [self.spec['application']['path'], *args]))

    def sql(self, database, statement, *, mutation=False):
        require(re.fullmatch('[a-z][a-z0-9_]{0,62}', database), 'database identifier')
        self.serial += 1
        label = f'sql-{self.serial:03}'
        user = self.spec['operator']['user']
        secret = regular(self.spec['operator']['password_ref'].removeprefix('file:'), 4096).decode()
        env = dict(PGPASSWORD=secret, PGCONNECT_TIMEOUT='5')
        script = ("SET statement_timeout='5s'; SET lock_timeout='2s'; "
                  "SET idle_in_transaction_session_timeout='5s';\n")
        if not mutation:
            script += 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n'
        script += statement + ('\n' if mutation else '\nCOMMIT;\n')
        write(self.root / (label + '.query.json'), dict(database=database, mutation=mutation, sql=script))
        raw = self.run(label, [self.spec['psql']['path'], '-X', '-A', '-t', '-q', '-w',
            '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', '5432', '-U', user, '-d', database],
            seconds=8, stdin=script.encode(), env=env)
        return raw.strip()

    def tables(self, database, schema=None):
        names = json.loads(self.sql(database, """SELECT coalesce(json_agg(schemaname||'.'||tablename
            ORDER BY schemaname,tablename),'[]') FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema')"""))
        require(all(re.fullmatch('[a-z_]+\\.[a-z_0-9]+', name) for name in names), 'table identifiers')
        if schema is not None:
            require(all(name.startswith(schema + '.') for name in names), 'only expected schema')
        if not names:
            return {}
        parts = [f"SELECT '{name}' AS name,coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]') AS rows "
                 f"FROM (SELECT *,xmin::text AS xmin FROM {name}) r" for name in names]
        value = json.loads(self.sql(database, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t;'))
        return {name.removeprefix(schema + '.'): rows for name, rows in value.items()} if schema else value

    def capture(self, label):
        value = dict(control=self.tables(self.config['postgres']['control']['database'], 'control'),
                     catalog=self.tables(self.config['postgres']['catalog']['database']))
        require(len(value['control']) == 75, 'complete current 75-table Control capture')
        write(self.root / (label + '.json'), value)
        return value

    def backends(self, label, absent=False):
        databases = [self.config['postgres'][key]['database'] for key in ('control', 'catalog')]
        require(all(re.fullmatch('[a-z][a-z0-9_]{0,62}', db) for db in databases), 'database identifiers')
        raw = self.sql(databases[0], "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,"
            "'database',datname,'user',usename,'state',state) ORDER BY pid),'[]') FROM pg_stat_activity "
            "WHERE datname IN ('" + "','".join(databases) + "') AND pid<>pg_backend_pid();")
        value = json.loads(raw)
        write(self.root / (label + '-backends.json'), value)
        require(not absent or not value, 'actual backend exclusion before reopening')
        return value

    def stop(self, child):
        if child.child.poll() is None:
            child.signal(signal.SIGTERM)
        child.join(8)
        self.backends(child.label + '-joined', absent=True)

    def failure_custody(self):
        # No new reader, dump, SQL mutation or process launch after a failure.
        if (self.root / 'failure-custody.json').exists():
            return
        result = []
        for child in reversed(self.children):
            if child.ended:
                continue
            try:
                if child.child.poll() is None:
                    child.signal(signal.SIGTERM)
                child.join(5, success=False)
            except BaseException as error:
                result.append(dict(label=child.label, error=type(error).__name__))
        write(self.root / 'failure-custody.json', dict(status='UNKNOWN_PRESERVED', errors=result,
            reopening='requires root process/backend/cgroup audit and explicit outcome recovery'))
