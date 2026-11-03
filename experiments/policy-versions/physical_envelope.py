"""Bounded physical-growth diagnostic for the existing current-policy laboratory.

This is an observer, never a reservation authority or OPEN65 qualification. The
sample command runs inside the exactly owned client container; the PostgreSQL
fd census runs inside the exactly owned PostgreSQL container, under their
configured lab UID (the containers deliberately have no extra capabilities).
"""
import argparse
import base64
from datetime import datetime
from decimal import Decimal
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import threading
import time

EVENT_BYTES = 262144
EVENT_COUNT = 63
CASES = ('compressible', 'highentropy')
# Declared before execution. A missed witness never extends these limits.
PLAN = dict(events=EVENT_COUNT, complete_event_bytes=EVENT_BYTES, bootstrap_requests=1,
    sample_interval_ms=100, pg_fd_interval_ms=500, automatic_work_seconds=30,
    ready_seconds=25, load_seconds=30, gate_seconds=45, case_seconds=150,
    maximum_samples=1600, maximum_sample_bytes=64*1024**2,
    maximum_output_bytes=4*1024**2, maximum_files=8192,
    original_file_cap=64, original_byte_cap=32*1024**2,
    log_capture_byte_cap=16*1024**2, checkpoint_stats_seconds=5)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def fixture_module(source):
    path = source / 'experiments/identity-contract/signed_fixture.py'
    spec = importlib.util.spec_from_file_location('physical_signed_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def preflight(source):
    # The maintained signer is a declared dependency; no download/install is
    # performed by the experiment. The separate equation checker is Python.
    require(importlib.metadata.version('secp256k1') == '0.14.0', 'secp256k1==0.14.0 required')
    fixture_module(source)
    require(1 + EVENT_COUNT == PLAN['original_file_cap'], 'absolute64 retained')
    return dict(plan=PLAN, signer='python-secp256k1==0.14.0',
                verifier='independent BIP340 group equation',
                scope='DIAGNOSTIC_ONLY; no physical reservation or OPEN65')


def make_corpus(path, case, source):
    require(case in CASES and not path.exists(), 'fresh declared corpus')
    signer = fixture_module(source)
    events, receipts = [], []
    with path.open('xb') as output:
        for index in range(1, EVENT_COUNT + 1):
            template = dict(pubkey=signer.GENERATOR[0].to_bytes(32, 'big').hex(),
                created_at=1700000000+index, kind=1, tags=[], content='',
                id='0'*64, sig='0'*128)
            content_bytes = EVENT_BYTES-len(compact(template))
            # SHAKE is a deterministic independent byte source, not a repeated
            # seed block. JSON-safe base64 preserves 6bits per byte for B.
            content = ('a'*content_bytes if case == 'compressible' else
                base64.b64encode(hashlib.shake_256(
                    f'bigbrotr/physical-envelope/B/{index}'.encode()).digest(
                        (content_bytes*3+3)//4)).decode()[:content_bytes])
            event = signer.sign(1, template['created_at'], 1, [], content)
            raw = compact(event)
            require(len(raw) == EVENT_BYTES, 'complete signed event exact size')
            message = hashlib.sha256(signer.canonical(event)).digest()
            require(message.hex() == event['id'] and signer.equation_verify(message,
                bytes.fromhex(event['pubkey']), bytes.fromhex(event['sig'])), 'independent signed oracle')
            output.write(raw+b'\n')
            events.append(event)
            receipts.append(dict(index=index, event_id=event['id'], bytes=len(raw), sha256=digest(raw)))
        output.flush()
        os.fsync(output.fileno())
    require(len({event['id'] for event in events}) == EVENT_COUNT, 'distinct event IDs')
    raw = path.read_bytes()
    require(len(raw) == EVENT_COUNT*(EVENT_BYTES+1), 'exact JSONL byte count')
    return events, dict(case=case, bytes=len(raw), sha256=digest(raw), events=receipts,
        canonical='NIP01 [0,pubkey,created_at,kind,tags,content] compact UTF8',
        generation='public test secret1; deterministic SHAKE256/base64 B; repeated a A')


def classify(role, relative):
    parts = Path(relative).parts
    name = parts[-1]
    if role == 'inputs':
        return 'INPUT'
    if role == 'source':
        return 'SOURCE_INPUT' if relative == 'source.jsonl' else 'SOURCE_SNAPSHOT'
    if role == 'postgres':
        if parts[0] == 'pg_wal': return 'PG_WAL'
        if any(part.startswith('pgsql_tmp') or part.startswith('t') and re.fullmatch(r't\d+_\d+', part)
               for part in parts): return 'PG_TEMP'
        return 'PG_DATA' if parts[0] in ('base', 'global') else 'PG_OTHER'
    if name.endswith(('.sst', '.ldb')): return 'SST'
    if re.fullmatch(r'\d+\.log', name): return 'WAL'
    if name.startswith('MANIFEST-'): return 'MANIFEST'
    if name == 'LOG' or name.startswith('LOG.old.'): return 'LOG'
    return 'NATIVE_OTHER'


def file_record(value, role, relative):
    return dict(device=value.st_dev, inode=value.st_ino, logical_bytes=value.st_size,
        allocated_bytes=value.st_blocks*512, links=value.st_nlink,
        classification=classify(role, relative))


class BoundRoot:
    """Observe the open directory, and refuse replacement of its published path."""
    def __init__(self, path, role):
        self.path, self.role = Path(path), role
        require(self.path.is_absolute() and self.path.resolve() == self.path, 'canonical actual root')
        self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        value = os.fstat(self.fd)
        self.identity = dict(device=value.st_dev, inode=value.st_ino)

    def close(self):
        os.close(self.fd)

    def sample(self, full=False):
        current = os.stat(self.path, follow_symlinks=False)
        require((current.st_dev, current.st_ino) == tuple(self.identity.values()), 'root path identity changed')
        vfs = os.fstatvfs(self.fd)
        records, missing, seen = {}, [], set()
        entries = 0

        def walk(directory, prefix=''):
            nonlocal entries
            with os.scandir(directory) as entries_iterator:
                for entry in entries_iterator:
                    relative = prefix+entry.name
                    if self.role == 'source' and not prefix and not (
                            entry.name == 'source.jsonl' or entry.name.startswith('.tmp')):
                        continue  # fixed corpus and real preflight snapshot only
                    if self.role == 'native' and relative == 'producer-inputs-v1':
                        continue  # separately observed; never charged twice
                    entries += 1
                    require(entries <= PLAN['maximum_files'], 'bounded filesystem census')
                    try:
                        value = os.stat(entry.name, dir_fd=directory, follow_symlinks=False)
                        require(not stat.S_ISLNK(value.st_mode), 'symlink in physical root')
                        require(value.st_dev == self.identity['device'], 'cross-device physical root')
                        if stat.S_ISDIR(value.st_mode):
                            child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                            try: walk(child, relative+'/')
                            finally: os.close(child)
                        else:
                            require(stat.S_ISREG(value.st_mode), 'special file in physical root')
                            key = (value.st_dev, value.st_ino)
                            if key not in seen:
                                seen.add(key)
                                records[relative] = file_record(value, self.role, relative)
                    except FileNotFoundError:
                        missing.append(relative)
        walk(self.fd)
        classes = {}
        for value in records.values():
            target = classes.setdefault(value['classification'], dict(files=0, logical_bytes=0, allocated_bytes=0))
            for field in ('logical_bytes', 'allocated_bytes'): target[field] += value[field]
            target['files'] += 1
        result = dict(identity=self.identity, classes=classes,
            census_sha256=digest(compact(sorted(records.items()))),
            disappeared_during_sample=missing, snapshot_atomic=False,
            filesystem=dict(fsid=vfs.f_fsid, block_bytes=vfs.f_frsize,
                available_bytes=vfs.f_bavail*vfs.f_frsize, free_bytes=vfs.f_bfree*vfs.f_frsize,
                total_bytes=vfs.f_blocks*vfs.f_frsize))
        if full: result['files'] = records
        return result


def open_unlinked(roots):
    """Container PID namespace only. Denials/races remain explicit unknowns."""
    records, errors, processes, descriptors = {}, [], 0, 0
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit(): continue
        processes += 1
        require(processes <= 256, 'container process bound')
        if int(proc.name) == os.getpid(): continue
        try:
            for fd in (proc/'fd').iterdir():
                descriptors += 1
                require(descriptors <= 8192, 'container fd bound')
                try:
                    target = os.readlink(fd)
                    if not target.endswith(' (deleted)'): continue
                    value = fd.stat()
                    if not stat.S_ISREG(value.st_mode): continue
                    matches = [(role, target[len(str(root.path))+1:-10]) for role, root in roots.items()
                               if target.startswith(str(root.path)+'/')]
                    if not matches: continue
                    role, relative = max(matches, key=lambda item: len(str(roots[item[0]].path)))
                    key = f'{value.st_dev}:{value.st_ino}'
                    records[key] = dict(file_record(value, role, relative), role=role, target=target)
                except OSError as error:
                    errors.append(dict(path=str(fd), errno=error.errno))
        except OSError as error:
            errors.append(dict(path=str(proc/'fd'), errno=error.errno))
    return dict(status='OBSERVED' if not errors else 'UNKNOWN_PARTIAL',
        pid_namespace=os.readlink('/proc/self/ns/pid'), processes=processes,
        observer_process_excluded=os.getpid(),
        descriptors=descriptors, files=records, errors=errors,
        allocated_bytes=sum(value['allocated_bytes'] for value in records.values()))


class LogCapture:
    """Retain incremental LOG bytes by inode across the four-file rotation."""
    def __init__(self, native, output):
        self.native, self.output = native, output
        self.seen, self.total = {}, 0
        self.output.mkdir(mode=0o755)  # protected by the outer laboratory0700

    def sample(self):
        captured = []
        for path in sorted(self.native.glob('LOG*')):
            if classify('native', path.name) != 'LOG': continue
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                value = os.fstat(descriptor)
                require(stat.S_ISREG(value.st_mode), 'regular Rocks LOG')
                key = f'{value.st_dev}-{value.st_ino}'
                offset = self.seen.get(key, 0)
                require(value.st_size >= offset, 'Rocks LOG truncation')
                # Read only the positively sampled size; later appended bytes
                # belong to the next sample, never an unbounded read-to-EOF.
                count = value.st_size-offset
                require(self.total+count <= PLAN['log_capture_byte_cap'], 'Rocks LOG capture cap')
                data = os.pread(descriptor, count, offset)
                require(len(data) == count, 'complete Rocks LOG read')
                with (self.output/(key+'.log')).open('ab') as stream: stream.write(data)
                self.seen[key] = value.st_size
                self.total += count
                captured.append(dict(path=path.name, device=value.st_dev, inode=value.st_ino,
                    captured_bytes=value.st_size, sha256=digest((self.output/(key+'.log')).read_bytes())))
            finally: os.close(descriptor)
        return dict(files=captured, total_captured_bytes=self.total,
            rollover_gap='not_excludable_between_samples; unmatched jobs remain NOT_WITNESSED')


def log_witness(directory):
    """Positive successful installs in one retained fresh-open LOG generation.

    JSON finished records alone are emitted even on installation errors. Flush
    requires its post-LogAndApply commit line; compaction requires the complete
    same-thread install-OK summary plus newly created positive output tables.
    """
    flushes, compactions, unmatched, errors = [], [], [], []
    paths = sorted(directory.glob('*.log'))
    # This deliberately declines jobs split across automatic LOG rotation.
    if len(paths) != 1:
        return dict(automatic_flush='NOT_WITNESSED', nontrivial_automatic_compaction='NOT_WITNESSED',
            flushes=[], compactions=[], unmatched_jobs=[], errors=['expected one fresh-open LOG generation'],
            scope='rotation or missing capture prevents this conservative LOG witness')
    for path in sorted(directory.glob('*.log')):
        text = path.read_text(errors='strict')
        sessions = set(re.findall(r'DB Session ID:\s+(\S+)', text))
        if len(sessions) != 1:
            errors.append('missing or mixed DB Session ID')
            continue
        session, = sessions
        starts, tables, flush_bindings, commits, summaries = {}, {}, {}, set(), {}
        for line in text.splitlines():
            thread = re.match(r'^\S+ \S+ ([0-9a-fA-F]+) ', line)
            thread = thread[1] if thread else None
            flush = re.search(r'\[([^\]]+)\] \[JOB (\d+)\] Level-0 flush table #(\d+): started', line)
            if flush:
                flush_bindings[(int(flush[2]),int(flush[3]))] = flush[1]
            commit = re.search(r'\[([^\]]+)\] Level-0 commit table #(\d+): memtable #\d+ done(?:\s|$)', line)
            if commit: commits.add((commit[1],int(commit[2])))
            summary = re.search(r'\[([^\]]+)\] compacted to: .*?, level (\d+), '
                r'files in\((\d+), (\d+)\).*? out\((\d+) \+0 blob\).*? OK, records in: (\d+), '
                r'records dropped: \d+ output_compression: ZSTD\s*$', line)
            if summary and thread:
                summaries[thread] = dict(cf=summary[1], level=int(summary[2]),
                    inputs=int(summary[3])+int(summary[4]), outputs=int(summary[5]), records=int(summary[6]))
            marker = 'EVENT_LOG_v1 '
            if marker not in line: continue
            def unique(pairs):
                result = {}
                for key,value in pairs:
                    require(key not in result, 'duplicate Rocks LOG JSON field')
                    result[key] = value
                return result
            try: event = json.loads(line.split(marker, 1)[1], object_pairs_hook=unique)
            except (ValueError,TypeError) as error:
                errors.append(repr(error))
                continue
            kind = event.get('event')
            if kind == 'table_file_creation':
                tables.setdefault(event.get('job'), []).append(event)
                continue
            if kind not in ('flush_started','flush_finished','compaction_started','compaction_finished'): continue
            family = kind.split('_')[0]
            key = (family, event['job'])
            if kind.endswith('_started'):
                starts.setdefault(key, []).append(event)
                continue
            candidates = starts.get(key, [])
            if not candidates:
                unmatched.append(dict(file=path.name, finish=event))
                continue
            start = candidates.pop(0)
            pair = dict(file=path.name, start=start, finish=event)
            output = [table for table in tables.get(event['job'], []) if table.get('status') == 'OK'
                and table.get('file_size',0) > 0 and table.get('table_properties',{}).get('num_entries',0) > 0
                and table['table_properties'].get('db_session_id') == session]
            if family == 'flush':
                # Atomic flush commits after flush_finished, checked after all
                # complete bytes below. Several CFs may share this job ID.
                pair['tables'] = [table for table in output
                    if flush_bindings.get((event['job'],table['file_number'])) == table.get('cf_name')]
                if start.get('flush_reason') in ('Write Buffer Manager','Write Buffer Full','WAL Full') \
                        and start.get('total_data_size',0) > 0 and pair['tables']:
                    flushes.append(pair)
            else:
                summary = summaries.pop(thread, None)
                inputs = {number for key, values in start.items() if key.startswith('files_L')
                          for number in values}
                output = [table for table in output if table.get('cf_name') == start.get('cf_name')
                          and table['file_number'] not in inputs]
                if start.get('compaction_reason') in ('LevelL0FilesNum','LevelMaxLevelSize') \
                        and start.get('input_data_size',0) > 0 and len(inputs) > 1 \
                        and event.get('total_output_size',0) > 0 and event.get('num_output_records',0) > 0 \
                        and len(output) == event.get('num_output_files') and summary is not None \
                        and summary == dict(cf=start.get('cf_name'),level=event.get('output_level'),
                            inputs=len(inputs),outputs=len(output),records=event.get('num_input_records')):
                    compactions.append(dict(pair, tables=output, installed_summary=summary))
        flushes = [pair for pair in flushes if any((table['cf_name'],table['file_number']) in commits
                                                  for table in pair['tables'])]
        unmatched += [dict(file=path.name,start=value) for values in starts.values() for value in values]
    if errors:
        flushes, compactions = [], []
    return dict(automatic_flush='WITNESSED' if flushes else 'NOT_WITNESSED',
        nontrivial_automatic_compaction='WITNESSED' if compactions else 'NOT_WITNESSED',
        flushes=flushes, compactions=compactions, unmatched_jobs=unmatched, errors=errors,
        source_format=['vendor/librocksdb-sys/rocksdb/db/flush_job.cc',
                       'vendor/librocksdb-sys/rocksdb/db/compaction/compaction_job.cc'],
        scope='positive paired jobs only; rollover gaps and unsampled peaks remain possible')


def sample_container(case):
    roots, logs, logs_frozen = {}, None, False
    started = time.monotonic_ns()
    count = 0
    try:
        with (case/'samples.jsonl').open('x') as output:
            while not (case/'sampler-stop').exists():
                require(time.monotonic_ns()-started <= PLAN['case_seconds']*10**9, 'sampler deadline')
                phase = (case/'phase').read_text().strip()
                for role, path in [('source',case), ('native',case/'store'), ('inputs',case/'store/producer-inputs-v1'),
                                   ('postgres',Path('/lab/data'))]:
                    if role not in roots and path.exists(): roots[role] = BoundRoot(path, role)
                if logs is None and 'native' in roots:
                    logs = LogCapture(case/'store',case/'rocks-log-capture')
                frame = dict(monotonic_ns=time.monotonic_ns(), elapsed_ns=time.monotonic_ns()-started,
                    phase=phase, roots={name:root.sample(full=count == 0) for name,root in roots.items()},
                    open_unlinked=open_unlinked({k:v for k,v in roots.items() if k != 'postgres'}),
                    rocks_log=logs.sample() if logs and not logs_frozen else None)
                output.write(json.dumps(frame, separators=(',', ':'))+'\n')
                output.flush()
                count += 1
                require(count <= PLAN['maximum_samples'] and output.tell() <= PLAN['maximum_sample_bytes'], 'sample output cap')
                if logs is not None and not logs_frozen and (case/'freeze-logs').exists():
                    snapshot = logs.sample()
                    logs_frozen = True
                    (case/'logs-frozen.json').write_text(json.dumps(snapshot)+'\n')
                time.sleep(PLAN['sample_interval_ms']/1000)
        result = dict(status='CAPTURED', samples=count,
            final={name:root.sample(full=True) for name,root in roots.items()},
            open_unlinked=open_unlinked({k:v for k,v in roots.items() if k != 'postgres'}),
            rocks_log=logs.sample() if logs and not logs_frozen else None,
            live_writer_log_capture_frozen=logs_frozen)
    except BaseException as error:
        result = dict(status='FAILED', error=repr(error), samples=count)
        raise
    finally:
        (case/'sampler-result.json').write_text(json.dumps(result, indent=2)+'\n')
        for root in roots.values(): root.close()


PG_FD_SCRIPT = r'''set -u
p=0; d=0
for directory in /proc/[0-9]*/fd; do
  p=$((p+1))
  if [ "$p" -gt 256 ]; then printf 'E\tprocess-cap\n'; exit 1; fi
  if [ ! -r "$directory" ]; then printf 'E\t%s\n' "$directory"; continue; fi
  for descriptor in "$directory"/*; do
    [ -L "$descriptor" ] || continue
    d=$((d+1))
    if [ "$d" -gt 8192 ]; then printf 'E\tfd-cap\n'; exit 1; fi
    target=$(readlink "$descriptor") || { printf 'E\t%s\n' "$descriptor"; continue; }
    case "$target" in
      /var/lib/postgresql/data/*' (deleted)')
        values=$(stat -L -c '%d:%i:%b:%s:%h:%F' "$descriptor") || { printf 'E\t%s\n' "$descriptor"; continue; }
        printf 'F\t%s\t%s\n' "$values" "$target";;
    esac
  done
done
printf 'S\t%s\t%s\t%s\n' "$p" "$d" "$(readlink /proc/self/ns/pid)"
'''


def pg_open_unlinked(lab, state):
    result = lab.docker('exec','-i',state['postgres'],'bash',
                        data=PG_FD_SCRIPT, timeout=10, check=False)
    files, errors, summary = {}, [], None
    for line in result.stdout.splitlines():
        fields = line.split('\t')
        if fields[0] == 'E': errors.append(line)
        elif fields[0] == 'S' and len(fields) == 4:
            require(summary is None, 'duplicate PG fd summary')
            summary = dict(processes=int(fields[1]),descriptors=int(fields[2]),pid_namespace=fields[3])
        elif fields[0] == 'F' and len(fields) == 3:
            values = fields[1].split(':',5)
            require(len(values) == 6 and values[5] in ('regular file','regular empty file'), 'PG deleted regular file')
            device,inode,blocks,size,links = map(int,values[:5])
            target = fields[2]
            relative = target[len('/var/lib/postgresql/data/'):-len(' (deleted)')]
            files[f'{device}:{inode}'] = dict(device=device,inode=inode,allocated_bytes=blocks*512,
                logical_bytes=size,links=links,target=target,classification=classify('postgres',relative))
        else: errors.append('unrecognized PG fd output')
    return dict(status='OBSERVED' if result.returncode == 0 and not errors and summary else 'UNKNOWN_PARTIAL',
        files=files,errors=errors,summary=summary,returncode=result.returncode,
        stderr=result.stderr,allocated_bytes=sum(value['allocated_bytes'] for value in files.values()))


PG_STATS_SQL = """SELECT jsonb_build_object('observed_at',clock_timestamp(),
    'wal_insert_lsn',pg_current_wal_insert_lsn(),'wal_flush_lsn',pg_current_wal_flush_lsn(),
    'checkpoint',to_jsonb(pg_control_checkpoint()),
    'bgwriter',(SELECT to_jsonb(s) FROM pg_stat_bgwriter s),
    'wal',(SELECT to_jsonb(s) FROM pg_stat_wal s))"""


def pg_stats(lab, state, database):
    return json.loads(lab.sql(state,database,'SELECT pg_stat_clear_snapshot(); '+PG_STATS_SQL))


def pg_checkpoint_verified(before, after):
    require(after['bgwriter']['checkpoints_req'] > before['bgwriter']['checkpoints_req'],
            'actual requested CHECKPOINT counter did not advance')
    def lsn(value):
        high,low = value.split('/')
        return (int(high,16)<<32)+int(low,16)
    require(lsn(after['checkpoint']['checkpoint_lsn']) > lsn(before['checkpoint']['checkpoint_lsn']),
            'actual checkpoint LSN did not advance')
    require(lsn(after['wal_insert_lsn']) >= lsn(before['wal_insert_lsn']) and
            lsn(after['wal_flush_lsn']) >= lsn(before['wal_flush_lsn']), 'WAL LSN regressed')


def checkpoint_overlap(logs, trace, sql_timestamps):
    """Require engine log start/completion and a real admission interval overlap.

    Docker timestamps describe emitted PG log records on this same host. SQL
    clock timestamps independently bracket the requested CHECKPOINT. Readiness
    and Docker launch/reap times are never the actual checkpoint interval.
    """
    def micros(text):
        return int(datetime.fromisoformat(text.replace('Z','+00:00')).timestamp()*1000000)
    lower = micros(sql_timestamps[0]['checkpoint_command_start'])
    upper = micros(sql_timestamps[1]['checkpoint_command_end'])
    pending,intervals = {},[]
    for line in logs.splitlines():
        match = re.match(r'^(\S+) .*?\[(\d+)\] LOG:\s+checkpoint (starting|complete):\s*(.*)',line)
        if not match: continue
        timestamp,pid,kind,detail = match.groups()
        at = micros(timestamp)
        if kind == 'starting' and 'force' in detail and 'wait' in detail:
            pending[pid] = at
        elif kind == 'complete' and pid in pending:
            start = pending.pop(pid)
            if lower <= start <= at <= upper:
                admissions = [item['value']['index'] for item in trace
                    if item['value']['event'] == 'physical-admitted'
                    and item['value']['admission_started_at_unix_us'] < at
                    and item['value']['observed_at_unix_us'] > start]
                intervals.append(dict(start_unix_us=start,end_unix_us=at,pid=int(pid),
                                      overlapping_admissions=admissions))
    return dict(status='WITNESSED' if any(value['overlapping_admissions'] for value in intervals)
        else 'NOT_WITNESSED',engine_intervals=intervals,sql_start_unix_us=lower,sql_end_unix_us=upper,
        scope='engine log emission times + real admission API intervals, diagnostic same-host wallclock')


def phase(case, value):
    temporary = case/'phase-tmp'
    temporary.write_text(value+'\n')
    temporary.replace(case/'phase')


def next_event(process, wanted, timeout, trace):
    deadline = time.monotonic()+timeout
    with process.condition:
        while True:
            require(not process.overflow, 'bounded diagnostic probe output')
            newline = process.stdout.find(b'\n',process.offset)
            if newline >= 0:
                raw = bytes(process.stdout[process.offset:newline])
                process.offset = newline+1
                event = json.loads(raw)
                require(isinstance(event,dict) and isinstance(event.get('event'),str), 'probe event envelope')
                trace.append(dict(received_monotonic_ns=time.monotonic_ns(),value=event))
                if event['event'] == wanted: return event
            else:
                require(time.monotonic() < deadline, 'probe gate deadline: '+wanted)
                require(process.process.poll() is None or process.threads[0].is_alive(), 'probe ended before '+wanted)
                process.condition.wait(min(.05,max(0,deadline-time.monotonic())))


def send(process, value):
    require(value in ('load','checkpoint-started','close','reopen','finish'), 'closed physical gate vocabulary')
    process.process.stdin.write((value+'\n').encode())
    process.process.stdin.flush()


def sample_summary(case):
    counts, peaks, errors = {}, {}, []
    with (case/'samples.jsonl').open() as stream:
        for line in stream:
            frame = json.loads(line)
            counts[frame['phase']] = counts.get(frame['phase'],0)+1
            if frame['open_unlinked']['status'] != 'OBSERVED': errors.append('native FD census partial')
            for role,root in frame['roots'].items():
                values = peaks.setdefault(role,dict(allocated_bytes=0,logical_bytes=0,classes={}))
                for field in ('allocated_bytes','logical_bytes'):
                    values[field] = max(values[field],sum(v[field] for v in root['classes'].values()))
                for kind,value in root['classes'].items():
                    values['classes'][kind] = max(values['classes'].get(kind,0),value['allocated_bytes'])
    return dict(phase_samples=counts, sampled_peaks=peaks, fd_coverage_errors=sorted(set(errors)),
                missing_phases=sorted(set(('bootstrap','load','checkpoint-load','automatic-work','shutdown','reopen'))-set(counts)),
                scope='sampled non-atomic diagnostic peaks; not a hard bound or reservation guarantee')


def expected_science(events, artifact_digest, source):
    """Literal extension of the maintained independent signed-import oracle.

    Keys/framing are the existing v1 event_oracle vocabulary. Inputs are signed
    source events, never native rows, receipt templates or a product serializer.
    """
    sys.path.insert(0,str(source/'experiments/ingest-bootstrap'))
    from oracle import accounted, cbor
    ds,slot = bytes([1])*16,bytes([66])*16
    be = lambda n,width=8:n.to_bytes(width,'big')
    sha = lambda value:hashlib.sha256(value).digest()
    domains = {4:bytes([64])*16,8:bytes([68])*16,9:bytes([69])*16}
    reference = lambda kind,n=1:[ds,kind,domains[kind],n]
    ref_key = lambda kind,n=1:ds+be(kind,2)+domains[kind]+be(n)
    src = lambda n:[ds,0,1,n]
    src_key = lambda n:ds+be(0,4)+be(1)+be(n)
    scope = reference(4)
    manifest = (bytes.fromhex('01010001000200010001')+ds+be(0)+bytes([61])*16+be(0,4)+be(1)
        +bytes([63])*16+bytes([67])*16+be(1)+bytes([1,3])
        +b''.join(be(k,2)+v for k,v in domains.items()))
    rows = {('default',b'\x01\x01'):manifest}
    facts,refs,requests,receipts,batches = {},{},{},{},{}
    def put(cf,key,value): rows[cf,key] = value
    def fact(sequence,kind,payload,at):
        value = b'bigbrotr/fact/1\0'+cbor([src(sequence),kind,1,scope,at,payload])
        facts[sequence],refs[sequence] = value,[src(sequence),sha(value)]
        put('facts',b'\x01\x20'+src_key(sequence),b'\x01\x20'+sha(value)+value)
    def index(tag,key,sequence):
        put('default',bytes([1,tag])+key,bytes([1,tag])+src_key(sequence)+refs[sequence][1])
    def mapping(sequence,kind,n,schema,value,at):
        fields = [1,schema,1,value]
        payload = [reference(kind,n),*fields]
        fact(sequence,'dictionary',payload,at)
        index(4,ref_key(kind,n),sequence)
        canonical = cbor(fields)
        put('default',b'\x01\x05'+ds+be(kind,2)+sha(canonical)+ref_key(kind,n),b'\x01\x05'+canonical)
        put('default',b'\x01\x03'+ds+be(kind,2)+domains[kind],b'\x01\x03'+be(n))
        return ['dictionary',1,payload]
    def finish(n,operation,intent,members,dependencies):
        original = b'bigbrotr/producer-request/1\0'+cbor([[slot,1,n],operation,1,intent])
        requests[n] = original
        batch = [refs[k] for k in members]
        receipt = b'bigbrotr/receipt/1\0'+cbor([1,[slot,1,n],sha(original),
            [ds,0,1,bytes([67])*16,1],batch,[refs[k] for k in dependencies]])
        receipts[n] = receipt
        first,last = members[0],members[-1]
        batch_digest = sha(b'bigbrotr/admission-batch/1\0'+cbor([ds,0,1,first,last,batch]))
        marker_key,marker = b'\x01\x21'+src_key(first),b'\x01\x21'+be(last)+batch_digest
        put('facts',marker_key,marker)
        put('default',b'\x01\x02'+ds+be(0,4)+be(1),b'\x01\x02'+be(last))
        put('receipts',b'\x01\x30'+slot+be(1)+be(n),receipt)
        put('receipts',b'\x01\x31'+slot+be(1),b'\x01\x31'+be(n)+be(1)+be(0))
        pending = len(marker_key)+len(marker)+sum(38+34+len(facts[k]) for k in members)
        batches[n] = dict(batch_digest=batch_digest.hex(),facts=[sha(facts[k]).hex() for k in members],
                          pending_bytes=pending)
    dictionaries = [mapping(1,4,1,'visibility',{'class':'public'},123),
        mapping(2,4,2,'visibility',{'class':'private'},123),
        mapping(3,9,1,'dataset-lineage',dict(format='nostr-jsonl',format_version=1,
            source_dataset_id=None,source_artifact_digest=bytes.fromhex(artifact_digest)),123)]
    finish(1,'admit-evidence',[1,scope,dictionaries],[1,2,3],[1,2,3])
    for n,event in enumerate(events,1):
        body = [bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],
                event['kind'],event['tags'],event['content']]
        signature = bytes.fromhex(event['sig'])
        first = 3*n+1
        mapping(first,8,n,'event',{'event_id':body[0]},456)
        fact(first+1,'event-payload',[*body,reference(8,n)],456)
        index(6,ds+body[0],first+1)
        fact(first+2,'event-eligibility',[body[0],sha(b'bigbrotr/body/1\0'+cbor(body)),
            signature,456,'import',reference(9)],456)
        index(7,ds+body[0]+ref_key(4),first+2)
        finish(n+1,'admit',[1,scope,[*body,signature],'import',reference(9),None],
               [first,first+1,first+2],[1,3,first,first+1,first+2])
    encoded = accounted((cf,key,value) for (cf,key),value in rows.items())
    native = [dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in encoded]
    originals = [[slot.hex()+'-0000000000000001-'+f'{n:016x}.cbor',
        dict(bytes=len(value),sha256=digest(value))] for n,value in requests.items()]
    return dict(native_rows=len(native),native_sha256=digest(compact(native)),originals=originals,
        requests=requests,receipts=receipts,batches=batches,
        manifest_sha256=digest(manifest),
        pending_bytes=sum(batch['pending_bytes'] for batch in batches.values()))


def verify_trace(trace, events, corpus, baseline, control, closed, reopened, complete, source):
    require(len(events) == EVENT_COUNT and len({event['id'] for event in events}) == EVENT_COUNT
            and all(len(compact(event)) == EVENT_BYTES for event in events), 'complete distinct source corpus')
    values = [item['value'] for item in trace]
    def one(name):
        found = [value for value in values if value['event'] == name]
        require(len(found) == 1,'exactly one '+name)
        return found[0]
    expected = expected_science(events,corpus['sha256'],source)
    validated = one('physical-input-validated')
    require(validated['source_sha256'] == corpus['sha256'] and validated['source_bytes'] == corpus['bytes']
        and validated['events'] == EVENT_COUNT and validated['signed_event_bytes'] == EVENT_BYTES
        and validated['absolute_request_limit'] == validated['outstanding_request_limit'] == 64
        and validated['retained_input_upper_bound'] <= PLAN['original_byte_cap']
        and expected['pending_bytes'] < validated['pending_high'], 'actual source/policy/spool preflight')
    ready = one('physical-ready')
    require(ready['profile'] == dict(block_cache_bytes=32*1024**2,write_buffer_manager_bytes=16*1024**2,
        per_cf_write_buffer_bytes=2*1024**2,max_total_wal_bytes=16*1024**2,max_background_jobs=2,
        max_open_files=64,native_allowance_bytes=128*1024**2), 'exact installed physical native profile')
    require(ready['receipt'] == expected['receipts'][1].hex() and ready['pending'] == expected['batches'][1],
            'independent complete bootstrap receipt and science')
    require(ready['credit'] == dict(issued=1,acknowledged=1,outstanding=0,retired=0), 'real bootstrap ACK1')
    require(one('physical-checkpoint-ready')['admitted_events'] == 31
        and one('physical-load-complete')['admitted_events'] == EVENT_COUNT, 'fixed load schedule')
    admitted = [value for value in values if value['event'] == 'physical-admitted']
    require(len(admitted) == EVENT_COUNT, 'all63 actual admissions')
    for index,(observed,event) in enumerate(zip(admitted,events),1):
        original,receipt = expected['requests'][index+1],expected['receipts'][index+1]
        require(observed['index'] == index and observed['request_sequence'] == index+1
            and observed['event_id'] == event['id'] and observed['recorded_at'] == 456
            and observed['request_digest'] == digest(original) and observed['input_bytes'] == len(original)
            and observed['receipt'] == receipt.hex() and observed['outcome_sha256'] == digest(receipt)
            and observed['pending'] == expected['batches'][index+1]
            and observed['line'] == index and observed['byte_start'] == (index-1)*(EVENT_BYTES+1)
            and observed['byte_end'] == index*(EVENT_BYTES+1)
            and observed['record_sha256'] == digest(compact(event)+b'\n')
            and observed['credit'] == dict(issued=index+1,acknowledged=1,outstanding=index,retired=0),
            'independent signed request/receipt/pending/credit/position '+str(index))
    require(closed['state'] == reopened['state'] and reopened['same_native_originals_outcomes_control'] is True,
            'exact native/PG/original state across ordinary reopen')
    for key in ('native_rows','native_sha256','originals'):
        require(closed['state'][key] == expected[key], 'independent complete native/input '+key)
    internal = {name:json.loads(rows) for name,rows in closed['state']['control_tables']}
    external = json.loads(json.dumps(control))
    for rows in external.values():
        for row in rows: row['xmin'] = row.pop('lab_xmin')
    ordered = lambda tables:{name:sorted(rows,key=lambda row:json.dumps(row,sort_keys=True)) for name,rows in tables.items()}
    require(ordered(internal) == ordered(external), 'independent complete current Control rows/xmins')
    require(control['jobs'] == baseline['jobs'], 'no synthetic acquisition jobs')
    producer, = control['producer_epochs']
    require(producer['producer_slot_id'] == '\\x'+'42'*16 and producer['producer_epoch'] == 1
        and producer['dataset_id'] == '\\x'+'01'*16 and producer['outstanding_request_limit'] == 64
        and producer['reserved_through_request'] == 64 and producer['state'] == 'open', 'actual bounded producer')
    for name in ('producer_requests','producer_outcomes'):
        require(len(control[name]) == 64, 'complete PG64 '+name)
        require({row['request_sequence'] for row in control[name]} == set(range(1,65)), 'whole PG request sequence set')
        for row in control[name]:
            n = row['request_sequence']
            require(1 <= n <= 64 and row['producer_slot_id'] == '\\x'+'42'*16
                and row['producer_epoch'] == 1 and row['dataset_id'] == '\\x'+'01'*16
                and row['manifest_sha256'] == '\\x'+expected['manifest_sha256']
                and row['request_digest'] == '\\x'+digest(expected['requests'][n]), 'bounded PG request identity')
            if name == 'producer_requests':
                require(row['acquisition_job_id'] is None and row['request_digest'] == '\\x'+digest(expected['requests'][n])
                    and row['input_bytes'] == len(expected['requests'][n]), 'PG original binding')
            else:
                require(row['outcome_bytes'] == '\\x'+expected['receipts'][n].hex()
                    and row['outcome_sha256'] == '\\x'+digest(expected['receipts'][n]), 'PG outcome copy binding')
    require(complete['credit'] == dict(issued=64,acknowledged=1,outstanding=63,retired=0)
        and complete['admitted_events'] == 63 and complete['source_bound_event_acknowledgement'] == 'NOT_RUN'
        and all(complete[key] == 'NOT_RUN' for key in ('reclamation','reserve_qualification','open65')),
        'bounded diagnostic scope')
    return dict(native_rows=expected['native_rows'],pending_bytes=expected['pending_bytes'],
                original_bytes=sum(len(value) for value in expected['requests'].values()))


def run(state, output, contents, record, prepare, call, inventory, source):
    """Two fresh current databases/native roots; existing lab owns cleanup."""
    import lab
    from fault_cases import Process
    activation = ('--request-id','04'*16,'--expected-revision','1')
    results = []
    lab.write_json(output/'physical-plan.json',dict(format='bigbrotr-physical-envelope-plan/1',**preflight(source)))
    for label in CASES:
        name = 'physical_'+label
        case = output/name
        case.mkdir(mode=0o700)
        events, corpus = make_corpus(case/'source.jsonl',label,source)
        lab.write_json(case/'source-oracle.json',corpus)
        phase(case,'preparation')
        database,config,local = prepare(name,'managed')
        call(name+'-initialize',database,'initialize',config)
        call(name+'-activate',database,'activate-initial',config,extra=activation)
        baseline = contents(database)
        lab.write_json(case/'control-before.json',baseline)
        accepted, = baseline['policy_revisions']
        settings = accepted['document']['settings']
        require(settings['import.max_event_bytes'] == settings['ingest.max_event_bytes'] == EVENT_BYTES,
                'accepted complete event envelope')
        require(settings['ingest.request_receipt_retention'] == 64, 'unchanged receipt retention64')
        require(settings['ingest.producer_outstanding_requests'] == 64, 'accepted outstanding64')
        # Conservative predeclared preflight, separately checked against exact
        # canonical originals/P by the independent oracle below. No green tuning.
        upper = EVENT_COUNT*(EVENT_BYTES+16384)+65536
        high = int(Decimal(settings['ingest.pending_max_bytes'])*Decimal(str(settings['ingest.pending_high_fraction'])))
        require(upper+64*65536 < PLAN['original_byte_cap'] and upper < high,
                'declared63-unit corpus exceeds accepted original/pending envelope')
        lab.write_json(case/'accepted-preflight.json',dict(settings=settings,
            conservative_original_and_pending_upper=upper,accepted_pending_high=high,
            original_byte_cap=PLAN['original_byte_cap'],original_file_cap=PLAN['original_file_cap']))
        remote_case = '/lab/'+name
        sampler = Process(['docker','exec',state['client'],'python3','-B',
            '/source/experiments/policy-versions/physical_envelope.py','--sample-container',remote_case],
            limit=PLAN['maximum_output_bytes'])
        probe = None
        checkpoint = None
        checkpoint_times = {}
        trace, pg_fd_frames, thread_errors = [], [], []
        stop = threading.Event()
        started = time.monotonic_ns()
        def pg_fds():
            try:
                while not stop.is_set():
                    pg_fd_frames.append(dict(monotonic_ns=time.monotonic_ns(),
                        phase=(case/'phase').read_text().strip(),observation=pg_open_unlinked(lab,state)))
                    require(len(pg_fd_frames) <= 320, 'PG fd sample cap')
                    stop.wait(PLAN['pg_fd_interval_ms']/1000)
            except BaseException as error: thread_errors.append(repr(error))
        pg_thread = threading.Thread(target=pg_fds)
        pg_thread.start()
        result = dict(case=label,status='FAILED_OR_UNKNOWN',plan=PLAN)
        try:
            phase(case,'bootstrap')
            probe = Process(['docker','exec','-i',state['client'],'/build/ingest_bootstrap_probe',
                'physical-growth-diagnostic',database,'/lab/secrets/application',remote_case+'/store',
                remote_case+'/source.jsonl',corpus['sha256'],str(corpus['bytes']),str(EVENT_COUNT)],
                input_pipe=True,limit=PLAN['maximum_output_bytes'])
            ready = next_event(probe,'physical-ready',PLAN['ready_seconds'],trace)
            lab.write_json(case/'ready.json',ready)
            phase(case,'load')
            send(probe,'load')
            next_event(probe,'physical-checkpoint-ready',PLAN['load_seconds'],trace)
            before = pg_stats(lab,state,database)
            phase(case,'checkpoint-load')
            checkpoint_times['launch_ns'] = time.monotonic_ns()
            checkpoint = Process(['docker','exec','-i',state['postgres'],'psql','-X','-A','-t','-q',
                '--no-password','-v','ON_ERROR_STOP=1','-U','postgres','-d',database],input_pipe=True,
                limit=65536)
            checkpoint.process.stdin.write(("SET application_name='physical-envelope-checkpoint';\n"
                "SELECT jsonb_build_object('checkpoint_command_start',clock_timestamp());\nCHECKPOINT;\n"
                "SELECT jsonb_build_object('checkpoint_command_end',clock_timestamp());\n").encode())
            checkpoint.process.stdin.close()
            # Launch and release immediately. Actual interval overlap is assessed
            # independently; this gate alone never proves it.
            send(probe,'checkpoint-started')
            next_event(probe,'physical-load-complete',PLAN['load_seconds'],trace)
            automatic_deadline = time.monotonic()+PLAN['automatic_work_seconds']
            phase(case,'automatic-work')
            require(checkpoint.wait(15) == 0 and not checkpoint.stderr, 'real PostgreSQL CHECKPOINT failed')
            checkpoint_times['reaped_ns'] = time.monotonic_ns()
            after = pg_stats(lab,state,database)
            stats_deadline = time.monotonic()+PLAN['checkpoint_stats_seconds']
            while after['bgwriter']['checkpoints_req'] <= before['bgwriter']['checkpoints_req'] \
                    and time.monotonic() < stats_deadline:
                time.sleep(.1)
                after = pg_stats(lab,state,database)
            pg_checkpoint_verified(before,after)
            checkpoint_events = [json.loads(line) for line in bytes(checkpoint.stdout).decode().splitlines()]
            require(len(checkpoint_events) == 2, 'actual CHECKPOINT timestamp pair')
            lab.write_json(case/'checkpoint.json',dict(before=before,after=after,
                command_timestamps=checkpoint_events,observer=checkpoint_times))
            pg_logs = lab.docker('logs','--timestamps',state['postgres'])
            pg_log_text = pg_logs.stdout+pg_logs.stderr
            (case/'postgres-checkpoint.log').write_text(pg_log_text)
            overlap = checkpoint_overlap(pg_log_text,trace,checkpoint_events)
            lab.write_json(case/'checkpoint-overlap.json',overlap)
            # Exactly the declared live interval, even if no compaction occurs.
            while time.monotonic() < automatic_deadline:
                require(probe.process.poll() is None and sampler.process.poll() is None
                        and not thread_errors, 'live diagnostic observer/probe ended')
                time.sleep(min(.1,max(0,automatic_deadline-time.monotonic())))
            (case/'freeze-logs').write_text('freeze\n')
            freeze_deadline = time.monotonic()+2
            while not (case/'logs-frozen.json').exists():
                require(sampler.process.poll() is None and time.monotonic() < freeze_deadline,
                        'live writer LOG capture barrier')
                time.sleep(.02)
            witness = log_witness(case/'rocks-log-capture')
            lab.write_json(case/'live-background-witness.json',witness)
            phase(case,'shutdown')
            send(probe,'close')
            closed = next_event(probe,'physical-closed',PLAN['gate_seconds'],trace)
            time.sleep(.2)
            phase(case,'reopen')
            send(probe,'reopen')
            reopened = next_event(probe,'physical-reopened',PLAN['gate_seconds'],trace)
            current_control = contents(database)
            time.sleep(.2)
            send(probe,'finish')
            complete = next_event(probe,'physical-complete',PLAN['gate_seconds'],trace)
            require(probe.wait(10) == 0 and not probe.stderr, 'probe completed and reaped normally')
            require(time.monotonic_ns()-started < PLAN['case_seconds']*10**9, 'whole case deadline')
            science = verify_trace(trace,events,corpus,baseline,current_control,closed,reopened,complete,source)
            phase(case,'after')
            result.update(status='CAPTURED_DIAGNOSTIC',witness=witness,complete=complete,science=science,
                          checkpoint_overlap=overlap)
        except BaseException as error:
            result['error'] = repr(error)
            raise
        finally:
            phase(case,'after')
            stop.set()
            pg_thread.join(12)
            (case/'sampler-stop').write_text('stop\n')
            try: result['sampler_returncode'] = sampler.wait(10)
            except BaseException as error: result['sampler_error'] = repr(error)
            for label_process,process in [('probe',probe),('checkpoint',checkpoint),('sampler',sampler)]:
                if process is None: continue
                process.stop()
                (case/(label_process+'.stdout')).write_bytes(bytes(process.stdout))
                (case/(label_process+'.stderr')).write_bytes(bytes(process.stderr))
            lab.write_json(case/'trace.json',trace)
            lab.write_json(case/'pg-open-unlinked.json',pg_fd_frames)
            result.update(pg_fd_thread_joined=not pg_thread.is_alive(),pg_fd_thread_errors=thread_errors,
                pg_fd_coverage='OBSERVED' if pg_fd_frames and all(frame['observation']['status'] == 'OBSERVED'
                    for frame in pg_fd_frames) else 'UNKNOWN_PARTIAL', elapsed_ns=time.monotonic_ns()-started)
            if (case/'samples.jsonl').exists(): result['samples'] = sample_summary(case)
            if result['status'] == 'CAPTURED_DIAGNOSTIC' and (result['pg_fd_coverage'] != 'OBSERVED'
                    or result.get('samples',{}).get('fd_coverage_errors')
                    or result.get('samples',{}).get('missing_phases')
                    or result['checkpoint_overlap']['status'] != 'WITNESSED'
                    or any(result['witness'][key] != 'WITNESSED'
                           for key in ('automatic_flush','nontrivial_automatic_compaction'))):
                result['status'] = 'CAPTURED_DIAGNOSTIC_WITH_GAPS'
            lab.write_json(case/'result.json',result)
        require(result['sampler_returncode'] == 0 and not result['pg_fd_thread_errors']
                and result['pg_fd_thread_joined'], 'complete sampler custody')
        record(name,True,result=result)
        results.append(result)
    return dict(status='CAPTURED_PHYSICAL_DIAGNOSTIC_WITH_GAPS' if any(
        result['status'].endswith('_WITH_GAPS') for result in results) else 'CAPTURED_PHYSICAL_DIAGNOSTIC',
        cases=results,plan=PLAN,
        not_run=['numeric foreground d/completion R guarantee or enforced filesystem funding',
            'OPEN65 or greater-than64 history; payload/original/receipt reclamation',
            'event ACK64 (direct imports have no acquisition_job_id); source-job ACK/reclaim',
            'ENOSPC/fsync failure, multiwriter admission, archive/index completion lanes',
            'full capacity, aged state, arm64, release'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample-container', type=Path, required=True)
    sample_container(parser.parse_args().sample_container)
