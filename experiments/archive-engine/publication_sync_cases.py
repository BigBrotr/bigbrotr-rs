#!/usr/bin/env python3
"""Owned exact-Git native syscall evidence; not a power-loss qualification.

The real production publication function is invoked by the compiled Rust tests.
strace replaces one selected fsync result with EIO/ENOSPC; it does not manufacture
an actual device writeback failure. Full signed PG journeys run separately.
"""
from collections import namedtuple
import ast
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import stat
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha


Call = namedtuple('Call', 'pid body entered completed')


def decoded_lines(text):
    """Retain both trace positions across interleaved syscall enter/return lines."""
    pending, result = {}, []
    for position, line in enumerate(text.splitlines()):
        match = re.match(r'^(\d+)\s+\d+\.\d+\s+(.*)$', line)
        if not match:
            continue
        pid, body = match.groups()
        if body.endswith(' <unfinished ...>'):
            assert pid not in pending
            pending[pid] = (body.removesuffix(' <unfinished ...>'), position)
        elif body.startswith('<... '):
            prefix, entered = pending.pop(pid)
            result.append(Call(pid, prefix+body.split('resumed>', 1)[1], entered, position))
        else:
            result.append(Call(pid, body, position, position))
    assert not pending
    return sorted(result, key=lambda call: call.entered)


def ordering_evidence(lines, file):
    opening = [c for c in lines if 'openat(' in c.body and ('"'+file+'"') in c.body and 'O_EXCL' in c.body]
    assert len(opening) == 1
    original = opening[0]
    fd = re.search(r'= (\d+)<', original.body).group(1)
    sync = [c for c in lines if c.pid == original.pid and c.body.startswith(f'fsync({fd}<{file}>)')]
    assert len(sync) == 1 and sync[0].body.endswith('= 0')
    assert sync[0].entered > original.completed
    # Native threads share the descriptor table; a different TID can close it.
    assert not any(c.body.startswith(f'close({fd}<') and
                   original.entered < c.entered <= sync[0].completed for c in lines)
    expected = [str(Path(file).parents[i]) for i in range(4)]
    following = [c for c in lines if c.entered > sync[0].completed and
                 c.pid == original.pid and c.body.startswith('fsync(')]
    assert len(following) >= 4
    previous = sync[0]
    for call, parent in zip(following[:4], expected):
        assert call.entered > previous.completed
        assert re.fullmatch(r'fsync\(\d+<'+re.escape(parent)+r'>\)\s+= 0', call.body)
        previous = call
    # Registration must START reading after the last directory sync returns.
    reads = [c for c in lines if 'openat(' in c.body and ('"'+file+'"') in c.body and 'O_RDONLY' in c.body]
    assert reads and min(c.entered for c in reads) > previous.completed
    return dict(path=file, retained_fd=int(fd), synchronized_parents=expected)


def check_oracle():
    file = '/objects/main/events/prepared-09/data.parquet'
    prefix = [f'101 1.0 openat(AT_FDCWD, "{file}", O_RDWR|O_CREAT|O_EXCL, 0600) = 9<{file}>',
              f'101 1.1 fsync(9<{file}>) = 0']
    parents = [f'101 1.2 fsync({i+10}<{Path(file).parents[i]}>) = 0' for i in range(4)]
    start = f'102 1.3 openat(AT_FDCWD, "{file}", O_RDONLY <unfinished ...>'
    end = f'102 1.4 <... openat resumed>) = 20<{file}>'
    valid = prefix+parents+[start, end]
    assert ordering_evidence(decoded_lines('\n'.join(valid)), file)['retained_fd'] == 9
    # Entry before the last sync, completion afterwards, previously passed.
    early_read = prefix+parents[:3]+[start, parents[3], end]
    cross_thread_close = [prefix[0], f'102 1.1 close(9<{file}>) = 0', *prefix[1:], *parents, start, end]
    renamed_close = [line.replace(file+'>) = 0', file+' (deleted)>) = 0')
                     if 'close(' in line else line for line in cross_thread_close]
    for invalid in [early_read, cross_thread_close, renamed_close]:
        try:
            ordering_evidence(decoded_lines('\n'.join(invalid)), file)
        except AssertionError:
            continue
        raise AssertionError('trace oracle accepted the negative control')
    print('Trace oracle: positive and 3 discriminating negative controls PASS')


ACK_TEST = ('storage::rocks::store::admission_tests::producer_ack_tests::'
            'native_ack_fsync_boundary')
ACK_KEY = bytes.fromhex('0131') + bytes([5])*16 + (1).to_bytes(8, 'big')
ACK_VALUE = bytes.fromhex('0131') + (1).to_bytes(8, 'big')*2 + bytes(8)
# initialize puts the manifest once; bootstrap puts it again plus eight other
# entries. Nine distinct final rows therefore represent ten native puts.
ACK_BEFORE_SEQUENCE = 10
ACK_AFTER_SEQUENCE = 11


def ack_record():
    # Closed fixture grammar, from the pinned RocksDB WriteBatch/WAL format:
    # sequence LE64, count LE32, CF-put tag5, receipts CF2, short key/value.
    payload = ACK_AFTER_SEQUENCE.to_bytes(8, 'little') + (1).to_bytes(4, 'little')
    payload += bytes([5, 2, len(ACK_KEY)]) + ACK_KEY + bytes([len(ACK_VALUE)]) + ACK_VALUE
    crc = 0xffffffff
    for byte in b'\x01' + payload:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    crc ^= 0xffffffff
    masked = (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff
    return masked.to_bytes(4, 'little') + len(payload).to_bytes(2, 'little') + b'\x01' + payload


def ack_suffix(before, after):
    # Equality includes masked CRC32C, framing, entire batch and exhaustion.
    # No replayed/product-generated value is used as the expected suffix.
    assert 0 < len(before) < len(after) < 32768
    assert after == before + ack_record(), 'not the one exact ACK WAL record'


def traced_write(call):
    match = re.fullmatch(
        r'(write|pwrite64)\((\d+)<([^>]+)>, ("(?:\\.|[^"\\])*"), (\d+)'
        r'(?:, (\d+))?\)\s+= (\d+)', call.body)
    if not match:
        return None
    operation, fd, path, encoded, count, offset, returned = match.groups()
    data = ast.literal_eval(encoded).encode('latin1')
    assert len(data) == int(count) == int(returned), 'short/truncated trace write'
    return dict(operation=operation, fd=int(fd), path=path, data=data,
                offset=None if offset is None else int(offset))


def ack_trace(lines, wal, descriptor, before, after, fault):
    ack_suffix(before, after)
    writes = [(call, traced_write(call)) for call in lines]
    def one(values):
        assert len(values) == 1
        return values[0]
    ready = one([c for c, w in writes if w and w['fd'] == 1 and b'"event":"ACK_READY"' in w['data']])
    begin = one([c for c, w in writes if w and w['fd'] == 1 and b'"event":"ACK_BEGIN"' in w['data']])
    result = one([c for c, w in writes if w and w['fd'] == 1 and b'"event":"ACK_RESULT"' in w['data']])
    result_bytes = next(w['data'] for c, w in writes if c == result)
    event = json.loads(result_bytes)
    if fault:
        assert event == dict(event='ACK_RESULT', result='CommitUnknown',
                             credit='CommitUnknown', capacity='CommitUnknown')
    else:
        assert event['result'] == 'Ok'
    exits = [c for c in lines if c.body.startswith('exit_group(0)')]
    assert len(exits) == 1 and exits[0].entered > result.completed
    ending = exits[0].entered
    assert result.pid == begin.pid == ready.pid and ready.completed < begin.entered
    sync = one([c for c in lines if begin.completed < c.entered < result.entered
                and re.match(r'fsync\('+str(descriptor)+r'<'+re.escape(wal)+r'>\)', c.body)])
    assert sync.pid == begin.pid and sync.completed < result.entered
    if fault:
        assert re.fullmatch(r'fsync\(\d+<'+re.escape(wal)+
                            r'>\)\s+= -1 EIO .*\(INJECTED\)', sync.body)
        assert [c for c in lines if '(INJECTED)' in c.body] == [sync]
    else:
        assert sync.body.endswith('= 0') and not any('(INJECTED)' in c.body for c in lines)
    selected = [(c, w) for c, w in writes if w and w['path'] == wal
                and begin.completed < c.entered < sync.entered]
    assert selected and all(c.pid == begin.pid and c.completed < sync.entered
                            and w['fd'] == descriptor for c, w in selected)
    offset = len(before)
    for _, w in selected:
        assert w['offset'] in (None, offset)
        offset += len(w['data'])
    assert b''.join(w['data'] for _, w in selected) == ack_record()
    # Across all TIDs: no descriptor replacement, path mutation, alternative
    # write syscall, later append or later sync can explain this result.
    for c in lines:
        if not (c.completed > ready.completed and c.entered < ending):
            continue
        b = c.body
        if re.match(r'(close|dup|dup2|dup3|fcntl|lseek|ftruncate)\('+str(descriptor)+r'[<,)]', b):
            raise AssertionError('ACK WAL descriptor changed during interval')
        assert not re.match(r'(close_range|dup|dup2|dup3)\(', b)
        if wal in b and re.match(r'(open|rename|unlink|link|symlink|truncate)', b):
            raise AssertionError('ACK WAL path changed during interval')
        # A native LOG write can mention the failed WAL in its diagnostic
        # payload. Only the syscall's leading descriptor identifies its target.
        target = re.match(r'([a-z0-9_]+)\(\d+<([^>]+)>(?:,|\))', b)
        operation, target_path = target.groups() if target else (None, None)
        if target_path == wal and operation in ('writev', 'pwritev', 'pwritev2', 'fdatasync'):
            raise AssertionError('unselected ACK WAL operation')
        if (target_path is not None and c.entered > sync.entered
                and re.fullmatch(re.escape(str(Path(wal).parent))+r'/[0-9]+\.log(?: \(deleted\))?', target_path)):
            assert operation not in ('write', 'pwrite64', 'writev', 'pwritev', 'pwritev2', 'fsync', 'fdatasync'), 'later ACK WAL write/sync'
    index = len([c for c in lines if c.pid == sync.pid and c.entered <= sync.entered
                 and c.body.startswith('fsync(')])
    assert index > 1, 'selector must discriminate earlier setup syncs'
    return dict(tid=int(sync.pid), fsync_index=index, fd=descriptor,
                begin=begin.entered, append=[c.entered for c, _ in selected],
                sync=sync.entered, result=result.entered, exit=ending)


def ack_oracle_controls():
    # Construct a valid complete input first, then mutate it outside the caught
    # verifier. Constructor errors never count as negative controls.
    wal, fd, prefix = '/case/db/000004.log', 9, b'prior-complete-records'
    suffix = ack_record()
    quoted = lambda b: '"'+''.join('\\x'+format(x, '02x') for x in b)+'"'
    begin = b'\n{"event":"ACK_BEGIN"}\n'
    ready = b'\n{"event":"ACK_READY"}\n'
    end = b'\n{"event":"ACK_RESULT","result":"CommitUnknown","credit":"CommitUnknown","capacity":"CommitUnknown"}\n'
    raw = [
        f'11 0.9 write(1<pipe:[1]>, {quoted(ready)}, {len(ready)}) = {len(ready)}',
        '11 1.0 fsync(8</case/db/MANIFEST-000001>) = 0',
        f'11 1.1 write(1<pipe:[1]>, {quoted(begin)}, {len(begin)}) = {len(begin)}',
        f'11 1.2 write({fd}<{wal}>, {quoted(suffix)}, {len(suffix)}) = {len(suffix)}',
        f'11 1.3 fsync({fd}<{wal}>) = -1 EIO (Input/output error) (INJECTED)',
        f'11 1.4 write(1<pipe:[1]>, {quoted(end)}, {len(end)}) = {len(end)}',
        '11 1.5 exit_group(0) = ?',
    ]
    verify = lambda r, after=prefix+suffix: ack_trace(decoded_lines('\n'.join(r)), wal, fd, prefix, after, True)
    assert verify(raw)['fsync_index'] == 2
    controls = {}
    diagnostic = 'While fsync: '+wal+': Input/output error\n'
    # Keep the literal path in the trace string: hex-escaping every payload byte
    # would not reproduce the old substring false refusal.
    log_write = f'12 1.35 write(3</case/db/LOG>, {json.dumps(diagnostic)}, {len(diagnostic)}) = {len(diagnostic)}'
    assert wal in log_write
    assert verify([*raw[:5], log_write, *raw[5:]])['fsync_index'] == 2
    controls['log_diagnostic_mentions_wal'] = 'ACCEPTED'
    log_vector = f'12 1.35 writev(3</case/db/LOG>, [{{iov_base={json.dumps(diagnostic)}, iov_len={len(diagnostic)}}}], 1) = {len(diagnostic)}'
    assert verify([*raw[:5], log_vector, *raw[5:]])['fsync_index'] == 2
    controls['log_vector_mentions_wal'] = 'ACCEPTED'
    mutations = {
        'setup_sync': [raw[0], raw[1].replace('= 0', '= -1 EIO (INJECTED)'), *raw[2:4],
                       raw[4].replace('-1 EIO (Input/output error) (INJECTED)', '0'), *raw[5:]],
        'early_sync': [*raw[:3], raw[4], raw[3], *raw[5:]],
        'cross_thread_close': [*raw[:4], f'12 1.25 close({fd}<{wal}>) = 0', *raw[4:]],
        'dup_replacement': [*raw[:4], f'12 1.25 dup2(8</other>, {fd}<{wal}>) = {fd}<{wal}>', *raw[4:]],
        'later_sync': [*raw[:5], f'12 1.35 fsync({fd}<{wal}>) = 0', *raw[5:]],
        'later_append': [*raw[:5], raw[3].replace('11 1.2', '12 1.35'), *raw[5:]],
        'later_vector_append': [*raw[:5], log_vector.replace('3</case/db/LOG>', f'{fd}<{wal}>'), *raw[5:]],
        'ordinary_error': [s.replace(' (INJECTED)', '') for s in raw],
        'different_tid': [s.replace('11 1.3', '12 1.3') for s in raw],
    }
    for name, invalid in mutations.items():
        try:
            verify(invalid)
        except AssertionError:
            controls[name] = 'REJECTED'
        else:
            raise AssertionError('accepted trace counterexample '+name)
    # Full-record equality rejects every corruption, including header/count/CF,
    # key/value, checksum, and unconsumed trailing data.
    for index in [0, 4, 6, 7, 15, 19, 20, 22, len(suffix)-1]:
        damaged = bytearray(suffix)
        damaged[index] ^= 1
        try:
            ack_suffix(prefix, prefix+damaged)
        except AssertionError:
            controls['record_byte_'+str(index)] = 'REJECTED'
        else:
            raise AssertionError('accepted changed record')
    try:
        ack_suffix(prefix, prefix+suffix+b'\x00')
    except AssertionError:
        controls['trailing_byte'] = 'REJECTED'
    else:
        raise AssertionError('accepted trailing record byte')
    return controls


def native_ack(root):
    """Same owned build container; one native child at a time, no PostgreSQL."""
    source = root/'build/source'
    assert root == Path('/out') and Path(__file__).resolve().parents[2] == source
    manifest_path = root/'build/source-manifest.json'
    manifest = json.loads(manifest_path.read_text())
    build = json.loads((root/'artifacts/ack-fault-build.json').read_text())
    binary = root/'artifacts/native_components'
    tracer = Path('/instrument/strace-exit-fixed')
    assert build['status'] == 'PASS_PRODUCER_ACK_FAULT_BUILD_ONLY'
    assert build['source'] == manifest['revision'] and build['tree'] == manifest['tree']
    assert build['source_manifest_sha256'] == sha(manifest_path)
    assert build['executables_sha256'] == {'native_components': sha(binary)}
    assert sha(tracer) == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
    assert all(sha(source/p) == digest for p, digest in manifest['files_sha256'].items())
    fixture = json.loads((source/'tests/fixtures/ingest/staged-bootstrap-crash.json').read_text())
    before_rows = fixture['after']
    after_rows = [[cf, key, ACK_VALUE.hex() if cf == 'receipts' and key == ACK_KEY.hex() else value]
                  for cf, key, value in before_rows]
    assert len(before_rows) == 9 and after_rows != before_rows
    original = bytes.fromhex(fixture['request_hex'])
    home = root/'ack-sync-cases'
    home.mkdir()
    result = dict(status='INCOMPLETE', source=build['source'], tree=build['tree'],
                  source_manifest_sha256=sha(manifest_path), build_sha256=sha(root/'artifacts/ack-fault-build.json'),
                  binary_sha256=sha(binary), tracer_sha256=sha(tracer), cases=[],
                  scope='Native private test target, real ACK write with substituted fsync EIO; no SQL/device/power-loss claim')
    def save():
        (home/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    def events(output):
        return [json.loads(line) for line in output.split('\n')[:-1] if line.startswith('{')]
    def files(path):
        found = {}
        for p in [path, *sorted(path.rglob('*'))]:
            s = p.lstat()
            assert stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode)
            assert s.st_nlink == 1 if stat.S_ISREG(s.st_mode) else True
            found[str(p.relative_to(path))] = dict(mode=s.st_mode, device=s.st_dev,
                inode=s.st_ino, links=s.st_nlink, bytes=s.st_size,
                sha256=sha(p) if stat.S_ISREG(s.st_mode) else None)
        return found
    def run(mode, selected=None):
        case = dict(name=mode, status='INCOMPLETE', native_exited=False,
                    enclosing_container_audit_required=True)
        result['cases'].append(case)
        save()
        directory = home/mode
        directory.mkdir()
        trace_path = directory/'native.trace'
        trace = [str(tracer), '-f', '-yy', '-ttt', '-s', '65536', '-o', str(trace_path), '-e',
                 'trace=fsync,fdatasync,openat,close,close_range,dup,dup2,dup3,fcntl,lseek,ftruncate,truncate,rename,renameat,renameat2,unlink,unlinkat,link,linkat,symlink,symlinkat,write,pwrite64,writev,pwritev,pwritev2,exit_group']
        if selected:
            trace += ['-e', 'inject=fsync:error=EIO:when='+str(selected)]
        argv = ['env', 'BIGBROTR_ACK_FAULT_HOME='+str(directory), 'BIGBROTR_ACK_FAULT_MODE='+mode,
                *trace, str(binary), ACK_TEST, '--ignored', '--exact', '--nocapture', '--test-threads=1']
        case['command'] = argv
        owned = {}
        def inspect_ready(process):
            deadline = time.monotonic()+10
            with process.condition:
                while True:
                    ready = [e for e in events(bytes(process.stdout).decode()) if e['event'] == 'ACK_READY']
                    if ready:
                        break
                    assert not process.overflow and process.process.poll() is None
                    assert time.monotonic() < deadline, 'ACK ready deadline'
                    process.condition.wait(.01)
            assert len(ready) == 1
            ready = ready[0]
            assert ready['native'] == dict(sequence=ACK_BEFORE_SEQUENCE, rows=before_rows)
            pid = ready['pid']
            status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines())
            assert int(status['PPid']) == int(status['TracerPid']) == process.process.pid
            owned.update(pid=pid, pidfd=os.pidfd_open(pid), tracer=process.process.pid)
            case['process'] = dict(pid=pid, tracer=process.process.pid,
                                   stat=Path(f'/proc/{pid}/stat').read_text(),
                                   tasks={p.name: (p/'stat').read_text()
                                          for p in Path(f'/proc/{pid}/task').iterdir()})
            wal, = (directory/'db').glob('*.log')
            s = wal.lstat()
            assert stat.S_ISREG(s.st_mode) and s.st_nlink == 1
            fds = []
            for p in Path(f'/proc/{pid}/fd').iterdir():
                try:
                    descriptor = p.stat()
                except FileNotFoundError:
                    continue  # a background descriptor, never assumed to be our WAL
                if (descriptor.st_dev, descriptor.st_ino) == (s.st_dev, s.st_ino):
                    fds.append(p)
            fd, = fds
            fdinfo = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/fdinfo/{fd.name}').read_text().splitlines())
            assert int(fdinfo['pos']) == s.st_size
            assert int(fdinfo['flags'], 8) & os.O_ACCMODE in (os.O_WRONLY, os.O_RDWR)
            prefix = wal.read_bytes()
            assert 0 < len(prefix) < 32768
            (directory/'before.wal').write_bytes(prefix)
            spool = directory/'db/producer-inputs-v1'
            input_path, = spool.iterdir()
            assert input_path.name == '05'*16+'-0000000000000001-0000000000000001.cbor'
            assert input_path.read_bytes() == original
            case['inputs_before'] = files(spool)
            case['wal'] = dict(path=str(wal), fd=int(fd.name), device=s.st_dev,
                               inode=s.st_ino, bytes=s.st_size, sha256=sha(wal), fdinfo=fdinfo)
            save()
            (directory/'continue').write_text('observed\n')
        try:
            execute(argv, 20, case, interact=inspect_ready)
            assert case['exit'] == 0 and not case['overflow']
            assert owned and select.select([owned['pidfd']], [], [], 0)[0], 'native child not exited'
            case['native_exited'] = True
            case['remote_exclusion'] = 'OWNED_PIDFD_EXITED_AND_TRACER_JOINED'
            assert trace_path.stat().st_size < 8*1024*1024
            case['trace_sha256'] = sha(trace_path)
            observed = events(case['stdout'])
            assert [e['event'] for e in observed] == ['ACK_READY', 'ACK_BEGIN', 'ACK_RESULT']
            wal = Path(case['wal']['path'])
            s = wal.lstat()
            assert (s.st_dev, s.st_ino) == (case['wal']['device'], case['wal']['inode'])
            prefix, post = (directory/'before.wal').read_bytes(), wal.read_bytes()
            (directory/'after.wal').write_bytes(post)
            case['physical_before_reopen'] = files(directory/'db')
            shutil.copytree(directory/'db', directory/'preserved-db')
            assert files(directory/'db') == case['physical_before_reopen']
            case['preserved_files'] = files(directory/'preserved-db')
            contents = lambda rows: {name: (r['mode'], r['bytes'] if r['sha256'] else None, r['sha256'])
                                     for name, r in rows.items()}
            assert contents(case['preserved_files']) == contents(case['physical_before_reopen'])
            assert files(directory/'db/producer-inputs-v1') == case['inputs_before']
            case['selector'] = ack_trace(decoded_lines(trace_path.read_text()), str(wal),
                                        case['wal']['fd'], prefix, post, mode == 'eio')
            assert str(case['selector']['tid']) in case['process']['tasks']
            if selected:
                assert case['selector']['fsync_index'] == selected
            else:
                assert observed[-1]['native'] == dict(sequence=ACK_AFTER_SEQUENCE, rows=after_rows)
            save()
            reopened = {}
            reader = ['env', 'BIGBROTR_ACK_FAULT_HOME='+str(directory), 'BIGBROTR_ACK_FAULT_MODE=reopen',
                      str(binary), ACK_TEST, '--ignored', '--exact', '--nocapture', '--test-threads=1']
            case['reopen'] = reopened
            reopened['command'] = reader
            execute(reader, 15, reopened)
            assert reopened['exit'] == 0 and not reopened['overflow']
            reopened['remote_exclusion'] = 'LOCAL_NATIVE_PROCESS_JOINED'
            final, = events(reopened['stdout'])
            assert final['event'] == 'ACK_REOPEN' and final['pid'] != owned['pid']
            assert final['native'] == dict(sequence=ACK_AFTER_SEQUENCE, rows=after_rows)
            assert files(directory/'db/producer-inputs-v1') == case['inputs_before']
            assert files(directory/'preserved-db') == case['preserved_files']
            case['status'] = 'PASS'
            return case['selector']['fsync_index']
        finally:
            if owned:
                if not select.select([owned['pidfd']], [], [], 0)[0]:
                    signal.pidfd_send_signal(owned['pidfd'], signal.SIGKILL)
                    case['forced_native_kill'] = True
                    select.select([owned['pidfd']], [], [], 5)
                case['native_exited'] = bool(select.select([owned['pidfd']], [], [], 0)[0])
                os.close(owned['pidfd'])
            save()
    try:
        result['controls'] = ack_oracle_controls()
        index = run('control')
        run('eio', index)
        assert build['executables_sha256'] == {'native_components': sha(binary)}
        assert all(sha(source/p) == digest for p, digest in manifest['files_sha256'].items())
        result['status'] = 'PASS_NATIVE_ACK_FSYNC_EIO'
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__+': '+str(error))
        raise
    finally:
        save()


def main(root):
    resources = json.loads((root/'resources.json').read_text())
    source = root/'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner = resources['container_id']
    state = json.loads(command('docker', 'inspect', runner))[0]
    assert state['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
    assert state['State']['Running'] and not state['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    binary = ready['publication_tests']['path']
    assert re.fullmatch(r'/out/build/target/release/deps/bigbrotr_archive_worker-[0-9a-f]+', binary)
    assert sha(root/binary.removeprefix('/')) == ready['publication_tests']['sha256']
    home = root/'out/sync-cases'
    home.mkdir()
    (home/'temp').mkdir()
    result = dict(source=resources['source'], status='INCOMPLETE', artifact=ready['publication_tests'],
                  harness_sha256=sha(Path(__file__)),
                  strace=command('docker', 'exec', runner, 'strace', '--version'), cases=[])
    def save():
        (home/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    def run(label, test, selected=None, errno='EIO'):
        case = dict(name=label, status='INCOMPLETE')
        result['cases'].append(case)
        out = '/out/sync-cases/'+label
        trace = ['strace', '-f', '-yy', '-ttt', '-e',
                 'trace=fsync,fdatasync,openat,close,rename,renameat,unlink,unlinkat', '-o', out+'.trace']
        if selected:
            trace += ['-P', selected, '-e', 'inject=fsync:error='+errno+':when=1']
        args = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib',
                '--env', 'BIGBROTR_ARCHIVE_TEST_BUNDLE=/out/archive-worker',
                '--env', 'TMPDIR=/out/sync-cases/temp',
                '--env', 'BIGBROTR_ARCHIVE_TEST_SYNC_HOME='+out, runner,
                *trace, binary, 'publication::tests::'+test, '--exact', '--nocapture']
        if selected:
            args.append('--ignored')
        case['command'] = args
        save()
        try:
            execute(args, 30, case)
            save()
            assert case['exit'] == 0 and not case['overflow']
            assert '1 passed; 0 failed; 0 ignored;' in case['stdout']
            path = home/(label+'.trace')
            case['trace_sha256'] = sha(path)
            lines = decoded_lines(path.read_text())
            injected = [(c.pid, c.body) for c in lines if '(INJECTED)' in c.body]
            if selected:
                assert len(injected) == 1
                assert re.fullmatch(r'fsync\(\d+<'+re.escape(selected)+r'>\)\s+= -1 '+errno+r' .*\(INJECTED\)', injected[0][1])
                case['injected'] = injected
                retained = sorted(str(p.relative_to(home/label)) for p in (home/label/'objects').rglob('*.parquet'))
                tables = ['archive_commits', 'dictionaries', 'events'] if label.startswith('receipt') else ['dictionaries']
                assert retained == [f'objects/main/{table}/prepared-'+('09'*16)+'/data.parquet' for table in tables]
                case['retained_files'] = {p: sha(home/label/p) for p in retained}
            else:
                assert not injected and not case['stderr']
                paths = sorted(set(re.findall(r'/out/sync-cases/temp/[^<>" ]+/prepared-[0-9a-f]+/data.parquet', path.read_text())))
                assert len(paths) == 3
                case['files'] = [ordering_evidence(lines, file) for file in paths]
            case['status'] = 'PASS'
        finally:
            save()
    try:
        run('ordered-sync', 'complete_rows_and_receipt_survive_fresh_native_read_and_duplicate_refuses')
        attempt = 'prepared-'+('09'*16)
        targets = {
            'file': f'objects/main/dictionaries/{attempt}/data.parquet',
            'attempt': f'objects/main/dictionaries/{attempt}',
            'table': 'objects/main/dictionaries', 'main': 'objects/main', 'root': 'objects',
            'receipt': f'objects/main/archive_commits/{attempt}/data.parquet',
        }
        for label, relative in targets.items():
            run(label, 'native_sync_refusal_prevents_committed_membership_and_attempt_reuse',
                f'/out/sync-cases/{label}/{relative}')
        for label in ['file', 'receipt']:
            run(label+'-enospc', 'native_sync_refusal_prevents_committed_membership_and_attempt_reuse',
                f'/out/sync-cases/{label}-enospc/{targets[label]}', 'ENOSPC')
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        raise
    finally:
        save()
    print(json.dumps(dict(status=result['status'], cases=len(result['cases']), source=resources['source'])))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--native-ack':
        native_ack(Path(sys.argv[2]).resolve())
    elif sys.argv[1:] == ['--check-ack-oracle']:
        print(json.dumps(ack_oracle_controls(), indent=2))
    elif sys.argv[1:] == ['--check-oracle']:
        check_oracle()
    else:
        assert len(sys.argv) == 2
        main(Path(sys.argv[1]).resolve())
