#!/usr/bin/env python3
"""Owned exact-Git native syscall evidence; not a power-loss qualification.

The real production publication function is invoked by the compiled Rust tests.
strace replaces one selected fsync result with EIO/ENOSPC; it does not manufacture
an actual device writeback failure. Full signed PG journeys run separately.
"""
from collections import namedtuple
import json
from pathlib import Path
import re
import sys

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
    assert not any(c.body.startswith(f'close({fd}<{file}>') and
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
    for invalid in [early_read, cross_thread_close]:
        try:
            ordering_evidence(decoded_lines('\n'.join(invalid)), file)
        except AssertionError:
            continue
        raise AssertionError('trace oracle accepted the negative control')
    print('Trace oracle: positive and 2 discriminating negative controls PASS')


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
    assert len(sys.argv) == 2
    if sys.argv[1] == '--check-oracle':
        check_oracle()
    else:
        main(Path(sys.argv[1]).resolve())
