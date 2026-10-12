"""Actual advanced-policy and delivery-loss qualification of a prepared gap.

Three disjoint copies of the frozen real gap; each is inspected again by a new
config-only process. Original resources and all outcomes remain retained.
"""
import json
from pathlib import Path
import shutil
import time

import lab
from fault_cases import Process, safe_event
from initial_gap import configuration
from initial_gap_policy import advance
from initial_gap_fault_oracle import verify, negative_controls, policy_setup
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest

SEED = 'ingest-gap-process-51a6b86'
DATABASE = 'bb_scope_bind_51a6b86_gap_seed'
SOURCE = '51a6b86c28d1278662f733d82721c844b8009631'
OWNER = '5a29b7f9e577f521a15a1c6ff689d17d'
FROZEN = {
    'result.json': 'a89efa2ef563714fb5057270c798cdcdd1c3ddab761a3f2d5a851728096973d3',
    'gap-seed.json': '12f090e4e8ba09a4f7455d1c12b96afd9b66640f1b432431b6d30b27b1490137',
    'gap-seed-after.json': '4eca961d5c36916bf154ebebe35e9ce24b9c26e8bf491671eddc437981c79216',
    'gap-original.json': 'f36d71bb27e8e135fffdae60c02e317e18ae5cbbf51933ae89ea03224f97c2d0',
    'gap-source-preserved.json': 'e33263d3e5fd032fe9d3a9592dbb4a0355242dd128ca20217187e966026f08a7',
}
CASES = ('gap-policy', 'gap-loss-before', 'gap-loss-after')


def wire_event(value):
    """Validate every proxy field before retaining its non-sensitive metadata."""
    extra = {'transaction_starts', 'gap_boundary', 'gap_proxy_sha256',
        'rollback_proxy_sha256', 'rollback_requests', 'rollback_completions',
        'rollback_released', 'rollback_ready', 'rollback_number', 'before_forward'}
    assert extra <= value.keys()
    safe_event({k: v for k, v in value.items() if k not in extra})
    assert value['gap_boundary'] in ('before', 'after') and value['before_forward'] is False
    for name in ('gap_proxy_sha256', 'rollback_proxy_sha256'):
        assert len(value[name]) == 64 and bytes.fromhex(value[name]).hex() == value[name]
    for name in extra - {'gap_boundary', 'gap_proxy_sha256', 'rollback_proxy_sha256', 'before_forward'}:
        assert type(value[name]) is int and 0 <= value[name] <= 64
    return value


def next_wire(process, wanted, seconds=5):
    end = time.monotonic() + seconds
    with process.condition:
        while True:
            assert not process.overflow, 'proxy output budget exceeded'
            newline = process.stdout.find(b'\n', process.offset)
            if newline >= 0:
                value = wire_event(json.loads(bytes(process.stdout[process.offset:newline])))
                process.offset = newline + 1
                assert value['event'] == wanted, 'unexpected proxy event'
                return value
            assert time.monotonic() < end and (process.process.poll() is None
                or process.threads[0].is_alive()), 'proxy event not observed'
            process.condition.wait(min(.05, max(0, end - time.monotonic())))


def invoke(pg, client, output, database, config, label, mode, held_capture):
    proxy = child = None
    started = time.monotonic()
    result = dict(status='INCOMPLETE', mode=mode, wire=None)
    try:
        extra = []
        if mode.startswith('gap-loss-'):
            boundary = mode.removeprefix('gap-loss-')
            source = Path(__file__).resolve().parents[2]
            proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
                '/source/experiments/ingest-bootstrap/initial_gap_proxy.py',
                '--upstream-port', '5432', '--boundary', boundary], input_pipe=True)
            result['wire'] = {}
            ready = next_wire(proxy, 'ready')
            expected_sources = {
                'proxy_sha256': source / 'experiments/control-migrations/fault_proxy.py',
                'rollback_proxy_sha256': source / 'experiments/relay-source/rollback_proxy.py',
                'gap_proxy_sha256': source / 'experiments/ingest-bootstrap/initial_gap_proxy.py',
            }
            assert all(ready[k] == digest(p) for k, p in expected_sources.items())
            assert ready['gap_boundary'] == boundary and ready['transaction_starts'] == 0
            result['wire'] = dict(ready=ready)
            extra = [str(ready['listen_port'])]
        command = 'successor-prepared-recover' if mode == 'prepared-recover' else 'successor-' + mode
        child = Process(['docker', 'exec', client, '/build/ingest_bootstrap_probe',
            command, database, '/run/secrets/application', config, *extra], limit=16*1024**2)
        if proxy:
            held = next_wire(proxy, 'held', 10)
            assert held['transaction_starts'] == 12 and held['faults'] == 1
            assert all(held[k] == 10 for k in ('commits_seen', 'commits_forwarded',
                'server_commit_complete', 'client_commit_complete'))
            counts = [held[k] for k in ('rollback_requests', 'rollback_completions',
                'rollback_released', 'rollback_ready')]
            assert counts == ([1, 1, 1, 1] if boundary == 'before' else [2, 2, 1, 1])
            result['wire']['held'] = held
            # Actual full stores and backend identity while the wire is held.
            # Native is synchronously idle at both selected boundaries.
            result['held'] = held_capture()
            assert child.process.poll() is None
            proxy.send('close')
            receipt = next_wire(proxy, 'receipt')
            assert receipt['reason'] == 'held-close' and receipt['hold_action'] == 'close'
            assert all(receipt[k] == held[k] for k in ('transaction_starts', 'faults',
                'commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete',
                'rollback_requests', 'rollback_completions', 'rollback_released', 'rollback_ready'))
            assert proxy.wait(5) == 0 and not proxy.stderr
            result['wire']['receipt'] = receipt
        result['returncode'] = child.wait(20)
        result['stdout'] = bytes(child.stdout).decode()
        result['stderr'] = bytes(child.stderr).decode()
        assert result['returncode'] == 0 and not result['stderr'], 'finite gap probe failed'
        result['status'] = 'RETURNED'
        return result
    except BaseException as error:
        result['error'] = type(error).__name__
        raise
    finally:
        forced, errors = [], []
        for name, process in [('probe', child), ('proxy', proxy)]:
            if process is None:
                continue
            if process.process.poll() is None:
                forced.append(name)
            try:
                process.stop()
            except BaseException as error:
                errors.append(name + ':' + type(error).__name__)
            if name == 'probe':
                result.update(returncode=process.process.returncode,
                    stdout=bytes(process.stdout).decode(errors='replace'),
                    stderr=bytes(process.stderr).decode(errors='replace'))
            else:
                try:
                    result['wire']['events'] = [wire_event(json.loads(line))
                        for line in process.stdout.splitlines()]
                except BaseException as error:
                    errors.append('proxy-metadata:' + type(error).__name__)
                result['wire'].update(exit_code=process.process.returncode,
                    stderr_nonempty=bool(process.stderr))
        result['seconds'] = time.monotonic() - started
        result['cleanup'] = dict(forced_wrapper_stop=forced, errors=errors,
            boundary='wrappers joined; actual PG absence and owned container terminal checked separately')
        if forced or errors:
            result['status'] = 'INCOMPLETE_CLEANUP'
        lab.write_json(output / (label + '-response.json'), result)
        assert not forced and not errors, 'fixture wrappers did not finish normally'


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / SEED / 'bindings'
    store = basis / 'gap-seed/store'
    assert all(digest(basis / name) == value for name, value in FROZEN.items())
    report = json.loads((basis / 'result.json').read_text())
    assert report['source'] == SOURCE and report['owner'] == OWNER
    assert report['status'] == 'PASS_PREPARED_INITIAL_GAP_COMPLETION'
    assert not report['cleanup_errors'] and not report['unrelated_changed'] and not report['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    previous, = [c for c in containers if c['Id'] == report['client']['id']]
    assert previous['Config']['Labels'][report['label']] == OWNER
    assert not previous['State']['Running'] and previous['State']['Pid'] == 0 and not previous['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    retained = json.loads((basis / 'gap-seed.json').read_text())
    seed = {k: retained[k] for k in ('control', 'native', 'inputs', 'sequence')}
    retired = json.loads((basis / 'gap-original.json').read_text())
    retired = {k: retired[k] for k in ('control', 'native', 'inputs')}
    assert rows(DATABASE) == seed['control'] and inputs_at(store) == seed['inputs']
    files = census(store)
    assert files == retained['files'] and sum(v['bytes'] for v in files.values()) < 1024**2
    absence = observe_backend_absence(pg, DATABASE, output, 'gap-fault-source')
    lab.write_json(output / 'gap-fault-source.json', dict(**seed, files=files,
        frozen=FROZEN, backend_absence=absence))
    cases = []

    def dump(label, directory):
        response, _ = capture(client, output, label + '-dump', 'dump', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        native = json.loads(response.stdout)
        lab.write_json(output / (label + '-native.json'), native)
        return native

    try:
        for mode in CASES:
            database = clone(mode, DATABASE)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == seed['native']
            assert inputs_at(directory / 'store') == seed['inputs']
            config = configuration(pg, client, output, database, directory, seed['control'])
            setup = None
            if mode == 'gap-policy':
                setup = advance(pg, database, rows, output, mode)
                setup['oracle'] = policy_setup(setup['before'], setup['after'])
                assert setup['oracle']['transaction_id'] == setup['transaction_id']
                lab.write_json(output / (mode + '-policy-qualified.json'), setup)
            prior = None
            attempts = []
            for operation in (mode, 'prepared-recover'):
                label = mode if prior is None else mode + '-recovery'
                before, native, inputs = rows(database), dump(label + '-before', directory), inputs_at(directory / 'store')
                lab.write_json(output / (label + '-before.json'), before)
                lab.write_json(output / (label + '-inputs.json'), inputs)

                def held_capture():
                    control = rows(database)
                    owner = max((r for r in control['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
                    backend = json.loads(lab.sql(pg, database, "SELECT json_build_object('pid',pid,"
                        "'backend_start',backend_start,'database',datname,'state',state,'query',query) "
                        f"FROM pg_stat_activity WHERE pid={owner['control_backend_pid']} "
                        "AND datname=current_database() AND usename='bigbrotr_control'"))
                    assert backend['backend_start'] == owner['control_backend_start']
                    assert backend['state'] == 'idle'
                    assert backend['query'] == ('COMMIT' if operation == 'gap-loss-before' else 'ROLLBACK')
                    observed = dict(control=control, native=dump(label + '-held', directory),
                        inputs=inputs_at(directory / 'store'), backend=backend)
                    lab.write_json(output / (label + '-held.json'), observed)
                    return observed

                clock = dict(before_us=server_clock(pg, database))
                lab.write_json(output / (label + '-clock.json'), clock)
                response = invoke(pg, client, output, database, config, label, operation, held_capture)
                clock['after_us'] = server_clock(pg, database)
                lab.write_json(output / (label + '-clock.json'), clock)
                absence = observe_backend_absence(pg, database, output, label)
                events = [json.loads(line) for line in response['stdout'].splitlines()]
                backend_events = [e for e in events if e['event'] == 'successor-backend']
                assert len(backend_events) == 2 and all(e['database'] == database for e in backend_events)
                context = dict(seed=seed, retired=retired, server_window=clock, prior=prior)
                checked = verify(events, before, native, inputs, operation, **context)
                mutations = negative_controls(events, before, native, inputs, operation, **context)
                final = [e for e in events if e['event'] == 'successor-state'][-1]
                after, after_native = rows(database), dump(label + '-after', directory)
                lab.write_json(output / (label + '-after.json'), after)
                assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
                assert after_native == dict(native, rows=final['rows'])
                assert inputs_at(directory / 'store') == inputs == seed['inputs']
                if response.get('held'):
                    held = response['held']
                    snap = next(e for e in events if e['event'] == 'successor-state'
                        and e['stage'] == 'gap-before-completion')
                    assert held['control'] == {t: json.loads(v) for t, v in snap['state']['control_tables']}
                    assert held['native'] == (native if operation == 'gap-loss-before' else after_native)
                    assert held['inputs'] == inputs
                    assert held['backend']['state'] == 'idle'
                    assert held['backend']['query'] == ('COMMIT' if operation == 'gap-loss-before' else 'ROLLBACK')
                attempts.append(dict(name=operation, status='PASS', oracle=checked,
                    negative_controls=mutations, backend_absence=absence, seconds=response['seconds']))
                prior = checked
                lab.write_json(output / (mode + '-attempts.json'), attempts)
            cases.append(dict(name=mode, status='PASS', attempts=attempts,
                policy_setup='guarded privileged fixture only' if setup else None))
            lab.write_json(output / 'gap-fault-cases.json', cases)
    finally:
        preserved = dict(control=rows(DATABASE) == seed['control'], files=census(store) == files,
            artifacts=all(digest(basis / name) == value for name, value in FROZEN.items()))
        lab.write_json(output / 'gap-fault-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='prepared-gap-policy-delivery', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=SOURCE, source_owner=OWNER,
        source_preserved=preserved, source_artifacts=FROZEN,
        scope='advanced current policy and before/after-native delivery loss with config-only fresh recovery',
        excluded='product policy activation, writer activation, empty-attempt retirement, native syscall uncertainty, power loss, installed restart and full M1')
