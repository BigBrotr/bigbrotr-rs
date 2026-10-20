"""Independent typed successor recovery expectations; no engines or subprocesses.

The pinned origin was qualified by the full signed Sync oracle. New expected
state permits only the exact closing bit and current Control recorder effects.
Privileged corruption is confined to separately owned negative copies.
"""
import copy
import re
from pathlib import Path

import installed_cycle_model as model
from installed_cycle_replay import process_closed as cycle_closed, CONTEXT_FIELDS
from installed_cycle_client import argv
from installed_family_capture import loads, number, backend_checked
import installed_progress_replay as progress

only, same, sha, encoded, instant = model.only, model.same, model.sha, model.encoded, model.instant

ORIGIN = {'name': 'activated',
 'run': 'installed-progress-75df760',
 'subdirectory': 'installed-progress-split',
 'database': 'bb_installed_progress_split_75df760_activated',
 'status': 'PASS_INSTALLED_PRODUCER_PROGRESS',
 'source': '75df76037cb5c60458165414f133d9deafcc4112',
 'owner': '5f8281b1e15ef94c9484e191ed2d5710',
 'label': 'org.bigbrotr.installed-progress-split',
 'client_id': '188e8837d2c9a4724381c637958aa59bd40ef30bbbd10108cbc0222d5bb2ae1d',
 'source_clone': 'activated',
 'final_case': 'activated-live3',
 'catalog_sha256': 'dbe3da23fb6aaea7989dfc8c1d0f821e9e1b6562dd10032b11f1c71fa01a4c99',
 'result_sha256': 'fe008c7a15bb2c121b72d4942902ba6242b8f959f42c43d5aeb671acb8bc2e7e',
 'snapshot': 'activated-live3-after.json',
 'snapshot_sha256': '0231a6829b07d955ab69523a562e55bc5e336e2973ff6fffa22f3c8ed04e55f9',
 'native_file': 'activated-live3-after-native.json',
 'native_sha256': 'c22fd32b9967077907a3b2339e33c5b8b4e9443459b52d7011d76a4317c4cdd8',
 'inputs_file': 'activated-live3-after-inputs.json',
 'inputs_sha256': '4a5c4292c78bddcca996cdc5d6bb4f64f876e602cd1ee85991d63eca214d749d',
 'store': 'activated/store'}
VERIFICATION_SHA256 = 'bf66c8a89b5f303caca29a35bc1e056719518cd25da3521cdba430973fa07620'

CLONES = ('positive', 'missing-closure', 'foreign-issuer')
CASES = ('positive-recover', 'positive-retry', 'missing-closure-refuse', 'foreign-issuer-refuse')
STATUS = 'PASS_INSTALLED_PRODUCER_PROGRESS_RECOVERY'
DIRECTORY = 'installed-progress-recovery'
LABEL = 'org.bigbrotr.installed-progress-recovery'
PREFIX = 'bb_progress_recovery_'
ERROR = 'error: successor recovery unavailable: producer operation unavailable: Progress(Backend)\n'
SQL_ERRORS = {
    'missing-closure': 'typed head requires its exact bounded retained children',
    'foreign-issuer': 'retained producer progress lost its historical Control attribution',
}
# Explicit privileged corruption on fresh, positively excluded derivatives.
# The application never executes these statements and no original is changed.
CORRUPTIONS = {
    'missing-closure': """BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='5s';
SET LOCAL session_replication_role=replica;
DELETE FROM control.producer_checkpoint_dependencies
WHERE producer_epoch=3 AND source_shard=0 AND source_epoch=1 AND source_sequence=3
RETURNING (to_jsonb(producer_checkpoint_dependencies)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
""",
    'foreign-issuer': """BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='5s';
SET LOCAL session_replication_role=replica;
UPDATE control.producer_checkpoints SET issuer_incarnation=decode(repeat('ee',16),'hex')
WHERE producer_epoch=3
RETURNING (to_jsonb(producer_checkpoints)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
""",
}


def origin_checked(basis, raw=lambda path: path.read_bytes()):
    """Recheck the pinned full signed oracle, not merely its PG digest."""
    objects = []
    for name, field in [('snapshot', 'snapshot_sha256'), ('native_file', 'native_sha256'),
                        ('inputs_file', 'inputs_sha256')]:
        data = raw(basis / ORIGIN[name]); assert sha(data) == ORIGIN[field]
        objects.append(loads(data))
    data = raw(basis / 'activated-live3-verification.json')
    assert sha(data) == VERIFICATION_SHA256
    context = loads(data)
    assert progress.verify(context)['status'] == 'PASS_INSTALLED_PRODUCER_PROGRESS'
    same(objects[0], context['after'])
    assert objects[1] == context['native_after'] and objects[2] == context['inputs_after']
    seed = tuple(objects)
    assert len(seed[0]) == 77
    known = model.science(seed)
    assert (known['head'], known['issued'], known['cut']) == (3, 2, 5)
    head = only(seed[0]['producer_checkpoints'])
    assert (head['producer_epoch'], head['checkpoint_revision'], head['complete_through_request'],
        head['progress_jobs'], head['dependency_count']) == (3, 4, 2, 1, 11)
    job = only(seed[0]['jobs'], request_key='\\x' + 'e4' * 16)
    assert job['state'] == 'queued' and job['job_revision'] == 2 and job['checkpoint_revision'] == 0
    return seed


def corruption_checked(mutation, seed, mode):
    assert set(mutation) == {'kind', 'mode', 'statement', 'sql_output', 'before', 'after'}
    assert mutation['kind'] == 'privileged-isolated-fixture-corruption' and mutation['mode'] == mode
    assert mutation['statement'] == CORRUPTIONS[mode]
    same(mutation['before'], seed[0])
    expected = copy.deepcopy(seed[0])
    returned = loads(mutation['sql_output'])
    if mode == 'missing-closure':
        old = only(expected['producer_checkpoint_dependencies'], producer_epoch=3,
            source_shard=0, source_epoch=1, source_sequence=3)
        assert returned == old
        expected['producer_checkpoint_dependencies'].remove(old)
    else:
        assert mode == 'foreign-issuer'
        old = only(expected['producer_checkpoints'])
        assert not any(r['process_incarnation'] == '\\x' + 'ee' * 16 for r in seed[0]['owner_epochs'])
        model.fresh_xmin(returned['xmin'], seed[0])
        assert returned == dict(old, issuer_incarnation='\\x' + 'ee' * 16, xmin=returned['xmin'])
        expected['producer_checkpoints'] = [returned]
    same(mutation['after'], expected)
    return expected


def specification(case):
    name, clone = case['name'], case['clone']
    assert name in CASES and clone in CLONES and case['origin'] == 'activated'
    assert case['operation'] == 'recover'
    assert name == clone + ('-retry' if name == 'positive-retry' else
        '-recover' if clone == 'positive' else '-refuse')
    assert case['predecessor'] == ('positive-recover' if name == 'positive-retry' else None)
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return clone


def process_closed(response, case):
    clone = specification(case)
    if clone == 'positive':
        return cycle_closed(response, case)
    assert set(response) == {'returncode', 'stdout', 'stderr', 'outcome', 'terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4 and response['stdout'] == '' and response['stderr'] == ERROR
    terminal = response['terminal']
    assert set(terminal) == {'pid','start_ticks','config','prefix','command','exit_code',
        'reaped','forced','error','elapsed_seconds'}
    number(terminal['pid'], 1, integer=True); number(terminal['start_ticks'], 1, integer=True)
    number(terminal['elapsed_seconds'], 0, 60)
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == argv('recover', Path(case['config']))
    assert type(terminal['exit_code']) is int and terminal['exit_code'] == 4
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
    return []


def failed_control(m, observed):
    """The failed inspection retains its actual claim; it does not release it."""
    before = copy.deepcopy(m.pg)
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r:r['epoch'])
    assert previous['state'] == 'released'
    current = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role = only(observed['role_instances'], role_instance_id=current['role_instance_id'])
    rid, inc = m.identity(role['role_instance_id'][2:]), m.identity(current['process_incarnation'][2:])
    for at in (role['created_at_us'], current['acquired_at_us']): model.clock(at, m.window)
    assert m.window['before_us'] <= instant(current['control_backend_start']) <= role['created_at_us']
    assert role['created_at_us'] <= current['acquired_at_us']
    assert current['lease_expires_at_us'] == current['acquired_at_us'] + 6_000_000
    assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
    model.fresh_xmin(current['xmin'], before)
    assert role['xmin'] == current['xmin']
    expected = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=rid,
        process_incarnation=inc, acquired_at_us=current['acquired_at_us'],
        lease_expires_at_us=current['lease_expires_at_us'], state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=current['xmin'])
    assert current == expected
    assert role == dict(role_instance_id=rid, dataset_id=m.ds['dataset_id'], role='control',
        created_at_us=role['created_at_us'], retired_at_us=None, xmin=current['xmin'])
    m.pg['owner_epochs'].append(expected); m.pg['role_instances'].append(role)
    return current


def diagnostic_checked(diagnostic, mode, current, window):
    assert set(diagnostic) == {'returncode', 'stdout', 'stderr'} and diagnostic['returncode'] == 0
    log = diagnostic['stdout'] + diagnostic['stderr']
    assert len(log.encode()) <= 256 * 1024
    pid = current['control_backend_pid']
    pattern = rf'\[{pid}\] ERROR:  (?:[0-9A-Z]{{5}}: )?' + re.escape(SQL_ERRORS[mode]) + '$'
    lines = log.splitlines()
    index, = [i for i,line in enumerate(lines) if re.search(pattern, line)]
    error = lines[index]
    number(instant(error.split()[0].replace('Z', '+00:00')), window['before_us'], window['after_us'], integer=True)
    # A nested digest failure puts the retained validator on a continuation
    # line. Bind the complete immediately following same-backend context block.
    assert index + 1 < len(lines) and f'[{pid}] CONTEXT:  ' in lines[index + 1]
    context = [lines[index + 1]]
    for line in lines[index + 2:]:
        if re.search(r'\[[0-9]+\] [A-Z]+:  ', line): break
        context.append(line)
    assert 'require_retained_producer_progress(bytea,numeric)' in '\n'.join(context), (
        'missing exact retained-head validator diagnostic')


def verify(context, seed, *, mutation=None, diagnostic=None):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; mode = specification(case)
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    before, after = context['before'], context['after']
    native, inputs, window = context['native_before'], context['inputs_before'], context['server_window']
    assert set(window) == {'before_us', 'after_us'}
    number(window['before_us'], 1, integer=True)
    number(window['after_us'], window['before_us'], window['before_us'] + 120_000_000, integer=True)
    assert set(before) == set(after) == set(seed[0]) and len(before) == 77
    assert inputs == context['inputs_after'] == seed[2]
    assert set(context['input_contents']) == set(inputs)
    for name, value in context['input_contents'].items():
        raw = bytes.fromhex(value); assert inputs[name] == dict(bytes=len(raw), sha256=sha(raw))
    assert context['snapshots'] == [] and context['fixture'] == dict(cleanup_errors=[])
    assert context['catalog_before'] == context['catalog_after']
    reports = process_closed(context['response'], case)
    backend_checked(context['backend'], case['database'], before, after)
    known = model.science(seed)
    expected_native = copy.deepcopy(seed[1])
    if case['name'] == 'positive-retry':
        values = model.native_map(expected_native)
        key = ('receipts', (b'\x01\x31' + model.binary(only(seed[0]['initial_ingest_successions'])['producer_slot_id']) + model.be(3)).hex())
        assert values[key] == (b'\x01\x31' + model.be(2) + bytes(16)).hex()
        values[key] = (b'\x02\x31' + model.be(2) + bytes(16) + b'\x01').hex()
        expected_native.update(rows=model.native_rows(values), sequence=expected_native['sequence'] + 1)
    assert native == expected_native
    if mode != 'positive':
        same(before, corruption_checked(mutation, seed, mode))
    elif case['name'] == 'positive-recover':
        same(before, seed[0])
    m = model.Model(before, native, inputs, window)
    if mode == 'positive':
        assert mutation is diagnostic is None
        m.completed_control(after); m.mutation(after, 'recover', known)
        assert reports == [dict(format='bigbrotr-ingest-recovery/1',state='successor-positive-outcomes-resolved',
            dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],
            producer_epoch='3',issued_through='2',reserved_through='2',resolved_requests='2',verified_through='3',
            native='closed',owner_release='committed',ready=False,source_closure='not-performed')]
    else:
        current = failed_control(m, after)
        diagnostic_checked(diagnostic, mode, current, window)
    # This includes every old xmin, typed head/closure/issuer, queued job and
    # uncertainty row. No ACK, retirement, source closure or history rewrite.
    same(m.pg, after)
    assert m.native == context['native_after']
    for table in ('producer_checkpoints','acquisition_progress','producer_checkpoint_dependencies','jobs'):
        same({table: before[table]}, {table: after[table]})
    return dict(status='PASS',case=case['name'],tables=77,producer_epoch='3',
        native_rows=len(m.native['rows']),native_sequence=m.native['sequence'],input_files=len(inputs),
        product_native_writes=int(case['name'] == 'positive-recover'),native_acknowledged='0',native_retired='0',
        typed_head_and_closure_preserved=True,queued_job_uncertainty_preserved=True,
        result='RECOVERED' if mode == 'positive' else 'REFUSED_AT_RETAINED_HEAD_VALIDATION')


def negative_controls(context, seed, *, mutation=None, diagnostic=None):
    changes = [
        ('unknown-process', lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN')),
        ('not-joined', lambda c:c['response']['terminal'].update(reaped=False)),
        ('wrong-command', lambda c:c['response']['terminal']['command'].append('--retire-successor')),
        ('live-backend', lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('native-sequence', lambda c:c['native_after'].update(sequence=c['native_after']['sequence'] + 1)),
        ('native-fact-loss', lambda c:c['native_after']['rows'].pop()),
        ('input-loss', lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))),
        ('old-outcome-xmin', lambda c:c['after']['producer_outcomes'][0].update(xmin='4294967000')),
        ('head-xmin', lambda c:c['after']['producer_checkpoints'][0].update(xmin='4294967001')),
        ('head-revision', lambda c:c['after']['producer_checkpoints'][0].update(checkpoint_revision=5)),
        ('closure-loss', lambda c:c['after']['producer_checkpoint_dependencies'].pop()),
        ('issuer-rewrite', lambda c:c['after']['producer_checkpoints'][0].update(issuer_epoch=1)),
        ('job-completion', lambda c:only(c['after']['jobs'],request_key='\\x'+'e4'*16).update(state='completed')),
        ('catalog-change', lambda c:c.update(catalog_after=c['catalog_after']+'\nchanged')),
    ]
    rejected = []
    for name, change in changes:
        value = copy.deepcopy(context); change(value)
        try: verify(value, seed, mutation=mutation, diagnostic=diagnostic)
        except (AssertionError, KeyError, ValueError, TypeError, StopIteration): rejected.append(name)
        else: raise AssertionError('accepted negative control: ' + name)
    if mutation is not None:
        bad = copy.deepcopy(mutation); bad['statement'] = 'SELECT 1;'
        try: verify(context, seed, mutation=bad, diagnostic=diagnostic)
        except (AssertionError, KeyError, ValueError, TypeError): rejected.append('unbound-corruption')
        else: raise AssertionError('accepted unbound corruption')
        bad = dict(diagnostic, stdout='', stderr='')
        try: verify(context, seed, mutation=mutation, diagnostic=bad)
        except (AssertionError, KeyError, ValueError, TypeError): rejected.append('wrong-refusal-diagnostic')
        else: raise AssertionError('accepted wrong refusal diagnostic')
    return dict(status='PASS', count=len(rejected), rejected=rejected)
