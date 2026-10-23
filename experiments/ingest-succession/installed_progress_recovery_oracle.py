"""Independent typed recovery and opt-in ACK refusal expectations; no engines.

The pinned origin was qualified by the full signed Sync oracle. New expected
state permits only the exact closing bit and current Control recorder effects.
Privileged corruption is confined to separately owned negative copies.
"""
import copy
import re
from pathlib import Path
import stat
import sys

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
ACK_ORIGIN = dict(name='initial', run='installed-ack-preparation-hole-dfbb8dc', subdirectory='bindings',
    database='bb_scope_bind_dfbb8dc_sync_signed_ack', status='PASS_INSTALLED_INITIAL_PRODUCER_ACK_PREPARATION_HOLE',
    source='dfbb8dc2d542e99726b19c162161d5fa7e136bbc', owner='55c7780aee15072598b7d38bc3417a44',
    label='org.bigbrotr.acquisition-binding',
    client_id='1a95a403ec9f5266275ed0b4bff72e405fc245da60ed501998426382f4d477bd',
    source_clone='sync-signed', final_case='sync-signed-retry', store='sync-signed/store',
    catalog_sha256='74507ca7f23056011cadcfcfc0546f01a70b0300d9388d018bd915f18a5ba103',
    result_sha256='74a57bd2c9cf7c878ea7ac48dc22368ae790baba315dc07e33bfd1908cbe9d18',
    snapshot='sync-signed-retry-after.json', snapshot_sha256='116aa28d71a3e9f2b39e3bb3aeb80a220abfd6b86662c9819f346cf64b7fc67d',
    native_file='sync-signed-retry-after-native.json', native_sha256='3420ad5211d77b091c628016762c08c36c70b89e080c5adc97245fb3fe72fcd5',
    inputs_file='sync-signed-retry-after-inputs.json', inputs_sha256='52116743d0c47d9782a278f517c9511c3df1ce3602bd49f242662697945f702e',
    origin_format='initial-ack-hole', verification_file='sync-signed-retry-verification.json',
    verification_sha256='cfc173f5d7578c2bd45f2ef327d87db5b2e30b39298aae34a65d8cd0b2c649bc')
ACK_CLONES = ('ack-ahead', 'missing-original')
ACK_ERRORS = {'ack-ahead': 'error: producer operation unavailable: Progress(Native(StateMismatch))\n',
              'missing-original': 'error: producer operation unavailable: Native(Input(Missing))\n'}
ACK_RAW_SHA256 = '124dac4b22a0c30c172ba300d4ffaff26749d1b4d862f67d2f13968d3d0fec53'


def settings(ack_refusals=False):
    assert type(ack_refusals) is bool
    if ack_refusals:
        return dict(origin=ACK_ORIGIN, clones=ACK_CLONES, cases=tuple(n+'-refuse' for n in ACK_CLONES),
            status='PASS_INSTALLED_PRODUCER_ACK_REFUSALS', directory='installed-ack-refusals',
            label='org.bigbrotr.installed-ack-refusals', prefix='bb_ack_refusals_')
    return dict(origin=ORIGIN, clones=CLONES, cases=CASES, status=STATUS,
        directory=DIRECTORY, label=LABEL, prefix=PREFIX)


def regular_read(path):
    assert stat.S_ISREG(path.lstat().st_mode), 'retained input must be regular'
    return path.read_bytes()


def initial_ack_origin_report(basis, origin, retained, raw=regular_read):
    """Closed, explicit initial report shape; never a successor fallback."""
    assert origin == ACK_ORIGIN
    assert sha(raw(basis/'result.json')) == origin['result_sha256']
    assert retained == loads(raw(basis/'result.json'))
    assert all(retained[k] == origin[k] for k in ('source','owner','label','status'))
    assert retained['databases'] == [origin['database']]
    assert all(retained[k] == [] for k in ('cleanup_errors','missing_owned','unrelated_changed'))
    assert set(retained['client']) == {'id','name'} and retained['client']['id'] == origin['client_id']
    group, = retained['cases']
    assert group['name'] == origin['source_clone'] and group['status'] == 'PASS'
    assert [c['name'] for c in group['recovery']] == ['sync-signed-recover',origin['final_case']]
    final = group['recovery'][-1]
    assert final['status'] == 'PASS' and final['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    assert type(final['returncode']) is int and final['returncode'] == 0
    assert final['database'] == origin['database'] and final['config'] == '/lab/'+origin['database']+'.toml'
    data = raw(basis/origin['verification_file']); assert sha(data) == origin['verification_sha256']
    context = loads(data)
    same(context['after'], loads(raw(basis/origin['snapshot'])))
    assert context['native_after'] == loads(raw(basis/origin['native_file']))
    assert context['inputs_after'] == loads(raw(basis/origin['inputs_file']))
    assert sha(context['catalog_after'].encode()) == origin['catalog_sha256']
    return origin['client_id']


def ack_origin_checked(basis, raw):
    """Reuse the qualified raw interpretation, rehash every input, redo science.

    Calling the old whole-reader inside this process would mix its module
    provenance with the new refusal modules. Its pinned complete receipt binds
    the original interpretation; all 2185 original files must still agree.
    """
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'experiments/ingest-bootstrap'))
    import installed_progress_context_oracle as initial
    data = raw(basis.parent/'raw-replay.json'); assert sha(data) == ACK_RAW_SHA256
    qualified = loads(data)
    assert qualified['status'] == 'PASS_RAW_ACK_PREPARATION_HOLE_REPLAY' and qualified['kind'] == 'initial'
    assert len(qualified['raw_files_sha256']) == 2185
    for name, wanted in qualified['raw_files_sha256'].items():
        path = Path(name); assert path.is_absolute() and stat.S_ISREG(path.lstat().st_mode)
        assert sha(raw(path)) == wanted, 'qualified origin file changed: '+name
    retained = loads(raw(basis/'result.json'))
    initial_ack_origin_report(basis, ACK_ORIGIN, retained, raw)
    live = loads(raw(basis/'sync-signed-progress-verification.json'))
    assert initial.verify_initial(live,ack=True,preparation_hole=True) == qualified['oracle']
    group, = retained['cases']; assert group['oracle'] == qualified['oracle']
    seed = live['after'], live['native'], live['inputs']; expected = seed
    for case, checked in zip(group['recovery'], qualified['recovery'], strict=True):
        context = loads(raw(basis/(case['name']+'-verification.json')))
        assert context['case']['database'] == ACK_ORIGIN['database']
        assert context['case']['config'] == '/lab/'+ACK_ORIGIN['database']+'.toml'
        same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before']) == expected[1:]
        answer = initial.verify_hole_recovery(context,seed)
        assert checked['name'] == case['name'] and checked['oracle'] == case['oracle'] == answer
        expected = context['after'], context['native_after'], context['inputs_after']
    for i,(field,digest) in enumerate((('snapshot','snapshot_sha256'),('native_file','native_sha256'),('inputs_file','inputs_sha256'))):
        data = raw(basis/ACK_ORIGIN[field]); assert sha(data) == ACK_ORIGIN[digest]
        assert loads(data) == expected[i]
    head = only(expected[0]['producer_checkpoints']); job = only(expected[0]['acquisition_progress'])
    assert (head['checkpoint_revision'],head['complete_through_request'],head['dependency_count']) == (8,7,27)
    assert (job['classified_events'],job['event_ordinal'],job['classification'],job['mutation_request']) == (4,3,5,None)
    assert only(expected[0]['producer_epochs'])['state'] == 'closing'
    assert only(expected[0]['owner_epochs'],role='control',epoch=3)['state'] == 'released'
    return expected


def missing_original(seed):
    request = only(seed[0]['producer_requests'],producer_epoch=1,request_sequence=5)
    assert request['progress_event_ordinal'] == 3
    name = request['producer_slot_id'][2:]+'-0000000000000001-0000000000000005.cbor'
    assert seed[2][name] == dict(bytes=request['input_bytes'],sha256=request['request_digest'][2:])
    return name


def ack_head(seed):
    head = dict(only(seed[0]['producer_checkpoints']),complete_through_request=6)
    jobs = sorted(seed[0]['acquisition_progress'],key=progress.job_record)
    deps = sorted(seed[0]['producer_checkpoint_dependencies'],key=progress.dependency_record)
    values = model.native_map(seed[1])
    head['checkpoint_digest'] = '\\x'+progress.head_digest(head,jobs,deps,
        lambda row:progress.native_reference(row,values)).hex()
    return head


def ack_statement(seed):
    head = ack_head(seed)
    return "BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\n" \
        "SET LOCAL session_replication_role=replica;\n" \
        "UPDATE control.producer_checkpoints SET complete_through_request=6,checkpoint_digest=decode('"+head['checkpoint_digest'][2:]+"','hex')\n" \
        "WHERE producer_slot_id=decode('"+head['producer_slot_id'][2:]+"','hex') AND producer_epoch=1\n" \
        "RETURNING (to_jsonb(producer_checkpoints)||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n"


def ack_corruption_checked(mutation, seed, mode):
    assert mode in ACK_CLONES
    assert set(mutation) == {'kind','mode','statement','sql_output','before','after','inputs_before',
        'inputs_after','spool_before','spool_after','quarantine'}
    assert mutation['kind'] == 'privileged-isolated-fixture-corruption' and mutation['mode'] == mode
    same(mutation['before'],seed[0]); assert mutation['inputs_before'] == seed[2]
    expected, inputs = copy.deepcopy((seed[0],seed[2]))
    before_files, after_files = mutation['spool_before'], mutation['spool_after']
    assert set(before_files) == {'.',*inputs}
    for name, identity in inputs.items():
        value = before_files[name]
        assert stat.S_ISREG(value['mode']) and value['mode'] & 0o777 == 0o600 and value['links'] == 1
        assert {k:value[k] for k in ('bytes','sha256')} == identity
    if mode == 'ack-ahead':
        assert mutation['statement'] == ack_statement(seed) and mutation['quarantine'] is None
        returned = loads(mutation['sql_output']); head = ack_head(seed)
        model.fresh_xmin(returned['xmin'],seed[0]); head['xmin'] = returned['xmin']
        assert returned == head; expected['producer_checkpoints'] = [head]
        assert before_files == after_files
    else:
        assert mutation['statement'] is mutation['sql_output'] is None
        name = missing_original(seed); identity = inputs.pop(name)
        q = mutation['quarantine']
        assert set(q) == {'name','path','hex','metadata'} and q['name'] == name
        assert q['path'] == mode+'/quarantined-'+name
        data = bytes.fromhex(q['hex']); assert dict(bytes=len(data),sha256=sha(data)) == identity
        assert q['metadata'] == before_files[name]
        assert {k:v for k,v in before_files.items() if k not in ('.',name)} == {k:v for k,v in after_files.items() if k != '.'}
        assert stat.S_ISDIR(after_files['.']['mode'])
        assert {k:v for k,v in before_files['.'].items() if k != 'bytes'} == {
            k:v for k,v in after_files['.'].items() if k != 'bytes'}, 'spool directory replaced'
    same(mutation['after'],expected); assert mutation['inputs_after'] == inputs
    return expected,seed[1],inputs
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


def origin_checked(basis, raw=regular_read, *, ack_refusals=False):
    """Recheck the pinned full signed oracle, not merely its PG digest."""
    if ack_refusals:
        return ack_origin_checked(basis,raw)
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


def specification(case, *, ack_refusals=False):
    name, clone = case['name'], case['clone']
    if ack_refusals:
        assert clone in ACK_CLONES and name == clone+'-refuse'
        assert case['origin'] == 'initial' and case['operation'] == 'recover-initial'
        assert case['predecessor'] is None
        assert re.fullmatch(r'[a-z0-9_]{1,63}',case['database'])
        assert case['config'] == '/lab/'+case['database']+'.toml'
        return clone
    assert name in CASES and clone in CLONES and case['origin'] == 'activated'
    assert case['operation'] == 'recover'
    assert name == clone + ('-retry' if name == 'positive-retry' else
        '-recover' if clone == 'positive' else '-refuse')
    assert case['predecessor'] == ('positive-recover' if name == 'positive-retry' else None)
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return clone


def process_closed(response, case, *, ack_refusals=False):
    clone = specification(case,ack_refusals=ack_refusals)
    if clone == 'positive':
        return cycle_closed(response, case)
    assert set(response) == {'returncode', 'stdout', 'stderr', 'outcome', 'terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4 and response['stdout'] == ''
    assert response['stderr'] == (ACK_ERRORS[clone] if ack_refusals else ERROR)
    terminal = response['terminal']
    assert set(terminal) == {'pid','start_ticks','config','prefix','command','exit_code',
        'reaped','forced','error','elapsed_seconds'}
    number(terminal['pid'], 1, integer=True); number(terminal['start_ticks'], 1, integer=True)
    number(terminal['elapsed_seconds'], 0, 60)
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == argv(case['operation'], Path(case['config']))
    assert type(terminal['exit_code']) is int and terminal['exit_code'] == 4
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['error'] is None
    return []


def failed_control(m, observed, *, renewed=False):
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
    if renewed:
        renewed_at = current['lease_expires_at_us'] - 6_000_000
        model.clock(renewed_at,m.window)
        assert current['acquired_at_us'] < renewed_at
        model.fresh_xmin(role['xmin'],before)
        assert int(current['xmin']) > int(role['xmin'])
    else:
        assert current['lease_expires_at_us'] == current['acquired_at_us'] + 6_000_000
        assert role['xmin'] == current['xmin']
    assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
    model.fresh_xmin(current['xmin'], before)
    expected = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=rid,
        process_incarnation=inc, acquired_at_us=current['acquired_at_us'],
        lease_expires_at_us=current['lease_expires_at_us'], state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=current['xmin'])
    assert current == expected
    assert role == dict(role_instance_id=rid, dataset_id=m.ds['dataset_id'], role='control',
        created_at_us=role['created_at_us'], retired_at_us=None, xmin=role['xmin'])
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


def verify(context, seed, *, mutation=None, diagnostic=None, ack_refusals=False):
    if ack_refusals:
        assert diagnostic is None
        return verify_ack_refusal(context,seed,mutation)
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


def negative_controls(context, seed, *, mutation=None, diagnostic=None, ack_refusals=False):
    if ack_refusals:
        assert diagnostic is None
        return ack_negative_controls(context,seed,mutation)
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


def verify_ack_refusal(context, seed, mutation):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; mode = specification(case,ack_refusals=True)
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    expected,native,inputs = ack_corruption_checked(mutation,seed,mode)
    before,after,window = context['before'],context['after'],context['server_window']
    same(before,expected); assert set(before) == set(after) == set(seed[0]) and len(before) == 77
    assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True)
    number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    assert context['native_before'] == context['native_after'] == native
    assert context['inputs_before'] == context['inputs_after'] == inputs
    assert set(context['input_contents']) == set(inputs)
    for name,value in context['input_contents'].items():
        data = bytes.fromhex(value); assert inputs[name] == dict(bytes=len(data),sha256=sha(data))
    assert context['snapshots'] == [] and context['fixture'] == dict(cleanup_errors=[])
    assert context['catalog_before'] == context['catalog_after']
    assert sha(context['catalog_before'].encode()) == ACK_ORIGIN['catalog_sha256']
    process_closed(context['response'],case,ack_refusals=True)
    backend_checked(context['backend'],case['database'],before,after)
    m = model.Model(before,native,inputs,window,initial=True)
    current = failed_control(m,after,renewed=mode=='missing-original')
    assert current['epoch'] == 4
    same(m.pg,after)
    return dict(status='PASS',case=case['name'],tables=77,producer_epoch='1',native_rows=88,
        native_sequence=113,input_files=len(inputs),product_native_writes=0,native_acknowledged=7,
        native_retired=0,complete_through_request=6 if mode=='ack-ahead' else 7,
        typed_jobs_dependencies_original_xmins_preserved=True,source_and_job_completion=False,
        control='ACTIVE_RENEWED' if mode=='missing-original' else 'ACTIVE_CLAIM_ONLY',
        result='REFUSED_AT_NATIVE_FRONTIER' if mode=='ack-ahead' else 'REFUSED_AT_ORIGINAL_REQUEST5')


def ack_negative_controls(context, seed, mutation):
    mode = context['case']['clone']
    changes = [
        ('unknown-process',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN')),
        ('unjoined-process',lambda c:c['response']['terminal'].update(reaped=False)),
        ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
        ('wrong-refusal-branch',lambda c:c['response'].update(stderr=ERROR)),
        ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('native-write',lambda c:c['native_after'].update(sequence=114)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))),
        ('original-request-xmin',lambda c:only(c['after']['producer_requests'],request_sequence=5).update(xmin='4294967000')),
        ('original-outcome-xmin',lambda c:only(c['after']['producer_outcomes'],request_sequence=5).update(xmin='4294967000')),
        ('head-xmin',lambda c:only(c['after']['producer_checkpoints']).update(xmin='4294967001')),
        ('head-revision',lambda c:only(c['after']['producer_checkpoints']).update(checkpoint_revision=9)),
        ('job-prefix-rewrite',lambda c:only(c['after']['acquisition_progress']).update(classified_events=5)),
        ('dependency-loss',lambda c:c['after']['producer_checkpoint_dependencies'].pop()),
        ('historical-issuer-rewrite',lambda c:only(c['after']['producer_checkpoints']).update(issuer_epoch=4)),
        ('job-completion',lambda c:only(c['after']['jobs']).update(state='completed')),
        ('source-closure',lambda c:only(c['after']['source_streams']).update(state='closed')),
        ('failed-control-released',lambda c:only(c['after']['owner_epochs'],role='control',epoch=4).update(state='released')),
        ('catalog-change',lambda c:c.update(catalog_after=c['catalog_after']+'\nchanged')),
    ]
    def wrong_renewal(c):
        current = only(c['after']['owner_epochs'],role='control',epoch=4)
        current['lease_expires_at_us'] = current['acquired_at_us']+6_000_000+(1 if mode=='ack-ahead' else 0)
    changes.append(('wrong-control-renewal',wrong_renewal))
    if mode == 'missing-original':
        changes.append(('silently-restored-input',lambda c:c['inputs_after'].update({missing_original(seed):seed[2][missing_original(seed)]})))
    rejected = []
    def reject(name,c,m):
        try: verify_ack_refusal(c,seed,m)
        except (AssertionError,KeyError,ValueError,TypeError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted negative control: '+name)
    for name,change in changes:
        value = copy.deepcopy(context); change(value); reject(name,value,mutation)
    bad = copy.deepcopy(mutation)
    if mode == 'ack-ahead':
        bad['statement'] = 'SELECT 1;'; reject('unbound-corruption',context,bad)
        bad = copy.deepcopy(mutation); only(bad['after']['producer_checkpoints'])['checkpoint_digest'] = '\\x'+'00'*32
        reject('invalid-lowered-head-digest',context,bad)
    else:
        bad['quarantine']['name'] = next(n for n in seed[2] if n != missing_original(seed))
        reject('wrong-missing-original',context,bad)
        bad = copy.deepcopy(mutation); bad['quarantine']['hex'] = '00'
        reject('lost-quarantined-original',context,bad)
        bad = copy.deepcopy(mutation); bad['spool_after']['.']['inode'] += 1
        reject('replaced-spool-directory',context,bad)
    return dict(status='PASS',count=len(rejected),rejected=rejected)
