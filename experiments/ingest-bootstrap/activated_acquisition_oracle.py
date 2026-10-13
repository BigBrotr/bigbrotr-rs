"""Exact whole-intent gaps for two independently qualified closed source2 seeds.

No product reducer supplies job selection, gap fields or scientific expectations.
The retained first-epoch gap and both browser-session histories stay immutable.
"""
import copy
import json
import math
import re

from activated_recovery_oracle import encoded, sha, backend_checked
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant

CASES = ('installed-activated-acquisition-off', 'installed-activated-acquisition-durable')
REASON = 'activated_initial_producer_abandoned'
MIGRATION = '19e2390b46d9eba7e4eac765eafc30fc7b2cac03e03c0918df64ed0b87642e54'
SEEDS = {
    'off': ('39f2342b25a073ee6494e1affc658e2a3b2459d507a709a703967d922828bef1',
        2, 5, 70, 85, 6, '05d4cca350996266cfbdfd651a1e4bc5194c5b95a27650447d542ac66c87d5bb',
        '42e09b5a41d12027920cb581d7941ec3'),
    'durable': ('419771186caca43878603af38fba2f322ef94b0da13ed9ba6c375832ed199803',
        1, 0, 53, 65, 5, '555415667f142b9852c068ba94613773538f5f5ff8ae48dc3fbca9a860df05c5',
        '6f79d9172718514568141925e805bf88'),
}
OLD_FUNCTIONS = {
    'control.guard_acquisition_gap()': '8606d827cb1192f8bf40bfade726254f7ef35adcd4c1f96688133653a2bb2299',
    'control.guard_acquisition_claim_owner()': 'fa6b545f63e2692ebd412202f5d9bca37031519f95a2c289986eb1d2f11a24ca',
    'control.require_acquisition_gap_complete()': '6fad9d97f3edd6fe22f236994f2fb191d516cf2db5137feb7485aa03a6c5a3ee',
}
NEW_FUNCTIONS = dict(OLD_FUNCTIONS, **{
    'control.guard_acquisition_gap()': '3ffdf1977d91b3789e44fa2af3da3f8baab0e1341a2a668c015330e75e411d32',
    'control.guard_acquisition_claim_owner()': '94e0b38c7064497d580c50ed96159166defc54757983e5bc5f8f984f296290f2',
})
REMOVED = ('acquisition_gaps_producer_epoch_check', 'acquisition_gaps_source_owner_epoch_check', 'acquisition_gaps_reason_check')
CHECK = "((producer_epoch = 1 AND source_owner_epoch = 1 AND reason = 'initial_producer_abandoned') OR (producer_epoch = 2 AND source_owner_epoch = 2 AND reason = 'activated_initial_producer_abandoned'))"


def selected(mode):
    for name in CASES:
        if mode in (name, name + '-retry'):
            return name.removeprefix('installed-activated-acquisition-'), mode.endswith('-retry')
    raise AssertionError('unselected abandonment mode')


def check_expression(definition):
    """Parse only this CHECK's equality/AND/OR subset, preserving grouping.

PostgreSQL adds numeric/text casts and parentheses when printing domain values.
Dropping all parentheses would accidentally accept a different Boolean predicate.
    """
    text = definition.removeprefix('CHECK ')
    text = re.sub(r'::(?:numeric|text)\b', '', text)
    tokens = re.findall(r"'[a-z_]+'|[a-z_]+|[0-9]+|AND|OR|[()=]", text)
    assert ''.join(tokens) == re.sub(r'\s+', '', text)
    position = 0

    def term():
        nonlocal position
        value = tokens[position]
        position += 1
        if value == '(':
            result = expression(0)
            assert tokens[position] == ')'
            position += 1
            return result
        assert re.fullmatch(r"'[a-z_]+'|[a-z_]+|[0-9]+", value)
        return value

    def expression(minimum):
        nonlocal position
        result = term()
        levels = {'OR': 1, 'AND': 2, '=': 3}
        while position < len(tokens) and levels.get(tokens[position], 0) > minimum:
            operator = tokens[position]
            position += 1
            result = (operator, result, expression(levels[operator]))
        return result

    result = expression(0)
    assert position == len(tokens)
    return result


def catalog_base(value):
    result = copy.deepcopy(value)
    result.pop('acquisition')
    identities = set()
    for row in result['triggers']:
        identity = row['table'], row['name']
        assert identity not in identities
        identities.add(identity)
        if row['internal']:
            assert re.fullmatch('RI_ConstraintTrigger_[ac]_[1-9][0-9]*', row['name'])
            row['name'] = row['name'].rsplit('_', 1)[0]
    result['triggers'].sort(key=encoded)
    return result


def catalog_old(catalog, retained):
    baseline = dict(retained, acquisition={})
    assert catalog_base(catalog) == catalog_base(baseline)
    extra = catalog['acquisition']
    assert set(extra) == {'functions', 'checks', 'indexes', 'privileges'}
    assert len(extra['functions']) == 3
    for row in extra['functions']:
        assert set(row) == {'name', 'definition', 'execute'} and type(row['execute']) is bool
        assert sha(row['definition'].encode()) == OLD_FUNCTIONS[row['name']]
    assert {r['name'] for r in extra['functions']} == set(OLD_FUNCTIONS)
    assert only(extra['functions'], name='control.guard_acquisition_gap()')['execute'] is True
    assert only(extra['functions'], name='control.require_acquisition_gap_complete()')['execute'] is True
    assert extra['privileges'] == dict(gap_select=True, gap_insert=True, gap_update=False,
        gap_delete=False, gap_truncate=False, job_select=True, job_update=True)
    assert len({r['name'] for r in extra['checks']}) == len(extra['checks'])
    for name, expression in zip(REMOVED, ('producer_epoch = 1', 'source_owner_epoch = 1',
            "reason = 'initial_producer_abandoned'"), strict=True):
        assert check_expression(only(extra['checks'], name=name)['definition']) == check_expression(expression)
    indexes = dict(acquisition_gaps_pkey=('acquisition_gaps', 'CREATE UNIQUE INDEX acquisition_gaps_pkey ON control.acquisition_gaps USING btree (job_id)'),
        acquisition_gaps_producer=('acquisition_gaps', 'CREATE INDEX acquisition_gaps_producer ON control.acquisition_gaps USING btree (producer_slot_id, producer_epoch, job_id)'),
        acquisition_jobs_owner=('jobs', "CREATE INDEX acquisition_jobs_owner ON control.jobs USING btree (owner_resource_id, owner_epoch, job_id) WHERE (kind = 'acquisition'::text)"))
    assert extra['indexes'] == sorted((dict(name=n, table=t, definition=d) for n, (t, d) in indexes.items()),
        key=lambda r: (r['table'], r['name']))


def catalog_migrated(old, new):
    wanted = copy.deepcopy(old)
    for name in REMOVED:
        wanted['constraints'].remove(only(wanted['constraints'], table='acquisition_gaps', name=name))
        wanted['acquisition']['checks'].remove(only(wanted['acquisition']['checks'], name=name))
    wanted['constraints'].append(dict(table='acquisition_gaps', name='acquisition_gap_epoch_reason',
        type='c', validated=True, deferrable=False, deferred=False))
    wanted['constraints'].sort(key=lambda r: (r['table'], r['name']))
    check = only(new['acquisition']['checks'], name='acquisition_gap_epoch_reason')
    assert set(check) == {'name', 'definition'}
    assert check_expression(check['definition']) == check_expression(CHECK)
    wanted['acquisition']['checks'].append(check)
    wanted['acquisition']['checks'].sort(key=lambda r: r['name'])
    for row in wanted['acquisition']['functions']:
        actual = only(new['acquisition']['functions'], name=row['name'])
        assert sha(actual['definition'].encode()) == NEW_FUNCTIONS[row['name']]
        row['definition'] = actual['definition']
    assert new == wanted, 'migration may only change the selected paired CHECK and two guard bodies'


def seed_checked(seed):
    assert set(seed) == {'name', 'verification', 'control', 'native', 'inputs', 'contents', 'catalog'}
    pin, issued, cut, count, sequence, inputs, digest, job = SEEDS[seed['name']]
    retained = seed['verification']
    assert sha(encoded(retained)) == pin
    same(seed['control'], retained['after'])
    assert seed['native'] == retained['native_after'] and seed['inputs'] == retained['inputs_after']
    assert seed['contents'] == retained['input_contents']
    assert len(seed['control']) == 72 and len(native(seed['native'])) == count
    assert seed['native']['sequence'] == sequence and len(seed['inputs']) == inputs
    for name, raw in seed['contents'].items():
        value = bytes.fromhex(raw)
        assert seed['inputs'][name] == dict(bytes=len(value), sha256=sha(value))
    catalog_old(seed['catalog'], retained['setup']['catalog_after'])
    source = only(seed['control']['source_streams'], owner_epoch=2)
    assert source['state'] == 'closed' and source['last_complete_sequence'] == cut
    assert source['closure_digest'] == '\\x' + digest
    assert only(seed['control']['producer_epochs'], producer_epoch=2)['state'] == 'closing'
    activation = only(seed['control']['initial_ingest_activations'])
    slot = only(seed['control']['producer_slots'])
    assert activation['activated_by_epoch'] == 8 and slot['registered_by_epoch'] == 3
    candidates = [r for r in seed['control']['jobs'] if r['kind'] == 'acquisition' and (
        r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation']) == (
        activation['activated_by_resource'], activation['activated_by_epoch'], activation['activated_by_incarnation'])]
    selected_job, = candidates
    assert selected_job['job_id'] == '\\x' + job and selected_job['job_revision'] == 2
    assert selected_job['state'] == 'queued' and selected_job['checkpoint_revision'] == 0
    assert selected_job['acquisition_job_protocol'] == 1 and selected_job['input_complete'] is True
    assert not any(r['producer_epoch'] == 2 for r in seed['control']['acquisition_gaps'])
    return dict(issued=issued, cut=cut, digest=digest, job=selected_job, activation=activation, source=source)


def migrated(seed, setup, clock):
    assert setup['migration_sha256'] == MIGRATION
    upgrade = setup['migration']
    same(upgrade['before'], seed['control'])
    assert upgrade['grant_statements'] == []
    assert len(upgrade['responses']) == 2 and all(r['returncode'] == 0 and r['stderr'] == '' for r in upgrade['responses'])
    assert not any(k in upgrade for k in ('outcome', 'incomplete_response'))
    added = only(upgrade['after']['schema_versions'], migration=36)
    fresh_xmin(added['xmin'], seed['control'])
    assert type(added['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in seed['control']['schema_versions']) <= added['applied_at_us'] <= clock['before_us']
    assert added == dict(only(seed['control']['schema_versions'], migration=35), migration=36,
        name='0036_activated_acquisition_abandonment.sql', checksum='\\x' + MIGRATION,
        applied_at_us=added['applied_at_us'], xmin=added['xmin'])
    wanted = copy.deepcopy(seed['control'])
    wanted['schema_versions'].append(added)
    same(wanted, upgrade['after'])
    same(upgrade['after'], upgrade['reapplied'])
    assert upgrade['physical_before'] == upgrade['physical_after'] and set(upgrade['physical_before']) == set(wanted)
    catalog_old(upgrade['catalog_before'], seed['verification']['setup']['catalog_after'])
    assert catalog_base(upgrade['catalog_before']) == catalog_base(seed['catalog'])
    assert upgrade['catalog_before']['acquisition'] == seed['catalog']['acquisition']
    catalog_migrated(upgrade['catalog_before'], upgrade['catalog_after'])
    assert setup['catalog_before'] == setup['catalog_after'] == upgrade['catalog_after']
    return wanted


def response_checked(value, mode, report):
    assert value['outcome'] == 'PROCESS_EXITED' and type(value['returncode']) is int and value['returncode'] == 0
    assert value['stderr'] == ''
    assert not any(k in value for k in ('output_limit_exceeded','orchestration_error','terminal_error','clock_error','holder_error'))
    assert math.isfinite(value['seconds']) and 0 <= value['seconds'] <= 120
    terminal = value['terminal']
    assert terminal['exit_code'] == 0 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--abandon-activated-acquisition']
    def unique(pairs):
        assert len(pairs) == len(dict(pairs)), 'duplicate report field'
        return dict(pairs)
    lines = value['stdout'].splitlines()
    assert len(lines) == 1 and encoded(json.loads(lines[0], object_pairs_hook=unique)) == encoded(report)


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, predecessor=None):
    name, retry = selected(mode)
    assert seed['name'] == name
    window(server_window)
    known = seed_checked(seed)
    expected = migrated(seed, setup, server_window)
    if retry:
        assert predecessor is not None and predecessor['mode'] == mode.removesuffix('-retry')
        assert predecessor['seed'] == seed and predecessor['setup']['migration'] == setup['migration']
        verify(**predecessor)
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        expected = predecessor['after']
    else:
        assert predecessor is None
    same(before, expected)
    assert len(before) == len(after) == 72
    assert native_before == native_after == seed['native'], 'abandonment does not rewrite native source2'
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    projected, current = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    if not retry:
        job = only(wanted['jobs'], job_id=known['job']['job_id'])
        activation = known['activation']
        gap = only(projected['acquisition_gaps'], producer_epoch=2)
        fresh_xmin(gap['xmin'], before)
        assert gap['xmin'] != current['xmin']
        at = gap['abandoned_at_us']
        assert type(at) is int and max(known['source']['closed_at_us'], job['created_at_us'],
            current['lease_expires_at_us'] - 20_000_000) <= at <= current['released_at_us']
        assert gap == dict(job_id=job['job_id'], dataset_id=job['dataset_id'],
            producer_slot_id=activation['producer_slot_id'], producer_epoch=2, shard_id=0, source_owner_epoch=2,
            last_complete_sequence=known['cut'], source_closure_digest='\\x' + known['digest'],
            previous_job_revision=2, previous_owner_resource=activation['activated_by_resource'],
            previous_owner_epoch=8, previous_owner_incarnation=activation['activated_by_incarnation'],
            issuer_resource=current['resource_id'], issuer_epoch=current['epoch'],
            issuer_incarnation=current['process_incarnation'], owner_role='control',
            abandoned_at_us=at, reason=REASON, xmin=gap['xmin'])
        wanted['acquisition_gaps'].append(gap)
        job.update(state='cancelled', job_revision=4, reason_code=REASON, completed_at_us=at,
            owner_resource_id=current['resource_id'], owner_epoch=current['epoch'],
            owner_incarnation=current['process_incarnation'], xmin=gap['xmin'])
    same(wanted, projected)
    intent = only(before['initial_ingest_successions'])
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-acquisition-abandoned',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2',
        issued_through=str(known['issued']), reserved_through=str(known['issued']), resolved_requests=str(known['issued']),
        source_closure=dict(last_complete_sequence=str(known['cut']), sha256=known['digest']),
        acquisition_abandonment=dict(jobs='1', reason=REASON, coverage='unknown'),
        native='closed', owner_release='committed', ready=False)
    response_checked(response, mode, report)
    backend_checked(backend, current, response, server_window)
    if retry:
        assert backend['absence']['database'] == predecessor['backend']['absence']['database']
        old = predecessor['response']['terminal']
        assert (response['terminal']['pid'], response['terminal']['start_ticks']) != (old['pid'], old['start_ticks'])
        previous = predecessor['backend']['exact']['expected']
        assert (current['control_backend_pid'], current['control_backend_start']) != (previous['pid'], previous['backend_start'])
        assert predecessor['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        native_rows=len(native(native_after)), native_sequence=native_after['sequence'], original_inputs=len(inputs_after),
        abandoned_jobs=1, new_gaps=0 if retry else 1, native_writes=0, sessions_unchanged=True,
        source_closed=True, producer_retired=False, ready=False)


def negative_controls(**context):
    verify(**context)
    checks = []
    def reject(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            checks.append(name)
            return
        raise AssertionError('counterexample accepted: ' + name)
    def gap(c):
        return only(c['after']['acquisition_gaps'], producer_epoch=2)
    def job(c):
        return only(c['after']['jobs'], job_id=gap(c)['job_id'])
    reject('slot-registrar-is-not-activator', lambda c: gap(c).update(previous_owner_epoch=3))
    reject('wrong-original-claim-incarnation', lambda c: gap(c).update(previous_owner_incarnation='\\x'+'11'*16))
    reject('missing-terminal-gap', lambda c: c['after']['acquisition_gaps'].remove(gap(c)))
    reject('old-gap-rewritten', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=1).update(xmin='999999'))
    reject('gap-copy-digest', lambda c: gap(c).update(source_closure_digest='\\x'+'00'*32))
    reject('split-gap-job-transaction', lambda c: job(c).update(xmin='999999'))
    reject('adopted-running-job', lambda c: job(c).update(state='running', job_revision=3))
    reject('invented-checkpoint', lambda c: job(c).update(checkpoint_revision=1))
    reject('lost-original-intent', lambda c: c['after']['acquisition_jobs'].pop())
    reject('new-job-execution', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(reserved_through_request=99))
    reject('native-write', lambda c: c['native_after'].update(sequence=c['native_after']['sequence']+1))
    reject('input-original-mutated', lambda c: c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('outcome-recorder-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=1).update(recorder_epoch=99))
    reject('source-attribution-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closed_at_us=1))
    reject('ingest-adopted', lambda c: only(c['after']['owner_epochs'], role='ingest', epoch=2).update(lease_expires_at_us=99))
    reject('session-renewed-or-revoked', lambda c: c['after']['browser_sessions'][0].update(revoked_at_us=1))
    reject('catalog-trigger-disabled', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    def mixed_union(c):
        for catalog in (c['setup']['catalog_before'], c['setup']['catalog_after'],
                c['setup']['migration']['catalog_after']):
            only(catalog['acquisition']['checks'], name='acquisition_gap_epoch_reason')['definition'] = (
                "CHECK ((producer_epoch = 1 AND source_owner_epoch = 2 AND reason = 'initial_producer_abandoned') "
                "OR (producer_epoch = 2 AND source_owner_epoch = 1 AND reason = 'activated_initial_producer_abandoned'))")
    reject('coherent-mixed-epoch-check', mixed_union)
    def stale_transaction(c):
        retained = only(c['before']['acquisition_gaps'], producer_epoch=1)['xmin']
        gap(c)['xmin'] = job(c)['xmin'] = retained
    reject('coherent-old-gap-transaction', stale_transaction)
    reject('unexpected-grant', lambda c: c['setup']['migration']['grant_statements'].append('GRANT ALL'))
    reject('wrong-command', lambda c: c['response']['terminal']['command'].__setitem__(-1,'--abandon-acquisition'))
    reject('driver-not-collected', lambda c: c['response']['terminal'].update(reaped=False))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True))
    reject('unknown-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('clock-capture-failed', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    if selected(context['mode'])[1]:
        def rewritten(c):
            current = max((r for r in c['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            gap(c).update(issuer_epoch=current['epoch'],issuer_incarnation=current['process_incarnation'],xmin='999999',abandoned_at_us=current['acquired_at_us'])
            job(c).update(owner_epoch=current['epoch'],owner_incarnation=current['process_incarnation'],xmin='999999',completed_at_us=current['acquired_at_us'])
        reject('retry-rewrites-terminal-attribution', rewritten)
    else:
        def early(c):
            at = c['server_window']['before_us']-1
            gap(c)['abandoned_at_us']=at
            job(c)['completed_at_us']=at
        reject('coherent-gap-before-current-authority', early)
    return dict(status='PASS', rejected=len(checks), controls=checks)
