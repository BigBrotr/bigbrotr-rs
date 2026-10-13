"""Exact twelve-row retirement of two independently qualified activated targets.

Native science, input bytes and historical SQL attribution come from frozen
abandonment captures. No product retirement reducer supplies expected rows.
"""
import copy
import json
import math

import activated_acquisition_oracle as acquisition
from activated_recovery_oracle import encoded, sha, backend_checked
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant

CASES = ('installed-activated-retirement-off', 'installed-activated-retirement-durable')
SEEDS = {
    'off': '3dd7847983c5a6d4f05bd82348da5231ad8892fcfd8ef604b8c870461842b7c6',
    'durable': 'ec2e7f65c1d2a4adf6056e62e8c3b8fb514895d169592d6bdf10178522314b89',
}
CATALOG_FUNCTIONS = (
    'control.guard_producer_epoch()',
    'control.lock_activated_initial_retirement(bytea)',
    'control.require_activated_initial_retirement(bytea)',
    'control.require_activated_initial_retirement_transition(bytea,bytea,bigint)',
    'control.guard_activated_retirement_role()',
)
GRANTS = [f'GRANT EXECUTE ON FUNCTION {name} TO bigbrotr_control;' for name in CATALOG_FUNCTIONS[1:4]]
# Reviewed additive SQL. Function printing is independently reconstructed from
# its exact source; the same printer reproduces the retained 0035 definitions.
MIGRATION = 'ea882fd887d0800ab1880646026ac85eeb6485f5fa4fd3323eee09b4bf96eac8'
OLD_PRODUCER_GUARD = '48cbb594391dce2cfbfda9b0f21e3e65c50ddc1020c064217492e792afbdbf10'
NEW_FUNCTIONS = {
    'control.lock_activated_initial_retirement(bytea)': 'a916ba404287eafe4b4f0ba92e6ef5dccf3fa5d0ec7f2ae19c6bb349914041a2',
    'control.require_activated_initial_retirement(bytea)': '2bd78e12a69c488361e21017d1bb6296d438472e16480e36cdfd5f261cc80f0a',
    'control.require_activated_initial_retirement_transition(bytea,bytea,bigint)': '8bb4e3e0efc3898186598c23083e10015438d181f23c0a00b6496a668495785d',
    'control.guard_producer_epoch()': 'eff8ac0cb842ee8457c4020827b0592a961e96fc5d05a2a887300755135afad2',
    'control.require_complete_initial_empty_retirement()': '404135d55b244efb302762678da530098a617d2760cd313d018861f96ce8490e',
    'control.guard_activated_retirement_role()': 'eaa8cb5cfd1175dddef1b9415ea02cd68bf74d170ba2994868244bef4fee58e3',
}


def selected(mode):
    for value in CASES:
        if mode in (value, value + '-retry'):
            return value.removeprefix('installed-activated-retirement-'), mode.endswith('-retry')
    raise AssertionError('unselected activated retirement mode')


def baseline_catalog(value):
    result = copy.deepcopy(value)
    result.pop('retirement')
    # Preserve every acquisition-specific field while normalizing only the
    # independently qualified internal RI numeric trigger-name suffix.
    extra = result['acquisition']
    return dict(acquisition.catalog_base(result), acquisition=extra)


def seed_checked(seed):
    assert set(seed) == {'name', 'verification', 'control', 'native', 'inputs', 'contents', 'catalog'}
    retained = seed['verification']
    assert sha(encoded(retained)) == SEEDS[seed['name']]
    # This whole qualified capture is immutable. Reuse its independently
    # checked scientific basis; the new wave does not replay old CLI campaigns.
    known = acquisition.seed_checked(retained['seed'])
    same(seed['control'], retained['after'])
    assert seed['native'] == retained['native_after']
    assert seed['inputs'] == retained['inputs_after'] and seed['contents'] == retained['input_contents']
    assert baseline_catalog(seed['catalog']) == baseline_catalog(dict(
        retained['setup']['catalog_after'], retirement={}))
    extra = seed['catalog']['retirement']
    assert set(extra) == {'functions', 'privileges'}
    row, = extra['functions']
    assert row['name'] == 'control.guard_producer_epoch()' and row['execute'] is True
    assert OLD_PRODUCER_GUARD is not None and sha(row['definition'].encode()) == OLD_PRODUCER_GUARD
    assert set(extra['privileges']) == {'producer_epochs', 'source_streams', 'allocation_domains', 'owner_epochs', 'role_instances'}
    assert all(r == dict(select=True, update=True, delete=False, truncate=False) for r in extra['privileges'].values())
    gap = only(seed['control']['acquisition_gaps'], producer_epoch=2)
    assert gap['previous_owner_epoch'] == 8 and gap['issuer_epoch'] == 11
    job = only(seed['control']['jobs'], job_id=gap['job_id'])
    assert job['state'] == 'cancelled' and job['job_revision'] == 4
    assert max(r['epoch'] for r in seed['control']['owner_epochs'] if r['role'] == 'control') == 12
    assert not any(r['producer_epoch'] == 2 for r in seed['control']['producer_input_bindings'])
    assert not any(r['producer_epoch'] == 2 for r in seed['control']['producer_checkpoints'])
    # The pinned scientific input recipes classify off request2 and durable
    # request1 as Events. Their already completed gaps belong to activator8.
    event_sequence = 2 if seed['name'] == 'off' else 1
    request = only(seed['control']['producer_requests'], producer_epoch=2, request_sequence=event_sequence)
    assert request['acquisition_job_id'] == gap['job_id']
    return known


def catalog_migrated(old, new, granted):
    assert MIGRATION is not None and NEW_FUNCTIONS
    wanted = copy.deepcopy(old)
    for collection in (wanted['functions'], wanted['retirement']['functions']):
        for row in collection:
            if row['name'] in NEW_FUNCTIONS:
                actual = only(new['functions'] + new['retirement']['functions'], name=row['name'])
                assert sha(actual['definition'].encode()) == NEW_FUNCTIONS[row['name']]
                row['definition'] = actual['definition']
    for name in CATALOG_FUNCTIONS[1:]:
        row = only(new['retirement']['functions'], name=name)
        assert set(row) == {'name', 'definition', 'execute'}
        assert sha(row['definition'].encode()) == NEW_FUNCTIONS[name]
        assert row['execute'] is (granted and name in CATALOG_FUNCTIONS[1:4])
        wanted['retirement']['functions'].append(row)
    wanted['retirement']['functions'].sort(key=lambda r: r['name'])
    for name in ('activated_retirement_role_guard', 'activated_retirement_role_truncate_guard'):
        wanted['triggers'].append(dict(table='role_instances', name=name, enabled='O', internal=False,
            function='control.guard_activated_retirement_role()'))
    wanted['triggers'].sort(key=lambda r: (r['table'], r['name']))
    assert new == wanted, 'only reviewed retirement guards, classifier and role-history triggers may change'


def migrated(seed, setup, server_window):
    assert MIGRATION is not None and setup['migration_sha256'] == MIGRATION
    upgrade = setup['migration']
    same(upgrade['before'], seed['control'])
    assert len(upgrade['responses']) == 2
    assert all(r['returncode'] == 0 and r['stderr'] == '' for r in upgrade['responses'])
    assert not any(k in upgrade for k in ('outcome', 'incomplete_response'))
    row = only(upgrade['after']['schema_versions'], migration=37)
    fresh_xmin(row['xmin'], seed['control'])
    assert type(row['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in seed['control']['schema_versions']) <= row['applied_at_us'] <= server_window['before_us']
    assert row == dict(only(seed['control']['schema_versions'], migration=36), migration=37,
        name='0037_activated_initial_retirement.sql', checksum='\\x' + MIGRATION,
        applied_at_us=row['applied_at_us'], xmin=row['xmin'])
    expected = copy.deepcopy(seed['control'])
    expected['schema_versions'].append(row)
    same(expected, upgrade['after'])
    same(upgrade['after'], upgrade['reapplied'])
    assert upgrade['physical_before'] == upgrade['physical_after'] and set(upgrade['physical_before']) == set(expected)
    assert baseline_catalog(upgrade['catalog_before']) == baseline_catalog(seed['catalog'])
    assert upgrade['catalog_before']['retirement'] == seed['catalog']['retirement']
    catalog_migrated(upgrade['catalog_before'], upgrade['catalog_ungranted'], False)
    assert upgrade['grant_statements'] == GRANTS and upgrade['grant_responses'] == [''] * len(GRANTS)
    catalog_migrated(upgrade['catalog_before'], upgrade['catalog_after'], True)
    assert setup['catalog_before'] == setup['catalog_after'] == upgrade['catalog_after']
    return expected


def response_checked(value, mode, report):
    assert value['outcome'] == 'PROCESS_EXITED' and type(value['returncode']) is int and value['returncode'] == 0
    assert value['stderr'] == ''
    assert not any(k in value for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    assert math.isfinite(value['seconds']) and 0 <= value['seconds'] <= 120
    terminal = value['terminal']
    assert terminal['exit_code'] == 0 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--retire-activated-initial']
    def unique(pairs):
        assert len(pairs) == len(dict(pairs)), 'duplicate report field'
        return dict(pairs)
    lines = value['stdout'].splitlines()
    assert len(lines) == 1 and encoded(json.loads(lines[0], object_pairs_hook=unique)) == encoded(report)


def target_rows(rows):
    owner = only(rows['owner_epochs'], role='ingest', epoch=2)
    role = only(rows['role_instances'], role_instance_id=owner['role_instance_id'])
    producer = only(rows['producer_epochs'], producer_epoch=2)
    domains = [r for r in rows['allocation_domains'] if r['owner_epoch'] == 2]
    assert len(domains) == 9 and sorted(r['dictionary_kind'] for r in domains) == list(range(1, 10))
    return owner, role, producer, domains


def retire_targets(wanted, observed, known, current, clock, *, released):
    """Apply only the independently expected twelve-row terminal transaction.

    Observed is the complete after snapshot, including the current Control role.
    Callers first validate the actual current Control lifecycle. A lost COMMIT
    response has no release timestamp; its capture clock bounds the same effect.
    """
    owner, role, producer, domains = target_rows(wanted)
    assert owner['state'] == 'active' and role['retired_at_us'] is None and producer['state'] == 'closing'
    assert all(r['state'] == 'active' for r in domains)
    retired = only(observed['producer_epochs'], producer_epoch=2)
    at, xid = retired['retired_at_us'], retired['xmin']
    assert type(at) is int
    fresh_xmin(xid, wanted)
    assert xid != current['xmin']
    assert xid != only(observed['role_instances'], role_instance_id=current['role_instance_id'])['xmin']
    upper = current['released_at_us'] if released else clock['after_us']
    assert max(known['source']['closed_at_us'], current['lease_expires_at_us'] - 20_000_000) <= at <= upper
    assert at < current['lease_expires_at_us']
    producer.update(state='retired', retired_at_us=at, closure_digest='\\x' + known['digest'], xmin=xid)
    owner.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
        exclusion_digest='\\x' + known['digest'], xmin=xid)
    role.update(retired_at_us=at, xmin=xid)
    for row in domains:
        row.update(state='closed', closed_at_us=at, closure_digest='\\x' + known['digest'], xmin=xid)
    return at, xid


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
    assert native_before == native_after == seed['native'], 'retirement has no native write or reclamation'
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    projected, current = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    owner, role, producer, domains = target_rows(wanted)
    if not retry:
        at, _ = retire_targets(wanted, after, known, current, server_window, released=True)
    else:
        assert owner['state'] == 'released' and producer['state'] == 'retired'
        at = producer['retired_at_us']
    same(wanted, projected)
    intent = only(before['initial_ingest_successions'])
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-initial-retired',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2',
        issued_through=str(known['issued']), reserved_through=str(known['issued']), resolved_requests=str(known['issued']),
        source_closure=dict(last_complete_sequence=str(known['cut']), sha256=known['digest']),
        ingest_retirement=dict(allocation_domains='9', acquisition_gaps='1', owner_release='committed',
            native_acknowledged='0', native_retired='0'), native='closed', owner_release='committed', ready=False)
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
        target_rows_changed=0 if retry else 12, retired_at_us=at, native_writes=0, retained_acquisition_gaps=1,
        source_unchanged=True, jobs_unchanged=True, sessions_unchanged=True, producer_retired=True, ready=False)


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
    def targets(c):
        return target_rows(c['after'])
    reject('producer-only-retirement', lambda c: targets(c)[3][0].update(state='active', closed_at_us=None, closure_digest=None))
    reject('split-terminal-transaction', lambda c: targets(c)[1].update(xmin='999999999'))
    reject('changed-role-history', lambda c: targets(c)[1].update(created_at_us=1))
    reject('deleted-target-role', lambda c: c['after']['role_instances'].remove(targets(c)[1]))
    reject('different-domain-time', lambda c: targets(c)[3][0].update(closed_at_us=targets(c)[2]['retired_at_us'] + 1))
    reject('wrong-exclusion-digest', lambda c: targets(c)[0].update(exclusion_digest='\\x' + '00' * 32))
    reject('renewed-historical-ingest', lambda c: targets(c)[0].update(lease_expires_at_us=1))
    reject('rewritten-source-time', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closed_at_us=targets(c)[2]['retired_at_us']))
    reject('rewritten-source-xmin', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin=targets(c)[2]['xmin']))
    reject('old-source-changed', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(last_complete_sequence=0))
    reject('gap-issuer-is-not-current-control', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=2).update(issuer_epoch=13))
    reject('slot-registrar-is-not-activator', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=2).update(previous_owner_epoch=3))
    reject('outcome-copy-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=1).update(recorder_epoch=13))
    reject('job-revision-advanced', lambda c: only(c['after']['jobs'], job_id=only(c['after']['acquisition_gaps'], producer_epoch=2)['job_id']).update(job_revision=5))
    reject('session-revoked', lambda c: c['after']['browser_sessions'][0].update(revoked_at_us=1))
    reject('native-reclamation', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('lost-original-input', lambda c: c['input_contents'].pop(next(iter(c['input_contents']))))
    reject('disabled-role-guard', lambda c: only(c['setup']['catalog_after']['triggers'], name='activated_retirement_role_guard', table='role_instances').update(enabled='D'))
    reject('unexpected-recovery-grant', lambda c: c['setup']['migration']['grant_statements'].append('GRANT ALL'))
    reject('wrong-cli-mode', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--abandon-activated-acquisition'))
    reject('unjoined-native-owner', lambda c: c['response']['terminal'].update(reaped=False))
    reject('forced-child', lambda c: c['response']['terminal'].update(forced=True))
    reject('unknown-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('clock-missing', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True))
    def retime(c, at, xid):
        owner, role, producer, domains = targets(c)
        owner.update(released_at_us=at, xmin=xid)
        role.update(retired_at_us=at, xmin=xid)
        producer.update(retired_at_us=at, xmin=xid)
        for row in domains:
            row.update(closed_at_us=at, xmin=xid)
    reject('coherent-retirement-before-authority', lambda c: retime(c, c['server_window']['before_us'] - 1, targets(c)[2]['xmin']))
    reject('coherent-old-transaction', lambda c: retime(c, targets(c)[2]['retired_at_us'], only(c['before']['source_streams'], owner_epoch=2)['xmin']))
    if selected(context['mode'])[1]:
        reject('retry-rewrites-all-terminal-rows', lambda c: retime(c, c['server_window']['before_us'], '999999999'))
    return dict(status='PASS', rejected=len(checks), controls=checks)
