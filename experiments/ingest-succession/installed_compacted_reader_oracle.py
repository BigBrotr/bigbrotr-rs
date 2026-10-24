"""Independent literal initial-floor reader expectations; no engine or product calls.

The qualified signed seed supplies the immutable original science. Only actual
installed abandonment and retirement may establish this fixture's eligibility.
The subsequent raw floor installation is synthetic, never reclamation evidence.
"""
import copy
from pathlib import Path
import re
import stat

import installed_progress_recovery_oracle as retained
from installed_cycle_client import argv
from installed_cycle_replay import CONTEXT_FIELDS
from installed_family_capture import backend_checked, loads, number

model = retained.model
only, same, sha, binary, be = model.only, model.same, model.sha, model.binary, model.be
ORIGIN = retained.ACK_ORIGIN
DIRECTORY = 'installed-compacted-reader'
LABEL = 'org.bigbrotr.installed-compacted-reader'
STATUS = 'PASS_SYNTHETIC_POST_GC_INITIAL_READER'
CLONES = ('positive','incompatible-head','floor-digest','missing-pending')
STEPS = {name:('abandon','retire',*(('residual','absent') if name == 'positive' else ('refuse',))) for name in CLONES}
CASES = tuple(name+'-'+step for name in CLONES for step in STEPS[name])
# The retained ACK origin has recovery grants, but never exercised abandonment
# or retirement. Provision their existing prerequisites before the new reader's
# three column reads; these are fixture setup, not product privilege changes.
GRANTS = ('GRANT SELECT,INSERT ON control.acquisition_gaps TO bigbrotr_control;',
          'GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(), '
          'control.require_acquisition_gap_complete(), '
          'control.require_initial_retirement(bytea,numeric,bytea) TO bigbrotr_control;',
          'GRANT SELECT(owner_resource_id) ON control.initial_ingest_successions, '
          'control.ingest_successions, control.ingest_activations TO bigbrotr_control;')
NOT_RUN = ('Actual product reclamation/GC and unlink', 'Compacted successor preparation/activation/recovery',
           'Crash/unknown-commit and interrupted unlink', 'Mixed restore/backup and capacity')
MANIFEST = '4d6754fa296d44b3b140da79b6829cf1b7f777ff109c30170db473ce124bb042'
HEAD = 'a3948e3f2727a98d9b07bb0cd6815ca1004391143604c4a18e44683f57f00894'
ERRORS = {'incompatible-head':'error: producer operation unavailable: Progress(InvalidContext)\n',
          'floor-digest':'error: producer operation unavailable: Control(Backend)\n',
          'missing-pending':'error: event admission unavailable: StateMismatch\n'}


def catalog_checked(original, current):
    def statements(value):
        return [line for line in value.splitlines() if line.strip() and not line.startswith('--')]
    before,after = statements(original),statements(current)
    added = [line for line in after if line not in before]
    tables = ('initial_ingest_successions','ingest_successions','ingest_activations')
    assert len(added) == 7
    for table in tables:
        assert len([line for line in added if re.fullmatch(
            r'GRANT SELECT\s*\(owner_resource_id\) ON (?:TABLE )?control\.'+table+r' TO bigbrotr_control;',line)]) == 1
    assert 'GRANT SELECT,INSERT ON TABLE control.acquisition_gaps TO bigbrotr_control;' in added
    # PostgreSQL dumps the sole function privilege EXECUTE as ALL. This does
    # not allow ALL table rights or any function outside these exact signatures.
    for function in ('guard_acquisition_gap()', 'require_acquisition_gap_complete()',
            'require_initial_retirement(wanted_slot bytea, wanted_epoch numeric, wanted_digest bytea)'):
        assert len([line for line in added if re.fullmatch(
            r'GRANT (?:ALL|EXECUTE) ON FUNCTION control\.'+re.escape(function)+r' TO bigbrotr_control;',line)]) == 1
    assert [line for line in after if line not in added] == before


def qualification_checked(built, component, qualification, source, *, initial_journey=False, initial_preparation=False, compacted_activated=False, compacted_terminal=False, empty_initial=False):
    assert type(empty_initial) is bool and (not empty_initial or initial_journey)
    assert not (empty_initial and (initial_preparation or compacted_activated or compacted_terminal))
    assert type(compacted_terminal) is bool and (not compacted_terminal or compacted_activated)
    assert type(initial_journey) is bool
    assert type(initial_preparation) is bool and (not initial_preparation or initial_journey)
    assert type(compacted_activated) is bool and (not compacted_activated or initial_journey)
    assert not (initial_preparation and compacted_activated)
    assert re.fullmatch(r'[0-9a-f]{40}',source)
    assert component['status'] == 'PASS' and qualification['status'] == 'PASS_COMPACTED_PREFIX_COMPONENTS'
    assert built['source'] == component['source'] == qualification['source'] == source
    for key in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert built[key] == component[key] == qualification[key]
    assert component['missing_controls'] == {}
    counts = component['test_counts']
    assert counts == qualification['test_counts']
    selectors={'compacted_prefix','compacted_progress','initial_reconstruction','request_inputs','successor_history','archive_retained_frontier'}
    if initial_preparation or compacted_activated or empty_initial:
        selectors |= {'claim_custody','control_runtime_entrypoint'}
    assert set(counts) == selectors
    assert all(type(n) is int and n > 0 for n in counts.values())
    assert counts['compacted_prefix'] == 7 and counts['compacted_progress'] == 1
    assert set(component['test_names']) == set(counts)
    for name, names in component['test_names'].items():
        assert len(names) == len(set(names)) == counts[name]
    expected = ['compacted_floor_codec_has_exact_bytes_and_legacy_refusal',
        'compacted_prefix_validates_all_outcome_kinds_and_residual_originals_without_writes',
        'compacted_prefix_requires_pending_indices_and_complete_scientific_census',
        'compacted_outcome_reducer_refuses_changed_rows_and_incomplete_closure',
        'compacted_prefix_binds_custody_inputs_cancellation_and_grants',
        'compacted_outcomes_check_protection_batch_digest_and_residual_names',
        'compacted_outcomes_preserve_full_width_historical_policy_and_reject_row_mismatch']
    assert set(component['test_names']['compacted_prefix']) == {
        'storage::rocks::store::admission_tests::compacted_prefix_tests::'+n for n in expected}
    assert component['test_names']['compacted_progress'] == [
        'storage::postgres::control_owner::producer_progress::recovery::tests::compacted_floor_requires_exact_retained_checkpoint']
    assert set(component['test_names']['successor_history']) == {
        'storage::postgres::control_owner::successor_history::tests::'+n for n in (
        'terminal_genesis_kind_comes_from_activation_even_at_zero',
        'pending_and_prepared_closing_do_not_certify_the_target',
        'activated_nonterminal_and_noncontiguous_heads_refuse',
        'positive_summary_binds_kind_epoch_count_cut_and_digest',
        'compacted_initial_summary_is_separate_from_original_replay')}
    assert set(component['test_names']['archive_retained_frontier']) >= {
        'storage::postgres::control_owner::archive_retained_frontier::tests::'+n for n in (
        'only_complete_terminal_history_is_a_loader_prerequisite',
        'compacted_history_is_not_an_archive_frontier_prerequisite')}
    required_native = {
        'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::compacted_history_tests::'+n for n in (
        'compacted_history_replays_later_originals_and_binds_initial_recorded_digest',
        'compacted_history_pending_requires_its_exact_owner_and_final_authority',
        'compacted_history_refuses_stale_completion_and_original_only_construction',
        'compacted_history_cannot_mint_handoff_gap_empty_or_activation_capabilities',
        'compacted_history_reuses_one_permit_for_full_census_and_canonical_rows',
        'compacted_history_correction_census_counts_reclaimed_requests_beyond_spool_cap')}
    assert required_native <= set(component['test_names']['initial_reconstruction'])
    if initial_journey:
        required_handoff = {
            'storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_gap_tests::compacted_handoff_tests::'+n for n in (
            'compacted_handoff_exact_four_writes_and_zero_write_prepared_retry',
            'compacted_handoff_refuses_stale_custody_inputs_and_dispatch_without_writes',
            'compacted_handoff_requires_full_census_original_manifest_cut_and_digest',
            'compacted_handoff_wrong_owner_and_prepared_pending_context_refuse',
            'compacted_handoff_prepared_target_must_be_empty_open_and_census_complete',
            'compacted_candidate_binds_fresh_target_and_explicit_activation_only',
            'compacted_handoff_postwrite_authority_refusal_retains_exact_retry')}
        required_handoff.add('storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::compacted_ancestor_tests::compacted_later_ancestor_cannot_mint_initial_handoff')
        assert required_handoff <= set(component['test_names']['initial_reconstruction'])
    if initial_preparation or compacted_activated or empty_initial: preparation_qualification(component)
    if compacted_activated: activated_qualification(component)
    if compacted_terminal: terminal_qualification(component)
    if empty_initial: empty_qualification(component)
    executables={'bigbrotr','ingest_bootstrap_probe','native_components'}
    if initial_preparation or compacted_activated or empty_initial: executables |= {'claim_custody','control_runtime_entrypoint'}
    assert set(component['executables_sha256']) == executables
    assert built['binary_sha256'] == component['executables_sha256']['bigbrotr']
    assert built['build_info']['source'] == dict(state='builder-supplied',revision=source,
        tree=built['tree'],dirty=False,manifest_sha256=built['source_manifest_sha256'])


def known(seed):
    pg,native,inputs = seed
    assert len(pg) == 77 and len(native['rows']) == 88 and native['sequence'] == 113 and len(inputs) == 7
    p = only(pg['producer_epochs']); head = only(pg['producer_checkpoints'])
    assert p['producer_epoch'] == p['owner_epoch'] == 1 and p['reserved_through_request'] == 7
    assert head['manifest_sha256'] == '\\x'+MANIFEST and head['checkpoint_digest'] == '\\x'+HEAD
    assert (head['checkpoint_revision'],head['complete_through_request'],head['dependency_count']) == (8,7,27)
    values = model.native_map(native)
    assert sha(bytes.fromhex(values['default','0101'])) == MANIFEST
    slot,ds = binary(p['producer_slot_id']),binary(p['dataset_id'])
    state_key = (b'\x01\x31'+slot+be(1)).hex()
    assert values['receipts',state_key] == (b'\x02\x31'+be(7)+be(7)+be(0)+b'\x01').hex()
    source_key = (b'\x01\x02'+ds+bytes(4)+be(1)).hex()
    assert values['default',source_key] == (b'\x01\x02'+be(27)).hex()
    preimage = b'bigbrotr/initial-source-closure/1\0'+bytes.fromhex(MANIFEST)+slot+be(1)+be(7)
    receipts = []
    assert len(pg['producer_requests']) == len(pg['producer_outcomes']) == 7
    for sequence in range(1,8):
        r = only(pg['producer_requests'],request_sequence=sequence)
        o = only(pg['producer_outcomes'],request_sequence=sequence)
        key = (b'\x01\x30'+slot+be(1)+be(sequence)).hex()
        assert o['request_digest'] == r['request_digest'] and o['manifest_sha256'] == r['manifest_sha256'] == '\\x'+MANIFEST
        assert o['outcome_sha256'] == '\\x'+sha(binary(o['outcome_bytes']))
        assert values['receipts',key] == o['outcome_bytes'][2:]
        receipts.append(dict(cf='receipts',key=key,value=values['receipts',key]))
        preimage += be(sequence)+binary(r['request_digest'])+binary(o['outcome_sha256'])
    return dict(digest=sha(preimage+be(27)),state_key=state_key,receipts=receipts,
        pending_key=(b'\x01\x21'+ds+bytes(4)+be(1)+be(1)).hex())


def floor_bytes(seed, mode):
    k = known(seed); digest = bytearray.fromhex(k['digest'])
    if mode == 'floor-digest': digest[0] ^= 1
    value = b'\x03\x31'+be(7)*3+b'\x01'+bytes.fromhex(MANIFEST)+be(27)+digest+be(8)+bytes.fromhex(HEAD)
    assert len(value) == 139
    return value


def compacted_native(seed, mode, *, missing=False):
    k = known(seed); result = copy.deepcopy(seed[1]); values = model.native_map(result)
    for row in k['receipts']: assert values.pop((row['cf'],row['key'])) == row['value']
    values['receipts',k['state_key']] = floor_bytes(seed,mode).hex()
    if missing:
        assert mode == 'missing-pending'
        removed = bytes.fromhex(values.pop(('facts',k['pending_key'])))
        assert len(removed) == 42 and removed[:10] == b'\x01\x21'+be(2)
    result.update(rows=model.native_rows(values),sequence=121+int(missing))
    return result


def terminal_checked(pg, seed):
    k = known(seed); digest = '\\x'+k['digest']
    p = only(pg['producer_epochs']); s = only(pg['source_streams'])
    owner = only(pg['owner_epochs'],role='ingest'); role = only(pg['role_instances'],role_instance_id=owner['role_instance_id'])
    assert p['state'] == 'retired' and s['state'] == 'closed' and s['last_complete_sequence'] == 27
    assert p['closure_digest'] == s['closure_digest'] == owner['exclusion_digest'] == digest
    assert owner['state'] == 'released' and owner['exclusion_method'] == 'storage_fenced'
    at = p['retired_at_us']; assert s['closed_at_us'] <= at == owner['released_at_us'] == role['retired_at_us']
    assert len(pg['allocation_domains']) == 9
    assert all(r['state'] == 'closed' and r['closed_at_us'] == at and r['closure_digest'] == digest for r in pg['allocation_domains'])
    gap = only(pg['acquisition_gaps']); job = only(pg['jobs'])
    assert gap['reason'] == job['reason_code'] == 'initial_producer_abandoned'
    assert gap['source_closure_digest'] == digest and gap['last_complete_sequence'] == 27
    assert job['state'] == 'cancelled' and job['job_revision'] == 4 and job['checkpoint_revision'] == 0
    assert s['closed_at_us'] <= gap['abandoned_at_us'] == job['completed_at_us'] <= at
    for table in ('producer_requests','producer_outcomes','producer_checkpoints','acquisition_progress','producer_checkpoint_dependencies'):
        same({table:pg[table]},{table:seed[0][table]})
    for table in ('initial_ingest_successions','initial_ingest_activations','ingest_successions','ingest_activations','ingest_terminal_transactions','producer_input_bindings'):
        assert pg[table] == []
    return k


def mutation_checked(mutation, seed, expected, wanted, *, complete=True):
    assert set(mutation) == {'kind','mode','clone','floor','before','control','inputs','after','response'}
    assert mutation['kind'] == 'privileged-synthetic-post-GC-reader-fixture'
    mode,name = mutation['mode'],mutation['clone']
    assert mode in ('install','missing-pending') and name in CLONES
    terminal_checked(mutation['control'],seed); same(mutation['control'],expected[0])
    assert mutation['before'] == expected[1] and mutation['inputs'] == expected[2] == seed[2]
    assert mutation['floor'] == floor_bytes(seed,name).hex()
    assert wanted == compacted_native(seed,name,missing=mode == 'missing-pending')
    response = mutation['response']; assert set(response) == {'returncode','stdout','stderr'}
    assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
    first,last = [loads(line) for line in response['stdout'].splitlines()]
    k = known(seed)
    removed = k['receipts'] if mode == 'install' else [only(expected[1]['rows'],cf='facts',key=k['pending_key'])]
    assert first == dict(event='synthetic-post-gc-reader-fixture',phase='before',mode=mode,
        rows=expected[1]['rows'],sequence=expected[1]['sequence'],floor=mutation['floor'],removed=removed)
    assert last == dict(event='synthetic-post-gc-reader-fixture',phase='after',mode=mode,
        rows=wanted['rows'],sequence=wanted['sequence'],native='closed')
    assert mutation['after'] == (wanted if complete else None)
    return wanted


def head_mutation_checked(mutation, before, seed):
    assert set(mutation) == {'kind','before','statement','sql_output','after'}
    assert mutation['kind'] == 'privileged-isolated-valid-head-corruption'
    same(mutation['before'],before); terminal_checked(before,seed)
    assert mutation['statement'] == retained.ack_statement(seed)
    returned = loads(mutation['sql_output']); model.fresh_xmin(returned['xmin'],before)
    wanted = retained.ack_head(seed); wanted['xmin'] = returned['xmin']
    assert returned == wanted
    after = copy.deepcopy(before); after['producer_checkpoints'] = [wanted]
    same(mutation['after'],after)
    return after


def quarantine_checked(record, seed):
    assert set(record) == {'kind','before','after','path','files','quarantine_after','status'}
    assert record['kind'] == 'reversible-fixture-original-quarantine' and record['status'] == 'RETAINED'
    assert record['path'] == 'positive/quarantined-originals'
    assert set(record['before']) == set(record['quarantine_after']) == {'.',*seed[2]}
    assert set(record['after']) == {'.'} and set(record['files']) == set(seed[2])
    for name,identity in seed[2].items():
        saved = record['files'][name]; assert set(saved) == {'hex','metadata'}
        raw = bytes.fromhex(saved['hex']); assert identity == dict(bytes=len(raw),sha256=sha(raw))
        value = saved['metadata']; assert value == record['before'][name] == record['quarantine_after'][name]
        assert stat.S_ISREG(value['mode']) and value['mode'] & 0o777 == 0o600 and value['links'] == 1
        assert {k:value[k] for k in ('bytes','sha256')} == identity
    assert stat.S_ISDIR(record['after']['.']['mode'])
    assert {k:v for k,v in record['before']['.'].items() if k != 'bytes'} == {
        k:v for k,v in record['after']['.'].items() if k != 'bytes'}


def initial_mutation(m, after, step, seed):
    digest = '\\x'+known(seed)['digest']; source = only(m.pg['source_streams'])
    producer = only(m.pg['producer_epochs']); job = only(m.pg['jobs'])
    def coordinates(row, field, lower):
        at,tx = row[field],row['xmin']; model.clock(at,m.window); model.fresh_xmin(tx,m.pg)
        assert max(lower,m.control['acquired_at_us']) <= at <= m.control['released_at_us'] < m.control['lease_expires_at_us']
        return at,tx
    if step == 'abandon':
        assert source['state'] == 'open' and producer['state'] == 'closing' and job['state'] == 'queued'
        at,tx = coordinates(only(after['source_streams']),'closed_at_us',source['opened_at_us'])
        source.update(state='closed',last_complete_sequence=27,closure_digest=digest,closed_at_us=at,xmin=tx)
        gap = only(after['acquisition_gaps']); at,tx = coordinates(gap,'abandoned_at_us',at)
        slot = only(m.pg['producer_slots'])
        assert (job['owner_resource_id'],job['owner_epoch'],job['owner_incarnation']) == tuple(
            slot[k] for k in ('registered_by_resource_id','registered_by_epoch','registered_by_incarnation'))
        assert m.pg['acquisition_gaps'] == [] and job['job_revision'] == 2
        m.pg['acquisition_gaps'].append(dict(job_id=job['job_id'],dataset_id=m.ds['dataset_id'],
            producer_slot_id=m.slot,producer_epoch=1,shard_id=0,source_owner_epoch=1,last_complete_sequence=27,
            source_closure_digest=digest,previous_job_revision=2,previous_owner_resource=job['owner_resource_id'],
            previous_owner_epoch=job['owner_epoch'],previous_owner_incarnation=job['owner_incarnation'],
            issuer_resource=m.control['resource_id'],issuer_epoch=m.control['epoch'],
            issuer_incarnation=m.control['process_incarnation'],owner_role='control',abandoned_at_us=at,
            reason='initial_producer_abandoned',xmin=tx))
        job.update(state='cancelled',job_revision=4,reason_code='initial_producer_abandoned',completed_at_us=at,
            owner_resource_id=m.control['resource_id'],owner_epoch=m.control['epoch'],
            owner_incarnation=m.control['process_incarnation'],xmin=tx)
    else:
        assert step == 'retire' and source['state'] == 'closed' and producer['state'] == 'closing'
        gap = only(m.pg['acquisition_gaps']); assert job['state'] == 'cancelled'
        at,tx = coordinates(only(after['producer_epochs']),'retired_at_us',gap['abandoned_at_us'])
        producer.update(state='retired',retired_at_us=at,closure_digest=digest,xmin=tx)
        owner = only(m.pg['owner_epochs'],role='ingest'); assert owner['state'] == 'active'
        owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin=tx)
        only(m.pg['role_instances'],role_instance_id=owner['role_instance_id']).update(retired_at_us=at,xmin=tx)
        assert len(m.pg['allocation_domains']) == 9
        for row in m.pg['allocation_domains']:
            assert row['state'] == 'active'
            row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=tx)
    same(m.pg,after)


def specification(case):
    mode = case['clone']
    selected = STEPS if mode in CLONES else {n:('abandon','retire') for n in (*HISTORY_CLONES,'original-gap','original-prepared','original')}
    assert mode in selected
    step = case['name'].removeprefix(mode+'-'); assert step in selected[mode]
    assert case['name'] == mode+'-'+step and case['origin'] == 'initial'
    index = selected[mode].index(step)
    assert case['predecessor'] == (mode+'-'+selected[mode][index-1] if index else None)
    assert case['operation'] == ('abandon-initial' if step == 'abandon' else 'retire-initial')
    assert re.fullmatch(r'[a-z0-9_]{1,63}',case['database'])
    assert case['config'] == '/lab/'+case['database']+'.toml'
    return mode,step


def process_closed(response, case):
    mode,step = specification(case); failed = step == 'refuse'; code = 4 if failed else 0
    assert set(response) == {'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == code
    assert response['stderr'] == (ERRORS[mode] if failed else '')
    t = response['terminal']
    assert set(t) == {'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config'] == case['config'] and t['prefix'] == '/lab/'+case['name']
    assert t['command'] == argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code'] == code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failed: assert response['stdout'] == ''; return None
    report, = [loads(line) for line in response['stdout'].splitlines()]
    assert report['native'] == ('compacted' if step in ('residual','absent') else 'closed')
    assert report['owner_release'] == 'committed' and report['ready'] is False
    return report


def verify(context, seed, expected, *, diagnostic=None):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; mode,step = specification(case)
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    before,after = context['before'],context['after']; same(before,expected[0])
    assert context['native_before'] == context['native_after'] == expected[1]
    assert context['inputs_before'] == context['inputs_after'] == expected[2]
    assert set(context['input_contents']) == set(expected[2])
    for name,value in context['input_contents'].items():
        raw = bytes.fromhex(value); assert expected[2][name] == dict(bytes=len(raw),sha256=sha(raw))
    assert context['snapshots'] == [] and context['fixture'] == dict(cleanup_errors=[])
    assert context['catalog_before'] == context['catalog_after']
    window = context['server_window']; assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    report = process_closed(context['response'],case)
    if step == 'refuse' and mode == 'missing-pending':
        # Complete native census precedes Control acquisition in this branch.
        backend = context['backend']; assert set(backend) == {'absence','exact','all'}
        assert backend['exact'] == backend['all'] == []
        absence = backend['absence']
        assert set(absence) == {'database','user','limit_seconds','status','attempts','seconds'}
        assert absence['database'] == case['database'] and absence['user'] == 'bigbrotr_control'
        assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
        assert absence['attempts']
        for attempt in absence['attempts']:
            assert set(attempt) == {'seconds','count','backends'}
            number(attempt['seconds'],0,5)
            assert type(attempt['count']) is int and attempt['count'] == 0 and attempt['backends'] == []
        number(absence['seconds'],absence['attempts'][-1]['seconds'],6)
    else:
        backend_checked(context['backend'],case['database'],before,after)
    m = model.Model(before,expected[1],expected[2],window,initial=True)
    if step == 'refuse':
        if mode == 'missing-pending': same(before,after)
        else:
            current = retained.failed_control(m,after); same(m.pg,after)
            if mode == 'floor-digest':
                assert diagnostic is not None and diagnostic['returncode'] == 0
                lines = (diagnostic['stdout']+diagnostic['stderr']).splitlines()
                pattern = rf'\[{current["control_backend_pid"]}\] ERROR:  (?:[0-9A-Z]{{5}}: )?retirement requires the closed source and fenced original Control$'
                index, = [i for i,line in enumerate(lines) if re.search(pattern,line)]
                number(retained.instant(lines[index].split()[0].replace('Z','+00:00')),window['before_us'],window['after_us'],integer=True)
                assert 'require_initial_retirement' in '\n'.join(lines[index+1:index+4])
    else:
        assert diagnostic is None
        m.completed_control(after)
        if step in ('abandon','retire'): initial_mutation(m,after,step,seed)
        else: same(m.pg,after); terminal_checked(after,seed)
        compacted = step in ('residual','absent')
        wanted = dict(format='bigbrotr-ingest-recovery/1',state='acquisition-abandoned' if step == 'abandon' else 'initial-retired',
            dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch='1',
            issued_through='7',reserved_through='7',resolved_requests='0' if compacted else '7',
            native='compacted' if compacted else 'closed',owner_release='committed',ready=False,
            source_closure=dict(last_complete_sequence='27',sha256=known(seed)['digest']))
        if step == 'abandon': wanted['acquisition_abandonment'] = dict(jobs='1',reason='initial_producer_abandoned',coverage='unknown')
        else: wanted['ingest_retirement'] = dict(allocation_domains='9',acquisition_gaps='1',owner_release='committed',
            native_acknowledged='7',native_retired='7' if compacted else '0')
        if compacted: wanted.update(compacted_requests='7',recovery_basis='closed-producer-floor-v3')
        assert report == wanted
    return dict(status='PASS',case=case['name'],kind='synthetic-post-GC-reader-fixture',
        native_rows=len(expected[1]['rows']),native_sequence=expected[1]['sequence'],input_files=len(expected[2]),
        product_native_writes=0,tables=77,result='REFUSED' if step == 'refuse' else 'READ' if step in ('residual','absent') else 'TERMINAL_PREREQUISITE')


def negative_controls(context, seed, expected, *, diagnostic=None):
    changes = [('unjoined',lambda c:c['response']['terminal'].update(reaped=False)),
        ('unknown-outcome',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN')),
        ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
        ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('native-write',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('historical-checkpoint',lambda c:only(c['after']['producer_checkpoints']).update(xmin='4294967000')),
        ('historical-request',lambda c:c['after']['producer_requests'][0].update(input_bytes=1)),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if context['inputs_after']: changes.append(('input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))))
    else: changes.append(('input-reappeared',lambda c:c['inputs_after'].update(seed[2])))
    if context['case']['name'] == 'positive-residual':
        def wrong_report(c):
            import json
            r = loads(c['response']['stdout']); r['resolved_requests'] = '7'; c['response']['stdout'] = json.dumps(r)+'\n'
        changes.append(('original-replay-claim',wrong_report))
    rejected = []
    for name,change in changes:
        value = copy.deepcopy(context); change(value)
        try: verify(value,seed,expected,diagnostic=diagnostic)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted negative control: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)

# Connected successor history remains a separate explicit fixture profile. The
# original installed retire-initial profile above keeps its exact case set.
HISTORY_SEED_MODE = 'successor-live-compacted-history-seed'
HISTORY_CLONES = ('history-terminal','history-positive','history-strict','history-missing-required',
    'history-incompatible-head','history-corrupt-residual','history-missing-pending')
HISTORY_SETUP = ('abandon','retire','dictionary2','source2','retire2','prepare3')
HISTORY_STEPS = {n:(HISTORY_SETUP[:-1]+('full',) if n == 'history-terminal' else
    HISTORY_SETUP+(('full','partial','absent') if n == 'history-positive' else ('refuse',)))
    for n in HISTORY_CLONES}
HISTORY_ERRORS = {
    'history-strict':'Native(Native(StateMismatch))',
    'history-missing-required':'Native(Native(RequestInput(Missing)))',
    'history-incompatible-head':'Progress(InvalidContext)',
    'history-corrupt-residual':'Native(Native(RequestInput(Conflict)))',
    'history-missing-pending':'Native(Native(StateMismatch))',
}
HISTORY_FUNCTIONS = (
    'veto_unactivated_producer()', 'require_prepared_initial_successor(bytea)',
    'guard_initial_ingest_succession()',
    'initial_empty_retirement_digest(control.initial_ingest_successions)',
    'lock_initial_empty_retirement(bytea,bytea)',
    'require_initial_empty_claims(control.initial_ingest_successions)',
    'require_empty_initial_retirement(bytea,bytea)',
    'require_initial_empty_transition(bytea,bytea,bigint)',
    'require_complete_initial_empty_retirement()', 'require_active_initial_writer(bytea)',
    'guard_initial_ingest_activation()', 'require_initial_activation_commit()',
    'guard_initial_activation_checkpoint()', 'guard_initial_activation_target()',
    'require_activated_initial_recovery(bytea)', 'require_activated_initial_source(bytea)',
    'lock_activated_initial_retirement(bytea)', 'require_activated_initial_retirement(bytea)',
    'require_activated_initial_retirement_transition(bytea,bytea,bigint)',
    'ingest_lineage_uint(bytea,integer,integer)', 'ingest_lineage_u64(numeric)',
    'ingest_lineage_manifest(bytea)', 'ingest_lineage_manifest_at(bytea,numeric)',
    'ingest_lineage_handoff_at(bytea,numeric)', 'ingest_lineage_activation(bytea,numeric)',
    'ingest_lineage_closure_digest(bytea,numeric,boolean)',
    'require_ingest_lineage_workflows(bytea,numeric,numeric)', 'lock_ingest_lineage(bytea,numeric)',
    'require_ingest_successor(bytea,numeric,text)', 'require_empty_ingest_claims(bytea,numeric)',
    'require_ingest_retirement_transition(bytea,numeric,bytea,bigint)',
    'require_ingest_admission(bytea,numeric,bytea,bytea,bytea,numeric,bytea)')
HISTORY_GRANTS = GRANTS + (
    'GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control;',
    'GRANT SELECT,INSERT ON control.initial_ingest_activations, control.ingest_successions, '
    'control.ingest_activations, control.ingest_terminal_transactions TO bigbrotr_control;',
    'GRANT EXECUTE ON FUNCTION '+', '.join('control.'+n for n in HISTORY_FUNCTIONS)+' TO bigbrotr_control;')
# The product uses only the existing initial preparation/activation surface.
# Complete fixture snapshots also read the three later-lineage tables; no
# later-lineage writes or activated recovery functions are granted.
JOURNEY_GRANTS = GRANTS + (
    'GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control;',
    'GRANT SELECT,INSERT ON control.initial_ingest_activations TO bigbrotr_control;',
    'GRANT SELECT ON control.ingest_successions, control.ingest_activations, '
    'control.ingest_terminal_transactions TO bigbrotr_control;',
    'GRANT EXECUTE ON FUNCTION '+', '.join('control.'+n for n in HISTORY_FUNCTIONS[:14])+' TO bigbrotr_control;')
HISTORY_NOT_RUN = ('Actual product reclamation/GC and unlink',
    'Preparation/activation/recovery or writer publication after compaction',
    'Crash/unknown-commit and interrupted unlink', 'Mixed restore/backup and capacity')


class HistoryProfile:
    DIRECTORY = 'installed-compacted-history'
    LABEL = 'org.bigbrotr.installed-compacted-history'
    STATUS = 'PASS_SYNTHETIC_POST_GC_SUCCESSOR_HISTORY'
    CLONES, STEPS = HISTORY_CLONES, HISTORY_STEPS
    CASES = tuple(n+'-'+s for n in HISTORY_CLONES for s in HISTORY_STEPS[n])
    GRANTS, NOT_RUN = HISTORY_GRANTS, HISTORY_NOT_RUN
    DATABASE_PREFIX = 'bb_compacted_history_'


def history_catalog_checked(original, current, *, grants=HISTORY_GRANTS):
    """Exact ACL union; all nonapplication ACL and catalog bytes stay unchanged."""
    def key(kind, value):
        if kind == 'FUNCTION':
            name, args = value.rstrip(')').split('(',1)
            # pg_dump prints argument names; the grant API names exact types.
            args = ','.join(a.strip().split()[-1] for a in args.split(',') if a.strip())
            return kind,name+'('+args+')'
        return 'TABLE',value
    def parse(value):
        acl, rest = {},[]
        for line in value.splitlines():
            if not line.strip() or line.startswith('--'): continue
            m = re.fullmatch(r'GRANT (.+) ON (TABLE |FUNCTION )?(.+) TO bigbrotr_control;',line)
            if not m: rest.append(line); continue
            rights, kind, target = m.groups(); kind=(kind or 'TABLE').strip()
            rightset = {'EXECUTE'} if kind == 'FUNCTION' and rights == 'ALL' else set(rights.split(','))
            acl.setdefault(key(kind,target),set()).update(rightset)
        return acl,rest
    before, rest = parse(original); after, current_rest = parse(current)
    assert rest == current_rest
    wanted = copy.deepcopy(before)
    assert grants in (HISTORY_GRANTS,JOURNEY_GRANTS,ACTIVATED_GRANTS,TERMINAL_GRANTS,EMPTY_GRANTS)
    for statement in grants:
        m = re.fullmatch(r'GRANT (.+) ON (FUNCTION )?(.+) TO bigbrotr_control;',statement)
        rights, kind, targets = m.groups(); kind=(kind or 'TABLE').strip()
        parts = re.split(r',\s*(?=control\.)',targets)
        for target in parts:
            wanted.setdefault(key(kind,target),set()).update(rights.split(','))
    # Whitespace in the existing column privilege is presentation only.
    normalize = lambda a:{k:{re.sub(r'\s+','',r) for r in v} for k,v in a.items()}
    assert normalize(after) == normalize(wanted)


def history_seed_plan(pg, native, inputs, observed, seed, *, compacted_initial=False, installed_deadline=None, fixed_initial_target=False):
    """Fresh identity/time coordinates only; complete portable bytes are ours."""
    terminal_checked(pg,seed)
    assert type(compacted_initial) is bool
    assert type(fixed_initial_target) is bool
    assert not fixed_initial_target or installed_deadline is None
    assert installed_deadline is None or compacted_initial
    assert native == (compacted_native(seed,'positive') if compacted_initial else seed[1])
    assert inputs == ({} if compacted_initial else seed[2])
    ds,p = only(pg['datasets']),only(pg['producer_epochs'])
    assert ds['topology_generation'] == 0 and ds['policy_revision'] == ds['config_revision'] == 1
    assert p['outstanding_request_limit'] == 2
    m = model.Model(pg,native,inputs,dict(before_us=observed['created_at_us'],after_us=observed['created_at_us']),initial=True)
    role,inc = (m.identity(observed[k]) for k in ('target_role','target_incarnation'))
    assert [r[0] for r in observed['domains']] == list(range(1,10))
    if fixed_initial_target:
        # The finite preparation probe uses literal domains, not a fresh writer
        # candidate. Allocation-domain freshness is independent of command IDs.
        assert observed['target_role']=='c9'*16 and observed['target_incarnation']=='ca'*16
        expected_domains=[[k,f'{160+k:02x}'*16] for k in range(1,10)]
        assert observed['domains']==expected_domains
        domains=[(k,'\\x'+v) for k,v in expected_domains]
        values={v for _,v in domains}
        assert len(values)==9 and values.isdisjoint(r['allocation_domain'] for r in pg['allocation_domains'])
    else:
        domains = [(k,m.identity(v)) for k,v in observed['domains']]
    old = bytes.fromhex(model.native_map(native)['default','0101'])
    target = bytearray(old); target[2:4]=be(2,2); target[26:34]=be(1); target[54:62]=be(2)
    target[62:78]=binary(inc)
    for k,v in domains: target[104+(k-1)*18:104+k*18]=be(k,2)+binary(v)
    target=bytes(target); digest=known(seed)['digest']
    handoff=b'\x01\x0a'+binary(p['producer_slot_id'])+be(1)+be(7)+be(27)+bytes.fromhex(digest)+be(266,2)+old+be(266,2)+target
    at=observed['created_at_us']; assert at >= p['retired_at_us']
    deadline=at+60_000_000
    if installed_deadline is not None:
        assert set(observed)=={'target_role','target_incarnation','domains','created_at_us'}
        number(installed_deadline,at+1,at+6_000_000,integer=True); deadline=installed_deadline
    plan=dict(event='successor-plan',dataset=ds['dataset_id'][2:],instance=ds['instance_id'][2:],
        resource=p['owner_resource_id'][2:],producer_slot=p['producer_slot_id'][2:],previous_epoch=1,target_epoch=2,
        shard=0,previous_topology=0,target_topology=1,target_incarnation=inc[2:],recovery_generation=old[78:94].hex(),
        target_role=role[2:],created_at_us=at,lease_expires_at_us=deadline,window=2,
        policy_revision=1,config_revision=1,policy_digest=only(pg['policy_revisions'],revision=1)['document_digest'][2:],
        config_digest=only(pg['config_revisions'],revision=1)['document_digest'][2:],domains=[[k,v[2:]] for k,v in domains],
        issued_through=7,last_complete_sequence=27,closure_digest=digest,previous_manifest=old.hex(),
        target_manifest=target.hex(),handoff=handoff.hex())
    if installed_deadline is None: assert observed == plan
    assert len(handoff)==610
    rows={(cf,bytes.fromhex(k)):bytes.fromhex(v) for (cf,k),v in model.native_map(native).items()}
    rows['default',b'\x01\x01']=target
    rows['default',b'\x01\x0a'+binary(ds['dataset_id'])+binary(p['owner_resource_id'])+be(1)]=handoff
    rows['default',b'\x01\x02'+binary(ds['dataset_id'])+be(0,4)+be(2)]=b'\x01\x02'+be(0)
    rows['receipts',b'\x01\x31'+binary(p['producer_slot_id'])+be(2)]=b'\x01\x31'+be(0)*3
    assert len(rows)==(84 if compacted_initial else 91)
    return plan,rows


def history_snapshot(event, expected_native, inputs, table_names, *, event_name='successor-state', stage):
    assert set(event)=={'event','stage','state','rows'} and event['event']==event_name and event['stage']==stage
    s=event['state']; assert set(s)=={'native_rows','native_sha256','sequence','control_sha256','control_tables','originals'}
    assert event['rows']==expected_native['rows'] and type(s['sequence']) is int and s['sequence']==expected_native['sequence']
    assert s['native_rows']==len(event['rows']) and s['native_sha256']==sha(model.encoded(event['rows']))
    assert s['originals']==[[n,inputs[n]] for n in sorted(inputs)]
    tables=s['control_tables']; assert len(tables)==77 and [n for n,_ in tables]==sorted(table_names)
    assert s['control_sha256']==sha(model.encoded(tables))
    return {n:loads(v) for n,v in tables}


def seed_closed(response,database):
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    events=[loads(v) for v in response['stdout'].splitlines()]
    assert events[-1]==dict(event='compacted-history-seed-complete',native='closed',control='released-and-joined',observer='joined',requests=1,events=0)
    present,absent=[e for e in events if e['event']=='successor-backend']
    assert present['database']==database and present['present'] is True and absent==dict(present,present=False)
    assert events[-2]==dict(event='live-complete',mode=HISTORY_SEED_MODE,native='closed',
        control='released-and-joined',writer=False,admission=True)
    return events


def verify_history_seed(events, expected, seed, case, window, *, compacted_initial=False):
    import initial_live_oracle as live
    import initial_succession_oracle as initial
    assert type(compacted_initial) is bool
    mode=JOURNEY_LIVE_MODE if compacted_initial else HISTORY_SEED_MODE
    final_event="compacted-initial-seed-complete" if compacted_initial else "compacted-history-seed-complete"
    pg,native,inputs=copy.deepcopy(expected)
    plan,rows=history_seed_plan(pg,native,inputs,only(events,event='successor-plan'),seed,compacted_initial=compacted_initial)
    live.clock(plan['created_at_us'],window)
    position=0
    def take(name,stage=None,phase=None):
        nonlocal position
        e=events[position]; position+=1
        assert (e['event'],e.get('stage'),e.get('phase'))==(name,stage,phase)
        return e
    def snap(stage): return history_snapshot(take('successor-state',stage),native,inputs,pg,stage=stage)
    assert take('successor-control','initial','claim-planned')==dict(event='successor-control',stage='initial',
        phase='claim-planned',role='d3'*16,incarnation='dd'*16,lease_ttl_us='20000000')
    same(pg,snap('initial-before-claim'))
    claimed=take('successor-control','initial','claimed'); after=snap('initial-after-claim')
    control=initial.claim(pg,after,1); model.clock(control['acquired_at_us'],window)
    assert claimed==dict(event='successor-control',stage='initial',phase='claimed',resource=control['resource_id'][2:],
        epoch=control['epoch'],role='d3'*16,incarnation='dd'*16)
    pg=after
    if compacted_initial: assert take('successor-compacted-proof')==journey_proof(seed)
    assert take('successor-plan')==plan
    after=snap('before-prepare'); control=initial.renewed(pg,after,control,plan['created_at_us'])
    model.fresh_xmin(control['xmin'],pg); pg=after
    native=dict(event='dump-state',rows=model.native_rows({(cf,k.hex()):v.hex() for (cf,k),v in rows.items()}),sequence=125 if compacted_initial else 117)
    after=snap('prepared'); initial.prepared(pg,after,plan,control,compacted_history=True); pg=after
    backend=take('successor-backend','initial')
    assert backend==dict(event='successor-backend',stage='initial',database=case['database'],pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
    assert take('live-activation-start')==dict(event='live-activation-start',mode=mode,control_epoch=control['epoch'])
    activated=take('live-activation-result'); after=snap('activated-zero')
    assert activated==dict(event='live-activation-result',writer=True,error=None,activated_at_us=activated['activated_at_us'])
    live.activated(pg,after,plan,control,window,activated['activated_at_us']); pg=after
    science=live.Science(plan,rows,inputs,compacted_history=not compacted_initial,compacted_initial=compacted_initial); receipt=science.advance('dictionary')
    assert take('live-receipt','dictionary')==dict(event='live-receipt',stage='dictionary',request=1,receipt=receipt.hex())
    native=dict(event='dump-state',rows=model.native_rows({(cf,k.hex()):v.hex() for (cf,k),v in science.rows.items()}),sequence=133 if compacted_initial else 125)
    inputs=science.inputs; after=snap('dictionary'); live.admitted(pg,after,science,1,control,window); pg=after
    same(pg,snap('final-before-control-end'))
    assert take('successor-control','final','released-and-joined')==dict(event='successor-control',stage='final',
        phase='released-and-joined',resource=control['resource_id'][2:],epoch=control['epoch'])
    after=snap('final-after-control-end'); initial.ended(pg,after,control,True)
    released=only(after['owner_epochs'],role='control',epoch=control['epoch'])
    model.fresh_xmin(released['xmin'],pg); model.clock(released['released_at_us'],window); pg=after
    assert take('successor-backend','initial')==dict(backend,present=False)
    same(pg,snap('final'))
    assert take('live-complete')==dict(event='live-complete',mode=mode,native='closed',control='released-and-joined',writer=False,admission=True)
    assert take(final_event)==dict(event=final_event,native='closed',control='released-and-joined',observer='joined',requests=1,events=0)
    assert position==len(events)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],native_rows=90 if compacted_initial else 97,native_sequence=133 if compacted_initial else 125,
        required_originals=1,initial_originals=0 if compacted_initial else 7,public_scope_ordinal=1,new_events=0)


def history_closure2(pg):
    intent=only(pg['initial_ingest_successions']); request=only(pg['producer_requests'],producer_epoch=2)
    outcome=only(pg['producer_outcomes'],producer_epoch=2)
    assert request['request_sequence']==outcome['request_sequence']==1
    return sha(b'bigbrotr/activated-initial-source-closure/1\0'+binary(intent['handoff_sha256'])+
        binary(intent['target_manifest_sha256'])+binary(intent['producer_slot_id'])+be(2)+be(1)+be(1)+
        binary(request['request_digest'])+binary(outcome['outcome_sha256'])+be(1))


def history_native_compacted(expected,seed,*,missing=False):
    k=known(seed); result=copy.deepcopy(expected[1]); values=model.native_map(result)
    head=max(r['producer_epoch'] for r in expected[0]['producer_epochs'])
    assert head in (2,3) and (not missing or head==3)
    assert (len(values),result['sequence'])==((93,138) if missing else (100,130) if head==3 else (97,126))
    if missing:
        ds=binary(only(seed[0]['datasets'])['dataset_id'])
        value=bytes.fromhex(values.pop(('facts',(b'\x01\x21'+ds+be(0,4)+be(2)+be(1)).hex())))
        assert len(value)==42 and value[:10]==b'\x01\x21'+be(1)
        sequence=139
    else:
        for row in k['receipts']: assert values.pop((row['cf'],row['key']))==row['value']
        assert values['receipts',k['state_key']]==model.native_map(seed[1])['receipts',k['state_key']]
        values['receipts',k['state_key']]=floor_bytes(seed,'positive').hex(); sequence=138 if head==3 else 134
    result.update(rows=model.native_rows(values),sequence=sequence)
    return result


def history_specification(case):
    clone=case['clone']; assert clone in HISTORY_CLONES and case['origin']=='initial'
    step=case['name'].removeprefix(clone+'-'); steps=HISTORY_STEPS[clone]
    assert step in steps and case['name']==clone+'-'+step
    i=steps.index(step); assert case['predecessor']==(clone+'-'+steps[i-1] if i else None)
    operation={'abandon':'abandon-initial','retire':'retire-initial','dictionary2':HISTORY_SEED_MODE,
        'source2':'source-activated','retire2':'retire-activated','prepare3':'successor-prepare'}.get(step,
        'successor-lineage-history' if clone=='history-strict' else 'successor-lineage-compacted-history')
    assert case['operation']==operation and re.fullmatch(r'[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return clone,step


def history_installed_closed(response,case):
    _,step=history_specification(case)
    if step in ('abandon','retire'): return process_closed(response,case)
    assert step in ('source2','retire2')
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code']==0 and t['reaped'] is True and t['forced'] is False and t['error'] is None
    report,=[loads(v) for v in response['stdout'].splitlines()]
    assert report['native']=='closed' and report['owner_release']=='committed' and report['ready'] is False
    return report


def verify_history_installed(context,seed,expected):
    _,step=history_specification(context['case'])
    if step in ('abandon','retire'): return verify(context,seed,expected)
    assert set(context)==CONTEXT_FIELDS
    case=context['case']; assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    before,after=context['before'],context['after']; same(before,expected[0])
    assert context['native_before']==expected[1] and context['inputs_before']==context['inputs_after']==expected[2]
    assert set(context['input_contents'])==set(expected[2])
    for n,value in context['input_contents'].items():
        raw=bytes.fromhex(value); assert expected[2][n]==dict(bytes=len(raw),sha256=sha(raw))
    assert context['snapshots']==[] and context['fixture']==dict(cleanup_errors=[])
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    backend_checked(context['backend'],case['database'],before,after)
    report=history_installed_closed(context['response'],case)
    m=model.Model(before,expected[1],expected[2],window); m.completed_control(after)
    p=only(m.pg['producer_epochs'],producer_epoch=2); source=only(m.pg['source_streams'],owner_epoch=2)
    digest=history_closure2(before)
    def coordinates(row,field,lower):
        at,tx=row[field],row['xmin']; model.clock(at,window); model.fresh_xmin(tx,m.pg)
        assert max(lower,m.control['acquired_at_us'])<=at<=m.control['released_at_us']<m.control['lease_expires_at_us']
        return at,tx
    if step=='source2':
        assert p['state']=='open' and source['state']=='open'
        tx=only(after['producer_epochs'],producer_epoch=2)['xmin']; model.fresh_xmin(tx,m.pg)
        p.update(state='closing',xmin=tx)
        at,tx=coordinates(only(after['source_streams'],owner_epoch=2),'closed_at_us',source['opened_at_us'])
        source.update(state='closed',last_complete_sequence=1,closure_digest='\\x'+digest,closed_at_us=at,xmin=tx)
        values=model.native_map(m.native); key=('receipts',(b'\x01\x31'+binary(m.slot)+be(2)).hex())
        assert values[key]==(b'\x01\x31'+be(1)+bytes(16)).hex()
        values[key]=(b'\x02\x31'+be(1)+bytes(16)+b'\x01').hex()
        m.native.update(rows=model.native_rows(values),sequence=126)
    else:
        assert step=='retire2' and p['state']=='closing' and source['state']=='closed'
        at,tx=coordinates(only(after['producer_epochs'],producer_epoch=2),'retired_at_us',source['closed_at_us'])
        p.update(state='retired',retired_at_us=at,closure_digest='\\x'+digest,xmin=tx)
        owner=only(m.pg['owner_epochs'],role='ingest',epoch=2)
        owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest='\\x'+digest,xmin=tx)
        only(m.pg['role_instances'],role_instance_id=owner['role_instance_id']).update(retired_at_us=at,xmin=tx)
        domains=[r for r in m.pg['allocation_domains'] if r['owner_epoch']==2]; assert len(domains)==9
        for row in domains: row.update(state='closed',closed_at_us=at,closure_digest='\\x'+digest,xmin=tx)
    same(m.pg,after); assert m.native==context['native_after']
    wanted=dict(format='bigbrotr-ingest-recovery/1',state='activated-source-closed' if step=='source2' else 'activated-initial-retired',
        dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch='2',
        issued_through='1',reserved_through='1',resolved_requests='1',source_closure=dict(last_complete_sequence='1',sha256=digest),
        native='closed',owner_release='committed',ready=False)
    if step=='retire2': wanted['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps='0',owner_release='committed',native_acknowledged='0',native_retired='0')
    assert report==wanted
    return dict(status='PASS',case=case['name'],native_rows=97,native_sequence=m.native['sequence'],input_files=8,source_digest=digest)


def history_lineage(expected,seed,*,compacted):
    pg,native,inputs=expected; intent=only(pg['initial_ingest_successions']); ds=binary(intent['dataset_id']); slot=binary(intent['producer_slot_id'])
    head=max(r['producer_epoch'] for r in pg['producer_epochs']); assert head in (2,3)
    values=model.native_map(native); initial=known(seed); digest2=history_closure2(pg)
    manifests=[binary(intent['previous_manifest']),binary(intent['target_manifest'])]
    incoming=[bytes(32),binary(intent['handoff_sha256'])]
    if head==3:
        later=only(pg['ingest_successions']); assert later['target_epoch']==3
        manifests.append(binary(later['target_manifest'])); incoming.append(binary(later['handoff_sha256']))
    entries=[]
    for i,manifest in enumerate(manifests,1):
        count,cut,digest,kind=(7,27,initial['digest'],'initial') if i==1 else (1,1,digest2,'activated') if i==2 else (0,0,None,None)
        closed=i<3
        marker=(b'\x03\x31' if compacted and i==1 else b'\x02\x31' if closed else b'\x01\x31')
        state=bytes.fromhex(values['receipts',(b'\x01\x31'+slot+be(i)).hex()])
        assert state.startswith(marker)
        if i==1:
            assert state==(floor_bytes(seed,'positive') if compacted else bytes.fromhex(model.native_map(seed[1])['receipts',initial['state_key']]))
        else: assert state==(marker+be(count)+bytes(16)+(b'\x01' if closed else b''))
        assert values['default',(b'\x01\x02'+ds+be(0,4)+be(i)).hex()]==(b'\x01\x02'+be(cut)).hex()
        entries.append(dict(manifest=manifest.hex(),issued_through=count,source_through=cut,closing=closed,
            incoming_sha256=incoming[i-1].hex(),terminal_kind=kind if i<head else None,terminal_digest=digest if i<head else None))
        if i<=2:
            producer=only(pg['producer_epochs'],producer_epoch=i); source=only(pg['source_streams'],owner_epoch=i)
            assert producer['state']=='retired' and source['state']=='closed' and producer['closure_digest']==source['closure_digest']=='\\x'+digest
    assert values['default','0101']==manifests[-1].hex()
    summaries=[dict(epoch=1,terminal_kind='initial',resolved_requests=7,last_complete_sequence=27,digest=initial['digest'],event_request_mask=120),
        dict(epoch=2,terminal_kind='activated',resolved_requests=1,last_complete_sequence=1,digest=digest2,event_request_mask=0)]
    return dict(slot=slot.hex(),head=head,through=2,state='Terminal' if head==2 else 'PreparedOpen',entries=entries,summaries=summaries,counts=[7,1])


def history_resources(t):
    resource=t.take('resources')
    assert set(resource)=={'event','metadata_bytes','metadata_scratch_bytes','metadata_entry_bytes','input_bytes','outcome_bytes',
        'effect_bytes','native_bytes','transport_bytes','observation_bytes','operation_timeout_ms','frame_bytes'}
    assert resource==dict(resource,metadata_bytes=262144,metadata_scratch_bytes=131072,metadata_entry_bytes=512,native_bytes=128*1024**2,
        transport_bytes=4*1024**2,observation_bytes=32*1024**2,operation_timeout_ms=10000,frame_bytes=16*1024**2)
    for k in ('input_bytes','outcome_bytes','effect_bytes'): number(resource[k],1,256*1024**2,integer=True)
    assert sum(resource[k] for k in ('metadata_bytes','input_bytes','outcome_bytes','effect_bytes','transport_bytes','observation_bytes'))<=1024**3


def history_claim(t,case,*,transport=None):
    m=t.model; planned=t.take('control'); assert set(planned)=={'event','phase','role','incarnation','lease_ttl_us'}
    assert planned['phase']=='claim-planned' and planned['lease_ttl_us']=='20000000'
    t.snapshot('before-claim')
    if transport is not None: assert t.take('fault')==transport
    claimed,policy,backend=t.take('control'),t.take('policy'),t.take('backend')
    t.snapshot('after-claim',lambda pg:m.claim(pg,planned))
    assert claimed==dict(event='lineage-history-control',phase='claimed',resource=m.control['resource_id'][2:],epoch=m.control['epoch'],
        role=planned['role'],incarnation=planned['incarnation'])
    assert backend==dict(event='lineage-history-backend',pid=m.control['control_backend_pid'],backend_start=m.control['control_backend_start'],
        database=case['database'],control_epoch=m.control['epoch'],present=True)
    t.policy(policy,'acquired',None,None); t.snapshot('before-read')
    return backend


def history_end(t,backend,*,success,transition=False,control_failed=False):
    assert type(control_failed) is bool
    m=t.model
    t.snapshot('after-transition' if transition else 'after-read')
    assert t.take('native')==dict(event='lineage-history-native',state='closed')
    t.snapshot('before-control-end')
    assert t.take('driver')==dict(event='lineage-history-driver',driver='control',failed=control_failed,joined=True)
    assert t.take('backend')==dict(backend,present=False)
    phase='released-and-joined' if success else 'dropped-and-joined'
    assert t.take('control')==dict(event='lineage-history-control',phase=phase,resource=m.control['resource_id'][2:],epoch=m.control['epoch'])
    t.snapshot('after-control-end',m.release if success else None); t.snapshot('final')
    assert t.take('driver')==dict(event='lineage-history-driver',driver='observer',failed=False,joined=True)
    wanted=dict(event='lineage-transition-complete' if transition else 'lineage-history-complete',native='closed',control=phase,observer='joined',success=success,admission=False)
    wanted.update(operation='prepare',activation=False) if transition else wanted.update(repair=False)
    assert t.take('complete',transition)==wanted and t.position==len(t.events)


def verify_history_probe(events,expected,seed,case,window):
    import transition_replay as tr
    clone,step=history_specification(case); prepare=step=='prepare3'; compacted=not prepare and clone!='history-strict'
    m=tr.Model(*expected,window,compacted_history=True); t=tr.Transcript(events,m)
    candidate=None
    if prepare:
        candidate=t.take('candidate',True)
        assert set(candidate)=={'event','role','incarnation','namespaces'} and [r['kind'] for r in candidate['namespaces']]==list(range(1,10))
        assert all(set(r)=={'kind','id'} for r in candidate['namespaces'])
    elif compacted:
        profile=t.take('profile')
        assert set(profile)=={'event','profile','compacted_outcome_bytes'} and profile['profile']=='compacted-initial'
        number(profile['compacted_outcome_bytes'],1,256*1024**2,integer=True)
    history_resources(t)
    if prepare:
        assert t.take('resources',True)==dict(event='lineage-transition-resources',operation='prepare',mutation_data_bytes=8192,
            mutation_native_bytes=8192,lease_ttl_us='20000000',outstanding_request_limit=2)
    backend=history_claim(t,case)
    refusal=step=='refuse'
    corrupt=refusal and clone=='history-corrupt-residual'
    if not refusal or corrupt:
        known_history=history_lineage(expected,seed,compacted=compacted)
        if not compacted: t.history(known_history)
        else:
            assert t.take('loaded')==dict(event='lineage-history-loaded',state=known_history['state'],native_epoch=known_history['head'],
                registered_epoch=known_history['head'],replay_through=2,producer_slot=known_history['slot'],pending_target=None,
                metadata_bytes=131072+512*known_history['head'],memberships=known_history['head']*(known_history['head']+1)//2,
                entries=known_history['entries'])
            t.snapshot('loaded'); t.refresh(1,None)
            if corrupt:
                assert t.take('compacted-initial-refused')==dict(event='lineage-history-compacted-initial-refused',epoch=1,cursor_unchanged=True)
            else:
                assert t.take('compacted-initial')==dict(event='lineage-history-compacted-initial',compacted_requests=7,
                    original_requests_replayed=0,last_complete_sequence=27,digest=known(seed)['digest'])
                t.snapshot('compacted-initial'); t.refresh(2,1)
                assert t.take('request')==dict(event='lineage-history-request',producer_slot=known_history['slot'],epoch=2,sequence=1,existing_copy_checked=True)
                t.snapshot('resolved-2-1'); t.refresh(2,None)
                assert t.take('epoch')==dict(event='lineage-history-epoch',summary=known_history['summaries'][1])
                t.snapshot('epoch-2'); t.refresh(None,None)
                assert t.take('compacted-proof')==dict(event='lineage-history-compacted-proof',compacted_initial_requests=7,original_initial_requests_replayed=0)
                assert t.take('proof')==dict(event='lineage-history-proof',verified_through=2,last_epoch=known_history['summaries'][1],matches_history=True,admission=False)
                t.snapshot('proof')
    if prepare:
        e=t.take('plan',True); at,deadline=e['created_at_us'],e['lease_expires_at_us']
        assert re.fullmatch('[1-9][0-9]*',at) and re.fullmatch('[1-9][0-9]*',deadline)
        plan=m.plan(candidate['role'],candidate['incarnation'],[r['id'] for r in candidate['namespaces']],3,int(at),int(deadline))
        record=tr.record_value(plan['record'],3)
        assert e==dict(event='lineage-transition-plan',operation='prepare',record=record,role=candidate['role'],created_at_us=at,
            lease_expires_at_us=deadline,outstanding_request_limit=2,policy=dict(config_revision=1,policy_revision=1,
            config_digest=only(m.pg['config_revisions'],revision=1)['document_digest'][2:],policy_digest=only(m.pg['policy_revisions'],revision=1)['document_digest'][2:]))
        t.snapshot('planned'); t.snapshot('transition-returned',lambda pg:m.prepare(pg,plan))
        assert t.take('observed',True)==dict(event='lineage-transition-observed',operation='prepare',prepared=True,record=record,activation=False)
        assert t.take('result',True)==dict(event='lineage-transition-result',operation='prepare',stage='complete',epoch=None,sequence=None,error=None,
            observation=dict(prepared=True,target_epoch=3,retired_at_us=None))
    else:
        stage='compacted-initial' if corrupt else 'inspect' if refusal else 'complete'
        error=HISTORY_ERRORS[clone] if refusal else None
        assert t.take('result')==dict(event='lineage-history-result',stage=stage,epoch=1 if corrupt else None,sequence=None,error=error)
    history_end(t,backend,success=not refusal,transition=prepare)
    return (m.pg,m.native,m.inputs),dict(status='PASS',case=case['name'],snapshots=t.snapshots,tables=77,
        native_rows=len(m.native['rows']),native_sequence=m.native['sequence'],input_files=len(m.inputs),
        compacted_initial_requests=7 if compacted and not refusal else 0,original_initial_requests_replayed=7 if prepare else 0,
        ordinary_requests_replayed=1 if not refusal else 0,result='PREPARED' if prepare else 'REFUSED' if refusal else 'READ')


def history_mutation_checked(record,expected,seed):
    assert set(record)=={'kind','mode','clone','floor','before','control','inputs','after','response'}
    assert record['kind']=='privileged-synthetic-post-GC-history-fixture' and record['clone'] in HISTORY_CLONES
    assert record['mode'] in ('install','missing-later-pending')
    missing=record['mode']=='missing-later-pending'
    assert not missing or record['clone']=='history-missing-pending'
    same(record['control'],expected[0]); assert record['before']==expected[1] and record['inputs']==expected[2]
    assert record['floor']==floor_bytes(seed,'positive').hex()
    wanted=history_native_compacted(expected,seed,missing=missing)
    reply=record['response']; assert set(reply)=={'returncode','stdout','stderr'} and type(reply['returncode']) is int
    assert reply['returncode']==0 and reply['stderr']==''
    first,last=[loads(v) for v in reply['stdout'].splitlines()]
    k=known(seed); ds=binary(only(seed[0]['datasets'])['dataset_id'])
    removed=[only(expected[1]['rows'],cf='facts',key=(b'\x01\x21'+ds+be(0,4)+be(2)+be(1)).hex())] if missing else k['receipts']
    assert first==dict(event='synthetic-post-gc-history-fixture',phase='before',mode=record['mode'],rows=expected[1]['rows'],
        sequence=expected[1]['sequence'],floor=record['floor'],removed=removed)
    assert last==dict(event='synthetic-post-gc-history-fixture',phase='after',mode=record['mode'],rows=wanted['rows'],sequence=wanted['sequence'],native='closed')
    assert record['after']==wanted
    return expected[0],wanted,expected[2]


def history_quarantine_checked(record,expected,seed,clone,step):
    assert set(record)=={'kind','status','path','before','after','quarantine_after','files','replacement'}
    assert record['kind']=='reversible-history-input-quarantine' and record['status']=='RETAINED'
    assert record['path']==clone+'/quarantined-'+step
    before,after=record['before'],record['after']
    assert set(before)=={'.',*expected[2]} and set(record['quarantine_after'])=={'.',*record['files']}
    for name,value in expected[2].items():
        meta=before[name]
        assert stat.S_ISREG(meta['mode']) and meta['mode']&0o777==0o600 and meta['links']==1
        assert {k:meta[k] for k in ('bytes','sha256')}==value
    for meta in (before['.'],after['.'],record['quarantine_after']['.']):
        assert stat.S_ISDIR(meta['mode']) and meta['mode']&0o777==0o700 and meta['sha256'] is None
    assert all(after['.'][k]==before['.'][k] for k in ('mode','inode','device','links'))
    if step!='corrupt-residual': assert record['replacement'] is None
    initial=sorted(n for n in expected[2] if n in seed[2])
    if step=='partial': names=initial[:3]; assert len(initial)==7
    elif step=='absent': names=initial; assert len(initial)==4
    elif step=='missing-required': names=[n for n in expected[2] if n not in seed[2]]; assert len(names)==1
    else: assert step=='corrupt-residual'; names=[initial[0]]; assert len(initial)==7
    assert sorted(record['files'])==names
    wanted=copy.deepcopy(expected[2])
    for name,saved in record['files'].items():
        assert set(saved)=={'hex','metadata'}
        raw=bytes.fromhex(saved['hex']); meta=saved['metadata']
        assert expected[2][name]==dict(bytes=len(raw),sha256=sha(raw))
        assert meta==before[name]==record['quarantine_after'][name]
        assert stat.S_ISREG(meta['mode']) and meta['mode']&0o777==0o600 and meta['links']==1
        wanted.pop(name)
        if step=='corrupt-residual':
            changed=raw[:-1]+bytes([raw[-1]^1]); wanted[name]=dict(bytes=len(changed),sha256=sha(changed))
            assert record['replacement']==dict(name=name,hex=changed.hex())
    assert set(after)=={'.',*wanted}
    assert stat.S_ISDIR(after['.']['mode']) and after['.']['mode']&0o777==0o700
    for name,value in wanted.items():
        assert stat.S_ISREG(after[name]['mode']) and after[name]['mode']&0o777==0o600 and after[name]['links']==1
        assert {k:after[name][k] for k in ('bytes','sha256')}==value
        if name not in names: assert after[name]==before[name]
    assert len(wanted)<=64 and sum(r['bytes'] for r in wanted.values())<=32*1024**2
    return expected[0],expected[1],wanted


def verify_history_context(context,seed,expected):
    assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
        'catalog_before','catalog_after','backend_absence','backends','custody','arguments'}
    case=context['case']; clone,step=history_specification(case)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert context['native_before']==expected[1] and context['inputs_before']==expected[2]
    assert context['catalog_before']==context['catalog_after'] and context['backends']==[]
    assert context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
    absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
    assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
    for v in absence['attempts']:
        number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
    assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
    response=context['response']; assert set(response)=={'returncode','stdout','stderr','seconds'}
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    number(response['seconds'],0,120)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
    if step!='dictionary2': args.append('262144')
    assert context['arguments']==args
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    events=[loads(v) for v in response['stdout'].splitlines()]
    if step=='dictionary2':
        seed_closed(response,case['database']); after,result=verify_history_seed(events,expected,seed,case,window)
    else:
        after,result=verify_history_probe(events,expected,seed,case,window)
    same(context['after'],after[0]); assert (context['native_after'],context['inputs_after'])==after[1:]
    assert len(after[2])<=64 and sum(r['bytes'] for r in after[2].values())<=32*1024**2
    return result


def history_controls(context,seed,expected,*,installed=False):
    verifier=verify_history_installed if installed else verify_history_context
    changes=[('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('initial-checkpoint-xmin',lambda c:only(c['after']['producer_checkpoints'],producer_epoch=1).update(xmin='4294967000')),
        ('initial-outcome-byte',lambda c:only(c['after']['producer_outcomes'],producer_epoch=1,request_sequence=1).update(outcome_bytes='\\x00')),
        ('required-input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if installed:
        changes += [('unjoined',lambda c:c['response']['terminal'].update(reaped=False)),
            ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
            ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)]))]
    else:
        changes += [('missing-custody',lambda c:c['custody'].update(status='UNKNOWN')),
            ('wrong-arguments',lambda c:c['arguments'].append('wrong')),
            ('live-backend',lambda c:c['backends'].append(dict(pid=1)))]
        if context['case']['name'].endswith(('-full','-partial','-absent')):
            def original_replay(c):
                import json
                events=[loads(v) for v in c['response']['stdout'].splitlines()]
                only(events,event='lineage-history-compacted-initial')['original_requests_replayed']=7
                c['response']['stdout']='\n'.join(json.dumps(e) for e in events)+'\n'
            changes.append(('fabricated-initial-original-replay',original_replay))
    rejected=[]
    for name,change in changes:
        c=copy.deepcopy(context); change(c)
        try: verifier(c,seed,expected)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted history capture mutation: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def history_head_checked(record,expected,seed):
    assert set(record)=={'kind','before','statement','sql_output','after'}
    assert record['kind']=='privileged-isolated-valid-head-corruption' and record['statement']==retained.ack_statement(seed)
    same(record['before'],expected[0]); returned=loads(record['sql_output']); model.fresh_xmin(returned['xmin'],expected[0])
    wanted=retained.ack_head(seed); wanted['xmin']=returned['xmin']; assert returned==wanted
    after=copy.deepcopy(expected[0]); assert after['producer_checkpoints']==seed[0]['producer_checkpoints']
    after['producer_checkpoints']=[wanted]; same(record['after'],after)
    return after,expected[1],expected[2]


# A qualified retained endpoint, not a newly emitted dump, supplies each fault
# clone. Its original failed attempt and all quarantined inputs remain separate.
FAULT_ORIGIN = dict(name='compacted',run='installed-compacted-history-6ece98c',
    subdirectory='installed-compacted-history',database='bb_compacted_history_6ece98c_history_positive',
    status='PASS_SYNTHETIC_POST_GC_SUCCESSOR_HISTORY',source='6ece98c868e842d46fc4e13a359d19dd9a924d76',
    owner='b5be20fe11a6ba90e682e33afbe4ab32',label='org.bigbrotr.installed-compacted-history',
    client_id='45e3e57671294b4749737609f20c4038b7cc30fe90e5016a33d33e91c0cad317',
    source_clone='history-positive',final_case='history-positive-absent',
    catalog_sha256='5d81aff0c2679613b5269de7cf2d718bd2bc74d9d0ceca75d3053033dde094b5',
    result_sha256='bdb3e3599c1c624ee8e1e0bf264a3b9a2f37688da431d16d5146dd45ddfc6574',
    snapshot='history-positive-final-sql.json',snapshot_sha256='8f9461276913dca7813c1f1af49fbebea94e8e71324d4b94bf28593b8ec74293',
    native_file='history-positive-absent-after-native.json',native_sha256='04c9d39b537572914fcfe43346f4296c17b8fe02f00b680f6517f7d598c1c02a',
    inputs_file='history-positive-absent-after-inputs.json',inputs_sha256='ff91e9db582bc212abf1ee9ecda78ab78f4b6f56c56a2bc7d32b4b4e2bd6d5ff',
    store='history-positive/store')
JOURNEY_ORIGIN = dict(name='compacted-initial',run='installed-compacted-reader-6ece98c',
    subdirectory='installed-compacted-reader',database='bb_compacted_reader_6ece98c_positive',
    status=STATUS,source='6ece98c868e842d46fc4e13a359d19dd9a924d76',
    owner='e7324816352cfbaa95e3cef0919bf69d',label=LABEL,
    client_id='f5d5d694fc542596c70ec5a6001ac87ec6946e810a9c72d4d7650e12dff00f3e',
    source_clone='positive',final_case='positive-absent',
    catalog_sha256='e7951ae9d3013d0acbf109c46e92571e84d5da9f876c7af16a3f41a4b1b3ba3c',
    result_sha256='80a4566505016987322b0c6690c0953c2951bc1104405e1e0c79b3e7aec9fe47',
    snapshot='positive-final-sql.json',snapshot_sha256='82eee0aba08dbe581b5027a9884ae68340b7665744079c25b240d8cb823a7c34',
    native_file='positive-absent-after-native.json',native_sha256='4eba225b321127dd42c34873f5761db3f02c5a67755f2258e39422ce0229f86d',
    inputs_file='positive-absent-after-inputs.json',inputs_sha256='ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356',
    store='positive/store')
JOURNEY_REPLAY_SHA = '5855e37b2c2ef4339e0c42b00ee985b79ea6aca1453cdb904525c3c5e391b73a'
JOURNEY_PHYSICAL_SHA = 'ef0beb7c32a1d712d99f7a854674a7b5a4d51502987420fb765597d69fa037b0'
FAULT_REPLAY_SHA = 'f70a58b8f093c86577e8f53a68e7a88c8e44665f5a3602f5147ae7f9c13274a5'
FAULT_PRODUCT = 'f4d1261fe85d69cec003a5f4266fdf1025824485'
FAULT_MODE = 'successor-lineage-compacted-fault'
FAULT_STAGES = ('after-factory','before-compacted','compacted-returned','before-final','final-returned')
FAULT_CLONES = ('baseline','head-after-factory','head-before-final','rollback-before','rollback-after','cancel','expiry')


class HistoryFaultProfile:
    DIRECTORY = 'installed-compacted-history-faults'
    LABEL = 'org.bigbrotr.installed-compacted-history-faults'
    STATUS = 'PASS_SYNTHETIC_COMPACTED_HISTORY_DELIVERY_FAULTS'
    DATABASE_PREFIX = 'bb_compacted_fault_'
    CLONES = FAULT_CLONES
    STEPS = {n:('read',) for n in FAULT_CLONES}
    CASES = tuple(n+'-read' for n in FAULT_CLONES)
    GRANTS = HISTORY_GRANTS
    NOT_RUN = ('Actual product reclamation/GC and unlink',
        'Post-confirmation client-is-closed instruction race',
        'Later-original rollback3 individually gated measurement',
        'Preparation/activation/recovery or writer publication after compaction',
        'Crash/unknown-commit, mixed restore/backup and capacity')


def fault_origin_checked(root,raw=None):
    return qualified_endpoint(root,raw,initial_journey=False)


def journey_origin_checked(root,raw=None):
    return qualified_endpoint(root,raw,initial_journey=True)


def qualified_endpoint(root,raw,*,initial_journey,prepared_origin=None,activated_origin=None,terminal_origin=None,empty_origin=None):
    """Bind the prior exact-source receipt; never replay it through new imports."""
    from installed_family_capture import physical, spool_checked
    assert type(initial_journey) is bool
    assert prepared_origin is None or (not initial_journey and prepared_origin in PREPARATION_RETAINED_ORIGINS)
    assert activated_origin is None or (not initial_journey and prepared_origin is None and activated_origin==ACTIVATED_LIVE_ORIGIN)
    assert terminal_origin is None or (not initial_journey and prepared_origin is None and activated_origin is None and terminal_origin==TERMINAL_TAIL_ORIGIN)
    assert empty_origin is None or (not initial_journey and prepared_origin is None and activated_origin is None and terminal_origin is None and empty_origin in EMPTY_PREPARED_ORIGINS)
    prepared_origin=prepared_origin or activated_origin or empty_origin
    pin=terminal_origin or prepared_origin or (JOURNEY_ORIGIN if initial_journey else FAULT_ORIGIN)
    product=EMPTY_PREPARATION_PRODUCT if empty_origin else TERMINAL_TAIL_PRODUCT if terminal_origin else PREPARATION_PRODUCT if prepared_origin else FAULT_PRODUCT
    root=Path(root); raw=raw or retained.regular_read
    basis=root.parent/pin['run']/pin['subdirectory']
    receipt_path=basis.parent/'raw-replay.json'; data=raw(receipt_path)
    assert sha(data)==(EMPTY_PREPARATION_REPLAY_SHA if empty_origin else TERMINAL_TAIL_REPLAY_SHA if terminal_origin else PREPARATION_REPLAY_SHA if prepared_origin else JOURNEY_REPLAY_SHA if initial_journey else FAULT_REPLAY_SHA)
    replay=loads(data)
    if empty_origin:
        qualified=raw(basis.parent/'verified-run.json')
        assert sha(qualified)=='e72b1cb05ccd2676329266829013b0326ff330cf0e519021ec353834a4de7786'
        receipt=loads(qualified)
        assert receipt['source']==pin['source'] and receipt['product_source']==product
        assert receipt['status']==replay['status'] and receipt['driver_exit']==receipt['replay_driver_exit']==0
        assert (receipt['cases'],receipt['checker_controls'],receipt['bound_files'])==(23,232,7898)
        for name,digest in receipt['files'].items(): assert sha(raw(basis.parent/name))==digest
    assert replay['status']==(InitialPreparationProfile.STATUS if empty_origin else ActivatedProfile.STATUS if terminal_origin else InitialJourneyProfile.STATUS if prepared_origin else STATUS if initial_journey else HistoryProfile.STATUS)+'_CAPTURE_REPLAY'
    assert replay['source']==pin['source'] and replay['product_source']==product
    assert len(replay['cases'])==(23 if empty_origin else 24 if terminal_origin else 16 if prepared_origin else 13 if initial_journey else 50)
    assert sum(v['negative_controls']['count'] for v in replay['cases'])==(232 if empty_origin else 291 if terminal_origin else 149 if prepared_origin else 131 if initial_journey else 454)
    assert len(replay['bound_files'])==(7898 if empty_origin else 7827 if terminal_origin else 6114 if prepared_origin else 4629 if initial_journey else 5482)
    def bound(path):
        path=Path(path); value=raw(path)
        assert sha(value)==replay['bound_files'][str(path)]
        return value
    def read(path): return loads(bound(path))
    report=read(basis/'result.json'); assert sha(raw(basis/'result.json'))==pin['result_sha256']
    assert all(report[k]==pin[k] for k in ('status','source','owner','label'))
    assert report['client']==pin['client_id']
    assert all(report[k]==[] for k in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'))
    clone=only(report['clones'],name=pin['source_clone']); case=only(report['cases'],name=pin['final_case'])
    assert clone['database']==pin['database'] and clone['status']==case['status']=='PASS'
    assert clone['final_case']==case['name'] and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    assert sha(clone['final_catalog'].encode())==pin['catalog_sha256']
    final=[]
    for file_key,hash_key in (('snapshot','snapshot_sha256'),('native_file','native_sha256'),('inputs_file','inputs_sha256')):
        path=basis/pin[file_key]; value=bound(path); assert sha(value)==pin[hash_key]; final.append(loads(value))
    endpoint=tuple(final)
    context=read(basis/(pin['final_case']+'-verification.json'))
    assert sha(raw(basis/(pin['final_case']+'-verification.json')))==(pin['verification_sha256'] if prepared_origin or terminal_origin else 'afc231f444467632c3962edff4160510cf837a5b12d2bb9cf61501229316ce89' if initial_journey else 'e5fb3c077346f79ff016d2acb0e35ec96ba91b18771a0e46a7cc495fe42088df')
    assert (context['after'],context['native_after'],context['inputs_after'])==endpoint
    assert context['response']==read(basis/(pin['final_case']+'-response.json'))
    if empty_origin:
        response=context['response']; code=4 if pin==EMPTY_PREPARED_ORIGINS[1] else 0
        assert case['returncode']==response['returncode']==response['terminal']['exit_code']==code
        assert response['outcome']=='PROCESS_EXITED' and response['terminal']['reaped'] is True
        assert response['terminal']['forced'] is False and response['terminal']['error'] is None
        if code: assert response['stdout']=='' and response['stderr']=='error: initial dictionary batch failed: RequestInput(Missing)\n'
        else: assert response['stderr']==''
    custody=read(basis/(pin['final_case']+'-custody.json'))
    if not initial_journey and not terminal_origin and (not prepared_origin or pin['source_clone']=='gap'): assert context['custody']==custody
    assert custody['status']=='OBSERVED_CLOSED_AND_JOINED' and not custody['errors']
    terminal=read(basis/'client-terminal.json'); assert terminal['Id']==pin['client_id']
    assert terminal['State']['Status']=='exited' and terminal['State']['Pid']==0 and terminal['State']['ExitCode']==143
    build=Path(only(terminal['Mounts'],Destination='/build')['Source'])
    built,component,qualification=[read(p) for p in (build/'build-result.json',build/'component-result.json',build.parent/'qualification.json')]
    qualification_checked(built,component,qualification,product,initial_journey=prepared_origin is not None or terminal_origin is not None,
        compacted_activated=terminal_origin is not None,initial_preparation=empty_origin is not None)
    assert report['build']==built and report['component']==component
    manifest=read(build.parent/'build/source-manifest.json')
    assert sha(raw(build.parent/'build/source-manifest.json'))==built['source_manifest_sha256']
    for name,digest in component['executables_sha256'].items(): assert sha(bound(build/name))==digest
    provenance=read(basis/'progress-source.json'); old_source=basis.parent/'source'
    assert provenance['revision']==pin['source'] and provenance['source']==str(old_source)
    assert provenance['tree']==report['tree']
    # Recheck this endpoint's actual oracle/probe/transitive source bytes, not
    # a blanket rerun of the old 5482-file capture or a relaxed source gate.
    for name,digest in provenance['files_sha256'].items():
        if name.startswith(('experiments/','apps/','crates/','migrations/')) or name in ('Cargo.toml','Cargo.lock','rust-toolchain.toml'):
            assert sha(bound(old_source/name))==digest
            if not name.startswith('experiments/'): assert manifest['files_sha256'][name]==digest
    initial=retained.origin_checked(root.parent/ORIGIN['run']/ORIGIN['subdirectory'],raw,ack_refusals=True)
    known(initial)
    if initial_journey:
        terminal_checked(endpoint[0],initial)
        assert endpoint[1]==compacted_native(initial,'positive') and endpoint[2]=={}
        assert (len(endpoint[1]['rows']),endpoint[1]['sequence'])==(81,121)
        store=basis/pin['store']
        # This prior replay qualified the logical endpoint, not its physical DB
        # files. Bind a separate complete newly observed census without opening
        # the original engine; the first excluded clone must match all81rows.
        assert not any(n.startswith(str(store)+'/') for n in replay['bound_files'])
        assert sha(model.encoded(physical(store,raw)))==JOURNEY_PHYSICAL_SHA
        spool_checked(store/'producer-inputs-v1',{},raw,complete=True)
        q=read(basis/'positive-quarantine.json'); quarantine_checked(q,initial)
        assert physical(basis/q['path'],bound)==q['quarantine_after']
        for name,value in q['files'].items(): assert bound(basis/q['path']/name).hex()==value['hex']
        return endpoint,initial
    if terminal_origin:
        assert len(endpoint[0])==77 and sum(map(len,endpoint[0].values()))==193
        producer=only(endpoint[0]['producer_epochs'],producer_epoch=2)
        assert producer['reserved_through_request']==1 and producer['state']=='closing'
        assert only(endpoint[0]['source_streams'],owner_epoch=2)['state']=='open'
        assert not [r for table in ('producer_outcomes','producer_checkpoints') for r in endpoint[0][table] if r['producer_epoch']==2]
        assert len(endpoint[2])==1
        expected=preparation_native(compacted_native(initial,'positive'),endpoint[0],initial)
        expected=activated_native(expected,producer['producer_slot_id'],close=True)
        assert endpoint[1]==expected and (len(expected['rows']),expected['sequence'])==(84,126)
    elif prepared_origin:
        assert len(endpoint[0])==77
        if empty_origin:
            compacted=pin['name']=='empty-compacted'
            expected_native=preparation_native(compacted_native(initial,'positive') if compacted else initial[1],endpoint[0],initial)
            assert endpoint[1]==expected_native and not endpoint[0]['initial_ingest_activations']
            originals=copy.deepcopy(initial[2]); missing=next(n for n in originals if n.endswith('-0000000000000003.cbor')); del originals[missing]
            assert endpoint[2]==({} if compacted else originals)
            assert (len(endpoint[1]['rows']),endpoint[1]['sequence'])==((84,125) if compacted else (91,117))
            if not compacted:
                q=read(basis/'original-prepared-required-quarantine.json')
                assert sha(raw(basis/'original-prepared-required-quarantine.json'))=='172742b7f1d1c045ec4dfaa85fc8e3d4de9f09da249d1cc9b89ef001cee15b6b'
                assert preparation_quarantine_checked(q,(endpoint[0],endpoint[1],initial[2]),initial)==endpoint
                assert physical(basis/q['path'],bound)==q['quarantine_after']
                assert bound(basis/q['path']/q['name']).hex()==q['hex']
        elif activated_origin:
            assert (len(endpoint[1]['rows']),endpoint[1]['sequence'],len(endpoint[2]))==(90,133,1)
            assert sum(len(rows) for rows in endpoint[0].values())==192
            assert only(endpoint[0]['producer_epochs'],producer_epoch=2)['reserved_through_request']==1
            assert not [r for r in endpoint[0]['producer_checkpoints'] if r['producer_epoch']==2]
            assert len([r for r in endpoint[0]['producer_outcomes'] if r['producer_epoch']==2])==1
        else:
            assert endpoint[2]=={}
            assert endpoint[1]==preparation_native(compacted_native(initial,'positive'),endpoint[0],initial)
            assert (len(endpoint[1]['rows']),endpoint[1]['sequence'])==(84,125)
        if empty_origin: pass
        elif pin['source_clone']=='gap': assert endpoint[0]['initial_ingest_activations']==[]
        else:
            assert len(endpoint[0]['initial_ingest_activations'])==1
            assert only(endpoint[0]['producer_epochs'],producer_epoch=2)['reserved_through_request']==(1 if activated_origin else 0)
    else:
        history_lineage(endpoint,initial,compacted=True)
    assert prepared_origin or terminal_origin or len(endpoint[0])==77 and (len(endpoint[1]['rows']),endpoint[1]['sequence'],len(endpoint[2]))==(93,138,1)
    assert prepared_origin or all('-0000000000000002-' in n for n in endpoint[2])
    store=basis/pin['store']
    spool_checked(store/'producer-inputs-v1',endpoint[2],bound,complete=True)
    files=physical(store,bound)
    prefix=str(store)+'/'
    qualified={name[len(prefix):]:digest for name,digest in replay['bound_files'].items() if name.startswith(prefix)}
    assert len(qualified)==(16 if empty_origin and pin['name']=='empty-compacted' else 23 if empty_origin else 18 if terminal_origin else 17 if activated_origin else 16 if prepared_origin else 20)
    assert {name:value['sha256'] for name,value in files.items() if stat.S_ISREG(value['mode'])}==qualified
    directories={str(parent) for name in qualified for parent in Path(name).parents}
    if prepared_origin: directories.add('producer-inputs-v1')
    assert {name for name,value in files.items() if stat.S_ISDIR(value['mode'])}==directories
    if prepared_origin or terminal_origin: return endpoint,initial
    for step in ('partial','absent'):
        q=read(basis/('history-positive-'+step+'-quarantine.json'))
        assert physical(basis/q['path'],bound)==q['quarantine_after']
        for name,value in q['files'].items():
            data=bound(basis/q['path']/name); assert data.hex()==value['hex']
            assert initial[2][name]==dict(bytes=len(data),sha256=sha(data))
    return endpoint,initial


def fault_specification(case):
    name=case['clone']; assert name in FAULT_CLONES
    assert case['name']==name+'-read' and case['origin']=='compacted' and case['predecessor'] is None
    assert case['operation']==FAULT_MODE and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,('wire' if name.startswith('rollback-') else name)


def fault_wire_event(value):
    from fault_cases import safe_event
    extra={'rollback_requests','rollback_completions','rollback_released','rollback_ready','rollback_number','before_forward'}
    assert extra<=value.keys()
    safe_event({k:v for k,v in value.items() if k not in extra})
    assert type(value['before_forward']) is bool
    for key in extra-{'before_forward'}: number(value[key],0,64,integer=True)
    counts=[value[k] for k in ('rollback_requests','rollback_completions','rollback_released','rollback_ready')]
    assert counts==sorted(counts,reverse=True)
    assert value['commit_number']==64 and value['mode']=='pass'
    return value


def fault_counts(value):
    return [value[k] for k in ('rollback_requests','rollback_completions','rollback_released','rollback_ready')]


def fault_backend(value,backend,window,*,held_before=False):
    assert set(value)=={'observed_at_us','pid','backend_start','database','user','state','xact_start','query'}
    assert value['pid']==backend['pid'] and value['database']==backend['database'] and value['user']=='bigbrotr_control'
    assert retained.instant(value['backend_start'])==retained.instant(backend['backend_start'])
    model.clock(value['observed_at_us'],window)
    assert value['state']==('idle in transaction' if held_before else 'idle')
    assert (value['xact_start'] is not None)==held_before
    if held_before:
        assert value['query'].startswith('SELECT control.require_control_owner(')
        assert retained.instant(value['xact_start'])<=value['observed_at_us']
    else: assert value['query'] in ('COMMIT','ROLLBACK')


def fault_closed(response,database,wire):
    """Gate reopening on complete native/driver/proxy custody, never the error."""
    assert response['returncode']==0 and response['outcome']=='PROCESS_EXITED'
    assert response['readers_joined'] is True and response['overflow'] is False and response['stderr']==''
    assert wire['status']=='CAPTURED' and wire['exit_code']==0 and wire['readers_joined'] is True
    assert wire['forced_wrapper_stop'] is False and wire['stderr']=='' and wire['overflow'] is False
    events=[loads(v) for v in response['stdout'].splitlines()]
    assert not any(v['event']=='lineage-history-timeout' for v in events)
    native,=[i for i,v in enumerate(events) if v==dict(event='lineage-history-native',state='closed')]
    drivers=[(i,v) for i,v in enumerate(events) if v['event']=='lineage-history-driver']
    assert len(drivers)==2
    control,observer=drivers
    assert set(control[1])=={'event','driver','failed','joined'} and control[1]['driver']=='control'
    assert type(control[1]['failed']) is bool and control[1]['joined'] is True
    assert observer[1]==dict(event='lineage-history-driver',driver='observer',failed=False,joined=True)
    present,absent=[v for v in events if v['event']=='lineage-history-backend']
    assert present['present'] is True and present['database']==database and absent==dict(present,present=False)
    assert native<control[0]<events.index(absent)<observer[0]<len(events)-1
    complete=events[-1]
    assert complete['event']=='lineage-history-complete' and complete['native']=='closed' and complete['observer']=='joined'
    assert complete['control'] in ('released-and-joined','dropped-and-joined')
    return events


def verify_history_fault(context,initial,expected,baseline=None):
    import transition_replay as tr
    assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response',
        'server_window','catalog_before','catalog_after','backend_absence','backends','custody','arguments','fault'}
    case=context['case']; name,mode=fault_specification(case)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before'])==expected[1:]
    assert context['native_after']==expected[1] and context['inputs_after']==expected[2]
    assert context['catalog_before']==context['catalog_after'] and context['backends']==[]
    assert context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
    assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
    for item in absence['attempts']:
        number(item['seconds'],0,5); assert type(item['count']) is int and item['count']==len(item['backends'])
    assert absence['attempts'][-1]['count']==0
    fault=context['fault']; assert set(fault)=={'mode','prefix','target','before_forward','wire','gates','held','errors'}
    assert fault['mode']==mode and fault['prefix']=='/lab/'+case['name']+'-gate' and fault['errors']==[]
    target=2 if name in ('rollback-before','rollback-after','cancel','expiry') else 0
    assert fault['target']==target and fault['before_forward']==(name=='rollback-before')
    wire=fault['wire']; assert set(wire)=={'status','sources','ready','receipt','events','stdout','stderr','exit_code','readers_joined','overflow','forced_wrapper_stop','command'}
    assert set(wire['sources'])=={'fault_proxy.py','rollback_proxy.py'}
    assert all(re.fullmatch('[0-9a-f]{64}',h) for h in wire['sources'].values())
    events=[fault_wire_event(loads(v)) for v in wire['stdout'].splitlines()]; assert events==wire['events']
    assert events[0]==wire['ready'] and events[-1]==wire['receipt']
    ready=wire['ready']; port=ready['listen_port']; number(port,1,65535,integer=True)
    assert ready['event']=='ready' and ready['rollback_number']==target and ready['before_forward']==fault['before_forward']
    assert ready['proxy_sha256']==wire['sources']['fault_proxy.py'] and ready['listen_host']=='127.0.0.1' and ready['upstream_port']==5432
    assert ready['deadline_seconds']==60 and ready['max_frame_bytes']==1024**2 and ready['max_wire_bytes']==64*1024**2 and ready['max_messages']==20000
    assert fault_counts(ready)==[0]*4 and ready['faults']==0
    proxy_args=['docker','exec','-i',wire['command'][3],'python3','-B','/lab/rollback_proxy.py','--upstream-port','5432','--listen-port','0','--rollback-number',str(target)]
    if fault['before_forward']: proxy_args.append('--before-forward')
    assert wire['command']==proxy_args
    args=[FAULT_MODE,case['database'],'/run/secrets/application',case['config'],'262144',str(port),mode,fault['prefix']]
    assert context['arguments']==args
    response=context['response']; assert set(response)=={'returncode','stdout','stderr','seconds','outcome','readers_joined','overflow','command'}
    assert response['command']==['docker','exec',wire['command'][3],'/build/ingest_bootstrap_probe',*args]
    number(response['seconds'],0,120)
    parsed=fault_closed(response,case['database'],wire)
    loss=name.startswith('rollback-')
    driver=only(parsed,event='lineage-history-driver',driver='control')
    assert not driver['failed'] or loss
    m=tr.Model(*expected,window,compacted_history=True); t=tr.Transcript(parsed,m)
    profile=t.take('profile'); assert set(profile)=={'event','profile','compacted_outcome_bytes'} and profile['profile']=='compacted-initial'
    number(profile['compacted_outcome_bytes'],1,256*1024**2,integer=True)
    history_resources(t)
    backend=history_claim(t,case,transport=dict(event='lineage-history-fault',mode=mode,control_port=port,observer_port=5432,prefix=fault['prefix'],proxied=True))
    known_history=history_lineage(expected,initial,compacted=True)
    assert t.take('loaded')==dict(event='lineage-history-loaded',state='PreparedOpen',native_epoch=3,registered_epoch=3,replay_through=2,
        producer_slot=known_history['slot'],pending_target=None,metadata_bytes=132608,memberships=6,entries=known_history['entries'])
    t.snapshot('loaded')
    observed_gates=[]; clocks=[]
    def stage(value,error=None):
        assert t.take('fault-stage')==dict(event='lineage-history-fault-stage',mode=mode,stage=value,error=error)
    def gate(value):
        index=len(observed_gates); record=fault['gates'][index]
        assert set(record)=={'ready','status','backend','control','head','go_before_us','go_after_us','released'}
        path=fault['prefix']+('.'+value+'.go' if mode=='baseline' else '.go')
        common=dict(event='lineage-history-fault-gate',mode=mode,stage=value,prefix=fault['prefix'],path=path,gate_timeout_ms=10000,poll_ms=5)
        assert t.take('fault-gate')==record['ready']==dict(common,phase='ready')
        sample=record['status']; assert sample in events and sample['event']=='status'
        assert sample['rollback_number']==target and sample['faults']==0
        count={'after-factory':1,'before-compacted':1,'compacted-returned':2,'before-final':4,'final-returned':5}[value]
        assert fault_counts(sample)==[count]*4
        assert len({sample[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')})==1
        fault_backend(record['backend'],backend,window); same(record['control'],m.pg)
        model.clock(record['go_before_us'],window); model.clock(record['go_after_us'],window)
        assert record['backend']['observed_at_us']<=record['go_before_us']<=record['go_after_us']
        if name in ('head-after-factory','head-before-final'):
            assert value==name.removeprefix('head-')
            changed=history_head_checked(record['head'],(m.pg,m.native,m.inputs),initial)
            xid=only(changed[0]['producer_checkpoints'])['xmin']; m.fresh(xid)
            m.pg['producer_checkpoints']=changed[0]['producer_checkpoints']
        else: assert record['head'] is None
        assert t.take('fault-gate')==record['released']==dict(common,phase='released')
        observed_gates.append(record)
    def clock(value):
        item=t.take('fault-clock'); assert set(item)=={'event','mode','stage','server_us','control_deadline_us','remaining_us','error'}
        assert item['mode']==mode and item['stage']==value and item['error'] is None
        model.clock(item['server_us'],window)
        assert item['control_deadline_us']==m.control['lease_expires_at_us'] and item['remaining_us']==item['control_deadline_us']-item['server_us']
        if clocks: assert item['server_us']>=clocks[-1]['server_us']
        clocks.append(item); return item
    cursor=dict(event='lineage-history-fault-cursor',mode=mode,current_epoch=1,needs_compacted_initial=True,next_request=None)
    stage('after-factory')
    if name in ('baseline','head-after-factory'): gate('after-factory')
    t.refresh(1,None); assert t.take('fault-cursor')==dict(cursor,stage='before')
    stage('before-compacted')
    if name=='baseline': gate('before-compacted')
    if name in ('cancel','expiry'):
        refreshed=clock('refreshed'); assert refreshed['remaining_us']>0
        gate('before-compacted'); dispatch=clock('dispatch')
        assert dispatch['remaining_us']>0
        if name=='expiry': assert 1_000_000<=dispatch['remaining_us']<=2_000_000
        else: assert t.take('fault-cancel')==dict(event='lineage-history-fault-cancel',mode=mode,phase='applied',cancelled=True)
    early=name not in ('baseline','head-before-final')
    error='Progress(InvalidContext)' if name.startswith('head-') else 'DeliveryUnknown' if early else None
    returned=dict(cursor,stage='returned') if early else dict(cursor,stage='returned',current_epoch=2,needs_compacted_initial=False,
        next_request=dict(producer_slot=known_history['slot'],epoch=2,sequence=1))
    assert t.take('fault-cursor')==returned
    stage('compacted-returned',error if early else None)
    if name=='baseline': gate('compacted-returned')
    if name in ('cancel','expiry'):
        last=clock('returned')
        if name=='expiry': assert last['remaining_us']<=0
    if early:
        assert t.take('compacted-initial-refused')==dict(event='lineage-history-compacted-initial-refused',epoch=1,cursor_unchanged=True)
    else:
        assert t.take('compacted-initial')==dict(event='lineage-history-compacted-initial',compacted_requests=7,original_requests_replayed=0,
            last_complete_sequence=27,digest=known(initial)['digest'])
        t.snapshot('compacted-initial'); t.refresh(2,1)
        assert t.take('request')==dict(event='lineage-history-request',producer_slot=known_history['slot'],epoch=2,sequence=1,existing_copy_checked=True)
        t.snapshot('resolved-2-1'); t.refresh(2,None)
        assert t.take('epoch')==dict(event='lineage-history-epoch',summary=known_history['summaries'][1])
        t.snapshot('epoch-2'); t.refresh(None,None)
        stage('before-final'); gate('before-final')
        stage('final-returned',error)
        if name=='baseline':
            gate('final-returned')
            assert t.take('compacted-proof')==dict(event='lineage-history-compacted-proof',compacted_initial_requests=7,original_initial_requests_replayed=0)
            assert t.take('proof')==dict(event='lineage-history-proof',verified_through=2,last_epoch=known_history['summaries'][1],matches_history=True,admission=False)
            t.snapshot('proof')
    assert len(observed_gates)==len(fault['gates'])
    assert t.take('result')==dict(event='lineage-history-result',stage='compacted-initial' if early else 'finish' if name=='head-before-final' else 'complete',
        epoch=1 if early else None,sequence=None,error=error)
    history_end(t,backend,success=name=='baseline',control_failed=driver['failed'])
    same(m.pg,context['after'])
    held=fault['held']; receipt=wire['receipt']; assert receipt['event']=='receipt'
    expected_events=[ready,*[g['status'] for g in observed_gates]]
    assert all(v['rollback_number']==target and v['before_forward']==fault['before_forward'] for v in events)
    assert [v['elapsed_ns'] for v in events]==sorted(v['elapsed_ns'] for v in events)
    if target:
        assert baseline is not None and baseline['status']=='PASS' and baseline['case']=='baseline-read'
        assert set(held)=={'wire','backend','control','action_before_us','action_after_us','cancel','expiry'}
        h=held['wire']; assert h in events and h['event']=='held' and h['faults']==1
        expected_events.append(h)
        assert fault_counts(h)==([2,1,1,1] if fault['before_forward'] else [2,2,1,1])
        commits=baseline['wire_stages']['before-compacted']['commits']
        assert all(h[k]==commits for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete'))
        fault_backend(held['backend'],backend,window,held_before=fault['before_forward'])
        # Only the normal Control refreshes precede this hold; no operation SQL writes.
        same(held['control'],m.pg)
        for k in ('action_before_us','action_after_us'): model.clock(held[k],window)
        assert held['backend']['observed_at_us']<=held['action_before_us']<=held['action_after_us']
        if name=='cancel':
            assert held['cancel']==dict(event='lineage-history-fault-cancel',mode='cancel',phase='applied',cancelled=True)
            assert held['expiry']==[]
        elif name=='expiry':
            assert held['cancel'] is None and held['expiry']
            previous=held['backend']['observed_at_us']
            for item in held['expiry']:
                assert set(item)=={'server_us','remaining_us'}; number(item['server_us'],previous,window['after_us'],integer=True); previous=item['server_us']
                assert item['remaining_us']==m.control['lease_expires_at_us']-item['server_us']
            assert held['expiry'][-1]['remaining_us']<=-50_000 and previous<=held['action_before_us']
        else: assert held['cancel'] is None and held['expiry']==[]
        assert receipt['faults']==1
        if loss:
            assert receipt['reason']=='held-close' and receipt['hold_action']=='close'
            assert fault_counts(receipt)==fault_counts(h)
        else:
            released=only(events,event='released')
            assert released==dict(h,event='released',elapsed_ns=released['elapsed_ns'],held=False,hold_action='release')
            expected_events.append(released)
            # The two joined pumps race on clean close; only their exact EOF
            # outcomes are interchangeable, never a product error or boundary.
            assert receipt['reason'] in ('frontend-eof','backend-eof') and receipt['hold_action']=='release'
            assert fault_counts(receipt)==[2]*4
    else:
        assert held is None and receipt['faults']==0 and receipt['reason'] in ('frontend-eof','backend-eof')
        assert receipt['hold_action'] is None
        if name=='baseline': assert fault_counts(receipt)==[5]*4
    assert receipt['frames'] and receipt['declared_wire_bytes']<=64*1024**2
    assert events==[*expected_events,receipt]
    return dict(status='PASS',case=case['name'],tables=77,native_rows=93,native_sequence=138,input_files=1,
        positive_proof=name=='baseline',compacted_summary_delivered=not early,original_initial_requests_replayed=0,
        error=error,control_driver_failed=driver['failed'],
        wire_stages={g['ready']['stage']:dict(rollbacks=fault_counts(g['status']),commits=g['status']['commits_seen']) for g in observed_gates})


def history_fault_controls(context,initial,expected,baseline=None):
    def cursor(c):
        import json
        events=[loads(v) for v in c['response']['stdout'].splitlines()]
        row=only(events,event='lineage-history-fault-cursor',stage='returned')
        row['current_epoch']=99
        c['response']['stdout']='\n'.join(json.dumps(v) for v in events)+'\n'
    def boundary(c):
        fault=c['fault']; row=fault['held']['wire'] if fault['held'] else fault['gates'][0]['status']
        row['rollback_completions']+=1
    changes=[('native-sequence',lambda c:c['native_after'].update(sequence=139)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('checkpoint-xmin',lambda c:only(c['after']['producer_checkpoints']).update(xmin='4294967000')),
        ('required-input-loss',lambda c:c['inputs_after'].clear()),
        ('catalog-change',lambda c:c.update(catalog_after='changed')),
        ('wire-source',lambda c:c['fault']['wire']['sources'].update({'fault_proxy.py':'00'*32})),
        ('wire-unjoined',lambda c:c['fault']['wire'].update(readers_joined=False)),
        ('wrong-arguments',lambda c:c['arguments'].append('wrong')),
        ('unknown-outcome',lambda c:c['response'].update(outcome='UNKNOWN')),
        ('live-backend',lambda c:c['backends'].append(dict(pid=1))),
        ('wrong-rollback-boundary',boundary),('cursor-advance',cursor)]
    name=context['case']['clone']
    if name.startswith('head-'):
        changes.append(('head-change-not-exact',lambda c:only(c['fault']['gates'][0]['head']['after']['producer_checkpoints']).update(checkpoint_revision=99)))
    elif name=='expiry':
        changes.append(('deadline-still-live',lambda c:c['fault']['held']['expiry'][-1].update(remaining_us=1)))
    rejected=[]
    for name,change in changes:
        value=copy.deepcopy(context); change(value)
        try: verify_history_fault(value,initial,expected,baseline)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration,IndexError): rejected.append(name)
        else: raise AssertionError('accepted altered compacted fault evidence: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


JOURNEY_LIVE_MODE = 'successor-live-compacted-initial-seed'
JOURNEY_PREPARE_MODE = 'successor-compacted-normal'
JOURNEY_GAP_MODE = 'successor-compacted-gap-only'
JOURNEY_READ_MODE = 'successor-compacted-prepared-recover'
# Exact source-derived refusal chains; positive raw engine runs qualify them separately.
JOURNEY_FAILURES = {
    'head': ('compacted-load','Progress(InvalidContext)'),
    'target-nonzero': ('compacted-native','StateMismatch'),
    'target-input': ('compacted-native','RequestInput(Identity)'),
}
JOURNEY_STEPS = {
    'prepared': ('prepare','inspect','strict'),
    'gap': ('prepare-gap','current-refuse','complete','inspect'),
    'live': ('dictionary','strict'),
    'head': ('prepare','refuse'),
    'target-nonzero': ('prepare','refuse'),
    'target-input': ('prepare','refuse'),
    'installed': ('serve',),
}


class InitialJourneyProfile:
    DIRECTORY = 'installed-compacted-initial-journey'
    LABEL = 'org.bigbrotr.installed-compacted-initial-journey'
    STATUS = 'PASS_SYNTHETIC_COMPACTED_INITIAL_SUCCESSION'
    DATABASE_PREFIX = 'bb_compacted_initial_'
    CLONES = tuple(JOURNEY_STEPS)
    STEPS = JOURNEY_STEPS
    CASES = tuple(n+'-'+s for n in CLONES for s in JOURNEY_STEPS[n])
    GRANTS = JOURNEY_GRANTS
    NOT_RUN = ('Actual product reclamation/GC and unlink',
        'Activated replacement recovery, later live frontier, Archive and empty retirement',
        'Crash/unknown-commit and interrupted unlink', 'Mixed restore/backup and capacity')


def journey_proof(initial):
    return dict(event='successor-compacted-proof',compacted_requests=7,original_requests_replayed=0,
        last_complete_sequence=27,closure_digest=known(initial)['digest'])


def journey_specification(case):
    name=case['clone']; assert name in JOURNEY_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in JOURNEY_STEPS[name]
    assert case['name']==name+'-'+step and case['origin']=='compacted-initial'
    index=JOURNEY_STEPS[name].index(step)
    assert case['predecessor']==(name+'-'+JOURNEY_STEPS[name][index-1] if index else None)
    operation={'prepare':JOURNEY_PREPARE_MODE,'prepare-gap':JOURNEY_GAP_MODE,'current-refuse':'retire-initial',
        'dictionary':JOURNEY_LIVE_MODE,'strict':'successor-prepared-recover','serve':'live-initial'}.get(step,JOURNEY_READ_MODE)
    assert case['operation']==operation and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def journey_closed(response,database):
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    events=[loads(v) for v in response['stdout'].splitlines()]
    complete=events[-1]
    assert complete['event'] in ('successor-complete','successor-history-complete','successor-gap-complete','compacted-initial-seed-complete')
    assert complete['native']=='closed' and complete['control'] in ('released-and-joined','dropped-and-joined')
    backends=[e for e in events if e['event']=='successor-backend']
    if backends:
        before,after=backends
        assert before['database']==database and before['present'] is True and after==dict(before,present=False)
        number(before['pid'],1,integer=True)
        assert before['backend_start']
    if complete['event'] in ('successor-history-complete','successor-gap-complete'):
        driver,=[e for e in events if e['event']=='successor-driver']
        assert driver==dict(event='successor-driver',stage='history' if complete['event']=='successor-history-complete' else 'gap',failed=False,joined=True)
    if 'readers_joined' in response:
        assert response['readers_joined'] is True and response['overflow'] is False and response['outcome']=='PROCESS_EXITED'
    return events


def journey_dump(rows,sequence):
    return dict(event='dump-state',rows=model.native_rows({(cf,k.hex()):v.hex() for (cf,k),v in rows.items()}),sequence=sequence)


def verify_journey_prepare(events,expected,initial,case,window,gate,*,compacted_initial=True):
    import initial_succession_oracle as preparation
    assert type(compacted_initial) is bool
    pg,native,inputs=copy.deepcopy(expected)
    basis='compacted' if compacted_initial else 'original'
    gap=case['operation']=='successor-'+basis+'-gap-only'
    assert case['operation']=='successor-'+basis+('-gap-only' if gap else '-normal')
    plan,rows=history_seed_plan(pg,native,inputs,only(events,event='successor-plan'),initial,compacted_initial=compacted_initial,fixed_initial_target=True)
    assert plan['target_role']=='c9'*16 and plan['target_incarnation']=='ca'*16
    assert plan['domains']==[[k,f'{160+k:02x}'*16] for k in range(1,10)]
    model.clock(plan['created_at_us'],window)
    position=0; control=None
    def take(event,stage=None,phase=None):
        nonlocal position
        value=events[position]; position+=1
        assert (value['event'],value.get('stage'),value.get('phase'))==(event,stage,phase),value
        return value
    def snap(stage): return history_snapshot(take('successor-state',stage),native,inputs,pg,stage=stage)
    def claim(stage,ordinal):
        nonlocal pg,control
        role,inc=f'{210+ordinal:02x}'*16,f'{220+ordinal:02x}'*16
        assert take('successor-control',stage,'claim-planned')==dict(event='successor-control',stage=stage,phase='claim-planned',role=role,incarnation=inc,lease_ttl_us='20000000')
        same(pg,snap(stage+'-before-claim'))
        claimed=take('successor-control',stage,'claimed'); after=snap(stage+'-after-claim')
        control=preparation.claim(pg,after,ordinal); model.clock(control['acquired_at_us'],window)
        assert claimed==dict(event='successor-control',stage=stage,phase='claimed',role=role,incarnation=inc,
            resource=control['resource_id'][2:],epoch=control['epoch'])
        pg=after
    def end(stage,release):
        nonlocal pg
        same(pg,snap(stage+'-before-control-end'))
        phase='released-and-joined' if release else 'dropped-and-joined'
        assert take('successor-control',stage,phase)==dict(event='successor-control',stage=stage,phase=phase,
            resource=control['resource_id'][2:],epoch=control['epoch'])
        after=snap(stage+'-after-control-end'); preparation.ended(pg,after,control,release)
        if release: model.fresh_xmin(only(after['owner_epochs'],role='control',epoch=control['epoch'])['xmin'],pg)
        pg=after
    def attempt(stage,first=False):
        nonlocal pg,native
        assert take('successor-attempt',stage)==dict(event='successor-attempt',stage=stage,role=plan['target_role'],control_epoch=control['epoch'])
        if gap:
            assert take('successor-'+basis+'-gap-native')==dict(event='successor-'+basis+'-gap-native',native=native)
            assert gate is not None and gate['errors']==[]
            for phase in ('ready','released'):
                wanted=dict(event='successor-'+basis+'-gap-gate',phase=phase,path='/lab/'+case['name']+'-gate.go',timeout_ms=10000,poll_ms=5)
                assert take('successor-'+basis+'-gap-gate',phase=phase)==gate[phase]==wanted
            preparation.prepared(pg,gate['control'],plan,control,compacted_history=True)
            assert gate['native']==native and gate['inputs']==inputs
            for k in ('observed_before_us','observed_after_us','go_before_us','go_after_us'): model.clock(gate[k],window)
            assert gate['observed_before_us']<=gate['observed_after_us']<=gate['go_before_us']<=gate['go_after_us']
        error='Control(InvalidPolicy)' if gap else None
        assert take('successor-result',stage)==dict(event='successor-result',stage=stage,control_epoch=control['epoch'],validations=3 if gap else 4,error=error)
        if gap: assert take('successor-session-poisoned')==dict(event='successor-session-poisoned',control_epoch=control['epoch'])
        elif first: native=journey_dump(rows,125 if compacted_initial else 117)
        after=snap(stage)
        if first: preparation.prepared(pg,after,plan,control,compacted_history=True)
        else: same(pg,after)
        if gap: same(after,gate['control'])
        pg=after
    claim('initial',1)
    if compacted_initial: assert take('successor-compacted-proof')==journey_proof(initial)
    assert take('successor-plan')==plan
    after=snap('before-prepare'); control=preparation.renewed(pg,after,control,plan['created_at_us']); model.fresh_xmin(control['xmin'],pg); pg=after
    attempt('prepared-native-old' if gap else 'prepared',True)
    if not gap:
        attempt('same-control-retry'); end('first',True); claim('fresh',2); attempt('fresh-control-retry')
    end('final',not gap); same(pg,snap('final'))
    assert take('successor-complete')==dict(event='successor-complete',mode=case['operation'],native='closed',
        control='dropped-and-joined' if gap else 'released-and-joined',admission=False)
    assert position==len(events)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],result='PREPARED_NATIVE_GAP' if gap else 'PREPARED_WITH_EXACT_RETRIES',
        native_rows=len(native['rows']),native_sequence=native['sequence'],compacted_requests=7 if compacted_initial else 0,original_requests_replayed=0 if compacted_initial else 7)


def journey_plan_from_intent(pg,initial):
    intent=only(pg['initial_ingest_successions'])
    assert intent['issued_through']==7 and intent['last_complete_sequence']==27
    assert intent['closure_digest']=='\\x'+known(initial)['digest'] and intent['outstanding_request_limit']==2
    old=binary(intent['previous_manifest']); target=binary(intent['target_manifest'])
    assert sha(old)==MANIFEST and target[:4]==b'\x01\x01\x00\x02' and target[54:62]==be(2)
    handoff=b'\x01\x0a'+binary(intent['producer_slot_id'])+be(1)+be(7)+be(27)+bytes.fromhex(known(initial)['digest'])+be(266,2)+old+be(266,2)+target
    assert binary(intent['handoff'])==handoff
    for key in ('previous_manifest','target_manifest','handoff'): assert sha(binary(intent[key]))==intent[key+'_sha256'][2:]
    return intent,handoff


def verify_journey_read(events,expected,initial,case,window):
    import initial_history_oracle as original
    import initial_succession_oracle as preparation
    pg,native,inputs=copy.deepcopy(expected); name,step=journey_specification(case)
    intent,handoff=journey_plan_from_intent(pg,initial)
    route='gap' if native['sequence']==121 else 'history'
    strict=step=='strict'; failed=strict or step=='refuse'
    # Exact stage/errors below are source-derived; runtime is a separate result.
    failure=('history-load','Progress(Native(StateMismatch))' if name=='prepared' else 'Native(StateMismatch)') if strict else JOURNEY_FAILURES.get(name)
    position=0
    def take(event,stage=None,phase=None):
        nonlocal position
        value=events[position]; position+=1
        assert (value['event'],value.get('stage'),value.get('phase'))==(event,stage,phase),value
        return value
    def snap(stage):
        stage=route+'-'+stage
        return history_snapshot(take('successor-state',stage),native,inputs,pg,stage=stage)
    manifest=model.native_map(native)['default','0101']
    assert take('successor-recovery-selection')==dict(event='successor-recovery-selection',route=route,owner_epoch=1 if route=='gap' else 2,
        key_schema=1 if route=='gap' else 2,manifest=manifest,admission=False)
    planned=take('successor-control',route,'claim-planned')
    assert set(planned)=={'event','stage','phase','role','incarnation','lease_ttl_us'} and planned['lease_ttl_us']=='20000000'
    same(pg,snap('before-claim'))
    claimed=take('successor-control',route,'claimed'); policy=take('successor-policy',route,'acquired'); backend=take('successor-backend',route)
    after=snap('after-claim'); prior=max((r for r in pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    control=original.claim(pg,after,planned,window,exclude_previous=prior['state']=='active'); pg=after
    assert claimed==dict(event='successor-control',stage=route,phase='claimed',resource=control['resource_id'][2:],epoch=control['epoch'],role=planned['role'],incarnation=planned['incarnation'])
    assert policy==dict(event='successor-policy',stage=route,phase='acquired',sequence=None,control_epoch=control['epoch'],config_revision=1,policy_revision=1,
        config_digest=only(pg['config_revisions'],revision=1)['document_digest'][2:],policy_digest=only(pg['policy_revisions'],revision=1)['document_digest'][2:])
    assert backend==dict(event='successor-backend',stage=route,pid=control['control_backend_pid'],backend_start=control['control_backend_start'],database=case['database'],control_epoch=control['epoch'],present=True)
    same(pg,snap('before-read'))
    if not failed:
        proof=dict(event='successor-'+route+'-compacted-proof',compacted_requests=7,original_requests_replayed=0,last_complete_sequence=27,
            closure_digest=known(initial)['digest'],previous_manifest=intent['previous_manifest'][2:],target_manifest=intent['target_manifest'][2:],handoff=handoff.hex())
        assert take(proof['event'])==proof; same(pg,snap('compacted-loaded'))
        if route=='gap':
            rows={(cf,bytes.fromhex(k)):bytes.fromhex(v) for (cf,k),v in model.native_map(native).items()}
            rows['default',b'\x01\x01']=binary(intent['target_manifest'])
            rows['default',b'\x01\x0a'+binary(intent['dataset_id'])+binary(intent['owner_resource_id'])+be(1)]=handoff
            rows['default',b'\x01\x02'+binary(intent['dataset_id'])+be(0,4)+be(2)]=b'\x01\x02'+be(0)
            rows['receipts',b'\x01\x31'+binary(intent['producer_slot_id'])+be(2)]=b'\x01\x31'+be(0)*3
            assert len(rows)==84; native=journey_dump(rows,125)
        for stage in ('completed','exact-retry'):
            assert take('successor-'+route+'-compacted-completion',stage)==dict(event='successor-'+route+'-compacted-completion',stage=stage,handoff=handoff.hex(),admission=False)
            same(pg,snap(stage))
    assert take('successor-'+route+'-result',failure[0] if failed else 'complete')==dict(event='successor-'+route+'-result',stage=failure[0] if failed else 'complete',sequence=None,error=failure[1] if failed else None)
    same(pg,snap('after-read')); same(pg,snap('before-control-end'))
    assert take('successor-driver',route)==dict(event='successor-driver',stage=route,failed=False,joined=True)
    assert take('successor-backend',route)==dict(backend,present=False)
    phase='dropped-and-joined' if failed else 'released-and-joined'
    assert take('successor-control',route,phase)==dict(event='successor-control',stage=route,phase=phase,resource=control['resource_id'][2:],epoch=control['epoch'])
    after=snap('after-control-end'); preparation.ended(pg,after,control,not failed); pg=after
    same(pg,snap('final'))
    assert take('successor-'+route+'-complete')==dict(event='successor-'+route+'-complete',native='closed',control=phase,admission=False,success=not failed)
    assert position==len(events)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],result='REFUSED' if failed else 'RECONSTRUCTED_EXISTING_INTENT',
        native_rows=len(native['rows']),native_sequence=native['sequence'],compacted_requests=0 if failed else 7,original_requests_replayed=0)


def journey_mutation_checked(record,expected,initial,name):
    pg,native,inputs=expected
    assert record['kind']=='isolated-compacted-initial-'+name
    assert record['before']==dict(control=pg,native=native,inputs=inputs)
    if name=='head':
        result=history_head_checked(record['head'],expected,initial)
    elif name=='target-nonzero':
        intent,_=journey_plan_from_intent(pg,initial)
        key=(b'\x01\x02'+binary(intent['dataset_id'])+be(0,4)+be(2)).hex()
        wanted=copy.deepcopy(native); row=only(wanted['rows'],cf='default',key=key)
        assert row['value']==(b'\x01\x02'+be(0)).hex(); row['value']=(b'\x01\x02'+be(1)).hex(); wanted['sequence']+=1
        response=record['response']; assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
        events=[loads(v) for v in response['stdout'].splitlines()]
        assert events==[dict(event='successor-history-corruption',phase=phase,mutation='target-nonzero',cf='default',key=key,
            value=(b'\x01\x02'+be(n)).hex(),sequence=state['sequence'],rows=state['rows'])
            for phase,n,state in [('before',0,native),('after',1,wanted)]]
        result=pg,wanted,inputs
    else:
        assert name=='target-input' and inputs=={}
        source_name=only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000001.cbor'
        target_name=source_name.replace('-0000000000000001-','-0000000000000002-',1)
        data=bytes.fromhex(record['input']['hex']); assert initial[2][source_name]==dict(bytes=len(data),sha256=sha(data))
        assert record['input']['source_name']==source_name and record['input']['target_name']==target_name
        assert record['input']['before']=={'.':record['input']['before']['.']}
        after=record['input']['after']; assert set(after)=={'.',target_name}
        v=after[target_name]; assert stat.S_ISREG(v['mode']) and stat.S_IMODE(v['mode'])==0o600 and v['links']==1
        assert (v['bytes'],v['sha256'])==(len(data),sha(data))
        result=pg,native,{target_name:dict(bytes=len(data),sha256=sha(data))}
    assert record['after']==dict(control=result[0],native=result[1],inputs=result[2])
    return result


def journey_installed_closed(response,case):
    _,step=journey_specification(case); code=4 if step=='current-refuse' else 0
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert type(response['returncode']) is int and response['returncode']==code and response['outcome']=='PROCESS_EXITED'
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv(case['operation'],Path(case['config']))
    assert t['exit_code']==code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    if code:
        assert response['stdout']=='' and response['stderr']=='error: producer operation unavailable: Control(InvalidMetadata)\n'
        return None
    assert response['stderr']==''
    activated,stopped=[loads(v) for v in response['stdout'].splitlines()]
    assert stopped==dict(format='bigbrotr-initial-successor/1',state='stopped',native='closed',owner_release='committed',source_closure='not-performed',producer_epoch='2',ready=False)
    return activated


def verify_journey_context(context,initial,expected):
    case=context['case']; name,step=journey_specification(case)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert context['native_before']==expected[1] and context['inputs_before']==expected[2]
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    if step in ('current-refuse','serve'):
        assert set(context)==CONTEXT_FIELDS
        report=journey_installed_closed(context['response'],case)
        backend_checked(context['backend'],case['database'],expected[0],context['after'])
        assert context['fixture']['cleanup_errors']==[]
        if step=='current-refuse':
            assert context['snapshots']==[] and context['input_contents']=={} and expected[2]=={}
            import installed_live_oracle as installed
            installed.startup(expected[0],context['after'],None,window)
            assert context['native_after']==expected[1] and context['inputs_after']==expected[2]
            return dict(status='PASS',case=case['name'],result='NO_SUCCESSOR_REFUSED',native_rows=81,native_sequence=121)
        return verify_journey_installed(context,initial,expected,report)
    assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
        'catalog_before','catalog_after','backend_absence','backends','custody','arguments','gate'}
    assert context['backends']==[] and context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
    absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
    assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
    for v in absence['attempts']:
        number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
    assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
    response=context['response']; number(response['seconds'],0,120)
    args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
    if step=='prepare-gap': args.append('/lab/'+case['name']+'-gate')
    else: assert context['gate'] is None
    assert context['arguments']==args
    events=journey_closed(response,case['database'])
    if step=='dictionary': after,result=verify_history_seed(events,expected,initial,case,window,compacted_initial=True)
    elif step in ('prepare','prepare-gap'): after,result=verify_journey_prepare(events,expected,initial,case,window,context['gate'])
    else: after,result=verify_journey_read(events,expected,initial,case,window)
    same(context['after'],after[0]); assert (context['native_after'],context['inputs_after'])==after[1:]
    assert len(after[2])<=64 and sum(r['bytes'] for r in after[2].values())<=32*1024**2
    return result


def journey_controls(context,initial,expected):
    changes=[('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('checkpoint-xmin',lambda c:only(c['after']['producer_checkpoints'],producer_epoch=1).update(xmin='4294967000')),
        ('outcome-byte',lambda c:only(c['after']['producer_outcomes'],producer_epoch=1,request_sequence=1).update(outcome_bytes='\\x00')),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if context['case']['operation'] in ('retire-initial','live-initial'):
        changes += [('unjoined-process',lambda c:c['response']['terminal'].update(reaped=False)),
            ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
            ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)]))]
    else:
        changes += [('missing-custody',lambda c:c['custody'].update(status='UNKNOWN')),
            ('wrong-arguments',lambda c:c['arguments'].append('wrong')),('live-backend',lambda c:c['backends'].append(dict(pid=1)))]
        if context['gate'] is not None:
            changes.append(('gap-native-dispatched',lambda c:c['gate']['native'].update(sequence=125)))
    if context['inputs_after']:
        changes.append(('required-input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))))
    events=[loads(v) for v in context['response']['stdout'].splitlines()]
    proof_events=[v for v in events if 'original_requests_replayed' in v]
    if proof_events or context['case']['operation']=='live-initial':
        def fabricated_originals(value):
            import json
            events=[loads(v) for v in value['response']['stdout'].splitlines()]
            if value['case']['operation']=='live-initial': events[0]['predecessor']['resolved_requests']='7'
            else: only([v for v in events if 'original_requests_replayed' in v])['original_requests_replayed']=7
            value['response']['stdout']='\n'.join(json.dumps(v) for v in events)+'\n'
        changes.append(('fabricated-original-replay-count',fabricated_originals))
    if context['case']['operation']=='live-initial':
        def shifted_immutable_deadline(value):
            import json
            deadline=only(value['snapshots'][0]['control']['initial_ingest_successions'])['lease_expires_at_us']+1
            for capture in [value['after'],*(s['control'] for s in value['snapshots']),value['fixture']['renewal']['current']['control']]:
                only(capture['initial_ingest_successions'])['lease_expires_at_us']=deadline
            events=[loads(v) for v in value['response']['stdout'].splitlines()]
            events[0]['initial_lease_expires_at_us']=str(deadline)
            value['response']['stdout']='\n'.join(json.dumps(v) for v in events)+'\n'
            value['fixture']['renewal']['initial_deadline_us']=deadline
        changes.append(('coordinated-immutable-deadline-shift',shifted_immutable_deadline))
        def stale_control(value):
            old=max((r for r in value['snapshots'][0]['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            renewed=value['snapshots'][1]
            assert old['lease_expires_at_us']<only(renewed['control']['owner_epochs'],role='ingest',epoch=2)['lease_expires_at_us']
            for capture in (renewed['control'],value['fixture']['renewal']['current']['control']):
                row=only(capture['owner_epochs'],role='control',epoch=old['epoch']); row.clear(); row.update(copy.deepcopy(old))
        def shared_renewal_transaction(value):
            for capture in (value['snapshots'][1]['control'],value['fixture']['renewal']['current']['control']):
                control=max((r for r in capture['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                only(capture['owner_epochs'],role='ingest',epoch=2)['xmin']=control['xmin']
        def early_release(value):
            control=max((r for r in value['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            at=value['fixture']['signal']['before_us']-1
            control['released_at_us']=at
            only(value['after']['role_instances'],role_instance_id=control['role_instance_id'])['retired_at_us']=at
        changes += [('coordinated-stale-control-snapshot',stale_control),
            ('coordinated-shared-renewal-transaction',shared_renewal_transaction),
            ('shared-final-release-transaction',lambda c:only(c['after']['owner_epochs'],role='ingest',epoch=2).update(
                xmin=max((r for r in c['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])['xmin'])),
            ('coordinated-release-before-signal',early_release),
            ('renewal-clock-outside-window',lambda c:c['fixture']['renewal'].update(observed_at_us=c['server_window']['after_us']+1)),
            ('renewal-clock-after-snapshot',lambda c:c['fixture']['renewal'].update(observed_at_us=c['snapshots'][1]['server_us']+1))]

    rejected=[]
    for name,change in changes:
        value=copy.deepcopy(context); change(value)
        try: verify_journey_context(value,initial,expected)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted compacted initial capture mutation: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def journey_expected_preparation(before,plan,control,transaction):
    """Independent expected intermediate; never normalize a captured snapshot.

    The same immutable recipe as initial_succession_oracle.prepared, retaining
    that verifier as a second complete comparison of this bounded constructor.
    """
    import initial_succession_oracle as original
    model.fresh_xmin(transaction,before)
    p=plan; tx=transaction; role='\\x'+p['target_role']; inc='\\x'+p['target_incarnation']; at=p['created_at_us']
    wanted=copy.deepcopy(before); old=only(before['owner_epochs'],role='ingest')
    wanted['owner_epochs'].append(dict(old,epoch=2,role_instance_id=role,process_incarnation=inc,acquired_at_us=at,
        lease_expires_at_us=p['lease_expires_at_us'],state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=tx))
    wanted['role_instances'].append(dict(only(before['role_instances'],role_instance_id=old['role_instance_id']),
        role_instance_id=role,created_at_us=at,retired_at_us=None,xmin=tx))
    wanted['producer_epochs'].append(dict(only(before['producer_epochs']),producer_epoch=2,producer_role_instance_id=role,
        producer_incarnation=inc,owner_epoch=2,owner_incarnation=inc,state='open',reserved_through_request=0,outstanding_request_limit=2,
        opened_at_us=at,retired_at_us=None,closure_digest=None,xmin=tx))
    wanted['source_streams'].append(dict(only(before['source_streams']),owner_epoch=2,owner_incarnation=inc,state='open',opened_at_us=at,
        closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin=tx))
    for kind,domain in p['domains']:
        wanted['allocation_domains'].append(dict(only(before['allocation_domains'],dictionary_kind=kind),allocation_domain='\\x'+domain,
            owner_epoch=2,owner_incarnation=inc,state='active',created_at_us=at,closed_at_us=None,closure_digest=None,xmin=tx))
    wanted['topology_generations'].append(dict(only(before['topology_generations']),generation=1,previous_generation=0,created_at_us=at,sealed=True,xmin=tx))
    wanted['topology_streams'] += [dict(only(before['topology_streams']),topology_generation=1,owner_epoch=e,
        handoff_last_complete_sequence=27 if e==1 else None,xmin=tx) for e in (1,2)]
    wanted['datasets']=[dict(only(before['datasets']),topology_generation=1,xmin=tx)]
    wanted['initial_ingest_successions']=[dict(dataset_id='\\x'+p['dataset'],owner_resource_id='\\x'+p['resource'],target_epoch=2,owner_role='ingest',
        target_incarnation=inc,target_role_instance_id=role,producer_slot_id='\\x'+p['producer_slot'],producer_epoch=2,shard_id=0,previous_topology=0,target_topology=1,
        issued_through=7,last_complete_sequence=27,closure_digest='\\x'+p['closure_digest'],previous_manifest='\\x'+p['previous_manifest'],
        target_manifest='\\x'+p['target_manifest'],handoff='\\x'+p['handoff'],previous_manifest_sha256='\\x'+sha(bytes.fromhex(p['previous_manifest'])),
        target_manifest_sha256='\\x'+sha(bytes.fromhex(p['target_manifest'])),handoff_sha256='\\x'+sha(bytes.fromhex(p['handoff'])),
        config_revision=1,policy_revision=1,config_digest='\\x'+p['config_digest'],policy_digest='\\x'+p['policy_digest'],outstanding_request_limit=2,
        created_at_us=at,lease_expires_at_us=p['lease_expires_at_us'],prepared_by_resource=control['resource_id'],prepared_by_epoch=control['epoch'],
        prepared_by_role='control',prepared_by_incarnation=control['process_incarnation'],xmin=tx)]
    original.prepared(before,wanted,plan,control,compacted_history=True)
    return wanted


def journey_lease_delta(expected,observed,window,control_epoch,*,stopped=False):
    import initial_succession_oracle as initial
    before=copy.deepcopy(expected)
    assert only(observed['owner_epochs'],role='control',epoch=control_epoch)['xmin']!=only(observed['owner_epochs'],role='ingest',epoch=2)['xmin']
    for role,epoch in (('control',control_epoch),('ingest',2)):
        old=only(before['owner_epochs'],role=role,epoch=epoch); actual=only(observed['owner_epochs'],role=role,epoch=epoch)
        row=only(expected['owner_epochs'],role=role,epoch=epoch)
        deadline=actual['lease_expires_at_us']; number(deadline,old['lease_expires_at_us'],window['after_us']+6_000_000,integer=True)
        model.clock(deadline-6_000_000,window)
        if actual['xmin']!=old['xmin']: model.fresh_xmin(actual['xmin'],before)
        else: assert deadline==old['lease_expires_at_us']
        row.update(lease_expires_at_us=deadline,xmin=actual['xmin'])
        if stopped and role=='control':
            at=actual['released_at_us']; model.clock(at,window); assert deadline-6_000_000<=at<deadline
            assert actual['xmin']!=old['xmin']
            row.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=initial.exclusion(old,old['process_incarnation']))
            role_row=only(expected['role_instances'],role_instance_id=old['role_instance_id'])
            actual_role=only(observed['role_instances'],role_instance_id=old['role_instance_id'])
            model.clock(actual_role['retired_at_us'],window); assert at<=actual_role['retired_at_us']
            role_row.update(retired_at_us=actual_role['retired_at_us'],xmin=actual['xmin'])
    same(expected,observed)


def verify_journey_installed(context,initial,expected,report):
    import installed_live_oracle as installed
    import initial_live_oracle as live
    before,native,inputs=expected; window=context['server_window']; snapshots=context['snapshots']; case=context['case']
    assert [s['stage'] for s in snapshots]==['activated','renewed','before-stop'] and inputs==context['inputs_after']=={} and context['input_contents']=={}
    for item in snapshots:
        assert set(item)=={'stage','server_us','control','inputs','observed_by_us'} and item['inputs']=={}
        model.clock(item['server_us'],window); model.clock(item['observed_by_us'],window)
        assert item['server_us']<=item['observed_by_us']
        control=max((r for r in item['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        target=only(item['control']['owner_epochs'],role='ingest',epoch=2)
        assert control['state']==target['state']=='active' and control['xmin']!=target['xmin']
        assert control['lease_expires_at_us']>=target['lease_expires_at_us']>item['server_us']
    assert snapshots[0]['observed_by_us']<=snapshots[1]['server_us']
    assert snapshots[1]['observed_by_us']<=snapshots[2]['server_us']
    activated=snapshots[0]['control']; intent=only(activated['initial_ingest_successions'])
    coordinates=dict(target_role=intent['target_role_instance_id'][2:],target_incarnation=intent['target_incarnation'][2:],created_at_us=intent['created_at_us'],
        domains=[[k,only(activated['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'][2:]] for k in range(1,10)])
    plan,rows=history_seed_plan(before,native,inputs,coordinates,initial,compacted_initial=True,installed_deadline=intent['lease_expires_at_us'])
    # Only fresh Control coordinates come from this actual startup. This model
    # constructs every expected owner/role column from the qualified predecessor.
    m=model.Model(*expected,window,initial=True)
    control=retained.failed_control(m,activated,renewed=True)
    assert plan['lease_expires_at_us']==control['acquired_at_us']+6_000_000
    installed.startup(before,m.pg,None,window)
    preparation=journey_expected_preparation(m.pg,plan,control,intent['xmin'])
    activation=only(activated['initial_ingest_activations']); model.fresh_xmin(activation['xmin'],preparation)
    expected_activation=copy.deepcopy(preparation)
    fields=('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id','producer_epoch',
        'previous_manifest_sha256','target_manifest_sha256','handoff_sha256','prepared_by_resource','prepared_by_epoch','prepared_by_incarnation',
        'config_revision','policy_revision','config_digest','policy_digest')
    wanted_intent=only(preparation['initial_ingest_successions'])
    expected_activation['initial_ingest_activations']=[dict({k:wanted_intent[k] for k in fields},activated_by_resource=control['resource_id'],
        activated_by_epoch=control['epoch'],activated_by_incarnation=control['process_incarnation'],activated_at_us=activation['activated_at_us'],
        activation_transaction=activation['activation_transaction'],xmin=activation['xmin'])]
    live.activated(preparation,expected_activation,plan,control,window,activation['activated_at_us'])
    # These projections are independently constructed expected states. The
    # unchanged actual snapshot is compared in full after exact lease deltas.
    journey_lease_delta(expected_activation,activated,window,control['epoch'])
    wanted=dict(format='bigbrotr-initial-successor/1',state='activated',dataset_id=plan['dataset'],owner_resource_id=plan['resource'],
        producer_slot_id=plan['producer_slot'],producer_epoch='2',process_incarnation=plan['target_incarnation'],topology_generation='1',config_revision='1',policy_revision='1',
        config_sha256=plan['config_digest'],policy_sha256=plan['policy_digest'],outstanding_request_limit='2',created_at_us=str(plan['created_at_us']),
        initial_lease_expires_at_us=str(plan['lease_expires_at_us']),activated_at_us=str(activation['activated_at_us']),
        predecessor=dict(producer_epoch='1',issued_through='7',reserved_through='7',resolved_requests='0',compacted_requests='7',
            recovery_basis='closed-producer-floor-v3',last_complete_sequence='27',sha256=known(initial)['digest']),
        native_profile=1,native_allowance_bytes=134217728,acquisition='not-started',ready=False,
        listeners=[dict(kind='private-session-http',host='127.0.0.1',port=18081,profile='initial-source-commands')])
    assert report==wanted
    for snapshot in snapshots[1:]: journey_lease_delta(expected_activation,snapshot['control'],window,control['epoch'])
    fixture=context['fixture']; assert set(fixture)=={'cleanup_errors','renewal','signal'}
    renewal=fixture['renewal']; assert renewal==dict(initial_deadline_us=plan['lease_expires_at_us'],observed_at_us=renewal['observed_at_us'],
        initial_target=only(activated['owner_epochs'],role='ingest',epoch=2),current=snapshots[1])
    model.clock(renewal['observed_at_us'],window)
    assert snapshots[0]['observed_by_us']<=renewal['observed_at_us']<=snapshots[1]['server_us']
    assert renewal['observed_at_us']>max(plan['lease_expires_at_us'],plan['created_at_us']+6_000_000)
    assert only(snapshots[1]['control']['owner_epochs'],role='ingest',epoch=2)['lease_expires_at_us']>plan['lease_expires_at_us']
    signal=fixture['signal']; assert set(signal)=={'kind','identity','before_us','after_us'} and signal['kind']=='SIGTERM'
    identity={k:context['response']['terminal'][k] for k in ('pid','start_ticks','command','config','prefix')}; assert signal['identity']==identity
    model.clock(signal['before_us'],window); model.clock(signal['after_us'],window)
    assert snapshots[-1]['observed_by_us']<=signal['before_us']<=signal['after_us']
    journey_lease_delta(expected_activation,context['after'],window,control['epoch'],stopped=True)
    assert only(context['after']['owner_epochs'],role='control',epoch=control['epoch'])['released_at_us']>=signal['before_us']
    assert context['native_after']==journey_dump(rows,125)
    return dict(status='PASS',case=case['name'],result='INSTALLED_ACTIVATED_RENEWED_STOPPED',native_rows=84,native_sequence=125,
        compacted_requests=7,original_requests_replayed=0,epoch2_originals=0,acquisition='NOT_RUN')


# Explicit existing-intent installed completion. These routes never prepare a
# target in the CLI, activate it, retire it, or reclaim an input/receipt.
PREPARATION_ORIGINAL_GAP = 'successor-original-gap-only'
PREPARATION_STEPS = {
    'original-gap': ('abandon','retire','prepare-gap','complete','retry'),
    'original-prepared': ('abandon','retire','prepare','complete','retry','missing-original'),
    'compacted-gap': ('missing-intent','prepare-gap','complete','retry'),
    'compacted-prepared': ('complete','retry'),
    'head': ('refuse',), 'outcome': ('refuse',), 'target-nonzero': ('refuse',),
    'target-closing': ('refuse',), 'target-input': ('refuse',), 'activated-zero': ('refuse',),
}
PREPARATION_ORIGIN_NAMES = {n:'initial' if n.startswith('original-') else 'compacted-initial' if n=='compacted-gap'
    else 'activated-initial' if n=='activated-zero' else 'prepared-initial' for n in PREPARATION_STEPS}
PREPARATION_PRODUCT = '9bea0ff71c5573f3d470ed7ab8a27e827bac2743'
PREPARATION_REPLAY_SHA = '46b5ff1773a1bf1ea3d4ecf2fb025209d86698c890acc4acfd0f43ec55b879d7'
PREPARATION_RETAINED_ORIGINS = tuple(dict(
    name=name,run='installed-compacted-initial-journey-f51a8a3',subdirectory='installed-compacted-initial-journey',
    database='bb_compacted_initial_f51a8a3_'+clone,status=InitialJourneyProfile.STATUS,
    source='f51a8a344907c259fd53320511aa552658ea45f6',owner='322f878d86032d1ba2f0f5473214d906',label=InitialJourneyProfile.LABEL,
    client_id='90222c7d5dc6253def5d7c6390a831764071bf409c15d5798567321b93c28018',source_clone=clone,final_case=case,
    catalog_sha256='3c97ef7d8b0c915115f8d285387f0cf1bc08a852ce100e572f7d4b8de176b73c',
    result_sha256='8eb90d549905667b1f683c7f57675f9d84bb0d5123c7381b2774462421908556',
    snapshot=clone+'-final-sql.json',snapshot_sha256=sql_sha,native_file=case+'-after-native.json',native_sha256=native_sha,
    inputs_file=case+'-after-inputs.json',inputs_sha256='ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356',
    verification_sha256=context_sha,store=clone+'/store') for name,clone,case,sql_sha,native_sha,context_sha in (
    ('prepared-initial','gap','gap-inspect','1da24330fb513c0d93a64b53aba69d2c344122af81e321e8c198f5c3dcb1ddbe',
     'c1582e3425b4b1c8b6fa100919e3f6527993de2f004ed536a2d47a9217c52b70','94e3fc1427163787110da9a90bae0b75200aa8869061030607e268340c4651cd'),
    ('activated-initial','installed','installed-serve','c1e57b4ec46fd42eb703664ab39c51a79dcd4f2c3acddc78c849cc9f8adb744e',
     '46855de80b2cdcd1b818fce97dac7a5c6fd9139ee80c174f1049dcb6a84391f6','80775b8e49747a18298281e5b6fdaa7c2d01da4d2af9b5d7f3188580998de977')))
PREPARATION_ORIGINS = (ORIGIN,JOURNEY_ORIGIN,*PREPARATION_RETAINED_ORIGINS)


class InitialPreparationProfile:
    DIRECTORY = 'installed-initial-preparation'
    LABEL = 'org.bigbrotr.installed-initial-preparation'
    STATUS = 'PASS_INSTALLED_INITIAL_PREPARATION_COMPLETION'
    DATABASE_PREFIX = 'bb_initial_preparation_'
    CLONES = tuple(PREPARATION_STEPS)
    STEPS = PREPARATION_STEPS
    CASES = tuple(n+'-'+s for n in CLONES for s in PREPARATION_STEPS[n])
    GRANTS = JOURNEY_GRANTS
    NOT_RUN = ('Actual product reclamation/GC and unlink',
        'Genuine epoch2/SQL-pending3 gap; completed native3 is not that fixture',
        'Initial completion rollback/commit delivery loss, cancellation and lease expiry',
        'Activated replacement recovery, later live frontier, Archive and empty retirement',
        'Mixed restore/backup and capacity; synthetic target-closing is not a product closing journey')


def preparation_qualification(component):
    names=component['test_names']
    assert set(names)=={'compacted_prefix','compacted_progress','initial_reconstruction','request_inputs','successor_history',
        'archive_retained_frontier','claim_custody','control_runtime_entrypoint'}
    prefix='storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_gap_tests::compacted_handoff_tests::'
    assert {prefix+n for n in ('initial_preparation_selector_accepts_original_and_compacted_without_writes',
        'initial_preparation_selector_refuses_incompatible_head_and_census',
        'initial_preparation_selector_requires_epoch_two_budget_and_cancellation')} <= set(names['initial_reconstruction'])
    assert set(names['claim_custody'])=={'interfaces::serve::initial_ingest::recovery::successor_family::tests::'+n
        for n in ('interrupted_claim_closes_native_before_retained_future','claim_guard_covers_unwind_and_disarms_after_delivery')}
    assert {'explicit_ingest_recovery_preserves_missing_store_before_credentials',
        'explicit_recovery_modes_conflict_before_configuration_io'} <= set(names['control_runtime_entrypoint'])


def preparation_origins_checked(root,raw=None):
    endpoint,initial=journey_origin_checked(root,raw)
    results={'initial':initial,'compacted-initial':endpoint}
    for pin in PREPARATION_RETAINED_ORIGINS:
        value,basis=qualified_endpoint(root,raw,initial_journey=False,prepared_origin=pin)
        assert basis==initial
        results[pin['name']]=value
    return results,initial


def preparation_specification(case,*,delivery=False):
    assert type(delivery) is bool
    if delivery: return preparation_delivery_specification(case)
    name=case['clone']; assert name in PREPARATION_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in PREPARATION_STEPS[name]
    assert case['name']==name+'-'+step and case['origin']==PREPARATION_ORIGIN_NAMES[name]
    index=PREPARATION_STEPS[name].index(step)
    assert case['predecessor']==(name+'-'+PREPARATION_STEPS[name][index-1] if index else None)
    if step in ('prepare','prepare-gap'):
        basis='original' if name.startswith('original-') else 'compacted'
        operation='successor-'+basis+('-normal' if step=='prepare' else '-gap-only')
    else: operation={'abandon':'abandon-initial','retire':'retire-initial'}.get(step,'complete-initial')
    assert case['operation']==operation and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def preparation_native(native,pg,initial):
    """Four literal handoff writes constructed from independently checked intent."""
    intent,handoff=journey_plan_from_intent(pg,initial)
    values=model.native_map(native).copy()
    assert values['default','0101']==intent['previous_manifest'][2:]
    additions={('default',(b'\x01\x0a'+binary(intent['dataset_id'])+binary(intent['owner_resource_id'])+be(1)).hex()):handoff.hex(),
        ('default',(b'\x01\x02'+binary(intent['dataset_id'])+be(0,4)+be(2)).hex()):(b'\x01\x02'+be(0)).hex(),
        ('receipts',(b'\x01\x31'+binary(intent['producer_slot_id'])+be(2)).hex()):(b'\x01\x31'+be(0)*3).hex()}
    assert set(additions).isdisjoint(values)
    values.update(additions); values['default','0101']=intent['target_manifest'][2:]
    return dict(event='dump-state',rows=model.native_rows(values),sequence=native['sequence']+4)


# The exact Display chains are source expectations, qualified only by the real
# installed profile. No alternative error, stage or successful report is accepted.
PREPARATION_ERRORS = {
    'missing-intent': 'producer operation unavailable: Control(InvalidMetadata)',
    'head': 'producer operation unavailable: Progress(InvalidContext)',
    'outcome': 'producer operation unavailable: Control(Backend)',
    'activated-zero': 'producer operation unavailable: Control(Backend)',
    'target-nonzero': 'initial dictionary batch failed: StateMismatch',
    'target-closing': 'initial dictionary batch failed: StateMismatch',
    'target-input': 'initial dictionary batch failed: RequestInput(Identity)',
    'missing-original': 'initial dictionary batch failed: RequestInput(Missing)',
    'delivery-before': 'producer operation unavailable: Control(Backend)',
    'delivery-after': 'producer operation unavailable: DeliveryUnknown',
}
PREPARATION_PRECLAIM = {'target-nonzero','target-closing','target-input','missing-original'}


def preparation_failure(name,step,*,delivery=False):
    if delivery: return 'delivery-'+name.rsplit('-',1)[1] if step=='loss' else None
    return step if step in ('missing-intent','missing-original') else name if step=='refuse' else None


def preparation_closed(response,case,*,delivery=False):
    name,step=preparation_specification(case,delivery=delivery)
    if step in ('abandon','retire'): return process_closed(response,case)
    failure=preparation_failure(name,step,delivery=delivery); code=4 if failure else 0
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==code
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code']==code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failure:
        assert response['stdout']=='' and response['stderr']=='error: '+PREPARATION_ERRORS[failure]+'\n'
        return None
    assert response['stderr']==''
    report,=[loads(v) for v in response['stdout'].splitlines()]
    return report


def preparation_no_claim(backend,database):
    assert set(backend)=={'absence','exact','all'} and backend['exact']==backend['all']==[]
    a=backend['absence']; assert set(a)=={'database','user','limit_seconds','status','attempts','seconds'}
    assert a['database']==database and a['user']=='bigbrotr_control' and a['status']=='OBSERVED_ABSENT' and a['limit_seconds']==5
    assert a['attempts']
    for v in a['attempts']:
        assert set(v)=={'seconds','count','backends'}; number(v['seconds'],0,5)
        assert type(v['count']) is int and v['count']==0 and v['backends']==[]
    number(a['seconds'],a['attempts'][-1]['seconds'],6)


def preparation_control(m,observed,*,success,renewed=False,lease_us=6_000_000):
    import initial_succession_oracle as original
    assert type(lease_us) is int and lease_us in (6_000_000,8_000_000)
    before=copy.deepcopy(m.pg)
    previous=max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    if previous['state']=='active':
        # The genuine gap's preparer was joined, not released. The next real
        # claim fences it in the same transaction that creates the new role.
        current=only(observed['owner_epochs'],role='control',epoch=previous['epoch']+1)
        released=only(observed['owner_epochs'],role='control',epoch=previous['epoch'])
        retired=only(observed['role_instances'],role_instance_id=previous['role_instance_id'])
        new_role=only(observed['role_instances'],role_instance_id=current['role_instance_id'])
        model.clock(released['released_at_us'],m.window); model.clock(retired['retired_at_us'],m.window)
        model.fresh_xmin(released['xmin'],m.pg)
        assert previous['acquired_at_us']<=released['released_at_us']<=retired['retired_at_us']<=new_role['created_at_us']<=current['acquired_at_us']
        old_role=only(m.pg['role_instances'],role_instance_id=previous['role_instance_id'])
        assert retired==dict(old_role,retired_at_us=retired['retired_at_us'],xmin=released['xmin'])
        assert released==dict(previous,state='released',released_at_us=released['released_at_us'],exclusion_method='storage_fenced',
            exclusion_digest=original.exclusion(previous,current['process_incarnation']),xmin=released['xmin'])
        previous.update(released); old_role.update(retired)
        if success: assert int(current['xmin'])>int(released['xmin'])
        elif not renewed: assert current['xmin']==new_role['xmin']==released['xmin']
        else: assert new_role['xmin']==released['xmin']
    if success: m.completed_control(observed,lease_us=lease_us)
    else:
        # The original snapshot owns freshness: fencing and the new claim may
        # share one transaction. The expected fence above must not make that
        # transaction appear pre-existing. A later refresh has a distinct xmin.
        current=only(observed['owner_epochs'],role='control',epoch=previous['epoch']+1)
        role=only(observed['role_instances'],role_instance_id=current['role_instance_id'])
        rid,inc=m.identity(role['role_instance_id'][2:]),m.identity(current['process_incarnation'][2:])
        for at in (role['created_at_us'],current['acquired_at_us'],current['lease_expires_at_us']-lease_us): model.clock(at,m.window)
        assert m.window['before_us']<=retained.instant(current['control_backend_start'])<=role['created_at_us']<=current['acquired_at_us']
        model.fresh_xmin(role['xmin'],before); model.fresh_xmin(current['xmin'],before)
        if renewed:
            assert current['acquired_at_us']<current['lease_expires_at_us']-lease_us
            assert int(role['xmin'])<int(current['xmin'])
        else:
            assert current['lease_expires_at_us']==current['acquired_at_us']+lease_us
            assert role['xmin']==current['xmin']
        number(current['control_backend_pid'],1,integer=True)
        wanted=dict(previous,epoch=previous['epoch']+1,role_instance_id=rid,process_incarnation=inc,
            acquired_at_us=current['acquired_at_us'],lease_expires_at_us=current['lease_expires_at_us'],
            state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
            control_backend_pid=current['control_backend_pid'],control_backend_start=current['control_backend_start'],xmin=current['xmin'])
        assert current==wanted
        assert role==dict(role_instance_id=rid,dataset_id=m.ds['dataset_id'],role='control',created_at_us=role['created_at_us'],retired_at_us=None,xmin=role['xmin'])
        m.pg['owner_epochs'].append(wanted); m.pg['role_instances'].append(role)



def verify_preparation_context(context,initial,expected,*,delivery=False,baseline=None):
    case=context['case']; name,step=preparation_specification(case,delivery=delivery)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert context['native_before']==expected[1] and context['inputs_before']==expected[2]
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    if step in ('abandon','retire'): return verify(context,initial,expected)
    if step in ('prepare','prepare-gap'):
        assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
            'catalog_before','catalog_after','backend_absence','backends','custody','arguments','gate'}
        assert context['backends']==[] and context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
        assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
        for v in absence['attempts']:
            number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
        assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
        args=[case['operation'],case['database'],'/run/secrets/application',case['config']]
        if step=='prepare-gap': args.append('/lab/'+case['name']+'-gate')
        else: assert context['gate'] is None
        assert context['arguments']==args; number(context['response']['seconds'],0,120)
        events=journey_closed(context['response'],case['database'])
        after,result=verify_journey_prepare(events,expected,initial,case,window,context['gate'],compacted_initial=not name.startswith('original-'))
        same(context['after'],after[0]); assert (context['native_after'],context['inputs_after'])==after[1:]
        return result
    assert set(context)==CONTEXT_FIELDS and context['snapshots']==[]
    wire=delivery and step in ('baseline','loss')
    assert context['fixture']['cleanup_errors']==[]
    assert set(context['fixture'])==({'cleanup_errors','delivery_fault'} if wire else {'cleanup_errors'})
    assert context['inputs_after']==expected[2] and set(context['input_contents'])==set(expected[2])
    for filename,value in context['input_contents'].items():
        data=bytes.fromhex(value); assert expected[2][filename]==dict(bytes=len(data),sha256=sha(data))
    assert len(expected[2])<=64 and sum(v['bytes'] for v in expected[2].values())<=32*1024**2
    report=preparation_closed(context['response'],case,delivery=delivery); failure=preparation_failure(name,step,delivery=delivery)
    if failure in PREPARATION_PRECLAIM:
        preparation_no_claim(context['backend'],case['database']); same(context['after'],expected[0])
    else:
        backend_checked(context['backend'],case['database'],expected[0],context['after'])
        m=model.Model(*expected,window,initial=not expected[0]['initial_ingest_successions'])
        preparation_control(m,context['after'],success=failure is None,renewed=delivery and step=='loss'); same(m.pg,context['after'])
    native=expected[1]; compacted=not name.startswith('original-')
    manifest=bytes.fromhex(model.native_map(native)['default','0101']); epoch=int.from_bytes(manifest[54:62],'big')
    assert epoch in (1,2)
    if failure is None:
        intent,handoff=journey_plan_from_intent(expected[0],initial)
        assert not expected[0]['initial_ingest_activations']
        if epoch==1: native=preparation_native(native,expected[0],initial)
        count='7'; digest=known(initial)['digest']
        assert report==dict(format='bigbrotr-ingest-recovery/1',state='initial-preparation-completed',dataset_id=intent['dataset_id'][2:],
            owner_resource_id=intent['owner_resource_id'][2:],producer_slot_id=intent['producer_slot_id'][2:],producer_epoch='2',
            from_owner_epoch=str(epoch),issued_through='0',reserved_through='0',resolved_original_requests='0' if compacted else count,
            compacted_requests=count if compacted else '0',recovery_basis='closed-producer-floor-v3' if compacted else 'original-inputs',
            original_source_closure=dict(producer_epoch='1',issued_through=count,last_complete_sequence='27',sha256=digest),
            prepared_target=dict(owner_epoch='2',incarnation=intent['target_incarnation'][2:],handoff_sha256=sha(handoff)),
            native='closed',owner_release='committed',ready=False)
    if failure=='delivery-after' and epoch==1: native=preparation_native(native,expected[0],initial)
    assert context['native_after']==native
    outcome={'delivery-before':'COMPLETION_NOT_DISPATCHED','delivery-after':'DELIVERY_UNKNOWN'}.get(failure,
        'REFUSED' if failure else 'COMPLETED_EXISTING_PREPARATION')
    result=dict(status='PASS',case=case['name'],result=outcome,
        from_owner_epoch=epoch,product_native_writes=native['sequence']-expected[1]['sequence'],
        native_rows=len(native['rows']),native_sequence=native['sequence'],input_files=len(expected[2]),
        compacted_requests=7 if not failure and compacted else 0,resolved_original_requests=7 if not failure and not compacted else 0,tables=77)
    if wire: result['wire']=preparation_delivery_checked(context,expected,baseline)
    return result


def preparation_controls(context,initial,expected,*,delivery=False,baseline=None):
    import json
    _,step=preparation_specification(context['case'],delivery=delivery)
    changes=[('native-write-count',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('historical-head-xmin',lambda c:only(c['after']['producer_checkpoints'],producer_epoch=1).update(xmin='4294967000')),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if context['after']['initial_ingest_successions']:
        changes.append(('preparing-control-attribution',lambda c:only(c['after']['initial_ingest_successions']).update(prepared_by_epoch=999)))
    if context['inputs_after']:
        changes.append(('required-input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))))
    else: changes.append(('original-reappeared',lambda c:c['inputs_after'].update(initial[2])))
    if step in ('prepare','prepare-gap'):
        changes.extend([('unclosed-native',lambda c:c['custody'].update(status='UNKNOWN')),
            ('wrong-arguments',lambda c:c['arguments'].append('wrong')),('live-backend',lambda c:c['backends'].append(dict(pid=1)))])
        if step=='prepare-gap': changes.append(('gap-native-dispatched',lambda c:c['gate']['native'].update(sequence=c['gate']['native']['sequence']+4)))
    else:
        changes.extend([('unjoined-process',lambda c:c['response']['terminal'].update(reaped=False)),
            ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
            ('unknown-outcome',lambda c:c['response'].update(outcome='UNKNOWN')),
            ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)]))])
        if step in ('complete','retry','baseline'):
            def wrong_count(c):
                v=loads(c['response']['stdout']); v['resolved_original_requests'],v['compacted_requests']=v['compacted_requests'],v['resolved_original_requests']
                c['response']['stdout']=json.dumps(v)+'\n'
            changes.append(('swapped-original-compacted-counts',wrong_count))
    if delivery and step in ('baseline','loss'):
        changes.extend([('unjoined-proxy',lambda c:c['fixture']['delivery_fault']['wire'].update(readers_joined=False)),
            ('wrong-wire-counter',lambda c:c['fixture']['delivery_fault']['wire']['receipt'].update(transaction_starts=63)),
            ('unrestored-config',lambda c:c['fixture']['delivery_fault']['configuration'].update(restored='wrong')),
            ('wrong-proxy-source',lambda c:c['fixture']['delivery_fault']['wire']['sources'].update({'initial_gap_proxy.py':'00'*32}))])
        if step=='loss': changes.append(('held-SQL-delta',lambda c:only(c['fixture']['delivery_fault']['held']['control']['initial_ingest_successions']).update(prepared_by_epoch=999)))
    rejected=[]
    for name,change in changes:
        c=copy.deepcopy(context); change(c)
        try: verify_preparation_context(c,initial,expected,delivery=delivery,baseline=baseline)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration,IndexError): rejected.append(name)
        else: raise AssertionError('accepted altered installed initial preparation evidence: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def preparation_outcome_statement(initial):
    slot=only(initial[0]['producer_epochs'])['producer_slot_id'][2:]
    return "BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\n" \
        "SET LOCAL session_replication_role=replica;\n" \
        "DELETE FROM control.producer_outcomes WHERE producer_slot_id=decode('"+slot+"','hex') " \
        "AND producer_epoch=1 AND request_sequence=3\n" \
        "RETURNING (to_jsonb(producer_outcomes)||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n"


def preparation_mutation_checked(record,expected,initial,name):
    if name in ('head','target-nonzero','target-input'): return journey_mutation_checked(record,expected,initial,name)
    pg,native,inputs=copy.deepcopy(expected)
    assert record['kind']=='isolated-initial-preparation-'+name
    assert record['before']==dict(control=pg,native=native,inputs=inputs)
    if name=='outcome':
        assert record['statement']==preparation_outcome_statement(initial)
        removed=only(pg['producer_outcomes'],producer_epoch=1,request_sequence=3)
        assert loads(record['sql_output'])==removed; pg['producer_outcomes'].remove(removed)
    else:
        assert name=='target-closing'
        intent,_=journey_plan_from_intent(pg,initial)
        key=(b'\x01\x31'+binary(intent['producer_slot_id'])+be(2)).hex()
        old=b'\x01\x31'+be(0)*3; new=b'\x02\x31'+be(0)*3+b'\x01'
        row=only(native['rows'],cf='receipts',key=key); assert row['value']==old.hex()
        row['value']=new.hex(); native['sequence']+=1
        response=record['response']; assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
        assert [loads(v) for v in response['stdout'].splitlines()]==[
            dict(event='successor-history-corruption',phase=phase,mutation='target-closing',cf='receipts',key=key,
                value=value.hex(),sequence=state['sequence'],rows=state['rows'])
            for phase,value,state in [('before',old,expected[1]),('after',new,native)]]
    result=pg,native,inputs
    assert record['after']==dict(control=pg,native=native,inputs=inputs)
    return result


def preparation_quarantine_checked(q,expected,initial):
    assert set(q)=={'kind','status','path','name','hex','before','after','quarantine_after'}
    assert q['kind']=='reversible-required-initial-original-quarantine' and q['status']=='RETAINED'
    assert q['path']=='original-prepared/quarantined-required'
    name=only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000003.cbor'
    assert q['name']==name and expected[2]==initial[2]
    data=bytes.fromhex(q['hex']); assert initial[2][name]==dict(bytes=len(data),sha256=sha(data))
    before,after,saved=q['before'],q['after'],q['quarantine_after']
    assert set(before)=={'.',*expected[2]} and set(after)==set(before)-{name} and set(saved)=={'.',name}
    assert before[name]==saved[name]
    for filename,identity in expected[2].items():
        value=before[filename]
        assert stat.S_ISREG(value['mode']) and stat.S_IMODE(value['mode'])==0o600 and value['links']==1
        assert {k:value[k] for k in ('bytes','sha256')}==identity
        if filename!=name: assert after[filename]==value
    assert {k:v for k,v in before['.'].items() if k!='bytes'}=={k:v for k,v in after['.'].items() if k!='bytes'}
    assert stat.S_ISDIR(saved['.']['mode']) and stat.S_IMODE(saved['.']['mode'])==0o700
    inputs=copy.deepcopy(expected[2]); del inputs[name]
    return expected[0],expected[1],inputs


DELIVERY_STEPS = {
    'gap-baseline': ('prepare-gap','baseline'), 'prepared-baseline': ('baseline',),
    'gap-before': ('prepare-gap','loss','retry'), 'gap-after': ('prepare-gap','loss','retry'),
    'prepared-before': ('loss','retry'), 'prepared-after': ('loss','retry'),
}
DELIVERY_ORIGINS = (JOURNEY_ORIGIN,PREPARATION_RETAINED_ORIGINS[0])
DELIVERY_ORIGIN_NAMES = {n:'compacted-initial' if n.startswith('gap-') else 'prepared-initial' for n in DELIVERY_STEPS}
DELIVERY_SOURCES = {'initial_gap_proxy.py':'experiments/ingest-bootstrap/initial_gap_proxy.py',
    'rollback_proxy.py':'experiments/relay-source/rollback_proxy.py','fault_proxy.py':'experiments/control-migrations/fault_proxy.py'}


class InitialPreparationDeliveryProfile:
    DIRECTORY = 'installed-initial-preparation-faults'
    LABEL = 'org.bigbrotr.installed-initial-preparation-faults'
    STATUS = 'PASS_INSTALLED_INITIAL_PREPARATION_DELIVERY'
    DATABASE_PREFIX = 'bb_initial_delivery_'
    CLONES = tuple(DELIVERY_STEPS)
    STEPS = DELIVERY_STEPS
    CASES = tuple(n+'-'+s for n in CLONES for s in DELIVERY_STEPS[n])
    GRANTS = JOURNEY_GRANTS
    NOT_RUN = ('Actual product reclamation/GC and unlink',
        'Original-input completion delivery faults; the injected routes use compacted initial ancestors',
        'Initial completion cancellation/lease expiry, process/power loss and native syscall uncertainty',
        'Genuine epoch2/SQL-pending3 gap, activated replacement recovery, later consumers, mixed restore/backup and capacity')


def preparation_delivery_origins(root,raw=None):
    current,initial=journey_origin_checked(root,raw)
    prepared,basis=qualified_endpoint(root,raw,initial_journey=False,prepared_origin=PREPARATION_RETAINED_ORIGINS[0])
    assert initial==basis
    return {'compacted-initial':current,'prepared-initial':prepared},initial


def preparation_delivery_specification(case):
    name=case['clone']; assert name in DELIVERY_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in DELIVERY_STEPS[name]
    assert case['name']==name+'-'+step and case['origin']==DELIVERY_ORIGIN_NAMES[name]
    index=DELIVERY_STEPS[name].index(step)
    assert case['predecessor']==(name+'-'+DELIVERY_STEPS[name][index-1] if index else None)
    assert case['operation']==(JOURNEY_GAP_MODE if step=='prepare-gap' else 'complete-initial')
    assert re.fullmatch('[a-z0-9_]{1,63}',case['database']) and case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def preparation_wire_event(value):
    from fault_cases import safe_event
    extra={'transaction_starts','gap_boundary','gap_proxy_sha256','rollback_proxy_sha256',
        'rollback_requests','rollback_completions','rollback_released','rollback_ready','rollback_number','before_forward'}
    assert extra<=value.keys(); safe_event({k:v for k,v in value.items() if k not in extra})
    assert value['gap_boundary'] in ('observe','before','after') and value['before_forward'] is False
    assert value['mode']=='pass' and value['commit_number']==64
    for k in ('gap_proxy_sha256','rollback_proxy_sha256'): assert re.fullmatch('[0-9a-f]{64}',value[k])
    for k in extra-{'gap_boundary','gap_proxy_sha256','rollback_proxy_sha256','before_forward'}: number(value[k],0,64,integer=True)
    counts=fault_counts(value); assert counts==sorted(counts,reverse=True)
    return value


def preparation_delivery_checked(context,expected,baseline):
    import tomllib
    case=context['case']; name,step=preparation_delivery_specification(case)
    assert step in ('baseline','loss')
    route='gap' if name.startswith('gap-') else 'prepared'
    start,rollback=(5,3) if route=='gap' else (4,2)
    boundary='observe' if step=='baseline' else name.rsplit('-',1)[1]
    fault=context['fixture']['delivery_fault']; window=context['server_window']
    assert set(fault)=={'status','boundary','start_number','rollback_number','wire','held','configuration','errors'}
    assert fault['status']=='JOINED_CONFIG_RESTORED' and fault['errors']==[]
    assert (fault['boundary'],fault['start_number'],fault['rollback_number'])==(boundary,start,rollback)
    wire=fault['wire']
    assert set(wire)=={'command','sources','ready','held','receipt','events','stdout','stderr','exit_code','readers_joined','overflow','forced_wrapper_stop'}
    assert wire['exit_code']==0 and type(wire['exit_code']) is int and wire['readers_joined'] is True
    assert wire['stderr']=='' and wire['overflow'] is False and wire['forced_wrapper_stop'] is False
    command=wire['command']; assert command==['docker','exec','-i',command[3],'python3','-B','/lab/initial_gap_proxy.py',
        '--upstream-port','5432','--listen-port','0','--boundary',boundary,'--start-number',str(start),'--rollback-number',str(rollback)]
    assert re.fullmatch('bigbrotr-initial-delivery-[0-9a-f]{7}',command[3])
    sources={name:sha(retained.regular_read(Path(__file__).resolve().parents[2]/path)) for name,path in DELIVERY_SOURCES.items()}
    assert wire['sources']==sources
    events=[preparation_wire_event(loads(v)) for v in wire['stdout'].splitlines()]
    assert events==wire['events'] and [v['elapsed_ns'] for v in events]==sorted(v['elapsed_ns'] for v in events)
    ready,receipt=wire['ready'],wire['receipt']
    assert ready==events[0] and ready['event']=='ready' and receipt==events[-1] and receipt['event']=='receipt'
    for e in events:
        assert e['gap_boundary']==boundary and e['rollback_number']==(rollback if boundary=='after' else 0)
        assert (e['gap_proxy_sha256'],e['rollback_proxy_sha256'])==(sources['initial_gap_proxy.py'],sources['rollback_proxy.py'])
    assert ready['proxy_sha256']==sources['fault_proxy.py']
    assert ready['listen_host']=='127.0.0.1' and ready['upstream_port']==5432
    number(ready['listen_port'],1,65535,integer=True); number(ready['pid'],1,integer=True)
    assert (ready['max_frame_bytes'],ready['max_wire_bytes'],ready['max_messages'],ready['deadline_seconds'])==(1024**2,64*1024**2,20000,30)
    commits=('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')
    assert ready['transaction_starts']==0 and fault_counts(ready)==[0]*4 and all(ready[k]==0 for k in commits)
    assert ready['faults']==0 and ready['held'] is False and ready['hold_action'] is None
    config=fault['configuration']; assert set(config)=={'path','direct','proxied','restored'} and config['path']==case['config']
    assert config['restored']==config['direct']
    direct,proxied=tomllib.loads(config['direct']),tomllib.loads(config['proxied'])
    assert direct['postgres']['control']['endpoint']=='127.0.0.1:5432'
    wanted=copy.deepcopy(direct); wanted['postgres']['control']['endpoint']='127.0.0.1:'+str(ready['listen_port']); assert proxied==wanted
    section=re.search(r'(?ms)(^\[postgres\.control\]\n)(.*?)(?=^\[|\Z)',config['direct']); assert section is not None
    body,count=re.subn(r'(?m)^endpoint = .*$', 'endpoint = "127.0.0.1:'+str(ready['listen_port'])+'"',section[2]); assert count==1
    assert config['proxied']==config['direct'][:section.start()]+section[1]+body+config['direct'][section.end():]
    assert receipt['frames'] and receipt['declared_wire_bytes']<=64*1024**2
    if step=='baseline':
        assert baseline is None and fault['held'] is None and wire['held'] is None and events==[ready,receipt]
        assert receipt['faults']==0 and receipt['held'] is False and receipt['hold_action'] is None
        assert receipt['reason'] in ('frontend-eof','backend-eof')
        assert receipt['transaction_starts']==start+1 and fault_counts(receipt)==[rollback]*4
        assert all(receipt[k]==3 for k in commits)
    else:
        assert baseline is not None and baseline['status']=='PASS' and baseline['case']==route+'-baseline-baseline'
        assert baseline['wire']==dict(boundary='observe',transaction_starts=start+1,rollback_counts=[rollback]*4,commit_counts=[3]*4)
        held=wire['held']; assert events==[ready,held,receipt] and held['event']=='held' and held['held'] is True and held['faults']==1
        assert held['hold_action'] is None and held['transaction_starts']==start
        assert all(held[k]==2 for k in commits)
        assert fault_counts(held)==([rollback-1]*4 if boundary=='before' else [rollback,rollback,rollback-1,rollback-1])
        assert receipt['reason']=='held-close' and receipt['hold_action']=='close' and receipt['held'] is True and receipt['faults']==1
        for k in (*commits,'transaction_starts','rollback_requests','rollback_completions','rollback_released','rollback_ready'):
            assert receipt[k]==held[k]
        capture=fault['held']; assert set(capture)=={'control','inputs','backend','before_us','after_us','close_before_us','close_after_us'}
        # Only SQL and spool bytes are observed while the installed native owner
        # is alive. The excluded external dump is independently checked later.
        same(capture['control'],context['after']); assert capture['inputs']==expected[2]
        for k in ('before_us','after_us','close_before_us','close_after_us'): model.clock(capture[k],window)
        assert capture['before_us']<=capture['after_us']<=capture['close_before_us']<=capture['close_after_us']
        control=max((r for r in capture['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        backend=capture['backend']
        assert backend==dict(pid=control['control_backend_pid'],backend_start=control['control_backend_start'],database=case['database'],
            user='bigbrotr_control',state='idle',xact_start=None,query='COMMIT' if boundary=='before' else 'ROLLBACK',observed_at_us=backend['observed_at_us'])
        assert capture['before_us']<=backend['observed_at_us']<=capture['after_us']
        assert control['lease_expires_at_us']>capture['close_after_us']
    return dict(boundary=boundary,transaction_starts=receipt['transaction_starts'],rollback_counts=fault_counts(receipt),commit_counts=[receipt[k] for k in commits])


# The two activated endpoints reuse the exact f51 raw receipt and independently
# qualified science; the nonzero endpoint has no typed epoch2 checkpoint.
ACTIVATED_LIVE_ORIGIN = dict(PREPARATION_RETAINED_ORIGINS[1],name='activated-live',source_clone='live',final_case='live-strict',
    database='bb_compacted_initial_f51a8a3_live',store='live/store',snapshot='live-final-sql.json',
    snapshot_sha256='74927499ddbf7601c075cc2cc0a951fcbc949410430224d296b87438c044827c',
    native_file='live-strict-after-native.json',native_sha256='19c2d09ea3c1869e034ba05bfa3611997a6de7fff59cc3e1405e3f2356cf40d9',
    inputs_file='live-strict-after-inputs.json',inputs_sha256='762e7b8b439b129a2a220db27b675d31b2c5c4a5983b713c04486d746a7ef7a2',
    verification_sha256='4a33601b70f07dc8f6f0c5cc5ead2735ad4ca18c0e1afc4bed6d4943833c3700')
ACTIVATED_RETAINED_ORIGINS = (PREPARATION_RETAINED_ORIGINS[1],ACTIVATED_LIVE_ORIGIN)
ACTIVATED_ORIGINS = (ORIGIN,JOURNEY_ORIGIN,*ACTIVATED_RETAINED_ORIGINS)
ACTIVATED_GRANTS = JOURNEY_GRANTS + ('GRANT EXECUTE ON FUNCTION control.require_activated_initial_recovery(bytea) TO bigbrotr_control;',)
ACTIVATED_STEPS = {'zero':('recover','retry','source','retire'),'live':('recover','retry'),'missing-input':('refuse',),
    'progress-wire':('seed','recover','retry'),'outcome-after':('seed','recover','retry'),
    'outcome-before':('seed','recover','retry'),'reserved-tail':('seed','refuse','retry'),
    'original':('abandon','retire','seed','recover','retry')}
ACTIVATED_ORIGIN_NAMES = {name:'initial' if name=='original' else 'activated-initial' if name=='zero' else
    'activated-live' if name in ('live','missing-input') else 'compacted-initial' for name in ACTIVATED_STEPS}
ACTIVATED_SEED_MODES = {name:'successor-live-'+('original' if name=='original' else 'compacted')+'-progress-'+
    ('wire' if name in ('original','progress-wire') else 'reservation-after' if name=='reserved-tail' else name)
    for name in ('progress-wire','outcome-after','outcome-before','reserved-tail','original')}
ACTIVATED_PROXY = 'experiments/control-migrations/fault_proxy.py'


class ActivatedProfile:
    DIRECTORY='installed-compacted-activated'
    LABEL='org.bigbrotr.installed-compacted-activated'
    STATUS='PASS_INSTALLED_COMPACTED_ACTIVATED_RECOVERY_ACK'
    DATABASE_PREFIX='bb_compacted_activated_'
    CLONES=tuple(ACTIVATED_STEPS)
    STEPS=ACTIVATED_STEPS
    CASES=tuple(name+'-'+step for name in ACTIVATED_STEPS for step in ACTIVATED_STEPS[name])
    GRANTS=ACTIVATED_GRANTS
    NOT_RUN=('Actual product reclamation/GC and unlink','Compacted source closure and retirement success',
        'Later activated successors, live frontier and retained Archive frontier',
        'Original-input completion delivery loss, cancellation and lease expiry',
        'Mixed restore/backup, physical loss and capacity')


def activated_origins_checked(root,raw=None):
    compacted,initial=journey_origin_checked(root,raw)
    result={'initial':initial,'compacted-initial':compacted}
    for pin in ACTIVATED_RETAINED_ORIGINS:
        value,basis=qualified_endpoint(root,raw,initial_journey=False,
            **({'activated_origin':pin} if pin==ACTIVATED_LIVE_ORIGIN else {'prepared_origin':pin}))
        assert basis==initial; result[pin['name']]=value
    return result,initial


def activated_specification(case):
    name=case['clone']; assert name in ACTIVATED_STEPS
    prefix=name+'-'; assert case['name'].startswith(prefix)
    step=case['name'][len(prefix):]; steps=ACTIVATED_STEPS[name]; index=steps.index(step)
    assert case['origin']==ACTIVATED_ORIGIN_NAMES[name]
    assert case['predecessor']==(prefix+steps[index-1] if index else None)
    operation=ACTIVATED_SEED_MODES[name] if step=='seed' else 'abandon-initial' if step=='abandon' else 'retire-initial' if name=='original' and step=='retire' else {'source':'source-activated','retire':'retire-activated'}.get(step,'recover-activated')
    assert case['operation']==operation
    assert re.fullmatch('[a-z0-9_]{1,63}',case['database']) and case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def activated_native(native,slot,*,close=False,ack=False):
    wanted=copy.deepcopy(native); values=model.native_map(wanted)
    key=('receipts',(b'\x01\x31'+binary(slot)+be(2)).hex()); value=bytes.fromhex(values[key])
    assert value[:2] in (b'\x01\x31',b'\x02\x31')
    assert len(value)==(26 if value[:2]==b'\x01\x31' else 27)
    if close and value[:2]==b'\x01\x31':
        value=b'\x02\x31'+value[2:]+b'\x01'; wanted['sequence']+=1
    if ack:
        issued=int.from_bytes(value[2:10],'big'); acknowledged=int.from_bytes(value[10:18],'big')
        assert issued==1 and acknowledged in (0,1)
        if acknowledged==0: value=value[:10]+be(1)+value[18:]; wanted['sequence']+=1
    values[key]=value.hex(); wanted['rows']=model.native_rows(values)
    return wanted


def activated_seed_closed(response,database):
    assert set(response)=={'returncode','stdout','stderr','seconds','outcome','readers_joined','overflow','command'}
    assert response['returncode']==0 and type(response['returncode']) is int and response['stderr']==''
    assert response['outcome']=='PROCESS_EXITED' and response['readers_joined'] is True and response['overflow'] is False
    events=[loads(line) for line in response['stdout'].splitlines()]
    done=only(events,event='progress-seed-complete')
    assert done['native']=='closed' and done['observer']=='joined'
    driver=only(events,event='progress-seed-driver'); assert driver['joined'] is True and type(driver['failed']) is bool
    if done['mode'].endswith('-wire'): assert driver['failed'] is False
    present,absent=[e for e in events if e['event']=='successor-backend']
    assert present['database']==database and present['present'] is True and absent==dict(present,present=False)
    assert only(events,event='progress-seed-native')==dict(event='progress-seed-native',state='closed')
    return events


def activated_reservation(before,observed,science,control,window):
    wanted=copy.deepcopy(before); request=only(observed['producer_requests'],producer_epoch=2,request_sequence=1)
    model.fresh_xmin(request['xmin'],before); model.clock(request['reserved_at_us'],window)
    activation=only(before['initial_ingest_activations'])
    assert activation['activated_at_us']<=request['reserved_at_us']<min(control['lease_expires_at_us'],science.plan['lease_expires_at_us'])
    common=dict(dataset_id='\\x'+science.ds.hex(),producer_slot_id='\\x'+science.slot.hex(),producer_epoch=2,request_sequence=1)
    row=dict(common,request_digest='\\x'+sha(science.requests[1]),input_bytes=len(science.requests[1]),input_profile=1,
        manifest_sha256='\\x'+sha(bytes.fromhex(science.plan['target_manifest'])),issuer_resource=control['resource_id'],
        issuer_epoch=control['epoch'],issuer_role='control',issuer_incarnation=control['process_incarnation'],
        reserved_at_us=request['reserved_at_us'],relay_endpoint_id=None,relay_access_scope_id=None,relay_enrollment_generation=None,
        relay_route_id=None,acquisition_scope_id=None,acquisition_job_id=None,progress_event_ordinal=None,progress_input_sha256=None,
        progress_request_id=None,progress_wire_sequence=None,xmin=request['xmin'])
    assert row==request
    producer=only(wanted['producer_epochs'],producer_epoch=2); assert producer['reserved_through_request']==0
    producer.update(reserved_through_request=1,xmin=row['xmin']); wanted['producer_requests'].append(row)
    return wanted


def activated_outcome(before,observed,receipt,control,window):
    wanted=copy.deepcopy(before); request=only(before['producer_requests'],producer_epoch=2,request_sequence=1)
    actual=only(observed['producer_outcomes'],producer_epoch=2,request_sequence=1)
    model.fresh_xmin(actual['xmin'],before); model.clock(actual['recorded_at_us'],window)
    assert max(request['reserved_at_us'],control['acquired_at_us'])<=actual['recorded_at_us']<control['lease_expires_at_us']
    if control['released_at_us'] is not None: assert actual['recorded_at_us']<=control['released_at_us']
    common={k:request[k] for k in ('dataset_id','producer_slot_id','producer_epoch','request_sequence','request_digest','manifest_sha256')}
    row=dict(common,outcome_kind=1,policy_epoch=1,outcome_bytes='\\x'+receipt.hex(),outcome_sha256='\\x'+sha(receipt),
        recorder_resource=control['resource_id'],recorder_epoch=control['epoch'],recorder_role='control',
        recorder_incarnation=control['process_incarnation'],recorded_at_us=actual['recorded_at_us'],xmin=actual['xmin'])
    assert actual==row; wanted['producer_outcomes'].append(row); return wanted


def activated_checkpoint(before,observed,science,control,window):
    import installed_progress_replay as progress
    wanted=copy.deepcopy(before); outcome=only(wanted['producer_outcomes'],producer_epoch=2,request_sequence=1)
    common={k:outcome[k] for k in ('dataset_id','producer_slot_id','producer_epoch')}
    actual=only(observed['producer_checkpoints'],producer_epoch=2)
    model.clock(actual['committed_at_us'],window)
    assert outcome['recorded_at_us']<=actual['committed_at_us']<control['lease_expires_at_us']
    head=dict(common,progress_format=1,manifest_sha256=outcome['manifest_sha256'],checkpoint_revision=1,complete_through_request=1,
        progress_jobs=0,dependency_count=2,issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],
        issuer_incarnation=control['process_incarnation'],committed_at_us=actual['committed_at_us'],xmin=outcome['xmin'],checkpoint_digest='\\x'+'00'*32)
    deps=[dict(common,source_shard=0,source_epoch=epoch,source_sequence=sequence,payload_digest='\\x'+science.refs[epoch,sequence][1].hex(),
        xmin=outcome['xmin']) for epoch,sequence in ((1,1),(2,1))]
    def check(row):
        key=('facts',b'\x01\x20'+science.ds+be(0,4)+be(row['source_epoch'])+be(row['source_sequence']))
        assert science.rows[key][2:34]==binary(row['payload_digest'])
    head['checkpoint_digest']='\\x'+progress.head_digest(head,[],deps,check).hex()
    wanted['producer_checkpoints'].append(head); wanted['producer_checkpoint_dependencies'].extend(deps)
    same(wanted,observed); return wanted


def verify_activated_seed(events,expected,initial,case,window,wire,baseline):
    import initial_live_oracle as live
    import initial_succession_oracle as original
    name,step=activated_specification(case); assert step=='seed'; compacted=name!='original'; mode=ACTIVATED_SEED_MODES[name]
    pg,native,inputs=copy.deepcopy(expected)
    plan,rows=history_seed_plan(pg,native,inputs,only(events,event='successor-plan'),initial,compacted_initial=compacted)
    model.clock(plan['created_at_us'],window); position=0
    def take(event,stage=None,phase=None):
        nonlocal position
        v=events[position]; position+=1; assert (v['event'],v.get('stage'),v.get('phase'))==(event,stage,phase); return v
    def snap(stage): return history_snapshot(take('successor-state',stage),native,inputs,pg,stage=stage)
    assert take('successor-control','initial','claim-planned')==dict(event='successor-control',stage='initial',phase='claim-planned',role='d3'*16,incarnation='dd'*16,lease_ttl_us='20000000')
    same(pg,snap('initial-before-claim')); claimed=take('successor-control','initial','claimed'); after=snap('initial-after-claim')
    control=original.claim(pg,after,1); model.clock(control['acquired_at_us'],window)
    assert claimed==dict(event='successor-control',stage='initial',phase='claimed',resource=control['resource_id'][2:],epoch=control['epoch'],role='d3'*16,incarnation='dd'*16)
    pg=after
    if compacted: assert take('successor-compacted-proof')==journey_proof(initial)
    assert take('successor-plan')==plan
    after=snap('before-prepare'); control=original.renewed(pg,after,control,plan['created_at_us']); model.fresh_xmin(control['xmin'],pg); pg=after
    native=journey_dump(rows,125 if compacted else 117)
    after=snap('prepared'); original.prepared(pg,after,plan,control,compacted_history=True); pg=after
    backend=take('successor-backend','initial')
    assert backend==dict(event='successor-backend',stage='initial',database=case['database'],pid=control['control_backend_pid'],backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
    assert take('live-activation-start')==dict(event='live-activation-start',mode=mode,control_epoch=control['epoch'])
    activated=take('live-activation-result'); after=snap('activated-zero')
    assert activated==dict(event='live-activation-result',writer=True,error=None,activated_at_us=activated['activated_at_us'])
    live.activated(pg,after,plan,control,window,activated['activated_at_us']); pg=after
    science=live.Science(plan,rows,inputs,compacted_history=not compacted,compacted_initial=compacted)
    receipt=science.advance('dictionary')
    assert take('progress-seed-request')==dict(event='progress-seed-request',mode=mode,request=1,preimage=science.requests[1].hex(),
        request_digest=sha(science.requests[1]),progress_data_bytes=2097152,progress_native_bytes=4096)
    error=None if name in ('progress-wire','original') else 'Reservation(CommitUnknown)' if name=='reserved-tail' else 'Outcome(CommitUnknown)'
    assert take('progress-seed-result')==dict(event='progress-seed-result',mode=mode,error=error,receipt=receipt.hex() if error is None else None)
    inputs=science.inputs
    if name!='reserved-tail': native=journey_dump(science.rows,133 if compacted else 125)
    if error is None: native=activated_native(native,'\\x'+science.slot.hex(),ack=True)
    after=snap('progress-returned'); reserved=activated_reservation(pg,after,science,control,window)
    has_copy=name in ('progress-wire','original','outcome-after')
    wanted=activated_outcome(reserved,after,receipt,control,window) if has_copy else reserved
    if has_copy: wanted=activated_checkpoint(wanted,after,science,control,window)
    same(wanted,after); pg=wanted
    if error is not None:
        assert take('successor-session-poisoned')==dict(event='successor-session-poisoned',control_epoch=control['epoch'])
    assert take('progress-seed-native')==dict(event='progress-seed-native',state='closed')
    same(pg,snap('progress-before-control-end'))
    driver=take('progress-seed-driver'); assert driver['joined'] is True and type(driver['failed']) is bool
    if error is None: assert driver['failed'] is False
    phase='released-and-joined' if error is None else 'dropped-and-joined'
    assert take('successor-control','progress',phase)==dict(event='successor-control',stage='progress',phase=phase,resource=control['resource_id'][2:],epoch=control['epoch'])
    after=snap('progress-after-control-end'); original.ended(pg,after,control,error is None)
    if error is None:
        ended=only(after['owner_epochs'],role='control',epoch=control['epoch']); model.fresh_xmin(ended['xmin'],pg); model.clock(ended['released_at_us'],window)
    pg=after; assert take('successor-backend','initial')==dict(backend,present=False); same(pg,snap('final'))
    assert take('progress-seed-complete')==dict(event='progress-seed-complete',mode=mode,native='closed',control=phase,observer='joined',
        compacted_predecessor_requests=7 if compacted else 0,original_predecessor_requests=0 if compacted else 7,requested_epoch2=1,events=0)
    assert position==len(events)
    wire_result=activated_wire_checked(wire,case,window,baseline,reserved,inputs,backend,control)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],native_rows=len(native['rows']),native_sequence=native['sequence'],
        original_target_files=1,compacted_requests=7 if compacted else 0,original_predecessor_requests=0 if compacted else 7,
        typed_target_checkpoint=has_copy,native_target_acknowledged=1 if error is None else 0,wire=wire_result)


def activated_wire_checked(wire,case,window,baseline,reserved,inputs,backend,control):
    from fault_cases import safe_event
    name,step=activated_specification(case); assert step=='seed'
    normal=name in ('progress-wire','original'); original=name=='original'
    target=21 if original else 6 if name=='reserved-tail' else 7
    mode='hold' if normal else 'before-forward' if name=='outcome-before' else 'after-complete'
    assert set(wire)=={'command','source_sha256','ready','held','receipt','events','stdout','stderr','exit_code','readers_joined','overflow','forced_wrapper_stop','errors'}
    assert wire['errors']==[] and wire['exit_code']==0 and type(wire['exit_code']) is int
    assert wire['readers_joined'] is True and wire['overflow'] is False and wire['forced_wrapper_stop'] is False and wire['stderr']==''
    # Original baseline targets outcome21; reservation20 is separately inferred
    # from the exact reservation snapshot, not asserted as wire-measured alone.
    command=wire['command']
    assert command==['docker','exec','-i',command[3],'python3','-B','/lab/fault_proxy.py','--upstream-port','5432','--listen-port','0',
        '--mode',mode,'--commit-number',str(target),'--later-commits','1','--deadline-seconds','30']
    assert re.fullmatch('bigbrotr-compacted-activated-[0-9a-f]{7}',command[3])
    source=sha(retained.regular_read(Path(__file__).resolve().parents[2]/ACTIVATED_PROXY)); assert wire['source_sha256']==source
    events=[safe_event(loads(v)) for v in wire['stdout'].splitlines()]
    assert events==wire['events']; assert [e['elapsed_ns'] for e in events]==sorted(e['elapsed_ns'] for e in events)
    ready,receipt=wire['ready'],wire['receipt']; assert events[0]==ready and events[-1]==receipt
    assert ready['proxy_sha256']==source and ready['listen_host']=='127.0.0.1' and ready['upstream_port']==5432
    number(ready['listen_port'],1,65535,integer=True); number(ready['pid'],1,integer=True)
    assert (ready['max_frame_bytes'],ready['max_wire_bytes'],ready['max_messages'],ready['deadline_seconds'])==(1024**2,64*1024**2,20000,30)
    counts=lambda e:[e[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]
    assert counts(ready)==[0]*4 and ready['faults']==0 and ready['held'] is False and ready['hold_action'] is None
    for event in events: assert (event['mode'],event['commit_number'])==(mode,target)
    assert receipt['event']=='receipt' and receipt['faults']==1 and receipt['frames'] and receipt['declared_wire_bytes']<=64*1024**2
    if normal:
        assert baseline is None
        assert [e['event'] for e in events]==['ready','held','released','receipt']
        held=wire['held']; assert set(held)=={'event','before_us','after_us','release_before_us','release_after_us','control','inputs','backend'}
        assert held['event']==events[1] and counts(events[1])==[target,target-1,target-1,target-1]
        assert events[1]['held'] is True and events[1]['hold_action'] is None
        same(held['control'],reserved); assert held['inputs']==inputs
        for key in ('before_us','after_us','release_before_us','release_after_us'): model.clock(held[key],window)
        assert held['before_us']<=held['after_us']<=held['release_before_us']<=held['release_after_us']<control['lease_expires_at_us']
        b=held['backend']; assert b==dict(pid=backend['pid'],backend_start=backend['backend_start'],database=case['database'],user='bigbrotr_control',state='idle in transaction',xact_start=b['xact_start'],query=b['query'],observed_at_us=b['observed_at_us'])
        assert ' '.join(b['query'].split())==ACTIVATED_HEAD_INSERT
        assert b['xact_start'] is not None and held['before_us']<=b['observed_at_us']<=held['after_us']
        assert counts(events[2])==counts(events[1]) and events[2]['held'] is False and events[2]['hold_action']=='release'
        assert counts(receipt)==[target+1]*4 and receipt['reason'] in ('frontend-eof','backend-eof')
        assert receipt['held'] is False and receipt['hold_action']=='release'
    else:
        assert wire['held'] is None and [e['event'] for e in events]==['ready','fault','receipt']
        assert baseline is not None and baseline['status']=='PASS' and baseline['case']=='progress-wire-seed'
        assert baseline['wire']==dict(outcome_commit=7,reservation_commit=6,commit_counts=[8]*4,boundary='hold-before-forward-released')
        count=[target,target-1,target-1,target-1] if mode=='before-forward' else [target,target,target,target-1]
        assert counts(events[1])==counts(receipt)==count and receipt['reason']==events[1]['reason']==mode
        assert receipt['held'] is False and receipt['hold_action'] is None
    return dict(outcome_commit=21 if original else 7,reservation_commit=20 if original else 6,
        commit_counts=counts(receipt),boundary='hold-before-forward-released' if normal else mode)


ACTIVATED_HEAD_INSERT = ('INSERT INTO control.producer_checkpoints (dataset_id,producer_slot_id,producer_epoch,checkpoint_revision,complete_through_request, '
    'checkpoint_digest,committed_at_us,progress_format,manifest_sha256,progress_jobs, dependency_count,issuer_resource,issuer_epoch,issuer_incarnation) '
    'VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::text::numeric,$5::text::numeric,$6::bytea, '
    '(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,1,$7::bytea,$8::integer, $9::integer,$10::bytea,$11::text::numeric,$12::bytea)')


ACTIVATED_ERRORS = {'missing-input':'error: producer operation unavailable: Native(RequestInput(Missing))\n',
    'reserved-tail':'error: producer operation unavailable: Native(Unresolved)\n',
    'source':'error: producer operation unavailable: Native(StateMismatch)\n',
    'retire':'error: producer operation unavailable: Native(StateMismatch)\n'}


def activated_failure(name,step):
    if name in ('missing-input','reserved-tail'): return name
    if name=='zero' and step in ('source','retire'): return step
    return None


def activated_closed(response,case):
    name,step=activated_specification(case)
    if name=='original' and step in ('abandon','retire'): return process_closed(response,case)
    failure=activated_failure(name,step); code=4 if failure else 0
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==code
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code']==code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failure:
        assert response['stdout']=='' and response['stderr']==ACTIVATED_ERRORS[failure]; return None
    assert response['stderr']==''; values=response['stdout'].splitlines(); assert len(values)==1
    return loads(values[0])


def activated_quarantine_checked(q,expected):
    assert set(q)=={'kind','path','name','hex','before','after','quarantine_after','status'}
    assert q['kind']=='reversible-required-target-quarantine' and q['status']=='RETAINED'
    assert len(expected[2])==1 and q['name']==next(iter(expected[2]))
    assert '-0000000000000002-0000000000000001.cbor' in q['name']
    assert q['path']=='missing-input/quarantined-required'
    data=bytes.fromhex(q['hex']); assert expected[2][q['name']]==dict(bytes=len(data),sha256=sha(data))
    assert q['before'].keys()=={'.',q['name']} and q['after'].keys()=={'.'} and q['quarantine_after'].keys()=={'.',q['name']}
    assert q['before'][q['name']]==q['quarantine_after'][q['name']]
    # Existing census metadata is independently bound by raw replay; content and
    # move identity, rather than a synthetic replacement, establish this fault.
    for v in (q['before'][q['name']],q['quarantine_after'][q['name']]):
        assert v['sha256']==sha(data) and v['bytes']==len(data) and stat.S_ISREG(v['mode']) and v['links']==1
    return expected[0],expected[1],{}


def verify_activated_context(context,initial,expected,*,baseline=None):
    case=context['case']; name,step=activated_specification(case)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before'])==expected[1:]
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    if name=='original' and step in ('abandon','retire'): return verify(context,initial,expected)
    if step=='seed':
        assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
            'catalog_before','catalog_after','backend_absence','backends','custody','arguments','wire'}
        assert context['backends']==[] and context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
        assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
        for v in absence['attempts']: number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
        assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
        assert context['arguments']==[case['operation'],case['database'],'/run/secrets/application',case['config'],str(context['wire']['ready']['listen_port'])]
        number(context['response']['seconds'],0,120)
        assert context['response']['command']==['docker','exec',context['wire']['command'][3],'/build/ingest_bootstrap_probe',*context['arguments']]
        events=activated_seed_closed(context['response'],case['database'])
        after,result=verify_activated_seed(events,expected,initial,case,window,context['wire'],baseline)
        same(context['after'],after[0]); assert (context['native_after'],context['inputs_after'])==after[1:]; return result
    assert set(context)==CONTEXT_FIELDS and context['snapshots']==[] and context['fixture']==dict(cleanup_errors=[])
    assert context['inputs_after']==expected[2] and set(context['input_contents'])==set(expected[2])
    for filename,value in context['input_contents'].items():
        data=bytes.fromhex(value); assert expected[2][filename]==dict(bytes=len(data),sha256=sha(data))
    assert len(expected[2])<=64 and sum(v['bytes'] for v in expected[2].values())<=32*1024**2
    report=activated_closed(context['response'],case); failure=activated_failure(name,step)
    backend_checked(context['backend'],case['database'],expected[0],context['after'])
    m=model.Model(*expected,window); preparation_control(m,context['after'],success=failure is None,renewed=failure=='reserved-tail')
    native=expected[1]; values=model.native_map(native); slot=m.slot
    value=bytes.fromhex(values['receipts',(b'\x01\x31'+binary(slot)+be(2)).hex()]); issued=int.from_bytes(value[2:10],'big')
    assert issued in (0,1); producer=only(m.pg['producer_epochs'],producer_epoch=2); reserved=producer['reserved_through_request']
    assert reserved in (0,1) and reserved>=issued
    if failure not in ('missing-input','source','retire'):
        assert producer['state'] in ('open','closing')
        if producer['state']=='open':
            actual=only(context['after']['producer_epochs'],producer_epoch=2)
            model.fresh_xmin(actual['xmin'],m.pg); producer.update(state='closing',xmin=actual['xmin'])
        native=activated_native(native,slot,close=True)
        copies=[r for r in m.pg['producer_outcomes'] if r['producer_epoch']==2]
        if issued and not copies:
            assert name=='outcome-before'
            receipt=bytes.fromhex(values['receipts',(b'\x01\x30'+binary(slot)+be(2)+be(1)).hex()])
            m.pg=activated_outcome(m.pg,context['after'],receipt,m.control,window)
        head=[r for r in m.pg['producer_checkpoints'] if r['producer_epoch']==2]
        if head:
            h,=head; assert h['complete_through_request']==issued==reserved==1
            native=activated_native(native,slot,ack=True)
        if failure is None:
            wanted=dict(format='bigbrotr-ingest-recovery/1',state='activated-positive-outcomes-resolved',dataset_id=m.ds['dataset_id'][2:],
                owner_resource_id=m.resource[2:],producer_slot_id=slot[2:],producer_epoch='2',issued_through=str(issued),reserved_through=str(reserved),
                resolved_requests=str(reserved),source_closure='not-performed',native='closed',owner_release='committed',ready=False)
            if name!='original': wanted.update(recovery_basis='closed-producer-floor-v3',compacted_requests='7')
            assert report==wanted
        else: assert reserved==1 and issued==0 and not head and not copies
    same(context['after'],m.pg); assert context['native_after']==native
    value=bytes.fromhex(model.native_map(native)['receipts',(b'\x01\x31'+binary(slot)+be(2)).hex()])
    return dict(status='PASS',case=case['name'],result='REFUSED' if failure else 'ACTIVATED_OUTCOMES_RESOLVED',
        native_rows=len(native['rows']),native_sequence=native['sequence'],product_native_writes=native['sequence']-expected[1]['sequence'],
        target_issued=issued,target_reserved=reserved,target_acknowledged=int.from_bytes(value[10:18],'big'),
        compacted_requests=7 if failure is None and name!='original' else 0,original_predecessor_requests=7 if failure is None and name=='original' else 0,
        resolved_target_requests=reserved if failure is None else 0,tables=77,input_files=len(expected[2]))


def activated_controls(context,initial,expected,*,baseline=None):
    import json
    name,step=activated_specification(context['case'])
    changes=[('native-write-count',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),('initial-typed-head-xmin',lambda c:only(c['after']['producer_checkpoints'],producer_epoch=1).update(xmin='4294967000')),
        ('initial-intent-attribution',lambda c:only(c['after']['initial_ingest_successions']).update(prepared_by_epoch=999)),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if name=='original' and step in ('abandon','retire'): changes.pop(3)
    if not (name=='original' and step in ('abandon','retire')):
        def wrong_ack(c):
            values=model.native_map(c['native_after']); slot=only(c['after']['initial_ingest_successions'])['producer_slot_id']
            key=('receipts',(b'\x01\x31'+binary(slot)+be(2)).hex()); value=bytes.fromhex(values[key])
            changed=value[:10]+be(0 if int.from_bytes(value[10:18],'big') else 1)+value[18:]
            values[key]=changed.hex(); c['native_after']['rows']=model.native_rows(values)
        changes.append(('target-ACK-byte',wrong_ack))
    if context['inputs_after']: changes.append(('required-input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))))
    if step=='seed':
        changes.extend([('unknown-native-custody',lambda c:c['custody'].update(status='UNKNOWN')),
            ('unjoined-proxy',lambda c:c['wire'].update(readers_joined=False)),('wrong-proxy-source',lambda c:c['wire'].update(source_sha256='00'*32)),
            ('wrong-wire-count',lambda c:c['wire']['receipt'].update(commits_seen=63)),('wrong-argv',lambda c:c['arguments'].__setitem__(-1,'1'))])
        if name in ('progress-wire','original'):
            changes.append(('held-ancestor-head-loss',lambda c:c['wire']['held']['control']['producer_checkpoints'].clear()))
        if name in ('progress-wire','original','outcome-after'):
            changes.append(('target-progress-dependency-loss',lambda c:c['after']['producer_checkpoint_dependencies'].pop()))
    else:
        changes.extend([('unreaped-child',lambda c:c['response']['terminal'].update(reaped=False)),('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
            ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),('unknown-outcome',lambda c:c['response'].update(outcome='UNKNOWN'))])
        if name=='zero' and step=='retire':
            previous=max((r for r in expected[0]['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            assert previous['state']=='active'
            def atomic_rows(c):
                old=only(c['after']['owner_epochs'],role='control',epoch=previous['epoch'])
                current=only(c['after']['owner_epochs'],role='control',epoch=previous['epoch']+1)
                return old,current,only(c['after']['role_instances'],role_instance_id=old['role_instance_id']),only(c['after']['role_instances'],role_instance_id=current['role_instance_id'])
            def stale_transaction(c):
                for row in atomic_rows(c): row['xmin']=previous['xmin']
            def split_transaction(c):
                _,current,_,role=atomic_rows(c)
                fresh=str(max(int(r['xmin']) for rows in c['after'].values() for r in rows)+1)
                current['xmin']=role['xmin']=fresh
            changes.extend([('atomic-claim-stale-xmin',stale_transaction),('atomic-claim-split-xmin',split_transaction),
                ('atomic-fence-digest',lambda c:atomic_rows(c)[0].update(exclusion_digest='\\x'+'00'*32)),
                ('unrefreshed-claim-deadline',lambda c:atomic_rows(c)[1].update(lease_expires_at_us=atomic_rows(c)[1]['lease_expires_at_us']+1))])
        if activated_failure(name,step) is None and not (name=='original' and step in ('abandon','retire')):
            def count(c):
                v=loads(c['response']['stdout']); v['resolved_requests']='7'; c['response']['stdout']=json.dumps(v)+'\n'
            changes.append(('ancestor-count-as-target-count',count))
            if name!='original':
                def compact(c):
                    v=loads(c['response']['stdout']); v['compacted_requests']='0'; c['response']['stdout']=json.dumps(v)+'\n'
                changes.append(('missing-compacted-count',compact))
    rejected=[]
    for label,change in changes:
        c=copy.deepcopy(context); change(c)
        try: verify_activated_context(c,initial,expected,baseline=baseline)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration,IndexError): rejected.append(label)
        else: raise AssertionError('accepted altered activated fixture evidence: '+label)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def activated_qualification(component):
    prefix='storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_writer_tests::activated_recovery_tests::compacted_activated_tests::'
    names=('compacted_activated_recovery_separates_ancestor_and_target_and_ack_preserves_all_bytes',
        'compacted_activated_pending_requires_exact_ancestor_owner_and_never_partially_advances',
        'compacted_activated_census_refuses_incompatible_floor_missing_science_and_required_originals',
        'compacted_activated_stamps_refuse_stale_sequence_sidecars_cancellation_and_small_grants',
        'compacted_activated_unreserved_tail_is_preserved_but_reserved_tail_cannot_ack',
        'compacted_activated_ack_target_rechecks_optional_tail_after_token_creation',
        'compacted_activated_context_binds_actual_root_and_cannot_mint_prepared_handoff',
        'compacted_activated_optional_range_keeps_the_physical_file_ceiling',
        'compacted_activated_logical_requests_can_exceed_physical_spool_limit')
    assert {prefix+n for n in names}<=set(component['test_names']['initial_reconstruction'])


# Explicit terminal profile. The retained reservation tail was created by a
# real lost reservation COMMIT, then independently qualified through cf1 replay.
TERMINAL_TAIL_PRODUCT='fc9388fc4c4f404ce57785eb5bfc66d11e10185a'
TERMINAL_TAIL_REPLAY_SHA='050b2ee51b9495eb462e0037b3e3b1642395735da402ce8145036e362c2b8dc2'
TERMINAL_TAIL_ORIGIN=dict(name='activated-tail',run='installed-compacted-activated-cf1a2ef',subdirectory=ActivatedProfile.DIRECTORY,
    database='bb_compacted_activated_cf1a2ef_reserved_tail',status=ActivatedProfile.STATUS,
    source='cf1a2ef9fd180188955ddba83fbaeca3d61eca2e',owner='f3bf296891b75c390b1de4106d3f7091',label=ActivatedProfile.LABEL,
    client_id='35be229c914d189eb4ad08387ab5acdd9d5032f2c57b3a0040405f51b01a457e',source_clone='reserved-tail',final_case='reserved-tail-retry',
    catalog_sha256='589292f3beb45bbfc4a1bfe1cc827356db1e47f857aa652b132e4cf60ec2acbd',
    result_sha256='6ecd93405b24f4c3550749354b6e5795c8e1a171b5c690c11278088920e45f82',
    snapshot='reserved-tail-final-sql.json',snapshot_sha256='fe84cfdbaf8e7b6dd3a2421cfdc5e4212ca77884d9a862c0c8260c384c2ab305',
    native_file='reserved-tail-retry-after-native.json',native_sha256='478402f7881ce010856ccf09ae3d902ecabcb70704a650dade6bfe8edb07abaa',
    inputs_file='reserved-tail-retry-after-inputs.json',inputs_sha256='7408506e1d05535071e56d7074723b4bad1d9308f94ed6afdd76b230a6a65dfe',
    verification_sha256='3fcf52a5c12bb32bc26c96564cb2aac1ece6c5615b6cdbc8db00c4dd41608570',store='reserved-tail/store')
TERMINAL_ORIGINS=(JOURNEY_ORIGIN,*ACTIVATED_RETAINED_ORIGINS,TERMINAL_TAIL_ORIGIN)
TERMINAL_GRANTS=ACTIVATED_GRANTS+(
    'GRANT EXECUTE ON FUNCTION control.require_activated_initial_source(bytea) TO bigbrotr_control;',
    'GRANT EXECUTE ON FUNCTION control.lock_activated_initial_retirement(bytea), '
    'control.require_activated_initial_retirement(bytea), '
    'control.require_activated_initial_retirement_transition(bytea,bytea,bigint) TO bigbrotr_control;')
TERMINAL_STEPS={'zero':('source','source-retry','retire','retire-retry'),
    'event':('seed','retire-open','source','source-retry','retire-unresolved','abandon','abandon-retry','retire','retire-retry'),
    'missing-copy':('source','source-retry','source-closed-missing'),
    'head':('source-refuse',),'unbound-event':('seed','abandon','retire-refuse'),
    'mixed':('source','retire-refuse'),'reserved-tail':('source-refuse','retire-refuse'),'missing-input':('source-refuse',)}
TERMINAL_ORIGIN_NAMES={name:'compacted-initial' if name in ('event','unbound-event') else
    'activated-live' if name in ('missing-copy','missing-input') else 'activated-tail' if name=='reserved-tail' else 'activated-initial'
    for name in TERMINAL_STEPS}
TERMINAL_SEED_MODE='successor-live-compacted-terminal-event'
TERMINAL_ATTRIBUTION={3:'a1992bc4cc2b57578e06da9ab64cdfe44d8c117a394e8fdb38355e955a64eee9',
    5:'4add1a3691de324ebdc7202debd5f0187cbd895a997d16ec499cdfea16827a26'}


class ActivatedTerminalProfile:
    DIRECTORY='installed-compacted-activated-terminal'
    LABEL='org.bigbrotr.installed-compacted-activated-terminal'
    STATUS='PASS_INSTALLED_COMPACTED_ACTIVATED_TERMINAL_POSITIVE_REFUSALS'
    DATABASE_PREFIX='bb_compacted_terminal_'
    CLONES=tuple(TERMINAL_STEPS)
    STEPS=TERMINAL_STEPS
    CASES=tuple(name+'-'+step for name in TERMINAL_STEPS for step in TERMINAL_STEPS[name])
    GRANTS=TERMINAL_GRANTS
    NOT_RUN=('Actual product reclamation/GC and unlink',
        'Final terminal COMMIT delivery loss, ancestor H/D drift after earlier completion, cancellation and lease expiry',
        'Final retirement Event-mask revalidation after successful original replay',
        'Zero-request claimed acquisition, unresolved import, draining owner and isolated native OPEN retirement guard',
        'Later activated successors, live frontier, retained Archive frontier and empty retirement',
        'Mixed restore/backup, physical loss and capacity')



# A separately selected extension: the first25 terminal profile stays exact.
TERMINAL_FAULT_STEPS={
    'baseline':('seed','source-baseline','abandon-baseline','retire-baseline'),
    **{op+'-'+side:('seed',*(('source',) if op=='abandon' else ('source','abandon') if op=='retire' else ()),op+'-loss',op+'-retry')
       for op in ('source','abandon','retire') for side in ('before','after')},
    **{mode:('seed','source','abandon','wait','retire') for mode in ('cancel','expiry')},
    **{mode:('seed','source','abandon','wait') for mode in ('head-drift','digest-drift','event-mask')},
    'zero-job':('seed','source','retire-unresolved','abandon','retire','retire-retry'),
}
TERMINAL_ZERO_MODE='successor-live-compacted-terminal-zero-claim'
TERMINAL_WAIT_MODES={name:'successor-activated-retirement-compacted-'+('gate' if name in ('head-drift','digest-drift','event-mask') else name)
    for name in ('cancel','expiry','head-drift','digest-drift','event-mask')}
# Source-derived selectors; each loss requires its matching measured baseline.
TERMINAL_COMMIT_SELECTORS={'source':6,'abandon':8,'retire':5}


class ActivatedTerminalFaultProfile:
    DIRECTORY='installed-compacted-terminal-faults'
    LABEL='org.bigbrotr.installed-compacted-terminal-faults'
    STATUS='PASS_INSTALLED_COMPACTED_TERMINAL_DELIVERY_WAITS'
    DATABASE_PREFIX='bb_compacted_terminal_fault_'
    CLIENT_PREFIX='bigbrotr-compacted-terminal-fault-'
    CLONES=tuple(TERMINAL_FAULT_STEPS)
    STEPS=TERMINAL_FAULT_STEPS
    CASES=tuple(name+'-'+step for name in TERMINAL_FAULT_STEPS for step in TERMINAL_FAULT_STEPS[name])
    GRANTS=TERMINAL_GRANTS
    NOT_RUN=('Actual product reclamation/GC and unlink',
        'Source/abandonment ancestor H/D drift after earlier proof; retirement gate does not qualify those final transactions',
        'Unresolved import, draining owner and isolated native OPEN retirement guard',
        'Later activated successors, live frontier, retained Archive frontier and empty retirement',
        'Mixed restore/backup, physical loss, process kill and capacity')



# Separate source/abandon final-proof revalidation, with measured refresh holds.
TERMINAL_PROOF_STEPS={name:('seed',*(('source',) if name.startswith('abandon-') else ()), 'proof')
    for name in ('source-baseline','abandon-baseline','source-head','source-digest','abandon-head','abandon-digest')}
TERMINAL_PROOF_SELECTORS={'source':5,'abandon':7}


class ActivatedSourceProofProfile:
    DIRECTORY='installed-compacted-source-proof'
    LABEL='org.bigbrotr.installed-compacted-source-proof'
    STATUS='PASS_INSTALLED_COMPACTED_SOURCE_FINAL_PROOF'
    DATABASE_PREFIX='bb_compacted_source_proof_'
    CLIENT_PREFIX='bigbrotr-compacted-source-proof-'
    CLONES=tuple(TERMINAL_PROOF_STEPS)
    STEPS=TERMINAL_PROOF_STEPS
    CASES=tuple(name+'-'+step for name in TERMINAL_PROOF_STEPS for step in TERMINAL_PROOF_STEPS[name])
    GRANTS=TERMINAL_GRANTS
    NOT_RUN=('Actual product reclamation/GC and unlink',
        'Other terminal/fault cases are qualified separately and are not re-executed by this profile',
        'Unresolved import, draining owner and isolated native OPEN retirement guard',
        'Later recovery/live frontier/Archive/empty retirement; mixed restore, physical loss, kill and capacity')


TERMINAL_IMPORT_MODE='successor-live-compacted-terminal-import-gate'
TERMINAL_GUARD_STEPS={'import':('seed','source','abandon','retire-refuse'),
    'draining':('source','retire','retire-retry'),'native-open':('retire-refuse',)}
TERMINAL_GUARD_ORIGIN_NAMES={name:'compacted-initial' if name=='import' else 'activated-initial' for name in TERMINAL_GUARD_STEPS}
TERMINAL_GUARD_ORIGINS=(JOURNEY_ORIGIN,PREPARATION_RETAINED_ORIGINS[1])


class ActivatedTerminalGuardProfile:
    DIRECTORY='installed-compacted-terminal-guards'
    LABEL='org.bigbrotr.installed-compacted-terminal-guards'
    STATUS='PASS_INSTALLED_COMPACTED_TERMINAL_GUARDS'
    DATABASE_PREFIX='bb_compacted_terminal_guard_'
    CLIENT_PREFIX='bigbrotr-compacted-terminal-guard-'
    CLONES=tuple(TERMINAL_GUARD_STEPS)
    STEPS=TERMINAL_GUARD_STEPS
    CASES=tuple(name+'-'+step for name in TERMINAL_GUARD_STEPS for step in TERMINAL_GUARD_STEPS[name])
    GRANTS=TERMINAL_GRANTS
    NOT_RUN=('Actual product reclamation/GC and unlink',
        'Import execution, closure, input binding or checkpoint; setup is ordinary guarded relational claim only',
        'Other terminal/delivery/proof cases are qualified separately and are not re-executed by this profile',
        'Later recovery/live frontier/Archive/empty retirement; mixed restore, physical loss, kill and capacity')


def terminal_guard_origins_checked(root,raw=None):
    compacted,initial=journey_origin_checked(root,raw)
    activated,basis=qualified_endpoint(root,raw,initial_journey=False,prepared_origin=PREPARATION_RETAINED_ORIGINS[1])
    assert basis==initial
    return {'compacted-initial':compacted,'activated-initial':activated},initial


def terminal_import_checked(value,pg,control,activated_at,case,window,waiting,released):
    from initial_empty_locks_oracle import app_identity,guards as guard_catalog
    assert set(value)=={'status','cleanup_errors','gates','before','after','control','backend','identity','created_at_us',
        'statements','transactions','server_window','observed_by_us','live_before','live_after','inputs_before','inputs_after',
        'guards_before','guards_after','actor_absence','actor_exit_code','go_before_us'}
    assert value['status']=='CAPTURED' and value['cleanup_errors']==[]
    same(value['before'],pg); assert value['control']==control and value['inputs_before']==value['inputs_after']=={}
    assert control['state']=='active'
    go='/lab/'+case['name']+'-gate.go'
    event=dict(event='live-terminal-import-gate',mode=TERMINAL_IMPORT_MODE,phase='waiting',go=go,gate_timeout_ms=10000,poll_ms=5,
        control_resource=control['resource_id'][2:],control_epoch=control['epoch'],control_incarnation=control['process_incarnation'][2:],activated_at_us=activated_at)
    assert waiting==event and released==dict(event,phase='released')
    assert len(value['gates'])==2 and value['gates'][0]['event']==waiting and value['gates'][1]['event']==released
    assert set(value['server_window'])=={'before_us','after_us'}
    times=[window['before_us'],activated_at,value['gates'][0]['observed_us'],value['server_window']['before_us'],
        value['created_at_us'],value['server_window']['after_us'],value['observed_by_us'],value['go_before_us'],
        value['gates'][1]['observed_us'],window['after_us']]
    assert all(type(v) is int for v in times) and times==sorted(times)
    assert value['go_before_us']<control['lease_expires_at_us'] and value['gates'][1]['observed_us']-value['gates'][0]['observed_us']<10_000_000
    live=dict(pid=control['control_backend_pid'],backend_start=control['control_backend_start'],database=case['database'],user='bigbrotr_control',state='idle',xact_start=None)
    assert value['live_before']==value['live_after']==live
    actor=value['backend']; assert set(actor)=={'pid','backend_start','database','user','application','isolation','synchronous_commit'}
    number(actor['pid'],1,integer=True); assert actor['pid']!=live['pid'] and actor['database']==case['database']
    assert actor['user']=='bigbrotr_control' and actor['application']=='terminal_import_claim'
    assert actor['isolation']=='read committed' and actor['synchronous_commit']=='on'
    assert value['server_window']['before_us']<=retained.instant(actor['backend_start'])<=value['created_at_us']
    app_identity(value['identity'],actor)
    assert value['actor_absence']=='0' and type(value['actor_exit_code']) is int
    guard_catalog(value['guards_before']); assert value['guards_after']==value['guards_before']
    return terminal_import_rows(value,pg,control)


def terminal_import_rows(value,pg,control):
    from initial_empty_preparer_oracle import claim_statements,JOB,PRINCIPAL,REQUEST,DIGEST
    assert value['statements']==claim_statements(control,value['created_at_us'])
    responses=value['transactions']; assert len(responses)==5
    assert all(responses[i]==dict(sqlstate='00000',rows=0,lines=[]) for i in (0,4))
    assert not any(r['job_id']==JOB or r['principal_id']==PRINCIPAL or r['request_key']==REQUEST for r in pg['jobs'])
    observed=only(value['after']['jobs'],job_id=JOB); xid=observed['xmin']; model.fresh_xmin(xid,pg)
    dataset=only(pg['datasets'])
    accepted=dict(job_id=JOB,dataset_id=dataset['dataset_id'],principal_id=PRINCIPAL,request_key=REQUEST,request_digest=DIGEST,
        kind='import',config_revision=dataset['config_revision'],policy_revision=dataset['policy_revision'],created_at_us=value['created_at_us'],
        state='accepted',job_revision=1,checkpoint_revision=0,reason_code=None,result_manifest_digest=None,
        owner_resource_id=None,owner_epoch=None,owner_role='control',owner_incarnation=None,completed_at_us=None,
        input_topology_generation=None,input_complete=False,acquisition_job_protocol=None,export_job_protocol=None,xmin=xid)
    sealed=dict(accepted,input_complete=True)
    queued=dict(sealed,state='queued',job_revision=2,owner_resource_id=control['resource_id'],owner_epoch=control['epoch'],owner_incarnation=control['process_incarnation'])
    for response,row in zip(responses[1:4],(accepted,sealed,queued),strict=True):
        assert response==dict(sqlstate='00000',rows=1,lines=response['lines']) and [loads(line) for line in response['lines']]==[row]
    wanted=copy.deepcopy(pg); wanted['jobs'].append(queued)
    wanted['job_admissions'].append(dict(job_id=JOB,admission_ordinal=max(r['admission_ordinal'] for r in pg['job_admissions'])+1,xmin=xid))
    same(value['after'],wanted)
    return wanted


def terminal_guard_mutation_statement(kind,expected,at):
    assert kind in ('draining','native-open') and type(at) is int
    pg,native,inputs=expected; basis=terminal_basis(expected); intent=only(pg['initial_ingest_successions'])
    slot=intent['producer_slot_id'][2:]; resource=intent['owner_resource_id'][2:]
    start="BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\nSET LOCAL session_replication_role=replica;\n"
    if kind=='draining':
        commands=[("owner_epochs","state='draining'",f"resource_id=decode('{resource}','hex') AND epoch=2 AND role='ingest' AND state='active'")]
    else:
        commands=[('producer_epochs',"state='closing'",f"producer_slot_id=decode('{slot}','hex') AND producer_epoch=2 AND state='open'"),
            ('source_streams',f"state='closed',closed_at_us={at},last_complete_sequence=0,closure_digest=decode('{basis['digest']}','hex')",
                f"owner_resource_id=decode('{resource}','hex') AND owner_epoch=2 AND state='open'")]
    return start+''.join('UPDATE control.'+table+' SET '+values+' WHERE '+predicate+'\nRETURNING (to_jsonb('+table+")||jsonb_build_object('xmin',xmin::text))::text;\n" for table,values,predicate in commands)+'COMMIT;\n'


def terminal_guard_mutation_checked(record,expected,kind):
    from initial_empty_locks_oracle import guards
    assert set(record)=={'kind','before','after','statement','sql_output','server_window','guards_before','guards_after'}
    assert record['kind']=='privileged-isolated-terminal-'+kind and record['before']==dict(control=expected[0],native=expected[1],inputs=expected[2])
    window=record['server_window']; assert set(window)=={'before_us','after_us'}
    at=number(window['before_us'],1,integer=True); number(window['after_us'],at,at+10_000_000,integer=True)
    assert record['statement']==terminal_guard_mutation_statement(kind,expected,at)
    guards(record['guards_before']); assert record['guards_after']==record['guards_before']
    pg,native,inputs=copy.deepcopy(expected); basis=terminal_basis(expected)
    assert not inputs and len(native['rows'])==84 and basis['issued']==0 and basis['cut']==0
    owner=only(pg['owner_epochs'],role='ingest',epoch=2); producer=only(pg['producer_epochs'],producer_epoch=2); source=only(pg['source_streams'],owner_epoch=2)
    assert owner['state']=='active' and all(r['state']=='active' for r in pg['allocation_domains'] if r['owner_epoch']==2)
    assert only(pg['role_instances'],role_instance_id=owner['role_instance_id'])['retired_at_us'] is None
    assert source['opened_at_us']<=at
    returned=[loads(line) for line in record['sql_output'].splitlines()]; assert len(returned)==(1 if kind=='draining' else 2)
    xid=returned[0]['xmin']; model.fresh_xmin(xid,pg)
    if kind=='draining':
        assert producer['state']=='closing' and source['state']=='closed' and native['sequence']==126
        assert source['closed_at_us']<=at and source['closure_digest']=='\\x'+basis['digest']; owner.update(state='draining',xmin=xid); assert returned==[owner]
    else:
        assert kind=='native-open' and producer['state']==source['state']=='open' and native['sequence']==125
        marker=only(native['rows'],cf='receipts',key=(b'\x01\x31'+binary(producer['producer_slot_id'])+be(2)).hex())
        assert marker['value']==(b'\x01\x31'+be(0)*3).hex()
        producer.update(state='closing',xmin=xid)
        source.update(state='closed',closed_at_us=at,last_complete_sequence=0,closure_digest='\\x'+basis['digest'],xmin=xid)
        assert returned==[producer,source]
    assert set(record['after'])=={'control','native','inputs'}
    same(record['after']['control'],pg); assert (record['after']['native'],record['after']['inputs'])==(native,inputs)
    return pg,native,inputs


def terminal_guard_mutation_controls(record,expected,kind):
    terminal_guard_mutation_checked(record,expected,kind)
    changes=[('unbound-statement',lambda r:r.update(statement='COMMIT;')),
        ('native-write',lambda r:r['after']['native'].update(sequence=r['after']['native']['sequence']+1)),
        ('extra-SQL-delta',lambda r:only(r['after']['control']['initial_ingest_successions']).update(prepared_by_epoch=999)),
        ('guard-bypass-left-enabled',lambda r:r['guards_after'].update(session_replication_role='replica'))]
    # Keep every actual field; perturb full captures rather than projecting rows.
    if kind=='native-open':
        changes.extend([('wrong-source-digest',lambda r:only(r['after']['control']['source_streams'],owner_epoch=2).update(closure_digest='\\x'+'00'*32)),
            ('different-row-transaction',lambda r:only(r['after']['control']['source_streams'],owner_epoch=2).update(xmin='4294967000'))])
    else:
        changes.extend([('owner-still-active',lambda r:only(r['after']['control']['owner_epochs'],role='ingest',epoch=2).update(state='active')),
            ('role-retired-early',lambda r:only(r['after']['control']['role_instances'],role_instance_id=only(r['after']['control']['owner_epochs'],role='ingest',epoch=2)['role_instance_id']).update(retired_at_us=r['server_window']['before_us']))])
    rejected=[]
    for label,change in changes:
        altered=copy.deepcopy(record); change(altered)
        try: terminal_guard_mutation_checked(altered,expected,kind)
        except (AssertionError,KeyError,TypeError,ValueError): rejected.append(label)
        else: raise AssertionError('accepted altered guard fixture: '+label)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def terminal_origins_checked(root,raw=None):
    previous,initial=activated_origins_checked(root,raw)
    tail,basis=qualified_endpoint(root,raw,initial_journey=False,terminal_origin=TERMINAL_TAIL_ORIGIN)
    assert basis==initial
    return {**{k:v for k,v in previous.items() if k!='initial'},'activated-tail':tail},initial


def terminal_specification(case,*,faults=False,post_proof=False,guards=False):
    assert all(type(v) is bool for v in (faults,post_proof,guards)) and sum((faults,post_proof,guards))<=1
    profile=ActivatedTerminalGuardProfile if guards else ActivatedSourceProofProfile if post_proof else ActivatedTerminalFaultProfile if faults else ActivatedTerminalProfile
    all_steps=profile.STEPS
    name=case['clone']; assert name in all_steps
    prefix=name+'-'; assert case['name'].startswith(prefix)
    step=case['name'][len(prefix):]; steps=all_steps[name]; index=steps.index(step)
    assert case['origin']==(TERMINAL_GUARD_ORIGIN_NAMES[name] if guards else 'compacted-initial' if faults or post_proof else TERMINAL_ORIGIN_NAMES[name])
    assert case['predecessor']==(prefix+steps[index-1] if index else None)
    operation=(TERMINAL_IMPORT_MODE if guards else TERMINAL_ZERO_MODE if faults and name=='zero-job' else TERMINAL_SEED_MODE) if step=='seed' else (TERMINAL_WAIT_MODES[name] if faults and step=='wait' else step.split('-')[0]+'-activated')
    if post_proof and step=='proof': operation=name.split('-')[0]+'-activated'
    assert case['operation']==operation
    assert re.fullmatch(profile.DATABASE_PREFIX+'[0-9a-f]{7}_'+name.replace('-','_'),case['database'])
    assert len(case['database'])<=63 and case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def terminal_network_checked(config,name,*,faults=False,post_proof=False,guards=False):
    assert all(type(v) is bool for v in (faults,post_proof,guards)) and sum((faults,post_proof,guards))<=1
    assert name in (TERMINAL_GUARD_STEPS if guards else TERMINAL_PROOF_STEPS if post_proof else TERMINAL_FAULT_STEPS if faults else TERMINAL_STEPS)
    event=name=='import' if guards else True if post_proof else (name!='zero-job') if faults else name in ('event','unbound-event')
    endpoint='ws://127.0.0.1:7447/'+('installed-fixture' if event else 'installed-successor-fixture')
    assert config['network']['private']['rules']==[dict(endpoint=endpoint,cidr='127.0.0.1/32',port=7447)]
    assert config['network']['dns']['resolver']=='127.0.0.1:9' and config['network']['tls']['extra_roots']==[]


def terminal_attribution(contents,initial):
    from installed_sync_oracle import decode
    slot=only(initial[0]['producer_epochs'])['producer_slot_id'][2:]
    assert set(contents)=={f'{slot}-0000000000000001-{n:016x}.cbor' for n in TERMINAL_ATTRIBUTION}
    decoded={}
    for n,digest in TERMINAL_ATTRIBUTION.items():
        name=f'{slot}-0000000000000001-{n:016x}.cbor'; data=bytes.fromhex(contents[name])
        assert sha(data)==digest and initial[2][name]==dict(bytes=len(data),sha256=digest)
        decoded[n]=decode(data.removeprefix(b'bigbrotr/producer-request/1\0'))
    assert decoded[3][0]==[bytes.fromhex(slot),1,3] and decoded[3][1:3]==['admit-evidence',1]
    assert decoded[5][0]==[bytes.fromhex(slot),1,5] and decoded[5][1:3]==['admit',1]
    return decoded


def terminal_event_science(plan,prepared_rows,inputs,contents,initial,report,receive_clock,window):
    import activated_source_durable_oracle as durable
    from bootstrap_oracle import cbor
    decoded=terminal_attribution(contents,initial); assert inputs=={}
    ds,slot=(bytes.fromhex(plan[k]) for k in ('dataset','producer_slot'))
    old=decoded[5][3]; scope=old[1]; observation=old[5]
    assert scope==[ds,4,bytes.fromhex('0e2a2cc69f18bd685ff795bd57faf3c0'),3]
    assert old[3:5]==['network',None] and old[2][0].hex()=='32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791'
    assert set(receive_clock)=={'clock','resolution_ns','resolution_us'} and receive_clock['clock']=='CLOCK_REALTIME'
    ns=receive_clock['resolution_ns']; resolution=receive_clock['resolution_us']
    number(ns,1,1_000_000_000,integer=True); assert resolution==(ns+999)//1000
    at=report['received_at_us']; model.clock(at,window); assert at>observation[0]
    sample=[at,observation[1],[observation[2][0],resolution,None,4 if resolution>1 else 0],observation[3]]
    event=durable.SIGNED
    body=[bytes.fromhex(event[k]) for k in ('id','pubkey')]+[1,1,[],'import then network']
    signature=bytes.fromhex(event['sig']); event_ref=[ds,8,bytes.fromhex(dict(plan['domains'])[8]),1]
    h=lambda data:bytes.fromhex(sha(data))
    raw=b'bigbrotr/producer-request/1\0'+cbor([[slot,2,1],'admit',1,[1,scope,[*body,signature],'network',None,sample]])
    rows=dict(prepared_rows); refs={}
    assert len(rows)==84 and ('default',b'\x01\x06'+ds+body[0]) not in rows
    for n in range(3,8):
        value=rows['facts',b'\x01\x20'+ds+be(0,4)+be(1)+be(n)]
        assert value[:2]==b'\x01\x20' and h(value[34:])==value[2:34]
        refs[1,n]=[[ds,0,1,n],value[2:34]]
    def ref_key(r): return r[0]+be(r[1],2)+r[2]+be(r[3])
    def fact(n,family,payload):
        value=b'bigbrotr/fact/1\0'+cbor([[ds,0,2,n],family,1,scope,at,payload])
        refs[2,n]=[[ds,0,2,n],h(value)]
        rows['facts',b'\x01\x20'+ds+be(0,4)+be(2)+be(n)]=b'\x01\x20'+h(value)+value
    def index(domain,key,n): rows['default',bytes([1,domain])+key]=bytes([1,domain])+ds+be(0,4)+be(2)+be(n)+refs[2,n][1]
    descriptor=[1,'event',1,dict(event_id=body[0])]
    fact(1,'dictionary',[event_ref,*descriptor]); index(4,ref_key(event_ref),1)
    rows['default',b'\x01\x05'+ds+be(8,2)+h(cbor(descriptor))+ref_key(event_ref)]=b'\x01\x05'+cbor(descriptor)
    rows['default',b'\x01\x03'+ds+be(8,2)+event_ref[2]]=b'\x01\x03'+be(1)
    fact(2,'event-payload',[*body,event_ref]); index(6,ds+body[0],2)
    fact(3,'event-eligibility',[body[0],h(b'bigbrotr/body/1\0'+cbor(body)),signature,at,'network',None]); index(7,ds+body[0]+ref_key(scope),3)
    observer=sample[1]; clock=sample[2]; endpoint,access,method=sample[3]
    fact(4,'event-receive',[body[0],at,observer,clock,None]); index(8,ds+body[0]+ref_key(scope),4)
    fact(5,'provenance',[body[0],endpoint,observer,access,at,clock,method,None]); index(9,ds+body[0]+ref_key(endpoint)+ref_key(observer)+ref_key(access)+ref_key(scope),5)
    batch=[refs[2,n] for n in range(1,6)]; deps=[refs[1,n] for n in range(3,8)]+batch
    receipt=b'bigbrotr/receipt/1\0'+cbor([1,[slot,2,1],h(raw),[ds,1,1,bytes.fromhex(plan['recovery_generation']),1],batch,deps])
    rows['default',b'\x01\x02'+ds+be(0,4)+be(2)]=b'\x01\x02'+be(5)
    rows['facts',b'\x01\x21'+ds+be(0,4)+be(2)+be(1)]=b'\x01\x21'+be(5)+h(b'bigbrotr/admission-batch/1\0'+cbor([ds,0,2,1,5,batch]))
    rows['receipts',b'\x01\x30'+slot+be(2)+be(1)]=receipt
    rows['receipts',b'\x01\x31'+slot+be(2)]=b'\x01\x31'+be(1)+be(0)*2
    assert len(rows)==98
    originals={slot.hex()+'-0000000000000002-0000000000000001.cbor':dict(bytes=len(raw),sha256=sha(raw))}
    assert report==dict(event='live-durable-input',request=1,event_id=event['id'],preimage=raw.hex(),request_digest=sha(raw),
        provenance=True,wire_sequence=1,wire_request=report['wire_request'],received_at_us=at,predecessor_received_at_us=observation[0])
    return dict(plan=plan,ds=ds,slot=slot,rows=rows,inputs=originals),raw,receipt


def verify_terminal_seed(events,expected,initial,case,window,attribution,receive_clock,*,import_claim=None,faults=False,post_proof=False,guards=False):
    import activated_source_durable_oracle as durable
    import initial_live_oracle as live
    import initial_succession_oracle as original
    name,step=terminal_specification(case,faults=faults,post_proof=post_proof,guards=guards); assert step=='seed'
    zero=faults and name=='zero-job'; mode=TERMINAL_IMPORT_MODE if guards else TERMINAL_ZERO_MODE if zero else TERMINAL_SEED_MODE
    pg,native,inputs=copy.deepcopy(expected)
    plan,rows=history_seed_plan(pg,native,inputs,only(events,event='successor-plan'),initial,compacted_initial=True)
    model.clock(plan['created_at_us'],window); position=0
    def take(event,stage=None,phase=None):
        nonlocal position
        v=events[position]; position+=1; assert (v['event'],v.get('stage'),v.get('phase'))==(event,stage,phase); return v
    def snap(stage): return history_snapshot(take('successor-state',stage),native,inputs,pg,stage=stage)
    assert take('successor-control','initial','claim-planned')==dict(event='successor-control',stage='initial',phase='claim-planned',role='d3'*16,incarnation='dd'*16,lease_ttl_us='20000000')
    same(pg,snap('initial-before-claim')); claimed=take('successor-control','initial','claimed'); after=snap('initial-after-claim')
    control=original.claim(pg,after,1); model.clock(control['acquired_at_us'],window)
    assert claimed==dict(event='successor-control',stage='initial',phase='claimed',resource=control['resource_id'][2:],epoch=control['epoch'],role='d3'*16,incarnation='dd'*16)
    pg=after; assert take('successor-compacted-proof')==journey_proof(initial); assert take('successor-plan')==plan
    after=snap('before-prepare'); control=original.renewed(pg,after,control,plan['created_at_us']); model.fresh_xmin(control['xmin'],pg); pg=after
    native=journey_dump(rows,125)
    after=snap('prepared'); original.prepared(pg,after,plan,control,compacted_history=True); pg=after
    backend=take('successor-backend','initial')
    assert backend==dict(event='successor-backend',stage='initial',database=case['database'],pid=control['control_backend_pid'],backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
    assert take('live-activation-start')==dict(event='live-activation-start',mode=mode,control_epoch=control['epoch'])
    activated=take('live-activation-result'); after=snap('activated-zero')
    assert activated==dict(event='live-activation-result',writer=True,error=None,activated_at_us=activated['activated_at_us'])
    live.activated(pg,after,plan,control,window,activated['activated_at_us']); pg=after
    if guards:
        gate_backend=take('successor-backend','terminal-import-gate'); assert gate_backend==dict(backend,stage='terminal-import-gate')
        waiting=take('live-terminal-import-gate',phase='waiting'); released_gate=take('live-terminal-import-gate',phase='released')
        pg=terminal_import_checked(import_claim,pg,control,activated['activated_at_us'],case,window,waiting,released_gate)
        same(pg,snap('terminal-import-released'))
    else: assert import_claim is None
    if zero:
        assert attribution is None
        pg,control=terminal_zero_claim(take,snap,pg,control,plan,window)
    else:
        job=take('live-durable-job'); after=snap('durable-job-claimed')
        control=durable.authenticated_job(pg,after,job,control,window,modern=True); pg=after
        for kind in ('visibility','observation'):
            certification=take('live-terminal-certification')
            assert certification==dict(event='live-terminal-certification',kind=kind,available=True) and certification['available'] is True
        wire=take('live-durable-wire'); subscription=wire['request'][1]
        assert re.fullmatch('[0-9a-f]{32}0000000000000001',subscription) and int(subscription[:32],16)
        assert wire==dict(event='live-durable-wire',request=['REQ',subscription,dict(kinds=[1],limit=1)],signed_event=durable.SIGNED,events=1,eose=True)
        assert take('live-durable-peer-closed')==dict(event='live-durable-peer-closed',eof=True)
        same(pg,snap('durable-received')); observation=take('live-durable-input')
        assert observation['wire_request']==subscription and job['accepted_at_us']<=observation['received_at_us']<job['deadline_at_us']
        science,raw,receipt=terminal_event_science(plan,rows,inputs,attribution,initial,observation,receive_clock,window)
        native=journey_dump(science['rows'],141); inputs=science['inputs']
        after=snap('terminal-event'); durable.admitted(pg,after,science,raw,receipt,control,observation,window,modern=True); pg=after
        assert take('live-terminal-event')==dict(event='live-terminal-event',request=1,receipt=receipt.hex(),dependencies=10,
            source_cut=5,native_rows=98,native_sequence=141,peer_joined=True,job_complete=False,session_revoked=False)
    same(pg,snap('final-before-control-end'))
    if zero:
        assert take('live-zero-claim-release')==dict(event='live-zero-claim-release',attempted=True,error=None)
        assert take('live-zero-claim-driver','control')==dict(event='live-zero-claim-driver',stage='control',joined=True,failed=False,aborted=False,error=None)
    assert take('successor-control','final','released-and-joined')==dict(event='successor-control',stage='final',phase='released-and-joined',resource=control['resource_id'][2:],epoch=control['epoch'])
    if zero:
        assert take('live-zero-claim-backend-absence')==dict(event='live-zero-claim-backend-absence',error=None)
        assert take('successor-backend','initial')==dict(backend,present=False)
    after=snap('final-after-control-end'); original.ended(pg,after,control,True)
    released=only(after['owner_epochs'],role='control',epoch=control['epoch'])
    model.fresh_xmin(released['xmin'],pg); model.clock(released['released_at_us'],window)
    assert (only(pg['jobs'],request_key='\\x'+'a6'*16)['created_at_us'] if zero else only(pg['producer_outcomes'],producer_epoch=2)['recorded_at_us'])<=released['released_at_us']; pg=after
    if not zero: assert take('successor-backend','initial')==dict(backend,present=False)
    same(pg,snap('final'))
    if zero: assert take('live-zero-claim-driver','observer')==dict(event='live-zero-claim-driver',stage='observer',joined=True,failed=False,aborted=False,error=None)
    complete=dict(event='live-complete',mode=mode,native='closed',control='released-and-joined',writer=False,admission=not zero)
    if zero: complete['passed']=True
    assert take('live-complete')==complete
    assert position==len(events)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],native_rows=84 if zero else 98,native_sequence=125 if zero else 141,
        original_target_files=0 if zero else 1,compacted_requests=7,original_predecessor_requests=0,target_events=0 if zero else 1,target_source_cut=0 if zero else 5,
        activating_control_epoch=control['epoch'],claimed_acquisition_jobs=1,job_complete=False,session_revoked=False,
        **(dict(claimed_import_jobs=1,import_execution='NOT_RUN') if guards else {}))


def terminal_failure(name,step,*,guards=False):
    if guards: return {'import':'import','native-open':'native-open'}.get(name) if step=='retire-refuse' else None
    if step=='seed': return None
    if name=='head': return 'head'
    if name=='missing-input': return 'missing-input'
    if name=='reserved-tail': return 'reserved-tail' if step.startswith('source') else 'retirement-prerequisite'
    if step in ('retire-open','retire-unresolved'): return 'retirement-prerequisite'
    if name=='mixed' and step=='retire-refuse': return 'retirement-prerequisite'
    if name=='unbound-event' and step=='retire-refuse': return 'unbound-event'
    if step=='source-closed-missing': return 'closed-missing-copy'
    return None


TERMINAL_ERRORS={
    'import':'producer operation unavailable: Control(Backend)',
    'native-open':'producer operation unavailable: Control(InvalidMetadata)',
    'head':'producer operation unavailable: Progress(InvalidContext)',
    'commit-loss':'producer operation unavailable: CommitUnknown',
    'proof-digest':'producer operation unavailable: Native(Compacted(StateMismatch))',
    'missing-input':'producer operation unavailable: Native(RequestInput(Missing))',
    'reserved-tail':'producer operation unavailable: Control(InvalidMetadata)',
    'retirement-prerequisite':'producer operation unavailable: Control(Backend)',
    'closed-missing-copy':'producer operation unavailable: Control(Backend)',
    # Retirement original replay rejects the native Event's missing job before
    # the final source-proof Event-mask check can be reached.
    'unbound-event':'producer operation unavailable: Outcome(Conflict)',
}


def terminal_seed_closed(response,database,*,zero=False,import_claim=None):
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    if import_claim is not None:
        assert import_claim['status']=='CAPTURED' and import_claim['cleanup_errors']==[] and 'failure' not in import_claim
        assert type(import_claim['actor_exit_code']) is int and import_claim['actor_absence']=='0'
        assert response['outcome']=='PROCESS_EXITED' and response['readers_joined'] is True and response['overflow'] is False
    events=[loads(v) for v in response['stdout'].splitlines()]
    wanted=dict(event='live-complete',mode=TERMINAL_IMPORT_MODE if import_claim is not None else TERMINAL_ZERO_MODE if zero else TERMINAL_SEED_MODE,native='closed',control='released-and-joined',writer=False,admission=not zero)
    if zero: wanted['passed']=True
    assert events[-1]==wanted
    backends=[e for e in events if e['event']=='successor-backend']
    if import_claim is not None:
        present,gate,absent=backends; assert gate==dict(present,stage='terminal-import-gate')
    else: present,absent=backends
    assert present['database']==database and present['present'] is True and absent==dict(present,present=False)
    if not zero: assert only(events,event='live-durable-peer-closed')==dict(event='live-durable-peer-closed',eof=True)
    else:
        for stage in ('control','observer'):
            assert only(events,event='live-zero-claim-driver',stage=stage)==dict(event='live-zero-claim-driver',stage=stage,joined=True,failed=False,aborted=False,error=None)
    # The exact source-bound probe drops the writer before release, joins the
    # Control driver, then joins the observer before returning process status0.
    return events


def terminal_closed(response,case,*,faults=False,post_proof=False,guards=False):
    name,step=terminal_specification(case,faults=faults,post_proof=post_proof,guards=guards); failure='commit-loss' if faults and step.endswith('-loss') else terminal_failure(name,step,guards=guards)
    if post_proof and step=='proof' and not name.endswith('-baseline'): failure='head' if name.endswith('-head') else 'proof-digest'
    code=4 if failure else 0
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==code
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code']==code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failure:
        assert response['stdout']=='' and response['stderr']=='error: '+TERMINAL_ERRORS[failure]+'\n'; return None
    assert response['stderr']==''; values=response['stdout'].splitlines(); assert len(values)==1
    return loads(values[0])


def terminal_basis(expected):
    pg,native,inputs=expected; intent=only(pg['initial_ingest_successions']); slot=binary(intent['producer_slot_id']); ds=binary(intent['dataset_id'])
    values=model.native_map(native); marker=bytes.fromhex(values['receipts',(b'\x01\x31'+slot+be(2)).hex()])
    assert marker[:2] in (b'\x01\x31',b'\x02\x31') and len(marker)==(26 if marker[:2]==b'\x01\x31' else 27)
    issued=int.from_bytes(marker[2:10],'big'); acknowledged=int.from_bytes(marker[10:18],'big')
    assert issued in (0,1) and acknowledged==0
    manifest=bytes.fromhex(values['default','0101']); assert sha(manifest)==intent['target_manifest_sha256'][2:]
    handoff=bytes.fromhex(values['default',(b'\x01\x0a'+ds+binary(intent['owner_resource_id'])+be(1)).hex()])
    assert sha(handoff)==intent['handoff_sha256'][2:] and handoff.hex()==intent['handoff'][2:]
    cut_value=bytes.fromhex(values['default',(b'\x01\x02'+ds+be(0,4)+be(2)).hex()]); assert cut_value[:2]==b'\x01\x02' and len(cut_value)==10
    cut=int.from_bytes(cut_value[2:],'big'); assert cut in (0,1,5)
    preimage=b'bigbrotr/activated-initial-source-closure/1\0'+bytes.fromhex(sha(handoff))+bytes.fromhex(sha(manifest))+slot+be(2)+be(issued)
    receipts={}
    for n in range(1,issued+1):
        request=only(pg['producer_requests'],producer_epoch=2,request_sequence=n)
        receipt=bytes.fromhex(values['receipts',(b'\x01\x30'+slot+be(2)+be(n)).hex()]); receipts[n]=receipt
        filename=slot.hex()+'-0000000000000002-'+f'{n:016x}.cbor'
        if filename in inputs: assert inputs[filename]==dict(bytes=request['input_bytes'],sha256=request['request_digest'][2:])
        preimage+=be(n)+binary(request['request_digest'])+bytes.fromhex(sha(receipt))
    return dict(issued=issued,acknowledged=acknowledged,cut=cut,digest=sha(preimage+be(cut)),receipts=receipts)


def terminal_effect_time(m,at,lower,*,released=True):
    model.clock(at,m.window)
    assert max(lower,m.control['acquired_at_us'])<=at<m.control['lease_expires_at_us']
    if released: assert at<=m.control['released_at_us']<m.control['lease_expires_at_us']
    else: assert m.control['state']=='active' and m.control['released_at_us'] is None


def terminal_source_delta(m,observed,basis,*,released=True):
    producer=only(m.pg['producer_epochs'],producer_epoch=2); source=only(m.pg['source_streams'],owner_epoch=2)
    if producer['state']=='open':
        actual=only(observed['producer_epochs'],producer_epoch=2); model.fresh_xmin(actual['xmin'],m.pg)
        producer.update(state='closing',xmin=actual['xmin'])
    assert producer['state']=='closing'
    if source['state']=='open':
        actual=only(observed['source_streams'],owner_epoch=2); at=actual['closed_at_us']
        model.clock(at,m.window); model.fresh_xmin(actual['xmin'],m.pg)
        terminal_effect_time(m,at,source['opened_at_us'],released=released)
        source.update(state='closed',last_complete_sequence=basis['cut'],closure_digest='\\x'+basis['digest'],closed_at_us=at,xmin=actual['xmin'])
    assert source['state']=='closed' and (source['last_complete_sequence'],source['closure_digest'])==(basis['cut'],'\\x'+basis['digest'])


def terminal_abandon_delta(m,observed,basis,*,released=True):
    activation=only(m.pg['initial_ingest_activations']); source=only(m.pg['source_streams'],owner_epoch=2)
    prior=[g for g in m.pg['acquisition_gaps'] if g['producer_epoch']==2]
    jobs=[j for j in m.pg['jobs'] if j['kind']=='acquisition' and (j['owner_resource_id'],j['owner_epoch'],j['owner_incarnation'])==
        tuple(activation[k] for k in ('activated_by_resource','activated_by_epoch','activated_by_incarnation'))]
    assert len(prior)+len(jobs)==1
    for job in jobs:
        assert job['state']=='queued' and job['job_revision']==2 and job['checkpoint_revision']==0
        gap=only(observed['acquisition_gaps'],job_id=job['job_id']); model.fresh_xmin(gap['xmin'],m.pg)
        at=gap['abandoned_at_us']; model.clock(at,m.window)
        terminal_effect_time(m,at,max(source['closed_at_us'],job['created_at_us']),released=released)
        wanted=dict(job_id=job['job_id'],dataset_id=job['dataset_id'],producer_slot_id=activation['producer_slot_id'],producer_epoch=2,
            shard_id=0,source_owner_epoch=2,last_complete_sequence=basis['cut'],source_closure_digest='\\x'+basis['digest'],
            previous_job_revision=2,previous_owner_resource=activation['activated_by_resource'],previous_owner_epoch=activation['activated_by_epoch'],
            previous_owner_incarnation=activation['activated_by_incarnation'],issuer_resource=m.control['resource_id'],issuer_epoch=m.control['epoch'],
            issuer_incarnation=m.control['process_incarnation'],owner_role='control',abandoned_at_us=at,reason='activated_initial_producer_abandoned',xmin=gap['xmin'])
        assert gap==wanted; m.pg['acquisition_gaps'].append(wanted)
        job.update(state='cancelled',job_revision=4,reason_code=wanted['reason'],completed_at_us=at,owner_resource_id=m.control['resource_id'],
            owner_epoch=m.control['epoch'],owner_incarnation=m.control['process_incarnation'],xmin=gap['xmin'])
    return len(jobs)


def terminal_retire_delta(m,observed,basis,*,released=True,allow_draining=False):
    assert type(allow_draining) is bool
    owner=only(m.pg['owner_epochs'],role='ingest',epoch=2); role=only(m.pg['role_instances'],role_instance_id=owner['role_instance_id'])
    producer=only(m.pg['producer_epochs'],producer_epoch=2); source=only(m.pg['source_streams'],owner_epoch=2)
    domains=[d for d in m.pg['allocation_domains'] if d['owner_epoch']==2]; assert len(domains)==9
    if producer['state']=='retired':
        assert owner['state']=='released' and all(d['state']=='closed' for d in domains)
        return 0
    assert producer['state']=='closing' and owner['state'] in (('active','draining') if allow_draining else ('active',)) and role['retired_at_us'] is None and all(d['state']=='active' for d in domains)
    actual=only(observed['producer_epochs'],producer_epoch=2); at,xid=actual['retired_at_us'],actual['xmin']
    model.clock(at,m.window); model.fresh_xmin(xid,m.pg)
    terminal_effect_time(m,at,source['closed_at_us'],released=released)
    producer.update(state='retired',retired_at_us=at,closure_digest='\\x'+basis['digest'],xmin=xid)
    owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest='\\x'+basis['digest'],xmin=xid)
    role.update(retired_at_us=at,xmin=xid)
    for d in domains: d.update(state='closed',closed_at_us=at,closure_digest='\\x'+basis['digest'],xmin=xid)
    return 12


def verify_terminal_context(context,initial,expected,*,faults=False,post_proof=False,guards=False,baseline=None):
    case=context['case']; name,step=terminal_specification(case,faults=faults,post_proof=post_proof,guards=guards)
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before'])==expected[1:]
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    if step=='seed':
        assert set(context)=={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
            'catalog_before','catalog_after','backend_absence','backends','custody','arguments','attribution','receive_clock'} | ({'import_claim'} if guards else set())
        assert context['backends']==[] and context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
        assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
        for v in absence['attempts']: number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
        assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
        assert context['arguments']==[case['operation'],case['database'],'/run/secrets/application',case['config']]+(['/lab/'+case['name']+'-gate'] if guards else [])
        number(context['response']['seconds'],0,120)
        profile=ActivatedTerminalGuardProfile if guards else ActivatedSourceProofProfile if post_proof else ActivatedTerminalFaultProfile if faults else ActivatedTerminalProfile
        revision=case['database'].removeprefix(profile.DATABASE_PREFIX)[:7]
        client_prefix=ActivatedTerminalGuardProfile.CLIENT_PREFIX if guards else ActivatedSourceProofProfile.CLIENT_PREFIX if post_proof else ActivatedTerminalFaultProfile.CLIENT_PREFIX if faults else 'bigbrotr-compacted-terminal-'
        assert context['response']['command']==['docker','exec',client_prefix+revision,'/build/ingest_bootstrap_probe',*context['arguments']]
        events=terminal_seed_closed(context['response'],case['database'],zero=faults and name=='zero-job',import_claim=context.get('import_claim'))
        after,result=verify_terminal_seed(events,expected,initial,case,window,context['attribution'],context['receive_clock'],import_claim=context.get('import_claim'),faults=faults,post_proof=post_proof,guards=guards)
        same(context['after'],after[0]); assert (context['native_after'],context['inputs_after'])==after[1:]
        return result
    if post_proof and step=='proof': return verify_terminal_delivery(context,initial,expected,baseline,post_proof=True)
    if faults and step=='wait': return verify_terminal_wait(context,initial,expected)
    if faults and step.endswith(('-baseline','-loss')):
        return verify_terminal_delivery(context,initial,expected,baseline)
    assert set(context)==CONTEXT_FIELDS | ({'guard_diagnostic'} if guards and name=='import' and step=='retire-refuse' else set()) and context['snapshots']==[] and context['fixture']==dict(cleanup_errors=[])
    assert context['inputs_after']==expected[2] and set(context['input_contents'])==set(expected[2])
    for filename,value in context['input_contents'].items():
        data=bytes.fromhex(value); assert expected[2][filename]==dict(bytes=len(data),sha256=sha(data))
    assert len(expected[2])<=64 and sum(v['bytes'] for v in expected[2].values())<=32*1024**2
    report=terminal_closed(context['response'],case,faults=faults,post_proof=post_proof,guards=guards); failure=terminal_failure(name,step,guards=guards)
    backend_checked(context['backend'],case['database'],expected[0],context['after'])
    m=model.Model(*expected,window); preparation_control(m,context['after'],success=failure is None,renewed=failure=='unbound-event')
    if guards and name=='native-open':
        p=only(expected[0]['producer_epochs'],producer_epoch=2); stream=only(expected[0]['source_streams'],owner_epoch=2)
        assert p['state']=='closing' and stream['state']=='closed' and expected[1]['sequence']==125 and len(expected[1]['rows'])==84 and expected[2]=={}
        marker=only(expected[1]['rows'],cf='receipts',key=(b'\x01\x31'+binary(p['producer_slot_id'])+be(2)).hex())
        assert marker['value']==(b'\x01\x31'+be(0)*3).hex()
    if guards and name=='import' and step=='retire-refuse':
        from activated_retirement_import_oracle import named_veto
        named_veto(context['guard_diagnostic'],{k:context['backend']['exact'][0][k] for k in ('pid','backend_start','database')},window)
    native=expected[1]; basis=terminal_basis(expected); rows_retired=jobs=0
    if failure is None:
        assert only(m.pg['producer_epochs'],producer_epoch=2)['reserved_through_request']==basis['issued']
        assert not [r for r in m.pg['producer_checkpoints'] if r['producer_epoch']==2]
        operation=case['operation']
        if operation in ('source-activated','abandon-activated'):
            native=activated_native(native,m.slot,close=True)
            if basis['issued'] and not [r for r in m.pg['producer_outcomes'] if r['producer_epoch']==2]:
                assert name=='missing-copy' and step=='source'
                m.pg=activated_outcome(m.pg,context['after'],basis['receipts'][1],m.control,window)
            terminal_source_delta(m,context['after'],basis)
            if operation=='abandon-activated': jobs=terminal_abandon_delta(m,context['after'],basis)
        else:
            assert operation=='retire-activated'
            rows_retired=terminal_retire_delta(m,context['after'],basis,allow_draining=guards and name=='draining')
        wanted=dict(format='bigbrotr-ingest-recovery/1',state='activated-initial-retired' if operation=='retire-activated' else
            'activated-acquisition-abandoned' if operation=='abandon-activated' else 'activated-source-closed',
            dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch='2',
            issued_through=str(basis['issued']),reserved_through=str(basis['issued']),resolved_requests=str(basis['issued']),
            source_closure=dict(last_complete_sequence=str(basis['cut']),sha256=basis['digest']),
            recovery_basis='closed-producer-floor-v3',compacted_requests='7',native='closed',owner_release='committed',ready=False)
        if operation=='abandon-activated': wanted['acquisition_abandonment']=dict(jobs=str(len([g for g in m.pg['acquisition_gaps'] if g['producer_epoch']==2])),reason='activated_initial_producer_abandoned',coverage='unknown')
        if operation=='retire-activated': wanted['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps=str(len([g for g in m.pg['acquisition_gaps'] if g['producer_epoch']==2])),owner_release='committed',native_acknowledged='0',native_retired='0')
        assert report==wanted
    same(context['after'],m.pg); assert context['native_after']==native
    return dict(status='PASS',case=case['name'],result='REFUSED' if failure else report['state'],
        native_rows=len(native['rows']),native_sequence=native['sequence'],product_native_writes=native['sequence']-expected[1]['sequence'],
        target_issued=basis['issued'],target_acknowledged=0,target_source_cut=basis['cut'],source_closure_sha256=basis['digest'],
        compacted_requests=0 if failure else 7,original_predecessor_requests=0,resolved_target_requests=0 if failure else basis['issued'],
        retired_rows=rows_retired,abandoned_jobs=jobs,tables=77,input_files=len(expected[2]))


def terminal_controls(context,initial,expected,*,faults=False,post_proof=False,guards=False,baseline=None):
    import json
    name,step=terminal_specification(context['case'],faults=faults,post_proof=post_proof,guards=guards)
    changes=[('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('ancestor-checkpoint',lambda c:only(c['after']['producer_checkpoints'],producer_epoch=1).update(checkpoint_digest='\\x'+'00'*32)),
        ('initial-intent-attribution',lambda c:only(c['after']['initial_ingest_successions']).update(prepared_by_epoch=999)),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    def wrong_ack(c):
        row=only(c['native_after']['rows'],cf='receipts',key=(b'\x01\x31'+binary(only(c['after']['initial_ingest_successions'])['producer_slot_id'])+be(2)).hex())
        value=bytes.fromhex(row['value']); assert int.from_bytes(value[10:18],'big')==0
        row['value']=(value[:10]+be(1)+value[18:]).hex()
    changes.append(('invented-target-ACK',wrong_ack))
    if context['inputs_after']: changes.append(('required-input-loss',lambda c:c['inputs_after'].clear()))
    if step=='seed':
        def false_certification(c,kind):
            events=[loads(line) for line in c['response']['stdout'].splitlines()]
            only(events,event='live-terminal-certification',kind=kind)['available']=False
            c['response']['stdout']=''.join(json.dumps(event)+'\n' for event in events)
        changes.extend([('unknown-custody',lambda c:c['custody'].update(status='UNKNOWN')),
            ('wrong-seed-command',lambda c:c['response']['command'].append('unexpected'))])
        if faults and name=='zero-job':
            def zero_execution(c):
                events=[loads(line) for line in c['response']['stdout'].splitlines()]
                only(events,event='live-zero-claim-state')['executions']=1
                c['response']['stdout']=''.join(json.dumps(e)+'\n' for e in events)
            changes.extend([('invented-zero-execution',zero_execution),
                ('wrong-claim-owner',lambda c:only(c['after']['jobs'],request_key='\\x'+'a6'*16).update(owner_epoch=999))])
        else:
            changes.extend([('false-visibility-certification',lambda c:false_certification(c,'visibility')),
                ('false-observation-certification',lambda c:false_certification(c,'observation')),
                ('attribution-original-loss',lambda c:c['attribution'].pop(next(iter(c['attribution'])))),
                ('receive-resolution',lambda c:c['receive_clock'].update(resolution_us=c['receive_clock']['resolution_us']+1))])
    elif faults and step=='wait':
        def event_change(c,event,key,value,stage=None):
            events=[loads(line) for line in c['response']['stdout'].splitlines()]
            fields=dict(event=event)
            if stage is not None: fields['stage']=stage
            only(events,**fields)[key]=value
            c['response']['stdout']=''.join(json.dumps(e)+'\n' for e in events)
        changes.extend([('unknown-custody',lambda c:c['custody'].update(status='UNKNOWN')),
            ('unjoined-readers',lambda c:c['response'].update(readers_joined=False)),
            ('wrong-wait-command',lambda c:c['response']['command'].append('unexpected')),
            ('different-typed-refusal',lambda c:event_change(c,'retirement-expiry-result','error','Control(Backend)' if name!='expiry' else 'Native(AuthorityUnavailable)')),
            ('missing-compacted-proof',lambda c:event_change(c,'retirement-compacted-proof','compacted_requests',0)),
            ('invented-original-proof',lambda c:event_change(c,'retirement-compacted-proof','original_requests_replayed',7)),
            ('holder-cleanup',lambda c:c['schedule']['cleanup_errors'].append(dict(error='unjoined'))),
            ('source-proof-digest',lambda c:event_change(c,'retirement-expiry-proof','sha256','00'*32))])
        if name in ('cancel','expiry'):
            changes.extend([('different-job-lock',lambda c:c['schedule']['holder'].update(statement='SELECT 1')),
                ('missing-blocking-witness',lambda c:c['schedule']['blocks'][-1]['waiter'].update(blockers=[])),
                ('holder-backend-present',lambda c:c['schedule']['holder']['closed'].update(observation='1'))])
        else:
            changes.extend([('mutation-before-proof',lambda c:c['schedule']['mutation'].update(before_us=c['server_window']['before_us'])),
                ('unbound-mutation-statement',lambda c:c['schedule']['mutation'].update(statement='SELECT 1;')),
                ('mutation-extra-table',lambda c:c['schedule']['mutation']['after']['jobs'].clear())])
    else:
        changes.extend([('unreaped-child',lambda c:c['response']['terminal'].update(reaped=False)),
            ('unexpected-argv',lambda c:c['response']['terminal']['command'].append('--successor')),
            ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),
            ('unknown-process',lambda c:c['response'].update(outcome='UNKNOWN'))])
        if terminal_failure(name,step,guards=guards) is None and not (faults and step.endswith('-loss')) and not (post_proof and step=='proof' and not name.endswith('-baseline')):
            def report_value(c,key,value):
                report=loads(c['response']['stdout']); report[key]=value; c['response']['stdout']=json.dumps(report)+'\n'
            changes.extend([('false-original-count',lambda c:report_value(c,'resolved_requests','7')),
                ('missing-compacted-count',lambda c:report_value(c,'compacted_requests','0')),
                ('source-cut',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(last_complete_sequence=999))])
            if context['case']['operation']=='abandon-activated':
                changes.append(('false-abandoned-total',lambda c:report_value(c,'acquisition_abandonment',
                    dict(jobs='0',reason='activated_initial_producer_abandoned',coverage='unknown'))))
            if step.startswith('retire'):
                changes.append(('one-domain-not-retired',lambda c:only(c['after']['allocation_domains'],owner_epoch=2,dictionary_kind=1).update(state='active')))
                changes.append(('retired-row-transaction',lambda c:only(c['after']['producer_epochs'],producer_epoch=2).update(xmin='4294967000')))
            if (name in ('event','unbound-event') or faults or post_proof or (guards and name=='import')) and (step.startswith(('abandon','retire')) or (post_proof and step=='proof' and name.startswith('abandon-'))):
                changes.append(('gap-attribution',lambda c:only(c['after']['acquisition_gaps'],producer_epoch=2).update(previous_owner_epoch=999)))
        else:
            changes.append(('different-typed-refusal',lambda c:c['response'].update(stderr='error: producer operation unavailable: Native(StateMismatch)\n')))
    if guards:
        if name=='import':
            changes.extend([('import-claim-removed',lambda c:c['after']['jobs'].remove(only(c['after']['jobs'],job_id='\\x'+'e6'*16))),
                ('import-claim-reassigned',lambda c:only(c['after']['jobs'],job_id='\\x'+'e6'*16).update(owner_epoch=999)),
                ('import-input-invented',lambda c:only(c['after']['jobs'],job_id='\\x'+'e6'*16).update(input_topology_generation=1))])
            if step=='seed':
                changes.extend([('import-superuser',lambda c:c['import_claim']['identity'].update(superuser=True)),
                    ('import-replica-bypass',lambda c:c['import_claim']['identity'].update(session_replication_role='replica')),
                    ('import-no-commit',lambda c:c['import_claim']['transactions'][-1].update(sqlstate='23514')),
                    ('import-actor-present',lambda c:c['import_claim'].update(actor_absence='1')),
                    ('import-actor-unjoined',lambda c:c['import_claim'].update(actor_exit_code=None)),
                    ('import-probe-unjoined',lambda c:c['response'].update(readers_joined=False)),
                    ('import-before-activation',lambda c:c['import_claim'].update(created_at_us=c['server_window']['before_us'])),
                    ('import-live-tuple',lambda c:c['import_claim']['live_after'].update(pid=1)),
                    ('import-admission-xmin',lambda c:only(c['import_claim']['after']['job_admissions'],job_id='\\x'+'e6'*16).update(xmin='4294967000')),
                    ('import-unbound-SQL',lambda c:c['import_claim']['statements'].__setitem__(0,'BEGIN; SET LOCAL session_replication_role=replica;'))])
            if step=='retire-refuse': changes.append(('import-named-veto-absent',lambda c:c['guard_diagnostic'].update(errors=[])))
        if name=='native-open':
            changes.append(('retirement-invents-closing',lambda c:c.update(native_after=activated_native(c['native_after'],only(c['after']['initial_ingest_successions'])['producer_slot_id'],close=True))))
    if (faults and step.endswith(('-baseline','-loss'))) or (post_proof and step=='proof'):
        changes.extend([('proxy-not-joined',lambda c:c['fixture']['delivery_fault']['wire'].update(readers_joined=False)),
            ('proxy-source',lambda c:c['fixture']['delivery_fault']['wire'].update(source_sha256='00'*32)),
            ('proxy-counter',lambda c:c['fixture']['delivery_fault']['wire']['receipt'].update(commits_seen=63)),
            ('unrestored-config',lambda c:c['fixture']['delivery_fault']['configuration'].update(restored='')),
            ('source-owner-attribution',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(owner_resource_id='\\x'+'ff'*16))])
        if step.endswith('-baseline') or post_proof:
            changes.append(('held-query',lambda c:c['fixture']['delivery_fault']['held']['backend'].update(query='SELECT 1')))
            changes.append(('held-ancestor-head',lambda c:only(c['fixture']['delivery_fault']['held']['control']['producer_checkpoints'],producer_epoch=1).update(checkpoint_digest='\\x'+'00'*32)))
        if step.startswith('abandon') and name.endswith('-after'):
            changes.append(('atomic-gap-job-xmin',lambda c:only(c['after']['jobs'],job_id=only(c['after']['acquisition_gaps'],producer_epoch=2)['job_id']).update(xmin='4294967000')))
        if step.startswith('retire') and name.endswith('-after'):
            changes.append(('atomic-domain-xmin',lambda c:only(c['after']['allocation_domains'],owner_epoch=2,dictionary_kind=1).update(xmin='4294967000')))
    if post_proof and step=='proof':
        changes.extend([('held-backend-in-transaction',lambda c:c['fixture']['delivery_fault']['held']['backend'].update(state='idle in transaction')),
            ('held-transaction-start',lambda c:c['fixture']['delivery_fault']['held']['backend'].update(xact_start='1970-01-01 00:00:00+00'))])
        if not name.endswith('-baseline'):
            changes.extend([('mutation-before-proof-hold',lambda c:c['fixture']['delivery_fault']['mutation'].update(before_us=c['server_window']['before_us'])),
                ('unbound-mutation-statement',lambda c:c['fixture']['delivery_fault']['mutation'].update(statement='SELECT 1;')),
                ('extra-final-gap',lambda c:c['after']['acquisition_gaps'].append(dict(unexpected=True)))])
    rejected=[]
    for label,change in changes:
        c=copy.deepcopy(context); change(c)
        try: verify_terminal_context(c,initial,expected,faults=faults,post_proof=post_proof,guards=guards,baseline=baseline)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration,IndexError): rejected.append(label)
        else: raise AssertionError('accepted altered terminal evidence: '+label)
    return dict(status='PASS',count=len(rejected),rejected=rejected)


TERMINAL_UNBOUND_FIELDS=('relay_endpoint_id','relay_access_scope_id','relay_enrollment_generation','relay_route_id','acquisition_scope_id','acquisition_job_id')


def terminal_mutation_kind(name,step):
    if name=='head': return 'head'
    if name=='missing-copy' and step in ('source','source-closed-missing'): return 'missing-copy'
    if name=='unbound-event' and step=='abandon': return 'unbound-event'
    if name=='mixed' and step=='retire-refuse': return 'mixed'
    return None


def terminal_mutation_statement(kind,expected,initial):
    if kind=='head': return retained.ack_statement(initial)
    pg=expected[0]; intent=only(pg['initial_ingest_successions']); slot=intent['producer_slot_id'][2:]
    start="BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\nSET LOCAL session_replication_role=replica;\n"
    if kind=='missing-copy':
        command="DELETE FROM control.producer_outcomes WHERE producer_slot_id=decode('"+slot+"','hex') AND producer_epoch=2 AND request_sequence=1"
        table='producer_outcomes'
    elif kind=='unbound-event':
        command='UPDATE control.producer_requests SET '+','.join(k+'=NULL' for k in TERMINAL_UNBOUND_FIELDS)+" WHERE producer_slot_id=decode('"+slot+"','hex') AND producer_epoch=2 AND request_sequence=1"
        table='producer_requests'
    else:
        assert kind=='mixed'; source=only(pg['source_streams'],owner_epoch=2); assert source['state']=='closed'
        command="UPDATE control.allocation_domains SET state='closed',closed_at_us="+str(source['closed_at_us'])+",closure_digest=decode('"+source['closure_digest'][2:]+"','hex') WHERE owner_resource_id=decode('"+intent['owner_resource_id'][2:]+"','hex') AND owner_epoch=2 AND dictionary_kind=1"
        table='allocation_domains'
    return start+command+'\nRETURNING (to_jsonb('+table+")||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n"


def terminal_mutation_checked(record,expected,initial,kind):
    assert set(record)=={'kind','before','after','statement','sql_output'}
    assert record['kind']=='isolated-terminal-'+kind and record['before']==dict(control=expected[0],native=expected[1],inputs=expected[2])
    assert record['statement']==terminal_mutation_statement(kind,expected,initial)
    pg,native,inputs=copy.deepcopy(expected); returned=loads(record['sql_output'])
    if kind=='missing-copy':
        row=only(pg['producer_outcomes'],producer_epoch=2,request_sequence=1); assert returned==row; pg['producer_outcomes'].remove(row)
    else:
        model.fresh_xmin(returned['xmin'],pg)
        if kind=='head':
            row=only(pg['producer_checkpoints'],producer_epoch=1); wanted=retained.ack_head(initial); wanted['xmin']=returned['xmin']
            assert returned==wanted; row.clear(); row.update(wanted)
        elif kind=='unbound-event':
            row=only(pg['producer_requests'],producer_epoch=2,request_sequence=1)
            assert all(row[k] is not None for k in TERMINAL_UNBOUND_FIELDS)
            row.update({k:None for k in TERMINAL_UNBOUND_FIELDS},xmin=returned['xmin']); assert returned==row
        else:
            assert kind=='mixed'; row=only(pg['allocation_domains'],owner_epoch=2,dictionary_kind=1); assert row['state']=='active'
            source=only(pg['source_streams'],owner_epoch=2)
            row.update(state='closed',closed_at_us=source['closed_at_us'],closure_digest=source['closure_digest'],xmin=returned['xmin']); assert returned==row
    assert set(record['after'])=={'control','native','inputs'}
    same(record['after']['control'],pg)
    assert (record['after']['native'],record['after']['inputs'])==(native,inputs)
    return pg,native,inputs


def terminal_qualification(component):
    activated_qualification(component)
    prefix='storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_writer_tests::activated_recovery_tests::compacted_activated_tests::terminal_tests::'
    names=('compacted_activated_source_preserves_genuine_digest_event_mask_and_counts',
        'compacted_activated_source_confirmation_and_order_refusals_never_advance',
        'compacted_activated_retirement_observes_complete_closing_without_native_writes',
        'compacted_activated_source_cut_and_retirement_root_are_exact',
        'compacted_activated_source_rechecks_sidecars_sequence_cancel_and_science',
        'compacted_activated_source_preserves_optional_tail_but_refuses_reserved_gaps',
        'compacted_activated_source_ack_handoff_retains_final_input_seal')
    assert {prefix+n for n in names}<=set(component['test_names']['initial_reconstruction'])



def terminal_held_state(expected,pg,window,operation):
    m=model.Model(*expected,window); preparation_control(m,pg,success=False,renewed=True)
    m.control=max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    if operation=='source':
        producer=only(m.pg['producer_epochs'],producer_epoch=2); actual=only(pg['producer_epochs'],producer_epoch=2)
        assert producer['state']=='open'; model.fresh_xmin(actual['xmin'],m.pg); producer.update(state='closing',xmin=actual['xmin'])
    same(pg,m.pg)
    return m


TERMINAL_SOURCE_FINAL_QUERY="UPDATE control.source_streams SET state='closed', closed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint, last_complete_sequence=$3::text::numeric,closure_digest=$4::bytea WHERE dataset_id=$1::bytea AND shard_id=$2::bigint AND owner_epoch=2 AND state='closing'"
TERMINAL_AUTHORITY_QUERY="SELECT lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric"


def terminal_delivery_checked(record,case,window,baseline,expected,after,*,post_proof=False,empty_initial=False):
    import tomllib
    from fault_cases import safe_event
    assert type(empty_initial) is bool and not (empty_initial and post_proof)
    if empty_initial:
        name,step=empty_delivery_specification(case); assert step in ('baseline','loss')
        normal=step=='baseline'; operation='retire-empty'; target,later=3,1
        mode='hold' if normal else 'before-forward' if name=='before' else 'after-complete'
    else:
        name,step=terminal_specification(case,faults=not post_proof,post_proof=post_proof)
        normal=name.endswith('-baseline') if post_proof else step.endswith('-baseline')
        operation=name.split('-')[0] if post_proof else step.split('-')[0]
        target=(TERMINAL_PROOF_SELECTORS if post_proof else TERMINAL_COMMIT_SELECTORS)[operation]; later=4 if post_proof else 3
        mode='hold-after-complete' if post_proof else 'hold' if normal else 'before-forward' if name.endswith('-before') else 'after-complete'
    assert set(record)=={'status','operation','mode','target_commit','wire','held','configuration','errors'}|({'mutation'} if post_proof else set())
    assert record['status']=='JOINED_CONFIG_RESTORED' and record['errors']==[]
    assert (record['operation'],record['mode'],record['target_commit'])==(operation,mode,target)
    settings=record['configuration']; assert set(settings)=={'path','direct','proxied','restored'} and settings['path']==case['config']
    assert settings['direct']==settings['restored']
    direct,proxied=(tomllib.loads(settings[k]) for k in ('direct','proxied'))
    if empty_initial: empty_delivery_config(direct,case)
    else: terminal_network_checked(direct,name,faults=not post_proof,post_proof=post_proof)
    wire=record['wire']; assert set(wire)=={'command','source_sha256','ready','receipt','events','stdout','stderr','exit_code','readers_joined','overflow','forced_wrapper_stop'}
    source=sha(retained.regular_read(Path(__file__).resolve().parents[2]/ACTIVATED_PROXY)); assert wire['source_sha256']==source
    assert wire['readers_joined'] is True and wire['overflow'] is False and wire['forced_wrapper_stop'] is False
    assert type(wire['exit_code']) is int and wire['exit_code']==0 and wire['stderr']==''
    events=[safe_event(loads(line)) for line in wire['stdout'].splitlines()]
    assert events==wire['events'] and [e['elapsed_ns'] for e in events]==sorted(e['elapsed_ns'] for e in events)
    ready,receipt=wire['ready'],wire['receipt']; assert events[0]==ready and events[-1]==receipt
    assert ready['proxy_sha256']==source and ready['listen_host']=='127.0.0.1' and ready['upstream_port']==5432
    number(ready['listen_port'],1,65535,integer=True); number(ready['pid'],1,integer=True)
    assert (ready['max_frame_bytes'],ready['max_wire_bytes'],ready['max_messages'],ready['deadline_seconds'])==(1024**2,64*1024**2,20000,30)
    predicted=copy.deepcopy(direct); assert predicted['postgres']['control']['endpoint']=='127.0.0.1:5432'
    predicted['postgres']['control']['endpoint']='127.0.0.1:'+str(ready['listen_port']); assert proxied==predicted
    profile=EmptyInitialDeliveryProfile if empty_initial else ActivatedSourceProofProfile if post_proof else ActivatedTerminalFaultProfile
    revision=case['database'].removeprefix(profile.DATABASE_PREFIX)[:7]
    assert wire['command']==['docker','exec','-i',profile.CLIENT_PREFIX+revision,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432',
        '--listen-port','0','--mode',mode,'--commit-number',str(target),'--later-commits',str(later),'--deadline-seconds','30']
    counts=lambda e:[e[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]
    assert counts(ready)==[0]*4 and ready['faults']==0 and ready['held'] is False and ready['hold_action'] is None
    for e in events: assert (e['mode'],e['commit_number'])==(mode,target)
    assert receipt['event']=='receipt' and receipt['faults']==1 and receipt['frames'] and receipt['declared_wire_bytes']<=64*1024**2
    wanted=dict(operation=operation,selected_commit=target,total_successful_commits=target+later,boundary='hold-after-complete-released' if post_proof else 'hold-before-forward-released')
    if normal or post_proof:
        if normal: assert baseline is None
        else:
            assert baseline is not None and baseline['status']=='PASS' and baseline['case']==operation+'-baseline-proof' and baseline['wire']==wanted
        assert [e['event'] for e in events]==['ready','held','released','receipt']
        held=record['held']; assert set(held)=={'event','before_us','after_us','release_before_us','release_after_us','control','inputs','backend'}
        assert held['event']==events[1] and counts(events[1])==([target,target,target,target-1] if post_proof else [target,target-1,target-1,target-1])
        assert events[1]['held'] is True and events[1]['hold_action'] is None
        m=terminal_held_state(expected,held['control'],window,operation); assert held['inputs']==expected[2]
        for key in ('before_us','after_us','release_before_us','release_after_us'): model.clock(held[key],window)
        assert held['before_us']<=held['after_us']<=held['release_before_us']<=held['release_after_us']<m.control['lease_expires_at_us']
        b=held['backend']; assert b==dict(pid=m.control['control_backend_pid'],backend_start=m.control['control_backend_start'],database=case['database'],
            user='bigbrotr_control',state='idle' if post_proof else 'idle in transaction',xact_start=b['xact_start'],query=b['query'],observed_at_us=b['observed_at_us'])
        assert (b['xact_start'] is None if post_proof else b['xact_start'] is not None) and held['before_us']<=b['observed_at_us']<=held['after_us']
        assert ' '.join(b['query'].split())==('SELECT control.require_empty_initial_retirement($1::bytea,$2::bytea)' if empty_initial else 'COMMIT' if post_proof else TERMINAL_SOURCE_FINAL_QUERY if operation=='source' else TERMINAL_AUTHORITY_QUERY)
        assert counts(events[2])==counts(events[1]) and events[2]['held'] is False and events[2]['hold_action']=='release'
        assert counts(receipt)==[target+later if normal else target]*4 and receipt['reason'] in ('frontend-eof','backend-eof') and receipt['held'] is False and receipt['hold_action']=='release'
        # The held claim and its exact last refresh survive until later release.
        current=only(after['owner_epochs'],role='control',epoch=m.control['epoch'])
        if normal:
            if empty_initial:
                assert current['lease_expires_at_us']==m.control['lease_expires_at_us']
                producer=only(after['producer_epochs'],producer_epoch=2)
                assert int(m.control['xmin'])<int(producer['xmin'])<int(current['xmin'])
                assert m.control['acquired_at_us']<=producer['retired_at_us']<=held['before_us']
            else: assert current['lease_expires_at_us']>m.control['lease_expires_at_us']
            assert int(current['xmin'])>int(m.control['xmin'])
        else: assert current==m.control
        if post_proof:
            if normal: assert record['mutation'] is None
            else:
                mutation=record['mutation']; same(mutation['before'],m.pg); same(mutation['after'],after)
                assert held['after_us']<=mutation['before_us']<=mutation['after_us']<=held['release_before_us']
        for key in ('resource_id','role_instance_id','process_incarnation','acquired_at_us','control_backend_pid','control_backend_start'):
            assert current[key]==m.control[key]
    else:
        assert baseline is not None and baseline['status']=='PASS' and baseline['case']==('baseline-baseline' if empty_initial else 'baseline-'+operation+'-baseline') and baseline['wire']==wanted
        assert record['held'] is None and [e['event'] for e in events]==['ready','fault','receipt']
        count=[target,target-1,target-1,target-1] if mode=='before-forward' else [target,target,target,target-1]
        assert counts(events[1])==counts(receipt)==count and receipt['reason']==events[1]['reason']==mode
        assert receipt['held'] is False and receipt['hold_action'] is None
    return wanted if normal else dict(operation=operation,selected_commit=target,counts=counts(receipt),boundary='post-proof-refusal' if post_proof else mode)


def verify_terminal_delivery(context,initial,expected,baseline,*,post_proof=False):
    case=context['case']; name,step=terminal_specification(case,faults=not post_proof,post_proof=post_proof)
    normal=name.endswith('-baseline') if post_proof else step.endswith('-baseline')
    assert set(context)==CONTEXT_FIELDS and context['snapshots']==[] and set(context['fixture'])=={'cleanup_errors','delivery_fault'}
    assert context['fixture']['cleanup_errors']==[]
    assert context['inputs_after']==expected[2] and set(context['input_contents'])==set(expected[2])
    for filename,value in context['input_contents'].items():
        data=bytes.fromhex(value); assert expected[2][filename]==dict(bytes=len(data),sha256=sha(data))
    assert len(expected[2])==1 and sum(v['bytes'] for v in expected[2].values())<=32*1024**2
    report=terminal_closed(context['response'],case,faults=not post_proof,post_proof=post_proof)
    backend_checked(context['backend'],case['database'],expected[0],context['after'])
    window=context['server_window']; m=model.Model(*expected,window); preparation_control(m,context['after'],success=normal,renewed=not normal)
    if not normal: m.control=max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    native=expected[1]; basis=terminal_basis(expected); operation=name.split('-')[0] if post_proof else step.split('-')[0]
    assert basis['issued']==1 and basis['cut']==5 and not [r for r in m.pg['producer_checkpoints'] if r['producer_epoch']==2]
    completed=normal or name.endswith('-after'); rows_retired=jobs=0
    if operation=='source':
        native=activated_native(native,m.slot,close=True)
        if completed: terminal_source_delta(m,context['after'],basis,released=normal)
        else:
            producer=only(m.pg['producer_epochs'],producer_epoch=2); actual=only(context['after']['producer_epochs'],producer_epoch=2)
            assert producer['state']=='open'; model.fresh_xmin(actual['xmin'],m.pg); producer.update(state='closing',xmin=actual['xmin'])
    elif operation=='abandon':
        assert only(m.pg['source_streams'],owner_epoch=2)['state']=='closed'
        if completed: jobs=terminal_abandon_delta(m,context['after'],basis,released=normal)
    else:
        assert operation=='retire' and len([g for g in m.pg['acquisition_gaps'] if g['producer_epoch']==2])==1
        if completed: rows_retired=terminal_retire_delta(m,context['after'],basis,released=normal)
    if post_proof and not normal:
        kind='head-drift' if name.endswith('-head') else 'digest-drift'
        m.pg=terminal_drift_checked(context['fixture']['delivery_fault']['mutation'],m.pg,initial,kind,window)
    same(context['after'],m.pg); assert context['native_after']==native
    if normal:
        wanted=dict(format='bigbrotr-ingest-recovery/1',state='activated-initial-retired' if operation=='retire' else 'activated-acquisition-abandoned' if operation=='abandon' else 'activated-source-closed',
            dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch='2',issued_through='1',reserved_through='1',resolved_requests='1',
            source_closure=dict(last_complete_sequence='5',sha256=basis['digest']),recovery_basis='closed-producer-floor-v3',compacted_requests='7',native='closed',owner_release='committed',ready=False)
        if operation=='abandon': wanted['acquisition_abandonment']=dict(jobs='1',reason='activated_initial_producer_abandoned',coverage='unknown')
        if operation=='retire': wanted['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps='1',owner_release='committed',native_acknowledged='0',native_retired='0')
        assert report==wanted
    wire=terminal_delivery_checked(context['fixture']['delivery_fault'],case,window,baseline,expected,context['after'],post_proof=post_proof)
    return dict(status='PASS',case=case['name'],result='COMPLETED' if normal else 'REFUSED' if post_proof else 'COMMIT_UNKNOWN',wire=wire,
        native_rows=len(native['rows']),native_sequence=native['sequence'],product_native_writes=native['sequence']-expected[1]['sequence'],
        target_issued=1,target_acknowledged=0,source_closure_sha256=basis['digest'],retired_rows=rows_retired,abandoned_jobs=jobs,
        target_original_files=1,ancestor_compacted_requests=7,original_predecessor_requests=0,tables=77)



def terminal_zero_claim(take,snap,pg,control,plan,window):
    import activated_source_durable_oracle as durable
    same(pg,snap('zero-claim-before'))
    for phase in ('begin','verify','finish'):
        assert take('live-zero-claim-login',phase=phase)==dict(event='live-zero-claim-login',phase=phase,error=None)
    assert take('live-zero-claim-refresh')==dict(event='live-zero-claim-refresh',error=None,policy=dict(config_revision=1,policy_revision=1,
        config_digest=plan['config_digest'],policy_digest=plan['policy_digest']))
    after_login=snap('zero-claim-refreshed'); accepted=take('live-zero-claim-acceptance'); after_accept=snap('zero-claim-accepted')
    claimed=take('live-zero-claim-claim'); after=snap('zero-claim-queued')
    key='\\x'+'a6'*16; job=only(after['jobs'],request_key=key); command=only(after['management_commands'],actor_id=durable.ACTOR,request_key=key)
    at=command['accepted_at_us']; scope='422b7d8b33d4b55977806fb711e2d082'
    # Construct the expected command envelope for the existing complete SQL
    # reducer. No network evidence or product report is inferred from it.
    expected_command=dict(event='live-durable-job',job=job['job_id'][2:],scope=scope,accepted_at_us=at,deadline_at_us=at+30_000_000,
        control_epoch=control['epoch'],request_key=key[2:],provenance=True,existing_dictionaries=True)
    control=durable.authenticated_job(pg,after,expected_command,control,window,modern=True)
    assert accepted==dict(event='live-zero-claim-acceptance',request_key=key[2:],error=None,commit_started=True,
        receipt=dict(command_key=key[2:],job_id=job['job_id'][2:],accepted_at_us=at,created=True))
    assert claimed==dict(event='live-zero-claim-claim',error=None,commit_started=True,job=dict(id=job['job_id'][2:],scope=scope,state='queued',
        job_revision=2,checkpoint_revision=0,accepted_at_us=at,deadline_at_us=at+30_000_000,
        owner=dict(resource=control['resource_id'][2:],epoch=control['epoch'],incarnation=control['process_incarnation'][2:])))
    expected_accept=copy.deepcopy(after); tx=command['xmin']; accepted_job=only(expected_accept['jobs'],request_key=key)
    accepted_job.update(state='accepted',job_revision=1,owner_resource_id=None,owner_epoch=None,owner_incarnation=None,xmin=tx)
    same(expected_accept,after_accept)
    expected_login=copy.deepcopy(expected_accept)
    for table in ('jobs','job_admissions','acquisition_jobs'):
        expected_login[table]=[r for r in expected_login[table] if r['job_id']!=job['job_id']]
    expected_login['management_commands'].remove(command)
    expected_login['management_audit']=[r for r in expected_login['management_audit'] if not (r['action']=='job.acquisition.accept' and r['subject_id']==key)]
    same(expected_login,after_login)
    assert only(after['producer_epochs'],producer_epoch=2)['reserved_through_request']==0
    for table in ('producer_requests','producer_outcomes','producer_input_bindings','producer_checkpoints','acquisition_gaps'):
        assert not [r for r in after[table] if r['producer_epoch']==2]
    assert take('live-zero-claim-state')==dict(event='live-zero-claim-state',error=None,native_rows=84,native_sequence=125,
        inputs_before=[],inputs_after=[],executions=0,passed=True)
    return after,control


TERMINAL_WAIT_ERRORS={'cancel':'Native(AuthorityUnavailable)','expiry':'Control(Backend)',
    'head-drift':'Progress(InvalidContext)','digest-drift':'Native(Compacted(StateMismatch))','event-mask':'Control(InvalidMetadata)'}


def terminal_digest_rows(pg,initial):
    from installed_sync_oracle import decode
    from bootstrap_oracle import cbor
    request=only(pg['producer_requests'],producer_epoch=1,request_sequence=1)
    outcome=only(pg['producer_outcomes'],producer_epoch=1,request_sequence=1)
    original=only(initial[0]['producer_outcomes'],producer_epoch=1,request_sequence=1)
    assert outcome==original and request==only(initial[0]['producer_requests'],producer_epoch=1,request_sequence=1)
    raw=binary(outcome['outcome_bytes']); prefix=b'bigbrotr/receipt/1\0'; assert raw.startswith(prefix)
    receipt=decode(raw[len(prefix):]); assert prefix+cbor(receipt)==raw
    assert len(receipt)==6 and receipt[0]==1 and receipt[1]==[binary(request['producer_slot_id']),1,1]
    assert receipt[2]==binary(request['request_digest'])==binary(outcome['request_digest'])
    changed=bytes.fromhex('b7'*32); assert changed!=receipt[2]
    receipt[2]=changed; changed_raw=prefix+cbor(receipt)
    return dict(request_digest='\\x'+changed.hex()),dict(request_digest='\\x'+changed.hex(),outcome_bytes='\\x'+changed_raw.hex(),outcome_sha256='\\x'+sha(changed_raw))


def terminal_drift_statement(kind,pg,initial):
    if kind in ('head-drift','event-mask'):
        return terminal_mutation_statement('head' if kind=='head-drift' else 'unbound-event',(pg,None,None),initial)
    assert kind=='digest-drift'; request,outcome=terminal_digest_rows(pg,initial)
    slot=only(pg['initial_ingest_successions'])['producer_slot_id'][2:]
    statement="BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\nSET LOCAL session_replication_role=replica;\n"
    for table,changes in (('producer_requests',request),('producer_outcomes',outcome)):
        statement+='UPDATE control.'+table+' SET '+','.join(k+"=decode('"+v[2:]+"','hex')" for k,v in changes.items())
        statement+=" WHERE producer_slot_id=decode('"+slot+"','hex') AND producer_epoch=1 AND request_sequence=1\n"
        statement+='RETURNING (to_jsonb('+table+")||jsonb_build_object('xmin',xmin::text))::text;\n"
    return statement+'COMMIT;\n'


def terminal_drift_checked(record,pg,initial,kind,window):
    assert set(record)=={'kind','before','after','statement','sql_output','before_us','after_us'} and record['kind']==kind
    same(record['before'],pg); assert record['statement']==terminal_drift_statement(kind,pg,initial)
    before=number(record['before_us'],window['before_us'],window['after_us'],integer=True); after=number(record['after_us'],window['before_us'],window['after_us'],integer=True); assert before<=after
    if kind!='digest-drift':
        synthetic=dict(kind='isolated-terminal-'+('head' if kind=='head-drift' else 'unbound-event'),
            before=dict(control=pg,native=None,inputs=None),after=dict(control=record['after'],native=None,inputs=None),
            statement=record['statement'],sql_output=record['sql_output'])
        return terminal_mutation_checked(synthetic,(pg,None,None),initial,'head' if kind=='head-drift' else 'unbound-event')[0]
    changes=terminal_digest_rows(pg,initial); returned=[loads(line) for line in record['sql_output'].splitlines()]
    assert len(returned)==2 and returned[0]['xmin']==returned[1]['xmin']; tx=returned[0]['xmin']; model.fresh_xmin(tx,pg)
    wanted=copy.deepcopy(pg)
    for table,changed,row in zip(('producer_requests','producer_outcomes'),changes,returned,strict=True):
        expected=only(wanted[table],producer_epoch=1,request_sequence=1); expected.update(changed,xmin=tx); assert row==expected
    same(record['after'],wanted)
    # Independently recompute the entire canonical stream digest. H and the
    # installed floor stay literal; the changed D cannot match the sealed proof.
    slot=binary(only(pg['initial_ingest_successions'])['producer_slot_id'])
    digest=b'bigbrotr/initial-source-closure/1\0'+bytes.fromhex(MANIFEST)+slot+be(1)+be(7)
    for n in range(1,8):
        request=only(wanted['producer_requests'],producer_epoch=1,request_sequence=n)
        outcome=only(wanted['producer_outcomes'],producer_epoch=1,request_sequence=n)
        assert request['request_digest']==outcome['request_digest'] and outcome['outcome_sha256']=='\\x'+sha(binary(outcome['outcome_bytes']))
        digest+=be(n)+binary(request['request_digest'])+binary(outcome['outcome_sha256'])
    assert sha(digest+be(27))!=known(initial)['digest']
    return wanted


def terminal_wait_closed(response,database):
    assert type(response['returncode']) is int and response['returncode']==0 and response['stderr']==''
    assert response['outcome']=='PROCESS_EXITED' and response['readers_joined'] is True and response['overflow'] is False
    events=[loads(line) for line in response['stdout'].splitlines()]
    complete=events[-1]; assert complete['event']=='retirement-expiry-complete' and complete['command'] in set(TERMINAL_WAIT_MODES.values())
    assert complete['native']=='closed' and complete['control']=='dropped-and-joined' and complete['custody_confirmed'] is True
    assert complete['operation_returned'] is True and complete['deadline_unchanged'] is True and complete['invalid_signal'] is False
    for stage in ('control','observer'):
        assert only(events,event='retirement-expiry-driver',stage=stage)==dict(event='retirement-expiry-driver',stage=stage,joined=True,failed=False,aborted=False,error=None)
    assert only(events,event='retirement-expiry-backend-absence')==dict(event='retirement-expiry-backend-absence',error=None)
    present,absent=[e for e in events if e['event']=='successor-backend']
    assert present['database']==database and present['present'] is True and absent==dict(present,present=False)
    return events


def verify_terminal_wait(context,initial,expected):
    import initial_succession_oracle as original
    from retirement_fault_replay import schedule_checked
    case=context['case']; name,step=terminal_specification(case,faults=True); assert step=='wait'
    mode='gate' if name in ('head-drift','digest-drift','event-mask') else name
    command=TERMINAL_WAIT_MODES[name]; prefix='/lab/'+case['name']+'-gate'; error=TERMINAL_WAIT_ERRORS[name]
    fields={'case','before','native_before','inputs_before','after','native_after','inputs_after','response','server_window',
        'catalog_before','catalog_after','backend_absence','backends','custody','arguments','schedule','diagnostic'}
    assert set(context)==fields and context['backends']==[]
    assert context['custody']==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
    absence=context['backend_absence']; assert absence['database']==case['database'] and absence['user']=='bigbrotr_control'
    assert absence['status']=='OBSERVED_ABSENT' and absence['limit_seconds']==5 and absence['attempts']
    for v in absence['attempts']: number(v['seconds'],0,5); assert type(v['count']) is int and v['count']==len(v['backends'])
    assert absence['attempts'][-1]['count']==0 and absence['attempts'][-1]['backends']==[]
    assert context['arguments']==[command,case['database'],'/run/secrets/application',case['config'],prefix]
    revision=case['database'].removeprefix(ActivatedTerminalFaultProfile.DATABASE_PREFIX)[:7]
    assert context['response']['command']==['docker','exec',ActivatedTerminalFaultProfile.CLIENT_PREFIX+revision,'/build/ingest_bootstrap_probe',*context['arguments']]
    number(context['response']['seconds'],0,120)
    pg,native,inputs=copy.deepcopy(expected); basis=terminal_basis(expected)
    assert basis['issued']==1 and basis['cut']==5 and len(inputs)==1
    assert only(pg['source_streams'],owner_epoch=2)['state']=='closed' and only(pg['producer_epochs'],producer_epoch=2)['state']=='closing'
    assert only(pg['acquisition_gaps'],producer_epoch=2)['job_id']==only(pg['jobs'],request_key='\\x'+'a6'*16)['job_id']
    window=context['server_window']; events=terminal_wait_closed(context['response'],case['database']); position=0
    def take(event,stage=None,phase=None):
        nonlocal position
        value=events[position]; position+=1; assert (value['event'],value.get('stage'),value.get('phase'))==(event,stage,phase); return value
    def snap(stage): return history_snapshot(take('successor-state','retirement-expiry-'+stage),native,inputs,pg,stage='retirement-expiry-'+stage)
    def clock(stage):
        v=take('retirement-compacted-clock',stage); at=number(v['server_us'],window['before_us'],window['after_us'],integer=True)
        assert v==dict(event='retirement-compacted-clock',command=command,mode=mode,stage=stage,server_us=at,
            lease_expires_at_us=control['lease_expires_at_us'],remaining_us=control['lease_expires_at_us']-at,error=None)
        return v
    assert take('retirement-compacted-mode')==dict(event='retirement-compacted-mode',command=command,mode=mode,metadata_bytes=132096,original_predecessor_replay=False,native_writes=False,refresh_after_proof=False)
    assert take('successor-control','retirement-expiry','claim-planned')==dict(event='successor-control',stage='retirement-expiry',phase='claim-planned',role='d5'*16,incarnation='df'*16,lease_ttl_us='20000000')
    same(pg,snap('before-claim')); claimed=take('successor-control','retirement-expiry','claimed'); after=snap('after-claim')
    control=original.claim(pg,after,3); model.clock(control['acquired_at_us'],window); pg=after
    assert claimed==dict(event='successor-control',stage='retirement-expiry',phase='claimed',resource=control['resource_id'][2:],epoch=control['epoch'],role='d5'*16,incarnation='df'*16)
    backend=take('successor-backend','retirement-expiry')
    assert backend==dict(event='successor-backend',stage='retirement-expiry',database=case['database'],pid=control['control_backend_pid'],backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
    for stage in ('loaded','producer-observed'): same(pg,snap(stage))
    assert take('retirement-compacted-predecessor')==dict(event='retirement-compacted-predecessor',command=command,mode=mode,compacted_requests=7,original_requests_replayed=0,needs_compacted_initial=False)
    same(pg,snap('compacted-predecessor'))
    after=snap('refreshed-1'); model.fresh_xmin(only(after['owner_epochs'],role='control',epoch=control['epoch'])['xmin'],pg)
    control=original.renewed(pg,after,control,window['after_us']); pg=after
    assert take('retirement-expiry-request')==dict(event='retirement-expiry-request',sequence=1,outcome='durable',receipt=basis['receipts'][1].hex(),rejection=None)
    same(pg,snap('resolved-1'))
    after=snap('final-refreshed'); model.fresh_xmin(only(after['owner_epochs'],role='control',epoch=control['epoch'])['xmin'],pg)
    old_deadline=control['lease_expires_at_us']; control=original.renewed(pg,after,control,window['after_us']); assert control['lease_expires_at_us']>old_deadline; pg=after
    assert take('retirement-expiry-proof')==dict(event='retirement-expiry-proof',resolved_requests=1,last_complete_sequence=5,sha256=basis['digest'])
    assert take('retirement-compacted-proof')==dict(event='retirement-compacted-proof',command=command,mode=mode,compacted_requests=7,original_requests_replayed=1,last_complete_sequence=5,sha256=basis['digest'])
    proof=clock('proof'); assert control['lease_expires_at_us']-20_000_000<=proof['server_us']<control['lease_expires_at_us']
    gates=[]
    for phase in ('waiting','released'):
        gate=take('retirement-expiry-gate',phase=phase)
        assert gate==dict(event='retirement-expiry-gate',command=command,mode=mode,gate='before-retirement',phase=phase,prefix=prefix,
            go=prefix+'.go',cancel=prefix+'.cancel',gate_timeout_ms=10000,poll_ms=5); gates.append(gate)
    dispatch=clock('dispatch'); assert proof['server_us']<=dispatch['server_us']<control['lease_expires_at_us']
    cancelled=None
    if mode=='cancel':
        cancelled=take('retirement-compacted-cancel',phase='applied')
        assert cancelled==dict(event='retirement-compacted-cancel',command=command,mode=mode,phase='applied',cancelled=True)
    assert take('retirement-expiry-result','retirement')==dict(event='retirement-expiry-result',command=command,mode=mode,stage='retirement',sequence=None,gaps=None,error=error)
    ordinary_clock=take('retirement-expiry-clock','returned'); returned=clock('returned')
    assert ordinary_clock=={k:v for k,v in dict(returned,event='retirement-expiry-clock').items() if k not in ('command','mode')}
    assert dispatch['server_us']<=returned['server_us']
    schedule=context['schedule']
    if mode=='gate':
        assert set(schedule)=={'mode','status','gates','blocks','confirmed_blocks','expiry_observations','cleanup_errors','holder',
            'control_backend','control','refreshed','before_holder','held','go_before_us','dispatch','mutation'}
        assert schedule['mode']=='gate' and schedule['status']=='CAPTURED' and schedule['holder'] is None
        assert all(schedule[k]==[] for k in ('blocks','confirmed_blocks','expiry_observations','cleanup_errors'))
        assert schedule['control_backend']==backend and schedule['control']==control and schedule['refreshed']==proof and schedule['dispatch']==dispatch
        same(schedule['before_holder'],pg)
        pg=terminal_drift_checked(schedule['mutation'],pg,initial,name,window); same(schedule['held'],pg)
        assert len(schedule['gates'])==2
        observed=[]
        for supplied,event in zip(schedule['gates'],gates,strict=True):
            assert supplied==dict(event=event,observed_us=supplied['observed_us']); observed.append(number(supplied['observed_us'],window['before_us'],window['after_us'],integer=True))
        go=number(schedule['go_before_us'],window['before_us'],window['after_us'],integer=True)
        assert proof['server_us']<=observed[0]<=schedule['mutation']['before_us']<=schedule['mutation']['after_us']<=go<=observed[1]
        assert go<=dispatch['server_us'] and observed[1]-observed[0]<10_000_000 and returned['server_us']<control['lease_expires_at_us']
        assert context['diagnostic'] is None
    else:
        schedule_checked(schedule,pg,control,backend,case,mode,proof,gates,dispatch,returned,cancelled,context['diagnostic'],window,compacted=True)
    same(pg,snap('after-retirement'))
    assert take('retirement-expiry-reuse')==dict(event='retirement-expiry-reuse',available=False,error='Unavailable')
    for stage in ('after-reuse','final-before-control-end'): same(pg,snap(stage))
    assert take('retirement-expiry-driver','control')==dict(event='retirement-expiry-driver',stage='control',joined=True,failed=False,aborted=False,error=None)
    assert take('successor-control','retirement-expiry','dropped-and-joined')==dict(event='successor-control',stage='retirement-expiry',phase='dropped-and-joined',resource=control['resource_id'][2:],epoch=control['epoch'])
    assert take('retirement-expiry-backend-absence')==dict(event='retirement-expiry-backend-absence',error=None)
    assert take('successor-backend','retirement-expiry')==dict(backend,present=False)
    for stage in ('final-after-control-end','final'): same(pg,snap(stage))
    assert take('retirement-expiry-driver','observer')==dict(event='retirement-expiry-driver',stage='observer',joined=True,failed=False,aborted=False,error=None)
    assert take('retirement-expiry-complete')==dict(event='retirement-expiry-complete',command=command,mode=mode,native='closed',control='dropped-and-joined',admission=False,
        expected_refusal=None if mode=='gate' else True,custody_confirmed=True,operation_returned=True,cancelled=mode=='cancel',deadline_unchanged=True,invalid_signal=False)
    assert position==len(events)
    same(context['after'],pg); assert (context['native_after'],context['inputs_after'])==(native,inputs)
    return dict(status='PASS',case=case['name'],result='REFUSED',error=error,compacted_requests=7,original_predecessor_requests=0,
        resolved_target_requests=1,target_source_cut=5,source_closure_sha256=basis['digest'],native_rows=98,native_sequence=142,
        product_native_writes=0,tables=77,input_files=1,failed_control_epoch=control['epoch'],boundary=name)


# This profile copies qualified prepared endpoints. It does not create a floor,
# reclaim originals, or treat an empty target as seven original requests.
EMPTY_PREPARATION_PRODUCT='a0e914e8b58650d94dec60c0b5d3c08a02e5e248'
EMPTY_PREPARATION_REPLAY_SHA='0c8e801a6032aebccc76527cd64168db8ae119e1d0ee5014f3e9438fead20442'
EMPTY_PREPARED_ORIGINS=tuple(dict(
    name='empty-'+kind,run='installed-initial-preparation-30d6554',subdirectory='installed-initial-preparation',
    database='bb_initial_preparation_30d6554_'+kind+'_prepared',status=InitialPreparationProfile.STATUS,
    source='30d6554015fdf09fe300db13cc61cae1fb3fdc40',owner='33c0567eb260e70243593e15f29af110',label=InitialPreparationProfile.LABEL,
    client_id='2e048a7a0f52f38812913aa2ecea42ffd906aa0624ce464364256c6bb2799263',source_clone=kind+'-prepared',
    final_case=kind+'-prepared-'+step,catalog_sha256='3c97ef7d8b0c915115f8d285387f0cf1bc08a852ce100e572f7d4b8de176b73c',
    result_sha256='f084a0909dd46fd06a11852bb97f5dece976f255d3120ea7b293eaf5caa42169',
    snapshot=kind+'-prepared-final-sql.json',snapshot_sha256=sql_sha,
    native_file=kind+'-prepared-'+step+'-after-native.json',native_sha256=native_sha,
    inputs_file=kind+'-prepared-'+step+'-after-inputs.json',inputs_sha256=inputs_sha,
    verification_sha256=context_sha,store=kind+'-prepared/store') for kind,step,sql_sha,native_sha,inputs_sha,context_sha in (
    ('compacted','retry','ecddc2f3a5db5163752aff2afe2865bb714a49dddc0f48f92dd409d9719a03bb',
     'c1582e3425b4b1c8b6fa100919e3f6527993de2f004ed536a2d47a9217c52b70',
     'ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356',
     '107a23dacb8a3f151232bac5c53a2fdedaec5844a3aff7d5763c9944f108ef85'),
    ('original','missing-original','c174b91b4a24a546ece3fac6df364c1498a4b547ec4dfc0c4df4b3b670fea5f0',
     '27e11abc7893b07bafcd75d2ceb5d0191037bec36bb27defd1b456e1cb0a4bde',
     '12fa6559369df9c3ea39ec0c6ae4f5fd6f26bc9a4007ad9bba68ab26948f0036',
     '226142614fdfcebeb4ae0536c4e695a0a7e5238a0c4cdaf2edb92b5a62576223')))
EMPTY_ORIGINS=(*EMPTY_PREPARED_ORIGINS,PREPARATION_RETAINED_ORIGINS[1],ACTIVATED_LIVE_ORIGIN)
EMPTY_STEPS={'compacted':('retire','retry'),'original':('retire','retry'),
    'native-first':('failure','complete','retry'),'head':('refuse',),'digest':('refuse',),
    'target-input':('refuse',),'activated-zero':('refuse',),'target-nonempty':('refuse',),'original-missing':('refuse',)}
EMPTY_ORIGIN_NAMES={n:'empty-original' if n.startswith('original') else 'activated-initial' if n=='activated-zero'
    else ACTIVATED_LIVE_ORIGIN['name'] if n=='target-nonempty' else 'empty-compacted' for n in EMPTY_STEPS}
EMPTY_GRANTS=JOURNEY_GRANTS+('GRANT EXECUTE ON FUNCTION '+', '.join('control.'+n for n in (
    'initial_empty_retirement_digest(control.initial_ingest_successions)','lock_initial_empty_retirement(bytea,bytea)',
    'require_initial_empty_claims(control.initial_ingest_successions)','require_empty_initial_retirement(bytea,bytea)',
    'require_initial_empty_transition(bytea,bytea,bigint)','require_complete_initial_empty_retirement()'))+' TO bigbrotr_control;',)


class EmptyInitialProfile:
    DIRECTORY='installed-compacted-empty-initial'
    LABEL='org.bigbrotr.installed-compacted-empty-initial'
    STATUS='PASS_INSTALLED_COMPACTED_EMPTY_INITIAL_RETIREMENT'
    DATABASE_PREFIX='bb_compacted_empty_'
    CLIENT_PREFIX='bigbrotr-compacted-empty-'
    CLONES=tuple(EMPTY_STEPS)
    STEPS=EMPTY_STEPS
    CASES=tuple(n+'-'+s for n in EMPTY_STEPS for s in EMPTY_STEPS[n])
    GRANTS=EMPTY_GRANTS
    NOT_RUN=('Actual product reclamation/GC, floor publication or unlink',
        'Final COMMIT wire baselines and before/after delivery loss',
        'Real preparing/current-Control import/acquisition claim vetoes and lock waits, cancellation and UTC expiry',
        'Proof-to-use root/sequence/input/H/D races and optional residual original census',
        'SQL-terminal/native-OPEN refusal, malformed floor and partial target SQL/native shapes',
        'Native write-unknown, driver/syscall faults, physical limits, mixed restore and capacity',
        'Later recovery, live frontier, Archive and later empty-attempt retirement')


def empty_origins_checked(root,raw=None,*,delivery=False):
    # The existing history owner registers the independent bootstrap codec used
    # by the reused empty oracle; importing it performs no engine operation.
    import history
    assert history.bootstrap_oracle.__file__==str(Path(__file__).resolve().parents[1]/'ingest-bootstrap/oracle.py')
    raw=raw or retained.regular_read
    results={}; initial=None
    assert type(delivery) is bool
    for pin in EMPTY_PREPARED_ORIGINS[:1] if delivery else EMPTY_ORIGINS:
        kwargs=dict(empty_origin=pin) if pin in EMPTY_PREPARED_ORIGINS else dict(activated_origin=pin) if pin==ACTIVATED_LIVE_ORIGIN else dict(prepared_origin=pin)
        endpoint,basis=qualified_endpoint(root,raw,initial_journey=False,**kwargs)
        if initial is None: initial=basis
        else: assert initial==basis
        results[pin['name']]=endpoint
    return results,initial


def empty_specification(case,*,delivery=False,locks=False):
    assert type(locks) is bool and not (locks and delivery)
    if locks: return empty_lock_specification(case)
    if delivery: return empty_delivery_specification(case)
    name=case['clone']; assert name in EMPTY_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in EMPTY_STEPS[name] and case['name']==name+'-'+step
    index=EMPTY_STEPS[name].index(step)
    assert case['origin']==EMPTY_ORIGIN_NAMES[name] and case['predecessor']==(name+'-'+EMPTY_STEPS[name][index-1] if index else None)
    assert case['operation']=='retire-empty' and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,step


# Frozen from the consumer's exact source before runtime; no fallback errors.
EMPTY_ERRORS={
    'failure':'producer operation unavailable: Control(Backend)',
    'head':'producer operation unavailable: Progress(InvalidContext)',
    'digest':'producer operation unavailable: Native(Compacted(StateMismatch))',
    'target-input':'initial dictionary batch failed: RequestInput(Identity)',
    'activated-zero':'producer operation unavailable: Control(Backend)',
    'target-nonempty':'initial dictionary batch failed: StateMismatch',
    'original-missing':'initial dictionary batch failed: RequestInput(Missing)',
}


def empty_closed(response,case,*,delivery=False,locks=False):
    name,step=empty_specification(case,delivery=delivery,locks=locks); failure=empty_lock_failure(name,step) if locks else step=='loss' if delivery else step in ('failure','refuse'); code=4 if failure else 0
    assert set(response)=={'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==code
    t=response['terminal']; assert set(t)=={'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config']==case['config'] and t['prefix']=='/lab/'+case['name'] and t['command']==argv('retire-empty',Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code']==code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failure:
        error=('initial Ingest cancelled; original attempt may need recovery' if name=='cancel' else 'producer operation unavailable: Control(Unavailable)' if name=='expiry' else 'producer operation unavailable: Control(Backend)') if locks else 'producer operation unavailable: CommitUnknown' if delivery else EMPTY_ERRORS['failure' if step=='failure' else name]
        assert response['stdout']=='' and response['stderr']=='error: '+error+'\n'
        return None
    assert response['stderr']==''; report,=[loads(v) for v in response['stdout'].splitlines()]; return report


def empty_basis(pg,initial):
    intent,handoff=journey_plan_from_intent(pg,initial)
    digest=sha(b'bigbrotr/initial-empty-attempt-closure/1\0'+bytes.fromhex(sha(handoff))+
        bytes.fromhex(sha(binary(intent['target_manifest'])))+binary(intent['producer_slot_id'])+be(2)+be(0)+be(0))
    assert digest!=known(initial)['digest']
    return intent,digest


def empty_restore_checked(record,expected,initial):
    assert set(record)=={'kind','source','name','hex','before','after','physical_before','physical_after'}
    assert record['kind']=='restore-retained-original-on-isolated-clone'
    pin=EMPTY_PREPARED_ORIGINS[1]; filename=only(initial[0]['producer_epochs'])['producer_slot_id'][2:]+'-0000000000000001-0000000000000003.cbor'
    assert record['source']==pin['run']+'/'+pin['subdirectory']+'/original-prepared/quarantined-required/'+filename
    assert record['name']==filename and record['before']==dict(control=expected[0],native=expected[1],inputs=expected[2])
    data=bytes.fromhex(record['hex']); assert dict(bytes=len(data),sha256=sha(data))==initial[2][filename]
    assert dict(expected[2],**{filename:initial[2][filename]})==initial[2]
    before,after=record['physical_before'],record['physical_after']; assert filename not in before and set(after)==set(before)|{filename}
    for name in before:
        if name!='.': assert after[name]==before[name]
    assert {k:after['.'][k] for k in ('mode','inode','device','links')}=={k:before['.'][k] for k in ('mode','inode','device','links')}
    v=after[filename]; assert stat.S_ISREG(v['mode']) and stat.S_IMODE(v['mode'])==0o600 and v['links']==1
    assert dict(bytes=v['bytes'],sha256=v['sha256'])==initial[2][filename]
    result=expected[0],expected[1],initial[2]
    assert record['after']==dict(control=result[0],native=result[1],inputs=result[2])
    return result


def empty_fault_checked(value,*,removed=False):
    from installed_empty_oracle import FAULT_INSTALL,FAULT_REMOVE
    from initial_empty_locks_oracle import guards
    assert set(value)=={'statement','hit','guards'}
    assert value['statement']==(FAULT_REMOVE if removed else FAULT_INSTALL)
    assert value['hit']==('1true' if removed else None)
    guards(value['guards'])


def verify_empty_context(context,initial,expected,*,delivery=False,baseline=None,locks=False):
    name,step=empty_specification(context['case'],delivery=delivery,locks=locks); failure=empty_lock_failure(name,step) if locks else step=='loss' if delivery else step in ('failure','refuse'); compacted=not name.startswith('original')
    wire=(delivery and step in ('baseline','loss')) or (locks and step=='attempt')
    assert set(context)==CONTEXT_FIELDS|({'empty_fault'} if name=='native-first' else {'empty_configuration'}|({'empty_claim_guard'} if name=='claim-commit' else set()) if locks else set())
    if locks:
        import tomllib
        empty_lock_config(tomllib.loads(context['empty_configuration']),context['case'])
    assert context['case']['status']=='RUNNING' and context['case']['custody']=='OBSERVED_CLOSED_AND_JOINED'
    same(context['before'],expected[0]); assert (context['native_before'],context['inputs_before'])==expected[1:]
    assert context['catalog_before']==context['catalog_after'] and context['snapshots']==[]
    assert set(context['fixture'])==({'cleanup_errors','delivery_fault'} if wire else {'cleanup_errors'}) and context['fixture']['cleanup_errors']==[]
    window=context['server_window']; assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    assert context['inputs_after']==expected[2] and set(context['input_contents'])==set(expected[2])
    for filename,value in context['input_contents'].items():
        data=bytes.fromhex(value); assert expected[2][filename]==dict(bytes=len(data),sha256=sha(data))
    assert len(expected[2])<=64 and sum(v['bytes'] for v in expected[2].values())<=32*1024**2
    report=empty_closed(context['response'],context['case'],delivery=delivery,locks=locks)
    m=model.Model(*expected,window,initial=False)
    if name in ('target-input','target-nonempty','original-missing'):
        preparation_no_claim(context['backend'],context['case']['database']); same(context['after'],expected[0])
    else:
        backend_checked(context['backend'],context['case']['database'],expected[0],context['after'])
        preparation_control(m,context['after'],success=not failure,renewed=step=='failure' or (delivery and failure) or (locks and step=='attempt'),lease_us=8_000_000 if locks and name=='expiry' else 6_000_000)
        if locks and name=='claim-commit':
            current=max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            empty_lock_guard_checked(context['empty_claim_guard'],current,context['case']['database'],window)
    if locks and step=='attempt':
        claimed=empty_lock_schedule_checked(context['fixture']['delivery_fault'],context['case'],window,expected,context['after'])
        if claimed is not None: only(m.pg['jobs'],job_id=claimed['job_id']).update(claimed)
        if name=='cancel':assert context['fixture']['delivery_fault']['schedule']['signal']['terminal']==context['response']['terminal']
    intent,empty_digest=empty_basis(expected[0],initial)
    native=expected[1]; changed=False; at=tx=None
    if delivery or not failure or step=='failure': native=activated_native(native,intent['producer_slot_id'],close=True)
    completed=not failure or (delivery and name=='after')
    if completed:
        producer=only(expected[0]['producer_epochs'],producer_epoch=2)
        changed=producer['state']=='open'
        if changed:
            from initial_empty_oracle import terminal
            at,tx=terminal(m.pg,context['after'],dict(empty_digest=empty_digest),window)
            # terminal() compares the complete independently projected 77-table state.
            control=only(m.pg['owner_epochs'],role='control',epoch=max(r['epoch'] for r in m.pg['owner_epochs'] if r['role']=='control'))
            if failure:
                assert control['acquired_at_us']<=at<control['lease_expires_at_us']
                assert control['state']=='active' and int(tx)>int(control['xmin'])
            else: assert control['acquired_at_us']<=at<=control['released_at_us'] and tx!=control['xmin']
        else:
            assert producer['state']=='retired'; same(m.pg,context['after'])
            at,tx=producer['retired_at_us'],producer['xmin']
    else: same(m.pg,context['after'])
    if not failure:
        count='0' if compacted else '7'
        wanted=dict(format='bigbrotr-ingest-recovery/1',state='empty-attempt-retired',dataset_id=intent['dataset_id'][2:],
            owner_resource_id=intent['owner_resource_id'][2:],producer_slot_id=intent['producer_slot_id'][2:],producer_epoch='2',
            issued_through='0',reserved_through='0',resolved_original_requests=count,
            original_source_closure=dict(producer_epoch='1',issued_through='7',resolved_requests=count,last_complete_sequence='27',sha256=known(initial)['digest']),
            empty_attempt_retirement=dict(owner_epoch='2',incarnation=intent['target_incarnation'][2:],retired_at_us=str(at),sha256=empty_digest,
                allocation_domains='9',native_acknowledged='0',native_retired='0',owner_release='committed'),
            native='closed',owner_release='committed',ready=False)
        if compacted: wanted.update(compacted_requests='7',recovery_basis='closed-producer-floor-v3')
        assert report==wanted and type(report['ready']) is bool
    assert context['native_after']==native
    if name=='native-first':
        f=context['empty_fault']; empty_fault_guard_delta(f)
        assert context['catalog_before']==f['catalog_installed' if step=='failure' else 'catalog_original']
        assert f['hit']=='1true'
        if step=='failure': assert f['removed'] is None
        else: empty_fault_checked(f['removed'],removed=True)
    result=dict(status='PASS',case=context['case']['name'],result='COMMIT_UNKNOWN' if delivery and failure else 'REFUSED' if failure else 'EMPTY_ATTEMPT_RETIRED',
        tables=77,native_rows=len(native['rows']),native_sequence=native['sequence'],product_native_writes=native['sequence']-expected[1]['sequence'],
        target_rows_changed=13 if changed else 0,retired_at_us=at,retirement_xmin=tx,
        compacted_requests=7 if compacted and not failure else 0,resolved_original_requests=7 if not compacted and not failure else 0,
        original_source_digest=known(initial)['digest'],empty_digest=empty_digest,input_files=len(expected[2]),ready=False)
    if wire and not locks: result['wire']=terminal_delivery_checked(context['fixture']['delivery_fault'],context['case'],window,baseline,expected,context['after'],empty_initial=True)
    if locks: result['claim_or_wait']=name if step=='attempt' else 'fresh-'+step
    return result


def empty_controls(context,initial,expected,*,delivery=False,baseline=None,locks=False):
    name,step=empty_specification(context['case'],delivery=delivery,locks=locks); verify_empty_context(context,initial,expected,delivery=delivery,baseline=baseline,locks=locks); rejected=[]
    def check(label,change):
        value=copy.deepcopy(context); change(value)
        try: verify_empty_context(value,initial,expected,delivery=delivery,baseline=baseline,locks=locks)
        except (AssertionError,ValueError,KeyError): rejected.append(label)
        else: raise AssertionError('empty oracle accepted '+label)
    def pg(c,table,filters,**changes): only(c['after'][table],**filters).update(changes)
    check('ancestor-xmin',lambda c:pg(c,'producer_epochs',dict(producer_epoch=1),xmin='4294967294'))
    check('immutable-intent',lambda c:pg(c,'initial_ingest_successions',{},prepared_by_epoch=999))
    check('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1))
    check('native-value',lambda c:c['native_after']['rows'][0].update(value='00'))
    check('inputs-changed',lambda c:c['inputs_after'].update({'extra.cbor':dict(bytes=1,sha256='00'*32)}))
    check('unknown-custody',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN'))
    check('forced-child',lambda c:c['response']['terminal'].update(forced=True))
    check('unreaped-child',lambda c:c['response']['terminal'].update(reaped=False))
    check('wrong-command',lambda c:c['response']['terminal']['command'].append('--retire-initial'))
    if context['backend']['exact']: check('backend-remains',lambda c:c['backend']['exact'][0].update(present=True))
    else: check('invented-backend',lambda c:c['backend']['exact'].append(dict(pid=123)))
    check('changed-acl',lambda c:c.update(catalog_after=c['catalog_after']+'\n-- unexpected\n'))
    control=max((r for r in context['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    check('control-expiry',lambda c:pg(c,'owner_epochs',dict(role='control',epoch=control['epoch']),lease_expires_at_us=(1<<63)-1))
    check('unrelated-error',lambda c:c['response'].update(stderr='error: unrelated\n'))
    if (not empty_lock_failure(name,step) if locks else step!='loss' if delivery else step not in ('failure','refuse')):
        def report(c,section,key,value):
            r=loads(c['response']['stdout']); (r if section is None else r[section])[key]=value
            c['response']['stdout']=model.encoded(r).decode()+'\n'
        check('original-count',lambda c:report(c,None,'resolved_original_requests','7' if not name.startswith('original') else '0'))
        check('nested-original-count',lambda c:report(c,'original_source_closure','resolved_requests','7' if not name.startswith('original') else '0'))
        check('empty-versus-ancestor-digest',lambda c:report(c,'empty_attempt_retirement','sha256',known(initial)['digest']))
        check('report-admission',lambda c:report(c,None,'ready',True))
        check('report-bool-type',lambda c:report(c,None,'ready',0))
        check('target-source-xmin',lambda c:pg(c,'source_streams',dict(owner_epoch=2),xmin='4294967294'))
        check('target-domain-time',lambda c:pg(c,'allocation_domains',dict(owner_epoch=2,dictionary_kind=1),closed_at_us=0))
        check('target-owner-release',lambda c:pg(c,'owner_epochs',dict(role='ingest',epoch=2),state='active'))
        if not name.startswith('original'): check('compacted-count',lambda c:report(c,None,'compacted_requests','0'))
    else:
        check('false-success-report',lambda c:c['response'].update(stdout='{}\n'))
        if name not in ('target-input','target-nonempty','original-missing'):
            check('invented-control-release',lambda c:pg(c,'owner_epochs',dict(role='control',epoch=control['epoch']),state='released'))
    if name=='native-first':
        check('fault-not-reached',lambda c:c['empty_fault'].update(hit='1false'))
        def changed_fault_catalog(c):
            f=c['empty_fault']; f['catalog_installed']+='\nGRANT DELETE ON TABLE control.jobs TO bigbrotr_control;\n'
            if step=='failure': c.update(catalog_before=f['catalog_installed'],catalog_after=f['catalog_installed'])
        check('coordinated-unrelated-fixture-acl',changed_fault_catalog)
        if step=='failure': check('native-close-missing',lambda c:c.update(native_after=copy.deepcopy(c['native_before'])))
    if delivery and step in ('baseline','loss'):
        empty_delivery_controls(check,context,initial,expected,baseline)
    if locks: empty_lock_controls(check,context,initial,expected)
    return dict(status='PASS',rejected=rejected,count=len(rejected))


def empty_digest_mutation_checked(record,expected,initial):
    assert set(record)=={'before','after','mutation'}
    assert record['before']==dict(control=expected[0],native=expected[1],inputs=expected[2])
    mutation=record['mutation']; window={k:mutation[k] for k in ('before_us','after_us')}
    pg=terminal_drift_checked(mutation,expected[0],initial,'digest-drift',window)
    assert set(record['after'])=={'control','native','inputs'}; same(record['after']['control'],pg)
    assert (record['after']['native'],record['after']['inputs'])==expected[1:]
    return pg,expected[1],expected[2]


def empty_fault_guard_delta(value):
    from initial_empty_locks_oracle import guards
    assert set(value)=={'installed','removed','hit','guards_original','catalog_original','catalog_installed','catalog_restored'}
    empty_fault_checked(value['installed']); guards(value['guards_original'])
    expected=copy.deepcopy(value['guards_original'])
    assert not any(t['name'].startswith('fixture_') for t in expected['triggers'])
    expected['triggers'].append(dict(table='source_streams',name='fixture_empty_source_rollback',enabled='O',internal=False,
        function='empty_retirement_fixture.reject_source()'))
    expected['triggers'].sort(key=lambda t:(t['table'],t['name']))
    assert value['installed']['guards']==expected
    # pg_dump --schema=control includes the trigger but not its separately
    # owned fixture schema/function/sequence. Preserve every other definition
    # and ACL line; only comments and blank presentation are ignored.
    lines=lambda text:[line for line in text.splitlines() if line.strip() and not line.startswith('--')]
    before,after=lines(value['catalog_original']),lines(value['catalog_installed'])
    trigger="CREATE TRIGGER fixture_empty_source_rollback BEFORE UPDATE ON control.source_streams FOR EACH ROW WHEN ((((new.owner_epoch)::numeric = (2)::numeric) AND (old.state = 'open'::text) AND (new.state = 'closing'::text))) EXECUTE FUNCTION empty_retirement_fixture.reject_source();"
    assert trigger not in before and after.count(trigger)==1
    assert [line for line in after if line!=trigger]==before
    if value['removed'] is not None:
        empty_fault_checked(value['removed'],removed=True)
        assert value['removed']['guards']==value['guards_original'] and value['catalog_restored']==value['catalog_original']
    else: assert value['catalog_restored'] is None


def empty_qualification(component):
    preparation_qualification(component)
    prefix='storage::rocks::store::admission_tests::initial_reconstruction_tests::initial_gap_tests::compacted_handoff_tests::compacted_empty_tests::'
    assert {prefix+n for n in (
        'compacted_empty_exact_one_write_preserves_science_and_fresh_closing_retry',
        'compacted_empty_requires_exact_canonical_origin_and_empty_two_entry_shape',
        'compacted_empty_rejects_foreign_root_and_changed_census_without_writes',
        'compacted_empty_full_effect_census_and_zero_target_are_required',
        'compacted_empty_grant_and_delivery_failure_preserve_exact_close_retry',
        'compacted_empty_original_cancellation_cannot_be_replaced_by_a_fresh_flag')} <= set(component['test_names']['initial_reconstruction'])


EMPTY_DELIVERY_STEPS={'baseline':('baseline',),'before':('loss','retry'),'after':('loss','retry')}


class EmptyInitialDeliveryProfile:
    DIRECTORY='installed-compacted-empty-initial-faults'
    LABEL='org.bigbrotr.installed-compacted-empty-initial-faults'
    STATUS='PASS_INSTALLED_COMPACTED_EMPTY_INITIAL_DELIVERY'
    DATABASE_PREFIX='bb_compacted_empty_fault_'
    CLIENT_PREFIX='bigbrotr-compacted-empty-fault-'
    CLONES=tuple(EMPTY_DELIVERY_STEPS)
    STEPS=EMPTY_DELIVERY_STEPS
    CASES=tuple(n+'-'+s for n in EMPTY_DELIVERY_STEPS for s in EMPTY_DELIVERY_STEPS[n])
    GRANTS=EMPTY_GRANTS
    NOT_RUN=tuple(v for v in EmptyInitialProfile.NOT_RUN if v!='Final COMMIT wire baselines and before/after delivery loss')+(
        'Original-input empty final COMMIT delivery loss; only compacted predecessor exercised here',)


def empty_delivery_specification(case):
    name=case['clone']; assert name in EMPTY_DELIVERY_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in EMPTY_DELIVERY_STEPS[name] and case['name']==name+'-'+step
    index=EMPTY_DELIVERY_STEPS[name].index(step)
    assert case['origin']=='empty-compacted' and case['predecessor']==(name+'-'+EMPTY_DELIVERY_STEPS[name][index-1] if index else None)
    assert case['operation']=='retire-empty' and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def empty_delivery_config(config,case,*,lease_us=6_000_000):
    assert type(lease_us) is int and lease_us in (6_000_000,8_000_000)
    assert config['storage']['ingest_root']=='/lab/'+case['clone']+'/store'
    assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref','tls')}==dict(
        database=case['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application',
        tls=dict(local_boundary='loopback',mode='local-plaintext'))
    assert {k:config['runtime'][k] for k in ('lease_ttl','heartbeat_interval','renewal_timeout','authorization_ttl','graceful_shutdown_timeout')}==dict(
        lease_ttl='8s' if lease_us==8_000_000 else '6s',heartbeat_interval='1s',renewal_timeout='5s' if lease_us==8_000_000 else '1s',authorization_ttl='5s',graceful_shutdown_timeout='2s')
    assert config['sync']['enabled'] is False and config['administration']['enabled'] is False
    assert config['ingest']['producer_outstanding_requests']==2


def empty_delivery_controls(check,context,initial,expected,baseline):
    import json
    def fault(c): return c['fixture']['delivery_fault']
    check('proxy-not-joined',lambda c:fault(c)['wire'].update(readers_joined=False))
    check('proxy-forced-stop',lambda c:fault(c)['wire'].update(forced_wrapper_stop=True))
    check('proxy-source-mismatch',lambda c:fault(c)['wire'].update(source_sha256='00'*32))
    check('configuration-not-restored',lambda c:fault(c)['configuration'].update(restored=''))
    check('wrong-selected-commit',lambda c:fault(c).update(target_commit=2))
    check('proxy-cleanup-error',lambda c:fault(c)['errors'].append(dict(phase='child-joined',error='unknown')))
    def counter(c):
        wire=fault(c)['wire'];wire['events'][-1]['client_commit_complete']+=1
        wire['receipt']=wire['events'][-1]
        wire['stdout']=''.join(json.dumps(e)+'\n' for e in wire['events'])
    check('coordinated-false-delivery-counter',counter)
    if context['case']['clone']=='baseline':
        check('wrong-final-query',lambda c:fault(c)['held']['backend'].update(query='COMMIT'))
        check('wrong-held-state',lambda c:fault(c)['held']['backend'].update(state='idle'))
        check('missing-held-transaction',lambda c:fault(c)['held']['backend'].update(xact_start=None))
        check('held-input-added',lambda c:fault(c)['held']['inputs'].update({'extra.cbor':dict(bytes=1,sha256='00'*32)}))
        check('held-target-prematurely-retired',lambda c:only(fault(c)['held']['control']['producer_epochs'],producer_epoch=2).update(state='retired'))
        check('held-current-lease-expired',lambda c:only(fault(c)['held']['control']['owner_epochs'],role='control',state='active').update(lease_expires_at_us=1))
    else:
        check('partial-terminal-batch',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(state='open' if context['case']['clone']=='after' else 'closed'))


EMPTY_LOCK_STEPS={'claim-commit':('attempt','refuse'),'claim-rollback':('attempt',),
    'retirement-first':('attempt','retry'),'cancel':('attempt','retry'),'expiry':('attempt','retry')}


class EmptyInitialLockProfile:
    DIRECTORY='installed-compacted-empty-initial-locks'
    LABEL='org.bigbrotr.installed-compacted-empty-initial-locks'
    STATUS='PASS_INSTALLED_COMPACTED_EMPTY_INITIAL_LOCKS'
    DATABASE_PREFIX='bb_compacted_empty_lock_'
    CLIENT_PREFIX='bigbrotr-compacted-empty-lock-'
    CLONES=tuple(EMPTY_LOCK_STEPS)
    STEPS=EMPTY_LOCK_STEPS
    CASES=tuple(n+'-'+s for n in EMPTY_LOCK_STEPS for s in EMPTY_LOCK_STEPS[n])
    GRANTS=EMPTY_GRANTS
    NOT_RUN=('Actual original preparing-Control claim; current-Control interval only',
        'Same-Control proof reuse after failure; malformed/foreign/cancelled proof use',
        'Original-input claim/lock/cancellation/expiry counterparts',
        'Optional residual originals; partial target shapes; SQL-terminal/nativeOPEN isolation',
        'No import execution/input binding/checkpoint; ordinary guarded relational claim only',
        'Prior empty13/delivery5 profiles remain separately qualified, not rerun here',
        'Actual GC/floor writer/unlink, mixed restore/physical loss/capacity and later consumers')


def empty_lock_specification(case):
    name=case['clone']; assert name in EMPTY_LOCK_STEPS
    step=case['name'].removeprefix(name+'-'); assert step in EMPTY_LOCK_STEPS[name] and case['name']==name+'-'+step
    index=EMPTY_LOCK_STEPS[name].index(step)
    assert case['origin']=='empty-compacted' and case['predecessor']==(name+'-'+EMPTY_LOCK_STEPS[name][index-1] if index else None)
    assert case['operation']=='retire-empty' and re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    return name,step


def empty_lock_failure(name,step):
    return name=='claim-commit' or (step=='attempt' and name in ('cancel','expiry'))


def empty_lock_config(config,case):
    assert config['runtime']['renewal_safety_margin']=='1s'
    empty_delivery_config(config,case,lease_us=8_000_000 if case['clone']=='expiry' else 6_000_000)


def empty_lock_setup_checked(value,expected):
    from initial_empty_locks_oracle import app_identity,guards,JOB,PRINCIPAL,REQUEST,DIGEST
    pg,native,inputs=expected; same(value['before'],pg)
    assert set(value)=={'before','after','job_id','principal_id','request_key','request_digest','guards_before','guards_after',
        'statement','transactions','status','backend','identity','server_window','created_at_us','cleanup_errors','actor_absence'}
    assert value['status']=='CAPTURED' and value['cleanup_errors']==[]
    assert (value['job_id'],value['principal_id'],value['request_key'],value['request_digest'])==(JOB,PRINCIPAL,REQUEST,DIGEST)
    assert not any(r['job_id']==JOB or r['principal_id']==PRINCIPAL or r['request_key']==REQUEST for r in pg['jobs'])
    b=value['backend']; app_identity(value['identity'],b)
    assert b['user']=='bigbrotr_control' and b['isolation']=='read committed' and b['synchronous_commit']=='on'
    assert b['application']; number(b['pid'],1,integer=True)
    assert value['actor_absence']==[dict(b,present=False)]
    guards(value['guards_before']); assert value['guards_after']==value['guards_before']
    w=value['server_window']; assert set(w)=={'before_us','after_us'}
    number(w['before_us'],1,integer=True); number(w['after_us'],w['before_us'],w['before_us']+30_000_000,integer=True)
    model.clock(value['created_at_us'],w)
    ds=only(pg['datasets']); xid=only(value['after']['jobs'],job_id=JOB)['xmin']; model.fresh_xmin(xid,pg)
    accepted=dict(job_id=JOB,dataset_id=ds['dataset_id'],principal_id=PRINCIPAL,request_key=REQUEST,request_digest=DIGEST,
        kind='import',config_revision=ds['config_revision'],policy_revision=ds['policy_revision'],created_at_us=value['created_at_us'],
        state='accepted',job_revision=1,checkpoint_revision=0,reason_code=None,result_manifest_digest=None,
        owner_resource_id=None,owner_epoch=None,owner_role='control',owner_incarnation=None,completed_at_us=None,
        input_topology_generation=None,input_complete=True,acquisition_job_protocol=None,export_job_protocol=None,xmin=xid)
    statements=['BEGIN ISOLATION LEVEL READ COMMITTED;',
        "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us) SELECT "
        f"decode('{JOB[2:]}','hex'),dataset_id,decode('{PRINCIPAL[2:]}','hex'),decode('{REQUEST[2:]}','hex'),decode('{DIGEST[2:]}','hex'),'import',config_revision,policy_revision,{value['created_at_us']} "
        "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
        f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB[2:]}','hex') RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",'COMMIT;']
    assert value['statement']==statements
    responses=value['transactions']; assert len(responses)==4
    assert all(responses[i]==dict(sqlstate='00000',rows=0,lines=[]) for i in (0,3))
    for response,row in zip(responses[1:3],(dict(accepted,input_complete=False),accepted),strict=True):
        assert response==dict(sqlstate='00000',rows=1,lines=response['lines']) and [loads(v) for v in response['lines']]==[row]
    wanted=copy.deepcopy(pg); wanted['jobs'].append(accepted)
    wanted['job_admissions'].append(dict(job_id=JOB,admission_ordinal=max(r['admission_ordinal'] for r in pg['job_admissions'])+1,xmin=xid))
    same(value['after'],wanted)
    return wanted,native,inputs


def empty_lock_guard_checked(value,current,database,window):
    assert set(value)=={'backend','since','until','observed_sqlstate','errors','contexts'}
    assert value['backend']==dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=database)
    assert retained.instant(value['since'])==window['before_us'] and retained.instant(value['until'])==window['after_us']
    error,=value['errors'];context,=value['contexts'];pid=current['control_backend_pid']
    matched=re.search(r'\['+str(pid)+r'\] ERROR:  (?:(?P<code>[0-9A-Z]{5}): )?empty attempt Control workflow claims remain unresolved$',error)
    assert matched and value['observed_sqlstate']==matched.group('code') and value['observed_sqlstate'] in (None,'23514')
    assert f'[{pid}] CONTEXT:  ' in context and re.search(r'function (?:control\.)?require_initial_empty_claims\(control.initial_ingest_successions\)',context)
    for line in (error,context):model.clock(retained.instant(line.split()[0]),window)


def empty_lock_schedule_checked(value,case,window,expected,after):
    import tomllib
    from fault_cases import safe_event
    from initial_empty_locks_oracle import app_identity,identity,sql_text,statement,claimed_job,JOB,CONTROL_WAIT
    name,step=empty_lock_specification(case); assert step=='attempt'
    first=name=='retirement-first';target,later=(3,1) if first else (2,2);mode='hold' if first else 'hold-after-complete'
    assert set(value)=={'status','mode','wire','held','configuration','schedule','errors'}
    assert value['status']=='JOINED_CONFIG_RESTORED' and value['mode']==name and value['errors']==[]
    cfg=value['configuration'];assert set(cfg)=={'path','direct','proxied','restored'} and cfg['path']==case['config'] and cfg['direct']==cfg['restored']
    direct,proxied=(tomllib.loads(cfg[k]) for k in ('direct','proxied'));empty_lock_config(direct,case)
    wire=value['wire'];assert set(wire)=={'command','source_sha256','ready','receipt','events','stdout','stderr','exit_code','readers_joined','overflow','forced_wrapper_stop'}
    source=sha(retained.regular_read(Path(__file__).resolve().parents[2]/ACTIVATED_PROXY));assert wire['source_sha256']==source
    assert wire['readers_joined'] is True and wire['overflow'] is False and wire['forced_wrapper_stop'] is False
    assert type(wire['exit_code']) is int and wire['exit_code']==0 and wire['stderr']==''
    events=[safe_event(loads(v)) for v in wire['stdout'].splitlines()];assert events==wire['events']
    assert [e['event'] for e in events]==['ready','held','released','receipt'];ready,held,released,receipt=events
    assert ready==wire['ready'] and receipt==wire['receipt'] and ready['proxy_sha256']==source
    assert (ready['listen_host'],ready['upstream_port'],ready['max_frame_bytes'],ready['max_wire_bytes'],ready['max_messages'],ready['deadline_seconds'])==('127.0.0.1',5432,1048576,67108864,20000,30)
    number(ready['listen_port'],1,65535,integer=True);number(ready['pid'],1,integer=True)
    for e in events: assert (e['mode'],e['commit_number'])==(mode,target)
    assert all(type(e['elapsed_ns']) is int and 0<=e['elapsed_ns']<30_000_000_000 for e in events)
    assert [e['elapsed_ns'] for e in events]==sorted(e['elapsed_ns'] for e in events)
    counts=lambda e:[e[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]
    assert counts(ready)==[0]*4 and ready['faults']==0 and ready['held'] is False and ready['hold_action'] is None
    assert counts(held)==counts(released)==([3,2,2,2] if first else [2,2,2,1])
    assert held['held'] is True and held['hold_action'] is None and held['faults']==1
    assert released['held'] is False and released['hold_action']=='release' and released['faults']==1
    assert counts(receipt)==([4]*4 if name in ('claim-rollback','retirement-first') else [2]*4)
    assert receipt['reason'] in ('frontend-eof','backend-eof') and receipt['faults']==1 and receipt['held'] is False and receipt['hold_action']=='release'
    assert 0<sum(receipt['frames'].values())<=20000 and 0<receipt['declared_wire_bytes']<=67108864
    predicted=copy.deepcopy(direct);predicted['postgres']['control']['endpoint']='127.0.0.1:'+str(ready['listen_port']);assert proxied==predicted
    rev=case['database'].removeprefix(EmptyInitialLockProfile.DATABASE_PREFIX)[:7]
    assert wire['command']==['docker','exec','-i',EmptyInitialLockProfile.CLIENT_PREFIX+rev,'python3','-B','/lab/fault_proxy.py','--upstream-port','5432','--listen-port','0','--mode',mode,'--commit-number',str(target),'--later-commits',str(later),'--deadline-seconds','30']
    h=value['held'];assert set(h)=={'event','before_us','after_us','release_before_us','release_after_us','control','inputs','backend'} and h['event']==held
    lease_us=8_000_000 if name=='expiry' else 6_000_000
    m=model.Model(*expected,window);preparation_control(m,h['control'],success=False,renewed=True,lease_us=lease_us)
    same(h['control'],m.pg);assert h['inputs']==expected[2]
    current=max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    for key in ('before_us','after_us','release_before_us','release_after_us'):model.clock(h[key],window)
    assert current['acquired_at_us']<=current['lease_expires_at_us']-lease_us<=h['before_us']
    assert h['before_us']<=h['after_us']<=h['release_before_us']<=h['release_after_us']<current['lease_expires_at_us']
    b=h['backend'];assert b==dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=case['database'],user='bigbrotr_control',state='idle in transaction' if first else 'idle',xact_start=b['xact_start'],query=b['query'],observed_at_us=b['observed_at_us'])
    assert (b['xact_start'] is not None if first else b['xact_start'] is None)
    assert sql_text(b['query'])==('SELECT control.require_empty_initial_retirement($1::bytea,$2::bytea)' if first else 'COMMIT')
    assert h['before_us']<=b['observed_at_us']<=h['after_us']
    s=value['schedule'];assert set(s)=={'status','actor','blocks','confirmed_blocks','pre_age','expiry','signal','end_before_us','end_after_us','cleanup_errors'}
    assert s['status']=='CAPTURED' and s['cleanup_errors']==[]
    a=s['actor'];assert set(a)=={'backend','identity','begin','transaction_identity','statement','result','end_statement','end','closed'}
    actor=a['backend'];app_identity(a['identity'],actor);app_identity(a['transaction_identity'],actor)
    assert actor['database']==case['database'] and actor['pid']!=current['control_backend_pid'] and actor['user']=='bigbrotr_control'
    assert actor['isolation']=='read committed' and actor['synchronous_commit']=='on' and actor['application']==case['name'].replace('-','_')+'_actor'
    assert a['begin']==dict(sqlstate='00000',rows=0,lines=[])
    assert sql_text(a['statement'])==statement(current,name in ('cancel','expiry'))
    answer=a['result'];assert answer==dict(sqlstate='00000',rows=1,lines=answer['lines']);row,=map(loads,answer['lines'])
    claimed=None
    if name in ('cancel','expiry'):assert row==current
    else:
        job=only(expected[0]['jobs'],job_id=JOB);assert job['state']=='accepted'
        model.fresh_xmin(row['xmin'],m.pg);claimed=claimed_job(job,current,row['xmin']);assert row==claimed
    assert a['end_statement']==('COMMIT;' if name in ('claim-commit','retirement-first') else 'ROLLBACK;') and a['end']==dict(sqlstate='00000',rows=0,lines=[])
    closed=a['closed'];assert set(closed)=={'backend','exit_code','absence','pending'} and closed['backend']==actor and closed['absence']==[dict(actor,present=False)]
    assert type(closed['exit_code']) is int and closed['pending'] is None
    control_backend=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=case['database'])
    waiter,holder=(actor,control_backend) if first else (control_backend,actor)
    last=h['after_us'] if first else h['release_before_us']
    assert s['blocks']
    for series in (s['blocks'],s['confirmed_blocks']):
        previous=0
        for sample in series:
            model.clock(sample['observed_at_us'],window);assert sample['observed_at_us']>=last;last=sample['observed_at_us']
            assert previous<=sample['sample_seconds']<=2;previous=sample['sample_seconds']
            identity(sample['waiter'],waiter);identity(sample['holder'],holder)
            assert sample['waiter']['user']==sample['holder']['user']=='bigbrotr_control'
        if series:
            blocked=series[-1];w,holderrow=blocked['waiter'],blocked['holder']
            assert w['state']=='active' and w['wait_event_type']=='Lock' and holder['pid'] in w['blockers']
            assert holderrow['state']=='idle in transaction' and holderrow['backend_xid'].isdecimal()
            assert blocked['waiting_locks'] and all(v['granted'] is False for v in blocked['waiting_locks'])
            assert any(v['type']=='transactionid' and v['mode']=='ShareLock' and v['transaction']==holderrow['backend_xid'] for v in blocked['waiting_locks'])
            assert sql_text(w['query'])==sql_text(a['statement'] if first else CONTROL_WAIT)
            if claimed is not None:assert (w if first else holderrow)['backend_xid']==claimed['xmin']
    assert last<=s['end_before_us']<=s['end_after_us'];model.clock(s['end_after_us'],window)
    if name in ('claim-commit','claim-rollback','retirement-first'):assert s['end_after_us']<current['lease_expires_at_us']
    if name=='expiry':
        assert s['signal'] is None and s['confirmed_blocks']
        age=s['pre_age'];expiry=s['expiry'];deadline=current['lease_expires_at_us'];refreshed=deadline-8_000_000
        assert age and age[0]['remaining_us']>4_000_000
        previous=h['after_us']
        for sample in age:
            assert previous<=sample['server_us']<deadline and sample['remaining_us']==deadline-sample['server_us'];previous=sample['server_us']
        assert 3_500_000<=age[-1]['remaining_us']<=4_000_000
        assert previous<=h['release_before_us'] and h['release_after_us']-refreshed<5_000_000
        assert expiry and s['blocks'][-1]['observed_at_us']<deadline
        previous=s['blocks'][-1]['observed_at_us']
        for sample in expiry:
            assert previous<=sample['server_us'] and sample['remaining_us']==deadline-sample['server_us'];previous=sample['server_us']
        assert deadline+50_000<=previous<=s['confirmed_blocks'][0]['observed_at_us']<=s['end_before_us']
        assert s['end_after_us']-h['release_before_us']<5_000_000
    else:assert s['pre_age']==s['expiry']==[]
    if name=='cancel':
        signal=s['signal'];assert set(signal)=={'kind','identity','before_us','after_us','terminal','terminal_observed_us','backend_absence'}
        assert signal['kind']=='SIGTERM' and s['blocks'][-1]['observed_at_us']<=signal['before_us']<=signal['after_us']<current['lease_expires_at_us']
        assert signal['after_us']-h['release_before_us']<1_000_000
        t=signal['terminal'];assert signal['identity']=={k:t[k] for k in ('pid','start_ticks','config','prefix','command')}
        assert t['exit_code']==4 and t['reaped'] is True and t['forced'] is False and t['error'] is None
        assert t['command']==argv('retire-empty',Path(case['config'])) and t['prefix']=='/lab/'+case['name']
        assert signal['after_us']<=signal['terminal_observed_us']<=s['end_before_us'] and signal['backend_absence']==[dict(control_backend,present=False)]
        assert not s['confirmed_blocks']
    elif name!='expiry':assert s['signal'] is None and not s['confirmed_blocks']
    actual=only(after['owner_epochs'],role='control',epoch=current['epoch'])
    if name in ('claim-rollback','retirement-first'):
        assert actual['lease_expires_at_us']==current['lease_expires_at_us'] and int(actual['xmin'])>int(current['xmin'])
        for k in ('resource_id','role_instance_id','process_incarnation','acquired_at_us','control_backend_pid','control_backend_start'):assert actual[k]==current[k]
    else:assert actual==current
    if first:
        producer=only(after['producer_epochs'],producer_epoch=2)
        assert s['blocks'][-1]['holder']['backend_xid']==producer['xmin']
        assert producer['retired_at_us']<=s['blocks'][-1]['observed_at_us']<=h['release_before_us']
        # The owner UPDATE can sample its clock before waiting for the claim's
        # SHARE lock. release() awaits that UPDATE before sampling the role clock.
        role=only(after['role_instances'],role_instance_id=current['role_instance_id'])
        assert s['end_before_us']<=role['retired_at_us']
        # These finite no-wrap transaction identities supplement the observed
        # lock and successful COMMIT; xmin order alone is not commit-time proof.
        assert int(producer['xmin'])<int(claimed['xmin'])<int(actual['xmin'])
    return claimed if name in ('claim-commit','retirement-first') else None


def empty_lock_controls(check,context,initial,expected):
    name,step=empty_lock_specification(context['case'])
    check('false-lock-config',lambda c:c.update(empty_configuration=c['empty_configuration'].replace('lease_ttl = "8s"','lease_ttl = "6s"') if name=='expiry' else c['empty_configuration'].replace('lease_ttl = "6s"','lease_ttl = "8s"')))
    if name=='claim-commit':
        check('missing-named-claim-veto',lambda c:c['empty_claim_guard'].update(errors=[]))
        check('claim-veto-wrong-backend',lambda c:c['empty_claim_guard']['backend'].update(pid=1))
    if step!='attempt':return
    def fault(c):return c['fixture']['delivery_fault']
    def schedule(c):return fault(c)['schedule']
    check('actor-not-excluded',lambda c:schedule(c)['actor']['closed']['absence'][0].update(present=True))
    check('actor-bypass',lambda c:schedule(c)['actor']['transaction_identity'].update(session_replication_role='replica'))
    check('actor-not-current',lambda c:schedule(c)['actor'].update(statement='SELECT 1;'))
    check('actor-transaction-unfinished',lambda c:schedule(c)['actor']['end'].update(sqlstate='23514'))
    check('actor-cleanup-failed',lambda c:schedule(c)['cleanup_errors'].append(dict(error='unknown')))
    check('proxy-unjoined',lambda c:fault(c)['wire'].update(readers_joined=False))
    check('proxy-source-changed',lambda c:fault(c)['wire'].update(source_sha256='00'*32))
    check('config-not-restored',lambda c:fault(c)['configuration'].update(restored=''))
    check('missing-real-wait',lambda c:schedule(c).update(blocks=[]))
    check('wrong-holder-incarnation',lambda c:schedule(c)['blocks'][-1]['holder'].update(backend_start='2000-01-01T00:00:00+00:00'))
    check('no-blocker',lambda c:schedule(c)['blocks'][-1]['waiter'].update(blockers=[]))
    check('no-waiting-lock',lambda c:schedule(c)['blocks'][-1].update(waiting_locks=[]))
    check('wrong-wait-query',lambda c:schedule(c)['blocks'][-1]['waiter'].update(query='SELECT 1'))
    check('held-target-change',lambda c:only(fault(c)['held']['control']['producer_epochs'],producer_epoch=2).update(state='retired'))
    check('held-native-implied-input',lambda c:fault(c)['held']['inputs'].update({'invented':dict(bytes=0,sha256=sha(b''))}))
    def counter(c):
        import json
        wire=fault(c)['wire'];wire['events'][-1]['client_commit_complete']+=1;wire['receipt']=wire['events'][-1]
        wire['stdout']=''.join(json.dumps(e)+'\n' for e in wire['events'])
    check('coordinated-wrong-wire-count',counter)
    if name=='claim-rollback':
        def future_refresh(c):
            h=fault(c)['held'];current=only(h['control']['owner_epochs'],role='control',state='active')
            deadline=h['before_us']+1+6_000_000;current['lease_expires_at_us']=deadline
            only(c['after']['owner_epochs'],role='control',epoch=current['epoch'])['lease_expires_at_us']=deadline
        check('coordinated-refresh-after-held-capture',future_refresh)
    if name in ('claim-commit','claim-rollback','retirement-first'):
        check('claim-ended-after-lease',lambda c:schedule(c).update(end_after_us=only(fault(c)['held']['control']['owner_epochs'],role='control',state='active')['lease_expires_at_us']+1))
    if name in ('claim-commit','retirement-first'):
        from initial_empty_locks_oracle import JOB
        check('claim-attribution-lost',lambda c:only(c['after']['jobs'],job_id=JOB).update(owner_epoch=1))
    if name=='retirement-first':
        def early_role(c):
            current=only(fault(c)['held']['control']['owner_epochs'],role='control',state='active')
            owner=only(c['after']['owner_epochs'],role='control',epoch=current['epoch'])
            at=schedule(c)['end_before_us']-1
            owner['released_at_us']=min(owner['released_at_us'],at)
            only(c['after']['role_instances'],role_instance_id=current['role_instance_id'])['retired_at_us']=at
        check('coordinated-role-retired-before-claim-commit',early_role)
        def shared_xmin(c):
            current=only(fault(c)['held']['control']['owner_epochs'],role='control',state='active')
            xid=only(c['after']['jobs'],job_id=JOB)['xmin']
            only(c['after']['owner_epochs'],role='control',epoch=current['epoch'])['xmin']=xid
            only(c['after']['role_instances'],role_instance_id=current['role_instance_id'])['xmin']=xid
        check('coordinated-claim-and-release-same-transaction',shared_xmin)
    if name=='cancel':
        check('signal-after-unlock',lambda c:schedule(c)['signal'].update(before_us=schedule(c)['end_after_us']+1))
        check('signal-wrong-process',lambda c:schedule(c)['signal']['identity'].update(start_ticks=1))
        check('signal-backend-still-present',lambda c:schedule(c)['signal']['backend_absence'][0].update(present=True))
        def after_local_deadline(c):
            s=schedule(c);at=fault(c)['held']['release_before_us']+1_100_000
            s['signal'].update(before_us=at,after_us=at+1,terminal_observed_us=at+100)
            s.update(end_before_us=at+200,end_after_us=at+201)
            c['server_window']['after_us']=max(c['server_window']['after_us'],at+1000)
        check('coordinated-signal-after-local-deadline',after_local_deadline)

    if name=='expiry':
        check('not-actually-expired',lambda c:schedule(c)['expiry'][-1].update(remaining_us=1))
        check('no-expired-wait',lambda c:schedule(c).update(confirmed_blocks=[]))
        check('fictional-preage',lambda c:schedule(c)['pre_age'][-1].update(remaining_us=1))
        check('refresh-held-over-local-deadline',lambda c:fault(c)['held'].update(release_after_us=only(fault(c)['held']['control']['owner_epochs'],role='control',state='active')['lease_expires_at_us']-2_000_000))


def empty_lock_setup_controls(value,expected):
    empty_lock_setup_checked(value,expected);rejected=[]
    def check(name,change):
        v=copy.deepcopy(value);change(v)
        try:empty_lock_setup_checked(v,expected)
        except (AssertionError,KeyError,ValueError):rejected.append(name)
        else:raise AssertionError('empty lock setup accepted '+name)
    check('setup-privileged-role',lambda v:v['identity'].update(superuser=True))
    check('setup-guard-disabled',lambda v:v['guards_after']['triggers'][0].update(enabled='D'))
    check('setup-actor-not-closed',lambda v:v['actor_absence'][0].update(present=True))
    check('setup-statement-bypass',lambda v:v['statement'].__setitem__(0,"BEGIN; SET session_replication_role=replica;"))
    check('setup-missing-commit',lambda v:v['transactions'][-1].update(sqlstate='23514'))
    check('setup-premature-claim',lambda v:only(v['after']['jobs'],job_id=v['job_id']).update(state='queued'))
    check('setup-admission-reordered',lambda v:only(v['after']['job_admissions'],job_id=v['job_id']).update(admission_ordinal=1))
    check('setup-unrelated-row',lambda v:v['after']['producer_epochs'][0].update(xmin='4294967294'))
    return dict(status='PASS',count=len(rejected),rejected=rejected)
