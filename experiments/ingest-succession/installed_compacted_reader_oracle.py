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


def qualification_checked(built, component, qualification, source):
    assert re.fullmatch(r'[0-9a-f]{40}',source)
    assert component['status'] == 'PASS' and qualification['status'] == 'PASS_COMPACTED_PREFIX_COMPONENTS'
    assert built['source'] == component['source'] == qualification['source'] == source
    for key in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert built[key] == component[key] == qualification[key]
    assert component['missing_controls'] == {}
    counts = component['test_counts']
    assert counts == qualification['test_counts']
    assert set(counts) == {'compacted_prefix','compacted_progress','initial_reconstruction','request_inputs','successor_history','archive_retained_frontier'}
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
    assert set(component['executables_sha256']) == {'bigbrotr','ingest_bootstrap_probe','native_components'}
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
    selected = STEPS if mode in CLONES else {n:('abandon','retire') for n in HISTORY_CLONES}
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


def history_catalog_checked(original, current):
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
    for statement in HISTORY_GRANTS:
        m = re.fullmatch(r'GRANT (.+) ON (FUNCTION )?(.+) TO bigbrotr_control;',statement)
        rights, kind, targets = m.groups(); kind=(kind or 'TABLE').strip()
        parts = re.split(r',\s*(?=control\.)',targets)
        for target in parts:
            wanted.setdefault(key(kind,target),set()).update(rights.split(','))
    # Whitespace in the existing column privilege is presentation only.
    normalize = lambda a:{k:{re.sub(r'\s+','',r) for r in v} for k,v in a.items()}
    assert normalize(after) == normalize(wanted)


def history_seed_plan(pg, native, inputs, observed, seed):
    """Fresh identity/time coordinates only; complete portable bytes are ours."""
    terminal_checked(pg,seed)
    assert native == seed[1] and inputs == seed[2]
    ds,p = only(pg['datasets']),only(pg['producer_epochs'])
    assert ds['topology_generation'] == 0 and ds['policy_revision'] == ds['config_revision'] == 1
    assert p['outstanding_request_limit'] == 2
    m = model.Model(pg,native,inputs,dict(before_us=observed['created_at_us'],after_us=observed['created_at_us']),initial=True)
    role,inc = (m.identity(observed[k]) for k in ('target_role','target_incarnation'))
    assert [r[0] for r in observed['domains']] == list(range(1,10))
    domains = [(k,m.identity(v)) for k,v in observed['domains']]
    old = bytes.fromhex(model.native_map(native)['default','0101'])
    target = bytearray(old); target[2:4]=be(2,2); target[26:34]=be(1); target[54:62]=be(2)
    target[62:78]=binary(inc)
    for k,v in domains: target[104+(k-1)*18:104+k*18]=be(k,2)+binary(v)
    target=bytes(target); digest=known(seed)['digest']
    handoff=b'\x01\x0a'+binary(p['producer_slot_id'])+be(1)+be(7)+be(27)+bytes.fromhex(digest)+be(266,2)+old+be(266,2)+target
    at=observed['created_at_us']; assert at >= p['retired_at_us']
    plan=dict(event='successor-plan',dataset=ds['dataset_id'][2:],instance=ds['instance_id'][2:],
        resource=p['owner_resource_id'][2:],producer_slot=p['producer_slot_id'][2:],previous_epoch=1,target_epoch=2,
        shard=0,previous_topology=0,target_topology=1,target_incarnation=inc[2:],recovery_generation=old[78:94].hex(),
        target_role=role[2:],created_at_us=at,lease_expires_at_us=at+60_000_000,window=2,
        policy_revision=1,config_revision=1,policy_digest=only(pg['policy_revisions'],revision=1)['document_digest'][2:],
        config_digest=only(pg['config_revisions'],revision=1)['document_digest'][2:],domains=[[k,v[2:]] for k,v in domains],
        issued_through=7,last_complete_sequence=27,closure_digest=digest,previous_manifest=old.hex(),
        target_manifest=target.hex(),handoff=handoff.hex())
    assert observed == plan and len(handoff)==610
    rows={(cf,bytes.fromhex(k)):bytes.fromhex(v) for (cf,k),v in model.native_map(native).items()}
    rows['default',b'\x01\x01']=target
    rows['default',b'\x01\x0a'+binary(ds['dataset_id'])+binary(p['owner_resource_id'])+be(1)]=handoff
    rows['default',b'\x01\x02'+binary(ds['dataset_id'])+be(0,4)+be(2)]=b'\x01\x02'+be(0)
    rows['receipts',b'\x01\x31'+binary(p['producer_slot_id'])+be(2)]=b'\x01\x31'+be(0)*3
    assert len(rows)==91
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


def verify_history_seed(events, expected, seed, case, window):
    import initial_live_oracle as live
    import initial_succession_oracle as initial
    pg,native,inputs=copy.deepcopy(expected)
    plan,rows=history_seed_plan(pg,native,inputs,only(events,event='successor-plan'),seed)
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
    pg=after; assert take('successor-plan')==plan
    after=snap('before-prepare'); control=initial.renewed(pg,after,control,plan['created_at_us'])
    model.fresh_xmin(control['xmin'],pg); pg=after
    native=dict(event='dump-state',rows=model.native_rows({(cf,k.hex()):v.hex() for (cf,k),v in rows.items()}),sequence=117)
    after=snap('prepared'); initial.prepared(pg,after,plan,control,compacted_history=True); pg=after
    backend=take('successor-backend','initial')
    assert backend==dict(event='successor-backend',stage='initial',database=case['database'],pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'],control_epoch=control['epoch'],present=True)
    assert take('live-activation-start')==dict(event='live-activation-start',mode=HISTORY_SEED_MODE,control_epoch=control['epoch'])
    activated=take('live-activation-result'); after=snap('activated-zero')
    assert activated==dict(event='live-activation-result',writer=True,error=None,activated_at_us=activated['activated_at_us'])
    live.activated(pg,after,plan,control,window,activated['activated_at_us']); pg=after
    science=live.Science(plan,rows,inputs,compacted_history=True); receipt=science.advance('dictionary')
    assert take('live-receipt','dictionary')==dict(event='live-receipt',stage='dictionary',request=1,receipt=receipt.hex())
    native=dict(event='dump-state',rows=model.native_rows({(cf,k.hex()):v.hex() for (cf,k),v in science.rows.items()}),sequence=125)
    inputs=science.inputs; after=snap('dictionary'); live.admitted(pg,after,science,1,control,window); pg=after
    same(pg,snap('final-before-control-end'))
    assert take('successor-control','final','released-and-joined')==dict(event='successor-control',stage='final',
        phase='released-and-joined',resource=control['resource_id'][2:],epoch=control['epoch'])
    after=snap('final-after-control-end'); initial.ended(pg,after,control,True)
    released=only(after['owner_epochs'],role='control',epoch=control['epoch'])
    model.fresh_xmin(released['xmin'],pg); model.clock(released['released_at_us'],window); pg=after
    assert take('successor-backend','initial')==dict(backend,present=False)
    same(pg,snap('final'))
    assert take('live-complete')==dict(event='live-complete',mode=HISTORY_SEED_MODE,native='closed',control='released-and-joined',writer=False,admission=True)
    assert take('compacted-history-seed-complete')==dict(event='compacted-history-seed-complete',native='closed',control='released-and-joined',observer='joined',requests=1,events=0)
    assert position==len(events)
    return (pg,native,inputs),dict(status='PASS',case=case['name'],native_rows=97,native_sequence=125,
        required_originals=1,initial_originals=7,public_scope_ordinal=1,new_events=0)


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


def history_claim(t,case):
    m=t.model; planned=t.take('control'); assert set(planned)=={'event','phase','role','incarnation','lease_ttl_us'}
    assert planned['phase']=='claim-planned' and planned['lease_ttl_us']=='20000000'
    t.snapshot('before-claim'); claimed,policy,backend=t.take('control'),t.take('policy'),t.take('backend')
    t.snapshot('after-claim',lambda pg:m.claim(pg,planned))
    assert claimed==dict(event='lineage-history-control',phase='claimed',resource=m.control['resource_id'][2:],epoch=m.control['epoch'],
        role=planned['role'],incarnation=planned['incarnation'])
    assert backend==dict(event='lineage-history-backend',pid=m.control['control_backend_pid'],backend_start=m.control['control_backend_start'],
        database=case['database'],control_epoch=m.control['epoch'],present=True)
    t.policy(policy,'acquired',None,None); t.snapshot('before-read')
    return backend


def history_end(t,backend,*,success,transition=False):
    m=t.model
    t.snapshot('after-transition' if transition else 'after-read')
    assert t.take('native')==dict(event='lineage-history-native',state='closed')
    t.snapshot('before-control-end')
    assert t.take('driver')==dict(event='lineage-history-driver',driver='control',failed=False,joined=True)
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
        error='Progress(InvalidContext)' if clone=='history-incompatible-head' else 'Native(Native(StateMismatch))' if refusal else None
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
