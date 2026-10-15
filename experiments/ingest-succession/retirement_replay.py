#!/usr/bin/env python3
"""Independent retirement, retry, refusal and activated-predecessor live replay.

Only fresh identities, server-clock coordinates and transaction IDs parameterize
the expected SQL state. Native receipts and closures retain the literal science
oracles. This module performs no engine or product operation.
"""
import copy
import re
import sys

sys.dont_write_bytecode = True
import acquisition_replay as acquisition
import live_replay as live
import recovery_replay as recovery
import transition_replay as transition

history = acquisition.history
encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads, exact = history.only, history.same, history.loads, live.exact
CASES = ('genuine-gap-retire3', 'genuine-gap-retry3', 'genuine-gap-live4',
         'dictionary-retire3', 'dictionary-live4', 'import-event-refusal3')
CLAIM_CASES = ('empty-claims3', 'empty-source3', 'empty-retire-refusal3')
NO_AUTHORITY = dict(source_closure=False, workflow=False, retirement=False, admission=False, activation=False)
PINS = {
    'genuine-gap': dict(sql_rows='5683e67a57a8195059eee548d20f0adabc6201a784ffea462d50f6b2478a2572',
        native_rows='88feef2fa0e9e62dc304a71ebc93a07f6af7c56c04346ee7ce7902938081595d',
        input_rows='5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'),
    'dictionary': dict(sql_rows='f78cb2712a144f40c0682a9b5300e4ecd808fcc7fc992886b16a2bd5ac6f518a',
        native_rows='8767aeb9386a026e3f8fe3c1b95bd3d950bec6735ab7f7fb4518f737942714a7',
        input_rows='00cb6dce0063ed0c60258b8542bc88a956d4cfae8ee4ba7f0d9be74a1f6c6f3c'),
    'import-event': dict(sql_rows='01e2da35847d22c1cbca5f5e440481209790b211c1c3c04e697c65bea2b31fe5',
        native_rows='26661bc8b9f3e803fbef808513db652276b89244bbff2a876f3909693b7df078',
        input_rows='b6d156632a2c0ff849cb2c752a6760388829fb04c22531542cb1b51334c6226a'),
}


def specification(case):
    name = case['name']
    assert name in CASES + CLAIM_CASES
    if name in CLAIM_CASES[:2]:
        acquisition.specification(case)
        return 'empty', False, False
    mode = ('empty' if name in CLAIM_CASES else 'genuine-gap' if name.startswith('genuine-gap-')
            else 'dictionary' if name.startswith('dictionary-') else 'import-event')
    predecessor = {'genuine-gap-retry3': CASES[0], 'genuine-gap-live4': CASES[1],
                   'dictionary-live4': CASES[3], 'empty-retire-refusal3': CLAIM_CASES[1]}.get(name)
    assert case['origin'] == case['clone'] == mode and case['predecessor'] == predecessor
    is_live = name.endswith('live4')
    assert case['operation'] == ('live-new' if is_live else 'retire')
    assert type(case['metadata_bytes']) is int and case['metadata_bytes'] == 262144
    assert case.get('proxy_port') is None and re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return mode, is_live, name.endswith('refusal3')


def terminal(pg, known, retired):
    """Require the entire ready or completed target, keeping source closure separate."""
    head = known['head']
    producer = only(pg['producer_epochs'], producer_epoch=head)
    intent = only(pg['ingest_successions'], target_epoch=head)
    source = only(pg['source_streams'], owner_epoch=head)
    owner = only(pg['owner_epochs'], resource_id=intent['owner_resource_id'], epoch=head)
    role = only(pg['role_instances'], role_instance_id=intent['target_role_instance_id'])
    domains = [r for r in pg['allocation_domains'] if r['owner_epoch'] == head]
    assert len(domains) == 9 and sorted(r['dictionary_kind'] for r in domains) == list(range(1, 10))
    summary = known['summaries'][-1]
    digest = '\\x' + summary['digest']
    assert source['state'] == 'closed' and source['closure_digest'] == digest
    assert source['last_complete_sequence'] == summary['last_complete_sequence']
    assert type(source['closed_at_us']) is int and source['closed_at_us'] > 0
    assert producer['reserved_through_request'] == known['issued']
    receipts = [r for r in pg['ingest_terminal_transactions'] if r['target_epoch'] == head]
    if not retired:
        assert producer['state'] == 'closing' and producer['retired_at_us'] is None and producer['closure_digest'] is None
        assert owner['state'] in ('active', 'draining') and owner['released_at_us'] is None
        assert owner['exclusion_method'] is owner['exclusion_digest'] is role['retired_at_us'] is None
        assert not receipts
        for row in domains:
            assert row['state'] == 'active' and row['closed_at_us'] is row['closure_digest'] is None
        return None
    receipt, = receipts
    at, xid = producer['retired_at_us'], producer['xmin']
    assert producer['state'] == 'retired' and producer['closure_digest'] == digest
    assert type(at) is int and at >= source['closed_at_us']
    assert owner['state'] == 'released' and owner['released_at_us'] == at
    assert owner['exclusion_method'] == 'storage_fenced' and owner['exclusion_digest'] == digest
    assert role['retired_at_us'] == at and owner['xmin'] == role['xmin'] == receipt['xmin'] == xid
    assert re.fullmatch('[1-9][0-9]*', receipt['terminal_transaction'])
    assert int(receipt['terminal_transaction']) % 2**32 == int(xid)
    assert source['xmin'] != xid
    for row in domains:
        assert row['state'] == 'closed' and row['closed_at_us'] == at
        assert row['closure_digest'] == digest and row['xmin'] == xid
    control = only(pg['owner_epochs'], resource_id=receipt['control_resource'], epoch=receipt['control_epoch'])
    assert control['role'] == 'control' and control['process_incarnation'] == receipt['control_incarnation']
    assert control['acquired_at_us'] <= at < control['lease_expires_at_us']
    return at


def science(pg, native, inputs, mode, retired):
    if mode in ('genuine-gap', 'empty'):
        known = acquisition.science(pg, native, inputs)
        assert known['closing']
        assert (len(native['rows']), native['sequence'], len(inputs)) == (55, 68, 4)
    else:
        origin = 'outcome-before-loss' if mode == 'dictionary' else 'activated-live3'
        known = recovery.science(pg, native, inputs, origin, source_purpose=True, retired_head=retired)
        assert (len(native['rows']), native['sequence'], len(inputs)) == ((79, 98, 7) if mode == 'dictionary' else (90, 112, 9))
    assert known['head'] == 3 and len(pg['producer_outcomes']) == sum(known['counts'])
    terminal(pg, known, retired)
    return known


def terminal_history(pg, known):
    """Literal science supplies the activated head; no empty-head substitution."""
    legacy = only(pg['initial_ingest_successions'])
    entries = []
    for epoch, summary in enumerate(known['summaries'], 1):
        if epoch == 1:
            manifest, incoming = binary(legacy['previous_manifest']), bytes(32)
        else:
            intent = legacy if epoch == 2 else only(pg['ingest_successions'], target_epoch=epoch)
            manifest, incoming = binary(intent['target_manifest']), bytes.fromhex(sha(binary(intent['handoff'])))
        entries.append(dict(manifest=manifest.hex(), issued_through=summary['resolved_requests'],
            source_through=summary['last_complete_sequence'], closing=True, incoming_sha256=incoming.hex(),
            terminal_kind=summary['terminal_kind'] if epoch < known['head'] else None,
            terminal_digest=summary['digest'] if epoch < known['head'] else None))
    return dict(slot=known['slot'].hex(), entries=entries, summaries=known['summaries'],
        counts=known['counts'], head=3, through=3, state='Terminal')


class Model(live.Model):
    def retire(self, observed, known):
        old = only(self.pg['producer_epochs'], producer_epoch=3)
        if old['state'] == 'retired':
            terminal(self.pg, known, True)
            same(self.pg, observed)
            return
        terminal(self.pg, known, False)
        receipt = only(observed['ingest_terminal_transactions'], target_epoch=3)
        actual = only(observed['producer_epochs'], producer_epoch=3)
        source = only(self.pg['source_streams'], owner_epoch=3)
        at = self.at(actual['retired_at_us'], source['closed_at_us'])
        xid = self.fresh(actual['xmin'], receipt['terminal_transaction'])
        assert at < self.control['lease_expires_at_us']
        digest = '\\x' + known['summaries'][-1]['digest']
        old.update(state='retired', retired_at_us=at, closure_digest=digest, xmin=xid)
        only(self.pg['owner_epochs'], resource_id=self.resource, epoch=3).update(state='released',
            released_at_us=at, exclusion_method='storage_fenced', exclusion_digest=digest, xmin=xid)
        intent = only(self.pg['ingest_successions'], target_epoch=3)
        only(self.pg['role_instances'], role_instance_id=intent['target_role_instance_id']).update(retired_at_us=at, xmin=xid)
        for row in self.pg['allocation_domains']:
            if row['owner_epoch'] == 3:
                row.update(state='closed', closed_at_us=at, closure_digest=digest, xmin=xid)
        self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource, target_epoch=3,
            terminal_transaction=receipt['terminal_transaction'], control_resource=self.control['resource_id'],
            control_epoch=self.control['epoch'], control_incarnation=self.control['process_incarnation'], xmin=xid))
        terminal(self.pg, known, True)
        same(self.pg, observed)


class Transcript(live.Transcript):
    def retirement(self, name):
        assert self.position < len(self.events)
        event = self.events[self.position]
        self.position += 1
        assert event['event'] == 'lineage-retirement-' + name
        return event

    def refresh_retirement(self, epoch, sequence):
        self.snapshot(f"retirement-refresh-returned-{epoch if epoch is not None else 'final'}-{sequence if sequence is not None else 'epoch'}", self.model.refresh)
        self.policy(self.take('policy'), 'refreshed', epoch, sequence)


def finish(t, m, backend, failure):
    t.snapshot('after-retirement')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='control', failed=False, joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    phase = 'dropped-and-joined' if failure else 'released-and-joined'
    exact(t.take('control'), dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', None if failure else m.release)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    exact(t.retirement('complete'), dict(event='lineage-retirement-complete', operation='retire', native='closed',
        control=phase, observer='joined', success=not failure, **dict(NO_AUTHORITY, retirement=not failure)))
    assert t.position == len(t.events), 'unclassified trailing telemetry'


def verify(events, before, native, inputs, case, server_window):
    mode, is_live, refused = specification(case)
    if case['name'] in CLAIM_CASES[:2]:
        return acquisition.verify(events, before, native, inputs, case, server_window)
    if case['predecessor'] is None:
        pin = PINS[mode]
        assert sha(encoded(history.normalized(before))) == pin['sql_rows'], 'original SQL rows/xmin differ'
        assert sha(encoded(native)) == pin['native_rows'] and sha(encoded(inputs)) == pin['input_rows']
    retry = case['name'] == 'genuine-gap-retry3'
    known = science(before, native, inputs, mode, retry or is_live)
    if is_live:
        return live.verify_live(events, before, native, inputs, case, server_window, terminal_history(before, known))
    m, old_at = Model(before, native, inputs, server_window), terminal(before, known, retry)
    t = Transcript(events, m)
    exact(t.retirement('resources'), dict(event='lineage-retirement-resources', data_bytes=131072,
        native_bytes=0, max_jobs=64, source_closure=False, workflow=False, admission=False))
    resources = t.take('resources')
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resources[field]) is int and 0 < resources[field] <= 256 * 1024**2
    exact(resources, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, input_bytes=resources['input_bytes'],
        outcome_bytes=resources['outcome_bytes'], effect_bytes=resources['outcome_bytes'], native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2))
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation='retire',
        mutation_data_bytes=4096, mutation_native_bytes=4096, lease_ttl_us='20000000', outstanding_request_limit=32))
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    t.snapshot('before-claim')
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'], control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-retirement')
    t.snapshot('retirement-inspect-returned')
    failure = None
    if mode == 'empty':
        claim = only(m.pg['jobs'], request_key='\\x' + 'a6' * 16)
        activation = only(m.pg['ingest_activations'], target_epoch=3)
        assert claim['state'] == 'queued' and claim['job_revision'] == 2 and claim['checkpoint_revision'] == 0
        assert (claim['owner_resource_id'], claim['owner_epoch'], claim['owner_incarnation']) == tuple(
            activation[k] for k in ('activated_by_resource', 'activated_by_epoch', 'activated_by_incarnation'))
        assert not any(r['producer_epoch'] == 3 for r in m.pg['producer_requests'] + m.pg['acquisition_gaps'])
        failure = dict(stage='inspect', epoch=None, sequence=None, error='Control(Backend)')
    else:
        exact(t.retirement('loaded'), dict(event='lineage-retirement-loaded', epoch=3, manifest=known['manifest'].hex(),
            producer_slot=known['slot'].hex(), issued_through=known['issued'], reserved_through=known['issued'],
            staged_input=None, barrier_confirmed=False, retired_at_us=old_at))
        t.snapshot('before-retirement-observe')
        t.snapshot('retirement-observe-returned')
        exact(t.retirement('observed'), dict(event='lineage-retirement-observed', epoch=3, confirmed=True, error=None))
        for epoch, count in enumerate(known['counts'], 1):
            for n in range(1, count + 1):
                t.refresh_retirement(epoch, n)
                t.snapshot(f'retirement-request-returned-{epoch}-{n}')
                if mode == 'import-event' and (epoch, n) == (3, 2):
                    request = only(m.pg['producer_requests'], producer_epoch=3, request_sequence=2)
                    assert all(request[k] is None for k in ('acquisition_job_id', 'acquisition_scope_id', 'relay_endpoint_id',
                        'relay_access_scope_id', 'relay_route_id', 'relay_enrollment_generation'))
                    assert known['summaries'][-1]['event_request_mask'] == 6
                    failure = dict(stage='request', epoch=3, sequence=2, error='Outcome(Conflict)')
                    break
                exact(t.retirement('request'), dict(event='lineage-retirement-request', producer_slot=known['slot'].hex(),
                    epoch=epoch, sequence=n, copy_mode='existing-retirement',
                    reservation=only(m.pg['producer_requests'], producer_epoch=epoch, request_sequence=n),
                    outcome=only(m.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=n)))
            if failure:
                break
            t.refresh_retirement(epoch, None)
            t.snapshot(f'retirement-epoch-returned-{epoch}')
            exact(t.retirement('epoch'), dict(event='lineage-retirement-epoch', summary=known['summaries'][epoch - 1]))
    observation = None
    if not failure:
        t.refresh_retirement(None, None)
        t.snapshot('retirement-finish-returned')
        exact(t.retirement('proof'), dict(event='lineage-retirement-proof', verified_through=3,
            last_epoch=known['summaries'][-1], **NO_AUTHORITY))
        t.snapshot('before-retirement-commit')
        t.snapshot('retirement-commit-returned', lambda pg: m.retire(pg, known))
        gaps = 1 if mode == 'genuine-gap' else 0
        assert len([r for r in m.pg['acquisition_gaps'] if r['producer_epoch'] == 3]) == gaps
        exact(t.retirement('committed'), dict(event='lineage-retirement-committed', epoch=3, gaps=gaps,
            previous_retired_at_us=old_at, error=None))
        observation = dict(target_epoch=3, issued_through=known['issued'], reserved_through=known['issued'],
            verified_through=3, last_epoch=known['summaries'][-1], gaps=gaps, **dict(NO_AUTHORITY, retirement=True))
    assert bool(failure) == refused
    exact(t.retirement('result'), dict(event='lineage-retirement-result', operation='retire', observation=observation,
        **(failure or dict(stage='complete', epoch=None, sequence=None, error=None))))
    finish(t, m, backend, bool(failure))
    expected_snapshots = 8 if mode == 'empty' else 30 if mode == 'import-event' else 14 + 2 * (sum(known['counts']) + 3)
    assert t.snapshots == expected_snapshots
    assert m.refreshes == (0 if mode == 'empty' else 10 if mode == 'import-event' else sum(known['counts']) + 4)
    return dict(status='PASS_EXPECTED_REFUSAL' if refused else 'PASS', case=case['name'], database=case['database'],
        snapshots=t.snapshots, tables=75, native_rows=len(native['rows']), sequence=native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, failure=failure, observation=observation,
        scope='Existing source and native bytes preserved; explicit retirement or exact refused boundary, with actual Control lifecycle')


def negative_controls(events, before, native, inputs, case, server_window):
    """Coherent rehashed alterations must fail the reader, not just its checksums."""
    if case['name'] in CLAIM_CASES[:2]:
        return acquisition.negative_controls(events, before, native, inputs, case, server_window)
    verify(events, before, native, inputs, case, server_window)
    mode, is_live, refused = specification(case)
    rejected = []

    def event(values, name, **fields):
        return only(values, event=name, **fields)

    def sql(values, mutation, stage=None):
        active = stage is None
        for value in values:
            if value['event'] == 'lineage-history-state':
                active = active or value['stage'] == stage
                if active:
                    state = value['state']
                    pg = {name: loads(rows) for name, rows in state['control_tables']}
                    mutation(pg)
                    state['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                    state['control_sha256'] = sha(encoded(state['control_tables']))

    def reject(name, mutate):
        altered = copy.deepcopy(events)
        mutate(altered)
        try:
            verify(altered, before, native, inputs, case, server_window)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def native_change(values):
        for value in values:
            if value['event'] == 'lineage-history-state':
                row = only(value['rows'], cf='default', key='0101')
                raw = bytearray.fromhex(row['value'])
                raw[-1] ^= 1
                row['value'] = raw.hex()
                value['state']['native_sha256'] = sha(encoded(value['rows']))

    reject('historical-xmin-rehashed', lambda v: sql(v,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='3999999999')))
    reject('native-manifest-rehashed', native_change)
    reject('source-closed-time-rewritten', lambda v: sql(v,
        lambda pg: only(pg['source_streams'], owner_epoch=3).update(closed_at_us=server_window['before_us'])))
    reject('source-xmin-rewritten', lambda v: sql(v,
        lambda pg: only(pg['source_streams'], owner_epoch=3).update(xmin='3999999999')))
    reject('source-cut-rewritten', lambda v: sql(v,
        lambda pg: only(pg['source_streams'], owner_epoch=3).update(last_complete_sequence=99)))
    reject('existing-copy-rewritten', lambda v: sql(v,
        lambda pg: only(pg['producer_outcomes'], producer_epoch=1, request_sequence=4).update(xmin='3999999999')))
    reject('input-changed', lambda v: event(v, 'lineage-history-state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('native-sequence-changed', lambda v: event(v, 'lineage-history-state', stage='final')['state'].update(sequence=native['sequence'] + 100))
    reject('control-driver-not-joined', lambda v: event(v, 'lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: event(v, 'lineage-history-driver', driver='observer').update(failed=True))
    reject('backend-not-absent', lambda v: event(v, 'lineage-history-backend', present=False).update(present=True))
    prefix = 'lineage-live-' if is_live else 'lineage-retirement-'
    reject('completion-success-inverted', lambda v: event(v, prefix + 'complete').update(success=refused))
    reject('unknown-result', lambda v: event(v, prefix + 'result').update(error='CommitUnknown'))
    if mode in ('genuine-gap', 'empty'):
        reject('unclaimed-intent-modified', lambda v: sql(v,
            lambda pg: only(pg['jobs'], request_key='\\x' + 'a7' * 16).update(job_revision=2)))
        reject('issued-session-revoked', lambda v: sql(v,
            lambda pg: only(pg['browser_sessions']).update(revoked_at_us=server_window['before_us'])))
        reject('claimed-job-rewritten', lambda v: sql(v,
            lambda pg: only(pg['jobs'], request_key='\\x' + 'a6' * 16).update(job_revision=3)))
    if is_live:
        reject('activated-predecessor-labelled-empty', lambda v:
            known_head(v).update(terminal_kind='unactivated-empty'))
        reject('old-event-signature-changed', lambda v: event(v, 'lineage-live-fixture')['old_event'].update(sig='00' * 64))
        reject('request-digest-changed', lambda v: event(v, 'lineage-live-input', stage='new-event').update(request_digest='00' * 32))
        reject('historical-dependency-labelled-parent', lambda v:
            event(v, 'lineage-live-certification', kind='historical')['dependencies'][0]['source_record_id'].update(owner_epoch='3'))
        reject('activation-row-rewritten', lambda v: sql(v,
            lambda pg: only(pg['ingest_activations'], target_epoch=4).update(activated_by_epoch=1), 'activated-zero'))
        reject('terminal-receipt-rewritten-during-live', lambda v: sql(v,
            lambda pg: only(pg['ingest_terminal_transactions'], target_epoch=3).update(control_epoch=1)))
    else:
        reject('writer-authority-invented', lambda v: event(v, prefix + 'complete').update(admission=True))
        reject('workflow-authority-invented', lambda v: event(v, prefix + 'complete').update(workflow=True))
        if refused:
            reject('wrong-refusal-stage', lambda v: event(v, prefix + 'result').update(stage='retire'))
            reject('wrong-refusal-epoch', lambda v: event(v, prefix + 'result').update(epoch=2))
            reject('invented-success-observation', lambda v: event(v, prefix + 'result').update(observation={}))
            reject('retirement-on-refusal', lambda v: sql(v,
                lambda pg: only(pg['producer_epochs'], producer_epoch=3).update(state='retired'), 'after-retirement'))
            reject('release-invented-on-drop', lambda v: event(v, 'lineage-history-control', phase='dropped-and-joined').update(phase='released-and-joined'))
        else:
            reject('proof-confers-retirement', lambda v: event(v, prefix + 'proof').update(retirement=True))
            reject('commit-observation-omitted', lambda v: v.remove(event(v, prefix + 'committed')))
            reject('wrong-target-gap-count', lambda v: event(v, prefix + 'committed').update(gaps=2))
            for name, table, fields, values in (
                ('terminal-issuer', 'ingest_terminal_transactions', dict(target_epoch=3), dict(control_epoch=1)),
                ('terminal-transaction', 'ingest_terminal_transactions', dict(target_epoch=3), dict(terminal_transaction='3999999999')),
                ('producer-digest', 'producer_epochs', dict(producer_epoch=3), dict(closure_digest='\\x' + '00' * 32)),
                ('role-time', 'role_instances', dict(role_instance_id=only(before['ingest_successions'], target_epoch=3)['target_role_instance_id']), dict(retired_at_us=1)),
                ('domain-xmin', 'allocation_domains', dict(owner_epoch=3, dictionary_kind=9), dict(xmin='3999999999')),
                ('owner-exclusion', 'owner_epochs', dict(resource_id=only(before['ingest_successions'], target_epoch=3)['owner_resource_id'], epoch=3), dict(exclusion_method=None)),
            ):
                reject(name + '-rewritten', lambda v, table=table, fields=fields, values=values: sql(v,
                    lambda pg: only(pg[table], **fields).update(values), 'retirement-commit-returned'))
            reject('retirement-after-authority-deadline', lambda v: sql(v,
                lambda pg: only(pg['producer_epochs'], producer_epoch=3).update(retired_at_us=server_window['after_us'] + 20_000_000),
                'retirement-commit-returned'))
            if case['name'] == 'genuine-gap-retry3':
                reject('retry-rewrites-terminal-time', lambda v: sql(v,
                    lambda pg: only(pg['producer_epochs'], producer_epoch=3).update(retired_at_us=server_window['before_us']),
                    'retirement-commit-returned'))
        if mode != 'empty':
            reject('barrier-confirmed-at-inspect', lambda v: event(v, prefix + 'loaded').update(barrier_confirmed=True))
            reject('tail-invented', lambda v: event(v, prefix + 'loaded').update(staged_input={}))
            reject('copy-mode-repair-invented', lambda v: event(v, prefix + 'request', epoch=1, sequence=1).update(copy_mode='current-positive'))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def known_head(events):
    return next(e['summary'] for e in events if e['event'] == 'lineage-history-epoch' and e['summary']['epoch'] == 3)
