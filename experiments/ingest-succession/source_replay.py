#!/usr/bin/env python3
"""Independent full-state finite successor source closure and exact retry.

The recovered originals are pinned; receipt/closure bytes use the independent
literal scientific kernel. A replay proof is not publication: only the observed
complete source row after the actual call establishes closure in this reader.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import sys
import tomllib

sys.dont_write_bytecode = True
import recovery_replay as recovery

history, transition = recovery.history, recovery.transition
encoded, sha, binary, be = recovery.encoded, recovery.sha, recovery.binary, recovery.be
only, same, loads, exact = recovery.only, recovery.same, recovery.loads, recovery.exact

PINS = {'activation-after-loss': {'name': 'activation-after-loss',
                           'run': 'ingest-succession-recovery-engine-e2eb140',
                           'subdirectory': 'recovery',
                           'database': 'bb_successor_recover_e2eb140_activation_after_loss',
                           'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                           'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                           'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                           'label': 'org.bigbrotr.succession-recovery',
                           'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                           'source_clone': 'activation-after-loss',
                           'final_case': 'activation-after-loss-retry',
                           'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                           'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                           'snapshot': 'activation-after-loss-retry-after.json',
                           'native_file': 'activation-after-loss-retry-after-native.json',
                           'inputs_file': 'activation-after-loss-retry-after-inputs.json',
                           'store': 'activation-after-loss/store',
                           'snapshot_sha256': 'c1c76f83af2b47c96552eb9fd9a317acb3177b459b70a312d588b2a11a51617a',
                           'native_sha256': '78b4a460f7e3abdbf513fb5d5625e7a2cf481445e23bf40437109370ac7c10b4',
                           'inputs_sha256': '2a60c0a8f80eab6c2875f5546ab47df9053e2fac9f69b799fc7e1d276560af25',
                           'sql_rows': '0eec8de703ccb01b8f2419d8490a440b04c01c02b1458899528f16f88c6613db',
                           'native_rows': '98fed707e3269cc012c1c58781ac34cc185cb7da7c70ca21c368d40ecd4a8ed9',
                           'input_rows': '28bb113a743f8327830392c13fff29edfdcea38b66d82b332020d20c63a0618b'},
 'outcome-before-loss': {'name': 'outcome-before-loss',
                         'run': 'ingest-succession-recovery-engine-e2eb140',
                         'subdirectory': 'recovery',
                         'database': 'bb_successor_recover_e2eb140_outcome_before_loss',
                         'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                         'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                         'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                         'label': 'org.bigbrotr.succession-recovery',
                         'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                         'source_clone': 'outcome-before-loss',
                         'final_case': 'outcome-before-loss-retry',
                         'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                         'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                         'snapshot': 'outcome-before-loss-retry-after.json',
                         'native_file': 'outcome-before-loss-retry-after-native.json',
                         'inputs_file': 'outcome-before-loss-retry-after-inputs.json',
                         'store': 'outcome-before-loss/store',
                         'snapshot_sha256': '5293a2ee51c91a6685fbbf41c57e35c3366bbcfc92ee2d27c5984680580853c7',
                         'native_sha256': 'a618e931f2bf82445986ebaaf5d602de69f4b1d58c1e034c2c4af0c025a3e087',
                         'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
                         'sql_rows': '7b03552ec74ee1cfcfc9586ad2c0452607d7722fc15ef710fde4b2cde877a99b',
                         'native_rows': '8767aeb9386a026e3f8fe3c1b95bd3d950bec6735ab7f7fb4518f737942714a7',
                         'input_rows': '00cb6dce0063ed0c60258b8542bc88a956d4cfae8ee4ba7f0d9be74a1f6c6f3c'},
 'outcome-after-loss': {'name': 'outcome-after-loss',
                        'run': 'ingest-succession-recovery-engine-e2eb140',
                        'subdirectory': 'recovery',
                        'database': 'bb_successor_recover_e2eb140_outcome_after_loss',
                        'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                        'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                        'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                        'label': 'org.bigbrotr.succession-recovery',
                        'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                        'source_clone': 'outcome-after-loss',
                        'final_case': 'outcome-after-loss-retry',
                        'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                        'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                        'snapshot': 'outcome-after-loss-retry-after.json',
                        'native_file': 'outcome-after-loss-retry-after-native.json',
                        'inputs_file': 'outcome-after-loss-retry-after-inputs.json',
                        'store': 'outcome-after-loss/store',
                        'snapshot_sha256': '90c29db8e66616ba26efdc46e11d4eff706f3310068a43a9af32c8ac4871e35a',
                        'native_sha256': '22312f5cc31c739c43ec9b8ff8d78a940a9e87494682a847f214099a47379239',
                        'inputs_sha256': 'f5dfa70f26aad25dc36e0676cab9e698373262080130c2715d69696495ccc80b',
                        'sql_rows': '5f754ba6c510fc3758979ccf324801e7663f3c7a47741d8d86c43171c8ac501b',
                        'native_rows': '4924b14ab1f06ab4819602235d86942ce893859448cf93ff65bd2dcf05af12ce',
                        'input_rows': 'acf1b4b067d5394f323f72c76fe976ea66b855f443a921abc0ffb4077b669059'},
 'activated-live3': {'name': 'activated-live3',
                     'run': 'ingest-succession-recovery-engine-e2eb140',
                     'subdirectory': 'recovery',
                     'database': 'bb_successor_recover_e2eb140_activated_live3',
                     'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                     'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                     'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                     'label': 'org.bigbrotr.succession-recovery',
                     'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                     'source_clone': 'activated-live3',
                     'final_case': 'activated-live3-retry',
                     'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                     'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                     'snapshot': 'activated-live3-retry-after.json',
                     'native_file': 'activated-live3-retry-after-native.json',
                     'inputs_file': 'activated-live3-retry-after-inputs.json',
                     'store': 'activated-live3/store',
                     'snapshot_sha256': '1239a1e4d7615ae4b641ee070f6576b4ab0f08ed66ebe8c8311b15ca4bf30334',
                     'native_sha256': '28e68d5e6ea304ef44e7968b7d341b8b4e70b56aa11c1200c260c6696727a4c8',
                     'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
                     'sql_rows': '0a8b8fd8fe2f0850a0b44505e55bce140d33865850f74fb338edc6596f4ed46e',
                     'native_rows': '26661bc8b9f3e803fbef808513db652276b89244bbff2a876f3909693b7df078',
                     'input_rows': 'b6d156632a2c0ff849cb2c752a6760388829fb04c22531542cb1b51334c6226a'},
 'empty-live3': {'name': 'empty-live3',
                 'run': 'ingest-succession-recovery-engine-e2eb140',
                 'subdirectory': 'recovery',
                 'database': 'bb_successor_recover_e2eb140_empty_live3',
                 'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                 'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                 'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                 'label': 'org.bigbrotr.succession-recovery',
                 'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                 'source_clone': 'empty-live3',
                 'final_case': 'empty-live3-retry',
                 'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
                 'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                 'snapshot': 'empty-live3-retry-after.json',
                 'native_file': 'empty-live3-retry-after-native.json',
                 'inputs_file': 'empty-live3-retry-after-inputs.json',
                 'store': 'empty-live3/store',
                 'snapshot_sha256': '1b3ddf145507be385c685cb308746d73202f2d6d2831bb8b1e04b829fd5bbb85',
                 'native_sha256': 'a90e114d12d15a7c96c41a5e87ea8c4a999edd78c0126d610277be3cac7738cd',
                 'inputs_sha256': 'be0224ca229820c162970bba46e8e9459d5c0aa2bbbd16b0fdd1c284fad5db18',
                 'sql_rows': '4cc0e08e004cde735e3c5659e9677264efec911d7bb9d6a84876ffe28ef4d4e3',
                 'native_rows': '9757521b3af3213c2366df20f8ecc87edf1b7de9406e5b2969a1b93ea6f8cf9a',
                 'input_rows': '9a03a9b8b63c56b980c436ecb48b9096c24f8b07d1851b3f494968bcfd189e5a'},
 'activated-later-live4': {'name': 'activated-later-live4',
                           'run': 'ingest-succession-recovery-engine-e2eb140',
                           'subdirectory': 'recovery',
                           'database': 'bb_successor_recover_e2eb140_activated_later_live4',
                           'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
                           'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
                           'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
                           'label': 'org.bigbrotr.succession-recovery',
                           'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
                           'source_clone': 'activated-later-live4',
                           'final_case': 'activated-later-live4-retry',
                           'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                           'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
                           'snapshot': 'activated-later-live4-retry-after.json',
                           'native_file': 'activated-later-live4-retry-after-native.json',
                           'inputs_file': 'activated-later-live4-retry-after-inputs.json',
                           'store': 'activated-later-live4/store',
                           'snapshot_sha256': 'de9ef1f396bf7979664860e4b28c0e75b097c3d105f6d81847fa0fc4a068419b',
                           'native_sha256': 'ebe8cbcf3d9fc04bc7f4fd585326875dc160235a8bedb4131a543c00c51fa29e',
                           'inputs_sha256': '90127efe7298437df3866ac4c44e83779fb325de5624a9a2b81f3c102f183afd',
                           'sql_rows': '08e3c3ce67d6d7435dbe726ca1eb8c419bac0dd6fec96df3da84a197b216d2b6',
                           'native_rows': '2221ceb5bc8383b7dcde5a05a406aae4be85e961e30e2379254b0725917e60e1',
                           'input_rows': 'c9b2988b85bcd51f111f6c2c4ca5587e336e5277f5347895aabbab9032eff7ca'}}

CLONES = tuple(PINS)
CASES = tuple(name + '-' + step for name in CLONES for step in ('source', 'retry'))
SHAPES = tuple((head, issued, cut, rows, sequence + 1, count)
               for head, issued, cut, rows, sequence, count in recovery.SHAPES)


def specification(case):
    index = CASES.index(case['name'])
    origin, retry = CLONES[index // 2], bool(index % 2)
    assert case['origin'] == case['clone'] == origin
    assert case['operation'] == 'source' and case['metadata_bytes'] == 262144
    assert case['predecessor'] == (origin + '-source' if retry else None)
    assert case.get('proxy_port') is None
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return origin, retry, SHAPES[index // 2]


class Model(recovery.Model):
    """Control lifecycle and existing closing/copies stay unchanged."""
    def __init__(self, before, native, inputs, window, head):
        super().__init__(before, native, inputs, window)
        self.head = head
        self.source_closed = only(before['source_streams'], owner_epoch=head)['state'] == 'closed'

    def resolve(self, observed, known, epoch, sequence):
        if self.source_closed:
            # A completed source is evidence to check, never permission to repair
            # a missing original SQL copy, even for a current Event request.
            assert only(self.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=sequence)
            same(self.pg, observed)
        else:
            super().resolve(observed, known, epoch, sequence)

    def publish(self, observed, known):
        source = only(self.pg['source_streams'], owner_epoch=self.head)
        summary = known['summaries'][-1]
        if self.source_closed:
            assert source['state'] == 'closed'
            assert source['last_complete_sequence'] == summary['last_complete_sequence']
            assert source['closure_digest'] == '\\x' + summary['digest']
        else:
            assert source['state'] == 'open'
            row = only(observed['source_streams'], owner_epoch=self.head)
            at = self.at(row['closed_at_us'], source['opened_at_us'])
            assert at < self.control['lease_expires_at_us']
            source.update(state='closed', closed_at_us=at, last_complete_sequence=summary['last_complete_sequence'],
                closure_digest='\\x' + summary['digest'], xmin=self.fresh(row['xmin']))
        same(self.pg, observed)


class Transcript(recovery.Transcript):
    def source(self, name):
        assert self.position < len(self.events)
        event = self.events[self.position]
        self.position += 1
        assert event['event'] == 'lineage-source-' + name
        return event

    def refresh_source(self, epoch, sequence):
        stage = f"source-refresh-returned-{epoch if epoch is not None else 'final'}-{sequence if sequence is not None else 'epoch'}"
        self.snapshot(stage, self.model.refresh)
        self.policy(self.take('policy'), 'refreshed', epoch, sequence)


def verify(events, before, native, inputs, case, server_window):
    """Verify one process; replay binds retry to its preceding complete state."""
    origin, retry, shape = specification(case)
    head, issued, cut, rows, sequence, input_count = shape
    if not retry:
        pin = PINS[origin]
        assert sha(encoded(history.normalized(before))) == pin['sql_rows'], 'original SQL rows/xmin differ'
        assert sha(encoded(native)) == pin['native_rows'] and sha(encoded(inputs)) == pin['input_rows']
    assert (len(native['rows']), native['sequence'], len(inputs)) == (rows, sequence, input_count)
    known = recovery.science(before, native, inputs, origin, source_purpose=True)
    assert only(before['producer_epochs'], producer_epoch=head)['state'] == 'closing'
    assert only(before['source_streams'], owner_epoch=head)['state'] == ('closed' if retry else 'open')
    assert len(before['producer_outcomes']) == sum(known['counts'])
    marker = ('receipts', (b'\x01\x31' + known['slot'] + be(head)).hex())
    assert recovery.native_map(native)[marker] == (b'\x02\x31' + be(issued) + bytes(16) + b'\x01').hex()
    m = Model(before, native, inputs, server_window, head)
    t = Transcript(events, m)
    resources = t.take('resources')
    assert set(resources) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resources[field]) is int and 0 < resources[field] <= 256 * 1024**2
    exact(resources, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, input_bytes=resources['input_bytes'],
        outcome_bytes=resources['outcome_bytes'], effect_bytes=resources['outcome_bytes'], native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2))
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation='source',
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
    t.snapshot('before-source')
    t.snapshot('source-inspect-returned')
    exact(t.source('loaded'), dict(event='lineage-source-loaded', epoch=head, manifest=known['manifest'].hex(),
        producer_slot=known['slot'].hex(), issued_through=issued, reserved_through=issued, staged_input=None,
        barrier_confirmed=False, sql_producer_state='closing', sql_source_state='closed' if retry else 'open'))
    t.snapshot('before-source-producer-close')
    t.snapshot('source-producer-close-returned', lambda pg: m.close(pg, known))
    exact(t.source('producer-closed'), dict(event='lineage-source-producer-closed', epoch=head, confirmed=True,
        issued_through=issued, reserved_through=issued, staged_input=None, error=None))
    for epoch, count in enumerate(known['counts'], 1):
        for n in range(1, count + 1):
            t.refresh_source(epoch, n)
            t.snapshot(f'source-request-returned-{epoch}-{n}', lambda pg: m.resolve(pg, known, epoch, n))
            exact(t.source('request'), dict(event='lineage-source-request', producer_slot=known['slot'].hex(),
                epoch=epoch, sequence=n, copy_mode='existing-terminal' if epoch < head else 'existing-source-closed' if retry else 'current-positive',
                reservation=only(m.pg['producer_requests'], producer_epoch=epoch, request_sequence=n),
                outcome=only(m.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=n)))
        t.refresh_source(epoch, None)
        t.snapshot(f'source-epoch-returned-{epoch}')
        exact(t.source('epoch'), dict(event='lineage-source-epoch', summary=known['summaries'][epoch - 1]))
    t.refresh_source(None, None)
    t.snapshot('source-finish-returned')
    no_authority = dict(source_closure=False, workflow=False, retirement=False, admission=False, activation=False)
    exact(t.source('proof'), dict(event='lineage-source-proof', verified_through=head,
        last_epoch=known['summaries'][-1], matches_source=True, **no_authority))
    t.snapshot('before-source-publish')
    t.snapshot('source-publish-returned', lambda pg: m.publish(pg, known))
    exact(t.source('published'), dict(event='lineage-source-published', epoch=head, confirmed=True,
        source=only(m.pg['source_streams'], owner_epoch=head), error=None))
    observation = dict(target_epoch=head, issued_through=issued, reserved_through=issued, staged_input=None,
        verified_through=head, last_epoch=known['summaries'][-1], positive_recovery=True, source_closure=True)
    exact(t.source('result'), dict(event='lineage-source-result', operation='source', stage='complete',
        epoch=None, sequence=None, error=None, observation=observation))
    t.snapshot('after-source')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='control', failed=False, joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    exact(t.take('control'), dict(event='lineage-history-control', phase='released-and-joined',
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', m.release)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    exact(t.source('complete'), dict(event='lineage-source-complete', operation='source', native='closed',
        control='released-and-joined', observer='joined', success=True, **dict(no_authority, source_closure=True)))
    assert t.position == len(events), 'unclassified trailing event'
    assert t.snapshots == 14 + 2 * (sum(known['counts']) + head)
    assert m.refreshes == sum(known['counts']) + head + 1
    return dict(status='PASS', case=case['name'], database=case['database'], snapshots=t.snapshots,
        tables=75, native_rows=rows, sequence=m.native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, observation=observation,
        scope='Finite source publication and exact retry on recovered heads; no native puts, copy repair, workflow/retirement, writer, installed or RSS authority')

def negative_controls(events, before, native, inputs, case, server_window):
    """Rehashed coherent alterations test the reader, not product execution."""
    verify(events, before, native, inputs, case, server_window)
    origin, retry, shape = specification(case)
    head = shape[0]
    rejected = []

    def event(values, name, **fields):
        return only(values, event=name, **fields)

    def sql(values, mutation, stage=None):
        active = stage is None
        for value in values:
            if value['event'] == 'lineage-history-state':
                active = active or value['stage'] == stage
                if not active:
                    continue
                state = value['state']
                pg = {name: loads(rows) for name, rows in state['control_tables']}
                mutation(pg)
                state['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                state['control_sha256'] = sha(encoded(state['control_tables']))
            elif active and value['event'] == 'lineage-source-published' and getattr(mutation, 'source', False):
                mutation({'source_streams': [value['source']]})
            elif active and value['event'] == 'lineage-source-request' and getattr(mutation, 'copies', False):
                mutation({'producer_outcomes': [value['outcome']]})

    def reject(name, mutate):
        altered = copy.deepcopy(events)
        mutate(altered)
        try:
            verify(altered, before, native, inputs, case, server_window)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def source_change(values, **fields):
        def mutation(pg):
            only(pg['source_streams'], owner_epoch=head).update(fields)
        mutation.source = True
        sql(values, mutation, 'source-publish-returned')

    def copy_change(values):
        def mutation(pg):
            for row in pg['producer_outcomes']:
                if (row['producer_epoch'], row['request_sequence']) == (1, 4):
                    row.update(recorded_at_us=row['recorded_at_us'] + 1, xmin='3999999999')
        mutation.copies = True
        sql(values, mutation, 'source-request-returned-1-4')

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
    reject('existing-copy-time-xmin-rewritten', copy_change)
    reject('activation-rewritten', lambda v: sql(v,
        lambda pg: only(pg['ingest_activations'], target_epoch=head).update(xmin='3999999999'), 'source-producer-close-returned'))
    reject('native-closing-rewritten', lambda v:
        event(v, 'lineage-history-state', stage='source-producer-close-returned')['state'].update(sequence=native['sequence'] + 1))
    reject('input-changed', lambda v:
        event(v, 'lineage-history-state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('source-cut-invented', lambda v: source_change(v, last_complete_sequence=shape[2] + 1))
    reject('source-digest-invented', lambda v: source_change(v, closure_digest='\\x' + '00' * 32))
    reject('publication-not-committed', lambda v: source_change(v, state='open', closed_at_us=None,
        last_complete_sequence=None, closure_digest=None))
    reject('proof-confers-publication', lambda v: event(v, 'lineage-source-proof').update(source_closure=True))
    reject('published-row-omitted', lambda v: v.remove(event(v, 'lineage-source-published')))
    reject('published-confirmation-false', lambda v: event(v, 'lineage-source-published').update(confirmed=False))
    reject('head-issued-invented', lambda v: event(v, 'lineage-source-loaded').update(issued_through=shape[1] + 1))
    reject('tail-input-invented', lambda v: event(v, 'lineage-source-loaded').update(staged_input={}))
    reject('barrier-confirmed-at-inspect', lambda v: event(v, 'lineage-source-loaded').update(barrier_confirmed=True))
    reject('request-omitted', lambda v: v.pop(next(i for i, e in enumerate(v) if e['event'] == 'lineage-source-request')))
    reject('ancestor-copy-relabelled-current', lambda v:
        event(v, 'lineage-source-request', epoch=1, sequence=1).update(copy_mode='current-positive'))
    reject('workflow-authority-invented', lambda v: event(v, 'lineage-source-proof').update(workflow=True))
    reject('retirement-authority-invented', lambda v: event(v, 'lineage-source-complete').update(retirement=True))
    reject('writer-authority-invented', lambda v: event(v, 'lineage-source-complete').update(admission=True))
    reject('control-driver-not-joined', lambda v: event(v, 'lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: event(v, 'lineage-history-driver', driver='observer').update(failed=True))
    reject('backend-not-absent', lambda v: event(v, 'lineage-history-backend', present=False).update(present=True))
    reject('unknown-publication-result', lambda v: event(v, 'lineage-source-result').update(error='CommitUnknown'))
    current = max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control') + 1
    claim = event(events, 'lineage-history-state', stage='after-claim')
    claimed_pg = {k: loads(v) for k, v in claim['state']['control_tables']}
    claim_xid = only(claimed_pg['owner_epochs'], role='control', epoch=current)['xmin']
    if retry:
        old = only(before['source_streams'], owner_epoch=head)
        reject('retry-rewrites-closed-time-xmin', lambda v:
            source_change(v, closed_at_us=old['closed_at_us'] + 1, xmin='3999999999'))
        if shape[1]:
            reject('closed-source-copy-repair-permitted', lambda v:
                event(v, 'lineage-source-request', epoch=head, sequence=1).update(copy_mode='current-positive'))
    else:
        published = event(events, 'lineage-source-published')['source']
        reject('source-published-before-proof', lambda v: sql(v,
            lambda pg: only(pg['source_streams'], owner_epoch=head).update(published), 'source-finish-returned'))
        reject('publication-reuses-claim-transaction', lambda v: source_change(v, xmin=claim_xid))
        final_refresh = event(events, 'lineage-history-state', stage='source-refresh-returned-final-epoch')
        refresh_pg = {k: loads(v) for k, v in final_refresh['state']['control_tables']}
        control = only(refresh_pg['owner_epochs'], role='control', epoch=current)
        reject('publication-reuses-refresh-transaction', lambda v: source_change(v, xmin=control['xmin']))
        reject('publication-after-Control-deadline', lambda v: source_change(v, closed_at_us=control['lease_expires_at_us']))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind prior pins, full raw response, process boundaries and external state."""
    root = Path(root).resolve()
    folder, bound = root / 'source-close', {}

    def raw(path):
        path = Path(path)
        content = path.read_bytes()
        bound[str(path)] = sha(content)
        return content

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_SOURCE_FIRST_WAVE'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == list(CLONES)
    assert set(report['origins']) == set(CLONES)
    assert len({c['database'] for c in report['clones']}) == 6
    assert report['databases'] == [c['database'] for c in report['clones']]
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    origins = {}
    for origin, pin in PINS.items():
        declaration = report['origins'][origin]
        locator = {k: v for k, v in pin.items() if k not in ('sql_rows', 'native_rows', 'input_rows')}
        assert declaration['pins'] == locator
        for field, expected in locator.items():
            if field != 'store':
                assert declaration[field] == expected
        basis = root.parent / pin['run'] / pin['subdirectory']
        assert Path(declaration['store']) == basis / pin['store']
        previous = read(basis / 'result.json')
        assert bound[str(basis / 'result.json')] == pin['result_sha256']
        for key in ('status', 'source', 'owner', 'label'):
            assert previous[key] == pin[key]
        assert previous['client'] == pin['client_id'] == declaration['previous_client']
        assert all(previous[k] == [] for k in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'))
        clone = only(previous['clones'], name=pin['source_clone'])
        assert clone['final_case'] == pin['final_case'] and clone['database'] == pin['database']
        final = only(previous['cases'], name=pin['final_case'])
        assert final['status'] == clone['status'] == 'PASS' and final['returncode'] == 0
        assert final['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert sha(clone['final_catalog'].encode()) == pin['catalog_sha256']
        rows, native, inputs = [read(basis / pin[k]) for k in ('snapshot', 'native_file', 'inputs_file')]
        for k, digest in (('snapshot', 'snapshot_sha256'), ('native_file', 'native_sha256'), ('inputs_file', 'inputs_sha256')):
            assert bound[str(basis / pin[k])] == pin[digest]
        same(rows, declaration['rows'])
        assert declaration['native'] == native and declaration['inputs'] == inputs
        assert sha(declaration['catalog'].encode()) == pin['catalog_sha256']
        same(rows, read(folder / (origin + '-original.json')))
        spool = basis / pin['store'] / 'producer-inputs-v1'
        assert sorted(p.name for p in spool.iterdir()) == sorted(inputs)
        for name, identity in inputs.items():
            content = raw(spool / name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
        preserved = dict(rows_unchanged=True, catalog_unchanged=True, native_unchanged=True,
            inputs_unchanged=True, physical_unchanged=True)
        exact(declaration['preserved'], preserved)
        exact(read(folder / (origin + '-preserved.json')), preserved)
        origins[origin] = rows, native, inputs

    chains, windows, clones = {}, {}, {}
    for clone in report['clones']:
        name = clone['name']
        assert clone['origin'] == name and clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        config = tomllib.loads(raw(folder / Path(clone['config']).name).decode())
        ds = only(origins[name][0]['datasets'])
        exact({k: config['postgres']['control'][k] for k in ('database', 'endpoint', 'user', 'password_ref')}, dict(
            database=clone['database'], endpoint='127.0.0.1:5432', user='bigbrotr_control', password_ref='file:/run/secrets/application'))
        assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:] and config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['runtime']['lease_ttl'] == '20s'
        validation = read(folder / (name + '-config-validation.json'))
        assert validation['returncode'] == 0 and validation['stderr'] == ''
        assert [r['action'] for r in clone['migrations']] == ['plan', 'apply', 'apply']
        for index, answer in enumerate(clone['migrations']):
            assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
            assert type(answer['exit_code']) is int and answer['exit_code'] == 0 and answer['stderr'] == ''
            exact(loads(answer['stdout']), dict(format='bigbrotr-control-migration/1', scope='postgres-control',
                target_version='38', source_version='38', current_version='38', status='planned' if index == 0 else 'current',
                applied_count='0', steps=[], dataset_initialization='not-performed', release_upgrade_qualification='not-performed'))
        grants = clone['grants']
        assert len(grants) == 4 and all(set(g) == {'statement', 'stdout'} for g in grants)
        assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
        assert [g['statement'] for g in grants[1:]] == [
            'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
            'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
            'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
        assert all(not g['stdout'].strip() for g in grants)
        assert isinstance(clone['catalog_before'], str) and clone['catalog_before']
        assert clone['catalog_before'] == clone['catalog_after'] == clone['final_catalog']
        # All four grants already exist on these qualified origins; applying
        # them again changes neither function bodies nor any ACL or constraint.
        assert sha(clone['catalog_before'].encode()) == PINS[name]['catalog_sha256']
        assert clone['final_backends'] == []
        same(origins[name][0], read(folder / (name + '-copy-before.json')))
        migrated = read(folder / (name + '-migrated.json'))
        same(origins[name][0], migrated)
        chains[name] = migrated, origins[name][1], origins[name][2]
        clones[name] = clone

    checked = []
    for case in report['cases']:
        name, origin = case['name'], case['origin']
        clone = clones[origin]
        assert case['database'] == clone['database'] and case['config'] == clone['config']
        assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(case['returncode']) is int and case['returncode'] == 0
        assert case['catalog_after'] == clone['catalog_before']
        case_read = lambda suffix: read(folder / (name + '-' + suffix + '.json'))
        before, native, inputs = case_read('before'), case_read('before-native'), case_read('inputs')
        previous = chains[origin]
        same(previous[0], before)
        assert native == previous[1] and inputs == previous[2], 'process boundary state differs'
        assert case_read('capture-errors') == []
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error', 'clock_error', 'response_error'))
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        window = case_read('clock')
        assert window == case['server_window'] and window['before_us'] >= windows.get(origin, 0)
        result = verify(events, before, native, inputs, case, window)
        final = only(events, event='lineage-history-state', stage='final')
        after, after_native, after_inputs = case_read('after'), case_read('after-native'), case_read('after-inputs')
        same({k: loads(v) for k, v in final['state']['control_tables']}, after)
        assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
        assert after_inputs == dict(final['state']['originals']) == inputs and case_read('backends') == []
        exact(case_read('custody'), dict(status='OBSERVED_CLOSED_AND_JOINED', database=case['database'], backend_absent=True, errors=[]))
        for input_name, identity in after_inputs.items():
            content = raw(folder / origin / 'store' / 'producer-inputs-v1' / input_name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window)
        checked.append(result)
        chains[origin], windows[origin] = (after, after_native, after_inputs), window['after_us']
    for name, clone in clones.items():
        assert clone['final_case'] == name + '-retry'
        same(chains[name][0], read(folder / (name + '-final-sql.json')))
        spool = folder / name / 'store' / 'producer-inputs-v1'
        assert sorted(p.name for p in spool.iterdir()) == sorted(chains[name][2])
    return dict(status='PASS_SUCCESSOR_SOURCE_FIRST_WAVE_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Twelve finite source closure/retry processes on already recovered heads; no native puts, installed, copy-repair, workflow/retirement, tail, failure-injection or RSS qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = replay(args.root)
    output = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')
