#!/usr/bin/env python3
"""Independent full-state reader for finite activated-successor recovery.

Pinned prior captures provide the immutable starting states. Literal CBOR,
receipt and closure encoders reconstruct science independently of the recovery
output; only fresh Control identity, UTC and transaction coordinates are observed.
No database, native engine or process access is performed by this reader.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import tomllib

import history_replay as history
import transition_replay as transition
import transition_fault_replay as fencing
import live_replay as live

encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads = history.only, history.same, history.loads
native_map, native_rows, bx = transition.native_map, transition.native_rows, transition.bx
exact, cbor = live.exact, live.cbor

# Each locator/hash was checked against the stopped original capture, including
# the fresh history refusal following each earlier fault. No loss projection.
PINS = {'activation-after-loss': {'name': 'activation-after-loss',
                           'run': 'ingest-succession-live-faults-engine-e3806a9',
                           'subdirectory': 'live-faults',
                           'database': 'bb_live_fault_e3806a9_activation_after',
                           'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
                           'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
                           'owner': '1d25e8dbff2141300432875866555117',
                           'label': 'org.bigbrotr.succession-live-fault',
                           'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
                           'source_clone': 'activation-after',
                           'final_case': 'activation-after-fresh',
                           'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                           'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
                           'snapshot': 'activation-after-fresh-after.json',
                           'snapshot_sha256': 'ca873ae8a78b917c74f2f6ad9dab9214674181cfe80cb60e0d42701666ceb0b2',
                           'native_file': 'activation-after-fresh-after-native.json',
                           'native_sha256': '3e104cc276e9b22192405252b936cb54b235f18aba56c9f19c29f90e3a6d6b6b',
                           'inputs_file': 'activation-after-fresh-after-inputs.json',
                           'inputs_sha256': '2a60c0a8f80eab6c2875f5546ab47df9053e2fac9f69b799fc7e1d276560af25',
                           'store': 'activation-after/store',
                           'sql_rows': '0fcebfc7880aaec13e9dc6ecab4d624e3d93d3e2e9d5cd0f53c4da06ed3e195d',
                           'native_rows': 'b3a70b365b8c00f93dbe7885f5c9d322abe22a6ce001024e4676de92395d7af9',
                           'input_rows': '28bb113a743f8327830392c13fff29edfdcea38b66d82b332020d20c63a0618b'},
 'outcome-before-loss': {'name': 'outcome-before-loss',
                         'run': 'ingest-succession-live-faults-engine-e3806a9',
                         'subdirectory': 'live-faults',
                         'database': 'bb_live_fault_e3806a9_outcome_before',
                         'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
                         'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
                         'owner': '1d25e8dbff2141300432875866555117',
                         'label': 'org.bigbrotr.succession-live-fault',
                         'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
                         'source_clone': 'outcome-before',
                         'final_case': 'outcome-before-fresh',
                         'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                         'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
                         'snapshot': 'outcome-before-fresh-after.json',
                         'snapshot_sha256': 'b33ee8dbc35f193ceac83bb4621b5899d89c0442b5c43ba9537200f2eb977092',
                         'native_file': 'outcome-before-fresh-after-native.json',
                         'native_sha256': '70cc31c03be545dbfbc4c3c5293b2617e413bf45d5b0af8a39fab41a4e9c7ed7',
                         'inputs_file': 'outcome-before-fresh-after-inputs.json',
                         'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
                         'store': 'outcome-before/store',
                         'sql_rows': '9479267e9132ac3dbadbbf81b7ab73b9958055c249f33c26711c0f910bce042b',
                         'native_rows': '4a74fa5d4990e6d6771a4cd3a98726c27c5c95b208703a8e27fb1f7826409afb',
                         'input_rows': '00cb6dce0063ed0c60258b8542bc88a956d4cfae8ee4ba7f0d9be74a1f6c6f3c'},
 'outcome-after-loss': {'name': 'outcome-after-loss',
                        'run': 'ingest-succession-live-faults-engine-e3806a9',
                        'subdirectory': 'live-faults',
                        'database': 'bb_live_fault_e3806a9_outcome_after',
                        'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
                        'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
                        'owner': '1d25e8dbff2141300432875866555117',
                        'label': 'org.bigbrotr.succession-live-fault',
                        'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
                        'source_clone': 'outcome-after',
                        'final_case': 'outcome-after-fresh',
                        'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                        'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
                        'snapshot': 'outcome-after-fresh-after.json',
                        'snapshot_sha256': '88620fc0f1520fdb0f861ef5ba09b703e021cd980746568769462df43c3c8b95',
                        'native_file': 'outcome-after-fresh-after-native.json',
                        'native_sha256': '8dc9f9dd47bc03a550c26307f007e111c24cd958655a7e5fbd97095f5112f637',
                        'inputs_file': 'outcome-after-fresh-after-inputs.json',
                        'inputs_sha256': 'f5dfa70f26aad25dc36e0676cab9e698373262080130c2715d69696495ccc80b',
                        'store': 'outcome-after/store',
                        'sql_rows': 'a8a24a09968d93e62b4bd36acfdc693b3bb4bcdace1cbecc9f3e0d1250e517ec',
                        'native_rows': 'ede2b46ac6dbdd2a60fd6232f8ba2360fac56ea6c1a95f5e7f5cc7b83c04439f',
                        'input_rows': 'acf1b4b067d5394f323f72c76fe976ea66b855f443a921abc0ffb4077b669059'},
 'activated-live3': {'name': 'activated-live3',
                     'run': 'ingest-succession-live-engine-006fe3d',
                     'subdirectory': 'live',
                     'database': 'bb_successor_live_006fe3d_activated',
                     'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
                     'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
                     'owner': '63d2ef283499bd9354aa47ad668ab38a',
                     'label': 'org.bigbrotr.succession-live',
                     'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
                     'source_clone': 'activated',
                     'final_case': 'activated-live3',
                     'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                     'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
                     'snapshot': 'activated-live3-after.json',
                     'snapshot_sha256': '72f166eb1deb56129e792a9d426cdf2e54924c6b33872c6cdf4278a94cca45e1',
                     'native_file': 'activated-live3-after-native.json',
                     'native_sha256': 'd14338f256988eeed3bd1e913f56e55eff7614dc50b24ea2500b8aaa5b8ee189',
                     'inputs_file': 'activated-live3-after-inputs.json',
                     'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
                     'store': 'activated/store',
                     'sql_rows': 'ac1b71dddeb992958ad24f1f5eb41340357190808f24604cb27563583e54765d',
                     'native_rows': '53bf5af54f5f5211b8286845e63ab27185e4492183d929994473b5d98f97e8af',
                     'input_rows': 'b6d156632a2c0ff849cb2c752a6760388829fb04c22531542cb1b51334c6226a'},
 'empty-live3': {'name': 'empty-live3',
                 'run': 'ingest-succession-live-engine-006fe3d',
                 'subdirectory': 'live',
                 'database': 'bb_successor_live_006fe3d_empty',
                 'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
                 'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
                 'owner': '63d2ef283499bd9354aa47ad668ab38a',
                 'label': 'org.bigbrotr.succession-live',
                 'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
                 'source_clone': 'empty',
                 'final_case': 'empty-live3',
                 'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
                 'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
                 'snapshot': 'empty-live3-after.json',
                 'snapshot_sha256': '1c9a755a2c3bdb3d0b87cf1feba032c98920dd29a921b1e747b53e7c3d0d6df9',
                 'native_file': 'empty-live3-after-native.json',
                 'native_sha256': 'ef0bc5537f80cf3bcc7eacc9ee3b669e5d49df37fa283af667271fbb2390074c',
                 'inputs_file': 'empty-live3-after-inputs.json',
                 'inputs_sha256': 'be0224ca229820c162970bba46e8e9459d5c0aa2bbbd16b0fdd1c284fad5db18',
                 'store': 'empty/store',
                 'sql_rows': '1de02bb10ba40d03a21ea74622eb42cd21b0bf0cb445a72458e8e264d73338e4',
                 'native_rows': '6d1abd9990c01b2f8017381e331fe68b741d317c005c4cac5c33354d0348b71c',
                 'input_rows': '9a03a9b8b63c56b980c436ecb48b9096c24f8b07d1851b3f494968bcfd189e5a'},
 'activated-later-live4': {'name': 'activated-later-live4',
                           'run': 'ingest-succession-live-engine-006fe3d',
                           'subdirectory': 'live',
                           'database': 'bb_successor_live_006fe3d_activated_later',
                           'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
                           'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
                           'owner': '63d2ef283499bd9354aa47ad668ab38a',
                           'label': 'org.bigbrotr.succession-live',
                           'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
                           'source_clone': 'activated-later',
                           'final_case': 'activated-later-live4',
                           'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                           'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
                           'snapshot': 'activated-later-live4-after.json',
                           'snapshot_sha256': 'bc4355fdef03f0633a7407687cec92d33c2ddf2682a573faabd453d0ce9fa4fd',
                           'native_file': 'activated-later-live4-after-native.json',
                           'native_sha256': '5a96206f1e8bd78decc280a5765e51bc407b12e778381bccf0b1d167b462c275',
                           'inputs_file': 'activated-later-live4-after-inputs.json',
                           'inputs_sha256': '90127efe7298437df3866ac4c44e83779fb325de5624a9a2b81f3c102f183afd',
                           'store': 'activated-later/store',
                           'sql_rows': '5dda6553a36149030a8da9c245b9a1e5fc3b7f9eedfe3843f57cf768da9d5da2',
                           'native_rows': 'da23d31ff0a09b07506235cdb34f733423b271e9893aa02380506bd53455b2cd',
                           'input_rows': 'c9b2988b85bcd51f111f6c2c4ca5587e336e5277f5347895aabbab9032eff7ca'}}

CLONES = tuple(PINS)
CASES = tuple(name + '-' + step for name in CLONES for step in ('recover', 'retry'))
# Head, issued/reserved, current source cut, native rows/sequence, input count.
SHAPES = ((3, 0, 0, 73, 89, 6), (3, 1, 1, 79, 97, 7),
          (3, 1, 1, 79, 97, 7), (3, 3, 4, 90, 111, 9),
          (3, 3, 4, 72, 89, 7), (4, 3, 4, 93, 116, 9))


def specification(case):
    index = CASES.index(case['name'])
    origin, retry = CLONES[index // 2], bool(index % 2)
    assert case['origin'] == case['clone'] == origin
    assert case['operation'] == 'recover' and case['metadata_bytes'] == 262144
    assert case['predecessor'] == (origin + '-recover' if retry else None)
    assert case.get('proxy_port') is None
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return origin, retry, SHAPES[index // 2]


def science(pg, native, inputs, origin):
    """Reconstruct every receipt/closure, including old-than-parent dependencies."""
    head, issued, cut, *_ = SHAPES[CLONES.index(origin)]
    values = native_map(native)
    legacy = only(pg['initial_ingest_successions'])
    ds, resource, slot = (binary(legacy[k]) for k in ('dataset_id', 'owner_resource_id', 'producer_slot_id'))
    manifests = [binary(legacy['previous_manifest']), binary(legacy['target_manifest'])]
    records = [None, binary(legacy['handoff'])]
    for epoch in range(3, head + 1):
        intent = only(pg['ingest_successions'], target_epoch=epoch)
        manifests.append(binary(intent['target_manifest']))
        records.append(binary(intent['handoff']))
    assert len(pg['ingest_successions']) == head - 2
    assert values['default', '0101'] == manifests[-1].hex()
    counts = [4, 0 if origin == 'empty-live3' else 2] + [0] * (head - 3) + [issued]
    cuts = [12, 0 if origin == 'empty-live3' else 5] + [0] * (head - 3) + [cut]
    kinds = ['initial', 'unactivated-empty' if origin == 'empty-live3' else 'activated'] + ['unactivated-empty'] * (head - 3) + ['activated']
    summaries, requests, preimages = [], {}, {}

    def fact(epoch, sequence, preimage=None):
        source = [ds, 0, epoch, sequence]
        key = (b'\x01\x20' + ds + be(0, 4) + be(epoch) + be(sequence)).hex()
        value = bytes.fromhex(values['facts', key])
        assert value[:2] == b'\x01\x20' and sha(value[34:]) == value[2:34].hex()
        if preimage is not None:
            assert value == b'\x01\x20' + bytes.fromhex(sha(preimage)) + preimage
        return [source, value[2:34]]

    current = manifests[-1]
    domains = {k: current[106 + 18 * (k - 1):122 + 18 * (k - 1)] for k in range(1, 10)}
    scope = [ds, 4, manifests[0][160:176], 3]
    lineage = [ds, 9, domains[9], 1]
    corpus = json.dumps([live.OLD_EVENT, live.NEW_EVENT], separators=(',', ':'), ensure_ascii=False).encode()
    descriptor = [1, 'dataset-lineage', 1, dict(format='bigbrotr-finite-successor-events',
        format_version=1, source_dataset_id=None, source_artifact_digest=bytes.fromhex(sha(corpus)))]
    for n in range(1, issued + 1):
        if n == 1:
            intent, operation = [1, scope, [['dictionary', 1, [lineage, *descriptor]]]], 'admit-evidence'
        else:
            event = live.OLD_EVENT if n == 2 else live.NEW_EVENT
            intent = [1, scope, [*live.body(event), bytes.fromhex(event['sig'])], 'import', lineage, None]
            operation = 'admit'
        preimages[head, n] = b'bigbrotr/producer-request/1\0' + cbor([[slot, head, n], operation, 1, intent])
    if issued:
        raw = b'bigbrotr/fact/1\0' + cbor([[ds, 0, head, 1], 'dictionary', 1, scope, 900000001, [lineage, *descriptor]])
        fact(head, 1, raw)
    if issued == 3:
        event, at = live.NEW_EVENT, 900000005
        event_id = bytes.fromhex(event['id'])
        event_ref = [ds, 8, domains[8], 1]
        payloads = [('dictionary', [event_ref, 1, 'event', 1, dict(event_id=event_id)]),
                    ('event-payload', [*live.body(event), event_ref]),
                    ('event-eligibility', [event_id, bytes.fromhex(sha(b'bigbrotr/body/1\0' + cbor(live.body(event)))),
                        bytes.fromhex(event['sig']), at, 'import', lineage])]
        for n, (family, payload) in enumerate(payloads, 2):
            raw = b'bigbrotr/fact/1\0' + cbor([[ds, 0, head, n], family, 1, scope, at, payload])
            fact(head, n, raw)

    for epoch, (manifest, count, source_cut, kind) in enumerate(zip(manifests, counts, cuts, kinds), 1):
        schema = min(epoch, 3)
        assert len(manifest) == 266 and manifest[:10] == b'\x01\x01' + be(schema, 2) + be(1, 2) * 3
        assert manifest[10:26] == ds and manifest[26:34] == be(epoch - 1)
        assert manifest[34:62] == resource + be(0, 4) + be(epoch) and manifest[102:104] == b'\x01\x09'
        assert [int.from_bytes(manifest[104 + 18 * n:106 + 18 * n], 'big') for n in range(9)] == list(range(1, 10))
        marker = bytes.fromhex(values['receipts', (b'\x01\x31' + slot + be(epoch)).hex()])
        assert marker == b'\x02\x31' + be(count) + bytes(16) + b'\x01' or (epoch == head and marker == b'\x01\x31' + be(count) + bytes(16))
        assert values['default', (b'\x01\x02' + ds + be(0, 4) + be(epoch)).hex()] == (b'\x01\x02' + be(source_cut)).hex()
        if epoch == 1:
            prefix = b'bigbrotr/initial-source-closure/1\0'
        else:
            record = records[epoch - 1]
            previous = summaries[-1]
            if epoch == 2:
                expected_record = b'\x01\x0a' + slot + be(1) + be(4) + be(12) + bytes.fromhex(previous['digest']) + b'\x01\x0a' + manifests[0] + b'\x01\x0a' + manifest
                domain = 'initial-empty-attempt' if kind == 'unactivated-empty' else 'activated-initial-source'
            else:
                previous_kind = 1 if kinds[epoch - 2] == 'activated' else 2
                expected_record = (b'\x02\x0a' + bytes([previous_kind]) + slot + be(epoch - 1)
                    + be(counts[epoch - 2]) + be(cuts[epoch - 2]) + bytes.fromhex(previous['digest'])
                    + bytes.fromhex(sha(records[epoch - 2])) + be(266, 2) + manifests[epoch - 2] + be(266, 2) + manifest)
                domain = 'ingest-successor-empty' if kind == 'unactivated-empty' else 'ingest-successor-source'
            assert record == expected_record
            assert values['default', (b'\x01\x0a' + ds + resource + be(epoch - 1)).hex()] == record.hex()
            prefix = b'bigbrotr/' + domain.encode() + b'-closure/1\0' + bytes.fromhex(sha(record))
        prefix += bytes.fromhex(sha(manifest)) + slot + be(epoch) + be(count)
        for n in range(1, count + 1):
            request = only(pg['producer_requests'], producer_epoch=epoch, request_sequence=n)
            name = slot.hex() + f'-{epoch:016x}-{n:016x}.cbor'
            assert inputs[name] == dict(bytes=request['input_bytes'], sha256=request['request_digest'][2:])
            if epoch == head:
                raw = preimages[epoch, n]
                assert request['input_bytes'] == len(raw) and binary(request['request_digest']) == bytes.fromhex(sha(raw))
                members = [1] if n == 1 else [] if n == 2 else [2, 3, 4]
                batch = [(head, k) for k in members]
                deps = ([(1, 3), (head, 1)] if n == 1 else
                        [(1, k) for k in (3, 8, 9, 10)] + [(head, 1)] if n == 2 else
                        [(1, 3)] + [(head, k) for k in range(1, 5)])
            elif epoch == 1:
                a, b = ((1, 2), (3, 3), (4, 7), (8, 12))[n - 1]
                batch = [(1, k) for k in range(a, b + 1)]
                deps = [(1, k) for k in ((1, 2), (3,), range(3, 8), range(3, 13))[n - 1]]
            else:
                assert epoch == 2
                batch = [(2, 1)] if n == 1 else [(2, k) for k in range(2, 6)]
                deps = [(1, 3)] + [(2, k) for k in range(1, 2 if n == 1 else 6)]
            outcome_kind = 1 if batch else 2
            policy = int.from_bytes(manifest[94:102], 'big')
            receipt = b'bigbrotr/receipt/1\0' + cbor([outcome_kind, [slot, epoch, n], binary(request['request_digest']),
                [ds, epoch - 1, 1, manifest[78:94], policy], [fact(*v) for v in batch] if batch else None,
                [fact(*v) for v in deps]])
            assert values['receipts', (b'\x01\x30' + slot + be(epoch) + be(n)).hex()] == receipt.hex(), 'literal receipt differs'
            assert binary(request['manifest_sha256']) == bytes.fromhex(sha(manifest))
            outcomes = [r for r in pg['producer_outcomes'] if r['producer_epoch'] == epoch and r['request_sequence'] == n]
            if outcomes:
                outcome, = outcomes
                assert binary(outcome['outcome_bytes']) == receipt and outcome['outcome_sha256'] == '\\x' + sha(receipt)
                assert outcome['request_digest'] == request['request_digest'] and outcome['manifest_sha256'] == request['manifest_sha256']
                assert outcome['policy_epoch'] == policy and outcome['outcome_kind'] == outcome_kind
            else:
                assert origin == 'outcome-before-loss' and (epoch, n) == (head, 1)
            requests[epoch, n] = dict(receipt=receipt, kind=outcome_kind, policy=policy)
            prefix += be(n) + binary(request['request_digest']) + bytes.fromhex(sha(receipt))
        digest = sha(prefix + be(source_cut))
        if epoch <= 2:
            assert digest == history.CLOSURES[0 if epoch == 1 else 2 if kind == 'unactivated-empty' else 1]
        source = only(pg['source_streams'], owner_epoch=epoch)
        producer = only(pg['producer_epochs'], producer_epoch=epoch)
        assert producer['reserved_through_request'] == count
        if epoch < head:
            assert source['state'] == 'closed' and source['closure_digest'] == '\\x' + digest
            assert source['last_complete_sequence'] == source_cut and producer['state'] == 'retired'
        else:
            assert source['state'] == 'open' and source['closure_digest'] is None and source['last_complete_sequence'] is None
            assert producer['state'] in ('open', 'closing')
            assert only(pg['ingest_activations'], target_epoch=head)
        summaries.append(dict(epoch=epoch, terminal_kind=kind, resolved_requests=count,
            last_complete_sequence=source_cut, digest=digest,
            event_request_mask=8 if epoch == 1 else (2 if epoch == 2 and count else 6 if epoch == head and count == 3 else 0)))
    assert len(inputs) == sum(counts) and len(pg['producer_requests']) == sum(counts)
    return dict(head=head, slot=slot, manifest=manifests[-1], counts=counts, summaries=summaries,
        requests=requests, preimages=preimages, issued=issued, cut=cut)


class Model(fencing.Model):
    """Reuse unchanged Control fencing; add only closing and positive-copy deltas."""
    def close(self, observed, known):
        epoch, count = known['head'], known['issued']
        row = only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
        if row['state'] == 'open':
            actual = only(observed['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
            row.update(state='closing', xmin=self.fresh(actual['xmin']))
        else:
            assert row['state'] == 'closing'
        values = native_map(self.native)
        key = ('receipts', (b'\x01\x31' + binary(self.slot) + be(epoch)).hex())
        closed = (b'\x02\x31' + be(count) + bytes(16) + b'\x01').hex()
        if values[key] != closed:
            assert values[key] == (b'\x01\x31' + be(count) + bytes(16)).hex()
            values[key] = closed
            self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 1)
        same(self.pg, observed)

    def resolve(self, observed, known, epoch, sequence):
        copies = [r for r in self.pg['producer_outcomes'] if r['producer_epoch'] == epoch and r['request_sequence'] == sequence]
        if not copies:
            assert epoch == known['head']
            row = only(observed['producer_outcomes'], producer_epoch=epoch, request_sequence=sequence)
            request = only(self.pg['producer_requests'], producer_epoch=epoch, request_sequence=sequence)
            scientific, c = known['requests'][epoch, sequence], self.control
            at = self.at(row['recorded_at_us'])
            assert at < c['lease_expires_at_us']
            base = {k: request[k] for k in ('producer_slot_id', 'producer_epoch', 'request_sequence',
                'dataset_id', 'request_digest', 'manifest_sha256')}
            self.pg['producer_outcomes'].append(dict(base, outcome_kind=scientific['kind'], policy_epoch=scientific['policy'],
                outcome_bytes=bx(scientific['receipt']), outcome_sha256='\\x' + sha(scientific['receipt']),
                recorder_resource=c['resource_id'], recorder_epoch=c['epoch'], recorder_role='control',
                recorder_incarnation=c['process_incarnation'], recorded_at_us=at, xmin=self.fresh(row['xmin'])))
        same(self.pg, observed)


class Transcript(transition.Transcript):
    def recovery(self, name):
        assert self.position < len(self.events)
        event = self.events[self.position]
        self.position += 1
        assert event['event'] == 'lineage-recovery-' + name
        return event

    def refresh_recovery(self, epoch, sequence):
        stage = f"recovery-refresh-returned-{epoch if epoch is not None else 'final'}-{sequence if sequence is not None else 'epoch'}"
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
    assert (len(native['rows']), native['sequence'], len(inputs)) == (rows, sequence + retry, input_count)
    known = science(before, native, inputs, origin)
    assert only(before['producer_epochs'], producer_epoch=head)['state'] == ('closing' if retry else 'open')
    if retry:
        assert len(before['producer_outcomes']) == sum(known['counts'])
    m = Model(before, native, inputs, server_window)
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
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation='recover',
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
    t.snapshot('before-recovery')
    t.snapshot('recovery-inspect-returned')
    exact(t.recovery('loaded'), dict(event='lineage-recovery-loaded', epoch=head, manifest=known['manifest'].hex(),
        producer_slot=known['slot'].hex(), issued_through=issued, reserved_through=issued, staged_input=None,
        barrier_confirmed=False, sql_producer_state='closing' if retry else 'open', sql_source_state='open'))
    t.snapshot('before-recovery-close')
    t.snapshot('recovery-close-returned', lambda pg: m.close(pg, known))
    exact(t.recovery('closed'), dict(event='lineage-recovery-closed', epoch=head, confirmed=True,
        issued_through=issued, reserved_through=issued, staged_input=None, error=None))
    for epoch, count in enumerate(known['counts'], 1):
        for n in range(1, count + 1):
            t.refresh_recovery(epoch, n)
            t.snapshot(f'recovery-request-returned-{epoch}-{n}', lambda pg: m.resolve(pg, known, epoch, n))
            exact(t.recovery('request'), dict(event='lineage-recovery-request', producer_slot=known['slot'].hex(),
                epoch=epoch, sequence=n, copy_mode='existing-terminal' if epoch < head else 'current-positive',
                reservation=only(m.pg['producer_requests'], producer_epoch=epoch, request_sequence=n),
                outcome=only(m.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=n)))
        t.refresh_recovery(epoch, None)
        t.snapshot(f'recovery-epoch-returned-{epoch}')
        exact(t.recovery('epoch'), dict(event='lineage-recovery-epoch', summary=known['summaries'][epoch - 1]))
    t.refresh_recovery(None, None)
    t.snapshot('recovery-finish-returned')
    no_authority = dict(source_closure=False, workflow=False, retirement=False, admission=False, activation=False)
    exact(t.recovery('proof'), dict(event='lineage-recovery-proof', verified_through=head,
        last_epoch=known['summaries'][-1], matches_recovery=True, **no_authority))
    observation = dict(target_epoch=head, issued_through=issued, reserved_through=issued, staged_input=None,
        verified_through=head, last_epoch=known['summaries'][-1], positive_recovery=True)
    exact(t.recovery('result'), dict(event='lineage-recovery-result', operation='recover', stage='complete',
        epoch=None, sequence=None, error=None, observation=observation))
    t.snapshot('after-recovery')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='control', failed=False, joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    exact(t.take('control'), dict(event='lineage-history-control', phase='released-and-joined',
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', m.release)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    exact(t.recovery('complete'), dict(event='lineage-recovery-complete', operation='recover', native='closed',
        control='released-and-joined', observer='joined', success=True, **no_authority))
    assert t.position == len(events), 'unclassified trailing event'
    assert t.snapshots == 12 + 2 * (sum(known['counts']) + head)
    assert m.refreshes == sum(known['counts']) + head + 1
    return dict(status='PASS', case=case['name'], database=case['database'], snapshots=t.snapshots,
        tables=75, native_rows=rows, sequence=m.native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, observation=observation,
        scope='Finite activated positive recovery and exact retry; no source/workflow/retirement, writer, installed or RSS authority')


def negative_controls(events, before, native, inputs, case, server_window):
    """Coherently rehashed capture mutations; these are reader checks only."""
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
            elif active and value['event'] == 'lineage-recovery-request':
                wrapped = {'producer_requests': [value['reservation']], 'producer_outcomes': [value['outcome']]}
                # Caller may use only table-independent mutation for propagated
                # request observations; other controls fail at their first state.
                if getattr(mutation, 'copies', False):
                    mutation(wrapped)

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

    def existing_copy(values):
        def change(pg):
            for row in pg['producer_outcomes']:
                if row['producer_epoch'] == 1 and row['request_sequence'] == 4:
                    row.update(recorded_at_us=row['recorded_at_us'] + 1, xmin='3999999999')
        change.copies = True
        sql(values, change, 'recovery-request-returned-1-4')

    def summary_change(values):
        for value in values:
            if value['event'] == 'lineage-recovery-epoch' and value['summary']['epoch'] == head:
                value['summary']['digest'] = '00' * 32
            if value['event'] == 'lineage-recovery-proof':
                value['last_epoch']['digest'] = '00' * 32
            if value['event'] == 'lineage-recovery-result':
                value['observation']['last_epoch']['digest'] = '00' * 32

    reject('historical-row-xmin-rehashed-throughout', lambda v: sql(v,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='3999999999')))
    reject('native-manifest-rehashed-throughout', native_change)
    reject('existing-copy-time-xmin-rewritten-coherently', existing_copy)
    reject('current-source-published', lambda v: sql(v,
        lambda pg: only(pg['source_streams'], owner_epoch=head).update(state='closed'), 'recovery-finish-returned'))
    reject('activation-rewritten', lambda v: sql(v,
        lambda pg: only(pg['ingest_activations'], target_epoch=head).update(xmin='3999999999'), 'recovery-close-returned'))
    reject('native-closing-put-omitted-or-repeated', lambda v:
        event(v, 'lineage-history-state', stage='recovery-close-returned')['state'].update(sequence=native['sequence'] + (1 if retry else 0)))
    reject('input-changed', lambda v:
        event(v, 'lineage-history-state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('coherent-current-closure-digest', summary_change)
    reject('head-issued-invented', lambda v: event(v, 'lineage-recovery-loaded').update(issued_through=shape[1] + 1))
    reject('tail-input-invented', lambda v: event(v, 'lineage-recovery-loaded').update(staged_input={}))
    reject('barrier-confirmed-before-close', lambda v: event(v, 'lineage-recovery-loaded').update(barrier_confirmed=True))
    reject('request-omitted', lambda v: v.pop(next(i for i, e in enumerate(v) if e['event'] == 'lineage-recovery-request')))
    reject('old-copy-relabelled-current', lambda v: event(v, 'lineage-recovery-request', epoch=1, sequence=1).update(copy_mode='current-positive'))
    reject('proof-invents-retirement-authority', lambda v: event(v, 'lineage-recovery-proof').update(retirement=True))
    reject('complete-invents-writer-authority', lambda v: event(v, 'lineage-recovery-complete').update(admission=True))
    reject('control-driver-not-joined', lambda v: event(v, 'lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: event(v, 'lineage-history-driver', driver='observer').update(failed=True))
    reject('backend-not-absent', lambda v: event(v, 'lineage-history-backend', present=False).update(present=True))
    reject('unexpected-failure', lambda v: event(v, 'lineage-recovery-result').update(error='CommitUnknown'))
    current = max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control') + 1
    claim = next(e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'after-claim')
    claimed_pg = {k: loads(v) for k, v in claim['state']['control_tables']}
    claim_xid = only(claimed_pg['owner_epochs'], role='control', epoch=current)['xmin']
    if not retry:
        reject('closing-reuses-claim-transaction', lambda v: sql(v,
            lambda pg: only(pg['producer_epochs'], producer_epoch=head).update(xmin=claim_xid), 'recovery-close-returned'))
    if origin == 'outcome-before-loss' and not retry:
        def bad_record(pg):
            for row in pg['producer_outcomes']:
                if (row['producer_epoch'], row['request_sequence']) == (head, 1):
                    row['recorder_epoch'] = current - 2
        bad_record.copies = True
        reject('repaired-copy-attributed-to-old-Control', lambda v: sql(v, bad_record, f'recovery-request-returned-{head}-1'))
        reject('missing-copy-not-repaired', lambda v: sql(v,
            lambda pg: pg['producer_outcomes'].__setitem__(slice(None), [r for r in pg['producer_outcomes'] if r['producer_epoch'] != head]),
            f'recovery-request-returned-{head}-1'))
    old = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    if old['state'] == 'active':
        def split(pg):
            only(pg['owner_epochs'], role='control', epoch=old['epoch'])['xmin'] = '3999999999'
            only(pg['role_instances'], role_instance_id=old['role_instance_id'])['xmin'] = '3999999999'
        reject('prior-fence-outside-fresh-claim-transaction', lambda v: sql(v, split, 'after-claim'))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind prior pins, full raw response, process boundaries and external state."""
    root = Path(root).resolve()
    folder, bound = root / 'recovery', {}

    def raw(path):
        path = Path(path)
        content = path.read_bytes()
        bound[str(path)] = sha(content)
        return content

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE'
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
    return dict(status='PASS_SUCCESSOR_RECOVERY_FIRST_WAVE_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Twelve finite positive recovery/retry processes; no installed, source/workflow/retirement, tail, failure-injection or RSS qualification')


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
