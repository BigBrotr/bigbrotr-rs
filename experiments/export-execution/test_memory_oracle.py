#!/usr/bin/env python3
"""Offline regression controls over a retained capture; never modify its files.

Matched selection mutations exercise the scope/manifest checks after the earlier
selection comparison. Artifact pins and the genuine baseline are checked by the
unmodified verification call first; these mutations are verifier controls, not
new runtime evidence.
"""
import copy
import json
from pathlib import Path
import sys
from unittest.mock import patch

import memory_oracle as oracle


def run(root):
    baseline = oracle.verify(root)
    initial = oracle.load(root / 'input.json')
    science_path = Path(initial['spec']['science'])
    science = oracle.load(science_path)
    original_scope = science['query']['scope']
    events = [json.loads(line) for line in (root / 'stdout').read_text().splitlines()]
    artifact = next(row['fields']['artifact'] for row in events if row.get('name') == 'reserved')
    manifest_path = Path(initial['case']['destination']) / artifact / 'manifest.json'
    original_manifest = manifest_path.read_bytes()
    original_load, read_bytes, read_text = oracle.load, Path.read_bytes, Path.read_text
    controls = []

    def check(name, *, scope=None, manifest=None, passes=False,
              reason='literal complete manifest bytes'):
        def load(path, maximum=16 * 1024 * 1024):
            value = original_load(path, maximum)
            if Path(path) == science_path and scope is not None:
                value = copy.deepcopy(value)
                value['query']['scope'] = scope
            return value

        def bytes_value(path):
            return manifest if path == manifest_path and manifest is not None else read_bytes(path)

        def text_value(path, *args, **kwargs):
            if path == root / 'stdout' and scope is not None:
                changed = copy.deepcopy(events)
                for row in changed:
                    if row.get('name') == 'reserved':
                        row['fields']['query']['scope'] = scope
                return ''.join(json.dumps(row) + '\n' for row in changed)
            return read_text(path, *args, **kwargs)

        with patch.object(oracle, 'load', load), patch.object(Path, 'read_bytes', bytes_value), \
                patch.object(Path, 'read_text', text_value):
            try:
                result = oracle.verify(root)
            except ValueError as error:
                if passes:
                    raise
                if str(error) != reason:
                    raise AssertionError('wrong rejection boundary: ' + name) from error
                controls.append(dict(name=name, status='REJECTED', reason=str(error)))
            else:
                if not passes or result != baseline:
                    raise AssertionError('incorrect oracle acceptance: ' + name)
                controls.append(dict(name=name, status='PASS_SAME_RESULT'))

    fields = ('dataset_id', 'dictionary_kind', 'allocation_domain', 'ordinal')
    for index in range(len(fields)):
        order = fields[index:] + fields[:index]
        check('scope-key-rotation-' + str(index),
              scope={key: original_scope[key] for key in order}, passes=True)
    check('scope-extra-key', scope=dict(original_scope, unrelated='forbidden'), reason='closed qualified scope')
    for key in fields:
        check('scope-missing-' + key, scope={k: v for k, v in original_scope.items() if k != key},
              reason='closed qualified scope')
    check('scope-wrong-value', scope=dict(original_scope, ordinal='999'))
    check('scope-wrong-type', scope=dict(original_scope, dictionary_kind='4'))
    manifest = json.loads(original_manifest)
    check('manifest-key-order', manifest=oracle.encoded(dict(reversed(list(manifest.items())))) + b'\n')
    check('manifest-extra-field', manifest=oracle.encoded(dict(manifest, unrelated=True)) + b'\n')
    check('manifest-duplicate-key', manifest=b'{"format":"bigbrotr-signed-events-export/1",' + original_manifest[1:])
    check('manifest-whitespace', manifest=b' ' + original_manifest)
    check('manifest-missing-newline', manifest=original_manifest[:-1])
    changed = copy.deepcopy(manifest)
    changed['selection']['scope']['ordinal'] = '999'
    check('manifest-wrong-scope-value', manifest=oracle.encoded(changed) + b'\n')
    return dict(status='PASS_OFFLINE_ORACLE_REGRESSION', baseline=baseline, controls=controls,
                scope='Read-only retained capture; virtual semantic/byte mutations, no engine or file changes')


def installed(payload):
    """Negative controls over an actual retained installed_five call payload."""
    # Resolve the same inert oracle dependencies as the setup owner. This import
    # performs no setup: all executable phases remain behind its main guard.
    import memory_setup
    from memory_setup_oracle import installed_five
    del memory_setup

    require_keys = {'before_capture', 'after_capture', 'selection', 'principal',
                    'commands', 'statuses', 'destination', 'science'}
    oracle.require(set(payload) == require_keys, 'closed installed oracle test payload')
    baseline = installed_five(**payload)
    controls = []
    new_jobs = {row['receipt']['job_id'] for row in payload['commands']}

    def row(changed, table, predicate=lambda value: True):
        before = changed['before_capture']['control'][table]
        return next(value for value in changed['after_capture']['control'][table]
                    if value not in before and predicate(value))

    def check(name, mutate):
        changed = copy.deepcopy(payload)
        mutate(changed)
        try:
            installed_five(**changed)
        except (ValueError, KeyError, StopIteration):
            controls.append(dict(name=name, status='REJECTED'))
        else:
            raise AssertionError('incorrect installed oracle acceptance: ' + name)

    check('missing-fifth-status', lambda value: value['statuses'].pop())
    check('changed-catalog', lambda value: value['after_capture']['catalog'].clear())
    check('missing-Control-table', lambda value: value['after_capture']['control'].pop('schema_versions'))
    check('duplicate-command-key', lambda value: value['commands'][1].update(key=value['commands'][0]['key']))
    check('historical-receipt', lambda value: value['commands'][0]['receipt'].update(created=False))
    check('false-terminal-status', lambda value: value['statuses'][0].update(state='running'))
    check('wrong-checkpoint', lambda value: row(value, 'jobs').update(checkpoint_revision=0))
    check('wrong-reservation-cap', lambda value: row(value, 'export_execution_units').update(max_output_bytes=1))
    check('wrong-artifact-digest', lambda value: row(value, 'artifacts').update(manifest_sha256='\\x' + 'aa' * 32))
    check('wrong-file-row-count', lambda value: row(value, 'artifact_files').update(row_count=0))
    check('missing-reader-release', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'released').update(state='active'))
    check('released-export-retention', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'active').update(state='released'))
    check('changed-retention-cut', lambda value: row(value, 'retention_claims').update(last_sequence=0))
    check('unrelated-audit-action', lambda value: row(value, 'management_audit').update(action='unrelated'))
    check('unreleased-Control', lambda value: row(value, 'owner_epochs').update(state='active'))
    check('rewritten-old-job', lambda value: next(item for item in value['after_capture']['control']['jobs']
        if item['job_id'][2:] not in new_jobs).update(xmin='999999999'))
    check('renewed-export-pin', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'active').update(lease_revision=2))
    return dict(status='PASS_OFFLINE_INSTALLED_ORACLE_REGRESSION', baseline=baseline, controls=controls,
                scope='Virtual mutations of caller-pinned retained captures only; no database, file or runtime changes')


def boundaries():
    """Offline signed-input/decoder/census controls; no product parser or engines."""
    import importlib.util
    import subprocess
    import memory_setup  # Establish the existing inert oracle imports only.
    from bootstrap_oracle import cbor
    from installed_sync_oracle import decode
    from installed_archive_read import decode_row
    import memory_relay as relay

    root = Path(__file__).resolve().parents[2]
    path = root / 'tests/fixtures/protocol/write_archive_event_response.py'
    spec = importlib.util.spec_from_file_location('boundary_author', path)
    author = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(author)
    sys.path.insert(0, str(root / 'experiments/identity-contract'))
    from signed_fixture import sign, verified

    controls = []

    def reject(name, call, reason=None):
        try:
            call()
        except (AssertionError, ValueError) as error:
            if reason is not None:
                assert str(error) == reason, (name, str(error))
            controls.append(dict(name=name, status='REJECTED', reason=str(error)))
        else:
            raise AssertionError('unexpected offline boundary acceptance: ' + name)

    subscription = '01' * 16 + '00' * 8
    events, metrics = {}, {}
    for case in author.CASES:
        raw, expected = author.build(case)
        name = 'archive-event-response.json' if case == 'archive-sample' else 'archive-event-' + case + '.json'
        path = root / 'tests/fixtures/protocol' / name
        assert path.read_bytes() == raw, 'unchanged independently reauthored fixture bytes'
        kind = 'frozen-archive-recipe' if case == 'archive-sample' else 'frozen-signed-event'
        event = oracle.event_expected(dict(kind=kind, path=str(path), sha256=oracle.digest(raw)))
        assert verified(event)
        body = relay.signed_event(path, kind=kind)
        frame = relay.event_frame(subscription, body, kind=kind)
        assert frame[:2] == b'\x81\x7f' and int.from_bytes(frame[2:10], 'big') == len(body) + 61
        assert frame[10:] == b'["EVENT","' + subscription.encode() + b'",' + body + b']'
        assert len(frame) == len(body) + 71 and oracle.digest(body) == expected['signed_event_sha256']
        # Count keys and containers recursively with a JSON parse tree, separately
        # from the author's closed-shape formula and the product's lexical scanner.
        def nodes(value):
            if isinstance(value, dict):
                return 1 + len(value) + sum(nodes(item) for item in value.values())
            if isinstance(value, list):
                return 1 + sum(map(nodes, value))
            return 1
        assert nodes(json.loads(frame[10:])) == expected['envelope_nodes']
        if case == 'archive-sample':
            assert relay.signed_event(path) == body and relay.event_frame(subscription, body) == frame
        else:
            reject(case + '-old-frame-default', lambda: relay.event_frame(subscription, body))
            reject(case + '-old-census-envelope', lambda: decode_row(oracle.encoded(event), 262144),
                   'native-reader-row-byte-bound')
            assert decode_row(oracle.encoded(event), 1048576) == event
        event_body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']),
                      event['created_at'], event['kind'], event['tags'], event['content']]
        encoded_body = cbor(event_body)
        assert decode(encoded_body, maximum=524288, collection_items=65536, max_nodes=131072) == event_body
        assert len(encoded_body) + 4096 < 524288 < 1048576, 'body plus finite metadata headroom'
        metrics[case] = dict(**expected, frame_bytes=len(frame), body_cbor_bytes=len(encoded_body))
        events[case] = event
    script = ("const fs=require('fs'),c=require('crypto');const events=JSON.parse(fs.readFileSync(0,'utf8'));"
              "console.log(JSON.stringify(Object.fromEntries(Object.entries(events).map(([k,e])=>"
              "[k,{id:c.createHash('sha256').update(JSON.stringify([0,e.pubkey,e.created_at,e.kind,e.tags,e.content])).digest('hex'),"
              "bytes:Buffer.byteLength(JSON.stringify(e))}]))));")
    crosscheck = json.loads(subprocess.check_output(['node', '-e', script], input=oracle.encoded(events)))
    assert all(crosscheck[name] == dict(id=event['id'], bytes=metrics[name]['signed_event_bytes'])
               for name, event in events.items()), 'independent Node canonical ID and normalized bytes'

    # Both overbounds remain valid signatures; each isolates exactly one limit.
    value = events['content-heavy']
    extra_byte = sign(3, 1, 1, value['tags'], value['content'] + 'a')
    assert verified(extra_byte) and len(oracle.encoded(extra_byte)) == 262145
    reject('valid-signed-byte-plus-one', lambda: author.supported(extra_byte),
           'installed normalized event byte bound')
    value = events['tag-heavy']
    tags = copy.deepcopy(value['tags'])
    tags[0].append('')  # Adds one node and three encoded bytes.
    extra_node = sign(3, 1, 1, tags, value['content'][:-3])
    assert verified(extra_node) and len(oracle.encoded(extra_node)) == 262144
    assert 18 + sum(1 + len(tag) for tag in tags) == 65537
    reject('valid-signed-node-plus-one', lambda: author.supported(extra_node),
           'whole EVENT envelope node bound')
    for size in (relay.MAX_TEXT_BYTES, relay.BOUNDARY_TEXT_BYTES):
        assert relay.server_text_frame(b'x' * size, maximum=size)
        reject('relay-byte-plus-one-' + str(size),
               lambda: relay.server_text_frame(b'x' * (size + 1), maximum=size), 'finite text bound')
    assert decode(cbor([''] * 1024)) == [''] * 1024
    reject('unchanged-default-collection-bound', lambda: decode(cbor([''] * 1025)),
           'fixture CBOR collection bound')
    options = dict(maximum=524288, collection_items=65536, max_nodes=131072)
    assert decode(cbor([''] * 65536), **options) == [''] * 65536
    reject('explicit-collection-plus-one', lambda: decode(cbor([''] * 65537), **options),
           'fixture CBOR collection bound')
    dense = [[''] * 65535, [''] * 65534]  # Three containers +131069 scalar nodes.
    assert decode(cbor(dense), **options) == dense
    dense[1].append('')
    reject('explicit-total-nodes-plus-one', lambda: decode(cbor(dense), **options),
           'fixture CBOR node bound')
    for maximum, header in ((65536, 3), (262144, 5), (524288, 5)):
        exact = 'x' * (maximum - header)
        assert len(cbor(exact)) == maximum and decode(cbor(exact), maximum=maximum) == exact
        reject('CBOR-byte-plus-one-' + str(maximum), lambda: decode(cbor(exact + 'x'), maximum=maximum))
    for maximum in (65536, 262144, 1048576):
        exact = b'"' + b'x' * (maximum - 3) + b'"'
        assert len(exact) == maximum - 1 and len(decode_row(exact, maximum)) == maximum - 3
        reject('census-exact-exclusive-bound-' + str(maximum),
               lambda: decode_row(b' ' + exact, maximum), 'native-reader-row-byte-bound')
    return dict(status='PASS_OFFLINE_SUPPORTED_INPUT_BOUNDARIES', inputs=metrics,
        independent_node_crosscheck=crosscheck, controls=controls,
        scope='Frozen signing/ID/byte/node, relay and independent decoder/census helper checks only. '
              'No production parser refusal, publication, native reader or memory/runtime qualification.')


def native_rows():
    """Author/verifier unit controls, not a fabricated native execution capture."""
    import importlib.util
    import subprocess
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / 'experiments/archive-engine'))
    import signed_publication_oracle as science
    path = root / 'tests/fixtures/protocol/write_archive_event_response.py'
    spec = importlib.util.spec_from_file_location('native_row_author', path)
    author = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(author)
    sys.path.insert(0, str(root / 'experiments/identity-contract'))
    from signed_fixture import verified

    controls, inputs, crosscheck = [], {}, {}
    def reject(name, call):
        try:
            call()
        except (AssertionError, ValueError):
            controls.append(dict(name=name, status='REJECTED_OFFLINE'))
        else:
            raise AssertionError('incorrect native-row oracle acceptance: ' + name)

    # Preserve the three earlier literal byte sequences, not merely their lengths.
    for case in author.CASES:
        raw, _ = author.build(case)
        name = 'archive-event-response.json' if case == 'archive-sample' else 'archive-event-' + case + '.json'
        assert (path.parent / name).read_bytes() == raw
    original_policy = oracle.load(root / 'experiments/archive-engine/receipt-policy.json')
    policy = oracle.load(root / 'experiments/archive-engine/native-row-policy.json')
    expected_policy = copy.deepcopy(original_policy)
    expected_policy['settings']['ingest.max_event_bytes'] = 1048576
    assert policy == expected_policy, 'one explicitly selected fresh fixture policy difference'
    identities = set()
    for mode, case in zip(science.NATIVE_ROW_MODES, author.NATIVE_CASES):
        raw, metrics = author.build(case)
        assert (path.parent / ('archive-event-' + case + '.json')).read_bytes() == raw
        value = science.mode_event(mode)
        assert verified(value) and len(raw) < policy['settings']['ingest.max_event_bytes']
        reject(mode + '-old-installed-input-limit', lambda: author.supported(value))
        row = science.native_payload_json(value)
        assert len(row) == 1048576 + (mode == 'row_over')
        batches, _ = science.corpus(value)
        assert all(len(bytes.fromhex(fact['preimage_hex'])) < 262144 for batch in batches for fact in batch['facts'])
        assert len(value['content'].encode()) < 262144 and not value['tags']
        new_ids = {bytes(item['attempt_id']) for item in science.input_expectations(mode)}
        assert len(new_ids) == 4 and not identities.intersection(new_ids)
        assert all(item[0] < 180 and len(set(item)) == 1 for item in new_ids)
        identities.update(new_ids)
        readback = science.expected(mode)
        observation = dict(result=dict(status='refused', error='ResourceLimit') if mode == 'row_over'
            else dict(status='observed', snapshot_id='3', event=value),
            elapsed_ms=0, cancelled=False, active_permits=0,
            profile=dict(name='export-native-row/1', engine_bytes=64*1024*1024,
                native_reserve_bytes=96*1024*1024, row_bytes=1048576, max_records=2,
                threads=1, deadline_ms=5000, scan_bytes=256*1024*1024))
        assert science.check_native_row(mode, readback, observation) == dict(
            bytes=len(row), sha256=oracle.digest(row))
        mutations = [
            ('truncated-preclamp-row', lambda r, o: r.update(native_payload_json=r['native_payload_json'][:-1])),
            ('different-scientific-content', lambda r, o: r['tables']['events'][0].update(content='bad')),
            ('wrong-fact-digest', lambda r, o: r['tables']['events'][0].update(payload_digest='00'*32)),
            ('wrong-native-budget', lambda r, o: o['profile'].update(native_reserve_bytes=384*1024*1024)),
            ('longer-deadline', lambda r, o: o['profile'].update(deadline_ms=15000)),
            ('retained-permit', lambda r, o: o.update(active_permits=1)),
            ('cancelled-observation', lambda r, o: o.update(cancelled=True)),
            ('wrong-result', lambda r, o: o.update(result=dict(status='absent'))),
            ('unexpected-metadata', lambda r, o: o.update(unrelated=True)),
        ]
        for label, change in mutations:
            changed_read, changed_observation = copy.deepcopy(readback), copy.deepcopy(observation)
            change(changed_read, changed_observation)
            reject(mode + '-' + label, lambda: science.check_native_row(mode, changed_read, changed_observation))
        if mode == 'row_over':
            changed = copy.deepcopy(observation)
            changed['result'] = dict(status='observed', snapshot_id='3', event=value)
            reject('valid-overbound-cannot-pass', lambda: science.check_native_row(mode, readback, changed))
            changed['result'] = dict(status='refused', error='ObservationUnknown')
            reject('unrelated-refusal-cannot-pass', lambda: science.check_native_row(mode, readback, changed))
        crosscheck[mode] = dict(event=value, row=json.loads(row))
        inputs[mode] = metrics
    script = ("const f=require('fs'),c=require('crypto'),v=JSON.parse(f.readFileSync(0,'utf8'));"
        "console.log(JSON.stringify(Object.fromEntries(Object.entries(v).map(([k,{event:e,row:r}])=>[k,"
        "{id:c.createHash('sha256').update(JSON.stringify([0,e.pubkey,e.created_at,e.kind,e.tags,e.content])).digest('hex'),"
        "row_bytes:Buffer.byteLength(JSON.stringify(r)),row_sha256:c.createHash('sha256').update(JSON.stringify(r)).digest('hex')}]))));")
    separate = json.loads(subprocess.check_output(['node', '-e', script], input=oracle.encoded(crosscheck)))
    assert all(separate[mode] == dict(id=metrics['event_id'], row_bytes=metrics['native_row_bytes'],
        row_sha256=metrics['native_row_sha256']) for mode, metrics in inputs.items())
    return dict(status='PASS_OFFLINE_NATIVE_ROW_AUTHOR_AND_ORACLE', inputs=inputs, controls=controls,
        node_crosscheck=separate,
        scope='Signed literals and constructed verifier unit inputs only. No observed publication, native row, refusal, custody or memory/runtime result.')


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '--native-row':
        print(json.dumps(native_rows(), indent=2))
    elif len(sys.argv) == 2 and sys.argv[1] == '--boundaries':
        print(json.dumps(boundaries(), indent=2))
    elif len(sys.argv) == 3 and sys.argv[1] == '--installed':
        print(json.dumps(installed(oracle.load(sys.argv[2])), indent=2))
    else:
        print(json.dumps(run(Path(sys.argv[1])), indent=2))
