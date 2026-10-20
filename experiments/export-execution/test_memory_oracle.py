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



def allocations():
    """Pure counter-verifier units; no claimed process, allocator or SQL execution."""
    def snap(live, peak):
        return dict(scope=oracle.ALLOCATION_SCOPE, live_bytes=live, peak_bytes=peak,
                    accounting_valid=True)
    # Separate serialized streams deliberately interleave in peak magnitude.
    # A nonzero first/final live value is valid; neither reset nor finalzero exists.
    markers = [dict(rust_allocations=snap(3, 3)), dict(rust_allocations=snap(2, 9))]
    ledger = [dict(rust_allocations=snap(1, 4)), dict(rust_allocations=snap(1, 8))]
    terminal = dict(event='export-allocation-terminal', profile=oracle.ALLOCATION_PROFILE,
        cut='after-runtime-drop-before-terminal-format',
        status='OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET',
        catalog_manager_bytes=oracle.MANAGER_BYTES, rust_allocations=snap(2, 9),
        operation_error=None, exclusions=oracle.ALLOCATION_EXCLUSIONS.copy())
    baseline = oracle.allocation_observation(markers, ledger, terminal)
    assert baseline['live_bytes_at_cut'] == 2 and baseline['whole_parent_peak_bytes'] == 9
    controls = [dict(name='interleaved-streams-and-nonzero-live', status='ACCEPTED_OFFLINE_UNIT')]

    def reject(name, mutate):
        m, l, t = copy.deepcopy((markers, ledger, terminal))
        mutate(m, l, t)
        try:
            oracle.allocation_observation(m, l, t)
        except (ValueError, KeyError):
            controls.append(dict(name=name, status='REJECTED_OFFLINE_UNIT'))
        else:
            raise AssertionError('incorrect allocation verifier acceptance: ' + name)

    reject('invalid-marker-counter', lambda m, l, t: m[0]['rust_allocations'].update(accounting_valid=False))
    reject('invalid-ledger-counter', lambda m, l, t: l[0]['rust_allocations'].update(accounting_valid=False))
    reject('invalid-final-counter', lambda m, l, t: t['rust_allocations'].update(accounting_valid=False))
    reject('peak-below-live', lambda m, l, t: m[0]['rust_allocations'].update(live_bytes=4))
    reject('negative-count', lambda m, l, t: t['rust_allocations'].update(live_bytes=-1))
    reject('bool-is-not-count', lambda m, l, t: t['rust_allocations'].update(live_bytes=True))
    reject('overflow-count', lambda m, l, t: t['rust_allocations'].update(peak_bytes=2**64))
    reject('marker-peak-regression', lambda m, l, t: m.reverse())
    reject('ledger-peak-regression', lambda m, l, t: l.reverse())
    reject('terminal-below-marker-peak', lambda m, l, t: t['rust_allocations'].update(peak_bytes=8))
    reject('terminal-below-ledger-peak', lambda m, l, t: l[-1]['rust_allocations'].update(peak_bytes=10))
    reject('wrong-scope', lambda m, l, t: t['rust_allocations'].update(scope='manager-only'))
    reject('undeclared-counter', lambda m, l, t: t['rust_allocations'].update(usable_bytes=9))
    reject('wrong-cut', lambda m, l, t: t.update(cut='before-runtime-drop'))
    reject('wrong-budget', lambda m, l, t: t.update(catalog_manager_bytes=oracle.MANAGER_BYTES + 1))
    reject('missing-exclusion', lambda m, l, t: t['exclusions'].pop())
    reject('false-success', lambda m, l, t: t['rust_allocations'].update(peak_bytes=oracle.MANAGER_BYTES + 1))
    reject('operation-failed', lambda m, l, t: t.update(operation_error='ResourceLimit'))
    reject('missing-ledger', lambda m, l, t: l.clear())
    for peak, status in ((oracle.MANAGER_BYTES, 'OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET'),
                         (oracle.MANAGER_BYTES + 1, 'INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER')):
        changed = copy.deepcopy(terminal)
        changed['rust_allocations']['peak_bytes'] = peak
        changed['status'] = status
        assert oracle.allocation_observation(markers, ledger, changed)['status'] == status
        controls.append(dict(name='threshold-' + str(peak), status='ACCEPTED_OFFLINE_UNIT'))
    return dict(status='PASS_OFFLINE_ALLOCATION_VERIFIER_UNITS', controls=controls,
        scope='Constructed counter records only. Rust allocator tests, real capture, drivers and memory remain NOT_RUN.')


def manager_layout_refusal():
    """Constructed refusal captures only; no engine, probe, SQL or runtime."""
    import tempfile
    import memory_setup  # Resolve the existing inert oracle dependency paths.
    from memory_setup_oracle import BX, manager_layout_refusal as verify
    del memory_setup

    controls = []
    with tempfile.TemporaryDirectory(prefix='manager-layout-refusal-unit-') as temporary:
        root = Path(temporary)
        for name in ('probe', 'bigbrotr-archive-worker', 'science.json'):
            (root / name).write_bytes(('constructed offline unit ' + name).encode())
        mib = 1048576
        case = dict(job='11'*16, event_id='22'*32, dataset='33'*16,
            control=dict(database='control', user='control_reader'), catalog=dict(database='catalog', user='catalog_reader'),
            worker_root=str(root), worker_bytes=(root/'bigbrotr-archive-worker').stat().st_size,
            worker_sha256=oracle.digest((root/'bigbrotr-archive-worker').read_bytes()), ledger=str(root/'ledger.jsonl'),
            limits=dict(row_bytes=mib, engine_memory_bytes=64*mib, native_reserve_bytes=96*mib,
                        data_bytes=768*mib, native_bytes=192*mib, control_bytes=32*mib))
        (root/'case.json').write_text(json.dumps(case))
        spec = dict(probe=str(root/'probe'), case=str(root/'case.json'), science=str(root/'science.json'))
        for key in ('probe', 'case', 'science'):
            spec[key+'_sha256'] = oracle.digest(Path(spec[key]).read_bytes())
        observer = dict(pid=1, start_ticks=10, ppid=0, executable='/usr/bin/python3.11', memory={})
        diagnostic = dict(pid=2, start_ticks=20, ppid=1, executable=spec['probe'], memory={})
        group = dict(current=100, peak=100, limit=str(1536*mib), events=dict(oom=0, oom_kill=0, high=0, max=0), pids=[1])
        initial = dict(spec=spec, case=case, observer=observer, cgroup_before=group)
        result = dict(status='UNKNOWN_OR_FAILED', returncode=4, reaped=True, forced=False, cleanup_errors=[], error=None,
            descendants_absent=True, command=[spec['probe'], 'measure', spec['case']], environment={'LC_ALL':'C'},
            case_sha256_after=spec['case_sha256'], worker_sha256_after=case['worker_sha256'],
            cgroup_after=copy.deepcopy(group), pid=2, elapsed_ns=1000, seen_processes=[observer, diagnostic])
        fields = [
            ('allocation-profile', dict(profile=oracle.ALLOCATION_PROFILE, catalog_manager_bytes=oracle.MANAGER_BYTES,
                catalog_extra_bytes=0, driver_drain_ms=2000, scope=oracle.ALLOCATION_SCOPE, reset=False, baseline_subtraction=False)),
            ('preflight', {key:case[key] for key in ('job','event_id','worker_bytes','worker_sha256')}),
            ('whole-call-admitted', dict(control_service_bytes=528*mib+8*65537, catalog_manager_bytes=oracle.MANAGER_BYTES,
                reader_bytes=mib, output_bytes=mib, query_parent_bytes=64*(mib+2048), native_bytes=160*mib,
                native_control_bytes=131072, fixture_bytes=16*mib, job_cap_qualification=False)),
            ('control-connected', dict(pid=41, start='2026-10-20 16:26:56.193553+00', **case['control'])),
            ('control-acquired', dict(epoch=2, incarnation='44'*16, commit_started=True)),
            ('catalog-preparation-start', {}),
            ('catalog-preparation-complete', dict(grant_bytes=oracle.MANAGER_BYTES, grant_held=True)),
            ('catalog-connected', dict(pid=42, start='2026-10-20 16:26:56.193554+00', **case['catalog'])),
            ('catalog-inspection-start', {}),
            ('catalog-driver-join-start', dict(grant_held=True, driver_present=True, drain_ms=2000)),
            ('catalog-driver-joined', dict(natural=True, failed=False)),
            ('catalog-grant-release-start', dict(grant_held=True, driver_present=False)),
            ('catalog-grant-released', dict(grant_held=False)),
            ('control-driver-joined', dict(natural=True, failed=False)),
            ('operation-returned', dict(error='ResourceLimit', cleanup_error=None)),
            ('grants-released', dict(all_released=True))]
        def counter(peak, live=2):
            return dict(scope=oracle.ALLOCATION_SCOPE, live_bytes=live, peak_bytes=peak, accounting_valid=True)
        events = [dict(event='export-measure', name=name, phase=i, elapsed_us=i,
                       fields=value, rust_allocations=counter(i+2)) for i,(name,value) in enumerate(fields,1)]
        events.append(dict(event='export-allocation-terminal', profile=oracle.ALLOCATION_PROFILE,
            cut='after-runtime-drop-before-terminal-format', status='UNKNOWN_OR_FAILED', catalog_manager_bytes=None,
            operation_error='ResourceLimit', exclusions=oracle.ALLOCATION_EXCLUSIONS.copy(), rust_allocations=counter(25)))
        # Preliminary native admission is valid even though native is never called.
        ledger = [dict(index=0, elapsed_us=1, phase=3, data_bytes=528*mib, native_bytes=160*mib, control_bytes=131072,
                       items=0, permits=2, rust_allocations=counter(8)),
                  dict(index=1, elapsed_us=17, phase=16, data_bytes=0, native_bytes=0, control_bytes=0,
                       items=0, permits=0, rust_allocations=counter(24))]
        samples = [dict(elapsed_ns=100, processes=[observer, diagnostic], cgroup=dict(group,pids=[1,2])),
                   dict(elapsed_ns=200, processes=[observer], cgroup=copy.deepcopy(group))]
        before = {name:[] for name in ['owner_epochs','role_instances','jobs', *('unchanged_'+str(i) for i in range(72))]}
        before['owner_epochs'] = [dict(role='control', resource_id=BX+'55'*16, epoch=1, state='released', released_at_us=10, xmin='10')]
        before['role_instances'] = [dict(role_instance_id=BX+'66'*16, retired_at_us=10, xmin='10')]
        before['jobs'] = [dict(job_id=BX+case['job'], state='accepted', job_revision=1, checkpoint_revision=0,
            owner_resource_id=None, owner_epoch=None, owner_incarnation=None, completed_at_us=None, xmin='11')]
        after = copy.deepcopy(before)
        role = dict(role_instance_id=BX+'77'*16, dataset_id=BX+case['dataset'], role='control',
                    created_at_us=100, retired_at_us=None, xmin='12')
        owner = dict(dataset_id=BX+case['dataset'], role='control', resource_id=BX+'55'*16, epoch=2,
            role_instance_id=role['role_instance_id'], process_incarnation=BX+'44'*16, state='active',
            acquired_at_us=101, lease_expires_at_us=5_000_101, released_at_us=None, exclusion_method=None,
            exclusion_digest=None, archive_frontier_protocol=None, control_backend_pid=41,
            control_backend_start='2026-10-20T16:26:56.193553+00:00', control_fence_version=1, xmin='12')
        after['owner_epochs'].append(owner);after['role_instances'].append(role)
        catalog = {'catalog_table_'+str(i):[] for i in range(32)}
        files = {name:{'.':dict(inode=1, sha256=None)} for name in ('store','data','state','destination','scratch','measure_scratch')}
        original = dict(objects=339, checksum='aa'*32, stored_checksum='aa'*32, identity_bytes=[], wire={})
        fault = dict(objects=340, checksum='bb'*32, stored_checksum='aa'*32, identity_bytes=[19333],
                     wire=dict(rows=340, max_name_bytes=8193, max_definition_bytes=8193))
        payload = dict(root=str(root), before=dict(control=before,catalog=catalog), after=dict(control=after,catalog=copy.deepcopy(catalog)),
            files_before=files, files_after=copy.deepcopy(files), layout_original=original, layout_before=fault,
            layout_after=copy.deepcopy(fault), dump_original='aa'*32, dump_before='bb'*32, dump_after='bb'*32,
            terminal=dict(forced=False,container=dict(State=dict(Status='exited',ExitCode=1,Pid=0,OOMKilled=False)),cgroup=dict(processes=[])),
            backends=[])
        # Independently serialized captures cannot share Python object aliases.
        baseline = json.loads(json.dumps(dict(initial=initial,result=result,events=events,
                                               ledger=ledger,samples=samples,payload=payload)))
        def check(value):
            for name,key in [('input.json','initial'),('result.json','result')]:
                (root/name).write_text(json.dumps(value[key]))
            for name,key in [('stdout','events'),('ledger.jsonl','ledger'),('samples.jsonl','samples')]:
                (root/name).write_text(''.join(json.dumps(row)+'\n' for row in value[key]))
            (root/'stderr').write_bytes(b'')
            return verify(value['payload'])
        answer = check(baseline)
        assert answer['status'] == 'PASS_REFUSAL_DRAIN' and answer['raw_status'] == 'UNKNOWN_OR_FAILED'
        assert answer['live_bytes_at_cut'] == 2 and answer['final_root_audit'] == 'PENDING'
        controls.append(dict(name='natural-refusal-preliminary-native-grant-nonzero-live',status='ACCEPTED_OFFLINE_UNIT'))
        changed = copy.deepcopy(baseline)
        changed['events'][-1]['rust_allocations']['peak_bytes'] = oracle.MANAGER_BYTES+1
        assert check(changed)['allocation_status'] == 'INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER'
        controls.append(dict(name='allocation-over-manager-is-inconclusive',status='ACCEPTED_OFFLINE_UNIT'))
        def reject(name, mutate, reason):
            value = copy.deepcopy(baseline)
            mutate(value)
            try:
                check(value)
            except (ValueError, KeyError) as error:
                assert str(error) == reason, (name,str(error),reason)
                controls.append(dict(name=name,status='REJECTED_OFFLINE_UNIT',reason=str(error)))
            else:
                raise AssertionError('incorrect refusal verifier acceptance: '+name)
        reject('raw-status-rewritten',lambda v:v['result'].update(status='CAPTURED'),'natural refused diagnostic and no descendants')
        reject('outer-exit0',lambda v:v['payload']['terminal']['container']['State'].update(ExitCode=0),'observer exit1 and external exclusion')
        reject('backend-residue',lambda v:v['payload']['backends'].append(dict(pid=41)),'observer exit1 and external exclusion')
        reject('extra-reservation-marker',lambda v:v['events'].insert(9,dict(name='reserved')),'exact refusal marker grammar')
        reject('wrong-operation-error',lambda v:v['events'][14]['fields'].update(error='WrongCatalog'),'natural refused drain and grant lifetime')
        reject('driver-forced',lambda v:v['events'][10]['fields'].update(natural=False),'natural refused drain and grant lifetime')
        reject('grant-released-before-join',lambda v:v['events'][9]['fields'].update(grant_held=False),'natural refused drain and grant lifetime')
        reject('terminal-budget-fabricated',lambda v:v['events'][-1].update(catalog_manager_bytes=oracle.MANAGER_BYTES),'raw unknown terminal retained')
        for key,value in [('index',False),('elapsed_us',-1),('items',True),('permits',-1)]:
            reject('invalid-ledger-'+key,lambda v,k=key,x=value:v['ledger'][0].update({k:x}),'closed refusal ledger attribution')
        reject('ledger-clock-regression',lambda v:v['ledger'][1].update(elapsed_us=0),'refusal ledger chronology')
        reject('ledger-phase-regression',lambda v:v['ledger'][1].update(phase=2),'refusal ledger chronology')
        reject('retained-permit',lambda v:v['ledger'][-1].update(permits=1),'all refused grants drained')
        reject('invalid-counter',lambda v:v['events'][0]['rust_allocations'].update(accounting_valid=False),'valid refused requested-live counter')
        reject('marker-counter-regression',lambda v:v['events'][1]['rust_allocations'].update(peak_bytes=2),'refusal counter stream monotonicity')
        reject('final-counter-below-ledger',lambda v:v['events'][-1]['rust_allocations'].update(peak_bytes=23),'refusal final counter covers both streams')
        reject('initial-extra-pid',lambda v:v['initial']['cgroup_before']['pids'].append(9),'initial sole observer and zero OOM; final descendants absent')
        reject('initial-oom',lambda v:v['initial']['cgroup_before']['events'].update(oom=1),'initial sole observer and zero OOM; final descendants absent')
        reject('foreign-executable',lambda v:v['samples'][1]['processes'][0].update(executable='/foreign'),'only exact observer and diagnostic observed; no worker or foreign process')
        reject('foreign-pid',lambda v:v['samples'][1]['processes'].append(dict(observer,pid=9)),'only exact observer and diagnostic observed; no worker or foreign process')
        reject('diagnostic-parent-changed',lambda v:v['samples'][0]['processes'][1].update(ppid=9),'observed one exact refusal diagnostic identity')
        reject('unbound-seen-identity',lambda v:v['result']['seen_processes'].append(dict(observer,pid=9)),'retained seen-process identities exactly bind sampled identities')
        reject('sample-event-change',lambda v:v['samples'][0]['cgroup']['events'].update(max=1),'finite refusal cgroup samples and unchanged events')
        reject('sample-over-cap',lambda v:v['samples'][0]['cgroup'].update(peak=1536*mib+1),'finite refusal cgroup samples and unchanged events')
        reject('unknown-unsampled-pid',lambda v:v['samples'][0]['cgroup']['pids'].append(9),'finite refusal cgroup samples and unchanged events')
        reject('job-mutated',lambda v:v['payload']['after']['control']['jobs'][0].update(state='running'),'unexpected complete-table/xmin change: jobs')
        reject('prior-owner-xmin-rewritten',lambda v:v['payload']['after']['control']['owner_epochs'][0].update(xmin='99'),'old row/xmin rewritten: owner_epochs')
        reject('extra-owner-row',lambda v:v['payload']['after']['control']['owner_epochs'].append(dict(owner)),'exactly one appended refused owner and role row')
        reject('new-owner-falsely-released',lambda v:v['payload']['after']['control']['owner_epochs'][-1].update(state='released'),'closed active/unretired refused owner rows; no SQL release fabricated')
        reject('new-role-falsely-retired',lambda v:v['payload']['after']['control']['role_instances'][-1].update(retired_at_us=102),'closed active/unretired refused owner rows; no SQL release fabricated')
        reject('catalog-mutated',lambda v:v['payload']['after']['catalog'].update(new=[]),'refused catalog userdata and physical roots unchanged')
        reject('physical-file-mutated',lambda v:v['payload']['files_after']['destination'].update(output=dict(bytes=1)),'refused catalog userdata and physical roots unchanged')
        reject('wire-query-unbounded',lambda v:v['payload']['layout_before']['wire'].update(max_name_bytes=19333),'independent oversized structural fault and unchanged injected baseline')
        reject('wrong-identity',lambda v:v['payload']['layout_before'].update(identity_bytes=[8193]),'independent oversized structural fault and unchanged injected baseline')
        reject('injected-dump-mutated',lambda v:v['payload'].update(dump_after='cc'*32),'independent oversized structural fault and unchanged injected baseline')
    return dict(status='PASS_OFFLINE_MANAGER_LAYOUT_REFUSAL_UNITS',controls=controls,
        scope='Constructed records in owned temporary files only. Real refusal, allocation, SQL/physical custody and runtime remain NOT_RUN.')


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '--manager-layout-refusal':
        print(json.dumps(manager_layout_refusal(), indent=2))
    elif len(sys.argv) == 2 and sys.argv[1] == '--allocations':
        print(json.dumps(allocations(), indent=2))
    elif len(sys.argv) == 2 and sys.argv[1] == '--native-row':
        print(json.dumps(native_rows(), indent=2))
    elif len(sys.argv) == 2 and sys.argv[1] == '--boundaries':
        print(json.dumps(boundaries(), indent=2))
    elif len(sys.argv) == 3 and sys.argv[1] == '--installed':
        print(json.dumps(installed(oracle.load(sys.argv[2])), indent=2))
    else:
        print(json.dumps(run(Path(sys.argv[1])), indent=2))
