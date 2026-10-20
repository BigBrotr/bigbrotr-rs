#!/usr/bin/env python3
"""Offline literal science/custody checks. No engines, fixture imports or signing.

This accepts independent frozen expectations, never an event decoded by the
measured reader. It reports memory observations, not five-export/RSS acceptance.
"""
import argparse
import hashlib
import json
from pathlib import Path

RECIPE_SHA = '815bf9e7e0fbcda73a77fbc3f91bebe0ec5c5f356e01bd665f695a8edb5af963'
FIELDS = ('id', 'pubkey', 'created_at', 'kind', 'tags', 'content', 'sig')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def load(path, maximum=16 * 1024 * 1024):
    raw = Path(path).read_bytes()
    require(len(raw) <= maximum, 'bounded retained input')
    return json.loads(raw)


def event_expected(spec):
    require(set(spec) == {'kind', 'path', 'sha256'}, 'event descriptor')
    raw = Path(spec['path']).read_bytes()
    require(len(raw) <= 4 * 1024 * 1024 and digest(raw) == spec['sha256'], 'frozen event input')
    value = json.loads(raw)
    if spec['kind'] == 'frozen-archive-recipe':
        require(spec['sha256'] == RECIPE_SHA, 'existing independently signed recipe')
        event = dict(value['event'], content=value['content_repeat'] * value['content_count'])
        require(value['signed_event_bytes'] == 94577, 'original signed sample')
    else:
        require(spec['kind'] == 'frozen-signed-event', 'explicit future frozen event kind')
        event = value
    require(set(event) == set(FIELDS), 'complete signed event fields')
    expected_id = digest(encoded([0, event['pubkey'], event['created_at'], event['kind'], event['tags'], event['content']]))
    require(event['id'] == expected_id and len(bytes.fromhex(event['sig'])) == 64, 'independent ID/literal signature')
    return {key: event[key] for key in FIELDS}


def verify_signed_files(destination, artifact, job, science):
    """Compare one complete signed file/manifest with independent frozen science.

    This checks bytes only. The caller owns SQL, physical custody, process joins
    and memory evidence; no successful runtime is inferred from existing files.
    """
    require(set(science) == {'format', 'event', 'query', 'catalog', 'frontier_revision', 'accepted_revisions'}
            and science['format'] == 'bigbrotr-export-memory-science/1', 'independent science descriptor')
    for value in (artifact, job):
        require(isinstance(value, str) and len(value) == 32
                and bytes.fromhex(value).hex() == value and int(value, 16) > 0,
                'actual generated artifact/job ID')
    expected = event_expected(science['event'])
    require(expected['id'] == science['query']['event_id'], 'selected signed ID')
    directory = Path(destination) / artifact
    raw = (directory / 'events.jsonl').read_bytes()
    expected_bytes = encoded(expected) + b'\n'
    require(raw == expected_bytes, 'full independent signed JSONL: content/tags/order/duplicates/signature')
    manifest_raw = (directory / 'manifest.json').read_bytes()
    query = science['query']
    scope_fields = ('dataset_id', 'dictionary_kind', 'allocation_domain', 'ordinal')
    require(set(query['scope']) == set(scope_fields), 'closed qualified scope')
    scope = {key: query['scope'][key] for key in scope_fields}
    manifest = dict(format='bigbrotr-signed-events-export/1', artifact_id=artifact, job_id=job,
        visibility='private', fidelity='full-signed-events', origin='retained-scoped-archive',
        source=dict(cut=dict(dataset_id=query['dataset'], topology_generation=query['topology_generation'],
            bounds=[dict(stream=dict(shard_id=b['shard_id'], owner_epoch=str(b['owner_epoch'])),
                         last_complete_sequence=str(b['complete_through'])) for b in query['bounds']]),
            catalog_id=science['catalog'], frontier_revision=str(science['frontier_revision']), snapshot_id=str(query['snapshot_id'])),
        selection=dict(event_id=expected['id'], scope=scope), accepted_revisions=science['accepted_revisions'],
        included_families=['signed-events'], excluded_families=['internal-payload-envelopes', 'eligibility-facts',
        'first-local-receive', 'relay-provenance', 'imported-claims', 'measurements', 'documents', 'dictionaries',
        'coverage', 'derived-results', 'deployment-state'],
        files=[dict(file_id='events', path='events.jsonl', format='nostr-jsonl/1', bytes=str(len(raw)), rows='1', sha256=digest(raw))])
    require(manifest_raw == encoded(manifest) + b'\n', 'literal complete manifest bytes')
    return dict(rows=1, bytes=len(raw), sha256=digest(raw), manifest_sha256=digest(manifest_raw))



# This descriptor's real ConnectionSettings uses LocalPlaintext and one bounded
# password input (65536 bytes plus its overbound sentinel), charged eight times.
# Keep this independent arithmetic aligned with that closed descriptor, not an
# observed password length or a baseline subtraction.
MANAGER_BYTES = 16 * 1048576 + 8 * (65536 + 1)
ALLOCATION_SCOPE = 'whole-probe-rust-requested-live/1'
ALLOCATION_PROFILE = 'reduced-plaintext-manager/1'
ALLOCATION_EXCLUSIONS = ['allocator-overhead-and-usable-size', 'transient-realloc-overlap',
    'native-and-TLS-allocators', 'child-processes', 'kernel-and-RSS',
    'terminal-record-formatting-after-cut']
REDUCED_NAMES = ['allocation-profile', 'preflight', 'whole-call-admitted',
    'control-connected', 'control-acquired', 'catalog-preparation-start',
    'catalog-preparation-complete', 'catalog-connected', 'catalog-inspection-start',
    'catalog-inspection-complete', 'reserved', 'catalog-capture-start',
    'catalog-capture-complete', 'captured-and-pinned', 'native-query-call',
    'native-query-returned', 'output-written', 'sql-completed', 'catalog-rollback-start',
    'catalog-rollback-complete', 'pin-released-capture-finished', 'catalog-driver-join-start',
    'catalog-driver-joined', 'catalog-grant-release-start', 'catalog-grant-released',
    'control-release-returned', 'control-driver-joined', 'operation-returned', 'grants-released']


def allocation_observation(markers, ledger, terminal):
    """Shared strict counter predicate; caller separately proves science/custody.

    Inputs to unit controls are constructed values, never fabricated captures.
    Two serialized streams are ordered separately: concurrent snapshot/emit
    timing does not establish a total order or a quiescent allocation cut.
    """
    def snapshot(value):
        require(set(value) == {'scope', 'live_bytes', 'peak_bytes', 'accounting_valid'}
                and value['scope'] == ALLOCATION_SCOPE and value['accounting_valid'] is True,
                'valid always-on allocation accounting')
        require(all(type(value[key]) is int and 0 <= value[key] < 2**64
                    for key in ('live_bytes', 'peak_bytes'))
                and value['live_bytes'] <= value['peak_bytes'], 'allocation snapshot bounds')
        return value['peak_bytes']

    require(markers and ledger, 'allocation observation streams present')
    maxima = []
    for stream in (markers, ledger):
        peaks = [snapshot(row['rust_allocations']) for row in stream]
        require(peaks == sorted(peaks), 'allocation peak monotonic within each stream')
        maxima.append(max(peaks))
    require(set(terminal) == {'event', 'profile', 'cut', 'status', 'catalog_manager_bytes',
            'rust_allocations', 'operation_error', 'exclusions'}
            and terminal['event'] == 'export-allocation-terminal'
            and terminal['profile'] == ALLOCATION_PROFILE
            and terminal['cut'] == 'after-runtime-drop-before-terminal-format'
            and type(terminal['catalog_manager_bytes']) is int
            and terminal['catalog_manager_bytes'] == MANAGER_BYTES
            and terminal['operation_error'] is None
            and terminal['exclusions'] == ALLOCATION_EXCLUSIONS,
            'closed post-runtime allocation cut')
    peak = snapshot(terminal['rust_allocations'])
    require(peak >= max(maxima), 'terminal allocation peak covers both streams')
    within = peak <= MANAGER_BYTES
    expected = ('OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET' if within
                else 'INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER')
    require(terminal['status'] == expected, 'allocation outcome derived from whole-parent peak')
    return dict(status=('OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET' if within
                        else 'INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER'),
        catalog_manager_bytes=MANAGER_BYTES, whole_parent_peak_bytes=peak,
        live_bytes_at_cut=terminal['rust_allocations']['live_bytes'],
        scope=ALLOCATION_SCOPE, cut=terminal['cut'], exclusions=ALLOCATION_EXCLUSIONS,
        per_owner_attribution=False, physical_memory_bound=False,
        scope_limit='Plaintext fixture capture only; external SQL/physical custody remains required.')


def reduced_boundaries(by_name, limits):
    require(limits['engine_memory_bytes'] == 64 * 1048576
            and limits['native_reserve_bytes'] == 96 * 1048576, 'unchanged reduced native profile')
    require(by_name['allocation-profile'] == dict(profile=ALLOCATION_PROFILE,
            catalog_manager_bytes=MANAGER_BYTES, catalog_extra_bytes=0, driver_drain_ms=2000,
            scope=ALLOCATION_SCOPE, reset=False, baseline_subtraction=False), 'actual reduced observation profile')
    require(by_name['catalog-preparation-complete'] == dict(grant_bytes=MANAGER_BYTES, grant_held=True),
            'prepared manager grant held')
    require(by_name['catalog-driver-join-start'] == dict(grant_held=True, driver_present=True, drain_ms=2000)
            and by_name['catalog-grant-release-start'] == dict(grant_held=True, driver_present=False)
            and by_name['catalog-grant-released'] == dict(grant_held=False), 'manager grant survives driver join')
    for name in ('catalog-preparation-start', 'catalog-inspection-start', 'catalog-inspection-complete',
                 'catalog-capture-start', 'catalog-capture-complete',
                 'catalog-rollback-start', 'catalog-rollback-complete'):
        require(by_name[name] == {}, 'closed reduced boundary marker')


def verify(root):
    initial, result = load(root / 'input.json'), load(root / 'result.json')
    spec, case = initial['spec'], initial['case']
    require(result['status'] == 'CAPTURED' and result['returncode'] == 0 and result['reaped']
            and not result['forced'] and not result['cleanup_errors'] and result['error'] is None
            and result['descendants_absent'], 'actual unforced reap and no remaining process')
    require(result['command'] == [spec['probe'], 'measure', spec['case']], 'actual finite command')
    require(result['environment'] == {'LC_ALL': 'C'}, 'no inherited loader or credential environment')
    require(digest(Path(spec['probe']).read_bytes()) == spec['probe_sha256'], 'parent exact bytes')
    require(digest(Path(spec['case']).read_bytes()) == spec['case_sha256'] == result['case_sha256_after'], 'case unchanged')
    require(load(spec['case']) == case, 'captured case equals pinned source')
    worker = Path(case['worker_root']) / 'bigbrotr-archive-worker'
    require(worker.stat().st_size == case['worker_bytes'] and digest(worker.read_bytes()) == case['worker_sha256']
            == result['worker_sha256_after'], 'worker preserved')
    require((root / 'stderr').read_bytes() == b'', 'caller stderr')
    events = [json.loads(line) for line in (root / 'stdout').read_text().splitlines()]
    names = ['preflight', 'whole-call-admitted', 'control-connected', 'control-acquired', 'catalog-connected',
             'reserved', 'captured-and-pinned', 'native-query-call', 'native-query-returned', 'output-written',
             'sql-completed', 'pin-released-capture-finished', 'catalog-driver-joined', 'control-release-returned',
             'control-driver-joined', 'operation-returned', 'grants-released']
    reduced = bool(events and events[-1].get('event') == 'export-allocation-terminal')
    terminal = events.pop() if reduced else None
    if reduced:
        names = REDUCED_NAMES
    require(len(events) == len(names) + 1, 'complete caller grammar')
    marker_keys = {'event', 'phase', 'name', 'elapsed_us', 'fields'} | ({'rust_allocations'} if reduced else set())
    for index, (entry, name) in enumerate(zip(events[:-1], names), 1):
        require(set(entry) == marker_keys
                and entry['event'] == 'export-measure' and entry['phase'] == index and entry['name'] == name,
                'caller marker order')
        require(type(entry['elapsed_us']) is int and entry['elapsed_us'] >= 0, 'finite caller timestamp')
    require([entry['elapsed_us'] for entry in events[:-1]] == sorted(entry['elapsed_us'] for entry in events[:-1]), 'caller clock order')
    require(events[-1] == dict(event='export-measure-complete', name=case['name'], status='COMPLETE',
            authority='one-accepted-job', inner_phases='not-observed'), 'complete only after cleanup')
    by_name = {entry['name']: entry['fields'] for entry in events[:-1]}
    mib = 1048576
    limits = case['limits']
    require(limits['row_bytes'] == mib, 'first baseline full1MiB event envelope')
    require(by_name['preflight'] == dict(job=case['job'], event_id=case['event_id'],
            worker_sha256=case['worker_sha256'], worker_bytes=case['worker_bytes']), 'actual selected inputs')
    require(by_name['whole-call-admitted'] == dict(control_service_bytes=528*mib+8*65537,
            catalog_manager_bytes=MANAGER_BYTES if reduced else 80*mib+8*65537, reader_bytes=mib, output_bytes=mib,
            query_parent_bytes=64*(limits['row_bytes']+2048),
            native_bytes=limits['engine_memory_bytes']+limits['native_reserve_bytes'],
            native_control_bytes=131072, fixture_bytes=16*mib, job_cap_qualification=False), 'owner-specific configured admission')
    if reduced:
        reduced_boundaries(by_name, limits)
    require(by_name['native-query-call'] == dict(row_bytes=limits['row_bytes'], engine_memory_bytes=limits['engine_memory_bytes'],
            native_reserve_bytes=limits['native_reserve_bytes'], scan_bytes=limits['scan_bytes']), 'actual whole native call limits')
    for name, connection in [('control-connected', case['control']), ('catalog-connected', case['catalog'])]:
        bound = by_name[name]
        require(set(bound) == {'pid', 'start', 'database', 'user'} and type(bound['pid']) is int and bound['pid'] > 0
                and isinstance(bound['start'], str) and bound['start'] and bound['database'] == connection['database']
                and bound['user'] == connection['user'], 'actual backend attribution')
    for name in ('catalog-driver-joined', 'control-driver-joined'):
        require(by_name[name] == dict(natural=True, failed=False), 'natural actual driver join')
    require(by_name['control-release-returned'] == dict(committed=True, error=None)
            and by_name['operation-returned'] == dict(error=None, cleanup_error=None)
            and by_name['grants-released'] == dict(all_released=True), 'known complete lifecycle')
    before, after = initial['cgroup_before'], result['cgroup_after']
    require(before['limit'] == after['limit'] and after['peak'] >= before['peak']
            and after['peak'] <= int(after['limit']) and after['pids'] == [initial['observer']['pid']], 'actual cgroup bound/exclusion')
    require(after['events'] == before['events'], 'no cgroup OOM/max/high/pressure event changes')
    require(digest(Path(spec['science']).read_bytes()) == spec['science_sha256'], 'science descriptor pin')
    science = load(spec['science'])
    require(set(science) == {'format', 'event', 'query', 'catalog', 'frontier_revision', 'accepted_revisions'}
            and science['format'] == 'bigbrotr-export-memory-science/1', 'independent science descriptor')
    expected = event_expected(science['event'])
    reserved = by_name['reserved']
    require(reserved['query'] == science['query'] and reserved['catalog'] == science['catalog']
            and reserved['frontier_revision'] == science['frontier_revision'], 'independent retained selection')
    require(expected['id'] == case['event_id'] == science['query']['event_id'], 'selected signed ID')
    artifact = reserved['artifact']
    require(len(artifact) == 32 and len(bytes.fromhex(artifact)) == 16, 'actual generated artifact ID')
    files = verify_signed_files(case['destination'], artifact, case['job'], science)
    require(by_name['output-written'] == files, 'caller actual file result')
    require(by_name['sql-completed']['job'] == case['job'] and by_name['sql-completed']['artifact'] == artifact
            and by_name['sql-completed']['manifest'] == files['manifest_sha256'], 'known SQL completion binds output')
    require(by_name['native-query-returned'] == dict(present=True, normalized_bytes=files['bytes']-1,
            worker='reaped-by-real-reader', includes_parent_decode=True), 'returned event science')
    samples = []
    with (root / 'samples.jsonl').open() as stream:
        for line in stream:
            require(len(samples) <= 14000 and len(line) <= 65536, 'bounded raw memory samples')
            samples.append(json.loads(line))
    require(samples and samples[-1]['elapsed_ns'] <= result['elapsed_ns'], 'sample time custody')
    require([s['elapsed_ns'] for s in samples] == sorted(s['elapsed_ns'] for s in samples), 'sample clock order')
    worker_samples = [p for s in samples for p in s['processes'] if p['executable'] == str(worker)]
    require(worker_samples and all(p['ppid'] == result['pid'] for p in worker_samples), 'actual observed child worker')
    require(len({(p['pid'], p['start_ticks']) for p in worker_samples}) == 1, 'exactly one measured worker')
    parent_samples = [p for s in samples for p in s['processes'] if p['pid'] == result['pid']]
    require(parent_samples and len({p['start_ticks'] for p in parent_samples}) == 1, 'actual parent identity')
    ledger = [json.loads(line) for line in Path(case['ledger']).read_text().splitlines()]
    require(ledger and len(ledger) <= 24001 and ledger[-1]['permits'] == 0, 'finite released ledger')
    for i, row in enumerate(ledger):
        require(row['index'] == i and 0 <= row['phase'] <= len(names), 'ledger sample attribution')
        for key in ('data_bytes', 'native_bytes', 'control_bytes'):
            require(0 <= row[key] <= case['limits'][key], 'real grant ceiling')
    native_phases = [r for r in ledger if r['phase'] == names.index('native-query-call') + 1]
    require(native_phases, 'actual whole-call ledger samples; no inferred internal phase')
    answer = dict(status='PASS_SCIENCE_AND_CAPTURE', rows=1, bytes=files['bytes'], samples=len(samples), ledger_samples=len(ledger),
        parent_rss_sample_max=max(p['memory'].get('VmRSS', 0) for p in parent_samples),
        worker_rss_sample_max=max(p['memory'].get('VmRSS', 0) for p in worker_samples),
        worker_hwm_observed_max=max(p['memory'].get('VmHWM', 0) for p in worker_samples),
        cgroup_lifetime_peak=after['peak'], cgroup_before_peak=before['peak'],
        ledger_sample_max={key:max(r[key] for r in ledger) for key in ('data_bytes', 'native_bytes', 'control_bytes')},
        native_call_ledger_sample_max={key:max(r[key] for r in native_phases) for key in ('data_bytes', 'native_bytes', 'control_bytes')},
        scratch_allocated_sample_max=max(s['scratch']['allocated_bytes'] for s in samples),
        qualification_256mib=False, five_exports_acceptance=False, precise_worker_inner_phases=False,
        limits='Sampling can miss peaks; cgroup lifetime includes observer and cache; external fresh-container/SQL audit still required.')
    if reduced:
        answer['manager_allocation_observation'] = allocation_observation(events[:-1], ledger, terminal)
    return answer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.root), indent=2))
