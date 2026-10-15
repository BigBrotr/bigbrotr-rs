#!/usr/bin/env python3
"""Offline binding of retirement captures; never open a DB/native engine.

Scientific interpretation is mandatory retirement_replay.verify. This reader
independently binds its inputs to pinned origins, actual retained sidecar bytes,
complete process boundaries and terminal captures. SQL preservation remains a
captured-run claim; file identities can be checked directly without an engine.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import stat
import sys
import tomllib

sys.dont_write_bytecode = True
import history_replay as history

sha, loads, same, only, instant = history.sha, json.loads, history.same, history.only, history.instant

# Independent literal identities of the three selected, already-qualified endpoints.
PINS = {'genuine-gap': {'name': 'genuine-gap',
                 'run': 'ingest-succession-acquisition-engine-f169ad7',
                 'subdirectory': 'acquisition',
                 'database': 'bb_successor_acquisition_f169ad7_empty',
                 'status': 'PASS_SUCCESSOR_ACQUISITION_FIRST_WAVE',
                 'source': 'f169ad73c0b2349032afc183b6b5b82182661eae',
                 'owner': '1c1f76d5bb56a7b669db61e634040a92',
                 'label': 'org.bigbrotr.succession-acquisition',
                 'client_id': 'b5e6f664d585b9bc19a0e0a9f0e659cc4aa9bcf7cc8979ac59c0402592dbf1ff',
                 'source_clone': 'empty',
                 'final_case': 'empty-abandon-retry3',
                 'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
                 'result_sha256': '2eee461317257f1b105a1b498043cd885c109c6dfcb9153038d2c4a29f3ba146',
                 'snapshot': 'empty-abandon-retry3-after.json',
                 'snapshot_sha256': '9b3a1e6a2f7ab530f04efb669fc5f8a1075d04b48ea3e8493efe51a552b851da',
                 'native_file': 'empty-abandon-retry3-after-native.json',
                 'native_sha256': '269bf396c8cfc09588405495364359df1fab33737036d30990469530aa5722bc',
                 'inputs_file': 'empty-abandon-retry3-after-inputs.json',
                 'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
                 'store': 'empty/store'},
 'dictionary': {'name': 'dictionary',
                'run': 'ingest-succession-source-engine-e6adc19',
                'subdirectory': 'source-close',
                'database': 'bb_successor_source_e6adc19_outcome_before_loss',
                'status': 'PASS_SUCCESSOR_SOURCE_FIRST_WAVE',
                'source': 'e6adc1902edd62b0490895c668c92f2eb21c3c35',
                'owner': '35bfc18e53fb981d70cd6b9a7ae5c7c4',
                'label': 'org.bigbrotr.succession-source',
                'client_id': '0f19cd029e76b27ce278303854c4b6132e7493c035b8df41059d5d9336a44d4d',
                'source_clone': 'outcome-before-loss',
                'final_case': 'outcome-before-loss-retry',
                'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                'result_sha256': '79ea3633475146c908edd058ff20ef1a1b2895aae81601eb525df67c9c2189ac',
                'snapshot': 'outcome-before-loss-retry-after.json',
                'snapshot_sha256': '2c26b49cb3cab2599df2dbf819419c55627d28210f72ac8439ab972ed8677485',
                'native_file': 'outcome-before-loss-retry-after-native.json',
                'native_sha256': 'a618e931f2bf82445986ebaaf5d602de69f4b1d58c1e034c2c4af0c025a3e087',
                'inputs_file': 'outcome-before-loss-retry-after-inputs.json',
                'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
                'store': 'outcome-before-loss/store'},
 'import-event': {'name': 'import-event',
                  'run': 'ingest-succession-source-engine-e6adc19',
                  'subdirectory': 'source-close',
                  'database': 'bb_successor_source_e6adc19_activated_live3',
                  'status': 'PASS_SUCCESSOR_SOURCE_FIRST_WAVE',
                  'source': 'e6adc1902edd62b0490895c668c92f2eb21c3c35',
                  'owner': '35bfc18e53fb981d70cd6b9a7ae5c7c4',
                  'label': 'org.bigbrotr.succession-source',
                  'client_id': '0f19cd029e76b27ce278303854c4b6132e7493c035b8df41059d5d9336a44d4d',
                  'source_clone': 'activated-live3',
                  'final_case': 'activated-live3-retry',
                  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
                  'result_sha256': '79ea3633475146c908edd058ff20ef1a1b2895aae81601eb525df67c9c2189ac',
                  'snapshot': 'activated-live3-retry-after.json',
                  'snapshot_sha256': '5cc5bd8b6c93893d9348640a4b2fb1efa9b04532e4d092537cf003cf2909adde',
                  'native_file': 'activated-live3-retry-after-native.json',
                  'native_sha256': '28e68d5e6ea304ef44e7968b7d341b8b4e70b56aa11c1200c260c6696727a4c8',
                  'inputs_file': 'activated-live3-retry-after-inputs.json',
                  'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
                  'store': 'activated-live3/store'}}

LEGACY_CATALOGS = {
    'activated': 'baa02a5fc187770cd8b5782ab47feab4dd11d54667aa2737d19411db546e9276',
    'empty': 'f275e865518402db42799a42f6571dfae4e7299e576b784c21a9c5d6e7ce3b1e',
}
# name, clone/origin, operation, previous case, reader status
EXPLICIT = (
    ('genuine-gap-retire3', 'genuine-gap', 'retire', None, 'PASS'),
    ('genuine-gap-retry3', 'genuine-gap', 'retire', 'genuine-gap-retire3', 'PASS'),
    ('genuine-gap-live4', 'genuine-gap', 'live-new', 'genuine-gap-retry3', 'PASS'),
    ('dictionary-retire3', 'dictionary', 'retire', None, 'PASS'),
    ('dictionary-live4', 'dictionary', 'live-new', 'dictionary-retire3', 'PASS'),
    ('import-event-refusal3', 'import-event', 'retire', None, 'PASS_EXPECTED_REFUSAL'),
)
IMPLICIT = (
    ('empty-claims3', 'empty', 'acquisition-claims', None, 'PASS'),
    ('empty-source3', 'empty', 'source', 'empty-claims3', 'PASS'),
    ('empty-retire-refusal3', 'empty', 'retire', 'empty-source3', 'PASS_EXPECTED_REFUSAL'),
)
PRESERVED = dict(rows_unchanged=True, catalog_unchanged=True, native_unchanged=True,
                 inputs_unchanged=True, physical_unchanged=True)
COMPLETIONS = {'retire': 'lineage-retirement-complete', 'live-new': 'lineage-live-complete',
               'source': 'lineage-source-complete', 'acquisition-claims': 'lineage-acquisition-complete'}


def physical(root, raw):
    """Actual original path/type/inode/link/content census, without engine open."""
    result = {}
    for path in [root, *sorted(root.rglob('*'))]:
        metadata = path.lstat()
        assert stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)
        result[str(path.relative_to(root))] = dict(mode=metadata.st_mode, inode=metadata.st_ino,
            device=metadata.st_dev, links=metadata.st_nlink, bytes=metadata.st_size,
            sha256=sha(raw(path)) if stat.S_ISREG(metadata.st_mode) else None)
    return result


def spool_checked(root, inputs, raw, *, complete):
    if complete:
        assert sorted(p.name for p in root.iterdir()) == sorted(inputs), 'unclassified sidecar'
    for name, identity in inputs.items():
        assert Path(name).name == name and name not in ('.', '..')
        path = root / name
        assert stat.S_ISREG(path.lstat().st_mode), 'sidecar must be a regular file'
        content = raw(path)
        assert dict(bytes=len(content), sha256=sha(content)) == identity


def custody(events, database, operation, expected_status):
    """Keep a known business refusal distinct from lost native/process custody."""
    native, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-native', state='closed')]
    drivers = [e for e in events if e.get('event') == 'lineage-history-driver']
    assert drivers == [dict(event='lineage-history-driver', driver=d, failed=False, joined=True)
                       for d in ('control', 'observer')]
    present, absent = [e for e in events if e.get('event') == 'lineage-history-backend']
    assert present['database'] == database and present['present'] is True
    assert type(present['pid']) is int and present['pid'] > 0
    assert type(present['control_epoch']) is int and present['control_epoch'] > 0
    instant(present['backend_start'])
    assert absent == dict(present, present=False)
    complete = events[-1]
    assert complete['event'] == COMPLETIONS[operation] and complete['operation'] == operation
    assert complete['native'] == 'closed' and complete['observer'] == 'joined'
    assert complete['success'] is (expected_status == 'PASS')
    assert complete['control'] == ('released-and-joined' if expected_status == 'PASS' else 'dropped-and-joined')
    assert native < events.index(drivers[0]) < events.index(absent) < events.index(drivers[1]) < len(events)-1
    assert not any(e.get('event') == 'lineage-history-timeout' for e in events)
    return present


def claim_diagnostic(log, events, database, window):
    return server_diagnostic(log, events, database, window,
        'lineage acquisition claims remain unresolved',
        r'PL/pgSQL function control\.require_ingest_lineage_workflows\(bytea,numeric,numeric\) line \d+ at RAISE')


def server_diagnostic(log, events, database, window, message, context_pattern):
    """Default PG log has millisecond UTC/PID, not database/start or SQLSTATE.

    Bind the database/start through the actual claim row and observed tuple.
    A printed millisecond denotes its entire 1ms interval, never invented us.
    """
    backend = only(events, event='lineage-history-backend', present=True)
    assert backend['database'] == database
    claimed = only(events, event='lineage-history-state', stage='after-claim')
    pg = {k: loads(v) for k, v in claimed['state']['control_tables']}
    owner = only(pg['owner_epochs'], role='control', epoch=backend['control_epoch'])
    assert owner['control_backend_pid'] == backend['pid']
    start = instant(backend['backend_start'])
    assert instant(owner['control_backend_start']) == start
    expression = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}) UTC \[(\d+)\] ([A-Z]+):  (.*)$')
    records = []
    for line in log.splitlines():
        match = expression.match(line)
        if match:
            records.append(dict(time=instant(match[1]+'+00:00'), pid=int(match[2]),
                level=match[3], message=match[4], lines=[line]))
        elif records:
            records[-1]['lines'].append(line)
    records = [r for r in records if r['pid'] == backend['pid']]
    selected = []
    for index, record in enumerate(records):
        state = None
        text = record['message']
        if text.startswith('23514:  '):
            state, text = '23514', text[len('23514:  '):]
        if record['level'] != 'ERROR' or text != message:
            continue
        if not max(start, window['before_us']) < record['time'] + 1000 or record['time'] > window['after_us']:
            continue
        assert index + 1 < len(records)
        context = records[index + 1]
        assert context['level'] == 'CONTEXT'
        assert record['time'] <= context['time'] <= window['after_us']
        assert re.search(context_pattern, '\n'.join(context['lines']))
        selected.append(dict(backend=backend, error_lines=record['lines'], context_lines=context['lines'],
            observed_sqlstate=state, log_precision_us=1000))
    result, = selected
    return result


def origin_checked(root, folder, name, pin, declaration, raw, read, *, explicit=True):
    """Bind one pinned origin, its captured SQL and actual retained files."""
    locator = pin if explicit else dict(name=name, kind=1 if name == 'activated' else 2,
        **{k: pin[k] for k in ('run','database','snapshot','snapshot_sha256','result_sha256','status')})
    assert set(declaration) == set(locator) | {'pins','store','files','inputs','native','previous_client','rows','catalog','preserved'}
    assert declaration['pins'] == pin
    for key, value in locator.items():
        if key != 'store':
            assert declaration[key] == value
    basis = root.parent / pin['run'] / (pin['subdirectory'] if explicit else 'bindings')
    assert Path(declaration['store']) == basis / pin['store']
    previous_bytes = raw(basis / 'result.json')
    assert sha(previous_bytes) == pin['result_sha256']
    previous = loads(previous_bytes)
    assert previous['status'] == pin['status']
    assert declaration['previous_client'] == previous['client']
    for key in ('cleanup_errors','missing_owned','unrelated_changed'):
        assert previous[key] == []
    if explicit:
        assert previous['unexpected_containers'] == []
        for key in ('source','owner','label'):
            assert previous[key] == pin[key]
        assert previous['client'] == pin['client_id']
        old = only(previous['clones'], name=pin['source_clone'])
        assert old['final_case'] == pin['final_case'] and old['database'] == pin['database']
        assert old['directory'] + '/store' == pin['store'] and old['status'] == 'PASS'
        last = only(previous['cases'], name=pin['final_case'])
        assert last['status'] == 'PASS' and last['returncode'] == 0 and last['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert sha(old['final_catalog'].encode()) == pin['catalog_sha256']
    captures = {key: raw(basis / pin[key]) for key in ('snapshot','native_file','inputs_file')}
    rows, native, original_inputs = [loads(captures[key]) for key in ('snapshot','native_file','inputs_file')]
    for key, digest in (('snapshot','snapshot_sha256'),('native_file','native_sha256'),('inputs_file','inputs_sha256')):
        assert sha(captures[key]) == pin[digest]
    inputs = original_inputs.get('inputs', original_inputs)
    same(rows, declaration['rows'])
    same(rows, read(folder / (name+'-original.json')))
    assert declaration['native'] == native and declaration['inputs'] == inputs
    assert sha(declaration['catalog'].encode()) == (pin['catalog_sha256'] if explicit else LEGACY_CATALOGS[name])
    store = basis / pin['store']
    assert physical(store, raw) == declaration['files'], 'original physical census changed'
    spool_checked(store / 'producer-inputs-v1', inputs, raw, complete=True)
    if 'contents' in original_inputs:
        assert all(raw(store/'producer-inputs-v1'/n).hex() == b for n,b in original_inputs['contents'].items())
    assert declaration['preserved'] == PRESERVED == read(folder / (name+'-preserved.json'))
    assert all(v is True for v in declaration['preserved'].values())
    return rows, native, inputs


def clone_checked(folder, clone, origin, raw, read, *, explicit=True):
    """Check configuration, migrations/grants and unchanged clone schema."""
    name, origin_name = clone["name"], clone["origin"]
    config = tomllib.loads(raw(folder / Path(clone['config']).name).decode())
    ds = only(origin[0]['datasets'])
    assert {k:config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref')} == dict(
        database=clone['database'], endpoint='127.0.0.1:5432', user='bigbrotr_control', password_ref='file:/run/secrets/application')
    assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
    assert config['topology']['dataset_id'] == ds['dataset_id'][2:] and config['topology']['instance_id'] == ds['instance_id'][2:]
    assert config['topology']['local_roles'] == ['control','ingest','archive']
    assert config['postgres']['control']['tls'] == dict(local_boundary='loopback', mode='local-plaintext')
    assert config['runtime']['lease_ttl'] == '20s'
    assert config['runtime']['processes']['application'] == dict(memory_max_bytes='1536MiB', baseline_bytes='128MiB',
        payload_bytes='1GiB', native_bytes='256MiB', completion_reserve_bytes='32MiB')
    validation = read(folder / (name+'-config-validation.json'))
    assert validation['returncode'] == 0 and validation['stderr'] == ''
    floor = 38 if explicit else history.PINS[origin_name]['floor']
    steps = [dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql', sha256=history.MIGRATIONS[v][1],version=str(v)) for v in range(floor+1,39)]
    assert [m['action'] for m in clone['migrations']] == ['plan','apply','apply']
    for index, answer in enumerate(clone['migrations']):
        assert set(answer) == {'action','exit_code','stdout','stderr'}
        assert type(answer['exit_code']) is int and answer['exit_code'] == 0 and answer['stderr'] == ''
        assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1', scope='postgres-control',target_version='38',
            source_version=str(floor) if index < 2 else '38', current_version=str(floor) if index == 0 else '38',
            status='planned' if index == 0 else ('applied' if index == 1 and steps else 'current'),
            applied_count=str(len(steps)) if index == 1 else '0', steps=steps if index < 2 else [],
            dataset_initialization='not-performed',release_upgrade_qualification='not-performed')
    grants = clone['grants']
    assert len(grants) == 4 and all(set(g) == {'statement','stdout'} and not g['stdout'].strip() for g in grants)
    assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
    assert [g['statement'] for g in grants[1:]] == ['GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
        'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;','GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
    assert clone['catalog_before'] == clone['catalog_after'] == clone['final_catalog']
    assert sha(clone['catalog_before'].encode()) == (PINS[origin_name]['catalog_sha256'] if explicit else PINS['genuine-gap']['catalog_sha256'])
    assert clone['final_backends'] == []
    same(origin[0], read(folder/(name+'-copy-before.json')))
    migrated = read(folder/(name+'-migrated.json'))
    if explicit:
        same(origin[0], migrated)
    else:
        history.baseline(migrated,origin[1],origin[2],origin_name,read(folder/'empty-claims3-clock.json'))
    return migrated, origin[1], origin[2]


def replay(root):
    from retirement_replay import verify, negative_controls
    root = Path(root).resolve(strict=True)
    folders = [root / name for name in ('retirement', 'retirement-claim-veto') if (root / name / 'result.json').is_file()]
    folder, = folders  # Separate exact roots, never silently merge histories.
    explicit = folder.name == 'retirement'
    specs = EXPLICIT if explicit else IMPLICIT
    names = ('genuine-gap', 'dictionary', 'import-event') if explicit else ('empty',)
    expected_status = 'PASS_SUCCESSOR_RETIREMENT_FIRST_WAVE' if explicit else 'PASS_SUCCESSOR_RETIREMENT_CLAIM_VETO'
    label = 'org.bigbrotr.succession-retirement' + ('' if explicit else '-claim-veto')
    prefix = 'bb_successor_retirement_' if explicit else 'bb_successor_retire_claim_'
    bound = {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == expected_status and report['label'] == label
    assert re.fullmatch(r'[0-9a-f]{40}', report['source']) and re.fullmatch(r'[0-9a-f]{40}', report['tree'])
    assert re.fullmatch(r'[0-9a-f]{32}', report['owner']) and re.fullmatch(r'[0-9a-f]{64}', report['client'])
    assert [c['name'] for c in report['cases']] == [s[0] for s in specs]
    assert [c['name'] for c in report['clones']] == list(names)
    assert len({c['database'] for c in report['clones']}) == len(names)
    assert report['databases'] == [c['database'] for c in report['clones']]
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    assert set(report['database_names_after']) == set(report['database_names_before']) | set(report['databases'])
    assert not set(report['database_names_before']) & set(report['databases'])
    started, finished = instant(report['started_at']), instant(report['finished_at'])
    assert started <= finished
    pins = PINS if explicit else history.PINS
    assert set(report['origins']) == set(pins)
    origins = {}
    for name, pin in pins.items():
        declaration = report['origins'][name]
        origins[name] = origin_checked(root, folder, name, pin, declaration, raw, read, explicit=explicit)

    chains, windows, clones = {}, {}, {}
    for clone in report['clones']:
        name = clone['name']
        assert clone['origin'] == name and clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['database'] == prefix + report['source'][:7] + '_' + name.replace('-','_')
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        chains[name] = clone_checked(folder, clone, origins[name], raw, read, explicit=explicit)
        clones[name] = clone

    checked, diagnostic = [], None
    for case, spec in zip(report['cases'], specs, strict=True):
        name, origin, operation, predecessor, status = spec
        clone = clones[origin]
        assert all(case[k] == v for k,v in dict(name=name,clone=origin,origin=origin,operation=operation,
            predecessor=predecessor,metadata_bytes=262144,proxy_port=None,status=status).items())
        assert type(case['metadata_bytes']) is int
        assert case['database'] == clone['database'] and case['config'] == clone['config']
        assert case['custody'] == 'OBSERVED_CLOSED_AND_JOINED' and type(case['returncode']) is int and case['returncode'] == 0
        assert case['catalog_after'] == clone['catalog_before']
        case_read = lambda suffix: read(folder/(name+'-'+suffix+'.json'))
        before, native, inputs = case_read('before'), case_read('before-native'), case_read('inputs')
        same(chains[origin][0],before)
        assert native == chains[origin][1] and inputs == chains[origin][2], 'process boundary differs'
        assert case_read('capture-errors') == []
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error','orchestration_error','cleanup_error','clock_error','response_error'))
        assert type(response['seconds']) in (int,float) and math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        custody(events,case['database'],operation,status)
        window = case_read('clock')
        assert window == case['server_window'] and started <= window['before_us'] <= window['after_us'] <= finished
        assert window['before_us'] >= windows.get(origin,0), 'process windows overlap'
        result = verify(events,before,native,inputs,case,window)
        assert result['database'] == case['database'] and result['status'] == status
        final = only(events,event='lineage-history-state',stage='final')
        after, after_native, after_inputs = case_read('after'), case_read('after-native'), case_read('after-inputs')
        same({k:loads(v) for k,v in final['state']['control_tables']},after)
        assert after_native == dict(event='dump-state',rows=final['rows'],sequence=final['state']['sequence'])
        assert after_inputs == dict(final['state']['originals']) and case_read('backends') == []
        if operation != 'live-new':
            assert after_inputs == inputs
        if operation == 'retire':
            assert after_native == native
        assert case_read('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        spool_checked(folder/origin/'store/producer-inputs-v1',after_inputs,raw,complete=False)
        if name == 'empty-retire-refusal3':
            diagnostic = claim_diagnostic(raw(folder/'postgres.log').decode(),events,case['database'],window)
        result['negative_controls'] = negative_controls(events,before,native,inputs,case,window)
        assert result['negative_controls']['status'] == 'PASS'
        checked.append(result)
        chains[origin], windows[origin] = (after,after_native,after_inputs),window['after_us']
    for name,clone in clones.items():
        assert clone['final_case'] == [s[0] for s in specs if s[1] == name][-1]
        same(chains[name][0],read(folder/(name+'-final-sql.json')))
        spool_checked(folder/name/'store/producer-inputs-v1',chains[name][2],raw,complete=True)
    terminal = read(folder/'terminal-containers.json')
    for filename,ident in [('client-terminal.json',report['client']),('postgres-terminal.json',report['postgres'])]:
        value = read(folder/filename)
        assert value == terminal[ident] and value['Id'] == ident
        state = value['State']
        assert state['Status'] == 'exited' and state['Pid'] == 0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
    assert report['postgres'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
    return dict(status=expected_status+'_REPLAY',source=report['source'],cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        diagnostic=diagnostic,bound_files=bound,
        original_preservation='Fresh read-only file census; SQL/catalog preservation is captured-run evidence, not a fresh query',
        scope='Finite retirement/retry/live4 or genuine unresolved claim refusal; no installed, fault, import closure or capacity qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('roots',type=Path,nargs='+')
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    assert 1 <= len(args.roots) <= 2
    results = [replay(root) for root in args.roots]
    if len(results) == 2:
        assert {r['status'] for r in results} == {'PASS_SUCCESSOR_RETIREMENT_FIRST_WAVE_REPLAY','PASS_SUCCESSOR_RETIREMENT_CLAIM_VETO_REPLAY'}
    output = json.dumps(results[0] if len(results) == 1 else dict(status='PASS_SUCCESSOR_RETIREMENT_CAPTURE_REPLAY',roots=results),indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(output)
    else:
        print(output,end='')
