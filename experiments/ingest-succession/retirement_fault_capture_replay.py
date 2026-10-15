#!/usr/bin/env python3
"""Bind retirement fault transcripts to pinned files and independent full-state replay.

No SQL, native engine or tested executable runs here. SQL/catalog preservation
is captured-run evidence; original file censuses and sidecars are checked now.
"""
import argparse
import json
import math
from pathlib import Path
import re
import stat
import sys

sys.dont_write_bytecode = True
import retirement_capture_replay as capture

sha, loads, same, only, instant = capture.sha, json.loads, capture.same, capture.only, capture.instant
SPECS = (
    ('before-forward', 'before', None, 'before-forward'),
    ('before-forward-fresh', 'before', 'before-forward', None),
    ('after-complete', 'after', None, 'after-complete'),
    ('after-complete-fresh', 'after', 'after-complete', None),
    ('final-job-cancel', 'cancel', None, 'cancel'),
    ('final-job-expiry', 'expiry', None, 'expiry'),
)
STATUS = 'PASS_SUCCESSOR_RETIREMENT_FAULTS'


def custody(events, database, *, loss, delivered):
    native, = [i for i,e in enumerate(events) if e == dict(event='lineage-history-native',state='closed')]
    control, observer = [e for e in events if e.get('event') == 'lineage-history-driver']
    assert type(control['failed']) is bool and (loss or control['failed'] is False)
    assert control == dict(event='lineage-history-driver',driver='control',failed=control['failed'],joined=True)
    assert observer == dict(event='lineage-history-driver',driver='observer',failed=False,joined=True)
    present, absent = [e for e in events if e.get('event') == 'lineage-history-backend']
    assert present['present'] is True and present['database'] == database
    assert type(present['pid']) is int and present['pid'] > 0
    assert type(present['control_epoch']) is int and present['control_epoch'] > 0
    instant(present['backend_start'])
    assert absent == dict(present,present=False)
    complete = events[-1]
    assert complete == dict(event='lineage-retirement-complete',operation='retire',native='closed',
        control='released-and-joined' if delivered else 'dropped-and-joined',observer='joined',
        success=delivered,source_closure=False,workflow=False,retirement=delivered,admission=False,activation=False)
    assert native < events.index(control) < events.index(absent) < events.index(observer) < len(events)-1
    assert not any(e.get('event') == 'lineage-history-timeout' for e in events)


def absence_checked(value, database, events):
    assert set(value) == {'database','user','limit_seconds','status','attempts','seconds'}
    assert value['database'] == database and value['user'] == 'bigbrotr_control'
    assert type(value['limit_seconds']) is int and value['limit_seconds'] == 5
    assert value['status'] == 'OBSERVED_ABSENT' and value['attempts']
    backend = only(events,event='lineage-history-backend',present=True)
    elapsed = 0
    for attempt in value['attempts']:
        assert set(attempt) == {'seconds','count','backends'}
        seconds = attempt['seconds']
        assert type(seconds) in (int,float) and math.isfinite(seconds) and elapsed <= seconds <= 5
        elapsed = seconds
        assert type(attempt['count']) is int and attempt['count'] == len(attempt['backends'])
        assert attempt['count'] in (0,1)
        for observed in attempt['backends']:
            assert set(observed) == {'pid','backend_start'}
            assert type(observed['pid']) is int and observed['pid'] == backend['pid']
            assert instant(observed['backend_start']) == instant(backend['backend_start'])
    assert value['attempts'][-1]['backends'] == []
    assert type(value['seconds']) in (int,float) and math.isfinite(value['seconds']) and value['seconds'] >= elapsed


def replay(root):
    from retirement_fault_replay import verify, negative_controls
    root = Path(root).resolve(strict=True)
    folder = root/'retirement-faults'
    bound = {}

    def raw(path):
        path = Path(path)
        content = path.read_bytes()
        bound[str(path)] = sha(content)
        return content

    def read(path):
        return loads(raw(path))

    report = read(folder/'result.json')
    assert report['status'] == STATUS and report['label'] == 'org.bigbrotr.succession-retirement-fault'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{40}',report['tree'])
    assert re.fullmatch('[0-9a-f]{32}',report['owner']) and re.fullmatch('[0-9a-f]{64}',report['client'])
    assert [c['name'] for c in report['cases']] == [s[0] for s in SPECS]
    assert [c['name'] for c in report['clones']] == ['before','after','cancel','expiry']
    assert report['databases'] == [c['database'] for c in report['clones']]
    assert len(set(report['databases'])) == 4
    assert not set(report['databases']) & set(report['database_names_before'])
    assert set(report['database_names_after']) == set(report['database_names_before']) | set(report['databases'])
    for key in ('cleanup_errors','missing_owned','unrelated_changed','unexpected_containers'):
        assert report[key] == []
    started, finished = instant(report['started_at']), instant(report['finished_at'])
    assert started <= finished
    assert set(report['origins']) == {'genuine-gap'}
    pin = capture.PINS['genuine-gap']
    origin = capture.origin_checked(root,folder,'genuine-gap',pin,report['origins']['genuine-gap'],raw,read)
    clones, chains, windows = {}, {}, {}
    for clone in report['clones']:
        name = clone['name']
        assert clone['origin'] == 'genuine-gap' and clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['database'] == 'bb_successor_retirement_fault_' + report['source'][:7] + '_' + name
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        chains[name] = capture.clone_checked(folder,clone,origin,raw,read)
        clones[name] = clone
    checked, diagnostics = [], {}
    for case,spec in zip(report['cases'],SPECS,strict=True):
        name, clone_name, predecessor, point = spec
        clone = clones[clone_name]
        loss, waiting, delivered = point in ('before-forward','after-complete'), point in ('cancel','expiry'), point is None
        assert all(case[k] == v for k,v in dict(name=name,clone=clone_name,origin='genuine-gap',
            operation='retire',predecessor=predecessor,metadata_bytes=262144,status='PASS',
            database=clone['database'],config=clone['config']).items())
        assert type(case['metadata_bytes']) is int
        assert case['catalog_after'] == clone['catalog_before']
        assert case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(case['returncode']) is int and case['returncode'] == 0
        item = lambda suffix: read(folder/(name+'-'+suffix+'.json'))
        before, native, inputs = item('before'), item('before-native'), item('inputs')
        same(chains[clone_name][0],before)
        assert native == chains[clone_name][1] and inputs == chains[clone_name][2]
        assert item('capture-errors') == []
        response = item('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error','orchestration_error','cleanup_error','clock_error','response_error','custody_error'))
        assert type(response['seconds']) in (int,float) and math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        assert 'outcome' not in response or response['outcome'] == 'PROCESS_EXITED'
        if waiting:
            assert response['readers_joined'] is True and response['overflow'] is False
            assert response['outcome'] == 'PROCESS_EXITED'
            assert response['command'] == ['docker','exec','bigbrotr-successor-retirement-fault-'+report['source'][:7],
                '/build/ingest_bootstrap_probe','successor-lineage-retire-wait',case['database'],
                '/run/secrets/application',case['config'],'262144',point,'/lab/'+name+'-gate']
        events = [loads(line) for line in response['stdout'].splitlines()]
        custody(events,case['database'],loss=loss,delivered=delivered)
        window = item('clock')
        assert window == case['server_window'] and started <= window['before_us'] <= window['after_us'] <= finished
        assert windows.get(clone_name,0) <= window['before_us']
        fault = item('fault')
        if loss:
            assert fault == case['fault'] and item('wire') == fault['wire']
            assert sha(raw(folder/'successor-retirement-fault-proxy.py')) == '6e737de8223818d0b05b47cb25f16038233d43181b12aec97bd13e3b44e3f829'
        else:
            assert fault is None and case['proxy_port'] is None
        schedule, diagnostic = (item('schedule'),item('diagnostic')) if waiting else (None,None)
        if waiting:
            for suffix in ('.go','.cancel') if point == 'cancel' else ('.go',):
                gate = folder/(name+'-gate'+suffix)
                assert stat.S_ISREG(gate.lstat().st_mode) and raw(gate) == b''
            if point == 'expiry':
                assert not (folder/(name+'-gate.cancel')).exists()
        args = dict(wire=fault,schedule=schedule,diagnostic=diagnostic)
        result = verify(events,before,native,inputs,case,window,**args)
        assert result['status'] == 'PASS' and result['database'] == case['database']
        final = only(events,event='lineage-history-state',stage='final')
        after, after_native, after_inputs = item('after'),item('after-native'),item('after-inputs')
        same({k:loads(v) for k,v in final['state']['control_tables']},after)
        assert after_native == dict(event='dump-state',rows=final['rows'],sequence=final['state']['sequence']) == native
        assert after_inputs == dict(final['state']['originals']) == inputs
        absence_checked(item('control-backends'),case['database'],events)
        assert item('backends') == []
        assert item('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        if point == 'expiry':
            log = raw(folder/'postgres.log').decode()
            observed = capture.server_diagnostic(log,events,case['database'],window,
                'lineage retirement Control authority expired',
                r'PL/pgSQL function control\.require_ingest_successor\(bytea,numeric,text\) line \d+ at RAISE')
            assert diagnostic['returncode'] == 0 and diagnostic['backend'] == observed['backend']
            assert diagnostic['observed_sqlstate'] == observed['observed_sqlstate']
            retained_lines = set(log.splitlines())
            for line in (diagnostic['stdout']+diagnostic['stderr']).splitlines():
                # docker logs --timestamps adds a transport timestamp to each
                # actual PostgreSQL line; retain its original server timestamp.
                line = re.sub(r'^\d{4}-\d\d-\d\dT\S+ ', '', line)
                assert not line or line in retained_lines, 'diagnostic differs from complete server log'
            diagnostics[name] = observed
        result['negative_controls'] = negative_controls(events,before,native,inputs,case,window,**args)
        assert result['negative_controls']['status'] == 'PASS'
        checked.append(result)
        chains[clone_name], windows[clone_name] = (after,after_native,after_inputs),window['after_us']
    for name,clone in clones.items():
        assert clone['final_case'] == [s[0] for s in SPECS if s[1] == name][-1]
        same(chains[name][0],read(folder/(name+'-final-sql.json')))
        capture.spool_checked(folder/name/'store/producer-inputs-v1',chains[name][2],raw,complete=True)
    terminal = read(folder/'terminal-containers.json')
    for filename,ident in [('client-terminal.json',report['client']),('postgres-terminal.json',report['postgres'])]:
        value = read(folder/filename)
        assert value == terminal[ident] and value['Id'] == ident
        state = value['State']
        assert state['Status'] == 'exited' and state['Pid'] == 0
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead')) and not state['Error']
    assert report['postgres'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert report['postgres_owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
    return dict(status=STATUS+'_REPLAY',source=report['source'],cases=checked,
        snapshots=sum(c['snapshots'] for c in checked),controls=sum(c['negative_controls']['count'] for c in checked),
        diagnostics=diagnostics,bound_files=bound,
        original_preservation='Actual retained file census; SQL/catalog preservation is captured-run evidence',
        scope='Finite C10 reply loss and fresh retry; final-job typed cancellation and real UTC expiry; no installed or capacity qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    result = json.dumps(replay(args.root),indent=2)+'\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(result)
    else:
        print(result,end='')
