#!/usr/bin/env python3
"""Actual Rust registry transactions in an explicitly owned PostgreSQL lab."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "experiments/control-migrations"))
import lab
from fault_cases import Process, require, safe_event

LABEL = "org.bigbrotr.ingest-registry-lab"
NAMESPACE, REGISTRATION = 0x42427472, 0x494e4752
CASES = ("direct", "before", "after", "cancel", "race")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(state, database):
    tables = json.loads(lab.sql(state, database, "SELECT json_agg(c.relname ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control' AND c.relkind='r';"))
    require(len(tables) == 23, "unexpected-table-set")
    for table in tables:
        require(table.replace('_', '').isascii() and table.replace('_', '').isalnum(), "unexpected-table-name")
    # One statement gives every family the same PostgreSQL snapshot; the row
    # limit is an excess discriminator, never silent truncation of an oracle.
    fields = [f"'{table}', (SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT * FROM control.{table} LIMIT 65) t)" for table in tables]
    result = json.loads(lab.sql(state, database, "SELECT jsonb_build_object(" + ",".join(fields) + ");"))
    require(all(len(rows) <= 64 for rows in result.values()), "unexpected-fixture-row-count")
    return result


def expected_registered(before):
    """Independent complete-row oracle from the SQL contract, not adapter output."""
    expected = copy.deepcopy(before)
    b = lambda n: "\\x" + f"{n:02x}" * 16
    expected['datasets'][0]['topology_generation'] = 0
    expected['role_instances'].append(dict(role_instance_id=b(7), dataset_id=b(1), role='ingest', created_at_us=100, retired_at_us=None))
    expected['owner_epochs'].append(dict(resource_id=b(6), epoch=1, dataset_id=b(1), role='ingest', role_instance_id=b(7), process_incarnation=b(5), acquired_at_us=100, lease_expires_at_us=60000100, state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None))
    expected['topology_generations'] = [dict(dataset_id=b(1), generation=0, previous_generation=None, created_at_us=100, sealed=True)]
    expected['topology_streams'] = [dict(dataset_id=b(1), topology_generation=0, shard_id=4294967295, owner_epoch=1, handoff_last_complete_sequence=None)]
    expected['source_streams'] = [dict(dataset_id=b(1), shard_id=4294967295, owner_epoch=1, owner_resource_id=b(6), owner_role='ingest', owner_incarnation=b(5), opened_at_us=100, state='open', closed_at_us=None, last_complete_sequence=None, closure_digest=None)]
    expected['allocation_domains'] = [dict(dataset_id=b(1), dictionary_kind=kind, allocation_domain=b(domain), owner_resource_id=b(6), owner_epoch=1, owner_role='ingest', owner_incarnation=b(5), created_at_us=100, state='active', closed_at_us=None, closure_digest=None) for kind, domain in [(4, 10), (8, 11)]]
    expected['producer_slots'] = [dict(producer_slot_id=b(12), dataset_id=b(1), registered_by_resource_id=b(3), registered_by_epoch=1, registered_by_role='control', registered_by_incarnation=b(5), created_at_us=100)]
    expected['producer_epochs'] = [dict(producer_slot_id=b(12), producer_epoch=1, dataset_id=b(1), producer_role_instance_id=b(7), producer_role='ingest', producer_incarnation=b(5), owner_resource_id=b(6), owner_epoch=1, owner_role='ingest', owner_incarnation=b(5), outstanding_request_limit=2, opened_at_us=100, state='open', retired_at_us=None, closure_digest=None)]
    return expected


def ordered(value):
    return {key: sorted(rows, key=lambda row: json.dumps(row, sort_keys=True)) for key, rows in value.items()}


def probe(container, mode, database, port=5432, role='application'):
    return Process(['docker', 'exec', container, '/build/ingest_registry_probe', mode, database, str(port), role, '/lab/secrets/' + role])


def result(process, mode):
    code = process.wait(45)
    require(code == 0 and not process.stderr, "probe-exit-or-stderr")
    rows = [json.loads(line) for line in process.stdout.splitlines()]
    require(rows and len(rows) <= 8, "probe-result-count")
    for row in rows:
        require(set(row) == {'format', 'event', 'result'} and row['format'] == 'bigbrotr-ingest-registry-probe/1', 'probe-result-shape')
        require(row['event'] in {mode, 'commit-unknown', 'resolving-held'} and row['result'] in {'PASS', 'observed', 'started'}, 'probe-result-value')
    require(rows[-1] == dict(format='bigbrotr-ingest-registry-probe/1', event=mode, result='PASS'), 'probe-result-final')
    return rows


def wait_for_registry_waiter(state, database):
    end = time.monotonic() + 3.5
    while time.monotonic() < end:
        rows = json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(json_build_object('pid',a.pid,'backend_start',a.backend_start,'wait_event_type',a.wait_event_type,'wait_event',a.wait_event,'blocking',pg_blocking_pids(a.pid),'granted',l.granted,'mode',l.mode)), '[]') FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND a.application_name='bigbrotr-registry-probe' AND l.locktype='advisory' AND l.classid={NAMESPACE} AND l.objid={REGISTRATION} AND l.objsubid=2;"))
        holders = {r['pid'] for r in rows if r['granted'] and r['mode'] == 'ExclusiveLock'}
        if any(not r['granted'] and r['mode'] == 'ExclusiveLock' and r['wait_event_type'] == 'Lock' and r['wait_event'] == 'advisory' and holders.intersection(r['blocking']) for r in rows):
            return rows
        time.sleep(.03)
    raise RuntimeError('actual-registry-lock-wait-not-observed')


def case(root, state, container, name, save):
    database = 'bb_registry_' + name
    lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
    migrated = lab.migrate(state, 'apply', lab.config(root, state, database), target=2)
    require(migrated.returncode == 0 and json.loads(migrated.stdout)['applied_count'] == '2', 'migration-prerequisite')
    lab.sql(state, database, (SOURCE / 'experiments/ingest-registry/seed.sql').read_text())
    before = snapshot(state, database)
    require(before['datasets'][0]['topology_generation'] is None and not before['source_streams'] and not before['producer_slots'], 'initial-registry-not-empty')
    expected = expected_registered(before)
    entry = {'name': name, 'status': 'RUNNING', 'database': database, 'before': before, 'expected_after': copy.deepcopy(expected), 'steps': []}
    save(entry)
    processes = []
    def direct(mode, role='application'):
        p = probe(container, mode, database, role=role); processes.append(p)
        entry['steps'].append(result(p, mode)); save(entry)
    try:
        direct('absent'); direct('rejects'); direct('migration-role', 'migrator')
        require(ordered(snapshot(state, database)) == ordered(before), 'refusals-mutated-registry')
        if name == 'direct':
            direct('register')
        else:
            mode = {'before': 'before-forward', 'after': 'after-complete', 'cancel': 'hold', 'race': 'hold'}[name]
            proxy = Process(['docker', 'exec', '-i', container, 'python3', '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432', '--mode', mode, '--deadline-seconds', '35'], input_pipe=True)
            processes.append(proxy)
            ready = proxy.event('ready', 5)
            require(ready['proxy_sha256'] == sha(SOURCE / 'experiments/control-migrations/fault_proxy.py'), 'proxy-source-mismatch')
            operation = {'before':'lost-before','after':'lost-after','cancel':'cancel-held','race':'register'}[name]
            first = probe(container, operation, database, ready['listen_port']); processes.append(first)
            if name in ('cancel', 'race'):
                proxy.event('held', 5)
                second = None
                if name == 'race':
                    second = probe(container, 'register', database); processes.append(second)
                entry['lock_wait'] = wait_for_registry_waiter(state, database); save(entry)
                require(ordered(snapshot(state, database)) == ordered(before), 'uncommitted-registry-visible')
                proxy.send('release')
                if second: entry['steps'].append(result(second, 'register'))
            entry['steps'].append(result(first, operation))
            receipt = proxy.event('receipt', 5)
            require(proxy.wait(5) == 0 and not proxy.stderr, 'proxy-incomplete')
            require(receipt['commits_seen'] == 1 and receipt['faults'] == 1, 'proxy-fault-not-exercised')
            if name == 'before': require(receipt['commits_forwarded'] == 0 and receipt['server_commit_complete'] == 0, 'before-boundary')
            else: require(receipt['commits_forwarded'] == 1 and receipt['server_commit_complete'] == 1, 'after-boundary')
            if name == 'after': require(receipt['client_commit_complete'] == 0, 'reply-not-lost')
            entry['proxy'] = [safe_event(row) for row in proxy.events]
        observed = snapshot(state, database)
        entry['after'] = observed
        require(ordered(observed) == ordered(expected), 'complete-external-rows-mismatch')
        direct('repeat'); direct('conflict')
        require(ordered(snapshot(state, database)) == ordered(expected), 'retry-or-conflict-mutated-registry')
        lab.sql(state, database, "UPDATE control.producer_epochs SET state='closing';")
        direct('stale')
        expected['producer_epochs'][0]['state'] = 'closing'
        final = snapshot(state, database)
        require(ordered(final) == ordered(expected), 'stale-observation-mutated-registry')
        entry['expected_final'] = copy.deepcopy(expected); entry['final'] = final
        deadline = time.monotonic() + 3
        while True:
            active = int(lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-registry-probe';"))
            if active == 0: break
            require(time.monotonic() < deadline, 'probe-backends-remain')
            time.sleep(.03)
        entry['status'] = 'PASS'; save(entry)
    finally:
        for process in processes: process.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--build', type=Path, required=True)
    args = parser.parse_args()
    root, build = args.root.resolve(strict=True), args.build.resolve(strict=True)
    state = lab.load(root)
    require(lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout == '', 'dirty-harness')
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    receipt = json.loads((build / 'registry-probe-build.json').read_text())
    require(receipt['source_revision'] == revision == state['build']['build_info']['source']['revision'], 'source-revision-mismatch')
    require(receipt['binary_sha256'] == sha(build / 'ingest_registry_probe'), 'probe-artifact-mismatch')
    require(sha(build / 'build-result.json') == receipt['parent_build_receipt_sha256'], 'parent-build-mismatch')
    require(Path(state['source']) == SOURCE and state['binary_sha256'] == sha(build / 'bigbrotr'), 'lab-source-artifact-mismatch')
    for name in (state['postgres'], state['client']):
        info = json.loads(lab.docker('inspect', name).stdout)[0]
        require(info['State']['Running'], 'lab-not-running')
        if name == state['client']:
            mounts = {m['Destination']: m for m in info['Mounts']}
            require(all(target in mounts and Path(mounts[target]['Source']) == path
                        and not mounts[target]['RW']
                        for target, path in [('/build', build), ('/source', SOURCE)]),
                    'parent-client-mount-mismatch')
    output = root / 'registry'; output.mkdir(mode=0o700, exist_ok=False)
    owner = secrets.token_hex(16); container = state['client'] + '-registry'
    report = {'format':'bigbrotr-ingest-registry-engine/1','status':'RUNNING','owner':owner,'container':container,'source':revision,'probe_build':receipt,'cases':[], 'limits':'2 CPU,4 GiB memory+swap,256 PIDs,readonly root,no capabilities,PG network namespace,no published ports','scope':'Synthetic control prerequisites; actual typed registry only. No validated product policy, native owner activation or event admission.'}
    def save(entry=None):
        if entry is not None:
            report['cases'] = [row for row in report['cases'] if row['name'] != entry['name']] + [copy.deepcopy(entry)]
        lab.write_json(output / 'report.json', report)
    save()
    require(lab.docker('container', 'inspect', container, check=False).returncode != 0, 'preexisting-probe-container')
    created = False
    try:
        lab.docker('run','-d','--init','--name',container,'--label',LABEL+'='+owner,'--user',f'{os.getuid()}:{os.getgid()}', '--network','container:'+state['postgres'],'--cpus','2','--memory','4g','--memory-swap','4g','--pids-limit','256','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,exec,nosuid,nodev,size=128m','--mount',f'type=bind,src={root},dst=/lab','--mount',f'type=bind,src={build},dst=/build,readonly','--mount',f'type=bind,src={SOURCE},dst=/source,readonly',state['client_image'],'python3','-c','import time; time.sleep(3600)')
        created = True
        info = json.loads(lab.docker('inspect', container).stdout)[0]
        require(info['Config']['Labels'].get(LABEL) == owner and info['State']['Running'], 'probe-ownership')
        host = info['HostConfig']
        require(host['Memory'] == host['MemorySwap'] == 4 * 1024 ** 3
                and host['NanoCpus'] == 2 * 10 ** 9 and host['PidsLimit'] == 256
                and host['ReadonlyRootfs'] and host['CapDrop'] == ['ALL']
                and any(option.startswith('no-new-privileges') for option in host['SecurityOpt'])
                and not host['PortBindings'] and host['NetworkMode'].startswith('container:'),
                'probe-resource-envelope')
        report['container_id'] = info['Id']; save()
        for name in CASES: case(root, state, container, name, save)
        require(len(report['cases']) == len(CASES) and all(row['status'] == 'PASS' for row in report['cases']), 'incomplete-cases')
        report['status'] = 'PASS'
    except Exception as error:
        report['status'] = 'FAIL'; report['failure'] = str(error); raise
    finally:
        if created:
            info = json.loads(lab.docker('inspect', container).stdout)[0]
            require(info['Config']['Labels'].get(LABEL) == owner, 'cleanup-ownership-mismatch')
            lab.docker('stop', '--time', '5', container)
            info = json.loads(lab.docker('inspect', container).stdout)[0]
            report['stopped'] = not info['State']['Running']; report['oom_killed'] = info['State']['OOMKilled']
            if not report['stopped'] or report['oom_killed']: report['status'] = 'FAIL'
        save()
    print(json.dumps({'status':report['status'],'cases':len(report['cases']),'report':str(output / 'report.json')}))


if __name__ == '__main__':
    main()
