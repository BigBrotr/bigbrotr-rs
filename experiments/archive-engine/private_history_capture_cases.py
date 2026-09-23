#!/usr/bin/env python3
"""Real post-native catalog barrier: paired successful delivery and backend loss.

Run after private_history_fault_cases.py in the exclusively owned laboratory.
The proxy holds only the final verification Bind, after reader traffic. The host
then verifies child absence, retained pin/claim and the actual manager session.
No SQL, credentials or event bytes are logged by the proxy.
"""
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
from signed_publication_cases import command, event, send, sha, Process


def main(root):
    resources = json.loads((root / 'resources.json').read_text())
    source = root / 'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels'][resources['owner_label']] == resources['owner'] and c['State']['Running']
    assert json.loads((root / 'private-history-fault-cases.json').read_text())['status'] == 'PASS'
    result = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE', cases=[])
    path = root / 'private-history-capture-cases.json'
    assert not path.exists()
    def save(): path.write_text(json.dumps(result, indent=2) + '\n')
    def sql(database, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, input=query)
    def control(query): return json.loads(sql('bbpub_control_normal', query))
    def counts():
        return control("SELECT json_build_object('active_pins',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),'active_claims',(SELECT count(*) FROM control.retention_claims WHERE state='active'),'proofs',(SELECT count(*) FROM control.reauthentication_proofs),'commands',(SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'))")
    def manager(pid):
        return json.loads(sql('bbpub_normal', f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'state',state,'transaction',xact_start IS NOT NULL)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager' AND pid={int(pid)}"))
    def files():
        return {str(p.relative_to(root / 'out')): sha(p) for p in sorted((root / 'out/signed_normal').rglob('*.parquet'))}
    inspect_process = """import json,sys
from pathlib import Path
parent=int(sys.argv[1]); children=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  stat=(p/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
  if int(fields[1])==parent:children.append(dict(pid=int(p.name),state=fields[0],start_ticks=int(fields[19])))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
stat=Path('/proc',str(parent),'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
print(json.dumps(dict(pid=parent,state=fields[0],start_ticks=int(fields[19]),children=children)))
"""
    try:
        revision = int(sql('bbpub_control_normal', "SELECT policy_revision FROM control.datasets WHERE dataset_id=decode('01010101010101010101010101010101','hex')"))
        result['files_before'] = files()
        assert len(result['files_before']) == 8
        for mode in ['capture-pass', 'manager-loss']:
            record = dict(case='http-history-' + mode, status='INCOMPLETE')
            result['cases'].append(record); save()
            proxy = client = None
            try:
                proxy = Process(['docker', 'exec', '-i', runner, 'python3', '-B', '/out/build/source/experiments/archive-engine/native_commit_proxy.py', '--mode', 'hold-capture-verify'], input_pipe=True)
                ready = record['proxy_ready'] = event(proxy, 'ready')
                assert ready['source_sha256'] == sha(source / 'experiments/archive-engine/native_commit_proxy.py')
                client = Process(['docker', 'exec', '-i', runner, 'python3', '-B', '/out/build/source/experiments/archive-engine/private_history_fault_client.py', mode, str(ready['port']), str(revision)], input_pipe=True)
                submitted = record['submitted'] = event(client, 'submitted')
                held = record['held'] = event(proxy, 'held', timeout=3)
                assert held['stage'] == 'capture-verify' and held['reader_queries'] > 0 and held['capture_verify_requests'] == 1
                record['process'] = json.loads(command('docker', 'exec', runner, 'python3', '-c', inspect_process, str(submitted['parent_pid'])))
                assert record['process']['state'] not in ('Z', 'X') and not record['process']['children']
                record['manager'] = manager(held['backend_pid'])
                assert len(record['manager']) == 1 and record['manager'][0]['state'] == 'idle in transaction' and record['manager'][0]['transaction']
                record['retained_at_barrier'] = counts()
                assert record['retained_at_barrier'] == dict(active_pins=1, active_claims=1, proofs=1, commands=3, active_control=1)
                record['pin'] = control("SELECT json_agg(json_build_object('lease_id',encode(l.lease_id,'hex'),'revision',l.lease_revision,'snapshot',f.snapshot_id,'remaining_us',l.expires_at_us-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) JOIN control.archive_frontier_snapshots f ON (f.consumer_id,f.frontier_revision)=(p.consumer_id,p.frontier_revision) WHERE l.state='active'")
                assert len(record['pin']) == 1 and record['pin'][0]['snapshot'] == 3 and record['pin'][0]['remaining_us'] > 2_000_000
                save()
                started = time.monotonic()
                if mode == 'manager-loss':
                    # The public backend PID came from this owner-verified proxy;
                    # check its role/database/transaction immediately before kill.
                    assert manager(held['backend_pid']) == record['manager']
                    assert sql('bbpub_normal', f"SELECT pg_terminate_backend({int(held['backend_pid'])},1000)") == 't'
                    assert manager(held['backend_pid']) == []
                    record['manager_termination'] = 'confirmed-absent'
                send(proxy, 'release\n'); event(proxy, 'released')
                send(client, 'observe\n')
                if mode == 'capture-pass':
                    record['healthy'] = event(client, 'healthy', timeout=3)
                    record['before_stop'] = counts()
                    assert record['before_stop'] == dict(active_pins=0, active_claims=0, proofs=1, commands=3, active_control=1)
                    send(client, 'stop\n')
                record['client_exit'] = client.wait(10)
                record['completion_ms'] = round((time.monotonic() - started) * 1000, 3)
                record['client_terminal'] = event(client, 'terminal')
                record['client_error'] = client.stderr.decode()
                assert record['client_exit'] == 0 and not client.stderr and not client.overflow
                record['durable_after'] = counts()
                assert record['durable_after'] == dict(active_pins=0, active_claims=0, proofs=1, commands=3, active_control=int(mode == 'manager-loss'))
                assert record['client_terminal']['parent_exit'] == (4 if mode == 'manager-loss' else 0)
                record['files_after'] = files()
                assert record['files_after'] == result['files_before']
            finally:
                cleanup_errors = []
                try:
                    if client is not None:
                        client.stop()
                        record['client_output'] = client.stdout.decode()
                        record['client_error'] = client.stderr.decode()
                except BaseException as error:
                    cleanup_errors.append(dict(stage='client', error=type(error).__name__))
                try:
                    if proxy is not None:
                        try:
                            if proxy.process.poll() is None: send(proxy, 'stop\n')
                            record['proxy_exit'] = proxy.wait(5)
                            record['proxy_terminal'] = event(proxy, 'terminal')
                        finally: proxy.stop()
                except BaseException as error:
                    cleanup_errors.append(dict(stage='proxy', error=type(error).__name__))
                try:
                    record['retained_terminal'] = counts()
                    record['terminal_backends'] = {database: int(sql(database, "SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in ['bbpub_normal', 'bbpub_control_normal']}
                except BaseException as error:
                    cleanup_errors.append(dict(stage='terminal-sql', error=type(error).__name__))
                record['cleanup_errors'] = cleanup_errors; save()
            assert not cleanup_errors and record['proxy_exit'] == 0 and record['proxy_terminal']['unexpected'] == 0
            assert record['retained_terminal'] == record['durable_after'] and not any(record['terminal_backends'].values())
            record['status'] = 'PASS'; save()
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = type(error).__name__
        raise
    finally: save()
    print(json.dumps(dict(status=result['status'], cases=len(result['cases']), source=resources['source'])))


if __name__ == '__main__': main(Path(sys.argv[1]).resolve())
