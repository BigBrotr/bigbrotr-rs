"""Late initializer faults; called by initialization_cli_cases.py --late-faults.

Real executable and engines only. The enclosing exact-owner audit must stop the
lab on any failure; closing a local docker-exec wrapper is never exclusion.
"""
import hashlib
import json
from pathlib import Path
import time

from catalog_install_cases import native_rows
from fault_cases import Process
from signed_publication_cases import event, send


# Open pidfds before signaling either process. Verify the full installed argv,
# executable, start time and parent relation while the selected COMMIT is held.
CRASH = r'''import json,os,select,signal,sys,time
from pathlib import Path
record=json.loads(Path(sys.argv[1]).read_text())
kind,target,label,nonce=sys.argv[2:]
def identity(pid):
 p=Path('/proc/'+str(pid)); fields=p.joinpath('stat').read_text().rsplit(')',1)[1].split()
 return dict(pid=pid,start=fields[19],parent=int(fields[1]),exe=os.readlink(p/'exe'),
             argv=[s.decode() for s in p.joinpath('cmdline').read_bytes().rstrip(bytes([0])).split(bytes([0]))])
parent=identity(record['pid']); assert parent['start']==record['start'] and parent['argv']==record['argv']
assert parent['exe']=='/out/native-tests/bigbrotr'
fds=[os.pidfd_open(parent['pid'])]; identities=[parent]
try:
 workers=[]
 for p in Path('/proc').iterdir():
  if not p.name.isdigit(): continue
  try:
   if p.joinpath('cmdline').read_bytes().split(bytes([0]))[0]!=b'/out/archive-worker/bigbrotr-archive-worker': continue
   workers.append(identity(int(p.name)))
  except (FileNotFoundError,ProcessLookupError): pass
 assert len(workers)==int(target=='native-schema')
 if workers:
  worker=workers[0]
  assert worker['parent']==parent['pid'] and worker['exe']=='/out/archive-worker/bigbrotr-archive-worker'
  assert worker['argv'][:4]==[worker['exe'],'initialize-reserved-catalog','/out/archive-worker','/out/initialization-cli/'+label+'-scratch/initialize-'+nonce]
  assert len(worker['argv'])==7 and all(s.isdecimal() for s in worker['argv'][4:])
  fds.append(os.pidfd_open(worker['pid']));identities.append(worker)
 for expected in identities: assert identity(expected['pid'])==expected
 selected=0 if kind=='coordinator' else 1
 started=time.monotonic();signal.pidfd_send_signal(fds[selected],signal.SIGKILL)
 observed=[]
 for index in ([0,1] if selected==0 and workers else [selected]):
  poll=select.poll();poll.register(fds[index],select.POLLIN)
  assert poll.poll(max(0,int((started+5-time.monotonic())*1000)))
  observed.append(identities[index]['pid'])
 print(json.dumps(dict(identities=identities,killed=identities[selected]['pid'],exited=observed,
                      seconds=time.monotonic()-started,pidfd_exit_before_proxy_stop=True)))
finally:
 for fd in fds: os.close(fd)
'''


def run(c):
    artifact = c.root / 'out/archive-worker/bigbrotr-archive-worker'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == c.report['artifacts']['worker']
    proxy_source = c.source / 'experiments/archive-engine/native_commit_proxy.py'
    c.report['late_faults_sources'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in [Path(__file__), proxy_source]}

    def dump_rows(db):
        dump = c.command(['docker', 'exec', c.state['postgres'], 'pg_dump', '-U', 'postgres',
                          '-d', db, '--data-only', '--schema=public'])
        return native_rows(dump)

    def root_state(root):
        path = c.root / root.removeprefix('/')
        values = []
        for p in [path, *sorted(path.rglob('*'))]:
            st = p.lstat()
            assert not p.is_symlink()
            values.append([str(p.relative_to(path)), st.st_dev, st.st_ino, st.st_mode,
                           hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None])
        assert any(v[-1] is not None for v in values), 'reservation marker missing'
        return values

    def resolved(db, complete, guard):
        assert c.sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'", db) == '28'
        assert c.sql("SELECT count(*) FROM pg_namespace WHERE nspname='bigbrotr_archive'", db) == str(int(guard))
        names = c.sql('SELECT table_name FROM public.ducklake_table ORDER BY table_name', db).splitlines()
        assert names == (['archive_commits', 'dictionaries', 'event_eligibility_facts',
                          'event_receive_facts', 'events', 'provenance_facts'] if complete else [])
        assert c.sql('SELECT count(*) FROM public.ducklake_column', db) == ('203' if complete else '0')
        snapshots = c.sql('SELECT snapshot_id::text FROM public.ducklake_snapshot ORDER BY snapshot_id', db).splitlines()
        assert snapshots == (['0', '1'] if complete else ['0'])
        assert c.sql('SELECT count(*) FROM public.ducklake_data_file', db) == '0'
        if complete:
            checksum = hashlib.sha256((c.source / 'contracts/archive/v1/schema.json').read_bytes()).hexdigest()
            # Compile-time canonical declaration checksum is independently read
            # from the checked-in contract, never inferred from the CLI result.
            assert c.sql("SELECT count(*) FROM public.ducklake_snapshot_changes WHERE snapshot_id=1 AND author='BigBrotr' AND commit_message='initialize archive schema 1' AND commit_extra_info='sha256:" + checksum + "'", db) == '1'
        _, oid = c.bootstrap(db)
        owner_oid = c.sql("SELECT oid FROM pg_roles WHERE rolname='CatalogManager'") if guard else str(oid)
        assert c.sql("SELECT bool_and(relowner=" + owner_oid + "::oid) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'", db) == 't'
        return dict(native_tables=28, scientific_tables=names, columns=203 if complete else 0,
                    snapshots=snapshots, data_files=0, guard_installed=guard, owner_oid=int(owner_oid))

    def one(item, label, target, mode, crash):
        db = c.fresh(label.replace('-', '_'))
        proxy = Process(['docker', 'exec', '-i', c.state['container'], 'python3', '-B',
            '/source/experiments/archive-engine/native_commit_proxy.py', '--target', target, '--mode', mode], input_pipe=True)
        c.children.append(proxy)
        hello = event(proxy, 'ready', 10)
        assert hello['source_sha256'] == c.report['late_faults_sources'][proxy_source.name]
        selected, root = c.config(db, label, port=hello['port'])
        direct, _ = c.config(db, label+'-direct', root=root)
        item.update(database=db, target=target, mode=mode, crash=crash); c.save()
        if crash:
            cli = c.managed_cli('initialize', selected, label)
            held = event(proxy, 'held', 15)
            assert held['stage'] == ('reply' if mode == 'hold-reply' else 'commit')
            item['held'] = held; c.save()
            item['process_exclusion'] = json.loads(c.command(['docker', 'exec', c.state['container'],
                'python3', '-c', CRASH, '/out/initialization-cli/'+label+'.pid', crash, target, label, c.bootstrap(db)[0].removeprefix('bb_init_')], timeout=10))
            observed = c.finished(cli)
            assert observed['exit'] == (-9 if crash == 'coordinator' else 4)
        else:
            observed, _ = c.invoke('initialize', selected)
            assert observed['exit'] == 4
            item['fault'] = event(proxy, 'fault', 10)
        item['observed'] = observed; c.save()
        send(proxy, 'stop\n')
        terminal = event(proxy, 'terminal', 10)
        assert proxy.wait(10) == 0 and terminal['unexpected'] == 0
        committed = mode in ('after-complete', 'hold-reply')
        assert terminal['data_queries'] >= 1 and terminal['data_commits'] == 1
        assert terminal['server_complete'] == int(committed) and terminal['client_complete'] == 0
        assert terminal['faults'] == int(not crash)
        item['proxy'] = terminal; c.save()
        # Positive proxy join, coordinator termination and a fresh server view;
        # advisory lock alone would not delimit a native transaction.
        until = time.monotonic()+5
        while c.sql("SELECT count(*) FROM pg_stat_activity WHERE datname='"+db+"'") != '0':
            assert time.monotonic() < until
            time.sleep(.02)
        complete = target == 'guard-install' or committed
        guard = target == 'guard-install' and committed
        item['before'] = resolved(db, complete, guard)
        native_before, files_before, durable_before = dump_rows(db), root_state(root), c.durable(db)
        # Recovery must operate even with the entire verified worker bundle absent.
        # Restore it before testing a fresh-initialize refusal and the next case.
        bundle = artifact.parent; offline = bundle.with_name('archive-worker-offline')
        assert not offline.exists()
        bundle.rename(offline)
        try:
            recovered, error = c.invoke('recover', direct)
            item['recovered'] = recovered; c.save()
            assert recovered['exit'] == (0 if complete else 4)
            if complete:
                assert recovered['output']['new_installation'] == (not guard)
                item['installed'] = c.installed(db)
                inspected, _ = c.invoke('inspect', direct)
                assert inspected['exit'] == 0
            else:
                assert b'IncompatibleLayout' in error
                assert c.durable(db)['database_sha256'] == durable_before['database_sha256']
            _, oid = c.bootstrap(db)
            assert c.sql(f'SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={oid}') == 't'
            assert c.sql(f'SELECT count(*) FROM pg_stat_activity WHERE usesysid={oid}') == '0'
            stable = c.durable(db)
            repeated, _ = c.invoke('recover', direct)
            assert repeated['exit'] == (0 if complete else 4)
            if complete: assert repeated['output']['new_installation'] is False
            assert c.durable(db) == stable
            item.update(repeated=repeated, recovery_without_worker_bundle=True, bootstrap_retired=True)
        finally:
            offline.rename(bundle)
        assert dump_rows(db) == native_before and root_state(root) == files_before
        item['after'] = resolved(db, complete, complete)
        stable = c.durable(db)
        retried, _ = c.invoke('initialize', direct)
        assert retried['exit'] == 4 and c.durable(db) == stable and root_state(root) == files_before
        item.update(fresh_initialize_retry=retried, native_rows_unchanged=True, root_unchanged=True,
                    native_rows_sha256=hashlib.sha256(json.dumps(native_before).encode()).hexdigest(),
                    root_state=files_before, durable=c.durable(db), remaining_backends=0)

    for target in ['native-schema', 'guard-install']:
        for mode in ['before-forward', 'after-complete']:
            label = target+'-'+mode
            c.case(label, lambda item, label=label, target=target, mode=mode: one(item, label, target, mode, None))
        for mode in ['hold-commit', 'hold-reply']:
            label = target+'-kill-'+mode
            c.case(label, lambda item, label=label, target=target, mode=mode: one(item, label, target, mode, 'coordinator'))
    c.case('native-worker-sigkill', lambda item: one(item, 'native-worker-sigkill', 'native-schema', 'hold-commit', 'worker'))
