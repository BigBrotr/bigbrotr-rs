#!/usr/bin/env python3
"""Actual installed CLI and sealed-FD libpq TLS journeys in an isolated Linux lab.

Only this lab's PostgreSQL HBA/settings are changed and restored. Server-enforced
clientcert=verify-ca plus SCRAM is deliberate: initializer, generated bootstrap,
manager and reader have separate password identities but share the configured
client certificate. No claim of certificate-CN/login binding is made.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
import tls_cases
from fault_cases import Process


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert state['root'] == str(root) and state['state'] == 'running'
    assert SOURCE == root / 'source'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    for key, path in [('control', 'native-tests/bigbrotr'), ('worker', 'archive-worker/bigbrotr-archive-worker'),
                      ('receipt_reader', 'native-tests/archive-receipt-probe')]:
        assert sha(root / 'out' / path) == ready[key]
    for key in ('container_id', 'postgres_id'):
        info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert info['State']['Running'] and not info['HostConfig']['PortBindings']
    network = json.loads(lab.docker('network', 'inspect', state['network_id']).stdout)[0]
    assert network['Internal'] and network['Labels']['org.bigbrotr.publication'] == state['owner']
    work = root / 'out/native-mtls'
    work.mkdir(mode=0o700)
    output = root / 'native-mtls.json'
    output.open('x').close()
    report = dict(source=state['source'], harness_sha256=sha(Path(__file__)), artifacts=ready,
                  status='RUNNING', cases=[], authentication='clientcert=verify-ca plus scram-sha-256')
    pg = state['postgres_id']
    original_hba = original_auto = original_settings = None
    children = []

    def save(): lab.write_json(output, report)
    def sql(query, db='postgres'): return lab.sql(state, db, query)
    def command(*args): return lab.docker('exec', state['container_id'], *args)
    def reload():
        before = sql('SELECT pg_conf_load_time();')
        assert sql('SELECT pg_reload_conf();') == 't'
        until = time.monotonic() + 5
        while sql('SELECT pg_conf_load_time();') == before:
            assert time.monotonic() < until
            time.sleep(.02)
        assert sql('SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL;') == '0'
    def replace(path, content):
        actual = json.loads(lab.docker('inspect', pg).stdout)[0]
        assert actual['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and actual['State']['Running']
        assert path in ('/var/lib/postgresql/data/pg_hba.conf', '/var/lib/postgresql/data/postgresql.auto.conf')
        lab.docker('exec', '-i', pg, 'sh', '-c', 'cat > "$1"', 'sh', path, data=content)
        assert lab.docker('exec', pg, 'cat', path).stdout == content
    def auth(method='scram-sha-256', client=False):
        options = ' clientcert=verify-ca' if client else ''
        replace('/var/lib/postgresql/data/pg_hba.conf',
                f'hostssl all all 127.0.0.1/32 {method}{options}\n' + original_hba)
        reload()
        rule = json.loads(sql("SELECT row_to_json(r) FROM pg_hba_file_rules r WHERE line_number=1"))
        assert rule['type'] == 'hostssl' and rule['auth_method'] == method
        if client: assert 'clientcert=verify-ca' in rule['options']
        report['current_hba'] = rule; save()
    def durable(db):
        dump = lab.docker('exec', pg, 'pg_dump', '-U', 'postgres', '-d', db).stdout
        dump = '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
        roles = sql("SELECT md5(json_build_object('roles',(SELECT json_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object('credential',md5(rolpassword),'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),'members',(SELECT json_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text) ORDER BY roleid,member,grantor) FROM pg_auth_members m),'comments',(SELECT json_agg(to_jsonb(c) ORDER BY objoid,classoid) FROM pg_shdescription c))::text)")
        return dict(database_sha256=hashlib.sha256(dump.encode()).hexdigest(), global_roles_md5=roles)
    def case(name, operation):
        item = dict(name=name, status='INCOMPLETE'); report['cases'].append(item); save()
        operation(item)
        item['status'] = 'PASS'; save()
    def config(name, *, client=None, ca='ca', server=tls_cases.SERVER_NAME, cert=None, key=None):
        db = 'bbtls_' + name
        sql(f'CREATE DATABASE {db};')
        sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC;', db)
        data = work / (name + '-data'); data.mkdir(mode=0o700)
        scratch = work / (name + '-scratch'); scratch.mkdir(mode=0o700)
        fields = dict(mode='verify-full', ca_file=f'/out/native-mtls/pki/{ca}.crt', server_name=server)
        if client:
            fields.update(client_cert_ref=f'file:/out/native-mtls/pki/{cert or client}.crt',
                          client_key_ref=f'file:/out/native-mtls/pki/{key or client}.key')
        config = work / (name + '.toml')
        config.write_text(f'''[config]
schema_version=1
recipe="archive-only"
authority_mode="managed"
[topology]
local_roles=["control","discoverer","validator","monitor","sync","ingest","archive"]
instance_id="{'03'*16}"
dataset_id="{'01'*16}"
[archive]
catalog_id="{'02'*16}"
[storage]
archive_root="/out/native-mtls/{data.name}"
scratch_root="/out/native-mtls/{scratch.name}"
[postgres.catalog]
endpoint="127.0.0.1:5432"
database="{db}"
user="CatalogManager"
password_ref="file:/out/native-mtls/manager"
[postgres.catalog.tls]
''' + ''.join(f'{k}={json.dumps(v)}\n' for k, v in fields.items()) + '''[runtime.processes.application]
payload_bytes="128MiB"
''')
        return db, '/out/native-mtls/' + config.name, data
    def cli(action, path, baseline=False):
        binary = '/out/baseline-bigbrotr' if baseline else '/out/native-tests/bigbrotr'
        args = [binary, 'archive', action, '--file', path]
        if action != 'inspect':
            args += ['--initializer-user', 'postgres', '--initializer-password-ref', 'file:/out/native-mtls/initializer']
        if action == 'initialize': args += ['--worker-root', '/out/archive-worker']
        start = time.monotonic()
        actual = subprocess.run(['docker', 'exec', state['container_id'], *args], capture_output=True, timeout=160)
        assert len(actual.stdout) + len(actual.stderr) < 8192
        return dict(exit=actual.returncode, output=json.loads(actual.stdout) if actual.stdout else None,
                    stderr_nonempty=bool(actual.stderr), seconds=time.monotonic()-start), actual.stderr
    def installed(db):
        database_oid = sql(f"SELECT oid FROM pg_database WHERE datname='{db}'")
        comment = sql(f"SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='bb_init_database_{database_oid}'")
        prefix, nonce, bootstrap_oid, manager_oid = comment.split(':')
        assert prefix == 'bigbrotr-catalog-initialization/1'
        assert sql("SELECT oid FROM pg_roles WHERE rolname='CatalogManager'") == manager_oid
        assert sql(f"SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE oid={bootstrap_oid} AND rolname='bb_init_{nonce}'") == 't'
        assert sql(f'SELECT count(*) FROM pg_stat_activity WHERE usesysid={bootstrap_oid}') == '0'
        native = json.loads((SOURCE / 'contracts/archive/catalog-native-v1.json').read_text())['tables']
        expected = sorted(['public|' + n + '|CatalogManager' for n in native] + ['bigbrotr_archive|' + n + '|CatalogManager' for n in ['installation', 'owner', 'attempts', 'scientific_schema']])
        actual = sql("SELECT n.nspname||'|'||c.relname||'|'||r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r' ORDER BY 1", db).splitlines()
        assert len(native) == 28 and actual == expected
        assert sql("SELECT ordinal::text||'|'||enabled::text FROM bigbrotr_archive.owner", db) == '0|false'
        assert sql('SELECT count(*) FROM bigbrotr_archive.attempts', db) == '0'
        logs = tls_cases.log_snapshot(state)
        matching = [line for line in logs.splitlines() if f'connection authorized: user=bb_init_{nonce} database={db}' in line]
        assert matching and all('SSL enabled (protocol=TLSv1.' in line for line in matching)
        return dict(bootstrap_oid=int(bootstrap_oid), bootstrap_retired=True, remaining_backends=0,
                    native_tables=28, guard_tables=4, attempts=0, native_authorized_tls=len(matching))
    def initialize(item, fixture):
        observed, _ = cli('initialize', fixture[1]); item['observed'] = observed; save()
        assert observed['exit'] == 0 and observed['output']['new_installation'] is True
        assert observed['output']['ready'] is False
        item['independent'] = installed(fixture[0])
    def refuse(item, fixture, exit_code, baseline=False):
        db, path, data = fixture
        before = durable(db)
        observed, error = cli('initialize', path, baseline)
        item.update(observed=observed, before=before, after=durable(db), root_entries=sorted(p.name for p in data.iterdir()))
        if baseline: assert b'native catalog mutual TLS is not implemented' in error
        if db == 'bbtls_long_ca':
            assert error == b'error: native catalog route exceeds its supported bounds\n'
        assert observed['exit'] == exit_code and item['after'] == before and item['root_entries'] == []
    def inspect(item, fixture, action):
        before = durable(fixture[0]); observed, _ = cli(action, fixture[1])
        item.update(observed=observed, before=before, after=durable(fixture[0]))
        assert observed['exit'] == 0 and item['after'] == before

    try:
        for target, secret in [('initializer', 'postgres'), ('manager', 'CatalogManager')]:
            path = work / target; path.write_bytes((root / 'secrets' / secret).read_bytes()); path.chmod(0o600)
        password = (work / 'manager').read_text()
        assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
        sql('CREATE ROLE "CatalogManager" LOGIN NOSUPERUSER CREATEROLE NOCREATEDB '
            "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "';")
        del password
        report['pki'] = tls_cases.generate_pki(work / 'pki'); save()
        original_hba = lab.docker('exec', pg, 'cat', '/var/lib/postgresql/data/pg_hba.conf').stdout
        original_auto = lab.docker('exec', pg, 'cat', '/var/lib/postgresql/data/postgresql.auto.conf').stdout
        original_settings = tls_cases.settings(state)
        no_tls = config('no_tls')
        case('server-without-tls-refused-before-reservation', lambda i: refuse(i, no_tls, 4))
        server_dir = '/var/lib/postgresql/data/native-mtls-pki'
        assert lab.docker('exec', pg, 'test', '-e', server_dir, check=False).returncode == 1
        lab.docker('cp', str(work / 'pki'), pg+':'+server_dir)
        lab.docker('exec', pg, 'chown', '-R', 'postgres:postgres', server_dir)
        for key, value in {'ssl':'on', 'ssl_cert_file':server_dir+'/server.crt',
                           'ssl_key_file':server_dir+'/server.key', 'ssl_ca_file':server_dir+'/ca.crt',
                           'log_connections':'on'}.items():
            sql(f"ALTER SYSTEM SET {key}='{value}';")
        reload(); assert tls_cases.settings(state)['ssl'] == 'on'
        auth()
        # Real CA readable by Rustls at the full path, beyond libpq's MAXPGPATH.
        long_dir = work / 'pki'
        while len('/out/native-mtls/' + str(long_dir.relative_to(work)) + '/ca.crt') < 1100:
            long_dir /= 'nested_ca_' + 'a' * 90
            long_dir.mkdir(mode=0o700)
        (long_dir / 'ca.crt').write_bytes((work / 'pki/ca.crt').read_bytes())
        long_ca = str((long_dir / 'ca').relative_to(work / 'pki'))
        long_fixture = config('long_ca', ca=long_ca)
        case('long-ca-refused-before-permanent-reservation', lambda i: refuse(i, long_fixture, 3))
        plain_tls = config('tls')
        case('initialize-verify-full-scram-128mib', lambda i: initialize(i, plain_tls))
        for action in ['inspect', 'recover']:
            case('tls-fresh-' + action, lambda i, a=action: inspect(i, plain_tls, a))
        for name, options in [('wrong_ca', dict(ca='wrong_ca')), ('wrong_name', dict(server='wrong.bigbrotr.invalid'))]:
            fixture = config(name, **options)
            case(name+'-before-reservation', lambda i, f=fixture: refuse(i, f, 4))
        auth(client=True)
        for name, options, code in [
            ('missing_certificate', {}, 4),
            ('untrusted_client', dict(client='wrong_client_ca'), 4),
            ('key_mismatch', dict(client='client', key='wrong_identity'), 3),
        ]:
            fixture = config(name, **options)
            case(name+'-before-reservation', lambda i, f=fixture, c=code: refuse(i, f, c))
        for name, content, mode, field in [('oversize', b'x'*(1048576+1), 0o600, 'cert'),
                                         ('public_key', (work/'pki/client.key').read_bytes(), 0o644, 'key')]:
            extension = '.crt' if field == 'cert' else '.key'
            p = work / 'pki' / (name + extension); p.write_bytes(content); p.chmod(mode)
            fixture = config(name, client='client', **{field:name})
            case(name+'-before-reservation', lambda i, f=fixture: refuse(i, f, 3))
        mtls = config('mtls', client='client')
        baseline = root / 'out/baseline-bigbrotr'
        assert sha(baseline) == 'e62e4c14036a6001bc4fde9b891081eee0b5e86160fc69aa6c5eaf6e6ac87f2c'
        baseline_info = json.loads(command('/out/baseline-bigbrotr', 'build-info').stdout)
        assert baseline_info['source']['revision'] == '78e77346cb75379955aeb423e9fb33e7bd38533b'
        report['baseline'] = dict(sha256=sha(baseline), build_info=baseline_info)
        case('published-baseline-refuses-valid-mtls', lambda i: refuse(i, mtls, 3, True))
        case('initialize-server-enforced-mtls-scram-128mib', lambda i: initialize(i, mtls))
        for action in ['inspect', 'recover']:
            case('mtls-fresh-' + action, lambda i, a=action: inspect(i, mtls, a))
        db = mtls[0]
        role = 'bigbrotr_receipt_reader'
        sql(f'REVOKE TEMP ON DATABASE {db} FROM PUBLIC; GRANT CONNECT ON DATABASE {db} TO {role}; GRANT USAGE ON SCHEMA public,bigbrotr_archive TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}; GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO {role};', db)
        base = dict(expected=dict(dataset_id='01'*16, schema_version=1, topology_generation='0',
            batch_digest='71'*32, record_count=1, input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=1, last_sequence=1)],
            attempt_id=[72]*16, publication_ordinal=1), catalog_id='02'*16,
            data_root='/out/native-mtls/'+mtls[2].name+'/', database=db, port=5432, role=role,
            max_receipts=1, reserve_bytes=384<<20, deadline_ms=15000, cancel_when_blocked=False,
            tls_server_name=tls_cases.SERVER_NAME, tls_ca_file='/out/native-mtls/pki/ca.crt',
            client_cert_ref='file:/out/native-mtls/pki/client.crt', client_key_ref='file:/out/native-mtls/pki/client.key')
        def receipt(item, name, expected, changes, blocked=False):
            directory = work / name; directory.mkdir(mode=0o700); (directory/'job').mkdir(mode=0o700)
            fixture = dict(base, **changes)
            (directory/'input.json').write_text(json.dumps(fixture)+'\n')
            before = durable(db)
            args = ['docker','exec',state['container_id'],'/out/native-tests/archive-receipt-probe',
                    f'/out/native-mtls/{name}/input.json',f'/out/native-mtls/{name}/job']
            if blocked:
                holder = Process(['docker','exec','-i',pg,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db], input_pipe=True)
                children.append(holder)
                holder.process.stdin.write(b'BEGIN; LOCK TABLE bigbrotr_archive.installation IN ACCESS EXCLUSIVE MODE; SELECT 182736;\n'); holder.process.stdin.flush()
                deadline = time.monotonic()+5
                with holder.condition:
                    while b'182736' not in holder.stdout:
                        assert time.monotonic()<deadline and not holder.overflow
                        holder.condition.wait(.02)
                reader = Process(args); children.append(reader)
                deadline = time.monotonic()+10
                while True:
                    active = json.loads(sql(f"SELECT coalesce(json_agg(json_build_object('pid',a.pid,'ssl',s.ssl,'version',s.version,'client_dn',s.client_dn,'wait',a.wait_event_type)), '[]'::json) FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.datname='{db}' AND a.usename='{role}' AND a.application_name='bigbrotr-archive-receipt' AND a.wait_event_type='Lock' AND cardinality(pg_blocking_pids(a.pid))>0"))
                    if active: break
                    assert time.monotonic()<deadline and reader.process.poll() is None
                    time.sleep(.02)
                assert len(active)==1 and active[0]['ssl'] is True and 'bigbrotr_migrator' in active[0]['client_dn']
                item['blocked_native_tls'] = active
                # Hold pidfds for the exact live probe and native child, then
                # cancel and prove native death before releasing the PG blocker.
                watcher = '''import json,os,select,sys,time
from pathlib import Path
expected=sys.argv[2:]
def record(p):
 s=(p/'stat').read_text().rsplit(')',1)[1].split()
 return dict(pid=int(p.name),start=s[19],ppid=int(s[1]),exe=os.readlink(p/'exe'),argv=[v.decode() for v in (p/'cmdline').read_bytes().rstrip(b'\\0').split(b'\\0')])
peers=[]
for p in Path('/proc').iterdir():
 if not p.name.isdecimal(): continue
 try: r=record(p)
 except (FileNotFoundError,PermissionError,ProcessLookupError): continue
 peers.append(r)
parent=[r for r in peers if r['exe']==expected[0] and r['argv']==expected]
assert len(parent)==1
parent=parent[0]
children=[r for r in peers if r['ppid']==parent['pid'] and r['exe']=='/out/archive-worker/bigbrotr-archive-worker']
assert len(children)==1
child=children[0]
assert 'inspect-receipt' in child['argv'] and expected[2] in child['argv']
fds=[]
try:
 for r in [parent,child]:
  fd=os.pidfd_open(r['pid']);fds.append(fd)
  assert record(Path('/proc')/str(r['pid']))==r
 poll=select.poll();poll.register(fds[1],select.POLLIN)
 assert not poll.poll(0)
 started=time.monotonic();Path(sys.argv[1]).touch()
 assert poll.poll(5000)
 print(json.dumps(dict(parent=parent,child=child,native_exited_before_unlock=True,seconds=time.monotonic()-started)))
finally:
 for fd in fds:os.close(fd)
'''
                observed_exit = json.loads(command('python3','-c',watcher,
                    f'/out/native-mtls/{name}/cancel',*args[3:]).stdout)
                item['native_exit'] = observed_exit
                assert reader.wait(5)==0
                assert not reader.stderr and not reader.overflow
                observed = json.loads(reader.stdout)
                holder.process.stdin.write(b'ROLLBACK;\n\\q\n'); holder.process.stdin.flush()
                assert holder.wait(5)==0 and not holder.stderr and not holder.overflow
            else:
                result = lab.command(args, timeout=25); assert not result.stderr
                observed = json.loads(result.stdout)
            item.update(observed=observed, before=before, after=durable(db)); save()
            assert observed['outcome']==expected and observed['active_permits']==0 and observed['cancelled']==blocked
            if blocked:
                # Distinguish signal-driven cancellation from the native deadline.
                assert observed['elapsed_ms'] < 15000 and fixture['deadline_ms'] == 60000
            assert item['after']==before
            deadline=time.monotonic()+5
            while sql(f"SELECT count(*) FROM pg_stat_activity WHERE datname='{db}' AND usename='{role}'")!='0':
                assert time.monotonic()<deadline; time.sleep(.02)
            item['remaining_clients']=0
        for name, changes in [
            ('native-valid', {}),
            ('native-missing-client', dict(client_cert_ref=None, client_key_ref=None)),
            ('native-untrusted-client', dict(client_cert_ref='file:/out/native-mtls/pki/wrong_client_ca.crt', client_key_ref='file:/out/native-mtls/pki/wrong_client_ca.key')),
            ('native-wrong-password', dict(wrong_password=True)),
            ('native-wrong-ca', dict(tls_ca_file='/out/native-mtls/pki/wrong_ca.crt')),
            ('native-wrong-name', dict(tls_server_name='wrong.bigbrotr.invalid')),
        ]:
            expected='Ok(Absent)' if name=='native-valid' else 'Err(ObservationUnknown)'
            case(name, lambda i,n=name,c=changes,e=expected: receipt(i,n,e,c))
        name='native-blocked-cancel'
        case(name, lambda i: receipt(i,name,'Err(ObservationUnknown)',dict(deadline_ms=60000,cancel_when_blocked=True,cancel_file=f'/out/native-mtls/{name}/cancel'),True))
        auth('trust', client=True)
        case('native-mtls-without-scram-refused', lambda i: receipt(i,'native-trust','Err(ObservationUnknown)',{}))
        auth(client=True)
        case('native-mtls-retry-after-refusals', lambda i: receipt(i,'native-retry','Ok(Absent)',{}))
        report['status']='COLLECTED'
    except BaseException as error:
        report.update(status='FAIL', failure_class=type(error).__name__)
        raise
    finally:
        errors=[]
        for child in children:
            try: child.stop()
            except BaseException as error: errors.append(dict(resource='local-wrapper',error=type(error).__name__))
        for path, original in [('/var/lib/postgresql/data/pg_hba.conf',original_hba),('/var/lib/postgresql/data/postgresql.auto.conf',original_auto)]:
            if original is not None:
                try: replace(path,original)
                except BaseException as error: errors.append(dict(resource=path,error=type(error).__name__))
        if original_settings is not None:
            try:
                reload(); assert tls_cases.settings(state)==original_settings
                report['restoration']='PASS'
            except BaseException as error: errors.append(dict(resource='reload',error=type(error).__name__))
        report['cleanup_errors']=errors
        if errors: report['status']='INCOMPLETE'
        elif report['status']=='COLLECTED': report['status']='PASS'
        save()
    assert report['status']=='PASS'
    print(json.dumps(dict(status='PASS',cases=len(report['cases']))),flush=True)


if __name__=='__main__':
    assert len(sys.argv)==2
    main(Path(sys.argv[1]).resolve(strict=True))
