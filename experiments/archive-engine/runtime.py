"""Bounded actual native-worker diagnostic cases; no publication qualification."""
import fcntl,hashlib,json,os,signal,subprocess,tempfile,time,shutil
from pathlib import Path
build=Path('/build');root=Path('/lab');bundle=build/'archive-worker';binary=bundle/'bigbrotr-archive-worker'
receipt=json.loads((build/'build-result.json').read_text())
assert receipt['build_info']['source']['revision']=='d459fcdc357ddb3b68d9ac89e044b22156cf72b0'
assert hashlib.sha256(binary.read_bytes()).hexdigest()==receipt['archive_worker']['sha256']
env={'PATH':'/usr/bin:/bin','HOME':'/lab/home','LANG':'C.UTF-8'}
(root/'home').mkdir(exist_ok=False)
report={'format':'bigbrotr-native-archive-engine-cases/1','source':receipt['build_info']['source'], 'worker_sha256':receipt['archive_worker']['sha256'],'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'status':'RUNNING','cases':[]}
def save(): (root/'result.json').write_text(json.dumps(report,indent=2)+'\n')
def invoke(name,args,expected):
    begin=time.monotonic();r=subprocess.run(args,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
    (root/(name+'.stdout')).write_bytes(r.stdout);(root/(name+'.stderr')).write_bytes(r.stderr)
    assert r.returncode==expected,(name,r.returncode,r.stderr)
    assert len(r.stdout)<16384 and len(r.stderr)<1024
    report['cases'].append({'name':name,'exit_code':r.returncode,'seconds':time.monotonic()-begin,'status':'EXECUTED'});save()
    return r

def command(temp,selected=bundle,memory='268435456'):
    return [str(binary),'inspect-engine',str(selected),str(temp),memory,'134217728','1']
try:
    temp=root/'positive-temp';temp.mkdir()
    result=invoke('offline-load', ['strace','-ff','-o',str(root/'network-trace'),'-e','trace=network',*command(temp)],0)
    actual=json.loads(result.stdout)
    assert actual['version']=='v1.5.5' and actual['source_id']=='d8cdaa33fd'
    assert {v['name'] for v in actual['extensions']}=={'ducklake','postgres_scanner','httpfs'}
    assert all(v['loaded'] and v['version'] for v in actual['extensions'])
    assert actual['settings']=={'allow_community_extensions':'false','allow_persistent_secrets':'false','allow_unsigned_extensions':'false','autoinstall_known_extensions':'false','autoload_known_extensions':'false','enable_progress_bar':'false','max_temp_directory_size':'128.0 MiB','memory_limit':'256.0 MiB','threads':'1'}
    traces=list(root.glob('network-trace.*'));assert traces
    calls=[line for p in traces for line in p.read_text().splitlines() if not line.startswith(('+++','---'))]
    assert not calls,calls
    report['cases'][-1]['network_syscalls']=0;save()
    ordinary=root/'ordinary-cli';shutil.copyfile(build/'bigbrotr',ordinary);ordinary.chmod(0o500)
    ldd=subprocess.check_output(['ldd',str(ordinary)],text=True);assert 'duckdb' not in ldd.lower()
    (root/'ordinary-ldd.txt').write_text(ldd)
    got=json.loads(invoke('ordinary-cli-without-assets',[str(ordinary),'build-info'],0).stdout)
    assert got==receipt['build_info']
    worker_ldd=subprocess.check_output(['ldd',str(binary)],text=True)
    assert str(bundle/'lib/libduckdb.so') in worker_ldd
    (root/'worker-ldd.txt').write_text(worker_ldd)
    temp=root/'invalid-limit-temp';temp.mkdir()
    invoke('zero-memory-refused',command(temp,memory='0'),1)
    temp=root/'nonempty-temp';temp.mkdir();(temp/'marker').write_bytes(b'preserve')
    invoke('nonempty-temp-refused',command(temp),1)
    assert (temp/'marker').read_bytes()==b'preserve'
    bad=root/'incomplete-bundle';(bad/'lib').mkdir(parents=True);(bad/'extensions').mkdir()
    shutil.copyfile(bundle/'lib/libduckdb.so',bad/'lib/libduckdb.so')
    for name in ['ducklake','postgres_scanner']:
        shutil.copyfile(bundle/'extensions'/(name+'.duckdb_extension'),bad/'extensions'/(name+'.duckdb_extension'))
    temp=root/'missing-temp';temp.mkdir()
    invoke('missing-extension-refused',command(temp,bad),1)
    path=bad/'extensions/httpfs.duckdb_extension'
    shutil.copyfile(bundle/'extensions/httpfs.duckdb_extension',path)
    with path.open('r+b') as f:
        f.seek(-1,2);v=f.read(1);f.seek(-1,2);f.write(bytes([v[0]^1]))
    temp=root/'tampered-temp';temp.mkdir()
    result=invoke('altered-extension-refused',command(temp,bad),1)
    assert b'HashMismatch' in result.stderr
    read,write=os.pipe()
    try:
        os.set_blocking(write,False);capacity=0
        while True:
            try:capacity+=os.write(write,b'x'*4096)
            except BlockingIOError:break
        os.set_blocking(write,True)
        temp=root/'blocked-temp';temp.mkdir()
        p=subprocess.Popen(command(temp),env=env,stdout=write,stderr=subprocess.DEVNULL,start_new_session=True)
        try:
            time.sleep(2)
            assert p.poll() is None,'required blocked destination did not block the process'
            begin=time.monotonic();os.killpg(p.pid,signal.SIGKILL);assert p.wait(timeout=5)==-signal.SIGKILL
            elapsed=time.monotonic()-begin
        finally:
            if p.poll() is None:os.killpg(p.pid,signal.SIGKILL)
            p.wait()
        os.close(write);write=None
        drained=bytearray()
        while data:=os.read(read,65536):drained.extend(data)
        assert drained==b'x'*capacity
        report['cases'].append({'name':'harness-kill-blocked-worker','status':'PASS','pipe_bytes':capacity,'seconds_until_reaped':elapsed,'scope':'Harness-owned kill and EOF; application supervisor/grants remain unimplemented'})
    finally:
        if write is not None:os.close(write)
        os.close(read)
    for case in report['cases']: case['status']='PASS'
    report['status']='PASS'
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__)
    report['cases'].append({'name':'unresolved-case-or-harness-failure','status':'FAIL','failure':type(error).__name__})
    save();raise
finally:save()
print(json.dumps({'status':report['status'],'cases':len(report['cases'])}))
