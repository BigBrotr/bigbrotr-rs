#!/usr/bin/env python3
"""Exact native CLI addition/retry/fault cases. Keep all generated keys private."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

binary, home, tracer = map(Path, sys.argv[1:])
home.mkdir(mode=0o700)
source = Path(__file__).resolve().parents[2]
vectors = json.loads((source/'tests/fixtures/read/keyring-vectors.json').read_text())
wire = bytes.fromhex(vectors['cases'][0]['hex'])
key_id = '55'*16
names = ['append-retry','missing-partial','capacity','locked','parallel',
         'file-eio','directory-eio','parent-eio','write-enospc',
         'kill-before-rename','kill-after-rename','kill-before-parent-sync','blocked-stdout']
report = dict(status='INCOMPLETE', binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
              tracer_sha256=hashlib.sha256(tracer.read_bytes()).hexdigest(),
              cases=[dict(name=name,status='NOT_RUN') for name in names])
assert report['tracer_sha256']=='509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
current = None
sequence = 0


def save():
    path=home/'result.partial';path.write_text(json.dumps(report,indent=2)+'\n');path.replace(home/'result.json')
def case(name):
    global current
    current=next(c for c in report['cases'] if c['name']==name);current['status']='RUNNING';save()
def passed(**fields): current.update(status='PASS',**fields);save()
def fixture(name, data=wire):
    root=home/name;root.mkdir(mode=0o700);keys=root/'keys';keys.mkdir(mode=0o700)
    if data is not None:
        (keys/'keyring').write_bytes(data);(keys/'keyring').chmod(0o600)
    text=(source/'config/recipes/archive-only.toml').read_text()
    needle='dataset_id = "'+'22'*16+'"';assert text.count(needle)==1
    text=text.replace(needle,'dataset_id = "'+vectors['dataset_id']+'"')
    config=root/'instance.toml';config.write_text(text+'\n[read.cursor]\nkeyring_ref='+json.dumps('file:'+str(keys/'keyring'))+'\n')
    return keys,config

def command(config, id=key_id):
    return [str(binary),'access','cursor-keyring','prepare-key','--file',str(config),'--key-id',id]
def record(result, expected=0):
    global sequence
    sequence+=1;stem=home/('command-'+str(sequence))
    stem.with_suffix('.stdout').write_bytes(result.stdout);stem.with_suffix('.stderr').write_bytes(result.stderr)
    current.setdefault('commands',[]).append(dict(log=stem.name,exit=result.returncode))
    assert result.returncode==expected,(result.returncode,result.stderr)
    if expected:
        assert not result.stdout
        return None
    assert not result.stderr
    value=json.loads(result.stdout)
    assert len(value)==8 and value['state']=='prepared-local' and value['cursor_authority']=='not-granted' and value['control_registration']=='not-checked'
    return value

def cli(config,id=key_id,expected=0):
    return record(subprocess.run(command(config,id),env={},capture_output=True,timeout=5),expected)
def snapshot(keys):
    return {p.name:dict(bytes=p.stat().st_size,inode=p.stat().st_ino,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(keys.iterdir())}
def extended(keys, original=wire, filename='keyring'):
    data=(keys/filename).read_bytes();count=int.from_bytes(original[40:42],'big')
    assert data[:40]==original[:40] and int.from_bytes(data[40:42],'big')==count+1
    assert len(data)==len(original)+48 and data[42:len(original)-32]==original[42:-32]
    assert data[-80:-64]==bytes.fromhex(key_id) and data[-64:-32]!=bytes(32)
    assert hashlib.sha256(data[:-32]).digest()==data[-32:]
    st=(keys/filename).stat();assert st.st_uid==os.geteuid() and st.st_nlink==1 and st.st_mode&0o7777==0o600
    return data

def trace_command(config,path,injection=None):
    follow=[] if injection and injection.startswith('write:') else ['-f']
    args=[str(tracer),*follow,'-yy','-ttt','-o',str(path),'-e','trace=fsync,write,renameat2,openat,fchmod,flock','-e','raw=write']
    if injection:args+=['-e','inject='+injection]
    return args+command(config)
def traced(keys,config,injection=None,expected=0):
    path=keys.parent/'syscalls.trace'
    result=subprocess.run(trace_command(config,path,injection),env={},capture_output=True,timeout=5)
    value=record(result,expected);text=path.read_text()
    current.update(trace=str(path.relative_to(home)),trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return text,value

def held(process,path,target):
    deadline=time.monotonic()+3
    while time.monotonic()<deadline:
        children=Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
        if children:
            assert len(children)==1;child=int(children[0])
            text=path.read_text() if path.exists() else ''
            matches=re.findall(r'^'+str(child)+r'\s+[0-9.]+ fsync\(\d+<([^>]+)>(.*)$',text,re.M)
            if matches:
                name,suffix=matches[-1]
                if target(name) and '= ' not in suffix:
                    assert Path(f'/proc/{child}/exe').resolve()==binary.resolve()
                    return child
        assert process.poll() is None
        time.sleep(.005)
    raise TimeoutError('actual pending fsync not observed')


try:
    save();case('append-retry');keys,config=fixture('ordered')
    trace,first=traced(keys,config);data=extended(keys)
    calls=[line for line in trace.splitlines() if 'fsync(' in line or 'renameat2(' in line]
    assert len(calls)==4 and all('= 0' in line for line in calls)
    assert '.partial>' in calls[0] and 'renameat2(' in calls[1] and 'RENAME_NOREPLACE' not in calls[1]
    assert str(keys)+'>' in calls[2] and str(keys.parent)+'>' in calls[3]
    assert first['dataset_id']==vectors['dataset_id'] and first['keyring_id']==vectors['keyring_id']
    assert first['key_ids']==['33'*16,'44'*16,key_id] and first['fingerprint_sha256']==data[-32:].hex()
    assert cli(config)==first and (keys/'keyring').read_bytes()==data
    passed(observation=first,files=snapshot(keys))

    case('missing-partial');keys,config=fixture('missing',None);cli(config,expected=4)
    assert not list(keys.iterdir())
    (keys/'keyring').write_bytes(wire);(keys/'keyring').chmod(0o600)
    (keys/'unresolved.partial').write_bytes(b'preserve independent staging');(keys/'unresolved.partial').chmod(0o600)
    before=snapshot(keys);cli(config,expected=4);assert snapshot(keys)==before
    passed(files=before)

    case('capacity');full=bytes.fromhex(next(c['hex'] for c in vectors['cases'] if c['name']=='sixteen-public-keys'))
    keys,config=fixture('capacity',full);original_id=full[42:58].hex()
    assert len(cli(config,original_id)['key_ids'])==16
    before=snapshot(keys);cli(config,'ee'*16,expected=4);assert snapshot(keys)==before
    assert (keys/'keyring').read_bytes()==full
    passed(files=before)

    case('locked');keys,config=fixture('locked');fd=os.open(keys,os.O_RDONLY|os.O_DIRECTORY)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);before=snapshot(keys);cli(config,expected=4)
        assert snapshot(keys)==before
    finally:os.close(fd)
    cli(config);extended(keys);passed(files=snapshot(keys))

    case('parallel');keys,config=fixture('parallel')
    processes=[subprocess.Popen(command(config),env={},stdout=subprocess.PIPE,stderr=subprocess.PIPE) for _ in range(8)]
    results=[]
    for p in processes:
        stdout,stderr=p.communicate(timeout=5);result=subprocess.CompletedProcess(p.args,p.returncode,stdout,stderr)
        value=record(result,0 if p.returncode==0 else 4)
        if value:results.append(value)
        else:assert b'Busy' in stderr
    assert results and all(v==results[0] for v in results)
    extended(keys);assert len(list(keys.iterdir()))==1
    passed(successes=len(results),busy=8-len(results),files=snapshot(keys))

    for name,injection,renamed in [('file-eio','fsync:error=EIO:when=1',False),('directory-eio','fsync:error=EIO:when=2',True),('parent-eio','fsync:error=EIO:when=3',True),('write-enospc','write:error=ENOSPC:when=1',False)]:
        case(name);keys,config=fixture(name);before=snapshot(keys)
        trace,_=traced(keys,config,injection,4);assert '(INJECTED)' in trace and ('ENOSPC' if name=='write-enospc' else 'EIO') in trace
        if renamed:
            data=extended(keys);assert len(list(keys.iterdir()))==1
            cli(config);assert (keys/'keyring').read_bytes()==data
        else:
            assert (keys/'keyring').read_bytes()==wire and snapshot(keys)['keyring']==before['keyring']
            assert len(list(keys.iterdir()))==2
            if name=='file-eio':extended(keys,filename=next(p.name for p in keys.iterdir() if p.name.endswith('.partial')))
            failed=snapshot(keys);cli(config,expected=4);assert snapshot(keys)==failed
        passed(files=snapshot(keys))

    for name,index,renamed in [('kill-before-rename',1,False),('kill-after-rename',2,True),('kill-before-parent-sync',3,True)]:
        case(name);keys,config=fixture(name);path=keys.parent/'syscalls.trace'
        stdout=(keys.parent/'stdout').open('wb');stderr=(keys.parent/'stderr').open('wb')
        process=subprocess.Popen(trace_command(config,path,f'fsync:delay_enter=5000000:when={index}'),env={},stdout=stdout,stderr=stderr,start_new_session=True)
        child=None
        try:
            predicate=(lambda n:n.endswith('.partial')) if index==1 else (lambda n:n==str(keys if index==2 else keys.parent))
            child=held(process,path,predicate)
            before=snapshot(keys)
            if renamed:extended(keys)
            else:
                assert (keys/'keyring').read_bytes()==wire and len(before)==2
                extended(keys,filename=next(p.name for p in keys.iterdir() if p.name.endswith('.partial')))
        finally:
            if process.poll() is None:os.killpg(process.pid,signal.SIGKILL)
            code=process.wait(timeout=3);stdout.close();stderr.close()
        assert code==-signal.SIGKILL and child is not None
        assert not (keys.parent/'stdout').read_bytes() and not (keys.parent/'stderr').read_bytes()
        deadline=time.monotonic()+3
        while Path(f'/proc/{child}').exists() and time.monotonic()<deadline:time.sleep(.01)
        assert not Path(f'/proc/{child}').exists()
        if renamed:
            data=extended(keys);cli(config);assert (keys/'keyring').read_bytes()==data
        else:
            cli(config,expected=4);assert snapshot(keys)==before
        passed(child_absent=True,trace=str(path.relative_to(home)),trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),files=snapshot(keys))

    case('blocked-stdout');keys,config=fixture('blocked');reader,writer=os.pipe()
    flags=fcntl.fcntl(writer,fcntl.F_GETFL);fcntl.fcntl(writer,fcntl.F_SETFL,flags|os.O_NONBLOCK)
    written=0
    try:
        while True:written+=os.write(writer,b'x'*4096)
    except BlockingIOError:pass
    capacity=fcntl.fcntl(writer,fcntl.F_GETPIPE_SZ);assert written==capacity and capacity>0
    fcntl.fcntl(writer,fcntl.F_SETFL,flags);start=time.monotonic()
    try:
        result=subprocess.run(command(config),env={},stdout=writer,stderr=subprocess.PIPE,timeout=5)
        elapsed=time.monotonic()-start;assert result.returncode==1 and not result.stderr
    finally:os.close(writer);os.close(reader)
    data=extended(keys);cli(config);assert (keys/'keyring').read_bytes()==data
    passed(exit=result.returncode,elapsed_seconds=elapsed,pipe_bytes=capacity,files=snapshot(keys))
    assert all(c['status']=='PASS' for c in report['cases']);report['status']='PASS'
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__)
    if current:current['status']='FAIL'
    raise
finally:save()
