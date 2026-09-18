import hashlib,json,subprocess,sys
from pathlib import Path
root=Path('/home/tux/bigbrotr-implementation-20260917/runs/configuration-992f0dc/runtime')
source=Path('/home/tux/bigbrotr-implementation-20260917/checkout')
sys.path.insert(0,str(source/'experiments/control-migrations'))
import lab
s=lab.load(root)
assert s['owner']=='8ff3fd9ddc7c624683f942cbb8b86acd'
assert lab.command(['git','-C',str(source),'rev-parse','HEAD']).stdout.strip()=='992f0dca6ca66af1eab5be27ff5e0f03391c2d12'
assert not lab.command(['git','-C',str(source),'status','--porcelain','--untracked-files=all']).stdout
before=lab.schema_digest(s,'bb_owner_basic')
script='''import hashlib,json,resource,subprocess,time
from pathlib import Path
assert hashlib.sha256(Path('/build/bigbrotr').read_bytes()).hexdigest()=='e4d81f348369207a02befa6ce6134eea5357984a6cd4bb08e9541838862733d6'
start=time.monotonic()
r=subprocess.run(['/build/bigbrotr','access','enroll-owner','--file','/lab/bb_owner_basic.toml','--challenge-id','32323232323232323232323232323232','--token-ref','file:/lab/secrets/replacement-token','--password-ref','file:/lab/secrets/owner-password'],capture_output=True)
duration=time.monotonic()-start;usage=resource.getrusage(resource.RUSAGE_CHILDREN)
assert r.returncode==3 and not r.stdout and r.stderr==b'error: owner enrollment refused: OwnerExists\\n'
print(json.dumps({'exit':r.returncode,'outcome':'OwnerExists','max_rss_kib':usage.ru_maxrss,'cpu_seconds':usage.ru_utime+usage.ru_stime,'wall_seconds':duration}))
'''
samples=[]
for _ in range(10):
    result=lab.docker('exec','-i',s['client'],'python3','-',data=script)
    samples.append(json.loads(result.stdout))
after=lab.schema_digest(s,'bb_owner_basic');assert before==after
report={'status':'PASS','source':'992f0dca6ca66af1eab5be27ff5e0f03391c2d12','binary_sha256':s['binary_sha256'],'samples':samples,'before_sha256':before,'after_sha256':after,'offered':10,'refused':10,'successful_enrollments':0,'scope':'Ten serial actual executable attempts; every attempt computes the full password hash before observing OwnerExists. Per-child Linux getrusage peak RSS/CPU and process wall time, not cgroup peak or mixed workload capacity.'}
(root/'hash-process-measurement.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'status':'PASS','samples':10,'max_rss_kib':max(s['max_rss_kib'] for s in samples),'wall_seconds_range':[min(s['wall_seconds'] for s in samples),max(s['wall_seconds'] for s in samples)]}))
