import hashlib,json,os,platform,re,shutil,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/source')
from tools.build import clean_environment,native_toolchain,file_hash
from tools.ci import execute
from tools.archive_bundle import build_environment
build=Path('/out/build');source=build/'source';out=Path('/out/native-tests')
manifest=json.loads((build/'source-manifest.json').read_text());receipt=json.loads((build/'build-result.json').read_text())
assert manifest['revision']=='0cdd80fd0fa6991ee0b7957b9e65103a7b3e9fc1'
assert file_hash(build/'source-manifest.json')==receipt['source_manifest_sha256']
def inventory():return {str(p.relative_to(source)):file_hash(p) for p in sorted(source.rglob('*')) if p.is_file()}
assert inventory()==manifest['files_sha256']
env=clean_environment();selected,native=native_toolchain(env,2);assert native==receipt['native_toolchain'];env.update(selected)
env.update(build_environment(build/'archive-worker'))
env.update(CARGO_TARGET_DIR=str(build/'target'),CARGO_HOME=str(build/'cargo-home'),BIGBROTR_SOURCE_REVISION=manifest['revision'],BIGBROTR_SOURCE_TREE=manifest['tree'],BIGBROTR_SOURCE_MANIFEST_SHA256=receipt['source_manifest_sha256'],BIGBROTR_BUILD_PROFILE='release')
checks=[
 ('protocol',['cargo','test','--locked','--release','-p','bigbrotr-protocol','--test','archive_batch']),
 ('prepare',['cargo','test','--locked','--release','-p','bigbrotr-archive-worker','--lib','prepare::tests','--','--test-threads=1']),
 ('executable',['cargo','test','--locked','--release','-p','bigbrotr-archive-worker','--test','handoff','--','--test-threads=1']),
 ('pending',['cargo','test','--locked','--release','-p','bigbrotr','--lib','storage::rocks::store::bootstrap_tests::synchronized_bootstrap_reopens_as_one_complete_pending_batch_and_exact_receipt','--','--exact','--test-threads=1'])]
import ctypes
engine=ctypes.CDLL(str(build/'archive-worker/lib/libduckdb.so'));engine.duckdb_vector_size.restype=ctypes.c_uint64
vector_size=engine.duckdb_vector_size();assert vector_size==2048
result=execute(checks,out,source,env,{'source':manifest['revision'],'native_toolchain':native,'host':{'os':platform.system(),'architecture':platform.machine()}},900)
assert inventory()==manifest['files_sha256']
if result['status']!='PASS':raise SystemExit(1)
for name,expected in {'protocol':5,'prepare':3,'executable':1,'pending':1}.items():
 summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', (out/(name+'.log')).read_text())
 assert summaries==[(str(expected),'0','0')],(name,summaries)

artifacts={};(out/'artifacts').mkdir()
for path in out.glob('*.log'):
 for relative in re.findall(r'Running [^\n]+ \(([^\n]+)\)',path.read_text()):
  executable=Path(relative)
  if not executable.is_absolute():executable=source/executable
  assert executable.is_file()
  with executable.open('rb') as stream:header=stream.read(20)
  assert header[:5]==b'\x7fELF\x02' and int.from_bytes(header[18:20],'little')==62
  name=executable.name;shutil.copy2(executable,out/'artifacts'/name)
  artifacts[name]={'sha256':file_hash(executable),'bytes':executable.stat().st_size}
assert len(artifacts)==4
(out/'artifacts.json').write_text(json.dumps(artifacts,indent=2)+'\n')

(out/'native-vector-size.json').write_text(json.dumps({'actual_vector_size':vector_size,'library_sha256':file_hash(build/'archive-worker/lib/libduckdb.so')})+'\n')

worker=build/'target/release/bigbrotr-archive-worker'
assert worker.is_file()
(out/'executed-worker.json').write_text(json.dumps({'path':str(worker),'sha256':file_hash(worker),'packaged_sha256':receipt['archive_worker']['sha256'],'matches_packaged':file_hash(worker)==receipt['archive_worker']['sha256']},indent=2)+'\n')
shutil.copy2(worker,out/'artifacts'/'executed-bigbrotr-archive-worker')

# Exercise the exact packaged artifact too: cargo test may relink a different
# feature-unified executable, whose identity is retained separately above.
import subprocess,time
fixture=json.loads((source/'crates/bigbrotr-protocol/tests/fixtures/archive/single.json').read_text())
wire=bytes.fromhex(fixture['wire_hex']);packaged=build/'archive-worker/bigbrotr-archive-worker'
assert file_hash(packaged)==receipt['archive_worker']['sha256']
results=[]
for name,data,success in [('complete',wire,True),('truncated',wire[:-1],False)]:
 temp=out/'runtime'/name;temp.mkdir(mode=0o700,parents=True)
 command=[str(packaged),'inspect-batch',str(build/'archive-worker'),str(temp),'134217728','67108864','1','16','1048576']
 start=time.monotonic();r=subprocess.run(command,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=30)
 result={'case':name,'command':command,'input_sha256':hashlib.sha256(data).hexdigest(),'exit':r.returncode,'seconds':time.monotonic()-start,'stdout_sha256':hashlib.sha256(r.stdout).hexdigest(),'stderr':r.stderr.decode()}
 if success:
  assert r.returncode==0 and not r.stderr
  report=json.loads(r.stdout);assert report['format']=='bigbrotr-private-preparation/1' and report['first']['sequence']=='2' and report['facts']==1 and report['staged_counts']==[0,1,0,0,0] and report['batch_digest']==fixture['batch_digest']
  result['inspection']=report
 else:
  assert r.returncode!=0 and not r.stdout and 'Incomplete' in r.stderr.decode()
 result['status']='PASS';results.append(result)
assert file_hash(packaged)==receipt['archive_worker']['sha256']
(out/'packaged-runtime.json').write_text(json.dumps({'status':'PASS','worker_sha256':file_hash(packaged),'cases':results},indent=2)+'\n')
print(json.dumps({'status':'PASS','source':manifest['revision'],'artifacts':artifacts,'packaged_cases':len(results)}))
