import hashlib,json,os,platform,re,shutil,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/source')
from tools.build import clean_environment,native_toolchain,file_hash
from tools.ci import execute
from tools.archive_bundle import build_environment
build=Path('/out/build');source=build/'source';out=Path('/out/native-tests')
manifest=json.loads((build/'source-manifest.json').read_text());receipt=json.loads((build/'build-result.json').read_text())
assert manifest['revision']=='2f4dd9266d1f8fd22287f87232d43a75d7327e45'
assert file_hash(build/'source-manifest.json')==receipt['source_manifest_sha256']
def inventory():return {str(p.relative_to(source)):file_hash(p) for p in sorted(source.rglob('*')) if p.is_file()}
assert inventory()==manifest['files_sha256']
env=clean_environment();selected,native=native_toolchain(env,2);assert native==receipt['native_toolchain'];env.update(selected)
env.update(build_environment(build/'archive-worker'))
env.update(CARGO_TARGET_DIR=str(build/'target'),CARGO_HOME=str(build/'cargo-home'),BIGBROTR_SOURCE_REVISION=manifest['revision'],BIGBROTR_SOURCE_TREE=manifest['tree'],BIGBROTR_SOURCE_MANIFEST_SHA256=receipt['source_manifest_sha256'],BIGBROTR_BUILD_PROFILE='release')
checks=[('binary',['cargo','test','--locked','--release','-p','bigbrotr-archive-worker','--lib','--','--test-threads=1'])]
import ctypes
engine=ctypes.CDLL(str(build/'archive-worker/lib/libduckdb.so'));engine.duckdb_vector_size.restype=ctypes.c_uint64
vector_size=engine.duckdb_vector_size();assert vector_size==2048
result=execute(checks,out,source,env,{'source':manifest['revision'],'native_toolchain':native,'host':{'os':platform.system(),'architecture':platform.machine()}},900)
assert inventory()==manifest['files_sha256']
if result['status']!='PASS':raise SystemExit(1)
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
assert len(artifacts)==1
(out/'artifacts.json').write_text(json.dumps(artifacts,indent=2)+'\n')
print(json.dumps({'status':'PASS','source':manifest['revision'],'artifacts':artifacts}))
(out/'native-vector-size.json').write_text(json.dumps({'actual_vector_size':vector_size,'library_sha256':file_hash(build/'archive-worker/lib/libduckdb.so')})+'\n')
