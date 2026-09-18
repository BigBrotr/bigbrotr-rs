#!/usr/bin/env python3
"""Exact-source catalog guard/bootstrap prerequisite; synthetic science only."""
import json,os,re,shutil,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
SOURCE=Path('/out/build/source');sys.path.insert(0,str(SOURCE))
from tools.build import clean_environment,native_toolchain,file_hash
from tools.archive_bundle import install_bundle,build_environment
from tools.ci import execute
build=SOURCE.parent;out=Path('/out/native-tests');bundle=Path('/out/archive-worker')
manifest=json.loads((build/'source-manifest.json').read_text())
def inventory():return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert inventory()==manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'),bundle,'x86_64-unknown-linux-gnu')
env=clean_environment();native_env,native=native_toolchain(env,2);env.update(native_env)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build/'cargo-home'),CARGO_TARGET_DIR=str(build/'target'),LD_LIBRARY_PATH=str(bundle/'lib'),BIGBROTR_ARCHIVE_TEST_BUNDLE=str(bundle),BIGBROTR_SOURCE_REVISION=manifest['revision'],BIGBROTR_SOURCE_TREE=manifest['tree'],BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'),BIGBROTR_BUILD_PROFILE='release')
checks=[('tests',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--test','archive_attempt','--test','catalog_guard','--test','control_schema']),('worker-tests',['cargo','test','--locked','--offline','--release','-p','bigbrotr-archive-worker','--test','catalog_bootstrap']),('build',['cargo','build','--locked','--offline','--release','-p','bigbrotr','--example','archive_attempt_probe','--example','catalog_guard_probe']),('worker-build',['cargo','build','--locked','--offline','--release','-p','bigbrotr-archive-worker','--bin','bigbrotr-archive-worker','--example','catalog-fence-probe']),('clippy',['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','-p','bigbrotr-archive-worker','--lib','--tests','--examples','--bins','--','-D','warnings'])]
result=execute(checks,out,SOURCE,env,{'source':manifest['revision'],'native_toolchain':native},1800);assert result['status']=='PASS'
worker=bundle/'bigbrotr-archive-worker';shutil.copy2(build/'target/release/bigbrotr-archive-worker',worker);worker.chmod(0o500)
probe=out/'catalog-guard-probe';shutil.copy2(build/'target/release/examples/catalog_guard_probe',probe);probe.chmod(0o500)
fence=out/'catalog-fence-probe';shutil.copy2(build/'target/release/examples/catalog-fence-probe',fence);fence.chmod(0o500)
identity={'source':manifest['revision'],'worker':file_hash(worker),'guard_probe':file_hash(probe),'native_writer':file_hash(fence),'native_toolchain':native}
(out/'artifacts.json').write_text(json.dumps(identity,indent=2)+'\n')
result=execute([('control',[str(build/'target/release/examples/archive_attempt_probe')])],out/'control',SOURCE,env,identity,180);assert result['status']=='PASS'
archive=Path('/out/archive');archive.mkdir();temp=Path('/out/bootstrap-temp');temp.mkdir()
def dsn(role):return 'host=postgres port=5432 dbname=catalog user='+role+' password='+Path('/secrets',role).read_text()+' sslmode=disable connect_timeout=5'
worker_env=env.copy();worker_env['BIGBROTR_ARCHIVE_PARENT_PID']=str(os.getpid())
bootstrap=subprocess.run([str(worker),'bootstrap-catalog',str(bundle),str(temp),str(128*1024**2),str(64*1024**2),'1'],input=json.dumps({'dsn':dsn('BootstrapMixed'),'data_root':'/out/archive/'}),text=True,env=worker_env,capture_output=True,timeout=60)
(out/'bootstrap.json').write_text(json.dumps({'exit':bootstrap.returncode,'stdout':bootstrap.stdout,'stderr':bootstrap.stderr},indent=2)+'\n')
assert bootstrap.returncode==0
assert json.loads(bootstrap.stdout)=={'format':'bigbrotr-native-bootstrap/1','status':'initialized'}
def guard(mode):
 r=execute([(mode,[str(probe),mode])],out/mode,SOURCE,env,identity,120);assert r['status']=='PASS',mode
guard('install');guard('activate-first')
old='bb_archive_'+'14'*16;new='bb_archive_'+'16'*16
credentials=Path('/out/old-native.secret');credentials.write_text(dsn(old));credentials.chmod(0o600)
def native_command(mode,path,batch):
 path.mkdir();return [str(fence),mode,str(bundle),str(path),'/out/archive/',str(credentials),str(batch)]
r=subprocess.run(native_command('init',Path('/out/native-init'),1),capture_output=True,text=True,env=env,timeout=60)
(out/'native-init.json').write_text(json.dumps({'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr},indent=2)+'\n');assert r.returncode==0
old_writer=subprocess.Popen(native_command('write',Path('/out/native-old'),1),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
try:
 import selectors
 selector=selectors.DefaultSelector();selector.register(old_writer.stdout,selectors.EVENT_READ)
 assert selector.select(30),'native ready timeout'
 ready=old_writer.stdout.readline();assert json.loads(ready)['stage']=='ready'
 guard('activate-second')
 assert old_writer.poll() is None
 stdout,stderr=old_writer.communicate('publish\n',timeout=30)
 assert old_writer.returncode!=0 and 'commit' in stderr
 (out/'old-writer.json').write_text(json.dumps({'ready':json.loads(ready),'exit':old_writer.returncode,'stdout':stdout,'stderr':stderr},indent=2)+'\n')
finally:
 if old_writer.poll() is None:old_writer.kill();old_writer.wait(timeout=10)
credentials.write_text(dsn(new))
r=subprocess.run(native_command('write',Path('/out/native-new'),1),input='publish\n',capture_output=True,text=True,env=env,timeout=60)
(out/'native-new.json').write_text(json.dumps({'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr},indent=2)+'\n');assert r.returncode==0
r=subprocess.run(native_command('read',Path('/out/native-read'),1),capture_output=True,text=True,env=env,timeout=60)
assert r.returncode==0 and json.loads(r.stdout)['counts']==[1,1,1]
(out/'native-read.json').write_text(r.stdout)
guard('replay-first');guard('inspect')
compatibility=[execute([(mode,[str(probe),mode])],out/'compatibility'/mode,SOURCE,env,identity,120) for mode in ['refuse-collation','inspect-sealed','refuse-damaged']]
assert all(r['status']=='PASS' for r in compatibility)
assert inventory()==manifest['files_sha256']
print(json.dumps({'status':'PASS',**identity}))
