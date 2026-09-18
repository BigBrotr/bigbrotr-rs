#!/usr/bin/env python3
"""Synthetic dependency fault qualification, never a product publication ACK."""
import hashlib,json,select,shlex,subprocess,time
from pathlib import Path
import argparse,re
parser=argparse.ArgumentParser(description='Mac-owned actual native PostgreSQL COMMIT-loss cases on an inventoried Tux lab.')
parser.add_argument('--evidence',type=Path,required=True)
parser.add_argument('--revision',required=True)
args=parser.parse_args();ROOT=args.evidence;REV=args.revision
assert re.fullmatch('[0-9a-f]{40}',REV)
ROOT.mkdir(parents=True,exist_ok=True)
NAME='bigbrotr-implementation-publication-'+REV[:7];PG=NAME+'-pg'
OUT=ROOT/REV[:7];OUT.mkdir(exist_ok=True)
def command(*args,input=None,timeout=30):
 return subprocess.run(['ssh','tux',shlex.join(args)],input=input,text=True,capture_output=True,timeout=timeout)
def checked(*args,input=None,timeout=30):
 r=command(*args,input=input,timeout=timeout)
 assert r.returncode==0,(args,r.returncode,r.stderr)
 return r.stdout.strip()
def sql(db,q):return checked('docker','exec','-i',PG,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=q)
def python(code):return checked('docker','exec','-i',NAME,'python3','-c',code)
def probe(mode,db,credential,tag):
 temp='/out/commit-temp-'+db+'-'+tag
 python(f"from pathlib import Path; Path({temp!r}).mkdir(mode=0o700)")
 return command('docker','exec','-i','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',NAME,'/out/native-tests/catalog-fence-probe',mode,'/out/archive-worker',temp,'/out/data-'+db,credential,'1',input='publish\n' if mode=='write' else '',timeout=45)
for _ in range(100):
 r=command('docker','exec',NAME,'test','-f','/out/native-tests/ready.json')
 if r.returncode==0:break
 time.sleep(.2)
else:raise RuntimeError('native build not ready')
ready=json.loads(checked('docker','exec',NAME,'cat','/out/native-tests/ready.json'));assert ready['source']==REV
(OUT/'ready.json').write_text(json.dumps(ready,indent=2)+'\n')
records=[]
for mode in ['pass','before-forward','after-complete']:
 db='bbcommit_'+mode.replace('-','_'); data='/out/data-'+db
 sql('postgres',f'CREATE DATABASE {db};')
 sql(db,'GRANT ALL ON SCHEMA public TO "BootstrapMixed";')
 python(f"from pathlib import Path; Path({data!r}).mkdir(mode=0o700)")
 def credentials(port,label):
  file='/out/commit-credential-'+db+'-'+label
  python(f"from pathlib import Path; p=Path({file!r}); p.write_text('hostaddr=127.0.0.1 port={port} dbname={db} user=BootstrapMixed password='+Path('/secrets/BootstrapMixed').read_text()+' sslmode=disable connect_timeout=5'); p.chmod(0o600)")
  return file
 direct=credentials(5432,'direct');init=probe('init',db,direct,'init');assert init.returncode==0,(init.stdout,init.stderr)
 proxy=subprocess.Popen(['ssh','tux',shlex.join(['docker','exec','-i',NAME,'python3','-B','/source/experiments/archive-engine/native_commit_proxy.py','--mode',mode])],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 assert select.select([proxy.stdout],[],[],10)[0],'proxy not ready'
 proxy_ready=json.loads(proxy.stdout.readline());assert proxy_ready['event']=='ready'
 assert proxy_ready['source_sha256']==hashlib.sha256(Path('experiments/archive-engine/native_commit_proxy.py').read_bytes()).hexdigest()
 via=credentials(proxy_ready['port'],'proxy');written=probe('write',db,via,'write')
 out,err=proxy.communicate('stop\n',timeout=10);assert proxy.returncode==0 and not err,(out,err)
 events=[proxy_ready]+[json.loads(line) for line in out.splitlines()];terminal=events[-1];assert terminal['event']=='terminal'
 snapshot=sql(db,'SELECT coalesce(json_agg(row_to_json(t)),\'[]\'::json) FROM (SELECT snapshot_id,schema_version,next_file_id FROM ducklake_snapshot ORDER BY snapshot_id)t;')
 files=json.loads(sql(db,"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT d.data_file_id,d.path,d.path_is_relative,d.file_size_bytes,d.record_count,t.table_name,t.path AS table_path,s.path AS schema_path FROM ducklake_data_file d JOIN ducklake_table t ON t.table_id=d.table_id JOIN ducklake_schema s ON s.schema_id=t.schema_id ORDER BY t.table_name)t;"))
 physical=json.loads(python(f"import json,hashlib; from pathlib import Path; p=Path({data!r}); print(json.dumps([{{'path':str(f.relative_to(p)),'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}} for f in sorted(p.rglob('*.parquet'))]))"))
 count=probe('read',db,direct,'count');body=probe('read-body',db,direct,'body')
 record=dict(mode=mode,write=dict(exit=written.returncode,stdout=written.stdout,stderr=written.stderr),proxy=events,snapshots=json.loads(snapshot),catalog_files=files,physical_files=physical,count=dict(exit=count.returncode,stdout=count.stdout,stderr=count.stderr),body=dict(exit=body.returncode,stdout=body.stdout,stderr=body.stderr))
 records.append(record);(OUT/'cases.json').write_text(json.dumps(dict(source=REV,harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),cases=records),indent=2)+'\n')
 assert terminal['data_queries']==terminal['data_commits']==1,record
 if mode=='pass':
  assert terminal['faults']==0 and terminal['server_complete']==terminal['client_complete']==1
  assert written.returncode==0 and len(files)==len(physical)==3 and body.returncode==0,record
  assert json.loads(body.stdout)=={'stage':'read-body','counts':[1,1,1],'body':'canary-ß','endpoint':'wss://example.invalid','digest':[0,255]},record
 elif mode=='before-forward':
  assert terminal['faults']==1 and terminal['server_complete']==terminal['client_complete']==0
  assert written.returncode==1 and not files and not physical and json.loads(count.stdout)['counts']==[0,0,0],record
 else:
  assert terminal['faults']==1 and terminal['server_complete']==1 and terminal['client_complete']==0
  assert written.returncode==1 and len(files)==3 and len(physical)==0 and body.returncode==1,record
 print(json.dumps({'mode':mode,'catalog_files':len(files),'physical_files':len(physical),'body_exit':body.returncode,'status':'observed'}),flush=True)
print('COMPLETE: actual native committed-file deletion reproduced',flush=True)
