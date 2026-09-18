import json,os,signal,subprocess,time
from pathlib import Path
root=Path('/lab');temp=root/'tmp';temp.mkdir()
command=['strace','-f','-k','-ttt','-o',str(root/'threads.log'),'-e','trace=clone,clone3','/build/archive-worker/bigbrotr-archive-worker','inspect-engine','/build/archive-worker',str(temp),'268435456','134217728','1']
with (root/'stdout.json').open('wb') as out,(root/'stderr.txt').open('wb') as err:
 p=subprocess.Popen(command,stdout=out,stderr=err,env={'PATH':'/usr/bin:/bin','HOME':str(root)},start_new_session=True)
 try:code=p.wait(timeout=30)
 finally:
  if p.poll() is None:os.killpg(p.pid,signal.SIGKILL)
  p.wait()
(root/'result.json').write_text(json.dumps({'exit':code,'command':command})+'\n')
raise SystemExit(code)
