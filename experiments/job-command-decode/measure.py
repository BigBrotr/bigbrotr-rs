#!/usr/bin/env python3
"""Isolated System allocator probe for the actual linked job decoder.

First build the exact trusted library with `cargo build -p bigbrotr --lib
--message-format=json > BUILD_MESSAGES`. This executable is research only; it
uses unsafe GlobalAlloc delegation while product packages keep unsafe forbidden.
Output is a scoped diagnostic, not process-RSS, native or release qualification.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p=argparse.ArgumentParser();p.add_argument('--build-messages',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
p.add_argument('--case',choices=['unknown','projection'],default='unknown')
a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
artifacts={}
for line in a.build_messages.read_text().splitlines():
    v=json.loads(line)
    if v.get('reason')=='compiler-artifact':
        for path in v['filenames']:
            if path.endswith('.rlib'):artifacts[v['target']['name']]=Path(path)
source=Path(__file__).with_name('measure.rs');binary=a.output/'probe'
args=['rustc','--edition=2024',str(source),'-o',str(binary),'-L','dependency='+str(artifacts['serde_json'].parent)]
for name in ['bigbrotr','serde_json']:args+=['--extern',name+'='+str(artifacts[name])]
subprocess.run(args,check=True)
result=subprocess.run([str(binary),a.case],capture_output=True,text=True)
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
(a.output/'stdout').write_text(result.stdout);(a.output/'stderr').write_text(result.stderr)
(a.output/'result.json').write_text(json.dumps(dict(exit=result.returncode,probe_sha256=sha(source),binary_sha256=sha(binary),
    libraries={k:dict(path=str(artifacts[k]),sha256=sha(artifacts[k])) for k in ['bigbrotr','serde_json']},
    rustc=subprocess.check_output(['rustc','--version','--verbose'],text=True),stdout=result.stdout),indent=2)+'\n')
print(result.stdout,end='');raise SystemExit(result.returncode)
