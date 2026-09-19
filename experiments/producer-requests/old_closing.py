#!/usr/bin/env python3
"""Exercise the unchanged old native writer on copies of actually closed stores."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0,str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute
from closing import expected_native
from import_originals import originals

TEST='storage::rocks::store::old_writer_closing_tests::unchanged_old_writer_refuses_actual_closed_directories'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def files(root): return {str(p.relative_to(root)):sha(p) for p in sorted(root.rglob('*')) if p.is_file()}


def main(root):
    resources=json.loads((root/'resources.json').read_text())
    work=root/'out/old-closing-probe'
    ready=json.loads((work/'ready.json').read_text());assert ready['status']=='READY'
    assert ready['baseline_product']=='442f3d878b3215e421a71160b3613b7ab0c93c7b'
    assert ready['sha256']==sha(work/'old-closing-tests')
    assert ready['baseline_files_identical_except_exact_test_hook'] and ready['source_unchanged']
    owner=json.loads(command('docker','inspect',resources['container_id']))[0]
    assert owner['State']['Running'] and not owner['State']['OOMKilled']
    assert owner['Config']['Labels']['org.bigbrotr.publication']==resources['owner']
    report=dict(status='INCOMPLETE',new_source=resources['source'],old=ready,
                observer_revision=command('git','-C',str(SOURCE),'rev-parse','HEAD'),copies={},process={})
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    target=root/'old-closing.json';assert not target.exists()
    def save():target.write_text(json.dumps(report,indent=2)+'\n')
    save()
    for tag,label in [('normal1','populated'),('empty1','empty')]:
        original=root/('out/closing-'+tag+'/db');copy=work/label
        qualification=json.loads((root/('closing-'+tag+'.json')).read_text())
        assert qualification['status']=='PASS' and qualification['source']==resources['source']
        assert not copy.exists()
        before=files(original)
        assert before and all(not p.is_symlink() for p in original.rglob('*'))
        shutil.copytree(original,copy,copy_function=shutil.copy2)
        assert files(copy)==before
        report['copies'][label]=dict(source=str(original.relative_to(root)),target=str(copy.relative_to(root)),
                                   qualification_sha256=sha(root/('closing-'+tag+'.json')),files_before=before)
        save()
    try:
        execute(['docker','exec','-e','BIGBROTR_OLD_WRITER_POPULATED=/out/old-closing-probe/populated',
                 '-e','BIGBROTR_OLD_WRITER_EMPTY=/out/old-closing-probe/empty',resources['container_id'],
                 '/out/old-closing-probe/old-closing-tests',TEST,'--ignored','--exact','--nocapture','--test-threads=1'],
                40,report['process'])
    finally: save()
    observation=report['process'];assert observation['exit']==0 and not observation['stderr']
    # libtest may prefix the first JSON line with the test name. Decode only the
    # explicit tagged messages; never infer success from the process exit alone.
    messages=[]
    for line in observation['stdout'].splitlines():
        at=line.find('{"baseline":')
        if at>=0:
            value=json.loads(line[at:]);assert value['event']=='old-writer-closing'
            assert value['baseline']==ready['baseline_product'];messages.append(value)
    phases={'populated':[('preflight','observed'),('event-stage-3','staged'),('event-admit-3','Err(Limit)'),
                         ('bootstrap-stage-1','staged'),('bootstrap-after-stage-1','Err(StateMismatch)'),('reopen','observed')],
            'empty':[('preflight','observed'),('bootstrap-direct-1','Err(StateMismatch)'),
                     ('bootstrap-stage-1','staged'),('bootstrap-after-stage-1','Err(StateMismatch)'),('reopen','observed')]}
    signed=originals(1)
    filename=lambda n:(bytes([66])*16).hex()+'-0000000000000001-'+f'{n:016x}'+'.cbor'
    for case,wanted_phases in phases.items():
        found=[m for m in messages if m['case']==case]
        assert [(m['phase'],m['result']) for m in found]==wanted_phases
        rows=[[cf,k.hex(),v.hex()] for cf,k,v in expected_native(case=='empty',True)]
        for m in found:
            assert m['state']['rows']==rows
            assert m['state']['sequence']==found[0]['state']['sequence']
            count=(2 if m['phase']=='preflight' else 3) if case=='populated' else (0 if m['phase'] in ['preflight','bootstrap-direct-1'] else 1)
            assert m['state']['originals']=={filename(n):signed[n].hex() for n in range(1,count+1)}
        original=root/report['copies'][case]['source']
        assert files(original)==report['copies'][case]['files_before']
        report['copies'][case]['source_unchanged']=True
    assert len(messages)==11
    report['checked']=dict(snapshots=11,old_scientific_writes=0,added_originals=dict(populated=[3],empty=[1]),
                           scope='actual old writer library; native boundary only, test callbacks grant no PG authority')
    report['status']='PASS';save();print(json.dumps(dict(status='PASS',**report['checked'])))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
