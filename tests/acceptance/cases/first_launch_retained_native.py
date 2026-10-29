"""Declared exact-source observer; opens only the joined store read-only."""
import json
import os
from pathlib import Path
import sys

from cases.first_launch import LABEL, digest, load, require, save

MIB = 1024*1024


def artifact(build, candidate):
    build = Path(build).resolve(strict=True)
    paths = {name:build/name for name in ('source-manifest.json','build-result.json',
        'ingest-bootstrap-probe-build.json','ingest_bootstrap_probe')}
    source, parent, receipt = (load(paths[n]) for n in (
        'source-manifest.json','build-result.json','ingest-bootstrap-probe-build.json'))
    require(receipt['format']=='bigbrotr-ingest-bootstrap-probe-build/1'
        and receipt['profile']=='release'
        and receipt['command']==['cargo','build','--locked','--release','-p','bigbrotr','--example','ingest_bootstrap_probe'],
        'declared maintained native observer build')
    require(source['revision']==receipt['source_revision']==candidate['artifact']['source']['revision']
        and source['tree']==receipt['source_tree']==candidate['artifact']['source']['tree'],
        'native observer/product source differs')
    require(digest(paths['source-manifest.json'])==receipt['source_manifest_sha256']==parent['source_manifest_sha256']
        ==candidate['artifact']['source']['manifest_sha256']
        and digest(paths['build-result.json'])==receipt['parent_build_receipt_sha256']
        and receipt['native_toolchain']==parent['native_toolchain']
        and parent['build_info']==candidate['artifact']['build_info']
        and digest(paths['ingest_bootstrap_probe'])==receipt['binary_sha256'], 'native observer provenance differs')
    require(os.access(paths['ingest_bootstrap_probe'],os.X_OK), 'native observer is not executable')
    return paths, {name:digest(path) for name,path in paths.items()}


def identity(info, runtime, entry):
    require(info['Name']=='/'+entry['name'] and info['Image']==runtime.resources['image']['id']
        and info['Config']['Labels'].get(LABEL)==runtime.owner, 'native observer ownership differs')
    require(entry['id'] in (None,info['Id']), 'native observer incarnation changed')
    mounts = {m['Destination']:(m['Source'],m['RW'],m['Type']) for m in info['Mounts']}
    require(mounts=={'/observer/probe':(str(runtime.native_paths['ingest_bootstrap_probe']),False,'bind'),
        '/observer/store':(entry.get('store',str(runtime.state/'data/ingest')),False,'bind')}, 'native observer mounts differ')
    host = info['HostConfig']
    require(host['NetworkMode']=='none' and host['ReadonlyRootfs'] is True
        and host['Memory']==host['MemorySwap']==512*MIB and host['NanoCpus']==1_000_000_000
        and host['PidsLimit']==64 and host['CapDrop']==['ALL']
        and host['SecurityOpt']==['no-new-privileges:true']
        and host['RestartPolicy']['Name']=='no', 'native observer resource envelope differs')
    require(info['Config']['Entrypoint']==['/observer/probe']
        and info['Config']['Cmd']==['dump-state','/observer/store']
        and info['Config']['User']==f'{os.geteuid()}:{os.getegid()}', 'native observer invocation differs')
    entry.update(id=info['Id'],status=info['State']['Status'])
    return info


def stop(runtime):
    entry = runtime.resources.get('native_observer')
    if entry is None:
        return
    result = runtime.docker('inspect',entry['id'] or entry['name'],check=False)
    if result.returncode:
        require(entry['id'] is None, 'recorded native observer disappeared')
        absent=runtime.docker('ps','--all','--filter','name=^/'+entry['name']+'$',
            '--format','{{.ID}}')
        require(not absent.stdout.strip(),'unknown-create native observer remains')
        entry['status']='absent';runtime.record_resources();return
    info, = json.loads(result.stdout)
    identity(info,runtime,entry)
    if info['State']['Running']:
        runtime.docker('stop','--time','5',info['Id'],timeout=10)
        info, = json.loads(runtime.docker('inspect',info['Id']).stdout)
        identity(info,runtime,entry)
    require(not info['State']['Running'] and info['State']['Pid']==0, 'native observer did not join')
    runtime.record_resources()  # Persistence must never precede the actual stop.
    save(runtime.root/'native-observer-terminal.json',dict(id=info['Id'],state=info['State']))


def census(runtime, output, *, store=None):
    # Original application/backend absence is independently proved first. The
    # read-only bind is a second boundary, not a reason to inspect a live store.
    runtime.backend_absence(output)
    original = load(runtime.root/'initial/terminal-container.json')
    info, = json.loads(runtime.docker('inspect',original['id']).stdout)
    require(not info['State']['Running'] and info['State']['Pid']==0
        and info['State']['ExitCode']==0 and not info['State']['OOMKilled'], 'writer must be positively joined')
    store=runtime.state/'data/ingest' if store is None else Path(store).resolve(strict=True)
    require(store.resolve(strict=True).is_relative_to((runtime.state/'data').resolve(strict=True)), 'native observer store outside owned data')
    require({name:digest(path) for name,path in runtime.native_paths.items()}==runtime.native_hashes,
        'native observer artifacts changed')
    serial=getattr(runtime,'native_observer_serial',0)+1
    runtime.native_observer_serial=serial
    if serial>1:
        runtime.resources.setdefault('native_observer_history',[]).append(dict(runtime.resources['native_observer']))
    name = runtime.project+'-native-observer'+('' if serial==1 else '-'+str(serial))
    require(runtime.docker('inspect',name,check=False).returncode!=0, 'native observer name collision')
    entry = dict(name=name,id=None,status='creating',store=str(store))
    runtime.resources['native_observer']=entry;runtime.record_resources()
    process = None
    try:
        result = runtime.docker('create','--name',name,'--label',LABEL+'='+runtime.owner,
            '--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true',
            '--memory','512m','--memory-swap','512m','--cpus','1','--pids-limit','64',
            '--user',f'{os.geteuid()}:{os.getegid()}',
            '--mount',f'type=bind,src={runtime.native_paths["ingest_bootstrap_probe"]},dst=/observer/probe,readonly',
            '--mount',f'type=bind,src={store},dst=/observer/store,readonly',
            '--entrypoint','/observer/probe',runtime.resources['image']['id'],'dump-state','/observer/store')
        entry['id']=result.stdout.strip();runtime.record_resources()
        info, = json.loads(runtime.docker('inspect',entry['id']).stdout)
        identity(info,runtime,entry)
        # Process caps each stream separately:1MiB each preserves2MiB aggregate.
        process = runtime.proxy_process(['docker','start','--attach',entry['id']],limit=MIB)
        code = process.wait(30)
        require(code==0 and not process.stderr and not process.overflow, 'native observer failed or incomplete')
        info, = json.loads(runtime.docker('inspect',entry['id']).stdout)
        identity(info,runtime,entry)
        require(not info['State']['Running'] and info['State']['Pid']==0
            and info['State']['ExitCode']==0 and not info['State']['OOMKilled'], 'native observer terminal differs')
        value = json.loads(process.stdout)
        require(value['event']=='dump-state' and len(value['rows'])<=2048, 'native observer census bound')
        return value
    finally:
        primary = sys.exception()
        try:
            if process is not None:
                process.stop()
                (output/'native-observer.stdout').write_bytes(process.stdout)
                (output/'native-observer.stderr').write_bytes(process.stderr)
                save(output/'native-observer-process.json',dict(pid=process.process.pid,
                    exit_code=process.process.returncode,overflow=process.overflow,
                    per_stream_limit_bytes=MIB,output_limit_bytes=2*MIB))
        except Exception as error:
            runtime.resources['cleanup_errors'].append(dict(resource='native-observer-output',error=type(error).__name__))
            if primary is None:
                primary=error
        try:
            stop(runtime)
        except Exception as error:
            runtime.resources['cleanup_errors'].append(dict(resource='native-observer-stop',error=type(error).__name__))
            if primary is None:
                primary=error
        if sys.exception() is None and primary is not None:
            raise primary
