"""Outside-checkout tutorial with owned TCP observations and Nostr negatives."""
import ipaddress
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import tomllib

from cases.first_launch import Runtime, digest, load, require, save
from cases import first_launch_index_baseline as baseline
from cases.first_launch_index_pin import ShortPinRuntime

# Absence of a reachable Nostr signer is a reviewed source claim, not a runtime
# counter. A different product needs a fresh reachable-path review before this
# closed tutorial qualification can advertise that absence.
REVIEWED_PRODUCT = '20f46ce9f810de43501982d971eff4625dcfad8c'
# Git objects bind all product code/dependencies/configuration, allowing a
# documentation/acceptance-only commit without pretending a new signer was reviewed.
# The profile review includes the storage/custody extension and the initial
# retirement ordering, reclamation allowance and live Index policy-lifetime fixes;
# Complete retained-group/current-retry changes hash receipt identities, read
# catalogs and retain custody; they add no Nostr signing credentials or calls.
# Current policy3/startup and complete initialization-retry validation were reviewed.
# Eight Nostr paths are unchanged; resolve.rs only removes unused version state.
# Fixed code/contract identities retain the full source-only exclusion guard.
REVIEWED_CODE = {
    'apps':'c4c383ec4d37e3a5b555a72a6587c3e9b314e6b5',
    'crates':'80acabc650a601d0f8b37462039158d791d7ae8d',
    'vendor':'cba63f2b14b7c9c7460ff260c56e85ba97f1b611',
    'Cargo.toml':'d3c301c263f5664b2eff1ae563b3a8079169c5b4',
    'Cargo.lock':'fd13862f44669853b73b9aaa03a189fbfeecb597',
    'rust-toolchain.toml':'c3f67b6771b777215340531caf051bc25cef066c',
    'contracts':'ba9aa9ccd6319abcf6edf02d99581af9d2579a14',
    'config':'1e22ccc281bc6bdda677b3db238584fa01e6cdd1',
}
NOSTR_PATHS = ('apps/bigbrotr/src/main.rs','apps/bigbrotr/src/cli.rs',
    'apps/bigbrotr/src/event.rs','crates/bigbrotr-protocol/src/event.rs',
    'apps/bigbrotr/src/interfaces/serve.rs','apps/bigbrotr/src/config/resolve.rs',
    'apps/bigbrotr/src/interfaces/serve/initial_ingest/sync/receive.rs',
    'apps/bigbrotr/src/acquisition/session.rs','apps/bigbrotr/src/network/websocket.rs')
UNWIRED = 'selected role is not implemented by this runtime; select only control explicitly'
ACTIVE_WRITE = 'monitor.active_write: signer purpose/endpoint policy adapter not implemented; refusing enablement'
LIMITS = [
    'The declared clean checkout supplies maintained clients/fixtures; candidate bundle, runtime cwd and fresh state/evidence are outside every working checkout. This is not source-free execution.',
    'The original signed acquisition, Archive publication and40s history/12s export journey run unchanged. The ordinary startup observation uses the existing bounded full-probe completion helper inside that40s window.',
    'TCP listener snapshots cover the exact owned application/client PIDs. PostgreSQL evidence covers its actual published loopback binding and reachability, not uninspected process descriptors.',
    'Zero Nostr signing is a reachable-path claim bound to reviewed product20f46ce9; zero outbound Nostr publication is checked by the actual nonempty peer transcript through EOF and process join. HMAC/TLS and Archive SQL publication are distinct operations.',
    'Unwired Monitor startup and active-write enablement must return their exact exit3 diagnostics without Ready output or changed state. Missing or incomplete peer/listener/source observations cannot pass.',
    'NOT_RUN: general egress exclusion, public serving, live Index succession, arbitrary signing-enabled profiles, broad capacity and the complete P00–P10 product.',
]


def canonical_inputs(args):
    """The command is portable with declared repository inputs, outside checkout."""
    from candidate import format
    source=format.safe_path(args.source_root,directory=True)
    require(args.source_root.is_absolute() and source==args.source_root and source.resolve(strict=True)==source,
            'tutorial source must be canonical absolute')
    require(args.evidence.is_absolute() and '..' not in args.evidence.parts,'tutorial evidence must be absolute without traversal')
    require(not os.path.lexists(args.evidence) and args.evidence.resolve()==args.evidence,
            'tutorial evidence must be fresh and canonical, without a final symlink')
    parent=format.safe_path(args.evidence.parent,directory=True)
    candidate=args.candidate_manifest
    require(candidate is not None and candidate.is_absolute(),'tutorial candidate must be absolute')
    format.safe_path(candidate.parent,directory=True)
    if candidate.exists() or candidate.is_symlink():
        format.safe_path(candidate)
    require(shutil.which('git') is not None,'tutorial requires Git for the outside-checkout check')
    for path in (Path.cwd(),parent):
        format.safe_path(path,directory=True)
        require(path!=source and source not in path.parents,
                'tutorial working/evidence path overlaps checkout')
        observed=subprocess.run(['git','-C',str(path),'rev-parse','--show-toplevel'],
            capture_output=True,text=True,timeout=5,env={'PATH':os.environ.get('PATH',os.defpath),'LANG':'C','LC_ALL':'C'})
        require(observed.returncode==128 and observed.stderr.startswith('fatal: not a git repository'),
                'tutorial working/evidence path must be outside a working checkout; ambiguous Git observation refuses')
    return dict(source=str(source),working_directory=str(Path.cwd()),evidence=str(args.evidence))


def process_start(root):
    raw=(root/'stat').read_bytes()
    require(len(raw)<=4096 and raw.count(b'(')>=1 and b') ' in raw,'bounded owned process identity')
    tail=raw[raw.rfind(b') ')+2:].split()
    require(len(tail)>=20 and tail[0]!=b'Z','owned process absent or zombie')
    return int(tail[19])


def owned_listeners(pid):
    require(type(pid) is int and pid>0,'positive owned listener PID')
    root=Path('/proc')/str(pid);started=process_start(root)
    fds=list((root/'fd').iterdir());require(len(fds)<=4096,'owned descriptor observation bound')
    inodes=set()
    for path in fds:
        try:
            target=os.readlink(path)
        except FileNotFoundError:
            continue  # A closed descriptor is not evidence of a listener.
        match=re.fullmatch(r'socket:\[(\d+)\]',target)
        if match:inodes.add(match[1])
    listeners=[]
    for family in ('tcp','tcp6'):
        with (root/'net'/family).open('rb') as opened:raw=opened.read(1024*1024+1)
        require(len(raw)<=1024*1024,'owned network-table observation bound')
        for line in raw.splitlines()[1:]:
            fields=line.split();require(len(fields)>=10,'network table shape')
            if fields[9].decode() not in inodes or fields[3]!=b'0A':continue
            address,port=fields[1].decode().split(':')
            encoded=b''.join(int(address[n:n+8],16).to_bytes(4,sys.byteorder) for n in range(0,len(address),8))
            listeners.append(dict(address=str(ipaddress.ip_address(encoded)),port=int(port,16),family=family))
    require(process_start(root)==started,'owned listener process changed')
    return dict(pid=pid,start_ticks=started,listeners=sorted(listeners,key=lambda row:(row['family'],row['address'],row['port'])))


def loopback_listener(value, port):
    require(value['listeners']==[dict(address='127.0.0.1',port=port,family='tcp')],
            'owned process has missing, extra or non-loopback TCP listener')


def refusal(result, reason):
    require(result.returncode==3 and result.stdout=='' and result.stderr=='error: '+reason+'\n',
            'expected exact installed refusal without startup/Ready output')


def transcript(home):
    require(not any((home/name).exists() for name in ('relay-error.json','relay-shutdown.json')),
            'peer transcript incomplete or failed')
    opened=sorted(home.glob('relay-open-*.json'));closed=sorted(home.glob('relay-closed-*.json'))
    require(3<=len(opened)==len(closed)<=128,'every accepted peer connection must close within original bound')
    nonces=set()
    for number,(before,after) in enumerate(zip(opened,closed),1):
        first,last=load(before),load(after)
        require(before.name==f'relay-open-{number:03}.json' and after.name==f'relay-closed-{number:03}.json',
                'peer connection inventory has a gap')
        require(all(last.get(k)==v for k,v in first.items()) and first['number']==number,'peer open/close binding differs')
        command=last['request']
        require(len(command)==3 and command[0]=='REQ' and command[2]==dict(kinds=[1],limit=3)
            and re.fullmatch('[0-9a-f]{48}',command[1]) and int(command[1][:32],16)>0
            and command[1][:32] not in nonces,'peer request is not the bounded acquisition command')
        nonces.add(command[1][:32])
        require(last['connected_at_us']<=last['request_at_us']<=last['closed_at_us']
            and 0<last['closed_at_us']-last['request_at_us']<3_000_000,'peer chronology/bound differs')
        require((last['frames'],last['completion'])==(
            (['signed','forged-signature','duplicate','EOSE'],'request-and-websocket-close') if number<=2
            else ([], 'quiet-eof')),'peer round lacks complete validated REQ/CLOSE/EOF traffic')
    return dict(connections=len(closed),signed_rounds=2,outbound_nostr_event_frames=0,
        scope='all accepted controlled-peer connections; parser requires CLOSE/empty WS close/TCP EOF or quiet EOF')


class TutorialRuntime(Runtime):
    def preflight(self):
        # run.py checks the original spelling before creating this directory.
        paths=getattr(self.args,'tutorial_paths',None)
        require(isinstance(paths,dict),'tutorial requires canonical input validation by the maintained runner')
        self.inputs['tutorial_paths']=paths
        super().preflight()

    def candidate(self):
        require(digest(__file__)==digest(self.source/'tests/acceptance/cases/first_launch_tutorial.py'),
                'tutorial helper source mismatch')
        super().candidate()
        revision=self.manifest['artifact']['source']['revision']
        for path,expected in REVIEWED_CODE.items():
            observed=self.command(['git','-C',str(self.source),'rev-parse',revision+':'+path],label='nostr-code-profile')
            require(observed.stdout.strip()==expected,'Nostr exclusion needs review of changed product code/dependencies/configuration')
        hashes={}
        for name in NOSTR_PATHS:
            recorded=self.command(['git','-C',str(self.source),'show',revision+':'+name],label='nostr-source')
            require(hashlib.sha256(recorded.stdout.encode()).hexdigest()==digest(self.source/name),
                    'reviewed Nostr source path differs from executed product source')
            hashes[name]=digest(self.source/name)
        self.inputs['nostr_source_exclusion']=dict(product_revision=revision,reviewed_revision=REVIEWED_PRODUCT,
            code_git_objects=REVIEWED_CODE,files_sha256=hashes,
            signing='no reachable Nostr signing API in selected profile',sending='session REQ/CLOSE only',
            mechanism='source review plus exact artifact identity; not a runtime call counter')

    def bootstrap(self):
        output=self.root/'capability-negatives';output.mkdir(mode=0o700)
        original=self.config.read_text();before=self.files(self.state/'data')
        unwired=self.set_value(original,'topology','local_roles',['control','ingest','sync','archive','monitor'])
        unwired=self.set_value(unwired,'monitor','enabled',True)
        path=output/'unwired.toml';path.write_text(unwired)
        valid=self.command([str(self.bundle/'bigbrotr'),'config','validate','--file',str(path)],label='unwired-static')
        require(valid.returncode==0 and not valid.stderr and json.loads(valid.stdout)['signing'] is False,'unwired negative must have valid static inputs')
        observed=self.command([str(self.bundle/'bigbrotr'),'serve','--file',str(path),'--roles','control,monitor'],
            check=False,timeout=10,label='unwired-role')
        refusal(observed,UNWIRED)
        path=output/'active-write.toml';path.write_text(self.set_value(original,'monitor.active_write','enabled',True))
        expected=tomllib.loads(original);expected['monitor'].setdefault('active_write',{})['enabled']=True
        require(tomllib.loads(path.read_text())==expected,'active-write negative changed another input')
        observed=self.command([str(self.bundle/'bigbrotr'),'config','validate','--file',str(path)],
            check=False,timeout=10,label='active-write-refusal')
        refusal(observed,ACTIVE_WRITE)
        require(self.config.read_text()==original and self.files(self.state/'data')==before,'negative changed original configuration/state')
        for port in (15432,18081,7447):
            with socket.socket() as connection:connection.bind(('127.0.0.1',port))
        save(output/'verdict.json',dict(status='PASS',unwired_role='exit3/no_startup_or_ready',
            active_write='exit3/signer-purpose-endpoint-adapter-refused',original_state='unchanged'))
        super().bootstrap()

    def listener_observation(self, entry, output, *, peer=False):
        info=self.inspect(entry);pid=info['State']['Pid']
        require(info['State']['Running'],'listener owner is not running')
        app=owned_listeners(pid);loopback_listener(app,18081)
        require(self.inspect(entry)['State']['Pid']==pid,'listener application incarnation changed')
        value=dict(application=app)
        if peer:
            require(self.client.poll() is None,'peer client already exited')
            value['controlled_peer']=owned_listeners(self.client.pid);loopback_listener(value['controlled_peer'],7447)
        pg=self.inspect(self.pg)
        expected={'5432/tcp':[dict(HostIp='127.0.0.1',HostPort='15432')]}
        require(pg['State']['Running'] and pg['HostConfig']['PortBindings']==expected
            and pg['NetworkSettings']['Ports']==expected,'PostgreSQL published listener differs')
        with socket.create_connection(('127.0.0.1',15432),timeout=1) as connection:
            value['postgres']=dict(id=pg['Id'],published=expected,peer=list(connection.getpeername()),
                scope='actual Docker published binding plus TCP reachability; process descriptors not inspected')
        save(output/'tutorial-listeners.json',value)

    def initial_publication(self, entry, output):
        self.listener_observation(entry,output,peer=True)
        value=ShortPinRuntime.roles(self,time.monotonic()+2,session_path=self.home/'session.private')
        require([r['role'] for r in value['roles']]==['control','ingest','sync','archive']
            and [c['capability'] for c in value['capabilities']]==[
                'signed_event_admission','finite_acquisition','archive_publication'],
            'selected profile advertises an absent or unwired capability')
        save(output/'tutorial-roles.json',value)

    def initial(self):
        super().initial()
        self.peer_observation=transcript(self.home)
        save(self.root/'initial/tutorial-peer-verdict.json',self.peer_observation)

    def ordinary_client(self, output, entry):
        self.listener_observation(entry,output)
        return baseline.ordinary_client(self,output,entry)

    def ordinary(self):
        super().ordinary()
        require(self.export_result['jsonl_bytes']==353,'tutorial must export the nonempty original signed event')
        value=dict(format='bigbrotr-installed-first-event-tutorial/1',status='PASS',
            paths=self.inputs['tutorial_paths'],listeners='owned initial and ordinary application plus controlled peer',
            signing='zero reachable Nostr signing calls in reviewed exact-source profile',
            publication=self.peer_observation,negative_capabilities='actual exact exit3 refusals',
            signed_export_bytes=353,archive_publication='required and independently verified',
            live_index_successor='NOT_RUN; retained P02-T06 full-product work')
        save(self.root/'ordinary/tutorial-verdict.json',value);self.inputs['tutorial']=value
