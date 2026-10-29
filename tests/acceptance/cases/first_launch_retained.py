"""Installed finite64 saturation with the independently parked Archive owner."""
import errno
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import time

from cases.first_launch import MissingPrerequisite, digest, load, require, save
from cases.first_launch_archive_outage import ArchiveOutageRuntime, STAGES
from cases.first_launch_index import DATABASE
from cases.first_launch_index_live import LiveTopologyIndexRuntime
from cases.first_launch_index_query_trace import QueryTraceRuntime, query_trace_selection
from cases import first_launch_index_oracle as index
from cases import first_launch_index_live_oracle as live_oracle
from cases import first_launch_index_topology_oracle as topology
from cases import first_launch_index_baseline as baseline
from cases import first_launch_retained_oracle as oracle
from cases import first_launch_retained_native as native

LIMITS = [
    'Fresh actual source3/Archive2/Index2 prefix under the original15s gate; only the catalog route is then closed and joined. Control, Sync and Index retain their actual owners.',
    'Writer-only lifecycle logging retains the existing1024-row queue/512-row flush threshold/7500ms interval and writer quota1. After independently verified prefix readiness, one ClickHouse clock query records a start-time cutoff before catalog fault/source release under the same15s deadline. After actual joins, the8192-row/4MiB/30s collector applies that cutoff to both count and rows. Later64-workload queries are excluded; incomplete pairs stay diagnostic INCOMPLETE. All-query coverage and remote termination/exclusion remain NOT_PROVEN; this does not close the earlier Code202 cause.',
    'Two bootstrap requests plus one source-dictionary request leave61 event mutation slots. The frozen62 signed events use public fixture keys1/3. Twenty quanta send3 new events; quantum21 sends event61, eligible event62 and proofevent1. The actual prefix must stop at frame1 before any later proof is released.',
    'One independently gated proof-only REQ then advances classification61→62 and checkpoint64→65, with complete64, source312 and all64 original requests unchanged. Up to100 legitimate emptyEOSE quanta follow or wait at the gate; later quanta are quiet. Original128 connections,3s socket,500ms product receive and65s submitted bounds remain; no issued input is retried.',
    'After128 completed rounds and the delivered proof, the finite peer closes its listener before recording completion and accepts no129th connection. Shutdown checks either an actual quiet receive or this independent finite-source end, including closed rounds/proof and a refused loopback connection. Finite completion adds no cancellation evidence or new product capability; Index64/cut312 must still arrive inside the original65s client gate.',
    'Exact publishedIndex64/cut312,640 chunk outcomes and all556 physical rows are independently checked. Do not require Ready: unchanged64-plan scheduling needs at least31.75s before final verification, beyond the product30s sweep. HTTP must preserve its truthful non-Ready historical observation.',
    'After positive writer/backend join, a separately declared exact-product-source observer opens the native store read-only in an owned networknone/512MiB/1CPU container,30s/2MiB output cap. All829 native rows and sequence1081, issued=ACK64/retired0,64 receipts/inputs and no65th effect are compared with literal encodings.',
    'Archive stays cut3/revision2 with issued attempt3, original input and custody; all82 Control tables and original Archive files remain checked. NOT_RUN: dynamic pending-byte hysteresis, GC, disk pressure, automatic recovery,64-plan current readiness, full M1 or general capacity. Earlier ACK/commit-loss qualification is reused, not inferred anew.',
]


def gate(value, now):
    require(set(value)=={'format','started_ns','deadline_ns'}
        and value['format']=='bigbrotr-retained-request-gate/1'
        and type(value['started_ns']) is type(value['deadline_ns']) is int
        and value['deadline_ns']-value['started_ns']==65_000_000_000,
        'closed retained-request client gate')
    require(value['started_ns']/1e9<=now<value['deadline_ns']/1e9,'retained-request client deadline expired')
    return value['deadline_ns']/1e9


def compacted_spool_sizes(directory, census, observe):
    """Bind actual file lengths to the unchanged shared metadata/hash census."""
    require(len(census)==3 and all(Path(name).name==name
        and '-0000000000000002-' in name for name in census),
        'successor did not reuse the freed shared spool')
    def sizes():
        root=directory.lstat()
        require(stat.S_ISDIR(root.st_mode) and root.st_uid==os.geteuid(),
            'shared spool directory ownership differs')
        require({p.name for p in directory.iterdir()}==set(census),
            'shared spool contents differ from census')
        result={}
        for name,expected in census.items():
            metadata=(directory/name).lstat()
            require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid==os.geteuid(),
                'shared spool input is not an owned regular file')
            identity=dict(mode=stat.S_IMODE(metadata.st_mode),inode=metadata.st_ino,
                device=metadata.st_dev,links=metadata.st_nlink)
            require(identity=={key:expected[key] for key in identity},
                'shared spool file identity differs from census')
            result[name]=metadata.st_size
        return result
    before=sizes()
    total=sum(before.values())
    require(total<=32*1024*1024,'successor retained inputs exceed shared32MiB spool')
    require(observe(directory)==census,'shared spool census changed during size observation')
    require(sizes()==before,'shared spool file sizes changed during census recheck')
    return dict(total_bytes=total,files=before,shared_limit_bytes=32*1024*1024)


class RetainedRuntime(QueryTraceRuntime, ArchiveOutageRuntime):
    retained_boundary = True
    source_cut, publication_revision, request_count = 312, 64, 64

    def preflight(self):
        super().preflight()
        build=getattr(self.args,'native_observer_build',None)
        if build is None or not all((build/name).is_file() for name in (
            'source-manifest.json','build-result.json','ingest-bootstrap-probe-build.json','ingest_bootstrap_probe')):
            raise MissingPrerequisite('finite64 needs a separately declared exact-source native observer build')

    def load_helpers(self):
        super().load_helpers()
        import installed_retained_transport
        from installed_sync_client import publish
        self.transport=installed_retained_transport
        self.publish_peer=publish

    def candidate(self):
        super().candidate()
        self.native_paths,self.native_hashes=native.artifact(self.args.native_observer_build,self.manifest)
        self.inputs['native_observer']=dict(artifact_sha256=self.native_hashes,
            source=load(self.native_paths['source-manifest.json'])['revision'],product_unchanged=True)
        fixture=self.source/'tests/fixtures/acquisition/retained-requests.json'
        self.events=self.transport.corpus(fixture)
        self.inputs['acceptance_files_sha256'][str(fixture.relative_to(self.source))]=digest(fixture)
        for module in (__file__,oracle.__file__,native.__file__,self.transport.__file__):
            path=Path(module)
            require(digest(path)==digest(self.source/path.relative_to(Path(__file__).resolve().parents[3])),
                'retained boundary helper source differs')

    def prepare_state(self):
        super().prepare_state()
        shutil.copyfile(self.source/'tests/fixtures/acquisition/retained-requests.json',self.home/'retained-requests.json')
        save(self.home/'retained-request-profile.json',self.transport.PROFILE)

    def command(self, command, **options):
        deadline=getattr(self,'boundary_deadline',None)
        if deadline is not None:
            remaining=deadline-time.monotonic()
            require(remaining>0,'retained-request client deadline before command')
            options['timeout']=min(options.get('timeout',30),remaining)
        result=super().command(command,**options)
        if deadline is not None:
            require(time.monotonic()<deadline,'retained-request command crossed client deadline')
        return result

    def initial(self):
        try:
            super().initial()
        finally:
            self.boundary_deadline=None

    def establish_prefix_fault(self, entry, output):
        verdict=output/'index-prefix-verdict.json'
        proof=load(verdict)
        require(proof.get('status')=='PASS' and proof.get('source_through')==3
            and proof.get('archive_revision')==proof.get('index_revision')==2,
            'query trace requires independently verified ready prefix')
        require(not (self.home/'index-prefix.continue').exists(),
            'query trace prefix source was already released')
        require(getattr(self,'prefix_deadline',None) is not None and time.monotonic()<self.prefix_deadline,
            'query trace prefix client deadline expired')
        observed=self.ch('SELECT toUnixTimestamp64Micro(now64(6)) AS cutoff_us','provisioner',rows=True)
        require(len(observed)==1 and set(observed[0])=={'cutoff_us'},'query trace prefix clock shape')
        cutoff=observed[0]['cutoff_us']
        require(cutoff is not None,'query trace prefix clock missing')
        selected=query_trace_selection(cutoff)
        save(self.trace_output/'prefix-scope.json',dict(format='bigbrotr-index-query-trace-prefix/1',
            scope='prefix_started_queries',query_start_through_us=cutoff,selection=selected,
            clock='ClickHouse toUnixTimestamp64Micro(now64(6))',clock_role='provisioner',
            clickhouse_container=self.ch_entry['id'],prefix_verdict_sha256=digest(verdict),
            source_released=False,catalog_fault=False,collection='after_actual_application_client_joins',
            all_query_coverage='NOT_PROVEN',termination_or_exclusion='NOT_PROVEN',
            earlier_code202_cause='UNKNOWN'))
        self.trace_start_through_us=cutoff
        return super().establish_prefix_fault(entry,output)

    def initial_archive_observation(self, entry, output):
        declared=load(self.home/'retained-request-deadline.json')
        self.boundary_deadline=deadline=gate(declared,time.monotonic())
        save(output/'retained-request-deadline.json',declared)
        def full():
            require(not (self.home/'relay-error.json').exists(),'controlled retained-request relay failed')
            return ((self.home/'relay-closed-021.json').exists() and self.sql('bigbrotr_control',
                'SELECT count(*) FROM control.acquisition_progress WHERE producer_epoch=1 AND classified_events=61')=='1')
        self.wait(full,'actual saturated producer prefix before proof release',deadline-time.monotonic(),entry,self.client)
        self.before_proof,catalog=self.capture(output,'retained-before-proof')
        index.exact(catalog,self.prefix_catalog)
        boundary=load(self.home/'relay-closed-021.json')
        oracle.boundary(self.before_proof,boundary,self.events[60])
        self.saturated_inputs=self.files(self.state/'data/ingest/producer-inputs-v1')
        require(len(self.saturated_inputs)==64,'64 actual retained input files')
        save(output/'retained-before-proof-inputs.json',self.saturated_inputs)
        release=dict(format='bigbrotr-retained-request-proof-release/1',boundary_request=boundary['request'][1],
            classified_events=61,complete_through_request=64,checkpoint_revision=64)
        self.publish_peer(self.home,'retained-proof.continue',release)
        save(output/'retained-proof-release.json',dict(release=release,released_monotonic_ns=time.monotonic_ns()))
        self.wait(lambda:self.sql('bigbrotr_control',
            "SELECT count(*) FROM control.consumer_frontiers f JOIN control.consumers c USING(consumer_id) WHERE c.consumer_role='index' AND f.frontier_revision=c.current_revision AND c.current_revision=64 AND f.last_complete_sequence=312")=='1',
            'exact admitted Index64/cut312',deadline-time.monotonic(),entry,self.client)
        self.wait(lambda:self.sql('bigbrotr_control',
            'SELECT count(*) FROM control.acquisition_progress WHERE producer_epoch=1 AND classified_events=62')=='1',
            'separate post-saturation proof',deadline-time.monotonic(),entry,self.client)

    def observed_publication(self, caps):
        return caps.get('index_publication',{}).get('state') in ('building','stale','unavailable')

    def initial_source_quiet(self):
        path=self.home/'relay-finite-complete.json'
        if not path.exists():
            return super().initial_source_quiet()
        require(not (self.home/'relay-error.json').exists(),'finite retained peer had an earlier relay failure')
        wires=[load(p) for p in sorted(self.home.glob('relay-closed-*.json'))]
        require({p.name for p in self.home.glob('relay-open-*.json')}==
            {f'relay-open-{n:03}.json' for n in range(1,129)},'finite retained peer accepted extra/missing connection')
        oracle.finite_peer(load(path),wires,self.events,digest(self.home/'relay-closed-128.json'))
        with socket.socket() as connection:
            connection.settimeout(1)
            refused=connection.connect_ex(('127.0.0.1',7447))
        require(refused==errno.ECONNREFUSED,'finite retained peer listener remains reachable')
        save(self.root/'initial/retained-peer-completion.json',dict(
            status='OBSERVED_FINITE_SOURCE_END',receipt_sha256=digest(path),connections=128,
            refusal_errno=refused,quiet_cancellation='NOT_RUN'))
        return True

    def validate_roles(self, value, before_us, after_us):
        index.live_roles(value,self.index_live,before_us,after_us,revision=64,sequence=312,
            archive_unavailable=True,retained_boundary=True)
        self.boundary_roles=value

    def initial_publication(self, entry, output):
        try:
            super().initial_publication(entry,output)
            proof, = [load(path) for path in sorted(self.home.glob('relay-closed-*.json'))
                      if load(path).get('retained_phase')=='proof-only']
            oracle.boundary(self.index_live,proof,self.events[0],proof=True)
            index.exact(self.files(self.state/'data/ingest/producer-inputs-v1'),self.saturated_inputs)
            save(output/'retained-boundary-live.json',dict(status='PASS',offered_events=62,
                admitted_events=61,requests=64,source_cut=312,checkpoint_revision=65,
                internal_refusal_token='NOT_OBSERVED',ready=False,roles=self.boundary_roles))
        finally:
            self.boundary_deadline=None

    def source_science(self, view, wires, owner, output):
        index.exact(self.files(self.state/'data/ingest/producer-inputs-v1'),self.saturated_inputs)
        dump=native.census(self,output)
        self.native_sequence=dump['sequence']
        wanted=oracle.science(view,self.before_proof,self.initialized,self.events,wires,dump,
            self.state/'data/ingest',owner['released_at_us'])
        for table in ('acquisition_progress','producer_checkpoints'):
            index.exact(self.index_live[table],view[table])
        return wanted

    def result_fields(self):
        return dict(format='bigbrotr-installed-retained-request-boundary/1',
            readiness='archive_unavailable_index_historical_nonready',source_inputs='64_exact_originals',
            typed_progress='61_mutations_then_one_separately_gated_proof',native_census='829_exact_rows',
            native_sequence=self.native_sequence,capacity='finite64_retained_request_boundary',
            offered_overflow_event='no_request_input_native_effect_or_ack65',internal_refusal_token='NOT_OBSERVED')

    def cleanup(self):
        try:
            native.stop(self)
        except Exception as error:
            self.resources['cleanup_errors'].append(dict(resource='native-observer',error=type(error).__name__))
        finally:
            super().cleanup()

RECLAMATION_STAGES=STAGES[:-1]+('closed_reclamation','compacted_successor',
    'compacted_consumers','compacted_history_export','cleanup')
RECLAMATION_LIMITS=LIMITS[:7]+[
    'After the same genuine64-request capture, explicit closure/abandonment/retirement must precede product reclamation and every successor intent. Exact139-byte floor,65 native sequence operations, all retained science/pending/PG/Archive evidence and zero remaining originals are checked independently.',
    'Fresh reclaim retry and fresh compacted retirement read precede actual enabled-Index epoch2 activation. Its ordinary signed client creates request1 only after the old spool is empty, proving cumulative request65; three genuine successor requests retain the unchanged per-producer64 and shared64-file/32MiB bounds.',
    'Both compacted epoch2 consumers and exact retries, complete322-row Archive science,572 raw Index rows, signed histories and exact353-byte first-event export are required. No Ready65 or scalable history claim follows. Native/SQL unknown and interrupted-unlink mechanics retain separate native/wire fault controls; physical power/fsync failure remains NOT_RUN.',
]


class ReclamationRuntime(RetainedRuntime):
    """The existing finite64 journey extended through real capacity return."""
    phase_guard=LiveTopologyIndexRuntime.phase_guard
    phase_wait=LiveTopologyIndexRuntime.phase_wait

    def verify_index(self):
        super().verify_index()
        self.initial_index_state=load(self.root/'index/after-control.json')

    def cleanup(self):
        for proxy in getattr(self,'reclamation_proxy_handles',[]):
            try:proxy.stop()
            except Exception as error:
                self.resources['cleanup_errors'].append(dict(resource='reclamation-proxy',error=type(error).__name__))
        super().cleanup()

    def finish_reclamation_proxy(self,proxy,original,output,recorded=None):
        import sys
        primary=sys.exception()
        def attempt(resource,operation):
            nonlocal primary
            try:return operation()
            except Exception as error:
                self.resources['cleanup_errors'].append(dict(resource=resource,error=type(error).__name__))
                if primary is None:primary=error
        attempt('reclamation-config-restore',lambda:self.config.write_text(original))
        attempt('reclamation-proxy-stop-command',lambda:self.proxy_send(proxy,'stop\n'))
        attempt('reclamation-proxy-join',lambda:require(proxy.wait(5)==0,'reclamation proxy did not join'))
        terminal=attempt('reclamation-proxy-observation',lambda:self.proxy_observation(proxy))
        if terminal is not None:
            attempt('reclamation-proxy-output',lambda:save(output/'proxy-terminal.json',terminal))
        attempt('reclamation-proxy-stop',proxy.stop)
        if recorded is not None:
            def persist():
                recorded.update(status='joined' if proxy.process.poll() is not None else 'unjoined',exit_code=proxy.process.returncode)
                self.record_resources()
            attempt('reclamation-proxy-resources',persist)
        if sys.exception() is None and primary is not None:raise primary
        return terminal

    def load_helpers(self):
        super().load_helpers()
        # Load/pin the existing independent encoders before candidate identity.
        import installed_family_science
        import installed_archive_catch_up_model
        import installed_archive_export_oracle
        import installed_archive_export_client
        relative="experiments/archive-engine/native_commit_proxy.py"
        self.inputs.setdefault("acceptance_files_sha256",{})[relative]=digest(self.source/relative)

    def candidate(self):
        super().candidate()
        relative='experiments/archive-engine/native_commit_proxy.py'
        self.inputs['acceptance_files_sha256'][relative]=digest(self.source/relative)

    def source_science(self,*args):
        self.retained_science=super().source_science(*args)
        self.retained_science['manifest']=next(v for cf,k,v in self.retained_science['native'] if cf=='default' and k==b'\x01\x01')
        self.retained_science['event']=self.events[0]
        return self.retained_science

    def native_capture(self,label,*,store=None):
        output=self.reclamation_output/label;output.mkdir(mode=0o700)
        value=native.census(self,output,store=store);save(output/'native.json',value)
        return value

    def preserve_reclaimed_files(self):
        require(self.files(self.state/'data/archive')==self.reclaimed_archive_files,'reclamation changed Archive objects')
        index.exact(self.custody(),self.custody_before)

    def refused_reclamation(self,label):
        before,catalog=self.capture(self.reclamation_output,label+'-before')
        inputs=self.files(self.state/'data/ingest/producer-inputs-v1')
        native_before=self.native_capture(label+'-native-before')
        entry=self.launch(label,'app','ingest','recover','--reclaim-initial','--file','/etc/bigbrotr/instance.toml')
        ended=self.docker('wait',entry['id'],timeout=120)
        info=self.inspect(entry);logs=self.docker('logs',entry['id'])
        (self.reclamation_output/(label+'.stdout')).write_text(logs.stdout)
        (self.reclamation_output/(label+'.stderr')).write_text(logs.stderr)
        require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
            and not info['State']['OOMKilled'],
            'unauthorized reclamation must refuse without success')
        disposition=oracle.reclamation_refusal(label,logs.stdout,logs.stderr)
        self.backend_absence(self.reclamation_output)
        after,after_catalog=self.capture(self.reclamation_output,label+'-after')
        oracle.control_only(before,after,claim=disposition)
        index.exact(catalog,after_catalog)
        index.exact(inputs,self.files(self.state/'data/ingest/producer-inputs-v1'))
        index.exact(native_before,self.native_capture(label+'-native-after'))
        # A refused claimant may leave its scheduling lease. Observe database
        # UTC expiry; no guessed rollback, epoch rewrite or manual release.
        self.wait(lambda:self.sql('bigbrotr_control',"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active' AND lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint")=='0',
            'refused Control scheduling lease expiry',65)
        save(self.reclamation_output/(label+'-verdict.json'),dict(status='PASS',exit_code=4,native='exact',inputs='exact',archive='exact'))

    def wait_reclamation_fault(self,proxy,entry,output):
        deadline=time.monotonic()+110
        expected_terminal=False
        def remaining():
            value=deadline-time.monotonic()
            require(value>0,'reclamation fault deadline')
            return value
        while True:
            try:
                fault=self.proxy_event(proxy,'fault',min(1,remaining()))
            except AssertionError as error:
                if error.args!=('fixture event deadline',):raise
                fault=None
            remaining()
            if fault is not None:return fault
            if expected_terminal:continue
            info=self.inspect(entry)
            remaining()
            if not info['State']['Running']:
                logs=self.docker('logs',entry['id'],timeout=min(30,remaining()))
                (output/'serve.stdout').write_text(logs.stdout)
                (output/'serve.stderr').write_text(logs.stderr)
                save(output/'serve-terminal.json',dict(container_id=entry['id'],state=info['State']))
                remaining()
                require(info['State']['ExitCode']==4 and info['State']['Pid']==0
                    and not info['State']['OOMKilled'] and logs.stdout==''
                    and logs.stderr=='error: producer operation unavailable: CommitUnknown\n',
                    'unexpected reclamation child result before proxy fault')
                # An expected child exit can precede the proxy reader's delivery
                # of its already-written fault. Still require that actual record.
                expected_terminal=True

    def reclamation_commit_loss(self,mode):
        import sys
        require(mode in ('before-forward','after-complete'),'closed reclamation COMMIT profile')
        output=self.reclamation_output/('commit-'+mode);output.mkdir(mode=0o700)
        before,catalog=self.capture(output,'before')
        native_before=self.native_capture('commit-'+mode+'-native-before')
        inputs=self.files(self.state/'data/ingest/producer-inputs-v1')
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        original=self.config.read_text()
        argv=[sys.executable,'-B',str(path),'--mode',mode,'--target','control-closed-reclamation',
            '--profile','retained-catch-up','--upstream-port','15432']
        if not hasattr(self,'reclamation_proxy_handles'):self.reclamation_proxy_handles=[]
        require(len(self.reclamation_proxy_handles)<2,'bounded reclamation proxy count')
        proxy=self.proxy_process(argv,input_pipe=True)
        self.reclamation_proxy_handles.append(proxy)
        recorded=dict(pid=proxy.process.pid,argv=argv,status='running')
        self.resources.setdefault('reclamation_proxies',[]).append(recorded)
        try:
            self.record_resources()
            save(output/'proxy-process.json',dict(argv=argv,pid=proxy.process.pid,source_sha256=digest(path)))
            ready=self.proxy_event(proxy,'ready',10)
            require(ready['source_sha256']==digest(path) and ready['target']=='control-closed-reclamation'
                and ready['upstream_port']==15432,'declared reclamation proxy identity')
            save(output/'proxy-ready.json',ready)
            self.config.write_text(self.set_value(original,'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            entry=self.launch('reclaim-commit-'+mode,'app','ingest','recover','--reclaim-initial','--file','/etc/bigbrotr/instance.toml')
            fault=self.wait_reclamation_fault(proxy,entry,output);save(output/'proxy-fault.json',fault)
            ended=self.docker('wait',entry['id'],timeout=15)
            info=self.inspect(entry);logs=self.docker('logs',entry['id'])
            (output/'serve.stdout').write_text(logs.stdout);(output/'serve.stderr').write_text(logs.stderr)
            require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
                and not info['State']['OOMKilled'] and logs.stdout==''
                and logs.stderr=='error: producer operation unavailable: CommitUnknown\n',
                'lost reclamation confirmation must report CommitUnknown without unlink')
            require(fault['faults']==1 and fault['client_complete']==0
                and fault['server_complete']==int(mode=='after-complete'),'actual reclamation COMMIT direction differs')
        finally:
            terminal=self.finish_reclamation_proxy(proxy,original,output,recorded)
        self.backend_absence(output)
        after,after_catalog=self.capture(output,'after')
        oracle.control_only(before,after,claim='failed',backend_pid=fault['backend_pid'])
        oracle.reclamation_fault(fault,terminal,mode)
        index.exact(catalog,after_catalog)
        index.exact(native_before,self.native_capture('commit-'+mode+'-native-after'))
        require(self.files(self.state/'data/ingest/producer-inputs-v1')==inputs,'unknown COMMIT unlinked originals')
        self.wait(lambda:self.sql('bigbrotr_control',"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active' AND lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint")=='0',
            'unknown reclamation Control lease expiry',65)
        save(output/'verdict.json',dict(status='PASS',mode=mode,reported='CommitUnknown',native='exact',inputs='exact',success_record=False))

    def closed_reclamation(self):
        self.source_unchanged()
        self.reclamation_output=self.root/'reclamation';self.reclamation_output.mkdir(mode=0o700)
        self.reclaimed_archive_files=self.files(self.state/'data/archive')
        self.refused_reclamation('refuse-open-producer')
        args=['ingest','recover','--file','/etc/bigbrotr/instance.toml']
        self.finite('reclamation-abandon-initial',*args,'--abandon-acquisition')
        retired=self.finite('reclamation-retire-initial',*args,'--retire-initial')
        before,catalog=self.capture(self.reclamation_output,'retired')
        self.restart_checks.retired(before,retired,1,self.retained_science,cut=312)
        require(not any(before[t] for t in ('initial_ingest_successions','initial_ingest_activations','ingest_successions','ingest_activations')),
            'reclamation must precede every successor intent')
        native_before=self.native_capture('retired-native')
        oracle.retired_initial_native(self.retained_science,native_before)
        inputs=self.state/'data/ingest/producer-inputs-v1'
        require(len(self.files(inputs))==64,'reclamation requires64 genuine original inputs')
        # Corruption remains an explicit reversible fixture on this excluded
        # retired owner. It is never passed as an authenticated deletion candidate.
        path=sorted(inputs.iterdir())[0];original=path.read_bytes()
        path.write_bytes(bytes([original[0]^1])+original[1:])
        try:self.refused_reclamation('refuse-corrupt-original')
        finally:path.write_bytes(original)
        for mode in ('before-forward','after-complete'):
            self.reclamation_commit_loss(mode)
        before,catalog=self.capture(self.reclamation_output,'before-reclaim')
        report=self.finite('reclaim-initial',*args,'--reclaim-initial')
        self.reclaimed,after_catalog=self.capture(self.reclamation_output,'after-reclaim')
        self.reclaimed_native=self.native_capture('reclaimed-native')
        oracle.reclaimed(before,self.reclaimed,native_before,self.reclaimed_native,self.retained_science,report)
        require(self.files(inputs)=={},'authenticated originals were not reclaimed')
        index.exact(catalog,after_catalog);self.preserve_reclaimed_files()
        self.retained_science['compacted_native']=self.reclaimed_native
        retry=self.finite('reclaim-initial-restart-retry',*args,'--reclaim-initial')
        after,_=self.capture(self.reclamation_output,'reclaim-retry')
        retry_native=self.native_capture('retry-native')
        oracle.reclaimed(self.reclaimed,after,self.reclaimed_native,retry_native,self.retained_science,retry,retry=True)
        restart=self.finite('reclaimed-retirement-restart',*args,'--retire-initial')
        require(restart['resolved_requests']=='0' and restart['compacted_requests']=='64'
            and restart['recovery_basis']=='closed-producer-floor-v3','restart must select genuine compacted evidence')
        require(self.files(inputs)=={},'restart recreated original inputs')
        self.preserve_reclaimed_files()
        self.mixed_checkpoint_restore_refusal()
        save(self.reclamation_output/'reclamation-verdict.json',dict(status='PASS',requests=64,original_files=0,
            successor_intent=False,science='exact',pending='exact',postgres='preserved',archive='preserved',retry='unchanged'))

    def mixed_checkpoint_restore_refusal(self):
        output=self.reclamation_output/'mixed-checkpoint-restore';output.mkdir(mode=0o700)
        database='reclaim_restore_'+self.owner
        clone=self.state/'data/reclamation-restore/ingest'
        clone.parent.mkdir(mode=0o700)
        self.backend_absence(output)
        shutil.copytree(self.state/'data/ingest',clone)
        self.sql('postgres','CREATE DATABASE '+database+' TEMPLATE bigbrotr_control')
        old=self.before_proof['producer_checkpoints'][0]
        require(old['checkpoint_revision']==64 and old['complete_through_request']==64,
            'actual earlier checkpoint capture missing')
        digest_value=index.raw(old['checkpoint_digest'],32).hex()
        statement=("BEGIN; SET LOCAL session_replication_role=replica; UPDATE control.producer_checkpoints "
            "SET checkpoint_revision=64,checkpoint_digest=decode('"+digest_value+"','hex') WHERE producer_epoch=1; COMMIT;")
        # One explicitly incompatible partial restore on an owned copy. It does
        # not rewrite the successful original's PG history or any native bytes.
        self.sql(database,statement)
        slot=index.raw(old['producer_slot_id'],16).hex()
        typed_refusal=("DO $proof$ BEGIN BEGIN PERFORM control.require_retained_producer_progress(decode('"+slot+"','hex'),1); "
            "RAISE EXCEPTION 'mixed typed head unexpectedly accepted'; EXCEPTION WHEN check_violation THEN "
            "IF SQLERRM <> 'retained producer progress is not a validated typed head' THEN RAISE; END IF; END; END $proof$;")
        self.sql(database,typed_refusal)
        save(output/'typed-head-refusal.json',dict(sqlstate='23514',message='retained producer progress is not a validated typed head',
            fixture='older_head_with_newer_progress_children',direct_floor_comparison='NOT_REACHED'))
        self.resources['reclamation_restore_fixture']=dict(database=database,native_root=str(clone),
            kind='post-reclamation-native-with-actual-older-typed-head',source_checkpoint_revision=64,status='retained')
        self.record_resources()
        before=self.rows({'postgres':self.pg['id']},database);save(output/'before-control.json',before)
        native_before=self.native_capture('mixed-native-before',store=clone)
        index.exact(native_before,self.reclaimed_native)
        original=self.config.read_text()
        try:
            changed=self.set_value(original,'postgres.control','database',database)
            changed=self.set_value(changed,'storage','ingest_root','/var/lib/bigbrotr/reclamation-restore/ingest')
            self.config.write_text(changed)
            entry=self.launch('mixed-checkpoint-refusal','app','ingest','recover','--reclaim-initial','--file','/etc/bigbrotr/instance.toml')
            ended=self.docker('wait',entry['id'],timeout=120)
            info=self.inspect(entry);logs=self.docker('logs',entry['id'])
            (output/'serve.stdout').write_text(logs.stdout);(output/'serve.stderr').write_text(logs.stderr)
            require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
                and not info['State']['OOMKilled'],
                'mixed restored checkpoint must refuse without success')
            disposition=oracle.reclamation_refusal('mixed-checkpoint-refusal',logs.stdout,logs.stderr)
        finally:self.config.write_text(original)
        require(self.sql(database,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()')=='0',
            'mixed fixture backend remains')
        after=self.rows({'postgres':self.pg['id']},database);save(output/'after-control.json',after)
        oracle.control_only(before,after,claim=disposition)
        index.exact(native_before,self.native_capture('mixed-native-after',store=clone))
        require(self.files(clone/'producer-inputs-v1')=={},'mixed restore fabricated old input files')
        index.exact(self.native_capture('mixed-original-preserved'),self.reclaimed_native)
        save(output/'verdict.json',dict(status='PASS',fixture='incompatible_partial_restore',native='exact',original='preserved',success_record=False))

    def compacted_successor(self):
        import subprocess
        import sys
        from cases.first_launch import stop_process
        self.source_unchanged();self.wait_login_capacity(self.reclamation_output)
        # Restore only the declared catalog route after its earlier joined
        # known refusal; the initial accepted scientific policy stays unchanged.
        self.config.write_text(self.set_value(self.config.read_text(),'postgres.catalog','endpoint','127.0.0.1:15432'))
        output=self.reclamation_output/'successor';output.mkdir(mode=0o700)
        home=self.root/'successor-client';home.mkdir(mode=0o700);self.successor_home=home
        for name in ('event.jsonl','fixture-profile.json','visibility.json'):
            shutil.copyfile(self.home/name,home/name)
        shutil.copyfile(self.source/'tests/fixtures/acquisition/selector-events.json',home/'selector-events.json')
        self.new_event=next(row['event'] for row in load(home/'selector-events.json')['events'] if row['name']=='included')
        require(self.files(self.state/'data/ingest/producer-inputs-v1')=={},'spool must be empty before activation')
        prebridge,_=self.capture(output,'prebridge')
        topology.unchanged_index(self.initial_index_state,prebridge)
        entry=self.launch('reclaimed-successor','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--activate-initial-successor','--roles','control,ingest,sync,index')
        started=self.wait(lambda:self.logs(entry,output),'compacted successor activation and retained Index bridge',60,entry=entry)[0]
        require(started['state']=='activated' and started['producer_epoch']=='2'
            and started['predecessor']['resolved_requests']=='0' and started['predecessor']['compacted_requests']=='64',
            'successor did not authenticate the compacted predecessor')
        self.compacted_started=started
        bridge,_=self.capture(output,'bridge')
        generation=live_oracle.retained_bridge(prebridge,bridge,self.state/'data/index',retained_boundary=True)
        live_oracle.startup_index(started['index'],generation,retained_boundary=True)
        index.exact(self.custody(),self.custody_before)
        self.compacted_bridge=bridge
        with (home/'client.stdout').open('x') as out,(home/'client.stderr').open('x') as err:
            client=subprocess.Popen([sys.executable,'-B',str(self.source/'experiments/ingest-bootstrap/installed_live_client.py'),
                'journey',str(home),'--profile',str(home),'--expected-config-revision','1','--expected-policy-revision','1','--live-index-proof'],
                env=self.environment,cwd=self.root,stdout=out,stderr=err,start_new_session=True)
        self.processes.append(client)
        try:
            self.wait(lambda:(home/'prepared.json').exists(),'successor source preparation',15,entry,client)
            (home/'prepared.continue').write_text('compacted activated owner independently observed\n')
            self.wait(lambda:(home/'submitted.json').exists(),'successor signed admission',15,entry,client)
            submitted=load(home/'submitted-phase.json')
            submitted_deadline=live_oracle.phase_deadline(submitted,'submitted',time.monotonic_ns())
            self.phase_wait(lambda:self.sql('bigbrotr_control','SELECT count(*) FROM control.acquisition_progress WHERE producer_epoch=2 AND classified_events=3')=='1',
                submitted_deadline,entry,client)
            admitted,_=self.capture(output,'admitted')
            initial=index.only(admitted['producer_epochs'],producer_epoch=1)
            current=index.only(admitted['producer_epochs'],producer_epoch=2)
            require(initial['reserved_through_request']==64 and current['reserved_through_request']==3,'cumulative request65 must exist')
            directory=self.state/'data/ingest/producer-inputs-v1'
            files=self.files(directory)
            save(output/'admitted-input-bytes.json',compacted_spool_sizes(directory,files,self.files))
            require(self.inspect(entry)['State']['Running'],'successor owner exited before submitted release')
            self.phase_guard(submitted_deadline,client)
            (home/'submitted.continue').write_text('three genuine new requests checked; independent Index phase follows\n')
            self.phase_guard(submitted_deadline,client)
            self.phase_wait(lambda:(home/'index-proof.json').exists(),time.monotonic_ns()+5_000_000_000,entry,client)
            phase=load(home/'index-proof-phase.json')
            deadline=live_oracle.phase_deadline(phase,'index-proof',time.monotonic_ns())
            self.phase_wait(lambda:self.sql('bigbrotr_control',"SELECT count(*) FROM control.consumers WHERE consumer_role='index' AND current_revision=68")=='1',
                deadline,entry,client)
            live,_=self.capture(output,'live-complete-publication')
            oracle.live_compacted_publication(live)
            live_oracle.append_only_history(self.compacted_bridge,live)
            require(self.inspect(entry)['State']['Running'],'successor owner exited before Index phase release')
            self.phase_guard(deadline,client)
            (home/'index-proof.continue').write_text('Index68 checked; Ready65 is not required\n')
            self.phase_guard(deadline,client)
            self.phase_wait(lambda:(home/'index-proof-phase-released.json').exists(),deadline,entry,client)
            release=load(home/'index-proof-phase-released.json')
            live_oracle.phase_release(phase,release,self.phase_guard(deadline,client))
            require(self.inspect(entry)['State']['Running'],'successor owner exited before Index phase verdict')
            save(output/'live-publication-phase.json',dict(phase=phase,release=release,
                parent_completed_monotonic_ns=self.phase_guard(deadline,client),ready65='NOT_CLAIMED'))
            self.wait(lambda:(home/'commands-finished.json').exists(),'successor client logout',15,entry,client)
            self.wait(lambda:any(load(p)['number']>=2 and not p.with_name(p.name.replace('open','closed')).exists()
                for p in home.glob('relay-open-*.json')),'successor quiet receive',5,entry,client)
            terminal=self.stop_container(entry,output)
            require(terminal['State']['ExitCode']==0,'compacted successor failed to join')
            (home/'commands-finished.continue').write_text('actual compacted successor joined\n')
            require(client.wait(timeout=6)==0 and not (home/'client.stderr').read_bytes(),'successor signed client failed')
        finally:
            stop_process(client);self.processes.remove(client)
        self.backend_absence(output)
        self.successor_wires=[load(p) for p in sorted(home.glob('relay-closed-*.json'))]
        args=['ingest','recover','--file','/etc/bigbrotr/instance.toml']
        self.finite('compacted-abandon-successor',*args,'--abandon-activated-acquisition')
        self.finite('compacted-retire-successor',*args,'--retire-activated-initial')
        self.retired_successor,_=self.capture(output,'retired')
        live_oracle.publication_unchanged(live,self.retired_successor)
        self.final_native=self.native_capture('successor-native')
        self.final_science=oracle.successor_science(self.reclaimed,self.retired_successor,started,self.retained_science,
            self.successor_wires,self.final_native,self.state/'data/ingest',time.time_ns()//1000)
        self.all_input_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        self.complete_index_state=self.retired_successor
        self.refused_reclamation('refuse-reclamation-after-successor')
        save(output/'reuse-verdict.json',dict(status='PASS',producer_requests=[64,3],cumulative65='genuine_epoch2_request1',
            shared_files=3,shared_file_limit=64,shared_byte_limit=32*1024*1024,index_revision=68,ready65='NOT_CLAIMED'))

    def compacted_consumers(self):
        output=self.reclamation_output/'consumers';output.mkdir(mode=0o700)
        # Finite Index verifies the already-published live compacted history;
        # neither exact retry may add physical rows or a publication revision.
        for owner_epoch,label in enumerate(('index-compacted-catch-up','index-compacted-exact-retry'),3):
            report=self.finite(label,'index','catch-up','--file','/etc/bigbrotr/instance.toml')
            require(report['published_revision_after']==68 and report['new_batches']==0,'finite compacted Index changed complete history')
            state,_=self.capture(output,label)
            rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
            live_oracle.no_work_rebind(self.complete_index_state,state,report,self.final_science,self.schema,rows,
                owner_epoch=owner_epoch,retained_boundary=True)
            live_oracle.append_only_history(self.compacted_bridge,state)
            index.exact(self.custody(),self.custody_before)
            self.complete_index_state=state
            save(output/(label+'-rows.json'),rows)
            require(self.files(self.state/'data/ingest/producer-inputs-v1')==self.all_input_files,'Index changed retained inputs')
        report=self.finite('archive-compacted-catch-up','archive','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.caught_up,self.caught_up_catalog=self.capture(output,'archive')
        live_oracle.publication_unchanged(self.complete_index_state,self.caught_up)
        require(report['terminal_epoch']==2 and report['frontier_revision']==5 and report['snapshot_id']==68
            and report['compacted_requests']=='64' and report['family_counts']==[73,62,62,62,63],
            'compacted epoch2 Archive publication differs')
        self.final_census=self.census(output,'complete')
        self.restart_checks.publication(self.caught_up,self.final_census,self.final_science,
            revision=5,snapshot=68,topology=1,cuts=[(1,312),(2,10)])
        self.caught_up_files=self.files(self.state/'data/archive')
        retry=self.finite('archive-compacted-exact-retry','archive','catch-up','--file','/etc/bigbrotr/instance.toml')
        after,catalog=self.capture(output,'archive-retry')
        live_oracle.publication_unchanged(self.complete_index_state,after)
        require(retry['new_attempts']==0 and retry['new_frontier_revisions']==0,'Archive retry added work')
        self.restart_checks.retry_unchanged(self.caught_up,after)
        index.exact(catalog,self.caught_up_catalog)
        require(self.files(self.state/'data/archive')==self.caught_up_files,'Archive retry changed objects')
        index.exact(self.native_capture('consumers-native'),self.final_native)
        require(self.files(self.state/'data/ingest/producer-inputs-v1')==self.all_input_files,'consumers recreated originals')

    def compacted_history_export(self):
        import secrets
        output=self.reclamation_output/'history-export';output.mkdir(mode=0o700)
        self.wait_login_capacity(output)
        destination=secrets.token_hex(16)
        changed=self.set_value(self.config.read_text(),'archive.reader','enabled',True)
        changed=self.set_value(changed,'export.destination','id',destination)
        changed=self.set_value(changed,'export.destination','root','/var/lib/bigbrotr/export')
        self.config.write_text(changed)
        self.finite('reclaimed-destination-initialize','export','destination-initialize','--file','/etc/bigbrotr/instance.toml')
        home=output/'export';home.mkdir(mode=0o700)
        for name in ('event.jsonl','fixture-profile.json'):
            shutil.copyfile(self.home/name,home/name)
        (home/'destination').mkdir(mode=0o700)
        shutil.copyfile(self.state/'data/export/destination.json',home/'destination/destination.json')
        save(home/'export-input.json',dict(scope=self.initialized['private_scope'],catalog_id=self.ids['catalog'],
            destination_id=destination,retained_compacted_epoch2=True,startup_quiescence=True))
        before,_=self.capture(output,'before')
        entry=self.launch('compacted-signed-export','app','serve','--file','/etc/bigbrotr/instance.toml','--roles','control')
        self.wait(lambda:self.logs(entry,output),'compacted ordinary Control',20,entry=entry)
        baseline.startup_client(self,output,entry,client_home=home,retained_compacted_epoch2=True)
        require(self.stop_container(entry,output)['State']['ExitCode']==0,'export owner did not join')
        after,_=self.capture(output,'export-after')
        live_oracle.publication_unchanged(self.complete_index_state,after)
        verdict=self.export_validate(before,after,home,self.initialized,self.ids['catalog'],expected_principal=self.enrolled['principal_id'],
            expected_table_count=82,expected_tables=self.tables,destination_root=self.state/'data/export',initial_cut=312,retained_compacted_epoch2=True)
        require(verdict['jsonl_bytes']==353,'complete original signed export bytes differ')
        save(output/'export-verdict.json',verdict)
        # A second fresh process proves ordinary signed history of both epochs.
        history=output/'history';history.mkdir(mode=0o700)
        shutil.copyfile(self.home/'fixture-profile.json',history/'fixture-profile.json')
        save(history/'events.json',[self.events[0],self.new_event])
        save(history/'read-input.json',dict(scope=self.initialized['private_scope'],catalog_id=self.ids['catalog'],
            frontier_revision=5,snapshot_id=68,config_revision='1',policy_revision='1',startup_quiescence=True))
        self.capture(history,'before')
        entry=self.launch('compacted-history-restart','app','serve','--file','/etc/bigbrotr/instance.toml','--roles','control')
        self.wait(lambda:self.logs(entry,output),'fresh compacted history Control',20,entry=entry)
        baseline.startup_client(self,history,entry,successor=True,client_home=history,retained_compacted_epoch2=True)
        require(load(history/'history-client-result.json')['status']=='PASS','fresh signed histories differ')
        require(self.stop_container(entry,output)['State']['ExitCode']==0,'history owner did not join')
        final,_=self.capture(output,'history-after')
        live_oracle.publication_unchanged(self.complete_index_state,final)
        require(self.files(self.state/'data/archive')==self.caught_up_files,'signed read/export changed Archive objects')
        index.exact(self.native_capture('history-native'),self.final_native)
        require(self.files(self.state/'data/ingest/producer-inputs-v1')==self.all_input_files,'history/export recreated originals')
        self.source_unchanged()
