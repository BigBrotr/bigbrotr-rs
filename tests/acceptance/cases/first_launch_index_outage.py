"""Actual owned ClickHouse stop while the same source and Archive continue."""
import errno
import json
from pathlib import Path
import socket
import time

from cases.first_launch import MissingPrerequisite, digest, load, require, save
from cases.first_launch_index import IndexRuntime, DATABASE, HTTP_PORT, TCP_PORT
from cases.first_launch_index_restart import RestartIndexRuntime
from cases.first_launch_index_pin import ShortPinRuntime, process_identity
from cases import first_launch_index_oracle as index
from cases import first_launch_index_outage_oracle as oracle
from cases import first_launch_archive_outage_oracle as outage
from cases import first_launch_retained_native as native

LIMITS = [
    'The original15s source3/Archive2/Index2 ready-prefix gate includes the ten-family physical/schema capture and positive stop of only the owned ClickHouse. Both18123/19000 must refuse before post-prefix release.',
    'The original four-request signed client and65s submitted gate remain: source12/Archive4 continues while Index retains revision2/cut3, lag9 and one immutable plan3[4..7]/ten prepared chunks; plan4 is absent.',
    'Authenticated same-owner status reports Index unavailable/dependency_unavailable with its original historical cut/age, without refreshing the session. The signed event arrives during outage; the pre-outage acknowledged prefix is three dictionaries.',
    'After actual writer/backend join, the separately declared exact-source read-only observer checks all49 native rows, four original inputs/receipts, ACK4/retired0. The original40s history/export journey and12s export gate run while ClickHouse remains stopped.',
    'NOT_RUN: backend restoration, automatic recovery, general remote uncertainty resolution, dynamic byte hysteresis, empirical capacity, continuous enabled-Index successor or complete M1. Limits, quotas and query semantics are unchanged.',
]


class IndexOutageRuntime(RestartIndexRuntime):
    inject_content_conflict = False

    def preflight(self):
        super().preflight()
        build=getattr(self.args,'native_observer_build',None)
        if build is None or not all((build/name).is_file() for name in (
                'source-manifest.json','build-result.json','ingest-bootstrap-probe-build.json','ingest_bootstrap_probe')):
            raise MissingPrerequisite('Index outage needs a separately declared exact-source native observer build')

    def load_helpers(self):
        super().load_helpers()
        from fault_cases import Process
        self.proxy_process=Process
        import installed_archive_oracle
        import installed_progress_context_oracle

    def candidate(self):
        for module in (__file__,oracle.__file__,native.__file__):
            path=Path(module)
            require(digest(path)==digest(self.source/'tests/acceptance/cases'/path.name),
                    'Index outage acceptance source mismatch')
        super().candidate()
        self.native_paths,self.native_hashes=native.artifact(self.args.native_observer_build,self.manifest)
        self.inputs['native_observer']=dict(artifact_sha256=self.native_hashes,
            source=load(self.native_paths['source-manifest.json'])['revision'],product_unchanged=True)

    def stopped_backend(self):
        info=self.inspect(self.ch_entry)  # Full identity/labels/image/mount/profile checks.
        require(not info['State']['Running'] and not info['State']['Paused']
            and info['State']['Pid']==0 and info['State']['ExitCode']==0 and not info['State']['OOMKilled'],
            'owned ClickHouse not positively stopped')
        refusals={}
        for port in (HTTP_PORT,TCP_PORT):
            with socket.socket() as connection:
                connection.settimeout(1)
                refusals[str(port)]=connection.connect_ex(('127.0.0.1',port))
            require(refusals[str(port)]==errno.ECONNREFUSED,'stopped ClickHouse endpoint is not refused')
        return dict(id=info['Id'],state=info['State'],refusal_errno=refusals)

    def establish_prefix_fault(self, entry, output):
        self.parent=process_identity(self.inspect(entry)['State']['Pid'])
        require(self.parent is not None,'actual prefix application process')
        self.prefix_inputs=self.files(self.state/'data/ingest/producer-inputs-v1')
        require(len(self.prefix_inputs)==2,'prefix must have exactly two acknowledged dictionary inputs')
        save(output/'index-outage-prefix-inputs.json',self.prefix_inputs)
        self.prefix_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(output/'index-outage-prefix-physical.json',self.prefix_rows)
        tables=self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name",rows=True)
        columns=self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position",rows=True)
        index.schema_metadata(self.schema,self.state/'data/index',tables,columns)
        save(output/'index-outage-prefix-schema.json',dict(tables=tables,columns=columns))
        before_us=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        self.prefix_timing=dict(before_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
        self.prefix_roles=ShortPinRuntime.roles(self,self.prefix_deadline,session_path=self.home/'session.private')
        self.prefix_timing['after_ns']=time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        after_us=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        index.live_roles(self.prefix_roles,self.prefix,before_us,after_us,revision=2,sequence=3)
        save(output/'index-outage-prefix-age.json',dict(roles=self.prefix_roles,timing=self.prefix_timing))
        self.stop_backend(output)
        return 'positive complete prefix checked; owned ClickHouse stopped with both endpoints refused'

    def stop_backend(self, output):
        info=self.inspect(self.ch_entry)
        require(info['State']['Running'] and not info['State']['Paused'],'owned ClickHouse must be running before stop')
        self.resources['index_backend_stop']=dict(id=info['Id'],status='stopping')
        self.record_resources()
        # The existing outer prefix deadline also bounds Docker's stop command.
        self.docker('stop','--time','5',info['Id'],timeout=10)
        observed=self.stopped_backend()
        self.resources['index_backend_stop'].update(status='stopped-retained')
        self.record_resources();save(output/'index-backend-stopped.json',observed)
        return observed

    def initial_publication(self, entry, output):
        observations=[];last=0
        deadline=time.monotonic()+45  # Existing Index observation bound; client65s remains independent.
        before_us=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        def observed():
            nonlocal last
            if time.monotonic()-last<.25:
                return False
            last=time.monotonic()
            require(len(observations)<256,'Index outage observation bound')
            timing=dict(before_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
            value=ShortPinRuntime.roles(self,deadline,session_path=self.home/'session.private')
            timing['after_ns']=time.clock_gettime_ns(time.CLOCK_BOOTTIME)
            observations.append(dict(roles=value,timing=timing));save(output/'index-outage-roles.json',observations)
            caps=value['capabilities']
            return len(caps)==4 and all(c['state']=='ready' for c in caps[:3]) and (
                caps[-1]['state'],caps[-1]['reason'])==('unavailable','dependency_unavailable')
        self.wait(observed,'Index unavailable with independent source/Archive',45,entry,self.client)
        after_us=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        self.index_live,_=self.capture(output,'index-outage-live')
        oracle.live(self.prefix,self.index_live,self.state/'data/index')
        prior=index.only(self.prefix['owner_epochs'],role='control',state='active')
        current=index.only(self.index_live['owner_epochs'],role='control',state='active')
        require(current['lease_expires_at_us']>prior['lease_expires_at_us'],'same Control lease did not renew')
        value=observations[-1]
        index.live_roles(value['roles'],self.index_live,before_us,after_us,index_unavailable=True)
        outage.retained_age(self.prefix_roles,value['roles'],self.prefix_timing,value['timing'],capability='index_publication')
        require(process_identity(self.inspect(entry)['State']['Pid'])==self.parent,'source Control process changed')
        index.exact(self.custody(),self.custody_before)
        save(output/'index-outage-live-verdict.json',dict(status='PASS',source_through=12,archive_revision=4,
            index_revision=2,index_through=3,lag_records=9,prepared_plan=3,prepared_range=[4,7],
            unplanned_range=[8,12],control='same_owner_renewed',session_idle='unchanged',
            historical_observation='original_age_preserved',clickhouse=self.stopped_backend()))

    def verify_index(self):
        self.source_unchanged()
        output=self.root/'index';output.mkdir(mode=0o700)
        after,_=self.capture(output,'after')
        clock=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        outage.terminal_preservation(self.index_live,after,clock,archive_renewal=True)
        before=load(self.root/'initial/before-control.json')
        view=self.current_owner_view(after,dict(control=before,resource=dict(catalog_id=self.ids['catalog'])))
        owner=index.only(view['owner_epochs'],role='control')
        census=self.census(output,'published')
        wires=[load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        science=self.science_checks.first_science(view,self.initialized,load(self.home/'event.jsonl'),
            wires,census,self.state/'data/ingest',owner['released_at_us'])
        self.science_checks.publication(view,census,science,revision=4,snapshot=5,topology=0,cuts=[(1,12)])
        dump=native.census(self,output);save(output/'native-state.json',dump)
        oracle.native_rows(dump,science)
        inputs=self.files(self.state/'data/ingest/producer-inputs-v1')
        require(len(inputs)==4 and all(inputs.get(k)==v for k,v in self.prefix_inputs.items()),
                'already acknowledged dictionary input bytes changed')
        result=oracle.publication(after,self.prefix_rows,science,self.schema,self.state/'data/index')
        index.exact(self.custody(),self.custody_before)
        final=[json.loads(line) for line in (self.root/'initial/serve.stdout').read_text().splitlines()][-1]
        require(final['index']['published_revision']=='2'
            and final['index']['complete_through']==[dict(shard_id=0,owner_epoch='1',sequence='3')]
            and final['index']['readiness']=='not-qualified','Index terminal diagnostic differs')
        save(output/'backend-still-stopped.json',self.stopped_backend())
        save(output/'verdict.json',result);self.inputs['index']=result
        self.source_unchanged()

    def ordinary(self):
        self.stopped_backend()
        super().ordinary()
        save(self.root/'ordinary/index-backend-still-stopped.json',self.stopped_backend())

    ordinary_client = IndexRuntime.baseline_client

    def cleanup(self):
        try:
            native.stop(self)
        except Exception as error:
            self.resources['cleanup_errors'].append(dict(resource='native-observer',error=type(error).__name__))
        finally:
            super().cleanup()
