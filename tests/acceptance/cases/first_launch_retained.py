"""Installed finite64 saturation with the independently parked Archive owner."""
import json
from pathlib import Path
import shutil
import time

from cases.first_launch import MissingPrerequisite, digest, load, require, save
from cases.first_launch_archive_outage import ArchiveOutageRuntime, STAGES
from cases.first_launch_index_query_trace import QueryTraceRuntime, query_trace_selection
from cases import first_launch_index_oracle as index
from cases import first_launch_retained_oracle as oracle
from cases import first_launch_retained_native as native

LIMITS = [
    'Fresh actual source3/Archive2/Index2 prefix under the original15s gate; only the catalog route is then closed and joined. Control, Sync and Index retain their actual owners.',
    'Writer-only lifecycle logging retains the existing1024-row queue/512-row flush threshold/7500ms interval and writer quota1. After independently verified prefix readiness, one ClickHouse clock query records a start-time cutoff before catalog fault/source release under the same15s deadline. After actual joins, the8192-row/4MiB/30s collector applies that cutoff to both count and rows. Later64-workload queries are excluded; incomplete pairs stay diagnostic INCOMPLETE. All-query coverage and remote termination/exclusion remain NOT_PROVEN; this does not close the earlier Code202 cause.',
    'Two bootstrap requests plus one source-dictionary request leave61 event mutation slots. The frozen62 signed events use public fixture keys1/3. Twenty quanta send3 new events; quantum21 sends event61, eligible event62 and proofevent1. The actual prefix must stop at frame1 before any later proof is released.',
    'One independently gated proof-only REQ then advances classification61→62 and checkpoint64→65, with complete64, source312 and all64 original requests unchanged. Up to100 legitimate emptyEOSE quanta follow or wait at the gate; later quanta are quiet. Original128 connections,3s socket,500ms product receive and65s submitted bounds remain; no issued input is retried.',
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
