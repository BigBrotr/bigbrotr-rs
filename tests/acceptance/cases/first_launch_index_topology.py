"""One installed composition: signed source, Index, successor, catch-up and retry."""
from pathlib import Path

from cases.first_launch import digest, load, require, save
from cases.first_launch_index import IndexRuntime, DATABASE, STAGES as INITIAL_STAGES
from cases.first_launch_index_restart import RestartIndexRuntime
from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology_oracle as oracle

STAGES = INITIAL_STAGES[:-1]+('restart_baseline','ingest_restart_resend',
    'archive_catch_up','catch_up_exact_retry','index_epoch2_catch_up',
    'index_epoch2_exact_retry','successor_history','cleanup')
LIMITS = [
    'One fresh enabled-Index composition publishes the initial signed event, then reuses the maintained epoch2 successor/resend, Archive recovery and independent source/C API science journey.',
    'Finite Index catch-up must verify the actual topology0→1 bridge, preserve original generation/storage/table identities and historical receipts, publish all epoch2 batches and make exact retry add no work.',
    'The unchanged successor signed fixture contributes one t=allowed posting. Its predecessor resend contributes no second original body, eligibility or receive record; all raw typed rows and multiplicities are compared.',
    'The initial physical-conflict negative remains its separate case. This case retains positive initial readiness and the original ordinary40s/export12s gates without injecting corrupt data into the successor input.',
    'NOT_RUN: continuous combined successor Index ownership, general topology recovery, unknown remote-query resolution, genuine unknown native termination, independent service outages/capacity, second-event export and complete M1.',
]


class TopologyIndexRuntime(IndexRuntime):
    inject_content_conflict = False
    ordinary_client = IndexRuntime.baseline_client
    custody = RestartIndexRuntime.custody

    def load_helpers(self):
        super().load_helpers()
        import index_publication_runtime
        self.handoff_sql=index_publication_runtime

    def backend_absence(self, output):
        # Keep each finite owner's diagnostic separate without binding super()
        # to the unrelated paused-prefix runtime class.
        observed=output/('backend-absence-'+str(self.number))
        observed.mkdir(mode=0o700)
        super().backend_absence(observed)

    def candidate(self):
        for path in (Path(__file__),Path(oracle.__file__)):
            require(digest(path)==digest(self.source/'tests/acceptance/cases'/path.name),
                    'Index topology acceptance source mismatch')
        super().candidate()
        sql='experiments/control-migrations/index_handoff_cases.sql'
        self.inputs['acceptance_files_sha256'][sql]=digest(self.source/sql)

    def verify_index(self):
        super().verify_index()
        self.initial_index_state=load(self.root/'index/after-control.json')
        self.initial_index_custody=self.custody()
        self.initial_index_schema=load(self.root/'initial/clickhouse-schema.json')

    def restart_ingest(self):
        super().restart_ingest()
        oracle.unchanged_index(self.initial_index_state,self.retired_successor)
        index.exact(self.custody(),self.initial_index_custody)

    def successor_abandoned(self):
        self.handoff_sql.run_handoff_clone(self,self.restart_output/'index-handoff-nonterminal',phase='nonterminal')

    def restart_archive(self):
        super().restart_archive()
        oracle.unchanged_index(self.initial_index_state,self.caught_up)
        index.exact(self.custody(),self.initial_index_custody)

    def index_preserved(self,label):
        self.backend_absence(self.topology_output)
        require(self.catalog_rows({'postgres':self.pg['id']},'bigbrotr_catalog')==self.caught_up_catalog,
                'Index topology catch-up changed Archive catalog')
        for folder,expected in (('archive',self.caught_up_files),('export',self.original_export_files),
                                ('ingest/producer-inputs-v1',self.all_input_files)):
            require(self.files(self.state/'data'/folder)==expected,'Index topology catch-up changed '+folder)
        index.exact(self.custody(),self.initial_index_custody)
        tables=self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name",rows=True)
        columns=self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position",rows=True)
        index.exact(dict(tables=tables,columns=columns),self.initial_index_schema)
        save(self.topology_output/(label+'-preservation.json'),dict(archive='unchanged',original_export='unchanged',
            original_inputs='unchanged',index_custody='unchanged',clickhouse_schema_and_uuids='unchanged'))

    def index_epoch2_catch_up(self):
        self.source_unchanged()
        self.topology_output=self.root/'index-topology';self.topology_output.mkdir(mode=0o700)
        self.handoff_sql.run_handoff_clone(self,self.topology_output/'sql-constraints',phase='terminal')
        before,_=self.capture(self.topology_output,'before')
        oracle.unchanged_index(self.initial_index_state,before)
        report=self.finite('index-epoch2-catch-up','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.topology_after,_=self.capture(self.topology_output,'caught-up')
        self.topology_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.topology_output/'clickhouse-caught-up.json',self.topology_rows)
        result=oracle.catch_up(before,self.topology_after,report,self.final_science,
            self.schema,self.state/'data/index',self.topology_rows)
        save(self.topology_output/'caught-up-verdict.json',result)
        self.inputs['index_topology']=result
        self.index_preserved('caught-up')

    def index_epoch2_retry(self):
        report=self.finite('index-epoch2-retry','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        after,_=self.capture(self.topology_output,'retry')
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.topology_output/'clickhouse-retry.json',rows)
        result=oracle.catch_up(self.topology_after,after,report,self.final_science,
            self.schema,self.state/'data/index',rows,retry=True)
        index.exact(rows,self.topology_rows)
        save(self.topology_output/'retry-verdict.json',result)
        self.index_preserved('retry')

