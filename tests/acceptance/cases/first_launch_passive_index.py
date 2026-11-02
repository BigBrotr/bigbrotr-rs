"""The existing passive journey with actual initial ClickHouse publication."""
import hashlib
from pathlib import Path

from cases.first_launch import digest, load, require, save
from cases.first_launch_index import DATABASE, IndexRuntime
from cases.first_launch_passive_monitor import PassiveMonitorRuntime, STAGES
from cases import first_launch_index_oracle as index
from cases import first_launch_passive_oracle as passive
from cases import first_launch_passive_index_oracle as oracle

LIMITS = [
    'One fresh operator-enrolled passive attempt, four indivisible source batches, Archive cut10 and all eleven schema2 Index outcomes, including one complete typed compound measurement row.',
    'The original passive peer, admission, Archive, same-data PostgreSQL restart and standalone typed inspection checks remain; exact ClickHouse fields and all44 target commitments use separate independent passive expectations.',
    'No Index/ClickHouse restart, readiness/corruption/outage, unknown dispatch recovery, native census, repeated scheduling, fleet or capacity qualification. The configured six-row ceiling admits the unchanged six-dictionary source batch; it is not a measured capacity claim.',
]


class PassiveIndexRuntime(PassiveMonitorRuntime, IndexRuntime):
    """Cooperative setup reuses the maintained engine owner, not its event oracle."""
    def candidate(self):
        for module in (Path(__file__), Path(oracle.__file__)):
            require(digest(module) == digest(self.source/'tests/acceptance/cases'/module.name),
                'passive Index acceptance source mismatch')
        super().candidate()

    def prepare_state(self):
        # IndexRuntime provisions credentials/config, then PassiveMonitorRuntime
        # selects its existing source. Restore only the additional local role.
        super().prepare_state()
        text = self.set_value(self.config.read_text(),'topology','local_roles',
            ['control','ingest','monitor','archive','index'])
        self.config.write_text(self.set_value(text,'indexer','batch_max_rows',oracle.BATCH_MAX_ROWS))
        self.inputs['initial_recipe_sha256'] = digest(self.config)

    def passive_launch(self):
        return self.launch('passive-monitor-index','index-initialize','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,monitor,archive,index','--initialize-ingest','--initialize-index',
            '--monitor-once','/var/lib/bigbrotr/monitor-once.json')

    def passive_source_completed(self, output):
        # A diagnostic alone cannot justify waiting for publication. Prove the
        # exact four known PG outcomes and the peer's terminal socket boundary.
        admitted, _ = self.capture(output,'admitted')
        passive.completed(self.report,admitted,self.initialized,self.selection)
        oracle.accepted_profile(admitted)
        oracle.source_completed(admitted,self.report,self.initialized,self.decode)
        require(load(self.home/'relay-complete.json')['connections'] == 1,
            'passive source did not finish exactly once')

    def passive_publication(self, entry, output, remaining):
        self.wait(lambda:self.sql('bigbrotr_control',
            "SELECT count(*) FROM control.consumer_frontiers f JOIN control.consumers c USING(consumer_id) "
            "WHERE c.consumer_role='index' AND f.frontier_revision=c.current_revision AND f.frontier_revision=4 "
            "AND f.topology_generation=0 AND f.shard_id=0 AND f.owner_epoch=1 AND f.last_complete_sequence=10") == '1',
            'complete passive Index source frontier',remaining(45),entry,self.client)
        self.index_live, _ = self.capture(output,'index-live')
        self.index_rows = self.ch_table_rows()
        save(output/'clickhouse-live.json',self.index_rows)
        self.index_schema = self.schema_capture()
        save(output/'clickhouse-schema.json',self.index_schema)
        require(self.inspect(entry)['State']['Running'], 'passive owner stopped before Index capture')
        # The ordinary writer's actual packaged account must remain DML-only.
        self.ch(f'ALTER TABLE {DATABASE}.measurement_facts ADD COLUMN forbidden UInt8','writer',error='ACCESS_DENIED')
        remaining(1)

    def schema_capture(self):
        tables = self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name",rows=True)
        columns = self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position",rows=True,schema_columns=True)
        index.schema_metadata(self.schema,self.state/'data/index',tables,columns)
        return dict(tables=tables,columns=columns)

    def passive_inspected(self, document, measurement, encoded, output, after_state):
        # Only bounded unpredictable clock scalars parameterize literal expected
        # facts. Every complete preimage must match the original producer receipt.
        science, rows = oracle.expected(self.stopped,self.initialized,self.refs,self.original_inputs,
            self.receipts,document,measurement,encoded,self.index_rows,self.before_us,self.wire['connected_at_us'],
            self.schema,self.cbor,self.decode)
        for state in (self.index_live,self.stopped,after_state):
            oracle.publication(state,self.state/'data/index',science,rows,self.schema)
        for table in index.INDEX_TABLES:
            index.exact(self.index_live[table],self.stopped[table])
            index.exact(after_state[table],self.stopped[table])
        for table in ('consumers','consumer_frontiers','consumer_frontier_revisions'):
            index.exact(after_state[table],self.stopped[table])
        oracle.physical(self.index_rows,rows,self.schema)
        after = self.ch_table_rows()
        oracle.physical(after,rows,self.schema)
        require(after == self.index_rows and self.schema_capture() == self.index_schema,
            'PostgreSQL restart/typed inspection changed ClickHouse publication')
        save(output/'clickhouse-after-inspection.json',after)
        result = dict(status='PASS',source_cut=10,frontier_revision=4,admission_batches=4,
            committed_chunks=44,batch_max_rows=6,schema_version=2,tables=11,columns=185,
            family_counts=dict(zip(index.TARGETS,oracle.COUNTS)),
            canonical_measurement_sha256=hashlib.sha256(encoded).hexdigest(),
            all_typed_fields_equal=True,independent_source_and_receipts=True,
            postgres_restart='PASS',index_restart='NOT_RUN',clickhouse_restart='NOT_RUN',readiness='NOT_RUN')
        save(output/'independent-index-verdict.json',result)
        self.inputs['passive_index'] = result
