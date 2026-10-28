"""Owned paused-engine backlog, same-store Index catch-up and exact retry.

This closed variant shares the signed client, source science and ten-family
oracle. It does not exercise unknown native termination or a lost COMMIT.
"""
import http.client
import json
from pathlib import Path
import time
import tomllib

from cases.first_launch import Runtime, digest, load, require, save
from cases.first_launch_index import IndexRuntime, DATABASE, input_identity
from cases import first_launch_index_oracle as index
from cases import first_launch_index_restart_oracle as oracle

STAGES = ('prerequisites','candidate','runtime_image','fresh_bootstrap','signed_acquisition',
          'index_restart_baseline','index_catch_up','index_exact_retry','index_publication',
          'ordinary_history_export','cleanup')
LIMITS = [
    'The fresh restart fixture declares archive.max_publish_delay=1s before initialization and checks both accepted revision1 documents; this dispatch cadence does not extend the prefix deadline.',
    'The signed client stops after enrollment at source3/Archive2/Index2; complete current physical readiness and owned ClickHouse pause must fit its original absolute15s barrier.',
    'While only that ClickHouse is paused, source/Archive reach12; real plan3 [4..7] remains prepared, plan4 is absent, and Index stays at revision2/cut3.',
    'After orderly application/client join and mandatory ClickHouse unpause, explicit source closure precedes same-store catch-up1→2 and exact retry2→3. All40 outcomes, original creator/publisher history and independent source/C API bytes are checked.',
    'The original353-byte signed history/export journey runs after recovery. Index catch-up reports ready=false and performs no acknowledgement, Ingest retirement or topology transition.',
    'NOT_RUN: unknown native termination, remote uncertainty resolution, lost COMMIT, independent process outage/capacity, native RocksDB census, topology handoff and complete M1. Pausing an engine does not qualify those cases.',
    'The original physical-corruption case remains a separate fresh case. This fixture has empty signed tags and an explicit zero tag outcome.',
]

PREFIX_FRONTIERS = """WITH observed AS (
    SELECT c.consumer_role,c.consumer_id,c.current_revision,
           f.frontier_revision,f.last_complete_sequence
    FROM control.consumers c LEFT JOIN control.consumer_frontiers f USING(consumer_id)
    WHERE c.consumer_role IN ('archive','index'))
SELECT json_build_object('observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
    'matching_count',count(*) FILTER (WHERE current_revision=2 AND frontier_revision=2 AND last_complete_sequence=3),
    'frontiers',COALESCE(json_agg(row_to_json(observed) ORDER BY consumer_role,consumer_id,frontier_revision),'[]'::json))
FROM observed"""


def prefix_published(value):
    require(set(value)=={'observed_at_us','matching_count','frontiers'}
        and type(value['observed_at_us']) is int and type(value['matching_count']) is int
        and isinstance(value['frontiers'],list) and len(value['frontiers'])<=32,'bounded prefix frontier observation')
    # Preserve every joined row, including duplicates. This is the original
    # count predicate; role labels do not turn it into a grouped membership test.
    matches=sum(r['current_revision']==2 and r['frontier_revision']==2 and r['last_complete_sequence']==3
                for r in value['frontiers'])
    require(value['matching_count']==matches,'prefix count differs from raw joined rows')
    return matches==2


def gate_deadline(value, now):
    require(set(value)=={'format','started_ns','deadline_ns'}
        and value['format']=='bigbrotr-installed-index-prefix-gate/1'
        and type(value['started_ns']) is type(value['deadline_ns']) is int
        and value['deadline_ns']-value['started_ns']==15_000_000_000,
        'prefix client deadline declaration')
    deadline = value['deadline_ns']/1_000_000_000
    require(value['started_ns']/1_000_000_000 <= now < deadline,'prefix client deadline expired')
    return deadline


class RestartIndexRuntime(IndexRuntime):
    def backend_absence(self, output):
        # Preserve each terminal observer receipt when several finite owners
        # share the restart directory; a later success cannot replace a failure.
        observed = output/('backend-absence-'+str(self.number))
        observed.mkdir(mode=0o700)
        super().backend_absence(observed)

    def candidate(self):
        for path in (Path(__file__),Path(oracle.__file__)):
            require(digest(path)==digest(self.source/'tests/acceptance/cases'/path.name),'Index restart acceptance source mismatch')
        super().candidate()

    def prepare_state(self):
        super().prepare_state()
        # The default first Archive dispatch is due after15s, leaving no useful
        # publication window inside this client's independent15s prefix gate.
        self.config.write_text(self.set_value(self.config.read_text(),'archive','max_publish_delay','1s'))
        self.inputs['initial_recipe_sha256'] = digest(self.config)
        save(self.home/'index-restart-profile.json',dict(format='bigbrotr-installed-index-restart/1'))

    def initial_launch(self):
        # Runtime.initial captured these accepted documents immediately before
        # this hook, with no application owner running. Never rewrite policy.
        oracle.prefix_profile(load(self.root/'initial/before-control.json'),tomllib.loads(self.config.read_text()))
        return super().initial_launch()

    def command(self, command, **options):
        deadline = getattr(self,'prefix_deadline',None)
        if deadline is not None:
            remaining = deadline-time.monotonic()
            require(remaining>0,'prefix client deadline expired before command')
            options['timeout'] = min(options.get('timeout',30),remaining)
        result = super().command(command,**options)
        if deadline is not None:
            require(time.monotonic()<deadline,'prefix command exceeded client deadline')
        return result

    def initial_source_barrier(self, entry, output):
        self.wait(lambda:(self.home/'index-prefix.json').exists(),'source prefix enrollment',15,entry,self.client)
        signal = load(self.home/'index-prefix-deadline.json')
        self.prefix_deadline = deadline = gate_deadline(signal,time.monotonic())
        try:
            frontiers=[]
            def published():
                raw=self.sql('bigbrotr_control',PREFIX_FRONTIERS)
                require(len(raw)<=16384 and len(frontiers)<256,'prefix frontier capture bound')
                observed=json.loads(raw);frontiers.append(observed)
                save(output/'index-prefix-frontier-observations.json',frontiers)
                return prefix_published(observed)
            self.wait(published,'literal Archive/Index prefix',deadline-time.monotonic(),entry,self.client)
            before,_ = self.capture(output,'index-prefix-before')
            oracle.backlog(before,self.state/'data/index',prefix=True)
            saved = load(self.home/'session.private')
            require(saved['format']=='bigbrotr-cli-session/1' and saved['origin']=='http://127.0.0.1:18081','prefix session binding')
            clock_before = int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
            observations = []
            def ready():
                remaining = deadline-time.monotonic()
                require(remaining>0,'prefix readiness exceeded client deadline')
                connection = http.client.HTTPConnection('127.0.0.1',18081,timeout=min(3,remaining))
                try:
                    connection.request('GET','/management/v1/roles',headers={'Origin':saved['origin'],
                        'Cookie':'BigBrotrLocalSession='+saved['session'],'X-CSRF-Token':saved['csrf']})
                    response=connection.getresponse(); raw=response.read(131073)
                    require(response.status==200 and len(raw)<=131072,'prefix authenticated role observation failed')
                    value=json.loads(raw)
                finally:
                    connection.close()
                require(len(observations)<256 and time.monotonic()<deadline,'prefix observation bound')
                observations.append(value); save(output/'index-prefix-role-observations.json',observations)
                caps=value['capabilities']
                return len(caps)==4 and all(c['state']=='ready' for c in caps)
            self.wait(ready,'positive complete prefix readiness',deadline-time.monotonic(),entry,self.client)
            clock_after = int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
            self.prefix,_ = self.capture(output,'index-prefix')
            oracle.backlog(self.prefix,self.state/'data/index',prefix=True)
            index.exact(before['browser_sessions'],self.prefix['browser_sessions'])
            index.live_roles(observations[-1],self.prefix,clock_before,clock_after,revision=2,sequence=3)
            save(output/'index-prefix-verdict.json',dict(status='PASS',source_through=3,archive_revision=2,index_revision=2,
                clock_before_us=clock_before,clock_after_us=clock_after,session_idle='unchanged',gate=signal))
            self.custody_before = self.custody()
            save(output/'index-custody-before.json',self.custody_before)
            self.pause_clickhouse()
            require(time.monotonic()<deadline,'prefix pause exceeded client deadline')
            (self.home/'index-prefix.continue').write_text('positive complete prefix checked; owned ClickHouse paused\n')
            require(time.monotonic()<deadline,'prefix release exceeded client deadline')
        finally:
            self.prefix_deadline = None

    def custody(self):
        root = self.state/'data/index'
        identity,_,_ = index.retained(root)
        directory = root/'.bigbrotr-index-custody'
        require({p.name for p in directory.iterdir()}=={'custody.lock','identity.json','tables.json'},'Index custody inventory')
        observed = dict(root=input_identity(root,directory=True),directory=input_identity(directory,directory=True),
            lock=input_identity(directory/'custody.lock',empty=True),
            files={name:input_identity(directory/name) for name in ('identity.json','tables.json')})
        for key,field in (('root','root_identity'),('directory','journal_identity'),('lock','lock_identity')):
            index.exact(identity[field],{k:observed[key][k] for k in ('device','inode')})
        return observed

    def pause_clickhouse(self):
        info = self.inspect(self.ch_entry)
        require(info['State']['Running'] and not info['State']['Paused'],'ClickHouse was not running unpaused')
        self.resources['index_restart_pause']=dict(id=info['Id'],status='pausing',events=['pausing'])
        self.record_resources()
        self.docker('pause',info['Id'])
        info = self.inspect(self.ch_entry)
        require(info['State']['Running'] and info['State']['Paused'],'owned ClickHouse pause not observed')
        self.resources['index_restart_pause'].update(status='paused')
        self.resources['index_restart_pause']['events'].append('paused')
        self.record_resources()

    def unpause_clickhouse(self):
        journal = self.resources.get('index_restart_pause')
        if journal is None:
            return
        info = self.inspect(self.ch_entry)
        require(info['Id']==journal['id'],'paused ClickHouse identity changed')
        if info['State']['Paused']:
            self.docker('unpause',info['Id'])
            info = self.inspect(self.ch_entry)
        require(info['State']['Running'] and not info['State']['Paused'],'owned ClickHouse not running unpaused')
        if journal['status']!='unpaused':
            journal.update(status='unpaused'); journal['events'].append('unpaused')
        self.record_resources()

    def initial_publication(self, entry, output):
        def prepared():
            return self.sql('bigbrotr_control',"SELECT count(*) FROM control.projection_plans WHERE frontier_revision=3 AND sealed")=='1'
        self.wait(prepared,'real retained Index plan3',15,entry,self.client)
        self.backlog_live,_ = self.capture(output,'index-backlog')
        oracle.backlog(self.backlog_live,self.state/'data/index')
        info = self.inspect(self.ch_entry)
        require(info['State']['Running'] and info['State']['Paused'],'ClickHouse unexpectedly resumed during backlog')
        require(self.inspect(entry)['State']['Running'] and self.archive_published(),'source/Archive stopped during backlog')
        save(output/'index-backlog-verdict.json',dict(status='PASS',source_through=12,archive_revision=4,
            index_revision=2,index_through=3,prepared_plan=3,prepared_range=[4,7],unplanned_range=[8,12],clickhouse='paused'))

    def index_restart_baseline(self):
        self.source_unchanged()
        output=self.root/'index-restart'; output.mkdir(mode=0o700); self.restart_output=output
        self.restart_before,self.restart_catalog=self.capture(output,'before')
        oracle.backlog(self.restart_before,self.state/'data/index')
        for table in index.INDEX_TABLES:
            index.exact(self.backlog_live[table],self.restart_before[table])
        final=load(self.root/'initial/initialized.json')
        terminal=[json.loads(line) for line in (self.root/'initial/serve.stdout').read_text().splitlines()][-1]
        require(terminal['index']['published_revision']=='2'
            and terminal['index']['complete_through']==[dict(shard_id=0,owner_epoch='1',sequence='3')]
            and terminal['index']['readiness']=='not-qualified','initial Index shutdown rewrote published prefix')
        self.unpause_clickhouse()  # Mandatory before any catch-up or recovery.
        self.archive_files=self.files(self.state/'data/archive')
        self.input_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        save(output/'before-archive-files.json',self.archive_files); save(output/'before-input-files.json',self.input_files)
        self.baseline_census=self.census(output,'before')
        before=load(self.root/'initial/before-control.json')
        view=self.current_owner_view(self.restart_before,dict(control=before,resource=dict(catalog_id=self.ids['catalog'])))
        owner=index.only(view['owner_epochs'],role='control')
        wires=[load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        self.index_science=self.science_checks.first_science(view,final,load(self.home/'event.jsonl'),wires,
            self.baseline_census,self.state/'data/ingest',owner['released_at_us'])
        index.exact(self.custody(),self.custody_before)
        report=self.finite('index-source-close','ingest','recover','--close-source','--file','/etc/bigbrotr/instance.toml')
        self.closed,_=self.capture(output,'source-closed')
        oracle.source_closed(self.restart_before,self.closed,report,self.index_science)
        oracle.backlog(self.closed,self.state/'data/index')
        self.preserved(output,'source-closed')

    def preserved(self, output, name):
        self.backend_absence(output)
        require(self.catalog_rows({'postgres':self.pg['id']},'bigbrotr_catalog')==self.restart_catalog,'Index recovery changed Archive catalog')
        require(self.files(self.state/'data/archive')==self.archive_files,'Index recovery changed Archive files')
        require(self.files(self.state/'data/ingest/producer-inputs-v1')==self.input_files,'Index recovery changed original inputs')
        index.exact(self.custody(),self.custody_before)
        save(output/(name+'-preservation.json'),dict(control_tables=81,archive_catalog='unchanged',archive_files='unchanged',
            input_bytes='unchanged',index_custody='unchanged'))

    def index_catch_up(self):
        report=self.finite('index-catch-up','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.caught_up,_=self.capture(self.restart_output,'caught-up')
        oracle.catch_up_report(report,self.caught_up)
        oracle.recovered(self.closed,self.caught_up,self.state/'data/index')
        self.preserved(self.restart_output,'caught-up')
        self.index_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.restart_output/'clickhouse-caught-up.json',self.index_rows)
        index.publication(self.caught_up,self.caught_up,self.index_rows,self.index_science,self.schema,self.state/'data/index',owner_count=2)

    def index_retry(self):
        report=self.finite('index-catch-up-retry','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.retried,_=self.capture(self.restart_output,'retry')
        oracle.catch_up_report(report,self.retried,retry=True)
        oracle.recovered(self.caught_up,self.retried,self.state/'data/index',retry=True)
        self.preserved(self.restart_output,'retry')
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.restart_output/'clickhouse-retry.json',rows)
        index.exact(rows,self.index_rows)

    def verify_index(self):
        result=index.publication(self.retried,self.retried,self.index_rows,self.index_science,self.schema,self.state/'data/index',owner_count=3)
        tables=self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name",rows=True)
        columns=self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position",rows=True)
        index.schema_metadata(self.schema,self.state/'data/index',tables,columns)
        save(self.restart_output/'clickhouse-schema.json',dict(tables=tables,columns=columns))
        index.exact(self.census(self.restart_output,'retry'),self.baseline_census)
        result.update(format='bigbrotr-installed-index-restart/1',live_observation=False,
            readiness='positive_prefix_before_pause',restart='same_store_backlog_and_exact_retry',
            origin_epochs=[1,1,1,2],publisher_epochs=[1,1,2,2],owner_epochs=[1,2,3],control_tables=81,
            remote_uncertainty_resolution='NOT_RUN',lost_commit='NOT_RUN',native_termination='NOT_RUN')
        save(self.restart_output/'verdict.json',result); self.inputs['index']=result
        self.source_unchanged()

    # Preserve the original signed history/export journey and its40s/12s bounds.
    ordinary_client = IndexRuntime.baseline_client

    def cleanup(self):
        try:
            self.unpause_clickhouse()
        except Exception as error:
            self.resources['cleanup_errors'].append(dict(resource='paused-clickhouse',error=type(error).__name__))
            save(self.root/'index-unpause-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
        super().cleanup()
