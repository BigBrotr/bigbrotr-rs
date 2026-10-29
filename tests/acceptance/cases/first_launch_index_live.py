"""Actual live retained-Index successor, sharing the finite topology journey."""
import http.client
import json
from pathlib import Path
import time
import tomllib

from cases import first_launch as base
from cases.first_launch_index import DATABASE
from cases.first_launch_index_topology import TopologyIndexRuntime, STAGES as FINITE_STAGES
from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology_oracle as topology
from cases import first_launch_index_live_oracle as oracle

STAGES = FINITE_STAGES[:-1]+('historical_index_topology','cleanup')

LIMITS = [
    'One actual retained enabled-Index epoch1→2 activation commits its zero-successor bridge before Sync and publishes all three existing signed successor batches while producer2 remains open.',
    'The maintained source, C API and ten-family scientific oracles independently compare all70 chunks/100 bounds, the original event/resend and the new t=allowed event; readiness requires the complete closed1/current2 vector.',
    'Stopping, source closure, Archive catch-up and two finite Index rebinds preserve the live bridge and add no logical publication; original history/export and all original client/query/resource bounds remain in force.',
    'A genuine epoch3/topology2 activation without Sync/Index preserves historical scientific state; actual runtime PostgreSQL login verifies the retained bridge in a read-only transaction and observes the shared current-authority guard refuse unsupported topology.',
    'NOT_RUN: continuous Archive, generic or arbitrary successor recovery,64-history readiness, broad capacity, unknown remote/native outcome resolution, second-event export and complete M1 task composition.',
]


class LiveTopologyIndexRuntime(TopologyIndexRuntime):
    def candidate(self):
        for path in (Path(__file__),Path(oracle.__file__)):
            base.require(base.digest(path)==base.digest(self.source/'tests/acceptance/cases'/path.name),
                         'live Index topology acceptance source mismatch')
        super().candidate()

    def successor_launch(self):
        return self.launch('successor','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--activate-initial-successor','--roles','control,ingest,sync,index')

    def successor_activated(self, started, state):
        self.live_bridge=state
        generation=oracle.retained_bridge(self.initial_index_state,state,self.state/'data/index')
        self.check_started_index(started,generation)
        index.exact(self.custody(),self.initial_index_custody)
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        index.physical_rows(rows,self.first_science,generation['generation_id'],self.schema)
        base.save(self.restart_output/'live-bridge-verdict.json',dict(status='PASS',source_epoch=2,
            producer='open',frontier_revision=5,published_cuts=[[1,12],[2,0]],new_physical_batches=0,
            retained_generation=True,physical_epoch1_rows='exact',before_successor_client=True))

    def check_started_index(self, started, generation):
        oracle.startup_index(started['index'],generation)

    def successor_client_options(self):
        return ['--live-index-proof']

    def phase_guard(self, deadline, client):
        base.require(time.monotonic_ns()<deadline,'successor client phase expired')
        base.require(client.poll() is None,'successor client exited during phase')
        now=time.monotonic_ns()
        base.require(now<deadline,'successor client phase expired')
        return now

    def phase_wait(self, predicate, deadline, entry, client):
        while True:
            self.phase_guard(deadline,client)
            result=predicate()
            self.phase_guard(deadline,client)
            if result:return result
            base.require(self.inspect(entry)['State']['Running'],'successor owner exited during phase')
            self.phase_guard(deadline,client)
            time.sleep(.05)

    def successor_submission(self, entry, client, classified):
        phase=base.load(self.successor_home/'submitted-phase.json')
        deadline=oracle.phase_deadline(phase,'submitted',time.monotonic_ns())
        self.phase_wait(classified,deadline,entry,client)
        (self.successor_home/'submitted.continue').write_text('finite three-event head observed\n')
        released=self.phase_guard(deadline,client)
        base.save(self.restart_output/'submitted-parent-phase.json',dict(phase,released_monotonic_ns=released))

    def successor_published(self, entry, client):
        home=self.successor_home
        # This transition wait cannot accept a late positive or a dead client.
        # Once opened, only the child's45s clock bounds all subsequent work.
        self.phase_wait(lambda:(home/'index-proof.json').exists(),
            time.monotonic_ns()+15_000_000_000,entry,client)
        phase=base.load(home/'index-proof-phase.json')
        deadline=oracle.phase_deadline(phase,'index-proof',time.monotonic_ns())
        self.phase_guard(deadline,client)
        saved=base.load(home/'session.private')
        base.require(saved['format']=='bigbrotr-cli-session/1' and saved['origin']=='http://127.0.0.1:18081',
                     'successor role session binding')
        self.phase_guard(deadline,client)
        before,_=self.capture(self.restart_output,'live-roles-before')
        self.phase_guard(deadline,client)
        clock_before=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        self.phase_guard(deadline,client)
        observations=[];last=0
        expected=oracle.cut(index.only(before['datasets'])['dataset_id'],10)
        def observed():
            nonlocal last
            if time.monotonic()-last<.25:return False
            last=time.monotonic()
            connection=http.client.HTTPConnection('127.0.0.1',18081,timeout=3)
            try:
                connection.request('GET','/management/v1/roles',headers=dict(Origin=saved['origin'],
                    Cookie='BigBrotrLocalSession='+saved['session'],**{'X-CSRF-Token':saved['csrf']}))
                response=connection.getresponse();raw=response.read(131073)
                base.require(response.status==200 and len(raw)<=131072,'successor role observation failed')
                value=json.loads(raw)
            finally:connection.close()
            base.require(len(observations)<256,'successor role observation bound')
            observations.append(value);base.save(self.restart_output/'live-role-observations.json',observations)
            base.require(value['format']=='bigbrotr-role-status/1' and value['authority']=='observation-only',
                         'successor role observation authority')
            cap=index.only(value['capabilities'],capability='index_publication')
            if cap['state']=='ready':
                # Even an intermediate Ready must describe the complete current
                # source. An old or single-epoch physical proof cannot pass.
                index.exact(value['source']['cut'],expected)
                index.exact(cap['published'],dict(revision='8',cut=expected))
                return True
            return False
        # Reuse the existing45s/256-sample/250ms Index acceptance envelope.
        # This is separate from classified3/submitted25, and is not a latency SLA.
        self.phase_wait(observed,deadline,entry,client)
        clock_after=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        self.phase_guard(deadline,client)
        self.live_successor,_=self.capture(self.restart_output,'live-ready')
        self.phase_guard(deadline,client)
        oracle.ready_roles(observations[-1],self.live_successor,clock_before,clock_after)
        oracle.append_only_history(self.initial_index_state,self.live_successor)
        oracle.append_only_history(self.live_bridge,self.live_successor)
        base.require(before['browser_sessions']==self.live_successor['browser_sessions'],
                     'successor role observation changed the private session')
        self.phase_guard(deadline,client)
        base.require(self.inspect(entry)['State']['Running'],'successor owner exited during phase')
        self.phase_guard(deadline,client)
        (home/'index-proof.continue').write_text('full-vector live Index proof independently checked\n')
        self.phase_guard(deadline,client)
        self.phase_wait(lambda:(home/'index-proof-phase-released.json').exists(),deadline,entry,client)
        release=base.load(home/'index-proof-phase-released.json')
        oracle.phase_release(phase,release,self.phase_guard(deadline,client))
        completed=self.phase_guard(deadline,client)
        base.save(self.restart_output/'live-role-verdict.json',dict(status='PASS',producer='open',
            source_cuts=[[1,12],[2,10]],published_revision=8,physical_verification='full-vector',
            before_successor_stop=True,session_idle='unchanged',observations=len(observations),
            clock_before_us=str(clock_before),clock_after_us=str(clock_after),
            phase=phase,client_release=release,parent_completed_monotonic_ns=completed))

    def restart_ingest(self):
        base.Runtime.restart_ingest(self)
        oracle.publication_unchanged(self.live_successor,self.retired_successor)
        index.exact(self.custody(),self.initial_index_custody)
        # Full physical verification was observed live. Diagnostic ten-family
        # reads consume no time from the client's source25s or Index45s phases.
        self.live_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        base.save(self.restart_output/'clickhouse-after-live-stop.json',self.live_rows)

    def successor_abandoned(self):
        # Product SQL/native controls own new-bridge refusal at this boundary.
        # This installed journey owns preservation of the already committed one.
        state,_=self.capture(self.restart_output,'live-closed-unretired')
        oracle.publication_unchanged(self.live_successor,state)

    def restart_archive(self):
        base.Runtime.restart_archive(self)
        oracle.publication_unchanged(self.live_successor,self.caught_up)
        self.topology_output=self.root/'index-topology';self.topology_output.mkdir(mode=0o700)
        result=topology.published_history(self.caught_up,self.final_science,self.schema,self.live_rows,owner_epoch=2)
        result.update(live_observation=True,producer_open_at_readiness=True,bridge_before_sync=True)
        base.save(self.topology_output/'live-publication-verdict.json',result)
        self.inputs['live_index_topology']=result
        self.index_preserved('live-publication')

    def index_epoch2_catch_up(self):
        self.source_unchanged()
        before,_=self.capture(self.topology_output,'before-rebind')
        oracle.publication_unchanged(self.live_successor,before)
        report=self.finite('index-live-history-rebind','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.topology_after,_=self.capture(self.topology_output,'caught-up')
        self.topology_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        index.exact(self.topology_rows,self.live_rows)
        result=oracle.no_work_rebind(before,self.topology_after,report,self.final_science,self.schema,
                                    self.topology_rows,owner_epoch=3)
        base.save(self.topology_output/'caught-up-verdict.json',result)
        self.index_preserved('caught-up')

    def index_epoch2_retry(self):
        report=self.finite('index-live-history-retry','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        after,_=self.capture(self.topology_output,'retry')
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        index.exact(rows,self.live_rows)
        result=oracle.no_work_rebind(self.topology_after,after,report,self.final_science,self.schema,rows,owner_epoch=4)
        base.save(self.topology_output/'retry-verdict.json',result)
        self.index_preserved('retry')

    def historical_index_topology(self):
        self.source_unchanged()
        output=self.root/'historical-index-topology';output.mkdir(mode=0o700)
        before,catalog=self.capture(output,'before')
        # The ordinary reader narrowed its local configuration. Activation uses
        # the original normal runtime profile, with no initializer credential.
        self.config.write_text((self.root/'ordinary/initial-config.toml').read_text())
        connection=tomllib.loads(self.config.read_text())['postgres']['control']
        index.exact({k:connection[k] for k in ('database','user','endpoint','password_ref')},
            dict(database='bigbrotr_control',user='bigbrotr_control',endpoint='127.0.0.1:15432',
                 password_ref='file:/run/secrets/control'))
        entry=self.launch('historical-topology','app','serve','--file','/etc/bigbrotr/instance.toml',
                          '--activate-successor','--roles','control,ingest')
        started=self.wait(lambda:self.logs(entry,output),'genuine epoch3 activation',20,entry=entry)[0]
        active,active_catalog=self.capture(output,'active')
        oracle.later_topology(before,active,started)
        index.exact(catalog,active_catalog)
        terminal=self.stop_container(entry,output)
        final=self.logs(entry,output)[-1]
        base.require(terminal['State']['ExitCode']==0,'historical successor did not stop normally')
        index.exact(final,dict(format='bigbrotr-ingest-successor/1',state='stopped',
            dataset_id=started['dataset_id'],owner_resource_id=started['owner_resource_id'],
            producer_slot_id=started['producer_slot_id'],producer_epoch='3',native='closed',
            owner_release='committed',source_closure='not-performed',ready=False))
        stopped,stopped_catalog=self.capture(output,'stopped')
        oracle.later_topology(before,stopped,started,stopped=True)
        index.exact(catalog,stopped_catalog)
        consumer=index.only(before['consumers'],consumer_role='index')['consumer_id']
        dataset=index.only(before['datasets'])['dataset_id']
        statement=oracle.historical_sql(consumer,dataset)
        base.save(output/'runtime-login-statement.json',dict(statement=statement))
        # Unlike Runtime.sql's database selector, this is a real TCP login under
        # the configured runtime role, using the mounted product secret only in
        # the child environment. The recorded argv never contains its value.
        result=self.docker('exec','-i','--env','PGCONNECT_TIMEOUT=5','--env',
            'PGOPTIONS=-c statement_timeout=5000 -c lock_timeout=1000',self.pg['id'],
            'sh','-ec',oracle.CONTROL_LOGIN,data=statement,timeout=10,check=False,
            label='historical-index-runtime-login')
        verdict=oracle.historical_sql_result(result.stdout,result.stderr,result.returncode,consumer,dataset)
        after,after_catalog=self.capture(output,'after-sql')
        index.exact(stopped,after);index.exact(catalog,after_catalog)
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        index.exact(rows,self.live_rows)
        self.index_preserved('historical-topology')
        self.backend_absence(output)
        verdict.update(status='PASS',producer_epoch=3,topology_generation=2,scientific_requests=0,
            product_postgres_profile={k:connection[k] for k in ('database','user','endpoint','password_ref')},
            owner_stopped_and_joined=True,historical_publication='exact',
            current_authority='shared_guard_refused_unsupported_topology',
            native_new_bridge_creation='NOT_RUN',both_authority_branches_independently='NOT_RUN')
        base.save(output/'verdict.json',verdict)
        self.inputs['historical_index_topology']=verdict
