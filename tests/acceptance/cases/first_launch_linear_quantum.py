"""Two current scopes, finite real socket turns and durable refusal obligations."""
import hashlib
import json
import sys
import time
import tomllib

from cases.first_launch import Runtime,load,save,require,digest,stop_process
from cases import first_launch_quantum_oracle as oracle
from cases import first_launch_retained_native as native

STAGES=('prerequisites','candidate','runtime_image','fresh_bootstrap','signed_acquisition','linear_quantum','cleanup')
LIMITS=[
    'Two original finite scopes share the current Control/Sync/native owner. Nine real requests exercise slow EVENT/EOSE, malformed EVENT then EOSE, frame/event caps then EOSE, CLOSED, deadline, empty and proof-only work.',
    'The finite listener closes after9; the first scope retains its repeated-second gap and observes the older empty remainder; the second closes its window on empty, with no next plan and pending coverage. An actual source refusal neither completes a traversal nor synthesizes a producer request. All original input bytes, full native rows/sequence and independently expected classification/dependency records are compared after positive owner join.',
    'Optional exact plan/terminal COMMIT before-forward/after-complete schedules prove no next socket/quantum and historical-only restart, including missing plan versus retained pending/terminal record. NOT_RUN: full walker, age bands, NEG/live, OPEN65, same-job adoption, physical capacity. Empty post-REQ authority withdrawal is isolated by the actual completed source-rollback reply; broader terminal races remain separate.',
]


class LinearQuantumRuntime(Runtime):
    def quantum_proxy_ordinal(self):
        return 1

    def fault_owner_joined(self, output):
        pass

    def prepare_gap_refusal(self, output):
        captured=self.sql('bigbrotr_control',"SELECT to_jsonb(q)||jsonb_build_object('xmin',xmin::text) FROM control.acquisition_quanta q; "
            "ALTER TABLE control.acquisition_quanta ADD CONSTRAINT fixture_gap_write_refusal CHECK(gap_reason IS NULL) NOT VALID")
        self.gap_plan_before=json.loads(captured)
        save(output/'gap-plan-before.json',self.gap_plan_before)

    def source_view(self,state):
        return self.current_owner_view(state,dict(control=load(self.root/'initial/before-control.json'),
            resource=dict(catalog_id=self.ids['catalog'])))

    def wires(self,count):
        require(not (self.home/'relay-error.json').exists(),'finite relay failed')
        opened=sorted(self.home.glob('relay-open-*.json'));closed=sorted(self.home.glob('relay-closed-*.json'))
        require(len(opened)==len(closed)==count,'missing finite transcript or extra accepted socket')
        wires=[]
        for number,(before,after) in enumerate(zip(opened,closed),1):
            require(before.name==f'relay-open-{number:03}.json' and after.name==f'relay-closed-{number:03}.json','finite transcript gap')
            first,last=load(before),load(after)
            require(all(last.get(key)==value for key,value in first.items()),'finite open/close binding changed')
            wires.append(last)
        return wires

    def candidate(self):
        super().candidate()
        require(self.args.native_observer_build is not None,'linear quantum requires declared exact-source native observer')
        self.native_paths,self.native_hashes=native.artifact(self.args.native_observer_build,self.manifest)
        self.inputs['native_observer_artifacts_sha256']=self.native_hashes
        for name in ('tests/acceptance/cases/first_launch_linear_quantum.py',
            'tests/acceptance/cases/first_launch_quantum_oracle.py',
            'experiments/ingest-bootstrap/linear_quantum_transport.py',
            'experiments/control-migrations/linear_quantum_capacity.sql'):
            self.inputs['acceptance_files_sha256'][name]=digest(self.source/name)

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0,str(self.source/'experiments/archive-engine'))
        from fault_cases import Process
        from signed_publication_cases import event,send,proxy_observation
        self.proxy_event,self.proxy_send,self.proxy_observation=event,send,proxy_observation
        self.proxy_process=Process
        import installed_archive_oracle
        import installed_sync_oracle
        import linear_quantum_transport
        # Both capped and window journeys use these decisive post-join owners.
        # Load the transitive closure before candidate() freezes verifier hashes.
        import installed_progress_replay
        import installed_progress_context_oracle
        import installed_family_science

    def image_and_state(self):
        super().image_and_state()
        from linear_quantum_transport import PROFILE
        save(self.home/'linear-quantum-profile.json',PROFILE)

    def bootstrap(self):
        # Keep unrelated initialization outside the finite fault proxy budget.
        # Initial owner startup and every dictionary/quantum statement still use it.
        super().bootstrap()
        self.quantum_fault=getattr(self.args,'linear_quantum_fault',None)
        if self.quantum_fault:save(self.home/'linear-quantum-fault.json',self.quantum_fault)
        if self.quantum_fault:
            original=self.config.read_text()
            upstream=tomllib.loads(original)['postgres']['control']['endpoint']
            require(upstream=='127.0.0.1:15432','quantum proxy requires declared direct Control upstream')
            direct_recipe_sha256=digest(self.config)
            boundary,direction=self.quantum_fault.split('-') if self.quantum_fault not in ('gap-write','stale-authority','empty-terminal-authority') else ('plan','held')
            path=self.source/'experiments/archive-engine/native_commit_proxy.py'
            self.inputs['acceptance_files_sha256'][str(path.relative_to(self.source))]=digest(path)
            profile='default' if self.quantum_fault in ('plan-before','plan-after') else 'retained-catch-up'
            mode=('hold-source-rollback-reply' if self.quantum_fault=='empty-terminal-authority' else
                'hold-reply' if direction=='held' else 'before-forward' if direction=='before' else 'after-complete')
            target=('control-quantum-source-rollback' if self.quantum_fault=='empty-terminal-authority' else
                'control-quantum-'+('plan' if boundary=='plan' else 'terminal'))
            # Body/authority captures include more framed Control work before
            # the selected boundary. Reuse the existing finite traffic profile;
            # this changes neither its64MiB/120s bounds nor product admission.
            self.quantum_proxy_binding=dict(source_sha256=digest(path),mode=mode,target=target,profile=profile,
                frame_limit=20000 if profile=='default' else 131072,
                connection_limit=16 if profile=='default' else 64,connection_limit_scope='lifetime',
                wire_limit=64*1024*1024,lifetime_seconds=120,upstream_host='127.0.0.1',upstream_port=15432)
            ordinal=self.quantum_proxy_ordinal()
            require(type(ordinal) is int and 1<=ordinal<=128,'invalid quantum proxy COMMIT ordinal')
            if ordinal!=1:self.quantum_proxy_binding['commit_ordinal']=ordinal
            self.inputs['quantum_proxy']=self.quantum_proxy_binding
            command=[sys.executable,'-B',str(path),'--mode',mode,'--target',target,'--upstream-port','15432']
            if ordinal!=1:command+=['--commit-ordinal',str(ordinal)]
            if profile!='default':command+=['--profile',profile]
            self.route=self.proxy_process(command,input_pipe=True)
            self.resources['quantum_proxy']=dict(pid=self.route.process.pid,status='running',port=None,
                upstream_endpoint=upstream,direct_recipe_sha256=direct_recipe_sha256)
            self.record_resources()
            save(self.root/'quantum-proxy-process.json',dict(pid=self.route.process.pid))
            ready=self.proxy_event(self.route,'ready',10)
            save(self.root/'quantum-proxy-ready.json',ready)
            self.validate_quantum_proxy(ready,'ready')
            self.quantum_proxy_ready=ready
            self.resources['quantum_proxy']['port']=ready['port']
            self.record_resources()
            self.config.write_text(self.set_value(original,'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            self.inputs['initial_recipe_sha256']=digest(self.config)

    def validate_quantum_proxy(self,value,event):
        require(value.get('event')==event and all(type(value.get(key)) is type(expected)
            and value[key]==expected for key,expected in self.quantum_proxy_binding.items()),
            'quantum proxy '+event+' binding differs')
        if event=='terminal':
            require(type(value.get('unexpected')) is int and value['unexpected']==0
                and all(type(value.get(key)) is int and 0<value[key]<=limit
                for key,limit in (('frames',value['frame_limit']),('connections',value['connection_limit']),
                    ('bytes',value['wire_limit']))),'incomplete quantum proxy terminal observation')

    def stop_quantum_proxy(self):
        if not hasattr(self,'route') or self.resources['quantum_proxy']['status']=='joined':return
        route=self.route;failures=[]
        if route.process.poll() is None:
            try:self.proxy_send(route,'stop\n')
            except Exception as error:failures.append(('control',error))
        try:require(route.wait(5)==0,'quantum proxy did not join')
        except Exception as error:failures.append(('join',error))
        # Process.stop owns bounded terminate/kill and pipe-reader joins, even
        # after a closed control stream or an unsuccessful wait.
        try:route.stop()
        except Exception as error:failures.append(('stop',error))
        code=route.process.poll()
        streams_joined=not any(thread.is_alive() for thread in route.threads)
        for name,data in (('quantum-proxy.stdout',route.stdout),('quantum-proxy.stderr',route.stderr)):
            try:(self.root/name).write_bytes(data)
            except Exception as error:failures.append(('output',error))
        try:save(self.root/'quantum-proxy-process.json',dict(pid=route.process.pid,
            exit_code=code,overflow=route.overflow,streams_joined=streams_joined))
        except Exception as error:failures.append(('process-record',error))
        try:
            result=self.proxy_observation(route);save(self.root/'quantum-proxy.json',result)
            require(code==0 and streams_joined and not result['stderr'] and not result['overflow'],
                'incomplete quantum proxy observation')
            if hasattr(self,'quantum_proxy_ready'):
                events=[json.loads(line) for line in result['stdout'].splitlines()]
                ready,=[value for value in events if value.get('event')=='ready']
                terminal,=[value for value in events if value.get('event')=='terminal']
                save(self.root/'quantum-proxy-terminal.json',terminal)
                require(ready==self.quantum_proxy_ready,'quantum proxy ready observation changed')
                self.validate_quantum_proxy(terminal,'terminal')
        except Exception as error:failures.append(('observation',error))
        self.resources['quantum_proxy'].update(status='joined' if code is not None and streams_joined else 'cleanup-incomplete',exit_code=code)
        self.resources['cleanup_errors'].extend(dict(resource='quantum-proxy',id=route.process.pid,
            operation=operation,error=type(error).__name__) for operation,error in failures)
        try:self.record_resources()
        except Exception:
            if failures:raise failures[0][1]
            raise
        if failures:raise failures[0][1]

    def control_lease_observation(self):
        return json.loads(self.sql('bigbrotr_control',"""
            WITH observed AS MATERIALIZED (
                SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS now_us
            ) SELECT jsonb_build_object('observed_at_us',observed.now_us,
                'owners',(SELECT coalesce(jsonb_agg(to_jsonb(o)||jsonb_build_object('xmin',o.xmin::text)
                    ORDER BY o.resource_id,o.epoch),'[]'::jsonb) FROM control.owner_epochs o WHERE o.role='control'),
                'job_owners',(SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_array(j.dataset_id,
                    j.owner_resource_id,j.owner_epoch,j.owner_incarnation)),'[]'::jsonb)
                    FROM control.jobs j WHERE j.kind='acquisition' AND j.owner_epoch IS NOT NULL),
                'active_unexpired',(SELECT count(*) FROM control.owner_epochs o
                    WHERE o.role='control' AND o.state='active' AND o.lease_expires_at_us>observed.now_us))
            FROM observed"""))

    def freeze_control_lease(self,terminal,output):
        require(not terminal['State']['Running'] and terminal['State']['Pid']==0
            and not terminal['State']['OOMKilled'],'original Control application is not positively stopped')
        profile=tomllib.loads(self.config.read_text()).get('runtime',{}).get('lease_ttl','60s')
        require(profile=='60s','quantum historical restart requires declared 60s Control lease profile')
        started=time.monotonic();sample=self.control_lease_observation()
        owners=sample['owners']
        require(owners and all(row['dataset_id']=='\\x'+self.initialized['dataset_id'] for row in owners),
            'original Control owner identity differs')
        job_owner,=sample['job_owners']
        owner,=[row for row in owners if [row['dataset_id'],row['resource_id'],row['epoch'],row['process_incarnation']]==job_owner]
        require(all(row==owner or row['state']=='released' for row in owners),
            'unexpected live Control owner after original application death')
        require(owner['state'] in ('active','released') and type(sample['observed_at_us']) is int,
            'original Control lease observation differs')
        remaining=max(0,owner['lease_expires_at_us']-sample['observed_at_us'])
        # Retained's existing65s qualification allows5s to observe a60s lease
        # through SQL. Freeze this budget once; native census consumes it too.
        self.dead_control_lease=dict(format='bigbrotr-dead-control-lease/1',container_id=terminal['Id'],
            profile=profile,maximum_remaining_us=60_000_000,original_owner=owner,observation=sample,
            remaining_us=remaining,started_monotonic=started,
            deadline_monotonic=started+remaining/1_000_000+5)
        save(output/'dead-control-lease.json',self.dead_control_lease)
        require(remaining<=60_000_000,'dead original Control lease exceeds configured maximum')

    def wait_control_expiry(self,output):
        frozen=self.dead_control_lease
        deadline=frozen['deadline_monotonic']
        witness=dict(format='bigbrotr-dead-control-expiry/1',status='FAIL',
            started_monotonic=time.monotonic(),deadline_monotonic=deadline,
            lease_expires_at_us=frozen['original_owner']['lease_expires_at_us'],observations=0)
        last_clock=frozen['observation']['observed_at_us']
        def expired():
            nonlocal last_clock
            require(time.monotonic()<deadline,'dead original Control expiry observation budget exhausted')
            sample=self.control_lease_observation()
            witness.update(last_observation=sample,observations=witness['observations']+1)
            require(time.monotonic()<deadline,'dead original Control expiry observation budget exhausted')
            require(sample['owners']==frozen['observation']['owners']
                and sample['job_owners']==frozen['observation']['job_owners'],'dead original Control lease changed')
            require(type(sample['observed_at_us']) is int and sample['observed_at_us']>=last_clock,
                'dead original Control database clock regressed')
            last_clock=sample['observed_at_us']
            return sample['active_unexpired']==0 and last_clock>=witness['lease_expires_at_us']
        try:
            if not expired():
                remaining=deadline-time.monotonic()
                require(remaining>0,'dead original Control expiry observation budget exhausted')
                self.wait(expired,'dead original Control lease expiry',remaining)
            require(time.monotonic()<deadline,'dead original Control expiry observation budget exhausted')
            witness['status']='PASS'
        finally:
            witness['finished_monotonic']=time.monotonic()
            save(output/'dead-control-expiry.json',witness)

    def withdraw_after_empty_source(self,output):
        # One short owned-PG statement snapshots exact retained science before
        # deleting the actor's grant. The source backend has already completed
        # ROLLBACK/ReadyForQuery, so no socket-held SHARE lock is bypassed.
        held=self.proxy_event(self.route,'held',20)
        require(held['stage']=='source-rollback-reply' and held['source_queries_complete']==held['source_server_rollback']==1
            and held['source_client_rollback']==0,'actual completed source rollback must be held')
        wire=load(self.home/'relay-closed-001.json')
        require(wire['scenario']=='empty' and wire['frames']==['EOSE'] and wire['teardown']==dict(
            request_close=True,websocket_close=True,ack_sent=True,eof=True),'actual empty REQ/EOSE/teardown required')
        completed=self.proxy_event(self.route,'source-rollback-complete',10)
        require(completed['server_ready'] is True and completed['backend_pid']==held['backend_pid'],'actual source ReadyForQuery required')
        save(output/'completed-source-rollback.json',completed)
        save(output/'held-source-rollback.json',held)
        tables=('acquisition_quanta','producer_requests','producer_outcomes','producer_checkpoints',
            'producer_checkpoint_dependencies','acquisition_progress')
        fields=','.join("'"+name+"',(SELECT coalesce(jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',xmin::text)),'[]'::jsonb) FROM control."+name+" t)" for name in tables)
        statement="WITH before AS MATERIALIZED (SELECT jsonb_build_object("+fields+") value), removed AS ("             "DELETE FROM control.principal_grants g WHERE g.action='roles.control' "             "AND g.dataset_id=(SELECT dataset_id FROM control.acquisition_quanta) "             "AND g.principal_id=(SELECT j.principal_id FROM control.jobs j JOIN control.acquisition_quanta q USING(job_id)) "             "AND (SELECT value IS NOT NULL FROM before) RETURNING 1) "             "SELECT jsonb_build_object('before',(SELECT value FROM before),'removed',(SELECT count(*) FROM removed))"
        captured=json.loads(self.sql('bigbrotr_control',statement))
        require(captured['removed']==1,'exact actor grant withdrawal did not commit')
        self.empty_source_before=captured['before']
        plan,=self.empty_source_before['acquisition_quanta']
        require(plan['terminal_kind'] is None and plan['continuation_upper'] is None,'empty plan already gained terminal credit')
        require(len(self.empty_source_before['producer_requests'])==len(self.empty_source_before['producer_outcomes'])==3
            and not self.empty_source_before['acquisition_progress'],'empty source must precede any event outcome')
        save(output/'empty-authority-before.json',captured)
        self.proxy_send(self.route,'release\n')
        delivered=self.proxy_event(self.route,'source-rollback-delivered',10)
        require(delivered['source_server_rollback']==delivered['source_client_rollback']==1,'source rollback result not delivered')
        save(output/'source-rollback-delivered.json',delivered)

    def initial(self):
        if not self.quantum_fault:return super().initial()
        entry,output,setup=self.initial_start()
        self.wait(lambda:(self.home/'prepared.json').exists(),'prepared quantum fault source',15,entry,self.client)
        prepared,_=self.capture(output,'prepared')
        self.validate_source(self.current_owner_view(prepared,setup),self.initialized,load(self.home/'prepared.json'))
        (self.home/'prepared.continue').write_text('fresh exact source accepted\n')
        if self.quantum_fault=='empty-terminal-authority':
            self.withdraw_after_empty_source(output)
        elif self.quantum_fault in ('gap-write','stale-authority'):
            held=self.proxy_event(self.route,'held',20);save(output/'held-plan-reply.json',held)
            require(held['stage']=='reply','plan must be server committed before injection')
            if self.quantum_fault=='gap-write':
                self.prepare_gap_refusal(output)
            else:
                baseline=self.sql('bigbrotr_control',"SELECT lease_expires_at_us FROM control.owner_epochs WHERE role='control' AND state='active'")
                removed=self.sql('bigbrotr_control',"WITH removed AS (DELETE FROM control.principal_grants WHERE action='roles.control' RETURNING 1) SELECT count(*) FROM removed")
                require(removed=='1','one exact actor execution grant must be withdrawn')
            self.proxy_send(self.route,'release\n')
            if self.quantum_fault=='stale-authority':
                self.wait(lambda:self.inspect(entry)['State']['Running'] and int(self.sql('bigbrotr_control',
                    "SELECT lease_expires_at_us FROM control.owner_epochs WHERE role='control' AND state='active'"))>int(baseline),
                    'fresh owner progress after execution withdrawal',15)
                require(not list(self.home.glob('relay-open-*.json')),'stale actor opened a socket')
                self.stop_container(entry,output)
        else:
            fault=self.proxy_event(self.route,'fault',20);save(output/'commit-fault.json',fault)
        if self.quantum_fault!='stale-authority':
            self.wait(lambda:not self.inspect(entry)['State']['Running'],'failed quantum owner join',15)
        terminal=self.inspect(entry);self.logs(entry,output)
        expected_exit=0 if self.quantum_fault=='stale-authority' else 4
        require(terminal['State']['ExitCode']==expected_exit and not terminal['State']['OOMKilled'],'quantum refusal exit differs')
        if self.quantum_fault=='empty-terminal-authority':
            report=oracle.terminal_authority_diagnostic((output/'serve.stderr').read_text())
            save(output/'terminal-authority-diagnostic.json',report)
        save(output/'terminal-container.json',dict(id=terminal['Id'],state=terminal['State']))
        self.freeze_control_lease(terminal,output)
        self.fault_owner_joined(output)
        stop_process(self.client);self.processes.remove(self.client)
        self.stop_quantum_proxy()
        # The proxy is gone. Historical reads/new Control use the declared direct
        # database; no failed original operation is repeated through this route.
        self.config.write_text(self.set_value(self.config.read_text(),'postgres.control','endpoint','127.0.0.1:15432'))
        self.inputs['initial_recipe_sha256']=digest(self.config)

    def initial_archive_observation(self,entry,output):
        def complete():
            require(not (self.home/'relay-error.json').exists(),'finite linear peer failed')
            if not (self.home/'linear-peer-complete.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)||':'||count(*) FILTER(WHERE entry_kind='request' AND terminal_kind IS NULL) FROM control.acquisition_quanta")=='9:0'
        self.wait(complete,'nine terminal results and both bounded stops',30,entry,self.client)
        live,_=self.capture(output,'linear-live')
        require(len(live['jobs'])==len(live['acquisition_jobs'])==2,'two real accepted scopes required')

    def initial_source_quiet(self):
        return (self.home/'linear-peer-complete.json').exists()

    def verify_linear_quantum(self):
        if self.quantum_fault:return self.verify_fault()

        from installed_archive_oracle import expected
        from installed_sync_oracle import decode
        from cases.first_launch_restart import originals
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'after')
        self.backend_absence(output)
        capacity=json.loads(self.sql('bigbrotr_control',(self.source/'experiments/control-migrations/linear_quantum_capacity.sql').read_text()))
        require(capacity==dict(scope='isolated_sql_accounting_not_native_authority',positives=3,refusals=3,
            boundaries=dict(typed_head_and_quanta=14510080,no_head_and_quanta=14515200,no_head_zero_quanta=16612352)),
            'actual reservation trigger accounting boundaries differ')
        save(output/'capacity-probe.json',capacity)
        restored,_=self.capture(output,'capacity-rollback')
        require(restored==state,'rollback-only SQL accounting probe changed retained Control evidence')
        dump=native.census(self,output)
        actual=[(row['cf'],bytes.fromhex(row['key']),bytes.fromhex(row['value'])) for row in dump['rows']]
        facts={int.from_bytes(key[-8:],'big'):value for cf,key,value in actual if cf=='facts' and key[:2]==b'\x01\x20'}
        at=lambda number:decode(facts[number][34:].split(b'\0',1)[1])[4]
        inputs=originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)
        received=decode(inputs[3].split(b'\0',1)[1])[3][5][0]
        event=load(self.home/'event.jsonl')
        wanted=expected(state,self.initialized,event,at(4),received,at(8),1,native_options=dict(acknowledge=True))
        require(actual==wanted['native'] and inputs==wanted['inputs'],'complete independent native/input science differs')
        # Three dictionary operations13+9+21, one event17 and four ACK puts;
        # proofs, malformed input and empty/gap records add no native operation.
        require(dump['sequence']==67,'unexpected native sequence or synthetic producer mutation')
        wires=[load(path) for path in sorted(self.home.glob('relay-closed-*.json'))]
        rows=state['acquisition_quanta']
        refs=oracle.reference_bytes(oracle.receipt_refs(wanted['receipts'][3]))
        declarations={}
        commands=load(self.home/'submitted.json')
        selectors={item['body']['scope_id']:item['input']['selector'] for item in commands if item['path']=='acquisition-scopes'}
        declared={oracle.wire_json(value) for value in selectors.values()}
        require(declared=={oracle.wire_json(dict(version=1,filters=[{'kinds':[1],'until':86399}],history_since=0,ephemeral='retain')),
            oracle.wire_json(dict(version=1,filters=[{'kinds':[1],'since':1,'until':86399}],history_since=0,ephemeral='retain'))},'two literal collection declarations differ')
        for item in commands:
            if item['path']=='jobs':
                declarations[item['body']['job_id']]=dict(selector=selectors[item['input']['scope_id']],
                    scope_id=item['input']['scope_id'],accepted_at_us=item['body']['accepted_at_us'])
        totals=oracle.scenarios(state,wires,event,refs,declarations)
        statuses=load(self.home/'linear-status-observations.json')
        require({value['job_id'] for value in statuses}==set(declarations),'both known-plan actor statuses required')
        from installed_sync_client import quantum_status,quantum_window
        for value in statuses:
            declared=declarations[value['job_id']]
            scope_number=declared['selector']['filters'][0].get('since',0)
            upper=86400 if scope_number else 1
            count=3 if scope_number==0 else 6
            uppers={number:86400 if scope_number else (86400,2,1)[number-1] for number in range(1,count+1)}
            status=quantum_status(value,declared['scope_id'],declared['accepted_at_us'],known_plan=True,
                selector=declared['selector'],expected_uppers=uppers)
            job_rows=[row for row in rows if row['job_id']=='\\x'+value['job_id']]
            latest=max(job_rows,key=lambda row:row['quantum_number'])
            lower,upper,horizon,_=quantum_window(declared['selector'],declared['accepted_at_us'],expected_upper=upper)
            require(len(job_rows)==count and status==dict(format='bigbrotr-linear-quantum-status/2',quantum_number=str(count),
                scope_id=declared['scope_id'],entry_kind='request',disposition='window_closed',
                lower=str(lower),upper=str(upper),window_end='86400',horizon=str(horizon),window_width='86400',
                query_budget=128,queries_spent=count,attempted_prefix='86400',gap_count=0 if scope_number else 1,
                pending_work='settled',pending_interval=None,result='linear_observed',terminal='eose',gap=None,
                continuation_upper='86400',retained_events=0,classified_prefix=0,coverage='pending')
                and latest['terminal_kind']=='eose','known final stopped status differs')
        require(set(totals)=={job['job_id'] for job in state['jobs']},'both controlled scopes must receive finite turns')
        require(sum(totals.values())==5,'finite expected classification total differs')
        for head in state['acquisition_progress']:
            require(head['classified_events']==head['successful_events']==totals[head['job_id']] and head['rejected_events']==0,'independent per-job progress differs')
        head,=state['producer_checkpoints'];require(head['checkpoint_revision']==8 and head['complete_through_request']==4,'native completion or classified checkpoint differs')
        save(output/'verdict.json',dict(status='PASS',requests=9,scopes=2,terminal_records=9,pending_plans=0,
            classifications=5,mutation_requests=4,native_rows=len(actual),native_sequence=67,coverage='pending'))

    def verify_fault(self):
        from installed_sync_oracle import expected,decode
        from cases.first_launch_restart import originals
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'fault')
        dump=native.census(self,output,expected_writer_exit=0 if self.quantum_fault=='stale-authority' else 4)
        actual=[(row['cf'],bytes.fromhex(row['key']),bytes.fromhex(row['value'])) for row in dump['rows']]
        facts={int.from_bytes(key[-8:],'big'):value for cf,key,value in actual if cf=='facts' and key[:2]==b'\x01\x20'}
        at=lambda number:decode(facts[number][34:].split(b'\0',1)[1])[4]
        boundary,direction=self.quantum_fault.split('-') if self.quantum_fault not in ('gap-write','stale-authority','empty-terminal-authority') else ('terminal' if self.quantum_fault=='gap-write' else 'plan','known')
        requests=3 if boundary=='plan' else 4
        inputs=originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,requests)
        received=decode(inputs[3].split(b'\0',1)[1])[3][5][0] if boundary=='terminal' else None
        wanted,originals_expected,receipts=expected(state,self.initialized,load(self.home/'event.jsonl'),at(4),
            received,at(8) if boundary=='terminal' else None,1,acknowledge=True)
        require(actual==wanted and inputs==originals_expected,'complete independent fault native/input science differs')
        require(dump['sequence']==(49 if boundary=='plan' else 67),'fault dispatched extra native work')
        plans=state['acquisition_quanta']
        require(len(plans)==(0 if self.quantum_fault=='plan-before' else 1),'unknown COMMIT retried or invented a new quantum')
        if plans:
            plan,=plans
            require((plan['terminal_kind'] is not None)==(self.quantum_fault=='terminal-after'),'actual terminal commit direction differs')
        opened=list(self.home.glob('relay-open-*.json'))
        require(len(opened)==(1 if self.quantum_fault=='empty-terminal-authority' else 0 if boundary=='plan' else 1),'unknown COMMIT permitted a socket or next quantum')
        require(len(state['producer_requests'])==len(state['producer_outcomes'])==requests,'unknown metadata commit invented a producer request')
        if self.quantum_fault=='gap-write':
            wire=load(self.home/'relay-closed-001.json')
            require(plan==self.gap_plan_before,'failed gap UPDATE changed original pending plan/charge/cursor')
            job=next(job for job in state['jobs'] if job['job_id']==plan['job_id'])
            oracle.window(plan,job,wire)
            require(wire['scenario']=='body_closed' and wire['frames']==['EVENT','CLOSED']
                and wire['terminal_frame_hex']==oracle.wire_json(['CLOSED',wire['request'][1],'rate-limited']).hex(),
                'gap-write fault did not actually attempt a gap')
        if self.quantum_fault=='empty-terminal-authority':
            source_output=self.root/'initial'
            oracle.empty_authority(self.empty_source_before,state,load(self.home/'relay-closed-001.json'),
                load(source_output/'completed-source-rollback.json'),load(source_output/'held-source-rollback.json'),
                load(source_output/'source-rollback-delivered.json'))
        # Existing ownership is immutable. Wait for the positively dead owner's
        # actual lease expiry, then start ordinary Control for historical status;
        # this never adopts a same-job Sync/producer lifetime.
        self.historical_quantum_restart(state,output)
        save(output/'verdict.json',dict(status='PASS',fault=self.quantum_fault,requests=requests,
            native_sequence=dump['sequence'],plans=len(plans),socket_requests=len(opened),recovery='historical_only',coverage='pending'))

    def historical_quantum_restart(self,state,output):
        self.wait_control_expiry(output)
        entry=self.launch('quantum-historical','app','serve','--file','/etc/bigbrotr/instance.toml','--roles','control')
        self.wait(lambda:self.logs(entry,output),'historical Control restart',20,entry=entry)
        after,_=self.capture(output,'historical')
        for table in ('acquisition_quanta','producer_requests','producer_outcomes','producer_checkpoints',
            'producer_checkpoint_dependencies','acquisition_progress','jobs','acquisition_jobs'):
            require(after[table]==state[table],'historical restart mutated or adopted '+table)
        self.stop_container(entry,output)
        self.backend_absence(output)

    def cleanup(self):
        try:
            if hasattr(self,'route'):self.stop_quantum_proxy()
            if hasattr(self,'native_paths'):native.stop(self)
        finally:super().cleanup()
