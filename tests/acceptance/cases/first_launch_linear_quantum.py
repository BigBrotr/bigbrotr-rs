"""Two current scopes, finite real socket turns and durable refusal obligations."""
import hashlib
import json
from pathlib import Path
import sys
import subprocess
import time

from cases.first_launch import Runtime,load,save,require,digest,stop_process
from cases import first_launch_quantum_oracle as oracle
from cases import first_launch_retained_native as native

STAGES=('prerequisites','candidate','runtime_image','fresh_bootstrap','signed_acquisition','linear_quantum','cleanup')
LIMITS=[
    'Two original finite scopes share the current Control/Sync/native owner. Eight real requests exercise slow EVENT/EOSE, malformed EVENT then EOSE, frame/event caps then EOSE, CLOSED, deadline, empty and proof-only work.',
    'The finite listener closes after8; two charged unfinished plans remain pending. An actual source refusal neither completes a traversal nor synthesizes a producer request. All original input bytes, full native rows/sequence and independently expected classification/dependency records are compared after positive owner join.',
    'Optional exact plan/terminal COMMIT before-forward/after-complete schedules prove no next socket/quantum and historical-only restart, including missing plan versus retained pending/terminal record. NOT_RUN: full walker, age bands, NEG/live, OPEN65, same-job adoption, physical capacity. Empty post-REQ authority withdrawal is isolated by the actual completed source-rollback reply; broader terminal races remain separate.',
]


class LinearQuantumRuntime(Runtime):
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

    def image_and_state(self):
        super().image_and_state()
        from linear_quantum_transport import PROFILE
        save(self.home/'linear-quantum-profile.json',PROFILE)

    def bootstrap(self):
        self.quantum_fault=getattr(self.args,'linear_quantum_fault',None)
        if self.quantum_fault:save(self.home/'linear-quantum-fault.json',self.quantum_fault)
        if self.quantum_fault:
            boundary,direction=self.quantum_fault.split('-') if self.quantum_fault not in ('gap-write','stale-authority','empty-terminal-authority') else ('plan','held')
            path=self.source/'experiments/archive-engine/native_commit_proxy.py'
            self.inputs['acceptance_files_sha256'][str(path.relative_to(self.source))]=digest(path)
            self.route=self.proxy_process([sys.executable,'-B',str(path),'--mode',
                'hold-source-rollback-reply' if self.quantum_fault=='empty-terminal-authority' else
                'hold-reply' if direction=='held' else 'before-forward' if direction=='before' else 'after-complete',
                '--target','control-quantum-source-rollback' if self.quantum_fault=='empty-terminal-authority' else
                'control-quantum-'+('plan' if boundary=='plan' else 'terminal'),
                '--upstream-port','15432'],input_pipe=True)
            self.resources['quantum_proxy']=dict(pid=self.route.process.pid,status='running',port=None)
            self.record_resources()
            save(self.root/'quantum-proxy-process.json',dict(pid=self.route.process.pid))
            ready=self.proxy_event(self.route,'ready',10)
            require(ready['source_sha256']==digest(path),'exact quantum proxy source')
            self.resources['quantum_proxy']['port']=ready['port']
            self.record_resources()
            self.config.write_text(self.set_value(self.config.read_text(),'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            self.inputs['initial_recipe_sha256']=digest(self.config)
        super().bootstrap()

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
        except Exception as error:failures.append(('observation',error))
        self.resources['quantum_proxy'].update(status='joined' if code is not None and streams_joined else 'cleanup-incomplete',exit_code=code)
        self.resources['cleanup_errors'].extend(dict(resource='quantum-proxy',id=route.process.pid,
            operation=operation,error=type(error).__name__) for operation,error in failures)
        try:self.record_resources()
        except Exception:
            if failures:raise failures[0][1]
            raise
        if failures:raise failures[0][1]

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
        output=self.root/'initial';output.mkdir(mode=0o700)
        entry=self.initial_launch()
        self.initialized=self.wait(lambda:self.logs(entry,output),'initial quantum fault owner',20,entry=entry)[0]
        save(output/'initialized.json',self.initialized)
        save(self.home/'visibility.json',self.initialized['private_scope'])
        with (self.home/'client.stdout').open('x') as out,(self.home/'client.stderr').open('x') as err:
            self.client=subprocess.Popen([sys.executable,'-B',str(self.source/'experiments/ingest-bootstrap/installed_archive_client.py'),str(self.home)],
                env=self.environment,cwd=self.root,stdout=out,stderr=err,start_new_session=True)
        self.processes.append(self.client)
        self.wait(lambda:(self.home/'prepared.json').exists(),'prepared quantum fault source',15,entry,self.client)
        (self.home/'prepared.continue').write_text('fresh exact source accepted\n')
        if self.quantum_fault=='empty-terminal-authority':
            self.withdraw_after_empty_source(output)
        elif self.quantum_fault in ('gap-write','stale-authority'):
            held=self.proxy_event(self.route,'held',20);save(output/'held-plan-reply.json',held)
            require(held['stage']=='reply','plan must be server committed before injection')
            if self.quantum_fault=='gap-write':
                captured=self.sql('bigbrotr_control',"SELECT to_jsonb(q)||jsonb_build_object('xmin',xmin::text) FROM control.acquisition_quanta q; "
                    "ALTER TABLE control.acquisition_quanta ADD CONSTRAINT fixture_gap_write_refusal CHECK(gap_reason IS NULL) NOT VALID")
                self.gap_plan_before=json.loads(captured)
                save(output/'gap-plan-before.json',self.gap_plan_before)
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
            return self.sql('bigbrotr_control',"SELECT count(*) FROM control.acquisition_quanta WHERE terminal_kind IS NULL")=='2'
        self.wait(complete,'eight terminal results and two pending refusals',30,entry,self.client)
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
        from linear_quantum_transport import SCENARIOS
        require([wire['scenario'] for wire in wires]==list(SCENARIOS),'finite socket scenarios differ')
        rows=state['acquisition_quanta'];oracle.shape(state)
        pending=[row for row in rows if row['terminal_kind'] is None]
        require(len(rows)==10 and len(pending)==2 and len({row['job_id'] for row in pending})==2,'unfinished plans or finite turn count differs')
        by_request={oracle.raw(row['request_id']).decode():row for row in rows if row['request_id'] is not None}
        refs=oracle.reference_bytes(oracle.receipt_refs(wanted['receipts'][3]));totals={}
        declarations={}
        commands=load(self.home/'submitted.json')
        selectors={item['body']['scope_id']:item['input']['selector'] for item in commands if item['path']=='acquisition-scopes'}
        declared={oracle.wire_json(value) for value in selectors.values()}
        require(declared=={oracle.wire_json(dict(version=1,filters=[{'kinds':[1]}],history_since=0,ephemeral='retain')),
            oracle.wire_json(dict(version=1,filters=[{'kinds':[1],'until':86399}],history_since=0,ephemeral='retain'))},'two literal collection declarations differ')
        for item in commands:
            if item['path']=='jobs':
                declarations[item['body']['job_id']]=dict(selector=selectors[item['input']['scope_id']],
                    scope_id=item['input']['scope_id'],accepted_at_us=item['body']['accepted_at_us'])
        for wire in wires:
            row=by_request[wire['request'][1]];job=next(job for job in state['jobs'] if job['job_id']==row['job_id'])
            declared=declarations[job['job_id'][2:]]
            require(row['scope_id']=='\\x'+declared['scope_id'],'quantum declaration identity differs')
            classified=oracle.scenario(row,wire,job,event,refs,selector=declared['selector'],accepted_at_us=declared['accepted_at_us'])
            totals[row['job_id']]=totals.get(row['job_id'],0)+classified
        statuses=load(self.home/'linear-status-observations.json')
        require({value['job_id'] for value in statuses}==set(declarations),'both known-plan actor statuses required')
        from installed_sync_client import quantum_status,quantum_window
        for value in statuses:
            declared=declarations[value['job_id']]
            actual=quantum_status(value,declared['scope_id'],declared['accepted_at_us'],known_plan=True,selector=declared['selector'])
            job_rows=[row for row in rows if row['job_id']=='\\x'+value['job_id']]
            latest=max(job_rows,key=lambda row:row['quantum_number'])
            lower,upper,horizon,_=quantum_window(declared['selector'],declared['accepted_at_us'])
            require(actual==dict(format='bigbrotr-linear-quantum-status/1',quantum_number=str(len(job_rows)),
                scope_id=declared['scope_id'],lower=str(lower),upper=str(upper),horizon=str(horizon),result='pending',
                terminal=None,gap=None,continuation_upper=None,retained_events=None,classified_prefix=None,coverage='pending')
                and latest['terminal_kind'] is None,'known final pending status differs')
        require(set(totals)=={job['job_id'] for job in state['jobs']},'both controlled scopes must receive finite turns')
        require(sum(totals.values())==5,'finite expected classification total differs')
        for head in state['acquisition_progress']:
            require(head['classified_events']==head['successful_events']==totals[head['job_id']] and head['rejected_events']==0,'independent per-job progress differs')
        head,=state['producer_checkpoints'];require(head['checkpoint_revision']==8 and head['complete_through_request']==4,'native completion or classified checkpoint differs')
        save(output/'verdict.json',dict(status='PASS',requests=8,scopes=2,terminal_records=8,pending_plans=2,
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
        self.wait(lambda:self.sql('bigbrotr_control',"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active' AND lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint")=='0',
            'dead original Control lease expiry',20)
        entry=self.launch('quantum-historical','app','serve','--file','/etc/bigbrotr/instance.toml','--roles','control')
        self.wait(lambda:self.logs(entry,output),'historical Control restart',20,entry=entry)
        after,_=self.capture(output,'historical')
        for table in ('acquisition_quanta','producer_requests','producer_outcomes','producer_checkpoints',
            'producer_checkpoint_dependencies','acquisition_progress','jobs','acquisition_jobs'):
            require(after[table]==state[table],'historical restart mutated or adopted '+table)
        self.stop_container(entry,output)
        self.backend_absence(output)
        save(output/'verdict.json',dict(status='PASS',fault=self.quantum_fault,requests=requests,
            native_sequence=dump['sequence'],plans=len(plans),socket_requests=len(opened),recovery='historical_only',coverage='pending'))

    def cleanup(self):
        try:
            if hasattr(self,'route'):self.stop_quantum_proxy()
            if hasattr(self,'native_paths'):native.stop(self)
        finally:super().cleanup()
