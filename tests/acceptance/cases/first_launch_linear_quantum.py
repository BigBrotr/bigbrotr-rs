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
    'Optional exact plan/terminal COMMIT before-forward/after-complete schedules prove no next socket/quantum and historical-only restart, including missing plan versus retained pending/terminal record. NOT_RUN: full walker, age bands, NEG/live, OPEN65, same-job adoption, physical capacity. The stale actor schedule is before dialing and does not qualify every terminal authority race.',
]


class LinearQuantumRuntime(Runtime):
    def candidate(self):
        super().candidate()
        require(self.args.native_observer_build is not None,'linear quantum requires declared exact-source native observer')
        self.native_paths,self.native_hashes=native.artifact(self.args.native_observer_build,self.manifest)
        self.inputs['native_observer_artifacts_sha256']=self.native_hashes
        for name in ('tests/acceptance/cases/first_launch_linear_quantum.py',
            'tests/acceptance/cases/first_launch_quantum_oracle.py',
            'experiments/ingest-bootstrap/linear_quantum_transport.py'):
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
        if self.quantum_fault:
            boundary,direction=self.quantum_fault.split('-') if self.quantum_fault not in ('gap-write','stale-authority') else ('plan','held')
            path=self.source/'experiments/archive-engine/native_commit_proxy.py'
            self.inputs['acceptance_files_sha256'][str(path.relative_to(self.source))]=digest(path)
            self.route=self.proxy_process([sys.executable,'-B',str(path),'--mode',
                'hold-reply' if direction=='held' else 'before-forward' if direction=='before' else 'after-complete',
                '--target','control-quantum-'+('plan' if boundary=='plan' else 'terminal'),
                '--upstream-port','15432'],input_pipe=True)
            ready=self.proxy_event(self.route,'ready',10)
            require(ready['source_sha256']==digest(path),'exact quantum proxy source')
            self.config.write_text(self.set_value(self.config.read_text(),'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            self.inputs['initial_recipe_sha256']=digest(self.config)
            self.resources['quantum_proxy']=dict(pid=self.route.process.pid,status='running',port=ready['port'])
            self.record_resources()
        super().bootstrap()

    def stop_quantum_proxy(self):
        if not hasattr(self,'route') or self.resources['quantum_proxy']['status']=='joined':return
        self.proxy_send(self.route,'stop\n');require(self.route.wait(5)==0,'quantum proxy did not join')
        result=self.proxy_observation(self.route);save(self.root/'quantum-proxy.json',result)
        require(not result['stderr'] and not result['overflow'],'incomplete quantum proxy observation')
        self.resources['quantum_proxy']['status']='joined';self.record_resources()

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
        if self.quantum_fault in ('gap-write','stale-authority'):
            held=self.proxy_event(self.route,'held',20);save(output/'held-plan-reply.json',held)
            require(held['stage']=='reply','plan must be server committed before injection')
            if self.quantum_fault=='gap-write':
                self.sql('bigbrotr_control',"ALTER TABLE control.acquisition_quanta ADD CONSTRAINT fixture_gap_write_refusal CHECK(terminal_kind IS NULL) NOT VALID")
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
        expected=(('eose',1,1),('eose',2,0),('frame_cap',0,0),('event_cap',3,3),
            ('closed',0,0),('deadline',0,0),('eose',0,0),('eose',1,1))
        for wire,(terminal,retained,classified) in zip(wires,expected):
            row=by_request[wire['request'][1]];records=oracle.effects(row['effect_records'])
            require((row['terminal_kind'],row['retained_events'],row['classified_prefix'])==(terminal,retained,classified),'actual terminal or classified prefix differs')
            require(len(records)==classified,'unclassified suffix gained an outcome')
            require(oracle.raw(row['dependencies'])==(refs if classified else b''),'per-quantum proof union differs')
            for ordinal,record in enumerate(records,1):
                kind,request=(2,4) if wire['number']==1 else (5,0)
                require(record==(ordinal,ordinal,hashlib.sha256(oracle.wire_json(event)).digest(),kind,request,event['created_at'],1),'independent actual classification differs')
            count=totals.setdefault(row['job_id'],0);totals[row['job_id']]=count+classified
            require(row['continuation_upper']==(2 if classified and terminal=='eose' else 86400),'pending seconds overlap differs')
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
        boundary,direction=self.quantum_fault.split('-') if self.quantum_fault not in ('gap-write','stale-authority') else ('terminal' if self.quantum_fault=='gap-write' else 'plan','known')
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
        require(len(opened)==(0 if boundary=='plan' else 1),'unknown COMMIT permitted a socket or next quantum')
        require(len(state['producer_requests'])==len(state['producer_outcomes'])==requests,'unknown metadata commit invented a producer request')
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
