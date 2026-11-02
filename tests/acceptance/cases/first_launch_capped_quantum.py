"""Installed capped continuation with a delivered prepopulation gate."""
import shutil
import time
import tomllib
import json

from cases.first_launch import Runtime,load,save,require,digest,stop_process
from cases.first_launch_linear_quantum import LinearQuantumRuntime
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_capped_oracle as oracle
from cases import first_launch_retained_native as native
from cases import first_launch_restart as source
from cases import first_launch_capped_corruption as corruption

LIMITS=[
    'One private relay caps requested3 to at most2. A admits24/23; B continues past an all-known page to alternate23 and older1. Repeated boundaries retain gaps, A closes a selector-excluded remainder, and B queries its older empty remainder.',
    'Exactly6 sockets and7 settled history entries, no extra plan/socket,7 original producer inputs/receipts and the full independent native census. The pre-B gate independently verifies the delivered A input/checkpoint/history state.',
    'This finite same-owner profile does not qualify the full500-event walker, partitions, sparse growth, adoption, physical capacity or Archive publication/reads.',
    'Optional mutations affect B-page2 while preserving the complete history geometry. Only the two source-supported native-integrity errors at quantum-plan qualify; restored guards, natural owner exit and unchanged native/input/receipt data are required.',
]

ADOPTION_LIMITS=[
    'NOT_RUN until installed execution: one accepted finite job crosses an actual stopped initial owner into a locally minted activated successor. The old Q2 plan COMMIT is lost before forwarding; independent SQL must prove Q2 absent after process/backend exclusion.',
    'Five literal REQs [0,86400),[0,24),[0,23),[0,2),[0,1) deliver24/23,known23,older1,repeat1,empty. Three unique signed bodies, five EVENT observations, both unresolved same-second gaps, original128-query budget/deadline and no sixth socket are required.',
    'Full old inputs, native facts/receipts, historical SQL rows/xmins and immutable source/principal/policy are preserved. Sealing cannot change job ownership; one exact queued rev2 to rev3/current-Control CAS and actual native handoff/adoption are checked.',
    'This bounded initial-to-successor case does not qualify negative installed refusal schedules, repeated successor loss, OPEN65, full500-event walker, partitions, sparse growth, fleet capacity, Archive publication or future CLI coverage.',
]


class CappedQuantumRuntime(LinearQuantumRuntime):
    def candidate(self):
        super().candidate()
        self.inputs['linear_quantum_scenario']='capped-continuation'
        names=('experiments/ingest-bootstrap/linear_capped_transport.py',
            'experiments/ingest-bootstrap/linear_capped_client.py',
            'tests/acceptance/cases/first_launch_capped_quantum.py',
            'tests/acceptance/cases/first_launch_capped_oracle.py',
            'tests/acceptance/cases/first_launch_capped_corruption.py',
            'tests/acceptance/cases/first_launch_quantum_science.py')
        for name in names:self.inputs['acceptance_files_sha256'][name]=digest(self.source/name)
        from linear_capped_transport import IMPORT_SHA256,REFUSAL_SHA256,WINDOW_SHA256
        self.inputs['capped_corpus_sha256']={name:digest(self.source/name) for name in (
            'tests/fixtures/acquisition/selector-events.json','tests/fixtures/ingest/import-event.jsonl',
            'tests/fixtures/acquisition/linear-window-events.json')}
        require(list(self.inputs['capped_corpus_sha256'].values())==[REFUSAL_SHA256,IMPORT_SHA256,WINDOW_SHA256],'capped corpus identity differs')

    def load_helpers(self):
        super().load_helpers()
        from installed_sync import validate_source
        from cases.first_launch_tutorial import owned_listeners,loopback_listener
        import linear_capped_transport
        import linear_capped_client
        self.owned_listeners,self.loopback_listener=owned_listeners,loopback_listener
        self.validate_source=lambda state,report,commands:validate_source(state,report,commands,selector_document=oracle.SELECTORS[0])

    def image_and_state(self):
        Runtime.image_and_state(self)
        from linear_capped_transport import PROFILE,corpus
        shutil.copyfile(self.source/'tests/fixtures/acquisition/selector-events.json',self.home/'selector-events.json')
        shutil.copyfile(self.source/'tests/fixtures/acquisition/linear-window-events.json',self.home/'linear-window-events.json')
        save(self.home/'linear-capped-profile.json',PROFILE)
        self.events=corpus(self.home)

    def source_unchanged(self):
        super().source_unchanged()
        for name,expected in self.inputs['capped_corpus_sha256'].items():
            require(digest(self.source/name)==expected,'source capped corpus changed')
        require(digest(self.home/'selector-events.json')==self.inputs['capped_corpus_sha256']['tests/fixtures/acquisition/selector-events.json'],
            'copied capped corpus changed')
        require(digest(self.home/'linear-window-events.json')==self.inputs['capped_corpus_sha256']['tests/fixtures/acquisition/linear-window-events.json'],
            'copied capped window corpus changed')

    def initial_launch(self):
        return self.launch('initial','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,sync','--initialize-ingest')

    def initial_submission(self,entry,output):
        self.wait(lambda:(self.home/'capped-prepopulated.json').exists(),'prepopulation command barrier',15,entry,self.client)
        def completed():
            require(not (self.home/'relay-error.json').exists(),'capped prepopulation relay failed')
            require(not (self.home/'relay-open-003.json').exists(),'B socket preceded independent prepopulation gate')
            if not (self.home/'relay-closed-002.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=3 AND bool_and(disposition IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(completed,'two delivered A pages and local exclusion before B',15,entry,self.client)
        state,_=self.capture(output,'capped-prepopulated')
        wires=self.wires(2);commands=load(self.home/'capped-prepopulated.json')
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,5)
        verdict=oracle.prepopulation(self.source_view(state),wires,self.events,commands,inputs,self.initialized,principal=self.enrolled['principal_id'])
        save(output/'capped-prepopulated-verdict.json',verdict)
        self.prepopulated=state
        self.prepopulation_gate=dict(format='bigbrotr-capped-prepopulation-release/1',
            verdict_sha256=digest(output/'capped-prepopulated-verdict.json'),released_at_us=time.time_ns()//1000)
        save(self.home/'capped-prepopulated.continue',self.prepopulation_gate)
        Runtime.initial_submission(self,entry,output)

    def initial_archive_observation(self,entry,output):
        def completed():
            require(not (self.home/'relay-error.json').exists(),'capped continuation relay failed')
            if not (self.home/'capped-peer-complete.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=7 AND bool_and(disposition IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(completed,'six completed capped requests and local exclusion',30,entry,self.client)
        state,_=self.capture(output,'capped-completed')
        require(len(state['producer_requests'])==len(state['producer_outcomes'])==7,'capped continuation mutation count differs')
        self.wires(6)

    def initial_publication(self,entry,output):
        completion=load(self.home/'capped-peer-complete.json')
        require(completion['requests']==6 and completion['listener_closed'] is False and completion['listening'] is True,
            'capped relay disappeared at the stopping boundary')
        require(self.client.poll() is None,'capped client ended before stop')
        listener=self.owned_listeners(self.client.pid);self.loopback_listener(listener,7447)
        save(output/'capped-live-listener.json',dict(completion=completion,process=listener))

    def initial_source_quiet(self):
        require(not (self.home/'relay-error.json').exists(),'capped relay failed before original owner stop')
        require(not (self.home/'relay-open-007.json').exists(),'stopped capped jobs opened another socket')
        return self.sql('bigbrotr_control',"SELECT count(*)=7 AND bool_and(disposition IS NOT NULL) FROM control.acquisition_quanta")=='t'

    def initial(self):
        profile=getattr(self.args,'linear_quantum_corruption',None)
        if profile is None:return super().initial()
        self.inputs['linear_quantum_corruption']=profile
        require(tomllib.loads(self.config.read_text()).get('runtime',{}).get('lease_ttl','60s')=='60s',
            'corruption fixture requires the unchanged 60s owner lease profile')
        entry,output,setup=self.initial_start()
        self.initial_commands(entry,output,setup)
        baseline,_=self.capture(output,'corruption-baseline');view=self.source_view(baseline)
        wires=self.wires(6);commands=load(self.home/'submitted.json')
        oracle.source_rows(view,self.initialized,commands,principal=self.enrolled['principal_id'])
        receipts=[source.raw(row['outcome_bytes']) for row in sorted(view['producer_outcomes'],key=lambda row:row['request_sequence'])]
        oracle.quanta(view,wires,self.events,receipts,commands)
        oracle.statuses(load(self.home/'linear-status-observations.json'),commands)
        self.corruption_inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,7)
        self.corruption_input_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        self.corruption_baseline=baseline
        old,expected=corruption.mutation(view,self.events,profile)
        active=self.inspect(entry)
        require(active['State']['Running'] and active['State']['Pid']>0,'corruption must target the original live owner')
        save(output/'corruption-live-owner.json',dict(id=active['Id'],state=active['State']))
        text=self.sql('bigbrotr_control',corruption.mutation_sql(old,expected))
        (output/'corruption-transaction.stdout').write_text(text)
        changed=corruption.mutation_receipt(text,old,expected)
        self.corruption_rows=(old,changed)
        save(output/'corruption-mutation.json',dict(profile=profile,before=old,after=changed,triggers=corruption.TRIGGERS))
        self.wait(lambda:not self.inspect(entry)['State']['Running'],'natural corrupted quantum planning refusal',15)
        terminal=self.inspect(entry);self.logs(entry,output)
        report=corruption.natural_refusal(terminal['State'],(output/'serve.stderr').read_text(),
            harness_stop=(self.home/'stop-requested').exists())
        save(output/'terminal-container.json',dict(id=terminal['Id'],state=terminal['State']))
        self.corruption_observed_at_us=int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        save(output/'corruption-natural-refusal.json',dict(diagnostic=report,observed_at_us=self.corruption_observed_at_us,
            container_state=terminal['State'],harness_stop_before_refusal=False))
        after,_=self.capture(output,'corruption-after')
        modes=load_modes(self)
        require(modes==corruption.TRIGGERS,'corruption fixture trigger modes were not restored after COMMIT')
        save(output/'corruption-restored-triggers.json',modes)
        corruption.preserved(baseline,after,old,changed,self.corruption_observed_at_us)
        (self.home/'commands-finished.continue').write_text('natural refusal observed; original owner already joined\n')
        require(self.client.wait(timeout=6)==0 and not (self.home/'client.stderr').read_bytes(),'corruption client did not join cleanly')
        stop_process(self.client);self.processes.remove(self.client)
        require(load(self.home/'client-result.json')['status']=='PASS','missing corruption client baseline result')
        self.wires(6);self.backend_absence(output)

    def verify_valid_state(self,state,wires,science,commands,completed):
        view=self.source_view(state)
        declared=oracle.source_rows(view,self.initialized,commands,principal=self.enrolled['principal_id'])
        require(int(declared[1]['accepted_at_us'])>=self.prepopulation_gate['released_at_us']>=wires[1]['closed_at_us'],
            'B acceptance preceded verified A prepopulation')
        quantum.transition(self.prepopulated,state)
        require(all(row in state['producer_requests'] for row in self.prepopulated['producer_requests'])
            and all(row in state['producer_outcomes'] for row in self.prepopulated['producer_outcomes']),
            'prepopulated native-confirmed inputs/outcomes changed')
        oracle.quanta(view,wires,self.events,science['receipts'],commands)
        oracle.statuses(load(self.home/'linear-status-observations.json'),commands)
        first=source.attribution(view,'b1')[2];second=source.attribution(view,'b2')[2]
        bindings={4:source.request_binding(self.events[0],wires[0],1),5:source.request_binding(self.events[1],wires[0],2),
            6:source.request_binding(self.events[3],wires[3],1),7:source.request_binding(self.events[2],wires[3],2)}
        source.requests(view,1,science['manifest'],science['inputs'],science['receipts'],
            source.only(view['owner_epochs'],role='control'),bindings,{4:first,5:first,6:second,7:second})
        oracle.progress(view,self.initialized,wires,self.events,science,completed)

    def verify_linear_quantum(self):
        if getattr(self.args,'linear_quantum_corruption',None) is not None:return self.verify_corruption()
        self.source_unchanged()
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'after');self.backend_absence(output)
        view=self.source_view(state)
        wires=self.wires(6);oracle.wires_checked(wires,self.events)
        joined=load(self.home/'capped-peer-joined.json')
        require(joined['requests']==6 and joined['listener_closed'] is True and joined['errors']==0,'capped peer did not join cleanly')
        dump=native.census(self,output)
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,7)
        completed=time.time_ns()//1000
        science=oracle.science(view,self.initialized,self.events,wires,dump,inputs,completed)
        commands=load(self.home/'submitted.json')
        self.verify_valid_state(state,wires,science,commands,completed)
        save(output/'verdict.json',dict(status='PASS',scenario='capped-continuation',requests=6,settled_history_entries=7,terminal_requests=6,local_exclusions=1,
            pending_plans=0,mutation_requests=7,classified_events=8,native_rows=len(science['native']),native_sequence=121,
            exclusive_uppers=dict(A=[86400,24],B=[86400,24,2,1]),coverage='pending',
            all_known_first_page_continued=True,older_event_retained=self.events[2]['id']))

    def verify_corruption(self):
        self.source_unchanged()
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'corruption-final');self.backend_absence(output)
        old,changed=self.corruption_rows
        corruption.preserved(self.corruption_baseline,state,old,changed,self.corruption_observed_at_us)
        require(load_modes(self)==corruption.TRIGGERS,'restored corruption triggers changed')
        wires=self.wires(6)
        joined=load(self.home/'capped-peer-joined.json')
        require(joined['requests']==6 and joined['listener_closed'] is True and joined['errors']==0,'corruption peer did not join cleanly')
        dump=native.census(self,output,expected_writer_exit=4)
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,7)
        require(inputs==self.corruption_inputs and self.files(self.state/'data/ingest/producer-inputs-v1')==self.corruption_input_files,
            'corruption refusal changed original input custody')
        completed=time.time_ns()//1000
        science=oracle.science(self.source_view(self.corruption_baseline),self.initialized,self.events,wires,dump,inputs,completed)
        self.verify_valid_state(self.corruption_baseline,wires,science,load(self.home/'submitted.json'),completed)
        body=corruption.native_foreign_payload(science,self.events)
        save(output/'verdict.json',dict(status='PASS',scenario='capped-continuation',corruption=self.args.linear_quantum_corruption,
            natural_refusal=load(self.root/'initial/corruption-natural-refusal.json')['diagnostic']['native_refusal'],harness_stop_before_refusal=False,settled_history_entries=7,terminal_requests=6,local_exclusions=1,pending_plans=0,
            mutation_requests=7,native_rows=len(science['native']),native_sequence=121,native_foreign_payload=body,
            trigger_modes=corruption.TRIGGERS,coverage='pending'))


def load_modes(runtime):
    import json
    return json.loads(runtime.sql('bigbrotr_control',"SELECT jsonb_agg(jsonb_build_object('name',tgname,'enabled',tgenabled) ORDER BY tgname) "
        "FROM pg_trigger WHERE tgrelid='control.acquisition_quanta'::regclass "
        "AND tgname IN ('acquisition_quantum_guard','zzz_acquisition_quantum_owner')"))


class CappedAdoptionRuntime(CappedQuantumRuntime):
    """The capped family with an actual settled-owner lifecycle, not a pause."""
    def quantum_proxy_ordinal(self):
        return 2

    def candidate(self):
        super().candidate()
        self.inputs['linear_quantum_scenario']='settled-owner-adoption'
        path='tests/fixtures/acquisition/settled-owner-adoption.json'
        self.inputs['adoption_expectation_sha256']=digest(self.source/path)
        oracle.adoption_manifest(load(self.source/path))

    def load_helpers(self):
        super().load_helpers()
        from installed_sync import validate_source
        self.validate_source=lambda state,report,commands:validate_source(state,report,commands,selector_document=oracle.ADOPTION_SELECTOR)

    def image_and_state(self):
        Runtime.image_and_state(self)
        from linear_capped_transport import ADOPTION_PROFILE,corpus
        for name in ('selector-events.json','linear-window-events.json'):
            shutil.copyfile(self.source/'tests/fixtures/acquisition'/name,self.home/name)
        save(self.home/'linear-capped-adoption-profile.json',ADOPTION_PROFILE)
        self.events=corpus(self.home)[:3]

    def initial(self):
        require(self.quantum_fault=='plan-before','adoption requires the declared before-forward Q2 COMMIT fault')
        entry,output,setup=self.initial_start()
        self.wait(lambda:(self.home/'prepared.json').exists(),'prepared adoption source',15,entry,self.client)
        prepared,_=self.capture(output,'prepared')
        self.validate_source(self.current_owner_view(prepared,setup),self.initialized,load(self.home/'prepared.json'))
        (self.home/'prepared.continue').write_text('exact authenticated original source checked\n')
        self.wait(lambda:(self.home/'adoption-old-cut.json').exists(),'single accepted acquisition',15,entry,self.client)
        fault=self.proxy_event(self.route,'fault',20);save(output/'commit-fault.json',fault)
        self.wait(lambda:not self.inspect(entry)['State']['Running'],'old adoption owner join',15)
        terminal=self.inspect(entry);self.logs(entry,output)
        require(terminal['State']['ExitCode']==4 and not terminal['State']['OOMKilled'],'old adoption owner fault differs')
        save(output/'terminal-container.json',dict(id=terminal['Id'],state=terminal['State']))
        self.freeze_control_lease(terminal,output)
        self.stop_quantum_proxy()
        proxy=load(self.root/'quantum-proxy-terminal.json')
        require(proxy['mode']=='before-forward' and proxy['target']=='control-quantum-plan' and proxy['commit_ordinal']==2
            and proxy['matched_commits']==2 and proxy['selected_commits']==1 and proxy['skipped_commits']==1
            and proxy['selected_server_complete']==proxy['selected_client_complete']==0 and proxy['faults']==1,
            'before-forward Q2 plan fault was not reached exactly')
        self.config.write_text(self.set_value(self.config.read_text(),'postgres.control','endpoint','127.0.0.1:15432'))
        self.inputs['initial_recipe_sha256']=digest(self.config)
        self.backend_absence(output)
        self.adoption_before,_=self.capture(output,'settled-old-cut')
        oracle.adoption_source(self.source_view(self.adoption_before),self.initialized,load(self.home/'adoption-old-cut.json'),
            principal=self.enrolled['principal_id'])
        require(self.sql('bigbrotr_control',"SELECT count(*)=1 AND max(quantum_number)=1 "
            "AND bool_and(disposition='overlap' AND continuation_upper=24) FROM control.acquisition_quanta")=='t',
            'Q2 must be independently absent after old backend exclusion')
        wires=self.wires(1);oracle.adoption_wires(wires,self.events,old_cut=True)
        require(self.client.poll() is None,'old process loss closed the independent peer')
        self.loopback_listener(self.owned_listeners(self.client.pid),7447)
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,5)
        self.adoption_original_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        dump=native.census(self,output,expected_writer_exit=4)
        from cases.first_launch_quantum_science import initial_science
        self.adoption_first=initial_science(self.adoption_before,self.initialized,self.events,wires,dump,inputs,time.time_ns()//1000,
            mutations=((0,4,0,8),(1,5,0,13)),first_accepted_at_us=self.adoption_before['jobs'][0]['created_at_us'],native_sequence=85)
        oracle.adoption_history(self.adoption_before,wires,self.events,self.adoption_first)
        oracle.adoption_progress(self.adoption_before,self.initialized,self.events,wires,self.adoption_first,time.time_ns()//1000,epoch=1)
        self.wait_control_expiry(output)
        args=('ingest','recover','--file','/etc/bigbrotr/instance.toml')
        seal=self.finite('seal-acquisition',*args,'--seal-acquisition')
        save(output/'seal-report.json',seal)
        self.adoption_sealed,_=self.capture(output,'sealed')
        self.backend_absence(output);self.wires(1)
        closed_output=output/'seal-native';closed_output.mkdir(mode=0o700)
        closed=native.census(self,closed_output,expected_writer_exit=4)
        oracle.adoption_closed_native(self.adoption_first,'\\x'+self.initialized['producer_slot_id'],closed)
        retired=self.finite('retire-sealed-initial',*args,'--retire-initial')
        save(output/'retirement-report.json',retired)
        self.adoption_retired,_=self.capture(output,'retired')
        self.backend_absence(output);self.wires(1)
        successor_output=self.root/'adoption-successor';successor_output.mkdir(mode=0o700)
        successor=self.launch('adoption-successor','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--activate-initial-successor','--roles','control,ingest,sync')
        started=self.wait(lambda:self.logs(successor,successor_output),'actual adoption successor',20,entry=successor)[0]
        require(started['state']=='activated' and started['producer_epoch']=='2' and started['acquisition']=='finite-initial-successor'
            and started['config_revision']==started['policy_revision']=='1' and started['ready'] is False,
            'actual locally activated successor differs')
        self.adoption_started=started;save(successor_output/'initialized.json',started)
        def settled():
            require(not (self.home/'relay-error.json').exists(),'adoption relay failed')
            require(not (self.home/'relay-open-006.json').exists(),'adoption opened a sixth socket')
            if not (self.home/'capped-adoption-peer-complete.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=5 AND bool_and(disposition IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(settled,'five globally numbered settled acquisition queries',20,successor,self.client)
        self.adoption_live,_=self.capture(successor_output,'settled')
        self.wires(5)
        completion=load(self.home/'capped-adoption-peer-complete.json')
        require(completion['requests']==5 and completion['listening'] is True and completion['listener_closed'] is False,
            'peer availability did not outlive settled source')
        (self.home/'adoption-old-cut.continue').write_text('same job settled under real activated successor\n')
        self.wait(lambda:(self.home/'commands-finished.json').exists(),'successor status and logout',15,successor,self.client)
        terminal=self.stop_container(successor,successor_output)
        require(terminal['State']['ExitCode']==0,'activated successor did not stop cleanly')
        (self.home/'commands-finished.continue').write_text('actual successor owner joined\n')
        require(self.client.wait(timeout=6)==0 and not (self.home/'client.stderr').read_bytes(),'adoption client failed')
        stop_process(self.client);self.processes.remove(self.client)
        require(load(self.home/'client-result.json')['status']=='PASS','missing adoption HTTP client result')
        self.backend_absence(successor_output)

    def verify_linear_quantum(self):
        self.source_unchanged()
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'after');self.backend_absence(output)
        for snapshot in (self.adoption_before,self.adoption_sealed,self.adoption_retired,state):
            for producer in snapshot['producer_epochs']:
                if producer['producer_slot_id']=='\\x'+self.initialized['producer_slot_id']:
                    oracle.adoption_cardinality(snapshot,producer['producer_slot_id'],producer['producer_epoch'])
        wires=self.wires(5);oracle.adoption_wires(wires,self.events)
        joined=load(self.home/'capped-adoption-peer-joined.json')
        require(joined['requests']==5 and joined['listener_closed'] is True and joined['errors']==0,'adoption peer did not join')
        require(source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,5)==self.adoption_first['inputs'],
            'old original input bytes changed')
        files=self.files(self.state/'data/ingest/producer-inputs-v1')
        require(all(files.get(key)==value for key,value in self.adoption_original_files.items()),'old input file custody changed')
        dump=native.census(self,output,writer_terminal=self.root/'adoption-successor/terminal-container.json')
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],2,3)
        second=oracle.adoption_successor_science(state,self.adoption_started,self.events,wires,dump,inputs,self.adoption_first,time.time_ns()//1000,
            before=self.adoption_before)
        oracle.adoption_transition(self.adoption_before,self.adoption_sealed,self.adoption_retired,state,self.adoption_first)
        summary=oracle.adoption_history(state,wires,self.events,self.adoption_first,second)
        oracle.adoption_progress(state,self.initialized,self.events,wires,second,time.time_ns()//1000,epoch=2)
        status,=load(self.home/'linear-status-observations.json')
        from linear_capped_client import adoption_status
        adoption_status(status,dict(scope_id=self.adoption_before['acquisition_jobs'][0]['scope_id'][2:]),
            dict(job_id=self.adoption_before['jobs'][0]['job_id'][2:],accepted_at_us=str(self.adoption_before['jobs'][0]['created_at_us'])))
        save(output/'verdict.json',dict(status='PASS',scenario='settled-owner-adoption',same_job=True,
            unique_bodies=3,event_observations=5,mutation_requests_by_epoch={'1':5,'2':3},
            pending_plans=0,native_rows=len(dump['rows']),native_sequence=dump['sequence'],coverage='pending',**summary))
