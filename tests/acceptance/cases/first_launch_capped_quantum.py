"""Installed capped continuation with a delivered prepopulation gate."""
import shutil
import time
import tomllib

from cases.first_launch import Runtime,load,save,require,digest,stop_process
from cases.first_launch_linear_quantum import LinearQuantumRuntime
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_capped_oracle as oracle
from cases import first_launch_retained_native as native
from cases import first_launch_restart as source
from cases import first_launch_capped_corruption as corruption

LIMITS=[
    'One private relay caps requested3 to at most2. Sequential jobs A and B share endpoint/access/visibility. A admits signed24/23 and stops on repeated23; B receives all-known24/23, reaches new1 through overlap, then stops on repeated1.',
    'Exactly5 requests and5 terminal quanta, no next plan/socket,6 original producer inputs/receipts and all independently encoded native rows are required. The pre-B gate checks delivered SQL/input/checkpoint progress; complete native science is checked after the original writer joins.',
    'Archive setup belongs to the reused installed bundle lifecycle; this scenario does not qualify Archive publication, reads, a full walker, saturated-second partitioning, sparse growth, adoption or continuation COMMIT faults.',
    'Optional cursor-time/foreign-payload profiles mutate exactly one completed row after the valid five-page boundary. They require a natural exact Quantum(Context) planning refusal, restored triggers and unchanged independently checked native science.',
]


class CappedQuantumRuntime(LinearQuantumRuntime):
    def candidate(self):
        super().candidate()
        self.inputs['linear_quantum_scenario']='capped-continuation'
        names=('experiments/ingest-bootstrap/linear_capped_transport.py',
            'experiments/ingest-bootstrap/linear_capped_client.py',
            'tests/acceptance/cases/first_launch_capped_quantum.py',
            'tests/acceptance/cases/first_launch_capped_oracle.py',
            'tests/acceptance/cases/first_launch_capped_corruption.py')
        for name in names:self.inputs['acceptance_files_sha256'][name]=digest(self.source/name)
        from linear_capped_transport import IMPORT_SHA256,REFUSAL_SHA256
        self.inputs['capped_corpus_sha256']={name:digest(self.source/name) for name in (
            'tests/fixtures/acquisition/selector-events.json','tests/fixtures/ingest/import-event.jsonl')}
        require(list(self.inputs['capped_corpus_sha256'].values())==[REFUSAL_SHA256,IMPORT_SHA256],'capped corpus identity differs')

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
        save(self.home/'linear-capped-profile.json',PROFILE)
        self.events=corpus(self.home)

    def source_unchanged(self):
        super().source_unchanged()
        for name,expected in self.inputs['capped_corpus_sha256'].items():
            require(digest(self.source/name)==expected,'source capped corpus changed')
        require(digest(self.home/'selector-events.json')==self.inputs['capped_corpus_sha256']['tests/fixtures/acquisition/selector-events.json'],
            'copied capped corpus changed')

    def initial_launch(self):
        return self.launch('initial','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,sync','--initialize-ingest')

    def source_view(self,state):
        return self.current_owner_view(state,dict(control=load(self.root/'initial/before-control.json'),
            resource=dict(catalog_id=self.ids['catalog'])))

    def wires(self,count):
        require(not (self.home/'relay-error.json').exists(),'capped relay failed')
        opened=sorted(self.home.glob('relay-open-*.json'));closed=sorted(self.home.glob('relay-closed-*.json'))
        require(len(opened)==len(closed)==count,'missing capped transcript or extra accepted socket')
        wires=[]
        for number,(before,after) in enumerate(zip(opened,closed),1):
            require(before.name==f'relay-open-{number:03}.json' and after.name==f'relay-closed-{number:03}.json','capped transcript gap')
            first,last=load(before),load(after)
            require(all(last.get(key)==value for key,value in first.items()),'capped open/close binding changed')
            wires.append(last)
        return wires

    def initial_submission(self,entry,output):
        self.wait(lambda:(self.home/'capped-prepopulated.json').exists(),'prepopulation command barrier',15,entry,self.client)
        def completed():
            require(not (self.home/'relay-error.json').exists(),'capped prepopulation relay failed')
            require(not (self.home/'relay-open-003.json').exists(),'B socket preceded independent prepopulation gate')
            if not (self.home/'relay-closed-002.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=2 AND bool_and(terminal_kind IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(completed,'two delivered A pages before B',15,entry,self.client)
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
            if not (self.home/'capped-peer-five-complete.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=5 AND bool_and(terminal_kind IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(completed,'five completed capped requests',30,entry,self.client)
        state,_=self.capture(output,'capped-completed')
        require(len(state['producer_requests'])==len(state['producer_outcomes'])==6,'capped continuation mutation count differs')
        self.wires(5)

    def initial_publication(self,entry,output):
        completion=load(self.home/'capped-peer-five-complete.json')
        require(completion['requests']==5 and completion['listener_closed'] is False and completion['listening'] is True,
            'capped relay disappeared at the stopping boundary')
        require(self.client.poll() is None,'capped client ended before stop')
        listener=self.owned_listeners(self.client.pid);self.loopback_listener(listener,7447)
        save(output/'capped-live-listener.json',dict(completion=completion,process=listener))

    def initial_source_quiet(self):
        require(not (self.home/'relay-error.json').exists(),'capped relay failed before original owner stop')
        require(not (self.home/'relay-open-006.json').exists(),'stopped capped jobs opened another socket')
        return self.sql('bigbrotr_control',"SELECT count(*)=5 AND bool_and(terminal_kind IS NOT NULL) FROM control.acquisition_quanta")=='t'

    def initial(self):
        profile=getattr(self.args,'linear_quantum_corruption',None)
        if profile is None:return super().initial()
        self.inputs['linear_quantum_corruption']=profile
        require(tomllib.loads(self.config.read_text()).get('runtime',{}).get('lease_ttl','60s')=='60s',
            'corruption fixture requires the unchanged 60s owner lease profile')
        entry,output,setup=self.initial_start()
        self.initial_commands(entry,output,setup)
        baseline,_=self.capture(output,'corruption-baseline');view=self.source_view(baseline)
        wires=self.wires(5);commands=load(self.home/'submitted.json')
        oracle.source_rows(view,self.initialized,commands,principal=self.enrolled['principal_id'])
        receipts=[source.raw(row['outcome_bytes']) for row in sorted(view['producer_outcomes'],key=lambda row:row['request_sequence'])]
        oracle.quanta(view,wires,self.events,receipts,commands)
        oracle.statuses(load(self.home/'linear-status-observations.json'),commands)
        self.corruption_inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,6)
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
        self.wires(5);self.backend_absence(output)

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
            6:source.request_binding(self.events[2],wires[3],2)}
        source.requests(view,1,science['manifest'],science['inputs'],science['receipts'],
            source.only(view['owner_epochs'],role='control'),bindings,{4:first,5:first,6:second})
        oracle.progress(view,self.initialized,wires,self.events,science,completed)

    def verify_linear_quantum(self):
        if getattr(self.args,'linear_quantum_corruption',None) is not None:return self.verify_corruption()
        self.source_unchanged()
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'after');self.backend_absence(output)
        view=self.source_view(state)
        wires=self.wires(5);oracle.wires_checked(wires,self.events)
        joined=load(self.home/'capped-peer-joined.json')
        require(joined['requests']==5 and joined['listener_closed'] is True and joined['errors']==0,'capped peer did not join cleanly')
        dump=native.census(self,output)
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,6)
        completed=time.time_ns()//1000
        science=oracle.science(view,self.initialized,self.events,wires,dump,inputs,completed)
        commands=load(self.home/'submitted.json')
        self.verify_valid_state(state,wires,science,commands,completed)
        save(output/'verdict.json',dict(status='PASS',scenario='capped-continuation',requests=5,terminal_quanta=5,
            pending_plans=0,mutation_requests=6,classified_events=8,native_rows=len(science['native']),native_sequence=103,
            exclusive_uppers=dict(A=[86400,24],B=[86400,24,2]),coverage='pending',
            all_known_first_page_continued=True,older_event_retained=self.events[2]['id']))

    def verify_corruption(self):
        self.source_unchanged()
        output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'corruption-final');self.backend_absence(output)
        old,changed=self.corruption_rows
        corruption.preserved(self.corruption_baseline,state,old,changed,self.corruption_observed_at_us)
        require(load_modes(self)==corruption.TRIGGERS,'restored corruption triggers changed')
        wires=self.wires(5)
        joined=load(self.home/'capped-peer-joined.json')
        require(joined['requests']==5 and joined['listener_closed'] is True and joined['errors']==0,'corruption peer did not join cleanly')
        dump=native.census(self,output,expected_writer_exit=4)
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,6)
        require(inputs==self.corruption_inputs and self.files(self.state/'data/ingest/producer-inputs-v1')==self.corruption_input_files,
            'corruption refusal changed original input custody')
        completed=time.time_ns()//1000
        science=oracle.science(self.source_view(self.corruption_baseline),self.initialized,self.events,wires,dump,inputs,completed)
        self.verify_valid_state(self.corruption_baseline,wires,science,load(self.home/'submitted.json'),completed)
        body=corruption.native_body_one(science,self.events)
        save(output/'verdict.json',dict(status='PASS',scenario='capped-continuation',corruption=self.args.linear_quantum_corruption,
            natural_refusal='Quantum(Context)',harness_stop_before_refusal=False,terminal_quanta=5,pending_plans=0,
            mutation_requests=6,native_rows=len(science['native']),native_sequence=103,native_body_one=body,
            trigger_modes=corruption.TRIGGERS,coverage='pending'))


def load_modes(runtime):
    import json
    return json.loads(runtime.sql('bigbrotr_control',"SELECT jsonb_agg(jsonb_build_object('name',tgname,'enabled',tgenabled) ORDER BY tgname) "
        "FROM pg_trigger WHERE tgrelid='control.acquisition_quanta'::regclass "
        "AND tgname IN ('acquisition_quantum_guard','zzz_acquisition_quantum_owner')"))
