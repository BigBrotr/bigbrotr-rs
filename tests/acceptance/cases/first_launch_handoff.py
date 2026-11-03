"""One explicitly selected zero-query epoch2→epoch3 installed handoff."""
import time

from cases.first_launch import load,save,require,digest,stop_process
from cases.first_launch_capped_quantum import CappedAdoptionRuntime,ADOPTION_LIMITS
from cases import first_launch_capped_oracle as base
from cases import first_launch_handoff_oracle as oracle
from cases import first_launch_restart as source
from cases import first_launch_retained_native as native

LIMITS=ADOPTION_LIMITS+[
    'The explicit zero-query handoff keeps Sync selected. An independent ordinal1 before-forward plan COMMIT fault occurs after rev3 adoption and ordinary attribution1; epoch2 has no committed quantum or REQ. All original/attribution inputs and native113 are authenticated before exclusive close/seal/retirement, then a genuine epoch3 writer adopts rev4 and resumes global query2.',
    'Only this profile replaces the original60s wait with actual joined-native/driver/backend exclusion and product lock fencing before expiry. Two unchanged accepted30s stability intervals must fit the original120s job and existing110s client gate. Long Serve phases renew normal policy/SQL authority; short recovery operations retain file-bound root5s checks. NOT_RUN until installed; no lost-candidate, full500 or origin-negative proof.',
]


class ZeroQueryHandoffRuntime(CappedAdoptionRuntime):
    def load_helpers(self):
        super().load_helpers()
        # Load common encoders before candidate/source-file custody freezes.
        import installed_family_science
        import initial_succession_oracle
        import transition_replay
        import installed_progress_context_oracle

    def candidate(self):
        super().candidate()
        require(self.args.linear_quantum_handoff=='zero-query','unknown handoff profile')
        self.inputs['linear_quantum_handoff']='zero-query'
        for name in ('tests/acceptance/cases/first_launch_handoff.py','tests/acceptance/cases/first_launch_handoff_oracle.py'):
            self.inputs['acceptance_files_sha256'][name]=digest(self.source/name)

    def image_and_state(self):
        super().image_and_state()
        save(self.home/'linear-quantum-handoff.json','zero-query')

    def wait_adoption_owner_exclusion(self,output):
        require(self.quantum_fault=='plan-before' and self.resources['quantum_proxy']['status']=='joined','handoff requires joined original fault')
        self.backend_absence(output)
        observer=load(self.root/'native-observer-terminal.json')
        require(observer['state']['ExitCode']==0 and observer['state']['Pid']==0 and not observer['state']['Running']
            and not observer['state']['OOMKilled'],'original native observer is not joined')
        sample=self.control_lease_observation()
        require(sample['active_unexpired']==1,'handoff must qualify genuine lock fencing before expiry')
        save(output/'exclusive-recovery-prerequisite.json',dict(strategy='joined-native-and-advisory-lock-fencing',
            native_observer=observer,control_observation=sample,requires_actual_recovery=True))

    def clock(self):
        return int(self.sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))

    def remaining_budget(self,output,stable_intervals):
        require(type(stable_intervals) is int and stable_intervals in (1,2),'invalid handoff stability interval count')
        state=self.adoption_retired;job=source.only(state['jobs']);detail=source.only(state['acquisition_jobs'])
        config=source.only(state['config_revisions']);policy=source.only(state['policy_revisions'])
        stable=config['document']['settings']['runtime.pressure_stable_duration']
        source.exact(stable,policy['document']['settings']['runtime.pressure_stable_duration'],'accepted stability differs')
        source.clock(stable,1,2**64-1)
        assert config['revision']==policy['revision']==policy['config_revision']==job['config_revision']==job['policy_revision']==1
        assert detail['deadline_at_us']-job['created_at_us']==120_000_000
        boottime=getattr(time,'CLOCK_BOOTTIME',None)
        require(boottime is not None,'handoff budget requires Linux CLOCK_BOOTTIME')
        started=time.clock_gettime_ns(boottime);now=self.clock();elapsed=(time.clock_gettime_ns(boottime)-started+999)//1000
        require(elapsed>=0,'handoff budget clock regressed')
        needed=stable_intervals*stable+20_000_000
        client_deadline=job['created_at_us']+110_000_000
        fits=now+elapsed+needed<min(detail['deadline_at_us'],client_deadline)
        save(output/'observation-budget.json',dict(stable_intervals=stable_intervals,stable_duration_us=stable,settlement_allowance_us=20_000_000,
            needed_us=needed,observed_at_us=now,clock_roundtrip_us=elapsed,original_deadline_at_us=detail['deadline_at_us'],
            unchanged_client_deadline_at_us=client_deadline,fits=fits))
        require(fits,'handoff prerequisite: accepted stability and settlement cannot fit original job/client budget')
        return (stable+20_000_000)/1_000_000

    def validate_handoff_proxy(self,value,event):
        self.validate_commit_proxy(value,event,self.handoff_proxy_binding)

    def validate_handoff_fault_terminal(self,value):
        self.validate_handoff_proxy(value,'terminal')
        expected=dict(data_queries=1,data_commits=1,matched_commits=1,skipped_commits=0,selected_commits=1,
            selected_server_complete=0,selected_client_complete=0,server_complete=0,client_complete=0,faults=1)
        require(all(type(value.get(k)) is int and value[k]==v for k,v in expected.items()),'epoch2 first plan fault differs')
        require(all(value[k]>0 for k in ('frames','bytes','connections')),'epoch2 fault lacks traffic')

    def stop_handoff_proxy(self):
        if hasattr(self,'handoff_route'):
            self.stop_owned_proxy(self.handoff_route,'handoff_proxy','handoff-proxy',getattr(self,'handoff_proxy_ready',None),self.validate_handoff_proxy)

    def cleanup(self):
        try:self.stop_handoff_proxy()
        finally:super().cleanup()

    def after_adoption_retirement(self,output,closed):
        base.adoption_recovery_prefix(self.adoption_before,self.adoption_sealed,self.adoption_retired,self.adoption_first,initial_exclusion='exclusive-before-expiry')
        folder=self.root/'zero-query-handoff';folder.mkdir(mode=0o700)
        self.start_commit_proxy('handoff','before-forward','control-quantum-plan')
        document=digest(self.config)
        resolved=self.finite('handoff-zero-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
        require(digest(self.config)==document,'epoch2 root file changed during config show')
        ttl=base.adoption_root_authorization(resolved,document,self.adoption_retired)
        timeout=self.remaining_budget(folder,2)
        before=self.clock()
        entry=self.launch('handoff-zero-successor','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--activate-initial-successor','--roles','control,ingest,sync')
        fault=self.proxy_event(self.handoff_route,'fault',timeout);save(folder/'commit-fault.json',fault)
        ended=self.docker('wait',entry['id'],timeout=180);returned=self.clock()
        records=self.logs(entry,folder);info=self.inspect(entry)
        save(folder/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
            and not info['State']['Running'] and not info['State']['OOMKilled'],'epoch2 plan fault owner did not exit4 naturally')
        require(len(records)==1 and records[0]['state']=='activated' and records[0]['producer_epoch']=='2'
            and records[0]['acquisition']=='finite-initial-successor' and records[0]['ready'] is False,'Sync-selected epoch2 adoption report differs')
        base.adoption_fault_error('',(folder/'serve.stderr').read_text())
        self.handoff_started=records[0];save(folder/'initialized.json',records[0])
        require(digest(self.config)==document,'epoch2 root file changed during Serve')
        preserved=folder/'instance-epoch2.toml';preserved.write_bytes(self.config.read_bytes());preserved.chmod(0o600)
        self.stop_handoff_proxy();self.validate_handoff_fault_terminal(load(self.root/'handoff-proxy-terminal.json'))
        self.config.write_text(self.set_value(self.config.read_text(),'postgres.control','endpoint','127.0.0.1:15432'))
        self.inputs['initial_recipe_sha256']=digest(self.config)
        self.backend_absence(folder);self.handoff_zero,_=self.capture(folder,'zero')
        dump=native.census(self,folder,expected_writer_exit=4,writer_terminal=folder/'terminal-container.json')
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],2,1)
        self.handoff_attribution=oracle.zero_query_cut(self.adoption_retired,self.handoff_zero,self.adoption_first,records[0],
            self.wires(1),self.events,dump,inputs,before,returned,fault)
        owner=source.only(self.handoff_zero['owner_epochs'],role='control',epoch=source.only(self.handoff_zero['jobs'])['owner_epoch'])
        ingest=source.only(self.handoff_zero['owner_epochs'],role='ingest',epoch=2)
        activated=int(records[0]['activated_at_us'])
        require(owner['lease_expires_at_us']-60_000_000>activated and ingest['lease_expires_at_us']-60_000_000>activated,
            'epoch2 long Serve lacks ordinary post-activation SQL renewals')
        save(folder/'serve-authority.json',dict(root_config_document_sha256=document,root_authorization_ttl_us=ttl,
            config_sha256=records[0]['config_sha256'],policy_sha256=records[0]['policy_sha256'],
            actual_plan_backend_pid=fault['backend_pid'],returned_us=returned,activated_at_us=activated,
            control_lease_expires_at_us=owner['lease_expires_at_us'],ingest_lease_expires_at_us=ingest['lease_expires_at_us'],
            policy_basis='actual plan validates current root authority before COMMIT; startup authority is not reused'))
        self.handoff_input_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        save(folder/'zero-cut-observation.json',dict(before_us=before,returned_us=returned,native_sequence=113,
            producer_requests=1,job_bound_requests=0,committed_epoch2_quanta=0,wire_queries=1,job_revision=3))
        require(self.client.poll() is None,'handoff peer exited before epoch2 recovery')
        self.loopback_listener(self.owned_listeners(self.client.pid),7447)
        self.recover_epoch2(folder)
        self.run_epoch3(folder)
        return True

    def recover_epoch2(self,folder):
        bounds=[];reports=[]
        document=digest(self.config)
        resolved=self.finite('handoff-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
        require(digest(self.config)==document,'handoff recovery root file changed during show')
        ttl=base.adoption_root_authorization(resolved,document,self.handoff_zero)
        self.handoff_direct_document=document;self.handoff_root_ttl=ttl
        require(getattr(time,'CLOCK_BOOTTIME',None) is not None,'handoff recovery requires Linux CLOCK_BOOTTIME')
        for label,flag,stem in (('seal-zero-query-initial','--seal-activated-acquisition','sealed'),
                ('retire-zero-query-initial','--retire-activated-initial','retired')):
            lower=self.clock();started=time.clock_gettime_ns(time.CLOCK_BOOTTIME)
            report=self.finite(label,'ingest','recover','--file','/etc/bigbrotr/instance.toml',flag)
            elapsed=time.clock_gettime_ns(time.CLOCK_BOOTTIME)-started;upper=self.clock()
            require(0<=elapsed<ttl*1000 and digest(self.config)==document,'short epoch2 recovery exceeded actual root authority or changed root')
            save(folder/(stem+'-authorization.json'),dict(clock='CLOCK_BOOTTIME',elapsed_ns=elapsed,root_authorization_ttl_us=ttl,
                root_config_document_sha256=document,root_config_show_sha256=digest(self.root/'bootstrap/handoff-root-config.stdout'),before_us=lower,returned_us=upper))
            save(folder/(stem+'-report.json'),report)
            state,_=self.capture(folder,stem);self.backend_absence(folder)
            if stem=='sealed':self.handoff_sealed=state
            else:self.handoff_retired=state
            bounds.append((lower,upper));reports.append(report)
        oracle.epoch2_recovery(self.handoff_zero,self.handoff_sealed,self.handoff_retired,self.handoff_attribution,bounds,reports)
        # Both finite consumers close native and join their actual driver before returning.
        closed_output=folder/'closed-native';closed_output.mkdir(mode=0o700)
        dump=native.census(self,closed_output,expected_writer_exit=4,writer_terminal=folder/'terminal-container.json')
        oracle.closed_attribution(self.handoff_attribution,'\\x'+self.initialized['producer_slot_id'],dump)
        self.handoff_closed=dump
        require(self.files(self.state/'data/ingest/producer-inputs-v1')==self.handoff_input_files,'epoch2 recovery changed original input files')
        self.wires(1)

    def run_epoch3(self,folder):
        output=self.root/'handoff-successor';output.mkdir(mode=0o700)
        timeout=self.remaining_budget(output,1)
        require(digest(self.config)==self.handoff_direct_document,'epoch3 root differs from actual direct config show')
        self.handoff_final_lower=self.clock()
        phase_deadline=time.clock_gettime_ns(time.CLOCK_BOOTTIME)+int(timeout*1_000_000_000)
        def remaining():
            seconds=(phase_deadline-time.clock_gettime_ns(time.CLOCK_BOOTTIME))/1_000_000_000
            require(seconds>0,'epoch3 handoff observation stage exhausted its derived budget')
            return seconds
        entry=self.launch('handoff-successor','app','serve','--file','/etc/bigbrotr/instance.toml','--activate-successor','--roles','control,ingest,sync')
        started=self.wait(lambda:self.logs(entry,output),'actual epoch3 handoff writer',remaining(),entry=entry)[0]
        remaining()
        require(started['state']=='activated' and started['producer_epoch']=='3' and started['acquisition']=='finite-successor'
            and started['ready'] is False,'epoch3 actual writer differs')
        self.handoff_final_started=started;save(output/'initialized.json',started)
        def settled():
            require(not (self.home/'relay-error.json').exists(),'handoff relay failed')
            if not (self.home/'capped-adoption-peer-complete.json').exists():return False
            return self.sql('bigbrotr_control',"SELECT count(*)=5 AND bool_and(disposition IS NOT NULL) FROM control.acquisition_quanta")=='t'
        self.wait(settled,'five globally numbered epoch1/3 queries',remaining(),entry,self.client)
        remaining()
        self.wires(5);self.handoff_live,_=self.capture(output,'settled')
        completion=load(self.home/'capped-adoption-peer-complete.json')
        require(completion['requests']==5 and completion['listening'] is True and completion['listener_closed'] is False
            and self.client.poll() is None,'handoff peer did not remain available through all settled queries')
        (self.home/'adoption-old-cut.continue').write_text('same job settled after zero-query handoff\n')
        self.wait(lambda:(self.home/'commands-finished.json').exists(),'handoff status and logout',15,entry,self.client)
        terminal=self.stop_container(entry,output)
        require(terminal['State']['ExitCode']==0,'epoch3 handoff writer did not stop cleanly')
        self.handoff_final_upper=self.clock()
        require(digest(self.config)==self.handoff_direct_document,'epoch3 root changed during Serve')
        save(output/'serve-authority.json',dict(root_config_document_sha256=self.handoff_direct_document,
            root_authorization_ttl_us=self.handoff_root_ttl,before_us=self.handoff_final_lower,returned_us=self.handoff_final_upper,
            basis='actual per-operation policy validation and SQL renewals; no whole-Serve root-TTL bound'))
        (self.home/'commands-finished.continue').write_text('actual epoch3 writer joined\n')
        require(self.client.wait(timeout=6)==0 and not (self.home/'client.stderr').read_bytes(),'handoff client failed')
        stop_process(self.client);self.processes.remove(self.client)
        result=load(self.home/'client-result.json')
        require(result['status']=='PASS' and result.get('handoff')=='zero-query' and result.get('gate_seconds')==110
            and result['job_status']['job_revision']=='4','missing exact handoff client result')
        self.backend_absence(output)

    def verify_linear_quantum(self):
        self.source_unchanged();output=self.root/'linear-quantum';output.mkdir(mode=0o700)
        state,_=self.capture(output,'after');self.backend_absence(output)
        dump=native.census(self,output,writer_terminal=self.root/'handoff-successor/terminal-container.json')
        inputs=source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],3,3)
        science=oracle.final_rows(self.handoff_retired,state,self.adoption_first,self.handoff_attribution,self.handoff_final_started,
            self.wires(5),self.events,dump,inputs,self.handoff_final_lower,self.handoff_final_upper,preparation_ttl_us=self.handoff_root_ttl)
        for epoch,count,expected in ((1,5,self.adoption_first['inputs']),(2,1,self.handoff_attribution['inputs'])):
            require(source.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],epoch,count)==expected,'ancestor input bytes changed')
        files=self.files(self.state/'data/ingest/producer-inputs-v1')
        require(all(files.get(k)==v for k,v in self.handoff_input_files.items()),'ancestor input file metadata changed')
        require(len(files)==len(self.handoff_input_files)+3,'handoff retained an extra producer input')
        joined=load(self.home/'capped-adoption-peer-joined.json')
        require(joined['requests']==5 and joined['listener_closed'] is True and joined['errors']==0,'handoff peer did not join exactly five requests')
        save(output/'verdict.json',dict(status='PASS',scenario='settled-owner-adoption',handoff='zero-query',same_job=True,job_revision=4,
            wire_queries=5,epoch2_quanta=0,mutation_requests_by_epoch={'1':5,'2':1,'3':3},continuation_seals=2,adopted_seals=2,
            native_sequence=dump['sequence'],native_rows=len(dump['rows']),coverage='pending'))
