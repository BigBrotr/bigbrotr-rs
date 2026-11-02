"""Select terminal C4 COMMIT loss, then use installed historical recovery."""
import json
from pathlib import Path
import sys
import time

from cases.first_launch import digest, load, require, save, stop_process, MissingPrerequisite
from cases.first_launch_passive_monitor import PassiveMonitorRuntime, publish_selection
from cases import first_launch_passive_oracle as passive
from cases import first_launch_retained_native as native
from cases import first_launch_monitor_ack_recovery_oracle as oracle

STAGES=('prerequisites','candidate','runtime_image','fresh_bootstrap',
    'passive_checkpoint_reply_loss','monitor_ack_recovery','monitor_ack_retry','cleanup')
NATIVE_OUTCOME_STAGES=STAGES[:4]+('passive_native_outcome_loss','monitor_outcome_recovery','monitor_outcome_retry','cleanup')
REPAIR_FAULT_STAGES=NATIVE_OUTCOME_STAGES[:5]+('monitor_outcome_rollback','monitor_outcome_commit_loss')+NATIVE_OUTCOME_STAGES[5:]
EXISTING_OUTCOME_STAGES=NATIVE_OUTCOME_STAGES[:5]+('monitor_existing_outcome_seed',
    'monitor_existing_outcome_recovery','monitor_existing_outcome_retry','cleanup')
LIMITS=[
    'One passive socket operation; lose only the actual generic revision4/C4 COMMIT reply after PostgreSQL completed it. Independent joined-store census must first prove I4/A3/R0 with the exact ten facts/four headers/four receipts.',
    'Installed default ingest recover uses fresh Control and exclusive historical native custody, closes the original producer and confirms ACK4 without new source I/O, Monitor token, Sync/acquisition progress or retirement. Exact retry adds no native write.',
    'All original attempt/request/input/receipt/science and checkpoint/dependency xmins remain retained. The existing peer observes through both recoveries, bounded to120s. This does not qualify nativeI4/C3 advancement, staged-before-PG, lost RAM, durable start, renewal, fleet cadence, reclamation or consumer publication during recovery.',
]
NATIVE_OUTCOME_LIMITS=[
    'One passive socket operation; withhold the selected generic C4 COMMIT before forwarding, after exact same-session UPDATE1. After positive writer/backend exclusion, require PG C3/revision3/nine dependencies and outcomes1..3, but original Native I4/A3/R0/ten Facts/four PendingBatch headers/four exact receipts.',
    'Installed default fresh-Control recovery must atomically copy the independently reconstructed original outcome4 with C4 and dependency10, close Native I4/A4/R0, and preserve every original input/scientific value and prior dependency/outcome xmin. Exact retry rewrites neither Native nor PG checkpoint. No new socket or Sync work.',
    'Fact10 recorded_at is an observed signed scalar bounded by the independent fixture window; the complete Fact/receipt/dependency expectation is rebuilt from original measurement and fixed identities, never from a fabricated PG outcome. The peer profile remains120s. This case is NOT_RUN until the repaired product is independently qualified; old C4/A3 runs are not C3 repair proof. Full PG rollback, lost repair COMMIT and existing-outcome/C3 gates remain separate.',
]
REPAIR_FAULT_LIMITS=NATIVE_OUTCOME_LIMITS[:2]+[
    'The same one-peer120s journey additionally withholds repair COMMIT before forwarding, then loses a fresh repair COMMIT reply after completion. Both installed commands must exit CommitUnknown without ACK4. The first adds only the closing native write and retains exact PG C3; the second commits original outcome4/C4/dependency10 atomically and adds no Native write. Fresh direct completion adds only ACK4; exact retry adds none and neither rewrites the committed PG rows. Real execution is NOT_RUN; other PG refusal and exact-existing-outcome/C3 gates remain separate.',
]
EXISTING_OUTCOME_LIMITS=[NATIVE_OUTCOME_LIMITS[0],
    'A separately attributed ignored cfg(test) Control-owner fixture uses genuine runtime login/claim/closing custody to replay original request4 and compare its receipt with the independent Python expectation. It commits only actual outcome4, retaining C3 and charging the one Native closing write to this explicit seed phase. It is not a production copy qualification.',
    'Ordinary installed recovery must preserve the seeded outcome bytes, recorder, timestamp and xmin while fresh Control atomically advances only C4 and dependency10. Native ACK adds one write; exact retry changes neither store. Complete policy documents and deployment/manifest bindings are frozen before measurement admission. The120s peer and all prior bounds remain unchanged; Rust fixture and connected execution are NOT_RUN until independently qualified. Other connected refusal gates remain open.']


class MonitorAckRecoveryRuntime(PassiveMonitorRuntime):
    native_outcome=False
    repair_faults=False
    existing_outcome=False

    def __init__(self, args, inputs):
        self.repair_faults=args.case=='p04-monitor-native-outcome-faults'
        self.existing_outcome=args.case=='p04-monitor-existing-outcome-recovery'
        self.native_outcome=self.existing_outcome or self.repair_faults or args.case=='p04-monitor-native-outcome-recovery'
        super().__init__(args,inputs)

    @property
    def fault_mode(self):
        return 'before-forward' if self.native_outcome else 'after-complete'

    def recovery_label(self, *, retry):
        if self.existing_outcome:return 'monitor-existing-outcome-'+('retry' if retry else 'recovery')
        return ('monitor-outcome-' if self.native_outcome else 'monitor-ack-')+('retry' if retry else 'recovery')

    def preflight(self):
        if self.existing_outcome:
            compiled=getattr(self.args,'monitor_outcome_seed_build',None)
            if compiled is None or not all((compiled/name).is_file() for name in (
                    'build/source-manifest.json','build/parent-product-source-manifest.json',
                    'artifacts/test-tool-artifact.json','artifacts/bigbrotr-library-tests')):
                raise MissingPrerequisite('existing-outcome case requires the separately declared test-only seed build')
        super().preflight()

    def start_checkpoint_proxy(self, output, mode):
        self.direct_recipe=self.config.read_text()
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        args=[sys.executable,'-B',str(path),'--upstream-port','15432','--mode',mode,
            '--target','control-measurement-checkpoint','--profile','retained-catch-up','--defer-arm']
        self.fault_proxy=self.proxy_process(args,input_pipe=True)
        ready=self.proxy_event(self.fault_proxy,'ready',5)
        save(output/'proxy-ready.json',ready)
        save(output/'proxy-process.json',dict(argv=args,pid=self.fault_proxy.process.pid,source_sha256=digest(path)))
        require(ready['source_sha256']==digest(path) and ready['armed'] is False
            and ready['upstream_host']=='127.0.0.1' and ready['upstream_port']==15432
            and ready['mode']==mode and ready['target']=='control-measurement-checkpoint'
            and tuple(ready[k] for k in ('profile','frame_limit','connection_limit','connection_limit_scope',
                'wire_limit','lifetime_seconds'))==('retained-catch-up',131072,64,'lifetime',64*1024*1024,120),
            'wrong deferred checkpoint proxy')
        self.config.write_text(self.set_value(self.direct_recipe,'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))

    def select_checkpoint(self):
        self.proxy_send(self.fault_proxy,'select-measurement '+self.initialized['dataset_id']+' '+self.initialized['producer_slot_id']+'\n')
        selected=self.proxy_event(self.fault_proxy,'measurement-selected',1)
        self.proxy_send(self.fault_proxy,'arm\n');self.proxy_event(self.fault_proxy,'armed',1)
        return selected

    def checkpoint_fault(self, output, selected, mode, *, deadline=None):
        def remaining(maximum):
            if deadline is None:return maximum
            left=deadline-time.monotonic()
            require(left>0, 'repair command fixture deadline')
            return min(maximum,left)
        update=self.proxy_event(self.fault_proxy,'measurement-checkpoint-update-complete',remaining(20))
        fault=self.proxy_event(self.fault_proxy,'fault',remaining(5))
        check=oracle.fault_before_forward if mode=='before-forward' else oracle.fault
        check(fault,self.initialized['dataset_id'],self.initialized['producer_slot_id'])
        require(selected['measurement_identity_sha256']==update['measurement_identity_sha256']==fault['measurement_identity_sha256']
            and update['backend_pid']==fault['backend_pid'], 'fault changed its selected identity/backend')
        save(output/'proxy-fault.json',fault)
        return fault

    def joined_unknown(self, entry, output, *, timeout=20):
        ended=self.docker('wait',entry['id'],timeout=timeout,label='monitor-fault-owner-join')
        info=self.inspect(entry);logged=self.docker('logs',entry['id'])
        (output/'serve.stdout').write_text(logged.stdout);(output/'serve.stderr').write_text(logged.stderr)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
            and not info['State']['Running'] and not info['State']['OOMKilled'] and 'CommitUnknown' in logged.stderr,
            'checkpoint reply loss did not join as explicit uncertainty')
        return [json.loads(line) for line in logged.stdout.splitlines()]

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0,str(self.source/'experiments/archive-engine'))
        from signed_publication_cases import event,send
        self.proxy_event,self.proxy_send=event,send

    def candidate(self):
        for module in (Path(__file__),Path(oracle.__file__)):
            require(digest(module)==digest(self.source/'tests/acceptance/cases'/module.name),
                'Monitor ACK recovery source mismatch')
        super().candidate()
        relative='experiments/archive-engine/native_commit_proxy.py'
        self.inputs['acceptance_files_sha256'][relative]=digest(self.source/relative)
        if self.existing_outcome:
            from cases import first_launch_monitor_outcome_seed as seed
            relative='tests/acceptance/cases/first_launch_monitor_outcome_seed.py'
            require(digest(Path(seed.__file__))==digest(self.source/relative), 'Monitor outcome seed source mismatch')
            self.inputs['acceptance_files_sha256'][relative]=digest(self.source/relative)
            self.seed_paths,self.seed_identity=seed.artifact(self.args.monitor_outcome_seed_build,self.manifest,self.source)
            self.seed_hashes={name:digest(path) for name,path in self.seed_paths.items()}
            self.inputs['monitor_outcome_seed']=self.seed_identity
            for name in (seed.OWNER,seed.FIXTURE,'tests/fixtures/configuration/policy-v3.json'):
                self.inputs['acceptance_files_sha256'][name]=digest(self.source/name)

    def bootstrap(self):
        super().bootstrap()
        if self.existing_outcome:
            from cases import first_launch_monitor_outcome_seed as seed
            report=self.finite('seed-policy-candidate','config','show','--file','/etc/bigbrotr/instance.toml')
            self.seed_policies=seed.policy(self.source,report,digest(self.config))
            save(self.root/'bootstrap/seed-policy-frozen.json',self.seed_policies)

    def passive_begin(self, output):
        entry=super().passive_begin(output)
        if self.existing_outcome:
            from cases import first_launch_monitor_outcome_seed as seed
            self.seed_binding=seed.registration(load(output/'prepared-control.json'),self.initialized,self.ids)
            save(output/'seed-registration-frozen.json',self.seed_binding)
        return entry

    def prepare_state(self):
        super().prepare_state()
        save(self.home/'monitor-ack-recovery.json',dict(format='bigbrotr-monitor-ack-recovery-peer/1',
            boundary='checkpoint-'+self.fault_mode,observed_seconds=120))

    def passive_launch(self):
        return self.launch('monitor-native-outcome-loss' if self.native_outcome else 'monitor-checkpoint-reply-loss',
            'app','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,monitor','--initialize-ingest','--monitor-once','/var/lib/bigbrotr/monitor-once.json')

    def finish_fault_proxy(self, output, *, failed):
        """Retain a graceful diagnostic before forced cleanup; preserve failure."""
        proxy=self.fault_proxy;problem=None
        try:
            if proxy.process.poll() is None:
                self.proxy_send(proxy,'stop\n')
            code=proxy.wait(5)
            terminal=self.proxy_event(proxy,'terminal',1)
            save(output/'proxy-report.json',terminal)
            require(code==0 and terminal['unexpected']==0 and not proxy.stderr and not proxy.overflow,
                'checkpoint proxy did not terminate cleanly')
        except Exception as error:
            problem=error
            save(output/'proxy-stop-failure.json',dict(error_type=type(error).__name__))
            self.resources['cleanup_errors'].append(dict(resource='measurement-checkpoint-proxy',error=type(error).__name__))
        finally:
            try:
                proxy.stop()
            except Exception as error:
                self.resources['cleanup_errors'].append(dict(resource='measurement-checkpoint-proxy',error='forced join '+type(error).__name__))
                if problem is None:problem=error
            finally:
                (output/'proxy.stdout').write_bytes(proxy.stdout)
                (output/'proxy.stderr').write_bytes(proxy.stderr)
                save(output/'proxy-terminal.json',dict(exit_code=proxy.process.returncode,overflow=proxy.overflow))
                self.fault_proxy=None
        if problem is not None and not failed:
            raise problem

    def checkpoint_reply_loss(self):
        output=self.root/('passive-native-outcome-loss' if self.native_outcome else 'passive-checkpoint-reply-loss');output.mkdir(mode=0o700)
        try:
            self.start_checkpoint_proxy(output,self.fault_mode)
            entry=self.passive_begin(output)
            self.writer=entry
            selected=self.select_checkpoint()
            publish_selection(self.selection_path,self.selection)
            (self.home/'prepared.continue').write_text('real source checked; exact C4 selector armed\n')
            fault=self.checkpoint_fault(output,selected,self.fault_mode)
            notices=self.joined_unknown(entry,output)
            monitor=[row for row in notices if row.get('format')=='bigbrotr-monitor-once/1']
            require(monitor and monitor[-1]['state']=='parked'
                and all(row['state'] in ('selected','parked') for row in monitor), 'lost reply reported completion or repeated work')
            self.wait(lambda:(self.home/'relay-complete.json').exists(),'one passive source teardown',5,client=self.client)
            require(not (self.home/'relay-error.json').exists(), 'peer failed')
        finally:
            failed=sys.exception() is not None
            self.config.write_text(self.direct_recipe)
            if getattr(self,'fault_proxy',None) is not None:self.finish_fault_proxy(output,failed=failed)
        exclusion=output/'old-owner-exclusion';exclusion.mkdir(mode=0o700)
        self.backend_absence(exclusion)
        self.before_recovery,self.catalog=self.capture(output,'uncommitted' if self.native_outcome else 'committed')
        dispatch=passive.only(self.before_recovery['measurement_dispatches'])
        control=passive.only(self.before_recovery['owner_epochs'],role='control',resource_id=dispatch['issuer_resource'],epoch=dispatch['issuer_epoch'])
        require(control['control_backend_pid']==fault['backend_pid'], 'fault was not the original Monitor Control backend')
        oracle.eligible(self.before_recovery)
        self.original_inputs=self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)
        self.original_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        if self.native_outcome:
            self.after_us=time.time_ns()//1000
            require(self.before_us<=self.after_us, 'host UTC regressed across admission window')
            save(output/'admission-window.json',dict(before_us=self.before_us,after_us=self.after_us,
                clock='host-system-utc',scope='before original writer launch through positive writer/backend exclusion'))
            self.before_dump=native.census(self,output,expected_writer_exit=4,writer_terminal=output/'terminal-container.json')
            self.receipts=oracle.native_outcome(self.before_recovery,self.before_dump,self.initialized,self.selection,
                self.original_inputs,self.cbor,self.decode,load(self.home/'relay-exchange.json'),
                load(self.home/'event.jsonl'),self.before_us,self.after_us)
        else:
            self.receipts=oracle.terminal(self.before_recovery,self.initialized,self.selection,self.original_inputs,self.cbor,self.decode)
            self.before_dump=native.census(self,output,expected_writer_exit=4,writer_terminal=output/'terminal-container.json')
            passive.native_ack(self.before_dump,self.before_recovery,self.initialized,self.receipts,self.cbor,phase='reply-lost')
        save(output/'native-census.json',self.before_dump)
        save(output/'independent-verdict.json',dict(status='PASS',checkpoint=3 if self.native_outcome else 4,
            native_issued=4,native_acknowledged=3,native_retired=0,
            terminal='native-known-pg-absent' if self.native_outcome else 'known-durable',
            original_writer='joined',original_pg_backends='absent',new_probe=False))
        self.source_unchanged()

    def repair_fault(self, *, committed):
        self.source_unchanged()
        label='monitor-outcome-commit-loss' if committed else 'monitor-outcome-rollback'
        mode='after-complete' if committed else 'before-forward'
        output=self.root/label;output.mkdir(mode=0o700)
        before=self.after_recovery if committed else self.before_recovery
        old_dump=self.after_dump if committed else self.before_dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'peer observer lost before repair fault')
        self.backend_absence(output)
        try:
            self.start_checkpoint_proxy(output,mode)
            selected=self.select_checkpoint()
            deadline=time.monotonic()+30
            entry=self.launch(label,'app','ingest','recover','--file','/etc/bigbrotr/instance.toml')
            fault=self.checkpoint_fault(output,selected,mode,deadline=deadline)
            left=deadline-time.monotonic()
            require(left>0, 'repair command fixture deadline')
            notices=self.joined_unknown(entry,output,timeout=min(20,left))
            require(time.monotonic()<deadline, 'repair command fixture deadline')
            require(not any(row.get('format') in ('bigbrotr-ingest-recovery/1','bigbrotr-monitor-once/1') for row in notices),
                'uncertain repair reported completion or ran Monitor')
        finally:
            failed=sys.exception() is not None
            self.config.write_text(self.direct_recipe)
            if getattr(self,'fault_proxy',None) is not None:self.finish_fault_proxy(output,failed=failed)
        exclusion=output/'fault-owner-exclusion';exclusion.mkdir(mode=0o700)
        self.backend_absence(exclusion)
        after,catalog=self.capture(output,'committed' if committed else 'rolled-back')
        require(catalog==self.catalog, 'repair fault changed scientific catalog')
        if committed:oracle.repair_commit_loss(before,after,self.initialized,self.receipts,self.cbor)
        else:oracle.repair_rollback(before,after,self.initialized,self.receipts)
        control=max((row for row in after['owner_epochs'] if row['role']=='control'),key=lambda row:row['epoch'])
        require(control['control_backend_pid']==fault['backend_pid'], 'fault was not the fresh recovery Control backend')
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)==self.original_inputs
            and self.files(self.state/'data/ingest/producer-inputs-v1')==self.original_files, 'repair fault changed original inputs')
        if committed:
            oracle.repaired_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode)
        else:
            oracle.before_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode)
        dump=native.census(self,output,expected_writer_exit=4,writer_terminal=output/'terminal-container.json')
        if committed:
            passive.native_ack(dump,after,self.initialized,self.receipts,self.cbor,phase='closing-unacknowledged')
        else:
            passive.native_physical(dump,self.initialized,self.receipts,self.cbor,
                passive.raw(after['producer_requests'][0]['manifest_sha256'],32),phase='closing-unacknowledged')
        oracle.native_repair_fault(old_dump,dump,self.initialized,committed=committed)
        save(output/'native-census.json',dump)
        self.after_recovery,self.after_dump=after,dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'repair fault issued another peer connection')
        save(output/'independent-verdict.json',dict(status='PASS',product_result='CommitUnknown',
            checkpoint=4 if committed else 3,native_issued=4,native_acknowledged=3,native_retired=0,
            native_writes=0 if committed else 1,producer='closing',new_probe=False,sync_jobs=0,acquisition_progress=0))
        self.source_unchanged()

    def rollback_repair(self):
        self.repair_fault(committed=False)

    def lose_repair_commit(self):
        self.repair_fault(committed=True)

    def seed_existing_outcome(self):
        from cases import first_launch_monitor_outcome_seed as seed
        import re
        self.source_unchanged()
        output=self.root/'monitor-existing-outcome-seed';output.mkdir(mode=0o700)
        exclusion=output/'old-owner-exclusion';exclusion.mkdir(mode=0o700)
        self.backend_absence(exclusion)
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'peer lost before explicit seed')
        manifest=passive.native_rows(self.before_dump)[('default',b'\x01\x01')]
        require(manifest.hex()==self.seed_binding['manifest_hex'], 'Native manifest changed from pre-admission binding')
        request=output/'seed-request.json'
        save(request,dict(format='bigbrotr-monitor-outcome-seed/1',**self.seed_binding,
            policies=self.seed_policies,expected_receipt_hex=(b'bigbrotr/receipt/1\0'+self.cbor(self.receipts[3])).hex()))
        require(request.stat().st_size<=512*1024, 'closed seed request exceeds its bound')
        result_path=self.state/'data/monitor-outcome-seed-result.json'
        require(not result_path.exists(), 'seed result must be fresh')
        entry=seed.launch(self,request)
        ended=self.docker('wait',entry['id'],timeout=30,label='monitor-seed-join')
        info=self.inspect(entry);logged=self.docker('logs',entry['id'])
        (output/'seed.stdout').write_text(logged.stdout);(output/'seed.stderr').write_text(logged.stderr)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        require(ended.stdout.strip()=='0' and info['State']['ExitCode']==0 and info['State']['Pid']==0
            and not info['State']['Running'] and not info['State']['OOMKilled'] and not logged.stderr
            and seed.TEST in logged.stdout
            and re.search(r'test result: ok\. 1 passed; 0 failed; 0 ignored;',logged.stdout), 'test-only seed did not positively join')
        require(load(result_path)==dict(format='bigbrotr-monitor-outcome-seed-result/1',status='PASS',checkpoint=3,outcome=4,acknowledged=3),
            'seed receipt incomplete')
        exclusion=output/'seed-owner-exclusion';exclusion.mkdir(mode=0o700)
        self.backend_absence(exclusion)
        after,catalog=self.capture(output,'seeded')
        require(catalog==self.catalog, 'seed changed scientific catalog')
        oracle.seeded_outcome(self.before_recovery,after,self.initialized,self.receipts,self.cbor)
        oracle.seeded_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode,repaired=False)
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)==self.original_inputs
            and self.files(self.state/'data/ingest/producer-inputs-v1')==self.original_files, 'seed changed original inputs')
        dump=native.census(self,output,writer_terminal=output/'terminal-container.json')
        passive.native_ack(dump,after,self.initialized,self.receipts,self.cbor,phase='closing-unacknowledged')
        oracle.native_repair_fault(self.before_dump,dump,self.initialized,committed=False)
        save(output/'native-census.json',dump)
        self.before_recovery,self.before_dump=after,dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'seed issued new peer work')
        save(output/'independent-verdict.json',dict(status='PASS',scope='explicit-test-only-seed',checkpoint=3,
            outcome=4,native_issued=4,native_acknowledged=3,native_retired=0,native_writes=1,new_probe=False))
        self.source_unchanged()

    def recovery(self, *, retry):
        self.source_unchanged()
        label=self.recovery_label(retry=retry)
        output=self.root/label;output.mkdir(mode=0o700)
        before=self.after_recovery if retry or self.repair_faults else self.before_recovery
        old_dump=self.after_dump if retry or self.repair_faults else self.before_dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'peer observer lost before recovery')
        self.backend_absence(output)
        report=self.finite(label,'ingest','recover','--file','/etc/bigbrotr/instance.toml',timeout=30)
        oracle.completion([report],self.initialized)
        entry=self.resources['containers'][-1];info=self.inspect(entry)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        after,catalog=self.capture(output,'recovered')
        require(catalog==self.catalog, 'ACK recovery changed scientific catalog')
        if self.existing_outcome:oracle.repaired_existing(before,after,self.initialized,self.receipts,retry=retry)
        elif self.repair_faults:oracle.repair_ack(before,after,self.initialized,self.receipts,self.cbor)
        elif self.native_outcome:oracle.repaired(before,after,self.initialized,self.receipts,self.cbor,retry=retry)
        else:oracle.recovered(before,after,retry=retry)
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)==self.original_inputs
            and self.files(self.state/'data/ingest/producer-inputs-v1')==self.original_files, 'recovery changed original inputs')
        if self.existing_outcome:
            receipts=oracle.seeded_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode,repaired=True)
        elif self.native_outcome:
            receipts=oracle.repaired_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode)
        else:receipts=oracle.terminal(after,self.initialized,self.selection,self.original_inputs,self.cbor,self.decode)
        require(receipts==self.receipts, 'recovery changed exact receipts')
        dump=native.census(self,output,writer_terminal=output/'terminal-container.json')
        passive.native_ack(dump,after,self.initialized,receipts,self.cbor,phase='recovered')
        if (self.repair_faults or self.existing_outcome) and not retry:oracle.native_repair_ack(old_dump,dump,self.initialized)
        else:oracle.native_transition(old_dump,dump,self.initialized,retry=retry)
        save(output/'native-census.json',dump)
        self.after_recovery,self.after_dump=after,dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'recovery issued another peer connection')
        save(output/'independent-verdict.json',dict(status='PASS',checkpoint=4,native_issued=4,native_acknowledged=4,
            native_retired=0,producer='closing',exact_retry=retry,new_probe=False,sync_jobs=0,acquisition_progress=0))
        self.source_unchanged()

    def recover_ack(self):
        self.recovery(retry=False)

    def retry_ack(self):
        self.recovery(retry=True)
        (self.home/'observed.continue').write_text('both installed recoveries joined; exact native and PG state checked\n')
        require(self.client.wait(timeout=6)==0 and not (self.home/'client.stderr').read_bytes(), 'retained passive peer failed')
        stop_process(self.client);self.processes.remove(self.client)
        require(load(self.home/'client-result.json')['status']=='PASS', 'peer result incomplete')
        ended=load(self.home/'listener-ended.json')
        require(ended['connections']==1 and ended['completed'] is True and ended['final_queue_empty'] is True
            and ended['listener_closed'] is True, 'peer did not positively join after exact retry')
        save(self.root/self.recovery_label(retry=True)/'peer-verdict.json',dict(status='PASS',connections=1,auth_responses=0,publishes=0,
            listener_joined=True,no_replacement_probe=True))

    def cleanup(self):
        proxy=getattr(self,'fault_proxy',None)
        if proxy is not None:
            proxy.stop();self.fault_proxy=None
        super().cleanup()
