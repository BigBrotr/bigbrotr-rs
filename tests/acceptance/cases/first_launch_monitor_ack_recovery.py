"""Select terminal C4 COMMIT loss, then use installed historical recovery."""
import json
from pathlib import Path
import sys
import time

from cases.first_launch import digest, load, require, save, stop_process
from cases.first_launch_passive_monitor import PassiveMonitorRuntime, publish_selection
from cases import first_launch_passive_oracle as passive
from cases import first_launch_retained_native as native
from cases import first_launch_monitor_ack_recovery_oracle as oracle

STAGES=('prerequisites','candidate','runtime_image','fresh_bootstrap',
    'passive_checkpoint_reply_loss','monitor_ack_recovery','monitor_ack_retry','cleanup')
NATIVE_OUTCOME_STAGES=STAGES[:4]+('passive_native_outcome_loss','monitor_outcome_recovery','monitor_outcome_retry','cleanup')
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


class MonitorAckRecoveryRuntime(PassiveMonitorRuntime):
    native_outcome=False

    def __init__(self, args, inputs):
        self.native_outcome=args.case=='p04-monitor-native-outcome-recovery'
        super().__init__(args,inputs)

    @property
    def fault_mode(self):
        return 'before-forward' if self.native_outcome else 'after-complete'

    def recovery_label(self, *, retry):
        return ('monitor-outcome-' if self.native_outcome else 'monitor-ack-')+('retry' if retry else 'recovery')

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
        self.direct_recipe=self.config.read_text()
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        args=[sys.executable,'-B',str(path),'--upstream-port','15432','--mode',self.fault_mode,
            '--target','control-measurement-checkpoint','--profile','retained-catch-up','--defer-arm']
        self.fault_proxy=self.proxy_process(args,input_pipe=True)
        try:
            ready=self.proxy_event(self.fault_proxy,'ready',5)
            save(output/'proxy-ready.json',ready)
            save(output/'proxy-process.json',dict(argv=args,pid=self.fault_proxy.process.pid,source_sha256=digest(path)))
            require(ready['source_sha256']==digest(path) and ready['armed'] is False
                and ready['upstream_host']=='127.0.0.1' and ready['upstream_port']==15432
                and ready['mode']==self.fault_mode and ready['target']=='control-measurement-checkpoint'
                and tuple(ready[k] for k in ('profile','frame_limit','connection_limit','connection_limit_scope',
                    'wire_limit','lifetime_seconds'))==('retained-catch-up',131072,64,'lifetime',64*1024*1024,120),
                'wrong deferred checkpoint proxy')
            self.config.write_text(self.set_value(self.direct_recipe,'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            entry=self.passive_begin(output)
            self.writer=entry
            self.proxy_send(self.fault_proxy,'select-measurement '+self.initialized['dataset_id']+' '+self.initialized['producer_slot_id']+'\n')
            selected=self.proxy_event(self.fault_proxy,'measurement-selected',1)
            self.proxy_send(self.fault_proxy,'arm\n');self.proxy_event(self.fault_proxy,'armed',1)
            publish_selection(self.selection_path,self.selection)
            (self.home/'prepared.continue').write_text('real source checked; exact C4 selector armed\n')
            update=self.proxy_event(self.fault_proxy,'measurement-checkpoint-update-complete',20)
            fault=self.proxy_event(self.fault_proxy,'fault',5)
            check=oracle.fault_before_forward if self.native_outcome else oracle.fault
            check(fault,self.initialized['dataset_id'],self.initialized['producer_slot_id'])
            require(selected['measurement_identity_sha256']==update['measurement_identity_sha256']==fault['measurement_identity_sha256']
                and update['backend_pid']==fault['backend_pid'], 'fault changed its selected identity/backend')
            save(output/'proxy-fault.json',fault)
            ended=self.docker('wait',entry['id'],timeout=20,label='monitor-fault-owner-join')
            info=self.inspect(entry);logged=self.docker('logs',entry['id'])
            (output/'serve.stdout').write_text(logged.stdout);(output/'serve.stderr').write_text(logged.stderr)
            save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
            require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and info['State']['Pid']==0
                and not info['State']['Running'] and not info['State']['OOMKilled'] and 'CommitUnknown' in logged.stderr,
                'checkpoint reply loss did not join as explicit uncertainty')
            notices=[json.loads(line) for line in logged.stdout.splitlines()]
            monitor=[row for row in notices if row.get('format')=='bigbrotr-monitor-once/1']
            require(monitor and monitor[-1]['state']=='parked'
                and all(row['state'] in ('selected','parked') for row in monitor), 'lost reply reported completion or repeated work')
            self.wait(lambda:(self.home/'relay-complete.json').exists(),'one passive source teardown',5,client=self.client)
            require(not (self.home/'relay-error.json').exists(), 'peer failed')
        finally:
            failed=sys.exception() is not None
            self.config.write_text(self.direct_recipe)
            self.finish_fault_proxy(output,failed=failed)
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

    def recovery(self, *, retry):
        self.source_unchanged()
        label=self.recovery_label(retry=retry)
        output=self.root/label;output.mkdir(mode=0o700)
        before=self.after_recovery if retry else self.before_recovery
        old_dump=self.after_dump if retry else self.before_dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'peer observer lost before recovery')
        self.backend_absence(output)
        report=self.finite(label,'ingest','recover','--file','/etc/bigbrotr/instance.toml',timeout=30)
        oracle.completion([report],self.initialized)
        entry=self.resources['containers'][-1];info=self.inspect(entry)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        after,catalog=self.capture(output,'recovered')
        require(catalog==self.catalog, 'ACK recovery changed scientific catalog')
        if self.native_outcome:oracle.repaired(before,after,self.initialized,self.receipts,self.cbor,retry=retry)
        else:oracle.recovered(before,after,retry=retry)
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)==self.original_inputs
            and self.files(self.state/'data/ingest/producer-inputs-v1')==self.original_files, 'recovery changed original inputs')
        if self.native_outcome:
            receipts=oracle.repaired_terminal(after,self.initialized,self.selection,self.original_inputs,self.receipts,self.cbor,self.decode)
        else:receipts=oracle.terminal(after,self.initialized,self.selection,self.original_inputs,self.cbor,self.decode)
        require(receipts==self.receipts, 'recovery changed exact receipts')
        dump=native.census(self,output,writer_terminal=output/'terminal-container.json')
        passive.native_ack(dump,after,self.initialized,receipts,self.cbor,phase='recovered')
        oracle.native_transition(old_dump,dump,self.initialized,retry=retry)
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
