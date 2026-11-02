"""Lose only the known terminal C4 reply, then use installed historical recovery."""
import json
from pathlib import Path
import sys

from cases.first_launch import digest, load, require, save, stop_process
from cases.first_launch_passive_monitor import PassiveMonitorRuntime, publish_selection
from cases import first_launch_passive_oracle as passive
from cases import first_launch_retained_native as native
from cases import first_launch_monitor_ack_recovery_oracle as oracle

STAGES=('prerequisites','candidate','runtime_image','fresh_bootstrap',
    'passive_checkpoint_reply_loss','monitor_ack_recovery','monitor_ack_retry','cleanup')
LIMITS=[
    'One passive socket operation; lose only the actual generic revision4/C4 COMMIT reply after PostgreSQL completed it. Independent joined-store census must first prove I4/A3/R0 with the exact ten facts/four headers/four receipts.',
    'Installed default ingest recover uses fresh Control and exclusive historical native custody, closes the original producer and confirms ACK4 without new source I/O, Monitor token, Sync/acquisition progress or retirement. Exact retry adds no native write.',
    'All original attempt/request/input/receipt/science and checkpoint/dependency xmins remain retained. The existing peer observes through both recoveries, bounded to120s. This does not qualify nativeI4/C3 advancement, staged-before-PG, lost RAM, durable start, renewal, fleet cadence, reclamation or consumer publication during recovery.',
]


class MonitorAckRecoveryRuntime(PassiveMonitorRuntime):
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
        self.config.write_text(self.set_value(self.config.read_text(),'topology','local_roles',['control','ingest','monitor']))
        self.inputs['initial_recipe_sha256']=digest(self.config)
        save(self.home/'monitor-ack-recovery.json',dict(format='bigbrotr-monitor-ack-recovery-peer/1',
            boundary='checkpoint-after-complete',observed_seconds=120))

    def passive_launch(self):
        return self.launch('monitor-checkpoint-reply-loss','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,monitor','--initialize-ingest','--monitor-once','/var/lib/bigbrotr/monitor-once.json')

    def checkpoint_reply_loss(self):
        output=self.root/'passive-checkpoint-reply-loss';output.mkdir(mode=0o700)
        self.direct_recipe=self.config.read_text()
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        args=[sys.executable,'-B',str(path),'--upstream-port','15432','--mode','after-complete',
            '--target','control-measurement-checkpoint','--defer-arm']
        self.fault_proxy=self.proxy_process(args,input_pipe=True)
        try:
            ready=self.proxy_event(self.fault_proxy,'ready',5)
            save(output/'proxy-ready.json',ready)
            save(output/'proxy-process.json',dict(argv=args,pid=self.fault_proxy.process.pid,source_sha256=digest(path)))
            require(ready['source_sha256']==digest(path) and ready['armed'] is False and ready['upstream_port']==15432,
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
            oracle.fault(fault,self.initialized['dataset_id'],self.initialized['producer_slot_id'])
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
            self.proxy_send(self.fault_proxy,'stop\n')
            require(self.fault_proxy.wait(5)==0 and not self.fault_proxy.stderr and not self.fault_proxy.overflow,
                'checkpoint proxy did not terminate cleanly')
        finally:
            self.config.write_text(self.direct_recipe)
            self.fault_proxy.stop()
            (output/'proxy.stdout').write_bytes(self.fault_proxy.stdout)
            (output/'proxy.stderr').write_bytes(self.fault_proxy.stderr)
            save(output/'proxy-terminal.json',dict(exit_code=self.fault_proxy.process.returncode,overflow=self.fault_proxy.overflow))
            self.fault_proxy=None
        exclusion=output/'old-owner-exclusion';exclusion.mkdir(mode=0o700)
        self.backend_absence(exclusion)
        self.before_recovery,self.catalog=self.capture(output,'committed')
        dispatch=passive.only(self.before_recovery['measurement_dispatches'])
        control=passive.only(self.before_recovery['owner_epochs'],role='control',resource_id=dispatch['issuer_resource'],epoch=dispatch['issuer_epoch'])
        require(control['control_backend_pid']==fault['backend_pid'], 'fault was not the original Monitor Control backend')
        oracle.eligible(self.before_recovery)
        self.original_inputs=self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)
        self.original_files=self.files(self.state/'data/ingest/producer-inputs-v1')
        self.receipts=oracle.terminal(self.before_recovery,self.initialized,self.selection,self.original_inputs,self.cbor,self.decode)
        self.before_dump=native.census(self,output,expected_writer_exit=4,writer_terminal=output/'terminal-container.json')
        passive.native_ack(self.before_dump,self.before_recovery,self.initialized,self.receipts,self.cbor,phase='reply-lost')
        save(output/'native-census.json',self.before_dump)
        save(output/'independent-verdict.json',dict(status='PASS',checkpoint=4,native_issued=4,native_acknowledged=3,
            native_retired=0,terminal='known-durable',original_writer='joined',original_pg_backends='absent',new_probe=False))
        self.source_unchanged()

    def recovery(self, *, retry):
        self.source_unchanged()
        output=self.root/('monitor-ack-retry' if retry else 'monitor-ack-recovery');output.mkdir(mode=0o700)
        before=self.after_recovery if retry else self.before_recovery
        old_dump=self.after_dump if retry else self.before_dump
        require(self.client.poll() is None and not (self.home/'relay-error.json').exists(), 'peer observer lost before recovery')
        self.backend_absence(output)
        report=self.finite('monitor-ack-retry' if retry else 'monitor-ack-recovery','ingest','recover','--file','/etc/bigbrotr/instance.toml',timeout=30)
        oracle.completion([report],self.initialized)
        entry=self.resources['containers'][-1];info=self.inspect(entry)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        after,catalog=self.capture(output,'recovered')
        require(catalog==self.catalog, 'ACK recovery changed scientific catalog')
        oracle.recovered(before,after,retry=retry)
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)==self.original_inputs
            and self.files(self.state/'data/ingest/producer-inputs-v1')==self.original_files, 'recovery changed original inputs')
        receipts=oracle.terminal(after,self.initialized,self.selection,self.original_inputs,self.cbor,self.decode)
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
        save(self.root/'monitor-ack-retry/peer-verdict.json',dict(status='PASS',connections=1,auth_responses=0,publishes=0,
            listener_joined=True,no_replacement_probe=True))

    def cleanup(self):
        proxy=getattr(self,'fault_proxy',None)
        if proxy is not None:
            proxy.stop();self.fault_proxy=None
        super().cleanup()
