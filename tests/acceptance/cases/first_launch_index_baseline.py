"""Closed shared Index ordinary baseline; all startup observation consumes40s."""
import subprocess
import sys
import time

from cases.first_launch import load, require, save, stop_process
from cases.first_launch_index_pin import ShortPinRuntime, process_identity
from cases import first_launch_index_oracle as oracle


def ordinary_client(self, output, entry):
    """Observe startup completion inside the original40s client deadline."""
    selected = load(self.home/'export-input.json')
    require('readiness_mode' not in selected, 'original ordinary journey profile')
    save(self.home/'export-input.json',dict(selected,startup_quiescence=True))
    deadline = time.monotonic()+40
    command = [sys.executable,'-B',str(self.source/'experiments/ingest-bootstrap/installed_archive_export_client.py'),str(self.home)]
    with (output/'ordinary-client.stdout').open('w') as out,(output/'ordinary-client.stderr').open('w') as err:
        client = subprocess.Popen(command,env=self.environment,cwd=self.root,stdin=subprocess.DEVNULL,
            stdout=out,stderr=err,start_new_session=True)
        self.processes.append(client)
        save(output/'ordinary-client-process.json',dict(pid=client.pid,argv=command,limit_seconds=40,status='running'))
        try:
            marker = self.home/'ordinary-setup.json'
            self.wait(lambda:marker.exists(),'ordinary startup observation',max(.001,deadline-time.monotonic()),entry,client)
            signal = load(marker)
            require(signal['phase'] == 'setup' and type(signal['started_ns']) is int
                    and 0 < signal['deadline_ns']-signal['started_ns'] <= 30_000_000_000,
                    'bounded startup observation declaration')
            phase_deadline = min(deadline,signal['deadline_ns']/1_000_000_000)
            before = load(output/'before-control.json')
            startup_quiescence(self,output/'startup',entry,before,phase_deadline)
            require(time.monotonic() < phase_deadline, 'startup observation consumed original client window')
            (self.home/'ordinary-setup.continue').write_text('completed full startup probe independently checked\n')
            client.wait(timeout=max(.001,deadline-time.monotonic()))
            require(client.returncode == 0 and time.monotonic() < deadline
                    and not (output/'ordinary-client.stderr').read_bytes(), 'original ordinary client failed or exceeded40s')
        finally:
            stop_process(client)
            self.processes.remove(client)
            save(output/'ordinary-client-process.json',dict(pid=client.pid,argv=command,limit_seconds=40,status='joined',exit_code=client.returncode))


def startup_quiescence(self, output, entry, historical, deadline):
    output.mkdir(mode=0o700)
    parent = process_identity(self.inspect(entry)['State']['Pid'])
    require(parent is not None, 'actual startup Control process')
    clock_before = ShortPinRuntime.bounded_sql(self,'bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
    observations = []
    while True:
        require(len(observations) < 100 and time.monotonic() < deadline, 'startup observation bound')
        value = ShortPinRuntime.roles(self,deadline); observations.append(value)
        save(output/'roles.json',observations)
        # NotObserved also exists before the first probe. A status response can
        # precede pin admission; an independent capture must establish completion.
        if len(value['capabilities']) == 2 and all(cap['state'] == 'building'
                and cap['reason'] == 'not_observed' for cap in value['capabilities']):
            state,catalog = self.capture(output,'complete')
            clock_after = ShortPinRuntime.bounded_sql(self,'bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
            oracle.ordinary_roles(value,state,clock_before,clock_after,'setup')
            if oracle.startup_reader_pending(historical,state):
                require(process_identity(parent['pid']) == parent and time.monotonic() < deadline,
                        'startup reader pending outside original authority/deadline')
                save(output/('pending-'+str(len(observations))+'.json'),dict(status='PENDING',
                    reason='startup_reader_not_completed',clock_after_us=clock_after,
                    reader_rows={name:state[name] for name in
                        ('snapshot_leases','archive_snapshot_pins','retention_claims')}))
                require(self.inspect(entry)['State']['Running'], 'owner ended while startup reader pending')
                time.sleep(.05)
                continue
            worker = ShortPinRuntime.worker(self,parent,allow_absent=True)
            owner = oracle.ordinary_startup(historical,state,value,clock_before,clock_after,worker)
            require(process_identity(parent['pid']) == parent and time.monotonic() < deadline, 'same startup parent within original deadline')
            save(output/'verdict.json',dict(status='PASS',probe='complete',operation_cache='empty',
                capabilities='building/not_observed',parent=parent,worker=worker,
                clock_before_us=clock_before,clock_after_us=clock_after))
            return state,catalog,value,clock_after,parent,owner
        require(self.inspect(entry)['State']['Running'], 'owner ended during startup observation')
        time.sleep(.05)
