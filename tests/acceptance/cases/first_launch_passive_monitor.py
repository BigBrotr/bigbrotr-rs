"""First installed passive measurement through persisted standalone inspection."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib

from cases.first_launch import Runtime, digest, load, require, save, stop_process
from cases import first_launch_passive_oracle as oracle

STAGES = ('prerequisites','candidate','runtime_image','fresh_bootstrap',
          'passive_measurement','restarted_measurement_inspection','cleanup')
LIMITS = [
    'One operator-enrolled direct private endpoint, one literal passive kind1/limit1 request and one compound terminal A05 fact, persisted through Archive and read by the installed exact typed inspector after process/PostgreSQL restart.',
    'No Sync scope/job or event-body admission; peer EVENT is verification input only. No fleet cadence, authority renewal, crash/unknown recovery, availability reducer, public observatory, full P04-T07 or M2 qualification.',
    'Independent Python wire/CBOR/PG expectations; no native RocksDB/C-API census. Configured budgets are not measured capacity. Private credentials, sessions and scientific captures remain protected.',
]
ENDPOINT = 'ws://127.0.0.1:7447/installed-fixture'


def deployment_recipe(text, set_value):
    for section,key,value in [
        ('topology','local_roles',['control','ingest','monitor','archive']),
        ('sync','enabled',False),('monitor','enabled',True),
        ('monitor','checks',['open','read']),('monitor','store_checks',['open','read']),
        ('monitor','check_timeout','15s'),('monitor','retries',1),
        ('monitor.endpoints','mode','fixed'),('monitor.endpoints','urls',[ENDPOINT])]:
        text = set_value(text,section,key,value)
    value = tomllib.loads(text)
    require(value['archive']['reader']['enabled'] is False, 'reader must start explicitly disabled')
    return text


def publish_selection(path, selection):
    """The product owns the already pinned directory; publish one complete inode."""
    require(not path.exists() and not path.is_symlink(), 'one-shot selection already exists')
    data = (json.dumps(selection,separators=(',',':'))+'\n').encode()
    require(len(data) <= 4096, 'bounded source selection')
    partial = path.with_name(path.name+'.partial')
    descriptor = os.open(partial,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_CLOEXEC,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(partial,path)
    directory = os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    require(path.stat().st_mode & 0o777 == 0o600 and path.stat().st_nlink == 1, 'protected source selection inode')


def inspection_records(stdout, stderr, code):
    require(code == 0 and not stderr, 'standalone measurement inspection refused or unresolved')
    lines = stdout.splitlines()
    require(len(lines) == 2 and len(lines[0].encode())+1 <= 20*1024, 'one bounded science document and completion required')
    document, completion = map(json.loads,lines)
    require(completion == dict(format='bigbrotr-archive-measurement-completion/1',state='complete',
        owner_release='committed',policy_revision='1'), 'inspection cleanup did not complete')
    require(document.get('format') == 'bigbrotr-archive-measurement/1' and document.get('state') == 'present',
        'typed original measurement missing')
    return document


class PassiveMonitorRuntime(Runtime):
    def load_helpers(self):
        super().load_helpers()
        import installed_passive_monitor_client
        from installed_sync_oracle import decode
        from installed_archive_catch_up_oracle import cut_digest
        from cases.first_launch_restart import originals
        self.decode, self.cut_digest, self.originals = decode, cut_digest, originals
        spec = importlib.util.spec_from_file_location('passive_identity_oracle',self.source/'experiments/identity-contract/probe.py')
        encoder = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = encoder
        spec.loader.exec_module(encoder)
        self.cbor = encoder.cbor

    def candidate(self):
        require(digest(Path(__file__)) == digest(self.source/'tests/acceptance/cases/first_launch_passive_monitor.py'),
            'passive Monitor driver source mismatch')
        require(digest(Path(oracle.__file__)) == digest(self.source/'tests/acceptance/cases/first_launch_passive_oracle.py'),
            'passive Monitor oracle source mismatch')
        super().candidate()

    def prepare_state(self):
        super().prepare_state()
        self.config.write_text(deployment_recipe(self.config.read_text(),self.set_value))
        self.inputs['initial_recipe_sha256'] = digest(self.config)
        destination = self.root/'passive-monitor'
        self.home.rename(destination)
        self.home = destination
        self.selection_path = self.state/'data/monitor-once.json'
        require(not self.selection_path.exists(), 'Monitor selection must initially be absent')

    def passive_measurement(self):
        self.source_unchanged()
        output = self.root/'passive-measurement'; output.mkdir(mode=0o700)
        before, _ = self.capture(output,'before')
        require(not before['producer_requests'] and not before['measurement_dispatches'], 'Monitor setup not fresh')
        self.before_us = time.time_ns()//1000
        entry = self.launch('passive-monitor','app','serve','--file','/etc/bigbrotr/instance.toml',
            '--roles','control,ingest,monitor,archive','--initialize-ingest','--monitor-once','/var/lib/bigbrotr/monitor-once.json')
        first = self.wait(lambda:self.logs(entry,output),'armed passive Monitor',20,entry=entry)[0]
        require(first['state'] == 'initialized' and first['ready'] is False
            and first['acquisition'] == 'not-started'
            and first['monitor_once'] == dict(format='bigbrotr-monitor-once/1',state='armed',profile=1,ready=False),
            'wrong Monitor startup profile')
        self.initialized = first
        save(output/'initialized.json',first)
        save(self.home/'visibility.json',first['private_scope'])
        with (self.home/'client.stdout').open('x') as stdout, (self.home/'client.stderr').open('x') as stderr:
            self.client = subprocess.Popen([sys.executable,'-B',str(self.source/'experiments/ingest-bootstrap/installed_archive_client.py'),str(self.home)],
                env=self.environment,cwd=self.root,stdout=stdout,stderr=stderr,start_new_session=True)
        self.processes.append(self.client)
        save(output/'client-process.json',dict(pid=self.client.pid,process_group=self.client.pid))
        self.wait(lambda:(self.home/'prepared.json').exists(),'real Monitor endpoint and enrollment',15,entry,self.client)
        prepared, _ = self.capture(output,'prepared')
        self.selection = oracle.source_selection(load(self.home/'prepared.json'),prepared,self.initialized)
        require(load(self.home/'source-selection.json') == self.selection, 'client source selection differs from actual enrollment')
        require((self.home/'listener-started.json').exists() and not (self.home/'relay-exchange.json').exists()
            and not (self.home/'relay-error.json').exists() and not self.selection_path.exists(), 'work began before one-shot selection')
        deadline = time.monotonic()+65
        def remaining(maximum):
            left = deadline-time.monotonic()
            require(left > 0, 'passive measurement/publication fixture deadline')
            return min(maximum,left)
        publish_selection(self.selection_path,self.selection)
        (self.home/'prepared.continue').write_text('real enrollment and protected source selection checked\n')
        def terminal():
            require(not (self.home/'relay-error.json').exists(), 'passive peer failed')
            records = [row for row in self.logs(entry,output) if row.get('format') == 'bigbrotr-monitor-once/1']
            states = [row['state'] for row in records]
            # The watch-based diagnostic observer may coalesce selected→completed.
            # A selected notice is not an admission and cannot satisfy this gate.
            require(states in ([],['selected'],['completed'],['selected','completed']),
                'Monitor repeated or no-I/O/refused/parked outcome')
            if states and states[-1] == 'completed':
                require(records[-1]['result'] == 'admitted', 'Monitor did not admit a measurement')
                return records[-1]
        self.report = self.wait(terminal,'admitted compound measurement',remaining(20),entry,self.client)
        self.wait(lambda:(self.home/'relay-complete.json').exists(),'passive CLOSE/WSclose/EOF',remaining(5),entry,self.client)
        self.wait(lambda:self.sql('bigbrotr_control',"SELECT count(*) FROM control.consumer_frontiers WHERE last_complete_sequence=10") == '1',
            'complete measurement Archive frontier',remaining(60),entry,self.client)
        self.published, self.catalog = self.capture(output,'published')
        self.refs = oracle.completed(self.report,self.published,self.initialized,self.selection)
        self.wire = load(self.home/'relay-exchange.json')
        require(load(self.home/'relay-complete.json')['connections'] == 1, 'unexpected peer connection count')
        self.receipts = [self.decode(oracle.raw(row['outcome_bytes']).split(b'\0',1)[1])
            for row in sorted(self.published['producer_outcomes'],key=lambda row:row['request_sequence'])]
        self.cut = oracle.publication(self.published,self.initialized,self.ids['catalog'],self.receipts,self.cbor,self.cut_digest)
        remaining(1)  # SQL/capture/oracle time belongs to the same finite gate.
        save(output/'monitor-completed.json',self.report)
        (self.home/'observed.continue').write_text('exact outcome and published cut checked\n')
        self.wait(lambda:(self.home/'commands-finished.json').exists(),'Monitor client logout',15,entry,self.client)
        terminal_info = self.stop_container(entry,output)
        final = self.logs(entry,output)[-1]
        require(terminal_info['State']['ExitCode'] == 0 and final['state'] == 'stopped'
            and final['native'] == 'closed' and final['owner_release'] == 'committed', 'Monitor owner did not join and release')
        (self.home/'commands-finished.continue').write_text('real owner joined\n')
        require(self.client.wait(timeout=6) == 0 and not (self.home/'client.stderr').read_bytes(), 'passive client failed')
        stop_process(self.client); self.processes.remove(self.client)
        require(load(self.home/'client-result.json')['status'] == 'PASS', 'passive client did not complete')
        ended = load(self.home/'listener-ended.json')
        require(ended['connections'] == 1 and ended['completed'] is True and ended['listener_closed'] is True,
            'owned passive listener did not join')
        self.stopped, stopped_catalog = self.capture(output,'stopped')
        self.refs = oracle.completed(self.report,self.stopped,self.initialized,self.selection)
        require(self.catalog == stopped_catalog, 'orderly stop changed scientific catalog')
        dispatch = oracle.only(self.stopped['measurement_dispatches'])
        owner = oracle.only(self.stopped['owner_epochs'],role='control',resource_id=dispatch['issuer_resource'],epoch=dispatch['issuer_epoch'])
        require(owner['state'] == 'released', 'Control release not retained')
        self.after_us = time.time_ns()//1000
        require(self.before_us <= self.after_us, 'host UTC regressed across fixture bracket')
        self.original_inputs = self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4)
        self.archive_files = self.files(self.state/'data/archive')
        save(output/'archive-files.json',self.archive_files)
        self.backend_absence(output)
        self.source_unchanged()

    def restarted_measurement_inspection(self):
        self.source_unchanged()
        output = self.root/'restarted-measurement-inspection'; output.mkdir(mode=0o700)
        # Real same-data PostgreSQL restart; no replay, fake pin or fixture DB write.
        require(self.stop_container(self.pg)['State']['ExitCode'] == 0, 'PostgreSQL did not stop cleanly')
        self.docker('start',self.pg['id'])
        self.wait(lambda:self.docker('exec',self.pg['id'],'pg_isready','-h','127.0.0.1','-U','postgres','-d','postgres',check=False).returncode == 0,
            'same-data PostgreSQL restart',60)
        before, catalog = self.capture(output,'restarted')
        for table in ('measurement_dispatches','producer_requests','producer_outcomes','archive_frontier_snapshots','archive_frontier_batches','consumer_frontiers'):
            require(before[table] == self.stopped[table], 'restart changed retained '+table)
        require(catalog == self.catalog, 'restart changed scientific catalog')
        original = self.finite('measurement-reader-disabled','config','show','--file','/etc/bigbrotr/instance.toml')
        self.config.write_text(self.set_value(self.config.read_text(),'archive.reader','enabled',True))
        enabled = self.finite('measurement-reader-enabled','config','show','--file','/etc/bigbrotr/instance.toml')
        require(original['initial_policy'] == enabled['initial_policy'], 'reader deployment root changed persisted policy')
        self.inputs['inspection_recipe_sha256'] = digest(self.config)
        coordinate = self.report['measurement']
        entry = self.launch('measurement-inspection','app','archive','measurement-inspect','--file','/etc/bigbrotr/instance.toml',
            '--shard-id',str(coordinate['shard_id']),'--owner-epoch',coordinate['owner_epoch'],
            '--sequence',coordinate['sequence'],'--payload-digest',coordinate['payload_digest'])
        ended = self.docker('wait',entry['id'],timeout=45)
        logged = self.docker('logs',entry['id'])
        (output/'inspection.stdout').write_text(logged.stdout); (output/'inspection.stderr').write_text(logged.stderr)
        info = self.inspect(entry)
        require(ended.stdout.strip() == '0' and info['State']['Pid'] == 0 and not info['State']['OOMKilled'], 'inspection child did not join')
        document = inspection_records(logged.stdout,logged.stderr,info['State']['ExitCode'])
        require(document['catalog_id'] == self.ids['catalog'] and document['frontier_revision'] == '4'
            and document['topology_generation'] == '0' and document['cut'] == self.cut, 'inspection selected a different publication')
        measurement, encoded = oracle.scientific(document,self.report,self.refs,self.wire,load(self.home/'event.jsonl'),self.before_us,self.after_us,self.cbor)
        receipts = oracle.retained(self.stopped,self.initialized,self.report,self.refs,measurement,self.original_inputs,self.cbor,self.decode)
        require(oracle.publication(self.stopped,self.initialized,self.ids['catalog'],receipts,self.cbor,self.cut_digest) == self.cut,
            'retained original changed publication')
        after, after_catalog = self.capture(output,'after')
        require(after_catalog == catalog and self.files(self.state/'data/archive') == self.archive_files,
            'typed inspection mutated persisted scientific data')
        require(self.originals(self.state/'data/ingest',self.initialized['producer_slot_id'],1,4) == self.original_inputs,
            'typed inspection changed original producer inputs')
        for table in ('measurement_dispatches','producer_requests','producer_outcomes'):
            require(after[table] == before[table], 'typed inspection changed retained '+table)
        oracle.inspection_pin(before,after,self.ids['catalog'])
        self.backend_absence(output)
        self.source_unchanged()
        save(output/'independent-verdict.json',dict(status='PASS',attempt_id=self.report['attempt_id'],
            measurement=self.report['measurement'],canonical_fact_sha256=hashlib.sha256(encoded).hexdigest(),
            family_counts=oracle.COUNTS,source_cut=10,producer_requests=4,snapshot_id=5,frontier_revision=4,
            real_peer_connections=1,auth_responses=0,publishes=0,sync_jobs=0,event_family_facts=0,
            native_census='NOT_RUN',qualification_scope='one-passive-attempt-and-restarted-typed-inspection'))
