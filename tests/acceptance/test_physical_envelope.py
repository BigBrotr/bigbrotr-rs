"""Portable physical diagnostic controls; no PostgreSQL/native result simulated."""
import copy
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT/'experiments/policy-versions/run.py'
SPEC = importlib.util.spec_from_file_location('physical_envelope_controls',RUNNER.with_name('physical_envelope.py'))
PHYSICAL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PHYSICAL)


def captured_log():
    lines = ['2026/11/03 00:00:00.000000 123 DB Session ID:  session1']
    def event(value):
        lines.append('2026/11/03 00:00:00.000001 123 EVENT_LOG_v1 '+json.dumps(value))
    event(dict(event='flush_started',job=1,flush_reason='Write Buffer Full',total_data_size=100))
    lines.append('2026/11/03 00:00:00.000002 123 [facts] [JOB 1] Level-0 flush table #5: started')
    table = dict(event='table_file_creation',job=1,cf_name='facts',file_number=5,file_size=100,
                 status='OK',table_properties=dict(num_entries=10,db_session_id='session1'))
    event(table)
    event(dict(event='flush_finished',job=1))
    lines.append('2026/11/03 00:00:00.000003 123 [facts] Level-0 commit table #5: memtable #1 done')
    event(dict(event='compaction_started',job=2,cf_name='facts',compaction_reason='LevelL0FilesNum',
               input_data_size=200,files_L0=[5,6]))
    event(dict(table,job=2,file_number=7,file_size=150))
    lines.append('2026/11/03 00:00:00.000004 123 [facts] compacted to: files[0 1], '
        'MB/sec: 1.0 rd, 1.0 wr, level 1, files in(2, 0) filtered(0, 0) out(1 +0 blob) '
        'MB in(1.0, 0.0 +0.0 blob) filtered(0.0, 0.0) out(1.0 +0.0 blob), '
        'read-write-amplify(1.0) write-amplify(1.0) OK, records in: 10, records dropped: 0 output_compression: ZSTD')
    event(dict(event='compaction_finished',job=2,output_level=1,num_output_files=1,
               total_output_size=150,num_input_records=10,num_output_records=10))
    # Exact EnvLogger grammar: combined date-time, decimal thread token.
    return ('\n'.join(lines)+'\n').replace('2026/11/03 00:00:','2026/11/03-00:00:')


def verifier_fixture(events,corpus):
    """Synthetic metadata for false-PASS controls, never engine qualification."""
    expected = PHYSICAL.expected_science(events,corpus['sha256'],ROOT)
    control = dict(jobs=[],producer_epochs=[dict(producer_slot_id='\\x'+'42'*16,producer_epoch=1,
        dataset_id='\\x'+'01'*16,outstanding_request_limit=64,reserved_through_request=64,state='open',lab_xmin='1')],
        producer_requests=[],producer_outcomes=[])
    for n in range(1,65):
        identity = dict(request_sequence=n,producer_slot_id='\\x'+'42'*16,producer_epoch=1,dataset_id='\\x'+'01'*16,
            manifest_sha256='\\x'+expected['manifest_sha256'],request_digest='\\x'+PHYSICAL.digest(expected['requests'][n]),lab_xmin=str(n))
        control['producer_requests'].append(dict(identity,input_bytes=len(expected['requests'][n]),
            acquisition_job_id=None,relay_endpoint_id=None,acquisition_scope_id=None,
            relay_access_scope_id=None,relay_enrollment_generation=None,relay_route_id=None))
        control['producer_outcomes'].append(dict(identity,outcome_bytes='\\x'+expected['receipts'][n].hex(),
            outcome_sha256='\\x'+PHYSICAL.digest(expected['receipts'][n]),outcome_kind=1,policy_epoch=1))
    tables = copy.deepcopy(control)
    for rows in tables.values():
        for row in rows: row['xmin'] = row.pop('lab_xmin')
    state = dict(native_rows=expected['native_rows'],native_sha256=expected['native_sha256'],
        originals=expected['originals'],sequence=777,control_tables=[[name,json.dumps(rows)] for name,rows in tables.items()])
    credit = lambda n:dict(issued=n+1,acknowledged=1,outstanding=n,retired=0)
    values = [dict(event='physical-input-validated',source_sha256=corpus['sha256'],source_bytes=corpus['bytes'],
        events=63,signed_event_bytes=262144,absolute_request_limit=64,outstanding_request_limit=64,
        retained_input_upper_bound=20*1024**2,pending_high=256*1024**3),
        dict(event='physical-ready',receipt=expected['receipts'][1].hex(),pending=expected['batches'][1],credit=credit(0),
            profile=dict(block_cache_bytes=32*1024**2,write_buffer_manager_bytes=16*1024**2,
            per_cf_write_buffer_bytes=2*1024**2,max_total_wal_bytes=16*1024**2,max_background_jobs=2,
            max_open_files=64,native_allowance_bytes=128*1024**2)),
        dict(event='physical-checkpoint-ready',admitted_events=31)]
    for index,event in enumerate(events,1):
        values.append(dict(event='physical-admitted',index=index,request_sequence=index+1,event_id=event['id'],recorded_at=456,
            request_digest=PHYSICAL.digest(expected['requests'][index+1]),input_bytes=len(expected['requests'][index+1]),
            receipt=expected['receipts'][index+1].hex(),outcome_sha256=PHYSICAL.digest(expected['receipts'][index+1]),
            pending=expected['batches'][index+1],line=index,byte_start=(index-1)*262145,byte_end=index*262145,
            record_sha256=PHYSICAL.digest(PHYSICAL.compact(event)+b'\n'),credit=credit(index)))
    values.append(dict(event='physical-load-complete',admitted_events=63))
    closed = dict(event='physical-closed',state=state)
    reopened = dict(event='physical-reopened',state=copy.deepcopy(state),same_native_originals_outcomes_control=True)
    complete = dict(event='physical-complete',credit=credit(63),admitted_events=63,
        source_bound_event_acknowledgement='NOT_RUN',reclamation='NOT_RUN',reserve_qualification='NOT_RUN',open65='NOT_RUN')
    return [dict(value=value) for value in values],events,corpus,dict(jobs=[]),control,closed,reopened,complete,ROOT


def mutate_control(fixture,name,index,**fields):
    fixture[4][name][index].update(fields)
    tables = copy.deepcopy(fixture[4])
    for rows in tables.values():
        for row in rows: row['xmin'] = row.pop('lab_xmin')
    serialized = [[table,json.dumps(rows)] for table,rows in tables.items()]
    fixture[5]['state']['control_tables'] = serialized
    fixture[6]['state']['control_tables'] = copy.deepcopy(serialized)


class PhysicalScienceControls(unittest.TestCase):
    def test_literal_extension_matches_the_existing_independent_single_import_oracle(self):
        sys.path.insert(0,str(ROOT/'experiments/ingest-bootstrap'))
        from event_oracle import expected_event
        raw = (ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes()
        expected = PHYSICAL.expected_science([json.loads(raw)],hashlib.sha256(raw).hexdigest(),ROOT)
        rows,receipt,facts = expected_event(bytes([1])*16,True)
        # This diagnostic exercises the existing genuine bootstrap ACK1; the
        # older one-import fixture deliberately retains ACK0. Science is equal.
        key = b'\x01\x31'+bytes([66])*16+(1).to_bytes(8,'big')
        rows = [(cf,k,b'\x01\x31'+(2).to_bytes(8,'big')+(1).to_bytes(8,'big')+bytes(8)
                 if cf == 'receipts' and k == key else v) for cf,k,v in rows]
        native = [dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in rows]
        self.assertEqual(expected['native_sha256'],PHYSICAL.digest(PHYSICAL.compact(native)))
        self.assertEqual(expected['native_rows'],len(rows))
        self.assertEqual(expected['receipts'][2],receipt)
        self.assertEqual(expected['batches'][2]['facts'],[hashlib.sha256(value).hexdigest() for value in facts])
        self.assertEqual(expected['pending_bytes'],sum(len(k)+len(v) for cf,k,v in rows if cf == 'facts'))
        changed = PHYSICAL.expected_science([json.loads(raw)],'00'*32,ROOT)
        self.assertNotEqual(expected['native_sha256'],changed['native_sha256'])
        self.assertNotEqual(expected['receipts'][1],changed['receipts'][1])
        self.assertEqual(expected['requests'][2],changed['requests'][2])  # same qualified lineage reference
        self.assertNotEqual(expected['receipts'][2],changed['receipts'][2])  # changed dependency fact

    def test_declared_corpora_are_distinct_valid_signed_exact_maximum_units(self):
        try: available = importlib.metadata.version('secp256k1') == '0.14.0'
        except importlib.metadata.PackageNotFoundError: available = False
        if not available: self.skipTest('declared optional signer secp256k1==0.14.0 unavailable')
        import zlib
        with tempfile.TemporaryDirectory() as temporary:
            ratios = {}
            for case in PHYSICAL.CASES:
                events,descriptor = PHYSICAL.make_corpus(Path(temporary)/(case+'.jsonl'),case,ROOT)
                self.assertEqual(len(events),63)
                self.assertEqual(len({event['id'] for event in events}),63)
                self.assertEqual(descriptor['bytes'],63*262145)
                self.assertTrue(all(len(PHYSICAL.compact(event)) == 262144 for event in events))
                ratios[case] = len(zlib.compress(events[0]['content'].encode()))/len(events[0]['content'])
                if case == 'compressible':
                    fixture = verifier_fixture(events,descriptor)
                    PHYSICAL.verify_trace(*fixture)
                    for change in [lambda value:value[5]['state'].update(native_sha256='00'*32),
                        lambda value:value[6]['state']['originals'].pop(),
                        lambda value:mutate_control(value,'producer_requests',1,acquisition_job_id='\\x'+'11'*16),
                        lambda value:mutate_control(value,'producer_requests',1,relay_endpoint_id='\\x'+'11'*16),
                        lambda value:mutate_control(value,'producer_requests',1,relay_route_id='\\x'+'11'*16),
                        lambda value:mutate_control(value,'producer_requests',1,acquisition_scope_id='\\x'+'11'*16),
                        lambda value:mutate_control(value,'producer_outcomes',1,request_sequence=1),
                        lambda value:mutate_control(value,'producer_outcomes',1,outcome_kind=2),
                        lambda value:mutate_control(value,'producer_outcomes',1,policy_epoch=2),
                        lambda value:mutate_control(value,'producer_epochs',0,outstanding_request_limit=65),
                        lambda value:value[0][3]['value']['pending'].update(pending_bytes=0),
                        lambda value:value[7].update(open65='PASS')]:
                        changed = copy.deepcopy(fixture)
                        change(changed)
                        with self.assertRaises(ValueError): PHYSICAL.verify_trace(*changed)
            self.assertLess(ratios['compressible'],.01)
            self.assertGreater(ratios['highentropy'],.70)


def initialized_baseline():
    document = json.loads((ROOT/'tests/fixtures/configuration/policy-v3.json').read_text())
    common = dict(dataset_id='\\x'+'01'*16,revision=1,previous_revision=None,schema_version=3,
        document=document,document_digest='\\x77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b')
    return dict(datasets=[dict(dataset_id='\\x'+'01'*16,instance_id='\\x'+'02'*16,
        config_revision=1,policy_revision=1,topology_generation=None)],
        config_revisions=[dict(common,authority_mode='managed',accepted_source_digest='\\x'+'03'*32)],
        policy_revisions=[dict(common,config_revision=1)],schema_versions=[dict(version=45)],
        principals=[],management_audit=[],management_commands=[],owner_epochs=[],producer_epochs=[],jobs=[])


class PhysicalSetupControls(unittest.TestCase):
    def test_connected_initializer_captures_success_and_checks_the_fixed_baseline(self):
        with tempfile.TemporaryDirectory() as temporary:
            case = Path(temporary)
            baseline = initialized_baseline()
            lab = SimpleNamespace(docker=Mock(return_value=SimpleNamespace(returncode=0,stdout='',stderr='')),
                write_json=lambda path,value:path.write_text(json.dumps(value)))
            contents,record = Mock(return_value=baseline),Mock()
            observed = PHYSICAL.initialize_connected_fixture(lab,dict(client='owned'),case,
                'physical_compressible','fresh_database',contents,record,ROOT)
            self.assertEqual(observed,baseline)
            lab.docker.assert_called_once_with('exec','owned','/build/ingest_bootstrap_probe','initialize-fixture',
                'fresh_database','/lab/secrets/application','/lab/unused',check=False)
            self.assertEqual((case/'initialize-fixture.stderr').read_text(),'')
            proof = json.loads((case/'connected-fixture-setup.json').read_text())
            self.assertEqual(proof['accepted_source_digest'],'03'*32)
            self.assertEqual(proof['setup'],'connected-adapter-fixture')
            self.assertEqual(proof['cli_initialization_qualification'],'NOT_RUN')
            self.assertEqual(proof['exit_code'],0)

    def test_wrong_baseline_dataset_refuses_before_any_physical_process_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            baseline = initialized_baseline()
            baseline['datasets'][0]['dataset_id'] = '\\x'+'07'*16
            lab = SimpleNamespace(docker=Mock(return_value=SimpleNamespace(returncode=0,stdout='',stderr='')),
                write_json=lambda path,value:path.write_text(json.dumps(value)))
            launch = Mock(side_effect=AssertionError('physical launch before fixed baseline'))
            call = Mock(side_effect=AssertionError('physical setup must use connected initializer'))
            with patch.dict(sys.modules,dict(lab=lab,fault_cases=SimpleNamespace(Process=launch))), \
                    patch.object(PHYSICAL,'preflight',return_value={}), \
                    patch.object(PHYSICAL,'make_corpus',return_value=([],dict(sha256='00'*32,bytes=0))):
                with self.assertRaisesRegex(ValueError,'dataset01/instance02 before launch'):
                    PHYSICAL.run(dict(client='owned'),output,Mock(return_value=baseline),Mock(),
                        Mock(return_value=('fresh_database','/lab/config','unused')),call,{},ROOT)
            launch.assert_not_called()
            call.assert_not_called()
            self.assertEqual(lab.docker.call_args.args[3],'initialize-fixture')
            retained = json.loads((output/'physical_compressible/control-before.json').read_text())
            self.assertEqual(retained['datasets'][0]['dataset_id'],'\\x'+'07'*16)

    def test_initializer_nonzero_exit_or_stderr_refuses_before_baseline_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            for index,(code,error) in enumerate(((1,''),(0,'initializer failed'))):
                case = Path(temporary)/str(index)
                case.mkdir()
                lab = SimpleNamespace(docker=Mock(return_value=SimpleNamespace(returncode=code,stdout='',stderr=error)))
                contents = Mock(side_effect=AssertionError('baseline after failed initializer'))
                with self.assertRaisesRegex(ValueError,'exit0 with empty stderr'):
                    PHYSICAL.initialize_connected_fixture(lab,dict(client='owned'),case,'physical',
                        'fresh_database',contents,Mock(),ROOT)
                contents.assert_not_called()
                self.assertEqual((case/'initialize-fixture.stderr').read_text(),error)


class PhysicalTelemetryControls(unittest.TestCase):
    def test_allocated_bytes_inode_dedup_and_separate_input_domain(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            with (root/'000001.sst').open('wb') as stream:
                stream.seek(1024*1024)
                stream.write(b'x')
            os.link(root/'000001.sst',root/'000002.sst')
            (root/'producer-inputs-v1').mkdir()
            (root/'producer-inputs-v1'/'input.cbor').write_bytes(b'input')
            bound = PHYSICAL.BoundRoot(root,'native')
            try:
                sample = bound.sample(full=True)
                self.assertEqual(len(sample['files']),1)
                value, = sample['files'].values()
                actual = (root/'000001.sst').stat()
                self.assertEqual(value['device'],actual.st_dev)
                self.assertEqual(value['inode'],actual.st_ino)
                self.assertEqual(value['allocated_bytes'],actual.st_blocks*512)
                self.assertEqual(value['classification'],'SST')
                self.assertLess(value['allocated_bytes'],value['logical_bytes'])
                self.assertEqual(sample['directories']['.']['inode'],root.stat().st_ino)
                self.assertEqual(sample['directory_allocated_bytes'],root.stat().st_blocks*512)
                self.assertEqual(sample['classes']['DIRECTORY_METADATA']['allocated_bytes'],root.stat().st_blocks*512)
                frame = dict(phase='load',roots=dict(native=copy.deepcopy(sample)),open_unlinked=dict(
                    status='OBSERVED',files={'1:2':dict(role='native',allocated_bytes=4096)}))
                frame['roots']['native']['disappeared_during_sample'] = ['vanished.log']
                (root/'samples.jsonl').write_text(json.dumps(frame)+'\n')
                summary = PHYSICAL.sample_summary(root)
                self.assertEqual(summary['census_race_samples'],dict(native=1))
                self.assertEqual(summary['open_unlinked_sampled_peaks'],dict(native=4096))
            finally: bound.close()

    def test_descriptor_binding_refuses_path_replacement_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary).resolve()
            root = parent/'root'
            root.mkdir()
            bound = PHYSICAL.BoundRoot(root,'native')
            try:
                root.rename(parent/'original')
                root.mkdir()
                with self.assertRaisesRegex(ValueError,'identity changed'): bound.sample()
            finally: bound.close()
            (root/'unsafe').symlink_to(parent/'original',target_is_directory=True)
            bound = PHYSICAL.BoundRoot(root,'native')
            try:
                with self.assertRaisesRegex(ValueError,'symlink'): bound.sample()
            finally: bound.close()

    @unittest.skipUnless(Path('/proc/self/fd').exists(),'Linux procfs only')
    def test_open_unlinked_is_positively_counted_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            path = root/'000001.log'
            path.write_bytes(b'x'*4096)
            stream = path.open('rb')
            duplicate = os.dup(stream.fileno())
            path.unlink()
            bound = PHYSICAL.BoundRoot(root,'native')
            original_iterdir = Path.iterdir
            observer = os.getpid()
            def scoped(path):
                # Inspect only this fixture process, never the host process set.
                if path == Path('/proc'): return iter([Path('/proc')/str(observer)])
                if path == Path('/proc')/str(observer)/'fd':
                    return iter([path/str(stream.fileno()),path/str(duplicate)])
                return original_iterdir(path)
            try:
                with patch.object(Path,'iterdir',scoped),patch.object(PHYSICAL.os,'getpid',return_value=0):
                    observed = PHYSICAL.open_unlinked(dict(native=bound))
                self.assertEqual(observed['status'],'OBSERVED')
                self.assertEqual(len(observed['files']),1)
                self.assertEqual(observed['allocated_bytes'],4096)
            finally:
                bound.close()
                stream.close()
                os.close(duplicate)

    def test_pg_fd_denial_is_unknown_instead_of_zero(self):
        lab = SimpleNamespace(docker=Mock(return_value=SimpleNamespace(returncode=0,
            stdout='E\t/proc/4/fd\nS\t3\t9\tpid:[12]\n',stderr='')))
        observed = PHYSICAL.pg_open_unlinked(lab,dict(postgres='owned'))
        self.assertEqual(observed['status'],'UNKNOWN_PARTIAL')
        self.assertTrue(observed['errors'])
        self.assertEqual(lab.docker.call_args.args[:4],('exec','-i','owned','bash'))
        lab.docker.return_value.stdout = 'F\t1:2:8:4096:1:regular file\t/var/lib/postgresql/data/pg_wal/old (deleted)\nS\t3\t9\tpid:[12]\n'
        alias = PHYSICAL.pg_open_unlinked(lab,dict(postgres='owned'))
        self.assertEqual(alias['status'],'UNKNOWN_PARTIAL')
        self.assertEqual(alias['allocated_bytes'],0)
        self.assertTrue(alias['aliases_with_remaining_links'])


class PhysicalLogControls(unittest.TestCase):
    def witness(self,text,rotated=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root/'1-2.log').write_text(text)
            if rotated: (root/'1-3.log').write_text('rotated')
            return PHYSICAL.log_witness(root)

    def test_success_requires_file_creation_and_successful_installs(self):
        actual = self.witness(captured_log())
        self.assertEqual(actual['automatic_flush'],'WITNESSED')
        self.assertEqual(actual['nontrivial_automatic_compaction'],'WITNESSED')
        for old,new,key in [
            ('memtable #1 done','memtable #1 failed','automatic_flush'),
            ('Level-0 commit table #5','Level-0 commit table #6','automatic_flush'),
            ('[facts] [JOB 1]','[other] [JOB 1]','automatic_flush'),
            ('Write Buffer Full','Shut down','automatic_flush'),
            (' OK, records in:',' IO error, records in:','nontrivial_automatic_compaction'),
            ('output_compression: ZSTD','output_compression:','nontrivial_automatic_compaction'),
            ('LevelL0FilesNum','ManualCompaction','nontrivial_automatic_compaction'),
            ('"file_number": 7','"file_number": 5','nontrivial_automatic_compaction'),
            ('"files_L0": [5, 6]','"files_L0": [5]','nontrivial_automatic_compaction'),
            ('"cf_name": "facts", "compaction_reason"','"cf_name": "other", "compaction_reason"',
             'nontrivial_automatic_compaction'),
            ('"num_output_records": 10','"num_output_records": 0','nontrivial_automatic_compaction')]:
            with self.subTest(old=old):
                self.assertIn(old,captured_log())
                self.assertEqual(self.witness(captured_log().replace(old,new))[key],'NOT_WITNESSED')
        for text in [captured_log().replace('"status": "OK"','"status": "IO error"'),
                     captured_log().replace('"db_session_id": "session1"','"db_session_id": "other"'),
                     captured_log()+'2026/11/03 00:00:00.001 123 DB Session ID:  session2\n',
                     captured_log()+'2026/11/03 00:00:00.001 123 EVENT_LOG_v1 {"event":\n',
                     captured_log()+'2026/11/03 00:00:00.001 123 EVENT_LOG_v1 {"job":1,"job":2}\n']:
            actual = self.witness(text)
            self.assertEqual(actual['automatic_flush'],'NOT_WITNESSED')
            self.assertEqual(actual['nontrivial_automatic_compaction'],'NOT_WITNESSED')
        self.assertEqual(self.witness(captured_log(),rotated=True)['automatic_flush'],'NOT_WITNESSED')
        duplicate_output = captured_log().replace('"num_output_files": 1','"num_output_files": 2')
        creation, = [line for line in duplicate_output.splitlines() if '"job": 2' in line and 'table_file_creation' in line]
        duplicate_output = duplicate_output.replace(creation,creation+'\n'+creation)
        self.assertEqual(self.witness(duplicate_output)['nontrivial_automatic_compaction'],'NOT_WITNESSED')
        # An earlier OK with a lost finish cannot fund a later failed install.
        old = captured_log().replace('"event": "compaction_finished"','"event": "lost_finished"')
        later = captured_log().split('"event": "compaction_started"',1)[1]
        later = '2026/11/03-00:00:00.000100 123 EVENT_LOG_v1 {"event": "compaction_started"'+later
        later = later.replace('"job": 2','"job": 3').replace(' OK, records in:',' IO error, records in:')
        self.assertEqual(self.witness(old+later)['nontrivial_automatic_compaction'],'NOT_WITNESSED')

    def test_real_checkpoint_and_admission_intervals_must_overlap(self):
        logs = ('2026-11-03T00:00:01.000000Z 2026-11-03 00:00:01 UTC [9] LOG:  checkpoint starting: immediate force wait\n'
                '2026-11-03T00:00:02.000000Z 2026-11-03 00:00:02 UTC [9] LOG:  checkpoint complete: wrote 20 buffers\n')
        start = 1793664000000000
        trace = [dict(value=dict(event='physical-admitted',index=32,
            admission_started_at_unix_us=start+1500000,observed_at_unix_us=start+1600000))]
        stamps = [dict(checkpoint_command_start='2026-11-03T00:00:00+00:00'),
                  dict(checkpoint_command_end='2026-11-03T00:00:03+00:00')]
        self.assertEqual(PHYSICAL.checkpoint_overlap(logs,trace,stamps)['status'],'WITNESSED')
        trace[0]['value'].update(admission_started_at_unix_us=start+2500000,observed_at_unix_us=start+2600000)
        self.assertEqual(PHYSICAL.checkpoint_overlap(logs,trace,stamps)['status'],'NOT_WITNESSED')
        self.assertEqual(PHYSICAL.checkpoint_overlap(logs.replace('force wait','time'),trace,stamps)['status'],'NOT_WITNESSED')


class PhysicalArtifactControls(unittest.TestCase):
    def preflight(self,changes='',tampered=False,receipt_fault=None):
        with tempfile.TemporaryDirectory() as temporary:
            root,build = Path(temporary)/'fresh',Path(temporary)/'build'
            build.mkdir()
            for name in ('bigbrotr','ingest_bootstrap_probe'): (build/name).write_bytes(name.encode())
            (build/'source/tools').mkdir(parents=True)
            (build/'source/tools/build.py').write_bytes(b'maintained-source-fixture')
            helper_hash = hashlib.sha256(b'maintained-source-fixture').hexdigest()
            manifest = dict(format='bigbrotr-source/1',revision='a'*40,tree='c'*40,
                files_sha256={'tools/build.py':helper_hash})
            (build/'source-manifest.json').write_text(json.dumps(manifest))
            manifest_hash = PHYSICAL.file_sha(build/'source-manifest.json')
            receipt = dict(format='bigbrotr-build-result/1',source_manifest_sha256=manifest_hash,
                binary_sha256=hashlib.sha256(b'bigbrotr').hexdigest(),helper_sha256=helper_hash,
                native_toolchain=dict(tools={}),build_info=dict(source=dict(state='builder-supplied',
                    revision='a'*40,tree='c'*40,dirty=False,manifest_sha256=manifest_hash)))
            (build/'build-result.json').write_text(json.dumps(receipt))
            probe = dict(format='bigbrotr-ingest-bootstrap-probe-build/1',source_revision='a'*40,source_tree='c'*40,
                source_manifest_sha256=manifest_hash,parent_build_receipt_sha256=PHYSICAL.file_sha(build/'build-result.json'),
                binary_sha256=hashlib.sha256(b'ingest_bootstrap_probe').hexdigest(),native_toolchain=dict(tools={}),
                profile='release',command=['cargo','build','--locked','--release','-p','bigbrotr','--example','ingest_bootstrap_probe'])
            if receipt_fault in ('parent-hash','probe-tree','native-toolchain'):
                probe[{'parent-hash':'parent_build_receipt_sha256','probe-tree':'source_tree','native-toolchain':'native_toolchain'}[receipt_fault]] = 'wrong'
            (build/'ingest-bootstrap-probe-build.json').write_text(json.dumps(probe))
            if receipt_fault == 'archive': (build/'source/tools/build.py').write_bytes(b'changed after build')
            if receipt_fault == 'manifest': (build/'source-manifest.json').write_text('{}')
            if tampered: (build/'ingest_bootstrap_probe').write_bytes(b'changed')
            class Boundary(Exception): pass
            def command(args):
                return SimpleNamespace(stdout='c'*40+'\n' if args[-1] == 'a'*40+'^{tree}' else
                    ('b'*40 if changes else 'a'*40)+'\n' if args[-1] == 'HEAD'
                    else changes if 'diff' in args else '')
            lab = SimpleNamespace(command=Mock(side_effect=command),setup=Mock(side_effect=Boundary),
                                  docker=Mock(side_effect=AssertionError('premature engine')))
            permitted = {'experiments/policy-versions/run.py','experiments/policy-versions/README.md',
                'experiments/policy-versions/physical_envelope.py','tests/acceptance/test_physical_envelope.py'}
            reason = 'current ingest probe differs' if tampered else \
                'retained source archive changed' if receipt_fault == 'archive' else \
                'parent source manifest hash' if receipt_fault == 'manifest' else \
                'exact probe-parent build binding' if receipt_fault else \
                'built product source changed beyond policy harness' if set(changes.splitlines())-permitted else None
            with patch.object(sys,'argv',[str(RUNNER),str(root),str(build),'--physical-envelope']), \
                patch.object(sys,'path',list(sys.path)),patch.dict(sys.modules,dict(lab=lab,
                    oracle=SimpleNamespace(expected=Mock()),physical_envelope=PHYSICAL)),patch.object(PHYSICAL,'preflight',Mock()):
                with self.assertRaisesRegex((AssertionError,ValueError),reason) if reason else self.assertRaises(Boundary):
                    runpy.run_path(str(RUNNER),run_name='__main__')
            self.assertEqual(lab.setup.call_count,0 if reason else 1)
            self.assertFalse(root.exists())
            lab.docker.assert_not_called()

    def test_exact_artifacts_and_noncompiled_fence_precede_engines(self):
        self.preflight()
        self.preflight('experiments/policy-versions/physical_envelope.py\n')
        self.preflight(tampered=True)
        for fault in ('parent-hash','probe-tree','native-toolchain','archive','manifest'):
            with self.subTest(fault=fault): self.preflight(receipt_fault=fault)
        for path in ('apps/bigbrotr/examples/support/ingest_physical_growth.rs',
                     'apps/bigbrotr/src/storage/rocks/options.rs','Cargo.lock',
                     'tests/fixtures/configuration/policy-v3.json','unknown.py'):
            with self.subTest(path=path): self.preflight(path+'\n')


if __name__ == '__main__': unittest.main()
