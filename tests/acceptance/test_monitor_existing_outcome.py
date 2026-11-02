"""Strict seeded-outcome/C3 boundaries; no connected fixture or ELF execution."""
import copy
import ast
import hashlib
import unittest
import json
from pathlib import Path
import tempfile
import shutil
from types import SimpleNamespace
from unittest.mock import Mock,patch

from cases import first_launch_monitor_ack_recovery_oracle as oracle
from cases import first_launch_passive_oracle as passive
from cases import first_launch_monitor_outcome_seed as seed
from cases import first_launch_monitor_ack_recovery as case
from cases.first_launch import first_launch
from test_monitor_ack_recovery import (native_outcome_fixture,recovered_fixture,
    closing_native_fixture,fixture)
from test_passive_monitor import encoder,decode


def seed_fixture(before):
    after=recovered_fixture(before)
    control=after['owner_epochs'][-1]
    original=fixture()[0]['producer_outcomes'][-1]
    after['producer_outcomes'].append(dict(original,recorder_resource=control['resource_id'],
        recorder_epoch=control['epoch'],recorder_incarnation=control['process_incarnation'],
        recorded_at_us=control['acquired_at_us']+28,xmin='610'))
    return after


def repaired_fixture(before):
    after=recovered_fixture(before,retry=True);control=after['owner_epochs'][-1]
    # Frozen C4 digest already cross-checked by the independent Node fixture.
    after['producer_checkpoints'][0].update(checkpoint_revision=4,complete_through_request=4,
        dependency_count=10,checkpoint_digest='\\xac79a894d24aaccec13adf1dbc5f317557a35f6ee2b746d200ffb37fe753fbe9',
        committed_at_us=control['acquired_at_us']+28,issuer_resource=control['resource_id'],
        issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],xmin='620')
    receipt=decode(passive.raw(after['producer_outcomes'][-1]['outcome_bytes']).split(b'\0',1)[1])
    after['producer_checkpoint_dependencies'].append(dict(after['producer_checkpoint_dependencies'][-1],
        source_sequence=10,payload_digest='\\x'+receipt[4][0][1].hex(),xmin='620'))
    return after


class ExistingOutcomeControls(unittest.TestCase):
    def test_actual_seed_and_repair_keep_old_outcome_attribution_and_transaction_distinct(self):
        before,dump,initialized,receipts,originals,selection=native_outcome_fixture()
        seeded=seed_fixture(before);repaired=repaired_fixture(seeded)
        oracle.seeded_outcome(before,seeded,initialized,receipts,encoder.cbor)
        self.assertEqual(oracle.seeded_terminal(seeded,initialized,selection,originals,receipts,encoder.cbor,decode,repaired=False),receipts)
        oracle.repaired_existing(seeded,repaired,initialized,receipts)
        self.assertEqual(oracle.seeded_terminal(repaired,initialized,selection,originals,receipts,encoder.cbor,decode,repaired=True),receipts)
        retry=recovered_fixture(repaired,retry=True)
        oracle.repaired_existing(repaired,retry,initialized,receipts,retry=True)
        self.assertEqual(repaired['producer_outcomes'],seeded['producer_outcomes'])
        head=passive.only(repaired['producer_checkpoints']);outcome=passive.only(repaired['producer_outcomes'],request_sequence=4)
        self.assertEqual((outcome['recorder_epoch'],outcome['xmin'],head['issuer_epoch'],head['xmin']),(2,'610',3,'620'))
        closed=closing_native_fixture(dump);acknowledged=closing_native_fixture(closed,acknowledged=4)
        oracle.native_repair_fault(dump,closed,initialized,committed=False)
        oracle.native_repair_ack(closed,acknowledged,initialized)
        oracle.native_transition(acknowledged,copy.deepcopy(acknowledged),initialized,retry=True)

    def test_plain_missing_outcome_and_old_atomic_C4_oracles_remain_strict(self):
        before,_,initialized,receipts,originals,selection=native_outcome_fixture()
        seeded=seed_fixture(before);repaired=repaired_fixture(seeded)
        with self.assertRaises(AssertionError):oracle.repaired(seeded,repaired,initialized,receipts,encoder.cbor)
        with self.assertRaises(AssertionError):passive.checkpoint(repaired,initialized,receipts)
        with self.assertRaises(AssertionError):oracle.repaired_terminal(repaired,initialized,selection,originals,receipts,encoder.cbor,decode)

    def test_seed_cannot_advance_C3_or_rewrite_previous_history(self):
        before,_,initialized,receipts,*_=native_outcome_fixture();seeded=seed_fixture(before)
        for table in ('producer_checkpoints','producer_checkpoint_dependencies','producer_outcomes','producer_requests'):
            changed=copy.deepcopy(seeded);changed[table][0]['xmin']='999'
            with self.subTest(table=table),self.assertRaises(AssertionError):oracle.seeded_outcome(before,changed,initialized,receipts,encoder.cbor)
        for field,value in (('recorder_epoch',1),('recorded_at_us',0),('xmin','403'),('outcome_bytes','\\x00'),('request_digest','\\x'+'00'*32)):
            changed=copy.deepcopy(seeded);changed['producer_outcomes'][-1][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):oracle.seeded_outcome(before,changed,initialized,receipts,encoder.cbor)

    def test_repair_preserves_every_seeded_outcome_field_and_xmin(self):
        before,_,initialized,receipts,*_=native_outcome_fixture();seeded=seed_fixture(before);repaired=repaired_fixture(seeded)
        for field,value in (('recorder_resource','\\x'+'ab'*16),('recorder_epoch',3),('recorder_incarnation','\\x'+'ab'*16),
                ('recorded_at_us',999),('xmin','620'),('outcome_bytes','\\x00'),('outcome_sha256','\\x'+'ab'*32)):
            changed=copy.deepcopy(repaired);changed['producer_outcomes'][-1][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):oracle.repaired_existing(seeded,changed,initialized,receipts)

    def test_new_head_and_dependency_are_atomic_under_fresh_control(self):
        before,_,initialized,receipts,*_=native_outcome_fixture();seeded=seed_fixture(before);repaired=repaired_fixture(seeded)
        for change in ('head-xmin','dependency-xmin','old-control','prefix-base','prior-dependency','wrong-digest'):
            changed=copy.deepcopy(repaired)
            if change=='head-xmin':changed['producer_checkpoints'][0]['xmin']='610'
            elif change=='dependency-xmin':changed['producer_checkpoint_dependencies'][-1]['xmin']='999'
            elif change=='old-control':changed['producer_checkpoints'][0]['issuer_epoch']=2
            elif change=='prefix-base':changed['producer_checkpoints'][0]['prefix_active_generation']=1
            elif change=='prior-dependency':changed['producer_checkpoint_dependencies'][0]['xmin']='999'
            else:changed['producer_checkpoints'][0]['checkpoint_digest']='\\x'+'ab'*32
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.repaired_existing(seeded,changed,initialized,receipts)

    def test_retry_rejects_checkpoint_dependency_and_seeded_outcome_rewrites(self):
        before,_,initialized,receipts,*_=native_outcome_fixture();repaired=repaired_fixture(seed_fixture(before))
        for table in ('producer_checkpoints','producer_checkpoint_dependencies','producer_outcomes'):
            retry=recovered_fixture(repaired,retry=True);retry[table][-1]['xmin']='999'
            with self.subTest(table=table),self.assertRaises(AssertionError):oracle.repaired_existing(repaired,retry,initialized,receipts,retry=True)

    def test_closed_policy_is_complete_typed_and_independent_of_database_documents(self):
        root=Path(__file__).resolve().parents[2]
        # Independent explicit normalized acceptance profile; root identities do
        # not occur in the persisted policy document.
        literal=json.loads((root/'tests/fixtures/configuration/policy-v3.json').read_text())
        values=literal['settings'];values.update(dict([
            ('ingest.producer_outstanding_requests',32),('discoverer.events.enabled',False),('validator.enabled',False),
            ('bootstrap.seed',False),('relay_policy.auto_enroll_validated_public',False),('relay_policy.candidate_validation','disabled'),
            ('read.execution.max_memory_bytes',1073741824),('sync.enabled',False),('monitor.enabled',True),
            ('monitor.checks',['open','read']),('monitor.store_checks',['open','read']),('monitor.check_timeout',15000000),
            ('monitor.retries',1),('monitor.endpoints.mode','fixed'),('monitor.endpoints.urls',['ws://127.0.0.1:7447/installed-fixture'])]))
        body=json.dumps(literal,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+body).hexdigest()
        def shown(value):
            if type(value) is int:return str(value)
            if type(value) is list:return list(map(shown,value))
            if type(value) is dict:return {k:shown(v) for k,v in value.items()}
            return value
        report=dict(format='bigbrotr-config-static/1',status='valid-static',document_sha256='ab'*32,
            settings={name:dict(value=shown(value),deployment_locked=False) for name,value in values.items()},
            initial_policy=dict(format=literal['format'],sha256=digest,encoded_bytes=str(len(body))))
        report['settings']['runtime.lease_ttl']=dict(value='60000000',deployment_locked=True)
        frozen=seed.policy(root,report,'ab'*32)
        self.assertEqual(frozen['config'],dict(revision=1,document=literal,sha256=digest))
        self.assertEqual(frozen['config'],frozen['policy'])
        self.assertIs(type(frozen['policy']['document']['settings']['monitor.retries']),int)
        for change in ('missing','extra','wrong-boolean','wrong-integer','policy-digest','deployment-digest'):
            altered=copy.deepcopy(report)
            if change=='missing':altered['settings'].pop('monitor.enabled')
            elif change=='extra':altered['settings']['not_a_key']=dict(value=True,deployment_locked=False)
            elif change=='wrong-boolean':altered['settings']['monitor.enabled']['value']=1
            elif change=='wrong-integer':altered['settings']['monitor.retries']['value']=1
            elif change=='policy-digest':altered['initial_policy']['sha256']='00'*32
            else:altered['document_sha256']='00'*32
            with self.subTest(change=change),self.assertRaises(ValueError):seed.policy(root,altered,'ab'*32)

    def test_seed_case_is_explicit_and_seed_failure_preserves_later_NOT_RUN(self):
        root=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            import subprocess,sys
            directory=Path(directory)
            result=subprocess.run([sys.executable,'-B',str(root/'tests/acceptance/run.py'),'p04-monitor-existing-outcome-recovery',
                '--source-root',str(root),'--candidate-manifest',str(directory/'absent'),'--evidence',str(directory/'evidence')],capture_output=True,timeout=5)
            report=json.loads((directory/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([row['name'] for row in report['checks']],['first_launch_'+name for name in case.EXISTING_OUTCOME_STAGES])
            self.assertFalse(list((directory/'evidence/commands').iterdir()))
        with tempfile.TemporaryDirectory() as directory:
            args=SimpleNamespace(case='p04-monitor-existing-outcome-recovery',evidence=Path(directory),source_root=root);checks=[]
            with patch.object(case,'MonitorAckRecoveryRuntime') as runtime:
                runtime.return_value.seed_existing_outcome.side_effect=ValueError('unproven actual seed')
                first_launch(args,checks,{})
                runtime.return_value.recover_ack.assert_not_called();runtime.return_value.retry_ack.assert_not_called()
            self.assertEqual([row['result'] for row in checks],['PASS']*5+['FAIL','NOT_RUN','NOT_RUN','PASS'])

    def test_seed_mount_contract_rejects_writable_or_extra_inputs_and_wrong_target(self):
        entry=dict(service='app',monitor_outcome_seed=dict(binary='/owned/fixture',request='/owned/request'))
        info=dict(Mounts=[dict(Destination='/fixture/seed',RW=False),dict(Destination='/fixture/request.json',RW=False)],
            Config=dict(Entrypoint=['/fixture/seed'],Cmd=[seed.TEST,'--ignored','--exact','--nocapture','--test-threads=1'],
                Env=['BIGBROTR_MONITOR_OUTCOME_SEED=/fixture/request.json']))
        self.assertEqual(seed.mounts(info,entry),{'/fixture/seed':Path('/owned/fixture'),'/fixture/request.json':Path('/owned/request')})
        for change in ('writable','command','environment','service','extra-input'):
            altered=copy.deepcopy(info);target=copy.deepcopy(entry)
            if change=='writable':altered['Mounts'][0]['RW']=True
            elif change=='command':altered['Config']['Cmd'][0]='another_test'
            elif change=='environment':altered['Config']['Env']=[]
            elif change=='service':target['service']='initialize'
            else:target['monitor_outcome_seed']['extra']='untracked'
            with self.subTest(change=change),self.assertRaises(ValueError):seed.mounts(altered,target)

    def test_separate_fixture_artifact_rejects_changed_product_inputs_or_unbound_source(self):
        for change in (None,'product','cargo-config','external-example','owner-hook','fixture-source','policy-source','parent','binary','test-target','compiler','unqualified-build'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);source=root/'source';compiled=root/'compiled'
                (source/seed.OWNER).parent.mkdir(parents=True);(compiled/'build').mkdir(parents=True);(compiled/'artifacts').mkdir()
                original=b'original production owner\n';owner=original+seed.HOOK.encode();fixture_body=b'private cfg(test) fixture\n'
                (source/seed.OWNER).write_bytes(owner);(source/seed.FIXTURE).write_bytes(fixture_body)
                digest=lambda value:hashlib.sha256(value).hexdigest()
                baseline=dict(revision='11'*20,tree='22'*20,archive_sha256='aa'*32,
                    files_sha256={seed.OWNER:digest(original),'Cargo.lock':'bb'*32})
                parent=compiled/'build/parent-product-source-manifest.json';parent.write_text(json.dumps(baseline))
                files=dict(baseline['files_sha256'],**{seed.OWNER:digest(owner),seed.FIXTURE:digest(fixture_body)})
                for name in ('tests/fixtures/configuration/policy-v3.json','tests/acceptance/cases/first_launch_monitor_outcome_seed.py'):
                    (source/name).parent.mkdir(parents=True,exist_ok=True)
                    (source/name).write_bytes(b'synthetic source input')
                    files[name]=seed.digest(source/name)
                    if not name.startswith('tests/acceptance/'):baseline['files_sha256'][name]=files[name]
                parent.write_text(json.dumps(baseline))
                if change=='product':files['Cargo.lock']='cc'*32
                if change=='cargo-config':files['.cargo/config.toml']='cc'*32
                if change=='external-example':files['experiments/export-files/probe.rs']='cc'*32
                if change=='owner-hook':
                    (source/seed.OWNER).write_bytes(b'changed production owner\n'+seed.HOOK.encode())
                    files[seed.OWNER]=seed.digest(source/seed.OWNER)
                manifest=dict(revision='33'*20,tree='44'*20,archive_sha256='dd'*32,files_sha256=files)
                mp=compiled/'build/source-manifest.json';mp.write_text(json.dumps(manifest))
                binary=compiled/'artifacts/bigbrotr-library-tests';binary.write_bytes(b'synthetic artifact; never executed');binary.chmod(0o500)
                native=dict(os='Linux',architecture='x86_64',tools={'CC':dict(sha256='ee'*32)})
                candidate=dict(artifact=dict(source=dict(revision=baseline['revision'],tree=baseline['tree'],manifest_sha256=seed.digest(parent)),
                    build_info=dict(rustc='qualified compiler'),native_toolchain=dict(os='Linux',architecture='x86_64',tools_sha256={'CC':'ee'*32})))
                receipt=dict(format='bigbrotr-test-tool-artifact/1',kind='TEST_ONLY_COMPILATION_NOT_RELEASE',
                    test_source=manifest['revision'],test_tree=manifest['tree'],source_manifest_sha256=seed.digest(mp),test_source_manifest=manifest,
                    cargo_artifact=dict(target=dict(name='bigbrotr',kind=['lib'],src_path='/out/build/source/apps/bigbrotr/src/lib.rs'),profile=dict(test=True)),
                    compilation=dict(status='PASS_MONITOR_OUTCOME_SEED_BUILD_ONLY',clippy_scope='all bigbrotr targets including cfg(test)',
                        lockfile_sha256=files['Cargo.lock'],tests_executed=0,source=manifest['revision'],tree=manifest['tree'],
                        source_manifest_sha256=seed.digest(mp),rustc='qualified compiler',native_toolchain=native),
                    artifact=dict(bytes=binary.stat().st_size,sha256=seed.digest(binary)))
                if change=='fixture-source':(source/seed.FIXTURE).write_bytes(b'altered fixture')
                elif change=='policy-source':(source/'tests/fixtures/configuration/policy-v3.json').write_bytes(b'altered policy')
                elif change=='parent':candidate['artifact']['source']['manifest_sha256']='00'*32
                elif change=='binary':
                    binary.chmod(0o700);binary.write_bytes(b'altered synthetic artifact');binary.chmod(0o500)
                elif change=='test-target':receipt['cargo_artifact']['profile']['test']=False
                elif change=='compiler':receipt['compilation']['rustc']='different compiler'
                elif change=='unqualified-build':receipt['compilation']['status']='PASS_ANOTHER_PROFILE'
                (compiled/'artifacts/test-tool-artifact.json').write_text(json.dumps(receipt))
                if change:
                    with self.assertRaises(ValueError):seed.artifact(compiled,candidate,source)
                else:
                    paths,identity=seed.artifact(compiled,candidate,source)
                    self.assertEqual(paths['binary'],binary);self.assertEqual(identity['source'],manifest['revision'])
                    self.assertEqual(identity['parent_product'],candidate['artifact']['source'])

    def test_registration_freezes_full_manifest_before_measurement_using_existing_independent_codec(self):
        from bootstrap_oracle import expected
        dataset='01'*16;owner='3d'*16;slot='42'*16;incarnation='3f'*16;generation='43'*16
        domains=[(kind,bytes([64+kind])*16) for kind in range(1,10)]
        rows,_,_=expected(bytes.fromhex(dataset),False,namespaces=domains)
        manifest=next(value for cf,key,value in rows if cf=='default' and key==b'\x01\x01')
        state=dict(owner_epochs=[dict(role='ingest',dataset_id='\\x'+dataset,resource_id='\\x'+owner,epoch=1,process_incarnation='\\x'+incarnation)],
            producer_epochs=[dict(dataset_id='\\x'+dataset,producer_slot_id='\\x'+slot,producer_epoch=1,owner_incarnation='\\x'+incarnation)],
            allocation_domains=[dict(dataset_id='\\x'+dataset,dictionary_kind=kind,allocation_domain='\\x'+domain.hex(),
                owner_resource_id='\\x'+owner,owner_epoch=1,owner_incarnation='\\x'+incarnation) for kind,domain in domains],
            producer_requests=[dict(request_sequence=n,manifest_sha256='\\x'+hashlib.sha256(manifest).hexdigest()) for n in (1,2)])
        initialized=dict(dataset_id=dataset,topology_generation='0',owner_resource_id=owner,producer_slot_id=slot,recovery_generation=generation)
        ids=dict(dataset=dataset,instance='02'*16)
        self.assertEqual(seed.registration(state,initialized,ids),dict(dataset_id=dataset,instance_id=ids['instance'],producer_slot_id=slot,manifest_hex=manifest.hex()))
        for change in ('dataset','domain','incarnation','manifest','already-admitted'):
            bad=copy.deepcopy(state)
            if change=='dataset':bad['owner_epochs'][0]['dataset_id']='\\x'+'ab'*16
            elif change=='domain':bad['allocation_domains'][0]['allocation_domain']='\\x'+'ab'*16
            elif change=='incarnation':bad['producer_epochs'][0]['owner_incarnation']='\\x'+'ab'*16
            elif change=='manifest':bad['producer_requests'][0]['manifest_sha256']='\\x'+'ab'*32
            else:bad['producer_requests'].append(dict(bad['producer_requests'][0],request_sequence=3))
            with self.subTest(change=change),self.assertRaises(ValueError):seed.registration(bad,initialized,ids)

    def test_maintained_seed_builder_is_compile_only_and_retains_failure(self):
        owner=Path(__file__).resolve().parents[2]/'experiments/ingest-succession/native.py'
        tree=ast.parse(owner.read_text())
        branch=next(node for node in tree.body if isinstance(node,ast.If)
            and ast.unparse(node.test)=='producer_ack_fault or monitor_outcome_seed')
        for status in ('PASS','FAIL'):
            with self.subTest(status=status),tempfile.TemporaryDirectory() as directory:
                out=Path(directory);source=out/'build/source';source.mkdir(parents=True)
                build=source.parent;(build/'target').mkdir()
                executable=build/'target/synthetic-test';executable.write_bytes(b'\x7fELFsynthetic; never executed')
                manifest=dict(revision='ab'*20,tree='cd'*20,files_sha256={})
                parent_bytes=b'{"fixture":"explicit parent input"}\n'
                (build/'parent-product-source-manifest.json').write_bytes(parent_bytes)
                def path(value):
                    selected=Path(value)
                    return out/selected.relative_to('/out') if selected.is_relative_to('/out') else selected
                def execute(steps,checks,*args):
                    self.assertEqual([name for name,_ in steps],['compile','clippy'])
                    self.assertEqual(steps[0][1],['cargo','test','--locked','--offline','--release','-p',
                        'bigbrotr','--lib','--no-run','--message-format=json'])
                    self.assertEqual(steps[1][1],['cargo','clippy','--locked','--offline','--release','-p',
                        'bigbrotr','--all-targets','--','-D','warnings'])
                    checks.mkdir()
                    artifact=dict(reason='compiler-artifact',executable=str(executable),
                        target=dict(name='bigbrotr',kind=['lib'],src_path=str(source/'apps/bigbrotr/src/lib.rs')),
                        profile=dict(test=True))
                    (checks/'compile.log').write_text(json.dumps(artifact)+'\n')
                    return dict(status=status)
                commands=Mock(side_effect=execute);parents=Mock()
                namespace=dict(producer_ack_fault=False,monitor_outcome_seed=True,identity=dict(source='ab'*20,source_manifest_sha256='ef'*32),
                    artifacts=out/'artifacts',Path=path,execute=commands,SOURCE=source,build=build,
                    env={},native=dict(tools={}),manifest=manifest,sources=lambda:{},file_hash=seed.digest,
                    parent_bytes=parent_bytes,parent={},parent_sources=parents,json=json,shutil=shutil,
                    sys=SimpleNamespace(exit=lambda code:(_ for _ in ()).throw(SystemExit(code))))
                with self.assertRaises(SystemExit if status=='PASS' else AssertionError) as ended,patch('builtins.print'):
                    exec(compile(ast.Module(body=[branch],type_ignores=[]),str(owner),'exec'),namespace)
                commands.assert_called_once();parents.assert_called_once()
                qualification=json.loads((out/'qualification.json').read_text())
                self.assertEqual(qualification['runtime_status'],'NOT_RUN')
                self.assertEqual(qualification['tests_executed'],0)
                self.assertEqual(qualification['status'],'PASS_MONITOR_OUTCOME_SEED_BUILD_ONLY' if status=='PASS' else 'FAIL')
                if status=='PASS':
                    self.assertEqual(ended.exception.code,0)
                    receipt=json.loads((out/'artifacts/test-tool-artifact.json').read_text())
                    self.assertEqual(receipt['kind'],'TEST_ONLY_COMPILATION_NOT_RELEASE')
                    self.assertEqual(receipt['compilation']['clippy_scope'],'all bigbrotr targets including cfg(test)')
                else:self.assertFalse((out/'artifacts/test-tool-artifact.json').exists())

    def test_seed_phase_uses_the_independent_receipt_and_charges_only_the_closing_write(self):
        before,dump,initialized,receipts,originals,selection=native_outcome_fixture();seeded=seed_fixture(before)
        closed=closing_native_fixture(dump)
        manifest=passive.native_rows(dump)[('default',b'\x01\x01')]
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.existing_outcome=runtime.native_outcome=True
            runtime.root=Path(directory);runtime.home=runtime.root/'peer';runtime.home.mkdir();runtime.state=runtime.root/'instance';(runtime.state/'data').mkdir(parents=True)
            runtime.seed_binding=dict(dataset_id=initialized['dataset_id'],instance_id='02'*16,producer_slot_id=initialized['producer_slot_id'],manifest_hex=manifest.hex())
            runtime.seed_policies={};runtime.before_recovery=before;runtime.before_dump=dump;runtime.initialized=initialized
            runtime.receipts=receipts;runtime.original_inputs=originals;runtime.selection=selection;runtime.cbor=encoder.cbor;runtime.decode=decode
            runtime.original_files={};runtime.originals=Mock(return_value=originals);runtime.files=Mock(return_value={})
            runtime.catalog=[];runtime.capture=Mock(return_value=(seeded,[]));runtime.source_unchanged=Mock();runtime.backend_absence=Mock()
            runtime.client=Mock();runtime.client.poll.return_value=None
            runtime.inspect=Mock(return_value=dict(Id='fresh',State=dict(Pid=0,ExitCode=0,Running=False,OOMKilled=False)))
            runtime.docker=Mock(side_effect=[SimpleNamespace(stdout='0'),SimpleNamespace(stdout='test '+seed.TEST+' ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 700 filtered out;\n',stderr='')])
            def launched(selected,path):
                self.assertIs(selected,runtime)
                self.assertEqual(json.loads(path.read_text())['expected_receipt_hex'],(b'bigbrotr/receipt/1\0'+encoder.cbor(receipts[3])).hex())
                (runtime.state/'data/monitor-outcome-seed-result.json').write_text(json.dumps(dict(format='bigbrotr-monitor-outcome-seed-result/1',status='PASS',checkpoint=3,outcome=4,acknowledged=3)))
                return dict(id='fresh')
            with patch.object(seed,'launch',side_effect=launched) as launch,patch.object(case.native,'census',return_value=closed) as census:
                runtime.seed_existing_outcome()
                launch.assert_called_once();census.assert_called_once_with(runtime,runtime.root/'monitor-existing-outcome-seed',writer_terminal=runtime.root/'monitor-existing-outcome-seed/terminal-container.json')
            self.assertEqual(runtime.before_recovery,seeded);self.assertEqual(runtime.before_dump,closed)
            verdict=json.loads((runtime.root/'monitor-existing-outcome-seed/independent-verdict.json').read_text())
            self.assertEqual((verdict['scope'],verdict['checkpoint'],verdict['native_writes']),('explicit-test-only-seed',3,1))

    def test_existing_outcome_driver_uses_default_recovery_and_preserves_seed_on_retry(self):
        before,dump,initialized,receipts,originals,selection=native_outcome_fixture();seeded=seed_fixture(before)
        closed=closing_native_fixture(dump);acknowledged=closing_native_fixture(closed,acknowledged=4)
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.existing_outcome=runtime.native_outcome=True
            runtime.root=Path(directory);runtime.home=runtime.root/'peer';runtime.home.mkdir();runtime.state=runtime.root/'instance'
            runtime.before_recovery=seeded;runtime.before_dump=closed;runtime.initialized=initialized;runtime.receipts=receipts
            runtime.original_inputs=originals;runtime.selection=selection;runtime.original_files={};runtime.catalog=[]
            runtime.originals=Mock(return_value=originals);runtime.files=Mock(return_value={});runtime.cbor=encoder.cbor;runtime.decode=decode
            runtime.source_unchanged=Mock();runtime.backend_absence=Mock();runtime.client=Mock();runtime.client.poll.return_value=None
            runtime.resources=dict(containers=[dict(id='fresh')]);runtime.inspect=Mock(return_value=dict(Id='fresh',State=dict(Pid=0)))
            runtime.finite=Mock(return_value=dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',dataset_id=initialized['dataset_id'],
                owner_resource_id=initialized['owner_resource_id'],producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',issued_through='4',
                reserved_through='4',resolved_requests='4',native='closed',owner_release='committed',ready=False))
            for retry in (False,True):
                after=recovered_fixture(runtime.after_recovery,retry=True) if retry else repaired_fixture(seeded)
                runtime.capture=Mock(return_value=(after,[]))
                with patch.object(case.native,'census',return_value=copy.deepcopy(acknowledged)):runtime.recovery(retry=retry)
                label='monitor-existing-outcome-'+('retry' if retry else 'recovery')
                runtime.finite.assert_called_with(label,'ingest','recover','--file','/etc/bigbrotr/instance.toml',timeout=30)
                self.assertEqual(after['producer_outcomes'],seeded['producer_outcomes'])


if __name__=='__main__':unittest.main()
