"""Portable source/fixture controls; no socket, PostgreSQL or native qualification."""
import ast
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
with patch.object(sys,'path',[str(ROOT/'experiments/policy-versions'),*sys.path]):
    spec = importlib.util.spec_from_file_location('fresh_socket_setup',ROOT/'experiments/acquisition-scope/socket_setup.py')
    SOCKET = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(SOCKET)


def write_json(path,value):
    path.write_text(json.dumps(value))


def record(name,condition,**details):
    if not condition:
        raise RuntimeError(name)


class SocketArtifactControls(unittest.TestCase):
    def check(self,changes=(),tampered=None):
        with tempfile.TemporaryDirectory() as temporary:
            build=Path(temporary)
            receipt=dict(source='a'*40,probe_name='ingest_bootstrap_probe')
            for name,key in [('bigbrotr','binary_sha256'),('ingest_bootstrap_probe','probe_sha256')]:
                data=('independent fixture '+name).encode()
                (build/name).write_bytes(data)
                receipt[key]=hashlib.sha256(data).hexdigest()
            if tampered:
                (build/tampered).write_bytes(b'changed after build receipt')
            lab=SimpleNamespace(command=Mock(return_value=SimpleNamespace(stdout='\n'.join(changes))))
            result=SOCKET.require_build(lab,ROOT,build,receipt,'b'*40)
            self.assertEqual(result,list(changes))
            self.assertEqual(lab.command.call_args.args[0][-5:],
                ['--name-only','--no-renames','a'*40,'b'*40,'--'])

    def test_exact_and_only_the_reviewed_noncompiled_paths_are_admitted(self):
        self.check()
        self.check(sorted(SOCKET.HARNESS_PATHS))

    def test_changed_product_or_probe_bytes_are_refused(self):
        for name in ('bigbrotr','ingest_bootstrap_probe'):
            with self.subTest(name=name),self.assertRaisesRegex(AssertionError,'differs from its exact-source'):
                self.check(tampered=name)

    def test_compiled_schema_fixture_unknown_and_rename_source_are_refused(self):
        for name in ('apps/bigbrotr/examples/ingest_bootstrap_probe.rs','Cargo.lock','tools/build.py',
                     'migrations/postgres/0001_control.sql','tests/fixtures/configuration/policy-v3.json',
                     'experiments/policy-versions/schema_inventory.py','unknown.py'):
            with self.subTest(name=name),self.assertRaisesRegex(AssertionError,'beyond socket harness'):
                self.check([name,'experiments/acquisition-scope/socket_setup.py'])


class SocketFixtureControls(unittest.TestCase):
    def test_alice_seed_preserves_all_original_rows_and_rejects_unexpected_changes(self):
        before=dict(datasets=[dict(dataset_id='\\x'+'22'*16,xmin='1')],principals=[],
                    local_accounts=[],local_credentials=[],principal_grants=[],other=[dict(x=1,xmin='1')])
        observed=[]
        lab=SimpleNamespace(sql=Mock())
        def capture(name,condition,**details):
            observed.append((condition,details))
        # First obtain the declared expectation by supplying the literal rows
        # that the existing fixture's INSERTs must create.
        principal='\\x'+'10'*16;dataset=before['datasets'][0]['dataset_id']
        after=copy.deepcopy(before)
        after['principals']=[dict(principal_id=principal,dataset_id=dataset,kind='local',created_at_us=0,
                                  database_role='fixture',local_os_uid=0,xmin='2')]
        after['local_accounts']=[dict(principal_id=principal,dataset_id=dataset,username='alice',
            initial_owner=True,active=True,credential_revision=1,grant_revision=1,xmin='2')]
        after['local_credentials']=[dict(principal_id=principal,dataset_id=dataset,credential_revision=1,
            format='argon2id-v1',argon_version=19,memory_kib=65536,passes=3,lanes=1,salt='\\x'+'02'*16,
            password_hash='\\xa9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff',xmin='2')]
        after['principal_grants']=[dict(dataset_id=dataset,principal_id=principal,action='policy.change',
            resource_kind='dataset',resource_id=dataset,xmin='2')]
        with patch.object(SOCKET,'rows',side_effect=[before,after]):
            SOCKET.seed_alice(lab,{},'bb_source_template',{},capture)
        self.assertTrue(observed[-1][0])
        self.assertIn('BEGIN;',lab.sql.call_args.args[-1])
        for table in ('other','datasets'):
            changed=copy.deepcopy(after);changed[table][0]['xmin']='3'
            with self.subTest(table=table),patch.object(SOCKET,'rows',side_effect=[before,changed]), \
                    self.assertRaisesRegex(RuntimeError,'alice-only-template-seed'):
                SOCKET.seed_alice(lab,{},'bb_source_template',{},record)

    def test_socket_keeps_real_accepted_policy_and_refuses_drift_without_replica_repair(self):
        tree=ast.parse((ROOT/'experiments/acquisition-scope/bindings.py').read_text())
        function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='arrival_policy')
        document=json.loads((ROOT/'tests/fixtures/configuration/policy-v3.json').read_text())
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+json.dumps(
            document,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        before=dict(owner_epochs=[],producer_requests=[],other=[dict(xmin='5')],
            config_revisions=[dict(document=document,document_digest='\\x'+digest,schema_version=3,xmin='3')],
            policy_revisions=[dict(document=document,document_digest='\\x'+digest,schema_version=3,xmin='4')])
        for drift in (False,True):
            actual=copy.deepcopy(before)
            if drift:actual['policy_revisions'][0]['document_digest']='\\x'+'00'*32
            with self.subTest(drift=drift),tempfile.TemporaryDirectory() as temporary:
                lab=SimpleNamespace(sql=Mock(),write_json=write_json)
                namespace=dict(socket=True,SOURCE=ROOT,json=json,output=Path(temporary),lab=lab,pg={},
                    rows=Mock(side_effect=[actual,copy.deepcopy(actual)]),require=lambda condition,reason:record(reason,condition),
                    sha=lambda value:hashlib.sha256(value).hexdigest())
                exec(compile(ast.Module(body=[function],type_ignores=[]),'<actual arrival_policy>','exec'),namespace)
                if drift:
                    with self.assertRaisesRegex(RuntimeError,'fresh-socket-accepted-policy-differs'):
                        namespace['arrival_policy']('fresh','socket',provenance='local_first_witness',schema={})
                else:
                    namespace['arrival_policy']('fresh','socket',provenance='local_first_witness',schema={})
                    saved=json.loads((Path(temporary)/'socket-policy-setup.json').read_text())
                    self.assertEqual(saved,dict(before=before,after=before))
                lab.sql.assert_not_called()

    def test_current_rows_refuses_same_count_table_substitution_before_capture(self):
        inventory=SOCKET.declared_inventory(ROOT)
        altered=sorted(set(inventory['tables'])-{'producer_requests'}|{'unrelated_fixture'})
        lab=SimpleNamespace(sql=Mock(return_value=json.dumps(altered)))
        with self.assertRaisesRegex(AssertionError,'producer_requests'):
            SOCKET.rows(lab,{},'bb_source_template',inventory)
        self.assertEqual(lab.sql.call_count,1)

    def test_fresh_attempt_uses_current_setup_and_real_managed_cli_without_reusing_names(self):
        names=[]
        for failed in (False,True):
            with self.subTest(failed=failed),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);pg_root=root/'pg'
                state=dict(owner='c'*32)
                def setup(*args,**kwargs):
                    args[0].mkdir();names.append(args[3])
                lab=SimpleNamespace(setup=Mock(side_effect=setup),load=Mock(return_value=state),
                    write_json=write_json,cli=Mock(return_value=SimpleNamespace(
                        returncode=3 if failed else 0,stdout='actual CLI fixture response',stderr='refusal' if failed else '')))
                document=json.loads((ROOT/'tests/fixtures/configuration/policy-v3.json').read_text())
                digest='\\x'+hashlib.sha256(b'bigbrotr/product-policy/3\0'+json.dumps(
                    document,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                accepted=dict(document=document,schema_version=3,document_digest=digest)
                current=dict(config_revisions=[accepted],policy_revisions=[accepted],owner_epochs=[],
                             producer_requests=[],producer_outcomes=[])
                with patch.object(SOCKET,'prepare_database',return_value=(
                        'bb_source_template','/lab/bb_source_template.toml',pg_root/'bb_source_template.toml')) as prepare, \
                        patch.object(SOCKET,'rows',return_value=current),patch.object(SOCKET,'seed_alice') as seed, \
                        patch.object(SOCKET,'stop_setup') as stop,patch.dict('os.environ',{'BIGBROTR_SOCKET_SUBNET':'10.253.240.64/28'}):
                    if failed:
                        with self.assertRaisesRegex(RuntimeError,'socket-template-initialize'):
                            SOCKET.prepare(lab,root,root/'build',ROOT,'a'*40)
                    else:
                        returned=SOCKET.prepare(lab,root,root/'build',ROOT,'a'*40)
                        self.assertEqual(returned[0],pg_root)
                        self.assertEqual(returned[2]['target'],43)
                        self.assertEqual(lab.cli.call_args_list[0].args[1:],
                            ('config','initialize','--file','/lab/bb_source_template.toml'))
                        self.assertEqual(lab.cli.call_args_list[1].args[1:],
                            ('config','activate-initial','--file','/lab/bb_source_template.toml',
                             '--request-id','77'*16,'--expected-revision','1'))
                        seed.assert_called_once()
                    self.assertEqual(prepare.call_args.args[5],'managed')
                    self.assertEqual(lab.setup.call_args.kwargs['subnet'],'10.253.240.64/28')
                    stop.assert_called_once()
                report=json.loads((pg_root/'socket-setup-result.json').read_text())
                self.assertEqual(report['status'],'FAIL' if failed else 'PASS_SOCKET_SETUP')
        self.assertNotEqual(*names)

    def test_shared_role_profile_rejects_privilege_or_custody_drift(self):
        verify=SOCKET.prepare_database.__globals__['verify_application_role']
        inventory=SOCKET.declared_inventory(ROOT)
        table=dict(owner='bigbrotr_migrator',select=True,insert=True,update=True,
                   delete=False,truncate=False,references=False,trigger=False)
        role=dict(role='bigbrotr_control',login=True,superuser=False,create_database=False,
            create_role=False,replication=False,bypass_rls=False,memberships=0,schema_create=False,
            database_owner='bigbrotr_migrator',schema_owner='bigbrotr_migrator',schema_usage=True,
            tables={name:dict(table) for name in inventory['tables']},
            functions={'control.fixture()':dict(owner='bigbrotr_migrator',execute=True,security_definer=False)})
        for changed in (None,'superuser','database_owner','delete','missing-table'):
            actual=copy.deepcopy(role)
            if changed=='superuser':actual['superuser']=True
            if changed=='database_owner':actual['database_owner']='bigbrotr_control'
            if changed=='delete':actual['tables']['producer_requests']['delete']=True
            if changed=='missing-table':actual['tables'].pop('producer_requests')
            lab=SimpleNamespace(sql=Mock(return_value=json.dumps(actual)))
            with self.subTest(changed=changed):
                if changed:
                    with self.assertRaises((AssertionError,RuntimeError)):
                        verify(lab,{},'template','bb_source_template',inventory,record)
                else:
                    verify(lab,{},'template','bb_source_template',inventory,record)

    def test_existing_pg_or_output_is_refused_before_setup(self):
        for entry in ('pg','bindings'):
            with self.subTest(entry=entry),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);(root/entry).mkdir()
                lab=SimpleNamespace(setup=Mock())
                with self.assertRaisesRegex(AssertionError,'already exists'):
                    SOCKET.prepare(lab,root,root/'build',ROOT,'a'*40)
                lab.setup.assert_not_called()

    def test_cleanup_never_stops_a_foreign_label_or_accepts_unjoined_state(self):
        for foreign,pid in ((True,0),(False,123)):
            with self.subTest(foreign=foreign,pid=pid),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                write_json(root/'resources.json',dict(root=str(root),format='bigbrotr-pg-lab/1',
                    owner='ours',client='client',postgres='postgres'))
                item=dict(Id='actual-owned-id',Config=dict(Labels={'lab-owner':'foreign' if foreign else 'ours'}),
                          State=dict(Running=False,Pid=pid,OOMKilled=False))
                lab=SimpleNamespace(LABEL='lab-owner',write_json=write_json,
                    docker=Mock(return_value=SimpleNamespace(returncode=0,stdout=json.dumps([item]))))
                report=dict(status='PASS_SOCKET_SETUP')
                with self.assertRaisesRegex(RuntimeError,'cleanup incomplete'):
                    SOCKET.stop_setup(lab,root,report)
                self.assertEqual(report['status'],'INCOMPLETE_LAB_AUDIT')
                self.assertFalse(any(call.args[0]=='stop' for call in lab.docker.call_args_list))
