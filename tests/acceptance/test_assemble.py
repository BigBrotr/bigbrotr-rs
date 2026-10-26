"""Offline packaging trust-boundary controls; no product or engine is executed."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('candidate_assembler', ROOT / 'packaging/release/assemble.py')
assembler = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(assembler)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + '\n')


class CandidateAssembly(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.source, self.artifacts = self.home / 'assembly', self.home / 'artifacts'
        self.source.mkdir(); self.artifacts.mkdir()
        self.manifest = self.home / 'explicit-source-manifest.json'
        for name in (*assembler.ASSEMBLY_FILES, *assembler.PUBLIC_FILES):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((ROOT / name).read_bytes())
        # Tiny synthetic assets exercise exact bytes without fetching/loading a native engine.
        def asset(name, data):
            return {'name': name, 'bytes': len(data), 'sha256': sha(data)}
        self.library = b'fake pinned library'
        self.selected = {'extension_platform':'linux_amd64',
                         'archive':{'name':'unused.zip','url':'https://example.invalid/unused',
                                    'bytes':1,'sha256':'1'*64},
                         'library':asset('libduckdb.so', self.library),
                         'header':asset('duckdb.h', b'header'),
                         'extensions':[asset(name, name.encode()) for name in ('ducklake','postgres_scanner','httpfs')]}
        write(self.source / assembler.REGISTRY, {'format':'bigbrotr-archive-engine/1',
              'platforms':{assembler.TARGET:self.selected}})
        def put(name, data, mode=0o444):
            p = self.artifacts / name; p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data); p.chmod(mode)
        # A recognizable ELF header is data only; the assembler must never run it.
        header = b'\x7fELF\x02\x01\x01' + bytes(11) + b'\x3e\x00'
        put('bigbrotr', header + b'CLI', 0o555)
        put('archive-worker/bigbrotr-archive-worker', header + b'worker', 0o555)
        put('archive-worker/lib/libduckdb.so', self.library)
        put('archive-worker/lib/duckdb.h', b'header')
        for name in ('ducklake','postgres_scanner','httpfs'):
            put('archive-worker/extensions/' + name + '.duckdb_extension', name.encode())
        files = {name:sha((self.source/name).read_bytes()) for name in (*assembler.ASSEMBLY_FILES,*assembler.PUBLIC_FILES)}
        files.update({name:sha(name.encode()) for name in assembler.INPUT_KEYS})
        self.source_value = {'format':'bigbrotr-source/1','revision':'a'*40,'tree':'b'*40,
                             'archive_sha256':'c'*64,'files_sha256':files}
        write(self.manifest, self.source_value)
        worker_data=(self.artifacts/'archive-worker/bigbrotr-archive-worker').read_bytes()
        worker={'bytes':len(worker_data),'sha256':sha(worker_data)}
        info={'format':'bigbrotr-build/1','product':'BigBrotr','version':'0.1.0-dev',
              'target':assembler.TARGET,'profile_assertion':'release','optimization':'3','debug_info':'false',
              'archive_worker':worker,'capabilities':['event-verification','config-static'],
              'capability_scope':'synthetic offline artifact only','rustc':'synthetic compiler identity',
              'protocol_registry_format':'bigbrotr-protocol-versions/1',
              'protocols':{'event_verifier':'bigbrotr-nip01-verifier/1'},
              'schemas':{'build_info':'bigbrotr-build/1'},
              'source':{'state':'builder-supplied','revision':'a'*40,'tree':'b'*40,'dirty':False,
                        'manifest_sha256':sha(self.manifest.read_bytes())},
              'inputs_sha256':{name:files[name] for name in assembler.INPUT_KEYS}}
        self.result={'format':'bigbrotr-build-result/1',
                     'binary_sha256':sha((self.artifacts/'bigbrotr').read_bytes()),
                     'source_manifest_sha256':sha(self.manifest.read_bytes()),
                     'helper_sha256':files['tools/build.py'],'build_info':info,
                     'archive_worker':dict(worker,path='/old/build/worker',registry_sha256=files[assembler.REGISTRY],
                                           assets=self.selected,build_environment={'LD_LIBRARY_PATH':'/old/build/lib'}),
                     'native_toolchain':{'os':'Linux','architecture':'x86_64',
                                        'tools':{name:{'sha256':sha(name.encode()),'path':'/host/tool'}
                                                 for name in ('CC','CXX','AR','LIBCLANG_PATH','discovery')}}}
        self.save_result()
        subprocess.run(['git','init','-q',str(self.source)], check=True, capture_output=True)
        self.commit()
        self.addCleanup(patch.stopall)
        patch.object(assembler,'ROOT',self.source).start()
        patch('tools.archive_bundle.REGISTRY',self.source / assembler.REGISTRY).start()

    def commit(self):
        subprocess.run(['git','-C',str(self.source),'add','.'],check=True,capture_output=True)
        subprocess.run(['git','-C',str(self.source),'-c','user.name=Packaging test',
                        '-c','user.email=packaging@example.invalid','-c','core.hooksPath=/dev/null',
                        '-c','commit.gpgsign=false','commit','-qm','Synthetic assembly inputs'],
                       check=True,capture_output=True)

    def save_result(self):
        write(self.artifacts/'build-result.json',self.result)

    def run_assembly(self,name='candidate',**kwargs):
        return assembler.assemble(self.artifacts,self.manifest,self.home/name,**kwargs)

    def test_deterministic_closed_bundle_and_detached_acyclic_identity(self):
        # Adjacent test executables, source trees and planted secrets are not selected.
        (self.artifacts/'ingest_bootstrap_probe').write_bytes(b'test executable')
        (self.artifacts/'secret.env').write_bytes(b'PLANTED_SECRET_CANARY')
        (self.artifacts/'source').mkdir()
        (self.artifacts/'source'/'ignored').write_bytes(b'private checkout')
        one=self.run_assembly('one'); two=self.run_assembly('two')
        self.assertEqual(one,two)
        self.assertNotEqual(one['assembly']['revision'],one['artifact']['source']['revision'])
        self.assertEqual(one['artifact']['source']['revision'],'a'*40)
        archive=self.home/'one'/one['bundle']['name']
        self.assertEqual(sha(archive.read_bytes()),one['bundle']['sha256'])
        with tarfile.open(archive) as tar:
            names=tar.getnames()
            inventory=json.load(tar.extractfile('members.json'))
            self.assertNotIn('members.json',inventory['files'])
            self.assertNotIn('candidate-manifest.json',names)
            self.assertNotIn('build-result.json',names)
            self.assertFalse(any('secret.env' in x or 'ingest_bootstrap_probe' in x or x.startswith('source/') for x in names))
            self.assertEqual({m.name for m in tar if m.isfile()},set(inventory['files'])|{'members.json'})
            for name,record in inventory['files'].items():
                member=tar.getmember(name); data=tar.extractfile(member).read()
                self.assertEqual((len(data),sha(data),member.mode),(record['bytes'],record['sha256'],record['mode']))
                self.assertEqual((member.uid,member.gid,member.mtime),(0,0,0))
            self.assertEqual(tar.getmember('bigbrotr').mode,0o555)
        self.assertEqual(sorted(p.name for p in (self.home/'one').iterdir()),
                         ['bigbrotr-'+assembler.TARGET+'.tar.gz','candidate-manifest.json'])

    def test_explicit_formatless_lab_receipt_and_optional_native_receipt(self):
        r=self.result; worker=r['build_info']['archive_worker']
        r.pop('format'); r['source']='a'*40; r['tree']='b'*40
        r['archive_registry_sha256']=r['archive_worker']['registry_sha256']
        r['archive_assets']=self.selected; r['archive_worker']=worker
        r['postcheck_status']='PASS'; r['lockfile_sha256']=r['build_info']['inputs_sha256']['Cargo.lock']
        r['archive_verification']={'format':'bigbrotr-worker-verification/1','executable':worker,
            'result':'verified','scope':'bundle bytes only; no engine, catalog or readiness check','target':assembler.TARGET}
        self.source_value.pop('format'); write(self.manifest,self.source_value)
        r['source_manifest_sha256']=r['build_info']['source']['manifest_sha256']=sha(self.manifest.read_bytes())
        self.save_result()
        self.assertEqual(self.run_assembly()['artifact']['build_receipt_kind'],'archive-installed-lab')

    def test_normal_builder_native_bundle_receipt_is_checked_not_copied(self):
        receipt={'format':'bigbrotr-native-archive-bundle/1','target':assembler.TARGET,
                 'registry_sha256':self.result['archive_worker']['registry_sha256'],
                 'assets':self.selected,'status':'Prepared and hashed; runtime qualification is separate'}
        path=self.artifacts/'archive-worker/bundle.json';write(path,receipt)
        self.run_assembly()
        receipt['registry_sha256']='0'*64;write(path,receipt)
        with self.assertRaisesRegex(ValueError,'native bundle receipt'):
            self.run_assembly('bad')

    def test_mutation_missing_and_extra_native_files_are_rejected(self):
        path=self.artifacts/'archive-worker/lib/libduckdb.so'
        path.chmod(0o644); path.write_bytes(b'X'+self.library[1:])
        with self.assertRaises(ValueError):self.run_assembly()
        path.unlink()
        with self.assertRaises(ValueError):self.run_assembly()
        path.write_bytes(self.library)
        (path.parent/'unexpected-secret').write_bytes(b'secret')
        with self.assertRaisesRegex(ValueError,'missing/extra'):self.run_assembly()
        self.assertFalse((self.home/'candidate').exists())

    def test_wrong_source_manifest_registry_and_worker_binding_are_rejected(self):
        original=self.result['build_info']['source']['revision']
        self.result['build_info']['source']['revision']='d'*40;self.save_result()
        with self.assertRaisesRegex(ValueError,'source differs'):self.run_assembly()
        self.result['build_info']['source']['revision']=original
        self.result['archive_worker']['registry_sha256']='e'*64;self.save_result()
        with self.assertRaisesRegex(ValueError,'registry/source'):self.run_assembly()
        self.result['archive_worker']['registry_sha256']=self.source_value['files_sha256'][assembler.REGISTRY]
        self.result['build_info']['archive_worker']['sha256']='f'*64;self.save_result()
        with self.assertRaisesRegex(ValueError,'worker receipt'):self.run_assembly()

    def test_symlinked_selected_files_parents_and_special_files_are_rejected(self):
        path=self.artifacts/'archive-worker/lib/libduckdb.so'
        path.unlink();path.symlink_to(self.home/'elsewhere')
        with self.assertRaises(ValueError):self.run_assembly()
        path.unlink();os.mkfifo(path)
        with self.assertRaises(ValueError):self.run_assembly()
        path.unlink();path.write_bytes(self.library)
        alias=self.home/'alias';alias.symlink_to(self.artifacts,target_is_directory=True)
        with self.assertRaises(ValueError):assembler.assemble(alias,self.manifest,self.home/'candidate')

    def test_overwrite_and_unsafe_output_do_not_remove_other_owners(self):
        existing=self.home/'candidate';existing.mkdir();(existing/'keep').write_bytes(b'owned')
        with self.assertRaises(FileExistsError):self.run_assembly()
        self.assertEqual((existing/'keep').read_bytes(),b'owned')
        for output in (self.artifacts/'nested',self.source/'nested',self.home/'missing'/'..'/'escape'):
            with self.assertRaises((ValueError,OSError)):
                assembler.assemble(self.artifacts,self.manifest,output)
        link=self.home/'linked';link.symlink_to(existing,target_is_directory=True)
        with self.assertRaises(ValueError):assembler.assemble(self.artifacts,self.manifest,link/'nested')

    def test_metadata_paths_duplicates_booleans_and_unsupported_target_fail_closed(self):
        for name in ('../escape','/absolute','a/../b','a//b','a\\b'):
            with self.subTest(name=name),self.assertRaises(ValueError):assembler.relative(name)
        with self.assertRaises(ValueError):assembler.blob({'bytes':True,'sha256':'a'*64})
        original=self.result['build_info']['schemas']
        self.result['build_info']['schemas']={'build_info':False};self.save_result()
        with self.assertRaisesRegex(ValueError,'version map'):self.run_assembly()
        self.result['build_info']['schemas']=original
        self.result['build_info']['target']='aarch64-unknown-linux-gnu';self.save_result()
        with self.assertRaisesRegex(ValueError,'unsupported build'):self.run_assembly()
        (self.artifacts/'build-result.json').write_text('{"x":1,"x":2}')
        with self.assertRaisesRegex(ValueError,'duplicate JSON'):self.run_assembly()

    def test_journey_hook_requires_exact_committed_set(self):
        with self.assertRaises(OSError):self.run_assembly(include_journey=True)
        for name in assembler.JOURNEY_FILES:
            path=self.source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'public example')
        with self.assertRaisesRegex(ValueError,'clean and committed'):self.run_assembly(include_journey=True)
        self.commit()
        manifest=self.run_assembly(include_journey=True)
        self.assertEqual(manifest['journey_files'],list(assembler.JOURNEY_FILES))
        with tarfile.open(self.home/'candidate'/manifest['bundle']['name']) as tar:
            for name in assembler.JOURNEY_FILES:self.assertEqual(tar.extractfile(name).read(),b'public example')

    def test_copy_time_mutation_fails_and_removes_only_new_output(self):
        real=assembler.copy_verified
        def change(stream,destination,expected):
            if destination.name=='bigbrotr':
                (self.artifacts/'bigbrotr').chmod(0o755)
                with (self.artifacts/'bigbrotr').open('r+b') as f:f.seek(-1,2);f.write(b'X')
            return real(stream,destination,expected)
        with patch.object(assembler,'copy_verified',side_effect=change):
            with self.assertRaisesRegex(ValueError,'hash mismatch'):self.run_assembly()
        self.assertFalse((self.home/'candidate').exists())

    def test_cli_hash_and_non_elf_replacement_do_not_execute(self):
        path=self.artifacts/'bigbrotr';path.chmod(0o755);path.write_bytes(b'#!/bin/sh\ntouch SHOULD_NOT_EXIST\n')
        with self.assertRaisesRegex(ValueError,'CLI hash'):self.run_assembly()
        self.result['binary_sha256']=sha(path.read_bytes());self.save_result()
        with self.assertRaisesRegex(ValueError,'ELF'):self.run_assembly()
        self.assertFalse((self.home/'SHOULD_NOT_EXIST').exists())


if __name__=='__main__':
    unittest.main()
