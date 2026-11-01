"""Exact corruption/refusal negatives; no SQL, engine or product execution."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from test_linear_capped_quantum import fixture
from test_first_launch import ROOT
from cases import first_launch_capped_corruption as check
from cases import first_launch_capped_oracle as cap
from cases.first_launch_capped_quantum import CappedQuantumRuntime
from cases import first_launch_quantum_oracle as quantum
from installed_sync_oracle import decode
from bootstrap_oracle import cbor


def refusal():
    return dict(format='bigbrotr-initial-sync-diagnostic/1',reason='operation-error',
        scope='caller-progress-not-commit-outcome',timing='monotonic-offset-microseconds-saturating',
        stage='quantum-plan',entered_us=10,returned_us=20,dispatch_deadline_us=30,captured_us=21,
        frame_ordinal=None,request_sequence=None,last_delivered_admission_sequence=6)


def stderr(value):
    return 'error: initial Sync unavailable: Quantum(Context); sync_dispatch='+json.dumps(value)+'\n'


def receipt(old,changed):
    updated=dict(changed,xmin=str(int(old['xmin'])+1))
    return [dict(stage='before',triggers=copy.deepcopy(check.TRIGGERS),exact_triggers=1,five_terminal=1,row=old),
        dict(stage='changed',changed=1,exact_one=1,row=updated),
        dict(stage='restored',triggers=copy.deepcopy(check.TRIGGERS),exact_triggers=1,row=updated)]


class CappedCorruptionControls(unittest.TestCase):
    def test_literal_mutations_change_only_cursor_time_or_foreign_native_id_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,_,events,_,_,_,_,_=fixture(Path(temporary))
            for profile,created,upper in (('cursor-time',0,1),('foreign-payload',1,2)):
                old,changed=check.mutation(state,events,profile)
                self.assertEqual(quantum.effects(changed['effect_records'])[0][5:],(created,1,bytes.fromhex(events[2]['id'])))
                self.assertEqual(changed['continuation_upper'],upper)
                self.assertEqual({key:value for key,value in old.items() if key not in ('effect_records','continuation_upper')},
                    {key:value for key,value in changed.items() if key not in ('effect_records','continuation_upper')})
                sql=check.mutation_sql(old,changed)
                self.assertEqual(sql.count('BEGIN;'),1);self.assertEqual(sql.count('COMMIT;'),1)
                for name in ('acquisition_quantum_guard','zzz_acquisition_quantum_owner'):
                    self.assertEqual(sql.count('DISABLE TRIGGER '+name+';'),1)
                    self.assertEqual(sql.count('ENABLE TRIGGER '+name+';'),1)
                    self.assertLess(sql.index('DISABLE TRIGGER '+name),sql.index('UPDATE control.acquisition_quanta'))
                    self.assertLess(sql.index('ENABLE TRIGGER '+name),sql.index('COMMIT;'))

    def test_exactly_one_declared_row_and_both_trigger_restorations_are_mandatory(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,_,events,_,_,_,_,_=fixture(Path(temporary));old,changed=check.mutation(state,events,'foreign-payload')
            values=receipt(old,changed);text=lambda rows:'\n'.join(json.dumps(row) for row in rows)
            check.mutation_receipt(text(values),old,changed)
            for change in ('missing-trigger','disabled-trigger','zero-row','extra-effect','unchanged-xmin','missing-restoration'):
                value=copy.deepcopy(values)
                if change=='missing-trigger':value[-1]['triggers'].pop()
                elif change=='disabled-trigger':value[-1]['triggers'][0]['enabled']='D'
                elif change=='zero-row':value[1]['changed']=0
                elif change=='extra-effect':
                    for row in value[1:]:row['row']['charged_bytes']+=1
                elif change=='unchanged-xmin':
                    for row in value[1:]:row['row']['xmin']=old['xmin']
                else:value.pop()
                with self.subTest(change=change),self.assertRaises(AssertionError):check.mutation_receipt(text(value),old,changed)

    def test_only_exact_natural_context_planning_refusal_can_pass(self):
        terminal=dict(Running=False,Pid=0,ExitCode=4,OOMKilled=False)
        self.assertEqual(check.natural_refusal(terminal,stderr(refusal()),harness_stop=False),refusal())
        substitutions=(dict(stage='quantum-terminal'),dict(reason='deadline'),dict(returned_us=None),
            dict(returned_us=30),dict(frame_ordinal=0),dict(request_sequence=6),dict(last_delivered_admission_sequence=5))
        for fields in substitutions:
            with self.subTest(fields=fields),self.assertRaises(AssertionError):
                check.natural_refusal(terminal,stderr(dict(refusal(),**fields)),harness_stop=False)
        for message in (stderr(refusal()).replace('Quantum(Context)','Quantum(Interval)'),
                stderr(refusal()).replace('Quantum(Context)','Source(Unavailable)'),
                'error: arbitrary exit4\n'):
            with self.subTest(message=message),self.assertRaises(AssertionError):check.natural_refusal(terminal,message,harness_stop=False)
        for fields in (dict(ExitCode=0),dict(Pid=1),dict(OOMKilled=True),dict(Running=True)):
            with self.subTest(fields=fields),self.assertRaises(AssertionError):
                check.natural_refusal(dict(terminal,**fields),stderr(refusal()),harness_stop=False)
        with self.assertRaises(AssertionError):check.natural_refusal(terminal,stderr(refusal()),harness_stop=True)

    def test_no_extra_quantum_or_product_effect_is_excused_by_a_corruption_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,_,events,_,_,_,_,_=fixture(Path(temporary));old,changed=check.mutation(before,events,'cursor-time')
            changed=dict(changed,xmin='12');after=copy.deepcopy(before)
            after['acquisition_quanta']=[changed if row['quantum_id']==old['quantum_id'] else row for row in after['acquisition_quanta']]
            check.preserved(before,after,old,changed,300_000_000_000)
            for change in ('extra-quantum','another-row','producer-effect','trigger-row-scope'):
                value=copy.deepcopy(after)
                if change=='extra-quantum':value['acquisition_quanta'].append(copy.deepcopy(changed))
                elif change=='another-row':value['acquisition_quanta'][0]['xmin']='other'
                elif change=='producer-effect':value['producer_requests'].append(dict(request_sequence=7))
                else:value['jobs'][0]['state']='cancelled'
                with self.subTest(change=change),self.assertRaises(AssertionError):check.preserved(before,value,old,changed,300_000_000_000)
            before['owner_epochs'][-1]['xmin']='1';after['owner_epochs'][-1]['xmin']='2'
            after['owner_epochs'][-1]['lease_expires_at_us']+=1
            check.preserved(before,after,old,changed,300_000_000_000)
            after['owner_epochs'][-1]['lease_expires_at_us']+=60_000_001
            with self.assertRaises(AssertionError):check.preserved(before,after,old,changed,300_000_000_000)

    def test_globally_present_body_one_is_verified_from_independent_native_science(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,dump,inputs,_,_=fixture(Path(temporary))
            science=cap.science(state,report,events,wires,dump,inputs,200_001_010_000)
            self.assertEqual(check.native_body_one(science,events)['created_at'],1)
            changed=copy.deepcopy(science)
            for at,(cf,key,value) in enumerate(changed['native']):
                if cf=='facts' and key[:2]==b'\x01\x20' and key.endswith((19).to_bytes(8,'big')):
                    fact=decode(value[34:].split(b'\0',1)[1]);fact[5][2]=0
                    preimage=b'bigbrotr/fact/1\0'+cbor(fact)
                    changed['native'][at]=(cf,key,b'\x01\x20'+hashlib.sha256(preimage).digest()+preimage)
            with self.assertRaises(AssertionError):check.native_body_one(changed,events)

    def test_parent_setup_failure_propagates_without_becoming_a_negative_pass(self):
        runtime=object.__new__(CappedQuantumRuntime)
        runtime.args=SimpleNamespace(linear_quantum_corruption='cursor-time');runtime.inputs={}
        runtime.config=Mock();runtime.config.read_text.return_value='[runtime]\nlease_ttl="60s"\n'
        runtime.initial_start=Mock(side_effect=ValueError('original setup failure'))
        runtime.initial_commands=Mock()
        with self.assertRaisesRegex(ValueError,'original setup failure'):runtime.initial()
        runtime.initial_commands.assert_not_called()

    def test_cli_rejects_corruption_outside_its_capped_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            variants=(('p01-t01','--binary',sys.executable),('p03-linear-quantum',),
                ('p03-linear-quantum','--linear-quantum-scenario','interruptions'),
                ('p03-linear-quantum','--linear-quantum-scenario','capped-continuation','--linear-quantum-fault','plan-before'))
            for number,args in enumerate(variants):
                evidence=root/str(number)
                result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),*args,
                    '--linear-quantum-corruption','cursor-time','--source-root',str(ROOT),'--evidence',str(evidence)],
                    capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,2,result.stderr);self.assertFalse(evidence.exists())


if __name__=='__main__':unittest.main()
