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
        frame_ordinal=None,request_sequence=None,last_delivered_admission_sequence=7)


def stderr(value,variant='Quantum(Context)'):
    return 'error: initial Sync unavailable: '+variant+'; sync_dispatch='+json.dumps(value)+'\n'


def receipt(old,changed):
    updated=dict(changed,xmin=str(int(old['xmin'])+1))
    return [dict(stage='before',triggers=copy.deepcopy(check.TRIGGERS),exact_triggers=1,seven_settled=1,row=old),
        dict(stage='changed',changed=1,exact_one=1,row=updated),
        dict(stage='restored',triggers=copy.deepcopy(check.TRIGGERS),exact_triggers=1,row=updated)]


class CappedCorruptionControls(unittest.TestCase):
    def test_both_native_corruptions_preserve_minimum_order_boundary_and_full_geometry(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,_,events,wires,_,_,receipts,commands=fixture(Path(temporary))
            cap.quanta(state,wires,events,receipts,commands)
            def geometry(value):
                quantum.shape(value);result=[]
                for scope,key in enumerate(('b1','b2')):
                    job=next(row for row in value['jobs'] if row['request_key']=='\\x'+key*16)
                    rows=sorted((row for row in value['acquisition_quanta'] if row['job_id']==job['job_id']),key=lambda row:row['quantum_number'])
                    boundary=None;known=set();upper=86400
                    for row,entry in zip(rows,cap.ENTRIES[scope]):
                        self.assertEqual((row['entry_kind'],row['interval_lower'],row['interval_upper'],row['window_end']),entry[:4])
                        if row['entry_kind']=='selector_exclusion':
                            result.append(('local',scope,row['interval_upper'],row['window_end']));continue
                        self.assertEqual(row['interval_upper'],upper)
                        records=[record for record in quantum.effects(row['effect_records']) if record[3] in (2,3,5)]
                        ordered=all(later[5]<=earlier[5] for earlier,later in zip(records,records[1:]));self.assertTrue(ordered)
                        if not records:
                            oldest=None;ids=set();disposition='window_closed';upper=row['window_end']
                        else:
                            oldest=min(record[5] for record in records)
                            ids={record[7] for record in records if record[5]==oldest}
                            if oldest!=boundary:boundary=oldest;known.clear()
                            if ids<=known:
                                disposition='saturated_second';upper=oldest;boundary=None;known.clear()
                            else:
                                disposition='overlap';upper=oldest+1;known|=ids
                        self.assertEqual((row['disposition'],row['continuation_upper']),(disposition,upper))
                        result.append((scope,row['query_number'],oldest,tuple(sorted(ids)),ordered,disposition,upper))
                return result
            before=geometry(state)
            for profile in ('cursor-time','foreign-payload'):
                old,changed=check.mutation(state,events,profile);value=copy.deepcopy(state)
                value['acquisition_quanta']=[changed if row['quantum_id']==old['quantum_id'] else row for row in value['acquisition_quanta']]
                with self.subTest(profile=profile):
                    self.assertEqual(geometry(value),before)
                    self.assertEqual(quantum.effects(changed['effect_records'])[1],quantum.effects(old['effect_records'])[1])
                    self.assertEqual({k:v for k,v in changed.items() if k!='effect_records'},
                        {k:v for k,v in old.items() if k!='effect_records'})
                    self.assertEqual(changed['continuation_upper'],2)

    def test_literal_mutations_change_only_the_nonminimum_alt23_effect(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,_,events,_,_,_,_,_=fixture(Path(temporary))
            for profile,created,event_index in (('cursor-time',22,3),('foreign-payload',23,1)):
                old,changed=check.mutation(state,events,profile)
                self.assertEqual(quantum.effects(changed['effect_records'])[0][5:],(created,1,bytes.fromhex(events[event_index]['id'])))
                self.assertEqual(changed['continuation_upper'],2)
                self.assertEqual({key:value for key,value in old.items() if key!='effect_records'},
                    {key:value for key,value in changed.items() if key!='effect_records'})
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
        for variant in ('Quantum(Context)','Progress(InvalidContext)'):
            self.assertEqual(check.natural_refusal(terminal,stderr(refusal(),variant),harness_stop=False),dict(refusal(),native_refusal=variant))
        substitutions=(dict(stage='quantum-terminal'),dict(reason='deadline'),dict(returned_us=None),
            dict(returned_us=30),dict(frame_ordinal=0),dict(request_sequence=7),dict(last_delivered_admission_sequence=6),
            dict(last_delivered_admission_sequence=8),dict(stage='admission'),dict(reason='cancelled'))
        for fields in substitutions:
            with self.subTest(fields=fields),self.assertRaises(AssertionError):
                check.natural_refusal(terminal,stderr(dict(refusal(),**fields)),harness_stop=False)
        for message in (stderr(refusal()).replace('Quantum(Context)','Quantum(Interval)'),
                stderr(refusal()).replace('Quantum(Context)','Source(Unavailable)'),
                stderr(refusal(),'Quantum(Backend)'),stderr(refusal(),'Progress(Backend)'),
                stderr(refusal(),'Native(Context)'),
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
                elif change=='producer-effect':value['producer_requests'].append(dict(request_sequence=8))
                else:value['jobs'][0]['state']='cancelled'
                with self.subTest(change=change),self.assertRaises(AssertionError):check.preserved(before,value,old,changed,300_000_000_000)
            before['owner_epochs'][-1]['xmin']='1';after['owner_epochs'][-1]['xmin']='2'
            after['owner_epochs'][-1]['lease_expires_at_us']+=1
            check.preserved(before,after,old,changed,300_000_000_000)
            after['owner_epochs'][-1]['lease_expires_at_us']+=60_000_001
            with self.assertRaises(AssertionError):check.preserved(before,after,old,changed,300_000_000_000)

    def test_globally_present_original23_is_verified_and_absent_from_target_union(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,dump,inputs,_,_=fixture(Path(temporary))
            science=cap.science(state,report,events,wires,dump,inputs,200_001_010_000)
            self.assertEqual(check.native_foreign_payload(science,events),dict(event_id=events[1]['id'],created_at=23,source_epoch=1,source_sequence=14))
            old,_=check.mutation(state,events,'foreign-payload')
            refs=quantum.raw(old['dependencies'])
            self.assertNotIn((1,14),[(int.from_bytes(refs[at+24:at+32],'big'),int.from_bytes(refs[at+32:at+40],'big'))
                for at in range(0,len(refs),72)])
            changed=copy.deepcopy(science)
            for at,(cf,key,value) in enumerate(changed['native']):
                if cf=='facts' and key[:2]==b'\x01\x20' and key.endswith((14).to_bytes(8,'big')):
                    fact=decode(value[34:].split(b'\0',1)[1]);fact[5][2]=22
                    preimage=b'bigbrotr/fact/1\0'+cbor(fact)
                    changed['native'][at]=(cf,key,b'\x01\x20'+hashlib.sha256(preimage).digest()+preimage)
            with self.assertRaises(AssertionError):check.native_foreign_payload(changed,events)

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
