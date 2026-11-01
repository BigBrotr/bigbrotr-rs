"""Portable verifier controls; no native/PG readiness is fabricated here."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[2]
SPEC=importlib.util.spec_from_file_location('connected_quota_controls',
    ROOT/'experiments/policy-versions/connected_quota.py')
QUOTA=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUOTA)


def baseline():
    return dict(producer_epochs=[dict(reserved_through_request=1,lab_xmin='10')],
                producer_outcomes=[dict(outcome_bytes='\\x010203',
                    outcome_sha256='\\x'+hashlib.sha256(b'\x01\x02\x03').hexdigest(),lab_xmin='11')],
                unchanged=[dict(value='retained',lab_xmin='12')])


def trace(case):
    control=baseline()
    values=copy.deepcopy(control)
    for rows in values.values():
        for row in rows: row['xmin']=row.pop('lab_xmin')
    native=[('default',b'key',b'independent-literal')]
    state=dict(sequence=13,native_rows=1,native_sha256='fixed',originals=[['original',dict(bytes=3,sha256='old')]],
        control_tables=[(name,json.dumps(rows)) for name,rows in sorted(values.items())])
    event=lambda phase:dict(event='quota-state',phase=phase,state=copy.deepcopy(state),
        rows=[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in native])
    events=[event('seed'),event('reopened'),event('before-proof'),dict(event='quota-proof',
        mode='quota-reconcile-'+case,result='Ok(Pressure)' if case=='pressure'
        else 'Err(Outcome(Conflict))' if case=='outcome-conflict' else 'Err(Control(InvalidMetadata))',
        status='Pressure' if case=='pressure' else 'RecoveryBlocked'),event('after-proof')]
    if case=='pressure': events.append(event('after-known-retry'))
    events += [dict(event='quota-next-refused',error='Pressure' if case=='pressure' else 'Control(Unavailable)'),
               event('after-next'),dict(event='quota-complete',mode='quota-reconcile-'+case,next_input=False,
                   status='Pressure' if case=='pressure' else 'RecoveryBlocked')]
    return events,control,native,dict(tables=sorted(control))


class ConnectedQuotaControls(unittest.TestCase):
    def test_injection_changes_only_the_declared_clone_rows(self):
        original=baseline()
        for case in QUOTA.CASES:
            sql,planned=QUOTA.injection(original,case)
            actual=copy.deepcopy(planned)
            if case in ('reserved-ahead','outcome-conflict'):
                table='producer_epochs' if case=='reserved-ahead' else 'producer_outcomes'
                actual[table][0]['lab_xmin']='20'
            QUOTA.verify_injection(original,planned,actual,case)
            self.assertEqual(bool(sql),case!='pressure')
            changed=copy.deepcopy(actual)
            changed['unchanged'][0]['value']='unexpected'
            with self.subTest(case=case),self.assertRaises(AssertionError):
                QUOTA.verify_injection(original,planned,changed,case)
            if case in ('reserved-ahead','outcome-conflict'):
                unchanged_xmin=copy.deepcopy(actual)
                unchanged_xmin[table][0]['lab_xmin']=original[table][0]['lab_xmin']
                with self.assertRaises(AssertionError):
                    QUOTA.verify_injection(original,planned,unchanged_xmin,case)
        self.assertEqual(original,baseline())
        _,planned=QUOTA.injection(original,'outcome-conflict')
        row=planned['producer_outcomes'][0]
        self.assertEqual(row['outcome_bytes'],'\\x000203')
        self.assertEqual(row['outcome_sha256'],'\\x'+hashlib.sha256(b'\x00\x02\x03').hexdigest())

    def test_refusal_requires_exact_error_and_no_new_effects(self):
        for case in QUOTA.CASES:
            args=trace(case)
            QUOTA.verify_events(*args[:1],case,*args[1:])
            changes=[lambda e:e[3].update(result='Ok(Available)'),
                     lambda e:e[-1].update(next_input=True),
                     lambda e:e[-1].update(status='Available'),
                     lambda e:e[-2]['state'].update(sequence=14),
                     lambda e:e[-2]['state']['originals'].append(['next',dict(bytes=1,sha256='new')]),
                     lambda e:e[-2]['rows'][0].update(value='00'),
                     lambda e:e[-2]['state']['control_tables'].pop()]
            if case=='pressure': changes.append(lambda e:e.pop(next(i for i,v in enumerate(e)
                if v.get('phase')=='after-known-retry')))
            for change in changes:
                events=copy.deepcopy(args[0]);change(events)
                with self.subTest(case=case,change=change),self.assertRaises((AssertionError,ValueError)):
                    QUOTA.verify_events(events,case,*args[1:])


if __name__=='__main__': unittest.main()
