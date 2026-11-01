"""Two literal privileged fixture mutations and exact natural refusal evidence."""
import hashlib
import json
import re

from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source

TRIGGERS=[dict(name='acquisition_quantum_guard',enabled='O'),dict(name='zzz_acquisition_quantum_owner',enabled='O')]


def mutation(state,events,profile):
    assert profile in ('cursor-time','foreign-payload')
    job=source.only(state['jobs'],request_key='\\x'+'b2'*16)
    old=source.only(state['acquisition_quanta'],job_id=job['job_id'],quantum_number=2)
    alternate,older=events[3],events[2]
    assert old['entry_kind']=='request' and old['quantum_profile']==2
    assert (old['interval_lower'],old['interval_upper'],old['window_end'],old['continuation_upper'])==(0,24,86400,2)
    assert old['disposition']=='overlap' and old['gap_reason'] is None
    predicate=json.loads(quantum.raw(old['request_predicate']))
    assert predicate[2]==dict(kinds=[1],since=0,until=23,limit=3)
    records=quantum.effects(old['effect_records'])
    assert records==[(number,number,hashlib.sha256(quantum.wire_json(event)).digest(),2,request,
        event['created_at'],1,bytes.fromhex(event['id']))
        for number,event,request in ((1,alternate,6),(2,older,7))]
    body=bytearray(quantum.raw(old['effect_records']))
    if profile=='cursor-time':
        body[52:60]=(22).to_bytes(8,'big')
    else:
        # Same authenticated signed time; the older1 minimum, boundary set and
        # complete following geometry stay unchanged by either corruption.
        body[61:93]=bytes.fromhex(events[1]['id'])
        refs=quantum.raw(old['dependencies'])
        assert all((int.from_bytes(refs[at+24:at+32],'big'),int.from_bytes(refs[at+32:at+40],'big'))!=(1,14)
            for at in range(0,len(refs),72)),'foreign original23 payload already belongs to B-page2'
    return old,dict(old,effect_records='\\x'+body.hex())


def mutation_sql(old,changed):
    identity=source.raw(old['quantum_id']);assert len(identity)==16
    assert re.fullmatch('[0-9]{1,10}',old['xmin'])
    for row in (old,changed):assert type(row['continuation_upper']) is int and 0<row['continuation_upper']<2**64
    before=quantum.raw(old['effect_records']).hex();after=quantum.raw(changed['effect_records']).hex()
    assert len(before)==len(after)==372
    trigger_query="""SELECT jsonb_agg(jsonb_build_object('name',tgname,'enabled',tgenabled) ORDER BY tgname) AS modes,
        1/((count(*)=2 AND coalesce(bool_and(tgenabled='O'),false))::integer) AS guard
        FROM pg_trigger WHERE tgrelid='control.acquisition_quanta'::regclass
        AND tgname IN ('acquisition_quantum_guard','zzz_acquisition_quantum_owner')"""
    return f"""BEGIN;
        SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='3s';
        LOCK TABLE control.acquisition_quanta IN ACCESS EXCLUSIVE MODE;
        WITH modes AS ({trigger_query}) SELECT jsonb_build_object('stage','before','triggers',modes.modes,
            'exact_triggers',modes.guard,'row',(SELECT to_jsonb(q)||jsonb_build_object('xmin',q.xmin::text)
                FROM control.acquisition_quanta q WHERE quantum_id=decode('{identity.hex()}','hex')),
            'seven_settled',1/((SELECT count(*)=7 AND bool_and(disposition IS NOT NULL)
                FROM control.acquisition_quanta)::integer)) FROM modes;
        ALTER TABLE control.acquisition_quanta DISABLE TRIGGER acquisition_quantum_guard;
        ALTER TABLE control.acquisition_quanta DISABLE TRIGGER zzz_acquisition_quantum_owner;
        WITH changed AS (UPDATE control.acquisition_quanta q SET effect_records=decode('{after}','hex'),
            continuation_upper={changed['continuation_upper']} WHERE quantum_id=decode('{identity.hex()}','hex')
            AND effect_records=decode('{before}','hex') AND continuation_upper={old['continuation_upper']}
            AND xmin::text='{old['xmin']}' RETURNING to_jsonb(q)||jsonb_build_object('xmin',q.xmin::text) AS item)
        SELECT jsonb_build_object('stage','changed','changed',count(*),'exact_one',1/((count(*)=1)::integer),
            'row',(jsonb_agg(item))->0) FROM changed;
        ALTER TABLE control.acquisition_quanta ENABLE TRIGGER acquisition_quantum_guard;
        ALTER TABLE control.acquisition_quanta ENABLE TRIGGER zzz_acquisition_quantum_owner;
        WITH modes AS ({trigger_query}) SELECT jsonb_build_object('stage','restored','triggers',modes.modes,
            'exact_triggers',modes.guard,'row',(SELECT to_jsonb(q)||jsonb_build_object('xmin',q.xmin::text)
                FROM control.acquisition_quanta q WHERE quantum_id=decode('{identity.hex()}','hex'))) FROM modes;
        COMMIT;"""


def mutation_receipt(text,old,changed):
    values=[json.loads(line) for line in text.splitlines() if line.strip()]
    assert len(values)==3 and [row['stage'] for row in values]==['before','changed','restored']
    before,updated,restored=values
    assert before['triggers']==restored['triggers']==TRIGGERS
    assert before['exact_triggers']==before['seven_settled']==restored['exact_triggers']==1
    assert before['row']==old
    assert updated['changed']==updated['exact_one']==1 and updated['row']==restored['row']
    row=updated['row']
    assert {key:value for key,value in row.items() if key!='xmin'}=={key:value for key,value in changed.items() if key!='xmin'}
    assert re.fullmatch('[0-9]{1,10}',row['xmin']) and row['xmin']!=old['xmin']
    return row


def diagnostic(text):
    # Both source-proven job orders fail at the same planning boundary. B's
    # own reader reports Quantum(Context); A's settled no-op validates the
    # producer-wide history and reports Progress(InvalidContext).
    variants=('Quantum(Context)','Progress(InvalidContext)')
    prefixes={f'error: initial Sync unavailable: {variant}; sync_dispatch=':variant for variant in variants}
    matches=[prefix for prefix in prefixes if text.startswith(prefix)]
    assert len(matches)==1,'exact source-supported native-integrity error required'
    prefix,=matches
    assert text.endswith('\n'),'complete native-integrity diagnostic required'
    value,used=json.JSONDecoder().raw_decode(text[len(prefix):])
    assert text[len(prefix)+used:].strip()==''
    assert value['format']=='bigbrotr-initial-sync-diagnostic/1'
    assert value['scope']=='caller-progress-not-commit-outcome' and value['timing']=='monotonic-offset-microseconds-saturating'
    assert value['stage']=='quantum-plan' and value['reason']=='operation-error'
    assert value['frame_ordinal'] is value['request_sequence'] is None
    for key in ('entered_us','returned_us','dispatch_deadline_us','captured_us'):
        assert type(value[key]) is int
    assert 0<=value['entered_us']<=value['returned_us']<value['dispatch_deadline_us']
    assert value['captured_us']>=value['returned_us'] and type(value['last_delivered_admission_sequence']) is int
    assert value['last_delivered_admission_sequence']==7
    return dict(value,native_refusal=prefixes[prefix])


def natural_refusal(terminal,stderr,*,harness_stop):
    assert harness_stop is False,'harness stop cannot qualify a natural refusal'
    assert type(terminal['Pid']) is type(terminal['ExitCode']) is int
    assert terminal['Running'] is False and terminal['Pid']==0 and terminal['ExitCode']==4 and terminal['OOMKilled'] is False
    return diagnostic(stderr)


def preserved(before,after,old,changed,observed_at_us):
    assert set(before)==set(after)
    canonical=lambda rows:sorted(json.dumps(row,sort_keys=True,separators=(',',':'),allow_nan=False) for row in rows)
    for table in before.keys()-{'acquisition_quanta','owner_epochs'}:
        assert canonical(before[table])==canonical(after[table]),'corruption refusal changed '+table
    expected=[changed if row['quantum_id']==old['quantum_id'] else row for row in before['acquisition_quanta']]
    assert len(expected)==len(after['acquisition_quanta'])==7
    assert all(row['disposition'] is not None for row in after['acquisition_quanta'])
    assert canonical(after['acquisition_quanta'])==canonical(expected),'product changed the injected terminal history'
    identity=lambda row:(row['dataset_id'],row['resource_id'],row['epoch'],row['role'],row['process_incarnation'])
    owners={identity(row):row for row in before['owner_epochs']}
    actual={identity(row):row for row in after['owner_epochs']}
    assert len(owners)==len(before['owner_epochs']) and len(actual)==len(after['owner_epochs']) and owners.keys()==actual.keys()
    for key,row in owners.items():
        current=actual[key]
        if current==row:continue
        assert row['state']=='active' and row['role'] in ('control','ingest')
        assert {field:value for field,value in row.items() if field not in ('lease_expires_at_us','xmin')}=={
            field:value for field,value in current.items() if field not in ('lease_expires_at_us','xmin')}
        assert row['lease_expires_at_us']<=current['lease_expires_at_us']<=observed_at_us+60_000_000
        assert re.fullmatch('[0-9]{1,10}',current['xmin'])


def native_foreign_payload(science,events):
    from installed_sync_oracle import decode
    wanted=[]
    for cf,key,value in science['native']:
        if cf!='facts' or key[:2]!=b'\x01\x20':continue
        fact=decode(value[34:].split(b'\0',1)[1])
        if fact[1]=='event-payload' and fact[5][0]==bytes.fromhex(events[1]['id']):wanted.append(fact)
    fact,=wanted
    assert fact[0][1:]==[0,1,14] and fact[5][2]==23
    return dict(event_id=events[1]['id'],created_at=23,source_epoch=1,source_sequence=14)
