"""Bounded historical lookup against installed native stores and isolated PG clones."""
import hashlib
import json
from pathlib import Path
from fractions import Fraction
import copy
import time

import lab

CURRENT_PUBLIC_FAULTS = (
    'config-body-stale-digest', 'policy-body-stale-digest',
    'missing-intake-stale-digest', 'missing-intake-recomputed-digest',
    'missing-unrelated-stale-digest', 'missing-unrelated-recomputed-digest',
    'pair-mismatch-recomputed-digest', 'pair-rounded-fraction-recomputed-digest',
    'policy-schema', 'policy-format-recomputed-digest',
    'policy-null-settings-recomputed-digest', 'retention-range-recomputed-digest',
)
CURRENT_PRIVATE_FAULTS = (None, 'config-body-stale-digest', 'policy-body-stale-digest',
    'missing-unrelated-recomputed-digest', 'pair-mismatch-recomputed-digest')


def current_database_name(prefix, name):
    """Normalize declared fixture labels before interpolation as SQL names."""
    database=prefix+name.replace('-','_')
    assert 1<=len(database)<=63 and database[0] in 'abcdefghijklmnopqrstuvwxyz_'
    assert all(c in 'abcdefghijklmnopqrstuvwxyz_0123456789' for c in database)
    return database


def current_case_names():
    names = ['history-original', 'current-history-original']
    names += ['current-history-cap-'+name for name in ('zero','too-large','one-byte-short','exact')]
    names += ['current-history-later-heads']
    names += ['current-history-'+name for name in CURRENT_PUBLIC_FAULTS]
    names += ['current-history-duplicate-history', 'history-edited', 'current-history-edited-pair',
              'current-history-edited-later-heads', 'current-history-swapped-original-digest', 'history-mismatch']
    names += ['private-'+phase+'-'+('positive' if fault is None else fault)
              for phase in ('activation','ack') for fault in CURRENT_PRIVATE_FAULTS]
    names += ['private-'+phase+'-positive-distinct-original-pair' for phase in ('activation','ack')]
    return names + ['current-history-original-source-preserved']


def verify_policy_trace(events, before, phase, fault, *, original_policy=1):
    """Pin current pair -> original pair -> fresh current delivery fence.

    The mutation recipe, required complete key set and independent canonical
    digest determine the expected validation result, never the probe's result.
    """
    begin, = [i for i,e in enumerate(events) if e['event']=='history-integrity-state' and e['stage']=='before-action']
    end, = [i for i,e in enumerate(events) if e['event']=='history-integrity-state' and e['stage']=='after-action']
    actual = [e for e in events[begin+1:end] if e['event']=='history-integrity-policy']
    rows = {name:json.loads(value) for name,value in before['state']['control_tables']}
    head, = rows['datasets']
    config, = [r for r in rows['config_revisions'] if r['revision']==head['config_revision']]
    policy, = [r for r in rows['policy_revisions'] if r['revision']==head['policy_revision']]
    original, = [r for r in rows['policy_revisions'] if r['revision']==original_policy]
    old_config, = [r for r in rows['config_revisions'] if r['revision']==original['config_revision']]
    def entry(row,valid=True):
        return dict(event='history-integrity-policy',digest=row['document_digest'][2:],valid=valid)
    current=[entry(config),entry(policy)]
    assert current[0]['digest']!=current[1]['digest'], 'distinct current digests are a discriminator'
    wanted=current+[entry(old_config,fault!='config-body-stale-digest')]
    if fault!='config-body-stale-digest':
        wanted.append(entry(original,fault not in ('policy-body-stale-digest','missing-unrelated-recomputed-digest')))
    if phase=='activation' and fault is None: wanted+=current
    assert actual==wanted, (actual,wanted)


def verify_activation(before, after):
    """The sole activation effect must copy the independently retained intent."""
    old={k:json.loads(v) for k,v in before['state']['control_tables']}
    new={k:json.loads(v) for k,v in after['state']['control_tables']}
    assert old.keys()==new.keys()
    assert all(old[k]==new[k] for k in old.keys()-{'initial_ingest_activations'})
    assert not old['initial_ingest_activations']
    active,=new['initial_ingest_activations']
    intent,=new['initial_ingest_successions']
    copied=('dataset_id','owner_resource_id','target_epoch','target_incarnation',
        'target_role_instance_id','producer_slot_id','producer_epoch',
        'previous_manifest_sha256','target_manifest_sha256','handoff_sha256',
        'prepared_by_resource','prepared_by_epoch','prepared_by_incarnation',
        'config_revision','policy_revision','config_digest','policy_digest')
    wanted={key:intent[key] for key in copied}
    wanted.update({f'activated_by_{key}':intent[f'prepared_by_{key}']
                   for key in ('resource','epoch','incarnation')})
    assert {k:v for k,v in active.items() if k not in ('xmin','activated_at_us','activation_transaction')}==wanted
    control,=[r for r in new['owner_epochs'] if r['resource_id']==active['activated_by_resource']
              and r['epoch']==active['activated_by_epoch']]
    assert intent['created_at_us']<=active['activated_at_us']<min(intent['lease_expires_at_us'],control['lease_expires_at_us'])
    assert int(active['activation_transaction'])>0 and int(active['xmin'])>0
    for key in ('native_rows','native_sha256','sequence','originals'):
        assert before['state'][key]==after['state'][key]


def verify_control_rotation(events, stage, next_role, next_incarnation):
    """Real release/join/acquire changes only the declared Control lifecycle."""
    def frame(suffix):
        value,=[e for e in events if e['event']=='history-integrity-state' and e['stage']==stage+'-'+suffix]
        return value,{k:json.loads(v) for k,v in value['state']['control_tables']}
    before,old=frame('before-control-release')
    ended,released=frame('after-control-release')
    after,current=frame('after-control-acquire')
    assert old.keys()==released.keys()==current.keys()
    for name in old.keys()-{'owner_epochs','role_instances'}:
        assert old[name]==released[name]==current[name],name
    for snapshot in (ended,after):
        assert snapshot['rows']==before['rows']
        for key in ('native_rows','native_sha256','sequence','originals'):
            assert snapshot['state'][key]==before['state'][key]
    original,=[r for r in old['owner_epochs'] if r['role']=='control' and r['state']=='active']
    if stage=='seed':
        registrar,=old['producer_slots']
        expected=(registrar['registered_by_resource_id'],registrar['registered_by_epoch'],registrar['registered_by_incarnation'])
    else:
        activation,=old['initial_ingest_activations']
        expected=(activation['activated_by_resource'],activation['activated_by_epoch'],activation['activated_by_incarnation'])
    identity=(original['resource_id'],original['epoch'],original['process_incarnation'])
    assert identity==expected
    owners=lambda rows:{(r['resource_id'],r['epoch']):r for r in rows['owner_epochs']}
    roles=lambda rows:{r['role_instance_id']:r for r in rows['role_instances']}
    prior,end,now=map(owners,(old,released,current))
    key=identity[:2]
    assert prior.keys()==end.keys() and all(prior[k]==end[k] for k in prior.keys()-{key})
    digest=hashlib.sha256(b'bigbrotr/control-exclusion/1\0'+bytes.fromhex(original['dataset_id'][2:])
        +bytes.fromhex(identity[0][2:])+identity[1].to_bytes(8,'big')+bytes.fromhex(identity[2][2:])*2).hexdigest()
    assert isinstance(end[key]['released_at_us'],int) and int(end[key]['xmin'])>0
    assert end[key]==dict(original,state='released',exclusion_method='storage_fenced',
        exclusion_digest='\\x'+digest,released_at_us=end[key]['released_at_us'],xmin=end[key]['xmin'])
    old_roles,end_roles,new_roles=map(roles,(old,released,current))
    old_role=original['role_instance_id']
    assert old_roles.keys()==end_roles.keys()
    assert all(old_roles[k]==end_roles[k] for k in old_roles.keys()-{old_role})
    assert old_roles[old_role]['retired_at_us'] is None and isinstance(end_roles[old_role]['retired_at_us'],int)
    assert end_roles[old_role]==dict(old_roles[old_role],retired_at_us=end_roles[old_role]['retired_at_us'],xmin=end_roles[old_role]['xmin'])
    assert all(now[k]==v for k,v in end.items()) and len(now)==len(end)+1
    new_key,=now.keys()-end.keys()
    fresh=now[new_key]
    assert new_key==(identity[0],identity[1]+1)
    assert fresh['control_fence_version']==1 and fresh['control_backend_pid']>0 and fresh['control_backend_start'] is not None
    assert (fresh['control_backend_pid'],fresh['control_backend_start'])!=(original['control_backend_pid'],original['control_backend_start'])
    assert fresh['lease_expires_at_us']-fresh['acquired_at_us']==60_000_000
    assert fresh==dict(original,epoch=identity[1]+1,role_instance_id='\\x'+bytes([next_role]*16).hex(),
        process_incarnation='\\x'+bytes([next_incarnation]*16).hex(),acquired_at_us=fresh['acquired_at_us'],
        lease_expires_at_us=fresh['lease_expires_at_us'],control_backend_pid=fresh['control_backend_pid'],
        control_backend_start=fresh['control_backend_start'],xmin=fresh['xmin'])
    assert all(new_roles[k]==v for k,v in end_roles.items()) and len(new_roles)==len(end_roles)+1
    role,=new_roles.keys()-end_roles.keys()
    assert role==fresh['role_instance_id']
    assert new_roles[role]==dict(old_roles[old_role],role_instance_id=role,
        created_at_us=new_roles[role]['created_at_us'],xmin=new_roles[role]['xmin'])
    event,=[e for e in events if e['event']=='history-integrity-control' and e['stage']==stage]
    assert event==dict(event='history-integrity-control',stage=stage,resource=identity[0][2:],epoch=identity[1],
        role=old_role[2:],incarnation=identity[2][2:],released=True,driver_joined=True,backend_absent=True)


def verify_current_domains(native, tables):
    """Bind all nine actual namespaces without adding scientific records."""
    manifest,=[bytes.fromhex(r['value']) for r in native['rows'] if r['cf']=='default' and r['key']=='0101']
    assert len(manifest)==266 and manifest[102:104]==b'\x01\x09'
    domains={int.from_bytes(manifest[i:i+2],'big'):'\\x'+manifest[i+2:i+18].hex() for i in range(104,266,18)}
    assert sorted(domains)==list(range(1,10)) and len(set(domains.values()))==9
    epoch=int.from_bytes(manifest[54:62],'big')
    rows=tables['allocation_domains']
    assert len(rows)==epoch*9
    assert all(r['dataset_id']=='\\x'+manifest[10:26].hex()
               and r['owner_resource_id']=='\\x'+manifest[34:50].hex() for r in rows)
    assert {r['dictionary_kind']:r['allocation_domain'] for r in rows if r['owner_epoch']==1}=={
        n:'\\x'+bytes([60+n]*16).hex() for n in range(1,10)}
    assert {r['dictionary_kind']:r['allocation_domain'] for r in rows if r['owner_epoch']==epoch}==domains
    if epoch==2:
        assert not set(domains.values())&{r['allocation_domain'] for r in rows if r['owner_epoch']==1}


def expected(state, native):
    """Independent full registration projection from captured original rows/bytes."""
    manifest, = [bytes.fromhex(r['value']) for r in native['rows']
        if r['cf'] == 'default' and r['key'] == '0101']
    integer = lambda start, size: int.from_bytes(manifest[start:start+size], 'big')
    ident = lambda start: '\\x' + manifest[start:start+16].hex()
    owner, = [r for r in state['owner_epochs'] if r['resource_id'] == ident(34)]
    producer, = state['producer_epochs']
    slot, = state['producer_slots']
    control, = [r for r in state['owner_epochs'] if r['resource_id'] == slot['registered_by_resource_id']
        and r['epoch'] == slot['registered_by_epoch']]
    policy, = [r for r in state['policy_revisions'] if r['revision'] == integer(94, 8)]
    config, = [r for r in state['config_revisions'] if r['revision'] == policy['config_revision']]
    topology, = [r for r in state['topology_generations'] if r['generation'] == integer(26, 8)]
    settings = policy['document']['settings']
    limit = settings['ingest.pending_max_bytes']
    intake = dict(pending_limit=limit,
        pending_high=int(limit*Fraction(str(settings['ingest.pending_high_fraction']))),
        pending_low=int(limit*Fraction(str(settings['ingest.pending_low_fraction']))),
        max_pending_unit=settings['ingest.group_commit_max_bytes'],
        disk_resume_margin=settings['runtime.disk_resume_margin_bytes'],
        stable_duration_us=settings['runtime.pressure_stable_duration'],
        request_receipt_retention=settings['ingest.request_receipt_retention'])
    return dict(event='history-loaded', dataset_id=manifest[10:26].hex(),
        instance_id=state['datasets'][0]['instance_id'][2:], control_resource=control['resource_id'][2:],
        control_epoch=control['epoch'], control_role_instance=control['role_instance_id'][2:],
        control_incarnation=control['process_incarnation'][2:], ingest_resource=owner['resource_id'][2:],
        ingest_role_instance=owner['role_instance_id'][2:], ingest_incarnation=owner['process_incarnation'][2:],
        shard_id=integer(50, 4), config_revision=config['revision'], policy_revision=policy['revision'],
        config_digest=config['document_digest'][2:], policy_digest=policy['document_digest'][2:],
        previous_topology=topology['previous_generation'],
        allocation_domains=sorted([[r['dictionary_kind'],r['allocation_domain'][2:]] for r in state['allocation_domains']]),
        producer_slot=slot['producer_slot_id'][2:], window=producer['outstanding_request_limit'], intake_policy=intake,
        created_at_us=owner['acquired_at_us'], lease_expires_at_us=owner['lease_expires_at_us'], manifest=manifest.hex())


def run(pg, client, output, database, name, rows, clone):
    # Historical runner dependencies stay with its matching source/profile.
    from installed import run as initialize
    from registrations import upgrade as upgrade_registry
    from scopes import upgrade as upgrade_scopes
    assert name == 'history'
    initialize(pg, client, output, database, 'window-local', rows)
    base = rows(database)
    assert len(base) == 69
    cases = []

    def dump(seed, label):
        result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/'+seed+'/store')
        assert result.returncode == 0 and not result.stderr
        (output/(label+'-native.json')).write_text(result.stdout)
        return json.loads(result.stdout)

    def inspect(db, label, seed='window-local', error=None, mode='history-read'):
        before = rows(db)
        native = dump(seed, label+'-before')
        lab.write_json(output/(label+'-before.json'), before)
        result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, db,
            '/run/secrets/application', '/lab/'+seed+'/store',
            before['datasets'][0]['dataset_id'][2:], before['datasets'][0]['instance_id'][2:],
            check=False, timeout=20)
        (output/(label+'.stdout')).write_text(result.stdout)
        (output/(label+'.stderr')).write_text(result.stderr)
        after, final_native = rows(db), dump(seed, label+'-after')
        lab.write_json(output/(label+'-after.json'), after)
        assert before == after and native == final_native, label+' changed retained state'
        assert result.returncode == 0 and not result.stderr, label+' probe failed'
        actual, = [json.loads(line) for line in result.stdout.splitlines()]
        wanted = dict(event='history-refused', error=error) if error else expected(before, native)
        assert actual == wanted, (label, actual, wanted)
        assert lab.sql(pg,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        cases.append(dict(name=label,status='PASS',expected=error or 'complete-original-registration',control_tables=len(before),native_rows=len(native['rows'])))
        return actual

    original = inspect(database, 'history-original')
    assert original['previous_topology'] is None and original['window'] == 32
    assert original['control_epoch'] == 1
    assert next(r for r in base['owner_epochs'] if r['role']=='control')['state']=='released'
    inspect(database, 'history-identical-retry')
    inspect(database, 'history-instance', error='WrongDeployment', mode='history-wrong-instance')
    inspect(database, 'history-cap-zero', error='InvalidInput', mode='history-cap-zero')
    inspect(database, 'history-cap-too-large', error='InvalidInput', mode='history-cap-too-large')
    inspect(database, 'history-cap-one-byte-short', error='InvalidMetadata', mode='history-cap-one-byte-short')
    assert inspect(database, 'history-cap-exact', mode='history-cap-exact') == original

    changed = clone('history_heads', database)
    statements = []
    for table in ['config_revisions','policy_revisions']:
        document = json.loads(json.dumps(base[table][0]['document']))
        document['settings']['ingest.producer_outstanding_requests'] = 16
        document['settings']['ingest.request_receipt_retention'] = 62
        document['settings']['ingest.pending_max_bytes'] = 1024**3
        encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        digest = hashlib.sha256(b'bigbrotr/product-policy/3\0'+encoded).hexdigest()
        patch = dict(revision=2, previous_revision=1, document=document, document_digest='\\x'+digest)
        if table == 'policy_revisions': patch['config_revision'] = 2
        patch_bytes = json.dumps(patch,separators=(',',':')).encode().hex()
        statements.append(f"INSERT INTO control.{table} SELECT (jsonb_populate_record(NULL::control.{table},to_jsonb(t)||convert_from(decode('{patch_bytes}','hex'),'UTF8')::jsonb)).* FROM control.{table} t WHERE revision=1")
    statements.append('UPDATE control.datasets SET config_revision=2,policy_revision=2')
    lab.sql(pg,changed,'BEGIN; SET LOCAL session_replication_role=replica; '+';'.join(statements)+'; COMMIT;')
    assert inspect(changed, 'history-new-heads') == original

    # Privileged corruption is confined to newly named clones. Capture it before
    # the application read; the tested operation must preserve even invalid rows.
    faults = {
        'missing-slot': ("DELETE FROM control.producer_slots", 'InvalidMetadata'),
        'missing-domain': ("DELETE FROM control.allocation_domains WHERE dictionary_kind=4", 'InvalidMetadata'),
        'source-incarnation': ("UPDATE control.source_streams SET owner_incarnation=decode(repeat('ab',16),'hex')", 'InvalidMetadata'),
        'policy-schema': ("UPDATE control.policy_revisions SET schema_version=4 WHERE revision=1", 'InvalidMetadata'),
        # These clones retain healthy current revision2, but their original
        # revision1 body changes without its digest. Metadata-only identity or
        # projecting the intake fields would incorrectly accept that history.
        'config-body-stale-digest': ("UPDATE control.config_revisions SET document=jsonb_set(document,'{settings,ingest.producer_outstanding_requests}','31') WHERE revision=1", 'InvalidMetadata'),
        'policy-body-stale-digest': ("UPDATE control.policy_revisions SET document=jsonb_set(document,'{settings,ingest.producer_outstanding_requests}','31') WHERE revision=1", 'InvalidMetadata'),
        'intake-body-stale-digest': ("UPDATE control.config_revisions SET document=jsonb_set(document,'{settings,ingest.request_receipt_retention}','62') WHERE revision=1; UPDATE control.policy_revisions SET document=jsonb_set(document,'{settings,ingest.request_receipt_retention}','62') WHERE revision=1", 'InvalidMetadata'),
        'ambiguous-slot': ("""
            INSERT INTO control.producer_slots
                SELECT (jsonb_populate_record(NULL::control.producer_slots,
                    to_jsonb(s)||jsonb_build_object('producer_slot_id',decode(repeat('ac',16),'hex')))).*
                FROM control.producer_slots s;
            INSERT INTO control.producer_epochs
                SELECT (jsonb_populate_record(NULL::control.producer_epochs,
                    to_jsonb(e)||jsonb_build_object('producer_slot_id',decode(repeat('ac',16),'hex')))).*
                FROM control.producer_epochs e
            """, 'InvalidMetadata'),
    }
    for fault, (sql, error) in faults.items():
        db = clone('history_'+fault.replace('-','_'), changed if 'body-stale-digest' in fault else database)
        lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; '+sql+'; COMMIT;')
        inspect(db, 'history-'+fault, error=error)

    for field, label in [('ingest.pending_low_fraction', 'missing-intake'),
                         ('relay_policy.banned', 'missing-unrelated')]:
        for recompute in [False, True]:
            fault = label + ('-recomputed-digest' if recompute else '-stale-digest')
            document = json.loads(json.dumps(base['policy_revisions'][0]['document']))
            del document['settings'][field]
            encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
            assignment = f"document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb"
            if recompute:
                digest = hashlib.sha256(b'bigbrotr/product-policy/3\0'+encoded).hexdigest()
                assignment += f",document_digest=decode('{digest}','hex')"
            db = clone('history_'+fault.replace('-','_'), changed)
            lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; '
                f'UPDATE control.policy_revisions SET {assignment} WHERE revision=1; COMMIT;')
            inspect(db, 'history-'+fault, error='InvalidMetadata')

    zero = clone('history_zero', 'bb_source_template')
    upgrade_registry(pg,client,output,zero,'history-zero',rows)
    upgrade_scopes(pg,client,output,zero,'history-zero',rows)
    initialize(pg,client,output,zero,'zero-topology',rows)
    result = inspect(zero, 'history-zero-predecessor', seed='zero-topology')
    assert result['previous_topology'] == 0 and int.from_bytes(bytes.fromhex(result['manifest'])[26:34],'big') == 1
    assert rows(database) == base
    return dict(name=name,status='PASS',cases=cases,scope='bounded historical lookup only; no native closing, request resolution or successor')


def current_science(dataset, *, target=None):
    """Independent exact scientific frames/markers from the declared inputs."""
    from oracle import cbor
    rows = []
    public = [dataset, 4, bytes([64])*16, 1]
    batches = [(1, bytes([64])*16, 1, [(1, 'public'), (2, 'private')], 123),
               (1, bytes([64])*16, 3, [(3, 'history-third')], 456)]
    if target is not None:
        domain = bytes.fromhex(target['reference']['allocation_domain'])
        assert target['reference']['dataset_id'] == dataset.hex()
        assert target['reference']['dictionary_kind'] == 4 and target['reference']['ordinal'] == '1'
        assert target['class'] == 'history-successor' and target['recorded_at'] == 789
        batches.append((2, domain, 1, [(1, 'history-successor')], 789))
    be = lambda value, width=8: value.to_bytes(width, 'big')
    for epoch, domain, first, entries, recorded_at in batches:
        refs = []
        for offset, (ordinal, label) in enumerate(entries):
            sequence = first + offset
            source = [dataset, 0, epoch, sequence]
            reference = [dataset, 4, domain, ordinal]
            mapping = [reference, 1, 'visibility', 1, {'class': label}]
            fact = b'bigbrotr/fact/1\0' + cbor([source, 'dictionary', 1, public, recorded_at, mapping])
            digest = hashlib.sha256(fact).digest()
            refs.append([source, digest])
            key = b'\x01\x20' + dataset + be(0, 4) + be(epoch) + be(sequence)
            rows.append(dict(cf='facts', key=key.hex(), value=(b'\x01\x20'+digest+fact).hex()))
        last = first + len(entries) - 1
        marker = b'\x01\x21' + be(last) + hashlib.sha256(
            b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, epoch, first, last, refs])).digest()
        key = b'\x01\x21' + dataset + be(0, 4) + be(epoch) + be(first)
        rows.append(dict(cf='facts', key=key.hex(), value=marker.hex()))
    return sorted(rows, key=lambda row: row['key'])


def current_originals(dataset):
    """Exact two dictionary input preimages, independent of native/PG output."""
    from oracle import cbor
    domain, slot = bytes([64])*16, bytes([66])*16
    scope = [dataset, 4, domain, 1]
    result = {}
    for sequence, entries in [(1, [(1, 'public'), (2, 'private')]), (2, [(3, 'history-third')])]:
        mappings = [['dictionary', 1, [[dataset, 4, domain, ordinal], 1, 'visibility', 1, {'class': label}]]
                    for ordinal, label in entries]
        value = b'bigbrotr/producer-request/1\0' + cbor([[slot, 1, sequence], 'admit-evidence', 1,
            [1, scope, mappings]])
        name = f'{slot.hex()}-0000000000000001-{sequence:016x}.cbor'
        result[name] = dict(bytes=len(value), sha256=hashlib.sha256(value).hexdigest())
    return result


def current_pair_rows(before, config_revision=2, policy_revision=5, *, mismatched=False,
                      retention=62, pending_limit=1024**3):
    """Declared accepted-head precondition with complete immutable chains.

    Intermediate policies retain the previous healthy body's own configuration;
    only the final policy adopts the changed intake. Privileged insertion still
    obeys the adjacent-revision CHECK and preserves every original row.
    """
    config_head=before['datasets'][0]['config_revision']
    policy_head=before['datasets'][0]['policy_revision']
    assert policy_head<policy_revision
    if not mismatched: assert config_revision==config_head+1
    rows=[]
    for table in ('config_revisions','policy_revisions'):
        head=config_head if table=='config_revisions' else policy_head
        original,=[r for r in before[table] if r['revision']==head]
        if mismatched and table=='config_revisions': continue
        document=copy.deepcopy(original['document'])
        document['settings']['ingest.request_receipt_retention']=retention
        document['settings']['ingest.pending_max_bytes']=pending_limit
        if table=='policy_revisions': document['settings']['sync.linear_limit']=400
        target=config_revision if table=='config_revisions' else policy_revision
        for revision in range(head+1,target+1):
            body=document if revision==target else copy.deepcopy(original['document'])
            encoded=json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
            digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+encoded).hexdigest()
            patch=dict(revision=revision,previous_revision=revision-1,document=body,document_digest='\\x'+digest)
            if table=='policy_revisions':
                patch['config_revision']=(config_head if mismatched else config_revision) if revision==target else original['config_revision']
            rows.append((table,head,patch))
    return rows


def run_current(state, output, contents, record, prepare, call, inventory):
    """One PG-only current-format batch in the existing policy runner.

    Later Config2/Policy5 rows are explicit privileged application preconditions;
    this does not implement or qualify the future managed-apply command. Every
    scientific input, progress commit/ACK, closed reclamation, successor witness
    and private capability is produced by the actual connected Rust operations.
    """
    from fault_cases import Process
    import shutil
    dataset, instance = '01'*16, '02'*16
    cases = []

    def compact(value):
        return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()

    def sql(database, statement):
        return lab.sql(state, database, statement)

    def checkpoint(name, passed, **details):
        record(name, passed, **details)
        cases.append(name)

    def initialize(name):
        database, path, local = prepare('hi_'+name, 'managed')
        text = local.read_text()
        assert '[ingest]' not in text
        text = lab.change(text, 'topology', 'dataset_id', dataset)
        text = lab.change(text, 'topology', 'instance_id', instance)
        local.write_text(text+'\n[ingest]\nproducer_outstanding_requests=2\n')
        call(name+'-initialize', database, 'initialize', path)
        call(name+'-activate', database, 'activate-initial', path,
             extra=('--request-id','04'*16,'--expected-revision','1'))
        return database

    def clone(name, template):
        database = current_database_name('bb_hi_',name)
        sql('postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {template}')
        assert contents(database) == contents(template)
        return database

    def append_pair(database, config_revision=2, policy_revision=5, *, mismatched=False,
                    retention=62,pending_limit=1024**3):
        before = contents(database)
        statements = []
        planned=current_pair_rows(before,config_revision,policy_revision,mismatched=mismatched,
                                  retention=retention,pending_limit=pending_limit)
        for table,head,patch in planned:
            patch_bytes = compact(patch).hex()
            statements.append(f"INSERT INTO control.{table} SELECT (jsonb_populate_record(NULL::control.{table},to_jsonb(t)||convert_from(decode('{patch_bytes}','hex'),'UTF8')::jsonb)).* FROM control.{table} t WHERE revision={head}")
        target_config=before['datasets'][0]['config_revision'] if mismatched else config_revision
        statements.append(f'UPDATE control.datasets SET config_revision={target_config},policy_revision={policy_revision}')
        sql(database, 'BEGIN; SET LOCAL session_replication_role=replica; '+';'.join(statements)+'; COMMIT;')
        after = contents(database)
        for table in before.keys()-{'config_revisions','policy_revisions','datasets'}:
            assert before[table] == after[table]
        for table in ('config_revisions','policy_revisions'):
            assert all(row in after[table] for row in before[table])
            assert len(after[table])==len(before[table])+sum(name==table for name,_,_ in planned)
        for table,_,patch in planned:
            row,=[r for r in after[table] if r['revision']==patch['revision']]
            assert {key:row[key] for key in patch}==patch
        return after

    def capture(name, mode, database, directory):
        if not mode.startswith('history-integrity-'):
            assert mode.startswith('history-')
            mode = 'history-current-' + mode.removeprefix('history-')
        result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', mode,
            database, '/lab/secrets/application', '/lab/'+directory.name+'/store', dataset, instance,
            check=False, timeout=120)
        (output/(name+'.stdout')).write_text(result.stdout)
        (output/(name+'.stderr')).write_text(result.stderr)
        assert result.returncode == 0 and not result.stderr, (name, result.returncode, result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines()]

    def dump(name, directory):
        result = lab.docker('exec',state['client'],'/build/ingest_bootstrap_probe','dump',
            '/lab/'+directory.name+'/store',check=False)
        (output/(name+'-native.json')).write_text(result.stdout)
        assert result.returncode == 0 and not result.stderr
        return json.loads(result.stdout)

    def only(events, event, **fields):
        found, = [value for value in events if value['event'] == event and all(value.get(k)==v for k,v in fields.items())]
        return found

    def original_inputs(directory):
        return {p.name:dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
                for p in (directory/'store/producer-inputs-v1').iterdir()}

    def verify_native(native, *, target=None):
        science = [row for row in native['rows'] if row['cf']=='facts']
        assert science == current_science(bytes.fromhex(dataset),target=target)
        pending = sum(len(bytes.fromhex(row['key']))+len(bytes.fromhex(row['value'])) for row in science)
        counter, = [bytes.fromhex(r['value']) for r in native['rows'] if r['cf']=='default' and r['key']=='010b']
        manifest, = [bytes.fromhex(r['value']) for r in native['rows'] if r['cf']=='default' and r['key']=='0101']
        assert manifest[4:10] == bytes.fromhex('000200010001')
        assert len(counter)==50 and counter[:34] == b'\x01\x0b'+hashlib.sha256(manifest).digest()
        assert int.from_bytes(counter[34:42],'big') == pending
        epoch = int.from_bytes(manifest[54:62],'big')
        through = 3 if epoch==1 else 1 if target is not None else 0
        assert int.from_bytes(counter[42:50],'big') == through
        source_key = (b'\x01\x02'+manifest[10:26]+manifest[50:62]).hex()
        source, = [bytes.fromhex(r['value']) for r in native['rows'] if r['cf']=='default' and r['key']==source_key]
        assert source == b'\x01\x02'+through.to_bytes(8,'big')
        return pending

    def seed(name, database, *, refusal=False):
        directory = output/name
        directory.mkdir(mode=0o700)
        before = contents(database)
        events = capture(name, 'history-integrity-seed-refuse' if refusal else 'history-integrity-seed', database, directory)
        after = contents(database)
        lab.write_json(output/(name+'-before.json'), before)
        lab.write_json(output/(name+'-after.json'), after)
        if refusal:
            assert before == after and not (directory/'store').exists()
            assert only(events,'history-integrity-result') == dict(event='history-integrity-result',phase='startup',success=False,error='InvalidPolicy')
            checkpoint(name,True,all_control_rows_preserved=True,native_created=False)
            return directory
        originals = only(events,'history-integrity-state',stage='seed-originals')
        compacted = only(events,'history-integrity-state',stage='seed-compacted')
        verify_control_rotation(events,'seed',33,43)
        verify_current_domains(dict(rows=compacted['rows']),after)
        assert dict(originals['state']['originals']) == current_originals(bytes.fromhex(dataset))
        assert dict(compacted['state']['originals']) == {} and original_inputs(directory) == {}
        assert verify_native(dict(rows=originals['rows'])) == verify_native(dict(rows=compacted['rows']))
        assert not [r for r in compacted['rows'] if r['cf']=='receipts' and r['key'].startswith('0130')]
        marker, = [bytes.fromhex(r['value']) for r in compacted['rows'] if r['cf']=='receipts' and r['key'].startswith('0131')]
        assert len(marker)==139 and marker[:2]==b'\x03\x31' and marker[2:26]==(2).to_bytes(8,'big')*3 and marker[26]==1
        manifest, = [bytes.fromhex(r['value']) for r in compacted['rows'] if r['cf']=='default' and r['key']=='0101']
        assert marker[27:59]==hashlib.sha256(manifest).digest() and int.from_bytes(marker[59:67],'big')==3
        assert after['producer_epochs'][0]['state']=='retired' and after['source_streams'][0]['state']=='closed'
        assert after['source_streams'][0]['last_complete_sequence']==3
        assert len(after['producer_requests'])==len(after['producer_outcomes'])==2
        assert len(after['producer_checkpoints'])==1 and after['producer_checkpoints'][0]['complete_through_request']==2
        head=after['producer_checkpoints'][0]
        assert int.from_bytes(marker[99:107],'big')==head['checkpoint_revision']
        assert marker[107:139].hex()==head['checkpoint_digest'][2:]
        assert marker[67:99].hex()==after['source_streams'][0]['closure_digest'][2:]
        assert all(row['state']=='released' for row in after['owner_epochs'])
        assert sql(database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
        result = only(events,'history-integrity-result')
        checkpoint(name,True,result=result,control_tables=len(after),native_rows=len(compacted['rows']),
            genuine_original_requests=2,source_facts=3,real_progress_ack_and_closed_reclamation=True)
        return directory

    database = initialize('history-original')
    directory = seed('history-original',database)
    baseline = contents(database)
    native = dump('history-original',directory)

    def lookup(name, database, *, error=None, mode='history-read', original_dir=directory):
        before = contents(database)
        old_native = dump(name+'-before',original_dir)
        inputs = original_inputs(original_dir)
        events = capture(name,mode,database,original_dir)
        after = contents(database)
        final_native = dump(name+'-after',original_dir)
        assert before==after and old_native==final_native and inputs==original_inputs(original_dir)
        actual, = events
        wanted = dict(event='history-refused',error=error) if error else expected(before,old_native)
        assert actual==wanted,(name,actual,wanted)
        lab.write_json(output/(name+'-before.json'),before)
        lab.write_json(output/(name+'-after.json'),after)
        checkpoint(name,True,expected_error=error,all_control_rows_preserved=True,native_and_inputs_preserved=True)
        return actual

    original = lookup('current-history-original',database)
    for mode,error in [('zero','InvalidInput'),('too-large','InvalidInput'),('one-byte-short','InvalidMetadata'),('exact',None)]:
        lookup('current-history-cap-'+mode,database,error=error,mode='history-cap-'+mode)
    later = clone('later',database)
    append_pair(later)
    assert lookup('current-history-later-heads',later)==original

    def corrupt(database, fault, original_policy=1):
        before = contents(database)
        table = 'config_revisions' if fault.startswith('config-') else 'policy_revisions'
        policy_row, = [r for r in before['policy_revisions'] if r['revision']==original_policy]
        revision = policy_row['config_revision'] if table=='config_revisions' else original_policy
        row, = [r for r in before[table] if r['revision']==revision]
        document = copy.deepcopy(row['document'])
        if fault.endswith('body-stale-digest'):
            document['settings']['sync.linear_limit'] = 399
        elif fault.startswith('missing-intake'):
            del document['settings']['ingest.pending_low_fraction']
        elif fault.startswith('missing-unrelated'):
            del document['settings']['relay_policy.banned']
        elif fault=='pair-mismatch-recomputed-digest':
            document['settings']['ingest.request_receipt_retention'] = 62
        elif fault=='pair-rounded-fraction-recomputed-digest':
            document['settings']['ingest.pending_high_fraction'] = 0.9000000000001
        elif fault=='policy-schema':
            pass
        elif fault=='policy-format-recomputed-digest':
            document['format']='bigbrotr-product-policy/2'
        elif fault=='policy-null-settings-recomputed-digest':
            document['settings']=None
        elif fault=='retention-range-recomputed-digest':
            document['settings']['ingest.request_receipt_retention']=65
        else:
            raise AssertionError(fault)
        encoded = compact(document)
        assignment = f"document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb"
        if fault.endswith('recomputed-digest'):
            domain=b'bigbrotr/product-policy/2\0' if fault=='policy-format-recomputed-digest' else b'bigbrotr/product-policy/3\0'
            assignment += ",document_digest=decode('"+hashlib.sha256(domain+encoded).hexdigest()+"','hex')"
        if fault in ('policy-schema','policy-format-recomputed-digest'):
            assignment += ',schema_version='+('4' if fault=='policy-schema' else '2')
        sql(database,'BEGIN; SET LOCAL session_replication_role=replica; '
            f'UPDATE control.{table} SET {assignment} WHERE revision={revision}; COMMIT;')
        after = contents(database)
        for name in before.keys()-{table}: assert before[name]==after[name]
        lab.write_json(output/(database+'-corruption-before.json'),before)
        lab.write_json(output/(database+'-corruption-after.json'),after)
        return after

    for fault in CURRENT_PUBLIC_FAULTS:
        bad = clone('public_'+fault,later)
        corrupt(bad,fault)
        lookup('current-history-'+fault,bad,error='InvalidMetadata')
    duplicate=clone('duplicate_history',later)
    sql(duplicate,"""BEGIN; SET LOCAL session_replication_role=replica;
        INSERT INTO control.producer_slots SELECT (jsonb_populate_record(NULL::control.producer_slots,
            to_jsonb(s)||jsonb_build_object('producer_slot_id',decode(repeat('ac',16),'hex')))).*
            FROM control.producer_slots s;
        INSERT INTO control.producer_epochs SELECT (jsonb_populate_record(NULL::control.producer_epochs,
            to_jsonb(e)||jsonb_build_object('producer_slot_id',decode(repeat('ac',16),'hex')))).*
            FROM control.producer_epochs e;
        COMMIT;""")
    lookup('current-history-duplicate-history',duplicate,error='InvalidMetadata')

    # This validates different revision NUMBERS with a matching changed pair in
    # the actual fresh capture/registration and subsequent original lookup.
    edited = initialize('history-edited')
    append_pair(edited)
    edited_dir = seed('history-edited',edited)
    captured = lookup('current-history-edited-pair',edited,original_dir=edited_dir)
    assert (captured['config_revision'],captured['policy_revision'])==(2,5)
    assert captured['intake_policy']['request_receipt_retention']==62
    assert captured['intake_policy']['pending_limit']==1024**3
    edited_later=clone('edited_later',edited)
    append_pair(edited_later,3,8,retention=60,pending_limit=2*1024**3)
    edited_native=dump('history-edited',edited_dir)
    edited_baseline=contents(edited)
    assert lookup('current-history-edited-later-heads',edited_later,original_dir=edited_dir)==captured
    swapped=clone('swapped_original_digest',edited_later)
    sql(swapped,"""BEGIN; SET LOCAL session_replication_role=replica;
        UPDATE control.policy_revisions SET document_digest=(SELECT document_digest
            FROM control.config_revisions WHERE revision=2) WHERE revision=5;
        COMMIT;""")
    lookup('current-history-swapped-original-digest',swapped,error='InvalidMetadata',original_dir=edited_dir)
    mismatched = initialize('history-mismatch')
    append_pair(mismatched,policy_revision=2,mismatched=True)
    seed('history-mismatch',mismatched,refusal=True)

    plans=[(phase,fault,later,directory,native,'') for phase in ['activation','ack'] for fault in CURRENT_PRIVATE_FAULTS]
    plans += [(phase,None,edited_later,edited_dir,edited_native,'-distinct-original-pair') for phase in ['activation','ack']]
    for phase,fault,template,source_dir,source_native,suffix in plans:
        name = 'private-'+phase+'-'+('positive' if fault is None else fault)+suffix
        db = clone(name,template)
        current = output/name
        current.mkdir(mode=0o700)
        shutil.copytree(source_dir/'store',current/'store')
        assert dump(name+'-copied',current)==source_native and original_inputs(current)=={}
        mode='history-integrity-'+phase+('-ok' if fault is None else '-refuse')
        args=['docker','exec',state['client'],'/build/ingest_bootstrap_probe',mode,db,
            '/lab/secrets/application','/lab/'+current.name+'/store',dataset,instance]
        process=Process(args,limit=16*1024*1024)
        try:
            deadline=time.monotonic()+30
            ready=current/'store.integrity-ready.json'
            while not ready.exists():
                assert process.process.poll() is None,'private probe ended before target boundary'
                assert time.monotonic()<deadline,'private readiness timeout'
                time.sleep(.02)
            ready_value=json.loads(ready.read_text())
            assert ready_value['phase']==('activation' if phase=='activation' else 'compacted-ack')
            if fault is not None: corrupt(db,fault)
            go=current/'store.integrity-go'
            temporary=go.with_suffix('.tmp')
            temporary.write_bytes(b'go\n')
            temporary.rename(go)
            code=process.wait(90)
            stdout,stderr=bytes(process.stdout).decode(),bytes(process.stderr).decode()
            (output/(name+'.stdout')).write_text(stdout)
            (output/(name+'.stderr')).write_text(stderr)
            assert code==0 and not stderr,(name,code,stderr)
            events=[json.loads(line) for line in stdout.splitlines()]
            if phase=='ack': verify_control_rotation(events,'ack',212,222)
            before=only(events,'history-integrity-state',stage='before-action')
            after=only(events,'history-integrity-state',stage='after-action')
            assert sorted(k for k,_ in before['state']['control_tables'])==inventory['tables']
            assert sorted(k for k,_ in after['state']['control_tables'])==inventory['tables']
            verify_policy_trace(events,before,phase,fault,original_policy=5 if suffix else 1)
            assert before['rows']==after['rows']
            if fault is not None or phase=='ack': assert before['state']==after['state']
            else:
                verify_activation(before,after)
            target=None if phase=='activation' else only(events,'history-integrity-target-input')
            verify_native(dict(rows=after['rows']),target=target)
            verify_current_domains(dict(rows=after['rows']),{k:json.loads(v) for k,v in after['state']['control_tables']})
            actual=only(events,'history-integrity-result')
            assert actual['success']==(fault is None)
            assert actual['error']==(None if fault is None else 'Registry(InvalidMetadata)')
            if phase=='ack': assert actual['issued']==actual['acknowledged']==1 and actual['idempotent']
            final=contents(db)
            assert len(final)==len(inventory['tables'])==83
            # Driver shutdown/release is outside the tested action. It cannot
            # repair the corrupted original or mutate any non-owner relation.
            observed={k:json.loads(v) for k,v in after['state']['control_tables']}
            strip=lambda rows:sorted(({k:v for k,v in row.items() if k not in ('xmin','lab_xmin')}
                                     for row in rows),key=compact)
            for table in final.keys()-{'owner_epochs','role_instances'}:
                assert strip(final[table])==strip(observed[table]), (name,table)
            assert dump(name+'-final',current)['rows']==after['rows']
            assert original_inputs(current)==dict(after['state']['originals'])
            assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
            lab.write_json(output/(name+'-final.json'),final)
            checkpoint(name,True,result=actual,original_corruption=fault,
                all_control_rows_preserved_at_refusal=fault is not None,
                independent_scientific_frames=True,native_and_inputs_preserved=True)
        finally:
            process.stop()
            (output/(name+'.stdout')).write_bytes(bytes(process.stdout))
            (output/(name+'.stderr')).write_bytes(bytes(process.stderr))
    assert contents(database)==baseline and dump('history-source-preserved',directory)==native
    assert contents(edited)==edited_baseline and dump('history-edited-source-preserved',edited_dir)==edited_native
    checkpoint('current-history-original-source-preserved',True)
    assert cases==current_case_names(), (cases,current_case_names())
    return dict(status='PASS_SCOPED_HISTORICAL_ORIGINAL_INTEGRITY',cases=cases,
        scope='current original-body/pair integrity; two genuine dictionary originals, real closed reclamation and activation; nonzero idempotent compacted recovered ACK',
        not_run=['general managed apply/CAS','lost-ACK watermark advance','OPEN request65','physical growth envelope','signed-event acquisition retirement','load','arm64'])
