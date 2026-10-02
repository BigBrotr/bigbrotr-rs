"""Bounded historical lookup against installed native stores and isolated PG clones."""
import hashlib
import json
from pathlib import Path

import lab
from installed import run as initialize
from registrations import upgrade as upgrade_registry
from scopes import upgrade as upgrade_scopes


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
    return dict(event='history-loaded', dataset_id=manifest[10:26].hex(),
        instance_id=state['datasets'][0]['instance_id'][2:], control_resource=control['resource_id'][2:],
        control_epoch=control['epoch'], control_role_instance=control['role_instance_id'][2:],
        control_incarnation=control['process_incarnation'][2:], ingest_resource=owner['resource_id'][2:],
        ingest_role_instance=owner['role_instance_id'][2:], ingest_incarnation=owner['process_incarnation'][2:],
        shard_id=integer(50, 4), config_revision=config['revision'], policy_revision=policy['revision'],
        config_digest=config['document_digest'][2:], policy_digest=policy['document_digest'][2:],
        previous_topology=topology['previous_generation'],
        allocation_domains=sorted([[r['dictionary_kind'],r['allocation_domain'][2:]] for r in state['allocation_domains']]),
        producer_slot=slot['producer_slot_id'][2:], window=producer['outstanding_request_limit'],
        created_at_us=owner['acquired_at_us'], lease_expires_at_us=owner['lease_expires_at_us'], manifest=manifest.hex())


def run(pg, client, output, database, name, rows, clone):
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

    changed = clone('history_heads', database)
    statements = []
    for table in ['config_revisions','policy_revisions']:
        document = json.loads(json.dumps(base[table][0]['document']))
        document['settings']['ingest.producer_outstanding_requests'] = 16
        encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        digest = hashlib.sha256(b'bigbrotr/product-policy/2\0'+encoded).hexdigest()
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
        'policy-schema': ("UPDATE control.policy_revisions SET schema_version=3 WHERE revision=1", 'HeadsChanged'),
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
        db = clone('history_'+fault.replace('-','_'), database)
        lab.sql(pg,db,'BEGIN; SET LOCAL session_replication_role=replica; '+sql+'; COMMIT;')
        inspect(db, 'history-'+fault, error=error)

    zero = clone('history_zero', 'bb_source_template')
    upgrade_registry(pg,client,output,zero,'history-zero',rows)
    upgrade_scopes(pg,client,output,zero,'history-zero',rows)
    initialize(pg,client,output,zero,'zero-topology',rows)
    result = inspect(zero, 'history-zero-predecessor', seed='zero-topology')
    assert result['previous_topology'] == 0 and int.from_bytes(bytes.fromhex(result['manifest'])[26:34],'big') == 1
    assert rows(database) == base
    return dict(name=name,status='PASS',cases=cases,scope='bounded historical lookup only; no native closing, request resolution or successor')
