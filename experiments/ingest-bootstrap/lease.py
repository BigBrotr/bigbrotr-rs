"""Scheduling-only renewal oracle; original corpus and complete state comparisons."""
import hashlib
import json
from bootstrap_oracle import cbor, expected
import lab


def run(pg, client, output, database, name, rows):
    mode = 'lease-' + name
    before = rows(database)
    assert not before['owner_epochs'] and not before['producer_requests']
    lab.write_json(output / (name + '-before.json'), before)
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, database,
        '/run/secrets/application', '/lab/' + name, check=False, timeout=45)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    assert result.returncode == 0 and not result.stderr, (name, result.returncode)
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    first = next(m for m in messages if m['event'] == 'lease-before')
    final = messages[-1]
    assert final['event'] == 'lease-finished' and final['mode'] == mode and final['status'] == 'PASS'
    original, renewed = first['state'], final['state']
    for field in ['native_rows', 'native_sha256', 'sequence', 'originals']:
        assert original[field] == renewed[field], field
    states = [{n: json.loads(v) for n, v in s['control_tables']} for s in (original, renewed)]
    old, new = states
    assert len(old) == len(before) and old.keys() == before.keys()
    if name == 'pass':
        assert final['original_expiry'] == first['original_expiry']
        assert final['original_expiry'] < final['observed_at'] < final['renewed_expiry']
        assert {n for n in old if old[n] != new[n]} == {'owner_epochs'}
        control = next(r for r in old['owner_epochs'] if r['role'] == 'control')
        assert control in new['owner_epochs']
        assert len(old['owner_epochs']) == len(new['owner_epochs']) == 2
        old_ingest, new_ingest = [dict(next(r for r in s['owner_epochs'] if r['role'] == 'ingest')) for s in states]
        assert old_ingest.pop('lease_expires_at_us') == first['original_expiry']
        assert new_ingest.pop('lease_expires_at_us') == control['lease_expires_at_us'] == final['renewed_expiry']
        assert old_ingest.pop('xmin') != new_ingest.pop('xmin')
        assert old_ingest == new_ingest
    else:
        assert original == renewed and final['zero_writes']
    if name == 'cancelled':
        assert next(m for m in messages if m['event'] == 'lease-cancelled') == dict(
            event='lease-cancelled', actual_backend_lock=True, retained_control_fence=True)
    else:
        decision = next(m for m in messages if m['event'] == 'lease-result')
        assert decision['commit_started'] == (name == 'pass')
        assert decision['validated'] == (0 if name == 'wrong-control' else 1 if name == 'policy-first' else 2)
        wanted = ('Ok(())' if name == 'pass' else 'Err(InvalidInput)' if name == 'wrong-control'
                  else 'Err(InvalidMetadata)' if name in ['closing','draining','wrong-registration']
                  else 'Err(InvalidPolicy)' if name in ['policy-first','policy-second'] else 'Err(Unavailable)')
        assert decision['result'] == wanted
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    for table in before:
        assert all(r in after[table] for r in before[table] if table != 'datasets'), table
    dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    wanted, receipt, _ = expected(dataset, True)
    assert actual == wanted and len(actual) == 12
    reference = lambda n: [dataset, 4, bytes([64]) * 16, n]
    entries = [['dictionary', 1, [reference(n), 1, 'visibility', 1, {'class': label}]]
               for n, label in [(1, 'public'), (2, 'private')]]
    request = b'bigbrotr/producer-request/1\0' + cbor([
        [bytes([66]) * 16, 1, 1], 'admit-evidence', 1, [1, reference(1), entries]])
    inputs = list((output / name / 'producer-inputs-v1').glob('*.cbor'))
    assert len(inputs) == 1 and inputs[0].read_bytes() == request
    assert len(after['producer_requests']) == len(after['producer_outcomes']) == 1
    assert after['producer_requests'][0]['request_digest'] == '\\x' + hashlib.sha256(request).hexdigest()
    assert after['producer_outcomes'][0]['outcome_bytes'] == '\\x' + receipt.hex()
    assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
    return dict(name=name, status='PASS', actual_pg_rocks=True,
                complete_native_rows=len(actual), original_inputs=len(inputs),
                only_scheduling_renewed=name == 'pass', control_tables=len(old))
