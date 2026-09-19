"""Independent whole-byte original input oracle for the frozen two-line import."""
import hashlib
import json
from pathlib import Path
from oracle import cbor


def originals(repetitions=2):
    dataset, slot = bytes([1])*16, bytes([66])*16
    raw = (Path(__file__).resolve().parents[2]/'tests/fixtures/ingest/import-event.jsonl').read_bytes()
    assert repetitions in (1, 2)
    digest = hashlib.sha256(raw*repetitions).digest()
    assert len(raw) == 353 and digest.hex() == {
        1:'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae',
        2:'ce439b495fd2f99031706e9625529e264cd3043ffc16af4fd6002243503e90fc'}[repetitions]
    scope = [dataset, 4, bytes([64])*16, 1]
    lineage = [dataset, 9, bytes([69])*16, 1]
    dictionaries = [['dictionary', 1, [[dataset, 4, bytes([64])*16, n], 1, 'visibility', 1, {'class':name}]]
                    for n, name in [(1, 'public'), (2, 'private')]]
    dictionaries.append(['dictionary', 1, [lineage, 1, 'dataset-lineage', 1,
        {'format':'nostr-jsonl', 'format_version':1, 'source_dataset_id':None, 'source_artifact_digest':digest}]])
    event = json.loads(raw)
    signed = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
              event['kind'], event['tags'], event['content'], bytes.fromhex(event['sig'])]
    preimage = lambda n, operation, intent: b'bigbrotr/producer-request/1\0'+cbor([[slot, 1, n], operation, 1, intent])
    return {1:preimage(1, 'admit-evidence', [1, scope, dictionaries]),
            2:preimage(2, 'admit', [1, scope, signed, 'import', lineage, None]),
            3:preimage(3, 'admit', [1, scope, signed, 'import', lineage, None])}


def check(control, files, wanted_native, count, *, repetitions=2):
    expected = originals(repetitions)
    slot = bytes([66])*16
    manifest = next(value for cf, key, value in wanted_native if cf == 'default' and key == b'\x01\x01')
    manifest_sha = '\\x'+hashlib.sha256(manifest).hexdigest()
    name = lambda n: slot.hex()+'-0000000000000001-'+f'{n:016x}'+'.cbor'
    assert set(files) == {name(n) for n in range(1, count+1)}
    for sequence in range(1, count+1):
        assert files[name(sequence)] == expected[sequence]
    assert [r['request_sequence'] for r in control['requests']] == list(range(1, count+1))
    for table in ['requests', 'outcomes', 'bindings']:
        for row in control[table]:
            original = expected[row['request_sequence']]
            assert row['dataset_id'] == '\\x'+(bytes([1])*16).hex()
            assert row['producer_slot_id'] == '\\x'+slot.hex() and row['producer_epoch'] == 1
            assert row['request_digest'] == '\\x'+hashlib.sha256(original).hexdigest()
            assert row['manifest_sha256'] == manifest_sha
            if table != 'outcomes':
                assert row['input_profile'] == 1 and row['input_bytes'] == len(original)
