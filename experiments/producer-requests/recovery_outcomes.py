"""Independent native/input bytes for the additional connected recovery classes."""
import hashlib
import json
from pathlib import Path

from closing import expected_native
from event_oracle import expected_event
from existing_outcome import expected as already_durable
from import_originals import originals
from oracle import cbor


def inputs(scenario):
    values = originals(1)
    if scenario == 'rejected':
        event = json.loads((Path(__file__).resolve().parents[2]/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
        dataset, slot = bytes([1])*16, bytes([66])*16
        signed = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']),
                  event['created_at'], event['kind'], event['tags'], event['content'], bytes(64)]
        values[2] = b'bigbrotr/producer-request/1\0'+cbor([[slot, 1, 2], 'admit', 1,
            [1, [dataset, 4, bytes([64])*16, 1], signed, 'import', [dataset, 9, bytes([69])*16, 1], None]])
    return {n:values[n] for n in range(1, 4 if scenario == 'already' else 3)}


def native(scenario, closed):
    if scenario not in ['already', 'rejected']:
        return expected_native(False, closed)
    dataset, slot = bytes([1])*16, bytes([66])*16
    be = lambda n:n.to_bytes(8, 'big')
    if scenario == 'already':
        wanted = already_durable(1)[0]
        issued = 3
    else:
        wanted = expected_event(dataset, False)[0]
        original = inputs(scenario)[2]
        rejection = (b'bigbrotr/rejection/1\0'+slot+be(1)+be(2)
                     +hashlib.sha256(original).digest()+dataset+be(1)+b'\x04')
        wanted.append(('receipts', b'\x01\x30'+slot+be(1)+be(2), rejection))
        issued = 2
    key = b'\x01\x31'+slot+be(1)
    wanted = [r for r in wanted if (r[0], r[1]) != ('receipts', key)]
    value = (b'\x02\x31' if closed else b'\x01\x31')+be(issued)+bytes(16)+(b'\x01' if closed else b'')
    wanted.append(('receipts', key, value))
    return sorted(wanted)
