#!/usr/bin/env python3
"""Independent fixed-width export command fingerprints; no Rust dependency."""
import copy
import hashlib
import json
from pathlib import Path


def fingerprint(dataset, actor, key, command):
    query = command['query']
    scope = query['scope']
    allocation = command['allocation']
    destination = command['destination']
    raw = bytearray(b'bigbrotr/export-event/1\0')
    for value in [dataset, actor, key, scope['dataset_id']]:
        raw += bytes.fromhex(value)
    raw += (4).to_bytes(2, 'big')
    raw += bytes.fromhex(scope['allocation_domain'])
    raw += int(scope['ordinal']).to_bytes(8, 'big')
    raw += bytes.fromhex(query['filters'][0]['ids'][0])
    raw += query.get('page_size', 100).to_bytes(2, 'big')
    raw += bytes.fromhex(allocation['catalog_id'])
    raw += int(allocation['frontier_revision']).to_bytes(8, 'big')
    raw += bytes.fromhex(destination['id'])
    raw += bytes.fromhex(destination['binding_sha256'])
    raw += b'\x01\x01'  # signed-events-jsonl profile1, private artifact
    for field in ['expected_config_revision', 'expected_policy_revision']:
        raw += int(command[field]).to_bytes(8, 'big')
    return bytes(raw)


base = dict(kind='export', format='signed-events-jsonl', query=dict(
    version=1, resource='event_history', mode='snapshot',
    scope=dict(dataset_id='01' * 16, dictionary_kind=4, allocation_domain='02' * 16, ordinal='7'),
    filters=[dict(ids=['03' * 32])], page_size=100, cursor=None, projection=None),
    allocation=dict(catalog_id='04' * 16, frontier_revision='5'),
    destination=dict(id='05' * 16, binding_sha256='06' * 32),
    expected_config_revision='1', expected_policy_revision='2')
vectors = []
for name in ['ordinary', 'maximum', 'implicit-read-defaults']:
    command = copy.deepcopy(base)
    if name == 'maximum':
        command['query']['scope']['ordinal'] = str(2**64 - 1)
        command['query']['page_size'] = 1000
        command['allocation']['frontier_revision'] = str(2**64 - 1)
        command['expected_config_revision'] = str(2**64 - 1)
        command['expected_policy_revision'] = str(2**64 - 1)
    if name == 'implicit-read-defaults':
        for field in ['mode', 'page_size', 'cursor', 'projection']:
            del command['query'][field]
    raw = fingerprint('01' * 16, '07' * 16, '08' * 16, command)
    vectors.append(dict(name=name, dataset='01' * 16, actor='07' * 16, command_key='08' * 16,
                        command=command, bytes_hex=raw.hex(), sha256=hashlib.sha256(raw).hexdigest()))
Path(__file__).with_name('export-command.json').write_text(json.dumps(dict(
    format='bigbrotr-export-command-fixture/1', oracle='write_export_command.py', vectors=vectors), indent=2) + '\n')
