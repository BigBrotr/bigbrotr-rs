"""Independent fixed-width command binding; no production code imports."""
import hashlib
import json
import struct
from pathlib import Path

dataset, actor, target, key = (bytes([n])*16 for n in (1,2,3,4))
command = dict(principal_id=target.hex(), action='history.read',
    scope=dict(dataset_id=dataset.hex(),dictionary_kind=4,allocation_domain=(bytes([5])*16).hex(),ordinal='7'),
    enabled=True,expected_config_revision='11',expected_policy_revision='13',expected_grant_revision='17',
    allocation=dict(catalog_id=(bytes([6])*16).hex(),frontier_revision='19'))

def preimage(enabled, action_code=1):
    value = (b'bigbrotr/scope-grant-change/1\0'+dataset+actor+key+target
        +dataset+struct.pack('>H',4)+bytes([5])*16+struct.pack('>Q',7)
        +bytes([action_code,int(enabled)])+struct.pack('>QQQ',11,13,17)+bytes([int(enabled)]))
    return value+(bytes([6])*16+struct.pack('>Q',19) if enabled else b'')

vectors=[]
for enabled in [True,False]:
    request=dict(command,enabled=enabled,allocation=command['allocation'] if enabled else None)
    raw=preimage(enabled)
    vectors.append(dict(name='enable' if enabled else 'revoke',dataset=dataset.hex(),actor=actor.hex(),
                        command_key=key.hex(),command=request,preimage_hex=raw.hex(),sha256=hashlib.sha256(raw).hexdigest()))
native_command = dict(command, principal_id=(bytes([16])*16).hex(), enabled=False,
    expected_config_revision='1', expected_policy_revision='1', expected_grant_revision='1', allocation=None)
native = (b'bigbrotr/scope-grant-change/1\0'+bytes([1])*16+bytes([16])*16+bytes([1])*16+bytes([16])*16
    +bytes([1])*16+struct.pack('>H',4)+bytes([5])*16+struct.pack('>Q',7)
    +bytes([1,0])+struct.pack('>QQQ',1,1,1)+bytes([0]))
vectors.append(dict(name='native-reauth-revoke',dataset=(bytes([1])*16).hex(),actor=(bytes([16])*16).hex(),
    command_key=(bytes([1])*16).hex(),command=native_command,preimage_hex=native.hex(),sha256=hashlib.sha256(native).hexdigest()))
# Retain the literal native fixture identities used by the PostgreSQL probe.
for enabled, revision, key_byte in [(True, 1, 0xc1), (False, 2, 0xc2)]:
    request = dict(command, principal_id='10'*16, enabled=enabled,
        scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
        expected_config_revision='1',expected_policy_revision='1',expected_grant_revision=str(revision),
        allocation=dict(catalog_id='5a'*16,frontier_revision='1') if enabled else None)
    raw = (b'bigbrotr/scope-grant-change/1\0'+bytes([1])*16+bytes([16])*16+bytes([key_byte])*16+bytes([16])*16
        +bytes([1])*16+struct.pack('>H',4)+bytes([4])*16+struct.pack('>Q',1)
        +bytes([1,int(enabled)])+struct.pack('>QQQ',1,1,revision)+bytes([int(enabled)]))
    if enabled: raw += bytes([0x5a])*16+struct.pack('>Q',1)
    vectors.append(dict(name='native-scope-enable' if enabled else 'native-scope-revoke',dataset='01'*16,actor='10'*16,
        command_key=bytes([key_byte]*16).hex(),command=request,preimage_hex=raw.hex(),sha256=hashlib.sha256(raw).hexdigest()))
for action, code in [('export',2), ('private-export',3)]:
    for enabled in [True, False]:
        request = dict(command,action=action,enabled=enabled,allocation=command['allocation'] if enabled else None)
        raw = preimage(enabled,code)
        vectors.append(dict(name=action+('-enable' if enabled else '-revoke'),dataset=dataset.hex(),actor=actor.hex(),
            command_key=key.hex(),command=request,preimage_hex=raw.hex(),sha256=hashlib.sha256(raw).hexdigest()))
Path(__file__).with_name('scope-grant.json').write_text(json.dumps(dict(format='bigbrotr-scope-grant-vectors/1',vectors=vectors),indent=2)+'\n')
