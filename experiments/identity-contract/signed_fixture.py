"""Generate/verify a fixed scientific fixture; test keys only, never product crypto.

Signing/one verification uses installed maintained python-secp256k1/libsecp256k1.
A separate slow BIP340 equation verifier validates all signatures and an official
known vector. Logical assertions reuse Model but have manually enumerated sets.
"""
from __future__ import annotations
import argparse
import copy
import hashlib
import importlib.metadata
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIELD = 2**256 - 2**32 - 977
ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
GENERATOR = (0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
             0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8)


def plus(a, b):
    if a is None: return b
    if b is None: return a
    ax, ay = a; bx, by = b
    if ax == bx and (ay + by) % FIELD == 0: return None
    slope = ((3*ax*ax) * pow(2*ay, -1, FIELD) if a == b
             else (by-ay) * pow(bx-ax, -1, FIELD)) % FIELD
    x = (slope*slope - ax - bx) % FIELD
    return x, (slope*(ax-x) - ay) % FIELD


def times(scalar, point):
    result = None
    for bit in range(scalar.bit_length() - 1, -1, -1):
        result = plus(result, result)
        if scalar >> bit & 1: result = plus(result, point)
    return result


def equation_verify(message, public, signature):
    if len(public) != 32 or len(signature) != 64: return False
    x = int.from_bytes(public, 'big')
    r, s = int.from_bytes(signature[:32], 'big'), int.from_bytes(signature[32:], 'big')
    if x >= FIELD or r >= FIELD or s >= ORDER: return False
    y = pow((x*x*x + 7) % FIELD, (FIELD+1)//4, FIELD)
    if y*y % FIELD != (x*x*x+7) % FIELD: return False
    if y % 2: y = FIELD-y
    tag = hashlib.sha256(b'BIP0340/challenge').digest()
    e = int.from_bytes(hashlib.sha256(tag+tag+signature[:32]+public+message).digest(), 'big') % ORDER
    point = plus(times(s, GENERATOR), times(ORDER-e, (x,y)))
    return point is not None and point[1] % 2 == 0 and point[0] == r


def canonical(event):
    return json.dumps([0,event['pubkey'],event['created_at'],event['kind'],event['tags'],event['content']],
                      ensure_ascii=False,separators=(',', ':')).encode()


def sign(author, created, kind, tags, content, aux=0):
    import secp256k1
    # Deliberately public test secrets 1/2/3: these must never control real funds/accounts.
    key = secp256k1.PrivateKey(int(author).to_bytes(32,'big'), raw=True)
    public = key.pubkey.serialize()[1:]
    event = dict(pubkey=public.hex(),created_at=created,kind=kind,tags=tags,content=content)
    message = hashlib.sha256(canonical(event)).digest()
    params = secp256k1.ffi.new('secp256k1_schnorrsig_extraparams *')
    for i, b in enumerate(bytes.fromhex('da6fb38c')): params.magic[i] = b
    auxiliary = secp256k1.ffi.new('unsigned char [32]', bytes([aux])*32)
    params.ndata = auxiliary
    signature = secp256k1.ffi.new('unsigned char [64]')
    assert secp256k1.lib.secp256k1_schnorrsig_sign_custom(
        secp256k1.secp256k1_ctx,signature,message,32,key.keypair,params) == 1
    event.update(id=message.hex(),sig=bytes(secp256k1.ffi.buffer(signature,64)).hex())
    return event


def verified(event):
    import secp256k1
    message = hashlib.sha256(canonical(event)).digest()
    if message.hex() != event['id']: return False
    public = bytes.fromhex(event['pubkey']); signature = bytes.fromhex(event['sig'])
    library = secp256k1.PublicKey(b'\x02'+public, raw=True).schnorr_verify(message,signature,None,raw=True)
    return library and equation_verify(message,public,signature)


def build():
    import secp256k1
    pubs = {name:secp256k1.PrivateKey(i.to_bytes(32,'big'), raw=True).pubkey.serialize()[1:].hex()
            for i,name in enumerate(('A','B','C'),1)}
    events = {}
    events['E_private'] = sign(1,1,1,[['x'],['x',''],['t','é'],['t','é']], 'same event\nwith ordered tags', 0)
    events['E_public'] = sign(1,1,1,events['E_private']['tags'],events['E_private']['content'], 1)
    events['I'] = sign(3,1,1,[], 'import then network')
    events['A_old'] = sign(1,10,0,[], '{"name":"public-old"}')
    events['A_private_new'] = sign(1,20,0,[], '{"name":"private-new"}')
    events['A_private_delete'] = sign(1,25,5,[['e',events['A_old']['id']]], 'private request')
    events['A_follows'] = sign(1,10,3,[['p',pubs['B']],['p',pubs['C']],['p',pubs['B']]], '')
    events['B_follows'] = sign(2,10,3,[['p',pubs['C']]], '')
    events['A_empty'] = sign(1,20,3,[], '')
    events['B_note'] = sign(2,15,1,[], 'arrives after its authorized deletion')
    events['B_delete'] = sign(2,20,5,[['e',events['B_note']['id']]], 'public request before target')
    events['A_expiring'] = sign(1,30,0,[['expiration','250']], '{"name":"expires"}')
    events['C_tie_a'] = sign(3,40,0,[], '{"name":"tie-a"}')
    events['C_tie_b'] = sign(3,40,0,[], '{"name":"tie-b"}')
    events['A_public_delete'] = sign(1,35,5,[['e',events['A_old']['id']]], 'public request')
    actions = []
    def receive(label, time, scope='public', received=None, origin='network', endpoint='R_public', valid=True):
        actions.append(dict(step=len(actions)+1,event=label,scope=scope,origin=origin,
                            admitted_at_us=time*1_000_000,
                            received_at_us=(time if received is None else received)*1_000_000 if origin=='network' else None,
                            endpoint=endpoint if origin=='network' else None,valid=valid))
    receive('E_private',10,'private',endpoint='R_private')
    receive('E_public',20)
    receive('E_private',30)  # Another valid signature in existing public scope, not a new representative.
    receive('E_public',40,received=15)
    receive('I',45,origin='import')
    receive('I',46)
    receive('A_old',100)
    receive('A_private_new',110,'private',endpoint='R_private')
    receive('A_private_delete',120,'private',endpoint='R_private')
    receive('A_follows',130)
    receive('B_follows',140)
    receive('A_empty',150)
    receive('B_delete',160)
    receive('B_note',170)
    receive('A_expiring',180)
    receive('C_tie_a',190)
    receive('C_tie_b',200)
    receive('A_public_delete',210)
    receive('E_public',220,received=22,endpoint='R_second')
    receive('E_public',230,received=1,valid=False)
    tie = min(('C_tie_a','C_tie_b'), key=lambda label:events[label]['id'])
    public_base = ['E_public','I']
    def case(name, step, labels, graph=(), now=249):
        return dict(name=name,after_step=step,visibility=['public'],now_nostr_seconds=now,
                    event_labels=labels,event_ids=sorted(events[e]['id'] for e in labels),
                    graph_edges=[list(edge) for edge in graph],
                    graph_pubkeys=[[pubs[a],pubs[b]] for a,b in graph])
    expected = [
        case('private-first-is-not-public',1,[]),
        case('public-representative',2,['E_public']),
        case('late-earlier-correction',4,['E_public']),
        case('import-only',5,public_base),
        case('import-then-network',6,public_base),
        case('private-newer-and-private-delete-do-not-suppress-public',9,public_base+['A_old']),
        case('duplicate-tags-distinct-logical-edges',11,public_base+['A_old','A_follows','B_follows'], [('A','B'),('A','C'),('B','C')]),
        case('empty-winner-removes-old-edges',12,public_base+['A_old','A_empty','B_follows'], [('B','C')]),
        case('unknown-deletion-target',13,public_base+['A_old','A_empty','B_follows','B_delete'], [('B','C')]),
        case('later-target-stays-suppressed',14,public_base+['A_old','A_empty','B_follows','B_delete'], [('B','C')]),
        case('before-expiry',15,public_base+['A_expiring','A_empty','B_follows','B_delete'], [('B','C')]),
        case('expired-winner-does-not-resurrect-old',15,public_base+['A_empty','B_follows','B_delete'], [('B','C')], now=250),
        case('tie-uses-smallest-event-id',17,public_base+['A_expiring','A_empty','B_follows','B_delete',tie], [('B','C')]),
        case('final-before-expiry',20,public_base+['A_expiring','A_empty','B_follows','B_delete',tie,'A_public_delete'], [('B','C')]),
        case('final-after-expiry',20,public_base+['A_empty','B_follows','B_delete',tie,'A_public_delete'], [('B','C')],now=250),
    ]
    historical_public = ['E_public','I','A_old','A_follows','B_follows','A_empty','B_delete','B_note','A_expiring','C_tie_a','C_tie_b','A_public_delete']
    historical_private = ['E_private','A_private_new','A_private_delete']
    dictionaries = {}
    for provenance in (False, True):
        entries = []
        def mapping(kind, ordinal, value, visibility='public'):
            entries.append(dict(source_record_id=['11'*16,0,1,len(entries)+1],
                qualified_ref=['11'*16,kind,'22'*16,ordinal],visibility=visibility,
                normalization_version=1,value=value))
        mapping(4,1,{'label':'public'}); mapping(4,2,{'label':'private'},'private')
        mapping(2,1,{'label':'fixture-observer','clock_resolution_us':1})
        mapping(9,1,{'source_dataset':'33'*16,'artifact_sha256':hashlib.sha256(b'fixture-import').hexdigest()})
        for ordinal,eventid in enumerate(sorted({e['id'] for e in events.values()}),1):
            mapping(8,ordinal,{'event_id':eventid},'private')  # Internal event ordinals never in public value export.
        if provenance:
            for ordinal,(label,url) in enumerate((('R_public','wss://public.example'),
                                                ('R_private','wss://private.example'),
                                                ('R_second','wss://second.example')),1):
                mapping(1,ordinal,{'label':label,'normalized_url':url},'private' if label=='R_private' else 'public')
            mapping(3,1,{'label':'public-access'}); mapping(3,2,{'label':'private-access'},'private')
            mapping(5,1,{'label':'nip01-bip340-full-verification','version':1})
        dictionaries[str(provenance).lower()] = entries
    return dict(schema='bigbrotr-signed-acceptance-fixture/1', test_keys='public integers 1,2,3; never real accounts',
                authors=pubs,events=events,actions=actions,expected_queries=expected,
                dictionaries=dict(dataset_id='11'*16,allocation_domain='22'*16,
                    bootstrap_by_provenance=dictionaries,
                    bootstrap='Commit the enumerated mappings first in shard0/epoch1, then append event facts in that same stream; producer is authorized for these fixture scopes. Disabled provenance has no endpoint/access/method witness dictionaries.'),
                expected_history_exports=dict(public_labels=historical_public,private_labels=historical_private,
                    public_event_ids=sorted(events[e]['id'] for e in historical_public),
                    private_event_ids=sorted(events[e]['id'] for e in historical_private),
                    public_E_signature=events['E_public']['sig'],private_E_signature=events['E_private']['sig'],
                    forbidden_public_metadata=['private first admission time 10000000','E_private signature','R_private endpoint/descriptor','original E body fact envelope/ordinal']),
                expected_metadata=dict(E_public_first_eligible_us=20_000_000,E_public_first_receive_at_step2_us=20_000_000,
                    E_public_first_receive_at_step4_us=15_000_000,E_private_first_receive_us=10_000_000,
                    I_first_eligible_us=45_000_000,I_receive_at_step5=None,I_receive_at_step6_us=46_000_000),
                expected_final_graph=dict(vertices=['A','B','C'],edges=[['B','C']],
                    follow_in_degree={'A':0,'B':0,'C':1},max_normalized_in_degree={'A':'0','B':'0','C':'1'},
                    score_basis='explicit graph indegree fixture only, not PageRank or a future production trust score'),
                expected_E_provenance_when_enabled=[dict(scope='private',endpoint='R_private',first_received_at_us=10_000_000),
                    dict(scope='public',endpoint='R_public',first_received_at_us=15_000_000),
                    dict(scope='public',endpoint='R_second',first_received_at_us=22_000_000)],
                expected_provenance_when_disabled=[], negative_signature='Flip byte 0 of E_public.sig at step20; do not alter its event ID or signed fields.')


def to_candidate(action, events):
    import probe
    event = events[action['event']]
    expiry = next((int(t[1]) for t in event['tags'] if len(t)>1 and t[0]=='expiration'),None)
    target = next((t[1] for t in event['tags'] if len(t)>1 and t[0]=='e'),None) if event['kind']==5 else None
    return probe.candidate(event['id'],action['scope'],action['received_at_us'],action['admitted_at_us'],
            event['sig'],action['origin'],action['endpoint'],action['valid'],event['created_at'],
            str(event['kind']) if event['kind'] in (0,3) else None,expiry,target,event['pubkey'])


def check(fixture):
    import probe
    # Official BIP340 vector 0 independently anchors the research equation checker.
    vector_pub=bytes.fromhex('F9308A019258C31049344F85F89D5229B531C845836F99B08601F113BCE036F9')
    vector_sig=bytes.fromhex('E907831F80848D1069A5371B402410364BDF1C5F8307B0084C55F1CE2DCA821525F66A4A85EA8B71E482A74F382D2CE5EBEEE8FDB2172F477DF4900D310536C0')
    assert equation_verify(bytes(32),vector_pub,vector_sig)
    assert not equation_verify(bytes([1])+bytes(31),vector_pub,vector_sig)
    events=fixture['events']
    assert all(verified(event) for event in events.values())
    # Independent JSON implementation verifies canonical IDs without Python serializer.
    script = "const fs=require('fs'),c=require('crypto'); const f=JSON.parse(fs.readFileSync(0,'utf8')); console.log(JSON.stringify(Object.fromEntries(Object.entries(f).map(([k,e])=>[k,c.createHash('sha256').update(JSON.stringify([0,e.pubkey,e.created_at,e.kind,e.tags,e.content])).digest('hex')]))));"
    node_ids=json.loads(subprocess.check_output(['node','-e',script],input=json.dumps(events).encode()))
    assert all(node_ids[k]==v['id'] for k,v in events.items())
    assert events['E_private']['id']==events['E_public']['id']
    assert events['E_private']['sig']!=events['E_public']['sig']
    bad=copy.deepcopy(events['E_public']); bad['sig']=('%02x'%(int(bad['sig'][:2],16)^1))+bad['sig'][2:]
    assert not verified(bad)
    bad_id=copy.deepcopy(events['E_public']); bad_id['content']+='!'
    assert not verified(bad_id)
    cuts={}; full_cuts={}; checks=0
    by_id={e['id']:e for e in events.values()}
    for provenance in (False,True):
        model=probe.Model(provenance); at_step={0:0}
        for action in fixture['actions']:
            result=model.admit(str(action['step']),to_candidate(action,events))
            assert (result=='Invalid') == (not action['valid'])
            at_step[action['step']]=len(model.facts)
        cuts[str(provenance).lower()]={str(step):seq for step,seq in at_step.items()}
        bootstrap = fixture['dictionaries']['bootstrap_by_provenance'][str(provenance).lower()]
        assert len(bootstrap)==(24 if provenance else 18)
        assert [row['source_record_id'] for row in bootstrap]==[['11'*16,0,1,i+1] for i in range(len(bootstrap))]
        full_cuts[str(provenance).lower()]={str(step):['11'*16,1,[[0,1,len(bootstrap)+seq]]]
                                           for step,seq in at_step.items()}
        counts={kind:sum(f['kind']==kind for f in model.facts) for kind in ('body','eligibility','receive','edge')}
        assert counts==dict(body=14,eligibility=15,receive=16,edge=17 if provenance else 0)
        assert len(model.facts)==(62 if provenance else 45)
        assert full_cuts[str(provenance).lower()]['20'][2]==[[0,1,86 if provenance else 63]]
        for expected in fixture['expected_queries']:
            cut=at_step[expected['after_step']]
            actual=model.read(expected['visibility'],cut,expected['now_nostr_seconds'])
            assert sorted(actual)==expected['event_ids'],expected['name']
            assert actual==probe.history_oracle(model.history,expected['visibility'],cut,expected['now_nostr_seconds'])
            edges=set()
            for eventid in actual:
                event=by_id[eventid]
                if event['kind']==3:
                    for tag in event['tags']:
                        if len(tag)>1 and tag[0]=='p': edges.add((event['pubkey'],tag[1]))
            assert edges==set(map(tuple,expected['graph_pubkeys'])),expected['name']
            checks+=1
        E=events['E_public']['id']; I=events['I']['id']
        assert model.read(['public'],at_step[2])[E]['sig']==events['E_public']['sig']
        assert model.read(['public'],at_step[2])[E]['first_received_at']==20_000_000
        assert model.read(['public'],at_step[4])[E]['first_received_at']==15_000_000
        assert model.read(['public'],at_step[5])[I]['first_received_at'] is None
        assert model.read(['public'],at_step[6])[I]['first_received_at']==46_000_000
        assert model.read(['private'])[E]['sig']==events['E_private']['sig']
        for scope in ('public','private'):
            elig={f['event']:f for f in model.facts if f['kind']=='eligibility' and f['scope']==scope}
            assert sorted(elig)==fixture['expected_history_exports'][scope+'_event_ids']
            exported={eventid:dict(by_id[eventid],sig=fact['value']['sig']) for eventid,fact in elig.items()}
            assert all(verified(event) for event in exported.values())
            assert all(set(event)=={'id','pubkey','created_at','kind','tags','content','sig'} for event in exported.values())
            if scope=='public':
                assert exported[E]['sig']==events['E_public']['sig']
                assert 'R_private' not in json.dumps(exported)
        witnesses={}
        for fact in model.facts:
            if fact['kind']=='edge' and fact['event']==E:
                key=(fact['scope'],fact['value']['edge'])
                witnesses[key]=min(witnesses.get(key,2**63-1),fact['value']['received'])
        expected={(v['scope'],v['endpoint']):v['first_received_at_us'] for v in fixture['expected_E_provenance_when_enabled']} if provenance else {}
        assert witnesses==expected
        # A restrictive current policy masks even the previously pinned source population.
        model.restricted.add('public'); assert not model.read(['public'],at_step[19],249)
    return dict(status='PASS',fixture_schema=fixture['schema'],signed_variants=len(events),
                unique_event_ids=len(by_id),valid_same_id_signatures=2,actions=len(fixture['actions']),
                public_query_and_graph_cases=checks,public_and_private_export_roundtrips=4,
                verification=['python-secp256k1/libsecp256k1','independent BIP340 group equation research checker',
                              'official BIP340 vector 0 and corrupted-message negative control',
                              'Node.js JSON.stringify/SHA-256 canonical ID cross-check'],
                python_secp256k1=importlib.metadata.version('secp256k1'),
                event_fact_count_after_step=cuts,
                complete_source_cuts_after_step=full_cuts,
                cut_interpretation='dataset 11*16,topology1,one shard0/epoch1 stream: bootstrap dictionaries first (18 off,24 on), then atomic event-fact batches. Final complete sequence63(off)/86(on); no storage adapter executed.',
                measured_not_claimed=['No product admission/serialization/parser execution','No engine durability, network or restore execution',
                                     'Research equation checker is neither constant-time nor proposed for production'],
                negative_controls=dict(changed_signed_content_rejected=True,corrupted_actual_signature_rejected=True,
                    private_signature_not_selected=True,private_delete_cannot_suppress_public=True,
                    expired_winner_does_not_resurrect_old=True,empty_follow_winner_has_no_old_edges=True))


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--generate',action='store_true'); args=parser.parse_args()
    path=HERE/'signed-fixture.json'
    if args.generate:
        fixture=build(); evidence=check(fixture)
        fixture['event_fact_count_after_step']=evidence['event_fact_count_after_step']
        fixture['complete_source_cuts_after_step']=evidence['complete_source_cuts_after_step']
        path.write_text(json.dumps(fixture,ensure_ascii=False,indent=2)+'\n')
    else:
        fixture=json.loads(path.read_text()); evidence=check(fixture)
        assert fixture['event_fact_count_after_step']==evidence['event_fact_count_after_step']
        assert fixture['complete_source_cuts_after_step']==evidence['complete_source_cuts_after_step']
    evidence['fixture_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    evidence['script_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    evidence['command']='python3 experiments/identity-contract/signed_fixture.py'
    evidence_path=HERE/'signed-fixture-evidence.json'
    evidence_path.write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n')
    print(json.dumps({k:v for k,v in evidence.items() if k not in
                      ('event_fact_count_after_step','complete_source_cuts_after_step')},indent=2,sort_keys=True))


if __name__=='__main__': main()
