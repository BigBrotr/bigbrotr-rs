#!/usr/bin/env python3
import hashlib, json
from pathlib import Path
class CMap:
    def __init__(self, pairs): self.pairs=pairs

def head(major,n):
    if n < 24: return bytes([(major<<5)|n])
    if n <= 0xff: return bytes([(major<<5)|24,n])
    if n <= 0xffff: return bytes([(major<<5)|25])+n.to_bytes(2,'big')
    if n <= 0xffffffff: return bytes([(major<<5)|26])+n.to_bytes(4,'big')
    if n <= 0xffffffffffffffff: return bytes([(major<<5)|27])+n.to_bytes(8,'big')
    raise ValueError(n)
def enc(v):
    if v is None: return b'\xf6'
    if isinstance(v,bytes): return head(2,len(v))+v
    if isinstance(v,str):
        raw=v.encode(); return head(3,len(raw))+raw
    if isinstance(v,int) and v>=0: return head(0,v)
    if isinstance(v,list): return head(4,len(v))+b''.join(enc(x) for x in v)
    if isinstance(v,CMap): return head(5,len(v.pairs))+b''.join(enc(k)+enc(x) for k,x in v.pairs)
    raise TypeError(type(v))

dataset=bytes([0x11])*16
allocation=bytes([0x22])*16
slot=bytes([0x33])*16
def ref(kind,ordinal): return [dataset,kind,allocation,ordinal]
def payload(reference,schema,value): return [reference,1,schema,1,value]
method_value=CMap([
 ('input','nip01-event/1'),('algorithm','sha256-bip340'),('capability','event-id-and-signature'),
 ('output_unit','verification-result'),('failure_taxonomy','bigbrotr-nip01-verifier/1'),
 ('algorithm_version',1),('protocol_revision','a2494f4f81d46684e5814a9bf35e2b1df978f955')])
def entries(scope_class='public',observer_ordinal=3):
 return [
  payload(ref(4,1),'visibility',CMap([('class',scope_class)])),
  payload(ref(5,2),'event-verification',method_value),
  payload(ref(2,observer_ordinal),'observer',CMap([
    ('clock','system-utc'),('vantage','fixture-vantage'),('software',None),('method_id',ref(5,2)),
    ('public_key',None),('route_class','public'),('software_version',None)])),
  payload(ref(3,4),'access-scope',CMap([('class','public'),('label',None),('visibility_scope_id',ref(4,1))])),
 ]
def intent(values): return [65535,ref(4,1),[['dictionary',1,v] for v in values]]
def request(values): return [ [slot,0xffffffffffffffff,1], 'admit-evidence', 1, intent(values)]
def vector(name,values,full=False):
 inner=enc(intent(values)); pre=b'bigbrotr/producer-request/1\0'+enc(request(values))
 out={'name':name,'entry_order':['visibility','event-verification','observer','access-scope'] if name!='changed-order' else ['access-scope','observer','event-verification','visibility'],'encoded_intent_bytes':len(inner),'request_preimage_bytes':len(pre),'sha256':hashlib.sha256(pre).hexdigest()}
 if full: out['preimage_hex']=pre.hex()
 return out
base=entries()
vectors=[vector('base',base,True),vector('changed-descriptor',entries('private')),vector('changed-reference',entries(observer_ordinal=5)),vector('changed-order',list(reversed(base)))]
out={'schema':'bigbrotr-evidence-intent-v1-vectors/1','authoring':'Independent Python definite-CBOR encoder; no Rust product/library import.','producer_request_id':{'producer_slot_id':slot.hex(),'producer_epoch':0xffffffffffffffff,'request_sequence':1},'admission_policy_version':65535,'dataset_id':dataset.hex(),'allocation_domain':allocation.hex(),'visibility_scope':{'dictionary_kind':4,'ordinal':1},'signature_dimension':'not-applicable: dictionary-only evidence records contain no signed event or signature field','vectors':vectors}
path=Path(__file__).with_name('evidence-intent-v1.json')
path.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'vectors':[{k:v for k,v in x.items() if k!='preimage_hex'} for x in vectors]},indent=2))
