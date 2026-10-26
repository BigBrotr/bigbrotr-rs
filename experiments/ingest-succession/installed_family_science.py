"""Literal network effects for an activated epoch, preserving every old byte.

Only clock scalars are extracted from observations. Complete fact/receipt/input
bytes are rebuilt independently; observed receipts never supply expectations.
"""
import json

from live_replay import Science as FactEncoding, OLD_EVENT, NEW_EVENT, body, cbor
from history_replay import binary, be, sha
from transition_replay import native_map, native_rows

ENDPOINT = 'ws://127.0.0.1:7447/installed-successor-fixture'


def decode(data):
    assert len(data) <= 65536
    offset = 0
    def item(depth=0):
        nonlocal offset
        assert depth <= 16 and offset < len(data)
        tag = data[offset]; offset += 1
        if tag == 246:
            return None
        major, size = tag >> 5, tag & 31
        if size >= 24:
            assert size in (24, 25, 26, 27)
            width = 1 << (size - 24)
            assert offset + width <= len(data)
            size = int.from_bytes(data[offset:offset + width], 'big'); offset += width
        if major == 0:
            return size
        if major in (2, 3):
            assert offset + size <= len(data)
            raw = data[offset:offset + size]; offset += size
            return raw if major == 2 else raw.decode()
        assert size <= 1024
        if major == 4:
            return [item(depth + 1) for _ in range(size)]
        assert major == 5
        result = {}
        for _ in range(size):
            key = item(depth + 1)
            assert key not in result
            result[key] = item(depth + 1)
        return result
    value = item()
    assert offset == len(data) and cbor(value) == data
    return value


class NetworkScience(FactEncoding):
    def __init__(self, model, plan, *, acknowledge=False, new_event=None, endpoint=ENDPOINT):
        assert type(acknowledge) is bool
        self.acknowledge = acknowledge
        # Explicit independent fixture literals; the caller pins their source.
        # Existing family callers retain the original event and endpoint.
        self.new_event = NEW_EVENT if new_event is None else new_event
        self.endpoint = endpoint
        self.m, self.plan = model, plan
        self.values = native_map(model.native)
        self.ds, self.slot = binary(model.ds['dataset_id']), binary(model.slot)
        self.epoch, self.manifest = plan['epoch'], plan['manifest']
        self.domains = {k: binary(d) for k, d in enumerate(plan['domains'], 1)}
        old = binary(model.legacy['previous_manifest'])
        self.scope = [self.ds, 4, old[160:176], 3]
        self.refs, self.inputs, self.receipts = {}, {}, {}
        self.writes = 0
        for n in (3, 4, 8, 9, 10, 11):
            raw = bytes.fromhex(self.values['facts', (b'\x01\x20' + self.source(1, n)).hex()])
            assert raw[:2] == b'\x01\x20' and sha(raw[34:]) == raw[2:34].hex()
            self.refs[1, n] = [[self.ds, 0, 1, n], raw[2:34]]
        assert ('default', (b'\x01\x06' + self.ds + bytes.fromhex(self.new_event['id'])).hex()) not in self.values

    def commit(self, n, original, members, dependencies):
        batch = [self.refs[self.epoch, k] for k in members]
        assert members and dependencies == sorted(set(dependencies))
        receipt = b'bigbrotr/receipt/1\0' + cbor([1, [self.slot, self.epoch, n], bytes.fromhex(sha(original)),
            self.context(), batch, [self.refs[pair] for pair in dependencies]])
        first, last = members[0], members[-1]
        self.put('default', b'\x01\x02' + self.ds + be(0, 4) + be(self.epoch), b'\x01\x02' + be(last))
        pending = bytes.fromhex(sha(b'bigbrotr/admission-batch/1\0' + cbor([self.ds, 0, self.epoch, first, last, batch])))
        self.put('facts', b'\x01\x21' + self.source(self.epoch, first), b'\x01\x21' + be(last) + pending)
        self.put('receipts', b'\x01\x30' + self.slot + be(self.epoch) + be(n), receipt)
        self.put('receipts', b'\x01\x31' + self.slot + be(self.epoch), b'\x01\x31' + be(n)
            + (be(n-1)+be(0) if self.acknowledge else bytes(16)))
        if self.acknowledge:
            self.put('receipts', b'\x01\x31' + self.slot + be(self.epoch), b'\x01\x31'+be(n)+be(n)+be(0))
        name = self.slot.hex() + f'-{self.epoch:016x}-{n:016x}.cbor'
        assert name not in self.m.inputs
        self.m.inputs[name] = dict(bytes=len(original), sha256=sha(original))
        self.inputs[n], self.receipts[n] = original, receipt
        self.m.native.update(rows=native_rows(self.values), sequence=self.m.native['sequence'] + self.writes)
        self.writes = 0

    def observation(self, received, resolution, provenance):
        quality = [self.ref(2), resolution, None, 4 if resolution > 1 else 0]
        witness = [self.ref(1), self.ref(3), self.ref(5)] if provenance else None
        return [received, self.ref(2), quality, witness]

    def network(self, provenance, dictionary_at, received, recorded, resolution, historical=None,
                *, additional_events=()):
        # Optional finite encoder seam only. Callers must independently bind
        # all event literals and clocks; no ACK or complete-run result is inferred.
        if additional_events:
            assert isinstance(additional_events, (tuple,list)) and len(additional_events)==2
            assert historical is None
            assert len({self.new_event['id'],*(event['id'] for event,_,_,_ in additional_events)})==3
        definitions = [(2, 'observer', dict(clock='system-utc', vantage=None, software=None,
            method_id=None, public_key=None, route_class=None, software_version=None))]
        if provenance:
            definitions += [(1, 'endpoint', dict(url=self.endpoint)),
                (3, 'access-scope', {'class': 'public', 'label': None, 'visibility_scope_id': self.scope}),
                (5, 'event-verification', dict(input='nip01-event/1', algorithm='sha256-bip340',
                    capability='event-id-and-signature', output_unit='verification-result',
                    failure_taxonomy='bigbrotr-nip01-verifier/1', algorithm_version=1,
                    protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955'))]
        entries = [self.dictionary(n, kind, schema, value, dictionary_at)
            for n, (kind, schema, value) in enumerate(definitions, 1)]
        n = len(entries)
        original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, self.epoch, 1], 'admit-evidence', 1,
            [1, self.scope, entries]])
        self.commit(1, original, list(range(1, n + 1)), [(1, 3)] + [(self.epoch, k) for k in range(1, n + 1)])
        event, eid = self.new_event, bytes.fromhex(self.new_event['id'])
        self.dictionary(n + 1, 8, 'event', dict(event_id=eid), recorded)
        self.fact(n + 2, 'event-payload', [*body(event), self.ref(8)], recorded)
        self.index(6, self.ds + eid, n + 2)
        self.fact(n + 3, 'event-eligibility', [eid, bytes.fromhex(sha(b'bigbrotr/body/1\0' + cbor(body(event)))),
            bytes.fromhex(event['sig']), recorded, 'network', None], recorded)
        self.index(7, self.ds + eid + self.ref_key(self.scope), n + 3)
        observation = self.observation(received, resolution, provenance)
        quality = observation[2]
        self.fact(n + 4, 'event-receive', [eid, received, self.ref(2), quality, None], recorded)
        self.index(8, self.ds + eid + self.ref_key(self.scope), n + 4)
        last = n + 4
        if provenance:
            last += 1
            self.provenance(last, eid, received, quality, recorded)
        original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, self.epoch, 2], 'admit', 1,
            [1, self.scope, [*body(event), bytes.fromhex(event['sig'])], 'network', None, observation]])
        self.commit(2, original, list(range(n + 1, last + 1)), [(1, 3)] + [(self.epoch, k) for k in range(1, last + 1)])
        if additional_events:
            for ordinal,(event,at,committed,quality) in enumerate(additional_events,2):
                first=last+1
                last=self.append_network_event(event,ordinal,first,ordinal+1,
                    at,committed,quality,provenance,n)
        elif provenance:
            old_received, old_recorded = historical
            eid = bytes.fromhex(OLD_EVENT['id'])
            observation = self.observation(old_received, resolution, True)
            self.provenance(last + 1, eid, old_received, observation[2], old_recorded)
            original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, self.epoch, 3], 'admit', 1,
                [1, self.scope, [*body(OLD_EVENT), bytes.fromhex(OLD_EVENT['sig'])], 'network', None, observation]])
            # Original receive11 retains its original observer4. The new witness
            # does not replace that receive or the payload/eligibility/indexes.
            deps = [(1, k) for k in (3, 4, 8, 9, 10, 11)] + [(self.epoch, k) for k in (1, 2, 3, 4, last + 1)]
            self.commit(3, original, [last + 1], deps)
        else:
            assert historical is None

    def append_network_event(self, event, ordinal, first, request, received, recorded,
                             resolution, provenance, dictionary_count):
        """A new signed network event after the original first event encoder."""
        assert ordinal in (2,3) and request==ordinal+1 and event['kind']==1
        assert dictionary_count==(4 if provenance else 1)
        eid=bytes.fromhex(event['id'])
        assert ('default',(b'\x01\x06'+self.ds+eid).hex()) not in self.values
        canonical=json.dumps([0,event['pubkey'],event['created_at'],event['kind'],
            event['tags'],event['content']],ensure_ascii=False,separators=(',',':')).encode()
        assert sha(canonical)==event['id'] and len(bytes.fromhex(event['sig']))==64
        self.dictionary(first,8,'event',dict(event_id=eid),recorded,ordinal=ordinal)
        self.fact(first+1,'event-payload',[*body(event),self.ref(8,ordinal)],recorded)
        self.index(6,self.ds+eid,first+1)
        self.fact(first+2,'event-eligibility',[eid,bytes.fromhex(sha(b'bigbrotr/body/1\0'+cbor(body(event)))),
            bytes.fromhex(event['sig']),recorded,'network',None],recorded)
        self.index(7,self.ds+eid+self.ref_key(self.scope),first+2)
        observation=self.observation(received,resolution,provenance)
        self.fact(first+3,'event-receive',[eid,received,self.ref(2),observation[2],None],recorded)
        self.index(8,self.ds+eid+self.ref_key(self.scope),first+3)
        last=first+3
        if provenance:
            last+=1
            self.provenance(last,eid,received,observation[2],recorded)
        original=b'bigbrotr/producer-request/1\0'+cbor([[self.slot,self.epoch,request],'admit',1,
            [1,self.scope,[*body(event),bytes.fromhex(event['sig'])],'network',None,observation]])
        dependencies=[(1,3)]+[(self.epoch,k) for k in range(1,dictionary_count+1)]+[
            (self.epoch,k) for k in range(first,last+1)]
        self.commit(request,original,list(range(first,last+1)),dependencies)
        return last

    def provenance(self, n, eid, received, quality, recorded):
        self.fact(n, 'provenance', [eid, self.ref(1), self.ref(2), self.ref(3), received,
            quality, self.ref(5), None], recorded)
        self.index(9, self.ds + eid + self.ref_key(self.ref(1)) + self.ref_key(self.ref(2))
            + self.ref_key(self.ref(3)) + self.ref_key(self.scope), n)
