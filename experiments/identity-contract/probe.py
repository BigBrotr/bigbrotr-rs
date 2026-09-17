"""Isolated paper-contract model/encoding probe, never a product implementation."""
from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
import platform
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def cbor(value):
    """Bounded experiment encoder; a maintained codec must be used in product."""
    def head(major, argument):
        if not 0 <= argument <= 2**64 - 1:
            raise ValueError('integer overflow')
        if argument < 24:
            return bytes([major * 32 + argument])
        for width, additional in ((1, 24), (2, 25), (4, 26), (8, 27)):
            if argument < 2 ** (width * 8):
                return bytes([major * 32 + additional]) + argument.to_bytes(width, 'big')
        raise AssertionError('unreachable')
    if value is None:
        return b'\xf6'
    if isinstance(value, bool):
        return b'\xf5' if value else b'\xf4'
    if isinstance(value, int):
        if value < -(2**63):
            raise ValueError('signed overflow')
        return head(0, value) if value >= 0 else head(1, -1-value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        encoded = value.encode('utf-8', errors='strict')
        return head(3, len(encoded)) + encoded
    if isinstance(value, list):
        return head(4, len(value)) + b''.join(map(cbor, value))
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError('closed map field keys must be text')
        keys = sorted(value, key=cbor)
        return head(5, len(keys)) + b''.join(cbor(k) + cbor(value[k]) for k in keys)
    raise ValueError('unsupported CBOR value')


def unpack(value):
    if isinstance(value, list):
        return [unpack(v) for v in value]
    if isinstance(value, dict):
        if set(value) == {'$bytes'}:
            return bytes.fromhex(value['$bytes'])
        if set(value) == {'$int'}:
            return int(value['$int'])
        return {k: unpack(v) for k, v in value.items()}
    return value


def exact_fields(value, required):
    if set(value) != set(required):
        raise ValueError('missing or unknown fields')


def fingerprint(domain, value):
    return hashlib.sha256(f'bigbrotr/{domain}/1\0'.encode('ascii') + cbor(value)).hexdigest()


def immutable_json(value):
    def encode_set(v):
        if isinstance(v, set):
            return sorted(v)
        raise TypeError(f'unsupported value {type(v).__name__}')
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      default=encode_set)


def candidate(event='E', scope='public', received=20, admitted=20, sig='A',
              origin='network', edge='relay-1', valid=True, created=10,
              coordinate=None, expires=None, deletes=None, author='author-1'):
    return dict(event=event, scope=scope, received=received if origin == 'network' else None,
                admitted=admitted, sig=sig, origin=origin, edge=edge, valid=valid,
                created=created, coordinate=coordinate, expires=expires,
                deletes=deletes, author=author)


class Model:
    """Incremental inventory plus immutable facts. No engine or fsync simulation."""
    def __init__(self, provenance):
        self.provenance = provenance
        self.facts = []
        self.requests = {}
        self.retired = set()
        self.history = []
        self.restricted = set()

    def snapshot(self):
        return immutable_json(self.__dict__)

    def bound_intent(self, c):
        result = {k: v for k, v in c.items() if k not in ('admitted', 'valid')}
        if not self.provenance:
            result.pop('edge')
        return result

    def _records(self, kind, event, scope=None):
        return [f for f in self.facts if f['kind'] == kind and f['event'] == event
                and (scope is None or f['scope'] == scope)]

    def _append(self, kind, c, value):
        fact = dict(seq=len(self.facts) + 1, kind=kind, event=c['event'],
                    scope=c['scope'], value=copy.deepcopy(value))
        fact['digest'] = hashlib.sha256(immutable_json(fact).encode()).hexdigest()
        self.facts.append(fact)

    def effects(self, c):
        result = []
        bodies = self._records('body', c['event'])
        body = {k: c[k] for k in ('event', 'created', 'coordinate', 'expires', 'deletes', 'author')}
        if bodies and bodies[0]['value'] != body:
            return 'Invalid'
        if not bodies:
            result.append(('body', body))
        if not self._records('eligibility', c['event'], c['scope']):
            result.append(('eligibility', {k: c[k] for k in ('sig', 'admitted', 'origin')}))
        if c['origin'] == 'network':
            receives = self._records('receive', c['event'], c['scope'])
            if not receives or c['received'] < min(f['value'] for f in receives):
                result.append(('receive', c['received']))
            if self.provenance:
                edges = [f for f in self._records('edge', c['event'], c['scope'])
                         if f['value']['edge'] == c['edge']]
                if not edges or c['received'] < min(f['value']['received'] for f in edges):
                    result.append(('edge', {k: c[k] for k in ('edge', 'received')}))
        return result

    def admit(self, request, c):
        digest = hashlib.sha256(immutable_json(self.bound_intent(c)).encode()).hexdigest()
        if request in self.retired:
            return 'RetiredRequest'
        if request in self.requests:
            return self.requests[request][1] if self.requests[request][0] == digest else 'RequestConflict'
        if not c['valid']:
            return 'Invalid'
        if c['scope'] in self.restricted:
            return 'Excluded'
        effects = self.effects(c)
        if effects == 'Invalid':
            return 'Invalid'
        for kind, value in effects:
            self._append(kind, c, value)
        outcome = 'NewFacts' if effects else 'AlreadyDurable'
        self.requests[request] = [digest, outcome]
        self.history.append([len(self.facts), copy.deepcopy(c)])
        return outcome

    def certify(self, c):
        if not c['valid']:
            return 'Invalid'
        if c['scope'] in self.restricted:
            return 'Excluded'
        effects = self.effects(c)
        if effects == 'Invalid':
            return 'Invalid'
        if effects:
            return 'NeedsAdmission'
        facts = self._records('body', c['event'])[:1]
        facts += self._records('eligibility', c['event'], c['scope'])[:1]
        if c['origin'] == 'network':
            facts += [min(self._records('receive', c['event'], c['scope']), key=lambda f: f['value'])]
            if self.provenance:
                edges = [f for f in self._records('edge', c['event'], c['scope'])
                         if f['value']['edge'] == c['edge']]
                facts += [min(edges, key=lambda f: f['value']['received'])]
        return {'intent': self.bound_intent(c), 'deps': sorted([[f['seq'], f['digest']] for f in facts])}

    def validate_restored_proof(self, proof):
        available = {(f['seq'], f['digest']) for f in self.facts}
        supplied = set(map(tuple, proof['deps']))
        if not supplied <= available:
            return False
        # The original sufficient basis remains valid after a later correction;
        # validate that basis, not equality with today's preferred minimum.
        basis = Model(self.provenance)
        basis.restricted = set(self.restricted)
        basis.facts = [f for f in self.facts if (f['seq'], f['digest']) in supplied]
        expected = basis.certify(dict(proof['intent'], admitted=0, valid=True,
                                     edge=proof['intent'].get('edge')))
        return isinstance(expected, dict) and set(map(tuple, expected['deps'])) <= supplied

    def read(self, scopes, cut=None, now=0):
        cut = len(self.facts) if cut is None else cut
        scopes = set(scopes) - self.restricted
        facts = [f for f in self.facts if f['seq'] <= cut]
        payloads = {f['event']: f['value'] for f in facts if f['kind'] == 'body'}
        results = {}
        for event, payload in payloads.items():
            elig = [f for f in facts if f['kind'] == 'eligibility' and f['event'] == event and f['scope'] in scopes]
            if not elig:
                continue
            e = min(elig, key=lambda f: (f['value']['admitted'], f['seq']))
            times = [f['value'] for f in facts if f['kind'] == 'receive' and f['event'] == event and f['scope'] in scopes]
            results[event] = dict(payload, sig=e['value']['sig'], first_eligible_at=e['value']['admitted'],
                                  first_received_at=min(times) if times else None)
        deleted = {v['deletes'] for v in results.values() if v['deletes'] and
                   v['deletes'] in results and results[v['deletes']]['author'] == v['author']}
        winners = {}
        ordinary = {}
        for event, value in results.items():
            if value['coordinate'] is None:
                ordinary[event] = value
            else:
                key = (value['author'], value['coordinate'])
                old = winners.get(key)
                if old is None or (-value['created'], event) < (-old['created'], old['event']):
                    winners[key] = value
        ordinary.update({v['event']: v for v in winners.values()})
        return {event: value for event, value in ordinary.items()
                if event not in deleted and (value['expires'] is None or now < value['expires'])}


def history_oracle(history, scopes, cut, now, restricted=()):
    """Independent all-arrivals reduction; never consults incremental facts/indexes."""
    allowed = [c for committed, c in history if committed <= cut and c['scope'] in scopes
               and c['scope'] not in restricted and c['valid']]
    # Distinct scope acquisition is the first encounter in admission order,
    # even when a later wall clock value is numerically smaller.
    first = {}
    for c in allowed:
        first.setdefault((c['event'], c['scope']), c)
    selected = {}
    for event in {c['event'] for c in allowed}:
        representatives = [c for (e, _), c in first.items() if e == event]
        representatives.sort(key=lambda c: (c['admitted'], allowed.index(c)))
        rep = representatives[0]
        times = [c['received'] for c in allowed if c['event'] == event and c['origin'] == 'network']
        selected[event] = {k: rep[k] for k in ('event', 'created', 'coordinate', 'expires', 'deletes', 'author')}
        selected[event].update(sig=rep['sig'], first_eligible_at=rep['admitted'],
                               first_received_at=min(times) if times else None)
    result = {}
    for event, row in selected.items():
        peers = [r for r in selected.values() if row['coordinate'] is not None
                 and r['coordinate'] == row['coordinate'] and r['author'] == row['author']]
        if peers and sorted(peers, key=lambda r: (-r['created'], r['event']))[0]['event'] != event:
            continue
        if any(c['deletes'] == event and c['author'] == row['author'] for c in selected.values()):
            continue
        if row['expires'] is not None and now >= row['expires']:
            continue
        result[event] = row
    return result


def check_model():
    cases = 0
    comparisons = 0
    negative = {}
    # All orders and prefixes; each scoped acquisition retains its own signature.
    base = [candidate(scope='private', admitted=10, received=10, sig='private-signature'),
            candidate(admitted=20, received=20, sig='public-signature'),
            candidate(admitted=30, received=5, sig='valid-alternate'),
            candidate(admitted=0, origin='import', sig='import-signature')]
    for provenance, order in itertools.product((False, True), itertools.permutations(base)):
        model = Model(provenance)
        for i, c in enumerate(order):
            model.admit(str(i), c)
            for scopes in (['public'], ['private'], ['public', 'private']):
                for cut in [0] + [entry[0] for entry in model.history]:
                    assert model.read(scopes, cut) == history_oracle(model.history, scopes, cut, 0)
                    comparisons += 1
            # Byte-identical replay outcome, with zero new facts or acquisition time.
            before = model.snapshot()
            assert model.admit(str(i), c) in ('NewFacts', 'AlreadyDurable')
            assert model.snapshot() == before
        restored = Model(provenance)
        restored.__dict__ = json.loads(model.snapshot())
        restored.retired = set(restored.retired)
        restored.restricted = set(restored.restricted)
        assert restored.read(['public']) == model.read(['public'])
        cases += 1

    for provenance in (False, True):
        model = Model(provenance)
        private, public = base[:2]
        model.admit('private', private)
        assert model.certify(public) == 'NeedsAdmission'
        model.admit('public', public)
        out = model.read(['public'])['E']
        assert out['sig'] == 'public-signature' and out['first_received_at'] == 20
        negative['global_first_signature'] = out['sig'] != private['sig']
        negative['global_first_receive'] = out['first_received_at'] != private['received']
        later = dict(public, admitted=40, received=40, sig='later-valid')
        before = model.snapshot()
        proof = model.certify(later)
        assert isinstance(proof, dict) and model.snapshot() == before
        assert model.validate_restored_proof(proof)
        missing = copy.deepcopy(proof)
        missing['deps'] = [d for d in missing['deps'] if model.facts[d[0]-1]['kind'] != 'eligibility']
        negative['proof_missing_eligibility'] = not model.validate_restored_proof(missing)
        broken = copy.deepcopy(model)
        broken.facts = [f for f in broken.facts if f['kind'] != 'body']
        negative['backup_missing_body'] = not broken.validate_restored_proof(proof)
        assert model.certify(dict(later, received=1)) == 'NeedsAdmission'
        assert model.certify(dict(later, edge='relay-2')) == ('NeedsAdmission' if provenance else proof)
        assert model.certify(dict(later, valid=False)) == 'Invalid'
        negative['id_hit_bypasses_signature_verifier'] = model.certify(dict(later, valid=False)) != proof
        assert model.admit('public', dict(public, sig='different-valid')) == 'RequestConflict'
        assert model.admit('public', dict(public, received=21)) == 'RequestConflict'
        negative['same_event_means_same_request'] = model.admit('public', dict(public, sig='different-valid')) == 'RequestConflict'
        replay_before = model.snapshot()
        assert model.admit('public', dict(public, admitted=9000)) == 'NewFacts'
        assert model.snapshot() == replay_before
        model.admit('new-correction', dict(public, received=2))
        assert model.validate_restored_proof(proof)
        model.retired.add('public')
        del model.requests['public']
        assert model.admit('public', public) == 'RetiredRequest'
        assert model.admit('public', dict(public, received=1)) == 'RetiredRequest'
        model.restricted.add('public')
        assert model.read(['public']) == {} and model.certify(later) == 'Excluded'
        model.restricted.clear()
        assert model.read(['public'])['E']['sig'] == out['sig']
        assert model.read(['public'])['E']['first_received_at'] == 2
        # Import does not invent a local network observation; network does.
        imported = Model(provenance)
        imported.admit('import', base[3])
        assert imported.read(['public'])['E']['first_received_at'] is None
        assert imported.certify(public) == 'NeedsAdmission'
        imported.admit('network', public)
        assert imported.read(['public'])['E']['first_received_at'] == 20
        assert imported.read(['public'])['E']['sig'] == 'import-signature'
        assert sum(f['kind'] == 'edge' for f in imported.facts) == int(provenance)
        # Later ordinary duplicate is effect-free, earlier correction preserves old cut.
        cut = len(imported.facts)
        count = len(imported.facts)
        assert imported.admit('later', later) == 'AlreadyDurable'
        assert len(imported.facts) == count
        assert imported.admit('earlier', dict(public, received=1)) == 'NewFacts'
        assert imported.read(['public'], cut)['E']['first_received_at'] == 20
        assert imported.read(['public'])['E']['first_received_at'] == 1
        cases += 1

    # Independent explicit expected sets discriminate order-of-operations bugs.
    old = candidate('old', created=10, coordinate='profile')
    newer = candidate('newer', created=20, coordinate='profile', scope='private')
    public_delete = candidate('delete', deletes='old', scope='public')
    private_delete = dict(public_delete, scope='private')
    m = Model(False)
    for i, c in enumerate((old, newer, private_delete)):
        m.admit(str(i), c)
    assert set(m.read(['public'])) == {'old'}
    wrong_winner = max((old, newer), key=lambda c: c['created'])
    wrong_public = {wrong_winner['event']} if wrong_winner['scope'] == 'public' else set()
    negative['winner_before_authorization'] = wrong_public != set(m.read(['public']))
    wrongly_unscoped_deletions = {c['deletes'] for _, c in m.history if c['deletes']}
    wrong_deleted = set(m.read(['public'])) - wrongly_unscoped_deletions
    negative['private_deletion_suppresses_public'] = wrong_deleted != set(m.read(['public']))
    assert m.read(['public']) == history_oracle(m.history, ['public'], len(m.facts), 0)
    m.admit('public-delete', public_delete)
    assert 'old' not in m.read(['public'])
    x = Model(False)
    x.admit('old', old)
    x.admit('expired', candidate('expired', coordinate='profile', created=30, expires=50))
    assert set(x.read(['public'], now=49)) == {'expired'}
    assert x.read(['public'], now=50) == {}
    wrongly_expiry_filtered = [c for _, c in x.history if c['expires'] is None or c['expires'] > 50]
    wrong_filter_first = {max(wrongly_expiry_filtered, key=lambda c: c['created'])['event']}
    negative['expiry_before_winner_resurrects_old'] = set(x.read(['public'], now=50)) != wrong_filter_first
    assert x.read(['public'], now=50) == history_oracle(x.history, ['public'], len(x.facts), 50)
    assert all(negative.values()), negative
    return dict(scenario_runs=cases + 2, independent_history_comparisons=comparisons + 2,
                negative_controls=negative, negative_control_count=len(negative))


def check_acquisition_progress():
    """Bounded contract cases; these are not producer/NEG engine adapters."""
    negative = {}
    comparisons = 0

    def local_inventory(model, scope, edge, cut):
        facts = [f for f in model.facts if f['seq'] <= cut]
        if scope in model.restricted:
            return set()
        eligible = {f['event'] for f in facts
                    if f['kind'] == 'eligibility' and f['scope'] == scope}
        if model.provenance:
            witnessed = {f['event'] for f in facts if f['kind'] == 'edge'
                         and f['scope'] == scope and f['value']['edge'] == edge}
            eligible &= witnessed
        return {(f['value']['created'], f['event']) for f in facts
                if f['kind'] == 'body' and f['event'] in eligible}

    def inventory_history_oracle(history, scope, edge, cut, provenance, restricted):
        # Independent encounter reduction, with no fact/index/certify calls.
        return {(c['created'], c['event']) for committed, c in history
                if committed <= cut and c['valid'] and c['scope'] == scope
                and scope not in restricted and
                (not provenance or (c['origin'] == 'network' and c['edge'] == edge))}

    private = candidate(scope='private', received=10, admitted=10, sig='private')
    public = candidate(scope='public', received=20, admitted=20, sig='public')
    for provenance, order in itertools.product((False, True),
                                               ((private, public), (public, private))):
        m = Model(provenance)
        for i, c in enumerate(order):
            m.admit(str(i), c)
            for scope in ('private', 'public'):
                for cut in [0] + [position for position, _ in m.history]:
                    expected = inventory_history_oracle(m.history, scope, 'relay-1',
                                                        cut, provenance, m.restricted)
                    assert local_inventory(m, scope, 'relay-1', cut) == expected
                    comparisons += 1
            if i == 0:
                other = 'public' if c['scope'] == 'private' else 'private'
                remote = {(10, 'E')}
                assert remote - local_inventory(m, other, 'relay-1', len(m.facts)) == remote
                wrong_unscoped = {(f['value']['created'], f['event']) for f in m.facts
                                  if f['kind'] == 'body'}
                assert remote - wrong_unscoped != remote
                negative['private_witness_or_payload_suppresses_other_scope'] = True
        m.restricted.add('public')
        assert local_inventory(m, 'public', 'relay-1', len(m.facts)) == set()
        assert inventory_history_oracle(m.history, 'public', 'relay-1', len(m.facts),
                                        provenance, m.restricted) == set()
        comparisons += 1

    # Even public import eligibility must not launder an existing private witness.
    m = Model(True)
    m.admit('private', private)
    m.admit('public-import', dict(public, origin='import', received=None))
    assert local_inventory(m, 'public', 'relay-1', len(m.facts)) == set()
    eligible = {f['event'] for f in m.facts
                if f['kind'] == 'eligibility' and f['scope'] == 'public'}
    wrong_edges = {f['event'] for f in m.facts
                   if f['kind'] == 'edge' and f['value']['edge'] == 'relay-1'}
    negative['public_eligibility_launders_private_witness'] = eligible & wrong_edges == {'E'}
    assert m.certify(public) == 'NeedsAdmission'

    checkpoints = []
    for provenance in (False, True):
        m = Model(provenance)
        m.admit('seed', public)
        baseline = m.snapshot()
        # Eight deliveries exceed a declared mutation window of two. They need
        # no requests/ACKs; one bounded page checkpoint carries deduplicated basis.
        deliveries = [dict(public, received=30+i, admitted=30+i) for i in range(8)]
        proofs = [m.certify(c) for c in deliveries]
        assert all(isinstance(p, dict) for p in proofs) and m.snapshot() == baseline
        assert m.certify(deliveries[0]) == proofs[0]  # Lost read reply: re-evaluate.

        def checkpoint(bases, receipt_only=False):
            if receipt_only and any('receipt' not in basis for basis in bases):
                return None
            if not all(m.validate_restored_proof(p) for p in bases):
                return None
            return {'through_position': len(bases),
                    'dependencies': sorted({tuple(d) for p in bases for d in p['deps']})}

        result = checkpoint(proofs)
        # Seed has precisely body, eligibility, receive and optional edge. This
        # expected closure is enumerated separately from proof construction.
        expected_kinds = {'body', 'eligibility', 'receive'} | ({'edge'} if provenance else set())
        expected_dependencies = sorted((f['seq'], f['digest']) for f in m.facts
                                       if f['kind'] in expected_kinds)
        assert result == {'through_position': 8, 'dependencies': expected_dependencies}
        assert m.snapshot() == baseline and len(m.requests) == 1
        negative['receipt_only_checkpoint_blocks_certified_page'] = checkpoint(proofs, True) is None
        wrong = copy.deepcopy(m)
        for i, c in enumerate(deliveries):
            assert wrong.admit(f'duplicate-{i}', c) == 'AlreadyDurable'
        assert wrong.facts == m.facts
        negative['synthetic_mutation_fallback_writes_per_duplicate'] = len(wrong.requests) == len(m.requests) + 8
        # Restore from facts, without the seed's request receipt cache.
        restored = Model(provenance)
        restored.facts = copy.deepcopy(m.facts)
        assert all(restored.validate_restored_proof(p) for p in proofs)
        restored.facts = [f for f in restored.facts if f['kind'] != 'eligibility']
        negative['checkpoint_restore_missing_scope_dependency'] = not all(
            restored.validate_restored_proof(p) for p in proofs)
        m.restricted.add('public')
        negative['revoked_authority_advances_new_checkpoint'] = checkpoint(proofs) is None
        checkpoints.append({'provenance': provenance, 'delivered': 8,
                            'mutation_window': 2, 'new_mutation_requests': 0,
                            'new_facts': 0, 'logical_checkpoint_commits': 1,
                            'retained_dependencies': len(expected_dependencies)})
    assert all(negative.values()), negative
    return dict(inventory_history_comparisons=comparisons,
                proof_only_checkpoints=checkpoints, negative_controls=negative,
                negative_control_count=len(negative),
                limits='Symbolic signatures, in-memory facts and logical checkpoint assertions; no NIP-77 exchange, PG transaction, fsync, ACK adapter or physical restart executed.')


def check_allocation_and_compatibility():
    # Qualified identities are exact tuples; no hash or ordinal alone is an ID.
    dataset_a, dataset_b, domain_a, domain_b = (bytes([n]) * 16 for n in range(1, 5))
    source_a = (dataset_a, 4, domain_a, 1)
    source_b = (dataset_b, 4, domain_a, 1)
    source_c = (dataset_a, 4, domain_b, 1)
    inventory = {source_a: 'private', source_b: 'public', source_c: 'another-private'}
    assert len(inventory) == 3
    wrong_bare_ordinal = {reference[-1]: value for reference, value in inventory.items()}
    assert len(wrong_bare_ordinal) == 1
    imported_local = {source_a: (dataset_b, 4, domain_b, 2)}
    assert imported_local[source_a] != source_b  # Explicit remap, no ordinal equality grant.
    # Serialized allocation watermark survives restore and reserves a whole batch.
    def reserve(state, count):
        if count <= 0 or state['next'] + count - 1 > 2**64-1:
            raise OverflowError('batch does not fit')
        first = state['next']
        state['next'] += count
        return list(range(first, first + count))
    state = {'next': 1}
    assert reserve(state, 2) == [1, 2]
    restored = json.loads(json.dumps(state))
    assert reserve(restored, 1) == [3]
    exhausted = {'next': 2**64-1}
    before = dict(exhausted)
    try:
        reserve(exhausted, 2)
    except OverflowError:
        pass
    else:
        raise AssertionError('partial overflow allocation accepted')
    assert exhausted == before
    # Controlled handoff preserves all previous mappings; a fresh domain is distinct.
    handoff = dict(inventory)
    handoff[(dataset_a, 4, bytes([9])*16, 1)] = 'new-domain'
    assert all(handoff[k] == v for k, v in inventory.items())
    # Exact negotiated intersection before writes; no speculative peer downgrade.
    old_peer = {('fact', 1), ('admit', 1)}
    new_peer = old_peer | {('fact', 2)}
    writes = []
    def negotiate_and_write(operation):
        if operation not in old_peer & new_peer:
            return 'IncompatibleSchema'
        writes.append(operation)
        return 'Compatible'
    assert negotiate_and_write(('fact', 2)) == 'IncompatibleSchema' and not writes
    assert negotiate_and_write(('fact', 1)) == 'Compatible' and writes == [('fact', 1)]
    return dict(qualified_domain_alias_rejected=True, restore_no_reuse=True,
                atomic_overflow_refusal=True, handoff_preserves_old_mapping=True,
                incompatible_peer_refused_before_write=True)


def check_vectors():
    fixtures = json.loads((HERE / 'golden-vectors.json').read_text())
    node = json.loads(subprocess.check_output(['node', str(HERE / 'reference.mjs'),
                                             str(HERE / 'golden-vectors.json')], text=True))
    independent = {row['name']: row for row in node}
    for vector in fixtures['vectors']:
        raw = bytes.fromhex(vector['raw_hex']) if vector['mode'] == 'raw' else cbor(unpack(vector['value']))
        preimage = (f"bigbrotr/{vector['domain']}/1\0".encode() if vector['domain'] else b'') + raw
        assert preimage.hex() == vector['preimage_hex'], vector['name']
        assert hashlib.sha256(preimage).hexdigest() == vector['sha256'], vector['name']
        openssl = subprocess.check_output(['openssl', 'dgst', '-sha256', '-binary'], input=preimage)
        assert openssl.hex() == vector['sha256'], vector['name']
        assert independent[vector['name']]['preimage_hex'] == preimage.hex()
        assert independent[vector['name']]['sha256'] == vector['sha256']
    for value in (2**64, -(2**63)-1, 1.5, '\ud800'):
        try:
            cbor(value)
        except (ValueError, UnicodeError):
            pass
        else:
            raise AssertionError(f'invalid value accepted: {value!r}')
    exact_fields({'required': '', 'nullable': None}, {'required', 'nullable'})
    for value in ({'required': None}, {'required': '', 'nullable': None, 'extra': 0}, {}):
        try:
            exact_fields(value, {'required', 'nullable'})
        except ValueError:
            pass
        else:
            raise AssertionError('missing/unknown field accepted')
    negative = {
        'u64_via_float': int(float(2**64-1)) != 2**64-1,
        'null_equals_empty': cbor(None) != cbor('') != cbor([]),
        'sort_signed_tags': cbor([['z'], ['a'], ['a']]) != cbor([['a'], ['a'], ['z']]),
        'drop_repeated_tags': cbor([['a'], ['a']]) != cbor([['a']]),
        'little_endian': cbor(256) != bytes.fromhex('190001'),
        'domain_alias': fingerprint('body', []) != fingerprint('fact', []),
        'request_signature_omitted': fingerprint('producer-request', ['E','A']) != fingerprint('producer-request', ['E','B']),
    }
    assert all(negative.values())
    # CBOR example from RFC 8949 Appendix A, independently fixed literal bytes.
    assert cbor([1, [2, 3], [4, 5]]) == bytes.fromhex('8301820203820405')
    assert cbor(2**64-1) == bytes.fromhex('1bffffffffffffffff')
    return dict(golden_vectors=len(fixtures['vectors']), independent_encoders=['Python', 'Node.js'],
                digest_implementations=['Python hashlib', 'Node crypto', 'system OpenSSL'],
                negative_controls=negative)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    result = dict(status='PASS', scope='isolated encoding/reference-model evidence only',
                  product_implemented=False, engine_or_crash_test=False,
                  cryptographic_event_verification_test=False,
                  python=sys.version.split()[0], node=subprocess.check_output(['node', '--version'], text=True).strip(),
                  openssl=subprocess.check_output(['openssl', 'version'], text=True).strip(),
                  platform=platform.system() + ' ' + platform.machine(),
                  vectors=check_vectors(), model=check_model(),
                  acquisition_progress=check_acquisition_progress(),
                  identity_and_compatibility=check_allocation_and_compatibility(),
                  command='python3 experiments/identity-contract/probe.py --evidence docs/research/evidence/identity-contract.json',
                  sources={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in [Path(__file__), HERE/'reference.mjs', HERE/'golden-vectors.json', HERE/'write_vectors.py']},
                  limits=['Symbolic event signatures; invalid-input flag models an independent verifier boundary.',
                          'No filesystem sync, database, process fencing, benchmark or Tux workload.',
                          'Deterministic CBOR subset probe is research code, not a proposed product codec.',
                          'History oracle covers bounded scenarios; no exhaustive state-space proof.'])
    text = json.dumps(result, indent=2, sort_keys=True) + '\n'
    if args.evidence:
        args.evidence.write_text(text)
    print(text)


if __name__ == '__main__':
    main()
