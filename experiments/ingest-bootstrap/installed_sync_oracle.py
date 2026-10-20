"""Independent installed Sync bytes; only identities and clock scalars are parameters.

No production serializer or observed payload supplies an expected field or key.
The minimal decoder only extracts unpredictable clock parameters, subsequently
bounded by the independent peer and compared inside a complete literal encoding.
"""
import hashlib
from bootstrap_oracle import cbor, expected as bootstrap
from installed import append_private_expected

ENDPOINT = 'ws://127.0.0.1:7447/installed-fixture'
SIGNED_SHA256 = 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'


def decode(data, maximum=65536, *, collection_items=1024, max_nodes=None):
    """Explicit larger fixture bounds leave all prior call defaults unchanged."""
    assert maximum in (65536, 262144, 524288) and len(data) <= maximum
    assert collection_items in (1024, 65536)
    # Every original CBOR item consumes at least one encoded byte. The default
    # therefore preserves the old accepted domain while making work finite.
    max_nodes = maximum if max_nodes is None else max_nodes
    assert type(max_nodes) is int and 0 < max_nodes <= maximum
    offset, nodes = 0, 0

    def item(depth=0):
        nonlocal offset, nodes
        assert depth <= 16 and offset < len(data)
        nodes += 1
        assert nodes <= max_nodes, 'fixture CBOR node bound'
        tag = data[offset]; offset += 1
        if tag == 246:
            return None
        major, size = tag >> 5, tag & 31
        if size >= 24:
            assert size in (24, 25, 26, 27)
            width = 1 << (size - 24)
            assert offset + width <= len(data)
            size = int.from_bytes(data[offset:offset+width], 'big'); offset += width
        if major == 0:
            return size
        if major in (2, 3):
            assert offset + size <= len(data)
            value = data[offset:offset+size]; offset += size
            return value if major == 2 else value.decode('utf8')
        assert size <= collection_items and size <= len(data) - offset, 'fixture CBOR collection bound'
        if major == 4:
            return [item(depth+1) for _ in range(size)]
        if major == 5:
            value = {}
            for _ in range(size):
                key = item(depth+1)
                assert key not in value
                value[key] = item(depth+1)
            return value
        raise AssertionError('unsupported fixture CBOR')

    value = item()
    assert offset == len(data) and cbor(value) == data, 'noncanonical fixture CBOR'
    return value


def expected(state, report, event, dictionary_time, received=None, recorded=None, resolution=1,
             *, expected_body_digest='1e05f420946c84c3ee8b178daf05dd564c9904c7ed18f7ab99874aec878337fb'):
    dataset = bytes.fromhex(state['datasets'][0]['dataset_id'][2:])
    owner, = [r for r in state['owner_epochs'] if r['role'] == 'ingest']
    domains = {r['dictionary_kind']: bytes.fromhex(r['allocation_domain'][2:])
               for r in state['allocation_domains']}
    assert sorted(domains) == list(range(1, 10)) and len(set(domains.values())) == 9
    assert report['owner_resource_id'] == owner['resource_id'][2:]
    slot, recovery = bytes.fromhex(report['producer_slot_id']), bytes.fromhex(report['recovery_generation'])
    topology = int(report['topology_generation'])
    be = lambda n, width=8: n.to_bytes(width, 'big')
    sha = lambda value: hashlib.sha256(value).digest()
    ref = lambda kind, n=1: [dataset, kind, domains[kind], n]
    ref_key = lambda kind, n=1: dataset + be(kind, 2) + domains[kind] + be(n)
    src = lambda n: [dataset, 0, 1, n]
    src_key = lambda n: dataset + be(0, 4) + be(1) + be(n)
    scope = ref(4, 3)
    public = dict(dataset_id=dataset.hex(), dictionary_kind=4, allocation_domain=domains[4].hex(), ordinal='1')
    assert report['public_scope'] == public and report['private_scope'] == dict(public, ordinal='3')
    assert report['dictionary_count'] == 3
    rows, first_receipt, _ = bootstrap(dataset, True, domain=domains[4], slot=slot,
        owner=bytes.fromhex(owner['resource_id'][2:]), incarnation=bytes.fromhex(owner['process_incarnation'][2:]),
        recovery=recovery, topology=topology, namespaces=sorted(domains.items()), recorded_at=owner['acquired_at_us'])
    entries = [['dictionary', 1, [ref(4, n), 1, 'visibility', 1, {'class': label}]]
               for n, label in [(1, 'public'), (2, 'private')]]
    first_input = b'bigbrotr/producer-request/1\0' + cbor([[slot, 1, 1], 'admit-evidence', 1, [1, ref(4), entries]])
    rows, private_input, private_receipt = append_private_expected(rows, dataset, domains[4], slot,
        recovery, owner['acquired_at_us'], topology)
    inputs, receipts = [first_input, private_input], [first_receipt, private_receipt]
    rows = {(cf, k): v for cf, k, v in rows}
    # References below come only from independently constructed bootstrap bytes.
    refs = {int.from_bytes(k[-8:], 'big'): [src(int.from_bytes(k[-8:], 'big')), v[2:34]]
            for (cf, k), v in rows.items() if cf == 'facts' and k[:2] == b'\x01\x20'}

    def fact(n, family, payload, at):
        encoded = b'bigbrotr/fact/1\0' + cbor([src(n), family, 1, scope, at, payload])
        refs[n] = [src(n), sha(encoded)]
        rows['facts', b'\x01\x20'+src_key(n)] = b'\x01\x20'+sha(encoded)+encoded

    def index(domain, key, n):
        prefix = bytes([1, domain])
        rows['default', prefix+key] = prefix+src_key(n)+refs[n][1]

    def dictionary(n, kind, schema, value, at):
        descriptor = [1, schema, 1, value]
        mapping = [ref(kind), *descriptor]
        fact(n, 'dictionary', mapping, at)
        index(4, ref_key(kind), n)
        canonical = cbor(descriptor)
        rows['default', b'\x01\x05'+dataset+be(kind, 2)+sha(canonical)+ref_key(kind)] = b'\x01\x05'+canonical
        rows['default', b'\x01\x03'+dataset+be(kind, 2)+domains[kind]] = b'\x01\x03'+be(1)
        return ['dictionary', 1, mapping]

    def commit(sequence, first, last, original, dependencies):
        batch = [refs[n] for n in range(first, last+1)]
        receipt = b'bigbrotr/receipt/1\0'+cbor([1, [slot, 1, sequence], sha(original),
            [dataset, topology, 1, recovery, 1], batch, [refs[n] for n in dependencies]])
        rows['default', b'\x01\x02'+dataset+be(0, 4)+be(1)] = b'\x01\x02'+be(last)
        rows['facts', b'\x01\x21'+src_key(first)] = b'\x01\x21'+be(last)+sha(
            b'bigbrotr/admission-batch/1\0'+cbor([dataset, 0, 1, first, last, batch]))
        rows['receipts', b'\x01\x30'+slot+be(1)+be(sequence)] = receipt
        rows['receipts', b'\x01\x31'+slot+be(1)] = b'\x01\x31'+be(sequence)+be(0)+be(0)
        inputs.append(original); receipts.append(receipt)

    definitions = [(2, 'observer', dict(clock='system-utc', vantage=None, software=None,
            method_id=None, public_key=None, route_class=None, software_version=None)),
        (1, 'endpoint', dict(url=ENDPOINT)),
        (3, 'access-scope', dict(class_='public', label=None, visibility_scope_id=scope)),
        (5, 'event-verification', dict(input='nip01-event/1', algorithm='sha256-bip340',
            capability='event-id-and-signature', output_unit='verification-result',
            failure_taxonomy='bigbrotr-nip01-verifier/1', algorithm_version=1,
            protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955'))]
    definitions[2][2]['class'] = definitions[2][2].pop('class_')
    entries = [dictionary(n, kind, schema, value, dictionary_time)
               for n, (kind, schema, value) in enumerate(definitions, 4)]
    original = b'bigbrotr/producer-request/1\0'+cbor([[slot, 1, 3], 'admit-evidence', 1, [1, scope, entries]])
    commit(3, 4, 7, original, range(3, 8))
    if received is not None:
        assert recorded is not None
        body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
                event['kind'], event['tags'], event['content']]
        signature = bytes.fromhex(event['sig'])
        body_digest = sha(b'bigbrotr/body/1\0'+cbor(body))
        assert body_digest.hex() == expected_body_digest
        dictionary(8, 8, 'event', dict(event_id=body[0]), recorded)
        fact(9, 'event-payload', [*body, ref(8)], recorded)
        index(6, dataset+body[0], 9)
        fact(10, 'event-eligibility', [body[0], body_digest, signature, recorded, 'network', None], recorded)
        index(7, dataset+body[0]+ref_key(4, 3), 10)
        clock = [ref(2), resolution, None, 4 if resolution > 1 else 0]
        fact(11, 'event-receive', [body[0], received, ref(2), clock, None], recorded)
        index(8, dataset+body[0]+ref_key(4, 3), 11)
        fact(12, 'provenance', [body[0], ref(1), ref(2), ref(3), received, clock, ref(5), None], recorded)
        index(9, dataset+body[0]+ref_key(1)+ref_key(2)+ref_key(3)+ref_key(4, 3), 12)
        original = b'bigbrotr/producer-request/1\0'+cbor([[slot, 1, 4], 'admit', 1,
            [1, scope, [*body, signature], 'network', None, [received, ref(2), clock, [ref(1), ref(3), ref(5)]]]])
        commit(4, 8, 12, original, range(3, 13))
    assert len(rows) == (49 if received is not None else 35)
    return sorted((cf, k, v) for (cf, k), v in rows.items()), inputs, receipts
