"""Expected original installed batches and full scientific prefix, independent of publisher.

Identities/clock scalars have the same externally checked parameter boundaries as
installed_sync_oracle. No captured frontier, attempt or worker output generates
expected facts. Attempt receipts and snapshot certification need separate checks.
"""
import hashlib
from bootstrap_oracle import cbor
from installed_sync_oracle import decode, expected as native_expected

TABLES = ('dictionaries', 'events', 'event_eligibility_facts', 'event_receive_facts', 'provenance_facts')
DESCRIPTOR_FIELDS = ('event_id class url clock vantage software method_id public_key route_class '
    'software_version label visibility_scope_id input algorithm capability output_unit '
    'failure_taxonomy algorithm_version protocol_revision format format_version '
    'source_dataset_id source_artifact_digest').split()


def reference(value):
    dataset, kind, domain, ordinal = value
    return dict(dataset_id=dataset.hex().upper(), dictionary_kind=kind,
        allocation_domain=domain.hex().upper(), ordinal=ordinal)


def source(value):
    dataset, shard, epoch, sequence = value
    return dict(dataset_id=dataset.hex().upper(), shard_id=shard, owner_epoch=epoch, sequence=sequence)


def clock(value):
    identity, resolution, uncertainty, flags = value
    return dict(clock_id=reference(identity), resolution_us=resolution, uncertainty_us=uncertainty, anomaly_flags=flags)


def expected(state, report, event, dictionary_time, received, recorded, resolution=1,
             *, native_options=None, fact_bytes=65536, decoder_options=None):
    assert received is not None and recorded is not None
    provenance = (native_options or {}).get('provenance', True)
    native, inputs, receipts = native_expected(state, report, event, dictionary_time, received, recorded,
        resolution, **(native_options or {}))
    assert len(native) == (49 if provenance else 35) and len(inputs) == len(receipts) == 4
    facts = []
    tables = {name: [] for name in TABLES}
    for cf, key, value in native:
        if cf != 'facts' or key[:2] != b'\x01\x20':
            continue
        preimage = value[34:]
        assert preimage.startswith(b'bigbrotr/fact/1\0')
        fact_digest = hashlib.sha256(preimage).digest()
        assert value[:34] == b'\x01\x20' + fact_digest
        identity, family, version, visibility, at, payload = decode(
            preimage[len(b'bigbrotr/fact/1\0'):], maximum=fact_bytes, **(decoder_options or {}))
        facts.append(dict(sequence=identity[3], digest=fact_digest.hex(), preimage_hex=preimage.hex()))
        row = dict(source_record_id=source(identity), record_schema_version=version,
            record_kind=family, visibility_scope_id=reference(visibility), payload_digest=fact_digest.hex().upper(), recorded_at=at)
        if family == 'dictionary':
            ref, normalization, schema, descriptor_version, fields = payload
            descriptor = dict.fromkeys(DESCRIPTOR_FIELDS)
            for name, entry in fields.items():
                assert name in descriptor
                if name in ('method_id', 'visibility_scope_id') and entry is not None:
                    entry = reference(entry)
                elif isinstance(entry, bytes):
                    entry = entry.hex().upper()
                descriptor[name] = entry
            row.update(reference=reference(ref), normalization_version=normalization,
                descriptor_schema=schema, descriptor_version=descriptor_version, descriptor_value=descriptor)
            table = 'dictionaries'
        elif family == 'event-payload':
            event_id, pubkey, created, kind, tags, content, ordinal = payload
            row.update(event_id=event_id.hex().upper(), pubkey=pubkey.hex().upper(), created_at=created,
                kind=kind, tags=tags, content=content, event_ordinal=reference(ordinal))
            table = 'events'
        elif family == 'event-eligibility':
            event_id, body_digest, signature, eligible, origin, lineage = payload
            assert lineage is None
            row.update(event_id=event_id.hex().upper(), body_digest=body_digest.hex().upper(),
                sig=signature.hex().upper(), first_eligible_at=eligible, origin=origin, import_lineage=None)
            table = 'event_eligibility_facts'
        elif family == 'event-receive':
            event_id, at, observer, quality, corrects = payload
            assert corrects is None
            row.update(event_id=event_id.hex().upper(), received_at=at, observer_id=reference(observer),
                clock_quality=clock(quality), corrects=None)
            table = 'event_receive_facts'
        else:
            assert family == 'provenance'
            event_id, endpoint, observer, access, at, quality, verification, corrects = payload
            assert corrects is None
            row.update(event_id=event_id.hex().upper(), endpoint_id=reference(endpoint),
                observer_id=reference(observer), access_scope_id=reference(access), received_at=at,
                clock_quality=clock(quality), verification_method_id=reference(verification), corrects=None)
            table = 'provenance_facts'
        tables[table].append(row)
    assert [f['sequence'] for f in facts] == list(range(1, 13 if provenance else 9))
    assert {t: len(v) for t, v in tables.items()} == dict(zip(TABLES,
        (8, 1, 1, 1, 1) if provenance else (5, 1, 1, 1, 0)))
    dataset = bytes.fromhex(state['datasets'][0]['dataset_id'][2:])
    topology = int(report['topology_generation'])
    batches = []
    ranges = ((1, 2), (3, 3), (4, 7), (8, 12)) if provenance else ((1, 2), (3, 3), (4, 4), (5, 8))
    for first, last in ranges:
        selected = facts[first-1:last]
        refs = [[[dataset, 0, 1, r['sequence']], bytes.fromhex(r['digest'])] for r in selected]
        admission = hashlib.sha256(b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, 1, first, last, refs])).hexdigest()
        archive = hashlib.sha256(b'bigbrotr/archive-batch/1\0' + cbor([dataset, 1, topology, [[0, 1, first, last]], refs])).hexdigest()
        batches.append(dict(first=first, last=last, facts=selected, admission_digest=admission, archive_digest=archive))
    return dict(native=native, inputs=inputs, receipts=receipts, batches=batches, tables=tables)
