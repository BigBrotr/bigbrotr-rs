"""Bind literal initial mutation coordinates to the existing independent encoder."""
import hashlib

from cases import first_launch_restart as source


def initial_science(state, report, events, wires, dump, inputs, completed, *,
                    mutations, first_accepted_at_us, native_sequence):
    """Coordinates/sequence are declared by the fixture, never read from SQL.

    Each mutation tuple is (event index, request number, wire index, event-record
    sequence). Three independently encoded setup mutations precede these bodies.
    Only bounded recorded/receive clocks are taken from actual observations.
    """
    from installed_sync_oracle import expected, decode
    from bootstrap_oracle import cbor
    assert [item[1] for item in mutations] == list(range(4, 4 + len(mutations)))
    assert len(inputs) == 3 + len(mutations)
    assert type(native_sequence) is int and native_sequence > 0
    assert dump['event'] == 'dump-state' and dump['sequence'] == native_sequence
    actual = [(row['cf'], bytes.fromhex(row['key']), bytes.fromhex(row['value'])) for row in dump['rows']]
    facts = {int.from_bytes(key[-8:], 'big'): value for cf, key, value in actual
             if cf == 'facts' and key[:2] == b'\x01\x20'}
    recorded = lambda number: decode(facts[number][34:].split(b'\0', 1)[1])[4]
    dictionary = source.clock(recorded(4), first_accepted_at_us, wires[0]['connected_at_us'])
    parameters = []
    for index, request, wire_number, sequence in mutations:
        event, wire = events[index], wires[wire_number]
        received = decode(inputs[request - 1].split(b'\0', 1)[1])[3][5][0]
        source.clock(received, wire['sent_at_us'], wire['closed_at_us'])
        at = source.clock(recorded(sequence), received, completed)
        body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
                1, event['tags'], event['content']]
        digest = hashlib.sha256(b'bigbrotr/body/1\0' + cbor(body)).hexdigest()
        parameters.append((event, received, at, wire['clock_resolution_us'], digest))
    first = parameters[0]
    rows, wanted_inputs, receipts = expected(state, report, first[0], dictionary,
        first[1], first[2], first[3], expected_body_digest=first[4],
        additional_events=parameters[1:], acknowledge=True)
    assert actual == rows, 'complete independently encoded native rows'
    assert inputs == wanted_inputs, 'all independently encoded original inputs'
    return dict(native=rows, inputs=wanted_inputs, receipts=receipts,
                manifest=next(value for cf, key, value in rows if cf == 'default' and key == b'\x01\x01'))
