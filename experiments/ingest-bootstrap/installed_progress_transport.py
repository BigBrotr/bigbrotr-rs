"""Opt-in split EVENT selection; socket deadlines and framing stay with the peer.

The gate reuses the successor fixture format. Waiting never occurs on a relay
socket: unreleased REQs receive no frames and take the ordinary quiet timeout.
"""
import hashlib
import json
import stat
import time

# Frozen NEW_EVENT, selector included and selector wrong-author. The actual
# fixture selector is kinds:[1]; the latter name belongs to another selector.
THREE_EVENTS = (
    ('57b20f44c0095dfc789765ed38caa8dd54ae37635dab32237c563146c8865b02',
     'd39a55d2ffe08c235a784c3f911416269469fbbab53a423ac0a8eee2f74e4086'),
    ('32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791',
     'f73b7f7847fb282bea496e79fa87a30e5bf6248a3e7cf3cec5c635a4feffcdc3'),
    ('3ac1dd47ad27c66939e5ed24e2002e71fcfac11c63254b622b3d54ea8491ce58',
     '38ca405dd59ae379c607fe64f4a9c07689f67ae604768a5974718e855a852c17'),
)

SUCCESSOR_EVENTS=(THREE_EVENTS[0],('7815649941b6434d7c525bb633ff461fb7ee341954f1bcb0bce2c9535c2bb7f7','228da010e18049c96e25f37ccb37c0704c174756d2110306a2734492270807ac'),THREE_EVENTS[2])

def three_events(events, *, successor=False):
    """Closed transport inputs only; no signature or checkpoint qualification."""
    assert isinstance(events, (tuple, list)) and len(events)==3
    assert type(successor) is bool
    for event, (ident, digest) in zip(events, SUCCESSOR_EVENTS if successor else THREE_EVENTS):
        assert set(event)=={'id','pubkey','created_at','kind','tags','content','sig'}
        assert event['id']==ident and event['kind']==1
        raw=json.dumps(event,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()
        assert len(raw)<=1024 and hashlib.sha256(raw).hexdigest()==digest, 'frozen three-event payload'
    # Own the selected bytes before any send: the caller cannot change phase2.
    return tuple(json.loads(json.dumps(event)) for event in events)


def ack_events(directory, *, successor=False):
    """Read an explicit finite transport recipe; never infer ACK authority."""
    path = directory / 'progress-ack-events.json'
    if not path.exists():
        return None
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and metadata.st_size <= 4096
    assert stat.S_IMODE(metadata.st_mode) & 0o077 == 0
    def closed(pairs):
        value = {}
        for key, item in pairs:
            assert key not in value, 'duplicate ACK transport key'
            value[key] = item
        return value
    value = json.loads(path.read_text(), object_pairs_hook=closed)
    assert set(value) == {'format', 'events'} and value['format'] == 'bigbrotr-ack-events/1'
    return three_events(value['events'],successor=successor)


class SplitEvents:
    def __init__(self, directory, event, kind, *, event_sequence=None):
        from installed_family_client import progress_release
        path = directory / 'progress-context.json'
        assert stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= 256
        assert json.loads(path.read_text()) == dict(
            format='bigbrotr-progress-context/1' if event_sequence is None else 'bigbrotr-ack-context/1',kind=kind)
        self.read_release = progress_release
        self.directory, self.event = directory, event
        self.event_sequence = None if event_sequence is None else three_events(event_sequence)
        self.mutation, self.proof_sent = None, False

    def select(self, ident, number):
        if number == 1:
            assert self.mutation is None
            self.mutation = ident
            return 'mutation', None
        if self.proof_sent:
            return None
        release = self.read_release(self.directory, self.mutation)
        if release is None:
            return None
        self.proof_sent = True  # A partial send is never replayed on another REQ.
        return 'proof-only', release

    def describe(self, trace, selected):
        phase, release = selected
        if self.event_sequence is not None:
            values=three_events(self.event_sequence)
            trace.update(progress_phase=phase, frames=['signed']*3+['EOSE'],
                event_ids=[value['id'] for value in values], event_json_sha256=[hashlib.sha256(json.dumps(
                    value,ensure_ascii=False,separators=(',',':')).encode()).hexdigest() for value in values],
                sent_at_us=time.time_ns()//1000)
            if release is not None:
                trace['progress_release']=release
            return values
        trace.update(progress_phase=phase, frames=['signed', 'EOSE'],
            event_ids=[self.event['id']], event_json_sha256=[hashlib.sha256(json.dumps(
                self.event, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()],
            sent_at_us=time.time_ns() // 1000)
        if release is not None:
            trace['progress_release'] = release
        return (self.event,)
