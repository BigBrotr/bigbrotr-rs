"""Closed 61-admission/one-refusal schedule on the existing bounded relay."""
import hashlib
import json
import stat
import time

CORPUS_SHA256 = '9534ec146a38123c10d5927a212000284c7c2fcae4b74ab0c1956d4879600860'
PROFILE = dict(format='bigbrotr-installed-retained-requests/1')


def corpus(path):
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode) and metadata.st_size < 65536
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == CORPUS_SHA256
    value = json.loads(data)
    assert value['format'] == 'bigbrotr-retained-request-events/1'
    events = value['events']
    assert len(events) == len({event['id'] for event in events}) == 62
    assert all(event['kind'] == 1 and event['tags'] == [] for event in events)
    return events


class RetainedEvents:
    def __init__(self, directory):
        assert json.loads((directory/'retained-request-profile.json').read_text()) == PROFILE
        self.directory = directory
        self.events = corpus(directory/'retained-requests.json')
        assert self.events[0] == json.loads((directory/'event.jsonl').read_text())
        self.boundary_request = None
        self.proof_sent = False
        self.empty_rounds = 0

    def empty(self):
        if (self.directory/'commands-finished.json').exists() or self.empty_rounds == 100:
            return None
        self.empty_rounds += 1
        return 'empty', []

    def select(self, ident, number):
        if number <= 20:
            return 'admission', self.events[(number-1)*3:number*3]
        if number == 21:
            assert self.boundary_request is None
            self.boundary_request = ident
            return 'saturation', [self.events[60], self.events[61], self.events[0]]
        if self.proof_sent:
            return self.empty()
        path = self.directory/'retained-proof.continue'
        if not path.exists():
            return self.empty()
        assert stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= 512
        release = json.loads(path.read_text())
        assert release == dict(format='bigbrotr-retained-request-proof-release/1',
            boundary_request=self.boundary_request, classified_events=61,
            complete_through_request=64, checkpoint_revision=64)
        self.proof_sent = True  # Partial sends are not retried on another REQ.
        return 'proof-only', [self.events[0]]

    def describe(self, trace, selected):
        phase, values = selected
        trace.update(retained_phase=phase, frames=['signed']*len(values)+['EOSE'],
            event_ids=[value['id'] for value in values],
            event_json_sha256=[hashlib.sha256(json.dumps(value, ensure_ascii=False,
                separators=(',', ':')).encode()).hexdigest() for value in values],
            sent_at_us=time.time_ns()//1000)
        return values
