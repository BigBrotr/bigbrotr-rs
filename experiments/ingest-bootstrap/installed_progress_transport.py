"""Opt-in split EVENT selection; socket deadlines and framing stay with the peer.

The gate reuses the successor fixture format. Waiting never occurs on a relay
socket: unreleased REQs receive no frames and take the ordinary quiet timeout.
"""
import hashlib
import json
import stat
import time


class SplitEvents:
    def __init__(self, directory, event, kind):
        from installed_family_client import progress_release
        path = directory / 'progress-context.json'
        assert stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= 256
        assert json.loads(path.read_text()) == dict(format='bigbrotr-progress-context/1',kind=kind)
        self.read_release = progress_release
        self.directory, self.event = directory, event
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
        trace.update(progress_phase=phase, frames=['signed', 'EOSE'],
            event_ids=[self.event['id']], event_json_sha256=[hashlib.sha256(json.dumps(
                self.event, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()],
            sent_at_us=time.time_ns() // 1000)
        if release is not None:
            trace['progress_release'] = release
        return (self.event,)
