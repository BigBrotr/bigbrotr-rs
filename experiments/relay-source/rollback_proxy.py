#!/usr/bin/env python3
"""Hold one exact rollback reply in a finite, owned plaintext PG laboratory.

Reuse the existing bounded framing, startup, cancellation and control channel.
The hold occurs after PostgreSQL completed ROLLBACK, before its reply reaches
Control. No SQL payload or credential enters the metadata transcript.
"""
import argparse
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'control-migrations'))
from fault_proxy import Session, bounded_int


class RollbackSession(Session):
    def __init__(self, args):
        super().__init__(args)
        self.requests = self.completions = self.released = self.ready = 0
        self.pending_ready = False

    def emit(self, event, **fields):
        super().emit(event, rollback_requests=self.requests,
            rollback_completions=self.completions, rollback_released=self.released,
            rollback_ready=self.ready, rollback_number=self.args.rollback_number,
            before_forward=self.args.before_forward, **fields)

    async def read_frame(self, reader, direction):
        message = await super().read_frame(reader, direction)
        if message is None:
            return None
        tag, _, body = message
        if direction == 'frontend' and tag == b'Q' and body == b'ROLLBACK\0':
            self.requests += 1
            if self.args.before_forward and self.requests == self.args.rollback_number:
                self.faults += 1
                self.held = True
                self.emit('held')
                await self.hold_gate
        if direction == 'backend' and tag == b'C' and body == b'ROLLBACK\0':
            self.completions += 1
            if not self.args.before_forward and self.completions == self.args.rollback_number:
                self.faults += 1
                self.held = True
                self.emit('held')
                await self.hold_gate
            self.released += 1
            self.pending_ready = True
        if direction == 'backend' and tag == b'Z' and self.pending_ready:
            self.ready += 1
            self.pending_ready = False
        return message


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream-port', type=bounded_int(1, 65535), required=True)
    parser.add_argument('--listen-port', type=bounded_int(0, 65535), default=6543)
    # Zero observes a positive baseline without a hold.
    parser.add_argument('--rollback-number', type=bounded_int(0, 64), required=True)
    parser.add_argument('--before-forward', action='store_true')
    args = parser.parse_args()
    args.mode, args.commit_number, args.later_commits = 'pass', 64, 0
    args.deadline_seconds = 60
    args.max_frame_bytes, args.max_wire_bytes, args.max_messages = 1024**2, 64*1024**2, 20000
    async def run():
        return await RollbackSession(args).serve()
    return asyncio.run(run())


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (BrokenPipeError, KeyboardInterrupt):
        sys.exit(3)
    except Exception:
        print('error: rollback fixture ended without receipt', file=sys.stderr)
        sys.exit(3)
