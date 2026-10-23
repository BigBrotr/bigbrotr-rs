#!/usr/bin/env python3
"""Hold one initial-gap boundary in the owned loopback PostgreSQL fixture.

Defaults preserve the original gap's twelfth transaction and second rollback.
Explicit selectors support another qualified caller; observe never holds. Before:
the selected transaction has not reached PostgreSQL. After: the selected ROLLBACK
completed on PostgreSQL after the synchronized native batch,
but its reply has not reached the caller. Explicit close loses delivery. Reuse
the bounded single-session framing/cleanup and existing rollback selector.
"""
import argparse
import asyncio
import hashlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'relay-source'))
import rollback_proxy
from rollback_proxy import RollbackSession, bounded_int

START = b'START TRANSACTION ISOLATION LEVEL READ COMMITTED\0'


class GapSession(RollbackSession):
    def __init__(self, args):
        super().__init__(args)
        self.starts = 0

    def emit(self, event, **fields):
        super().emit(event, transaction_starts=self.starts,
            gap_boundary=self.args.boundary,
            gap_proxy_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            rollback_proxy_sha256=hashlib.sha256(Path(rollback_proxy.__file__).read_bytes()).hexdigest(),
            **fields)

    async def read_frame(self, reader, direction):
        message = await super().read_frame(reader, direction)
        if message is not None:
            tag, _, body = message
            if direction == 'frontend' and tag == b'Q' and body == START:
                self.starts += 1
                if self.args.boundary == 'before' and self.starts == self.args.start_number:
                    self.faults += 1
                    self.held = True
                    self.emit('held')
                    await self.hold_gate
        return message


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream-port', type=bounded_int(1, 65535), required=True)
    parser.add_argument('--listen-port', type=bounded_int(0, 65535), default=0)
    parser.add_argument('--boundary', choices=('before', 'after', 'observe'), required=True)
    parser.add_argument('--start-number', type=bounded_int(1, 64), default=12)
    parser.add_argument('--rollback-number', type=bounded_int(1, 64), default=2)
    args = parser.parse_args()
    args.rollback_number = args.rollback_number if args.boundary == 'after' else 0
    args.before_forward = False
    args.mode, args.commit_number, args.later_commits = 'pass', 64, 0
    args.deadline_seconds = 30
    args.max_frame_bytes, args.max_wire_bytes, args.max_messages = 1024**2, 64*1024**2, 20000

    async def run():
        return await GapSession(args).serve()
    return asyncio.run(run())


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (BrokenPipeError, KeyboardInterrupt):
        sys.exit(3)
    except Exception:
        print('error: gap proxy ended without receipt', file=sys.stderr)
        sys.exit(3)
