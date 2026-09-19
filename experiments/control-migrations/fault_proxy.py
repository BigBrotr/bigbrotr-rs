#!/usr/bin/env python3
"""One-session, loopback-only PostgreSQL plaintext fault injector for owned labs."""
import argparse
import asyncio
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import struct
import sys
import time

STARTUP_MAX = 64 * 1024
STREAM_LIMIT = 16 * 1024
CONTROL_MAX = 32
CONTROL_COUNT_MAX = 32


class Refused(Exception):
    """Only fixed, non-sensitive reason codes may enter this exception."""


def is_commit(body):
    # Deliberately not a SQL parser. The selected driver sends exactly COMMIT.
    if not body.endswith(b"\x00") or len(body) > 128:
        return False
    query = body[:-1].strip(b" \t\n\r\f\v")
    if query.endswith(b";"):
        query = query[:-1].rstrip(b" \t\n\r\f\v")
    return query.upper() == b"COMMIT"


class Session:
    def __init__(self, args):
        self.args = args
        self.started_ns = time.monotonic_ns()
        self.stop = asyncio.get_running_loop().create_future()
        self.hold_gate = asyncio.get_running_loop().create_future()
        self.held = False
        self.hold_action = None
        self.pending_commit = False
        self.commits_seen = 0
        self.commits_forwarded = 0
        self.server_commit_complete = 0
        self.client_commit_complete = 0
        self.faults = 0
        self.frames = Counter()
        self.tags = {"frontend": Counter(), "backend": Counter()}
        self.declared_bytes = 0
        self.writers = []

    def emit(self, event, **fields):
        # No payload, startup parameters, backend key or exception text enters
        # this allowlisted metadata. Consumers must continuously drain stdout.
        item = {
            "format": "bigbrotr-pg-fault-proxy/1", "event": event,
            "elapsed_ns": time.monotonic_ns() - self.started_ns,
            "mode": self.args.mode, "commit_number": self.args.commit_number, "held": self.held,
            "hold_action": self.hold_action, "faults": self.faults,
            "commits_seen": self.commits_seen,
            "commits_forwarded": self.commits_forwarded,
            "server_commit_complete": self.server_commit_complete,
            "client_commit_complete": self.client_commit_complete,
        }
        item.update(fields)
        print(json.dumps(item, sort_keys=True), flush=True)

    def request_stop(self, reason):
        if not self.stop.done():
            self.stop.set_result(reason)

    def charge(self, length):
        if self.declared_bytes + length > self.args.max_wire_bytes:
            raise Refused("wire-budget")
        self.declared_bytes += length

    async def exact(self, reader, size):
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError:
            # Never render the exception: its partial bytes can contain secrets.
            raise Refused("truncated-frame") from None

    async def read_frame(self, reader, direction):
        tag = await reader.read(1)
        if not tag:
            return None
        header = await self.exact(reader, 4)
        size = struct.unpack("!I", header)[0]
        if not 4 <= size <= self.args.max_frame_bytes:
            raise Refused("frame-length")
        if sum(self.frames.values()) >= self.args.max_messages:
            raise Refused("message-budget")
        self.charge(size + 1)
        self.frames[direction] += 1
        self.tags[direction][tag.hex()] += 1
        return tag, header, await self.exact(reader, size - 4)

    async def pump(self, reader, writer, direction):
        while True:
            message = await self.read_frame(reader, direction)
            if message is None:
                return direction + "-eof"
            tag, header, body = message
            if direction == "frontend" and tag == b"Q" and is_commit(body):
                self.commits_seen += 1
                if self.commits_seen > self.args.commit_number:
                    raise Refused("multiple-commits")
                if self.commits_seen == self.args.commit_number and self.args.mode == "before-forward":
                    self.faults += 1
                    self.emit("fault", reason="before-forward")
                    return "before-forward"
                if self.commits_seen == self.args.commit_number and self.args.mode == "hold":
                    self.faults += 1
                    self.held = True
                    self.emit("held")
                    await self.hold_gate
                    # Only an explicit release completes this future. Close,
                    # stdin failure and deadline cancel without forwarding.
                self.pending_commit = self.commits_seen == self.args.commit_number
                self.commits_forwarded += 1
            if direction == "backend" and tag == b"C" and body == b"COMMIT\x00":
                self.server_commit_complete += 1
                matched = self.pending_commit
                self.pending_commit = False
                if matched and self.args.mode == "after-complete":
                    self.faults += 1
                    self.emit("fault", reason="after-complete")
                    return "after-complete"
                self.client_commit_complete += 1
            writer.write(tag + header + body)
            await writer.drain()

    async def controls(self, reader):
        try:
            for _ in range(CONTROL_COUNT_MAX):
                try:
                    line = await reader.readline()
                except ValueError:
                    raise Refused("control-length") from None
                if not line:
                    return
                if len(line) > CONTROL_MAX:
                    raise Refused("control-length")
                if line == b"status\n":
                    self.emit("status")
                elif line == b"release\n" and self.held:
                    self.held = False
                    self.hold_action = "release"
                    self.hold_gate.set_result(None)
                    self.emit("released")
                elif line == b"close\n":
                    self.hold_action = "close" if self.held else None
                    self.request_stop("held-close" if self.held else "control-close")
                    return
                else:
                    raise Refused("control-command")
            raise Refused("control-budget")
        except Refused as error:
            self.request_stop(str(error))
        except (OSError, ConnectionError):
            self.request_stop("control-io")

    async def run_connection(self, accepted):
        reader, writer = await accepted
        header = await self.exact(reader, 4)
        size = struct.unpack("!I", header)[0]
        if not 8 <= size <= STARTUP_MAX:
            raise Refused("startup-length")
        self.charge(size)
        startup = await self.exact(reader, size - 4)
        if startup[:4] != b"\x00\x03\x00\x00":
            raise Refused("startup-protocol")
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", self.args.upstream_port, limit=STREAM_LIMIT), 5
        )
        self.writers.append(upstream_writer)
        upstream_writer.write(header + startup)
        await upstream_writer.drain()
        pumps = [asyncio.create_task(self.pump(reader, upstream_writer, "frontend")),
                 asyncio.create_task(self.pump(upstream_reader, writer, "backend"))]
        try:
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            # Deterministic inspection retrieves exceptions from both tasks if
            # both ended on the same turn; all task outcomes are joined below.
            reasons = [task.result() for task in pumps if task in done]
            return next((r for r in reasons if r in ("before-forward", "after-complete")), reasons[0])
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)

    async def serve(self):
        loop = asyncio.get_running_loop()
        accepted = loop.create_future()
        listener = None
        control_transport = None
        tasks = []
        reason = "internal-error"

        def accept(reader, writer):
            if accepted.done():
                writer.close()
                return
            self.writers.append(writer)
            accepted.set_result((reader, writer))
            if listener:
                listener.close()

        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, self.request_stop, "terminated")
        try:
            control_reader = asyncio.StreamReader(limit=CONTROL_MAX)
            try:
                control_transport, _ = await loop.connect_read_pipe(
                    lambda: asyncio.StreamReaderProtocol(control_reader), sys.stdin.buffer
                )
                tasks.append(asyncio.create_task(self.controls(control_reader)))
            except (OSError, ValueError):
                if self.args.mode == "hold":
                    raise Refused("control-unavailable") from None
            listener = await asyncio.start_server(
                accept, "127.0.0.1", self.args.listen_port, limit=STREAM_LIMIT, backlog=1
            )
            port = listener.sockets[0].getsockname()[1]
            self.emit("ready", listen_host="127.0.0.1", listen_port=port,
                      upstream_port=self.args.upstream_port, pid=os.getpid(),
                      proxy_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      max_frame_bytes=self.args.max_frame_bytes,
                      max_wire_bytes=self.args.max_wire_bytes,
                      max_messages=self.args.max_messages,
                      deadline_seconds=self.args.deadline_seconds)
            connection = asyncio.create_task(self.run_connection(accepted))
            tasks.append(connection)
            done, _ = await asyncio.wait_for(
                asyncio.wait([connection, self.stop], return_when=asyncio.FIRST_COMPLETED),
                self.args.deadline_seconds,
            )
            reason = self.stop.result() if self.stop in done else connection.result()
        except asyncio.TimeoutError:
            reason = "deadline"
        except Refused as error:
            reason = str(error)
        except (OSError, ConnectionError):
            reason = "connection-io"
        finally:
            if listener:
                listener.close()
                await listener.wait_closed()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for writer in self.writers:
                writer.close()
            async def drain():
                await asyncio.gather(*(writer.wait_closed() for writer in self.writers),
                                     return_exceptions=True)
            try:
                await asyncio.wait_for(drain(), 2)
            except asyncio.TimeoutError:
                reason = "close-deadline"
            if control_transport:
                control_transport.close()
            for signum in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(signum)
        self.emit("receipt", reason=reason, frames=dict(self.frames),
                  tags={key: dict(value) for key, value in self.tags.items()},
                  declared_wire_bytes=self.declared_bytes)
        injected = reason in ("before-forward", "after-complete", "held-close")
        completed = reason in ("frontend-eof", "backend-eof")
        expected = self.args.mode == "pass" or (self.args.mode == "hold" and self.hold_action == "release")
        return 0 if injected or (completed and expected) else 3


def bounded_int(low, high):
    def parse(value):
        try:
            number = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError("expected a bounded integer") from None
        if not low <= number <= high:
            raise argparse.ArgumentTypeError("integer outside allowed range")
        return number
    return parse


def deadline(value):
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a bounded deadline") from None
    if not math.isfinite(number) or not 0.1 <= number <= 600:
        raise argparse.ArgumentTypeError("deadline must be between 0.1 and 600 seconds")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream-port", type=bounded_int(1, 65535), required=True)
    parser.add_argument("--listen-port", type=bounded_int(0, 65535), default=0)
    parser.add_argument("--mode", choices=("pass", "before-forward", "after-complete", "hold"), required=True)
    parser.add_argument("--commit-number", type=bounded_int(1, 64), default=1)
    parser.add_argument("--deadline-seconds", type=deadline, default=120)
    parser.add_argument("--max-frame-bytes", type=bounded_int(8, 16 * 1024 * 1024), default=1024 * 1024)
    parser.add_argument("--max-wire-bytes", type=bounded_int(64, 1024 * 1024 * 1024), default=64 * 1024 * 1024)
    parser.add_argument("--max-messages", type=bounded_int(1, 1000000), default=20000)
    args = parser.parse_args()
    async def run():
        return await Session(args).serve()
    try:
        return asyncio.run(run())
    except (BrokenPipeError, KeyboardInterrupt):
        return 3
    except Exception:
        # Last-resort static error: network exceptions can embed query bytes.
        print("error: fault proxy stopped without a complete receipt", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
