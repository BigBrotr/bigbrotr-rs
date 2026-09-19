"""Independent wire clients over real loopback sockets; no PostgreSQL claims."""
import asyncio
import json
from pathlib import Path
import struct
import sys
import unittest

SCRIPT = Path(__file__).with_name("fault_proxy.py")
STARTUP = b"\x00\x03\x00\x00user\x00lab\x00database\x00lab\x00\x00"
SECRET = b"fixture-password-never-record-this"


def frame(tag, body=b""):
    return tag + struct.pack("!I", len(body) + 4) + body


async def receive(reader):
    head = await reader.readexactly(5)
    size = struct.unpack("!I", head[1:])[0]
    return head[:1], await reader.readexactly(size - 4)


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.process = None
        self.tasks = []
        self.writers = []
        self.events = []
        self.accepted = 0
        self.backend_done = asyncio.get_running_loop().create_future()
        self.scenario = None

        def accept(reader, writer):
            self.accepted += 1
            self.writers.append(writer)
            self.tasks.append(asyncio.create_task(self.backend(reader, writer)))

        self.server = await asyncio.start_server(accept, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        if self.process and self.process.returncode is None:
            self.process.terminate()
            await asyncio.wait_for(self.process.wait(), 3)
        self.server.close()
        await self.server.wait_closed()
        for writer in self.writers:
            writer.close()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for writer in self.writers:
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        if self.backend_done.done() and not self.backend_done.cancelled():
            self.backend_done.exception()  # Retrieved even after a failed client assertion.

    async def backend(self, reader, writer):
        try:
            length = struct.unpack("!I", await reader.readexactly(4))[0]
            self.assertEqual(await reader.readexactly(length - 4), STARTUP)
            writer.write(frame(b"R", struct.pack("!I", 3)))
            await writer.drain()
            self.assertEqual(await receive(reader), (b"p", SECRET + b"\x00"))
            writer.write(frame(b"R", bytes(4)) + frame(b"K", b"KEYTEST!") + frame(b"Z", b"I"))
            await writer.drain()
            await self.scenario(reader, writer)
            self.backend_done.set_result(True)
        except Exception as error:
            if not self.backend_done.done():
                self.backend_done.set_exception(error)
        finally:
            writer.close()

    async def start(self, mode, scenario, *extra):
        self.assertTrue(SCRIPT.is_file(), "the bounded wire proxy is not implemented")
        self.scenario = scenario
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, str(SCRIPT), "--upstream-port", str(self.port),
            "--mode", mode, "--deadline-seconds", "5", *extra,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        ready = await self.event("ready")
        reader, writer = await asyncio.open_connection("127.0.0.1", ready["listen_port"])
        self.writers.append(writer)
        return reader, writer

    async def authenticate(self, reader, writer, fragmented=False):
        startup = struct.pack("!I", len(STARTUP) + 4) + STARTUP
        for chunk in ([bytes([v]) for v in startup] if fragmented else [startup]):
            writer.write(chunk)
            await writer.drain()
        self.assertEqual(await receive(reader), (b"R", struct.pack("!I", 3)))
        writer.write(frame(b"p", SECRET + b"\x00"))
        await writer.drain()
        self.assertEqual(await receive(reader), (b"R", bytes(4)))
        self.assertEqual(await receive(reader), (b"K", b"KEYTEST!"))
        self.assertEqual(await receive(reader), (b"Z", b"I"))

    async def event(self, kind):
        while True:
            line = await asyncio.wait_for(self.process.stdout.readline(), 7)
            self.assertTrue(line, "proxy exited before expected safe event: " + kind)
            item = json.loads(line)
            self.events.append(item)
            if item["event"] == kind:
                return item

    async def finish(self, code=0, backend=True):
        receipt = await self.event("receipt")
        self.assertEqual(await asyncio.wait_for(self.process.wait(), 3), code)
        stderr = await self.process.stderr.read()
        self.assertEqual(stderr, b"")
        output = json.dumps(self.events).encode()
        for value in [SECRET, b"KEYTEST!", b"secret-sql", b"secret-error"]:
            self.assertNotIn(value, output)
        if backend:
            await asyncio.wait_for(asyncio.shield(self.backend_done), 3)
        return receipt

    async def command(self, command):
        self.process.stdin.write(command)
        await self.process.stdin.drain()

    async def test_transparent_auth_and_fragmented_queries(self):
        query = b"SELECT 'secret-sql COMMIT'\x00"
        async def server(reader, writer):
            self.assertEqual(await receive(reader), (b"Q", query))
            writer.write(frame(b"C", b"SELECT 1\x00") + frame(b"Z", b"T"))
            await writer.drain()
            self.assertEqual(await receive(reader), (b"Q", b"COMMIT\x00"))
            writer.write(frame(b"C", b"COMMIT\x00") + frame(b"Z", b"I"))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server)
        await self.authenticate(reader, writer, fragmented=True)
        for chunk in [frame(b"Q", query)[:3], frame(b"Q", query)[3:]]:
            writer.write(chunk)
            await writer.drain()
        self.assertEqual(await receive(reader), (b"C", b"SELECT 1\x00"))
        self.assertEqual(await receive(reader), (b"Z", b"T"))
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        self.assertEqual(await receive(reader), (b"C", b"COMMIT\x00"))
        self.assertEqual(await receive(reader), (b"Z", b"I"))
        writer.close()
        result = await self.finish()
        self.assertEqual(result["commits_seen"], 1)
        self.assertEqual(result["commits_forwarded"], 1)
        self.assertEqual(result["client_commit_complete"], 1)
        self.assertEqual(result["faults"], 0)

    async def test_only_selected_commit_loses_its_reply(self):
        async def scenario(reader, writer):
            for _ in range(3):
                self.assertEqual(await receive(reader), (b"Q", b"COMMIT\x00"))
                writer.write(frame(b"C", b"COMMIT\x00") + frame(b"Z", b"I"))
                await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("after-complete", scenario, "--commit-number", "3")
        await self.authenticate(reader, writer)
        for _ in range(2):
            writer.write(frame(b"Q", b"COMMIT\x00")); await writer.drain()
            self.assertEqual(await receive(reader), (b"C", b"COMMIT\x00"))
            self.assertEqual(await receive(reader), (b"Z", b"I"))
        writer.write(frame(b"Q", b"COMMIT\x00")); await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish()
        self.assertEqual((result["commits_seen"], result["server_commit_complete"], result["client_commit_complete"], result["faults"]), (3, 3, 2, 1))

    async def test_before_forward_never_delivers_commit(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("before-forward", server)
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b" \ncommit \t;\r\n\x00"))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish()
        self.assertEqual((result["commits_seen"], result["commits_forwarded"], result["faults"]), (1, 0, 1))

    async def test_after_complete_suppresses_commit_and_coalesced_ready(self):
        async def server(reader, writer):
            self.assertEqual(await receive(reader), (b"Q", b"COMMIT\x00"))
            # One write deliberately coalesces both frames.
            writer.write(frame(b"C", b"COMMIT\x00") + frame(b"Z", b"I"))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("after-complete", server)
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish()
        self.assertEqual((result["commits_forwarded"], result["server_commit_complete"], result["client_commit_complete"]), (1, 1, 0))
        self.assertEqual(result["faults"], 1)

    async def test_hold_can_be_observed_and_closed_without_forward(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("hold", server)
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        await self.event("held")
        await self.command(b"status\n")
        status = await self.event("status")
        self.assertEqual(status["commits_forwarded"], 0)
        self.assertTrue(status["held"])
        await self.command(b"close\n")
        self.assertEqual(await reader.read(), b"")
        result = await self.finish()
        self.assertEqual(result["reason"], "held-close")
        self.assertEqual(result["commits_forwarded"], 0)

    async def test_hold_release_then_real_response(self):
        async def server(reader, writer):
            self.assertEqual(await receive(reader), (b"Q", b"COMMIT\x00"))
            writer.write(frame(b"C", b"COMMIT\x00") + frame(b"Z", b"I"))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("hold", server)
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        await self.event("held")
        await self.command(b"release\n")
        self.assertEqual(await receive(reader), (b"C", b"COMMIT\x00"))
        self.assertEqual(await receive(reader), (b"Z", b"I"))
        writer.close()
        result = await self.finish()
        self.assertEqual(result["hold_action"], "release")
        self.assertEqual(result["client_commit_complete"], 1)

    async def test_backend_error_is_not_a_commit_receipt(self):
        async def server(reader, writer):
            self.assertEqual(await receive(reader), (b"Q", b"COMMIT\x00"))
            writer.write(frame(b"E", b"Msecret-error\x00\x00") + frame(b"Z", b"I"))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("after-complete", server)
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        self.assertEqual(await receive(reader), (b"E", b"Msecret-error\x00\x00"))
        self.assertEqual(await receive(reader), (b"Z", b"I"))
        writer.close()
        result = await self.finish(code=3)
        self.assertEqual((result["faults"], result["server_commit_complete"]), (0, 0))

    async def test_substring_and_extended_query_are_not_targeted(self):
        queries = [(b"Q", b"SELECT 'COMMIT secret-sql'\x00"), (b"P", b"\x00COMMIT\x00\x00\x00")]
        async def server(reader, writer):
            for expected in queries:
                self.assertEqual(await receive(reader), expected)
            writer.write(frame(b"C", b"COMMIT\x00"))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("after-complete", server)
        await self.authenticate(reader, writer)
        for tag, body in queries:
            writer.write(frame(tag, body))
        await writer.drain()
        self.assertEqual(await receive(reader), (b"C", b"COMMIT\x00"))
        writer.close()
        result = await self.finish(code=3)
        self.assertEqual((result["commits_seen"], result["faults"]), (0, 0))

    async def test_oversized_frontend_frame_refused_before_body(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server)
        await self.authenticate(reader, writer)
        writer.write(b"Q" + struct.pack("!I", 0x7fffffff))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3)
        self.assertEqual(result["reason"], "frame-length")

    async def test_invalid_backend_length_is_refused(self):
        async def server(reader, writer):
            writer.write(b"C" + struct.pack("!I", 3))
            await writer.drain()
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server)
        await self.authenticate(reader, writer)
        self.assertEqual(await reader.read(), b"")
        self.assertEqual((await self.finish(code=3))["reason"], "frame-length")

    async def test_truncated_frame_is_refused_without_forwarding(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server)
        await self.authenticate(reader, writer)
        writer.write(b"Q" + struct.pack("!I", 100) + b"secret-sql")
        await writer.drain()
        writer.write_eof()
        self.assertEqual(await reader.read(), b"")
        self.assertEqual((await self.finish(code=3))["reason"], "truncated-frame")

    async def test_cumulative_wire_budget_refuses_next_frame(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server, "--max-wire-bytes", "200")
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"x" * 150))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3)
        self.assertEqual(result["reason"], "wire-budget")
        self.assertLessEqual(result["declared_wire_bytes"], 200)

    async def test_message_budget_refuses_next_frame(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server, "--max-messages", "5")
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3)
        self.assertEqual(result["reason"], "message-budget")
        self.assertEqual(sum(result["frames"].values()), 5)

    async def test_listener_closes_after_first_connection(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("pass", server)
        await self.authenticate(reader, writer)
        port = next(item["listen_port"] for item in self.events if item["event"] == "ready")
        with self.assertRaises(ConnectionRefusedError):
            await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        await self.finish()
        self.assertEqual(self.accepted, 1)

    async def test_oversized_startup_refused_before_backend_connect(self):
        reader, writer = await self.start("pass", None)
        writer.write(struct.pack("!I", 0x7fffffff))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3, backend=False)
        self.assertEqual(result["reason"], "startup-length")
        self.assertEqual(self.accepted, 0)

    async def test_encrypted_negotiation_refused(self):
        reader, writer = await self.start("pass", None)
        writer.write(struct.pack("!II", 8, 80877103))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3, backend=False)
        self.assertEqual(result["reason"], "startup-protocol")
        self.assertEqual(self.accepted, 0)

    async def test_unfinished_startup_hits_deadline(self):
        reader, writer = await self.start("pass", None, "--deadline-seconds", "0.2")
        writer.write(b"\x00")
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3, backend=False)
        self.assertEqual(result["reason"], "deadline")

    async def test_hold_deadline_never_releases_commit(self):
        async def server(reader, writer):
            self.assertEqual(await reader.read(), b"")
        reader, writer = await self.start("hold", server, "--deadline-seconds", "0.5")
        await self.authenticate(reader, writer)
        writer.write(frame(b"Q", b"COMMIT\x00"))
        await writer.drain()
        await self.event("held")
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3)
        self.assertEqual((result["reason"], result["commits_forwarded"]), ("deadline", 0))

    async def test_invalid_control_does_not_leak_input(self):
        reader, writer = await self.start("hold", None)
        await self.command(SECRET * 2 + b"\n")
        self.assertEqual(await reader.read(), b"")
        result = await self.finish(code=3, backend=False)
        self.assertEqual(result["reason"], "control-length")


if __name__ == "__main__":
    unittest.main()
