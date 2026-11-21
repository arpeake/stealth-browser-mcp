"""SB-POST-1: unit tests for the CDP-over-pipe transport helper.

Free of real browsers, real networks, and real browser profiles. A socket
pair stands in for Chrome's inherited pipe so the framing round-trips through
the real asyncio reader loop without any loopback socket being opened.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import sys
import unittest
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parents[1]
src_dir = str(ROOT / "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from stealth_browser_mcp import cdp_pipe  # noqa: E402
from stealth_browser_mcp.cdp_pipe import (  # noqa: E402
    CdpPipeError,
    CdpTransport,
    PipeCDPClient,
    build_chrome_launch_args,
    parse_cdp_transport,
    resolve_cdp_transport,
)


class ParseTransportTests(unittest.TestCase):
    """Acceptance test 1: unset, empty, and "port" => PORT; "pipe" => PIPE."""

    def test_unset_is_port(self) -> None:
        self.assertIs(parse_cdp_transport(None), CdpTransport.PORT)

    def test_empty_is_port(self) -> None:
        self.assertIs(parse_cdp_transport(""), CdpTransport.PORT)

    def test_whitespace_only_is_port(self) -> None:
        self.assertIs(parse_cdp_transport("   "), CdpTransport.PORT)

    def test_port_is_port(self) -> None:
        self.assertIs(parse_cdp_transport("port"), CdpTransport.PORT)

    def test_pipe_is_pipe(self) -> None:
        self.assertIs(parse_cdp_transport("pipe"), CdpTransport.PIPE)

    def test_pipe_is_pipe_case_insensitive(self) -> None:
        self.assertIs(parse_cdp_transport("PIPE"), CdpTransport.PIPE)

    def test_pipe_is_pipe_with_surrounding_whitespace(self) -> None:
        self.assertIs(parse_cdp_transport("  pipe  "), CdpTransport.PIPE)

    def test_unknown_is_error(self) -> None:
        with self.assertRaises(ValueError):
            parse_cdp_transport("tcp")

    def test_bogus_value_is_error_not_fallback(self) -> None:
        with self.assertRaises(ValueError):
            parse_cdp_transport("pipe-tcp")

    def test_transport_property_is_pipe(self) -> None:
        self.assertTrue(CdpTransport.PIPE.is_pipe)
        self.assertFalse(CdpTransport.PORT.is_pipe)

    def test_resolve_from_env_pipe(self) -> None:
        env_value = os.environ.get(cdp_pipe.CDP_TRANSPORT_ENV)
        os.environ[cdp_pipe.CDP_TRANSPORT_ENV] = "pipe"
        try:
            self.assertIs(resolve_cdp_transport(), CdpTransport.PIPE)
        finally:
            if env_value is None:
                del os.environ[cdp_pipe.CDP_TRANSPORT_ENV]
            else:
                os.environ[cdp_pipe.CDP_TRANSPORT_ENV] = env_value

    def test_resolve_from_env_unset(self) -> None:
        env_value = os.environ.get(cdp_pipe.CDP_TRANSPORT_ENV)
        os.environ.pop(cdp_pipe.CDP_TRANSPORT_ENV, None)
        try:
            self.assertIs(resolve_cdp_transport(), CdpTransport.PORT)
        finally:
            if env_value is not None:
                os.environ[cdp_pipe.CDP_TRANSPORT_ENV] = env_value


class ArgvBuilderPortTests(unittest.TestCase):
    """Acceptance test 2: port mode keeps --remote-debugging-port=N, no pipe."""

    def test_port_mode_includes_port_and_no_pipe(self) -> None:
        args = build_chrome_launch_args(
            ["--no-first-run"], transport=CdpTransport.PORT, port=1234
        )
        self.assertIn("--remote-debugging-port=1234", args)
        self.assertNotIn("--remote-debugging-pipe", args)

    def test_port_mode_respects_base_args(self) -> None:
        args = build_chrome_launch_args(
            ["--headless"], transport=CdpTransport.PORT, port=4000
        )
        self.assertIn("--headless", args)

    def test_port_mode_removes_stale_pipe_flag(self) -> None:
        args = build_chrome_launch_args(
            ["--remote-debugging-pipe"], transport=CdpTransport.PORT, port=999
        )
        self.assertIn("--remote-debugging-port=999", args)
        self.assertNotIn("--remote-debugging-pipe", args)

    def test_default_transport_is_port_when_unset(self) -> None:
        env_value = os.environ.get(cdp_pipe.CDP_TRANSPORT_ENV)
        os.environ.pop(cdp_pipe.CDP_TRANSPORT_ENV, None)
        try:
            args = build_chrome_launch_args(["--x"], port=5555)
            self.assertIn("--remote-debugging-port=5555", args)
            self.assertNotIn("--remote-debugging-pipe", args)
        finally:
            if env_value is not None:
                os.environ[cdp_pipe.CDP_TRANSPORT_ENV] = env_value


class ArgvBuilderPipeTests(unittest.TestCase):
    """Acceptance test 3: pipe mode emits --remote-debugging-pipe, no port."""

    def test_pipe_mode_includes_pipe_and_no_port(self) -> None:
        args = build_chrome_launch_args(
            ["--headless"], transport=CdpTransport.PIPE, port=1234
        )
        self.assertIn("--remote-debugging-pipe", args)
        self.assertNotIn("--remote-debugging-port", args)

    def test_pipe_mode_drops_any_port_flag(self) -> None:
        args = build_chrome_launch_args(
            ["--remote-debugging-port=4000"], transport=CdpTransport.PIPE, port=1234
        )
        self.assertIn("--remote-debugging-pipe", args)
        self.assertNotIn("--remote-debugging-port", args)

    def test_pipe_mode_preserves_base_args(self) -> None:
        args = build_chrome_launch_args(
            ["--headless", "--no-first-run"], transport=CdpTransport.PIPE
        )
        self.assertIn("--headless", args)
        self.assertIn("--no-first-run", args)
        self.assertIn("--remote-debugging-pipe", args)


class PipeFramingTests(unittest.IsolatedAsyncioTestCase):
    """Acceptance test 4: framing round-trips a request/response through a fake peer.

    Uses a socket pair as Chrome's inherited pipe so the client drives the real
    asyncio reader loop. No real browser and no loopback socket is opened.
    """

    async def _open_pair(self):
        client_sock, server_sock = socket.socketpair()
        reader, writer = await asyncio.open_connection(sock=client_sock)
        return reader, writer, client_sock, server_sock

    async def test_send_then_call_round_trip(self) -> None:
        reader, writer, client_sock, server_sock = await self._open_pair()

        async def peer():
            sreader, swriter = await asyncio.open_connection(sock=server_sock)
            header = await sreader.readexactly(cdp_pipe._LENGTH_PREFIX_SIZE)
            (length,) = struct.unpack(">I", header)
            body = await sreader.readexactly(length)
            request = json.loads(body.decode("utf-8"))
            self.assertEqual(request["method"], "Browser.getVersion")
            payload = json.dumps(
                {
                    "id": request["id"],
                    "result": {
                        "protocolVersion": "1.3",
                        "product": "Chrome/120",
                        "revision": "r1",
                        "userAgent": "ua",
                        "jsVersion": "js",
                    },
                }
            ).encode("utf-8")
            swriter.write(struct.pack(">I", len(payload)) + payload)
            await swriter.drain()
            swriter.close()

        try:
            task = asyncio.create_task(peer())
            client = await PipeCDPClient(reader, writer).start()
            result = await asyncio.wait_for(client.call("Browser.getVersion"), 5.0)
            self.assertEqual(result["protocolVersion"], "1.3")
            self.assertEqual(result["product"], "Chrome/120")
            await client.close()
            await task
        finally:
            client_sock.close()
            server_sock.close()

    async def test_request_id_sequence(self) -> None:
        reader, writer, client_sock, server_sock = await self._open_pair()
        seen: List[dict] = []

        async def peer():
            sreader, swriter = await asyncio.open_connection(sock=server_sock)
            for _ in range(3):
                header = await sreader.readexactly(cdp_pipe._LENGTH_PREFIX_SIZE)
                (length,) = struct.unpack(">I", header)
                body = await sreader.readexactly(length)
                seen.append(json.loads(body.decode("utf-8")))
                payload = json.dumps(
                    {"id": seen[-1]["id"], "result": {"ok": True}}
                ).encode("utf-8")
                swriter.write(struct.pack(">I", len(payload)) + payload)
                await swriter.drain()
            swriter.close()

        try:
            task = asyncio.create_task(peer())
            client = await PipeCDPClient(reader, writer).start()
            for i in range(3):
                resp = await asyncio.wait_for(client.call("Method.N", {"n": i}), 5.0)
                self.assertTrue(resp["ok"])
            self.assertEqual([s["id"] for s in seen], [1, 2, 3])
            self.assertEqual([s["params"]["n"] for s in seen], [0, 1, 2])
            await client.close()
            await task
        finally:
            client_sock.close()
            server_sock.close()

    async def test_multi_chunk_message(self) -> None:
        reader, writer, client_sock, server_sock = await self._open_pair()
        received: List[bytes] = []

        async def peer():
            sreader, swriter = await asyncio.open_connection(sock=server_sock)
            header = await sreader.readexactly(cdp_pipe._LENGTH_PREFIX_SIZE)
            (length,) = struct.unpack(">I", header)
            body = b""
            while len(body) < length:
                body += await sreader.read(3)
            received.append(body)
            payload = json.dumps(
                {"id": json.loads(body)["id"], "result": {"ok": True}}
            ).encode("utf-8")
            swriter.write(struct.pack(">I", len(payload)) + payload)
            await swriter.drain()
            swriter.close()

        try:
            task = asyncio.create_task(peer())
            client = await PipeCDPClient(reader, writer).start()
            result = await asyncio.wait_for(
                client.call("Method.Test", {"a": 1, "b": 2, "c": 3}), 5.0
            )
            self.assertTrue(result["ok"])
            self.assertEqual(len(received), 1)
            decoded = json.loads(received[0])
            self.assertEqual(decoded["method"], "Method.Test")
            self.assertEqual(decoded["params"], {"a": 1, "b": 2, "c": 3})
            await client.close()
            await task
        finally:
            client_sock.close()
            server_sock.close()

    async def test_error_response_raises(self) -> None:
        reader, writer, client_sock, server_sock = await self._open_pair()
        seen: List[dict] = []

        async def peer():
            sreader, swriter = await asyncio.open_connection(sock=server_sock)
            header = await sreader.readexactly(cdp_pipe._LENGTH_PREFIX_SIZE)
            (length,) = struct.unpack(">I", header)
            body = await sreader.readexactly(length)
            request = json.loads(body.decode("utf-8"))
            seen.append(request)
            payload = json.dumps(
                {"id": request["id"], "error": {"code": -32000, "message": "boom"}}
            ).encode("utf-8")
            swriter.write(struct.pack(">I", len(payload)) + payload)
            await swriter.drain()
            swriter.close()

        try:
            task = asyncio.create_task(peer())
            client = await PipeCDPClient(reader, writer).start()
            with self.assertRaises(CdpPipeError):
                await asyncio.wait_for(client.call("Method.Failing"), 5.0)
            self.assertEqual(seen[0]["method"], "Method.Failing")
            await client.close()
            await task
        finally:
            client_sock.close()
            server_sock.close()


if __name__ == "__main__":
    unittest.main()
