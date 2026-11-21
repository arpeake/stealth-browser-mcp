"""Opt-in CDP-over-pipe transport (SB-POST-1, closes residual F9).

The default launch path (and the only one unless STEALTH_CDP_TRANSPORT=pipe)
controls spawned Chrome over a loopback DevTools port on 127.0.0.1. That port
has no DevTools authentication, so another local user or another process in
the same network namespace can attach to the browser. See SECURITY.md section
"Accepted residual: the loopback DevTools port (F9)".

This module adds an opt-in alternative that talks CDP over Chrome's inherited
``--remote-debugging-pipe`` instead of a TCP port, closing that gap for
browsers this server spawns. Attaching to an already-running external browser
still needs the port and is untouched here.

The pipe is a plain in-process duplex channel. Chrome frames every CDP
message the way the DevTools protocol documents for pipes: a 4-byte big-endian
unsigned length prefix followed by the JSON message. This module implements
that framing plus a small async client used to drive a pipe-launched browser.

No nodriver import lives in this module so the parsing, argv building, and
framing stay unit-testable without a real browser or the nodriver package.
"""

from __future__ import annotations

import asyncio
import json
import os
import struct
from enum import Enum
from typing import Any, Dict, List, Optional

# Environment flag that selects the opt-in pipe transport. Exact match.
CDP_TRANSPORT_ENV = "STEALTH_CDP_TRANSPORT"

# The two supported transports.
TRANSPORT_PORT = "port"
TRANSPORT_PIPE = "pipe"

# Chrome pipe framing: 4-byte big-endian length prefix + JSON body.
_LENGTH_PREFIX_SIZE = 4

# Raised when Chrome replies with a CDP error object.
class CdpPipeError(Exception):
    """Raised when the pipe transport reports a CDP error from the peer."""

class CdpTransport(Enum):
    """Which transport to use to reach the spawned browser's DevTools."""

    PORT = "port"
    PIPE = "pipe"

    @property
    def is_pipe(self) -> bool:
        return self is CdpTransport.PIPE


def parse_cdp_transport(env_value: Optional[str]) -> CdpTransport:
    """Resolve a transport from the raw environment value.

    Unset, empty, and the literal string "port" all select the loopback-port
    path so v1.0.0 behavior is preserved bit for bit. The literal string
    "pipe" selects the pipe transport. Any other value is a hard error rather
    than a silent fall back, because a typo could leave F9 unaddressed.

    Args:
        env_value (Optional[str]): Raw env value (already unstripped or None).

    Returns:
        CdpTransport: PORT for unset/empty/"port", PIPE for "pipe".

    Raises:
        ValueError: When env_value is a non-empty value other than port/pipe.
    """
    if env_value is None:
        return CdpTransport.PORT
    value = env_value.strip().lower()
    if value == "" or value == TRANSPORT_PORT:
        return CdpTransport.PORT
    if value == TRANSPORT_PIPE:
        return CdpTransport.PIPE
    raise ValueError(
        f"unknown {CDP_TRANSPORT_ENV}={env_value!r}: expected "
        f"'{TRANSPORT_PORT}', '{TRANSPORT_PIPE}', or unset"
    )


def resolve_cdp_transport() -> CdpTransport:
    """Resolve the transport from the process environment."""
    return parse_cdp_transport(os.getenv(CDP_TRANSPORT_ENV))


def build_chrome_launch_args(
    base_args: Optional[List[str]] = None,
    *,
    transport: Optional[CdpTransport] = None,
    port: int = 0,
) -> List[str]:
    """Return the browser launch args with the DevTools flag for a transport.

    Args:
        base_args (Optional[List[str]]): Args already assembled for the spawn.
        transport (Optional[CdpTransport]): Transport to target.
        port (int): Port to emit only in PORT mode. Ignored in PIPE mode.

    Returns:
        List[str]: The base args plus exactly one DevTools flag. PORT mode
        emits ``--remote-debugging-port=<port>`` and no pipe flag. PIPE mode
        emits ``--remote-debugging-pipe`` and never a port flag.

    Raises:
        ValueError: When transport parsing of the base args fails.
    """
    args = list(base_args or [])
    resolved = transport or resolve_cdp_transport()

    if resolved.is_pipe:
        args = [a for a in args if not a.startswith("--remote-debugging-port")]
        if "--remote-debugging-pipe" not in args:
            args.append("--remote-debugging-pipe")
        return args

    args = [a for a in args if not a.startswith("--remote-debugging-pipe")]
    args.append(f"--remote-debugging-port={port}")
    return args


class PipeCDPClient:
    """Minimal async CDP client over an inherited Chrome pipe.

    The pipe is exposed to the server as a pair of file descriptors that
    already belong to this process, so this client does not open a socket:
    it wraps the write end to send frames and a reader loop on the read end
    to collect them. Chrome sends a 4-byte big-endian length prefix followed
    by the JSON message body.

    A typical construction is ``PipeCDPClient(r, w, loop)`` with ``r`` and
    ``w`` the read/write ends Chrome inherited. Callers are responsible for
    spawning Chrome with ``--remote-debugging-pipe`` and passing it the same
    descriptors.

    Args:
        reader: AsyncReaderProtocol exposing awaitable read primitives.
        writer: AsyncWriterProtocol exposing drain/close primitives.
        loop: The running event loop to bind the reader task to.

    Attributes:
        next_id (int): Monotonic CDP message id counter, starting at 1.
    """

    def __init__(
        self,
        reader: "Any",
        writer: "Any",
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._loop = loop or asyncio.get_running_loop()
        self.next_id = 1
        self._pending: Dict[int, asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._closed = False

    def send(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        message_id: Optional[int] = None,
    ) -> int:
        """Serialize and queue a CDP request.

        Args:
            method (str): CDP method name, e.g. "Browser.getVersion".
            params (Optional[Dict[str, Any]]): Optional method parameters.
            message_id (Optional[int]): Override the auto-assigned id.

        Returns:
            int: The message id that pairs with the response.
        """
        if message_id is not None:
            msg_id = message_id
        else:
            msg_id = self.next_id
            self.next_id = msg_id + 1
        request = {"id": msg_id, "method": method}
        if params is not None:
            request["params"] = params
        body = json.dumps(request).encode("utf-8")
        frame = struct.pack(">I", len(body)) + body
        self.writer.write(frame)
        return msg_id

    async def call(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        """Send a request and wait for its response.

        Args:
            method (str): CDP method name.
            params (Optional[Dict[str, Any]]): Optional method parameters.
            timeout (float): Seconds to wait for the response.

        Returns:
            Dict[str, Any]: The decoded "result" object.

        Raises:
            CdpPipeError: When the peer returns a CDP error object.
            asyncio.TimeoutError: When the response does not arrive in time.
        """
        if self._closed:
            raise CdpPipeError("client is closed")
        future = self._loop.create_future()
        msg_id = self.send(method, params)
        self._pending[msg_id] = future
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def _reader_loop(self) -> None:
        reader = self.reader
        while True:
            header = await reader.readexactly(_LENGTH_PREFIX_SIZE)
            (length,) = struct.unpack(">I", header)
            body = await reader.readexactly(length)
            message = json.loads(body.decode("utf-8"))
            msg_id = message.get("id")
            if "error" in message:
                future = self._pending.pop(msg_id, None)
                if future is not None and not future.done():
                    future.set_exception(
                        CdpPipeError(json.dumps(message["error"]))
                    )
                continue
            future = self._pending.pop(msg_id, None)
            if future is not None and not future.done():
                future.set_result(message.get("result", {}))

    async def start(self) -> "PipeCDPClient":
        """Start the background reader loop and return self."""
        self._reader_task = self._loop.create_task(self._reader_loop())
        return self

    async def close(self) -> None:
        """Stop the reader loop and close the pipe."""
        self._closed = True
        task = self._reader_task
        if task is not None and not task.done():
            task.cancel()
        self._pending.clear()
        task = self._reader_task
        self._reader_task = None
        if task is not None:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if getattr(self.writer, "close", None) is not None:
            try:
                self.writer.close()
            except Exception:
                pass
