"""Chrome process launch with a patient DevTools handshake and no leaked processes."""

import asyncio
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Callable, List, Optional

import nodriver as uc
import psutil
from nodriver import Browser, Tab
from nodriver.core import util as nodriver_util
from .cdp_pipe import (
    CdpTransport,
    PipeCDPClient,
    PipeConnection,
    PipeDescriptors,
    PipeWebSocket,
    build_chrome_launch_args,
    open_chrome_pipe,
    resolve_cdp_transport,
)

DEVTOOLS_READY_TIMEOUT = 30.0
DEVTOOLS_POLL_INTERVAL = 0.05
PROCESS_EXIT_TIMEOUT = 3.0
ACTIVE_PORT_FILE = "DevToolsActivePort"


def _is_listening(pid: int, port: int) -> bool:
    """
    Check whether a process or its children listen on a TCP port.

    Inspecting the process avoids probing a closed port, which takes about a
    second to be refused on Windows.

    Args:
        pid (int): Browser process id.
        port (int): DevTools port.

    Returns:
        bool: True once the port is in LISTEN state.
    """
    try:
        root = psutil.Process(pid)
        for process in [root, *root.children(recursive=True)]:
            for connection in process.net_connections(kind="tcp"):
                if connection.status == psutil.CONN_LISTEN and connection.laddr and connection.laddr.port == port:
                    return True
    except (psutil.Error, OSError):
        return False
    return False


def _read_active_port(profile_dir: Path) -> Optional[int]:
    """
    Read the DevTools port Chrome writes once its debugging server listens.

    Args:
        profile_dir (Path): Browser user data directory.

    Returns:
        Optional[int]: Port number, or None when the file is not written yet.
    """
    try:
        first_line = (profile_dir / ACTIVE_PORT_FILE).read_text(encoding="utf-8").splitlines()[0]
        return int(first_line.strip())
    except (OSError, ValueError, IndexError):
        return None


async def _stop_process(process: Any, browser: Optional[Any] = None) -> None:
    """
    Terminate a process, killing it if it does not exit in time.

    Args:
        process: Process to stop.
        browser: Optional browser instance to exit gracefully via CDP Browser.close.
    """
    if getattr(process, "returncode", None) is not None:
        if browser is not None and hasattr(browser, "_descriptors") and browser._descriptors is not None:
            browser._descriptors.close()
        return
    if hasattr(process, "poll") and process.poll() is not None:
        if browser is not None and hasattr(browser, "_descriptors") and browser._descriptors is not None:
            browser._descriptors.close()
        return

    if browser is not None and getattr(browser, "connection", None) is not None:
        try:
            await asyncio.wait_for(browser.connection.send(uc.cdp.browser.close()), 5.0)
            if hasattr(process, "wait"):
                wait_res = process.wait()
                if asyncio.iscoroutine(wait_res):
                    await asyncio.wait_for(wait_res, 10.0)
                else:
                    await asyncio.to_thread(process.wait, 10.0)
                if hasattr(browser, "_descriptors") and browser._descriptors is not None:
                    browser._descriptors.close()
                return
        except Exception:
            pass

    for signal_name in ("terminate", "kill"):
        signal_process = getattr(process, signal_name, None)
        if signal_process is None:
            continue
        try:
            signal_process()
            if hasattr(process, "wait"):
                wait_res = process.wait()
                if asyncio.iscoroutine(wait_res):
                    await asyncio.wait_for(wait_res, PROCESS_EXIT_TIMEOUT)
                else:
                    await asyncio.to_thread(process.wait, PROCESS_EXIT_TIMEOUT)
            if browser is not None and hasattr(browser, "_descriptors") and browser._descriptors is not None:
                browser._descriptors.close()
            return
        except ProcessLookupError:
            if browser is not None and hasattr(browser, "_descriptors") and browser._descriptors is not None:
                browser._descriptors.close()
            return
        except Exception:
            continue

    if browser is not None and hasattr(browser, "_descriptors") and browser._descriptors is not None:
        browser._descriptors.close()


class PipeTab(Tab):
    """nodriver.Tab driven over a flattened CDP session over the pipe."""

    def __init__(
        self,
        websocket_url: str,
        target: Any,
        browser: Any,
        session_id: str,
        websocket: PipeWebSocket,
    ):
        super().__init__(websocket_url, target, browser)
        self.session_id = session_id
        self._websocket = websocket
        if getattr(browser, "connection", None) is not None:
            real = getattr(browser.connection, "_real", browser.connection)
            if hasattr(real, "__count__"):
                self.__count__ = real.__count__
        self._listener_task = asyncio.ensure_future(self._listener())

    async def connect(self, **kwargs):
        if self._websocket.closed:
            await self._websocket.start()
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.ensure_future(self._listener())
        await self._register_handlers()


class PipeBrowser(Browser):
    """Subclass of nodriver.Browser that drives Chrome over an inherited pipe."""

    def __init__(self, config: uc.Config, descriptors: PipeDescriptors, **kwargs):
        super().__init__(config, **kwargs)
        self._descriptors = descriptors
        self._client: Optional[PipeCDPClient] = None
        self._ws: Optional[PipeWebSocket] = None

    async def start(self) -> "PipeBrowser":
        from nodriver.core._contradict import ContraDict

        self.info = ContraDict(
            {
                "Browser": "Chrome",
                "Protocol-Version": "1.3",
                "User-Agent": "stealth-browser",
                "V8-Version": "1.0",
                "WebKit-Version": "1.0",
                "webSocketDebuggerUrl": "ws://pipe",
            },
            silent=True,
        )

        nodriver_util.get_registered_instances().add(self)

        loop = asyncio.get_running_loop()
        self._client = PipeCDPClient(
            self._descriptors.response_r,
            self._descriptors.command_w,
            loop=loop,
        )
        await self._client.start()

        self._ws = PipeWebSocket(
            self._descriptors.response_r,
            self._descriptors.command_w,
            loop=loop,
            session_id=None,
            client=self._client,
        )
        await self._ws.start()

        self.connection = PipeConnection(self, self._ws)
        await self.connection.connect()

        self.connection.handlers[uc.cdp.target.TargetInfoChanged] = [
            self._handle_target_update
        ]
        self.connection.handlers[uc.cdp.target.TargetCreated] = [
            self._handle_target_update
        ]
        self.connection.handlers[uc.cdp.target.TargetDestroyed] = [
            self._handle_target_update
        ]
        self.connection.handlers[uc.cdp.target.TargetCrashed] = [
            self._handle_target_update
        ]

        await self.connection.send(uc.cdp.target.set_discover_targets(discover=True))

        await self.update_targets()

        for _ in range(50):
            if self.tabs:
                break
            if self._process is not None and self._process.returncode is not None:
                raise Exception(
                    f"Browser exited during startup with code {self._process.returncode}"
                )
            await asyncio.sleep(0.1)
            await self.update_targets()

        return self

    async def _attach_page_target(self, target_info: Any) -> Optional[PipeTab]:
        for tab in self.targets:
            if getattr(tab, "target_id", None) == target_info.target_id:
                return tab

        session_id = await self.connection.send(
            uc.cdp.target.attach_to_target(target_id=target_info.target_id, flatten=True)
        )
        session_id_str = str(session_id)

        tab_ws = PipeWebSocket(
            self._descriptors.response_r,
            self._descriptors.command_w,
            loop=asyncio.get_running_loop(),
            session_id=session_id_str,
            client=self._client,
        )
        await tab_ws.start()

        tab = PipeTab(
            websocket_url=f"ws://pipe/page/{target_info.target_id}",
            target=target_info,
            browser=self,
            session_id=session_id_str,
            websocket=tab_ws,
        )
        self.targets.append(tab)
        return tab

    async def update_targets(self):
        targets = await self.connection.send(uc.cdp.target.get_targets())
        current_target_ids = {t.target_id for t in targets}
        existing_tabs = {getattr(tab, "target_id", None): tab for tab in self.targets}

        for t in targets:
            if t.target_id in existing_tabs:
                existing_tabs[t.target_id].target.__dict__.update(t.__dict__)
            else:
                if t.type_ == "page":
                    await self._attach_page_target(t)

        for tid, tab in list(existing_tabs.items()):
            if tid is not None and tid not in current_target_ids:
                if tab in self.targets:
                    self.targets.remove(tab)
                asyncio.create_task(tab.disconnect())

        await asyncio.sleep(0)

    def _handle_target_update(self, event: Any, connection: Optional[Any] = None) -> None:
        if isinstance(event, uc.cdp.target.TargetCreated):
            if event.target_info.type_ == "page":
                asyncio.create_task(self._attach_page_target(event.target_info))
        elif isinstance(event, uc.cdp.target.TargetDestroyed):
            for tab in list(self.targets):
                if getattr(tab, "target_id", None) == event.target_id:
                    self.targets.remove(tab)
                    asyncio.create_task(tab.disconnect())
                    break
        elif isinstance(event, uc.cdp.target.TargetInfoChanged):
            for tab in self.targets:
                if getattr(tab, "target_id", None) == event.target_info.target_id:
                    tab.target.__dict__.update(event.target_info.__dict__)
                    break

    async def get(
        self,
        url: str = "chrome://welcome",
        new_tab: bool = False,
        new_window: bool = False,
    ):
        if new_tab or new_window:
            target_id = await self.connection.send(
                uc.cdp.target.create_target(
                    url, new_window=new_window, enable_begin_frame_control=True
                )
            )
            match = None
            for _ in range(50):
                match = next(
                    (
                        item
                        for item in self.targets
                        if item.type_ == "page" and getattr(item, "target_id", None) == target_id
                    ),
                    None,
                )
                if match is not None:
                    break
                await self.update_targets()
                match = next(
                    (
                        item
                        for item in self.targets
                        if item.type_ == "page" and getattr(item, "target_id", None) == target_id
                    ),
                    None,
                )
                if match is not None:
                    break
                await asyncio.sleep(0.1)

            if match is None:
                raise RuntimeError(f"Could not find created target {target_id}")
            match._browser = self
            return match
        else:
            if not self.targets:
                await self.update_targets()
            conn = self.main_tab
            frame_id, loader_id, *_ = await conn.send(uc.cdp.page.navigate(url))
            conn.frame_id = frame_id
            conn._browser = self
            await self
            return conn

    def stop(self):
        super().stop()
        if self._descriptors:
            self._descriptors.close()


async def launch_browser(
    config: uc.Config,
    timeout: float = DEVTOOLS_READY_TIMEOUT,
    args_modifier: Optional[Callable[[List[str]], List[str]]] = None,
) -> Browser:
    """
    Start Chrome for a nodriver config and connect to it.

    nodriver launches Chrome itself but gives the DevTools endpoint under
    three seconds to come up, which cold starts on CI machines and slow
    disks miss, and it leaves the process running when that happens. It
    also pipes Chrome's output without reading it, so a chatty browser can
    block on a full pipe. Here readiness is detected by watching the
    process listen on its debugging port, its output goes to the null device, startup gets up to `timeout`
    seconds, and a failed start kills the process and removes its temporary
    profile. Port 0 is avoided on purpose, because Chrome treats
    --remote-debugging-port=0 as automation and sets navigator.webdriver.

    Args:
        config (uc.Config): nodriver config describing the launch.
        timeout (float): Seconds to wait for the DevTools endpoint.
        args_modifier (Optional[Callable[[List[str]], List[str]]]): Optional modifier
            to transform launch arguments after config() is evaluated.

    Returns:
        Browser: Connected nodriver browser whose `_process` is the launched Chrome.

    Raises:
        Exception: When Chrome exits early or the endpoint does not come up in time.
    """
    transport = resolve_cdp_transport()
    if transport.is_pipe:
        profile_dir = Path(config.user_data_dir)
        profile_dir.mkdir(parents=True, exist_ok=True)
        (profile_dir / ACTIVE_PORT_FILE).unlink(missing_ok=True)
        config.host = None
        config.port = None
        launch_args = list(config())
        modifier = args_modifier or getattr(config, "args_modifier", None)
        if modifier is not None:
            launch_args = modifier(launch_args)
        launch_args = build_chrome_launch_args(launch_args, transport=CdpTransport.PIPE)

        descriptors = open_chrome_pipe()
        process = await asyncio.create_subprocess_exec(
            config.browser_executable_path,
            *launch_args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            close_fds=False,
            preexec_fn=descriptors.map_child_fds(),
        )
        descriptors.close_parent_unused()

        try:
            browser = PipeBrowser(config, descriptors)
            browser._process = process
            browser._process_pid = process.pid
            await asyncio.wait_for(browser.start(), timeout=timeout)
            return browser
        except BaseException:
            descriptors.close()
            await _stop_process(process)
            if not config.uses_custom_data_dir:
                shutil.rmtree(config.user_data_dir, ignore_errors=True)
            raise

    profile_dir = Path(config.user_data_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / ACTIVE_PORT_FILE).unlink(missing_ok=True)
    config.host = "127.0.0.1"
    config.port = nodriver_util.free_port()
    launch_args = list(config())
    modifier = args_modifier or getattr(config, "args_modifier", None)
    if modifier is not None:
        launch_args = modifier(launch_args)
    process = await asyncio.create_subprocess_exec(
        config.browser_executable_path,
        *launch_args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        close_fds=sys.platform != "win32",
    )
    try:
        deadline = time.monotonic() + timeout
        while not (
            _read_active_port(profile_dir) == config.port
            or await asyncio.to_thread(_is_listening, process.pid, config.port)
        ):
            if process.returncode is not None:
                raise Exception(f"Browser exited during startup with code {process.returncode}")
            if time.monotonic() >= deadline:
                raise Exception(
                    f"Browser DevTools endpoint did not start within {timeout:.0f}s. "
                    "On Linux, headed Chrome on Wayland without a DRM render node "
                    "never opens the port; set DISPLAY or use window=headless."
                )
            await asyncio.sleep(DEVTOOLS_POLL_INTERVAL)

        browser = Browser(config)
        await browser.start()
        browser._process = process
        browser._process_pid = process.pid
        return browser
    except BaseException:
        await _stop_process(process)
        if not config.uses_custom_data_dir:
            shutil.rmtree(config.user_data_dir, ignore_errors=True)
        raise
