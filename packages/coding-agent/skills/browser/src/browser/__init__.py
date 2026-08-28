"""Dependency-free CDP client for an already-running loopback Chrome."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import struct
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

_MAX_HEADERS = 64 * 1024
_MAX_BODY = 8 * 1024 * 1024
_MAX_FRAME = 8 * 1024 * 1024
_MAX_MESSAGE = 8 * 1024 * 1024
_MAX_EVENTS = 64
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class BrowserError(RuntimeError):
    """Base error for browser attachment and protocol failures."""


class BrowserConnectionError(BrowserError):
    """The configured Chrome endpoint or target connection is unavailable."""


class BrowserProtocolError(BrowserError):
    """Chrome returned a malformed response or a CDP command failed."""


class BrowserTargetError(BrowserError):
    """No unique page matched the requested target selector."""


@dataclass(frozen=True)
class Target:
    """A discoverable Chrome target."""

    target_id: str
    title: str
    url: str
    target_type: str
    web_socket_debugger_url: str | None


def _endpoint_url(endpoint: str) -> str:
    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
    except (AttributeError, ValueError) as error:
        raise ValueError("browser endpoint must be http://127.0.0.1:<port>") from error
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("browser endpoint must be http://127.0.0.1:<port>")
    return f"http://127.0.0.1:{port}"


def _target_from_json(value: Any) -> Target:
    if not isinstance(value, dict):
        raise BrowserProtocolError("Chrome target discovery returned a non-object entry")
    target_id, title, url, target_type = (value.get(key) for key in ("id", "title", "url", "type"))
    websocket_url = value.get("webSocketDebuggerUrl")
    if not all(isinstance(item, str) for item in (target_id, title, url, target_type)):
        raise BrowserProtocolError("Chrome target discovery returned an incomplete target")
    if websocket_url is not None and not isinstance(websocket_url, str):
        raise BrowserProtocolError("Chrome target discovery returned an invalid WebSocket endpoint")
    return Target(target_id, title, url, target_type, websocket_url)


async def _headers(reader: asyncio.StreamReader) -> tuple[str, dict[str, str]]:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
    except asyncio.LimitOverrunError as error:
        raise BrowserProtocolError("Chrome HTTP response headers exceeded the size limit") from error
    except asyncio.IncompleteReadError as error:
        raise BrowserConnectionError("Chrome closed the HTTP connection before responding") from error
    if len(raw) > _MAX_HEADERS:
        raise BrowserProtocolError("Chrome HTTP response headers exceeded the size limit")
    lines = raw[:-4].decode("iso-8859-1").split("\r\n")
    if not lines or not lines[0].startswith("HTTP/1."):
        raise BrowserProtocolError("Chrome returned a malformed HTTP status line")
    result: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            raise BrowserProtocolError("Chrome returned a malformed HTTP response header")
        name, value = line.split(":", 1)
        name = name.strip().lower()
        if not name or name in result:
            raise BrowserProtocolError("Chrome returned duplicate or empty HTTP response headers")
        result[name] = value.strip()
    return lines[0], result


async def _chunked(reader: asyncio.StreamReader) -> bytes:
    body = bytearray()
    while True:
        try:
            line = await reader.readline()
        except ValueError as error:
            raise BrowserProtocolError("Chrome returned malformed chunked HTTP data") from error
        if len(line) > 128 or not line.endswith(b"\r\n"):
            raise BrowserProtocolError("Chrome returned malformed chunked HTTP data")
        try:
            size = int(line[:-2].split(b";", 1)[0], 16)
        except ValueError as error:
            raise BrowserProtocolError("Chrome returned malformed chunked HTTP data") from error
        if size < 0 or len(body) + size > _MAX_BODY:
            raise BrowserProtocolError("Chrome HTTP response body exceeded the size limit")
        if size == 0:
            trailer_bytes = 0
            while True:
                try:
                    trailer = await reader.readline()
                except ValueError as error:
                    raise BrowserProtocolError("Chrome returned malformed chunked HTTP trailers") from error
                trailer_bytes += len(trailer)
                if trailer == b"\r\n":
                    return bytes(body)
                if not trailer or len(trailer) > _MAX_HEADERS or trailer_bytes > _MAX_HEADERS:
                    raise BrowserProtocolError("Chrome returned malformed chunked HTTP trailers")
        try:
            body.extend(await reader.readexactly(size))
            if await reader.readexactly(2) != b"\r\n":
                raise BrowserProtocolError("Chrome returned malformed chunked HTTP data")
        except asyncio.IncompleteReadError as error:
            raise BrowserConnectionError("Chrome closed the HTTP connection before responding") from error


async def _body(reader: asyncio.StreamReader, headers: dict[str, str]) -> bytes:
    encoding = headers.get("transfer-encoding", "").lower()
    if encoding:
        if encoding != "chunked":
            raise BrowserProtocolError("Chrome returned an unsupported HTTP transfer encoding")
        return await _chunked(reader)
    raw_length = headers.get("content-length")
    if raw_length is None:
        result = bytearray()
        while True:
            chunk = await reader.read(min(64 * 1024, _MAX_BODY + 1 - len(result)))
            if not chunk:
                return bytes(result)
            result.extend(chunk)
            if len(result) > _MAX_BODY:
                raise BrowserProtocolError("Chrome HTTP response body exceeded the size limit")
    try:
        length = int(raw_length)
    except ValueError as error:
        raise BrowserProtocolError("Chrome returned an invalid HTTP content length") from error
    if length < 0 or length > _MAX_BODY:
        raise BrowserProtocolError("Chrome HTTP response body exceeded the size limit")
    try:
        return await reader.readexactly(length)
    except asyncio.IncompleteReadError as error:
        raise BrowserConnectionError("Chrome closed the HTTP connection before responding") from error


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        async with asyncio.timeout(1):
            await writer.wait_closed()
    except (TimeoutError, ConnectionError, OSError):
        pass


class _WebSocket:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, timeout: float):
        self.reader = reader
        self.writer = writer
        self.timeout = timeout
        self.write_lock = asyncio.Lock()
        self.closed = False

    @classmethod
    async def connect(cls, url: str, timeout: float) -> _WebSocket:
        parsed = urlsplit(url)
        assert parsed.port is not None
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection("127.0.0.1", parsed.port, limit=_MAX_HEADERS)
                key = base64.b64encode(os.urandom(16)).decode("ascii")
                request = (
                    f"GET {parsed.path} HTTP/1.1\r\nHost: 127.0.0.1:{parsed.port}\r\n"
                    f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                    "Sec-WebSocket-Version: 13\r\n\r\n"
                ).encode("ascii")
                writer.write(request)
                await writer.drain()
                status, headers = await _headers(reader)
                expected = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
                connection = {item.strip().lower() for item in headers.get("connection", "").split(",")}
                if (
                    status.split(" ", 2)[1:2] != ["101"]
                    or headers.get("upgrade", "").lower() != "websocket"
                    or "upgrade" not in connection
                    or headers.get("sec-websocket-accept") != expected
                ):
                    raise BrowserProtocolError("Chrome rejected or malformed the WebSocket upgrade")
            return cls(reader, writer, timeout)
        except asyncio.CancelledError:
            if writer:
                await asyncio.shield(_close_writer(writer))
            raise
        except (BrowserError, OSError, TimeoutError) as error:
            if writer:
                await _close_writer(writer)
            if isinstance(error, BrowserError):
                raise
            raise BrowserConnectionError("could not open the Chrome WebSocket connection") from error

    async def _write(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            raise BrowserConnectionError("Chrome WebSocket connection is closed")
        if len(payload) > _MAX_FRAME:
            raise BrowserProtocolError("Chrome WebSocket frame exceeded the size limit")
        length = len(payload)
        if length < 126:
            header = bytes((0x80 | opcode, 0x80 | length))
        elif length <= 0xFFFF:
            header = bytes((0x80 | opcode, 0x80 | 126)) + struct.pack("!H", length)
        else:
            header = bytes((0x80 | opcode, 0x80 | 127)) + struct.pack("!Q", length)
        mask = os.urandom(4)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        async with self.write_lock:
            try:
                async with asyncio.timeout(self.timeout):
                    self.writer.write(header + mask + masked)
                    await self.writer.drain()
            except asyncio.CancelledError:
                raise
            except (ConnectionError, OSError, TimeoutError) as error:
                raise BrowserConnectionError("Chrome WebSocket write failed") from error

    async def send_text(self, value: str) -> None:
        payload = value.encode()
        if len(payload) > _MAX_MESSAGE:
            raise BrowserProtocolError("Chrome WebSocket message exceeded the size limit")
        await self._write(1, payload)

    async def _read(self) -> tuple[bool, int, bytes]:
        try:
            async with asyncio.timeout(self.timeout):
                first, second = await self.reader.readexactly(2)
                final, opcode = bool(first & 0x80), first & 0x0F
                if first & 0x70:
                    raise BrowserProtocolError("Chrome WebSocket frame used unsupported extensions")
                if second & 0x80:
                    raise BrowserProtocolError("Chrome sent an invalid masked WebSocket frame")
                length = second & 0x7F
                if length == 126:
                    length = struct.unpack("!H", await self.reader.readexactly(2))[0]
                elif length == 127:
                    encoded = await self.reader.readexactly(8)
                    if encoded[0] & 0x80:
                        raise BrowserProtocolError("Chrome sent an invalid WebSocket frame length")
                    length = struct.unpack("!Q", encoded)[0]
                if length > _MAX_FRAME:
                    raise BrowserProtocolError("Chrome WebSocket frame exceeded the size limit")
                if opcode >= 8 and (not final or length > 125):
                    raise BrowserProtocolError("Chrome sent a malformed WebSocket control frame")
                return final, opcode, await self.reader.readexactly(length)
        except asyncio.CancelledError:
            raise
        except BrowserError:
            raise
        except asyncio.IncompleteReadError as error:
            raise BrowserConnectionError("Chrome closed the WebSocket connection") from error
        except (ConnectionError, OSError, TimeoutError) as error:
            raise BrowserConnectionError("Chrome WebSocket read failed or timed out") from error

    async def receive_text(self) -> str:
        message, initial = bytearray(), None
        while True:
            final, opcode, payload = await self._read()
            if opcode == 8:
                if len(payload) == 1:
                    raise BrowserProtocolError("Chrome sent a malformed WebSocket close frame")
                if len(payload) >= 2:
                    code = struct.unpack("!H", payload[:2])[0]
                    if code < 1000 or code >= 5000 or code in {1004, 1005, 1006, 1015}:
                        raise BrowserProtocolError("Chrome sent a forbidden WebSocket close status code")
                    try:
                        payload[2:].decode()
                    except UnicodeDecodeError as error:
                        raise BrowserProtocolError("Chrome sent a malformed WebSocket close reason") from error
                try:
                    await self._write(8, payload)
                except BrowserError:
                    pass
                self.closed = True
                await _close_writer(self.writer)
                raise BrowserConnectionError("Chrome closed the WebSocket connection")
            if opcode == 9:
                await self._write(10, payload)
                continue
            if opcode == 10:
                continue
            if opcode in (1, 2):
                if initial is not None:
                    raise BrowserProtocolError("Chrome interleaved fragmented WebSocket messages")
                initial = opcode
            elif opcode == 0:
                if initial is None:
                    raise BrowserProtocolError("Chrome sent an unexpected WebSocket continuation")
            else:
                raise BrowserProtocolError("Chrome sent an unsupported WebSocket opcode")
            message.extend(payload)
            if len(message) > _MAX_MESSAGE:
                raise BrowserProtocolError("Chrome WebSocket message exceeded the size limit")
            if final:
                if initial != 1:
                    raise BrowserProtocolError("Chrome sent a non-text CDP WebSocket message")
                try:
                    return message.decode()
                except UnicodeDecodeError as error:
                    raise BrowserProtocolError("Chrome sent a non-UTF-8 CDP WebSocket message") from error

    async def close(self) -> None:
        if self.closed:
            return
        try:
            await self._write(8, struct.pack("!H", 1000))
        except BrowserError:
            pass
        self.closed = True
        await _close_writer(self.writer)


class Browser:
    """HTTP discovery client for one already-running Chrome instance."""

    def __init__(self, endpoint: str, timeout: float):
        self.endpoint, self._timeout = endpoint, timeout
        self._pages: set[Page] = set()
        self._closed = False

    async def __aenter__(self) -> Browser:
        self._ensure_open()
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrowserConnectionError("browser connection is closed")

    async def _request_json(self, method: str, path: str) -> Any:
        self._ensure_open()
        port = urlsplit(self.endpoint).port
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(self._timeout):
                reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=_MAX_HEADERS)
                writer.write(
                    f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n".encode()
                )
                await writer.drain()
                status, headers = await _headers(reader)
                response = await _body(reader, headers)
            parts = status.split(" ", 2)
            if len(parts) < 2 or not parts[1].isdigit() or not 200 <= int(parts[1]) < 300:
                raise BrowserConnectionError(f"Chrome CDP endpoint returned HTTP {parts[1] if len(parts) > 1 else 'error'}")
            try:
                return json.loads(response.decode())
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise BrowserProtocolError("Chrome CDP endpoint returned invalid JSON") from error
        except asyncio.CancelledError:
            raise
        except BrowserError:
            raise
        except (OSError, TimeoutError) as error:
            raise BrowserConnectionError(
                f"Chrome CDP endpoint {self.endpoint} did not return a valid response for {path}"
            ) from error
        finally:
            if writer:
                await asyncio.shield(_close_writer(writer))

    async def targets(self) -> list[dict[str, Any]]:
        """Return discoverable targets without opening a target WebSocket."""
        discovered = await self._request_json("GET", "/json/list")
        if not isinstance(discovered, list):
            raise BrowserProtocolError("Chrome target discovery did not return a list")
        return [asdict(_target_from_json(item)) for item in discovered]

    async def page(self, *, target_id: str | None = None, url_contains: str | None = None, title_contains: str | None = None) -> Page:
        """Open one unique page target selected from the existing Chrome state."""
        if sum(value is not None for value in (target_id, url_contains, title_contains)) > 1:
            raise ValueError("choose only one of target_id, url_contains, or title_contains")
        targets = [_target_from_json(item) for item in await self._request_json("GET", "/json/list")]
        pages = [target for target in targets if target.target_type == "page"]
        if target_id is not None:
            pages = [target for target in pages if target.target_id == target_id]
        elif url_contains is not None:
            pages = [target for target in pages if url_contains in target.url]
        elif title_contains is not None:
            pages = [target for target in pages if title_contains in target.title]
        if len(pages) != 1:
            selector = target_id or url_contains or title_contains or "the only open page"
            raise BrowserTargetError(f"expected one page matching {selector!r}, found {len(pages)}")
        page = Page(self, pages[0])
        self._pages.add(page)
        try:
            await page._open()
        except BaseException:
            self._pages.discard(page)
            raise
        return page

    async def new_page(self, url: str = "about:blank") -> Page:
        """Ask Chrome to create a tab, then attach to that page target."""
        _validate_navigation_url(url)
        target = _target_from_json(await self._request_json("PUT", f"/json/new?{quote(url, safe='')}"))
        if target.target_type != "page":
            raise BrowserProtocolError("Chrome created a non-page target")
        page = Page(self, target)
        self._pages.add(page)
        try:
            await page._open()
        except BaseException:
            self._pages.discard(page)
            raise
        return page

    async def close(self) -> None:
        """Close only skill-owned connections, never Chrome or its tabs."""
        if self._closed:
            return
        self._closed = True
        pages, self._pages = list(self._pages), set()
        if pages:
            await asyncio.gather(*(page.close() for page in pages), return_exceptions=True)


class Page:
    """One direct WebSocket connection to an existing Chrome page target."""

    def __init__(self, browser: Browser, target: Target):
        self.browser, self.target = browser, target
        self._socket: _WebSocket | None = None
        self._command_lock = asyncio.Lock()
        self._next_command_id = 1
        self._events: list[dict[str, Any]] = []
        self._closed = False

    async def __aenter__(self) -> Page:
        if self._closed:
            raise BrowserConnectionError("page connection is closed")
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def _validated_websocket_url(self) -> str:
        value = self.target.web_socket_debugger_url
        if value is None:
            raise BrowserTargetError(f"page target {self.target.target_id!r} has no WebSocket endpoint")
        parsed, endpoint = urlsplit(value), urlsplit(self.browser.endpoint)
        if (
            parsed.scheme != "ws"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != endpoint.port
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != f"/devtools/page/{self.target.target_id}"
            or parsed.query
            or parsed.fragment
        ):
            raise BrowserConnectionError("Chrome returned a non-loopback or malformed target WebSocket endpoint")
        return value

    async def _open(self) -> None:
        try:
            self._socket = await _WebSocket.connect(self._validated_websocket_url(), self.browser._timeout)
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception as error:
            await self.close()
            raise BrowserConnectionError(f"could not attach to Chrome page {self.target.target_id!r}") from error

    def _remember_event(self, message: dict[str, Any]) -> None:
        if not isinstance(message.get("method"), str):
            return
        self._events.append(message)
        if len(self._events) > _MAX_EVENTS:
            del self._events[: len(self._events) - _MAX_EVENTS]

    async def _command_unlocked(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._closed or self._socket is None:
            raise BrowserConnectionError("page connection is closed")
        command_id = self._next_command_id
        self._next_command_id += 1
        await self._socket.send_text(json.dumps({"id": command_id, "method": method, "params": params or {}}))
        while True:
            message = json.loads(await self._socket.receive_text())
            if not isinstance(message, dict):
                continue
            if message.get("id") != command_id:
                self._remember_event(message)
                continue
            error = message.get("error")
            if isinstance(error, dict):
                detail = error.get("message")
                raise BrowserProtocolError(
                    f"Chrome CDP command {method} failed: {detail if isinstance(detail, str) else 'unknown error'}"
                )
            result = message.get("result")
            if not isinstance(result, dict):
                raise BrowserProtocolError(f"Chrome CDP command {method} returned no result")
            return result

    async def _event_unlocked(self, method: str, matches: Callable[[Any], bool]) -> dict[str, Any]:
        for index, message in enumerate(self._events):
            if message.get("method") == method and matches(message.get("params")):
                return self._events.pop(index)
        if self._socket is None:
            raise BrowserConnectionError("page connection is closed")
        while True:
            message = json.loads(await self._socket.receive_text())
            if not isinstance(message, dict):
                continue
            if message.get("method") == method and matches(message.get("params")):
                return message
            self._remember_event(message)

    async def _command(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        async with self._command_lock:
            try:
                async with asyncio.timeout(self.browser._timeout):
                    return await self._command_unlocked(method, params)
            except asyncio.CancelledError:
                await asyncio.shield(self.close())
                raise
            except BrowserProtocolError:
                await self.close()
                raise
            except BrowserConnectionError as error:
                await self.close()
                raise BrowserConnectionError(f"Chrome page disconnected during {method}") from error
            except TimeoutError as error:
                await self.close()
                raise BrowserConnectionError(f"Chrome page disconnected during {method}") from error
            except (ValueError, TypeError) as error:
                await self.close()
                raise BrowserConnectionError(f"Chrome page disconnected during {method}") from error

    async def evaluate(self, expression: str, *, await_promise: bool = True) -> Any:
        """Evaluate JavaScript in the page and return its JSON-serializable value."""
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("expression must be a non-empty string")
        response = await self._command("Runtime.evaluate", {
            "expression": expression, "awaitPromise": await_promise, "returnByValue": True, "userGesture": True,
        })
        if "exceptionDetails" in response:
            details = response["exceptionDetails"]
            text = details.get("text") if isinstance(details, dict) else None
            raise BrowserProtocolError(f"page evaluation failed: {text or 'JavaScript exception'}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise BrowserProtocolError("page evaluation returned no remote object")
        return result.get("value", result.get("unserializableValue"))

    async def read_text(self, selector: str = "body") -> str:
        """Read visible text, or text content when visible text is unavailable."""
        _validate_selector(selector)
        value = await self.evaluate(f'''(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!element) throw new Error("browser selector did not match an element");
return element.innerText ?? element.textContent ?? "";
}})()''')
        if not isinstance(value, str):
            raise BrowserProtocolError("page text evaluation did not return a string")
        return value

    async def fill(self, selector: str, value: str) -> str:
        """Replace and verify an input value, then disconnect this terminal page action."""
        _validate_selector(selector)
        if not isinstance(value, str):
            raise TypeError("fill value must be a string")
        try:
            result = await self.evaluate(f'''(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!element || !("value" in element)) throw new Error("browser selector did not match a fillable element");
const previous = String(element.value);
element.focus(); element.value = {json.dumps(value)};
element.dispatchEvent(new Event("input", {{ bubbles: true }}));
element.dispatchEvent(new Event("change", {{ bubbles: true }}));
return {{ previous, current: String(element.value) }};
}})()''')
            if not isinstance(result, dict):
                raise BrowserProtocolError("page fill evaluation did not return its value receipt")
            previous = result.get("previous")
            current = result.get("current")
            if not isinstance(previous, str) or not isinstance(current, str):
                raise BrowserProtocolError("page fill evaluation returned an invalid value receipt")
            if current != value:
                raise BrowserProtocolError("page rejected or sanitized the requested fill value")
            return previous
        finally:
            await asyncio.shield(self.close())

    async def click(self, selector: str) -> None:
        """Click one matching element, then disconnect this terminal page action."""
        _validate_selector(selector)
        try:
            confirmed = await self.evaluate(f'''(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!(element instanceof HTMLElement) || element.matches(":disabled") || element.getAttribute("aria-disabled") === "true") {{
    throw new Error("browser selector did not match an enabled clickable element");
}}
let dispatched = false;
const observe = () => {{ dispatched = true; }};
element.addEventListener("click", observe, {{ capture: true }});
try {{ HTMLElement.prototype.click.call(element); }} finally {{ element.removeEventListener("click", observe, {{ capture: true }}); }}
return dispatched;
}})()''')
            if confirmed is not True:
                raise BrowserProtocolError("page click evaluation did not confirm the action")
        finally:
            await asyncio.shield(self.close())

    async def navigate(self, url: str) -> dict[str, Any]:
        """Navigate and wait for correlated main-frame completion."""
        _validate_navigation_url(url)
        async with self._command_lock:
            try:
                async with asyncio.timeout(self.browser._timeout):
                    await self._command_unlocked("Page.enable")
                    await self._command_unlocked("Page.setLifecycleEventsEnabled", {"enabled": True})
                    self._events = [
                        event for event in self._events
                        if event.get("method") not in {"Page.lifecycleEvent", "Page.navigatedWithinDocument"}
                    ]
                    result = await self._command_unlocked("Page.navigate", {"url": url})
                    if isinstance(result.get("errorText"), str) and result["errorText"]:
                        raise BrowserProtocolError(f"Chrome could not navigate to {url!r}: {result['errorText']}")
                    frame_id = result.get("frameId")
                    if not isinstance(frame_id, str) or not frame_id:
                        raise BrowserProtocolError("Chrome navigation returned no frame identity")
                    loader_id = result.get("loaderId")
                    if isinstance(loader_id, str) and loader_id:
                        await self._event_unlocked(
                            "Page.lifecycleEvent",
                            lambda params: isinstance(params, dict)
                            and params.get("name") == "load"
                            and params.get("frameId") == frame_id
                            and params.get("loaderId") == loader_id,
                        )
                    else:
                        normalized_url = _normalized_navigation_url(url)
                        await self._event_unlocked(
                            "Page.navigatedWithinDocument",
                            lambda params: isinstance(params, dict)
                            and params.get("frameId") == frame_id
                            and isinstance(params.get("url"), str)
                            and _normalized_navigation_url(params["url"]) == normalized_url,
                        )
                    return result
            except asyncio.CancelledError:
                await asyncio.shield(self.close())
                raise
            except BrowserProtocolError:
                await self.close()
                raise
            except BrowserConnectionError as error:
                await self.close()
                raise BrowserConnectionError("Chrome page disconnected during Page.navigate") from error
            except TimeoutError as error:
                await self.close()
                raise BrowserConnectionError("Chrome page disconnected during Page.navigate") from error
            except (ValueError, TypeError) as error:
                await self.close()
                raise BrowserConnectionError("Chrome page disconnected during Page.navigate") from error

    async def close(self) -> None:
        """Close only this CDP WebSocket, never the target tab."""
        if self._closed:
            return
        self._closed = True
        self.browser._pages.discard(self)
        socket, self._socket = self._socket, None
        if socket:
            await socket.close()


async def connect(endpoint: str, *, timeout: float = 5.0) -> Browser:
    """Connect to an explicit loopback Chrome CDP endpoint without owning Chrome."""
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("timeout must be a positive number")
    browser = Browser(_endpoint_url(endpoint), float(timeout))
    try:
        await browser.targets()
    except BaseException:
        await browser.close()
        raise
    return browser


def _validate_selector(selector: str) -> None:
    if not isinstance(selector, str) or not selector.strip():
        raise ValueError("selector must be a non-empty string")


def _validate_navigation_url(url: str) -> None:
    if not isinstance(url, str) or not url:
        raise ValueError("navigation URL must be a non-empty string")
    parsed = urlsplit(url)
    if url != "about:blank" and (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or _normalized_navigation_url(url) is None
    ):
        raise ValueError("navigation URL must use http, https, or about:blank")


def _normalized_navigation_url(url: str) -> str | None:
    if url == "about:blank":
        return url
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (AttributeError, ValueError):
        return None
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https") or not parsed.hostname:
        return None
    netloc = parsed.netloc.lower()
    if port == (443 if scheme == "https" else 80):
        netloc = netloc.rsplit(":", 1)[0]
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, parsed.fragment))


__all__ = ["Browser", "BrowserConnectionError", "BrowserError", "BrowserProtocolError", "BrowserTargetError", "Page", "Target", "connect"]
