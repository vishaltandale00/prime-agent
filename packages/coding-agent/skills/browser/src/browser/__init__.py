"""In-process Chrome DevTools Protocol client for an existing browser."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from websockets.asyncio.client import ClientConnection, connect as websocket_connect
from websockets.exceptions import ConnectionClosed


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
    except ValueError as error:
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
    target_id = value.get("id")
    title = value.get("title")
    url = value.get("url")
    target_type = value.get("type")
    web_socket_url = value.get("webSocketDebuggerUrl")
    if not all(isinstance(item, str) for item in (target_id, title, url, target_type)):
        raise BrowserProtocolError("Chrome target discovery returned an incomplete target")
    if web_socket_url is not None and not isinstance(web_socket_url, str):
        raise BrowserProtocolError("Chrome target discovery returned an invalid WebSocket endpoint")
    return Target(
        target_id=target_id,
        title=title,
        url=url,
        target_type=target_type,
        web_socket_debugger_url=web_socket_url,
    )


class Browser:
    """HTTP discovery client for one already-running Chrome instance."""

    def __init__(self, endpoint: str, client: httpx.AsyncClient):
        self.endpoint = endpoint
        self._client = client
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
        try:
            response = await self._client.request(method, path)
            response.raise_for_status()
            return response.json()
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPError, ValueError) as error:
            raise BrowserConnectionError(
                f"Chrome CDP endpoint {self.endpoint} did not return a valid response for {path}"
            ) from error

    async def targets(self) -> list[dict[str, Any]]:
        """Return discoverable targets without opening a target WebSocket."""
        discovered = await self._request_json("GET", "/json/list")
        if not isinstance(discovered, list):
            raise BrowserProtocolError("Chrome target discovery did not return a list")
        return [asdict(_target_from_json(item)) for item in discovered]

    async def page(
        self,
        *,
        target_id: str | None = None,
        url_contains: str | None = None,
        title_contains: str | None = None,
    ) -> Page:
        """Open one unique page target selected from the existing Chrome state."""
        selectors = [target_id is not None, url_contains is not None, title_contains is not None]
        if sum(selectors) > 1:
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
        """Close only skill-owned client connections, never Chrome or its tabs."""
        if self._closed:
            return
        self._closed = True
        pages = list(self._pages)
        self._pages.clear()
        if pages:
            await asyncio.gather(*(page.close() for page in pages), return_exceptions=True)
        await self._client.aclose()


class Page:
    """One direct WebSocket connection to an existing Chrome page target."""

    def __init__(self, browser: Browser, target: Target):
        self.browser = browser
        self.target = target
        self._socket: ClientConnection | None = None
        self._command_lock = asyncio.Lock()
        self._next_command_id = 1
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
        parsed = urlsplit(value)
        endpoint = urlsplit(self.browser.endpoint)
        if (
            parsed.scheme != "ws"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != endpoint.port
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.startswith("/devtools/page/")
            or parsed.query
            or parsed.fragment
        ):
            raise BrowserConnectionError("Chrome returned a non-loopback or malformed target WebSocket endpoint")
        return value

    async def _open(self) -> None:
        try:
            self._socket = await websocket_connect(
                self._validated_websocket_url(),
                open_timeout=5,
                close_timeout=1,
                max_size=8 * 1024 * 1024,
            )
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception as error:
            await self.close()
            raise BrowserConnectionError(f"could not attach to Chrome page {self.target.target_id!r}") from error

    async def _command(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._closed or self._socket is None:
            raise BrowserConnectionError("page connection is closed")
        async with self._command_lock:
            command_id = self._next_command_id
            self._next_command_id += 1
            try:
                await self._socket.send(json.dumps({"id": command_id, "method": method, "params": params or {}}))
                while True:
                    message = json.loads(await self._socket.recv())
                    if not isinstance(message, dict) or message.get("id") != command_id:
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
            except asyncio.CancelledError:
                await asyncio.shield(self.close())
                raise
            except BrowserError:
                raise
            except (ConnectionClosed, OSError, ValueError, TypeError) as error:
                await self.close()
                raise BrowserConnectionError(f"Chrome page disconnected during {method}") from error

    async def evaluate(self, expression: str, *, await_promise: bool = True) -> Any:
        """Evaluate JavaScript in the page and return its JSON-serializable value."""
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("expression must be a non-empty string")
        response = await self._command(
            "Runtime.evaluate",
            {
                "expression": expression,
                "awaitPromise": await_promise,
                "returnByValue": True,
                "userGesture": True,
            },
        )
        if "exceptionDetails" in response:
            details = response["exceptionDetails"]
            text = details.get("text") if isinstance(details, dict) else None
            raise BrowserProtocolError(f"page evaluation failed: {text or 'JavaScript exception'}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise BrowserProtocolError("page evaluation returned no remote object")
        if "value" in result:
            return result["value"]
        if "unserializableValue" in result:
            return result["unserializableValue"]
        return None

    async def read_text(self, selector: str = "body") -> str:
        """Read visible text, or text content when visible text is unavailable."""
        _validate_selector(selector)
        expression = f"""(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!element) throw new Error("browser selector did not match an element");
return element.innerText ?? element.textContent ?? "";
}})()"""
        value = await self.evaluate(expression)
        if not isinstance(value, str):
            raise BrowserProtocolError("page text evaluation did not return a string")
        return value

    async def fill(self, selector: str, value: str) -> str:
        """Replace an input value, dispatch input/change events, and return the previous value."""
        _validate_selector(selector)
        if not isinstance(value, str):
            raise TypeError("fill value must be a string")
        expression = f"""(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!element || !("value" in element)) throw new Error("browser selector did not match a fillable element");
const previous = String(element.value);
element.focus();
element.value = {json.dumps(value)};
element.dispatchEvent(new Event("input", {{ bubbles: true }}));
element.dispatchEvent(new Event("change", {{ bubbles: true }}));
return previous;
}})()"""
        previous = await self.evaluate(expression)
        if not isinstance(previous, str):
            raise BrowserProtocolError("page fill evaluation did not return the previous value")
        return previous

    async def click(self, selector: str) -> None:
        """Click one matching element."""
        _validate_selector(selector)
        expression = f"""(() => {{
const element = document.querySelector({json.dumps(selector)});
if (!element || typeof element.click !== "function") throw new Error("browser selector did not match a clickable element");
element.click();
return true;
}})()"""
        if await self.evaluate(expression) is not True:
            raise BrowserProtocolError("page click evaluation did not confirm the action")

    async def navigate(self, url: str) -> dict[str, Any]:
        """Begin navigation to an HTTP(S) URL or about:blank."""
        _validate_navigation_url(url)
        result = await self._command("Page.navigate", {"url": url})
        error_text = result.get("errorText")
        if isinstance(error_text, str) and error_text:
            raise BrowserProtocolError(f"Chrome could not navigate to {url!r}: {error_text}")
        return result

    async def close(self) -> None:
        """Close only this CDP WebSocket, never the target tab."""
        if self._closed:
            return
        self._closed = True
        self.browser._pages.discard(self)
        socket = self._socket
        self._socket = None
        if socket is not None:
            try:
                await socket.close()
            except (ConnectionClosed, OSError):
                pass


async def connect(endpoint: str, *, timeout: float = 5.0) -> Browser:
    """Connect to an explicit loopback Chrome CDP endpoint.

    The caller owns Chrome setup and lifecycle. Closing the returned object
    closes only skill-owned HTTP and WebSocket connections.
    """
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("timeout must be a positive number")
    normalized = _endpoint_url(endpoint)
    client = httpx.AsyncClient(base_url=normalized, timeout=float(timeout), trust_env=False)
    browser = Browser(normalized, client)
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
    if url == "about:blank":
        return
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("navigation URL must use http, https, or about:blank")


__all__ = [
    "Browser",
    "BrowserConnectionError",
    "BrowserError",
    "BrowserProtocolError",
    "BrowserTargetError",
    "Page",
    "Target",
    "connect",
]
