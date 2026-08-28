import { mkdirSync, readFileSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runInNewContext } from "node:vm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { type WebSocket, WebSocketServer } from "ws";
import { getBundledSkillsDir } from "../src/config.js";
import type { PythonSkillRuntimeInfo } from "../src/core/skills.js";
import { IpythonKernelProvisioner } from "../src/core/tools/ipython.js";

function bundledBrowserSkill(): PythonSkillRuntimeInfo {
	const packagePath = join(getBundledSkillsDir(), "browser");
	return {
		name: "browser",
		importName: "browser",
		packagePath,
		pyprojectPath: join(packagePath, "pyproject.toml"),
	};
}

class DomHTMLElement {
	disabled = false;
	clickCount = 0;
	readonly listeners = new Set<() => void>();

	matches(selector: string): boolean {
		return selector === ":disabled" && this.disabled;
	}

	getAttribute(_name: string): string | null {
		return null;
	}

	addEventListener(type: string, listener: () => void): void {
		if (type === "click") this.listeners.add(listener);
	}

	removeEventListener(type: string, listener: () => void): void {
		if (type === "click") this.listeners.delete(listener);
	}

	click(): void {
		if (this.disabled) return;
		this.clickCount += 1;
		for (const listener of [...this.listeners]) listener();
	}
}

class FakeChromeCdp {
	readonly server: Server;
	readonly sockets = new Set<WebSocket>();
	readonly webSocketServer = new WebSocketServer({ noServer: true });
	port = 0;
	inputValue = "before";
	clickCount = 0;
	connectionCount = 0;
	closeCount = 0;
	newPageRequests = 0;
	disabledClick = false;
	stallNavigationCompletion = false;
	sameDocumentNavigation = false;
	navigationRequests = 0;
	readonly evaluatedExpressions: string[] = [];
	private pendingNavigation: { socket: WebSocket; frameId: string; loaderId?: string } | undefined;

	constructor() {
		this.server = createServer((request, response) => {
			const targets = [
				{
					id: "page-existing",
					title: "Existing session",
					type: "page",
					url: "https://example.test/existing",
					webSocketDebuggerUrl: `ws://127.0.0.1:${this.port}/devtools/page/page-existing`,
				},
				{ id: "worker", title: "Worker", type: "service_worker", url: "https://example.test/worker" },
			];
			if (request.method === "GET" && (request.url === "/json/list" || request.url === "/json")) {
				response.writeHead(200, { "content-type": "application/json" });
				response.end(JSON.stringify(targets));
				return;
			}
			if (request.method === "PUT" && request.url?.startsWith("/json/new?")) {
				this.newPageRequests += 1;
				response.writeHead(200, { "content-type": "application/json" });
				response.end(
					JSON.stringify({
						...targets[0],
						id: "page-new",
						webSocketDebuggerUrl: `ws://127.0.0.1:${this.port}/devtools/page/page-new`,
					}),
				);
				return;
			}
			response.writeHead(404);
			response.end();
		});
		this.server.on("upgrade", (request, socket, head) => {
			if (request.url !== "/devtools/page/page-existing" && request.url !== "/devtools/page/page-new") {
				socket.destroy();
				return;
			}
			this.webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
				this.webSocketServer.emit("connection", webSocket, request);
			});
		});
		this.webSocketServer.on("connection", (socket) => {
			this.connectionCount += 1;
			this.sockets.add(socket);
			socket.once("close", () => {
				this.closeCount += 1;
				this.sockets.delete(socket);
			});
			socket.on("message", (data) => {
				const request = JSON.parse(data.toString()) as {
					id: number;
					method: string;
					params?: { expression?: string; url?: string };
				};
				const expression = request.params?.expression ?? "";
				if (request.method === "Page.enable" || request.method === "Page.setLifecycleEventsEnabled") {
					socket.send(JSON.stringify({ id: request.id, result: {} }));
					return;
				}
				if (request.method === "Page.navigate") {
					this.navigationRequests += 1;
					const frameId = "frame-1";
					const loaderId = this.sameDocumentNavigation ? undefined : "loader-new";
					socket.send(
						JSON.stringify({
							method: "Page.lifecycleEvent",
							params: { frameId, loaderId: "loader-old", name: "load" },
						}),
					);
					this.pendingNavigation = { socket, frameId, loaderId };
					if (this.sameDocumentNavigation) this.completeNavigation();
					socket.send(JSON.stringify({ id: request.id, result: { frameId, ...(loaderId ? { loaderId } : {}) } }));
					if (!this.sameDocumentNavigation && !this.stallNavigationCompletion)
						queueMicrotask(() => this.completeNavigation());
					return;
				}
				if (expression === "hold") return;
				if (expression === "drip") {
					const timer = setInterval(() => {
						if (socket.readyState !== socket.OPEN) return;
						socket.send(JSON.stringify({ id: request.id + 1000, result: {} }));
						socket.ping("drip");
					}, 10);
					socket.once("close", () => clearInterval(timer));
					return;
				}
				if (expression === "forbidden-close") {
					const rawSocket = (socket as unknown as { _socket: { write(data: Buffer): void } })._socket;
					rawSocket.write(Buffer.from([0x88, 0x02, 0x03, 0xed]));
					return;
				}
				if (expression === "oversize") {
					socket.send("x".repeat(8 * 1024 * 1024 + 1));
					return;
				}
				if (expression === "fragmented") {
					const message = JSON.stringify({
						id: request.id,
						result: { result: { type: "string", value: "fragmented-ok" } },
					});
					const split = Math.floor(message.length / 2);
					socket.send(message.slice(0, split), { fin: false });
					socket.send(message.slice(split), { fin: true });
					return;
				}
				if (expression === "ping") {
					socket.once("pong", (payload) => {
						if (payload.toString() !== "fixture-ping") return;
						socket.send(
							JSON.stringify({ id: request.id, result: { result: { type: "string", value: "pong-ok" } } }),
						);
					});
					socket.ping("fixture-ping");
					return;
				}
				if (expression === "disconnect") {
					socket.close();
					return;
				}
				if (expression === "throw") {
					socket.send(
						JSON.stringify({
							id: request.id,
							result: { exceptionDetails: { text: "fixture exception" }, result: { type: "undefined" } },
						}),
					);
					return;
				}
				this.evaluatedExpressions.push(expression);
				let value: unknown = true;
				if (expression.includes("innerText")) value = "existing marker";
				if (expression.includes("const previous")) {
					const previous = this.inputValue;
					const match = expression.match(/element\.value = ("(?:[^"\\]|\\.)*");/);
					if (!match) throw new Error("fill expression did not contain a JSON string value");
					this.inputValue = JSON.parse(match[1]!) as string;
					value = previous;
				}
				if (expression.includes("enabled clickable element")) {
					const element = new DomHTMLElement();
					element.disabled = this.disabledClick;
					try {
						value = runInNewContext(expression, {
							document: { querySelector: () => element },
							HTMLElement: DomHTMLElement,
						});
						this.clickCount += element.clickCount;
					} catch {
						socket.send(
							JSON.stringify({
								id: request.id,
								result: {
									exceptionDetails: { text: "fixture click exception" },
									result: { type: "undefined" },
								},
							}),
						);
						return;
					}
				}
				socket.send(
					JSON.stringify({
						id: request.id,
						result: { result: { type: typeof value, value } },
					}),
				);
			});
		});
	}

	completeNavigation(): void {
		const pending = this.pendingNavigation;
		if (!pending || pending.socket.readyState !== pending.socket.OPEN) return;
		this.pendingNavigation = undefined;
		if (pending.loaderId) {
			pending.socket.send(
				JSON.stringify({
					method: "Page.lifecycleEvent",
					params: { frameId: pending.frameId, loaderId: pending.loaderId, name: "load" },
				}),
			);
			return;
		}
		pending.socket.send(
			JSON.stringify({
				method: "Page.navigatedWithinDocument",
				params: { frameId: pending.frameId, url: "https://example.test/existing#next" },
			}),
		);
	}

	async start(): Promise<void> {
		await new Promise<void>((resolve, reject) => {
			this.server.once("error", reject);
			this.server.listen(0, "127.0.0.1", () => resolve());
		});
		const address = this.server.address();
		if (address === null || typeof address === "string") throw new Error("fake CDP server did not bind TCP");
		this.port = address.port;
	}

	async close(): Promise<void> {
		for (const socket of this.sockets) socket.terminate();
		await new Promise<void>((resolve) => this.webSocketServer.close(() => resolve()));
		await new Promise<void>((resolve, reject) => {
			this.server.close((error) => (error ? reject(error) : resolve()));
		});
	}
}

describe("browser skill over a fake loopback CDP endpoint", { tags: ["kernel-heavy"] }, () => {
	let tempDir: string;
	let provisioner: IpythonKernelProvisioner | undefined;
	let chrome: FakeChromeCdp;

	beforeEach(async () => {
		tempDir = join(tmpdir(), `pi-browser-skill-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });
		chrome = new FakeChromeCdp();
		await chrome.start();
		provisioner = new IpythonKernelProvisioner(tempDir, { pythonSkills: [bundledBrowserSkill()] });
	});

	afterEach(async () => {
		await provisioner?.dispose();
		provisioner = undefined;
		await chrome.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("ships no runtime PyPI dependency closure", () => {
		const packagePath = join(getBundledSkillsDir(), "browser");
		const pyproject = readFileSync(join(packagePath, "pyproject.toml"), "utf8");
		const source = readFileSync(join(packagePath, "src", "browser", "__init__.py"), "utf8");
		expect(pyproject).toContain("dependencies = []");
		expect(pyproject).toContain('requires-python = ">=3.11"');
		expect(source).not.toMatch(/^import (?:httpx|websockets)$/m);
		expect(source).not.toMatch(/^from (?:httpx|websockets)/m);
	});

	it("discovers existing state, performs a reversible action, and closes only its client", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
import json
endpoint = "http://127.0.0.1:${chrome.port}"
async with await browser.connect(endpoint) as chrome:
    targets = await chrome.targets()
    async with await chrome.page(title_contains="Existing") as page:
        observed = await page.read_text("#marker")
        previous = await page.fill("#name", "after")
        restored_from = await page.fill("#name", previous)
        await page.click("#action")
        navigation = await page.navigate("https://example.test/next")
        page_closed_inside = page.closed
    page_closed_after = page.closed
    browser_closed_inside = chrome.closed
browser_closed_after = chrome.closed
print(json.dumps({
    "targets": targets,
    "observed": observed,
    "previous": previous,
    "restored_from": restored_from,
    "navigation": navigation,
    "page_closed_inside": page_closed_inside,
    "page_closed_after": page_closed_after,
    "browser_closed_inside": browser_closed_inside,
    "browser_closed_after": browser_closed_after,
}, sort_keys=True))
`);

		expect(result.status).toBe("ok");
		expect(JSON.parse(result.stdout.trim())).toMatchObject({
			observed: "existing marker",
			previous: "before",
			restored_from: "after",
			navigation: { frameId: "frame-1" },
			page_closed_inside: false,
			page_closed_after: true,
			browser_closed_inside: false,
			browser_closed_after: true,
			targets: [
				{ target_id: "page-existing", title: "Existing session", target_type: "page" },
				{ target_id: "worker", target_type: "service_worker", web_socket_debugger_url: null },
			],
		});
		expect(chrome.inputValue).toBe("before");
		expect(chrome.clickCount).toBe(1);
		expect(chrome.connectionCount).toBe(1);
		await expect.poll(() => chrome.closeCount).toBe(1);
	});

	it("rejects a disabled click through the production page expression", async () => {
		chrome.disabledClick = true;
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
async with await browser.connect("http://127.0.0.1:${chrome.port}") as chrome:
    async with await chrome.page(target_id="page-existing") as page:
        try:
            await page.click("#disabled")
        except Exception as error:
            print(type(error).__name__, str(error))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("BrowserProtocolError page evaluation failed: fixture click exception");
		expect(chrome.clickCount).toBe(0);
		await expect.poll(() => chrome.closeCount).toBe(1);
	});

	it("waits for the exact navigation loader before allowing a later read", async () => {
		chrome.stallNavigationCompletion = true;
		const manager = await provisioner!.ensure();
		const execution = manager.execute(`
async with await browser.connect("http://127.0.0.1:${chrome.port}") as chrome:
    async with await chrome.page(target_id="page-existing") as page:
        navigation = await page.navigate("https://example.test/next")
        observed = await page.read_text("#marker")
        print(navigation["loaderId"], observed)
`);

		await expect.poll(() => chrome.navigationRequests).toBe(1);
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(chrome.evaluatedExpressions.some((expression) => expression.includes("innerText"))).toBe(false);
		chrome.completeNavigation();

		const result = await execution;
		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("loader-new existing marker");
		expect(chrome.evaluatedExpressions.some((expression) => expression.includes("innerText"))).toBe(true);
	});

	it("accepts a same-document navigation event buffered before the command response", async () => {
		chrome.sameDocumentNavigation = true;
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
async with await browser.connect("http://127.0.0.1:${chrome.port}") as chrome:
    async with await chrome.page(target_id="page-existing") as page:
        navigation = await page.navigate("https://example.test/existing#next")
        print(navigation["frameId"], await page.read_text("#marker"))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("frame-1 existing marker");
	});

	it("rejects non-loopback endpoints and reports target and protocol failures honestly", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
endpoint = "http://127.0.0.1:${chrome.port}"
errors = []
for invalid in ("http://localhost:${chrome.port}", "https://127.0.0.1:${chrome.port}", "http://127.0.0.1:${chrome.port}/secret"):
    try:
        await browser.connect(invalid)
    except Exception as error:
        errors.append(type(error).__name__ + ": " + str(error))
async with await browser.connect(endpoint) as chrome:
    try:
        await chrome.page(title_contains="Missing")
    except Exception as error:
        errors.append(type(error).__name__ + ": " + str(error))
    async with await chrome.page(target_id="page-existing") as page:
        try:
            await page.evaluate("throw")
        except Exception as error:
            errors.append(type(error).__name__ + ": " + str(error))
print("\\n".join(errors))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("ValueError: browser endpoint must be http://127.0.0.1:<port>");
		expect(result.stdout).toContain("BrowserTargetError: expected one page matching 'Missing', found 0");
		expect(result.stdout).toContain("BrowserProtocolError: page evaluation failed: fixture exception");
	});

	it("closes the page connection when an in-flight command is cancelled or disconnected", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
endpoint = "http://127.0.0.1:${chrome.port}"
async with await browser.connect(endpoint) as chrome:
    page = await chrome.page(target_id="page-existing")
    pending = asyncio.create_task(page.evaluate("hold"))
    await asyncio.sleep(0.05)
    pending.cancel()
    try:
        await pending
    except asyncio.CancelledError:
        pass
    print("cancelled", page.closed)
    page = await chrome.page(target_id="page-existing")
    try:
        await page.evaluate("disconnect")
    except browser.BrowserConnectionError as error:
        print("disconnected", page.closed, str(error))
    page = await chrome.page(target_id="page-existing")
    page.browser._timeout = 0.05
    try:
        await page.evaluate("hold")
    except browser.BrowserConnectionError as error:
        print("timed-out", page.closed, str(error))
    page = await chrome.page(target_id="page-existing")
    try:
        await page.evaluate("drip")
    except browser.BrowserConnectionError as error:
        print("drip-timed-out", page.closed, str(error))
    page.browser._timeout = 5
    page = await chrome.page(target_id="page-existing")
    try:
        await page.evaluate("oversize")
    except browser.BrowserProtocolError as error:
        print("oversized", page.closed, str(error))
    page = await chrome.page(target_id="page-existing")
    try:
        await page.evaluate("forbidden-close")
    except browser.BrowserProtocolError as error:
        print("forbidden-close", page.closed, str(error))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("cancelled True");
		expect(result.stdout).toContain("disconnected True Chrome page disconnected during Runtime.evaluate");
		expect(result.stdout).toContain("timed-out True Chrome page disconnected during Runtime.evaluate");
		expect(result.stdout).toContain("drip-timed-out True Chrome page disconnected during Runtime.evaluate");
		expect(result.stdout).toContain("oversized True Chrome WebSocket frame exceeded the size limit");
		expect(result.stdout).toContain("forbidden-close True Chrome sent a forbidden WebSocket close status code");
		expect(chrome.connectionCount).toBe(6);
		await expect.poll(() => chrome.closeCount).toBe(6);
	});

	it("handles fragmented messages and ping/pong without weakening the loopback page boundary", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
endpoint = "http://127.0.0.1:${chrome.port}"
async with await browser.connect(endpoint, timeout=1) as chrome:
    async with await chrome.page(target_id="page-existing") as page:
        print(await page.evaluate("fragmented"))
        print(await page.evaluate("ping"))
target = browser.Target("wrong-page", "Wrong", "about:blank", "page", "ws://127.0.0.1:${chrome.port}/devtools/page/page-existing")
try:
    browser.Page(chrome, target)._validated_websocket_url()
except Exception as error:
    print(type(error).__name__, str(error))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("fragmented-ok");
		expect(result.stdout).toContain("pong-ok");
		expect(result.stdout).toContain(
			"BrowserConnectionError Chrome returned a non-loopback or malformed target WebSocket endpoint",
		);
	});

	it("creates a tab only as an explicit action and leaves it open during cleanup", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
async with await browser.connect("http://127.0.0.1:${chrome.port}") as chrome:
    async with await chrome.new_page("about:blank") as page:
        print(page.target.target_id, page.closed)
    print("disconnected", page.closed)
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("page-new False");
		expect(result.stdout).toContain("disconnected True");
		expect(chrome.newPageRequests).toBe(1);
		expect(chrome.connectionCount).toBe(1);
		await expect.poll(() => chrome.closeCount).toBe(1);
	});

	it("reads close-delimited HTTP bodies to EOF and caps aggregate chunked trailers", async () => {
		const manager = await provisioner!.ensure();
		const result = await manager.execute(`
import json

async def split_response(reader, writer):
    await reader.readuntil(b"\\r\\n\\r\\n")
    body = json.dumps([]).encode()
    writer.write(b"HTTP/1.1 200 OK\\r\\nConnection: close\\r\\n\\r\\n" + body[:1])
    await writer.drain()
    await asyncio.sleep(0.02)
    writer.write(body[1:])
    await writer.drain()
    writer.close()
    await writer.wait_closed()

server = await asyncio.start_server(split_response, "127.0.0.1", 0)
port = server.sockets[0].getsockname()[1]
async with server:
    async with await browser.connect(f"http://127.0.0.1:{port}") as chrome:
        print("split", await chrome.targets())

async def excessive_trailers(reader, writer):
    await reader.readuntil(b"\\r\\n\\r\\n")
    writer.write(b"HTTP/1.1 200 OK\\r\\nTransfer-Encoding: chunked\\r\\n\\r\\n0\\r\\n" + b"X: y\\r\\n" * 11000 + b"\\r\\n")
    await writer.drain()
    writer.close()
    await writer.wait_closed()

server = await asyncio.start_server(excessive_trailers, "127.0.0.1", 0)
port = server.sockets[0].getsockname()[1]
async with server:
    try:
        await browser.connect(f"http://127.0.0.1:{port}")
    except Exception as error:
        print(type(error).__name__, str(error))

async def oversized_chunk_line(reader, writer):
    await reader.readuntil(b"\\r\\n\\r\\n")
    writer.write(b"HTTP/1.1 200 OK\\r\\nTransfer-Encoding: chunked\\r\\n\\r\\n" + b"1" * 70000 + b"\\r\\n")
    try:
        await writer.drain()
    except ConnectionError:
        pass
    writer.close()

server = await asyncio.start_server(oversized_chunk_line, "127.0.0.1", 0)
port = server.sockets[0].getsockname()[1]
async with server:
    try:
        await browser.connect(f"http://127.0.0.1:{port}")
    except Exception as error:
        print("chunk-line", type(error).__name__, str(error))

async def oversized_trailer_line(reader, writer):
    await reader.readuntil(b"\\r\\n\\r\\n")
    writer.write(b"HTTP/1.1 200 OK\\r\\nTransfer-Encoding: chunked\\r\\n\\r\\n0\\r\\nX: " + b"y" * 70000 + b"\\r\\n\\r\\n")
    try:
        await writer.drain()
    except ConnectionError:
        pass
    writer.close()

server = await asyncio.start_server(oversized_trailer_line, "127.0.0.1", 0)
port = server.sockets[0].getsockname()[1]
async with server:
    try:
        await browser.connect(f"http://127.0.0.1:{port}")
    except Exception as error:
        print("trailer-line", type(error).__name__, str(error))
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("split []");
		expect(result.stdout).toContain("BrowserProtocolError Chrome returned malformed chunked HTTP trailers");
		expect(result.stdout).toContain("chunk-line BrowserProtocolError Chrome returned malformed chunked HTTP data");
		expect(result.stdout).toContain(
			"trailer-line BrowserProtocolError Chrome returned malformed chunked HTTP trailers",
		);
	});
});
