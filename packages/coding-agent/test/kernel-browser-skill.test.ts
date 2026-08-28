import { mkdirSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
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

class FakeChromeCdp {
	readonly server: Server;
	readonly sockets = new Set<WebSocket>();
	readonly webSocketServer = new WebSocketServer({ noServer: true });
	port = 0;
	inputValue = "before";
	connectionCount = 0;
	closeCount = 0;

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
				response.writeHead(200, { "content-type": "application/json" });
				response.end(JSON.stringify({ ...targets[0], id: "page-new" }));
				return;
			}
			response.writeHead(404);
			response.end();
		});
		this.server.on("upgrade", (request, socket, head) => {
			if (request.url !== "/devtools/page/page-existing") {
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
				if (expression === "hold") return;
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
				let value: unknown = true;
				if (expression.includes("innerText")) value = "existing marker";
				if (expression.includes("const previous")) {
					const previous = this.inputValue;
					const match = expression.match(/element\.value = ("(?:[^"\\]|\\.)*");/);
					if (!match) throw new Error("fill expression did not contain a JSON string value");
					this.inputValue = JSON.parse(match[1]!) as string;
					value = previous;
				}
				if (request.method === "Page.navigate") {
					socket.send(JSON.stringify({ id: request.id, result: { frameId: "frame-1" } }));
					return;
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
		expect(chrome.connectionCount).toBe(1);
		await expect.poll(() => chrome.closeCount).toBe(1);
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
`);

		expect(result.status).toBe("ok");
		expect(result.stdout).toContain("cancelled True");
		expect(result.stdout).toContain("disconnected True Chrome page disconnected during Runtime.evaluate");
		expect(chrome.connectionCount).toBe(2);
		await expect.poll(() => chrome.closeCount).toBe(2);
	});
});
