import { mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import { createAssistantMessageEventStream, getModel } from "@earendil-works/pi-ai";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { AgentSession } from "../src/core/agent-session.js";
import { AuthStorage } from "../src/core/auth-storage.js";
import { convertToLlm } from "../src/core/messages.js";
import { ModelRegistry } from "../src/core/model-registry.js";
import { SessionManager } from "../src/core/session-manager.js";
import { SettingsManager } from "../src/core/settings-manager.js";
import type { Skill } from "../src/core/skills.js";
import { createSyntheticSourceInfo } from "../src/core/source-info.js";
import { createTestResourceLoader } from "./utilities.js";

const model = getModel("anthropic", "claude-sonnet-4-5")!;

describe("browser skill native-child propagation", () => {
	let tempDir: string;
	let root: AgentSession | undefined;

	beforeEach(() => {
		tempDir = join(tmpdir(), `pi-browser-propagation-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });
	});

	afterEach(() => {
		root?.dispose();
		root = undefined;
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("shares the root browser skill and IPython tool with a native child", async () => {
		const browserSkill: Skill = {
			name: "browser",
			description: "Attach to an existing Chrome CDP endpoint",
			filePath: join(tempDir, "browser", "SKILL.md"),
			baseDir: join(tempDir, "browser"),
			sourceInfo: createSyntheticSourceInfo(join(tempDir, "browser", "SKILL.md"), { source: "builtin" }),
			disableModelInvocation: false,
			kind: "python",
			python: {
				importName: "browser",
				packagePath: join(tempDir, "browser"),
				pyprojectPath: join(tempDir, "browser", "pyproject.toml"),
			},
		};
		const resourceLoader = createTestResourceLoader({ skills: [browserSkill] });
		const authStorage = AuthStorage.create(join(tempDir, "auth.json"));
		authStorage.setRuntimeApiKey("anthropic", "test-key");
		const agent = new Agent({
			convertToLlm,
			getApiKey: () => "test-key",
			initialState: { model, systemPrompt: "", tools: [], thinkingLevel: "off" },
			streamFn: () => {
				const stream = createAssistantMessageEventStream();
				queueMicrotask(() =>
					stream.push({
						type: "done",
						reason: "stop",
						message: {
							role: "assistant",
							content: [{ type: "text", text: "done" }],
							api: model.api,
							provider: model.provider,
							model: model.id,
							usage: {
								input: 0,
								output: 0,
								cacheRead: 0,
								cacheWrite: 0,
								totalTokens: 0,
								cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
							},
							stopReason: "stop",
							timestamp: Date.now(),
						},
					}),
				);
				return stream;
			},
		});
		root = new AgentSession({
			agent,
			sessionManager: SessionManager.create(tempDir, join(tempDir, "sessions")),
			settingsManager: SettingsManager.create(tempDir, tempDir),
			cwd: tempDir,
			modelRegistry: ModelRegistry.create(authStorage, join(tempDir, "models.json")),
			resourceLoader,
			rlmMaxDepth: 1,
		});

		const spawned = await root.runRlmChild("inspect the existing browser tab");
		const child = root.getRlmChildSession(spawned.rlm_child_id);

		expect(root.resourceLoader.getSkills().skills).toContain(browserSkill);
		expect(child?.resourceLoader).toBe(resourceLoader);
		expect(child?.resourceLoader.getSkills().skills).toContain(browserSkill);
		expect(child?.getActiveToolNames()).toContain("ipython");
	});
});
