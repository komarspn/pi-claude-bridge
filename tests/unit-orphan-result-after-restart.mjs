/**
 * A tool result with no parked query is orphaned only when pi delivers it on
 * an aborted signal, as it does after an abort during a tool call.
 *
 * A durable host (pi-durable) checkpoints a tool call, and when its process is
 * killed mid-tool and reopened, it resumes the run by handing the provider the
 * history ending at that tool result. The new process has no parked query for
 * it, exactly like the user-abort case, and the orphan branch used to end the
 * turn with an empty reply. These pin both sides: after a restart the result
 * starts a fresh query that carries the turn on; on an aborted signal it is
 * still dropped with an end_turn.
 */
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openSession } from "cc-session-io";

const claudeDir = mkdtempSync(join(tmpdir(), "claude-bridge-orphan-restart-cc-"));
process.env.CLAUDE_CONFIG_DIR = claudeDir;
process.on("exit", () => rmSync(claudeDir, { recursive: true, force: true }));

const mod = await import("../src/index.js");
const { setQuery, resetSharedSession, activeQueryContexts, markRebuildForSession } = mod.__test;
let providerConfig;
mod.default({
	on: () => {},
	registerProvider: (_name, config) => { providerConfig = config; },
	registerTool: () => {},
});
const streamSimple = providerConfig.streamSimple;
const model = providerConfig.models[0];

const calls = [];
const scripts = [];
const init = (id) => ({ type: "system", subtype: "init", session_id: id });
const ev = (event) => ({ type: "stream_event", event });
const toolUse = (id, name = "read") => [
	ev({ type: "message_start", message: { id: `msg_${id}`, usage: {} } }),
	ev({ type: "content_block_start", index: 0, content_block: { type: "tool_use", id, name: `mcp__custom-tools__${name}`, input: {} } }),
	ev({ type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: '{"path":"a"}' } }),
	ev({ type: "content_block_stop", index: 0 }),
	ev({ type: "message_delta", delta: { stop_reason: "tool_use" }, usage: {} }),
	ev({ type: "message_stop" }),
];
const text = (t) => [
	ev({ type: "message_start", message: { id: "msg_text", usage: {} } }),
	ev({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }),
	ev({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: t } }),
	ev({ type: "content_block_stop", index: 0 }),
	ev({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: {} }),
	ev({ type: "message_stop" }),
];
const result = (t) => ({ type: "result", subtype: "success", is_error: false, result: t });

let clock = 0;
const user = (t) => ({ role: "user", content: t, timestamp: clock++ });
const toolCall = (id) => ({ role: "assistant", content: [{ type: "toolCall", id, name: "read", arguments: { path: "a" } }],
	api: "claude-bridge", provider: "claude-bridge", model: "claude-bridge/opus",
	usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
	stopReason: "toolUse", timestamp: clock++ });
const toolResult = (id) => ({ role: "toolResult", toolCallId: id, toolName: "read", content: [{ type: "text", text: "file" }], isError: false, timestamp: clock++ });
const settle = () => new Promise((r) => setTimeout(r, 20));
const replyText = (message) => message.content.filter((b) => b.type === "text").map((b) => b.text).join("");

beforeEach(() => {
	resetSharedSession();
	activeQueryContexts.clear();
	calls.length = 0;
	scripts.length = 0;
	setQuery(({ prompt, options }) => {
		const script = scripts.shift();
		if (!script) throw new Error("no fake script queued");
		const call = { label: script.label, resume: options.resume, options };
		calls.push(call);
		// close() is how the bridge kills CC on abort; it ends a parked script.
		let closed = false, onClose;
		const killed = new Promise((r) => { onClose = r; });
		const gen = (async function* () {
			call.prompt = (await prompt[Symbol.asyncIterator]().next()).value.message.content[0].text;
			if (script.inspect) await script.inspect(call);
			for (const step of script.steps) {
				if (closed) return;
				if (typeof step === "function") { await step(killed); continue; }
				yield step;
			}
		})();
		gen.interrupt = async () => {};
		gen.close = () => { closed = true; onClose(); };
		return gen;
	});
});

afterEach(() => {
	for (const c of activeQueryContexts) {
		c.activeQuery?.close();
		c.releasePendingToolCalls("test ended");
	}
	setQuery(null);
});

async function mcpClient(server) {
	const pending = new Map();
	const transport = {
		start: async () => {}, close: async () => {},
		send: async (msg) => pending.get(msg.id)?.(msg),
	};
	await server.instance.connect(transport);
	let id = 0;
	return (method, params) => new Promise((resolve) => {
		pending.set(++id, resolve);
		transport.onmessage({ jsonrpc: "2.0", id, method, params });
	});
}

const readTool = () => ({ name: "read", description: "Read a file", parameters: { type: "object", properties: { path: { type: "string" } } } });

describe("mid-turn tool activation", () => {
	it("exposes newly enabled tools without another user message and imports the result once", async () => {
		const loader = { name: "subagents_enable", description: "Enable subagents", parameters: { type: "object", properties: {} } };
		const worker = { name: "subagent", description: "Run a subagent", parameters: { type: "object", properties: {} } };
		const ask = user("enable and run a subagent");
		scripts.push({ label: "loader", steps: [init("cc-loader"), ...toolUse("enable_1", loader.name), (killed) => killed] });
		const parked = await streamSimple(model, { messages: [ask], tools: [loader] }, { sessionId: "pi-dynamic" }).result();
		assert.equal(parked.content[0].name, "subagents_enable");
		const completed = { ...toolResult("enable_1"), toolName: loader.name, content: [{ type: "text", text: "subagents enabled" }] };
		scripts.push({ label: "updated", inspect: async (call) => {
			const request = await mcpClient(call.options.mcpServers["custom-tools"]);
			assert.deepEqual((await request("tools/list", {})).result.tools.map((t) => t.name), ["subagents_enable", "subagent"]);
			const session = openSession({ sessionId: call.resume, projectPath: call.options.cwd });
			const results = session.messages.flatMap((m) => Array.isArray(m.message?.content) ? m.message.content : [])
				.filter((b) => b.type === "tool_result" && b.tool_use_id === "enable_1");
			assert.equal(results.length, 1, "the enabling result must appear exactly once in rebuilt history");
		}, steps: [init("cc-updated"), ...toolUse("worker_1", worker.name), (killed) => killed] });

		const next = streamSimple(model, { messages: [ask, parked, completed], tools: [loader, worker] }, { sessionId: "pi-dynamic" });
		assert.equal(calls.length, 2, "changed tools must start an updated query in this same turn");
		const reply = await next.result();
		assert.equal(reply.content[0].name, "subagent");
	});

	for (const change of ["remove", "description", "schema"]) {
		it(`refreshes tools after a ${change} change, including in-place edits`, async () => {
			const tool = readTool();
			const ask = user("read a");
			scripts.push({ label: "old", steps: [init("cc-old"), ...toolUse("change_1"), (killed) => killed] });
			const parked = await streamSimple(model, { messages: [ask], tools: [tool] }, { sessionId: "pi-change" }).result();
			if (change === "description") tool.description = "Read updated content";
			if (change === "schema") tool.parameters.properties.path = { type: "integer" };
			const tools = change === "remove" ? [] : [tool];
			scripts.push({ label: "new", inspect: async (call) => {
				if (change === "remove") assert.equal(call.options.mcpServers, undefined);
				else {
					const request = await mcpClient(call.options.mcpServers["custom-tools"]);
					const [listed] = (await request("tools/list", {})).result.tools;
					assert.equal(listed.description, tool.description);
					assert.deepEqual(listed.inputSchema, tool.parameters);
				}
			}, steps: [init("cc-new"), ...text("continued"), result("continued")] });
			const next = streamSimple(model, { messages: [ask, parked, toolResult("change_1")], tools }, { sessionId: "pi-change" });
			assert.equal(calls.length, 2);
			assert.equal(replyText(await next.result()), "continued");
		});
	}

	it("reuses unchanged definitions despite tool and schema key ordering", async () => {
		const tools = [readTool(), { name: "write", description: "Write", parameters: { type: "object", properties: {} } }];
		const ask = user("read a");
		let response;
		scripts.push({ label: "reuse", inspect: async (call) => {
			const request = await mcpClient(call.options.mcpServers["custom-tools"]);
			response = request("tools/call", { name: "read", arguments: {}, _meta: { "claudecode/toolUseId": "same_1" } });
		}, steps: [init("cc-reuse"), ...toolUse("same_1"), async () => {
			assert.equal((await response).result.content[0].text, "file");
		}, ...text("same query"), result("same query")] });
		const parked = await streamSimple(model, { messages: [ask], tools }, { sessionId: "pi-reuse" }).result();
		const reordered = [tools[1], { ...readTool(), parameters: { properties: { path: { type: "string" } }, type: "object" } }];
		const reply = await streamSimple(model, { messages: [ask, parked, toolResult("same_1")], tools: reordered }, { sessionId: "pi-reuse" }).result();
		assert.equal(calls.length, 1, "unchanged tools must preserve the running query");
		assert.equal(replyText(reply), "same query");
	});

	it("refreshes a child without discarding its concurrently parked parent", async () => {
		const tools = [readTool()];
		const parentAsk = user("parent task");
		let parentResponse;
		scripts.push({ label: "parent", inspect: async (call) => {
			const request = await mcpClient(call.options.mcpServers["custom-tools"]);
			parentResponse = request("tools/call", { name: "read", arguments: {}, _meta: { "claudecode/toolUseId": "parent_1" } });
		}, steps: [init("cc-parent"), ...toolUse("parent_1"), async () => { await parentResponse; }, ...text("parent done"), result("parent done")] });
		const parent = await streamSimple(model, { messages: [parentAsk], tools }, { sessionId: "pi-parent" }).result();
		const childAsk = user("child task");
		scripts.push({ label: "child", steps: [init("cc-child"), ...toolUse("child_1"), (killed) => killed] });
		const child = await streamSimple(model, { messages: [childAsk], tools }, { sessionId: "pi-child" }).result();
		scripts.push({ label: "child-updated", steps: [init("cc-child-updated"), ...text("child done"), result("child done")] });
		const childReply = await streamSimple(model, { messages: [childAsk, child, toolResult("child_1")], tools: [] }, { sessionId: "pi-child" }).result();
		assert.equal(replyText(childReply), "child done");
		const parentReply = await streamSimple(model, { messages: [parentAsk, parent, toolResult("parent_1")], tools }, { sessionId: "pi-parent" }).result();
		assert.equal(replyText(parentReply), "parent done");
		assert.deepEqual(calls.map((c) => c.label), ["parent", "child", "child-updated"]);
	});
});

describe("tool result with no parked query", () => {
	it("after a restart, starts a fresh query that answers the turn", async () => {
		// A new process: the history ends at a checkpointed tool result, and no
		// query of this process ever parked on it or was aborted.
		scripts.push({ label: "resumed", steps: [init("cc-resumed"), ...text("answer after restart"), result("answer after restart")] });
		const reply = await streamSimple(model, { messages: [user("read a"), toolCall("toolu_1"), toolResult("toolu_1")], tools: [] },
			{ sessionId: "pi-restarted" }).result();

		assert.equal(calls.length, 1, "the result carries the turn into a fresh query");
		assert.match(calls[0].prompt, /interrupted and resumed/);
		assert.equal(replyText(reply), "answer after restart");
	});

	it("after compaction rewrote the history, tells Claude Code its context was compacted", async () => {
		// An overflow: pi compacts with no query parked, then retries from the
		// rewritten history, which ends at a tool result.
		markRebuildForSession("pi-compacted", "session_compact:overflow");
		scripts.push({ label: "rebuilt", steps: [init("cc-rebuilt"), ...text("continued"), result("continued")] });
		await streamSimple(model, { messages: [user("read a"), toolCall("toolu_7"), toolResult("toolu_7")], tools: [] },
			{ sessionId: "pi-compacted" }).result();

		assert.equal(calls.length, 1);
		assert.match(calls[0].prompt, /context was compacted/);
	});

	it("after an abort, ends the turn without a query", async () => {
		const controller = new AbortController();
		scripts.push({ label: "parked", steps: [init("cc-aborted"), ...toolUse("toolu_2"), (killed) => killed] });
		const first = streamSimple(model, { messages: [user("read a")], tools: [] }, { sessionId: "pi-aborted", signal: controller.signal });
		await settle();
		controller.abort();
		await first.result();
		activeQueryContexts.clear();

		// pi calls back with the run's signal, which the abort left aborted.
		const reply = await streamSimple(model, { messages: [user("read a"), toolCall("toolu_2"), toolResult("toolu_2")], tools: [] },
			{ sessionId: "pi-aborted", signal: controller.signal }).result();

		assert.equal(calls.length, 1, "no query starts for the orphaned result");
		assert.equal(replyText(reply), "", "the turn ends empty, as before");
	});

	it("a session aborted earlier still resumes a later retry ending at a tool result", async () => {
		const controller = new AbortController();
		scripts.push({ label: "parked", steps: [init("cc-earlier"), ...toolUse("toolu_5"), (killed) => killed] });
		const first = streamSimple(model, { messages: [user("read a")], tools: [] }, { sessionId: "pi-retry", signal: controller.signal });
		await settle();
		controller.abort();
		await first.result();
		activeQueryContexts.clear();

		// A later run (fresh signal) retries after an overflow or quota error, with
		// history ending at a tool result no query is parked on.
		scripts.push({ label: "retry", steps: [init("cc-retry"), ...text("resumed"), result("resumed")] });
		const reply = await streamSimple(model, { messages: [user("read b"), toolCall("toolu_6"), toolResult("toolu_6")], tools: [] },
			{ sessionId: "pi-retry", signal: new AbortController().signal }).result();

		assert.equal(calls.length, 2, "the retry starts a fresh query");
		assert.equal(replyText(reply), "resumed");
	});

	it("a parked query that fails resumes on pi's retry, even after an earlier abort (#192)", async () => {
		const controller = new AbortController();
		scripts.push({ label: "aborted", steps: [init("cc-old"), ...toolUse("toolu_8"), (killed) => killed] });
		const first = streamSimple(model, { messages: [user("read a")], tools: [] }, { sessionId: "pi-failed", signal: controller.signal });
		await settle();
		controller.abort();
		await first.result();

		// The query parks on a tool call, then fails (a quota error) once its result arrives.
		let fail;
		const failure = new Promise((r) => { fail = r; });
		scripts.push({ label: "failing", steps: [init("cc-failing"), ...toolUse("toolu_9"), async () => { await failure; throw new Error("rate limit (five_hour)"); }] });
		const ask = user("read b");
		const tools = [{ name: "read", description: "Read a file", parameters: { type: "object", properties: { path: { type: "string" } }, required: ["path"] } }];
		const parked = await streamSimple(model, { messages: [ask], tools }, { sessionId: "pi-failed", signal: new AbortController().signal }).result();
		assert.equal(parked.stopReason, "toolUse");
		const history = [ask, parked, toolResult("toolu_9")];
		const failing = streamSimple(model, { messages: history, tools }, { sessionId: "pi-failed", signal: new AbortController().signal });
		fail();
		assert.equal((await failing.result()).stopReason, "error");
		await settle();

		// pi retries the same history on a fresh signal.
		scripts.push({ label: "retry", steps: [init("cc-retry"), ...text("answer after retry"), result("answer after retry")] });
		const reply = await streamSimple(model, { messages: history, tools }, { sessionId: "pi-failed", signal: new AbortController().signal }).result();

		assert.equal(calls.length, 3, "the retry starts a fresh query");
		assert.equal(replyText(reply), "answer after retry");
	});

	it("an abort in one session does not orphan another session's resumed result", async () => {
		const controller = new AbortController();
		scripts.push({ label: "parked", steps: [init("cc-other"), ...toolUse("toolu_3"), (killed) => killed] });
		const first = streamSimple(model, { messages: [user("read a")], tools: [] }, { sessionId: "pi-other", signal: controller.signal });
		await settle();
		controller.abort();
		await first.result();
		activeQueryContexts.clear();

		scripts.push({ label: "resumed", steps: [init("cc-mine"), ...text("mine"), result("mine")] });
		const reply = await streamSimple(model, { messages: [user("read a"), toolCall("toolu_4"), toolResult("toolu_4")], tools: [] },
			{ sessionId: "pi-mine" }).result();

		assert.equal(calls.length, 2);
		assert.equal(replyText(reply), "mine");
	});
});
