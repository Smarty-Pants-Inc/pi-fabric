import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { RpcClient as PiRpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

type RpcEvent = Parameters<Parameters<PiRpcClient["onEvent"]>[0]>[0];
type Envelope = { topic: string; from: { id: string; name: string; kind: string }; to?: string; kind: string; text: string; data?: unknown };
type Published = Envelope & { id: string; sequence: number; createdAt: number };

const repo = fileURLToPath(new URL("../", import.meta.url));
const dist = path.join(repo, "dist");
const entry = path.join(dist, "index.js");
// The repo's own upstream Pi declares no pi.hostCapabilities, so its idle inbox wake stays off.
// This test runs the pinned fleet Pi that declares them; it never injects the test capability symbol.
const cli = process.env.FABRIC_IDLE_WAKE_PI_CLI;
const evidenceDir = process.env.FABRIC_IDLE_WAKE_EVIDENCE_DIR;
const PROVIDER = "idle-wake-loopback";
const WAKE_BUDGET_MS = 5_000;
// Longer than the wake budget: a counterexample that would wake does so inside it.
const QUIET_MS = 6_000;
const run = (Date.now() + process.pid).toString(36);
const MARK = `IDLEWAKE_${run}`;
const CANCEL_MARK = `IDLEWAKE_HELD_${run}`;
const ASK_MARK = `IDLEWAKE_ASK_${run}`;
const PROGRESS_MARK = `IDLEWAKE_PROGRESS_${run}`;
const NEG_BROADCAST = `IDLEWAKE_BROADCAST_${run}`;
const NEG_WRONG = `IDLEWAKE_WRONG_${run}`;
const NOISE = `IDLEWAKE_NOISE_${run}`;
const SEED_PROMPT = `seed ${run}`;
const CANCEL_PROMPT = `cancel-me ${run}`;
const RESUME_PROMPT = `resume ${run}`;
// Environment the child may inherit; everything else (PI_*, HERDR_*, credentials, tokens) is dropped.
const ENV_ALLOW = new Set(["PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ"]);

// Exactly the envelope bin/smarty-notify publishes (fleet tool lane-watcher, one `answer` to a Main).
const smartyNotify = (to: string, name: string, text: string): Envelope => ({
  topic: "fleet.work.tasks", from: { id: "fleet-tool:lane-watcher", name: "lane-watcher", kind: "main" },
  to, kind: "answer", text: `WATCHER ${name}: ${text}`, data: { key: `watcher-${name}` },
});

// The second process: Fabric's own built MeshStore, found the way factory/setup/factory/mesh-publish.mjs
// finds it (the chunk dist/fabric-runtime-state.js imports MeshStore from). Fails loudly, no fallback.
const PUBLISHER = `import { readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
const [dist, meshRoot] = process.argv.slice(2);
const source = readFileSync(join(dist, "fabric-runtime-state.js"), "utf8");
const chunk = [...source.matchAll(/import\\s*\\{([^}]*)\\}\\s*from\\s*"(\\.\\/chunks\\/[^"]+\\.js)"/g)]
  .find(([, names]) => names.split(",").some(name => name.trim() === "MeshStore"))?.[2];
const { MeshStore } = chunk ? await import(pathToFileURL(join(dist, chunk)).href) : {};
if (typeof MeshStore !== "function") throw new Error("no MeshStore export found through " + dist + "/fabric-runtime-state.js");
const store = new MeshStore(meshRoot, 256 * 1024, 500);
const published = [];
for (const request of JSON.parse(readFileSync(0, "utf8"))) published.push(await store.publish(request));
store.closeState?.();
process.stdout.write(JSON.stringify({ chunk, published }) + "\\n");
process.exit(0);
`;

// Real native Pi RPC Main (pinned CLI with host capabilities) + built Fabric with the default mesh
// (announce on). Inference is a private zero-spend loopback Anthropic Messages server. A fleet watcher
// publishes from a second process; the idle Main must start its own turn, without a manual prompt.
// Before the event-driven fix this took ~74 s (60 s steer grace + 15 s interval poll).
// Explicit artifact proof (Main runs it after `bun run build`):
//   FABRIC_IDLE_WAKE_REAL_CLI=1 \
//   FABRIC_IDLE_WAKE_PI_CLI=<pinned runtime>/node/node_modules/@earendil-works/pi-coding-agent/dist/cli.js \
//   [FABRIC_IDLE_WAKE_EVIDENCE_DIR=<dir>] bunx vitest run tests/root-inbox-idle-wake-cli.test.ts
describe.runIf(process.platform === "linux" && process.env.FABRIC_IDLE_WAKE_REAL_CLI === "1")("idle Main wakes on an addressed mesh answer", () => {
  it("wakes once within 5 s; broadcast, wrong recipient and a user cancel do not wake", async () => {
    if (!cli) throw new Error("FABRIC_IDLE_WAKE_PI_CLI must name a pinned Pi cli.js that declares pi.hostCapabilities");
    // Use the tested host's public RPC client too: Pi 1.1 expects admission data in prompt
    // responses that the pinned 0.87.1 host does not emit. Mixing those protocols fails setup.
    const { RpcClient } = await import(pathToFileURL(path.join(path.dirname(cli), "index.js")).href) as
      { RpcClient: typeof PiRpcClient };
    const capabilities = path.join(path.dirname(cli), "core/host-capabilities.js");
    // Precondition only (read, never injected): the Pi under test must declare both capabilities itself.
    expect(fs.existsSync(capabilities), `${capabilities} (this Pi predates host capabilities)`).toBe(true);
    expect(fs.readFileSync(capabilities, "utf8")).toMatch(/triggeredMessageQueuesBehindPreflight:\s*true[\s\S]*promptPendingVisible:\s*true/);

    if (evidenceDir) fs.mkdirSync(evidenceDir, { recursive: true });
    // With evidence, the scratch lives beside it and is retained; otherwise it is removed (hosted CI).
    const root = fs.realpathSync(fs.mkdtempSync(path.join(evidenceDir ?? os.tmpdir(), "fabric-idle-wake-")));
    const home = path.join(root, "home");
    const tmp = path.join(root, "tmp");
    const cwd = path.join(root, "main");
    const agentDir = path.join(root, "agent");
    const meshRoot = path.join(root, "mesh");
    for (const dir of [home, tmp, cwd, agentDir]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const publisher = path.join(root, "publisher.mjs");
    fs.writeFileSync(publisher, PUBLISHER);

    const events: Array<{ at: number; event: RpcEvent }> = [];
    const requests: Array<{ at: number; model: string; last: string; transcript: string; reply: string }> = [];
    const published: Array<{ at: number; label: string; events: Published[]; chunk?: string }> = [];
    const timing: Record<string, number | undefined> = {};
    const notes: string[] = [];
    const sockets = new Set<import("node:net").Socket>();

    // Event-driven waits: each RPC event or provider request re-checks the pending waiters; one bounded
    // deadline timer per wait. Nothing polls.
    const waiters = new Set<() => void>();
    const notify = () => { for (const waiter of [...waiters]) waiter(); };
    const until = (check: () => boolean, ms: number, what: string) => new Promise<void>((resolve, reject) => {
      const done = () => { clearTimeout(timer); waiters.delete(probe); };
      const probe = () => { if (check()) { done(); resolve(); } };
      const timer = setTimeout(() => { done(); reject(new Error(`timed out after ${ms} ms: ${what}`)); }, ms);
      waiters.add(probe);
      probe();
    });
    const quiet = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

    let count = 0;
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const data = JSON.parse(body) as { model: string; messages: unknown[] };
        const transcript = JSON.stringify(data.messages);
        const last = JSON.stringify(data.messages.at(-1) ?? null);
        // The cancel prompt's inference never answers: the owner aborts it mid-stream.
        const hold = last.includes(CANCEL_PROMPT);
        const reply = hold ? "held" : `ack ${requests.length}`;
        requests.push({ at: Date.now(), model: data.model, last, transcript, reply });
        notify();
        response.writeHead(200, { "content-type": "text/event-stream" });
        const event = (name: string, value: unknown) => response.write(`event: ${name}\ndata: ${JSON.stringify({ type: name, ...value as object })}\n\n`);
        event("message_start", { message: { id: `msg_${++count}`, type: "message", role: "assistant", model: data.model, content: [],
          stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
        if (hold) return;
        event("content_block_start", { index: 0, content_block: { type: "text", text: "" } });
        event("content_block_delta", { index: 0, delta: { type: "text_delta", text: reply } });
        event("content_block_stop", { index: 0 });
        event("message_delta", { delta: { stop_reason: "end_turn" }, usage: { output_tokens: 1 } });
        event("message_stop", {});
        response.end();
      });
    });
    server.on("connection", socket => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const model = (id: string) => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 200_000, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } });
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { [PROVIDER]: {
      baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, apiKey: "offline-only", api: "anthropic-messages",
      models: [model("main")] } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
    // The default mesh with announce on; optional services that would add their own turns stay off.
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: false, autoReload: false, mesh: { enabled: true, announce: true },
      mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false }, records: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, entropy: { compile: false }, speculation: { enabled: false },
    }));

    const base: Record<string, string | undefined> = {};
    for (const key of Object.keys(process.env)) if (!ENV_ALLOW.has(key)) base[key] = undefined; // spawn skips undefined
    const childEnv = {
      ...base, HOME: home, TMPDIR: tmp, XDG_CONFIG_HOME: path.join(home, ".config"), XDG_CACHE_HOME: path.join(home, ".cache"),
      XDG_DATA_HOME: path.join(home, ".local/share"), XDG_STATE_HOME: path.join(home, ".local/state"),
      PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: meshRoot,
      PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: cli, PI_OFFLINE: "1",
    };
    const publish = async (label: string, envelopes: Envelope[]): Promise<Published[]> => {
      const env: Record<string, string> = { HOME: home, TMPDIR: tmp };
      for (const key of ENV_ALLOW) if (process.env[key] !== undefined) env[key] = process.env[key]!;
      const stdout = await new Promise<string>((resolve, reject) => {
        const child = execFile(process.execPath, [publisher, dist, meshRoot], { cwd: tmp, env, timeout: 30_000 },
          (error, out, err) => error ? reject(new Error(`publisher ${label}: ${error.message}\n${err}`)) : resolve(out));
        child.stdin!.end(JSON.stringify(envelopes));
      });
      const result = JSON.parse(stdout) as { chunk: string; published: Published[] };
      published.push({ at: Date.now(), label, events: result.published, chunk: result.chunk });
      return result.published;
    };

    const client = new RpcClient({ cliPath: cli, cwd, provider: PROVIDER, model: "main", env: childEnv as Record<string, string>,
      args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
        "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry],
    });
    client.onEvent(event => { events.push({ at: Date.now(), event }); notify(); });
    const of = (type: string, since = 0) => events.filter(({ at, event }) => event.type === type && at >= since);
    const settled = () => of("agent_settled").length > 0 && of("agent_settled").length === of("agent_start").length;
    const mainSince = (since: number) => requests.filter(row => row.model === "main" && row.at >= since);
    let sessionId: string | undefined;
    let state: unknown;
    try {
      await client.start();
      // 1. Seed turn: the Main bootstraps Fabric, starts its inbox at the present, and settles.
      timing.seedPrompt = Date.now();
      await client.prompt(SEED_PROMPT);
      await until(() => settled() && requests.length > 0, 60_000, "seed turn settled");
      timing.seedSettled = Date.now();
      await quiet(1_000); // the host's settle handlers finish; no turn may start on its own
      expect(of("agent_start")).toHaveLength(1);
      state = await client.getState();
      expect(state).toMatchObject({ isStreaming: false, pendingMessageCount: 0 });
      sessionId = (state as { sessionId: string }).sessionId;
      const self = `session:${sessionId}`;

      // 2. Counterexamples: a broadcast and an answer addressed to another Main never wake this one.
      timing.negativesPublished = Date.now();
      const broadcast: Envelope = smartyNotify(self, "broadcast", NEG_BROADCAST);
      delete broadcast.to;
      await publish("broadcast+wrong-recipient", [broadcast, smartyNotify(`session:${randomUUID()}`, "wrong", NEG_WRONG)]);
      await quiet(QUIET_MS);
      expect(of("agent_start", timing.negativesPublished), "broadcast/wrong recipient started a turn").toHaveLength(0);
      expect(mainSince(timing.negativesPublished)).toHaveLength(0);

      // 3. The exact smarty-notify answer to this Main, followed in the same writer by unaddressed
      // writes, so the Main sees several filesystem notifications for one deliverable event.
      const [answer] = await publish("watcher-answer+noise", [smartyNotify(self, "test", MARK),
        { topic: "fabric.test.noise", from: { id: "fleet-tool:noise", name: "noise", kind: "main" }, kind: "note", text: NOISE },
        { topic: "fleet.work.noise", from: { id: "fleet-tool:noise", name: "noise", kind: "main" }, kind: "note", text: NOISE }]);
      expect(answer).toMatchObject({ topic: "fleet.work.tasks", to: self, kind: "answer", data: { key: "watcher-test" } });
      timing.published = answer!.createdAt;
      const sawMark = () => mainSince(timing.published!).some(row => row.transcript.includes(MARK));
      try {
        await until(() => of("agent_start", timing.published).length > 0 && sawMark(), WAKE_BUDGET_MS, "idle Main woke and inferred on the answer");
      } catch (error) {
        throw new Error(`${String(error)}; agent_start since publish: ${of("agent_start", timing.published).length}, ` +
          `main requests since publish: ${mainSince(timing.published).length}`);
      }
      timing.agentStart = of("agent_start", timing.published)[0]!.at;
      timing.inference = mainSince(timing.published).find(row => row.transcript.includes(MARK))!.at;
      expect(timing.agentStart - timing.published).toBeLessThan(WAKE_BUDGET_MS);
      expect(timing.inference - timing.published).toBeLessThan(WAKE_BUDGET_MS);
      await until(settled, 20_000, "woken turn settled");
      await quiet(QUIET_MS); // duplicate notifications and late reads must not buy another turn
      expect(of("agent_start", timing.published), "exactly one woken turn").toHaveLength(1);
      const woken = mainSince(timing.published);
      expect(woken, "exactly one inference for the answer").toHaveLength(1);
      expect(woken[0]!.transcript).toContain(MARK);
      for (const marker of [NEG_BROADCAST, NEG_WRONG, NOISE]) expect(woken[0]!.transcript).not.toContain(marker);
      const inbox = (await client.getMessages()).filter(message => message.role === "custom" &&
        (message as { customType?: string }).customType === "pi-fabric-inbox");
      expect(inbox.filter(message => JSON.stringify(message).includes(MARK))).toHaveLength(1);

      // 4. Exact-session work is independent of kind and bypasses the previous wake's cooldown.
      // An ask and then an ordinary progress event must each start one turn without another prompt.
      for (const [kind, marker] of [["ask", ASK_MARK], ["progress", PROGRESS_MARK]] as const) {
        const [addressed] = await publish(`exact-session-${kind}`, [{ ...smartyNotify(self, kind, marker), kind }]);
        const at = addressed!.createdAt;
        await until(() => of("agent_start", at).length > 0 && mainSince(at).some(row => row.transcript.includes(marker)),
          WAKE_BUDGET_MS, `exact-session ${kind} bypassed grace and cooldown`);
        expect(mainSince(at).find(row => row.transcript.includes(marker))!.at - at).toBeLessThan(WAKE_BUDGET_MS);
        await until(settled, 20_000, `${kind} turn settled`);
        await quiet(250);
        expect(of("agent_start", at)).toHaveLength(1);
        expect(mainSince(at)).toHaveLength(1);
      }

      // 5. Owner cancel: an aborted turn holds later work until an explicit prompt resumes.
      timing.cancelPrompt = Date.now();
      await client.prompt(CANCEL_PROMPT);
      await until(() => mainSince(timing.cancelPrompt!).some(row => row.reply === "held"), 20_000, "cancel prompt at inference");
      await client.abort();
      await until(settled, 20_000, "aborted turn settled");
      timing.cancelSettled = Date.now();
      const [held] = await publish("watcher-answer-after-cancel", [smartyNotify(self, "cancel", CANCEL_MARK)]);
      timing.heldPublished = held!.createdAt;
      await quiet(QUIET_MS);
      expect(of("agent_start", timing.heldPublished), "a cancelled Main woke on its own").toHaveLength(0);
      expect(mainSince(timing.heldPublished)).toHaveLength(0);
      timing.resumePrompt = Date.now();
      await client.prompt(RESUME_PROMPT);
      await until(() => settled() && mainSince(timing.resumePrompt!).length > 0, 20_000, "resumed turn settled");
      await quiet(2_000);
      const resumed = mainSince(timing.resumePrompt);
      expect(of("agent_start", timing.resumePrompt)).toHaveLength(1);
      expect(resumed).toHaveLength(1);
      expect(resumed[0]!.transcript).toContain(CANCEL_MARK);
      expect(resumed[0]!.transcript).toContain(RESUME_PROMPT);
      const all = (await client.getMessages()).filter(message => message.role === "custom" &&
        (message as { customType?: string }).customType === "pi-fabric-inbox");
      expect(all.filter(message => JSON.stringify(message).includes(CANCEL_MARK))).toHaveLength(1);
      expect(all.filter(message => JSON.stringify(message).includes(MARK))).toHaveLength(1);
    } catch (error) {
      notes.push(String(error));
      throw error;
    } finally {
      const entries = await client.getEntries().catch(() => undefined);
      await client.abort().catch(() => undefined);
      await client.stop().catch(() => undefined);
      for (const socket of sockets) socket.destroy();
      server.close();
      if (evidenceDir) {
        const write = (name: string, value: unknown) => fs.writeFileSync(path.join(evidenceDir, name),
          typeof value === "string" ? value : JSON.stringify(value, null, 2));
        write("rpc-events.json", events);
        write("provider-requests.json", requests);
        write("published.json", published);
        write("timing.json", { ...timing, sessionId, state, notes,
          publishToAgentStartMs: timing.agentStart !== undefined && timing.published !== undefined ? timing.agentStart - timing.published : null,
          publishToInferenceMs: timing.inference !== undefined && timing.published !== undefined ? timing.inference - timing.published : null });
        write("session-entries.json", entries ?? null);
        write("main-stderr.log", client.getStderr());
        write("artifact.json", { cli: fs.realpathSync(cli), entry, scratch: root,
          sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex"),
          command: "FABRIC_IDLE_WAKE_REAL_CLI=1 FABRIC_IDLE_WAKE_PI_CLI=<cli> FABRIC_IDLE_WAKE_EVIDENCE_DIR=<dir> bunx vitest run tests/root-inbox-idle-wake-cli.test.ts" });
      } else {
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      }
    }
  }, 180_000);
});
