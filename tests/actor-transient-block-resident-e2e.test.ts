import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentRoot, type ResidentDeliveryRecord, type ResidentHostConfig } from "../src/residency/protocol.js";
import { pruneActorSessionBackups } from "../src/storage/retention.js";

// RECORDED-CALL seam (#4664): keep real session rotations, backups and run logs, but never
// execute the product backup deleter, including on the resident overflow settlement path.
vi.mock("../src/storage/retention.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/storage/retention.js")>(),
  pruneActorSessionBackups: vi.fn(),
}));

// smarty-dev#6509 end to end. A real ResidentHost (its ActorManager, AgentManager and participant
// presence) on a temp mesh root runs the real Fabric worker (src/worker.ts), which runs a fake Pi
// RPC child (tests/fixtures/fake-pi-rpc.mjs). Its provider fails with a real transport error (or a
// 401) a set number of times, then succeeds. The cooldown is shortened through the actors config;
// no fake timers or clocks. A separate MeshStore plays the factory router: it reads the published
// actor record and routes the next review request only when that record is not blocked.
const COOLDOWN_MS = 1_500;
const MAX_MS = 4_000;
const TOPIC = "review.requests";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.unstubAllEnvs();
  vi.mocked(pruneActorSessionBackups).mockClear();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const waitFor = async (predicate: () => boolean, what: string, timeoutMs = 60_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
};

type Block = { code: string; class?: string; since: number; blockedAt?: number; count: number; retryAt?: number; backoffMs?: number;
  classification?: string };
type Presence = { status?: string; activationBlocked?: Block; activationBlockExpired?: Block & { expiredAt: number } } | undefined;

const fixture = (cooldownMs = COOLDOWN_MS) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-6509-e2e-"));
  roots.push(root);
  // Simulated upstream provider state belongs to this disposable fixture, not any
  // activation's private scratch. Do not add it to the actor's writable roots.
  const providerStateDir = path.join(root, "fake-provider-state");
  fs.mkdirSync(providerStateDir, { mode: 0o700 });
  vi.stubEnv("FAKE_PI_RPC_STATE_DIR", providerStateDir);
  const piBinary = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
  fs.chmodSync(piBinary, 0o755);
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:review-main", sessionId: "review-main",
    cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(meshRoot, "session:review-main"), fullCodeMode: true,
    agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("src/worker.ts"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary, claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fake", id: "fake-model" }], aliases: {}, defaultModel: "fake/fake-model" },
    // The knob under test: a short cooldown and cap instead of 10 and 60 minutes.
    actors: { ...DEFAULT_FABRIC_CONFIG.actors, transientBlockCooldownMs: cooldownMs, transientBlockMaxMs: MAX_MS },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const router = new MeshStore(meshRoot, 65_536, 100);
  const from: MeshIdentity = { id: "session:router", name: "router", kind: "main", sessionId: "router" };
  const token = randomUUID().replace(/-/g, "");
  const start = async (onIdle?: ConstructorParameters<typeof ResidentHost>[1]) => {
    const host = new ResidentHost(config, onIdle);
    closers.push(() => host.close());
    await host.start();
    return host;
  };
  return { config, router, token, start, providerStateDir,
    presence: (id: string): Presence => router.get(`actors/${config.sessionId}/${id}`)?.value as Presence,
    // Factory review routing: skip an actor whose published record is blocked or not live.
    routable: (id: string): boolean => {
      const value = router.get(`actors/${config.sessionId}/${id}`)?.value as Presence;
      // A dormant actor still accepts deliveries: delivery wakes it, so it is routable.
      return value !== undefined && value.activationBlocked === undefined && ["idle", "running", "queued", "dormant"].includes(String(value.status));
    },
    route: (text: string) => router.publish({ topic: TOPIC, from, text }),
  };
};

const createReviewer = (host: ResidentHost) => host.actors.create({ name: "review-route", instructions: "Review each request.",
  responseMode: "text", topics: [TOPIC], coalesce: false, residency: "durable" });

const settled = (host: ResidentHost, id: string) => {
  const status = host.actors.status(id);
  return !status.inFlightRun && !status.preparing && status.queued === 0;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("transient activation block on a resident host, end to end (smarty-dev#6509)", () => {
  // Keep reason: recorded fixture regression when each activation gained private TMPDIR.
  it("fake provider counters survive distinct activation TMPDIRs only with explicit upstream state", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-6509-provider-state-"));
    roots.push(root);
    const stateDir = path.join(root, "provider");
    fs.mkdirSync(stateDir, { mode: 0o700 });
    if (process.platform !== "win32") expect(fs.statSync(stateDir).mode & 0o777).toBe(0o700);
    for (const marker of ["TRANSIENT_FAILS", "AUTH_FAILS"]) {
      const token = randomUUID().replace(/-/g, "");
      const counter = `fake-pi-rpc-${marker}-${token}`;
      const scratches = Array.from({ length: 3 }, () => fs.mkdtempSync(path.join(root, "activation-")));
      expect(new Set(scratches).size).toBe(3);
      const activate = (scratch: string, state: string | undefined) => {
        const env: NodeJS.ProcessEnv = { ...process.env, TMPDIR: scratch, TMP: scratch, TEMP: scratch };
        // Delete rather than pass an empty value: absence must exercise os.tmpdir().
        delete env.FAKE_PI_RPC_STATE_DIR;
        if (state !== undefined) env.FAKE_PI_RPC_STATE_DIR = state;
        const child = spawnSync(process.execPath, [path.resolve("tests/fixtures/fake-pi-rpc.mjs")], {
          env, input: `${JSON.stringify({ type: "prompt", message: `${marker}=2#${token}` })}\n`,
          encoding: "utf8", timeout: 5_000,
        });
        expect(child.error).toBeUndefined();
        expect(child.signal).toBeNull();
        expect(child.status, child.stderr).toBe(0);
        const frames = child.stdout.trim().split("\n").map(line => JSON.parse(line));
        const message = frames.find(frame => frame.type === "message_end")?.message;
        expect(message).toBeDefined();
        if (message.stopReason === "error") {
          expect(message.errorMessage).toBe(marker === "AUTH_FAILS" ? "401 Unauthorized: invalid x-api-key" : "fetch failed");
        }
        return message.stopReason;
      };
      expect(scratches.map(scratch => activate(scratch, stateDir))).toEqual(["error", "error", "stop"]);
      expect(fs.readFileSync(path.join(stateDir, counter), "utf8")).toBe("2");
      for (const scratch of scratches) expect(fs.existsSync(path.join(scratch, counter))).toBe(false);
      // No explicit state: every fresh activation starts at its own first failure.
      expect(scratches.map(scratch => activate(scratch, undefined))).toEqual(["error", "error", "error"]);
      for (const scratch of scratches) expect(fs.readFileSync(path.join(scratch, counter), "utf8")).toBe("1");
      expect(activate(scratches[0]!, undefined)).toBe("error");
      expect(activate(scratches[0]!, undefined)).toBe("stop");
      expect(fs.readFileSync(path.join(stateDir, counter), "utf8")).toBe("2");
    }
  }, 30_000);

  // Keep reason: resident/router trust boundary and the recorded #10049/#9890 deaf actor incident.
  it("10049 a resident mailbox directive actor refuses an oversized item through the actual hook and routes NEXT_REVIEW without manual reset", async () => {
    const s = fixture();
    const log = path.join(s.config.cwd, "overflow-prompts.jsonl");
    const bootstrapError = path.join(s.config.cwd, "pi-bootstrap-error.log");
    vi.stubEnv("FAKE_PI_LOG", log);
    const prompts = (): Array<{ history: boolean; overflow: boolean; session: string; marker: string }> =>
      fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];

    // The shared offline fixture emits a successful text answer, not a directive tool call.
    // Adapt ONLY that simulated Pi boundary: invoke the real registered reply tool for its
    // successful answer. Import the unchanged fixture; forward every original RPC/log frame.
    const piBinary = path.join(s.config.cwd, "fake-pi-overflow-directive.mjs");
    fs.writeFileSync(piBinary, `#!/usr/bin/env node
import fs from "node:fs";
import { pathToFileURL } from "node:url";
process.on("uncaughtExceptionMonitor", error => {
  fs.appendFileSync(${JSON.stringify(bootstrapError)}, String(error.stack ?? error) + "\\n");
});
const { default: hook } = await import(pathToFileURL(process.env.PI_FABRIC_REPLY_HOOK).href);
let reply;
hook({ on() {}, registerTool(tool) { reply = tool; } });
const write = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, ...args) => {
  let frame;
  try { frame = JSON.parse(String(chunk)); } catch {}
  if (frame?.type === "message_end" && frame.message?.stopReason === "stop") {
    const message = frame.message.content.filter(block => block.type === "text").map(block => block.text).join("");
    void reply.execute("offline-verdict", { action: "message", message }).catch(error => {
      process.stderr.write(String(error)); process.exitCode = 1;
    });
  }
  return write(chunk, ...args);
};
await import(${JSON.stringify(new URL("./fixtures/fake-pi-overflow.mjs", import.meta.url).href)});
`, { mode: 0o700 });
    s.config.piBinary = piBinary;
    s.config.workerPath = path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts");
    s.config.piModels = { available: [{ provider: "test", id: "overflow" }], aliases: {}, defaultModel: "test/overflow" };
    s.config.agents = { ...s.config.agents, timeoutMs: 120_000, retainRuns: true };
    fs.writeFileSync(path.join(s.config.residencyRoot, "config.json"), JSON.stringify(s.config));
    const host = await s.start();
    const actor = await host.actors.create({ name: "resident-overflow-review", instructions: "Review each request.",
      model: "test/overflow", runner: "pi", transport: "process", extensions: false, tools: [],
      responseMode: "directive", delivery: "mailbox", triggerTurn: false, inferenceContext: "activation",
      topics: [TOPIC], coalesce: false, residency: "durable" });
    const waitForPi = async (predicate: () => boolean, what: string) => {
      try {
        await waitFor(() => {
          if (fs.existsSync(bootstrapError)) {
            throw new Error(`Simulated Pi bootstrap failed: ${fs.readFileSync(bootstrapError, "utf8")}`);
          }
          const actorStatus = host.actors.status(actor.id);
          if (actorStatus.lastError && !/context (?:overflow|exceeds window)/i.test(actorStatus.lastError)) {
            throw new Error(`Resident actor preparation/run failed: ${actorStatus.lastError}`);
          }
          const runId = actorStatus.lastRunId;
          const statusFile = runId && path.join(path.dirname(actor.sessionFile!), "runs", runId, "status.json");
          if (statusFile && fs.existsSync(statusFile)) {
            const result = JSON.parse(fs.readFileSync(statusFile, "utf8"));
            if (result.status === "failed" && result.errorCode !== "CONTEXT_OVERFLOW") {
              throw new Error(`Resident Pi run failed: ${JSON.stringify({ runId, errorCode: result.errorCode,
                error: result.error, stderr: result.stderr })}`);
            }
          }
          return predicate();
        }, what);
      } catch (error) {
        throw new Error(`${error}\n${JSON.stringify({ actor: host.actors.status(actor.id), presence: s.presence(actor.id),
          counts: { refusals: refusals().length, alarms: alarms().length, ownerDeliveries: ownerDeliveries().length },
          prompts: prompts(), messages: host.actors.messages(actor.id) }, null, 2)}`);
      }
    };
    const refusals = () => s.router.read({ topic: "fabric.actor.output" }).filter(event => event.kind === "actor.activation.overflow");
    const alarms = () => s.router.read({ topic: "ops.owner" }).filter(event => event.kind === "actor.alarm");
    const ownerDeliveries = () => s.router.listAll(residentDeliveryPrefix(s.config.rootId))
      .map(entry => entry.value as ResidentDeliveryRecord);
    await waitFor(() => s.routable(actor.id), "the resident reviewer to be routable");

    // First activation, with no carried history: this item cannot fit even a fresh session.
    await s.route("PREFLIGHT_ALWAYS");
    await waitForPi(() => refusals().length === 1 && alarms().length === 1 && ownerDeliveries().length === 1 &&
      settled(host, actor.id) && ["idle", "dormant"].includes(String(s.presence(actor.id)?.status)) && s.routable(actor.id),
      "overflow settlement and idle/dormant unblocked publication");
    expect(prompts()).toEqual([{ history: false, overflow: true, session: expect.any(String), marker: "PREFLIGHT_ALWAYS" }]);
    expect(fs.existsSync(`${log}.requests`)).toBe(false); // Hook refused before simulated provider dispatch.
    const failedRun = host.actors.status(actor.id).lastRunId!;
    const statusFile = path.join(path.dirname(actor.sessionFile!), "runs", failedRun, "status.json");
    const result = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    expect(result).toMatchObject({ status: "failed", errorCode: "CONTEXT_OVERFLOW", toolCalls: 0 });
    expect(result.error).toContain("Fabric activation window failed: Context exceeds window");
    const frames = fs.readFileSync(result.logFile, "utf8");
    expect(frames).toContain('"type":"fabric_activation_window_refusal"');
    expect(frames).toContain('"code":"context-overflow"');
    expect(refusals()[0]).toMatchObject({ from: { id: actor.id }, data: { actorId: actor.id, code: "context-overflow-item", error: "context-overflow",
      eventRef: { source: `mesh:${TOPIC}`, topic: TOPIC } } });
    expect(alarms()[0]).toMatchObject({ to: s.config.rootId, data: { actorId: actor.id, code: "context-overflow-item",
      errorCode: "CONTEXT_OVERFLOW", reason: "context_window", runId: failedRun } });
    expect(ownerDeliveries()[0]).toMatchObject({ source: "fabric-host", delivery: "followUp", triggerTurn: true,
      from: { id: actor.id }, data: { code: "context-overflow-item", errorCode: "CONTEXT_OVERFLOW", reason: "context_window", runId: failedRun } });
    const backups = fs.readdirSync(path.dirname(actor.sessionFile!)).filter(name => name.startsWith("session.jsonl.") && name.endsWith(".bak"));
    expect(backups).toHaveLength(1);
    expect(vi.mocked(pruneActorSessionBackups)).toHaveBeenCalledWith(actor.sessionFile);
    expect(s.presence(actor.id)).not.toHaveProperty("activationBlocked");

    // A separate router admits distinct next work from the published record; no resetSession,
    // re-create, cooldown clock manipulation or direct actor ask repairs the route.
    expect(s.routable(actor.id)).toBe(true);
    await s.route("NEXT_REVIEW");
    await waitForPi(() => host.actors.status(actor.id).lastRunId !== failedRun && settled(host, actor.id) &&
      host.actors.messages(actor.id).some(message => message.direction === "out" && message.action === "message" && message.text === "verdict: ok") &&
      ["idle", "dormant"].includes(String(s.presence(actor.id)?.status)) && s.routable(actor.id), "the router's next review to succeed");
    expect(prompts()).toEqual([
      { history: false, overflow: true, session: expect.any(String), marker: "PREFLIGHT_ALWAYS" },
      { history: false, overflow: false, session: expect.not.stringMatching(prompts()[0]!.session), marker: "NEXT_REVIEW" },
    ]);
    expect(fs.readFileSync(`${log}.requests`, "utf8")).toBe("request\n");
    expect(host.actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(s.presence(actor.id)).not.toHaveProperty("activationBlocked");
    expect(refusals()).toHaveLength(1);
    expect(alarms()).toHaveLength(1);
    expect(ownerDeliveries()).toHaveLength(1); // Mailbox success does not wake Main a second time.
  }, 150_000);

  // Pi-fabric#774: a resident host sleeps its idle durable actors and exits 30 s (IDLE_EXIT_MS) after
  // the last activity. A transient block whose cooldown outlasts that must keep the actor warm and
  // the host up until retryAt, or nothing is left to publish the expired record routers wait for.
  it("a transient cooldown longer than the 30 s idle exit keeps the blocked actor awake and the host up until retryAt", async () => {
    const cooldownMs = 34_000;
    const s = fixture(cooldownMs);
    const exits: Array<{ reason: string | undefined; at: number }> = [];
    const host = await s.start((reason) => { exits.push({ reason, at: Date.now() }); });
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => host.actors.status(actor.id).activationBlocked !== undefined && settled(host, actor.id), "the block");
    const blocked = host.actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ class: "transient", backoffMs: cooldownMs, retryAt: blocked.blockedAt! + cooldownMs });

    // Past the 30 s idle exit and up to retryAt: never dormant, the host never exits, the router keeps skipping it.
    const statuses = new Set<string>();
    while (Date.now() < blocked.retryAt! - 200) {
      statuses.add(String(host.actors.status(actor.id).status));
      expect(exits).toEqual([]);
      expect(s.routable(actor.id)).toBe(false);
      await sleep(100);
    }
    expect(statuses.has("dormant")).toBe(false);
    expect(exits).toEqual([]);

    // At retryAt the live host publishes the expired record, and only then may the actor sleep.
    await waitFor(() => s.presence(actor.id)?.activationBlockExpired !== undefined, "the expired record to be published", 10_000);
    expect(Date.now()).toBeGreaterThanOrEqual(blocked.retryAt!);
    expect(Date.now()).toBeLessThan(blocked.retryAt! + 5_000);
    expect(s.presence(actor.id)!.activationBlockExpired).toMatchObject({ code: "provider-transient", class: "transient",
      backoffMs: cooldownMs, expiredAt: blocked.retryAt });
    expect(s.presence(actor.id)).not.toHaveProperty("activationBlocked");
    expect(exits).toEqual([]);
    await waitFor(() => host.actors.status(actor.id).status === "dormant", "the expired actor to go dormant", 10_000);
    expect(s.routable(actor.id)).toBe(true);
  }, 120_000);

  it("a transient provider error blocks; after the configured cooldown the router re-admits it and the probe clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the transient block to be published");
    const blocked = s.presence(actor.id)!.activationBlocked!;
    expect(blocked).toMatchObject({ code: "provider-transient", class: "transient", count: 1, backoffMs: COOLDOWN_MS,
      retryAt: blocked.blockedAt! + COOLDOWN_MS });
    expect(blocked.classification).toMatch(/^transient: /);
    expect(host.actors.status(actor.id).activationBlocked).toMatchObject({ class: "transient", retryAt: blocked.retryAt });
    expect(s.routable(actor.id)).toBe(false);

    // No fake clock: the real cooldown passes and the host republishes the record unblocked.
    await waitFor(() => s.routable(actor.id), "the cooldown to expire", 20_000);
    expect(Date.now()).toBeGreaterThanOrEqual(blocked.retryAt!);
    expect(s.presence(actor.id)!.activationBlockExpired).toMatchObject({ code: "provider-transient", class: "transient",
      backoffMs: COOLDOWN_MS, expiredAt: blocked.retryAt });

    // The router routes the next request: the probe runs on the real worker, succeeds, and clears the block.
    const failedRun = host.actors.status(actor.id).lastRunId!;
    const actorScratch = (runId: string): string => {
      const runRoot = path.join(path.dirname(actor.sessionFile!), "runs", runId);
      const frames = fs.readFileSync(path.join(runRoot, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      const policy = frames.find(frame => frame.type === "write_policy.actor")?.policy;
      // smarty-dev#15878: a short private TMPDIR when one fits the host's temp path, else the run's own tmp.
      const recorded = (JSON.parse(fs.readFileSync(path.join(runRoot, "status.json"), "utf8")) as { temporaryDirectory?: string }).temporaryDirectory;
      expect(policy?.writableRoots).toEqual([recorded ?? path.join(s.config.residencyRoot, "runs", runId, "tmp")]);
      expect(policy.writableRoots).not.toContain(s.providerStateDir);
      return policy.writableRoots[0];
    };
    const failedScratch = actorScratch(failedRun);
    await s.route(`review 2 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => host.actors.status(actor.id).lastRunId !== failedRun && settled(host, actor.id) &&
      host.actors.status(actor.id).activationBlockExpired === undefined, "the probe to complete");
    expect(host.actors.status(actor.id)).not.toHaveProperty("activationBlocked");
    const out = host.actors.messages(actor.id).filter((message) => message.direction === "out");
    expect(out.at(-1)).not.toHaveProperty("error");
    expect(out.at(-1)?.text).toContain("validated actor response");
    expect(actorScratch(host.actors.status(actor.id).lastRunId!)).not.toBe(failedScratch);
    expect(fs.readFileSync(path.join(s.providerStateDir, `fake-pi-rpc-TRANSIENT_FAILS-${s.token}`), "utf8")).toBe("1");
    await waitFor(() => s.presence(actor.id) !== undefined && s.presence(actor.id)!.activationBlockExpired === undefined, "the clear to be published");
    expect(s.presence(actor.id)).not.toHaveProperty("activationBlocked");
    expect(s.routable(actor.id)).toBe(true);
  }, 120_000);

  it("a transient failure on the probe re-blocks with the backoff doubled; the next probe clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the first block");
    expect(s.presence(actor.id)!.activationBlocked).toMatchObject({ class: "transient", count: 1, backoffMs: COOLDOWN_MS });
    await waitFor(() => s.routable(actor.id), "the first cooldown to expire", 20_000);

    await s.route(`review 2 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked?.count === 2, "the failed probe to re-block");
    const second = s.presence(actor.id)!.activationBlocked!;
    expect(second).toMatchObject({ code: "provider-transient", class: "transient", backoffMs: 2 * COOLDOWN_MS,
      retryAt: second.blockedAt! + 2 * COOLDOWN_MS });
    expect(s.routable(actor.id)).toBe(false);
    // The first cooldown is not enough any more.
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, second.blockedAt! + COOLDOWN_MS + 250 - Date.now())));
    expect(s.routable(actor.id)).toBe(false);
    await waitFor(() => s.routable(actor.id), "the doubled cooldown to expire", 20_000);
    expect(Date.now()).toBeGreaterThanOrEqual(second.retryAt!);

    await s.route(`review 3 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlockExpired === undefined &&
      host.actors.status(actor.id).activationBlocked === undefined, "the second probe to clear the block");
    expect(host.actors.messages(actor.id).filter((message) => message.direction === "out").at(-1)).not.toHaveProperty("error");
  }, 120_000);

  it("a hard block (rejected credentials) never expires; only a completed run clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 AUTH_FAILS=1#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the hard block");
    const blocked = s.presence(actor.id)!.activationBlocked!;
    expect(blocked).toMatchObject({ code: "auth", class: "hard", count: 1 });
    expect(blocked.classification).toMatch(/^permanent: credential or authentication failure/);
    expect(blocked).not.toHaveProperty("retryAt");
    expect(blocked).not.toHaveProperty("backoffMs");
    // Well past any transient cooldown and cap: still blocked, still skipped by the router.
    await new Promise((resolve) => setTimeout(resolve, MAX_MS + COOLDOWN_MS));
    expect(s.routable(actor.id)).toBe(false);
    expect(s.presence(actor.id)).toMatchObject({ activationBlocked: { code: "auth", class: "hard" } });
    expect(host.actors.status(actor.id)).toMatchObject({ activationBlocked: { code: "auth", class: "hard" } });
    expect(host.actors.status(actor.id)).not.toHaveProperty("activationBlockExpired");

    // Fixed (the credentials work again): an explicit operator ask completes and clears it.
    await expect(host.actors.ask(actor.id, `repair probe AUTH_FAILS=1#${s.token}`)).resolves.toBeDefined();
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlocked === undefined, "the repair run to clear the block");
    await waitFor(() => s.routable(actor.id), "the clear to be published");
  }, 120_000);

  it("the block, its class, time and backoff survive a host restart, then expire and clear", async () => {
    const s = fixture(6_000);
    let host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => host.actors.status(actor.id).activationBlocked !== undefined && settled(host, actor.id), "the block");
    const blocked = host.actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ class: "transient", backoffMs: 6_000, retryAt: blocked.blockedAt! + 6_000 });

    await host.close();
    // The registry record on disk carries the class, the time and the backoff.
    const registry = (fs.readdirSync(s.config.cwd, { recursive: true, encoding: "utf8" }) as string[])
      .filter((file) => path.basename(file) === "actors.json")
      .map((file) => fs.readFileSync(path.join(s.config.cwd, file), "utf8")).find((text) => text.includes(actor.id)) ?? "";
    expect(registry).toContain(`"backoffMs":6000`);
    expect(registry).toContain(`"class":"transient"`);
    expect(registry).toContain(`"blockedAt":${blocked.blockedAt}`);

    host = await s.start();
    expect(Date.now()).toBeLessThan(blocked.retryAt!);
    expect(host.actors.status(actor.id).activationBlocked).toEqual(blocked);
    await waitFor(() => s.presence(actor.id)?.activationBlocked?.retryAt === blocked.retryAt, "the restored block to be published");
    expect(s.routable(actor.id)).toBe(false);
    await waitFor(() => s.routable(actor.id), "the restored cooldown to expire", 30_000);
    expect(Date.now()).toBeGreaterThanOrEqual(blocked.retryAt!);

    await s.route(`review 2 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlockExpired === undefined &&
      host.actors.status(actor.id).activationBlocked === undefined, "the probe after restart to clear the block");
  }, 120_000);
});
