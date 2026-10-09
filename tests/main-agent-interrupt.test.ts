import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, type Context } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, type ExtensionAPI, type AgentSession } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { loadFabricConfig } from "../src/config.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore } from "../src/mesh/store.js";

const from = { id: "session:receiver", name: "owner", kind: "main" as const };
const second = { id: "session:second", name: "second", kind: "main" as const };
const closers: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.unstubAllEnvs();
});
const wait = async (check: () => boolean) => vi.waitFor(() => expect(check()).toBe(true), { timeout: 5000, interval: 5 });
const router = (main: MainAgentController, identity = from) => new AgentMessageRouter(
  { status: () => { throw new Error("Unknown Fabric agent"); } } as any,
  { identity } as any, main,
  { get: () => undefined, scheduleRefresh: () => {}, lastKnown: () => undefined } as any,
  undefined, binding => binding,
);
const setup = async (flushMs = 120_000, interruptFrom: string[] = []) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-interrupt-"));
  closers.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { interruptFrom } }));
  const config = loadFabricConfig({ cwd: root, agentDir, projectTrusted: true });
  const faux = fauxProvider();
  const runtime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "absent-auth.json") });
  runtime.registerNativeProvider(faux.provider);
  let main!: MainAgentController;
  let aborts = 0;
  let abortAt = 0;
  const settings = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
  const loader = new DefaultResourceLoader({ cwd: root, agentDir: path.join(root, "agent"), settingsManager: settings,
    noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [(pi: ExtensionAPI) => {
      pi.on("tool_execution_start", (_event, ctx) => {
        const abort = ctx.abort;
        ctx.abort = () => { aborts++; abortAt = performance.now(); abort(); };
      });
      pi.on("session_start", (_event, ctx) => {
        main = new MainAgentController(pi, "session:receiver", true, root, undefined, true, undefined, { interruptFrom: () => config.agents.interruptFrom });
        main.attachFollowUpDrain(ctx, flushMs, path.join(root, "journal.json"));
      });
    }],
  });
  await loader.reload();
  const { session } = await createAgentSession({ cwd: root, agentDir: path.join(root, "agent"), modelRuntime: runtime,
    model: faux.getModel(), resourceLoader: loader, settingsManager: settings, sessionManager: SessionManager.inMemory(root), tools: ["bash"] });
  closers.push(async () => { await session.abort(); main.closeFollowUpDrain(); session.dispose(); });
  await session.bindExtensions({});
  const events: Array<{ type: string; at: number; event: unknown }> = [];
  session.subscribe(event => events.push({ type: event.type, at: performance.now(), event }));
  const contexts: Context[] = [];
  const reply = (text: string) => (context: Context) => { contexts.push(context); return fauxAssistantMessage(text); };
  return { root, faux, session, main, events, contexts, reply, aborts: () => aborts, abortAt: () => abortAt };
};
const work = (seconds: number) => fauxAssistantMessage(fauxToolCall("bash", {
  command: seconds === 0
    ? 'echo $$ > bash-pid; printf entered > entered; while ! test -f release; do sleep 0.01; done; printf completed > completed'
    : `echo $$ > bash-pid; printf entered > entered; sleep ${seconds}; printf completed > completed`,
  timeout: 30,
}), { stopReason: "toolUse" });
const received = (session: AgentSession) => session.messages.filter(message => message.role === "custom" &&
  (message as { customType?: string }).customType === "pi-fabric-agent-message");

// Actual Pi agent loop and native bash process group; only the model is deterministic.
describe("Main interrupt-priority steer (#7452)", () => {
  it.each([0, 120_000])("mesh interrupt aborts long bash within 1s and delivers HOLD next (flushMs=%s)", async flushMs => {
    const f = await setup(flushMs);
    f.faux.setResponses([work(30), f.reply("HOLD received; no retry")]);
    const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
    const owner = new FabricControlPlane(mesh, { id: "host:receiver", name: "receiver", kind: "main" }, {
      enabled: true, hostId: "host:receiver", pollMs: 20,
    });
    const sender = new FabricControlPlane(mesh, from, { enabled: true, hostId: from.id, pollMs: 20 });
    closers.push(() => owner.close(), () => sender.close());
    owner.start((command, identity, signal) => router(f.main).acceptControl(command, identity, signal, "mesh"));
    sender.start(() => ({ accepted: false }));
    const run = f.session.prompt("start a long check");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      const sentAt = performance.now();
      const receipt = await sender.request("host:receiver", f.main.id, "steer", { message: "HOLD: do not start the change", priority: "interrupt" });
      await wait(() => f.events.some(item => item.type === "tool_execution_end"));
      const ended = f.events.find(item => item.type === "tool_execution_end")!;
      const latencyMs = ended.at - sentAt;
      const abortLatencyMs = ended.at - f.abortAt();
      const metrics = { case: "mesh-interrupt", flushMs, sendToToolEndMs: latencyMs, nativeAbortToToolEndMs: abortLatencyMs };
      if (process.env.FABRIC_7452_EVIDENCE_DIR) fs.appendFileSync(path.join(process.env.FABRIC_7452_EVIDENCE_DIR, "interrupt-metrics.jsonl"), JSON.stringify(metrics) + "\n");
      console.log(JSON.stringify(metrics));
      expect(f.abortAt()).toBeGreaterThan(sentAt);
      expect(latencyMs).toBeLessThan(1000);
      expect(abortLatencyMs).toBeLessThan(1000);
      expect(f.aborts()).toBe(1);
      expect((ended.event as { isError: boolean }).isError).toBe(true);
      await run;
      await f.session.waitForIdle();
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(false);
      expect(received(f.session)).toHaveLength(1);
      expect(JSON.stringify(received(f.session)[0])).toContain(receipt.messageId);
      expect(JSON.stringify(f.contexts[0]?.messages)).toContain("HOLD: do not start the change");
      expect(f.contexts[0]?.messages.at(-1)?.role).toBe("user");
      expect(f.events.filter(item => item.type === "tool_execution_start")).toHaveLength(1); // No tool retry.
      expect(f.events.filter(item => item.type === "agent_start")).toHaveLength(2); // New HOLD run, not aborted continuation.
      const command = mesh.read({ topic: "fabric.control.command" })[0]!.data;
      expect(command).toMatchObject({ operation: "steer", priority: "interrupt" });
      console.log(JSON.stringify({ case: "mesh-interrupt", flushMs, latencyMs, messageId: receipt.messageId, bashExecutions: 1 }));
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  }, 15_000);

  it("delivers interrupt before an older held followUp", async () => {
    const f = await setup();
    f.faux.setResponses([work(30), f.reply("HOLD received first"), f.reply("older followUp read")]);
    const run = f.session.prompt("start work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      f.main.deliverAgent({ from, verification: "mesh", message: "older followUp", delivery: "followUp" });
      f.main.deliverAgent({ from, verification: "mesh", message: "priority HOLD", delivery: "steer", priority: "interrupt" });
      await run;
      await f.session.waitForIdle();
      const delivered = received(f.session);
      expect(delivered).toHaveLength(2);
      expect(JSON.stringify(delivered[0])).toContain("priority HOLD");
      expect(JSON.stringify(delivered[1])).toContain("older followUp");
      expect(f.aborts()).toBe(1);
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("coalesces distinct authorized senders in arrival order and aborts only once", async () => {
    const f = await setup(120_000, [second.id]);
    f.faux.setResponses([work(30), f.reply("first HOLD"), f.reply("second HOLD")]);
    const run = f.session.prompt("start work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      f.main.deliverAgent({ from, verification: "mesh", message: "first priority HOLD", delivery: "steer", priority: "interrupt" });
      const excess = f.main.deliverAgent({ from: second, verification: "mesh", message: "second priority HOLD", delivery: "steer", priority: "interrupt" });
      expect(excess.reason).toContain("ordinary steer");
      await run;
      await f.session.waitForIdle();
      const delivered = received(f.session);
      expect(delivered).toHaveLength(2);
      expect(JSON.stringify(delivered[0])).toContain("first priority HOLD");
      expect(JSON.stringify(delivered[1])).toContain("second priority HOLD");
      expect(f.aborts()).toBe(1);
      expect(f.events.filter(item => item.type === "tool_execution_start")).toHaveLength(1);
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("owner stop after interrupt admission still prevents an automatic wake", async () => {
    const f = await setup();
    f.faux.setResponses([work(30), f.reply("must not wake")]);
    const run = f.session.prompt("start work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      f.main.deliverAgent({ from, verification: "mesh", message: "priority HOLD", delivery: "steer", priority: "interrupt" });
      f.main.stop();
      await run;
      await f.session.waitForIdle();
      expect(f.contexts).toHaveLength(0);
      expect(f.events.filter(item => item.type === "agent_start")).toHaveLength(1);
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(false);
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("ordinary steer does not abort bash or deliver before its boundary", async () => {
    const f = await setup();
    f.faux.setResponses([work(0), f.reply("ordinary steer read")]);
    const run = f.session.prompt("ordinary work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      f.main.deliverAgent({ from, verification: "mesh", message: "ordinary correction", delivery: "steer" });
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(received(f.session)).toHaveLength(0);
      expect(f.events.some(item => item.type === "tool_execution_end")).toBe(false);
      fs.writeFileSync(path.join(f.root, "release"), "1");
      await run;
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(true);
      expect(received(f.session)).toHaveLength(1);
      expect(f.events.filter(item => item.type === "agent_start")).toHaveLength(1);
      expect(JSON.stringify(f.contexts[0]?.messages)).toContain("ordinary correction");
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("idle Main handles interrupt priority as a plain steer", async () => {
    const f = await setup();
    f.faux.setResponses([f.reply("idle correction read")]);
    const outcome = f.main.deliverAgent({ from, verification: "mesh", message: "idle HOLD", delivery: "steer", priority: "interrupt" });
    expect(outcome.triggered).toBe(true);
    await wait(() => f.contexts.length === 1);
    await f.session.waitForIdle();
    expect(received(f.session)).toHaveLength(1);
    expect(f.events.some(item => item.type === "tool_execution_start")).toBe(false);
    expect(f.aborts()).toBe(0);
  });

  it("unauthorized task sender is refused before delivery for ordinary and interrupt steer", async () => {
    const f = await setup();
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", "1");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "owned-task");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", "session:allowed-spawner");
    vi.stubEnv("PI_FABRIC_SPAWNER_CHAIN", "[]");
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", "[]");
    const task = router(f.main);
    for (const priority of [undefined, "interrupt"] as const) {
      await expect(task.routeMessage(f.main.id, "unauthorized HOLD", undefined, "steer", undefined, priority ? { priority } : {}))
        .rejects.toMatchObject({ code: "FABRIC_TASK_ESCALATION_TARGET_DENIED" });
    }
    expect(received(f.session)).toHaveLength(0);
    expect(f.events.some(item => item.type === "agent_start")).toBe(false);
    expect(f.aborts()).toBe(0);
    // The same authority check applies while a real tool is running, before native abort.
    f.faux.setResponses([work(0), f.reply("authorized work completes")]);
    const run = f.session.prompt("keep working");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      await expect(task.routeMessage(f.main.id, "unauthorized interrupt", undefined, "steer", undefined, { priority: "interrupt" }))
        .rejects.toMatchObject({ code: "FABRIC_TASK_ESCALATION_TARGET_DENIED" });
      expect(f.aborts()).toBe(0);
      expect(f.events.some(item => item.type === "tool_execution_end")).toBe(false);
      fs.writeFileSync(path.join(f.root, "release"), "1");
      await run;
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(true);
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("ordinary steer access cannot abort or deliver an unauthorized interrupt", async () => {
    const f = await setup();
    f.faux.setResponses([work(0), f.reply("ordinary peer steer read")]);
    const run = f.session.prompt("keep working");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      await expect(router(f.main, second).routeMessage(f.main.id, "forbidden HOLD", undefined, "steer", undefined, { priority: "interrupt" }))
        .rejects.toMatchObject({ code: "FABRIC_INTERRUPT_NOT_AUTHORIZED" });
      expect(f.aborts()).toBe(0); expect(received(f.session)).toHaveLength(0);
      await router(f.main, second).routeMessage(f.main.id, "ordinary peer correction", undefined, "steer");
      fs.writeFileSync(path.join(f.root, "release"), "1"); await run;
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(true);
      expect(received(f.session)).toHaveLength(1);
      expect(JSON.stringify(f.contexts)).not.toContain("forbidden HOLD");
      expect(JSON.stringify(f.contexts)).toContain("ordinary peer correction");
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("a host-allowlisted session aborts the native tool", async () => {
    const f = await setup(120_000, [second.id]);
    f.faux.setResponses([work(30), f.reply("allowlisted HOLD read")]);
    const run = f.session.prompt("start work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      await router(f.main, second).routeMessage(f.main.id, "allowlisted HOLD", undefined, "steer", undefined, { priority: "interrupt" });
      await run; await f.session.waitForIdle();
      expect(f.aborts()).toBe(1); expect(fs.existsSync(path.join(f.root, "completed"))).toBe(false);
      expect(JSON.stringify(received(f.session))).toContain("allowlisted HOLD");
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("cannot abort successive tools in the interrupted turn or repeat a sender inside cooldown", async () => {
    const f = await setup(120_000, [second.id]);
    f.faux.setResponses([work(30), work(0), f.reply("second tool finished with ordinary steer")]);
    const run = f.session.prompt("start work");
    try {
      await wait(() => fs.existsSync(path.join(f.root, "entered")));
      f.main.deliverAgent({ from, verification: "mesh", message: "first HOLD", delivery: "steer", priority: "interrupt" });
      await wait(() => f.events.filter(item => item.type === "tool_execution_start").length === 2);
      expect(() => f.main.deliverAgent({ from, verification: "mesh", message: "rate refused", delivery: "steer", priority: "interrupt" }))
        .toThrow(expect.objectContaining({ code: "FABRIC_INTERRUPT_RATE_LIMITED" }));
      f.main.deliverAgent({ from: second, verification: "mesh", message: "ordinary same-turn correction", delivery: "steer", priority: "interrupt" });
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(f.aborts()).toBe(1);
      expect(f.events.filter(item => item.type === "tool_execution_end")).toHaveLength(1);
      fs.writeFileSync(path.join(f.root, "release"), "1");
      await run; await f.session.waitForIdle();
      expect(fs.existsSync(path.join(f.root, "completed"))).toBe(true);
      expect(f.aborts()).toBe(1); expect(received(f.session)).toHaveLength(2);
      expect(JSON.stringify(f.contexts)).toContain("ordinary same-turn correction");
      expect(JSON.stringify(f.contexts)).not.toContain("rate refused");
    } finally { await f.session.abort(); await run.catch(() => undefined); }
  });

  it("validates priority and refuses non-Main targets instead of downgrading", async () => {
    const f = await setup();
    expect(() => f.main.deliverAgent({ from, verification: "mesh", message: "bad", delivery: "steer", priority: "urgent" as any })).toThrow("priority must");
    expect(() => f.main.deliverAgent({ from, verification: "mesh", message: "bad", delivery: "followUp", priority: "interrupt" })).toThrow("priority must");
    expect(() => f.main.deliverAgent({ from, verification: "mesh", message: "bad", delivery: "steer", priority: "interrupt", triggerTurn: false })).toThrow("triggering steer");
    await expect(router(f.main).routeMessage("actor:other", "HOLD", undefined, "steer", undefined, { priority: "interrupt" })).rejects.toThrow("only for a Main");
    const plane = new FabricControlPlane(new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100), from, { enabled: true, hostId: from.id });
    closers.push(() => plane.close());
    await expect(plane.request("owner", f.main.id, "steer", { priority: "urgent" as any })).rejects.toThrow("priority must");
    await expect(plane.request("owner", f.main.id, "followUp", { priority: "interrupt" })).rejects.toThrow("priority must");
  });
});
