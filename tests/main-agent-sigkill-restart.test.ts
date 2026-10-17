import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";

const workspace = path.resolve(".");
const mainAgentUrl = new URL("../src/main-agent.ts", import.meta.url).href;

const childSource = `
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { MainAgentController } from ${JSON.stringify(mainAgentUrl)};

const mode = process.argv[1];
const root = process.argv[2];
const journal = process.argv[3];
const sessionFile = process.argv[4];
const deliveryId = "sigkill-delivery";
const eventId = "sigkill-event";
const actor = { id: "actor-1", name: "sigkill-test", kind: "actor" };
if (!root || !journal || (mode !== "admit" && mode !== "restart")) throw new Error("bad child arguments");

const waitFor = async (predicate) => {
  const deadline = Date.now() + 15_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("child wait timed out");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
};

const faux = fauxProvider();
const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
modelRuntime.registerNativeProvider(faux.provider);
let main;
let toolStarted = false;
const loader = new DefaultResourceLoader({
  cwd: root,
  agentDir: path.join(root, "agent"),
  noSkills: true,
  noPromptTemplates: true,
  noThemes: true,
  noContextFiles: true,
  extensionFactories: [{
    name: "sigkill-delivery",
    factory: (pi) => {
      if (mode === "admit") pi.registerTool({
        name: "block", label: "block", description: "block until SIGKILL", parameters: Type.Object({}),
        execute: async () => {
          toolStarted = true;
          process.stdout.write("TOOL_STARTED\\n");
          await new Promise(() => {});
          return { content: [{ type: "text", text: "never" }], details: {} };
        },
      });
      pi.on("session_start", (_event, ctx) => {
        main = new MainAgentController(pi, "session:root", true, root, "root");
        main.attachFollowUpDrain(ctx, 60_000, journal);
      });
    },
  }],
});
await loader.reload();
const manager = mode === "admit"
  ? SessionManager.create(root, path.join(root, "sessions"))
  : SessionManager.open(sessionFile);
const { session } = await createAgentSession({
  cwd: root,
  agentDir: path.join(root, "agent"),
  modelRuntime,
  model: faux.getModel(),
  resourceLoader: loader,
  sessionManager: manager,
  tools: mode === "admit" ? ["block"] : [],
});
await session.bindExtensions({});
await waitFor(() => main !== undefined);

if (mode === "admit") {
  faux.setResponses([fauxAssistantMessage(fauxToolCall("block", {}), { stopReason: "toolUse" })]);
  void session.prompt("begin");
  await waitFor(() => toolStarted);
  main.deliverAgent({ from: actor, message: "exactly once", delivery: "steer", deliveryId,
    outcomeSend: { eventId, to: main.id, from: actor.id, mode: "steer" } });
  const file = manager.getSessionFile();
  if (!file) throw new Error("session file was not created");
  process.stdout.write("SESSION_FILE:" + file + "\\nADMITTED\\n");
  await new Promise(() => {});
}

faux.setResponses([fauxAssistantMessage("resumed")]);
await session.prompt("resume");
await waitFor(() => session.isIdle);
const delivered = session.messages.filter(message => message.role === "custom" && message.customType === "pi-fabric-agent-message");
const outcomeDir = path.join(path.dirname(path.dirname(journal)), "delivery-outcomes");
const rows = fs.existsSync(outcomeDir)
  ? fs.readdirSync(outcomeDir).flatMap(file => fs.readFileSync(path.join(outcomeDir, file), "utf8").trim().split("\\n").filter(Boolean).map(line => JSON.parse(line)))
  : [];
const duplicate = main.deliverAgent({ from: actor, message: "duplicate", delivery: "steer", deliveryId });
process.stdout.write("DELIVERED_COUNT:" + delivered.length + "\\nDELIVERED_ID:" + (delivered[0]?.details?.deliveryId ?? "") + "\\nDUPLICATE:" + Boolean(duplicate.duplicate) + "\\nOUTCOME_COUNT:" + rows.filter(row => row.eventId === eventId).length + "\\n");
main.closeFollowUpDrain();
session.dispose();
`;

const spawnChild = (mode: "admit" | "restart", root: string, journal: string, sessionFile?: string) => {
  const child = spawn("bun", ["-e", childSource, mode, root, journal, sessionFile ?? ""], {
    cwd: workspace,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NODE_ENV: "test" },
  });
  let output = "";
  let error = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { error += chunk; });
  child.once("error", cause => { error += String(cause); });
  // Track termination from spawn, before any output can arrive. `close` also drains stdio.
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  const deadline = setTimeout(() => child.kill("SIGKILL"), 30_000);
  void closed.then(() => clearTimeout(deadline));
  return { child, closed, output: () => output, error: () => error };
};

const waitForOutput = (run: ReturnType<typeof spawnChild>, marker: string) => new Promise<void>((resolve, reject) => {
  const finish = (failure?: Error) => {
    clearTimeout(deadline);
    run.child.stdout.off("data", check);
    if (failure) reject(failure); else resolve();
  };
  const check = () => { if (run.output().includes(marker)) finish(); };
  const deadline = setTimeout(() => finish(new Error(`timed out waiting for ${marker}: ${run.error()}\n${run.output()}`)), 20_000);
  run.child.stdout.on("data", check);
  void run.closed.then(() => run.output().includes(marker) ? finish() : finish(new Error(`child closed: ${run.error()}\n${run.output()}`)));
  check();
});

const stopChildren = async (runs: ReturnType<typeof spawnChild>[]) => {
  await Promise.all(runs.map(async run => {
    if (run.child.exitCode === null && run.child.signalCode === null) run.child.kill("SIGKILL");
    await run.closed;
  }));
};

const line = (output: string, prefix: string) => output.split("\n").find(value => value.startsWith(prefix))?.slice(prefix.length);

describe("real OS-process Main delivery restart", () => {
  // Windows does not expose a genuine POSIX SIGKILL exit signal; run this fault on Unix.
  it.skipIf(process.platform === "win32").each([1, 2, 3, 4, 5])("recovers once after SIGKILL (attempt %i)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-sigkill-"));
    const journal = path.join(root, "main-followups", "root.json");
    const runs: ReturnType<typeof spawnChild>[] = [];
    try {
      const admitted = spawnChild("admit", root, journal);
      runs.push(admitted);
      await waitForOutput(admitted, "ADMITTED\n");
      const sessionFile = line(admitted.output(), "SESSION_FILE:");
      expect(sessionFile).toBeTruthy();
      expect(fs.existsSync(journal)).toBe(true);
      expect(fs.readFileSync(journal, "utf8")).toContain("sigkill-delivery");
      expect(fs.readFileSync(sessionFile!, "utf8")).not.toContain("sigkill-delivery");
      expect(admitted.child.pid).toBeTruthy();
      process.kill(admitted.child.pid!, "SIGKILL");
      const killed = await admitted.closed;
      expect(killed.signal).toBe("SIGKILL");

      const restarted = spawnChild("restart", root, journal, sessionFile!);
      runs.push(restarted);
      const result = await restarted.closed;
      await waitForOutput(restarted, "OUTCOME_COUNT:1\n"); // also exercise observing an already-closed child
      expect(result.code).toBe(0);
      expect(line(restarted.output(), "DELIVERED_COUNT:")).toBe("1");
      expect(line(restarted.output(), "DELIVERED_ID:")).toBe("sigkill-delivery");
      expect(line(restarted.output(), "DUPLICATE:")).toBe("true");
      expect(line(restarted.output(), "OUTCOME_COUNT:")).toBe("1");
      expect(fs.existsSync(journal)).toBe(false);
    } finally {
      await stopChildren(runs);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it.skipIf(process.platform === "win32")("reaps its blocked admission child before cleaning up a failed attempt", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-sigkill-cleanup-"));
    const admitted = spawnChild("admit", root, path.join(root, "main-followups", "root.json"));
    await expect((async () => {
      try {
        await waitForOutput(admitted, "ADMITTED\n");
        throw new Error("injected assertion failure before SIGKILL");
      } finally {
        await stopChildren([admitted]);
        fs.rmSync(root, { recursive: true, force: true });
      }
    })()).rejects.toThrow("injected assertion failure before SIGKILL");
    expect((await admitted.closed).signal).toBe("SIGKILL");
    expect(fs.existsSync(root)).toBe(false);
  }, 30_000);
});
