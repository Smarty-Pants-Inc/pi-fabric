import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { parseDurableWorkerOptions } from "../src/durable/worker-options.js";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-host-"));
const extension = path.resolve("tests/fixtures/durable-pi-extension.ts");
let entry: string;
beforeAll(async () => {
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "junction");
  fs.writeFileSync(path.join(root, "package.json"), '{"type":"module"}');
  entry = path.join(root, "build", "worker.js");
  await build({ entryPoints: ["src/durable/worker.ts"], outdir: path.dirname(entry), bundle: true, packages: "external", platform: "node", format: "esm", splitting: true, target: "node24", logLevel: "silent" });
}, 30000);
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

async function worker(name: string, extra: string[] = [], environment: NodeJS.ProcessEnv = {}, discover = false) {
  const cwd = path.join(root, name); fs.mkdirSync(cwd, { recursive: true });
  const agentDir = path.join(cwd, "agent"); fs.mkdirSync(agentDir, { recursive: true });
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ defaultProjectTrust: environment.DURABLE_TEST_TRUST ?? "ask", compaction: { enabled: false }, retry: { enabled: false } }));
  const fixture = path.join(cwd, "fixture.txt"); if (!fs.existsSync(fixture)) fs.writeFileSync(fixture, "private fixture");
  const effect = path.join(cwd, "effect.txt");
  const child = spawn(process.execPath, [entry, "--mode", "rpc", "--durable-directory", path.join(cwd, "durable"), "--durable-run-id", name,
    "--session", path.join(cwd, "session.jsonl"), ...(discover ? [] : ["--no-extensions"]), "-e", extension, "--model", "durable-offline/test", ...extra],
    { cwd, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, DURABLE_TEST_FAUX_MODULE: import.meta.resolve("@earendil-works/pi-ai/providers/faux"), DURABLE_TEST_FILE: fixture, DURABLE_TEST_EFFECT: effect, ...environment }, stdio: ["pipe", "pipe", "pipe"] }) as ChildProcessWithoutNullStreams;
  const messages: any[] = []; let pending = "", stderr = ""; const change = new Set<() => void>();
  child.stdout.on("data", chunk => {
    pending += chunk.toString();
    let end: number;
    while ((end = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, end); pending = pending.slice(end + 1);
      try { messages.push(JSON.parse(line)); } catch { stderr += `\nNon-RPC output: ${line}`; }
    }
    for (const notify of change) notify();
  });
  child.stderr.on("data", chunk => { stderr += chunk.toString(); });
  child.on("exit", () => { for (const notify of change) notify(); });
  const send = (command: Record<string, unknown>) => child.stdin.write(`${JSON.stringify(command)}\n`);
  const wait = (predicate: (message: any) => boolean, from = 0) => new Promise<any>((resolve, reject) => {
    const timeout = setTimeout(() => done(new Error(`RPC timeout: ${stderr}\n${JSON.stringify(messages)}`)), 15000);
    const done = (error?: Error, value?: any) => { clearTimeout(timeout); change.delete(check); error ? reject(error) : resolve(value); };
    const check = () => {
      const found = messages.slice(from).find(predicate);
      if (found) done(undefined, found);
      else if (child.exitCode !== null || child.signalCode !== null) done(new Error(`Worker exited: ${stderr}\n${JSON.stringify(messages)}`));
    };
    change.add(check); check();
  });
  const stop = async (kill = false) => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exited = once(child, "exit");
    if (kill) child.kill("SIGKILL"); else child.stdin.end();
    await exited;
  };
  send({ type: "get_state", id: "ready" });
  try { await wait(message => message.type === "response" && message.id === "ready" && message.success); }
  catch (error) { await stop(true); throw error; }
  return { child, messages, send, wait, stop, cwd, fixture, effect };
}

const answer = (messages: any[]) => messages.filter(event => event.type === "message_end" && event.message?.role === "assistant")
  .flatMap(event => event.message.content).filter(part => part.type === "text").map(part => part.text).join("\n");

describe("durable native RPC worker", () => {
  it("parses the internal protocol, appends both prompts, and rejects unknown or missing options", () => {
    const args = ["--durable-directory", "state", "--durable-run-id", "run", "--append-system-prompt", "instructions", "--append-system-prompt", "schema", "--tools", "read,bash"];
    expect(parseDurableWorkerOptions(args).appendSystemPrompt).toBe("instructions\n\nschema");
    expect(parseDurableWorkerOptions(args).tools).toEqual(["read", "bash"]);
    expect(() => parseDurableWorkerOptions([])).toThrow("require");
    expect(() => parseDurableWorkerOptions([...args, "--unsafe"])).toThrow("Unsupported");
    expect(() => parseDurableWorkerOptions([...args, "--model"])).toThrow("Missing");
    expect(() => parseDurableWorkerOptions([...args, "--no-tools"])).toThrow("mutually exclusive");
  });

  it("runs real SDK tools, exports history, and reopens completed work without repeating it", async () => {
    const first = await worker("read", ["--tools", "read"]);
    try {
      first.send({ type: "prompt", id: "request-1", message: "read fixture" });
      await first.wait(event => event.type === "agent_settled");
      expect(answer(first.messages)).toContain("private fixture");
      expect(first.messages.some(event => event.type === "tool_execution_end")).toBe(true);
      expect(fs.readFileSync(path.join(first.cwd, "session.jsonl"), "utf8")).toContain("private fixture");
    } finally { await first.stop(); }
    const reopened = await worker("read", ["--tools", "read"]);
    try {
      reopened.send({ type: "prompt", id: "request-1", message: "read fixture" });
      await reopened.wait(event => event.type === "agent_settled");
      expect(reopened.messages.filter(event => event.type === "tool_execution_start")).toHaveLength(0);
    } finally { await reopened.stop(); }
  }, 40000);

  it("keeps native image input, model selection, and distinct later prompts", async () => {
    const host = await worker("images", ["--no-tools"]);
    try {
      host.send({ type: "set_thinking_level", id: "thinking", level: "off" });
      expect(await host.wait(event => event.type === "response" && event.id === "thinking")).toMatchObject({ success: true });
      host.send({ type: "prompt", id: "first", message: "inspect", images: [{ type: "image", mimeType: "image/png", data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==" }] });
      await host.wait(event => event.type === "agent_settled");
      expect(answer(host.messages)).toContain('"images":1');
      const boundary = host.messages.length;
      host.send({ type: "prompt", id: "second", message: "next" });
      await host.wait(event => event.type === "agent_settled", boundary);
      expect(answer(host.messages.slice(boundary))).toContain('"reply":"next"');
      host.send({ type: "get_state", id: "selected" });
      expect(await host.wait(event => event.id === "selected")).toMatchObject({ success: true, data: { model: { provider: "durable-offline", id: "test" }, thinkingLevel: "off" } });
    } finally { await host.stop(); }
  }, 30000);

  it("does not execute project extensions before headless trust, but honors explicit global trust", async () => {
    const cwd = path.join(root, "trust");
    const extensions = path.join(cwd, ".pi", "extensions");
    fs.mkdirSync(extensions, { recursive: true });
    const marker = path.join(cwd, "project-loaded");
    fs.writeFileSync(path.join(extensions, "probe.ts"), `import fs from "node:fs"; export default function () { fs.writeFileSync(${JSON.stringify(marker)}, "loaded"); }`);
    const denied = await worker("trust", ["--no-tools"], {}, true);
    try { expect(fs.existsSync(marker)).toBe(false); } finally { await denied.stop(); }
    const trusted = await worker("trust", ["--no-tools"], { DURABLE_TEST_TRUST: "always" }, true);
    try { expect(fs.readFileSync(marker, "utf8")).toBe("loaded"); } finally { await trusted.stop(); }
  }, 30000);

  it("accepts the first new prompt on a revisited session instead of deduplicating it", async () => {
    const host = await worker("switching", ["--no-tools"]);
    const prompt = async (text: string) => {
      const start = host.messages.length;
      host.send({ type: "prompt", id: text, message: text });
      await host.wait(event => event.type === "agent_settled", start);
      expect(answer(host.messages.slice(start))).toContain(JSON.stringify(text));
    };
    const command = async (id: string, args: Record<string, unknown>) => {
      host.send({ ...args, id });
      const response = await host.wait(event => event.type === "response" && event.id === id);
      expect(response, JSON.stringify(response)).toMatchObject({ success: true });
      return response;
    };
    try {
      await prompt("initial A");
      const initial = await command("state-a", { type: "get_state" });
      await command("new-b", { type: "new_session" });
      await prompt("initial B");
      const second = await command("state-b", { type: "get_state" });
      await command("switch-a", { type: "switch_session", sessionPath: initial.data.sessionFile });
      await prompt("later A");
      await command("switch-b", { type: "switch_session", sessionPath: second.data.sessionFile });
      await prompt("later B");
    } finally { await host.stop(); }
  }, 45000);

  it("preserves real write confinement through the SDK tool_call hook", async () => {
    const guard = path.resolve("src/agents/write-guard.ts");
    const host = await worker("guard", ["--tools", "write", "-e", guard], {
      PI_FABRIC_WRITE_POLICY: JSON.stringify({ readOnly: true, writableRoots: [], shell: "deny" }),
    });
    try {
      host.send({ type: "prompt", id: "guard", message: "write fixture" });
      await host.wait(event => event.type === "agent_settled");
      expect(fs.readFileSync(host.fixture, "utf8")).toBe("private fixture");
      expect(answer(host.messages)).toMatch(/denied|read.only|confine/i);
    } finally { await host.stop(); }
  }, 30000);

  it("does not rerun an uncertain unsafe tool after confirmed process death", async () => {
    const first = await worker("crash", ["--tools", "hold_effect"]);
    try {
      first.send({ type: "prompt", id: "crash", message: "hold effect" });
      await first.wait(event => event.type === "tool_execution_start");
      await expect.poll(() => fs.existsSync(first.effect), { timeout: 5000 }).toBe(true);
    } finally { await first.stop(true); }
    const resumed = await worker("crash", ["--tools", "hold_effect"]);
    try {
      resumed.send({ type: "prompt", id: "crash", message: "hold effect" });
      await resumed.wait(event => event.type === "agent_settled");
      expect(fs.readFileSync(resumed.effect, "utf8")).toBe("effect\n");
      expect(resumed.messages.some(event => JSON.stringify(event).match(/unsafe|interrupted|replay|failed/i))).toBe(true);
    } finally { await resumed.stop(); }
  }, 40000);
});
