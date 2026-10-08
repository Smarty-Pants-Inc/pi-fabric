import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { normalizeAgentRouterConfig } from "../src/agents/router-config.js";
import { routeAgentCreation, type SpawnRouterRequest } from "../src/agents/spawn-router.js";

const roots: string[] = [];
const root = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-spawn-router-")); roots.push(dir); return dir; };
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); vi.restoreAllMocks(); vi.unstubAllEnvs(); });
const pick = { model: "provider/model-b", thinking: "high", reason: "normal.policy", policyVersion: "v1" };
const defaults = { model: "provider/model-a", thinking: "medium" as const };
const command = (dir: string, body = `process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`): string[] => [
  process.execPath, "-e", `let input = ''; process.stdin.setEncoding('utf8'); process.stdin.on('data', c => input += c); process.stdin.on('end', () => {
    require('node:fs').writeFileSync(process.argv[1], input); ${body}
  });`, path.join(dir, "request.json"),
];
const options = (dir: string) => ({
  config: { command: command(dir), mode: "enforce" as const }, meshRoot: dir,
  kind: "spawn" as const, role: "task-agent", name: "implementation", cwd: dir, project: dir,
  task: "private task 🦉", parentId: "actor:parent", complexity: "normal" as const, defaults,
  explicit: false,
  validateModel: (model: string) => { if (model !== pick.model) throw new Error("private validation detail"); return model; },
});
const decisions = (dir: string): Array<Record<string, any>> => fs.readFileSync(path.join(dir, "router", "decisions.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
const request = (dir: string): SpawnRouterRequest => JSON.parse(fs.readFileSync(path.join(dir, "request.json"), "utf8"));

describe("spawn router configuration", () => {
  it("is optional and defaults to off/1500/no task disclosure", () => {
    expect(normalizeFabricConfig({}).agents.router).toBeUndefined();
    expect(normalizeFabricConfig({ agents: { router: { command: [process.execPath, "a b"] } } }).agents.router)
      .toEqual({ command: [process.execPath, "a b"], mode: "off", timeoutMs: 1500, includeTask: false });
  });
  it.each([[0, 200], [199, 200], [5001, 5000], [NaN, 1500], [Infinity, 1500]])("bounds deadline %s to %s", (value, expected) => {
    expect(normalizeAgentRouterConfig({ command: [process.execPath], timeoutMs: value })?.timeoutMs).toBe(expected);
  });
  it.each(["router | shell", [], [""], ["router"], ["./router"], ["../router"], [process.execPath, 1], [process.execPath, "\0"]].map(command => ({ command })))("rejects malformed argv $command without shell repair", ({ command }) => {
    expect(normalizeAgentRouterConfig({ command, mode: "enforce" })?.command).toEqual([]);
  });
  it.each([true, false])("workspace cannot enable/override/disclose task (trusted=%s)", projectTrusted => {
    const cwd = root(); const agentDir = root(); fs.mkdirSync(path.join(cwd, ".pi"));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { router: { command: [process.execPath, "host-router"], mode: "shadow" } } }));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: { router: { command: [process.execPath, "workspace-router"], mode: "enforce", includeTask: true } } }));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted }).agents.router)
      .toMatchObject({ command: [process.execPath, "host-router"], mode: "shadow", includeTask: false });
    fs.unlinkSync(path.join(agentDir, "fabric.json"));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted }).agents.router).toBeUndefined();
  });
});

describe("external spawn router behavior", () => {
  it("off starts no command and creates no ledger", async () => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, mode: "off" } })).toBeUndefined();
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(fs.existsSync(path.join(dir, "router"))).toBe(false);
  });
  it.each(["node", "./router", "../router"])("runtime rejects non-absolute executable %s without PATH lookup", async executable => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: [executable, ...command(dir).slice(1)] } })).toBeUndefined();
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "invalid-command" });
  });
  it("passes only the minimal explicit environment, never credentials or loader hooks", async () => {
    const dir = root(); const opts = options(dir);
    for (const name of ["SMARTY_AUTH", "GITHUB_TOKEN", "MY_TOKEN", "API_KEY", "PI_CODING_AGENT_DIR", "NODE_OPTIONS", "LD_PRELOAD"]) {
      vi.stubEnv(name, "must-not-reach-router");
    }
    vi.stubEnv("PATH", dir); vi.stubEnv("HOME", "/router-home"); vi.stubEnv("LANG", "C"); vi.stubEnv("TZ", "UTC");
    const body = `require('node:fs').writeFileSync(process.argv[1] + '.env', JSON.stringify(process.env)); process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(JSON.parse(fs.readFileSync(path.join(dir, "request.json.env"), "utf8"))).toEqual({ PATH: "/usr/bin:/bin", HOME: "/router-home", LANG: "C", TZ: "UTC" });
  });
  it("shadow preserves default and logs validated pick beside actual", async () => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, mode: "shadow" } })).toBeUndefined();
    expect(decisions(dir)).toEqual([expect.objectContaining({ pick, actual: defaults, error: null, mode: "shadow", decision: "default" })]);
  });
  it("enforce selects validated model/thinking and emits one private decision", async () => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation(opts)).toEqual({ model: pick.model, thinking: pick.thinking });
    const req = request(dir);
    expect(req).toMatchObject({ kind: "spawn", role: "task-agent", name: "implementation", cwd: dir, project: dir,
      parentId: "actor:parent", requestedComplexity: "normal", defaults,
      taskDigest: createHash("sha256").update(opts.task).digest("hex"), taskLength: Buffer.byteLength(opts.task), host: os.hostname() });
    expect(req).not.toHaveProperty("task");
    const log = decisions(dir);
    expect(log).toHaveLength(1);
    expect(log[0]).toMatchObject({ actual: { model: pick.model, thinking: pick.thinking }, pick, error: null, decision: "enforce" });
    expect(log[0]!.requestDigest).toMatch(/^[a-f0-9]{64}$/);
    expect(log[0]!.ts).toMatch(/^\d{4}-/);
    expect(log[0]!.latencyMs).toBeGreaterThanOrEqual(0);
    expect(JSON.stringify(log)).not.toContain(opts.task);
  });
  it.each(["simple", "normal", "complex", "delicate"] as const)("forwards %s complexity verbatim to router stdin", async complexity => {
    const dir = root();
    expect(await routeAgentCreation({ ...options(dir), complexity })).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(request(dir).requestedComplexity).toBe(complexity);
  });
  it("full task reaches stdin only with includeTask true, never the ledger", async () => {
    const dir = root(); const opts = options(dir);
    await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true } });
    expect(request(dir).task).toBe(opts.task);
    expect(JSON.stringify(decisions(dir))).not.toContain(opts.task);
  });
  it.each([
    ["normal.policy:task-agent_1-v2", "normal.policy:task-agent_1-v2"],
    ["a".repeat(100), "a".repeat(64)],
    ["private task 🦉", "redacted"],
    ["policy\nsecret-token", "redacted"],
    ["policy\n", "redacted"],
    ["policy\r\n", "redacted"],
    ["UPPERCASE", "redacted"],
    ["", "redacted"],
  ])("stores only a bounded reason/version code for %s", async (reason, expected) => {
    const dir = root(); const opts = options(dir);
    const body = `process.stdout.write(JSON.stringify({ model: '${pick.model}', thinking: '${pick.thinking}', reason: ${JSON.stringify(reason)}, policyVersion: ${JSON.stringify(reason)} }));`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true, command: command(dir, body) } })).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: expected, policyVersion: expected }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(opts.task);
  });
  it.each(["secret-task", "private task 🦉"])("redacts task text echoed through router metadata (%s)", async task => {
    const dir = root(); const opts = { ...options(dir), task };
    const body = `const task = JSON.parse(input).task; process.stdout.write(JSON.stringify({ model: '${pick.model}', thinking: '${pick.thinking}', reason: task, policyVersion: task }));`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true, command: command(dir, body) } })).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: "redacted", policyVersion: "redacted" }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(task);
  });
  it("explicit model/thinking starts no router but logs the actual binding", async () => {
    const dir = root();
    expect(await routeAgentCreation({ ...options(dir), explicit: true })).toBeUndefined();
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(decisions(dir)).toEqual([expect.objectContaining({ decision: "explicit", pick: null, actual: defaults, error: null })]);
  });
  it.each([
    ["process.stdout.write('not JSON');", "invalid-json"],
    ["process.stdout.write('[]');", "invalid-output"],
    ["process.stdout.write('{}');", "invalid-output"],
    ["process.stdout.write(JSON.stringify({ model: 'unknown', thinking: 'high' }));", "unknown-or-denied-model"],
    ["process.stdout.write(JSON.stringify({ model: 'provider/model-b', thinking: 'extreme' }));", "invalid-output"],
    ["process.stdout.write(JSON.stringify({ model: 'provider/model-b', thinking: 'high', reason: {} }));", "invalid-output"],
    ["process.stderr.write('secret task text'); process.exit(17);", "nonzero-exit"],
    ["process.stdout.write('x'.repeat(70000));", "output-too-large"],
  ])("falls back and logs a sanitized error for %s", async (body, error) => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toBeUndefined();
    expect(decisions(dir)).toEqual([expect.objectContaining({ actual: defaults, pick: null, error })]);
    expect(JSON.stringify(decisions(dir))).not.toContain("secret task text");
    expect(JSON.stringify(decisions(dir))).not.toContain("private validation detail");
  });
  it.each([[], ["/definitely/missing/spawn-router"]].map(argv => ({ argv })))("fails open on unavailable command $argv", async ({ argv }) => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: argv } })).toBeUndefined();
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: argv.length ? "command-error" : "missing-command" });
  });
  it("times out, reaps the command and logs fallback before returning", async () => {
    const dir = root(); const opts = options(dir);
    const body = `require('node:fs').writeFileSync(process.argv[1] + '.pid', String(process.pid)); setInterval(() => {}, 1000);`;
    const before = Date.now();
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body), timeoutMs: 200 } })).toBeUndefined();
    expect(Date.now() - before).toBeLessThan(5000);
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "timeout" });
    const pid = Number(fs.readFileSync(path.join(dir, "request.json.pid"), "utf8"));
    await vi.waitFor(() => expect(() => process.kill(pid, 0)).toThrow());
  });
  it("timeout settles and retires a lingering grandchild with inherited pipes", async () => {
    const dir = root(); const opts = options(dir);
    const grandchild = `require('node:fs').writeFileSync(process.argv[1], 'started'); setTimeout(() => require('node:fs').writeFileSync(process.argv[1] + '.late', 'leaked'), 800);`;
    const body = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(grandchild)}, process.argv[1] + '.grandchild'], { stdio: ['ignore', 'inherit', 'inherit'] }); setInterval(() => {}, 1000);`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body), timeoutMs: 400 } })).toBeUndefined();
    expect(decisions(dir)[0]).toMatchObject({ error: "timeout", actual: defaults });
    expect(fs.existsSync(path.join(dir, "request.json.grandchild"))).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 900));
    expect(fs.existsSync(path.join(dir, "request.json.grandchild.late"))).toBe(false);
  });
  it("argv metacharacters are forwarded literally, never interpreted by a shell", async () => {
    const dir = root(); const opts = options(dir); const literal = `space ; $(touch ${path.join(dir, "shell-leak")}) | &`;
    const argv = [...command(dir, `require('node:fs').writeFileSync(process.argv[1] + '.arg', process.argv[2]); process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`), literal];
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: argv } })).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(fs.readFileSync(path.join(dir, "request.json.arg"), "utf8")).toBe(literal);
    expect(fs.existsSync(path.join(dir, "shell-leak"))).toBe(false);
  });

  it("abort retires a running router and falls back", async () => {
    const dir = root(); const opts = options(dir); const controller = new AbortController();
    const pending = routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, "setInterval(() => {}, 1000);") }, signal: controller.signal });
    await vi.waitFor(() => expect(fs.existsSync(path.join(dir, "request.json"))).toBe(true));
    controller.abort(); await expect(pending).resolves.toBeUndefined();
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "aborted" });
  });
  it("a logging failure cannot fail or change an enforce selection", async () => {
    const dir = root(); fs.writeFileSync(path.join(dir, "router"), "not a directory");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32")("creates a private directory and regular ledger with 0700/0600 modes", async () => {
    const dir = root(); await routeAgentCreation({ ...options(dir), explicit: true });
    const directory = fs.lstatSync(path.join(dir, "router"));
    const ledger = fs.lstatSync(path.join(dir, "router/decisions.jsonl"));
    expect(directory.isDirectory()).toBe(true); expect(directory.uid).toBe(process.getuid!());
    expect(directory.mode & 0o777).toBe(0o700);
    expect(ledger.isFile()).toBe(true); expect(ledger.mode & 0o777).toBe(0o600);
  });
  it.skipIf(!process.getuid)("refuses a directory owned by another uid without failing selection", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router"), { mode: 0o700 });
    const uid = process.getuid!(); vi.spyOn(process, "getuid").mockReturnValue(uid + 1);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(fs.existsSync(path.join(dir, "router/decisions.jsonl"))).toBe(false);
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32")("refuses a non-private existing router directory", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router")); fs.chmodSync(path.join(dir, "router"), 0o755);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(fs.existsSync(path.join(dir, "router/decisions.jsonl"))).toBe(false);
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32").each(["directory", "ledger"])("refuses a %s symlink without changing the enforce selection", async target => {
    const dir = root(); const victim = root();
    const sentinel = path.join(victim, "decisions.jsonl"); fs.writeFileSync(sentinel, "untouched");
    if (target === "directory") fs.symlinkSync(victim, path.join(dir, "router"), "dir");
    else {
      fs.mkdirSync(path.join(dir, "router"), { mode: 0o700 });
      fs.symlinkSync(sentinel, path.join(dir, "router/decisions.jsonl"));
    }
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(fs.readFileSync(sentinel, "utf8")).toBe("untouched");
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
    expect(fs.readdirSync(victim)).toEqual(["decisions.jsonl"]);
  });
  it("refuses a non-regular ledger without failing selection", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router/decisions.jsonl"), { recursive: true, mode: 0o700 });
    fs.chmodSync(path.join(dir, "router"), 0o700);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual({ model: pick.model, thinking: pick.thinking });
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
    expect(fs.lstatSync(path.join(dir, "router/decisions.jsonl")).isDirectory()).toBe(true);
  });
  it("rotates at 8 MiB, serializes concurrent appends, and keeps one archive", async () => {
    const dir = root(); const router = path.join(dir, "router"); fs.mkdirSync(router, { mode: 0o700 });
    const file = path.join(router, "decisions.jsonl"); const limit = 8 * 1024 * 1024;
    const seed = "x".repeat(limit - 16);
    fs.writeFileSync(file, seed, { mode: 0o600 }); fs.writeFileSync(file + ".1", "obsolete archive");
    await Promise.all(Array.from({ length: 5 }, (_, n) => routeAgentCreation({ ...options(dir), task: `task-${n}`, explicit: true })));
    expect(decisions(dir)).toHaveLength(5);
    expect(fs.statSync(file).size).toBeLessThanOrEqual(limit);
    expect(fs.readFileSync(file + ".1", "utf8") === seed).toBe(true);
    fs.writeFileSync(file, "y".repeat(limit));
    await routeAgentCreation({ ...options(dir), explicit: true });
    expect(decisions(dir)).toHaveLength(1); expect(fs.statSync(file + ".1").size).toBe(limit);
    expect(fs.readFileSync(file + ".1", "utf8").startsWith("y")).toBe(true);
    expect(fs.readdirSync(router).sort()).toEqual(["decisions.jsonl", "decisions.jsonl.1"]);
  });
  it("concurrent spawns each append one complete JSONL record", async () => {
    const dir = root();
    await Promise.all(Array.from({ length: 5 }, (_, n) => routeAgentCreation({ ...options(dir), task: `task-${n}` })));
    expect(decisions(dir)).toHaveLength(5);
    expect(new Set(decisions(dir).map(d => d.requestDigest)).size).toBe(5);
  });
  it("owner-only actor instruction files expose only the supplied digest and unknown length", async () => {
    const dir = root(); const { task: _task, ...opts } = options(dir); const taskDigest = "a".repeat(64);
    await routeAgentCreation({ ...opts, kind: "actor", taskDigest, config: { ...opts.config, includeTask: true } });
    expect(request(dir)).toMatchObject({ kind: "actor", taskDigest, taskLength: null });
    expect(request(dir)).not.toHaveProperty("task");
  });
});
