import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import fabricWriteGuard, { bootstrapScratchRoot, writePolicyDenial } from "../src/agents/write-guard.js";

// smarty-dev#15856: a write-confined Pi child launched through the fleet's role-pi shim gets a NEW TMPDIR (a fresh
// 0700 session store, then the fleet bash guard's 0700 p.XXXXXX child of it). Fabric's writable root names the
// TMPDIR it set, so the write tool refused files under Bash's `mktemp -d`. The guard now also covers exactly that
// bootstrap p.XXXXXX directory, and nothing else outside the policy roots.

const cleanups: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const directory of cleanups.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

type Hook = (event: { toolName: string; input: unknown }, context: { cwd: string }) => { block: boolean; reason: string } | undefined;
const guardHook = (policy: object): Hook => {
  vi.stubEnv("PI_FABRIC_WRITE_POLICY", JSON.stringify(policy));
  let hook: Hook | undefined;
  fabricWriteGuard({ on: (_name: string, handler: Hook) => { hook = handler; } } as unknown as ExtensionAPI);
  if (!hook) throw new Error("write guard did not register");
  return hook;
};

/** The launcher's store and the bash guard's child, laid out and recorded as smarty-role and bash-guard.ts do. */
const bootstrap = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "wg-bootstrap-")));
  cleanups.push(base);
  const fabricScratch = path.join(base, "r.0123456789");
  const store = path.join(base, "pi-0123456789ab");
  const outside = path.join(base, "outside");
  for (const directory of [fabricScratch, store, outside]) fs.mkdirSync(directory, { mode: 0o700 });
  fs.writeFileSync(path.join(store, ".smarty-pi-lease"), "");
  const stat = fs.statSync(store, { bigint: true });
  const guardRoot = fs.mkdtempSync(path.join(store, "p."));
  vi.stubEnv("SMARTY_SESSION_STORE", store);
  vi.stubEnv("SMARTY_SESSION_STORE_ID", `${stat.dev}:${stat.ino}`);
  vi.stubEnv("TMPDIR", guardRoot);
  // The role's scratch: plain `mktemp -d` in the session's TMPDIR, exactly as roles/review-agent.md asks.
  const made = spawnSync("bash", ["-c", "mktemp -d"], { env: { ...process.env, TMPDIR: guardRoot }, encoding: "utf8" });
  expect(made.status, made.stderr).toBe(0);
  const scratch = made.stdout.trim();
  expect(path.dirname(scratch)).toBe(guardRoot);
  return { base, fabricScratch, store, guardRoot, outside, scratch,
    actorPolicy: { readOnly: false, writableRoots: [fabricScratch], shell: "unconfined" as const } };
};

describe.skipIf(process.platform === "win32")("write guard covers the fleet bootstrap's TMPDIR (smarty-dev#15856)", () => {
  it("lets write and edit reach a file in mktemp -d under the bootstrap TMPDIR; outside stays refused", () => {
    const f = bootstrap();
    const hook = guardHook(f.actorPolicy);
    const claim = path.join(f.scratch, "claim.json");
    for (const toolName of ["write", "edit"]) {
      expect(hook({ toolName, input: { path: claim } }, { cwd: f.base })).toBeUndefined();
      expect(hook({ toolName, input: { path: path.join(f.fabricScratch, "claim.json") } }, { cwd: f.base })).toBeUndefined();
      for (const refused of [path.join(f.outside, "claim.json"), path.join(f.store, ".smarty-pi-lease"),
        path.join(f.store, "other", "claim.json"), path.join(f.scratch, "..", "..", "claim.json")]) {
        const verdict = hook({ toolName, input: { path: refused } }, { cwd: f.base });
        expect(verdict?.block, refused).toBe(true);
        expect(verdict?.reason).toContain(`allowed roots: ${JSON.stringify([f.fabricScratch, f.guardRoot])}`);
      }
    }
    // A link out of the scratch is refused by the existing real-path check.
    fs.symlinkSync(f.outside, path.join(f.scratch, "escape"));
    expect(hook({ toolName: "write", input: { path: path.join(f.scratch, "escape", "x") } }, { cwd: f.base })?.block).toBe(true);
  });

  it("adds no root for read-only or shell-deny policies, or without the launcher's exact store", () => {
    const f = bootstrap();
    const claim = path.join(f.scratch, "claim.json");
    expect(writePolicyDenial({ ...f.actorPolicy, shell: "deny" }, "write", { path: claim }, f.base)).toMatch(/outside writable roots/);
    expect(writePolicyDenial({ readOnly: true, writableRoots: [], shell: "unconfined" }, "write", { path: claim }, f.base)).toMatch(/read-only/);
    expect(bootstrapScratchRoot()).toBe(f.guardRoot);
    const refusedWith = (env: Record<string, string | undefined>) => {
      for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
      expect(bootstrapScratchRoot()).toBeUndefined();
      expect(writePolicyDenial(f.actorPolicy, "write", { path: claim }, f.base)).toMatch(/outside writable roots/);
      vi.stubEnv("SMARTY_SESSION_STORE", f.store);
      vi.stubEnv("SMARTY_SESSION_STORE_ID", `${fs.statSync(f.store, { bigint: true }).dev}:${fs.statSync(f.store, { bigint: true }).ino}`);
      vi.stubEnv("TMPDIR", f.guardRoot);
    };
    refusedWith({ SMARTY_SESSION_STORE: undefined });
    refusedWith({ SMARTY_SESSION_STORE_ID: "1:2" });
    refusedWith({ TMPDIR: f.store }); // the store itself (lease, caches) is never a root
    refusedWith({ TMPDIR: f.scratch }); // deeper than the guard's direct p.XXXXXX child
    refusedWith({ TMPDIR: `${f.guardRoot}/` });
    fs.chmodSync(f.guardRoot, 0o755);
    refusedWith({});
    fs.chmodSync(f.guardRoot, 0o700);
    const alias = path.join(f.base, "alias");
    fs.symlinkSync(f.store, alias);
    refusedWith({ SMARTY_SESSION_STORE: alias, TMPDIR: path.join(alias, path.basename(f.guardRoot)) });
  });
});

// The real path: a real AgentManager actor activation (its own short TMPDIR and scratch-only write policy) whose
// worker launches the child through the REAL installed role-pi shim (smarty-role --session-pi), as the fleet sets
// PI_FABRIC_PI_BINARY. The shim execs a probe in place of Pi; the probe loads the REAL fleet bash guard, binds a
// session, runs `mktemp -d` through bash in the environment the guard exported, and asks this checkout's write guard
// about a write tool call in it and one outside, writing the file only when allowed. Skips off the fleet.
const SHIM = path.join(os.userInfo().homedir, ".local/share/smarty-dev/factory/current/bin/role-pi/pi");
const BASH_GUARD = path.join(os.userInfo().homedir, ".local/share/smarty-dev/factory/current/setup/pi/bash-guard.ts");
const probeSource = (outside: string) => `#!${process.execPath}
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
const launched = { TMPDIR: process.env.TMPDIR, store: process.env.SMARTY_SESSION_STORE };
const handlers = new Map();
const pi = new Proxy({ on: (name, handler) => { handlers.set(name, [...(handlers.get(name) ?? []), handler]); }, appendEntry: () => {} },
  { get: (target, key) => (key in target ? target[key] : () => {}) });
(await import(pathToFileURL(${JSON.stringify(BASH_GUARD)}).href)).default(pi);
(await import(${JSON.stringify(pathToFileURL(path.resolve("src/agents/write-guard.ts")).href)})).default(pi);
const ctx = { mode: 'print', cwd: process.cwd(), sessionManager: { getSessionId: () => 'pi-fabric-15856-bootstrap' } };
for (const handler of handlers.get('session_start') ?? []) await handler({ type: 'session_start' }, ctx);
const call = async (toolName, input) => {
  for (const handler of handlers.get('tool_call') ?? []) {
    const verdict = await handler({ toolName, toolCallId: toolName, input }, ctx);
    if (verdict?.block) return verdict;
  }
  return null;
};
const bashVerdict = await call('bash', { command: 'mktemp -d' });
const made = bashVerdict ? null : spawnSync('bash', ['-c', 'mktemp -d'], { env: process.env, encoding: 'utf8' });
const scratch = made?.stdout.trim();
const results = {};
for (const [label, file] of [['inside', scratch && path.join(scratch, 'claim.json')], ['outside', ${JSON.stringify(outside)}]]) {
  const verdict = file ? await call('write', { path: file, content: '{}' }) : { block: true, reason: 'no scratch' };
  if (!verdict) fs.writeFileSync(file, '{}');
  results[label] = { file, verdict, written: !verdict && fs.readFileSync(file, 'utf8') === '{}' };
}
// Leave the session store as the guard allows: remove only our scratch, strictly inside the guard's root.
if (scratch && !(await call('bash', { command: 'rm -rf -- ' + scratch }))) spawnSync('rm', ['-rf', '--', scratch]);
console.log(JSON.stringify({ launched, exported: process.env.TMPDIR, bashVerdict, scratch, results }));
`;
const workerSource = (shim: string, probe: string) => `import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
// As worker.ts does: --tmpdir and --write-policy become the child's TMPDIR and PI_FABRIC_WRITE_POLICY.
const env = { PATH: process.env.PATH, HOME: process.env.HOME, USER: process.env.USER, SMARTY_ROLE_PI_BINARY: ${JSON.stringify(probe)},
  PI_FABRIC_WRITE_POLICY: args.get('--write-policy'), PI_FABRIC_ACTOR_ID: args.get('--actor-id') ?? '',
  TMPDIR: args.get('--tmpdir'), TMP: args.get('--tmpdir'), TEMP: args.get('--tmpdir') };
const child = spawnSync(${JSON.stringify(shim)}, ['-p', 'probe'], { cwd: args.get('--cwd'), env, encoding: 'utf8' });
const now = Date.now();
fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id: args.get('--id'), name: args.get('--name'), task: 'probe', status: 'completed', runner: args.get('--runner'),
  transport: 'process', sessionId: String(process.pid), cwd: args.get('--cwd'), startedAt: now, updatedAt: now,
  finishedAt: now, turns: 1, toolCalls: 0, exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  text: JSON.stringify({ status: child.status, stderr: child.stderr, stdout: child.stdout, policy: args.get('--write-policy'),
    tmpdir: args.get('--tmpdir') }) }));`;

describe.skipIf(process.platform === "win32")("a Pi actor launched through the real fleet bootstrap (smarty-dev#15856)", () => {
  it("writes a file in mktemp -d under its TMPDIR with the write tool; a write outside is refused", async ({ skip }) => {
    if (!fs.existsSync(SHIM) || !fs.existsSync(BASH_GUARD)) return skip(`real role-pi shim or bash guard not installed (${SHIM})`);
    const fixtures = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "wg-real-")));
    cleanups.push(fixtures);
    const cwd = path.join(fixtures, "cwd");
    const outside = path.join(fixtures, "outside", "claim.json");
    fs.mkdirSync(cwd);
    fs.mkdirSync(path.dirname(outside));
    const probe = path.join(fixtures, "probe.mjs");
    fs.writeFileSync(probe, probeSource(outside), { mode: 0o700 });
    fs.writeFileSync(path.join(fixtures, "worker.mjs"), workerSource(SHIM, probe));
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 30_000 },
      { workerPath: path.join(fixtures, "worker.mjs"), runRoot: path.join(fixtures, "runs"), fullCodeMode: false });
    managers.push(manager);
    const result = await manager.run({ task: "probe", transport: "process", runner: "pi", actorId: "wg-bootstrap-actor" });
    expect(result.status, result.error).toBe("completed");
    const launch = JSON.parse(result.text) as { status: number; stderr: string; stdout: string; policy: string; tmpdir: string };
    expect(launch.status, launch.stderr).toBe(0);
    const report = JSON.parse(launch.stdout.trim().split("\n").at(-1)!) as {
      launched: { TMPDIR: string; store: string }; exported: string; bashVerdict: unknown; scratch: string;
      results: Record<"inside" | "outside", { file: string; verdict: { block: boolean; reason: string } | null; written: boolean }> };
    // The cause: Fabric's actor scratch is the only writable root, and the shim replaced that TMPDIR with its store.
    expect(JSON.parse(launch.policy)).toEqual({ readOnly: false, writableRoots: [launch.tmpdir], shell: "unconfined" });
    expect(report.launched.TMPDIR).toBe(report.launched.store);
    expect(report.launched.TMPDIR).not.toBe(launch.tmpdir);
    expect(path.dirname(report.exported)).toBe(report.launched.store);
    expect(report.bashVerdict).toBeNull();
    expect(path.dirname(report.scratch)).toBe(report.exported);
    // The fix: the write tool reaches the file in mktemp -d; outside stays refused.
    expect(report.results.inside.verdict, report.results.inside.verdict?.reason).toBeNull();
    expect(report.results.inside.written).toBe(true);
    expect(report.results.outside.verdict?.block).toBe(true);
    expect(report.results.outside.verdict?.reason).toContain("write outside actor scratch refused");
    expect(fs.existsSync(outside)).toBe(false);
    console.info("FLEET-BOOTSTRAP:", JSON.stringify({ fabricTmpdir: launch.tmpdir, launcherStore: report.launched.store,
      guardRoot: report.exported, scratch: report.scratch, inside: report.results.inside, outside: report.results.outside }));
  });
});
