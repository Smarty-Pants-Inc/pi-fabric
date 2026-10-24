import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// pi-fabric#1554 P2-3, smarty-dev#15878 "Done when": a process child of a native-pi Main runs `bash -c true` under the
// guard. This uses the REAL installed fleet bash guard (setup/pi/bash-guard.ts, with its own 60-byte rule) on real
// paths: no shifted byte budget. The child is a real AgentManager process run with the default short-TMPDIR source. It
// loads the guard as Pi loads an extension, binds a session, asks the guard about the bash tool call `true`, then
// runs the admitted command as Pi's bash tool does, `bash -c true`, in the environment the guard exported.
// It skips, saying why, when the guard is not installed (CI, non-fleet hosts) or when this run's private test TMPDIR is
// too long to stand in for a native-pi Main's `/tmp/p.XXXXXX` (a role or task layout, for example). To run it there,
// give vitest a native Main's TMPDIR: `TMPDIR=/tmp/p.XXXXXX bunx vitest run tests/short-tmpdir-real-guard.test.ts`.
const GUARD = path.join(os.userInfo().homedir, ".local/share/smarty-dev/factory/current/setup/pi/bash-guard.ts");
const GUARD_BYTES = 60;
const guardRootBytes = (parent: string): number => Buffer.byteLength(`${parent}/p.XXXXXX`, "utf8");

// The probe: what a Pi child with the fleet guard does for one bash tool call.
const probeSource = `import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
export const probe = async (guardPath) => {
  const inherited = process.env.TMPDIR;
  const guard = await import(pathToFileURL(guardPath).href);
  const handlers = new Map();
  const pi = new Proxy({ on: (name, handler) => { handlers.set(name, handler); }, appendEntry: () => {} },
    { get: (target, key) => (key in target ? target[key] : () => {}) });
  guard.default(pi);
  const ctx = { mode: 'print', cwd: process.cwd(), sessionManager: { getSessionId: () => 'pi-fabric-1554-real-guard' } };
  await handlers.get('session_start')?.({ type: 'session_start' }, ctx);
  const call = { toolName: 'bash', toolCallId: 'probe', input: { command: 'true' } };
  const verdict = (await handlers.get('tool_call')?.(call, ctx)) ?? null;
  const bash = verdict?.block ? null : spawnSync('bash', ['-c', call.input.command], { env: process.env, encoding: 'utf8' });
  return { inherited, exported: process.env.TMPDIR ?? null, verdict,
    bash: bash ? { status: bash.status, error: bash.error ? String(bash.error) : null } : null };
};
`;
const workerSource = `import fs from 'node:fs';
import { probe } from './probe.mjs';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
// Mirror worker.ts exporting --tmpdir to its execution child's environment before Pi loads its extensions.
if (args.has('--tmpdir')) for (const key of ['TMPDIR', 'TMP', 'TEMP']) process.env[key] = args.get('--tmpdir');
const report = await probe(${JSON.stringify(GUARD)});
const now = Date.now();
fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id: args.get('--id'), name: args.get('--name'), task: 'probe', status: 'completed',
  runner: args.get('--runner'), transport: 'process', sessionId: String(process.pid),
  cwd: args.get('--cwd'), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify({ ...report, argTmpdir: args.get('--tmpdir') ?? null }),
  exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }
}));`;
// The same probe, run directly in an inherited TMPDIR (the old nested layout, for contrast).
const directSource = `import { probe } from './probe.mjs';
console.log(JSON.stringify(await probe(${JSON.stringify(GUARD)})));`;

type Report = { inherited: string; exported: string | null; verdict: { block?: boolean; reason?: string } | null;
  bash: { status: number | null; error: string | null } | null; argTmpdir?: string | null };

const managers: AgentManager[] = [];
const cleanups: string[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const directory of cleanups.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("a child run's short TMPDIR under the real fleet bash guard (smarty-dev#15878)", () => {
  it("runs bash -c true under the real guard from a process child of a native-pi Main", async ({ skip }) => {
    if (!fs.existsSync(GUARD)) return skip(`real fleet bash guard not installed at ${GUARD}`);
    if (spawnSync("bash", ["-c", "true"]).status !== 0) return skip("no bash on this host");
    // This run's private TMPDIR plays the native-pi Main's TMPDIR; its child's short dir is <TMPDIR>/r.<10 hex>.
    const main = fs.realpathSync(os.tmpdir());
    const childBytes = guardRootBytes(path.join(main, "r.0123456789"));
    if (childBytes > GUARD_BYTES) {
      return skip(`this run's private TMPDIR ${main} leaves a child's guard root ${childBytes} bytes, over the real guard's ` +
        `${GUARD_BYTES}; rerun with a native-pi Main's TMPDIR such as /tmp/p.XXXXXX`);
    }
    const fixtures = fs.mkdtempSync(path.join(main, "fabric-real-guard-"));
    cleanups.push(fixtures);
    const cwd = path.join(fixtures, "cwd");
    fs.mkdirSync(cwd);
    fs.writeFileSync(path.join(fixtures, "probe.mjs"), probeSource);
    fs.writeFileSync(path.join(fixtures, "worker.mjs"), workerSource);
    fs.writeFileSync(path.join(fixtures, "direct.mjs"), directSource);
    // The native layout: the run root is <Main TMPDIR>/pi-fabric-runs-XXXXXX, where a managed root goes.
    const runRoot = fs.mkdtempSync(path.join(main, "pi-fabric-runs-"));
    cleanups.push(runRoot);
    // The default short-TMPDIR source (Fabric's data root, this TMPDIR) and the real guard's budget: no overrides.
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 15_000 },
      { workerPath: path.join(fixtures, "worker.mjs"), runRoot, fullCodeMode: false });
    managers.push(manager);
    const result = await manager.run({ task: "probe", transport: "process" });
    expect(result.status, result.error).toBe("completed");
    const report = JSON.parse(result.text) as Report;
    const run = manager.runDirectory(result.id)!;
    // The real guard admitted the child's bash call and `bash -c true` ran in the environment it exported...
    expect(report.verdict, report.verdict?.reason).toBeNull();
    expect(report.bash).toEqual({ status: 0, error: null });
    // ...under this session's private p.XXXXXX, which the guard bound inside the child's short dir: that dir sits
    // directly under the Main's TMPDIR, outside the run tree.
    expect(report.argTmpdir).toBe(result.temporaryDirectory);
    expect(report.inherited).toBe(result.temporaryDirectory);
    expect(path.dirname(report.inherited)).toBe(main);
    expect(path.dirname(report.exported!)).toBe(report.inherited);
    expect(path.basename(report.exported!)).toMatch(/^p\.[A-Za-z0-9]{6}$/);
    expect(Buffer.byteLength(report.exported!, "utf8")).toBeLessThanOrEqual(GUARD_BYTES);
    // Contrast: the same real guard refuses the old nested child TMPDIR, <run dir>/tmp, under this same Main.
    const nested = path.join(run, "tmp");
    fs.mkdirSync(nested, { mode: 0o700 });
    const direct = spawnSync(process.execPath, [path.join(fixtures, "direct.mjs")], { encoding: "utf8", cwd,
      env: { ...process.env, TMPDIR: nested, TMP: nested, TEMP: nested } });
    expect(direct.status, direct.stderr).toBe(0);
    const old = JSON.parse(direct.stdout.trim().split("\n").at(-1)!) as Report;
    expect(old.verdict?.block).toBe(true);
    expect(old.verdict?.reason).toContain(`needs ${guardRootBytes(nested)} bytes for p.XXXXXX (launcher limit 60 bytes)`);
    expect(old.bash).toBeNull();
    fs.rmdirSync(nested);
    console.info("REAL-GUARD:", JSON.stringify({ guard: GUARD, mainTmpdir: main, childTmpdir: report.inherited,
      guardRoot: report.exported, guardRootBytes: Buffer.byteLength(report.exported!, "utf8"), bash: report.bash,
      oldNestedTmpdir: nested, oldVerdict: old.verdict }));
    // The short dir leaves with its run.
    await manager.cleanup(result.id);
    expect(fs.existsSync(report.inherited)).toBe(false);
  });
});
