#!/usr/bin/env bun
/** Native installed-Pi RPC proof, on one freshly created PRIVATE scratch mesh.
 * Run after the lane's final build/source freeze:
 *   bun scripts/probe-session-actor-orphans.ts /absolute/installed/pi/dist/cli.js
 * The scratch extension supplies ONLY deterministic local provider responses and
 * native session witnesses. Final dist/index.js owns all actor/directory/presence
 * wiring. No network inference, manufactured owner, clock shift, or shortened grace.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync, type ChildProcess } from "node:child_process";
import { RpcClient } from "@earendil-works/pi-coding-agent";

const repo = path.resolve(import.meta.dirname, "..");
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const readJson = (file: string): any => JSON.parse(fs.readFileSync(file, "utf8"));
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const writeJson = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value, null, 2));
const absent = (pid: number): boolean => {
  try { process.kill(pid, 0); return false; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return true; throw error; }
};
const witness = (directory: string, extensions: RegExp) => {
  const files: string[] = [];
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (entry.isFile() && extensions.test(file)) files.push(file);
    }
  };
  walk(directory);
  const pins = files.sort().map(file => ({ path: path.relative(repo, file),
    sha256: createHash("sha256").update(fs.readFileSync(file)).digest("hex") }));
  return { files: pins.length, sha256: hash(JSON.stringify(pins)), pins };
};
const sourceWitness = () => witness(path.join(repo, "src"), /\.ts$/);
const bundleWitness = () => witness(path.join(repo, "dist"), /\.(?:js|mjs)$/);
const focus = new Set(["src/actors/directory.ts", "src/actors/manager.ts", "src/actors/session-orphans.ts",
  "src/actors/types.ts", "src/fabric-runtime-state.ts", "src/providers/agents-provider.ts", "src/residency/host.ts",
  "src/runtime/guest-types.ts", "src/topology/stall-alarms.ts"]);
const compactSource = (value: ReturnType<typeof sourceWitness>) => ({ files: value.files, sha256: value.sha256,
  pins: value.pins.filter(pin => focus.has(pin.path)) });

// Pattern from tests/fixtures/stall-alarms-pi.ts. No Fabric internals imported,
// no synthetic presence, no maintenance hook, no prototype/clock override.
const fixtureSource = `import fs from "node:fs";
import path from "node:path";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
export default function (pi: ExtensionAPI) {
  const dir = process.env.PROOF_GATE_DIR!;
  const faux = fauxProvider({ provider: "private-session-orphan-proof", api: "private-session-orphan-proof" });
  pi.registerProvider(faux.provider);
  const responses = (messages: ReturnType<typeof fauxAssistantMessage>[]) => faux.setResponses(messages.map(message => () => {
    fs.appendFileSync(path.join(dir, "local-provider-calls.jsonl"), JSON.stringify({ at: Date.now(), callCount: faux.state.callCount,
      provider: "private-session-orphan-proof", deterministic: true, network: false, cost: 0 }) + "\\n");
    return message;
  }));
  responses([fauxAssistantMessage("deterministic local completion")]);
  pi.on("session_start", (event, ctx) => {
    fs.writeFileSync(path.join(dir, "ready.json"), JSON.stringify({ id: "session:" + ctx.sessionManager.getSessionId(),
      sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), reason: event.reason,
      pid: process.pid, ppid: process.ppid, mode: ctx.mode, cwd: ctx.cwd, observedAt: Date.now() }));
  });
  pi.on("input", event => {
    if (event.text.startsWith("PROOF_EXEC ")) {
      const packet = JSON.parse(event.text.slice(11));
      responses([fauxAssistantMessage(fauxToolCall("fabric_exec", packet), { stopReason: "toolUse" }),
        fauxAssistantMessage("deterministic public tool completed")]);
    } else responses([fauxAssistantMessage("deterministic passive completion")]);
    return { action: "continue" as const };
  });
}
`;

type Native = { label: "A" | "B"; client: RpcClient; child: ChildProcess; pid: number;
  done: Promise<{ code: number | null; signal: NodeJS.Signals | null }>; receipt?: { code: number | null; signal: NodeJS.Signals | null };
  gate: string; events: any[]; ready?: any; launchArgs: string[]; env: Record<string, string> };

async function proof(cliArgument: string): Promise<void> {
  const cli = fs.realpathSync(cliArgument);
  assert(path.isAbsolute(cli), "installed Pi CLI must be absolute");
  assert(fs.statSync(cli).isFile());
  const extension = fs.realpathSync(path.join(repo, "dist/index.js"));
  assert(process.env.TMPDIR && path.isAbsolute(process.env.TMPDIR), "require lane-owned absolute TMPDIR");
  const tmp = fs.realpathSync(process.env.TMPDIR);
  const root = execFileSync("mktemp", ["-d", path.join(tmp, "proof-session-orphans-XXXXXX")], { encoding: "utf8" }).trim();
  assert.equal(path.dirname(root), tmp);
  const mesh = path.join(root, "mesh"), cwd = path.join(root, "project"), home = path.join(root, "home"), temp = path.join(root, "tmp");
  for (const dir of [mesh, cwd, home, temp]) fs.mkdirSync(dir, { mode: 0o700 });
  const fixture = path.join(root, "deterministic-provider.ts");
  fs.writeFileSync(fixture, fixtureSource);
  const evidence = path.join(repo, ".local/proof-session-orphans.txt");
  fs.mkdirSync(path.dirname(evidence), { recursive: true });
  const head = execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim();
  const source = sourceWitness(), bundle = bundleWitness();
  const natives: Native[] = [], notes: any[] = [], publicTools: any[] = [];
  let failure: unknown, result: any;
  const frozen = () => {
    assert.equal(sourceWitness().sha256, source.sha256, "all source bytes must remain frozen");
    assert.equal(bundleWitness().sha256, bundle.sha256, "all loaded lane bundle bytes must remain frozen");
  };
  const ownerPath = (sessionId: string) => path.join(mesh, "main-followups", `${encodeURIComponent(sessionId)}.owner.json`);
  const leasePath = (id: string) => path.join(mesh, "host-leases", `${hash(id).slice(0, 32)}.json`);
  const participantPath = (id: string) => path.join(mesh, "participants", `${hash(id)}.json`);
  const waitFor = async (check: () => boolean, deadline: number, diagnostic: () => string) => {
    while (!check()) { assert(Date.now() < deadline, diagnostic()); await pause(50); }
  };
  const start = async (label: "A" | "B"): Promise<Native> => {
    frozen();
    const gate = path.join(root, label), profile = path.join(gate, "profile");
    fs.mkdirSync(profile, { recursive: true, mode: 0o700 });
    writeJson(path.join(profile, "settings.json"), { compaction: { enabled: false }, retry: { enabled: false },
      cacheWarming: "off", enableInstallTelemetry: false, enableAnalytics: false });
    writeJson(path.join(profile, "fabric.json"), {
      executor: { kernel: "typescript" }, fullCodeMode: false,
      mesh: { enabled: true, announce: true, actorScope: "project" },
      agents: { budgetUsd: 0, nice: 19 },
      mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false }, records: { enabled: false },
      compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
    });
    const env: Record<string, string> = { PATH: process.env.PATH!, HOME: home, TMPDIR: temp,
      PI_CODING_AGENT_DIR: profile, PI_FABRIC_AGENT_DIR: path.join(gate, "exports"),
      PI_FABRIC_MESH_ROOT: mesh, PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(gate, "runs"),
      PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RELEASE_SHA: head,
      PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0", PROOF_GATE_DIR: gate };
    const launchArgs = ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
      "--approve", "--no-builtin-tools", "--thinking", "off", "--name", `PRIVATE ORPHAN PROOF ${label}`,
      "--session-dir", path.join(gate, "sessions"), "-e", extension, "-e", fixture];
    const client = new RpcClient({ cliPath: cli, cwd, provider: "private-session-orphan-proof", model: "faux-1", env, args: launchArgs });
    const events: any[] = [];
    client.onEvent(event => events.push(event));
    // RpcClient merges process.env. Temporarily admit ONLY our explicit private
    // allowlist at the real spawn boundary, then restore the orchestrator env.
    const inherited = process.env;
    try { process.env = { ...env }; await client.start(); }
    finally { process.env = inherited; }
    // Read-only receipt observation of RpcClient's actual ChildProcess handle.
    // Actor operations use public native fabric_exec; shutdown uses the documented
    // RPC stdin-close protocol and exact owned process signals, never a fake owner.
    const child = (client as unknown as { process: ChildProcess }).process;
    assert(child?.pid, "RpcClient must own a genuine native child");
    const native: Native = { label, client, child, pid: child.pid, gate, events, launchArgs, env,
      done: Promise.resolve({ code: null, signal: null }) };
    native.done = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => { native.receipt = { code, signal }; resolve(native.receipt); });
    });
    void native.done.catch(() => undefined);
    natives.push(native);
    const readyFile = path.join(gate, "ready.json");
    await waitFor(() => fs.existsSync(readyFile), Date.now() + 60_000,
      () => `${label} native session_start did not finish: ${client.getStderr()}`);
    native.ready = readJson(readyFile);
    assert.equal(native.ready.mode, "rpc");
    assert.equal(native.ready.pid, native.pid, "native witness must be the exact SDK-owned Pi subprocess");
    assert.equal(native.ready.ppid, process.pid);
    assert.equal(native.ready.cwd, cwd);
    await waitFor(() => fs.existsSync(ownerPath(native.ready.sessionId)) && fs.existsSync(leasePath(native.ready.id)) &&
      fs.existsSync(participantPath(native.ready.id)), Date.now() + 30_000,
      () => `${label} native Fabric root/lease/inbox not published: ${client.getStderr()}`);
    const state = await client.getState();
    assert.equal(state.sessionId, native.ready.sessionId);
    const owner = readJson(ownerPath(native.ready.sessionId)), lease = readJson(leasePath(native.ready.id));
    const presence = readJson(participantPath(native.ready.id));
    assert.equal(owner.pid, native.pid);
    assert.equal(lease.writer.pid, native.pid);
    assert.equal(presence.value.id, native.ready.id);
    assert.equal(presence.value.kind, "root");
    assert(lease.expiresAt > Date.now());
    if (process.platform === "linux") {
      const stat = fs.readFileSync(`/proc/${native.pid}/stat`, "utf8");
      const startTicks = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      assert.equal(owner.processStartedAt, startTicks);
      notes.push({ nativeProcessAttestation: { label, pid: native.pid, startTicks,
        exe: fs.readlinkSync(`/proc/${native.pid}/exe`),
        argv: fs.readFileSync(`/proc/${native.pid}/cmdline`, "utf8").split("\0").filter(Boolean) } });
    }
    frozen();
    notes.push({ label, ready: native.ready, initialState: state, inboxOwner: owner, rootPresence: presence, hostLease: lease,
      launch: { command: ["node", cli, "--mode", "rpc", "--provider", "private-session-orphan-proof", "--model", "faux-1", ...launchArgs],
        cwd, env }, productionMaintenanceWiring: true });
    return native;
  };
  const exec = async (native: Native, phase: string, code: string, payloads: Record<string, string> = {}) => {
    frozen();
    const events = await native.client.promptAndWait(`PROOF_EXEC ${JSON.stringify({ code, payloads })}`, undefined, 60_000);
    const ended = events.filter((event: any) => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
    assert.equal(ended.length, 1, `${phase}: exactly one native fabric_exec required`);
    const tool = ended[0] as any;
    publicTools.push({ native: native.label, phase, observedAt: Date.now(), code, payloads, tool });
    assert.equal(tool.isError, false, `${phase}: ${JSON.stringify(tool.result)}`);
    const audits: any[] = tool.result.details?.audits;
    assert(Array.isArray(audits) && audits.length > 0, `${phase}: native provider audit results must be exposed`);
    assert(events.some((event: any) => event.type === "agent_settled" && event.outcome === "completed"));
    frozen();
    return audits.map(audit => audit.result);
  };
  const createCode = "return await agents.create(JSON.parse(π.request));";
  const request = { name: "private-session-orphan", instructions: "Remain idle; no actor inference permitted.",
    scope: "session", residency: "session", events: [], topics: [], runner: "pi", extensions: false };
  const killOwned = () => { for (const native of natives) if (!native.receipt) native.child.kill("SIGKILL"); };
  const abort = () => { failure = new Error("Proof interrupted"); killOwned(); };
  process.on("SIGTERM", abort); process.on("SIGINT", abort);
  const finite = setTimeout(() => { failure = new Error("480-second command deadline"); killOwned(); }, 480_000);
  try {
    const A = await start("A");
    const [actor] = await exec(A, "A-public-create", createCode, { request: JSON.stringify(request) });
    assert.equal(actor.status, "idle", "A's real starting actor status must be idle, never synthesized running");
    assert.equal(actor.scope, "session");
    assert.equal(actor.residency, "session");
    assert.equal(actor.rootId, A.ready.id);
    assert.equal(actor.queued, 0);
    assert.equal(actor.messages, 0);
    const registry = path.join(mesh, "actors", A.ready.sessionId, "actors.json");
    assert.equal(readJson(registry).actors.find((row: any) => row.id === actor.id)?.status, "idle");
    const leaseBefore = readJson(leasePath(A.ready.id));
    await pause(6_000);
    const leaseAtDeath = readJson(leasePath(A.ready.id));
    assert(leaseAtDeath.updatedAt > leaseBefore.updatedAt, "native A production heartbeat must advance automatically");
    if (process.platform === "linux") {
      const children = fs.readFileSync(`/proc/${A.pid}/task/${A.pid}/children`, "utf8").trim();
      assert.equal(children, "", "native A must have no unaccounted descendants before SIGKILL");
    }
    const killedAt = Date.now();
    assert(leaseAtDeath.expiresAt > killedAt, "SIGKILL must end a genuinely fresh native Main root");
    assert(A.child.kill("SIGKILL"));
    const killedReceipt = await A.done;
    assert.equal(killedReceipt.signal, "SIGKILL");
    assert(absent(A.pid), "actual native A PID must be gone (ESRCH)");
    const expiry = Math.max(leaseAtDeath.expiresAt, leaseAtDeath.reloadUntil ?? 0, leaseAtDeath.session?.expiresAt ?? 0);
    notes.push({ SIGKILL: { pid: A.pid, rootId: A.ready.id, killedAt, killedReceipt, ESRCH: true, leaseAtDeath,
      effectiveLeaseExpiresAt: expiry, noDirectDescendants: true } });
    const B = await start("B");
    assert.notEqual(B.ready.id, A.ready.id);
    await pause(25_000);
    const beforeGrace = readJson(registry).actors.find((row: any) => row.id === actor.id);
    assert.equal(beforeGrace.status, "idle", "native maintenance must preserve the actor before real grace");
    notes.push({ beforeGrace: { observedAt: Date.now(), registry: beforeGrace, BRoot: readJson(participantPath(B.ready.id)) } });
    await pause(Math.max(0, expiry + 121_000 - Date.now()));
    // Read-only external registry observation. NO B fabric_exec/provider read is
    // sent during grace: the compiled production runtime must retire it itself.
    let row: any;
    while ((row = readJson(registry).actors.find((candidate: any) => candidate.id === actor.id))?.status !== "stopped") {
      assert(Date.now() < killedAt + 360_000, "automatic native Fabric maintenance did not retire the orphan within 360 seconds");
      assert(!B.receipt, `native B exited: ${B.client.getStderr()}`);
      await pause(1_000);
    }
    const stoppedBeforeFirstBProviderReadAt = Date.now();
    assert(row.sessionOrphan.orphanedAt - expiry >= 120_000);
    assert.equal(row.rootId, A.ready.id, "truth repair must not adopt the old actor into B");
    assert.equal(row.sessionOrphan.oldRoot, A.ready.id);
    assert.equal(row.sessionOrphan.oldHost, leaseAtDeath.writer.host);
    assert.equal(row.sessionOrphan.lastUpdated, actor.updatedAt);
    assert.match(row.lastError, /^root-gone:/);
    const queryCode = "const id = π.id; const status = await agents.actorStatus({ id }); const actors = await agents.actors({}); " +
      "const tasks = await agents.list({ scope: 'project' }); return { status, actors, tasks };";
    const [status, actors, tasks] = await exec(B, "B-public-orphan-reads", queryCode, { id: actor.id });
    assert.equal(status.status, "stopped", "native public actorStatus must not project unknown");
    assert.match(status.lastError, /^root-gone:/);
    assert.equal(status.rootId, A.ready.id);
    assert((actors as any[]).some(candidate => candidate.id === actor.id && candidate.status === "stopped"));
    assert.deepEqual(tasks, [], "no actor inference/task worker may have launched");
    const [replacement] = await exec(B, "B-public-same-name-create", createCode, { request: JSON.stringify(request) });
    assert.equal(replacement.name, actor.name);
    assert.notEqual(replacement.id, actor.id);
    assert.equal(replacement.rootId, B.ready.id);
    assert.equal(replacement.scope, "session");
    assert.equal(replacement.status, "idle");
    await pause(6_000);
    const [stableStatus, stableActors, stableTasks] = await exec(B, "B-repeated-public-orphan-reads", queryCode, { id: actor.id });
    assert.equal(stableStatus.status, "stopped");
    assert((stableActors as any[]).some(candidate => candidate.id === replacement.id && candidate.status === "idle"));
    assert.deepEqual(stableTasks, []);
    row = readJson(registry).actors.find((candidate: any) => candidate.id === actor.id);
    const allEvents = fs.readFileSync(path.join(mesh, "events.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
    const alarms = allEvents.filter(event => event.topic === "ops.owner" && event.kind === "actor.session.orphaned" && event.data?.actorId === actor.id);
    assert.equal(alarms.length, 1, "exactly one native root-gone alarm after repeated public reads");
    assert.equal(alarms[0].data.oldRoot, A.ready.id);
    assert.match(alarms[0].data.line, /re-run activation\.py/);
    assert(row.sessionOrphan.alarmPublishedAt, "durable alarm must be acknowledged");
    const statistics = await B.client.getSessionStats();
    assert.equal(statistics.cost, 0, "only deterministic zero-cost local provider responses are allowed");
    if (process.platform === "linux") {
      assert.equal(fs.readFileSync(`/proc/${B.pid}/task/${B.pid}/children`, "utf8").trim(), "",
        "native B must have no unaccounted descendants at handback");
    }
    const nativeEntries = await B.client.getEntries();
    assert(nativeEntries.entries.some((entry: any) => entry.type === "message" && entry.message?.role === "toolResult" && entry.message.toolName === "fabric_exec"));
    frozen();
    result = { passed: true, actor, replacement, oldRoot: A.ready.id, newRoot: B.ready.id, registry: row, alarms,
      killedAt, effectiveLeaseExpiresAt: expiry, realGraceAfterExpiryMs: row.sessionOrphan.orphanedAt - expiry,
      stoppedBeforeFirstBProviderReadAt, retiredByProductionNativeRuntimeBeforeProviderRead: true,
      status, actors, tasks, stableStatus, stableActors, stableTasks, statistics,
      BNativeSessionHeader: JSON.parse(fs.readFileSync(B.ready.sessionFile, "utf8").split("\n")[0]!),
      BNativeToolEntryCount: nativeEntries.entries.filter((entry: any) => entry.message?.toolName === "fabric_exec").length };
  } catch (error) { failure ??= error; }
  finally {
    clearTimeout(finite);
    // Account for exact SDK-owned Pi children. Public stop alone is not an exit
    // receipt: stdin close and the actual close event plus ESRCH establish that.
    for (const native of natives) {
      if (!native.receipt) native.child.stdin?.end();
      await Promise.race([native.done, pause(15_000)]);
      if (!native.receipt) { native.child.kill("SIGTERM"); await Promise.race([native.done, pause(10_000)]); }
      if (!native.receipt) { native.child.kill("SIGKILL"); await native.done; }
      if (!absent(native.pid)) failure ??= new Error(`native ${native.label} PID ${native.pid} still alive`);
    }
    notes.push({ nativeChildrenExited: natives.map(native => ({ label: native.label, pid: native.pid, ...native.receipt, ESRCH: absent(native.pid) })) });
    const localCalls = natives.map(native => ({ label: native.label, calls: fs.existsSync(path.join(native.gate, "local-provider-calls.jsonl"))
      ? fs.readFileSync(path.join(native.gate, "local-provider-calls.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [] }));
    const report = { passed: !failure && result?.passed === true,
      command: `bun scripts/probe-session-actor-orphans.ts ${cli}`, recordedAt: new Date().toISOString(), repo, head,
      privateScratch: root, mesh, installedCli: cli, laneExtension: extension,
      source: compactSource(source), bundle, fixtureSha256: hash(fixtureSource),
      nativePiRPC: true, productionMaintenanceWiring: true, inheritedEnvironmentStrippedAtSpawn: true,
      noClockOverride: true, noNetworkInference: true, noManualPresencePass: true,
      limitations: ["Non-UI installed Pi RPC sessions; no TUI/video evidence.",
        "A actor starts actually idle. No running/in-flight actor or worker is synthesized.",
        "Deterministic local faux provider emits owned public fabric_exec calls; no paid/network inference.",
        "Same-name B public creation checks admission, not an actor inference activation."],
      notes, result, publicTools, localCalls,
      nativeLogs: natives.map(native => ({ label: native.label, stderr: native.client.getStderr(),
        extensionErrors: native.events.filter(event => event.type === "extension_error") })),
      ...(failure ? { error: String(failure), stack: (failure as Error).stack } : {}) };
    // Preserve public native tool results, real PID receipts and bundle pins
    // before removing ONLY this invocation's exact mktemp private directory.
    fs.writeFileSync(evidence, JSON.stringify(report, null, 2) + "\n");
    if (natives.every(native => absent(native.pid))) fs.rmSync(root, { recursive: true, force: true });
    process.removeListener("SIGTERM", abort); process.removeListener("SIGINT", abort);
    console.log(JSON.stringify({ passed: report.passed, evidence, ownScratchRemoved: !fs.existsSync(root),
      nativeChildren: natives.map(native => ({ label: native.label, pid: native.pid, ...native.receipt, ESRCH: absent(native.pid) })) }));
  }
  if (failure) throw failure;
}

assert.equal(process.argv.length, 3, "Usage: bun scripts/probe-session-actor-orphans.ts /absolute/installed/pi/dist/cli.js");
await proof(process.argv[2]!);
