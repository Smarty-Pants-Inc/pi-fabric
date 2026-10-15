// Real-process defensive gate; run only in an authorized isolated TMPDIR.
// bun scripts/probe-starting-host-takeover.ts [--case live|death|handoff|birth|slow|unknown|unreadable]
// Retains all fixture roots. Never signals a process not spawned by this invocation.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID, createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { claimStartingHost, localStartTime, type StartingHostClaim } from "../src/topology/starting-host.js";
import * as startupApi from "../src/topology/starting-host.js";
import * as leases from "../src/topology/host-leases.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import { validProcessIncarnation } from "../src/core/atomic-write.js";

export const CASES = ["live", "death", "handoff", "birth", "slow", "unknown", "unreadable"] as const;
export type ProbeCase = typeof CASES[number];
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
type Wire = { phase?: string; request?: string; action?: string; error?: string; [key: string]: unknown };
const self = fileURLToPath(import.meta.url);
const emit = (value: Wire) => process.send?.(value);
const errorText = (error: unknown) => error instanceof Error ? `${error.name}: ${error.message}` : String(error);
// Normal progress is IPC, lease notifications, and process exit. These one-shot timers
// bound silence/negative assertions; there is no polling or mocked process liveness.
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const bounded = async <T>(work: Promise<T>, ms: number, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([work, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label}: exceeded ${ms}ms`)), ms);
  })]); } finally { clearTimeout(timer); }
};
const differentNativeBirth = (actual: string): string => {
  assert(validProcessIncarnation(actual), "fixture incumbent must have a native birth");
  let recorded: string;
  if (process.platform === "linux") recorded = (BigInt(actual) + 1n).toString();
  else if (process.platform === "win32") recorded = `win32:${BigInt(actual.slice(6)) + 1n}`;
  else {
    assert.equal(process.platform, "darwin", "unsupported process-identity platform");
    const old = new Date(Date.parse(`${actual.slice(7)} UTC`) - 1_000);
    assert(Number.isFinite(old.getTime()), "native Darwin birth must parse");
    const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const two = (value: number) => String(value).padStart(2, "0");
    recorded = `darwin:${days[old.getUTCDay()]} ${months[old.getUTCMonth()]} ${String(old.getUTCDate()).padStart(2, " ")} ` +
      `${two(old.getUTCHours())}:${two(old.getUTCMinutes())}:${two(old.getUTCSeconds())} ${old.getUTCFullYear()}`;
  }
  assert(validProcessIncarnation(recorded), "simulated reused-PID birth must be platform-native, not malformed");
  assert.notEqual(recorded, actual);
  return recorded;
};
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const meshAt = (root: string) => new MeshStore(root, 65_536, 1_000, { lockProtocol: 1, stateBackend: "file" });
const unavailable = (kind: string) => (id: string): never => { throw new Error(`Unknown Fabric ${kind}: ${id}`); };
const metadata = (meshRoot: string, id: string): FabricParticipantInfo | undefined => {
  const lease = leases.readHostLeaseCurrent(meshRoot, id);
  if (!lease || lease.startup) return undefined;
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: lease.ownerIncarnation, name: id, status: "idle", runner: "pi", transport: "host",
    controlProtocol: "v1", cwd: meshRoot, sessionId: id.slice(8), interactive: true,
    capabilities: ["followUp", "steer"], startedAt: lease.startedAt ?? lease.updatedAt,
    updatedAt: lease.updatedAt, local: false, stale: lease.expiresAt <= Date.now() };
};
const router = (meshRoot: string, who: MeshIdentity, main: Ports[2], control: FabricControlPlane) => new AgentMessageRouter(
  { status: unavailable("agent") } as unknown as Ports[0],
  { identity: who, owns: () => false, status: unavailable("actor") } as unknown as Ports[1], main,
  { get: (id: string) => metadata(meshRoot, id), lastKnown: () => undefined,
    scheduleRefresh: () => {}, writeStalled: () => undefined }, control, binding => binding,
);
const makePlane = (mesh: MeshStore, who: MeshIdentity, epoch?: string) => new FabricControlPlane(mesh, who,
  { enabled: true, hostId: who.id, canConsumeMesh: () => true, ...(epoch ? { ownerIncarnation: epoch } : {}) });

async function childMain(meshRoot: string, sessionId: string, slow: boolean, unreadablePid?: number) {
  // Read-refusal injection only, not mocked liveness: PID existence and recorded birth
  // remain real. This covers a trust-boundary failure that needs admin rights to create.
  if (unreadablePid) {
    const nativeRead = fs.readFileSync;
    fs.readFileSync = ((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === `/proc/${unreadablePid}/stat`) {
        emit({ phase: "birth-probe-refused", pid: unreadablePid });
        throw Object.assign(new Error("birth probe refused"), { code: "EPERM" });
      }
      return Reflect.apply(nativeRead, fs, [file, ...args]);
    }) as typeof fs.readFileSync;
  }
  // Process-owned finite deadline, including a stuck claim or lost parent IPC.
  setTimeout(() => process.exit(124), 75_000).unref();
  process.once("disconnect", () => process.exit(0));
  const id = `session:${sessionId}`, mesh = meshAt(meshRoot), who = identity(id);
  let claim: StartingHostClaim | undefined, plane: FabricControlPlane | undefined;
  let ready = false;
  const calls: Array<{ message: string; deliveryId?: string; data?: unknown }> = [];
  const markReady = async (expiryMs = 60_000) => {
    assert(claim, "claim required before readiness");
    const previous = claim.lease;
    const { startup: _startup, ...rest } = previous;
    const now = Date.now();
    const next = { ...rest, updatedAt: now, expiresAt: now + expiryMs,
      session: { ...previous.session!, updatedAt: now, expiresAt: now + expiryMs } };
    // Controlled fixture deadline/readiness write under exact-token custody. The
    // ordinary renewal API intentionally takes max(expiry), so cannot shorten a
    // fixture deadline. This does NOT bypass the successor's real claim/CAS fence.
    await leases.withOwnedHostLease(mesh, previous, () => leases.writeHostLease(meshRoot, next));
    claim.lease = next;
    await mesh.put({ identity: who, key: "topology/hosts/" + createHash("sha256").update(id).digest("hex"),
      value: { format: 1, id, rootId: id, identity: who, incarnationToken: next.incarnationToken,
        ownerIncarnation: next.ownerIncarnation, startedAt: next.startedAt, updatedAt: now, expiresAt: next.expiresAt } });
    if (!plane) {
      plane = makePlane(mesh, who, next.ownerIncarnation);
      // Deliberately not a deduping mock: count EVERY actual destination call so
      // control/router replay bugs cannot be hidden by fixture receipt suppression.
      const main: Ports[2] = { id, local: true, interactive: true, matches: target => target === id,
        deliverAgent: request => {
          calls.push({ message: request.message, deliveryId: request.deliveryId, data: request.data });
          emit({ phase: "delivered", calls: calls.slice() });
          return { queued: true, messageId: request.deliveryId ?? randomUUID(), routed: "local", triggered: true };
        } };
      const targetRouter = router(meshRoot, who, main, plane);
      plane.start((...args) => targetRouter.acceptControl(...args));
    }
    plane.notifyStartupReady(); ready = true;
    emit({ phase: "ready", token: next.incarnationToken, ownerIncarnation: next.ownerIncarnation });
  };
  process.on("message", (message: Wire) => {
    void (async () => {
      const { action, request } = message;
      if (action === "snapshot") emit({ request, calls: calls.slice(), ready, claim: claim?.lease });
      else if (action === "ready") { await markReady(Number(message.expiryMs ?? 60_000)); emit({ request, ok: true }); }
      else if (action === "retire") { await plane?.close(); emit({ request, ok: true }); }
      else if (action === "handoff") {
        assert(claim);
        // This helper must persist clean-handoff authority under lease custody.
        const helper = (startupApi as unknown as Record<string, unknown>).markHostLeaseCleanHandoff;
        assert.equal(typeof helper, "function", "Missing markHostLeaseCleanHandoff API; see lease-api.md");
        await plane?.close();
        // This minimal host publishes only its lease/host row; quiesce that row
        // before marking handoff, just as directory withdrawal precedes the API.
        await mesh.put({ identity: who, key: "topology/hosts/" + createHash("sha256").update(id).digest("hex"),
          value: { format: 1, id, rootId: id, identity: who, status: "reloading",
            incarnationToken: claim.lease.incarnationToken, ownerIncarnation: claim.lease.ownerIncarnation,
            startedAt: claim.lease.startedAt, updatedAt: Date.now(), expiresAt: Date.now() - 1 } });
        assert.equal(await (helper as (mesh: MeshStore, owner: leases.FabricHostLease,
          options: { ownIncarnation: string | undefined }) => Promise<boolean>)(mesh, claim.lease,
          { ownIncarnation: claim.lease.process?.startTime }), true, "clean-handoff identity proof refused");
        emit({ request, ok: true, phase: "handoff-written" });
      } else if (action === "stop") {
        await plane?.close(); emit({ request, ok: true });
        process.disconnect?.(); process.exit(0);
      }
    })().catch(error => emit({ request: message.request, error: errorText(error) }));
  });
  emit({ phase: "claim-entered", pid: process.pid, birth: localStartTime(process.pid) });
  claim = await claimStartingHost({ meshRoot, cwd: meshRoot, id, sessionId, lockProtocol: 1, stateBackend: "file" });
  assert(claim, "same-session new process unexpectedly declined claim");
  emit({ phase: "claimed", lease: claim.lease });
  if (!slow) await markReady();
}

class Peer {
  readonly child: ChildProcess;
  readonly messages: Wire[] = [];
  readonly bus = new EventEmitter();
  readonly exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  private stderr = "";
  constructor(readonly root: string, readonly sessionId: string, slow = false, unreadablePid?: number) {
    const env: NodeJS.ProcessEnv = { ...process.env, TMPDIR: root, TMP: root, TEMP: root,
      PI_FABRIC_MESH_ROOT: root, PI_FABRIC_PROJECT_ROOT: root, PI_CODING_AGENT_DIR: path.join(root, "agent") };
    for (const key of Object.keys(env)) {
      if ((key.startsWith("PI_FABRIC_") && !["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT"].includes(key)) ||
        ["HERDR_PANE_ID", "HERDR_SOCKET_PATH", "PI_SESSION_ID", "PI_SESSION_FILE", "SMARTY_AGENT", "SMARTY_ROLE"].includes(key)) delete env[key];
    }
    this.child = spawn(process.execPath, [self, "--child", root, sessionId, slow ? "slow" : "ready", String(unreadablePid ?? "")],
      { cwd: root, env, stdio: ["ignore", "ignore", "pipe", "ipc"] });
    this.child.stderr!.on("data", chunk => { this.stderr = (this.stderr + String(chunk)).slice(-8_000); });
    this.child.on("message", (message: Wire) => { this.messages.push(message); this.bus.emit("change"); });
    this.child.on("error", error => { this.messages.push({ error: errorText(error) }); this.bus.emit("change"); });
    this.exited = new Promise(resolve => this.child.once("exit", (code, signal) => {
      this.bus.emit("change"); resolve({ code, signal });
    }));
  }
  async wait(predicate: (wire: Wire) => boolean, ms = 10_000): Promise<Wire> {
    return bounded(new Promise<Wire>((resolve, reject) => {
      const check = () => {
        const error = this.messages.find(message => message.error && !message.request);
        const match = this.messages.find(predicate);
        if (error) { cleanup(); reject(new Error(String(error.error))); }
        else if (match) { cleanup(); resolve(match); }
        else if (this.child.exitCode !== null || this.child.signalCode !== null) {
          cleanup(); reject(new Error(`peer exited before event: ${this.stderr}`));
        }
      };
      const cleanup = () => this.bus.removeListener("change", check);
      this.bus.on("change", check); check();
    }), ms, `peer event (${this.child.pid}); stderr=${this.stderr}`);
  }
  async rpc(action: string, fields: Wire = {}): Promise<Wire> {
    const request = randomUUID(); this.child.send({ action, request, ...fields });
    const reply = await this.wait(message => message.request === request);
    assert(!reply.error, String(reply.error)); return reply;
  }
  claimed() { return this.messages.some(message => message.phase === "claimed"); }
  async calls(): Promise<Array<{ message: string; deliveryId?: string; data?: unknown }>> {
    return (await this.rpc("snapshot")).calls as Array<{ message: string; deliveryId?: string; data?: unknown }>;
  }
  async stop() {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    await this.rpc("stop");
    const result = await bounded(this.exited, 5_000, "owned child graceful stop");
    assert.equal(result.code, 0);
  }
  async killOwned() {
    assert(this.child.pid && this.child.exitCode === null && this.child.signalCode === null);
    assert.equal(this.child.kill("SIGKILL"), true);
    const result = await bounded(this.exited, 5_000, "owned incumbent exit");
    assert.equal(result.signal, "SIGKILL");
    assert.throws(() => process.kill(this.child.pid!, 0), (error: unknown) =>
      (error as NodeJS.ErrnoException).code === "ESRCH", "must observe pid gone before takeover evidence");
  }
}

export async function runCase(name: ProbeCase) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `fabric-takeover-${name}-`));
  fs.mkdirSync(path.join(root, "agent"), { mode: 0o700 });
  const sessionId = randomUUID(), id = `session:${sessionId}`, peers: Peer[] = [];
  const senderId = identity(`session:${randomUUID()}`), sender = makePlane(meshAt(root), senderId);
  const source = router(root, senderId, { id: senderId.id, local: true, interactive: true,
    matches: target => target === senderId.id, deliverAgent: () => { throw new Error("sender cannot receive fixture messages"); } }, sender);
  sender.start(() => ({ accepted: false }));
  const add = (slow = false, unreadablePid?: number) => { const peer = new Peer(root, sessionId, slow, unreadablePid); peers.push(peer); return peer; };
  const read = () => { const lease = leases.readHostLeaseCurrent(root, id); assert(lease); return lease; };
  const send = (message: string, key = message) => source.routeMessage(id, message,
    { privateMarker: message }, "followUp", undefined, { idempotencyKey: key });
  const receipt = { case: name, root, verdict: "PASS", realProcesses: true, payloadInspection: "recipient-calls-only" };
  try {
    const incumbent = add(); await incumbent.wait(message => message.phase === "ready");
    const original = read();
    assert(original.process);
    assert(original.process.pid === incumbent.child.pid && original.process.host === os.hostname());
    assert.equal(localStartTime(incumbent.child.pid!), original.process.startTime);
    if (name === "live") {
      assert.equal(await startupApi.markHostLeaseCleanHandoff(meshAt(root), original,
        { ownIncarnation: original.process.startTime }), false, "a public predecessor token is not handoff authority");
      assert.equal(read().cleanHandoff, undefined);
    }
    if (name === "unreadable") {
      assert.equal(process.platform, "linux", "the procfs read-refusal probe is Linux-only");
      await incumbent.rpc("ready", { expiryMs: 1_800 });
      const expiry = read().expiresAt;
      const successor = add(false, incumbent.child.pid!);
      await successor.wait(message => message.phase === "claim-entered");
      await delay(2_200);
      assert(Date.now() > expiry);
      assert(successor.messages.some(message => message.phase === "birth-probe-refused"));
      assert.equal(localStartTime(incumbent.child.pid!), original.process.startTime);
      assert(!successor.claimed(), "valid recorded birth with unreadable live probe must stay fenced after TTL");
      assert.equal(read().incarnationToken, original.incarnationToken);
      await incumbent.rpc("ready");
      await send("unreadable-incumbent-private");
      assert.deepEqual((await incumbent.calls()).map(call => call.message), ["unreadable-incumbent-private"]);
      assert.deepEqual(await successor.calls(), []);
      return receipt;
    }
    if (name === "birth" || name === "unknown") {
      const mesh = meshAt(root);
      // Simulated old birth is explicitly fixture data for a STILL LIVE real PID.
      // A nonempty malformed identity is UNKNOWN, not a comparable old birth.
      // Start with a future lease to catch pre-expiry malformed-identity takeover.
      // PID reuse means the live PID is NOT the old receiver. Keep the real process
      // alive, but retire its control listener before representing that unrelated PID.
      if (name === "birth") await incumbent.rpc("retire");
      const modified = { ...original, expiresAt: name === "birth" ? Date.now() - 1 : Date.now() + 60_000,
        process: { ...original.process, startTime: name === "birth"
          ? differentNativeBirth(original.process.startTime) : "unproven" } };
      await leases.withOwnedHostLease(mesh, original, () => leases.writeHostLease(root, modified));
      const successor = add(); await successor.wait(message => message.phase === "claim-entered");
      if (name === "birth") {
        await successor.wait(message => message.phase === "ready");
        process.kill(incumbent.child.pid!, 0); // Numeric PID is live; recorded incarnation is exited.
        assert.notEqual(read().incarnationToken, original.incarnationToken);
        await send("birth-private");
        assert.deepEqual((await successor.calls()).map(call => call.message), ["birth-private"]);
        assert.deepEqual(await incumbent.calls(), []);
      } else {
        assert(!validProcessIncarnation("unproven"), "counterexample must not be a comparable birth");
        const malformedBytes = fs.readFileSync(leases.hostLeasePath(root, id), "utf8");
        await delay(700);
        assert(Date.now() < modified.expiresAt, "malformed-identity check must happen BEFORE expiry");
        process.kill(incumbent.child.pid!, 0);
        assert(!successor.claimed(), "malformed nonempty birth must fail closed before lease expiry");
        assert.equal(fs.readFileSync(leases.hostLeasePath(root, id), "utf8"), malformedBytes);
        assert.equal(read().process?.startTime, "unproven");
        // TRANSITIONAL legacy compatibility: missing pid+birth retains its old TTL,
        // but cannot be replaced BEFORE that TTL. A known live identity never uses this.
        // Older releases can quiesce without writing the new handoff/identity fields.
        await incumbent.rpc("retire");
        const legacy = { ...modified, process: undefined, expiresAt: Date.now() + 1_800 };
        await leases.withOwnedHostLease(mesh, original, () => leases.writeHostLease(root, legacy));
        await delay(300);
        assert(!successor.claimed(), "legacy unknown identity must retain authority before expiry");
        assert.equal(read().incarnationToken, original.incarnationToken);
        await successor.wait(message => message.phase === "ready", 7_000);
        assert(Date.now() >= legacy.expiresAt, "legacy takeover must wait for TTL expiry");
        assert.notEqual(read().incarnationToken, original.incarnationToken);
        process.kill(incumbent.child.pid!, 0);
        await send("legacy-successor-private");
        assert.deepEqual((await successor.calls()).map(call => call.message), ["legacy-successor-private"]);
        assert.deepEqual(await incumbent.calls(), []);
      }
      return receipt;
    }
    // Shorten only the incumbent's owned fixture lease BEFORE the successor arms its
    // real claim watch. SIGKILL emits no file event; expiry must recheck pid+birth.
    await incumbent.rpc("ready", { expiryMs: name === "handoff" || name === "death" ? 60_000 : 1_800 });
    const before = fs.readFileSync(leases.hostLeasePath(root, id), "utf8");
    let successor = add(name === "slow");
    await successor.wait(message => message.phase === "claim-entered");
    await delay(200);
    assert(!successor.claimed(), "live incumbent was preempted by same-session startup");
    assert.equal(fs.readFileSync(leases.hostLeasePath(root, id), "utf8"), before, "pending claim mutated incumbent lease");
    await send("incumbent-private");
    assert.deepEqual((await incumbent.calls()).map(call => call.message), ["incumbent-private"]);
    assert.deepEqual(await successor.calls(), []);
    if (name === "live") {
      await delay(2_000); // Deliberately cross fixture expiry with real incumbent still alive.
      assert(!successor.claimed(), "expiry alone cannot authorize live-owner takeover");
      assert.equal(read().incarnationToken, original.incarnationToken);
      await incumbent.rpc("ready");
      await send("incumbent-still-private");
      assert.deepEqual((await incumbent.calls()).map(call => call.message), ["incumbent-private", "incumbent-still-private"]);
      assert.deepEqual(await successor.calls(), []);
    } else {
      const start = Date.now();
      if (name === "handoff") {
        // Keep the predecessor process alive after clean release: ONLY the actual
        // handoff receipt, not PID death or elapsed TTL, may authorize this claim.
        await incumbent.rpc("handoff");
      } else {
        // A fresh attempt must prove a gone PID immediately, even with a future TTL.
        // The slow case separately exercises a pending successor's single expiry wake.
        if (name === "death") await successor.stop();
        await incumbent.killOwned();
        if (name === "death") successor = add();
      }
      await successor.wait(message => message.phase === "claimed", 7_000);
      if (name === "death") assert(Date.now() - start < 1_500, "proven exit waited for future TTL");
      if (name === "handoff") {
        assert(Date.now() - start < 1_500, "clean handoff waited for ordinary expiry");
        process.kill(incumbent.child.pid!, 0);
      }
      assert.notEqual(read().incarnationToken, original.incarnationToken);
      if (name === "slow") {
        // Real startup claim is held without a runtime; no fake clocks. Preserve
        // the existing >30s slow-start acceptance and sender-close durability case.
        await delay(31_100);
        const first = await send("slow-private", "slow-once");
        const duplicate = await send("slow-private", "slow-once");
        assert.equal(first.reason, "startup-inbox-durable");
        assert.equal(duplicate.messageId, first.messageId);
        await sender.close();
        await successor.rpc("ready");
        await successor.wait(message => message.phase === "delivered");
        await delay(300);
        const calls = await successor.calls();
        assert.equal(calls.length, 1); assert.equal(calls[0]!.message, "slow-private");
        assert.equal(calls[0]!.deliveryId, first.messageId);
      } else {
        await successor.wait(message => message.phase === "ready");
        const first = await send("successor-private", "takeover-once");
        const duplicate = await send("successor-private", "takeover-once");
        assert.equal(duplicate.messageId, first.messageId);
        await delay(300);
        const calls = await successor.calls();
        assert.equal(calls.length, 1); assert.equal(calls[0]!.message, "successor-private");
        assert.equal(calls[0]!.deliveryId, first.messageId);
      }
    }
    return receipt;
  } finally {
    await sender.close();
    // No root/lease/session removal. Pending claim children accept stop over IPC.
    for (const peer of peers.reverse()) await peer.stop();
  }
}

if (process.argv[2] === "--child") {
  childMain(process.argv[3]!, process.argv[4]!, process.argv[5] === "slow", Number(process.argv[6]) || undefined).catch(error => {
    emit({ error: errorText(error) }); process.exitCode = 1; process.disconnect?.();
  });
} else if (path.resolve(process.argv[1] ?? "") === self) {
  const selected = process.argv.includes("--case") ? [process.argv[process.argv.indexOf("--case") + 1]] : [...CASES];
  assert(selected.every(name => CASES.includes(name as ProbeCase)), "invalid --case");
  for (const name of selected) {
    console.log(JSON.stringify(await bounded(runCase(name as ProbeCase), 65_000, `case ${name}`)));
  }
}
