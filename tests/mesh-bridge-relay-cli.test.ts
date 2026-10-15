// Hosted-CI-only expected path: Linux GitHub Actions, after the parent builds dist/.
// NOT runnable locally: stopping the real CLI can execute product mirror/lease deletion.
// Never override this guard or preload deletion mocks into the cross-process CLI canary.
// Allocate only this test's own short, private mkdtemp child; no provisioned CI path.
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const HOSTED = process.env.GITHUB_ACTIONS === "true" && process.platform === "linux";
const BIN = path.resolve("bin/mesh-bridge");
const BUILT_CLI = path.resolve("dist/mesh-bridge.js");
const PROCESS_DEADLINE_MS = 60_000;
const OBSERVATION_DEADLINE_MS = 15_000;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];

afterEach(async () => {
  // Everything in these lists is created only inside the hosted-only describe.
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const hostKey = (id: string): string => `topology/hosts/${hash(id)}`;
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;
const events = (store: MeshStore, topic: string) => store.read({ topic, after: 0, limit: 100 });

// Real store-byte evidence, not an assertion on bridge source or a mocked publish argument.
const markerFiles = (root: string, marker: string): string[] => {
  const found: string[] = [];
  const walk = (dir: string): void => {
    for (const name of fs.readdirSync(dir)) {
      const file = path.join(dir, name);
      try {
        const stat = fs.lstatSync(file);
        expect(stat.isSymbolicLink(), file).toBe(false);
        if (stat.isDirectory()) walk(file);
        else if (stat.isFile() && fs.readFileSync(file).includes(Buffer.from(marker))) found.push(path.relative(root, file));
      } catch (error) {
        // Ignore only an atomic writer's disappearing temporary entry, not I/O failures.
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
  };
  walk(root);
  return found;
};

// Normal progress is driven by the mesh root's rename/append notifications. One finite timer
// is a failed-observation deadline, not a polling carrier; install the watch before checking.
const observeStore = (root: string, condition: () => boolean): Promise<void> => new Promise((resolve, reject) => {
  let watcher: fs.FSWatcher | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  let lastError: unknown;
  const finish = (error?: Error): void => {
    watcher?.close();
    if (deadline) clearTimeout(deadline);
    if (error) reject(error); else resolve();
  };
  const check = (): void => {
    try { if (condition()) finish(); }
    catch (error) { lastError = error; }
  };
  watcher = fs.watch(root, check);
  watcher.on("error", finish);
  deadline = setTimeout(() => finish(new Error(`Store observation timed out at ${root}: ${String(lastError ?? "condition not reached")}`)), OBSERVATION_DEADLINE_MS);
  check();
});

const native = async (store: MeshStore, name: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `fixture:${identity.id}`;
  await store.put({ key: hostKey(identity.id), identity, value: {
    format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: participantKey(identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return { identity, ownerIncarnation };
};

interface LinkProcess {
  child: ChildProcess;
  closed: Promise<void>;
  ready: Promise<void>;
  diagnostics(): string;
}

const startLink = (base: string, remote: "ryzen2" | "ryzen3", env: NodeJS.ProcessEnv): LinkProcess => {
  // Each link is its own installed CLI process, not runBridge()/MeshBridge in the test process.
  // Each also runs the actual installed agent command as its stdio transport.
  const child = spawn(process.execPath, [BIN, "run", "--mesh", path.join(base, "ryzen1"), "--name", "ryzen1",
    "--remote", remote, "--cursor", path.join(base, `${remote}.json`), "--call-timeout-ms", "5000", "--",
    process.execPath, BIN, "agent", "--mesh", path.join(base, remote), "--peer", "ryzen1"], {
    env, stdio: ["ignore", "ignore", "pipe"], timeout: PROCESS_DEADLINE_MS, killSignal: "SIGTERM",
  });
  let stderr = "";
  let readyResolve!: () => void;
  let readyReject!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  void ready.catch(() => undefined);
  const deadline = setTimeout(() => readyReject(new Error(`CLI did not link ${remote}: ${stderr}`)), OBSERVATION_DEADLINE_MS);
  child.stderr!.setEncoding("utf8");
  child.stderr!.on("data", (chunk: string) => {
    stderr += chunk;
    if (stderr.includes(`linked ryzen1 <-> ${remote}`)) { clearTimeout(deadline); readyResolve(); }
  });
  const closed = new Promise<void>(resolve => {
    child.once("error", error => { clearTimeout(deadline); readyReject(error); resolve(); });
    child.once("close", (code, signal) => {
      clearTimeout(deadline);
      readyReject(new Error(`CLI ${remote} closed (${code}/${signal}): ${stderr}`));
      resolve();
    });
  });
  closers.push(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    const kill = setTimeout(() => child.kill("SIGKILL"), 8_000);
    try { await closed; } finally { clearTimeout(kill); }
  });
  return { child, closed, ready, diagnostics: () => stderr };
};

// The module may namespace sockets by physical mesh under the private relay directory.
// Resolve the real socket, without assuming an ungranted hash/subdirectory scheme.
const relaySocket = (runtime: string, remote: string): string => {
  const matches: string[] = [];
  const walk = (dir: string): void => {
    const stat = fs.lstatSync(dir);
    expect(stat.isSymbolicLink()).toBe(false);
    expect(stat.uid).toBe(process.getuid!());
    expect(stat.mode & 0o777).toBe(0o700);
    for (const name of fs.readdirSync(dir)) {
      const file = path.join(dir, name), entry = fs.lstatSync(file);
      expect(entry.isSymbolicLink()).toBe(false);
      if (entry.isDirectory()) walk(file);
      else if (entry.isSocket() && name === `relay-${remote}.sock`) matches.push(file);
    }
  };
  walk(path.join(runtime, "mesh-bridge", "relay"));
  expect(matches, `one live ${remote} relay socket for this physical mesh`).toHaveLength(1);
  return matches[0]!;
};

const fixture = async () => {
  expect(HOSTED, "real CLI fixture must never execute locally").toBe(true);
  expect(fs.existsSync(BUILT_CLI), "parent must run bun run build FIRST").toBe(true);
  // Exclusive private allocation. Product sockets and the connector probe use short
  // FD-anchored paths rather than assuming the inherited TMPDIR pathname is short.
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mbr-"));
  fs.chmodSync(base, 0o700);
  roots.push(base);
  expect(fs.lstatSync(base).isSymbolicLink()).toBe(false);
  expect(fs.statSync(base).mode & 0o777).toBe(0o700);
  expect(fs.statSync(base).uid).toBe(process.getuid!());
  const home = path.join(base, "h"), runtime = path.join(base, "x");
  for (const dir of [home, runtime]) fs.mkdirSync(dir, { mode: 0o700 });
  const stores = Object.fromEntries(["ryzen1", "ryzen2", "ryzen3"].map(name => [name,
    new MeshStore(path.join(base, name), 256 * 1024, 500, { stateBackend: "file" })]));
  const mains = Object.fromEntries(await Promise.all(Object.entries(stores).map(async ([name, store]) => [name, await native(store, name)] as const)));
  const env = { ...process.env, HOME: home, XDG_RUNTIME_DIR: runtime, PI_FABRIC_MESH_STATE_BACKEND: "file" };
  const links = [startLink(base, "ryzen2", env), startLink(base, "ryzen3", env)];
  await Promise.all(links.map(link => link.ready));
  expect(links[0]!.child.pid).toBeGreaterThan(0);
  expect(links[1]!.child.pid).toBeGreaterThan(0);
  expect(links[0]!.child.pid).not.toBe(links[1]!.child.pid);
  await observeStore(stores.ryzen3!.root, () => stores.ryzen3!.get(participantKey(mains.ryzen2!.identity.id), { fresh: true }) !== undefined);
  return { base, runtime, home, stores, mains, links };
};

describe.skipIf(!HOSTED)("real two-process built mesh-bridge relay (hosted Linux CI only; product deletion on stop)", () => {
  it("retains hub-local followUp semantics through the built source CLI", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, local = f.mains.ryzen1!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    const receiver = new FabricControlPlane(hub, local.identity, { enabled: true, hostId: local.identity.id,
      ownerIncarnation: local.ownerIncarnation });
    const marker = "pr1076-cli-hub-local-followUp";
    const received: unknown[] = [];
    sender.start(() => ({ accepted: false }));
    receiver.start((command, from) => {
      received.push({ command, from });
      return { accepted: true, messageId: "hub-local-delivered", triggered: true, pendingFollowUps: 1, oldestAgeS: 0 };
    });
    closers.push(async () => { await sender.close(); await receiver.close(); });
    await expect(sender.request(local.identity.id, local.identity.id, "followUp",
      { message: marker, data: { marker }, ownerIncarnation: local.ownerIncarnation }, local.identity.id,
      { routedRemoteHost: "ryzen1" })).resolves.toMatchObject({ acknowledged: true, messageId: "hub-local-delivered" });
    expect(received).toEqual([expect.objectContaining({ command: expect.objectContaining({ operation: "followUp", message: marker,
      data: { marker } }), from: expect.objectContaining({ id: source.identity.id, verified: "bridge" }) })]);
    expect(events(hub, "fabric.control.command")).toHaveLength(1);
    expect(markerFiles(hub.root, marker).length).toBeGreaterThan(0); // scanner positive control
    expect(markerFiles(f.stores.ryzen2!.root, marker)).toEqual([]);
    expect(f.links.map(link => link.diagnostics()).join("\n")).not.toContain(marker);
  }, 30_000);

  it.each([1, 3])("relays %s sensitive controls through two real link processes without hub body persistence", async count => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id,
      ownerIncarnation: target.ownerIncarnation });
    const markers = Array.from({ length: count }, (_, index) => `pr1076-cli-sensitive-${count}-${index}`);
    const ackMarker = `pr1076-cli-private-result-${count}`;
    const received: unknown[] = [];
    sender.start(() => ({ accepted: false }));
    receiver.start((command, from) => {
      received.push({ command, from });
      return { accepted: true, messageId: `on-B-${markers.indexOf(command.message ?? "")}`, triggered: true, result: { marker: ackMarker } };
    });
    closers.push(async () => { await sender.close(); await receiver.close(); });
    const sourceLink = f.links[1]!;
    // A real source backlog guarantees the negotiated input page contains the whole batch;
    // no mocked relay, RPC client, handler or peer credential helper replaces either CLI.
    expect(sourceLink.child.kill("SIGSTOP")).toBe(true);
    const pending = markers.map(marker => sender.request(target.identity.id, target.identity.id, "followUp",
      { message: marker, data: { nested: { marker } }, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: marker }));
    for (const request of pending) void request.catch(() => undefined);
    try { await observeStore(a.root, () => events(a, "fabric.control.command").length === count); }
    finally { sourceLink.child.kill("SIGCONT"); }
    await expect(Promise.all(pending)).resolves.toEqual(markers.map((_, index) => expect.objectContaining({
      acknowledged: true, messageId: `on-B-${index}`, routed: "mesh" })));
    expect(received).toHaveLength(count);
    for (const marker of markers) {
      expect(received).toEqual(expect.arrayContaining([expect.objectContaining({
        command: expect.objectContaining({ operation: "followUp", message: marker, data: { nested: { marker } },
          ownerIncarnation: target.ownerIncarnation }), from: expect.objectContaining({ id: source.identity.id, verified: "bridge" }) })]));
      expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
      expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
      expect(markerFiles(hub.root, marker)).toEqual([]);
      expect(markerFiles(f.runtime, marker)).toEqual([]);
      for (const remote of ["ryzen2", "ryzen3"]) expect(fs.readFileSync(path.join(f.base, `${remote}.json`), "utf8")).not.toContain(marker);
    }
    expect(events(b, "fabric.control.command")).toHaveLength(count);
    expect(events(b, "fabric.control.command").every(event => event.verification === "bridge" &&
      (event.data as { bridge: { from: string; via: string } }).bridge.from === "ryzen3" &&
      (event.data as { bridge: { from: string; via: string } }).bridge.via === "ryzen1")).toBe(true);
    expect(markerFiles(b.root, ackMarker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, ackMarker)).toEqual([]);
    expect(markerFiles(a.root, ackMarker)).toEqual([]);
    // Successful hub receipts may retain routing metadata, but never body fields.
    for (const receipt of events(hub, "fabric.control.command")) {
      for (const field of ["data", "text", "message"]) expect(receipt).not.toHaveProperty(field);
    }
    // ACKs still use the hub's existing allow-listed return leg, outcome-only.
    expect(events(hub, "fabric.control.ack")).toHaveLength(count);
    for (const ack of events(hub, "fabric.control.ack")) {
      expect(ack).not.toHaveProperty("text");
      expect(ack.data).not.toHaveProperty("result");
    }
    expect(events(a, "fabric.control.ack")).toHaveLength(count);
    for (const ack of events(a, "fabric.control.ack")) {
      expect(ack).not.toHaveProperty("text");
      expect(ack.data).not.toHaveProperty("result");
    }
    for (const remote of ["ryzen2", "ryzen3"]) {
      const socket = fs.lstatSync(relaySocket(f.runtime, remote));
      expect(socket.isSocket()).toBe(true);
      expect(socket.uid).toBe(process.getuid!());
      expect(socket.mode & 0o777).toBe(0o600);
    }
    const diagnostics = f.links.map(link => link.diagnostics()).join("\n");
    for (const marker of [...markers, ackMarker]) expect(diagnostics).not.toContain(marker);
  }, 30_000);

  it("destination process down returns a typed retryable hub refusal, without a destination-owner forgery or hub body", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    sender.start(() => ({ accepted: false }));
    closers.push(() => sender.close());
    // Crash B's hub-link process, retaining a live captured owner projection long enough
    // to reach the direct-IPC failure seam, rather than a pre-publication missing-route error.
    expect(f.links[0]!.child.kill("SIGKILL")).toBe(true);
    await f.links[0]!.closed;
    expect(directory.mirroredControlOwner(target.identity.id, target.identity.id, target.identity.id))
      .toMatchObject({ remoteHost: "ryzen2" });
    const marker = "pr1076-cli-destination-down-sensitive";
    const outcome = await sender.request(target.identity.id, target.identity.id, "followUp",
      { message: marker, data: { marker }, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "destination-down-stable-key" }).then(
        value => ({ value, error: undefined }), error => ({ value: undefined, error }));
    expect(outcome.error).toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    for (const field of ["notRun", "notDelivered", "result"]) expect(outcome.error).not.toHaveProperty(field);
    const refusal = events(a, "fabric.control.ack").find(event =>
      (event.data as { errorCode?: string }).errorCode === "FABRIC_RELAY_DESTINATION_UNAVAILABLE");
    expect(refusal).toMatchObject({ verification: "bridge", from: { id: "bridge:ryzen1" }, to: source.identity.id,
      data: { accepted: false, retryable: true, targetId: target.identity.id, bridge: { from: "ryzen1" } } });
    expect(refusal).not.toHaveProperty("text");
    for (const field of ["ownerIncarnation", "notRun", "notDelivered", "result", "message"]) expect(refusal!.data).not.toHaveProperty(field);
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker)).toEqual([]);
    expect(markerFiles(hub.root, marker)).toEqual([]);
    expect(markerFiles(f.runtime, marker)).toEqual([]);
    expect(events(hub, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.ack")).toEqual([]);
    expect(f.links.map(link => link.diagnostics()).join("\n")).not.toContain(marker);
  }, 30_000);

  it("refuses a same-UID non-bridge connector before receiving a relay body", async () => {
    const f = await fixture();
    const socketPath = relaySocket(f.runtime, "ryzen2");
    const before = events(f.stores.ryzen2!, "fabric.control.command");
    // This Vitest process already opened the same mesh stores (and has writer-census
    // registration), but its actual main script is not bin/mesh-bridge run. Same UID
    // or registration alone must not grant it relay authority. No guessed wire frame.
    const connection = await new Promise<{ connected: boolean; bytes: number }>((resolve, reject) => {
      // Pin the test-owned directory and use a short FD-anchored pathname even if
      // the module has added a mesh hash beneath a long inherited private TMPDIR.
      const directory = fs.openSync(path.dirname(socketPath), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
      const connectionPath = `/proc/self/fd/${directory}/${path.basename(socketPath)}`;
      expect(Buffer.byteLength(connectionPath)).toBeLessThan(108);
      const socket = net.createConnection(connectionPath);
      let connected = false, bytes = 0;
      const deadline = setTimeout(() => { socket.destroy(); reject(new Error("Non-bridge connector was not refused before body admission")); }, 8_000);
      socket.on("connect", () => { connected = true; });
      socket.on("data", chunk => { bytes += chunk.length; });
      socket.on("error", error => {
        // A connected peer may refuse by closing/resetting, but an unavailable
        // listener or failed connect is not evidence of non-bridge authentication.
        if (connected && ["ECONNRESET", "EPIPE"].includes((error as NodeJS.ErrnoException).code ?? "")) return;
        clearTimeout(deadline); reject(error);
      });
      socket.on("close", () => { clearTimeout(deadline); fs.closeSync(directory); resolve({ connected, bytes }); });
    });
    expect(connection.connected).toBe(true); // listening destination existed, not ENOENT/ECONNREFUSED
    expect(events(f.stores.ryzen2!, "fabric.control.command")).toEqual(before);
    expect(events(f.stores.ryzen1!, "fabric.control.command")).toEqual([]);
    expect(events(f.stores.ryzen1!, "fabric.control.ack")).toEqual([]);
  }, 30_000);
});
