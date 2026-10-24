import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { HostLeaseLockBusyError } from "../src/topology/host-lease-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY, readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import * as participantFiles from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// fabric-store review of pi-fabric#1562, P1: the participant namespace receipt (participant-namespace/.namespace)
// was never recovered by prepareParticipantFileLocks, which scanned only participants/.locks. Under the registry
// fence a holder whose PID is live (reused) or unprobeable (EPERM) is reported busy, so after such a holder died
// every fenced participant addition stayed ParticipantFileLockBusyError: on a files-only host with no bridge (no
// unfenced writer) its new sessions never reached the mesh.

const roots: string[] = [];
const children: ChildProcess[] = [];
const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const child of children.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-namespace-recovery-"));
  roots.push(root);
  return root;
};
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const PREFIX = "topology/participants/";
const namespaceLock = (root: string): string => path.join(root, "participant-namespace", ".namespace");
const namespaceOwner = (root: string): string | undefined => {
  try { return fs.readFileSync(path.join(namespaceLock(root), "owner"), "utf8"); } catch { return undefined; }
};
/** Two distinct valid start identities on this platform: the receipt's, and the one the reused PID has now. */
const START = {
  linux: ["4242", "4243"],
  win32: ["win32:639264528000000000", "win32:639264528000000001"],
  darwin: ["darwin:Thu Oct  1 12:00:00 2026", "darwin:Fri Oct  2 12:00:00 2026"],
}[process.platform as "linux" | "win32" | "darwin"] ?? ["4242", "4243"];

/** A live process standing in for whatever the OS gave the dead holder's PID to. */
const standIn = async (): Promise<number> => {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  children.push(child);
  await new Promise<void>((resolve, reject) => { child.once("spawn", () => resolve()); child.once("error", reject); });
  return child.pid!;
};

/** The holder probes the test controls: the reused PID's native start identity is the successor's, and
 * (eperm) probing that PID is refused as for another user's process. Every native read is recorded. */
const reusedPid = (pid: number, mode: "reused" | "eperm") => {
  const reads: number[] = [];
  vi.spyOn(atomic, "processIncarnation").mockImplementation(async (target: number) => {
    reads.push(target);
    return target === pid ? START[1] : undefined;
  });
  if (mode === "eperm") {
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
      if (target === pid && (signal === 0 || signal === undefined)) throw Object.assign(new Error("kill EPERM"), { code: "EPERM" });
      return kill(target, signal);
    }) as typeof process.kill);
  }
  return reads;
};

const plantNamespaceReceipt = (root: string, pid: number, start = START[0]!, token = "dead-namespace-holder"): string => {
  const lock = namespaceLock(root);
  fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
  const receipt = String(pid) + "\n" + start + "\n" + token + "\n";
  fs.writeFileSync(path.join(lock, "owner"), receipt, { mode: 0o600 });
  return receipt;
};

const record = (id: string, now = Date.now()): FabricParticipantRecord => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: "main", status: "idle",
  runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], cwd: "/tmp/project",
  sessionId: id.slice("session:".length), startedAt: now, updatedAt: now, pendingMessages: false, controlProtocol: "v1",
  label: "P-" + id.slice("session:".length),
});
const entryOf = (id: string) => {
  const identity: MeshIdentity = { id, name: "main", kind: "main" };
  return { key: PREFIX + digest(id), value: record(id), version: 1, updatedAt: Date.now(), updatedBy: identity };
};

/** A files-only host's participant directory publishing under a registry fence, as a ResidentHost does
 * (residency/host.ts withPublicationFence): every participant write it makes is registry-fenced. */
const fencedDirectory = async (root: string, name: string) => {
  const policy = new MeshStore(root, 65536, 100);
  await policy.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" },
    identity: { id: "policy", name: "policy", kind: "agent" } });
  const identity: MeshIdentity = { id: "session:" + name, name: "main", kind: "main", sessionId: name };
  let fenced = 0;
  const directory = new ParticipantDirectory(new MeshStore(root, 65536, 1000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000,
    reapDeadHosts: false,
    withPublicationFence: async publish => { fenced++; return publish(); },
  });
  cleanups.push(() => directory.close());
  directory.registerSource(() => [record(identity.id)]);
  const published = (): boolean => participantFiles.readParticipantFiles(root, { maxAgeMs: 0 })
    .some(entry => (entry.value as { id?: unknown }).id === identity.id);
  return { directory, identity, published, fenced: () => fenced };
};

describe("a dead holder of the participant namespace receipt (pi-fabric#1562 review P1)", () => {
  it.each(["reused", "eperm"] as const)("%s PID: a fenced addition is busy until prepareParticipantFileLocks recovers the receipt", async mode => {
    const root = path.join(scratch(), "mesh");
    const mesh = new MeshStore(root, 65536, 1000);
    const pid = await standIn();
    const reads = reusedPid(pid, mode);
    const receipt = plantNamespaceReceipt(root, pid);
    const entry = entryOf("session:fenced-add");
    const add = () => participantFiles.writeParticipantFileIf(mesh, entry.key, current => current ? undefined : entry,
      { registryFenced: true, ownIncarnation: undefined });
    // The reviewer's probe: under the fence the holder is busy, and the fence reads no native identity.
    await expect(add()).rejects.toBeInstanceOf(participantFiles.ParticipantFileLockBusyError);
    expect(reads).toEqual([]);
    expect(namespaceOwner(root)).toBe(receipt);
    // Preparation (outside every fence) reads the holder's native identity once and recovers the receipt.
    // 2dde3878: the receipt survived preparation and the fenced addition stayed busy.
    await participantFiles.prepareParticipantFileLocks(mesh);
    expect(reads.length).toBeGreaterThan(0);
    expect(new Set(reads)).toEqual(new Set([pid])); // read again under file custody before the removal
    expect(namespaceOwner(root)).toBeUndefined();
    await expect(add()).resolves.toBe(true);
    expect(participantFiles.readParticipantFile(root, entry.key)?.value).toMatchObject({ id: "session:fenced-add" });
    expect(fs.existsSync(namespaceLock(root))).toBe(false);
  });

  it("a live holder (its start identity unchanged) or an unknown one is never recovered by preparation", async () => {
    const root = path.join(scratch(), "mesh");
    const mesh = new MeshStore(root, 65536, 1000);
    const pid = await standIn();
    const reads: number[] = [];
    let answer: string | undefined = START[0];
    vi.spyOn(atomic, "processIncarnation").mockImplementation(async (target: number) => { reads.push(target); return answer; });
    const receipt = plantNamespaceReceipt(root, pid);
    await participantFiles.prepareParticipantFileLocks(mesh);
    expect(namespaceOwner(root)).toBe(receipt);
    answer = undefined;
    await participantFiles.prepareParticipantFileLocks(mesh);
    expect(namespaceOwner(root)).toBe(receipt);
    expect(reads).toEqual([pid, pid]);
  });

  it("preparation with no participant file yet creates neither participants/ nor participant-namespace/", async () => {
    const root = path.join(scratch(), "mesh");
    const mesh = new MeshStore(root, 65536, 1000);
    await participantFiles.prepareParticipantFileLocks(mesh);
    expect(fs.existsSync(path.join(root, "participants"))).toBe(false);
    expect(fs.existsSync(path.join(root, "participant-namespace"))).toBe(false);
  });

  it("a files-only host with no bridge: its fenced heartbeat fails busy, sets preparation, and the next one publishes", async () => {
    const root = path.join(scratch(), "mesh");
    vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
    const pid = await standIn();
    reusedPid(pid, "reused");
    plantNamespaceReceipt(root, pid);
    const host = await fencedDirectory(root, "files-only");
    await expect(host.directory.refresh()).rejects.toBeInstanceOf(participantFiles.ParticipantFileLockBusyError);
    expect(host.published()).toBe(false);
    // 2dde3878: this refresh failed busy again, and every later one did too.
    await host.directory.refresh();
    expect(host.published()).toBe(true);
    expect(host.fenced()).toBeGreaterThanOrEqual(2);
    expect(namespaceOwner(root)).toBeUndefined();
  });
});

// The bridge's mirror custody (bridge.ts withMirrorCustody): the participant namespace and the relevant key
// receipts, then the host-lease additions gate and the relevant per-host gates. A child takes exactly that
// custody, reports, and is SIGKILLed mid-pass (an OOM kill), leaving both receipts behind.
const MIRROR = String.raw`
  import { createJiti } from "jiti";
  import { pathToFileURL } from "node:url";
  import fs from "node:fs";
  const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
  const { MeshStore } = await jiti.import("./src/mesh/store.ts");
  const { withParticipantFileDirectoryCustody } = await jiti.import("./src/topology/participant-files.ts");
  const { withHostLeaseDirectoryCustody } = await jiti.import("./src/topology/host-leases.ts");
  const [root, ready, id] = process.argv.slice(1);
  const store = new MeshStore(root, 65536, 1000);
  const ids = new Set([id]);
  setInterval(() => {}, 1000);
  await withParticipantFileDirectoryCustody(store, () => withHostLeaseDirectoryCustody(store, async () => {
    fs.writeFileSync(ready, String(process.pid));
    await new Promise(() => {});
  }, ids), ids);
`;

const killedMirror = async (root: string, id: string): Promise<{ pid: number; namespace: string; additions: string; additionsOwner: string }> => {
  fs.mkdirSync(root, { recursive: true });
  const ready = path.join(path.dirname(root), "mirror-ready-" + digest(root).slice(0, 8));
  const child = spawn(process.execPath, ["--input-type=module", "-e", MIRROR, root, ready, id],
    { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"] });
  children.push(child);
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += chunk; });
  const exited = new Promise<void>(resolve => child.once("close", () => resolve()));
  await vi.waitFor(() => {
    if (child.exitCode !== null) throw new Error("mirror child exited early: " + stderr);
    expect(fs.existsSync(ready)).toBe(true);
  }, { timeout: 20_000, interval: 20 });
  const pid = Number(fs.readFileSync(ready, "utf8"));
  const namespace = namespaceOwner(root)!;
  // Mid-pass: it holds the participant namespace receipt and the host-lease additions gate.
  expect(namespace.split("\n")[0]).toBe(String(pid));
  const additions = path.join(root, "host-lease-commits", digest(".additions").slice(0, 16));
  expect(fs.readdirSync(additions).length).toBeGreaterThan(0);
  const additionsOwner = fs.readFileSync(path.join(additions, "custody.lock", "owner"), "utf8");
  child.kill("SIGKILL");
  await exited;
  return { pid, namespace, additions, additionsOwner };
};

// The host-lease additions gate is a HOST-QUALIFIED custody lock (MeshStore.leaseCustody, src/mesh/store.ts). Its dead
// holder is recovered only with this machine's identity: "A host-lease commit gate is never recovered by a
// foreign/unknown PID. Matching machine evidence is still mandatory when boot/process identity is unavailable"
// (src/mesh/custody-lock.ts clearDeadCustodyLock). That identity is read from /etc/machine-id and /proc only
// (src/core/atomic-write.ts readPhysicalMachineId/readPhysicalHostIdentity), so on Windows it is absent and the gate
// fails closed by design, as unreadable machine evidence does on Linux (tests/host-lease-round3.test.ts "unreadable
// /etc/machine-id fails closed"; tests/mesh-lock-namespace.test.ts "host-qualified custody needs a machine id").
const nativeMachineEvidence = atomic.readPhysicalMachineId() !== undefined;
/** The Windows view on a host that has machine evidence: the machine and boot identity files are unreadable. */
const withoutMachineEvidence = (): void => {
  if (nativeMachineEvidence) {
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if (file === "/etc/machine-id" || file === "/proc/sys/kernel/random/boot_id") {
        throw Object.assign(new Error("no machine or boot identity (as on Windows)"), { code: "ENOENT" });
      }
      return read(...args);
    }) as typeof fs.readFileSync);
  }
  expect(atomic.readPhysicalMachineId()).toBeUndefined();
  expect(atomic.readPhysicalHostIdentity()).toBeUndefined();
};
/** The reused-PID variant: the OS hands the killed mirror's PID to a new process (same receipt, another start). */
const reuseKilledPid = async (root: string, killed: { namespace: string }): Promise<void> => {
  const pid = await standIn();
  reusedPid(pid, "reused");
  const [, start, token] = killed.namespace.split("\n");
  plantNamespaceReceipt(root, pid, atomic.validProcessIncarnation(start) ? start! : START[0]!, token!);
};
/** The hub and far meshes of a later mirror pass, and its bridge (not yet started). */
const laterMirrorPass = async (base: string, root: string, farId: string) => {
  const far = new MeshStore(path.join(base, "far"), 65536, 1000);
  await nativeOn(far, farId);
  const hub = new MeshStore(root, 65536, 1000);
  await nativeOn(hub, "session:hub-native");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(hub, "far"),
    remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(base, "cursor.json") });
  cleanups.push(() => bridge.stop());
  return { hub, bridge };
};

/** A far mesh with one live native root, for a later mirror pass into the hub. */
const nativeOn = async (mesh: MeshStore, id: string): Promise<MeshIdentity> => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main" };
  await mesh.put({ key: "topology/hosts/" + digest(id), identity,
    value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
  await mesh.put({ key: PREFIX + digest(id), identity, value: record(id, now) });
  writeHostLease(mesh.root, { id, rootId: id, identityId: id, updatedAt: now, expiresAt: now + 600_000, startedAt: now });
  return identity;
};

describe("a mirror killed mid-pass while holding the namespace receipt and the additions gate (pi-fabric#1562 review P1)", () => {
  const cases = (["dead", "reused"] as const).flatMap(pid => (["fenced add", "mirror pass"] as const).map(after => ({ pid, after })));
  // Recovery needs this machine's identity (see nativeMachineEvidence): absent on Windows, which runs the fail-closed
  // cases below natively instead (pi-fabric#1562 Windows CI: MeshLockTimeoutError on host-lease-locks/064d58ce...).
  it.skipIf(!nativeMachineEvidence).each(cases)("$pid PID: a later $after recovers", async ({ pid: holder, after }) => {
    const base = scratch();
    const root = path.join(base, "hub");
    const farId = "session:far-native";
    const killed = await killedMirror(root, farId);
    expect(namespaceOwner(root)).toBe(killed.namespace);
    if (holder === "reused") await reuseKilledPid(root, killed);
    if (after === "fenced add") {
      vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
      const host = await fencedDirectory(root, "after-kill");
      let attempts = 0;
      for (; attempts < 3 && !host.published(); attempts++) {
        await host.directory.refresh().catch(error => {
          if (!(error instanceof participantFiles.ParticipantFileLockBusyError)) throw error;
        });
      }
      expect(host.published()).toBe(true);
      // A dead PID is recovered by the fenced write itself (ESRCH proves absence); a reused one needs the
      // prepared retry the busy refusal schedules: one refused heartbeat, then the published one.
      expect(attempts).toBe(holder === "dead" ? 1 : 2);
    } else {
      const { hub, bridge } = await laterMirrorPass(base, root, farId);
      await bridge.start();
      // The pass took (recovered) both receipts: the far root's mirror and its lease are installed.
      expect(readHostLeaseCurrent(root, farId)).toMatchObject({ id: farId });
      expect(hub.listAll(PREFIX).some(entry => (entry.value as { id?: unknown }).id === farId)).toBe(true);
    }
    expect(namespaceOwner(root)).toBeUndefined();
  }, 40_000);

  // Without machine evidence (Windows natively; simulated where /etc/machine-id exists) the dead mirror's additions
  // gate cannot be proven dead: every later fenced add or mirror pass is refused busy, and nothing takes it over.
  it.each(cases)("$pid PID, no machine evidence (as on Windows): a later $after stays busy and takes nothing over",
    async ({ pid: holder, after }) => {
      const base = scratch();
      const root = path.join(base, "hub");
      const farId = "session:far-native";
      const killed = await killedMirror(root, farId);
      withoutMachineEvidence();
      if (holder === "reused") await reuseKilledPid(root, killed);
      if (after === "fenced add") {
        vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
        const host = await fencedDirectory(root, "after-kill");
        const refusals: unknown[] = [];
        for (let attempt = 0; attempt < 3; attempt++) await host.directory.refresh().catch(error => { refusals.push(error); });
        expect(host.published()).toBe(false);
        expect(refusals).toHaveLength(3);
        for (const error of refusals) {
          expect(error instanceof HostLeaseLockBusyError || error instanceof participantFiles.ParticipantFileLockBusyError,
            String(error)).toBe(true);
        }
        // Whatever the namespace receipt did first, the last refusal is the unrecoverable additions gate.
        expect(refusals.at(-1)).toBeInstanceOf(HostLeaseLockBusyError);
      } else {
        const { hub, bridge } = await laterMirrorPass(base, root, farId);
        await expect(bridge.start()).rejects.toThrow(/FABRIC_MESH_LOCK_TIMEOUT: .*host-lease-commits[\\/]5f43c55dca0091e6[\\/]custody\.lock/);
        expect(readHostLeaseCurrent(root, farId)).toBeUndefined();
        expect(hub.listAll(PREFIX).some(entry => (entry.value as { id?: unknown }).id === farId)).toBe(false);
      }
      // No takeover: the killed mirror's receipt still owns the gate, and no recovery fence was made.
      expect(fs.readFileSync(path.join(killed.additions, "custody.lock", "owner"), "utf8")).toBe(killed.additionsOwner);
      expect(fs.readdirSync(killed.additions).filter(name => name.startsWith("custody.lock.dead."))).toEqual([]);
    }, 40_000);
});
