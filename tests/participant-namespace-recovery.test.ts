import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { HostLeaseLockBusyError, hostLeaseAdditionsGatePath } from "../src/topology/host-lease-lock.js";
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
/** Two distinct valid start identities per platform: the receipt's, and the one the reused PID has now. */
const STARTS = {
  linux: ["4242", "4243"],
  win32: ["win32:639264528000000000", "win32:639264528000000001"],
  darwin: ["darwin:Thu Oct  1 12:00:00 2026", "darwin:Fri Oct  2 12:00:00 2026"],
} as const;
const START = STARTS[process.platform as keyof typeof STARTS] ?? STARTS.linux;

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

const plantNamespaceReceipt = (root: string, pid: number, start: string = START[0], token = "dead-namespace-holder"): string => {
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
// custody, reports, and is SIGKILLed mid-pass (an OOM kill), leaving both receipts behind. With machine "none" the
// child sees no machine or boot identity either (as on Windows; a no-op there), so its gate record is the one a
// Windows or macOS mirror writes: this host's label instead of a machine id (src/mesh/custody-lock.ts).
const MIRROR = String.raw`
  import { createJiti } from "jiti";
  import { pathToFileURL } from "node:url";
  import fs from "node:fs";
  const [root, ready, id, machine] = process.argv.slice(1);
  if (machine === "none") {
    const read = fs.readFileSync;
    fs.readFileSync = (file, ...rest) => {
      if (file === "/etc/machine-id" || file === "/proc/sys/kernel/random/boot_id") {
        throw Object.assign(new Error("no machine or boot identity (as on Windows)"), { code: "ENOENT" });
      }
      return read.call(fs, file, ...rest);
    };
  }
  const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
  const { MeshStore } = await jiti.import("./src/mesh/store.ts");
  const { withParticipantFileDirectoryCustody } = await jiti.import("./src/topology/participant-files.ts");
  const { withHostLeaseDirectoryCustody } = await jiti.import("./src/topology/host-leases.ts");
  const store = new MeshStore(root, 65536, 1000);
  const ids = new Set([id]);
  setInterval(() => {}, 1000);
  await withParticipantFileDirectoryCustody(store, () => withHostLeaseDirectoryCustody(store, async () => {
    fs.writeFileSync(ready, String(process.pid));
    await new Promise(() => {});
  }, ids), ids);
`;

const killedMirror = async (root: string, id: string, machine: Machine = "native"):
  Promise<{ pid: number; namespace: string; additions: string; additionsOwner: string }> => {
  fs.mkdirSync(root, { recursive: true });
  const ready = path.join(path.dirname(root), "mirror-ready-" + digest(root).slice(0, 8));
  const child = spawn(process.execPath, ["--input-type=module", "-e", MIRROR, root, ready, id, machine],
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

// The host-lease additions gate is a HOST-QUALIFIED custody lock (MeshStore.leaseCustody, src/mesh/store.ts). With this
// machine's identity (/etc/machine-id and the boot id, src/core/atomic-write.ts) its dead holder is recovered on machine,
// boot and process evidence. Windows and macOS have no machine id, and there the gate stayed busy for good after its
// holder died, blocking every new host lease on the mesh (pi-fabric#1562 review/astra P1; Windows CI). Now
// (src/mesh/custody-lock.ts clearDeadCustodyLock) such a record names this host instead, and is recovered on the
// participant locks' process proof: its PID is gone, or is now another process (another native incarnation).
type Machine = "native" | "none";
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

/** The additions gate's custody lock: the 16-hex domain of ".additions" (the test's mesh has no legacy full-hash one). */
const additionsGate = (root: string): string => path.join(root, "host-lease-commits", digest(".additions").slice(0, 16), "custody.lock");
const deadFences = (lock: string): string[] => fs.readdirSync(path.dirname(lock))
  .filter(name => name.startsWith(path.basename(lock) + ".dead."))
  .map(name => fs.readFileSync(path.join(path.dirname(lock), name, "owner"), "utf8"));

describe("a mirror killed mid-pass while holding the namespace receipt and the additions gate (pi-fabric#1562 review P1)", () => {
  const cases = (machine: Machine) => (["dead", "reused"] as const)
    .flatMap(pid => (["fenced add", "mirror pass"] as const).map(after => ({ machine, pid, after })));
  const recovers = async ({ machine, pid: holder, after }: ReturnType<typeof cases>[number]) => {
    const base = scratch();
    const root = path.join(base, "hub");
    const farId = "session:far-native";
    const killed = await killedMirror(root, farId, machine);
    expect(namespaceOwner(root)).toBe(killed.namespace);
    // The gate record: the killed mirror's own native incarnation, and this machine's id or (with none) this host's label.
    const gate = killed.additionsOwner.split("\n");
    expect(gate[1]).toBe(String(killed.pid));
    expect(atomic.validProcessIncarnation(gate[3])).toBe(true);
    if (machine === "native") expect(gate[4]).toBe(atomic.readPhysicalMachineId());
    else {
      expect(gate[4]).toMatch(/^host:./);
      withoutMachineEvidence();
    }
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
    // The killed mirror's gate was recovered: renamed aside to its identity-named fence, never deleted.
    expect(deadFences(additionsGate(root))).toEqual([killed.additionsOwner]);
  };
  // This machine's identity: skipped where there is none (Windows and macOS run the cases below natively).
  it.skipIf(!nativeMachineEvidence).each(cases("native"))("$pid PID: a later $after recovers", recovers, 40_000);
  // No machine identity: natively on Windows and macOS; simulated here for the killed mirror and the later process alike.
  // Before pi-fabric#1562's fix these four stayed busy (Windows CI: MeshLockTimeoutError on
  // host-lease-commits/5f43c55dca0091e6/custody.lock, and HostLeaseLockBusyError for the fenced add).
  it.each(cases("none"))("$pid PID, no machine evidence (as on Windows): a later $after recovers", recovers, 40_000);
});

describe("the additions gate where there is no machine id: recovered only on process proof (pi-fabric#1562 review/astra P1)", () => {
  const DEAD = 999_999_999;
  type Scenario = { name: string; recovers: boolean; pid: "dead" | "live" | "denied"; native?: "other" | "same" | "unknown";
    /** The record's incarnation: "none" (an older unfenced session runtime's, or a failed native read) or "foreign"
     * (another platform's format: invalid here). Both still reach the PID probe; only ESRCH recovers them. */
    incarnation?: "none" | "foreign"; edit?: (fields: string[], starts: readonly string[]) => void };
  const scenarios: Scenario[] = [
    { name: "its PID is gone (ESRCH)", recovers: true, pid: "dead" },
    { name: "its PID is now another process (another native incarnation)", recovers: true, pid: "live", native: "other" },
    { name: "an unprobeable PID (EPERM) that is now another process", recovers: true, pid: "denied", native: "other" },
    { name: "a live holder (the same incarnation)", recovers: false, pid: "live", native: "same" },
    { name: "a live PID whose incarnation is unknown (unreadable or timed out)", recovers: false, pid: "live", native: "unknown" },
    { name: "a denied probe (EPERM) and an unknown incarnation", recovers: false, pid: "denied", native: "unknown" },
    // review-fleet-lead P1 on pi-fabric#1562: these two were kept busy for good, before the PID probe. ESRCH recovers
    // a record with no or an invalid incarnation, as holderAlive does; a live or EPERM PID keeps it.
    { name: "its PID is gone (ESRCH) and the record has no incarnation (a session runtime's)", recovers: true, pid: "dead", incarnation: "none" },
    { name: "a live PID and no incarnation in the record", recovers: false, pid: "live", incarnation: "none" },
    { name: "a denied probe (EPERM) and no incarnation in the record", recovers: false, pid: "denied", incarnation: "none" },
    { name: "its PID is gone (ESRCH) and the record has another platform's incarnation", recovers: true, pid: "dead", incarnation: "foreign" },
    { name: "a live PID and another platform's incarnation", recovers: false, pid: "live", incarnation: "foreign" },
    { name: "a dead PID, but another host", recovers: false, pid: "dead", edit: fields => { fields[4] = "host:another-host"; } },
    { name: "a dead PID, but an older build's record (no host label)", recovers: false, pid: "dead", edit: fields => { fields[4] = ""; } },
    { name: "a dead PID, but a holder that had a machine id", recovers: false, pid: "dead",
      edit: fields => { fields[4] = "0123456789abcdef0123456789abcdef"; } },
  ];
  const platforms = [...new Set([process.platform, "win32"] as const)].filter((platform): platform is keyof typeof STARTS => platform in STARTS);
  const table = platforms.flatMap(platform => scenarios.map(scenario => ({ ...scenario, platform,
    label: platform + (platform === process.platform ? "" : " (simulated)") + ": " + scenario.name + (scenario.recovers ? ": recovered" : ": kept busy") })));

  // One test per row: `it.each` would truncate these titles.
  for (const { label, platform, pid: kind, native, incarnation, edit, recovers } of table) it(label, async () => {
    const root = path.join(scratch(), "mesh");
    const mesh = new MeshStore(root, 65536, 1000);
    withoutMachineEvidence();
    if (platform !== process.platform) vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const starts = STARTS[platform];
    // What acquisition records here: this host's label in the machine slot, and the holder's incarnation.
    let fields: string[] = [];
    await mesh.leaseCustody(hostLeaseAdditionsGatePath(root), () => {
      fields = fs.readFileSync(path.join(additionsGate(root), "owner"), "utf8").split("\n");
    }, 0, { ownIncarnation: starts[0] });
    expect(fields).toHaveLength(7);
    expect(fields[3]).toBe(starts[0]);
    expect(fields[4]).toMatch(/^host:./);
    expect(fields[5]).toBe("");
    // A killed holder's receipt: the same record with its pid (and the scenario's edit).
    const pid = kind === "live" ? await standIn() : DEAD;
    fields = [...fields];
    fields[0] = "killed-holder";
    fields[1] = String(pid);
    if (incarnation === "none") fields[3] = "";
    if (incarnation === "foreign") fields[3] = starts === STARTS.linux ? STARTS.win32[0] : STARTS.linux[0];
    edit?.(fields, starts);
    const lock = additionsGate(root);
    fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
    const owner = fields.join("\n");
    fs.writeFileSync(path.join(lock, "owner"), owner, { mode: 0o600 });
    const reads: number[] = [];
    vi.spyOn(atomic, "processIncarnation").mockImplementation(async (target: number) => {
      reads.push(target);
      return native === "same" ? starts[0] : native === "other" ? starts[1] : undefined;
    });
    const kill = process.kill.bind(process);
    const probes = vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
      if (kind === "denied" && target === pid && (signal === 0 || signal === undefined)) {
        throw Object.assign(new Error("kill EPERM"), { code: "EPERM" });
      }
      return kill(target, signal);
    }) as typeof process.kill);
    const entered = vi.fn(() => "entered");
    const attempt = mesh.leaseCustody(hostLeaseAdditionsGatePath(root), entered, recovers ? 2_000 : 300, { ownIncarnation: undefined });
    if (recovers) {
      await expect(attempt).resolves.toBe("entered");
      expect(deadFences(lock)).toEqual([owner]);
    } else {
      await expect(attempt).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(entered).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(deadFences(lock)).toEqual([]);
    }
    // A refused record is judged before any probe of its PID; a gone PID needs no native read, and a record with no or an
    // invalid incarnation is judged on the PID probe alone (nothing to compare a native read with).
    if (edit) expect(probes.mock.calls.filter(([target]) => target === pid)).toEqual([]);
    if (incarnation) expect(probes.mock.calls.some(([target, signal]) => target === pid && signal === 0)).toBe(true);
    if (kind === "dead" || incarnation) expect(reads).toEqual([]);
    else expect(new Set(reads)).toEqual(new Set([pid]));
  }, 20_000);
});

// review-fleet-lead P1 on pi-fabric#1562: every session (and actor) start takes the mesh-wide additions gate through a
// ParticipantDirectory with NO publication fence (fabric-runtime-state.ts). Such a directory never waits on a native read
// for its lease; its lease locks passed ownIncarnation: undefined (participant-directory.ts), so its gate record had none.
// Off Linux that record was never recovered, so every later lease creation and mirror pass stayed busy. The child builds
// exactly that directory and is SIGKILLed while its first heartbeat holds the gate (a kill during a session start).
// Its process has already read its own incarnation (as participant-file writes or the mesh lock do). "recorded": that
// read succeeded, and the directory now passes it, so the record has it. "none": the read failed (a PowerShell read past
// its 2 s budget), so the record has none, as every unfenced record had before; ESRCH alone recovers it.
// The read is settled in every loaded instance of atomic-write (pi-fabric#1562 review/astra P1-1). jiti keys a module by
// the path it resolved, and on Windows it resolves the sources' "../core/atomic-write.js" (no such file: Node's
// require.resolve of the .ts, D:\...\atomic-write.ts) and a direct "./src/core/atomic-write.ts" (D:/.../atomic-write.ts)
// to two keys: two instances, each with its own memoized read. The child settled only its direct import's, so the
// directory's own instance had none and the 'recorded' rows failed on Windows alone. A build has one instance.
const CREATOR = String.raw`
  import { createJiti } from "jiti";
  import { createRequire } from "node:module";
  import { pathToFileURL } from "node:url";
  import childProcess from "node:child_process";
  import fs from "node:fs";
  const [root, ready, id, machine, incarnation] = process.argv.slice(1);
  const read = fs.readFileSync;
  fs.readFileSync = (file, ...rest) => {
    if (machine === "none" && (file === "/etc/machine-id" || file === "/proc/sys/kernel/random/boot_id")) {
      throw Object.assign(new Error("no machine or boot identity (as on Windows)"), { code: "ENOENT" });
    }
    if (incarnation === "none" && file === "/proc/" + process.pid + "/stat") {
      throw Object.assign(new Error("own native identity unavailable"), { code: "EACCES" });
    }
    return read.call(fs, file, ...rest);
  };
  // darwin's ps and win32's powershell.exe incarnation reads (src/core/atomic-write.ts); every other command runs. "none"
  // fails them; "recorded" answers with a valid start, since a cold hosted PowerShell read can itself exceed its 2 s budget.
  const canned = { win32: "639264528000000000\r\n", darwin: "Thu Oct  1 12:00:00 2026\n" }[process.platform];
  const execFile = childProcess.execFile;
  childProcess.execFile = function (file, args, ...rest) {
    const ownRead = file === "/bin/ps" || (Array.isArray(args) && args.some(arg => String(arg).includes("Get-Process -Id")));
    if (ownRead && (incarnation === "none" || canned)) {
      const callback = rest.at(-1);
      setImmediate(() => incarnation === "none"
        ? callback(Object.assign(new Error("own native identity unavailable"), { code: "ETIMEDOUT" }))
        : callback(null, canned, ""));
      return undefined;
    }
    return execFile.call(this, file, args, ...rest);
  };
  const base = pathToFileURL(process.cwd() + "/index.js").href;
  const jiti = createJiti(base);
  const { MeshStore } = await jiti.import("./src/mesh/store.ts");
  const { ParticipantDirectory } = await jiti.import("./src/topology/participant-directory.ts");
  const { hostLeaseAdditionsGatePath } = await jiti.import("./src/topology/host-lease-lock.ts");
  // Every atomic-write instance jiti loaded, the directory's among them (see above): each settles its own read.
  const instances = Object.entries(createRequire(base).cache)
    .filter(([file]) => /[\\/]src[\\/]core[\\/]atomic-write\.ts$/.test(file)).map(([, module]) => module.exports);
  for (const instance of instances) await instance.ownProcessIncarnation();
  const store = new MeshStore(root, 65536, 1000);
  // Hold the additions gate once the creator's own work under it is done: report the custody options it was given
  // (the record's incarnation comes from them), then block this thread until the parent kills it.
  const leaseCustody = store.leaseCustody.bind(store);
  store.leaseCustody = (file, operation, timeoutMs, options) => leaseCustody(file, file !== hostLeaseAdditionsGatePath(store.root)
    ? operation : async () => {
      const value = await operation();
      fs.writeFileSync(ready, JSON.stringify({ pid: process.pid, passed: Object.hasOwn(options ?? {}, "ownIncarnation"),
        ownIncarnation: options?.ownIncarnation ?? null, instances: instances.length }));
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
      return value;
    }, timeoutMs, options);
  const identity = { id, name: "main", kind: "main", sessionId: id.slice("session:".length) };
  const directory = new ParticipantDirectory(store, { enabled: true, hostId: id, rootId: id, identity,
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false });
  setInterval(() => {}, 1000);
  await directory.refresh();
`;

const killedCreator = async (root: string, id: string, machine: Machine, incarnation: "recorded" | "none"):
  Promise<{ pid: number; passed: boolean; ownIncarnation: string | null; instances: number; additionsOwner: string }> => {
  fs.mkdirSync(root, { recursive: true });
  const ready = path.join(path.dirname(root), "creator-ready-" + digest(root).slice(0, 8));
  const child = spawn(process.execPath, ["--input-type=module", "-e", CREATOR, root, ready, id, machine, incarnation],
    { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"] });
  children.push(child);
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += chunk; });
  const exited = new Promise<void>(resolve => child.once("close", () => resolve()));
  let reported: { pid: number; passed: boolean; ownIncarnation: string | null; instances: number } | undefined;
  await vi.waitFor(() => {
    if (child.exitCode !== null) throw new Error("creator child exited early: " + stderr);
    reported = JSON.parse(fs.readFileSync(ready, "utf8")) as typeof reported;
  }, { timeout: 20_000, interval: 20 });
  const additionsOwner = fs.readFileSync(path.join(additionsGate(root), "owner"), "utf8");
  child.kill("SIGKILL");
  await exited;
  return { ...reported!, additionsOwner };
};

/** A later session start on the same mesh: its own non-fenced directory, as fabric-runtime-state.ts builds it. */
const sessionDirectory = async (root: string, name: string) => {
  const policy = new MeshStore(root, 65536, 100);
  await policy.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" },
    identity: { id: "policy", name: "policy", kind: "agent" } });
  const identity: MeshIdentity = { id: "session:" + name, name: "main", kind: "main", sessionId: name };
  const directory = new ParticipantDirectory(new MeshStore(root, 65536, 1000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000,
    reapDeadHosts: false,
  });
  cleanups.push(() => directory.close());
  directory.registerSource(() => [record(identity.id)]);
  const published = (): boolean => participantFiles.readParticipantFiles(root, { maxAgeMs: 0 })
    .some(entry => (entry.value as { id?: unknown }).id === identity.id);
  return { directory, identity, published };
};

describe("a session (no publication fence) killed while its start holds the additions gate (pi-fabric#1562 review-fleet-lead P1)", () => {
  const cases = (machine: Machine) => (["recorded", "none"] as const).flatMap(incarnation =>
    (["session start", "mirror pass"] as const).map(after => ({ machine, incarnation, after })));
  const recovers = async ({ machine, incarnation, after }: ReturnType<typeof cases>[number]) => {
    const base = scratch();
    const root = path.join(base, "hub");
    const killed = await killedCreator(root, "session:killed-creator", machine, incarnation);
    // The unfenced creator passes the incarnation its process already read, or none; custody never reads one for it.
    expect(killed.instances).toBeGreaterThanOrEqual(1);
    expect(killed.passed).toBe(true);
    const gate = killed.additionsOwner.split("\n");
    expect(gate[1]).toBe(String(killed.pid));
    if (incarnation === "recorded") {
      expect(atomic.validProcessIncarnation(killed.ownIncarnation ?? undefined)).toBe(true);
      expect(gate[3]).toBe(killed.ownIncarnation);
    } else {
      expect(killed.ownIncarnation).toBeNull();
      expect(gate[3]).toBe("");
    }
    if (machine === "native") expect(gate[4]).toBe(atomic.readPhysicalMachineId());
    else {
      expect(gate[4]).toMatch(/^host:./);
      withoutMachineEvidence();
    }
    if (after === "session start") {
      const later = await sessionDirectory(root, "after-kill");
      // ESRCH proves the killed creator gone: the next session's first heartbeat recovers the gate and publishes.
      await later.directory.refresh();
      expect(later.published()).toBe(true);
    } else {
      const farId = "session:far-native";
      const { hub, bridge } = await laterMirrorPass(base, root, farId);
      await bridge.start();
      expect(readHostLeaseCurrent(root, farId)).toMatchObject({ id: farId });
      expect(hub.listAll(PREFIX).some(entry => (entry.value as { id?: unknown }).id === farId)).toBe(true);
    }
    // The killed creator's gate was recovered: renamed aside to its identity-named fence, never deleted.
    expect(deadFences(additionsGate(root))).toEqual([killed.additionsOwner]);
  };
  it.skipIf(!nativeMachineEvidence).each(cases("native"))("incarnation $incarnation: a later $after recovers", recovers, 40_000);
  // Before the fix all four stayed busy for good off Linux: the record named this host but carried no incarnation.
  it.each(cases("none"))("no machine evidence (as on Windows), incarnation $incarnation: a later $after recovers", recovers, 40_000);
});
