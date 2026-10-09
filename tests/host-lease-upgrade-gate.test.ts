import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LEASE_FORMAT_UUID_MIN } from "../src/agents/worker-protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { FabricPreUuidProcessAliveError, scanPreUuidFabricProcesses } from "../src/topology/host-lease-upgrade-gate.js";
import { commitHostLeaseUnderCustody, hostLeasePath, readHostLeaseCurrent, renewHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const realOpendir = fs.opendirSync, realOpen = fs.openSync, realRead = fs.readFileSync, realStat = fs.statSync, realExists = fs.existsSync;
const pid = 43210;
let base: string, procRoot: string, mesh: MeshStore, lease: FabricHostLease;
const redirect = (file: fs.PathLike): fs.PathLike => typeof file === "string" && /^\/proc\/\d+(?:\/|$)/.test(file)
  ? path.join(procRoot, file.slice("/proc/".length)) : file;
const markers = () => fs.existsSync(path.join(mesh.root, "host-leases"))
  ? fs.readdirSync(path.join(mesh.root, "host-leases")).filter(name => name.startsWith(".uuid-format-")) : [];
beforeEach(() => {
  base = fs.mkdtempSync(path.join(os.tmpdir(), "host-uuid-gate-"));
  procRoot = path.join(base, "proc"); fs.mkdirSync(procRoot);
  mesh = new MeshStore(path.join(base, "mesh"), 65_536, 100);
  lease = { id: "upgrade-host", rootId: "upgrade-root", identityId: "upgrade-owner", incarnationToken: randomUUID(),
    startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 };
  vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
    if (String(args[0]) === "/etc/machine-id") return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n";
    if (String(args[0]) === "/proc/sys/kernel/random/boot_id") return "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n";
    return realRead(...args);
  });
  vi.spyOn(fs, "opendirSync").mockImplementation((...args: Parameters<typeof fs.opendirSync>) =>
    realOpendir(String(args[0]) === "/proc" ? procRoot : args[0], args[1]));
  vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => realOpen(redirect(args[0]), args[1], args[2]));
  vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => realStat(redirect(args[0]), args[1]));
  vi.spyOn(fs, "existsSync").mockImplementation(file => realExists(redirect(file)));
});
afterEach(() => { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(base, { recursive: true, force: true }); });
const fakeProcess = (format?: number, evidence: "environ" | "cmdline" | "maps" | "record" = "environ", state = "S") => {
  const release = path.join(base, "fabric", "releases", "a".repeat(40));
  fs.mkdirSync(path.join(release, "dist"), { recursive: true });
  fs.writeFileSync(path.join(release, "dist", "worker-protocol.json"), JSON.stringify({ version: 1, ...(format === undefined ? {} : { leaseFormat: format }) }));
  const directory = path.join(procRoot, String(pid)); fs.mkdirSync(directory);
  fs.writeFileSync(path.join(directory, "stat"), `${pid} (pi) ${[state, "1", ...Array(17).fill("0"), "12345", "0"].join(" ")}\n`);
  const profile = path.join(base, "profile");
  fs.writeFileSync(path.join(directory, "environ"), `PI_CODING_AGENT_DIR=${profile}\0${evidence === "environ" ? `PI_FABRIC_RELEASE_ROOT=${release}\0` : ""}`);
  fs.writeFileSync(path.join(directory, "cmdline"), evidence === "cmdline" ? `/usr/bin/node\0${release}/dist/worker.js\0` : "/usr/bin/node\0/example/pi-runtime/cli.js\0");
  fs.writeFileSync(path.join(directory, "maps"), evidence === "maps" ? `0000-1000 r-xp 0000 00:00 1 ${release}/dist/index.js\n` : "");
  if (evidence === "record") {
    const records = path.join(profile, "fabric", "release-processes"); fs.mkdirSync(records, { recursive: true });
    fs.writeFileSync(path.join(records, `${pid}.json`), JSON.stringify({ pid, start: "12345", loadedRoot: release }));
  }
  return { directory, release, profile };
};

describe("first UUID claim mechanical upgrade gate", () => {
  it.each(["environ", "cmdline", "maps", "record"] as const)("refuses a pre-UUID process from %s and lists its PID", async evidence => {
    fakeProcess(undefined, evidence);
    const error = await renewHostLease(mesh, lease, { claim: true }).catch(error => error);
    expect(error).toBeInstanceOf(FabricPreUuidProcessAliveError);
    expect(error).toMatchObject({ code: "FABRIC_PRE_UUID_PROCESS_ALIVE", pids: [pid], retryable: false });
    expect(error.message).toContain("pre-UUID Fabric process alive");
    expect(error.processes[0].reason).toContain("pre-UUID release");
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toBeUndefined(); expect(markers()).toEqual([]);
  });
  it("refuses a SIGSTOP-paused old process even with an expired legacy lease", async () => {
    fakeProcess(undefined, "environ", "T");
    const legacy = { ...lease, expiresAt: Date.now() - 1 }; delete legacy.incarnationToken;
    writeHostLease(mesh.root, legacy);
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toEqual(legacy); expect(markers()).toEqual([]);
  });
  it("a live pre-UUID process refuses before any fresh-legacy TTL wait", async () => {
    fakeProcess(undefined);
    const legacy = { ...lease, expiresAt: Date.now() + 120_000 }; delete legacy.incarnationToken;
    writeHostLease(mesh.root, legacy);
    await expect(renewHostLease(mesh, lease, { claim: true, timeoutMs: 0 })).rejects.toMatchObject({
      code: "FABRIC_PRE_UUID_PROCESS_ALIVE", pids: [pid] });
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toEqual(legacy); expect(markers()).toEqual([]);
  });
  it("no Fabric process: writes the lease and first-claim marker", async () => {
    await renewHostLease(mesh, lease, { claim: true });
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toEqual(lease); expect(markers()).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "host-leases", markers()[0]!), "utf8"))).toMatchObject({
      leaseFormat: LEASE_FORMAT_UUID_MIN, hostId: lease.id, incarnationToken: lease.incarnationToken });
  });
  it("a known UUID release does not prevent the first claim", async () => {
    fakeProcess(LEASE_FORMAT_UUID_MIN);
    await renewHostLease(mesh, lease, { claim: true }); expect(markers()).toHaveLength(1);
  });
  it("later claims on the same physical host skip the scan (even another host label)", async () => {
    await renewHostLease(mesh, lease, { claim: true }); fakeProcess(undefined);
    const reader = vi.spyOn(fs, "opendirSync"); reader.mockClear();
    await renewHostLease(mesh, { ...lease, id: "another-host-label", incarnationToken: randomUUID() }, { claim: true });
    expect(reader).not.toHaveBeenCalled(); expect(markers()).toHaveLength(1);
  });
  it("unreadable process refuses with its PID", async () => {
    const { directory } = fakeProcess(LEASE_FORMAT_UUID_MIN);
    vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => {
      if (String(redirect(args[0])) === path.join(directory, "environ")) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return realOpen(redirect(args[0]), args[1], args[2]);
    });
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toBeUndefined(); expect(markers()).toEqual([]);
  });
  it("unreadable process directory is unknown, never mistaken for exit", async () => {
    const { directory } = fakeProcess(LEASE_FORMAT_UUID_MIN);
    vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => {
      if (String(redirect(args[0])) === directory) throw Object.assign(new Error("directory denied"), { code: "EACCES" });
      return realStat(redirect(args[0]), args[1]);
    });
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
    expect(markers()).toEqual([]);
  });
  it("unknown Pi release refuses, not an active-pin guess", async () => {
    const { directory } = fakeProcess(LEASE_FORMAT_UUID_MIN);
    fs.writeFileSync(path.join(directory, "environ"), "");
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
    expect(markers()).toEqual([]);
  });
  it("stale PID runtime record cannot admit unknown code", async () => {
    const { profile } = fakeProcess(LEASE_FORMAT_UUID_MIN, "record");
    fs.writeFileSync(path.join(profile, "fabric", "release-processes", `${pid}.json`), JSON.stringify({ pid, start: "old", loadedRoot: base }));
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
  });
  it("unreadable release manifest refuses with its PID", async () => {
    const { release } = fakeProcess(LEASE_FORMAT_UUID_MIN);
    fs.unlinkSync(path.join(release, "dist", "worker-protocol.json"));
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
  });
  it("zombies and other UIDs are not live same-UID Fabric writers", async () => {
    const { directory } = fakeProcess(undefined, "environ", "Z");
    expect(scanPreUuidFabricProcesses(procRoot)).toEqual([]);
    fs.writeFileSync(path.join(directory, "stat"), `${pid} (pi) S 1 ${Array(17).fill("0").join(" ")} 12345`);
    vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => {
      const result = realStat(redirect(args[0]), args[1]);
      return result && String(redirect(args[0])) === directory ? Object.assign(result, { uid: (process.getuid?.() ?? 0) + 1 }) : result;
    });
    expect(scanPreUuidFabricProcesses(procRoot)).toEqual([]);
  });
  it("an oversized process observation fails closed within the byte bound", async () => {
    const { directory } = fakeProcess(LEASE_FORMAT_UUID_MIN);
    fs.writeFileSync(path.join(directory, "environ"), "x".repeat(1024 * 1024 + 2));
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toMatchObject({ pids: [pid] });
  });
  it("retained reload admission refuses BEFORE a shared commit and stops retry scans", async () => {
    fakeProcess(undefined);
    const identity = { id: lease.id, name: "upgrade Main", kind: "main" as const };
    const predecessor = { ...lease, identityId: identity.id, rootId: identity.id, reloadUntil: lease.expiresAt };
    writeHostLease(mesh.root, predecessor);
    const key = "topology/hosts/" + createHash("sha256").update(identity.id).digest("hex");
    await mesh.put({ key, identity, value: { format: 1, id: identity.id, rootId: identity.id, identity,
      incarnationToken: predecessor.incarnationToken, startedAt: predecessor.startedAt,
      updatedAt: predecessor.updatedAt, expiresAt: predecessor.expiresAt } });
    const before = mesh.listAll("", { fresh: true });
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity, reapDeadHosts: false, heartbeatMs: 60_000 });
    try {
      const error = await directory.start().catch(error => error);
      expect(error).toMatchObject({ code: "FABRIC_PRE_UUID_PROCESS_ALIVE", pids: [pid] });
      expect(mesh.listAll("", { fresh: true })).toEqual(before);
      expect(readHostLeaseCurrent(mesh.root, lease.id)).toEqual(predecessor); expect(markers()).toEqual([]);
      expect(directory.canConsumeMesh()).toBe(false);
      const reader = vi.spyOn(fs, "opendirSync"); reader.mockClear();
      await expect(directory.refresh()).rejects.toBe(error); expect(reader).not.toHaveBeenCalled();
    } finally { await directory.close(); }
  });
  it("retained reload rechecks a newly observed old process at the FINAL shared commit boundary", async () => {
    const identity = { id: lease.id, name: "upgrade Main", kind: "main" as const };
    const predecessor = { ...lease, identityId: identity.id, rootId: identity.id, reloadUntil: lease.expiresAt };
    writeHostLease(mesh.root, predecessor);
    const key = "topology/hosts/" + createHash("sha256").update(identity.id).digest("hex");
    await mesh.put({ key, identity, value: { format: 1, id: identity.id, rootId: identity.id, identity,
      incarnationToken: predecessor.incarnationToken, startedAt: predecessor.startedAt,
      updatedAt: predecessor.updatedAt, expiresAt: predecessor.expiresAt } });
    const before = mesh.listAll("", { fresh: true }), writeBatch = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(input => writeBatch({ ...input,
      prepare: view => { const ops = input.prepare?.(view) ?? []; fakeProcess(undefined); return ops; } }));
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity, reapDeadHosts: false, heartbeatMs: 60_000 });
    try {
      await expect(directory.start()).rejects.toMatchObject({ code: "FABRIC_PRE_UUID_PROCESS_ALIVE", pids: [pid] });
      expect(mesh.listAll("", { fresh: true })).toEqual(before);
      expect(readHostLeaseCurrent(mesh.root, lease.id)).toEqual(predecessor); expect(markers()).toEqual([]);
      expect(directory.canConsumeMesh()).toBe(false);
    } finally { await directory.close(); }
  });
  it("retained reload with no old process still rotates ONLY after shared success", async () => {
    const identity = { id: lease.id, name: "upgrade Main", kind: "main" as const };
    const predecessor = { ...lease, identityId: identity.id, rootId: identity.id, reloadUntil: lease.expiresAt };
    writeHostLease(mesh.root, predecessor);
    const key = "topology/hosts/" + createHash("sha256").update(identity.id).digest("hex");
    await mesh.put({ key, identity, value: { format: 1, id: identity.id, rootId: identity.id, identity,
      incarnationToken: predecessor.incarnationToken, startedAt: predecessor.startedAt,
      updatedAt: predecessor.updatedAt, expiresAt: predecessor.expiresAt } });
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity, reapDeadHosts: false, heartbeatMs: 60_000 });
    try {
      await directory.start();
      const host = mesh.get(key, { fresh: true })!.value as { incarnationToken: string };
      expect(host.incarnationToken).not.toBe(predecessor.incarnationToken);
      expect(readHostLeaseCurrent(mesh.root, lease.id)?.incarnationToken).toBe(host.incarnationToken);
      expect(markers()).toHaveLength(1);
    } finally { await directory.close(); }
  });
  it("a forged admission callback cannot bypass a first-claim scan", async () => {
    await expect(mesh.leaseCustody(hostLeasePath(mesh.root, lease.id), () =>
      commitHostLeaseUnderCustody(mesh.root, lease, { claim: true, uuidAdmission: () => undefined })))
      .rejects.toThrow("census admission receipt");
    expect(readHostLeaseCurrent(mesh.root, lease.id)).toBeUndefined(); expect(markers()).toEqual([]);
  });
  it("a failed lease write must not create a successful-claim marker", async () => {
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, ...args) => {
      if (String(file).includes("host-leases") && !String(file).includes(".uuid-format-")) throw new Error("lease write failed");
      return write(file, ...args);
    });
    await expect(renewHostLease(mesh, lease, { claim: true })).rejects.toThrow("lease write failed"); expect(markers()).toEqual([]);
  });
});
