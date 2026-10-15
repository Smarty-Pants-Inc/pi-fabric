// Keep: #12785 native authority boundary and the Windows missing-birth/PID-reuse regression.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { assertNativeObserver } from "../src/actors/launcher-lead.js";
import { registerMainInbox, recordMainSuccessor, stageMainSuccessor, confirmMainSuccessor,
  registeredMainPredecessors } from "../src/topology/stall-alarms.js";
import { MeshStore } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import type { MeshIdentity } from "../src/mesh/store.js";

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const roots: string[] = [];
const registrationPlatforms = process.platform === "linux" ? ["linux", "win32"] as const : ["win32"] as const;
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = (osName: NodeJS.Platform) => {
  Object.defineProperty(process, "platform", { ...platform, value: osName });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "launcher-lead-platform-")); roots.push(root);
  const identity: MeshIdentity = { id: "session:native", sessionId: "native", kind: "main", name: "native Main" };
  registerMainInbox(root, identity, "native", undefined, "trusted-launcher", "trusted-project");
  writeHostLease(root, { id: identity.id, rootId: identity.id, identityId: identity.id, updatedAt: Date.now(),
    expiresAt: Date.now() + 60_000, incarnationToken: "lease-incarnation",
    writer: { pid: process.pid, host: os.hostname(), startedAt: Date.now(), releaseSha: "test", lockProtocol: 1, stateBackend: "file" } });
  const file = path.join(root, "main-followups", "native.owner.json");
  const owner = () => JSON.parse(fs.readFileSync(file, "utf8"));
  const save = (value: unknown) => fs.writeFileSync(file, JSON.stringify(value));
  const row = { rootId: "session:old", project: "trusted-project" };
  return { root, identity, owner, save, row };
};

describe("authenticated launcher identity (not a reusable environment label)", () => {
  const predecessor = (f: ReturnType<typeof setup>) => {
    const identity: MeshIdentity = { id: "session:old", sessionId: "old", kind: "main", name: "native Main" };
    registerMainInbox(f.root, identity, "old", path.join(f.root, "old.jsonl"), "trusted-launcher", "trusted-project");
    return { identity, file: path.join(f.root, "main-followups", "old.owner.json"), mesh: new MeshStore(f.root, 65536, 100) };
  };

  it.each(registrationPlatforms)("refuses independent same-label roots and same-root replacement incarnations on %s", async osName => {
    const f = setup(osName), old = predecessor(f);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
    await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).not.toThrow();
    const first = f.owner().nativeLauncherIdentity;
    registerMainInbox(f.root, f.identity, "native", undefined, "trusted-launcher", "trusted-project");
    expect(f.owner().nativeLauncherIdentity.activationId).not.toBe(first.activationId);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
  });

  it.each(registrationPlatforms)("native succession binds the original root/activation/PID/birth and ignores labels on %s", async osName => {
    const f = setup(osName), old = predecessor(f);
    const original = JSON.parse(fs.readFileSync(old.file, "utf8")).nativeLauncherIdentity;
    await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    expect(f.owner().nativeLauncherIdentity).toMatchObject({ rootId: f.identity.id, activationId: f.owner().activationId });
    expect(registeredMainPredecessors(f.root, f.identity.id)).toContainEqual(original);
    expect(original).toMatchObject({ rootId: "session:old", activationId: expect.any(String), pid: process.pid, host: os.hostname() });
    if (osName === "linux") expect(original.processStartedAt).toMatch(/^\d+$/);
    const before = assertNativeObserver(f.root, f.identity, "native", f.row, true);
    f.save({ ...f.owner(), nativeLauncher: "\nreused display-only label", name: "another display" });
    expect(assertNativeObserver(f.root, f.identity, "native", f.row, true)).toBe(before);
  });

  it.each(registrationPlatforms)("legacy missing identity never acquires a retirement grant through native succession on %s", async osName => {
    const f = setup(osName), old = predecessor(f);
    const owner = JSON.parse(fs.readFileSync(old.file, "utf8"));
    delete owner.nativeLauncherIdentity;
    fs.writeFileSync(old.file, JSON.stringify(owner));
    await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).not.toThrow();
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
  });

  it.each(registrationPlatforms)("authenticates each receiver's own identity across multiple native successors on %s", async osName => {
    const f = setup(osName), old = predecessor(f);
    await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    const next: MeshIdentity = { id: "session:next", sessionId: "next", kind: "main", name: "native Main" };
    registerMainInbox(f.root, next, "next", undefined, "trusted-launcher", "trusted-project");
    writeHostLease(f.root, { id: next.id, rootId: next.id, identityId: next.id, updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
      writer: { pid: process.pid, host: os.hostname(), startedAt: Date.now(), releaseSha: "test", lockProtocol: 1, stateBackend: "file" } });
    await recordMainSuccessor(old.mesh, f.identity.id, "native", next.id);
    expect(() => assertNativeObserver(f.root, next, "next", f.row, true)).not.toThrow();
    expect(() => assertNativeObserver(f.root, next, "next", { ...f.row, rootId: f.identity.id }, true)).not.toThrow();
  });

  it.each(["record", "stage"] as const)("%s cannot authenticate a source reconstructed from disk-only tokens", async mode => {
    const f = setup("win32"), old = predecessor(f), target = path.join(f.root, "native.jsonl");
    const owner = JSON.parse(fs.readFileSync(old.file, "utf8"));
    owner.activationId = owner.nativeLauncherIdentity.activationId = "disk-only-source-token";
    fs.writeFileSync(old.file, JSON.stringify(owner));
    if (mode === "record") await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    else {
      await stageMainSuccessor(old.mesh, old.identity.id, "old", target);
      const recorded: string[] = [], unlink = fs.unlinkSync.bind(fs);
      vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
        if (String(file).includes(`${path.sep}rotations${path.sep}`)) { recorded.push(String(file)); return; }
        unlink(file);
      });
      expect(await confirmMainSuccessor(old.mesh, f.identity.id, target)).toBe(true);
      expect(recorded).toHaveLength(1);
    }
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).not.toThrow();
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
  });

  it.skipIf(process.platform !== "linux")("a staged native replacement carries the original authenticated identity; intent deletion is recorded only", async () => {
    const f = setup("linux"), old = predecessor(f), target = path.join(f.root, "native.jsonl");
    await stageMainSuccessor(old.mesh, old.identity.id, "old", target);
    // Re-register the target as a fresh runtime would; it must not self-infer the old identity.
    registerMainInbox(f.root, f.identity, "native", target, "other-label", "trusted-project");
    const recorded: string[] = [], unlink = fs.unlinkSync.bind(fs);
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (String(file).includes(`${path.sep}rotations${path.sep}`)) { recorded.push(String(file)); return; }
      unlink(file);
    });
    expect(await confirmMainSuccessor(old.mesh, f.identity.id, target)).toBe(true);
    expect(recorded).toHaveLength(1);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).not.toThrow();
  });

  it.skipIf(process.platform !== "linux").each([true, false])("staged succession across module replacement requires fresh process-local registration (%s)", async registered => {
    const f = setup("linux"), old = predecessor(f), target = path.join(f.root, "replacement.jsonl");
    await stageMainSuccessor(old.mesh, old.identity.id, "old", target);
    vi.resetModules();
    const next = await import("../src/topology/stall-alarms.js");
    const { assertNativeObserver: assertNext } = await import("../src/actors/launcher-lead.js");
    if (registered) next.registerMainInbox(f.root, f.identity, "native", target, "trusted-launcher", "trusted-project");
    const unlink = fs.unlinkSync.bind(fs), recorded: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (String(file).includes(`${path.sep}rotations${path.sep}`)) { recorded.push(String(file)); return; }
      unlink(file);
    });
    expect(await next.confirmMainSuccessor(old.mesh, f.identity.id, target)).toBe(true);
    expect(recorded).toHaveLength(1);
    if (registered) expect(() => assertNext(f.root, f.identity, "native", f.row, true)).not.toThrow();
    else expect(() => assertNext(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
  });

  it.skipIf(process.platform !== "linux")("cannot reconstruct current registration or inherit an identity from disk-only activation bytes on Linux", async () => {
    const f = setup("linux"), old = predecessor(f);
    await recordMainSuccessor(old.mesh, old.identity.id, "old", f.identity.id);
    const otherRoot = fs.mkdtempSync(path.join(os.tmpdir(), "launcher-lead-disk-only-")); roots.push(otherRoot);
    fs.cpSync(f.root, otherRoot, { recursive: true });
    expect(() => assertNativeObserver(otherRoot, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
    const owner = f.owner();
    f.save({ ...owner, activationId: "disk-only-token" });
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, true)).toThrow(/trusted native launcher/);
  });
});

describe("native observer birth evidence across platforms", () => {
  it("accepts receiver registration without /proc birth on Windows, but rejects malformed supplied evidence", () => {
    const f = setup("win32");
    expect(f.owner()).not.toHaveProperty("processStartedAt");
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).not.toThrow();
    f.save({ ...f.owner(), processStartedAt: "not-a-birth" });
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).toThrow(/live native project root/);
  });

  it("refuses a disk-only activation shaped like a reused current PID on Windows", () => {
    const f = setup("win32");
    f.save({ ...f.owner(), activationId: "disk-only-random-activation" });
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).toThrow(/live native project root/);
  });

  it("refuses a replaced token and a registration token bound to a different root", () => {
    const f = setup("win32");
    const registered = f.owner().activationId;
    f.save({ ...f.owner(), activationId: "replaced-token" });
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).toThrow(/live native project root/);
    f.save({ ...f.owner(), activationId: registered });
    const otherRoot = fs.mkdtempSync(path.join(os.tmpdir(), "launcher-lead-other-root-")); roots.push(otherRoot);
    fs.cpSync(f.root, otherRoot, { recursive: true });
    expect(() => assertNativeObserver(otherRoot, f.identity, "native", f.row, false)).toThrow(/live native project root/);
  });

  it("preserves the existing supplied-birth path on Windows", () => {
    const f = setup("win32");
    f.save({ ...f.owner(), processStartedAt: "1" });
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).not.toThrow();
  });

  it.skipIf(process.platform !== "linux")("accepts a registered Linux process with a valid birth identity", () => {
    const f = setup("linux");
    expect(f.owner().processStartedAt).toMatch(/^\d+$/);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).not.toThrow();
  });

  it.skipIf(process.platform !== "linux").each([undefined, "not-a-birth"] as const)("keeps Linux missing/invalid birth evidence fail-closed (%s)", evidence => {
    const f = setup("linux");
    const owner = f.owner();
    if (evidence === undefined) delete owner.processStartedAt;
    else owner.processStartedAt = evidence;
    f.save(owner);
    expect(() => assertNativeObserver(f.root, f.identity, "native", f.row, false)).toThrow(/live native project root/);
  });
});
