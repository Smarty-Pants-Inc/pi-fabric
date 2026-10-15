// Keep: #12785 native authority boundary and the Windows missing-birth/PID-reuse regression.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { assertNativeObserver } from "../src/actors/launcher-lead.js";
import { registerMainInbox } from "../src/topology/stall-alarms.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import type { MeshIdentity } from "../src/mesh/store.js";

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const roots: string[] = [];
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
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
