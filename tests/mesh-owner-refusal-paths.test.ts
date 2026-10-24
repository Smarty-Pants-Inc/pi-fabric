import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockForeignHolderError, MeshLockTimeoutError, meshDeferredSummary } from "../src/core/atomic-write.js";
import { localFilesystemTestHooks, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";
import { RemoteBridgeSide, StoreBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { MeshFenceBusyError } from "../src/mesh/fence-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStateBusyError } from "../src/mesh/state-backend.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore } from "../src/mesh/store.js";

const TOKEN = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_TOKEN = "ABCDEF0123456789abcdef0123456789";
const OWN_NAMESPACE = "linux:0123456789abcdef0123456789abcdef:11111111-1111-4111-8111-111111111111:4026531836";
const FOREIGN_NAMESPACE = "linux:fedcba9876543210fedcba9876543210:22222222-2222-4222-8222-222222222222:4026532999";
const BAD_PID = "FENCE_PID_SECRET\r\x1b[31m;$(echo leak)";
const BAD_TOKEN = "RECOVERY_TOKEN_SECRET\r\x1b[31m;$(id)";
type Kind = "mesh" | "custody";
const roots: string[] = [];

// Existing namespace/reclaim seams make asynchronous refusal deterministic on every OS. These do not
// override SQLite's filesystem/private-root safety gates; every store below uses a real private root.
beforeEach(() => { pidNamespaceTestHooks.set(OWN_NAMESPACE); localFilesystemTestHooks.set(0xEF53); });
afterEach(() => {
  vi.restoreAllMocks();
  pidNamespaceTestHooks.reset();
  localFilesystemTestHooks.reset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owner-refusal-"));
  fs.chmodSync(root, 0o700);
  roots.push(root);
  return root;
};
const plantOwner = (root: string, kind: Kind, pid: string, token = TOKEN): { ownerPath: string; record: string; lock: string } => {
  const lock = path.join(root, kind === "mesh" ? ".lock" : "custody.lock");
  const record = `${token}\n${pid}\n${Date.now()}\n\n${FOREIGN_NAMESPACE}\n`;
  fs.mkdirSync(lock, { mode: 0o700 });
  const ownerPath = path.join(lock, "owner");
  fs.writeFileSync(ownerPath, record, { mode: 0o600 });
  return { ownerPath, record, lock };
};
const captureSync = (operation: () => unknown): unknown => {
  try { operation(); } catch (error) { return error; }
  throw new Error("unexpected success");
};
const captureAsync = async (operation: Promise<unknown>): Promise<unknown> => {
  try { await operation; } catch (error) { return error; }
  throw new Error("unexpected success");
};
const expectNoOwnerPayload = (text: string): void => {
  expect(text).not.toContain("FENCE_PID_SECRET");
  expect(text).not.toContain("RECOVERY_TOKEN_SECRET");
  expect(text).not.toContain("$(");
  expect(text).not.toContain("\r");
  expect(text).not.toContain("\x1b");
  // Do not reject a generic escaped "\\r": Windows JSON paths may contain \\runneradmin.
  expect(text).not.toContain("SECRET\\r");
  expect(text).not.toContain("\\u001b");
};

// This seam replays a production-created error at the bridge method boundary, not a claimed normal
// bridge read bypass of the state-backend wrapper. Both real encoder and real decoder run here.
const diagnosticSurfaces = async (error: MeshFenceBusyError): Promise<string[]> => {
  const store = new MeshStore(tempRoot(), 65_536, 100, { stateBackend: "file" });
  const side = new StoreBridgeSide(store, "owner-refusal");
  side.latestSequence = async () => { throw error; };
  const input = new PassThrough();
  const output = new PassThrough();
  let wire = "";
  output.on("data", chunk => { wire += String(chunk); });
  const serving = serveBridgeAgent(side, input, output);
  const remote = new RemoteBridgeSide(output, input, 1_000);
  const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const retry = new MeshBackgroundRetry("owner-refusal", 1, 1);
  try {
    const decoded = await captureAsync(remote.latestSequence());
    expect(decoded).toBeInstanceOf(Error);
    retry.failure(error);
    const logs = warning.mock.calls.map(call => call.map(String).join(" ")).join("\n");
    expect(logs).toContain("owner-refusal: mesh lock timeout");
    expect(logs).toContain("owner pid unreadable");
    expect(wire).toContain('"ok":false');
    expect((decoded as Error).message).toContain("Bridge agent:");
    expect(meshDeferredSummary(error)).toBe(error.message);
    return [error.holder, error.message, JSON.stringify(error), JSON.stringify(Object.entries(error)),
      meshDeferredSummary(error), logs, wire, (decoded as Error).message];
  } finally {
    retry.close();
    warning.mockRestore();
    remote.close();
    try { await serving; } finally {
      input.destroy();
      output.destroy();
      store.closeState();
    }
  }
};

describe("owner-file refusal producers (trust boundary and retained-cause regression)", () => {
  for (const kind of ["mesh", "custody"] as const) {
    it(`real synchronous SQLite ${kind} fence projects a secret/control PID on every diagnostic surface`, async () => {
      const root = tempRoot();
      const { ownerPath, record, lock } = plantOwner(root, kind, BAD_PID);
      const caught = captureSync(() => {
        // Close even if a regression unexpectedly admits this create.
        SqliteStateStore.openSync(root, 65_536, 100, { initialize: "create" }).close();
      });
      expect(caught).toBeInstanceOf(MeshFenceBusyError);
      const error = caught as MeshFenceBusyError;
      expect(error.holder).toBe(` (${path.basename(lock)} ${lock}, owner pid unreadable)`);
      expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", errcode: 5, attempts: 1, maxGapMs: 0 });
      for (const surface of await diagnosticSurfaces(error)) expectNoOwnerPayload(surface);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
    });

    it(`public SQLite MeshStore.get retains safe, useful ${kind} fence causes`, () => {
      const root = tempRoot();
      const { ownerPath, record } = plantOwner(root, kind, BAD_PID);
      // The existing suite fixture default enables create on fresh roots (as in mesh-state-backend).
      // No private-method mock or alternate backend: this is the public first-open/read path.
      const store = new MeshStore(root, 65_536, 100, { stateBackend: "sqlite" });
      try {
        expect(store.stateBackend).toBe("sqlite");
        const caught = captureSync(() => store.get("k"));
        expect(caught).toBeInstanceOf(MeshStateBusyError);
        expect(caught).toBeInstanceOf(MeshLockTimeoutError);
        const error = caught as MeshStateBusyError;
        expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", busyCode: "FABRIC_MESH_STATE_BUSY" });
        expect(error.cause).toBeInstanceOf(MeshLockTimeoutError);
        const timeout = error.cause as MeshLockTimeoutError;
        expect(timeout.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
        expect(timeout.cause).toBeInstanceOf(MeshFenceBusyError);
        const fence = timeout.cause as MeshFenceBusyError;
        expect(fence).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", errcode: 5, attempts: 1 });
        // The actual cause objects remain enumerable and retained, rather than being dropped/scrubbed.
        expect(Object.fromEntries(Object.entries(error)).cause).toBe(timeout);
        expect(Object.fromEntries(Object.entries(timeout)).cause).toBe(fence);
        expect(fence.holder).toContain("owner pid unreadable");
        for (const surface of [error.message, JSON.stringify(error), JSON.stringify(Object.entries(error)),
          fence.holder, timeout.message, fence.message]) expectNoOwnerPayload(surface);
        expect(JSON.parse(JSON.stringify(error)).cause.cause.holder).toBe(fence.holder);
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
      } finally { store.closeState(); }
    });

    for (const json of [false, true]) {
      it(`exact generated ${kind} recovery argv refuses malformed token safely (${json ? "JSON" : "plain"})`, async () => {
        const root = tempRoot();
        const { ownerPath, record, lock } = plantOwner(root, kind, String(process.pid), BAD_TOKEN);
        const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
        try {
          const acquisition = kind === "custody"
            ? acquireMeshCustodyLock(root, 0, { lockProtocol: 3, ownIncarnation: undefined })
            : new MeshLock(root, { lockProtocol: 3, lockTimeoutMs: 20 }, () => undefined).withLock(() => undefined);
          const caught = await captureAsync(acquisition);
          expect(caught).toBeInstanceOf(MeshLockForeignHolderError);
          const error = caught as MeshLockForeignHolderError;
          expect(error.holderToken).toBe("invalid-token");
          expect(error.recoveryArgv).toEqual(["fabric-mesh-backend", "lock-recover", "--root", root,
            "--lock", kind, "--holder", "invalid-token"]);
          let stdout = "", stderr = "";
          const argv = error.recoveryArgv.slice(1);
          const code = await main(json ? [...argv, "--json"] : argv, {
            stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
          });
          expect(code).toBe(3);
          const message = `FABRIC_MESH_LOCK_RECOVER_REFUSED: ${lock} is held by invalid-token, not invalid-token`;
          expect(stderr).toBe(`fabric-mesh-backend: lock-recover refused: ${message}\n`);
          if (json) expect(JSON.parse(stdout)).toEqual({ command: "lock-recover", ok: false, refused: true,
            code: "FABRIC_MESH_LOCK_RECOVER_REFUSED", error: message });
          else expect(stdout).toBe("");
          for (const surface of [error.message, JSON.stringify(error), stdout, stderr,
            JSON.stringify(warning.mock.calls)]) expectNoOwnerPayload(surface);
          expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
          expect(fs.readdirSync(root).filter(name => name.startsWith("lock-recoveries.jsonl"))).toEqual([]);
          expect(fs.readdirSync(root).some(name => name.includes(".dead."))).toBe(false);
        } finally { warning.mockRestore(); }
      });
    }

    it.each(["0", "-1", "1.5", "9007199254740992", "Infinity", ""])(
      `synchronous ${kind} fence rejects non-positive, fractional, unsafe, or absent PID %j`, pid => {
        const root = tempRoot();
        const { ownerPath, record, lock } = plantOwner(root, kind, pid);
        const caught = captureSync(() => SqliteStateStore.openSync(root, 65_536, 100, { initialize: "create" }).close());
        expect(caught).toBeInstanceOf(MeshFenceBusyError);
        expect((caught as MeshFenceBusyError).holder).toBe(` (${path.basename(lock)} ${lock}, owner pid unreadable)`);
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
      });

    it(`valid numeric PID presentation is unchanged for the synchronous ${kind} fence`, () => {
      const root = tempRoot();
      const { ownerPath, record, lock } = plantOwner(root, kind, String(process.pid));
      const caught = captureSync(() => SqliteStateStore.openSync(root, 65_536, 100, { initialize: "create" }).close());
      expect(caught).toBeInstanceOf(MeshFenceBusyError);
      const error = caught as MeshFenceBusyError;
      const holder = ` (${path.basename(lock)} ${lock}, held by pid ${process.pid})`;
      expect(error.holder).toBe(holder);
      expect(error.message).toBe(`FABRIC_MESH_LOCK_TIMEOUT: Timed out waiting for the Fabric mesh lock${holder} after 1 attempts, largest gap between attempts 0 ms`);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
    });

    it(`valid token mismatch preserves exact ${kind} recovery information and exit 3`, async () => {
      const root = tempRoot();
      const { ownerPath, record, lock } = plantOwner(root, kind, String(process.pid));
      const message = `FABRIC_MESH_LOCK_RECOVER_REFUSED: ${lock} is held by ${TOKEN}, not ${OTHER_TOKEN}`;
      for (const json of [false, true]) {
        let stdout = "", stderr = "";
        const argv = ["lock-recover", "--root", root, "--lock", kind, "--holder", OTHER_TOKEN];
        const code = await main(json ? [...argv, "--json"] : argv, {
          stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
        });
        expect(code).toBe(3);
        expect(stderr).toBe(`fabric-mesh-backend: lock-recover refused: ${message}\n`);
        if (json) expect(JSON.parse(stdout)).toEqual({ command: "lock-recover", ok: false, refused: true,
          code: "FABRIC_MESH_LOCK_RECOVER_REFUSED", error: message });
        else expect(stdout).toBe("");
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
        expect(fs.readdirSync(root).filter(name => name.startsWith("lock-recoveries.jsonl"))).toEqual([]);
      }
    });
  }
});
