import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockForeignHolderError, meshDeferredSummary } from "../src/core/atomic-write.js";
import { localFilesystemTestHooks, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";
import { RemoteBridgeSide, StoreBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";

const OWN_NAMESPACE = "linux:0123456789abcdef0123456789abcdef:11111111-1111-4111-8111-111111111111:4026531836";
const FOREIGN_NAMESPACE = "linux:fedcba9876543210fedcba9876543210:22222222-2222-4222-8222-222222222222:4026532999";
const VALID_TOKEN = "550e8400-e29b-41d4-a716-446655440000";
const roots: string[] = [];

// As in mesh-lock-namespace.test.ts, exercise the namespace rule on a known-local root on every CI OS.
beforeEach(() => { pidNamespaceTestHooks.set(OWN_NAMESPACE); localFilesystemTestHooks.set(0xEF53); });
afterEach(() => {
  vi.restoreAllMocks();
  pidNamespaceTestHooks.reset();
  localFilesystemTestHooks.reset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owner-diagnostics-"));
  roots.push(root);
  return root;
};

const plantOwner = (root: string, record: string, kind: "custody" | "mesh" = "custody"): string => {
  const lock = path.join(root, kind === "custody" ? MESH_CUSTODY_LOCK_NAME : ".lock");
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), record, { mode: 0o600 });
  return lock;
};

describe("foreign-holder diagnostics project owner metadata", () => {
  it.each([["custody", "token"], ["custody", "namespace"], ["mesh", "token"], ["mesh", "namespace"]] as const)(
    "%s refusal keeps a malformed %s secret out of every diagnostic surface", async (kind, field) => {
    const secret = "OWNER_FREE_TEXT_SECRET_94cd";
    const root = tempRoot();
    const token = field === "token" ? secret : VALID_TOKEN;
    const namespace = field === "namespace" ? secret : FOREIGN_NAMESPACE;
    const record = `${token}\n${process.pid}\n${Date.now()}\n\n${namespace}\n`;
    const lock = plantOwner(root, record, kind);
    const ownerPath = path.join(lock, "owner");
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let store: MeshStore | undefined;
    let remote: RemoteBridgeSide | undefined;
    let serving: Promise<void> | undefined;
    try {
      // Both production acquisition paths read the private record and construct the refusal themselves.
      const acquisition = kind === "custody"
        ? acquireMeshCustodyLock(root, 0, { lockProtocol: 3, ownIncarnation: undefined })
        : new MeshLock(root, { lockProtocol: 3, lockTimeoutMs: 20 }, () => undefined).withLock(() => undefined);
      const refused = await acquisition.then(() => new Error("unexpected lock acquisition"), error => error);
      expect(refused).toBeInstanceOf(MeshLockForeignHolderError);
      const error = refused as MeshLockForeignHolderError;
      expect(error.foreignCode).toBe(field === "token" ? "FABRIC_MESH_LOCK_FOREIGN_HOLDER" : "FABRIC_MESH_LOCK_UNPROVEN_HOLDER");
      expect(error.holderToken).toBe(field === "token" ? "invalid-token" : VALID_TOKEN);
      expect(error.holderNamespace).toBe(field === "namespace" ? "invalid-namespace" : FOREIGN_NAMESPACE);
      expect(error.recoveryArgv.at(-1)).toBe(field === "token" ? "invalid-token" : VALID_TOKEN);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);

      const summary = meshDeferredSummary(error);
      new MeshBackgroundRetry("owner-probe", 10, 20).failure(error);
      const warningText = warning.mock.calls.flat().map(String).join("\n");

      // Exercise the actual bridge serializer and decoder with the production-created refusal.
      store = new MeshStore(path.join(root, "rpc-store"), 65_536, 100, { stateBackend: "file" });
      const side = new StoreBridgeSide(store, "review");
      side.latestSequence = async () => { throw error; };
      const agentInput = new PassThrough();
      const agentOutput = new PassThrough();
      const frames: string[] = [];
      agentOutput.on("data", chunk => frames.push(String(chunk)));
      serving = serveBridgeAgent(side, agentInput, agentOutput);
      remote = new RemoteBridgeSide(agentOutput, agentInput, 1_000);
      const decoded = await remote.latestSequence().then(
        () => { throw new Error("unexpected successful bridge response"); },
        bridgeError => bridgeError as Error,
      );
      const frame = frames.join("");
      const serializedFields = JSON.stringify({
        error: JSON.stringify(error), enumerable: Object.entries(error), message: error.message,
        holder: error.holder, recovery: error.recovery, recoveryArgv: error.recoveryArgv,
      });
      for (const surface of [serializedFields, error.message, error.holder, error.recovery,
        JSON.stringify(error.recoveryArgv), summary, warningText, frame, decoded.message]) {
        expect(surface).not.toContain(secret);
      }
      expect(summary).toBe(error.message);
      expect(warningText).toContain("ALARM FABRIC_MESH_LOCK_");
      expect(warningText).toContain("owner-probe: mesh lock timeout");
      expect(frame).toContain('"ok":false');
      expect(decoded.message).toContain("Bridge agent:");
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(record);
    } finally {
      remote?.close();
      if (serving) await serving;
      store?.closeState();
      warning.mockRestore();
    }
  });

  it.each(["0123456789abcdef0123456789abcdef", VALID_TOKEN])("preserves the complete valid owner token %s", token => {
    const error = new MeshLockForeignHolderError(" (owner)", 1, 0, "foreign", FOREIGN_NAMESPACE, "/mesh/.lock", token);
    expect(error.holderToken).toBe(token);
    expect(error.recoveryArgv.at(-1)).toBe(token);
  });

  it.each(["", FOREIGN_NAMESPACE,
    "darwin:550e8400-e29b-41d4-a716-446655440000:11111111-1111-4111-8111-111111111111:host",
    "win32:550e8400-e29b-41d4-a716-446655440000:638640000000000000:host",
  ])("preserves native namespace or legacy absence %s", namespace => {
    const error = new MeshLockForeignHolderError(" (owner)", 1, 0, "unproven", namespace, "/mesh/.lock", VALID_TOKEN);
    expect(error.holderNamespace).toBe(namespace);
    if (!namespace) expect(error.message).toContain("no recorded PID namespace");
  });

  it.each(["deadbeef", "a".repeat(37), { secret: "OWNER_FREE_TEXT_SECRET_94cd" }])("drops invalid token shape %j", token => {
    const error = new MeshLockForeignHolderError(" (owner)", 1, 0, "unproven", "", "/mesh/.lock", token);
    expect(error.holderToken).toBe("invalid-token");
    expect(error.recoveryArgv.at(-1)).toBe("invalid-token");
    expect(JSON.stringify(error)).not.toContain("OWNER_FREE_TEXT_SECRET_94cd");
  });

  it.each([FOREIGN_NAMESPACE + "\n", FOREIGN_NAMESPACE + "1".repeat(128), { secret: "OWNER_FREE_TEXT_SECRET_94cd" }])(
    "drops control, overlong or non-string namespace %j", namespace => {
      const error = new MeshLockForeignHolderError(" (owner)", 1, 0, "unproven", namespace, "/mesh/.lock", VALID_TOKEN);
      expect(error.holderNamespace).toBe("invalid-namespace");
      expect(JSON.stringify(error)).not.toContain("OWNER_FREE_TEXT_SECRET_94cd");
    });

  it("preserves valid foreign-owner recovery details and leaves its owner bytes untouched", async () => {
    const root = tempRoot();
    const record = `${VALID_TOKEN}\n${process.pid}\n${Date.now()}\n\n${FOREIGN_NAMESPACE}\n`;
    const lock = plantOwner(root, record);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const refused = await acquireMeshCustodyLock(root, 0, { lockProtocol: 3, ownIncarnation: undefined })
        .then(() => new Error("unexpected custody acquisition"), error => error);
      expect(refused).toBeInstanceOf(MeshLockForeignHolderError);
      const error = refused as MeshLockForeignHolderError;
      const expectedArgv = ["fabric-mesh-backend", "lock-recover", "--root", root, "--lock", "custody", "--holder", VALID_TOKEN];
      expect(error.foreignCode).toBe("FABRIC_MESH_LOCK_FOREIGN_HOLDER");
      expect(error.verdict).toBe("foreign");
      expect(error.holderToken).toBe(VALID_TOKEN);
      expect(error.holderNamespace).toBe(FOREIGN_NAMESPACE);
      expect(error.recoveryArgv).toEqual(expectedArgv);
      expect(error.recovery).toBe(`fabric-mesh-backend lock-recover --root ${root} --lock custody --holder ${VALID_TOKEN}`);
      expect(error.message).toContain(error.recovery);
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(record);
    } finally {
      warning.mockRestore();
    }
  });
});
