import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Decision C MOCKED source-ordering probe for #13549, NOT Windows ACL proof.
// Keep reason: data-safety trust boundary before census publication / StateFile sweep.
// Native ACLs, real RuntimeState and custody are covered by mesh-state-windows-acl.test.ts.
// Only the proposed gate export is mocked; selection, factories and constructors are real.
let restoreFixture = (): void => {};
afterEach(() => {
  try { restoreFixture(); }
  finally {
    restoreFixture = () => {};
    vi.restoreAllMocks();
    vi.doUnmock("../src/mesh/state-gate.js");
    vi.resetModules();
  }
});

const fixture = async (kind: "file" | "sqlite" | "shadow", fallback = false, reject = false) => {
  vi.resetModules();
  // Intentionally never created: real reads see ENOENT, so sweeps cannot delete anything.
  // All constructor mutations below are recorded/no-op, even on the unfixed baseline.
  const root = path.join(os.tmpdir(), `fabric-bootstrap-order-${randomUUID()}`);
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const exitListeners = new Set(process.listeners("exit"));
  const handles: Array<{ close(): void }> = [];
  restoreFixture = () => {
    try { for (const handle of handles) handle.close(); }
    finally {
      Object.defineProperty(process, "platform", platform);
      for (const listener of process.listeners("exit")) {
        if (!exitListeners.has(listener)) process.removeListener("exit", listener);
      }
    }
  };
  const gate = await vi.importActual<typeof import("../src/mesh/state-gate.js")>("../src/mesh/state-gate.js");
  const refusal = new gate.MeshStateUnsupportedError(`unsafe bootstrap admission: ${root}`);
  const bootstrap = vi.fn((_root: string) => { if (reject) throw refusal; });
  vi.doMock("../src/mesh/state-gate.js", () => ({ ...gate, prepareStateRootForBootstrap: bootstrap }));
  const { MeshStore } = await import("../src/mesh/store.js");
  const { createStateBackend, STATE_BACKEND_FACTORIES } = await import("../src/mesh/state-backend.js");
  const { MeshLock } = await import("../src/mesh/mesh-lock.js");
  const factory = vi.spyOn(STATE_BACKEND_FACTORIES[kind], "create");
  if (kind !== "file") {
    vi.spyOn(STATE_BACKEND_FACTORIES[kind], "unavailable")
      .mockReturnValue(fallback ? "fixture: node:sqlite is unavailable" : undefined);
  }
  const sweep = vi.spyOn(fs, "readdirSync");
  const mutations = [
    vi.spyOn(fs, "mkdirSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "writeFileSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "renameSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "mkdtempSync").mockImplementation(() => `${root}-staging`),
  ];
  Object.defineProperty(process, "platform", { ...platform, value: "win32" });
  const options = { stateBackend: kind, lockProtocol: 1 as const, shadowVerifyMs: 0 };
  const open = () => {
    const store = new MeshStore(root, 65_536, 1_000, options);
    handles.push(store.stateBackendHandle);
    return store;
  };
  const direct = () => {
    const backend = createStateBackend({ root, maxEventBytes: 65_536, maxReadEvents: 1_000,
      lock: new MeshLock(root, {}, () => {}) }, options);
    handles.push(backend);
    return backend;
  };
  return { root, bootstrap, refusal, factory, sweep, mutations, open, direct };
};

const cases = [
  ["sqlite", false], ["shadow", false], ["sqlite", true], ["shadow", true],
] as const;

describe("Decision C MOCKED #13549 bootstrap source ordering (not Windows ACL proof)", () => {
  it.each(cases)("GAP13549-U1: MeshStore %s fallback=%s refuses before factory/sweep/mutation", async (kind, fallback) => {
    const p = await fixture(kind, fallback, true);
    expect(p.open).toThrow(p.refusal);
    expect(p.bootstrap).toHaveBeenCalledOnce();
    expect(p.bootstrap.mock.calls[0]?.[0]).toBe(p.root);
    expect(p.factory).not.toHaveBeenCalled();
    expect(p.sweep).not.toHaveBeenCalled();
    for (const mutation of p.mutations) expect(mutation).not.toHaveBeenCalled();
  });

  it.each(cases)("GAP13549-U2: MeshStore %s fallback=%s admits before real construction and census mkdir", async (kind, fallback) => {
    const p = await fixture(kind, fallback);
    const store = p.open();
    expect(store.stateBackend).toBe(fallback ? "file" : kind);
    if (fallback) expect(store.stateDiagnostics().fallback).toContain("fixture: node:sqlite is unavailable");
    expect(p.bootstrap).toHaveBeenCalledOnce();
    expect(p.bootstrap.mock.calls[0]?.[0]).toBe(p.root);
    const admission = p.bootstrap.mock.invocationCallOrder[0]!;
    if (!fallback) {
      expect(p.factory).toHaveBeenCalledOnce();
      expect(admission).toBeLessThan(p.factory.mock.invocationCallOrder[0]!);
    }
    if (kind === "shadow" || fallback) {
      expect(p.sweep).toHaveBeenCalledWith(p.root, { withFileTypes: true });
      expect(admission).toBeLessThan(p.sweep.mock.invocationCallOrder[0]!);
    }
    expect(p.mutations[0]).toHaveBeenCalledWith(p.root, { recursive: true, mode: 0o700 });
    expect(p.mutations[0]).toHaveBeenCalledWith(path.join(p.root, ".writer-census"), { recursive: true, mode: 0o700 });
    for (const mutation of p.mutations) {
      for (const order of mutation.mock.invocationCallOrder) expect(admission).toBeLessThan(order);
    }
  });

  it.each(["sqlite", "shadow"] as const)("GAP13549-U3: direct %s unavailable fallback admits before StateFile sweep", async kind => {
    const p = await fixture(kind, true, true);
    expect(p.direct).toThrow(p.refusal);
    expect(p.bootstrap).toHaveBeenCalledOnce();
    expect(p.factory).not.toHaveBeenCalled();
    expect(p.sweep).not.toHaveBeenCalled();
    for (const mutation of p.mutations) expect(mutation).not.toHaveBeenCalled();
  });

  it("GAP13549-U4: explicit file with no DB retains its no-ACL-bootstrap behavior", async () => {
    const p = await fixture("file", false, true);
    expect(p.open().stateBackend).toBe("file");
    expect(p.bootstrap).not.toHaveBeenCalled();
    expect(p.factory).toHaveBeenCalledOnce();
    expect(p.sweep).toHaveBeenCalledWith(p.root, { withFileTypes: true });
    expect(p.mutations[0]).toHaveBeenCalledWith(p.root, { recursive: true, mode: 0o700 });
  });
});
