import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WindowsStateAclRequest } from "../src/mesh/windows-state-acl.js";

// Decision C MOCKED source-routing tests for #13549 option A, NOT native Windows ACL/UNC proof.
// Keep: privacy/data-safety boundary before StateFile sweep and MeshStore census publication.
// Real selector, UNC classifier and bootstrap helper; only native ACL verdicts and I/O are faked.
// On Linux path stays POSIX: the entire UNC metadata chain is simulated, never an SMB share.
const ROOT = String.raw`\\decision-c\share\mesh`;
const kinds = ["sqlite", "shadow"] as const;
let restoreFixture = (): void => {};
afterEach(() => {
  try { restoreFixture(); }
  finally {
    restoreFixture = () => {};
    vi.restoreAllMocks();
    vi.doUnmock("../src/mesh/windows-state-acl.js");
    vi.resetModules();
  }
});

const fixture = async (kind: typeof kinds[number], verdict?: string) => {
  vi.resetModules();
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const listeners = new Set(process.listeners("exit"));
  const handles: Array<{ close(): void }> = [];
  restoreFixture = () => {
    try { for (const handle of handles) handle.close(); }
    finally {
      Object.defineProperty(process, "platform", platform);
      for (const listener of process.listeners("exit")) {
        if (!listeners.has(listener)) process.removeListener("exit", listener);
      }
    }
  };
  const acl = vi.fn((_request: WindowsStateAclRequest) => verdict);
  vi.doMock("../src/mesh/windows-state-acl.js", () => ({ windowsStateAclRefusal: acl }));
  const gate = await import("../src/mesh/state-gate.js");
  const { MeshStore } = await import("../src/mesh/store.js");
  const { createStateBackend } = await import("../src/mesh/state-backend.js");
  const { MeshLock } = await import("../src/mesh/mesh-lock.js");
  const directory = { isSymbolicLink: () => false, isDirectory: () => true, isFile: () => false } as fs.Stats;
  // Existing directories throughout the lexical chain. ONLY state.db is positively absent:
  // admitExistingStateRoot must not enter the existing-DB locality gate and mask safe fallback.
  const metadata = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) =>
    path.basename(String(file)) === "state.db" ? undefined : directory) as typeof fs.lstatSync);
  vi.spyOn(fs.realpathSync, "native").mockImplementation((file) => path.resolve(String(file)));
  vi.spyOn(fs, "readFileSync").mockImplementation(() => { throw Object.assign(new Error("absent fixture file"), { code: "ENOENT" }); });
  const sweep = vi.spyOn(fs, "readdirSync").mockReturnValue([]);
  // No real artifacts or deletions, even if the unfixed selector reaches census publication.
  const mutations = [
    vi.spyOn(fs, "mkdirSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "writeFileSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "renameSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "mkdtempSync").mockImplementation(() => `${ROOT}-unused-staging`),
    vi.spyOn(fs, "rmSync").mockImplementation(() => undefined),
    vi.spyOn(fs, "unlinkSync").mockImplementation(() => undefined),
  ];
  const output = [
    vi.spyOn(console, "error").mockImplementation(() => {}),
    vi.spyOn(console, "warn").mockImplementation(() => {}),
    vi.spyOn(console, "info").mockImplementation(() => {}),
    vi.spyOn(console, "log").mockImplementation(() => {}),
    vi.spyOn(console, "debug").mockImplementation(() => {}),
    vi.spyOn(process.stdout, "write").mockImplementation(() => true),
    vi.spyOn(process.stderr, "write").mockImplementation(() => true),
  ];
  Object.defineProperty(process, "platform", { ...platform, value: "win32" });
  expect(gate.filesystemRefusal(ROOT)).toBe(`network path ${ROOT}`);
  const options = { stateBackend: kind, lockProtocol: 1 as const, shadowVerifyMs: 0 };
  const direct = () => {
    const backend = createStateBackend({ root: ROOT, maxEventBytes: 65_536, maxReadEvents: 1_000,
      lock: new MeshLock(ROOT, {}, () => {}) }, options);
    handles.push(backend);
    return backend;
  };
  const open = () => {
    const store = new MeshStore(ROOT, 65_536, 1_000, options);
    // No closeState(): that would enter census deletion. Close only any opened backend.
    handles.push(store.stateBackendHandle);
    return store;
  };
  return { gate, acl, metadata, sweep, mutations, output, direct, open };
};

type Probe = Awaited<ReturnType<typeof fixture>>;
const expectPlan = (p: Probe, root: string, nth: number) => {
  const db = path.join(root, "state.db");
  expect(p.acl).toHaveBeenNthCalledWith(nth, {
    root, real: path.resolve(root), protect: false, requireDatabase: false,
    files: [db, `${db}-wal`, `${db}-shm`, `${db}-journal`],
    directories: [path.join(root, "state-checkpoint.flags")],
  });
};
const expectUntouched = (p: Probe) => {
  expect(p.sweep).not.toHaveBeenCalled();
  for (const mutation of p.mutations) expect(mutation).not.toHaveBeenCalled();
  for (const output of p.output) expect(output).not.toHaveBeenCalled();
};

const unsafe = [
  ["owner", `${ROOT} is owned by S-1-5-21-999, not the current user`],
  ["M", `${ROOT} grants S-1-5-11 read, write, delete or permission rights (mask 0x1301bf)`],
  ["R", `${ROOT} grants S-1-5-11 read, write, delete or permission rights (mask 0x120089)`],
  ["junction", `${ROOT} is a reparse point`],
] as const;

describe("Decision C MOCKED #13549 option A nonlocal fallback (not Windows ACL/native UNC proof)", () => {
  it.each(kinds)("GAP13549-F1: safe missing-DB %s falls back with the identical normal privacy plan", async kind => {
    const p = await fixture(kind);
    const local = String.raw`C:\decision-c\mesh`;
    p.gate.prepareStateRootForBootstrap(local);
    expectPlan(p, local, 1);
    const backend = p.direct();
    expect(backend.kind).toBe("file");
    expect(backend.diagnostics()).toEqual({ kind: "file", fallback: `${kind}: network path ${ROOT}` });
    expectPlan(p, ROOT, 2);
    expect(p.acl).toHaveBeenCalledTimes(2);
    expect(p.metadata).toHaveBeenCalledWith(path.join(ROOT, "state.db"), { throwIfNoEntry: false });
    expect(p.sweep).toHaveBeenCalledWith(ROOT, { withFileTypes: true });
    expect(p.acl.mock.invocationCallOrder[1]!).toBeLessThan(p.sweep.mock.invocationCallOrder[0]!);
    for (const mutation of p.mutations) expect(mutation).not.toHaveBeenCalled();
  });

  for (const kind of kinds) {
    it.each(unsafe)(`GAP13549-F2: MeshStore ${kind} refuses %s before sweep/census/artifacts`, async (_label, verdict) => {
      const p = await fixture(kind, verdict);
      expect(p.open).toThrowError(expect.objectContaining({
        code: "FABRIC_MESH_STATE_UNSUPPORTED", message: `Fabric mesh SQLite state refuses ${ROOT}: ${verdict}`,
      }));
      expectPlan(p, ROOT, 1);
      expect(p.acl).toHaveBeenCalledOnce();
      expectUntouched(p);
    });

    it.each(["timeout", "exit 7"])(`GAP13549-F3: MeshStore ${kind} preserves fixed technical %s refusal without output`, async reason => {
      const message = `Windows ACL read failed (${reason}) after 1 retry; root not opened: ${ROOT}`;
      const p = await fixture(kind, message);
      expect(p.open).toThrowError(expect.objectContaining({
        code: "FABRIC_MESH_STATE_UNSUPPORTED", message,
      }));
      expectPlan(p, ROOT, 1);
      expect(p.acl).toHaveBeenCalledOnce();
      expectUntouched(p);
    });
  }
});
