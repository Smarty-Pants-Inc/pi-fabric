import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { configureFabricDiagnostics, diagnostics, DIAGNOSTICS_MAX_BYTES, fabricDiagnosticsFile, flushFabricDiagnostics, notifyDiagnostic } from "../src/core/diagnostics.js";
import { warnUnfencedControlDelivery } from "../src/topology/control-plane.js";

// Keep: the principal-pane incident is a real catch; use real files for delivery and a
// recorded-call probe for rotation so no product overwrite/removal path executes locally.
afterEach(async () => { await flushFabricDiagnostics(); vi.restoreAllMocks(); vi.unstubAllEnvs(); });
const principal = (id = "principal-session") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-"));
  vi.stubEnv("SMARTY_ROLE", "org-agent@x");
  vi.stubEnv("PI_CODING_AGENT_DIR", root);
  configureFabricDiagnostics(id);
  return fabricDiagnosticsFile(id);
};
const records = (file: string) => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));

describe("principal Fabric diagnostics", () => {
  it("does no import/idle I/O and flushes bounded pre-session diagnostics into the actual session", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-startup-"));
    vi.stubEnv("SMARTY_ROLE", "org-agent@x"); vi.stubEnv("PI_CODING_AGENT_DIR", root); vi.stubEnv("PI_FABRIC_SESSION_ID", "");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.resetModules();
    const fresh = await import("../src/core/diagnostics.js");
    expect(mkdir).not.toHaveBeenCalled();
    for (let i = 0; i < 32; i++) fresh.diagnostics.warn("src/startup.ts", "x".repeat(64 * 1024));
    expect(mkdir).not.toHaveBeenCalled(); expect(warn).not.toHaveBeenCalled();
    fresh.configureFabricDiagnostics("actual-session");
    await fresh.flushFabricDiagnostics();
    const file = fresh.fabricDiagnosticsFile("actual-session");
    expect(records(file).length).toBeGreaterThan(0);
    expect(records(file).length).toBeLessThan(32);
    expect(fs.statSync(file).size).toBeLessThanOrEqual(DIAGNOSTICS_MAX_BYTES);
    expect(mkdir).toHaveBeenCalled();
  });

  it("buffers the first diagnostics until the lazy sink resolves and keeps session attribution", async () => {
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    let release!: () => void;
    const ready = new Promise<void>(resolve => { release = resolve; });
    const appendDiagnostic = vi.fn();
    const loads = vi.fn();
    vi.resetModules();
    vi.doMock("../src/core/diagnostics-log.js", async () => {
      loads(); await ready; return { appendDiagnostic };
    });
    try {
      const fresh = await import("../src/core/diagnostics.js");
      fresh.configureFabricDiagnostics("first-session");
      expect(loads).not.toHaveBeenCalled();
      fresh.diagnostics.warn("src/first.ts", "first buffered line");
      fresh.configureFabricDiagnostics("second-session");
      fresh.diagnostics.error("src/second.ts", "second buffered line");
      await vi.waitFor(() => expect(loads).toHaveBeenCalledOnce());
      expect(appendDiagnostic).not.toHaveBeenCalled();
      appendDiagnostic.mockImplementationOnce(() => {
        queueMicrotask(() => fresh.diagnostics.warn("src/late.ts", "line queued during drain completion"));
      });
      release(); await fresh.flushFabricDiagnostics();
      expect(appendDiagnostic.mock.calls.map(([id, line]) => [id, JSON.parse(line).message])).toEqual([
        ["first-session", "first buffered line"], ["second-session", "second buffered line"],
        ["second-session", "line queued during drain completion"],
      ]);
    } finally { release(); vi.doUnmock("../src/core/diagnostics-log.js"); }
  });

  it("writes the unfenced delivery warning privately with source, level and time", async () => {
    const file = principal();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    warnUnfencedControlDelivery("followUp", "session:target", 1);
    diagnostics.error("src/example.ts", "failed", new Error("disk unavailable"));
    await flushFabricDiagnostics();
    expect(warn).not.toHaveBeenCalled(); expect(error).not.toHaveBeenCalled();
    expect(records(file)).toEqual([
      { at: expect.any(String), level: "warning", source: "src/topology/control-plane.ts", message: '[pi-fabric] Unfenced legacy control delivery count=1 operation=followUp target="session:target"' },
      { at: expect.any(String), level: "error", source: "src/example.ts", message: expect.stringContaining("disk unavailable") },
    ]);
    expect(Number.isFinite(Date.parse(records(file)[0].at))).toBe(true);
    if (process.platform !== "win32") expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it.each(["org-agent@x", "org-agent-kate@x", "org-agent-playful@x"])("hides internal notifications for %s but allows explicit feedback", async role => {
    const file = principal(); vi.stubEnv("SMARTY_ROLE", role);
    const context = { ui: { notify: vi.fn() }, sessionManager: { getSessionId: () => "principal-session" } };
    notifyDiagnostic(context, "src/test.ts", "internal state", "warning");
    await flushFabricDiagnostics();
    expect(context.ui.notify).not.toHaveBeenCalled();
    expect(records(file).at(-1)).toMatchObject({ message: "internal state" });
    notifyDiagnostic(context, "src/test.ts", "command feedback", "error", { principalVisible: true });
    expect(context.ui.notify).toHaveBeenCalledExactlyOnceWith("command feedback", "error");
  });

  it("preserves non-principal console arguments and UI notifications without filesystem access", () => {
    vi.stubEnv("SMARTY_ROLE", "project-agent@x");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const context = { ui: { notify: vi.fn() } };
    warnUnfencedControlDelivery("followUp", "target", 2);
    const detail = new Error("detail");
    diagnostics.error("src/test.ts", "message", detail);
    notifyDiagnostic(context, "src/test.ts", "dashboard failure", "warning");
    expect(warn).toHaveBeenCalledExactlyOnceWith('[pi-fabric] Unfenced legacy control delivery count=2 operation=followUp target="target"');
    expect(error).toHaveBeenCalledExactlyOnceWith("message", detail);
    expect(context.ui.notify).toHaveBeenCalledExactlyOnceWith("dashboard failure", "warning");
    expect(mkdir).not.toHaveBeenCalled();
  });

  it("keeps raw Main diagnostics off principal stderr and preserves the exact non-principal stream", async () => {
    const file = principal();
    const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
    diagnostics.stderr("src/mesh/state-sqlite.ts", "checkpoint warning\n");
    await flushFabricDiagnostics();
    expect(stderr).not.toHaveBeenCalled();
    expect(records(file)[0]).toMatchObject({ source: "src/mesh/state-sqlite.ts", message: "checkpoint warning\n" });
    vi.stubEnv("SMARTY_ROLE", "project-agent@x");
    diagnostics.stderr("src/mesh/state-sqlite.ts", "checkpoint warning\n");
    expect(stderr).toHaveBeenCalledExactlyOnceWith("checkpoint warning\n");
  });

  it("caps oversized JSON records below the byte budget", async () => {
    const file = principal();
    diagnostics.warn("src/test.ts", "\u0000".repeat(DIAGNOSTICS_MAX_BYTES));
    await flushFabricDiagnostics();
    expect(fs.statSync(file).size).toBeLessThanOrEqual(DIAGNOSTICS_MAX_BYTES);
    expect(records(file)[0].message.length).toBe(64 * 1024);
  });

  it("rotates at the cap to one backup (recorded calls, no real overwrite)", async () => {
    const file = principal();
    const mkdir = vi.spyOn(fs, "mkdirSync").mockReturnValue(undefined);
    const open = vi.spyOn(fs, "openSync").mockReturnValue(42);
    vi.spyOn(fs, "fstatSync").mockReturnValue({ size: DIAGNOSTICS_MAX_BYTES, nlink: 1, isFile: () => true } as fs.Stats);
    const chmod = vi.spyOn(fs, "fchmodSync").mockImplementation(() => {});
    const close = vi.spyOn(fs, "closeSync").mockImplementation(() => {});
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => {});
    const write = vi.spyOn(fs, "writeSync").mockReturnValue(1);
    diagnostics.warn("src/test.ts", "first"); diagnostics.warn("src/test.ts", "second");
    await flushFabricDiagnostics();
    expect(mkdir).toHaveBeenCalled();
    expect(rename.mock.calls).toEqual([[file, `${file}.1`], [file, `${file}.1`]]);
    expect(open.mock.calls.every(call => call[2] === 0o600)).toBe(true);
    expect(chmod).toHaveBeenCalledWith(42, 0o600);
    expect(close).toHaveBeenCalledTimes(4);
    expect(write.mock.calls.every(call => Buffer.byteLength(String(call[1])) <= DIAGNOSTICS_MAX_BYTES)).toBe(true);
  });

  it("never falls back to console or UI if the private log cannot be written", async () => {
    principal();
    vi.spyOn(fs, "mkdirSync").mockImplementation(() => { throw new Error("unavailable"); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const context = { ui: { notify: vi.fn() } };
    expect(() => diagnostics.warn("src/test.ts", "internal")).not.toThrow();
    expect(() => notifyDiagnostic(context, "src/test.ts", "internal", "error")).not.toThrow();
    await flushFabricDiagnostics();
    expect(warn).not.toHaveBeenCalled(); expect(error).not.toHaveBeenCalled(); expect(context.ui.notify).not.toHaveBeenCalled();
  });
});
