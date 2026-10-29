import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { configureFabricDiagnostics, diagnostics, DIAGNOSTICS_MAX_BYTES, fabricDiagnosticsFile, flushFabricDiagnostics, logDiagnostic, notifyDiagnostic } from "../src/core/diagnostics.js";
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

  it.each(["org-agent@x", "project-agent@x", "worktree-agent@x", ""])("writes explicit log-only diagnostics privately for role %s", async role => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-log-only-"));
    vi.stubEnv("SMARTY_ROLE", role);
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
    const mkdir = vi.spyOn(fs, "mkdirSync");
    configureFabricDiagnostics("log-only-session");
    expect(mkdir).not.toHaveBeenCalled(); // Binding does not activate the lazy sink.
    logDiagnostic("src/internal.ts", "ignored internal warning");
    logDiagnostic("src/internal.ts", "real failure retained privately", "error");
    await flushFabricDiagnostics();
    expect(records(fabricDiagnosticsFile("log-only-session"))).toEqual([
      { at: expect.any(String), level: "warning", source: "src/internal.ts", message: "ignored internal warning" },
      { at: expect.any(String), level: "error", source: "src/internal.ts", message: "real failure retained privately" },
    ]);
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
  });

  it("binds pre-session log-only records for a principal without idle I/O", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-log-only-startup-"));
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    vi.stubEnv("PI_FABRIC_SESSION_ID", "");
    vi.resetModules();
    const fresh = await import("../src/core/diagnostics.js");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    fresh.logDiagnostic("src/internal.ts", "before session binding");
    expect(mkdir).not.toHaveBeenCalled();
    fresh.configureFabricDiagnostics("actual-worktree-session");
    await fresh.flushFabricDiagnostics();
    expect(records(fresh.fabricDiagnosticsFile("actual-worktree-session"))).toEqual([
      expect.objectContaining({ message: "before session binding" }),
    ]);
  });

  it.each(["project-agent@x", "worktree-agent@x", ""])("persists unbound log-only records for %s without a session id", async role => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-log-only-unbound-"));
    vi.stubEnv("SMARTY_ROLE", role);
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    vi.stubEnv("PI_FABRIC_SESSION_ID", undefined);
    vi.resetModules();
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
    const fresh = await import("../src/core/diagnostics.js");
    expect(mkdir).not.toHaveBeenCalled();
    fresh.logDiagnostic("src/internal.ts", "unbound warning retained");
    await fresh.flushFabricDiagnostics();
    const file = fresh.fabricDiagnosticsFile(`process-${process.pid}`);
    expect(records(file)).toEqual([expect.objectContaining({ message: "unbound warning retained" })]);
    fresh.configureFabricDiagnostics("later-session");
    fresh.logDiagnostic("src/internal.ts", "bound warning retained");
    await fresh.flushFabricDiagnostics();
    expect(records(fresh.fabricDiagnosticsFile("later-session"))).toEqual([
      expect.objectContaining({ message: "bound warning retained" }),
    ]);
    expect(records(file)).toHaveLength(1); // A late binding does not copy or relabel persisted rows.
    expect(warn).not.toHaveBeenCalled(); expect(error).not.toHaveBeenCalled(); expect(stderr).not.toHaveBeenCalled();
  });

  // Keep: data safety at the real process boundary, including exit before the lazy import can resolve.
  it.each([
    ["worktree-agent@x", "unbound", false], ["worktree-agent@x", "unbound", true],
    ["org-agent@x", "unbound", false], ["org-agent@x", "unbound", true],
    ["worktree-agent@x", "configured", true], ["project-agent@x", "environment", true],
    ["org-agent@x", "late-binding", false], ["org-agent@x", "late-binding", true],
  ] as const)("retains private rows across shutdown: role=%s binding=%s immediate=%s", async (role, binding, immediate) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-child-"));
    const output = await build({
      stdin: { resolveDir: process.cwd(), contents: `
        import { configureFabricDiagnostics, flushFabricDiagnostics, logDiagnostic } from ${JSON.stringify(path.resolve("src/core/diagnostics.ts"))};
        if (${JSON.stringify(binding)} === "configured") configureFabricDiagnostics("actual-session");
        logDiagnostic("src/internal.ts", "first warning");
        logDiagnostic("src/internal.ts", "last error", "error");
        if (${JSON.stringify(binding)} === "late-binding") configureFabricDiagnostics("actual-session");
        if (${immediate}) process.exit(0);
        await flushFabricDiagnostics();
      ` },
      bundle: true, packages: "external", platform: "node", format: "esm", write: false, logLevel: "silent",
    });
    const child = spawnSync(process.execPath, ["--input-type=module", "-e", output.outputFiles[0]!.text], {
      env: { ...process.env, SMARTY_ROLE: role, PI_CODING_AGENT_DIR: root,
        PI_FABRIC_SESSION_ID: binding === "environment" ? "actual-session" : undefined },
      encoding: "utf8", timeout: 10_000,
    });
    expect(child.error).toBeUndefined(); expect(child.signal).toBeNull(); expect(child.status, child.stderr).toBe(0);
    expect(child.stdout).toBe("");
    // Retain the inherited filesystem guard; only its own startup banner is unrelated stderr.
    expect(child.stderr.replace(/^hosted-only \(outside-temp deletion reporting\): push and let CI run it\r?\n/gm, "")).toBe("");
    const id = binding === "unbound" ? `process-${child.pid}` : "actual-session";
    const directory = path.join(root, "fabric", "diagnostics");
    expect(fs.readdirSync(directory)).toEqual([`${id}.jsonl`]);
    const file = path.join(directory, `${id}.jsonl`);
    expect(records(file)).toEqual([
      { at: expect.any(String), level: "warning", source: "src/internal.ts", message: "first warning" },
      { at: expect.any(String), level: "error", source: "src/internal.ts", message: "last error" },
    ]);
    if (process.platform !== "win32") {
      expect(fs.statSync(file).mode & 0o777).toBe(0o600);
      expect(fs.statSync(directory).mode & 0o777).toBe(0o700);
    }
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
    const write = vi.spyOn(fs, "appendFileSync").mockImplementation(() => {});
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
    expect(() => logDiagnostic("src/test.ts", "internal log-only")).not.toThrow();
    vi.stubEnv("SMARTY_ROLE", "worktree-agent@x");
    expect(() => logDiagnostic("src/test.ts", "non-principal log-only")).not.toThrow();
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    expect(() => diagnostics.warn("src/test.ts", "internal")).not.toThrow();
    expect(() => notifyDiagnostic(context, "src/test.ts", "internal", "error")).not.toThrow();
    await flushFabricDiagnostics();
    expect(warn).not.toHaveBeenCalled(); expect(error).not.toHaveBeenCalled(); expect(context.ui.notify).not.toHaveBeenCalled();
  });
});
