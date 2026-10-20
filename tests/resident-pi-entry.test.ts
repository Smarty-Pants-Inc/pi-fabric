import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";

const { runResidentHost } = vi.hoisted(() => ({ runResidentHost: vi.fn() }));
vi.mock("../src/residency/host.js", () => ({ runResidentHostFromConfigPath: runResidentHost }));

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  runResidentHost.mockReset();
});

it("reports resident startup failure and exits nonzero", async () => {
  const previousExitCode = process.exitCode;
  const failure = new Error("KV revision conflict on sandbox1.p.actor");
  let stderr = "";
  const write = vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array) => {
    stderr += String(chunk); return true;
  }) as typeof process.stderr.write);
  const shutdown = vi.fn();
  const handlers = new Map<string, (...args: unknown[]) => unknown>();
  const pi = { on: (event: string, handler: (...args: unknown[]) => unknown) => { handlers.set(event, handler); return () => {}; } } as unknown as ExtensionAPI;
  const context = { shutdown, modelRegistry: {}, sessionManager: { getSessionId: () => "resident-session" } } as unknown as ExtensionContext;
  try {
    process.exitCode = 0;
    vi.stubEnv("PI_FABRIC_RESIDENT_CONFIG", "/fixture/resident.json");
    runResidentHost.mockRejectedValueOnce(failure);
    const { default: entry } = await import("../src/residency/pi-entry.js");
    entry(pi);
    handlers.get("session_start")!({}, context);
    await vi.waitFor(() => expect(shutdown).toHaveBeenCalledOnce());
    await handlers.get("session_shutdown")!();
    expect(runResidentHost).toHaveBeenCalledOnce();
    expect(stderr).toContain("resident host failed to start");
    expect(stderr).toContain(failure.message);
    expect(process.exitCode).toBe(1);
  } finally {
    process.exitCode = previousExitCode;
    write.mockRestore();
  }
});
