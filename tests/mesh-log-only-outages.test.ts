import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshBackgroundQueue, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { configureFabricDiagnostics, fabricDiagnosticsFile, flushFabricDiagnostics, logDiagnostic } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// Keep: the background failure/recovery path is not cheaply reachable through a UI flow.
const roots: string[] = [];
afterEach(async () => {
  await flushFabricDiagnostics();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const diagnostics = async (session: string): Promise<string[]> => {
  await flushFabricDiagnostics();
  const file = fabricDiagnosticsFile(session);
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean)
    .map(line => JSON.parse(line).message as string) : [];
};
const setup = (session: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-log-only-"));
  roots.push(root);
  vi.stubEnv("SMARTY_ROLE", "worktree-agent@test");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  // Retries span multiple ordinary warning windows; the opt-in policy still emits only once.
  vi.stubEnv("PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS", "1");
  configureFabricDiagnostics(session);
  return root;
};

it("keeps the actual participant refusal/reap queue log-only through changing retries, recovery and a new outage", async () => {
  const session = "participant-refusal-outages";
  const root = setup(session);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const error = vi.spyOn(console, "error").mockImplementation(() => {});
  const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
  let failing = false;
  let attempts = 0;
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: "owner", rootId: "root", identity: { id: "owner", name: "owner", kind: "main" },
    heartbeatMs: 60_000, leaseMs: 600_000, reapDeadHosts: false, presencePassMs: 0,
    presencePass: async () => {
      attempts++;
      if (failing) throw new MeshLockTimeoutError(` changing holder ${attempts}`, attempts, attempts * 10);
    },
  });
  try {
    await directory.start();
    attempts = 0;
    failing = true;
    await directory.refresh();
    await vi.waitFor(() => expect(attempts).toBeGreaterThanOrEqual(3), { timeout: 3_000 });
    const outage = (await diagnostics(session)).filter(line => line.includes("participant refusal/reap"));
    expect(outage).toHaveLength(1);
    expect(outage[0]).toContain("retrying in 50 ms");
    expect(outage[0]).toContain("changing holder 1");
    failing = false;
    const beforeRecovery = attempts;
    await vi.waitFor(() => expect(attempts).toBeGreaterThan(beforeRecovery), { timeout: 3_000 });
    await vi.waitFor(async () => expect((await diagnostics(session)).filter(line => line.includes("participant refusal/reap")))
      .toHaveLength(2));
    expect((await diagnostics(session)).at(-1)).toContain("mesh lock recovered");
    await directory.refresh(); // Repeated successful work must not emit another recovery.
    failing = true;
    await directory.refresh();
    const next = (await diagnostics(session)).filter(line => line.includes("participant refusal/reap"));
    expect(next).toHaveLength(3);
    expect(next[2]).toContain("retrying in 50 ms");
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
  } finally {
    await directory.close();
    mesh.closeState();
  }
});

it("keeps real queue failures visible and fail-closed, rather than treating them as transient retries", async () => {
  const session = "participant-queue-real-error";
  setup(session);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const queue = new MeshBackgroundQueue("participant refusal/reap")
    .diagnosticsOncePerOutageTo(line => logDiagnostic("src/topology/participant-directory.ts", line));
  const operation = vi.fn(() => { throw new Error("broken notification"); });
  try {
    await queue.enqueue(operation);
    await expect(queue.checkpointForRelease()).rejects.toThrow("unconfirmed publication");
    expect(operation).toHaveBeenCalledOnce();
    expect(await diagnostics(session)).toEqual(["[pi-fabric] participant refusal/reap: background operation failed: broken notification"]);
    expect(warn).not.toHaveBeenCalled();
  } finally { await queue.close(); }
});
