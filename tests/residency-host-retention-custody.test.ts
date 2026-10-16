import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ResidentRequestRetention } from "../src/residency/retention.js";

// Keep reason: #1122's Windows data-safety regression was in the host wiring,
// not the collector. Construct the real host without starting an OS service.
vi.mock("../src/storage/retention-platform.js", () => ({ retentionV2Enabled: () => false }));
const recorded = vi.hoisted(() => ({
  collectors: [] as ResidentRequestRetention[],
  mutations: [] as Array<{ target: string; deleter: string; judged: unknown }>,
  compactions: [] as string[],
}));
vi.mock("../src/residency/retention.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/residency/retention.js")>();
  return {
    ...actual,
    ResidentRequestRetention: class extends actual.ResidentRequestRetention {
      constructor(...args: ConstructorParameters<typeof actual.ResidentRequestRetention>) {
        super(...args);
        recorded.collectors.push(this);
      }
    },
  };
});
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => ({
  ...await importOriginal<typeof import("../src/storage/verified-remove.js")>(),
  // Never invoke proceed/fallback/native removal, including event replacement.
  verifiedRemove: (target: string, judged: unknown, options: { deleter: string }) => {
    recorded.mutations.push({ target, judged, deleter: options.deleter });
    return { removed: false, reason: "recorded only (test)" };
  },
}));
vi.mock("../src/storage/retention.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/retention.js")>();
  return {
    ...actual,
    compactTerminalRunEvents: (...args: Parameters<typeof actual.compactTerminalRunEvents>) => {
      recorded.compactions.push(args[0]);
      return actual.compactTerminalRunEvents(...args);
    },
  };
});

const { AgentManager } = await import("../src/agents/manager.js");
const { ProcessTransport } = await import("../src/agents/transports/process-transport.js");
const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
const { ResidentHost } = await import("../src/residency/host.js");
const { acknowledgeResidentResponse } = await import("../src/residency/protocol.js");
const { newResidentRequestId } = await import("../src/residency/request-expiry.js");
const { fakeManagerHeapWorker } = await import("./fixtures/manager-heap-probe.js");
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import type { AgentRunResult } from "../src/agents/types.js";

const DAY = 86_400_000;
const writeJson = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const snapshot = (root: string): unknown => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name);
  return [name, fs.lstatSync(file).isDirectory() ? snapshot(file) : fs.readFileSync(file, "utf8")];
});
afterEach(() => {
  vi.restoreAllMocks();
  recorded.collectors.length = 0;
  recorded.mutations.length = 0;
  recorded.compactions.length = 0;
  // No teardown deletion: the invoking owner retains these private roots.
});

describe("ResidentHost legacy retention custody wiring", () => {
  it.each([false, true])("vetoes a real settled manager handle while unrelated runs progress (retainRuns=%s)", async retainRuns => {
    // Record every direct removal too, before even creating the manager.
    const direct = [
      vi.spyOn(fs, "rmSync").mockImplementation(() => {}),
      vi.spyOn(fs, "unlinkSync").mockImplementation(() => {}),
      vi.spyOn(fs, "rmdirSync").mockImplementation(() => {}),
    ];
    const directAsync = vi.spyOn(fs.promises, "rm").mockImplementation(async () => {});
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "residency-host-retention-custody-"));
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:host-custody", sessionId: "host-custody", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
      sessionActorRoot: path.join(root, "session-actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: false,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns, sessionExport: false },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("src/index.ts"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    for (const actorRoot of [config.actorRoot, config.sessionActorRoot!]) {
      writeJson(path.join(actorRoot, "actors.json"), { actors: [] });
    }
    const host = new ResidentHost(config);
    expect(host.agents).toBeUndefined(); // Production initializes this only on start.
    expect(recorded.collectors).toHaveLength(1);
    const collector = recorded.collectors[0]!;
    expect(collector.root).toBe(config.residencyRoot);
    const manager = new AgentManager(root, config.agents, {
      workerPath: config.workerPath, runRoot: path.join(config.residencyRoot, "runs"), fullCodeMode: false,
    });
    host.agents = manager; // Public production property; no test-only host API.
    try {
      const result = await manager.run({ task: "settled owned result", transport: "process", extensions: false });
      const owned = manager.runDirectory(result.id)!;
      expect(result.status).toBe("completed");
      expect(manager.hasRunCustody(result.id)).toBe(true);
      expect(manager.hasRunCustody("unrelated")).toBe(false);
      const seed = (value: AgentRunResult, writeStatus: boolean) => {
        const run = path.join(config.residencyRoot, "runs", value.id);
        if (writeStatus) writeJson(path.join(run, "status.json"), value);
        writeJson(path.join(run, "reply.json"), { text: value.text });
        fs.writeFileSync(path.join(run, "events.jsonl"), Array.from({ length: 401 },
          (_, sequence) => JSON.stringify({ sequence }) + "\n").join(""), { mode: 0o600 });
        writeJson(path.join(config.residencyRoot, "agents", `${value.id}.json`), {
          format: 1, id: value.id, rootId: config.rootId, runDirectory: run,
          createdAt: value.startedAt, updatedAt: value.updatedAt,
          handle: { id: value.id, name: value.name, cwd: root, runner: "pi", transport: "process", status: value.status, residency: "durable" },
        });
        writeJson(path.join(config.residencyRoot, "results", `${value.id}.json`), value);
        return run;
      };
      seed(result, false); // Preserve the real manager's authenticated status/result.
      const unrelated = seed({ ...result, id: "unrelated" }, true);
      // An acknowledged exchange referring to the owned run remains collectable
      // off V2: run custody must not accidentally enable reference custody.
      const requestId = newResidentRequestId(Date.now());
      // Seed the durable fence directly: recording temporary unlink after the
      // hard-link CAS would leave nlink=2 and correctly veto exchange collection.
      writeJson(path.join(config.residencyRoot, "decisions", `${requestId}.json`), {
        requestFormat: 3, operation: "foreground", requestId, state: "committed", id: result.id, ownerHostId: "host",
      });
      const response = { format: 1 as const, requestId, ok: true, completedAt: Date.now() };
      writeJson(path.join(config.residencyRoot, "responses", `${requestId}.json`), response);
      expect(acknowledgeResidentResponse(config.residencyRoot, response, response.completedAt, 3)).toBe(true);
      const before = snapshot(path.join(config.residencyRoot, "runs"));
      const resultsBefore = snapshot(path.join(config.residencyRoot, "results"));
      const recover = vi.spyOn(manager, "recoverPendingArchives");
      const reference = vi.spyOn(manager, "retentionCustodyVeto");
      const custody = vi.spyOn(manager, "hasRunCustody");
      const nativeExitChecks = vi.spyOn(process, "kill");
      // Admission's recorded transient-file cleanup is not a sweep mutation.
      for (const spy of direct) spy.mockClear();
      directAsync.mockClear();
      const now = Date.now() + 4 * DAY;
      collector.sweep(now, new Set(), 10_000);
      expect(collector.due(now)).toBe(false);
      expect(custody.mock.calls.map(([id]) => id).sort()).toEqual([result.id, "unrelated"].sort());
      expect(recover.mock.calls.map(([run]) => run)).toEqual([unrelated]);
      expect(reference).not.toHaveBeenCalled();
      expect(recorded.mutations).toContainEqual({
        target: path.join(config.residencyRoot, "responses", `${requestId}.json`),
        deleter: "residentRequestCollect", judged: expect.anything(),
      });
      expect(recorded.compactions).toEqual(retainRuns ? [unrelated] : []);
      expect(recorded.mutations.filter(({ deleter }) => deleter !== "residentRequestCollect")).toEqual([{
        target: retainRuns ? path.join(unrelated, "events.jsonl") : unrelated,
        deleter: retainRuns ? "compactTerminalRunEvents" : "residentRunRetention",
        judged: expect.objectContaining({ ino: expect.any(Number), dev: expect.any(Number) }),
      }]);
      expect(nativeExitChecks).toHaveBeenCalledWith(2147483647, 0);
      expect(recorded.mutations.some(({ target }) => target === owned || target.startsWith(owned + path.sep))).toBe(false);
      for (const spy of direct) {
        expect(spy.mock.calls.filter(([target]) => String(target).startsWith(path.join(config.residencyRoot, "runs") + path.sep))).toEqual([]);
      }
      expect(directAsync.mock.calls.filter(([target]) => String(target).startsWith(path.join(config.residencyRoot, "runs") + path.sep))).toEqual([]);
      expect(snapshot(path.join(config.residencyRoot, "runs"))).toEqual(before);
      expect(snapshot(path.join(config.residencyRoot, "results"))).toEqual(resultsBefore);
      expect(manager.status(result.id)).toMatchObject(result);
    } finally {
      collector.close();
      await manager.close(); // All removal effects remain intercepted through close.
    }
  });
});
