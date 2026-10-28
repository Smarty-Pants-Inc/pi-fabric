import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";

// Exercise close itself, not the separate detached retention sweep.
vi.mock("../src/storage/retention.js", async original => ({
  ...await original<typeof import("../src/storage/retention.js")>(),
  claimTempRunSweep: () => false,
}));

const roots: string[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  try {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) {
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }
});

describe("agent run retention", () => {
  it("defaults to retaining runs and preserves explicit true and false during normalization", () => {
    expect(DEFAULT_FABRIC_CONFIG.agents.retainRuns).toBe(true);
    expect(normalizeFabricConfig({}).agents.retainRuns).toBe(true);
    expect(normalizeFabricConfig({ agents: {} }).agents.retainRuns).toBe(true);
    expect(normalizeFabricConfig({ agents: { retainRuns: true } }).agents.retainRuns).toBe(true);
    expect(normalizeFabricConfig({ agents: { retainRuns: false } }).agents.retainRuns).toBe(false);
  });

  it.each([
    { name: "default", agents: {}, retained: true },
    { name: "explicit false", agents: { retainRuns: false }, retained: false },
  ])("$name governs completed run files across manager shutdown", async ({ agents, retained }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-retain-runs-"));
    roots.push(root);
    // Like the manager fixtures, use a managed temporary root, isolated from
    // the caller's inherited run tree, budget, session store and systemd scope.
    vi.spyOn(os, "tmpdir").mockReturnValue(root);
    vi.stubEnv("PI_FABRIC_TMPDIR", root);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
      "SMARTY_SESSION_STORE", "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
    const config = normalizeFabricConfig({ agents: { ...agents, budgetUsd: 0, sessionExport: false } });
    const manager = new AgentManager(root, config.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      preparePiModel: async model => model,
      rootSessionCwd: () => root,
      shortTmpdirSource: () => ({ tmpdir: root }),
      fullCodeMode: false,
    });
    managers.push(manager);

    const prompt = "Retain this completed run, exact prompt bytes: π\r\n";
    const result = await manager.run({ task: prompt, model: "test/retain-runs", transport: "process", extensions: false });
    expect(result.status).toBe("completed");
    const directory = manager.runDirectory(result.id)!;
    expect(path.relative(root, directory)).not.toMatch(/^\.\./);
    expect(fs.readFileSync(path.join(directory, "task.txt"))).toEqual(Buffer.from(prompt));
    const events = fs.readFileSync(path.join(directory, "events.jsonl"));
    expect(events.toString()).toContain('"type":"agent_settled"');
    // The fake worker has no model-backed Pi session; seed a binary-safe session
    // marker so close must preserve arbitrary transcript bytes, not just status.
    const session = Buffer.from('session-marker π\r\n\u0000end\n');
    fs.writeFileSync(path.join(directory, "session.jsonl"), session);
    const files = new Map([
      ["task.txt", Buffer.from(prompt)],
      ["events.jsonl", events],
      ["session.jsonl", session],
    ]);

    await manager.close();

    expect(fs.existsSync(directory)).toBe(retained);
    if (retained) {
      for (const [file, bytes] of files) expect(fs.readFileSync(path.join(directory, file))).toEqual(bytes);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
        .toMatchObject({ id: result.id, status: "completed" });
    } else {
      for (const file of files.keys()) expect(fs.existsSync(path.join(directory, file))).toBe(false);
    }
  });
});
