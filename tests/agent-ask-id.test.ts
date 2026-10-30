import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";

const ask = '["Smarty-Pants-Inc","org-feed://principal/Paul/block/12-abc"]';
const override = ' [ "Other Org", "pi-session://principal/session-1/entry/é\\u002d2" ] ';
const defaults = { runner: "pi" as const, timeoutMs: 10_000 };
const managers: AgentManager[] = [];
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Data safety: the retained provenance and digest must cover the exact submitted bytes.
describe("opaque agent askId", () => {
  it.each(["", null, 123, "ask\ninjected", "ask\rbreak", "ask\u0000control", "ask\u0085control", "a".repeat(513), "é".repeat(257)])("refuses invalid askId %#", value => {
    expect(() => normalizeAgentRunRequest({ task: "brief", askId: value }, defaults)).toThrow(/askId.*512 UTF-8 bytes.*control/);
  });
  it("accepts the byte boundary and opaque text without trimming, URI parsing, or public state spoofing", () => {
    for (const value of ["é".repeat(256), "  opaque-not-a-uri  ", ask, override]) {
      expect(normalizeAgentRunRequest({ task: "brief", askId: value, askIdState: "inherited", inheritedAskId: override }, defaults))
        .toMatchObject({ askId: value });
    }
    expect(normalizeAgentRunRequest({ task: "brief" }, defaults)).not.toHaveProperty("askId");
    expect(normalizeAgentRunRequest({ task: "brief", inheritedAskId: ask }, defaults)).not.toHaveProperty("inheritedAskId");
    expect(normalizeAgentRunRequest({ task: "brief", askId: ask, askIdState: "inherited" }, defaults)).not.toHaveProperty("askIdState");
  });

  it("keeps the marker first on an interrupted continuation without changing the original prompt hash", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ask-resume-")); roots.push(root);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true,
      sessionExport: false }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    managers.push(manager);
    const task = "RESUME_AFTER_STOP RESUME_FAIL_AFTER_STOP\noriginal brief";
    const receipt = await manager.spawn({ task, askId: ask, transport: "process" });
    expect(await manager.wait(receipt.id)).toMatchObject({ status: "failed", askId: ask, askIdState: "explicit" });
    const bytes = fs.readFileSync(receipt.promptPath!, "utf8");
    const marker = `Ask-ID: ${ask}\n`;
    expect(bytes.startsWith(marker)).toBe(true);
    expect(bytes.match(/Ask-ID:/g)).toHaveLength(1);
    expect(receipt.promptSha256).toBe(createHash("sha256").update(marker + task).digest("hex"));
    expect(manager.status(receipt.id)).toMatchObject({ inputSha256: createHash("sha256").update(bytes).digest("hex") });
    expect(bytes).toContain("[Fabric continuation]");
  });

  it.each([
    { state: "explicit", parent: undefined, value: ask, expected: ask },
    { state: "missing", parent: undefined, value: undefined, expected: null },
    { state: "inherited", parent: ask, value: undefined, expected: ask },
    { state: "explicit", parent: ask, value: override, expected: override },
  ])("carries $state through the built process worker, record, receipt and child environment", async ({ state, parent, value, expected }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ask-id-")); roots.push(root);
    vi.stubEnv("PI_FABRIC_ASK_ID", parent); vi.stubEnv("PI_FABRIC_DEPTH", "0");
    vi.stubEnv("FAKE_PI_BEHAVIOR", "child-contract");
    const warn = vi.spyOn(diagnostics, "warn");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true,
      sessionExport: false, timeoutMs: 10_000 }, { workerPath: path.resolve("dist/worker.js"),
      piBinary: path.resolve("tests/fixtures/fake-pi.mjs"), runRoot: path.join(root, "runs") });
    managers.push(manager);
    const receipt = await manager.spawn({ task: "exact brief π\n", transport: "process", extensions: false,
      ...(value === undefined ? {} : { askId: value }) });
    const identity = { askId: expected, askIdState: state };
    expect(receipt).toMatchObject(identity);
    const result = await manager.wait(receipt.id);
    expect(result.status, result.error).toBe("completed");
    expect(result).toMatchObject(identity);
    expect(JSON.parse(result.text).askId).toBe(expected); // Actual worker -> Pi process boundary.
    const bytes = fs.readFileSync(receipt.promptPath!, "utf8");
    const marker = expected === null ? "" : `Ask-ID: ${expected}\n`;
    expect(bytes).toBe(marker + "exact brief π\n");
    if (expected !== null) expect(bytes.split("\n")[0]).toBe("Ask-ID: " + expected);
    expect(receipt.promptSha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    expect(receipt.promptBytes).toBe(Buffer.byteLength(bytes));
    if (expected !== null) expect(receipt.promptSha256).not.toBe(createHash("sha256").update("exact brief π\n").digest("hex"));
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(receipt.promptPath!), "status.json"), "utf8"))).toMatchObject(identity);
    expect(warn.mock.calls.some(call => String(call[1]).includes("askId MISSING"))).toBe(state === "missing");
  }, 20_000);
});
