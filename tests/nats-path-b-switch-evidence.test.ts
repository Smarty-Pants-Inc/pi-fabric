// Keep reason: capture-admission trust boundary and private state/archive data safety (r1395 P1/PUBLIC-LOG).
// Broker-free: entry regressions provide an executable canary that MUST NOT be launched.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SwitchProcess } from "./helpers/nats-switch-runtime.js";
import { loadSwitchSeedHooks, requireSwitchSeedResult, verifySwitchCapture, preserveSwitchArchive,
  logSwitchCheckpointEntry, logSwitchPhase, type SwitchSeedHooks, type SwitchPopulationInput } from "./helpers/nats-switch-evidence.js";

// Only the inbound IPC regression uses spawn; real entry/archive probes use the untouched spawnSync.
const spawned = vi.hoisted(() => ({ child: undefined as import("node:events").EventEmitter | undefined }));
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  const { EventEmitter } = await import("node:events");
  return { ...actual, spawn: vi.fn(() => {
    const child = Object.assign(new EventEmitter(), { stderr: new EventEmitter() });
    spawned.child = child; return child;
  }) };
});
const temp = () => fs.mkdtempSync(path.join(process.env.TMPDIR!, "switch-evidence-"));
afterEach(() => { vi.restoreAllMocks(); });
const ownerId = "session:fixture-owner", senderId = "session:fixture-sender";
function population() {
  const historical = Array.from({ length: 454 }, (_, index) => ({ id: `session:captured-${index}`, stale: true }));
  const routes = () => [...historical.map(row => ({ ...row })), { id: ownerId, stale: false }, { id: senderId, stale: false }];
  const ownerRoutes = routes(), senderRoutes = routes();
  const seedResult = Object.freeze({ copiedRootsVerified: true, capturedParticipants: 454 });
  const saved = new WeakMap<object, boolean>([[seedResult, true]]);
  const evidence = { capturedOnOwner: 454, capturedOnSender: 454, historicalLiveRoutes: 0, freshLiveOnEach: 2,
    ownerTotalRoutes: 456, senderTotalRoutes: 456, checkedAt: Date.now(), elapsedMs: 0 };
  const hooks: SwitchSeedHooks = { seedSwitchFixture: () => seedResult, verifySwitchPopulation(input) {
    expect(saved.has(input.seedResult as object)).toBe(true);
    expect(input.ownerRoutes).toBe(ownerRoutes); expect(input.senderRoutes).toBe(senderRoutes);
    return evidence;
  } };
  const input: SwitchPopulationInput = { seedResult, ownerRoutes, senderRoutes, ownerId, senderId, carrier: "nats" };
  return { hooks, input, evidence, ownerRoutes, senderRoutes };
}

describe("capture-backed admission", () => {
  it("does not admit an absent seed module", async () => {
    expect(await loadSwitchSeedHooks(undefined)).toBeUndefined();
    expect(await loadSwitchSeedHooks("   ")).toBeUndefined();
  });
  it.each([undefined, null, false, true, 1, "claim", [], {}])("rejects empty or meaningless seed result %j", value => {
    expect(() => requireSwitchSeedResult(value)).toThrow("not capture-backed");
  });
  it("keeps private WeakMap-compatible seed identity and verifies both actual 454+2 native populations", async () => {
    const p = population();
    const seedResult = await p.hooks.seedSwitchFixture({} as Parameters<SwitchSeedHooks["seedSwitchFixture"]>[0]);
    expect(seedResult).toBe(p.input.seedResult);
    expect(await verifySwitchCapture(p.hooks, { ...p.input, seedResult })).toEqual(p.evidence);
  });
  it.each([undefined, null, false, true, {}, { captureBacked: true }, { capturedOnOwner: 454 },
    { capturedOnOwner: -1, capturedOnSender: -1 }])("rejects invalid verifier evidence %j", async value => {
    const p = population(); p.hooks.verifySwitchPopulation = () => value;
    await expect(verifySwitchCapture(p.hooks, p.input)).rejects.toThrow("not capture-backed");
  });
  it.each(["capturedOnOwner", "capturedOnSender", "historicalLiveRoutes", "freshLiveOnEach", "ownerTotalRoutes", "senderTotalRoutes", "checkedAt", "elapsedMs"])("rejects false count/timing claim %s", async key => {
    const p = population(); p.hooks.verifySwitchPopulation = () => ({ ...p.evidence, [key]: -1 });
    await expect(verifySwitchCapture(p.hooks, p.input)).rejects.toThrow("not capture-backed");
  });
  it("does not trust decorated claims when historical native routes are actually live", async () => {
    const p = population(); p.senderRoutes[0]!.stale = false;
    await expect(verifySwitchCapture(p.hooks, p.input)).rejects.toThrow("historical native routes must be stale");
  });
  it("rejects unequal historical IDs even when counts match", async () => {
    const p = population(); p.senderRoutes[0]!.id = "session:different-capture";
    await expect(verifySwitchCapture(p.hooks, p.input)).rejects.toThrow("populations differ");
  });
  it("requires both fresh native owners, not just a positive historical count", async () => {
    const p = population(); p.ownerRoutes.pop();
    await expect(verifySwitchCapture(p.hooks, p.input)).rejects.toThrow("both fresh routes required");
  });
  it("allowlists verifier evidence rather than publishing arbitrary hook fields", async () => {
    const p = population(); p.hooks.verifySwitchPopulation = () => ({ ...p.evidence, privateState: "HOOK_STATE_SENTINEL" });
    expect(await verifySwitchCapture(p.hooks, p.input)).toEqual(p.evidence);
  });
});

describe.skipIf(process.platform !== "linux")("actual R2 entry without native broker launch", () => {
  function entry(seedSource?: string) {
    const root = temp(), launched = path.join(root, "broker-launched"), executable = path.join(root, "nats-server"), artifacts = path.join(root, "artifacts");
    fs.mkdirSync(artifacts, { mode: 0o700 });
    fs.writeFileSync(executable, `#!/bin/sh\nprintf launched > '${launched}'\nexit 99\n`, { mode: 0o700 });
    const env: NodeJS.ProcessEnv = { ...process.env, PI_FABRIC_NATS_REHEARSAL: "1", NATS_AUTH_REQUIRED: "1", NATS_BIN: executable,
      NATS_SWITCH_PROBE_MODULE: path.resolve("tests/helpers/nats-switch-probe.ts"), NATS_SWITCH_ARTIFACT_DIR: artifacts };
    delete env.NATS_SWITCH_SEED_MODULE;
    if (seedSource !== undefined) {
      const module = path.join(root, "seed.mjs"); fs.writeFileSync(module, seedSource, { mode: 0o600 });
      Object.assign(env, { NATS_SWITCH_SEED_MODULE: module });
    }
    const result = spawnSync(process.execPath, [path.resolve("node_modules/vitest/vitest.mjs"), "run",
      "tests/nats-path-b-switch-rollback.test.ts", "-t", "R2 full capture-backed", "--reporter=json"],
    { cwd: process.cwd(), env, encoding: "utf8", timeout: 45_000, maxBuffer: 2 * 1024 * 1024 });
    expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
    expect(fs.existsSync(launched)).toBe(false); expect(fs.readdirSync(artifacts)).toEqual([]);
    expect(result.stdout).not.toContain('"type":"switch-r2-report"');
    expect(result.stdout).not.toContain('"captureBacked":true');
    const json = JSON.parse(result.stdout) as { numPassedTests: number; testResults: Array<{ assertionResults: Array<{ title: string; status: string; meta: Record<string, unknown>; failureMessages: string[] }> }> };
    const r2 = json.testResults.flatMap(row => row.assertionResults).find(row => row.title.startsWith("R2 full capture-backed"));
    expect(r2).toBeDefined();
    return { result, json, r2: r2! };
  }
  it("no seed skips as not capture-backed before fixture allocation and never reports capture PASS", () => {
    const { result, json, r2 } = entry();
    expect(result.status).toBe(0); expect(json.numPassedTests).toBe(0); expect(r2.status).toBe("skipped");
    expect(r2.meta.captureAdmission).toContain("not capture-backed");
  }, 60_000);
  it("explicit seed missing its verifier fails as not capture-backed before fixture allocation", () => {
    const { result, r2 } = entry("export function seedSwitchFixture() { return { seeded: true }; }\n");
    expect(result.status).toBe(1); expect(r2.status).toBe("failed");
    expect(r2.failureMessages.join("\n")).toContain("not capture-backed");
  }, 60_000);
});

describe("private evidence public projections", () => {
  it("actual SwitchProcess IPC entry logs only checkpoint metadata while retaining full private events", () => {
    const marker = "PRIVATE_IPC_STATE_SENTINEL_b7c9", info = vi.spyOn(console, "info").mockImplementation(() => {});
    const runtime = new SwitchProcess("runtime", "unused-config", temp());
    const reply = { event: "checkpoint-trace", result: { stage: "entry", entryKind: "releaseCheckpointReceipt",
      sequence: 2, meshRoot: marker, identityId: marker, stack: marker, state: { key: marker } } };
    spawned.child!.emit("message", reply);
    expect(runtime.events).toEqual([reply]); // Private IPC evidence is not redacted or discarded.
    expect(info.mock.calls.map(row => JSON.parse(String(row[0])))).toEqual([
      { type: "switch-checkpoint-entry", entryKind: "releaseCheckpointReceipt", sequence: 2 },
    ]);
    expect(info.mock.calls.flat().join(" ")).not.toContain(marker);
  });
  it("actual runtime checkpoint and phase log entry functions do not emit IPC/state/terminal sentinels", () => {
    const marker = "PRIVATE_STATE_SENTINEL_6f82", info = vi.spyOn(console, "info").mockImplementation(() => {});
    logSwitchCheckpointEntry({ entryKind: "checkpointForRelease", sequence: 1, meshRoot: marker,
      identityId: marker, incarnation: marker, stack: marker, state: { key: marker } });
    logSwitchPhase({ phase: "stop-all-completed", at: 10, elapsedMs: 4, phaseMs: 2,
      evidence: { stderr: marker, closeResult: { state: marker } } } as Parameters<typeof logSwitchPhase>[0]);
    const output = info.mock.calls.map(row => row.join(" ")).join("\n");
    expect(output).not.toContain(marker); expect(output).not.toContain(Buffer.from(marker).toString("base64"));
    expect(info.mock.calls.map(row => JSON.parse(String(row[0])))).toEqual([
      { type: "switch-checkpoint-entry", entryKind: "checkpointForRelease", sequence: 1 },
      { type: "switch-phase", label: "pre-projection", phase: "stop-all-completed", at: 10, elapsedMs: 4, phaseMs: 2 },
    ]);
  });
  it.skipIf(process.platform !== "linux")("real private state archive exists with hash/path, never raw/base64/chunk transport in stdout", () => {
    const root = temp(), marker = "PRIVATE_ARCHIVE_STATE_SENTINEL_94d7", state = JSON.stringify({ privateKey: marker, capture: marker });
    fs.writeFileSync(path.join(root, "state.json"), state, { mode: 0o600 });
    const lines: string[] = [];
    const info = vi.spyOn(console, "info").mockImplementation(line => {
      lines.push(String(line));
      const publicReceipt = JSON.parse(String(line));
      expect(Object.keys(publicReceipt).sort()).toEqual(["privateArchive", "sha256"]);
      // Artifact is actually readable at the logging callback, not merely promised after teardown.
      const data = fs.readFileSync(publicReceipt.privateArchive);
      expect(createHash("sha256").update(data).digest("hex")).toBe(publicReceipt.sha256);
    });
    const receipt = preserveSwitchArchive(root, []), archive = fs.readFileSync(receipt.privateArchive);
    expect(fs.statSync(receipt.privateArchive).mode & 0o777).toBe(0o600);
    const listing = spawnSync("tar", ["-xOzf", receipt.privateArchive, `${path.basename(root)}/state.json`], { encoding: "utf8", timeout: 5_000 });
    expect(listing.status).toBe(0); expect(listing.stdout).toBe(state);
    const output = lines.join("\n");
    expect(output).not.toContain(marker); expect(output).not.toContain(state);
    expect(output).not.toContain(Buffer.from(marker).toString("base64"));
    expect(output).not.toContain(archive.toString("base64")); expect(output).not.toContain("switch-archive-chunk");
    expect(info).toHaveBeenCalledTimes(1); expect(output).toContain(receipt.sha256); expect(output).toContain(receipt.privateArchive);
  });
});
