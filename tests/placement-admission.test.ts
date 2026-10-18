import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CliPlacementDecider, PlacementAdmissionError, createPlacementDecider,
  type PlacementDecider, type PlacementGrant, type PlacementNativeIdentity,
  type PlacementPlace, type PlacementQueue, type PlacementReleaseProof,
  type PlacementRequest, type PlacementShadowObservation,
} from "../src/agents/placement-admission.js";
import {
  PLACEMENT_MAX_BYTES, decodePlacementAnswer, parsePlacementJson,
  placementRegistrationRequest, placementReleaseRequest, validatePlacementBindings,
  validatePlacementRequest,
} from "../src/agents/placement-contract.js";
import { nativePlacementEnforceAvailable, nativePlacementPathAvailable, normalizePlacementDeciderConfig, VERIFIED_CALLER_CAPABILITY, type PlacementDeciderConfig } from "../src/agents/placement-decider-config.js";
import { loadFabricConfig, loadFabricConfigForScope, normalizeFabricConfig } from "../src/config.js";

// Keep reason: a security trust boundary. Exercise a real shell-free child and
// actual stdin/stdout/exits; these are not native writer-authentication proofs.
const roots: string[] = [];
const helperPidFiles: string[] = [];
const root = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "placement-admission-"));
  roots.push(dir);
  return dir;
};
const waitForProcessExit = async (pid: number): Promise<void> => {
  const deadline = performance.now() + 2_000;
  while (performance.now() < deadline) {
    try { process.kill(pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return; }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error(`placement test helper process survived cleanup: ${pid}`);
};
const killTrackedHelpers = async (): Promise<void> => {
  let failure: unknown;
  for (const file of helperPidFiles.splice(0)) {
    if (!fs.existsSync(file)) continue;
    const pid = Number(fs.readFileSync(file, "utf8"));
    if (!Number.isSafeInteger(pid) || pid < 1) continue;
    try { process.kill(pid, "SIGKILL"); } catch { /* Already exited. */ }
    try { await waitForProcessExit(pid); } catch (error) { failure ??= error; }
  }
  if (failure) throw failure;
};
afterEach(async () => {
  try { await killTrackedHelpers(); }
  finally {
    vi.restoreAllMocks();
    for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
  }
});
const sleepingHelper = (dir: string, detached: boolean, inheritedPipes: boolean) => {
  const script = path.join(dir, "sleeping-helper.cjs");
  const helperPidFile = path.join(dir, "helper.pid");
  const grandchildPidFile = path.join(dir, "grandchild.pid");
  helperPidFiles.push(helperPidFile, grandchildPidFile);
  const stdio = inheritedPipes ? `["ignore", "inherit", "inherit"]` : `"ignore"`;
  fs.writeFileSync(script, `const fs = require('node:fs');
const { spawn } = require('node:child_process');
fs.writeFileSync(${JSON.stringify(helperPidFile)}, String(process.pid));
const grandchild = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: ${detached}, stdio: ${stdio} });
fs.writeFileSync(${JSON.stringify(grandchildPidFile)}, String(grandchild.pid));
setInterval(() => {}, 1000);
`);
  return { script, helperPidFile, grandchildPidFile };
};
const expectPidGone = (file: string): void => {
  const pid = Number(fs.readFileSync(file, "utf8"));
  let alive = false;
  try { process.kill(pid, 0); alive = true; }
  catch (error) { alive = (error as NodeJS.ErrnoException).code !== "ESRCH"; }
  expect(alive, `helper PID ${pid} should be gone`).toBe(false);
};
const digest = `sha256:${"a".repeat(64)}`;
const request = (): PlacementRequest => ({
  schema: "smarty.placement.v1", operation: "decide", launch_id: "semantic-launch", agent_name: "task-agent-1", generation: 4,
  mode: "enforced", assignment: { org_id: "org-1", work_id: "issue-1" }, role: "task-agent", complexity: "complex",
  required_profile: "approved-complex-task", interaction: { class: "background-agent", origin_ref: "authenticated-event" },
  requirements: { capabilities: ["typescript"], runtime: "pi-rpc", source_pin: "source-sha", memory_profile_ref: "measured-task-envelope" },
  requested: { host: "host-1", model: "provider/model-1", thinking: "xhigh" },
});
const place = (): PlacementPlace => ({
  schema: "smarty.placement.v1", decision: "PLACE", mode: "enforced", launch_id: "semantic-launch", generation: 4, org_id: "org-1",
  decision_revision: 3, host: "host-1", model: "provider/model-1", thinking: "xhigh", route_id: "route-1", pool_id: "pool-1",
  gateway_policy_digest: digest, reason: "CHEAPEST_ELIGIBLE", evidence: [], reservation_id: "reservation-1", fence: 8,
  create_before: new Date(Date.now() + 60_000).toISOString(),
  provenance: { policy_digest: digest, facts_digest: digest, writer_epoch: 9 },
  grant: {
    schemaVersion: 2, status: "CLAIMED", requestId: "claim-operation-not-semantic-id", allocationRequestId: "semantic-launch",
    requesterSession: "original-requester-session", launchId: "native-launch-not-semantic-id", launchSpecId: "immutable-native-spec",
    configDigest: "b".repeat(64), expiresAt: new Date(Date.now() + 120_000).toISOString(), reservationId: "reservation-1", fence: 8,
    host: "host-1", model: "provider/model-1", thinking: "xhigh", requesterRef: "protected-requester", drainerSpawnerRef: "original-wake-owner",
    launchAllowed: true,
  },
});
const queue = (): PlacementQueue => ({
  schema: "smarty.placement.v1", decision: "QUEUE", mode: "enforced", launch_id: "semantic-launch", generation: 4,
  reason: "AGENT_CAPACITY_EXHAUSTED", retry_after: 4, evidence: [], queue_id: "writer-acked-position",
});
const shadow = (): PlacementShadowObservation => ({
  schema: "smarty.placement.shadow.v1", kind: "SHADOW_OBSERVATION", launch_id: "semantic-launch", generation: 4,
  pool_status: "NOT_ENFORCED", hypothetical: { decision: "PLACE", host: "host-1", model: "provider/model-1", thinking: "xhigh", pool_id: "pool-1" },
  actual_baseline_ref: null, observation_id: "observation-1", at: new Date().toISOString(), launch_kind: "task", org_id: "org-1",
  owner_ref: "owner-1", agent_ref: "agent-1", policy_digest: digest, facts_digest: digest,
});
interface Plan { reply?: unknown; stdout?: string; code?: number; stderr?: string; delay?: number; split?: boolean; invalidUtf8?: boolean }
const cli = (dir: string, plan: Plan, mode: PlacementDeciderConfig["mode"] = "enforce"): CliPlacementDecider => {
  const script = path.join(dir, "fake cli.cjs");
  const planFile = path.join(dir, "plan.json");
  fs.writeFileSync(planFile, JSON.stringify(plan));
  fs.writeFileSync(script, `const fs = require('node:fs');
const plan = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const record = process.argv[3];
let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  fs.appendFileSync(record, input + '\\n');
  const output = plan.stdout === undefined ? JSON.stringify(plan.reply) : plan.stdout;
  if (plan.stderr) process.stderr.write(plan.stderr);
  const done = () => {
    if (plan.invalidUtf8) process.stdout.write(Buffer.from([0xff]));
    process.stdout.write(plan.split ? output.slice(1) : output);
    process.exitCode = plan.code || 0;
  };
  if (plan.split) process.stdout.write(output.slice(0, 1));
  if (plan.delay) setTimeout(done, plan.delay); else done();
});
`);
  return new CliPlacementDecider({ command: [process.execPath, script, planFile, path.join(dir, "requests.jsonl")], mode, wakeSourceMachine: "ryzen1" });
};
const captured = (dir: string): unknown[] => fs.readFileSync(path.join(dir, "requests.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
const rejects = async (pending: Promise<unknown>, reason = "invalid-response"): Promise<void> => {
  try { await pending; throw new Error("Expected refusal"); }
  catch (error) { expect(error).toBeInstanceOf(PlacementAdmissionError); expect((error as PlacementAdmissionError).reason).toBe(reason); }
};

describe("placement CLI admission", () => {
  it("preserves complete CLAIMED PLACE and prompt-free request over literal argv/stdin", async () => {
    const dir = root(); const expected = place(); const intended = request();
    const decider: PlacementDecider = cli(dir, { reply: expected, stderr: "diagnostic only: not JSON\n" });
    const result = await decider.decide(intended);
    expect(result).toEqual(expected);
    expect(captured(dir)).toEqual([intended]);
    expect(result).not.toHaveProperty("task");
    expect(Object.isFrozen(result)).toBe(true);
    if (result.decision !== "PLACE") throw new Error("Missing PLACE");
    const grant: PlacementGrant = result;
    expect(grant.grant.allocationRequestId).toBe(intended.launch_id);
    expect(grant.grant.requestId).not.toBe(intended.launch_id);
    expect(grant.grant.launchId).not.toBe(intended.launch_id);
    expect(Object.isFrozen(grant.grant)).toBe(true);
  });
  it("returns QUEUE at exit 75 without any launch/grant authority", async () => {
    const q = await cli(root(), { reply: queue(), code: 75 }).decide(request());
    expect(q).toEqual(queue());
    expect(q).not.toHaveProperty("grant");
    expect(q).not.toHaveProperty("create_before");
  });
  it("allows anonymous unavailability QUEUE without inventing queue ACK/provenance", async () => {
    const offline = { schema: "smarty.placement.v1", decision: "QUEUE", mode: "enforced", reason: "DECIDER_UNAVAILABLE", retry_after: 0, evidence: [] };
    const result = await cli(root(), { reply: offline, code: 75 }).decide(request());
    expect(result).toEqual(offline);
    expect(result).not.toHaveProperty("queue_id");
    expect(result).not.toHaveProperty("provenance");
  });
  it("separates shadow observation from live decide and never sends wire mode shadow", async () => {
    const dir = root(); const s = shadow(); const decider = cli(dir, { reply: s, code: 78 }, "shadow");
    await rejects(decider.decide(request()), "mode-mismatch");
    expect(fs.existsSync(path.join(dir, "requests.jsonl"))).toBe(false);
    expect(await decider.observe(request())).toEqual(s);
    expect(captured(dir)).toEqual([request()]);
    await rejects(cli(root(), { reply: s, code: 78 }).decide(request()), "mode-mismatch");
    await rejects(cli(root(), { reply: place() }, "shadow").observe(request()), "mode-mismatch");
  });
  it("off/default factories invoke no CLI, and observation cannot run in enforce", async () => {
    const dir = root();
    await rejects(createPlacementDecider().decide(request()), "disabled");
    await rejects(cli(dir, { reply: place() }, "off").decide(request()), "disabled");
    await rejects(cli(dir, { reply: place() }).observe(request()), "mode-mismatch");
    expect(fs.existsSync(path.join(dir, "requests.jsonl"))).toBe(false);
  });
  it.each([
    [0, queue()], [75, place()], [78, place()], [1, place()], [74, place()], [null, place()],
  ])("refuses exit/schema mismatch %s without fallback", (exitCode, reply) => {
    expect(() => decodePlacementAnswer(JSON.stringify(reply), exitCode as number | null, request())).toThrow();
  });
  it.each([
    "not JSON", "{}", "null", "[]", "{}\n{}", "diagnostic\n{}", "{\"retry_after\":1e400}",
    "{\"decision\":\"PLACE\",\"decisi\\u006fn\":\"PLACE\"}",
  ])("fails closed on malformed/invalid stdout %s", async stdout => {
    await rejects(cli(root(), { stdout }).decide(request()));
  });
  it("refuses mismatched PLACE and non-success through the actual child-process boundary", async () => {
    const p = place(); p.grant.allocationRequestId = p.grant.requestId;
    await rejects(cli(root(), { reply: p }).decide(request()));
    await rejects(cli(root(), { reply: place(), code: 1 }).decide(request()));
    await rejects(cli(root(), { reply: queue(), code: 0 }).decide(request()));
  });
  it("rejects duplicate nested keys even when a complete otherwise valid PLACE follows", async () => {
    const stdout = JSON.stringify(place()).replace('"schemaVersion":2', '"schemaVersion":2,"schemaVersion":2');
    await rejects(cli(root(), { stdout }).decide(request()));
  });
  it("refuses invalid UTF-8 rather than normalizing authority-bearing identifiers", async () => {
    await rejects(cli(root(), { reply: place(), invalidUtf8: true }).decide(request()));
  });
  it("bounds stdout at 1 MiB while separately draining stderr diagnostics", async () => {
    await rejects(cli(root(), { stdout: " ".repeat(PLACEMENT_MAX_BYTES + 1) }).decide(request()));
    const p = place();
    expect(await cli(root(), { reply: p, stderr: "x".repeat(128 * 1024) }).decide(request())).toEqual(p);
  });
  it("uses one fixed total two-second deadline even after partial stdout", async () => {
    const start = performance.now();
    await rejects(cli(root(), { reply: place(), delay: 10_000, split: true }).decide(request()), "timeout");
    expect(performance.now() - start).toBeLessThan(3_000);
  }, 5_000);
  it.skipIf(process.platform === "win32")("waits for the whole POSIX process group before resolving timeout refusal", async () => {
    const dir = root();
    const helper = sleepingHelper(dir, false, false);
    const decider = new CliPlacementDecider({ command: [process.execPath, helper.script], mode: "enforce", wakeSourceMachine: "ryzen1" });
    const nativeKill = process.kill.bind(process);
    let groupKillDelivered = false;
    let groupProbeCalled = false;
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (typeof pid === "number" && pid < 0 && signal === 0) groupProbeCalled = true;
      if (typeof pid === "number" && pid < 0 && signal === "SIGKILL") {
        setTimeout(() => { try { nativeKill(pid, signal); groupKillDelivered = true; } catch { /* Assertion reports failure. */ } }, 100);
        return true;
      }
      return nativeKill(pid, signal);
    });
    await rejects(decider.decide(request(), { deadline: performance.now() + 1_000 }), "timeout");
    expect(groupKillDelivered).toBe(true);
    expect(groupProbeCalled).toBe(true);
    expectPidGone(helper.grandchildPidFile);
  }, 8_000);
  it("reports an unconfirmed helper teardown instead of a clean timeout", async () => {
    const dir = root();
    const helper = sleepingHelper(dir, process.platform !== "win32", true);
    const decider = new CliPlacementDecider({ command: [process.execPath, helper.script], mode: "enforce", wakeSourceMachine: "ryzen1" });
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    await rejects(decider.decide(request(), { deadline: performance.now() + 1_000 }), "helper-teardown-unconfirmed");
    const helperPid = Number(fs.readFileSync(helper.helperPidFile, "utf8"));
    expect(warning).toHaveBeenCalledOnce();
    expect(warning.mock.calls[0]?.[0]).toContain(`pid=${helperPid}`);
    expect(warning.mock.calls[0]?.[0]).toContain(process.platform === "win32" ? "pgid=unavailable" : `pgid=${helperPid}`);
  }, 8_000);
  it("snapshots request bindings before asynchronous CLI IO", async () => {
    const dir = root(); const intended = request(); const pending = cli(dir, { reply: place(), delay: 20 }).decide(intended);
    intended.launch_id = "mutation-after-invocation";
    intended.requested!.host = "wrong-host";
    expect((await pending).decision).toBe("PLACE");
    expect(captured(dir)).toEqual([request()]);
  });
  it.each([
    ["prompt text", (r: Record<string, unknown>) => { r.task = "private prompt must not be serialized"; }],
    ["wire shadow", (r: Record<string, unknown>) => { r.mode = "shadow"; }],
    ["ambiguous tier", (r: Record<string, unknown>) => { r.complexity = "standard"; }],
    ["fractional generation", (r: Record<string, unknown>) => { r.generation = 1.5; }],
    ["expired deadline", (r: Record<string, unknown>) => { r.deadline_at = "2020-01-01T00:00:00Z"; }],
  ])("refuses invalid request %s before invoking executable", async (_name, mutate) => {
    const dir = root(); const r = request(); mutate(r as unknown as Record<string, unknown>);
    await rejects(cli(dir, { reply: place() }).decide(r), "invalid-request");
    expect(fs.existsSync(path.join(dir, "requests.jsonl"))).toBe(false);
  });
});

describe("closed placement wire and binding validation", () => {
  it.each([
    ["launch_id", "other"], ["generation", 5], ["org_id", "other"], ["host", "other"], ["model", "other"],
    ["thinking", "high"], ["reservation_id", "other"], ["fence", 9], ["decision_revision", 0],
    ["create_before", "2026-02-30T00:00:00Z"], ["gateway_policy_digest", "not-a-digest"],
    ["extra", true], ["mode", "shadow"],
  ])("rejects mismatched/malformed outer %s", (field, value) => {
    const p = { ...place(), [field]: value };
    expect(() => decodePlacementAnswer(JSON.stringify(p), 0, request())).toThrow();
  });
  it.each([
    ["allocationRequestId", "wrong-semantic-id"], ["host", "other"], ["model", "other"], ["thinking", "high"],
    ["reservationId", "other"], ["fence", 9], ["status", "COMMITTED"], ["launchAllowed", false],
    ["expiresAt", "2026-02-30T00:00:00Z"], ["schemaVersion", 1], ["requesterRef", ""],
    ["configDigest", digest], ["native_binding", {}],
  ])("rejects mismatched/malformed protected grant %s", (field, value) => {
    const p = place(); Object.assign(p.grant, { [field]: value });
    expect(() => decodePlacementAnswer(JSON.stringify(p), 0, request())).toThrow();
  });
  it("requires every protected grant field and preserves optional requesterSession", () => {
    for (const field of Object.keys(place().grant).filter(key => key !== "requesterSession")) {
      const p = place(); delete (p.grant as unknown as Record<string, unknown>)[field];
      expect(() => decodePlacementAnswer(JSON.stringify(p), 0, request())).toThrow();
    }
    const p = place(); delete p.grant.requesterSession;
    expect(decodePlacementAnswer(JSON.stringify(p), 0, request())).toEqual(p);
  });
  it("checks create bounds, real UTC calendars and exact microsecond ordering", () => {
    const p = place();
    p.grant.expiresAt = "2099-01-01T00:00:00.123455Z";
    p.create_before = "2099-01-01T00:00:00.123456Z";
    expect(() => validatePlacementBindings(p, request())).toThrow();
    p.create_before = p.grant.expiresAt;
    expect(() => validatePlacementBindings(p, request())).not.toThrow();
    p.create_before = new Date(Date.now() - 1).toISOString();
    expect(() => validatePlacementBindings(p, request())).toThrow();
    const r = request(); r.deadline_at = new Date(Date.now() - 1).toISOString();
    expect(() => validatePlacementBindings(place(), r)).toThrow();
  });
  it("allows omitted/null trusted pins without changing selected triplet", () => {
    const r = request(); r.requested = { host: null, model: null, thinking: null };
    expect(decodePlacementAnswer(JSON.stringify(place()), 0, r)).toMatchObject({ host: "host-1", model: "provider/model-1", thinking: "xhigh" });
    delete r.requested;
    expect(() => decodePlacementAnswer(JSON.stringify(place()), 0, r)).not.toThrow();
  });
  it.each([
    { generation: undefined }, { launch_id: "wrong" }, { grant: place().grant }, { retry_after: -1 }, { retry_after: 86401 },
    { provenance: {} }, { reason: "REQUEST_INVALID" }, { fence: 1 },
  ])("rejects invalid QUEUE %#", changed => {
    expect(() => decodePlacementAnswer(JSON.stringify({ ...queue(), ...changed }), 75, request())).toThrow();
  });
  it("requires identity for capacity QUEUE and rejects forged queue positions on invalid requests", () => {
    const q = queue(); delete q.launch_id; delete q.generation; delete q.queue_id;
    expect(() => decodePlacementAnswer(JSON.stringify(q), 75, request())).toThrow();
    q.reason = "REQUEST_INVALID";
    expect(decodePlacementAnswer(JSON.stringify(q), 75, request())).toEqual(q);
    q.queue_id = "not-an-ack";
    expect(() => decodePlacementAnswer(JSON.stringify(q), 75, request())).toThrow();
  });
  it("shadow admits neither direct nor hypothetical grant/create/fence authority", () => {
    for (const field of ["grant", "reservation_id", "fence", "create_before"]) {
      const s = shadow();
      expect(() => decodePlacementAnswer(JSON.stringify({ ...s, [field]: "forged" }), 78, request())).toThrow();
      expect(() => decodePlacementAnswer(JSON.stringify({ ...s, hypothetical: { ...s.hypothetical, [field]: "forged" } }), 78, request())).toThrow();
    }
    const s = shadow(); s.hypothetical = { decision: "QUEUE", reason: "FACTS_UNKNOWN", retry_after: 2 };
    expect(decodePlacementAnswer(JSON.stringify(s), 78, request())).toEqual(s);
    s.owner_ref = "person@example";
    expect(() => decodePlacementAnswer(JSON.stringify(s), 78, request())).toThrow();
  });
  it("bounds/validates nested inputs, rejects nonfinite and duplicate decoded JSON keys", () => {
    const r = request(); r.requirements.capabilities = ["duplicate", "duplicate"];
    expect(() => validatePlacementRequest(r)).toThrow();
    r.requirements.capabilities = Array.from({ length: 101 }, (_, index) => `capability-${index}`);
    expect(() => validatePlacementRequest(r)).toThrow();
    for (const text of ['{"a":1,"\\u0061":2}', '{"nested":{"k":0,"k":1}}', '{"n":-1e999}', '{"a":NaN}', "[1,]", "[".repeat(34) + "]".repeat(34)]) {
      expect(() => parsePlacementJson(text)).toThrow();
    }
  });
});

describe("registration/cancel mapping and unpublished receipt gate", () => {
  const native = (p: PlacementPlace): PlacementNativeIdentity => ({
    launchId: p.grant.launchId, configDigest: p.grant.configDigest, eventId: "native-event-1", sessionId: "child-session-1",
    evidenceRef: "protected-native-receipt-ref", receipt: { opaque: "unpublished-receipt" },
  });
  it("maps register to published registered feedback, using native rather than semantic launch identity", async () => {
    const dir = root(); const p = place(); const identity = native(p);
    const result = await cli(dir, { reply: { status: "ACK" } }).register(p, identity);
    const expected = {
      schema: "smarty.placement.v1", operation: "feedback", launch_id: p.launch_id, generation: p.generation,
      reservation_id: p.reservation_id, fence: p.fence, native_launch_id: p.grant.launchId, config_digest: p.grant.configDigest,
      kind: "registered", event_id: identity.eventId, session_id: identity.sessionId, evidence_ref: identity.evidenceRef,
    };
    expect(captured(dir)).toEqual([expected]);
    expect(placementRegistrationRequest(p, identity)).toEqual(expected);
    expect(result).toMatchObject({ kind: "unverified-acknowledgement", registered: false, released: false });
    expect(JSON.stringify(captured(dir))).not.toContain("unpublished-receipt");
  });
  it("maps release to cancel envelope only; even caller terminal claims/exit-zero ACK cannot free debit", async () => {
    const dir = root(); const p = place(); const proof: PlacementReleaseProof = { receipt: { terminal: true, never_started: true, verified: true } };
    const result = await cli(dir, { reply: { status: "CANCEL_REQUESTED" } }).release(p, proof);
    const expected = {
      schema: "smarty.placement.v1", operation: "release", launch_id: p.launch_id, generation: p.generation,
      reservation_id: p.reservation_id, fence: p.fence, native_launch_id: p.grant.launchId, config_digest: p.grant.configDigest,
    };
    expect(captured(dir)).toEqual([expected]);
    expect(placementReleaseRequest(p, proof)).toEqual(expected);
    expect(result).toMatchObject({ registered: false, released: false });
  });
  it("permits expired-grant reconciliation but refuses swapped native identity", async () => {
    const p = place(); p.create_before = "2020-01-01T00:00:00Z"; p.grant.expiresAt = "2020-01-01T00:01:00Z";
    expect(() => placementReleaseRequest(p, { receipt: null })).not.toThrow();
    const dir = root();
    expect(await cli(dir, { reply: {} }).register(p, { ...native(p), launchId: p.launch_id }))
      .toMatchObject({ kind: "refused", reason: "invalid-request", registered: false, released: false });
    expect(fs.existsSync(path.join(dir, "requests.jsonl"))).toBe(false);
    expect(() => placementRegistrationRequest(p, { ...native(p), configDigest: "c".repeat(64) })).toThrow();
  });
  it("refuses lifecycle nonzero/malformed ACKs and shadow lifecycle calls", async () => {
    const p = place();
    expect(await cli(root(), { reply: {}, code: 75 }).register(p, native(p))).toMatchObject({ kind: "refused", registered: false, released: false });
    expect(await cli(root(), { stdout: "invalid" }).release(p, { receipt: null })).toMatchObject({ kind: "refused", released: false });
    const dir = root(); const decider = cli(dir, { reply: {} }, "shadow");
    expect(await decider.register(p, native(p))).toMatchObject({ kind: "refused", reason: "disabled" });
    expect(await decider.release(p, { receipt: null })).toMatchObject({ kind: "refused", reason: "disabled" });
    expect(fs.existsSync(path.join(dir, "requests.jsonl"))).toBe(false);
  });
  it("rejects invented release proof fields and unpaired native/config identity", () => {
    const r = placementReleaseRequest(place(), { receipt: null });
    expect(() => validatePlacementRequest({ ...r, verified: true })).toThrow();
    delete r.config_digest;
    expect(() => validatePlacementRequest(r)).toThrow();
  });
});

describe("host-only placement decider configuration", () => {
  const config = (): PlacementDeciderConfig => ({ command: [process.execPath, "literal argument; not a shell"], mode: "enforce", wakeSourceMachine: "ryzen1" });
  const nativeConfig = (output = "caller-verified=1\n", exitCode = 0, delayMs = 0, stderr = ""): PlacementDeciderConfig => {
    const dir = root(); const context = path.join(dir, "smarty-launch-context"); const recordRoot = path.join(dir, "launches");
    fs.writeFileSync(context, `#!${process.execPath}\nif (process.argv[2] === ${JSON.stringify(VERIFIED_CALLER_CAPABILITY.probe)}) {\n  setTimeout(() => {\n    if (${JSON.stringify(stderr)}) process.stderr.write(${JSON.stringify(stderr)});\n    process.stdout.write(${JSON.stringify(output)});\n    process.exit(${exitCode});\n  }, ${delayMs});\n} else process.exit(0);\n`, { mode: 0o700 });
    fs.chmodSync(context, 0o700); fs.mkdirSync(recordRoot, { mode: 0o700 });
    return { ...config(), contextCommand: [context], recordRoot };
  };
  it("is absent/off by default and normalizes a configured omitted mode to off", () => {
    expect(normalizeFabricConfig({}).agents.placementDecider).toBeUndefined();
    const value = { command: [process.execPath], wakeSourceMachine: "ryzen1" };
    expect(normalizePlacementDeciderConfig(value)).toEqual({ ...value, mode: "off" });
    expect(normalizeFabricConfig({ agents: { placementDecider: value } }).agents.placementDecider).toEqual({ ...value, mode: "off" });
    const copied = config(); const normalized = normalizePlacementDeciderConfig(copied)!;
    copied.command.push("mutation"); expect(normalized.command).not.toContain("mutation");
  });
  it.each(["string command", [], [""], ["relative-command"], ["./relative-command"], [process.execPath, "\0"], [process.execPath, 2], ["/bin/sh", "-c", "echo authority"], ["/does-not-exist"]].map(command => ({ command })))("rejects invalid/non-shell-free argv %#", ({ command }) => {
    expect(() => normalizePlacementDeciderConfig({ ...config(), command })).toThrow();
  });
  it.each(["", "a.b", "*", ">", "a b", "a\n", "user@example", "UpperCase", "under_score", "-leading", "a".repeat(33)])("rejects non-token wake machine %s", wakeSourceMachine => {
    expect(() => normalizePlacementDeciderConfig({ ...config(), wakeSourceMachine })).toThrow();
  });
  it("requires the exact verified-caller capability at config load and leaves shadow untouched", () => {
    expect(VERIFIED_CALLER_CAPABILITY).toEqual({ probe: "--capabilities", line: "caller-verified=1" });
    for (const unavailable of [
      nativeConfig(""), nativeConfig("systemd=1\ncaller-verified=0\n"), nativeConfig("systemd=1\n"),
      nativeConfig("systemd=1\nmalformed output\ncaller-verified=1\n"),
      nativeConfig("caller-verified=1\ncaller-verified=1\n"), nativeConfig("caller-verified=1\n", 1),
      nativeConfig("caller-verified=1\n", 0, 2_000), nativeConfig("caller-verified=1\n", 0, 0, "unexpected stderr\n"),
    ]) {
      expect(() => normalizeFabricConfig({ agents: { placementDecider: unavailable } })).toThrow(/smarty-dev#14903/);
    }
    const capable = nativeConfig("systemd=1\ncaller-verified=1\n");
    expect(nativePlacementEnforceAvailable(capable.contextCommand, undefined, capable.recordRoot)).toBe(true);
    expect(() => normalizeFabricConfig({ agents: { placementDecider: nativeConfig("systemd=1\ncaller-verified=1\n") } })).not.toThrow();
    const loadConfigured = (placementDecider: PlacementDeciderConfig): (() => unknown) => {
      const cwd = root(); const agentDir = root();
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { placementDecider } }));
      return () => loadFabricConfig({ cwd, agentDir, projectTrusted: true });
    };
    expect(loadConfigured(nativeConfig("systemd=1\ncaller-verified=0\n"))).toThrow(/smarty-dev#14903/);
    expect(loadConfigured(nativeConfig("systemd=1\ncaller-verified=1\n"))).not.toThrow();
    expect(() => normalizeFabricConfig({ agents: { placementDecider: { ...nativeConfig("caller-verified=0\n"), mode: "shadow" } } })).not.toThrow();
  });
  it("rejects unknown modes/keys and blocks enforce config when native support is absent", () => {
    expect(() => normalizePlacementDeciderConfig({ ...config(), mode: "enforced" })).toThrow();
    expect(() => normalizePlacementDeciderConfig({ ...config(), timeoutMs: 20_000 })).toThrow();
    expect(() => normalizeFabricConfig({ agents: { placementDecider: config() } }))
      .toThrow(/enforce is unavailable.*smarty-dev#14903/);
    expect(() => normalizeFabricConfig({ agents: { placementDecider: nativeConfig(), }, })).not.toThrow();
    expect(() => normalizeFabricConfig({ agents: { placementDecider: { ...nativeConfig(), recordRoot: "/dev/null/launches" } } }))
      .toThrow(/enforce is unavailable/);
    const available = nativeConfig();
    expect(nativePlacementPathAvailable(available.contextCommand, "/definitely/missing/systemd-run", available.recordRoot)).toBe(false);
    const statSync = fs.statSync.bind(fs);
    vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (String(file) === "/usr/bin/systemd-run") throw new Error("systemd-run unavailable");
      return (statSync as (...values: unknown[]) => fs.Stats)(file, ...args);
    }) as typeof fs.statSync);
    expect(() => normalizeFabricConfig({ agents: { placementDecider: available } })).toThrow(/enforce is unavailable/);
    vi.restoreAllMocks();
    expect(() => normalizeFabricConfig({ agents: { placementDecider: { ...nativeConfig(), mode: "shadow", contextCommand: undefined, recordRoot: "/dev/null/launches" } } })).not.toThrow();
    expect(() => normalizePlacementDeciderConfig(null)).toThrow();
    expect(() => normalizePlacementDeciderConfig({ ...config(), wakeSourceMachine: undefined })).toThrow();
  });
  it.each([true, false])("workspace cannot override host command/mode/wake or enable admission (trusted=%s)", projectTrusted => {
    const cwd = root(); const agentDir = root(); fs.mkdirSync(path.join(cwd, ".pi"));
    const host = { ...config(), mode: "shadow" as const };
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { placementDecider: host } }));
    // Deliberately invalid to prove project validation cannot execute or poison host admission.
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: { placementDecider: { command: "evil | shell", mode: "enforce", wakeSourceMachine: ">" } } }));
    const options = { cwd, agentDir, projectTrusted };
    expect(loadFabricConfig(options).agents.placementDecider).toEqual(host);
    expect(loadFabricConfigForScope(options, projectTrusted ? "project" : "global").agents.placementDecider).toEqual(host);
    fs.unlinkSync(path.join(agentDir, "fabric.json"));
    expect(loadFabricConfig(options).agents.placementDecider).toBeUndefined();
  });
});
