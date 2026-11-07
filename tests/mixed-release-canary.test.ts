import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { FabricBridgeStaleError, FabricParticipantStaleError } from "../src/topology/host-leases.js";
import { FabricPeerLeaseStaleError } from "../src/agents/peer-lease-stale.js";
import { FabricControlStaleIncarnationError, FabricControlIncarnationRequiredError, FabricRelayDestinationUnavailableError } from "../src/topology/control-plane.js";
import { evaluateReceipt, parseArgs, validateInputs, runAuthorityCanary, validateAuthoritySnapshot,
  rebaseAuthoritySnapshot, authorityPredicates, matchAuthorityRefusal, createAuthorityWriterPlan,
  classifyAuthorityCoverage, evaluateAuthorityAcceptance, validateCaptureProvenance, evaluateAuthorityWriterCoverage, collectNaturalExpiry, sanitizeAuthorityEvidence, runLiveAuthorityCanary, type AuthorityOptions, type AuthoritySnapshot } from "../scripts/mixed-release-canary.mjs";

const MIN_HOLD = 60_000;
const CANDIDATE_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const LEGACY_SHA = "41a73f8588e5351d9409736f725fee218810b78c";
const ROOT = "session:legacy-41a-root";
const SENTINEL = "session:historied-41a-sentinel";
const MODERN_OLD = "session:modern-old-root";
const MODERN_SHA = "c42f7f34125abe2ebbe660758253c9ec4d7765dd";
const HUB = "session:hub-root";
const NEW = "session:new-root";
const HUB_NAME = "hub";
const NEW_NAME = "new";
const OLD_NAME = "old-41a";
const STARTED = 100_000;
const ENDED = STARTED + MIN_HOLD;
const TOKEN = "native-seed-token";
const TOKEN_HASH = createHash("sha256").update(TOKEN).digest("hex");
const roots: string[] = [];
let tempRoot = "";

const citation = (file: string, line: number, text: string) => ({
  // The collector records canonical absolute release-artifact paths.
  file: path.posix.join("/fixture-release", file), line, text, sha256: "c".repeat(64),
});
const capableEvidence = {
  supported: true,
  evidence: [
    citation("src/mesh/bridge.ts", 10, "async relayPresence("),
    citation("src/mesh/bridge.ts", 20, "relayed && (!fromHub"),
    citation("dist/mesh-bridge.js", 30, "async relayPresence("),
    citation("dist/mesh-bridge.js", 40, "relayed && (!fromHub"),
  ],
};
const legacyEvidence = {
  supported: false,
  reason: "unsupported (pre-existing)",
  evidence: [
    citation("src/mesh/bridge.ts", 10, "if (remoteHostOf(record) !== undefined) continue"),
    citation("src/mesh/bridge.ts", 20, "return record && remoteHostOf(record) === undefined"),
    citation("dist/mesh-bridge.js", 30, "if (remoteHostOf(record) !== void 0) continue"),
    citation("dist/mesh-bridge.js", 40, "return record && remoteHostOf(record) === void 0"),
  ],
};

function makeValidEvidence(): Record<string, any> {
  const nodes = [
    { id: HUB, name: HUB_NAME, role: "hub", sha: CANDIDATE_SHA, ownerIncarnation: "epoch-hub", relayCapable: true, capabilityEvidence: structuredClone(capableEvidence) },
    { id: NEW, name: NEW_NAME, role: "new", sha: CANDIDATE_SHA, ownerIncarnation: "epoch-new", relayCapable: true, capabilityEvidence: structuredClone(capableEvidence) },
    // The historical 41a caller is genuinely tokenless and has no owner epoch.
    { id: ROOT, name: OLD_NAME, role: "old", sha: LEGACY_SHA, relayCapable: false, capabilityEvidence: structuredClone(legacyEvidence) },
    { id: MODERN_OLD, name: "old-modern", role: "old", sha: MODERN_SHA, ownerIncarnation: "epoch-modern", relayCapable: true, capabilityEvidence: structuredClone(capableEvidence) },
  ];
  const releases = [{ sha: CANDIDATE_SHA }, { sha: LEGACY_SHA }, { sha: MODERN_SHA }];
  const registrations = [
    { id: HUB, native: true, leaseLive: true, participant: true, ownerIncarnation: "epoch-hub", stateBackend: "sqlite", lockProtocol: 3, hostTokenHash: "d".repeat(64), leaseTokenHash: "d".repeat(64) },
    { id: NEW, native: true, leaseLive: true, participant: true, ownerIncarnation: "epoch-new", stateBackend: "sqlite", lockProtocol: 3, hostTokenHash: "e".repeat(64), leaseTokenHash: "e".repeat(64) },
    { id: ROOT, name: OLD_NAME, native: true, leaseLive: true, participant: true, stateBackend: "sqlite", lockProtocol: 2, tokenless: true },
    { id: MODERN_OLD, native: true, leaseLive: true, participant: true, ownerIncarnation: "epoch-modern", stateBackend: "sqlite", lockProtocol: 3, hostTokenHash: "f".repeat(64), leaseTokenHash: "f".repeat(64) },
  ];
  const links = [NEW_NAME, OLD_NAME, "old-modern"].map(name => ({ name, starts: 1, linkedAt: STARTED - 1, observedUntil: ENDED + 1, closedAt: null, relayAuthenticated: true }));
  const allPairs = nodes.flatMap(source => nodes.filter(target => target.id !== source.id).map(target => ({ source, target })));
  const requiredPairs = allPairs.filter(({ source, target }) => source.role === "hub" || target.role === "hub" || source.relayCapable && target.relayCapable);
  const presencePairs = allPairs.filter(({ source, target }) => source.role === "hub" || target.role === "hub" || source.relayCapable);
  const unsupported = allPairs.filter(pair => !requiredPairs.includes(pair)).map(({ source, target }) => ({
    source: source.id,
    target: target.id,
    reason: "unsupported (pre-existing)",
    lackingReleases: [source, target].filter(node => !node.relayCapable).map(node => ({
      sha: node.sha,
      evidence: structuredClone(node.capabilityEvidence.evidence),
    })),
  }));
  const routes = ["before", "after"].flatMap(phase => presencePairs.map(({ source, target }) => ({
    phase, source: source.id, target: target.id, remoteHost: target.name,
    ownerIncarnation: target.ownerIncarnation, via: "hub",
  })));
  const plans = requiredPairs.flatMap(({ source, target }) => ["before", "after"].map(round => ({
    marker: `marker:${source.id}->${target.id}:${round}`, source: source.id, target: target.id, round,
  })));
  const sends = plans.map((plan, index) => {
    const commandId = `command-${index + 1}`;
    return {
      ...plan, commandId,
      ack: { commandId, from: plan.target, to: plan.source, verification: "bridge" },
    };
  });
  const tallies = nodes.map(node => ({
    id: node.id,
    invalid: 0,
    duplicateCalls: 0,
    entries: plans.filter(plan => plan.target === node.id).map(plan => ({
      marker: plan.marker, calls: 1, verification: "bridge", from: plan.source,
      targetId: plan.target, commandId: sends.find(send => send.marker === plan.marker)!.commandId,
    })),
    ackEvents: sends.filter(send => send.source === node.id).map(send => ({
      verification: "bridge", from: { id: send.target }, to: send.source,
      data: { commandId: send.commandId, targetId: send.target, accepted: true, messageId: `ack:${send.marker}` },
    })),
  }));
  const rawOwner = { ownerHostId: SENTINEL, ownerIdentityId: SENTINEL, ownerIncarnation: "epoch-seed", remoteHost: "hub" };
  const mirror = { id: SENTINEL, hostTokenHash: TOKEN_HASH, leaseTokenHash: TOKEN_HASH, ownerIncarnation: "epoch-seed",
    remoteHost: "hub", hostHash: "1".repeat(64), leaseHash: "2".repeat(64), rowHash: "3".repeat(64), rawOwner };
  const marker = "history-only-marker", commandId = "history-only-command";
  const actualOwner = { remoteHost: "hub", ownerIncarnation: "epoch-seed", expiresAt: STARTED + 15_000 };
  const command = { from: { id: NEW }, to: SENTINEL, data: { commandId, targetId: SENTINEL, operation: "followUp",
    data: { marker }, message: marker, destinationRemoteHost: "hub", ownerIncarnation: "epoch-seed" } };
  const ack = { verification: "bridge", from: { id: SENTINEL }, to: NEW,
    data: { commandId, targetId: SENTINEL, accepted: true, messageId: `ack:${marker}` } };
  return {
    mode: "release-gate",
    holdMs: MIN_HOLD,
    releases,
    nodes,
    registrations,
    hold: { startedAt: STARTED, endedAt: ENDED, elapsedMs: MIN_HOLD },
    links,
    routes,
    unsupported,
    plans,
    sends,
    tallies,
    history: {
      fresh: { id: ROOT, before: { id: ROOT, host: null, lease: null, participant: null, row: null, owner: null } },
      old: { id: SENTINEL, name: OLD_NAME, sha: LEGACY_SHA, native: true, tokenless: true, leaseLive: true },
      seeded: clone(mirror), preserved: clone(mirror), after: clone(mirror),
      hub: { id: SENTINEL, remoteHost: OLD_NAME, tokenless: true },
      warning: { name: "STALE_INCARNATION_HISTORY", source: "bridge", rootId: SENTINEL, tokenHash: TOKEN_HASH,
        occurrenceCount: 1, rawLines: [{ link: "measured-new", line: `bridge hub: skipped stale relayed root ${SENTINEL}; preserving its mirror` }] },
      liveAtAttempt: { id: SENTINEL, native: true, tokenless: true, leaseLive: true, observedAt: STARTED, leaseExpiresAt: STARTED + 15_000 },
      attempt: { marker, source: NEW, intendedTarget: SENTINEL, actualOwner, publicRequestInvoked: true,
        observed: { id: SENTINEL, host: { incarnationToken: TOKEN }, owner: clone(actualOwner) },
        beganAt: STARTED + 1, endedAt: STARTED + 2, elapsedMs: 1, timeoutMs: 20_000, ackGraceMs: 15_000,
        receipt: { acknowledged: true, routed: "mesh", messageId: `ack:${marker}` }, commandEvents: [command], ackEvents: [ack] },
      destination: { native: { id: SENTINEL }, expected: [marker], invalid: 0, duplicateCalls: 0,
        liveAfter: { id: SENTINEL, native: true, tokenless: true, leaseLive: true, observedAt: ENDED },
        entries: [{ marker, from: NEW, targetId: SENTINEL, commandId, calls: 1, verification: "bridge" }],
        ackEvents: [{ ...clone(ack), verification: undefined }] },
    },
    restartCount: 0,
    cleanup: { ok: true, failures: [], owned: [] },
  };
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function makeRelease(directory: string, sha: string, relayCapable: boolean): void {
  fs.mkdirSync(path.join(directory, "bin"), { recursive: true });
  fs.mkdirSync(path.join(directory, "dist", "chunks"), { recursive: true });
  fs.mkdirSync(path.join(directory, "src", "mesh"), { recursive: true });
  fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ name: "pi-fabric", version: "0.0.0-test" }));
  fs.writeFileSync(path.join(directory, "bin", "mesh-bridge"), "fixture only\n");
  fs.writeFileSync(path.join(directory, "dist", "build-identity.json"), JSON.stringify({ sha }));
  if (relayCapable) {
    fs.writeFileSync(path.join(directory, "src", "mesh", "bridge.ts"), "async relayPresence() {}\nif (relayed && (!fromHub)) return;\n");
    fs.writeFileSync(path.join(directory, "dist", "mesh-bridge.js"), "async relayPresence() {}\nif (relayed && (!fromHub)) return;\n");
  } else {
    fs.writeFileSync(path.join(directory, "src", "mesh", "bridge.ts"), "if (remoteHostOf(record) !== undefined) continue\nreturn record && remoteHostOf(record) === undefined\n");
    fs.writeFileSync(path.join(directory, "dist", "mesh-bridge.js"), "if (remoteHostOf(record) !== void 0) continue\nreturn record && remoteHostOf(record) === void 0\n");
  }
}

beforeAll(() => {
  tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mixed-release-canary-tests-"));
  roots.push(tempRoot);
});
afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

describe("mixed-release canary exported argument and input validation", () => {
  it("parses repeated OLD releases and enforces the 60s default", () => {
    expect(parseArgs(["--new", "/candidate", "--old", "/old-a", "--old", "/old-b"])).toEqual({
      new: "/candidate", old: ["/old-a", "/old-b"], holdMs: MIN_HOLD, baselineControl: false,
    });
  });

  it.each([
    ["unknown flags", ["--new", "/new", "--old", "/old", "--wat"]],
    ["missing release", ["--new", "/new"]],
    ["missing flag value", ["--new", "/new", "--old"]],
    ["duplicate scalar flag", ["--new", "/new", "--new", "/other", "--old", "/old"]],
    ["duplicate baseline flag", ["--new", "/new", "--old", "/old", "--baseline-control", "--baseline-control"]],
    ["sub-minute hold", ["--new", "/new", "--old", "/old", "--hold-ms", "59999"]],
    ["non-integer hold", ["--new", "/new", "--old", "/old", "--hold-ms", "60000.5"]],
    ["unbounded hold", ["--new", "/new", "--old", "/old", "--hold-ms", "2147000001"]],
  ])("rejects %s", (_name, argv) => {
    expect(() => parseArgs(argv as string[])).toThrow();
  });

  it.skipIf(process.platform !== "linux" || process.release.name !== "node" || Number(process.versions.node.split(".")[0]) < 24)(
    "validates full release identities and source-plus-compiled relay capability (Linux Node >=24 only)", () => {
    const candidate = path.join(tempRoot, "candidate-valid");
    const legacy = path.join(tempRoot, "legacy-41a-valid");
    makeRelease(candidate, CANDIDATE_SHA, true);
    makeRelease(legacy, LEGACY_SHA, false);

    const validated = validateInputs({ new: candidate, old: [legacy], holdMs: MIN_HOLD, baselineControl: false });
    expect(validated.candidate.sha).toBe(CANDIDATE_SHA);
    expect(validated.candidate.relayCapability).toMatchObject({ supported: true, evidence: expect.arrayContaining([
      expect.objectContaining({ file: expect.stringContaining("src/mesh/bridge.ts") }),
      expect.objectContaining({ file: expect.stringContaining("dist/mesh-bridge.js") }),
    ]) });
    expect(validated.old[0]!.sha).toBe(LEGACY_SHA);
    expect(validated.old[0]!.relayCapability).toMatchObject({ supported: false, reason: "unsupported (pre-existing)" });
  });

  it.skipIf(process.platform !== "linux" || process.release.name !== "node" || Number(process.versions.node.split(".")[0]) < 24)(
    "allows duplicate release SHAs only for an explicitly labelled baseline control (Linux Node >=24 only)", () => {
    const release = path.join(tempRoot, "same-sha-release");
    makeRelease(release, CANDIDATE_SHA, true);
    const options = { new: release, old: [release], holdMs: MIN_HOLD, baselineControl: false };
    expect(() => validateInputs(options)).toThrow(/distinct candidate and old release SHAs/);
    expect(validateInputs({ ...options, baselineControl: true }).candidate.sha).toBe(CANDIDATE_SHA);
  });

  it.skipIf(process.platform === "linux" && process.release.name === "node" && Number(process.versions.node.split(".")[0]) >= 24)(
    "refuses validateInputs outside its Linux Node >=24 production runtime",
    () => {
      const reason = process.platform !== "linux" || process.release.name !== "node"
        ? /production authenticated relay requires Linux Node/
        : /Node >=24 required/;
      expect(() => validateInputs({ new: "/unused-new", old: ["/unused-old"], holdMs: MIN_HOLD, baselineControl: false })).toThrow(reason);
    },
  );
});

describe("mixed-release canary receipt is fail-closed", () => {
  it("passes a complete non-vacuous normal-gate fixture with a tokenless 41a caller", () => {
    const evidence = makeValidEvidence();
    expect(evidence.nodes.find((node: any) => node.id === ROOT)).not.toHaveProperty("ownerIncarnation");
    expect(evidence.nodes.find((node: any) => node.id === NEW).ownerIncarnation).toBe("epoch-new");
    const result = evaluateReceipt(evidence);
    expect(result.ok).toBe(true);
    expect(result.releaseGate).toBe(true);
    expect(result.checks.every(check => check.status === "PASS")).toBe(true);
  });

  it("passes an actual named public refusal without pretending the sentinel was delivered", () => {
    const e = makeValidEvidence();
    delete e.history.attempt.receipt;
    e.history.attempt.error = { source: "FabricControlPlane.request", name: "FabricControlStaleIncarnationError",
      code: "FABRIC_CONTROL_STALE_INCARNATION", message: `Stale Fabric owner incarnation for ${SENTINEL}` };
    e.history.attempt.ackEvents = [];
    e.history.destination.entries = [];
    e.history.destination.ackEvents = [];
    const result = evaluateReceipt(e);
    expect(result.ok).toBe(true);
    expect(result.checks.find(check => check.name === "historied-live-root-public-followup-outcome")?.evidence).toMatchObject({ outcome: "named-public-error" });
  });

  it("rejects a public refusal with a conflicting explicit target even when its message names the sentinel", () => {
    const e = makeValidEvidence();
    delete e.history.attempt.receipt;
    e.history.attempt.error = { source: "FabricControlPlane.request", name: "FabricControlStaleIncarnationError",
      code: "FABRIC_CONTROL_STALE_INCARNATION", targetId: "session:unrelated-target", message: `Stale route involving ${SENTINEL}` };
    e.history.attempt.ackEvents = [];
    e.history.destination.entries = [];
    e.history.destination.ackEvents = [];
    expect(evaluateReceipt(e).ok).toBe(false);
  });

  it("accepts a named public bridge-stale outcome verbatim, independently of normal controls on the same old mesh", () => {
    const e = makeValidEvidence();
    delete e.history.attempt.receipt;
    e.history.attempt.error = { source: "FabricControlPlane.request", name: "FabricBridgeStaleError", code: "FABRIC_BRIDGE_STALE",
      message: `Fabric bridge-stale for remote host hub, target ${SENTINEL}; no fresh mirror from that host; the remote owner's lease is unconfirmed; the outcome is unknown and it may still be delivered, so a retry can deliver it twice.` };
    e.history.attempt.ackEvents = [{ verification: "bridge", from: { id: "bridge:hub" }, to: NEW,
      data: { commandId: "history-only-command", targetId: SENTINEL, accepted: false, error: "Relay destination unavailable" } }];
    e.history.destination.entries = [];
    e.history.destination.ackEvents = [{ from: { id: ROOT }, to: HUB,
      data: { commandId: "normal-primary-command", targetId: ROOT, accepted: true } }];
    expect(evaluateReceipt(e).ok).toBe(true);
  });

  it.each([
    new FabricBridgeStaleError("hub", SENTINEL), new FabricPeerLeaseStaleError(SENTINEL, "hub", 1_000),
    new FabricParticipantStaleError(SENTINEL, 1_000), new FabricControlStaleIncarnationError(SENTINEL),
    new FabricControlIncarnationRequiredError(SENTINEL), new FabricRelayDestinationUnavailableError(SENTINEL, "canary-key"),
  ])("P2 accepts the actual public refusal constructor $name, including a target absent from its message", error => {
    const e = makeValidEvidence();
    delete e.history.attempt.receipt;
    e.history.attempt.error = { source: "FabricControlPlane.request", name: error.name, code: error.code,
      targetId: error.targetId, message: error.message };
    e.history.attempt.ackEvents = []; e.history.destination.entries = []; e.history.destination.ackEvents = [];
    expect(evaluateReceipt(e).ok).toBe(true);
    e.history.attempt.error.code = "UNRECOGNIZED";
    expect(evaluateReceipt(e).ok).toBe(false);
  });

  it.each(["TypeError", "RangeError", "UnknownNamedError", "toString", "__proto__"])("P2 rejects an undocumented public error class %s", name => {
    const e = makeValidEvidence();
    delete e.history.attempt.receipt;
    e.history.attempt.error = { source: "FabricControlPlane.request", name, message: `Broken request for ${SENTINEL}` };
    e.history.attempt.ackEvents = []; e.history.destination.entries = []; e.history.destination.ackEvents = [];
    expect(evaluateReceipt(e).ok).toBe(false);
  });

  it("counts repeated root-specific production warnings without claiming production exact-once", () => {
    const e = makeValidEvidence();
    e.history.warning.rawLines.push(clone(e.history.warning.rawLines[0]));
    e.history.warning.occurrenceCount = 2;
    expect(evaluateReceipt(e).ok).toBe(true);
  });

  it("passes a complete baseline control only as non-release-gate evidence", () => {
    const baseline = makeValidEvidence();
    baseline.mode = "baseline-control";
    baseline.history = {};
    const result = evaluateReceipt(baseline);
    expect(result.ok).toBe(true);
    expect(result.releaseGate).toBe(false);
  });

  it("does not pass empty evidence or an empty plan", () => {
    expect(evaluateReceipt({}).ok).toBe(false);
    const empty = makeValidEvidence();
    empty.plans = [];
    empty.sends = [];
    empty.tallies.forEach((tally: any) => { tally.entries = []; tally.ackEvents = []; });
    expect(evaluateReceipt(empty).ok).toBe(false);
  });

  it.each([
    ["a hold shorter than 60 seconds", (e: any) => { e.holdMs = MIN_HOLD - 1; e.hold.endedAt = e.hold.startedAt + MIN_HOLD - 1; e.hold.elapsedMs = MIN_HOLD - 1; }],
    ["a missing 41a bridge", (e: any) => { e.links = e.links.filter((link: any) => link.name !== OLD_NAME); }],
    ["a restarted 41a bridge", (e: any) => { e.links.find((link: any) => link.name === OLD_NAME).starts = 2; }],
    ["a modern registration with the wrong owner epoch", (e: any) => { e.registrations.find((row: any) => row.id === NEW).ownerIncarnation = "wrong-epoch"; }],
    ["a recorded restart even when link starts remains one", (e: any) => { e.restartCount = 1; }],
    ["an unclean descendant cleanup", (e: any) => { e.cleanup.ok = false; }],
    ["a missing direct hub-to-41a pair", (e: any) => { e.plans = e.plans.filter((plan: any) => !(plan.source === HUB && plan.target === ROOT)); }],
    ["the legacy root missing from NEW's before/after hub routes", (e: any) => {
      e.routes = e.routes.filter((route: any) => !(route.source === NEW && route.target === ROOT));
    }],
    ["a candidate hub/NEW release without relay capability", (e: any) => {
      for (const node of e.nodes.filter((item: any) => item.role === "hub" || item.role === "new")) {
        node.relayCapable = false;
        node.capabilityEvidence = structuredClone(legacyEvidence);
      }
      for (const unsupported of e.unsupported) {
        const source = e.nodes.find((node: any) => node.id === unsupported.source);
        const target = e.nodes.find((node: any) => node.id === unsupported.target);
        unsupported.lackingReleases = [source, target].filter((node: any) => !node.relayCapable).map((node: any) => ({ sha: node.sha, evidence: structuredClone(node.capabilityEvidence.evidence) }));
      }
    }],
    ["a duplicate destination handler call", (e: any) => { e.tallies.find((tally: any) => tally.id === ROOT).duplicateCalls = 1; }],
    ["a missing correlated authenticated ACK", (e: any) => {
      const send = e.sends[0];
      e.tallies.find((tally: any) => tally.id === send.source).ackEvents = [];
    }],
    ["a duplicate correlated authenticated ACK", (e: any) => {
      const send = e.sends[0];
      const tally = e.tallies.find((item: any) => item.id === send.source);
      const ack = tally.ackEvents.find((event: any) => event.data.commandId === send.commandId);
      tally.ackEvents.push(structuredClone(ack));
    }],
    ["a mismatched authenticated ACK", (e: any) => { e.sends[0].ack.from = "session:wrong-destination"; }],
    ["absent genuine history", (e: any) => { e.history = {}; }],
    ["forged genuine history", (e: any) => { e.history.seeded.hostTokenHash = "f".repeat(64); }],
    ["the primary root reused as the historied sentinel", (e: any) => { e.history.seeded.id = ROOT; }],
    ["a primary root with prior history", (e: any) => { e.history.fresh.before.host = { incarnationToken: TOKEN }; }],
    ["a retained mirror rebound to old-41a", (e: any) => { e.history.after.remoteHost = OLD_NAME; e.history.after.rawOwner.remoteHost = OLD_NAME; }],
    ["erased retained lease", (e: any) => { e.history.after.leaseHash = null; }],
    ["changed retained lease", (e: any) => { e.history.after.leaseHash = "changed"; }],
    ["changed retained owner epoch", (e: any) => { e.history.after.rawOwner.ownerIncarnation = "new"; }],
    ["an uncorrelated root warning", (e: any) => { e.history.warning.rawLines[0].line = "skipped stale relayed root somebody-else"; }],
    ["an invented canary warning", (e: any) => { e.history.warning.source = "canary"; }],
    ["an absent production warning", (e: any) => { e.history.warning.occurrenceCount = 0; e.history.warning.rawLines = []; }],
    ["an uninvoked public request", (e: any) => { e.history.attempt.publicRequestInvoked = false; }],
    ["an absent public outcome", (e: any) => { delete e.history.attempt.receipt; }],
    ["a request to the wrong root", (e: any) => { e.history.attempt.intendedTarget = ROOT; }],
    ["a forced old-spoke route", (e: any) => { e.history.attempt.actualOwner.remoteHost = OLD_NAME; }],
    ["a fabricated request precheck rejection", (e: any) => {
      delete e.history.attempt.receipt;
      e.history.attempt.error = { source: "canary-precheck", name: "Error", message: `No authentic owner for ${SENTINEL}` };
      e.history.attempt.ackEvents = []; e.history.destination.entries = []; e.history.destination.ackEvents = [];
    }],
    ["a generic harness timeout", (e: any) => {
      delete e.history.attempt.receipt;
      e.history.attempt.error = { source: "FabricControlPlane.request", name: "Error", message: `RPC deadline for ${SENTINEL}` };
      e.history.attempt.ackEvents = []; e.history.destination.entries = []; e.history.destination.ackEvents = [];
    }],
    ["an unknown public timeout outcome", (e: any) => {
      delete e.history.attempt.receipt;
      e.history.attempt.error = { source: "FabricControlPlane.request", name: "Error", message: `Fabric bridge not responding for ${SENTINEL}; the outcome is unknown` };
      e.history.attempt.ackEvents = []; e.history.destination.entries = []; e.history.destination.ackEvents = [];
    }],
    ["an expired native sentinel at invocation", (e: any) => { e.history.liveAtAttempt.leaseExpiresAt = STARTED - 1; }],
    ["a dead native sentinel after invocation", (e: any) => { e.history.destination.liveAfter.leaseLive = false; }],
    ["the wrong native sentinel after invocation", (e: any) => { e.history.destination.liveAfter.id = ROOT; }],
    ["an outcome outside the 35s public budget", (e: any) => { e.history.attempt.elapsedMs = 35_001; }],
    ["an artificially short request timeout", (e: any) => { e.history.attempt.timeoutMs = 1; }],
    ["an acknowledged history send without a handler", (e: any) => { e.history.destination.entries = []; }],
    ["a duplicate sentinel handler", (e: any) => { e.history.destination.entries[0].calls = 2; }],
    ["an unauthenticated sentinel handler", (e: any) => { e.history.destination.entries[0].verification = "local"; }],
    ["an uncorrelated sentinel ACK", (e: any) => { e.history.attempt.ackEvents[0].data.commandId = "wrong"; }],
    ["a missing sentinel ACK", (e: any) => { e.history.attempt.ackEvents = []; }],
    ["unsupported pair citations without source evidence", (e: any) => {
      for (const unsupported of e.unsupported) {
        for (const release of unsupported.lackingReleases) {
          release.evidence = release.evidence.map((cite: any) => ({ ...cite, file: "dist/mesh-bridge.js" }));
        }
      }
    }],
    ["unsupported pair source-only citations without compiled evidence", (e: any) => {
      for (const unsupported of e.unsupported) {
        for (const release of unsupported.lackingReleases) {
          release.evidence = release.evidence.map((cite: any) => ({ ...cite, file: "src/mesh/bridge.ts" }));
        }
      }
    }],
  ])("fails closed on %s", (_name, mutate) => {
    const evidence = makeValidEvidence();
    (mutate as (evidence: Record<string, any>) => void)(evidence);
    expect(evaluateReceipt(evidence).ok).toBe(false);
  });
});

// Keep reason: data-safety/trust boundary. Malformed captures must never be product RED,
// and only the actual compiled CLI can prove the mixed-version authority refusal.
function authorityFixture(site: "hub" | "work", sibling = false, leaseOnly = false): AuthoritySnapshot {
  const collectedAt = Date.now(), root = `session:${"1".repeat(32)}`, other = `session:${"3".repeat(32)}`;
  const tokenA = `h:${"a".repeat(32)}`, tokenB = `h:${"b".repeat(32)}`;
  const host = (id: string, rootId: string, token?: string) => ({ format: 1, id, rootId,
    identity: { id, kind: "main", name: "SYNTHETIC" }, startedAt: collectedAt - 1_000,
    updatedAt: collectedAt, expiresAt: collectedAt + 15_000, ...(token ? { incarnationToken: token } : {}) });
  const hosts = site === "hub" ? [host(root, root, leaseOnly ? undefined : tokenA),
    ...(sibling ? [host(`session:${"2".repeat(32)}`, root, tokenB)] : [])] : [host(other, other, tokenB)];
  const stateEntries = hosts.map((value, index) => ({ key: `topology/hosts/${createHash("sha256").update(value.id).digest("hex")}`,
    value, version: index + 1, updatedAt: collectedAt, updatedBy: value.identity }));
  const leaseFiles = hosts.map(value => ({ format: 1, id: value.id, rootId: value.rootId, identityId: value.identity.id,
    startedAt: value.startedAt, updatedAt: value.updatedAt, expiresAt: value.expiresAt,
    ...(value.incarnationToken ? { incarnationToken: value.incarnationToken } : {}) }));
  if (site === "work" && leaseOnly) leaseFiles.push({ format: 1, id: root, rootId: root, identityId: root,
    startedAt: collectedAt - 2_000, updatedAt: collectedAt, expiresAt: collectedAt + 15_000, incarnationToken: tokenA });
  return { schemaVersion: 1, site, name: site === "hub" ? "dev1" : "ryzen2", peer: site === "hub" ? "ryzen2" : "dev1",
    backend: site === "hub" ? "sqlite" : "file", collectedAt, stateMetadata: { format: 2, revisionFormat: 2, highWater: 2 },
    stateEntries, participantFiles: [], leaseFiles, cursors: [] };
}

describe("snapshot authority canary fail-closed boundary", () => {
  it("opts in explicitly without weakening the existing 60s default", () => {
    const parsed = parseArgs(["--authority-snapshot", "--hub", "/hub", "--work", "/work", "--hub-sha", "a".repeat(40),
      "--work-sha", "b".repeat(40), "--hub-snapshot", "/hub.json", "--work-snapshot", "/work.json"]);
    expect(parsed).toMatchObject({ authoritySnapshot: true, passes: 3, timeoutMs: 30_000 });
    expect(parseArgs(["--new", "/new", "--old", "/old"]).holdMs).toBe(MIN_HOLD);
  });
  it.each([
    ["missing backend", (s: any) => { delete s.backend; }],
    ["wrong backend", (s: any) => { s.backend = "file"; }],
    ["empty authority", (s: any) => { s.stateEntries = []; s.leaseFiles = []; }],
    ["raw tokens", (s: any) => { s.stateEntries[0].value.incarnationToken = "secret-raw-token"; }],
    ["raw history tokens", (s: any) => { s.leaseFiles[0].bridgeSupersededTokens = ["raw"]; }],
    ["raw history keys", (s: any) => { s.leaseFiles[0].bridgeSupersededTokenSeen = { raw: 0 }; }],
    ["bad history ordinal", (s: any) => { s.leaseFiles[0].bridgeSupersededTokenSeen = { [`h:${"a".repeat(32)}`]: -1 }; }],
    ["unknown authority namespace", (s: any) => { s.stateEntries[0].key = "events/private"; }],
    ["wrong key/id join", (s: any) => { s.stateEntries[0].value.id = "session:wrong"; }],
    ["lost version", (s: any) => { delete s.stateEntries[0].version; }],
    ["malformed host", (s: any) => { s.stateEntries[0].value.format = 99; }],
    ["duplicate lease", (s: any) => { s.leaseFiles.push(s.leaseFiles[0]); }],
    ["lost writer", (s: any) => { delete s.stateEntries[0].updatedBy; }],
    ["handoff with process-dependent native marker", (s: any) => { s.stateEntries.push({ key: "topology/handoffs/" + "a".repeat(64) }); }],
  ])("rejects %s before launching a release", (_reason, mutate) => {
    const snapshot = authorityFixture("hub"); (mutate as (s: any) => void)(snapshot);
    expect(() => validateAuthoritySnapshot(snapshot, "hub")).toThrow();
  });
  it("allows state-only and lease-only input but rejects both empty", () => {
    const state = authorityFixture("work"); state.leaseFiles = [];
    expect(validateAuthoritySnapshot(state, "work")).toBe(state);
    const lease = authorityFixture("work"); lease.stateEntries = [];
    expect(validateAuthoritySnapshot(lease, "work")).toBe(lease);
    lease.leaseFiles = [];
    expect(() => validateAuthoritySnapshot(lease, "work")).toThrow("empty authority population");
    expect(validateAuthoritySnapshot(authorityFixture("hub"), "hub").leaseFiles.length).toBeGreaterThan(0);
  });
  it("treats state-and-lease authority with zero participant files as full through the live input path", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    const result = await runLiveAuthorityCanary({ hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
      hubSnapshot: hub, workSnapshot: work, synthetic: true, ...fixtureProvenance(hub, work) });
    expect(result.diagnosticContext).toBeUndefined();
    expect(result.failure).toBeDefined();
    expect(classifyAuthorityCoverage({ hub, work })).toBeUndefined();
    expect(evaluateAuthorityAcceptance({ arms: [{ outcome: "RED" }, { outcome: "PASS" }] })).toBe(true);
  });
  it("treats pure lease-only authority as full through the live input path", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    hub.stateEntries = []; work.stateEntries = [];
    const result = await runLiveAuthorityCanary({ hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
      hubSnapshot: hub, workSnapshot: work, synthetic: true, ...fixtureProvenance(hub, work) });
    expect(result.diagnosticContext).toBeUndefined();
    expect(result.failure).toBeDefined();
    expect(classifyAuthorityCoverage({ hub, work })).toBeUndefined();
    expect(evaluateAuthorityAcceptance({ arms: [{ outcome: "RED" }, { outcome: "PASS" }] })).toBe(true);
  });
  it("marks valid state plus only unrelated leases partial through validation, live input and acceptance", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    const unrelated = `session:${"9".repeat(32)}`;
    work.leaseFiles[0] = { ...work.leaseFiles[0], id: unrelated, rootId: unrelated, identityId: unrelated };
    expect(validateAuthoritySnapshot(work, "work")).toBe(work);
    const diagnosticContext = classifyAuthorityCoverage({ hub, work });
    expect(diagnosticContext).toMatchObject({ partialDiagnostic: true, incidentFaithful: false, acceptanceEligible: false,
      label: "partial diagnostic: participant/lease coverage missing; mixed-era (hub 16:30 live, work 12:40 snapshot)",
      missingAuthority: ["work.participantFiles", "work.leaseFiles"], material: "mixed-era" });
    const result = await runLiveAuthorityCanary({ hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
      hubSnapshot: hub, workSnapshot: work, synthetic: true, ...fixtureProvenance(hub, work) });
    expect(result.diagnosticContext).toEqual(diagnosticContext);
    expect(result.acceptance).toBe(false);
    expect(evaluateAuthorityAcceptance({ arms: [{ outcome: "RED" }, { outcome: "PASS" }], diagnosticContext: diagnosticContext! })).toBe(false);
  });
  it.each<[string, (snapshot: AuthoritySnapshot) => void, boolean]>([
    ["expired before capture", snapshot => { snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt - 1; }, true],
    ["expiry exactly at capture", snapshot => { snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt; }, true],
    ["mixed unmatched host and unrelated lease", snapshot => {
      const unrelated = `session:${"8".repeat(32)}`;
      snapshot.leaseFiles[0] = { ...snapshot.leaseFiles[0], id: unrelated, rootId: unrelated, identityId: unrelated };
    }, true],
    ["one matching lease with multiple orphan hosts", snapshot => {
      const source = snapshot.stateEntries[0]!;
      for (const digit of ["4", "5"]) {
        const id = `session:${digit.repeat(32)}`;
        snapshot.stateEntries.push({ ...source, key: `topology/hosts/${createHash("sha256").update(id).digest("hex")}`,
          version: source.version + Number(digit), value: { ...source.value, id, rootId: id, identity: { ...source.value.identity, id },
            incarnationToken: `h:${digit.repeat(32)}` }, updatedBy: { ...source.updatedBy, id } });
      }
    }, false],
    ["same identity with a different incarnation", snapshot => { snapshot.leaseFiles[0]!.incarnationToken = `h:${"c".repeat(32)}`; }, false],
    ["expired base with a valid reload extension", snapshot => {
      snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt - 1;
      snapshot.leaseFiles[0]!.reloadUntil = snapshot.collectedAt + 1;
    }, false],
    ["expiry future at capture but expired now", snapshot => {
      const captured = Date.now() - 60_000;
      snapshot.collectedAt = captured;
      snapshot.stateEntries[0]!.value.expiresAt = captured + 30_000;
      snapshot.leaseFiles[0]!.expiresAt = captured + 30_000;
    }, false],
  ])("classifies coverage from capture-time matching lease state: %s", (_name, mutate, partial) => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    mutate(work);
    expect(validateAuthoritySnapshot(work, "work")).toBe(work);
    const diagnosticContext = classifyAuthorityCoverage({ hub, work });
    expect(Boolean(diagnosticContext?.partialDiagnostic)).toBe(partial);
    expect(evaluateAuthorityAcceptance({ arms: [{ outcome: "RED" }, { outcome: "PASS" }],
      ...(diagnosticContext ? { diagnosticContext } : {}) })).toBe(!partial);
  });
  it("labels partial BLOCKED receipts and cannot accept apparent RED/PASS or complete manifest claims", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work"); work.leaseFiles = [];
    const diagnosticContext = classifyAuthorityCoverage({ hub, work });
    expect(diagnosticContext).toMatchObject({ partialDiagnostic: true, incidentFaithful: false, acceptanceEligible: false,
      label: "partial diagnostic: participant/lease coverage missing; mixed-era (hub 16:30 live, work 12:40 snapshot)",
      missingAuthority: ["work.participantFiles", "work.leaseFiles"], material: "mixed-era" });
    const arms = [{ outcome: "RED" }, { outcome: "PASS" }];
    expect(evaluateAuthorityAcceptance({ arms, diagnosticContext: diagnosticContext! })).toBe(false);
    expect(evaluateAuthorityAcceptance({ arms })).toBe(true);
    expect(evaluateAuthorityAcceptance({ arms }, true)).toBe(false);
    const provenance = fixtureProvenance(hub, work);
    const options = { hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
      hubSnapshot: hub, workSnapshot: work, synthetic: true, ...provenance };
    const blocked = await runLiveAuthorityCanary(options);
    expect(blocked).toMatchObject({ acceptance: false, ok: false, outcome: "BLOCKED", diagnosticContext,
      captureAge: { work: { name: work.name, stateHostCount: 1 } } });
    expect(blocked.failure?.message).not.toMatch(/empty authority/);
    expect(validateCaptureProvenance(options, { hub, work }).diagnosticContext).toEqual(diagnosticContext);
    for (const context of [diagnosticContext, { ...diagnosticContext, incidentFaithful: true },
      { ...diagnosticContext, extra: "unclassified" }, { partialDiagnostic: false, acceptanceEligible: true }]) {
      const captureManifest = { ...provenance.captureManifest, diagnosticContext: context };
      const changed = { ...options, captureManifest,
        captureManifestSha256: createHash("sha256").update(JSON.stringify(captureManifest)).digest("hex") };
      if (context === diagnosticContext) expect(validateCaptureProvenance(changed, { hub, work }).diagnosticContext).toEqual(context);
      else expect(() => validateCaptureProvenance(changed, { hub, work })).toThrow("invalid bounded diagnostic context");
    }
    // A genuine full population keeps the original acceptance predicate; no incident-faithful assertion is added.
    for (const snapshot of [hub, work]) {
      const host = snapshot.stateEntries[0]!;
      const id = `${snapshot.name}-participant`;
      snapshot.participantFiles = [{ ...host, key: "topology/participants/" + createHash("sha256").update(id).digest("hex"),
        value: { format: 1, id, ownerHostId: host.value.id, ownerIdentityId: host.value.identity.id, rootId: host.value.rootId } }];
    }
    work.leaseFiles = authorityFixture("work").leaseFiles;
    expect(validateAuthoritySnapshot(work, "work")).toBe(work);
    expect(classifyAuthorityCoverage({ hub, work })).toBeUndefined();
  });
  it.skipIf(!process.env.PI_AUTHORITY_WORK_RELEASE)("seeds state-only input through the actual native backend without fabricated lease/participant files", async () => {
    const snapshot = authorityFixture("work"); snapshot.leaseFiles = [];
    const { seedAuthoritySnapshot } = await import(pathToFileURL(path.resolve("scripts/lib/mixed-release-worker.mjs")).href);
    const seed = await seedAuthoritySnapshot(snapshot, path.join(tempRoot, "partial-native-seed"), process.env.PI_AUTHORITY_WORK_RELEASE!, snapshot.collectedAt + 1000);
    expect(seed).toMatchObject({ backend: "file", stateEntries: snapshot.stateEntries.length, participantFiles: 0, leaseFiles: 0 });
    expect(snapshot.leaseFiles).toEqual([]);
  });
  it("rebases times, not history ordinals; preserves versions, writers, placement and absence", () => {
    const snapshot = authorityFixture("hub");
    const source = snapshot.stateEntries[0]!;
    source.value.bridgeSupersededTokenSeen = { [`h:${"c".repeat(32)}`]: 7 };
    source.value.pid = 42; source.value.processIncarnation = "imported-not-live";
    source.value.mainProcess = { pid: 42, incarnation: "not-live" };
    source.value.reloadUntil = snapshot.collectedAt + 5_000;
    snapshot.leaseFiles[0]!.mirror = { remoteHost: "ryzen2", observedAt: snapshot.collectedAt,
      sourceObservedAt: snapshot.collectedAt, sourceUpdatedAt: snapshot.collectedAt, sourceExpiresAt: snapshot.collectedAt + 5_000,
      bridgeProcess: { pid: 42, incarnation: "imported-process" } };
    const before = JSON.stringify(snapshot), rebased = rebaseAuthoritySnapshot(snapshot, snapshot.collectedAt + 50_000);
    expect(JSON.stringify(snapshot)).toBe(before);
    expect(rebased.snapshot.stateEntries[0]).toMatchObject({ version: source.version, updatedBy: source.updatedBy,
      updatedAt: source.updatedAt + 50_000, value: { startedAt: source.value.startedAt + 50_000,
        bridgeSupersededTokenSeen: { [`h:${"c".repeat(32)}`]: 7 } } });
    expect(rebased.snapshot.stateEntries[0]!.value).not.toHaveProperty("pid");
    expect(rebased.snapshot.stateEntries[0]!.value).not.toHaveProperty("mainProcess");
    expect(rebased.snapshot.stateEntries[0]!.value.reloadUntil).toBe(source.value.reloadUntil + 50_000);
    expect(rebased.snapshot.leaseFiles[0]!.mirror).not.toHaveProperty("bridgeProcess");
    expect(rebased.snapshot.participantFiles).toEqual([]);
  });
  it("diagnoses lease-only tokenless authority, including own-root presence", () => {
    const hub = authorityFixture("hub", false, true), work = authorityFixture("work", false, true);
    const record = hub.stateEntries[0]!.value;
    const predicates = authorityPredicates({ hosts: [{ record }] }, work, "dev1", true);
    expect(predicates[0]).toMatchObject({ hostId: record.id, rootId: record.rootId, eligible: true,
      tokenless: true, supersededHistory: false, predicateKind: "tokenless" });
    expect(predicates[0]!.leaseOnly).toEqual([{ hostId: record.id, tokenHash: `h:${"a".repeat(32)}` }]);
    expect(matchAuthorityRefusal({ code: "FABRIC_BRIDGE_STALE_INCARNATION", error: `Bridge presence for ${record.rootId} has an older incarnation than persisted authority` }, predicates)).toMatchObject({
      actualRoot: record.rootId, rootMatchesFirst: true, attribution: "unique" });
    expect(matchAuthorityRefusal({ error: "Refused a different root" }, predicates)).toMatchObject({ rootMatchesFirst: false, attribution: "unmatched" });
    expect(matchAuthorityRefusal({ error: `Bridge presence for ${record.rootId} has an older incarnation than persisted authority`, code: "SETUP_ERROR" }, predicates))
      .toMatchObject({ rootMatchesFirst: false, attribution: "unmatched" });
  });
  it("does not guess a sibling host from a root-only error or require tokenless input", () => {
    const hub = authorityFixture("hub", true), work = authorityFixture("work");
    const records = hub.stateEntries.map(entry => entry.value);
    work.leaseFiles.push(...records.map(record => ({ id: record.id, rootId: record.rootId, identityId: record.identity.id,
      incarnationToken: record.incarnationToken, bridgeSupersededTokens: records.map(record => record.incarnationToken) })));
    const predicates = authorityPredicates({ hosts: records.map(record => ({ record })) }, work, "dev1", true);
    expect(predicates.every(row => !row.tokenless && row.supersededHistory)).toBe(true);
    expect(matchAuthorityRefusal({ code: "FABRIC_BRIDGE_STALE_INCARNATION", error: `Bridge presence for ${records[0]!.rootId} has an older incarnation than persisted authority` }, predicates)).toMatchObject({
      rootMatchesFirst: true, attribution: "ambiguous-same-root", firstCandidate: { hostId: records[0]!.id } });
  });
  it.skipIf(process.platform !== "linux" || process.release.name !== "node" || Number(process.versions.node.split(".")[0]) < 24)("blocks missing or mismatched release identities before seeding", async () => {
    const hub = path.join(tempRoot, "authority-unidentified"), work = path.join(tempRoot, "authority-identified-work");
    makeRelease(hub, CANDIDATE_SHA, true); makeRelease(work, MODERN_SHA, true);
    fs.unlinkSync(path.join(hub, "dist/build-identity.json"));
    const options = { hub, hubSha: CANDIDATE_SHA, work, workSha: MODERN_SHA,
      hubSnapshot: authorityFixture("hub"), workSnapshot: authorityFixture("work") };
    const missing = await runAuthorityCanary(options);
    expect(missing).toMatchObject({ outcome: "BLOCKED", arms: [], failure: { message: expect.stringMatching(/identity/) } });
    fs.writeFileSync(path.join(hub, "dist/build-identity.json"), JSON.stringify({ sha: LEGACY_SHA }));
    const wrong = await runAuthorityCanary(options);
    expect(wrong).toMatchObject({ outcome: "BLOCKED", arms: [], failure: { message: expect.stringMatching(/identity mismatch/) } });
  });
  it("returns BLOCKED, never RED, for a missing snapshot", async () => {
    const result = await runAuthorityCanary({ hub: "/unused", hubSha: "a".repeat(40), work: "/unused", workSha: "b".repeat(40),
      hubSnapshot: "/nonexistent-authority-snapshot", workSnapshot: authorityFixture("work") });
    expect(result).toMatchObject({ ok: false, outcome: "BLOCKED", releaseGate: false, rolloutPermission: false, arms: [] });
  });
});

// SYNTHETIC only. Explicit release paths opt into the real compiled hub-run -> agent
// boundary; ordinary tests do not build releases or claim real-fleet reproduction.
const nativeAuthority = process.env.PI_AUTHORITY_HUB_RELEASE && process.env.PI_AUTHORITY_WORK_RELEASE;
function fixtureProvenance(hub: AuthoritySnapshot, work: AuthoritySnapshot) {
  const pairId = "explicit-SYNTHETIC-fixture-pair";
  const sites = Object.fromEntries([hub, work].map(snapshot => {
    const binding = { site: snapshot.site, name: snapshot.name, backend: snapshot.backend, collectedAt: snapshot.collectedAt,
      captureSha256: createHash("sha256").update(JSON.stringify(snapshot)).digest("hex") };
    return [snapshot.site, { ...binding, fixtureReceipt: { ...binding, pairId } }];
  }));
  const captureManifest = { schemaVersion: 1, kind: "synthetic-fixture", pairId, sites };
  return { captureManifest, captureManifestSha256: createHash("sha256").update(JSON.stringify(captureManifest)).digest("hex") };
}
async function runSyntheticAuthorityCanary(options: AuthorityOptions) {
  const hub = options.hubSnapshot as AuthoritySnapshot, work = options.workSnapshot as AuthoritySnapshot;
  return runAuthorityCanary({ ...options, synthetic: true, ...fixtureProvenance(hub, work) });
}
function retainAuthorityProbe(result: Record<string, any>, label: string): void {
  // The test sandbox intentionally forbids copying outside its private TMPDIR. Its
  // owner captures stdout durably; retain the bounded sanitized wire evidence there
  // before smarty-test-env cleans the private runtime (no deletion-guard bypass).
  const wire = result.arms.filter((arm: any) => arm.trace).map((arm: any) => ({ arm: arm.name,
    records: fs.readFileSync(arm.trace, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)),
    producerTraces: arm.producers?.map((producer: any) => ({ name: producer.name, records: fs.readFileSync(producer.trace, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) })),
    hubStderr: fs.readFileSync(path.join(path.dirname(arm.trace), result.coverageSlice === "LIVE_NATIVE_WRITERS" ? "hub-run.stderr.log" : "hub.stderr.log"), "utf8") }));
  console.log(`SYNTHETIC_PROBE ${JSON.stringify({ label, result, wire })}`);
}
describe.skipIf(!nativeAuthority)("SYNTHETIC actual compiled authority CLI RPC", () => {
  const options = () => ({ hub: process.env.PI_AUTHORITY_HUB_RELEASE!, hubSha: process.env.PI_AUTHORITY_HUB_SHA!,
    work: process.env.PI_AUTHORITY_WORK_RELEASE!, workSha: process.env.PI_AUTHORITY_WORK_SHA!, timeoutMs: 25_000, synthetic: true });
  it("refuses same-root token siblings through real repeated hub passes (no tokenless sample)", async () => {
    const result = await runSyntheticAuthorityCanary({ ...options(), hubSnapshot: authorityFixture("hub", true), workSnapshot: authorityFixture("work") });
    retainAuthorityProbe(result, "siblings");
    if (result.outcome !== "RED") console.error(JSON.stringify(result));
    expect(result).toMatchObject({ outcome: "RED", evidenceKind: "SYNTHETIC", releaseGate: false, rolloutPermission: false });
    expect(result.arms[0]!.passes.length).toBeGreaterThanOrEqual(3);
    expect(result.arms[0]!.passes.at(-1).directions[1]).toMatchObject({ refusal: { rootMatchesFirst: true,
      firstCandidate: { predicateKind: "superseded-history", tokenless: false } } });
  }, 40_000);
  it("keeps captured expired siblings expired and reports bounded NON_REPRO, not rollout permission", async () => {
    const hub = authorityFixture("hub", true), work = authorityFixture("work");
    hub.stateEntries[1]!.value.expiresAt = hub.collectedAt - 1_000;
    hub.leaseFiles[1]!.expiresAt = hub.collectedAt - 1_000;
    // Short-lived SYNTHETIC natives expire naturally, without clock changes/renewals.
    for (const snapshot of [hub, work]) {
      snapshot.stateEntries[0]!.value.expiresAt = snapshot.collectedAt + 5_000;
      snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt + 5_000;
    }
    const native = options();
    const result = await runSyntheticAuthorityCanary({ ...native, hubSnapshot: hub, workSnapshot: work,
      candidate: native.work, candidateSha: native.workSha });
    retainAuthorityProbe(result, "expired-sibling");
    expect(result).toMatchObject({ outcome: "NON_REPRO", ok: false, limitedCoverage: true, releaseGate: false, rolloutPermission: false });
    expect(result.arms[0]!.inventory.hub.nativeHosts).toHaveLength(2);
    expect(result.arms[0]!.passes.length).toBeGreaterThanOrEqual(3);
    expect(result.arms[0]!.passes[0]!.outboundHosts).toBe(1);
    expect(result.arms[0]!.passes.every((pass: any) => pass.outboundHosts <= 1)).toBe(true);
    expect(result.arms.every(arm => arm.boundedPassingOutcome === false)).toBe(true);
    expect(result.arms[0]!.clock.policy).toMatch(/no renewal or resurrection/);
    expect(result.arms.map(arm => [arm.name, arm.outcome])).toEqual([["primary", "NON_REPRO"], ["candidate-control", "NON_REPRO"]]);
    expect(result.arms[0]!.seed.hub.rebase.deltaMs).toBe(result.arms[0]!.seed.work.rebase.deltaMs);
  }, 65_000);
  it("reports continuously live bounded passing evidence without rollout permission", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    for (const snapshot of [hub, work]) {
      snapshot.stateEntries[0]!.value.expiresAt = snapshot.collectedAt + 45_000;
      snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt + 45_000;
    }
    const result = await runSyntheticAuthorityCanary({ ...options(), hubSnapshot: hub, workSnapshot: work });
    retainAuthorityProbe(result, "bounded-pass");
    expect(result).toMatchObject({ outcome: "NON_REPRO", ok: false, acceptance: false, diagnosticComplete: true,
      coverageSlice: "CAPTURED_ONLY_DIAGNOSTIC", limitedCoverage: false, releaseGate: false, rolloutPermission: false });
    expect(result.arms[0]).toMatchObject({ boundedPassingOutcome: true, rolloutPermission: false });
    expect(result.arms[0]!.passes.length).toBeGreaterThanOrEqual(3);
  }, 40_000);
  it("attributes a real hub-local refusal separately from the fromHub agent direction", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    const workHost = work.stateEntries[0]!.value;
    delete workHost.incarnationToken; delete work.leaseFiles[0]!.incarnationToken;
    hub.leaseFiles.push({ format: 1, id: workHost.id, rootId: workHost.rootId, identityId: workHost.identity.id,
      startedAt: workHost.startedAt, updatedAt: work.collectedAt, expiresAt: work.collectedAt + 15_000,
      incarnationToken: `h:${"a".repeat(32)}` });
    const result = await runSyntheticAuthorityCanary({ ...options(), hubSnapshot: hub, workSnapshot: work });
    retainAuthorityProbe(result, "hub-local");
    expect(result.outcome).toBe("RED");
    expect(result.arms[0]!.passes.at(-1).directions[0]).toMatchObject({ direction: "work-to-hub", refusal: {
      rootMatchesFirst: true, firstCandidate: { predicateKind: "tokenless", hostId: workHost.id } } });
  }, 40_000);
  it("refuses own-root tokenless input against lease-only authority via the real agent", async () => {
    const result = await runSyntheticAuthorityCanary({ ...options(), hubSnapshot: authorityFixture("hub", false, true), workSnapshot: authorityFixture("work", false, true) });
    retainAuthorityProbe(result, "lease-only");
    if (result.outcome !== "RED") console.error(JSON.stringify(result));
    expect(result).toMatchObject({ outcome: "RED", evidenceKind: "SYNTHETIC", releaseGate: false });
    expect(result.arms[0]!.passes.at(-1).directions[1]).toMatchObject({ refusal: { rootMatchesFirst: true,
      firstCandidate: { predicateKind: "tokenless", supersededHistory: false } } });
  }, 40_000);
});

describe("live four-role writer boundary", () => {
  it("registers all new CLI fields and public functions without changing the full gate", () => {
    const argv = ["--authority-snapshot", "--hub", "/hub", "--work", "/work", "--hub-sha", "a".repeat(40), "--work-sha", "b".repeat(40),
      "--hub-snapshot", "/h.json", "--work-snapshot", "/w.json", "--live-writers", "--synthetic", "--candidate-both-sides",
      "--hub-writer", "/c42", "--hub-writer-sha", MODERN_SHA, "--work-writer", "/c42", "--work-writer-sha", MODERN_SHA,
      "--candidate", "/rc2h", "--candidate-sha", CANDIDATE_SHA, "--candidate-work", "/rc2w", "--candidate-work-sha", CANDIDATE_SHA,
      "--candidate-work-writer", "/rc2p", "--candidate-work-writer-sha", CANDIDATE_SHA, "--capture-manifest", "/pair.json",
      "--capture-manifest-sha256", "d".repeat(64)];
    expect(parseArgs(argv as ["--authority-snapshot", ...string[]])).toMatchObject({ liveWriters: true, synthetic: true, candidateBothSides: true,
      hubWriter: "/c42", hubWriterSha: MODERN_SHA, workWriter: "/c42", workWriterSha: MODERN_SHA, candidateWork: "/rc2w",
      candidateWorkSha: CANDIDATE_SHA, candidateWorkWriter: "/rc2p", candidateWorkWriterSha: CANDIDATE_SHA,
      captureManifest: "/pair.json", captureManifestSha256: "d".repeat(64) });
    for (const fn of [runLiveAuthorityCanary, validateCaptureProvenance, createAuthorityWriterPlan, evaluateAuthorityWriterCoverage, collectNaturalExpiry, sanitizeAuthorityEvidence]) expect(typeof fn).toBe("function");
    expect(parseArgs(["--new", "/n", "--old", "/o"]).holdMs).toBe(60_000);
  });
  it("binds exact bytes, site, backend, timestamp and fixture receipt; rejects malformed or REAL object-only provenance", () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work"), provenance = fixtureProvenance(hub, work);
    const options = { hub: "/h", hubSha: CANDIDATE_SHA, work: "/w", workSha: MODERN_SHA, synthetic: true,
      hubSnapshot: hub, workSnapshot: work, ...provenance };
    expect(validateCaptureProvenance(options, { hub, work })).toMatchObject({ kind: "synthetic-fixture" });
    expect(() => validateCaptureProvenance({ ...options, captureManifestSha256: "0".repeat(64) }, { hub, work })).toThrow(/hash/);
    const changed = clone(hub); changed.name = "changed";
    expect(() => validateCaptureProvenance(options, { hub: changed, work })).toThrow(/binding/);
    expect(() => validateCaptureProvenance({ ...options, synthetic: false }, { hub, work })).toThrow();
  });
  it.skipIf(!fs.existsSync(process.env.PI_CAPTURE_PROPOSALS_V2_ROOT ?? path.resolve("../capture-proposals-v2")))(
    "validates both pinned capture proposals against original snapshot bytes without native replay", () => {
      const proposalRoot = process.env.PI_CAPTURE_PROPOSALS_V2_ROOT ?? path.resolve("../capture-proposals-v2");
      const originalRoot = process.env.PI_CAPTURE_ORIGINALS_ROOT ?? "/home/paul/lanes/fv2-canary3/.local/inputs/capture-1630";
      const index = JSON.parse(fs.readFileSync(path.join(proposalRoot, "index.json"), "utf8"));
      const originalNames = Object.fromEntries(index.originals.map((row: any) => [row.file, row]));
      const specs = index.results.map((row: any) => {
        const manifestPath = path.join(proposalRoot, row.id, "manifest.json");
        const manifestBytes = fs.readFileSync(manifestPath);
        expect(createHash("sha256").update(manifestBytes).digest("hex")).toBe(row.manifestSha256);
        const manifest = JSON.parse(manifestBytes.toString("utf8"));
        const hubPath = path.join(originalRoot, "hub.json"), workName = row.id === "mixed-live-hub-1240-work" ? "work-snap-1240.json" : "work.json";
        const workPath = path.join(originalRoot, workName);
        const hubBytes = fs.readFileSync(hubPath), workBytes = fs.readFileSync(workPath);
        expect(createHash("sha256").update(hubBytes).digest("hex")).toBe(originalNames["hub.json"].sha256);
        expect(createHash("sha256").update(workBytes).digest("hex")).toBe(originalNames[workName].sha256);
        const hub = JSON.parse(hubBytes.toString("utf8")), work = JSON.parse(workBytes.toString("utf8"));
        const options = { hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
          hubSnapshot: hubPath, workSnapshot: workPath, captureManifest: manifestPath, synthetic: false,
          captureManifestSha256: row.manifestSha256 };
        const validated = validateCaptureProvenance(options, { hub, work });
        return { id: row.id, manifestHashMatches: true, originalHashesMatch: true, provenanceKind: validated.kind,
          partialDiagnostic: validated.diagnosticContext?.partialDiagnostic === true, acceptanceEligible: validated.diagnosticContext?.acceptanceEligible ?? true,
          stateHosts: [hub.stateEntries.filter((entry: any) => entry.key.startsWith("topology/hosts/")).length,
            work.stateEntries.filter((entry: any) => entry.key.startsWith("topology/hosts/")).length],
          participantFiles: [hub.participantFiles.length, work.participantFiles.length], leaseFiles: [hub.leaseFiles.length, work.leaseFiles.length] };
      });
      console.log(JSON.stringify({ captureProvenanceValidation: specs }));
      expect(specs).toHaveLength(2);
      expect(specs[0]).toMatchObject({ manifestHashMatches: true, originalHashesMatch: true, partialDiagnostic: false, acceptanceEligible: true });
      expect(specs[1]).toMatchObject({ manifestHashMatches: true, originalHashesMatch: true, partialDiagnostic: true, acceptanceEligible: false,
        participantFiles: [expect.any(Number), 0], leaseFiles: [expect.any(Number), 0] });
    });
  it("excludes ALL capture reservations from one fixed additive identity plan", () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    const before = JSON.stringify({ hub, work }), plan = createAuthorityWriterPlan({ hub, work });
    expect(plan).toHaveLength(3); expect(new Set(plan.map(row => row.rootId)).size).toBe(3);
    for (const row of plan) expect(before).not.toContain(row.rootId);
    expect(JSON.stringify({ hub, work })).toBe(before);
  });
  it("never promotes renewals or a clock comparison to actual compiled expiry", () => {
    expect(evaluateAuthorityWriterCoverage({ expiry: [{ captured: true, compiled: false, kind: "clock-only", expiresAt: 0 }] }))
      .toMatchObject({ naturalExpiry: false, expiryCoverage: "INCOMPLETE/BLOCKED" });
    expect(evaluateAuthorityWriterCoverage({ expiry: [{ captured: true, compiled: true, kind: "compiled-compaction", applied: true,
      before: { key: "real-captured-row" }, after: null }] })).toMatchObject({ naturalExpiry: false });
    expect(evaluateAuthorityWriterCoverage({ expiry: [{ captured: true, compiled: true, kind: "compiled-presence-expiry", preservedExpiry: true,
      expiresAt: 10, before: { present: true, observedAt: 9 }, after: { present: false, observedAt: 11 } }] })).toMatchObject({ naturalExpiry: false });
  });
  it("F1 refuses an unexpired compiled delete and a before-state without expiry proof", () => {
    for (const before of [{ key: "captured" }, { key: "captured", value: { expiresAt: 61_000 } }]) {
      expect(evaluateAuthorityWriterCoverage({ expiry: [{ kind: "compiled-compaction", captured: true, compiled: true,
        applied: true, at: 1_000, before, after: null }] }).naturalExpiry).toBe(false);
    }
  });
  it("F1 emitter and predicate bind the original capture, matching leases and real reload extensions to causal deletion", () => {
    const capture = authorityFixture("hub"), delta = 500;
    capture.collectedAt = 1_000;
    Object.assign(capture.stateEntries[0]!.value, { startedAt: 500, expiresAt: 1_100 });
    Object.assign(capture.leaseFiles[0]!, { startedAt: 500, expiresAt: 1_200 });
    const seeded = rebaseAuthoritySnapshot(capture, 1_500).snapshot;
    const key = seeded.stateEntries[0]!.key;
    const trace = path.join(tempRoot, "expiry-emitter.jsonl");
    const batch = { type: "compiled-directory-batch", beganAt: 2_001, at: 2_002,
      before: { stateEntries: seeded.stateEntries, leaseFiles: [{ ...seeded.leaseFiles[0], reloadUntil: 2_000 }] },
      after: { stateEntries: [], leaseFiles: [] }, operations: [{ kind: "delete", key, ifVersion: seeded.stateEntries[0]!.version }],
      results: [{ key, applied: true }], process: { pid: 1 } };
    const emit = () => {
      fs.writeFileSync(trace, JSON.stringify(batch) + "\n");
      return collectNaturalExpiry({ hub: capture, work: { ...authorityFixture("work"), stateEntries: [] } }, delta,
        { hub: [], work: [] }, [{ site: "hub", trace, registration: { artifacts: {} } }], {}, {});
    };
    expect(evaluateAuthorityWriterCoverage({ expiry: emit() }).naturalExpiry).toBe(true);
    batch.beganAt = 1_999; // The actual matching reload lease is still live.
    expect(emit()).toEqual([]);
    batch.beganAt = 2_001;
    capture.stateEntries[0]!.value.expiresAt = 2_000; // Original host expiry rebases to 2500.
    expect(emit()).toEqual([]);
    capture.stateEntries[0]!.value.expiresAt = 1_100;
    capture.leaseFiles[0]!.expiresAt = 2_000; // Original matching lease cannot be shortened by before-state.
    expect(emit()).toEqual([]);
    capture.leaseFiles[0]!.expiresAt = 1_200;
    batch.after.leaseFiles = [{ ...seeded.leaseFiles[0], expiresAt: 3_000 }] as any;
    expect(emit()).toEqual([]);
    batch.after.leaseFiles = [];
    batch.results[0]!.applied = false; // Elapsed time alone never qualifies.
    expect(emit()).toEqual([]);
    batch.results[0]!.applied = true;
    const evidence = emit();
    expect(evidence).toHaveLength(1);
    expect(evaluateAuthorityWriterCoverage({ expiry: evidence }).naturalExpiry).toBe(true);
    delete evidence[0]!.expiryProof;
    expect(evaluateAuthorityWriterCoverage({ expiry: evidence }).naturalExpiry).toBe(false);
  });
  it("F1 refuses presence disappearance when the current host checkpoint or matching lease extends its deadline", () => {
    const hub = authorityFixture("hub"), work = { ...authorityFixture("work"), stateEntries: [] };
    hub.collectedAt = 1_000;
    Object.assign(hub.stateEntries[0]!.value, { startedAt: 500, expiresAt: 1_100 });
    Object.assign(hub.leaseFiles[0]!, { startedAt: 500, expiresAt: 1_200 });
    const seeded = rebaseAuthoritySnapshot(hub, 1_500).snapshot;
    const before = { at: 1_900, presence: { hosts: [{ record: seeded.stateEntries[0]!.value, expiresAt: 2_000 }] },
      authority: { stateEntries: seeded.stateEntries, leaseFiles: [{ ...seeded.leaseFiles[0], reloadUntil: 2_000 }] } };
    const after = { at: 2_100, presence: { hosts: [] }, authority: {
      stateEntries: [{ ...seeded.stateEntries[0], value: { ...seeded.stateEntries[0]!.value, expiresAt: 3_000 } }], leaseFiles: [] } };
    const emit = () => collectNaturalExpiry({ hub, work }, 500, { hub: [before, after], work: [] }, [], {}, { hub: {} });
    expect(emit()).toEqual([]);
    after.authority.stateEntries = seeded.stateEntries as any;
    after.authority.leaseFiles = [{ ...seeded.leaseFiles[0], reloadUntil: 3_000 }] as any;
    expect(emit()).toEqual([]);
    after.authority.leaseFiles = [];
    const evidence = emit();
    expect(evidence).toHaveLength(1);
    expect(evaluateAuthorityWriterCoverage({ expiry: evidence }).naturalExpiry).toBe(true);
  });
  it("F2 requires gap-free unique completed pass ordinals and excludes in-flight rounds", () => {
    const passes = (ordinals: number[]) => ordinals.map(pass => ({ pass, remoteMirror: "fulfilled", inboundHosts: 1,
      outboundHosts: 1, allWriterOrigins: true, bothMirrors: true }));
    expect(evaluateAuthorityWriterCoverage({ passes: passes([2, 9, 10]) }).continuity).toBe(false);
    expect(evaluateAuthorityWriterCoverage({ passes: passes([8, 9, 9]) }).continuity).toBe(false);
    expect(evaluateAuthorityWriterCoverage({ passes: passes([8, 9, 10]), inFlightAtMeasurementEnd: [9] } as any).continuity).toBe(false);
    expect(evaluateAuthorityWriterCoverage({ passes: passes([8, 9, 10]), inFlightAtMeasurementEnd: [11] } as any).continuity).toBe(true);
  });
  it("F3 hashes raw 32hex token values, history entries and map keys, preserving h32 and canonical IDs", () => {
    const raw = "a".repeat(32), stable = `h:${"b".repeat(32)}`;
    const expected = `h:${createHash("sha256").update(raw).digest("hex").slice(0, 32)}`;
    const input = { id: raw, rootId: raw, incarnationToken: raw, ownerIncarnation: raw, processIncarnation: raw,
      incarnation: raw, tokenHash: raw, history: [raw, stable], bridgeSupersededTokens: [raw, stable],
      bridgeSupersededTokenSeen: { [raw]: 7, [stable]: 9 }, currentTokens: [raw], knownHistory: [raw] };
    const clean = sanitizeAuthorityEvidence(input);
    expect(clean).toMatchObject({ id: raw, rootId: raw, incarnationToken: expected, ownerIncarnation: expected,
      processIncarnation: expected, incarnation: expected, tokenHash: expected, history: [expected, stable],
      bridgeSupersededTokens: [expected, stable], bridgeSupersededTokenSeen: { [expected]: 7, [stable]: 9 },
      currentTokens: [expected], knownHistory: [expected] });
    expect(expected).not.toBe(`h:${raw}`);
    expect(sanitizeAuthorityEvidence(clean)).toEqual(clean);
    expect(input.incarnationToken).toBe(raw);
  });
  it("F3 fails closed with a fixed safe error on unclassified token-like fields", () => {
    for (const key of ["newToken", "futureNonce", "ownerIncarnationExtra", "privateCredential"]) {
      expect(() => sanitizeAuthorityEvidence({ nested: { [key]: "sensitive-value" } })).toThrow(/^authority evidence rejected$/);
    }
  });
  it("F3 propagates an unclassified capture token field to BLOCKED without raw diagnostics or native work", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    hub.stateEntries[0]!.value.unclassifiedTokenField = "sensitive-value";
    const result = await runLiveAuthorityCanary({ hub: "/unused", hubSha: CANDIDATE_SHA, work: "/unused", workSha: MODERN_SHA,
      hubSnapshot: hub, workSnapshot: work, synthetic: true });
    expect(result).toMatchObject({ outcome: "BLOCKED", ok: false, acceptance: false, arms: [], failure: { message: "authority evidence rejected" } });
    expect(JSON.stringify(result)).not.toMatch(/unclassifiedTokenField|sensitive-value/);
  });
  it("hashes fresh native nonces only in evidence and preserves all history joins and the original wire object", () => {
    const nonce = "fresh-native-uuid-nonce", raw = { incarnationToken: nonce, ownerIncarnation: nonce,
      bridgeSupersededTokens: [nonce], bridgeSupersededTokenSeen: { [nonce]: 7 } };
    const clean = sanitizeAuthorityEvidence(raw);
    expect(clean.incarnationToken).toMatch(/^h:[a-f0-9]{32}$/);
    expect(clean.ownerIncarnation).toBe(clean.incarnationToken);
    expect(clean.bridgeSupersededTokens).toEqual([clean.incarnationToken]);
    expect(clean.bridgeSupersededTokenSeen).toEqual({ [clean.incarnationToken]: 7 });
    expect(raw.incarnationToken).toBe(nonce); expect(JSON.stringify(clean)).not.toContain(nonce);
  });
});

const liveNativeAuthority = nativeAuthority && process.env.PI_AUTHORITY_WRITER_RELEASE;
describe.skipIf(!liveNativeAuthority)("SYNTHETIC actual compiled live c42 producers on seeded SQLite/FILE", () => {
  const options = (hub: AuthoritySnapshot, work: AuthoritySnapshot): AuthorityOptions => ({
    hub: process.env.PI_AUTHORITY_HUB_RELEASE!, hubSha: process.env.PI_AUTHORITY_HUB_SHA!,
    work: process.env.PI_AUTHORITY_WORK_RELEASE!, workSha: process.env.PI_AUTHORITY_WORK_SHA!,
    hubWriter: process.env.PI_AUTHORITY_WRITER_RELEASE!, hubWriterSha: MODERN_SHA,
    workWriter: process.env.PI_AUTHORITY_WRITER_RELEASE!, workWriterSha: MODERN_SHA,
    liveWriters: true, synthetic: true, hubSnapshot: hub, workSnapshot: work, ...fixtureProvenance(hub, work), timeoutMs: 10_000,
  });
  it("runs actual native producers, two renewals, real captured expiry and both compiled mirror orientations", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    for (const snapshot of [hub, work]) { snapshot.stateEntries[0]!.value.expiresAt = snapshot.collectedAt + 6_000; snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt + 6_000; }
    const result = await runAuthorityCanary(options(hub, work));
    retainAuthorityProbe(result, "live-renewal-natural-expiry");
    expect(result.failure).toBeUndefined();
    expect(result.arms[0]).toMatchObject({ outcome: "NON_REPRO", cleanup: { ok: true }, coverage: { renewals: true, naturalExpiry: true, continuity: true } });
    expect(result.arms[0]!.producers).toHaveLength(3);
    expect(result.arms[0]!.producers.every((row: any) => row.identityVerified && row.observations.length >= 3)).toBe(true);
    const capturedKeys = new Set([...hub.stateEntries, ...work.stateEntries].map(row => row.key));
    for (const producer of result.arms[0]!.producers) {
      const rows = fs.readFileSync(producer.trace, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows.filter(row => row.type === "compiled-directory-batch").every(row => row.operations.every((op: any) => op.kind !== "put" || !capturedKeys.has(op.key)))).toBe(true);
      expect(producer.observations.every((row: any) => row.readback.id === producer.rootId)).toBe(true);
    }
    expect(result).toMatchObject({ ok: false, acceptance: false, evidenceKind: "SYNTHETIC", releaseGate: false, rolloutPermission: false });
    expect(result.scope).toBe("producer-boundary compatibility; principal session bootstrap and auth policy are not exercised");
  }, 25_000);
  it("F1 retains genuine compiled applied deletion/readback absence after natural captured expiry", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    for (const snapshot of [hub, work]) {
      const past = snapshot.collectedAt - 7 * 60 * 60 * 1_000;
      for (const entry of snapshot.stateEntries) {
        entry.updatedAt = past;
        Object.assign(entry.value, { startedAt: past - 1_000, updatedAt: past, expiresAt: past });
      }
      for (const lease of snapshot.leaseFiles) Object.assign(lease, { startedAt: past - 1_000, updatedAt: past, expiresAt: past });
    }
    const result = await runAuthorityCanary(options(hub, work));
    retainAuthorityProbe(result, "live-actual-compiled-compaction");
    // This short synthetic window tests real compaction, not a continuity waiver:
    // the initial mirror establishment round may leave fewer than three full rounds.
    expect(result.arms[0]).toMatchObject({ outcome: "NON_REPRO", cleanup: { ok: true }, coverage: { naturalExpiry: true, renewals: true } });
    expect(result.arms[0]!.expiry.some((row: any) => row.kind === "compiled-compaction" && row.applied && row.after === null)).toBe(true);
  }, 25_000);
  it("keeps missing natural captured expiry BLOCKED despite actual live renewals", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work");
    for (const snapshot of [hub, work]) { snapshot.stateEntries[0]!.value.expiresAt = snapshot.collectedAt + 45_000; snapshot.leaseFiles[0]!.expiresAt = snapshot.collectedAt + 45_000; }
    const result = await runAuthorityCanary({ ...options(hub, work), timeoutMs: 6_000 });
    retainAuthorityProbe(result, "live-missing-expiry");
    expect(result.arms[0]).toMatchObject({ outcome: "BLOCKED", coverage: { renewals: true, naturalExpiry: false }, cleanup: { ok: true } });
  }, 20_000);
  it("rejects the wrong four-role mix and a hub-only candidate before any native spawn", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work"), native = options(hub, work);
    const wrong = await runAuthorityCanary({ ...native, workWriterSha: CANDIDATE_SHA });
    expect(wrong).toMatchObject({ outcome: "BLOCKED", arms: [], failure: { message: expect.stringMatching(/four-role mix/) } });
    const hubOnly = await runAuthorityCanary({ ...native, candidate: native.hub, candidateSha: native.hubSha });
    expect(hubOnly).toMatchObject({ outcome: "BLOCKED", arms: [], failure: { message: expect.stringMatching(/BOTH RC2/) } });
  });
  it("cancels live setup/measurement without orphaning native writers or descendants", async () => {
    const hub = authorityFixture("hub"), work = authorityFixture("work"), controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("authorized synthetic cancellation")), 3_000);
    let result;
    try { result = await runAuthorityCanary({ ...options(hub, work), signal: controller.signal }); }
    finally { clearTimeout(timer); }
    retainAuthorityProbe(result!, "live-cancellation");
    expect(result!.arms[0]).toMatchObject({ outcome: "BLOCKED", cleanup: { ok: true } });
    expect(result!.arms[0]!.cleanup.owned.every((row: any) => row.groupEmpty)).toBe(true);
  }, 15_000);
  it("retains actual old stale refusal separately from post-fatal expiry and live producer renewals", async () => {
    const hub = authorityFixture("hub", true), work = authorityFixture("work");
    for (const snapshot of [hub, work]) for (const host of snapshot.stateEntries) host.value.expiresAt = snapshot.collectedAt + 6_000;
    for (const snapshot of [hub, work]) for (const lease of snapshot.leaseFiles) lease.expiresAt = snapshot.collectedAt + 6_000;
    const result = await runAuthorityCanary(options(hub, work));
    retainAuthorityProbe(result, "live-old-refusal");
    expect(result.arms[0]).toMatchObject({ outcome: "RED", observedRefusal: "MATCHED_NATIVE_RED", cleanup: { ok: true },
      coverage: { renewals: true, naturalExpiry: true } });
    expect(result.arms[0]!.expiryObservationScope).toMatch(/post-fatal/);
  }, 25_000);
});

describe("mixed-release canary CLI does not execute on import or write release inputs on invalid args", () => {
  const cli = path.resolve("scripts/mixed-release-canary.mjs");

  it("imports without invoking the CLI", () => {
    const result = spawnSync("node", ["--input-type=module", "-e", `await import(${JSON.stringify(pathToFileURL(cli).href)})`], {
      cwd: process.cwd(), encoding: "utf8", timeout: 10_000,
    });
    expect(result.status).toBe(0);
    expect(result.stdout).toBe("");
    expect(result.stderr).toBe("");
  });

  it("emits one JSON failure receipt for invalid flags and does not write under --new", () => {
    const releaseNew = path.join(tempRoot, "cli-new-input");
    const releaseOld = path.join(tempRoot, "cli-old-input");
    fs.mkdirSync(releaseNew, { recursive: true });
    fs.mkdirSync(releaseOld, { recursive: true });
    const before = fs.readdirSync(releaseNew);
    const result = spawnSync("node", [cli,
      "--new", releaseNew, "--old", releaseOld,
      "--receipt", path.join(releaseNew, "invalid-receipt.json"), "--unexpected",
    ], { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 });

    expect(result.status).not.toBe(0);
    expect(result.error).toBeUndefined();
    const lines = result.stdout.trim().split("\n");
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0]!).ok).toBe(false);
    expect(fs.readdirSync(releaseNew)).toEqual(before);
  });

  it("emits one JSON failure receipt for a sub-minute CLI hold", () => {
    const releaseNew = path.join(tempRoot, "cli-hold-new");
    const releaseOld = path.join(tempRoot, "cli-hold-old");
    fs.mkdirSync(releaseNew, { recursive: true });
    fs.mkdirSync(releaseOld, { recursive: true });
    const result = spawnSync("node", [cli,
      "--new", releaseNew, "--old", releaseOld, "--hold-ms", "59999",
    ], { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 });

    expect(result.status).not.toBe(0);
    expect(result.error).toBeUndefined();
    const lines = result.stdout.trim().split("\n");
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0]!).ok).toBe(false);
    expect(fs.readdirSync(releaseNew)).toEqual([]);
    expect(fs.readdirSync(releaseOld)).toEqual([]);
  });
});
