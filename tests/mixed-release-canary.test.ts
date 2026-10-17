import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { evaluateReceipt, parseArgs, validateInputs } from "../scripts/mixed-release-canary.mjs";

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
