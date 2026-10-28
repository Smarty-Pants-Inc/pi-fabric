#!/usr/bin/env node
// Real, private mixed-release mesh gate. All artifacts and raw logs are retained in scratch.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { pathToFileURL, fileURLToPath } from "node:url";
import { seedAuthoritySnapshot, validateAuthoritySnapshot, matchAuthorityRefusal, rebaseAuthoritySnapshot,
  sanitizeAuthorityEvidence, nativeProcessEvidence } from "./lib/mixed-release-worker.mjs";
export { sanitizeAuthorityEvidence } from "./lib/mixed-release-worker.mjs";
export { validateAuthoritySnapshot, rebaseAuthoritySnapshot, authorityPredicates, matchAuthorityRefusal } from "./lib/mixed-release-worker.mjs";

const SHA = /^[a-f0-9]{40}$/;
const hash = value => createHash("sha256").update(value).digest("hex");
const WORKER = fileURLToPath(new URL("./lib/mixed-release-worker.mjs", import.meta.url));
const MIN_HOLD = 60_000;

export function parseArgs(argv) {
  if (argv.includes("--authority-snapshot")) return parseAuthorityArgs(argv);
  const result = { old: [], holdMs: MIN_HOLD, baselineControl: false };
  const seen = new Set();
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === "--baseline-control") {
      if (seen.has(flag)) throw new Error(`duplicate ${flag}`);
      seen.add(flag); result.baselineControl = true; continue;
    }
    if (!["--new", "--old", "--receipt", "--hold-ms"].includes(flag)) throw new Error(`unknown argument: ${flag}`);
    const value = argv[++i];
    if (!value || value.startsWith("--")) throw new Error(`missing value for ${flag}`);
    if (flag !== "--old" && seen.has(flag)) throw new Error(`duplicate ${flag}`);
    seen.add(flag);
    if (flag === "--old") result.old.push(value);
    else if (flag === "--hold-ms") {
      if (!/^\d+$/.test(value)) throw new Error("--hold-ms must be an integer >= 60000");
      result.holdMs = Number(value);
    } else result[flag.slice(2)] = value;
  }
  if (!result.new || result.old.length < 1) throw new Error("usage: --new DIR --old DIR [--old DIR ...] [--receipt FILE] [--hold-ms 60000] [--baseline-control]");
  if (!Number.isSafeInteger(result.holdMs) || result.holdMs < MIN_HOLD || result.holdMs > 2_147_000_000)
    throw new Error("--hold-ms must be an integer >= 60000 within the bounded process deadline");
  return result;
}

function relayCapability(dir) {
  const sourceFile = path.join(dir, "src/mesh/bridge.ts"), compiledFile = path.join(dir, "dist/mesh-bridge.js");
  const source = fs.readFileSync(sourceFile, "utf8"), compiled = fs.readFileSync(compiledFile, "utf8");
  const locate = (file, text, pattern) => {
    const lines = text.split("\n"), index = lines.findIndex(line => pattern.test(line));
    return index < 0 ? null : { file, line: index + 1, text: lines[index].trim(), sha256: hash(text) };
  };
  const capable = [locate(sourceFile, source, /async relayPresence\(/), locate(sourceFile, source, /relayed && \(!fromHub/),
    locate(compiledFile, compiled, /async relayPresence\(/), locate(compiledFile, compiled, /relayed && \(!fromHub/)];
  if (capable.every(Boolean)) return { supported: true, evidence: capable };
  const preRelay = [locate(sourceFile, source, /if \(remoteHostOf\(record\) !== undefined\) continue/),
    locate(sourceFile, source, /return record && remoteHostOf\(record\) === undefined/),
    locate(compiledFile, compiled, /if \(remoteHostOf\(record\) !== void 0\) continue/),
    locate(compiledFile, compiled, /return record && remoteHostOf\(record\) === void 0/)];
  if (preRelay.every(Boolean) && !/async relayPresence\(/.test(compiled)) return { supported: false,
    reason: "unsupported (pre-existing)", evidence: preRelay };
  throw new Error(`cannot establish relay capability from matching source and compiled artifact: ${dir}`);
}

function releaseIdentity(input) {
  const dir = fs.realpathSync(input);
  const pkg = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
  if (pkg.name !== "pi-fabric") throw new Error(`not pi-fabric: ${dir}`);
  for (const file of ["bin/mesh-bridge", "dist/mesh-bridge.js", "dist/chunks"]) {
    if (!fs.existsSync(path.join(dir, file))) throw new Error(`missing release artifact: ${dir}/${file}`);
  }
  let sha, identitySource;
  for (const file of ["dist/build-identity.json", "build-identity.json", ".build-identity.json"]) {
    if (!fs.existsSync(path.join(dir, file))) continue;
    const identity = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
    sha = identity.sha ?? identity.commit ?? identity.gitSha ?? identity.releaseSha;
    if (!SHA.test(sha ?? "")) throw new Error(`invalid full release SHA in ${dir}/${file}`);
    identitySource = file; break;
  }
  if (!sha) {
    try {
      const top = fs.realpathSync(execFileSync("git", ["-C", dir, "rev-parse", "--show-toplevel"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 5_000 }).trim());
      if (top === dir) {
        sha = execFileSync("git", ["-C", dir, "rev-parse", "HEAD"], { encoding: "utf8", timeout: 5_000 }).trim();
        identitySource = "git:HEAD";
      }
    } catch { /* Installed immutable artifacts need not retain .git. */ }
  }
  // Fabric's installed artifact identity is its canonical full-SHA directory (the same
  // convention as ownFabricRelease). Never infer a shortened SHA or use the current link.
  if (!sha && SHA.test(path.basename(dir))) { sha = path.basename(dir); identitySource = "canonical-full-sha-artifact-directory"; }
  if (!SHA.test(sha ?? "")) throw new Error(`no full release build identity or exact Git root: ${dir}`);
  return { dir, sha, identitySource, version: pkg.version,
    bridgeSha256: hash(fs.readFileSync(path.join(dir, "dist/mesh-bridge.js"))), relayCapability: relayCapability(dir) };
}

export function validateInputs(options) {
  if (process.platform !== "linux" || process.release.name !== "node") throw new Error("production authenticated relay requires Linux Node");
  if (Number(process.versions.node.split(".")[0]) < 24) throw new Error("Node >=24 required for production SQLite");
  if (!options?.new || !Array.isArray(options.old) || !options.old.length || !Number.isSafeInteger(options.holdMs) || options.holdMs < MIN_HOLD)
    throw new Error("invalid gate inputs");
  const candidate = releaseIdentity(options.new), old = options.old.map(releaseIdentity);
  if (!options.baselineControl && new Set([candidate.sha, ...old.map(release => release.sha)]).size !== old.length + 1)
    throw new Error("normal gate requires distinct candidate and old release SHAs; identical releases are baseline-control only");
  return { candidate, old };
}

function validCapabilityEvidence(cites, supported) {
  if (!Array.isArray(cites) || cites.length < 4 || !cites.every(cite => typeof cite?.file === "string" &&
    Number.isInteger(cite.line) && cite.line > 0 && typeof cite.text === "string" && cite.text.length > 0 &&
    /^[a-f0-9]{64}$/.test(cite.sha256 ?? ""))) return false;
  const source = cites.find(cite => cite.file.endsWith("/src/mesh/bridge.ts"));
  if (!source) return false;
  const root = source.file.slice(0, -"src/mesh/bridge.ts".length);
  const artifacts = [
    ["src/mesh/bridge.ts", supported ? [/async relayPresence\(/, /relayed && \(!fromHub/] :
      [/if \(remoteHostOf\(record\) !== undefined\) continue/, /return record && remoteHostOf\(record\) === undefined/]],
    ["dist/mesh-bridge.js", supported ? [/async relayPresence\(/, /relayed && \(!fromHub/] :
      [/if \(remoteHostOf\(record\) !== void 0\) continue/, /return record && remoteHostOf\(record\) === void 0/]],
  ];
  return cites.every(cite => artifacts.some(([file]) => cite.file === root + file)) && artifacts.every(([file, patterns]) => {
    const rows = cites.filter(cite => cite.file === root + file);
    return rows.length >= 2 && rows.every(cite => cite.sha256 === rows[0].sha256) &&
      patterns.every(pattern => rows.some(cite => pattern.test(cite.text)));
  });
}

/** Pure fail-closed judgment over collected evidence. Missing evidence is never a PASS.
 * Schema: nodes [{id,name,role,sha,ownerIncarnation}], releases [{sha}], hold {startedAt,endedAt},
 * links [{name,starts,linkedAt,observedUntil,closedAt,relayAuthenticated}], registrations,
 * nodes additionally carry relayCapable and capabilityEvidence (source+compiled file/line/hash).
 * routes [{phase,source,target,remoteHost,ownerIncarnation,via}], unsupported [{source,target,reason,lackingReleases}],
 * plans [{marker,source,target,round}],
 * sends [{marker,source,target,commandId,ack}], tallies [{id,entries,invalid,duplicateCalls,ackEvents}],
 * history {fresh,seeded,preserved,after,old,hub,warning,attempt,liveAtAttempt,destination}.
 * The independent fresh primary remains a normal node; the historied sentinel is diagnostic only.
 * The CLI retains raw host/lease/participant/owner readbacks and public request outcomes.
 */
export function evaluateReceipt(e = {}) {
  const checks = [];
  const check = (name, pass, evidence) => checks.push({ name, status: pass ? "PASS" : "FAIL", evidence });
  const nodes = e.nodes ?? [], links = e.links ?? [], plans = e.plans ?? [], sends = e.sends ?? [], tallies = e.tallies ?? [];
  const baseline = e.mode === "baseline-control", spokes = nodes.filter(node => node.role !== "hub");
  const topology = nodes.length >= 3 && nodes.filter(node => node.role === "hub").length === 1 &&
    new Set(nodes.map(node => node.id)).size === nodes.length && new Set(nodes.map(node => node.name)).size === nodes.length &&
    spokes.filter(node => node.role === "new").length === 1 && spokes.some(node => node.role === "old") &&
    nodes.find(node => node.role === "hub")?.sha === spokes.find(node => node.role === "new")?.sha &&
    nodes.find(node => node.role === "hub")?.relayCapable === true && spokes.find(node => node.role === "new")?.relayCapable === true &&
    nodes.every(node => ["hub", "new", "old"].includes(node.role) && SHA.test(node.sha ?? "") && (node.ownerIncarnation === undefined ||
      typeof node.ownerIncarnation === "string" && node.ownerIncarnation.length > 0) && typeof node.relayCapable === "boolean" &&
      node.capabilityEvidence?.supported === node.relayCapable &&
      validCapabilityEvidence(node.capabilityEvidence.evidence, node.relayCapable));
  const releases = e.releases ?? [];
  check("release-identities-and-topology", topology && releases.length === spokes.length &&
    releases.every(release => SHA.test(release.sha ?? "") && spokes.some(node => node.sha === release.sha)) &&
    spokes.every(node => releases.some(release => release.sha === node.sha)) &&
    (baseline || new Set(releases.map(release => release.sha)).size === releases.length), { nodes, releases, mode: e.mode });
  const hold = e.hold ?? {};
  check("continuous-hold", Number.isFinite(hold.startedAt) && Number.isFinite(hold.endedAt) && e.holdMs >= MIN_HOLD &&
    hold.endedAt - hold.startedAt >= e.holdMs && (hold.elapsedMs === undefined || Number.isFinite(hold.elapsedMs) && hold.elapsedMs >= e.holdMs), { requestedMs: e.holdMs, ...hold });
  check("production-links-no-restarts", topology && (e.restartCount === undefined || e.restartCount === 0) &&
    links.length === spokes.length && spokes.every(node => {
    const matches = links.filter(link => link.name === node.name), link = matches[0];
    return matches.length === 1 && link.starts === 1 && Number.isFinite(link.linkedAt) && Number.isFinite(link.observedUntil) &&
      link.linkedAt <= hold.startedAt && link.observedUntil >= hold.endedAt && link.closedAt == null &&
      (link.restartCount === undefined || link.restartCount === 0) && link.exit == null && link.relayAuthenticated === true;
  }), { restartCount: e.restartCount, links });
  const registrations = e.registrations ?? [];
  check("native-directory-registrations", topology && registrations.length === nodes.length && nodes.every(node => {
    const row = registrations.find(row => row.id === node.id);
    return row?.native && row.leaseLive && row.participant && row.ownerIncarnation === node.ownerIncarnation &&
      row.stateBackend === "sqlite" && (node.role === "old" ? [2, 3].includes(row.lockProtocol) : row.lockProtocol === 3) &&
      (baseline || (node.role === "old" ? row.tokenless || row.hostTokenHash && row.hostTokenHash === row.leaseTokenHash :
        row.hostTokenHash && row.hostTokenHash === row.leaseTokenHash));
  }), registrations);
  const allPairs = nodes.flatMap(source => nodes.filter(target => source.id !== target.id).map(target => ({ source, target })));
  const pairs = allPairs.filter(({ source, target }) => source.role === "hub" || target.role === "hub" || source.relayCapable && target.relayCapable);
  const presencePairs = allPairs.filter(({ source, target }) => source.role === "hub" || target.role === "hub" || source.relayCapable);
  const unsupported = e.unsupported ?? [], unsupportedPairs = allPairs.filter(pair => !pairs.includes(pair));
  check("pre-existing-unsupported-pairs-explicit", topology && unsupported.length === unsupportedPairs.length && unsupportedPairs.every(({ source, target }) => {
    const matches = unsupported.filter(row => row.source === source.id && row.target === target.id), row = matches[0];
    const lacking = [source, target].filter(node => !node.relayCapable);
    return matches.length === 1 && row.reason === "unsupported (pre-existing)" && row.lackingReleases?.length === lacking.length &&
      lacking.every(node => row.lackingReleases.some(release => release.sha === node.sha && Array.isArray(release.evidence) &&
        release.evidence.length === node.capabilityEvidence.evidence.length && node.capabilityEvidence.evidence.every(cite =>
          release.evidence.some(copy => copy?.file === cite.file && copy.line === cite.line && copy.text === cite.text && copy.sha256 === cite.sha256))));
  }), unsupported);
  const routes = e.routes ?? [];
  check("hub-presence-and-all-directed-routes", topology && ["before", "after"].every(phase => presencePairs.every(({ source, target }) => {
    const route = routes.find(row => row.phase === phase && row.source === source.id && row.target === target.id);
    return route?.remoteHost === target.name && (source.ownerIncarnation === undefined || route.ownerIncarnation === target.ownerIncarnation) &&
      (source.role === "hub" || target.role === "hub" || route.via === "hub");
  })), { expectedPresencePairs: presencePairs.length, observed: routes });
  const planValid = topology && plans.length === pairs.length * 2 && new Set(plans.map(plan => plan.marker)).size === plans.length &&
    pairs.every(({ source, target }) => ["before", "after"].every(round => plans.filter(plan => plan.source === source.id &&
      plan.target === target.id && plan.round === round).length === 1));
  check("non-vacuous-directed-all-pairs", planValid, { expected: pairs.length * 2, planned: plans.length });
  const ackValid = planValid && sends.length === plans.length && new Set(sends.map(send => send.commandId)).size === plans.length &&
    plans.every(plan => {
      const matching = sends.filter(send => send.marker === plan.marker), send = matching[0];
      if (matching.length !== 1 || send.source !== plan.source || send.target !== plan.target || !send.commandId ||
          send.ack?.commandId !== send.commandId || send.ack?.from !== plan.target || send.ack?.to !== plan.source || send.ack?.verification !== "bridge") return false;
      const tally = tallies.find(tally => tally.id === plan.source);
      const acks = tally?.ackEvents?.filter(event => event.data?.commandId === send.commandId) ?? [];
      return acks.length === 1 && acks[0].verification === "bridge" && acks[0].from?.id === plan.target &&
        acks[0].to === plan.source && acks[0].data?.targetId === plan.target && acks[0].data?.accepted === true &&
        acks[0].data?.messageId === `ack:${plan.marker}`;
    });
  check("correlated-authenticated-acks", ackValid, { sent: sends.length, expected: plans.length, sends });
  check("authenticated-handler-exactly-once-through-hold", planValid && tallies.length === nodes.length && nodes.every(node => {
    const tally = tallies.find(row => row.id === node.id), expected = plans.filter(plan => plan.target === node.id);
    return tally?.invalid === 0 && tally?.duplicateCalls === 0 && tally.entries?.length === expected.length && expected.every(plan => {
      const matches = tally.entries.filter(entry => entry.marker === plan.marker), entry = matches[0];
      return matches.length === 1 && entry.calls === 1 && entry.verification === "bridge" && entry.from === plan.source &&
        entry.targetId === plan.target && entry.commandId === sends.find(send => send.marker === plan.marker)?.commandId;
    });
  }), tallies.map(({ id, entries, invalid, duplicateCalls }) => ({ id, entries, invalid, duplicateCalls })));
  const h = e.history ?? {}, seeded = h.seeded ?? {}, preserved = h.preserved ?? {}, after = h.after ?? {};
  const fresh = nodes.find(node => node.id === h.fresh?.id && node.role === "old" && node.name === h.old?.name);
  const freshValid = fresh && fresh.id !== seeded.id && fresh.sha === h.old?.sha &&
    registrations.some(row => row.id === fresh.id && row.tokenless && row.native) &&
    h.fresh.before?.id === fresh.id && ["host", "lease", "participant", "row", "owner"].every(key => h.fresh.before[key] === null) &&
    ["before", "after"].every(phase => routes.some(route => route.phase === phase && route.source === nodes.find(node => node.role === "new")?.id &&
      route.target === fresh.id && route.remoteHost === fresh.name && route.via === "hub"));
  check("independent-fresh-tokenless-root-reaches-new", baseline || Boolean(freshValid), baseline ? { skipped: "baseline-control" } : h.fresh);
  const historyValid = seeded.id && !nodes.some(node => node.id === seeded.id) && seeded.id === preserved.id && seeded.id === after.id &&
    seeded.id === h.old?.id && seeded.id === h.hub?.id && freshValid &&
    seeded.hostTokenHash && seeded.hostTokenHash === seeded.leaseTokenHash && seeded.hostTokenHash === preserved.hostTokenHash &&
    preserved.hostTokenHash === preserved.leaseTokenHash && preserved.remoteHost === "hub" && after.remoteHost === preserved.remoteHost &&
    preserved.ownerIncarnation === seeded.ownerIncarnation && after.ownerIncarnation === preserved.ownerIncarnation && preserved.rawOwner?.remoteHost === "hub" &&
    ["hostTokenHash", "leaseTokenHash", "hostHash", "leaseHash", "rowHash", "rawOwner"].every(key =>
      preserved[key] != null && JSON.stringify(preserved[key]) === JSON.stringify(after[key])) &&
    h.old.native && h.old.tokenless && h.old.leaseLive && h.hub.tokenless && h.hub.remoteHost === h.old.name &&
    h.warning?.name === "STALE_INCARNATION_HISTORY" && h.warning.source === "bridge" && h.warning.rootId === seeded.id &&
    h.warning.tokenHash === seeded.hostTokenHash && h.warning.occurrenceCount > 0 &&
    h.warning.occurrenceCount === h.warning.rawLines?.length && h.warning.rawLines.every(row =>
      row.link === "measured-new" && row.line.includes(seeded.id) && /skipped stale relayed root/i.test(row.line));
  check("stale-incarnation-history-warning-not-fatal", baseline || Boolean(historyValid), baseline ? { skipped: "baseline-control" } : h);
  const attempt = h.attempt ?? {}, destination = h.destination ?? {}, live = h.liveAtAttempt ?? {};
  const entries = destination.entries ?? [], commands = attempt.commandEvents ?? [], acks = attempt.ackEvents ?? [];
  const commandIds = [...new Set(commands.map(event => event.data?.commandId))];
  const publicAttempt = historyValid && attempt.publicRequestInvoked === true && attempt.source === nodes.find(node => node.role === "new")?.id &&
    attempt.intendedTarget === seeded.id && typeof attempt.marker === "string" && attempt.marker.length > 0 &&
    !plans.some(plan => plan.marker === attempt.marker) && attempt.timeoutMs === 20_000 && attempt.ackGraceMs === 15_000 &&
    Number.isFinite(attempt.beganAt) && Number.isFinite(attempt.endedAt) && Number.isFinite(attempt.elapsedMs) &&
    attempt.beganAt >= hold.startedAt && attempt.endedAt <= hold.endedAt && attempt.endedAt >= attempt.beganAt &&
    attempt.elapsedMs >= 0 && attempt.elapsedMs <= 35_000 && attempt.endedAt - attempt.beganAt <= 35_000 &&
    attempt.observed?.id === seeded.id && attempt.observed.host?.incarnationToken &&
    hash(attempt.observed.host.incarnationToken) === seeded.hostTokenHash &&
    JSON.stringify(attempt.actualOwner) === JSON.stringify(attempt.observed.owner) && attempt.actualOwner?.remoteHost === "hub" &&
    attempt.actualOwner.ownerIncarnation === seeded.ownerIncarnation && live.id === seeded.id && live.native && live.tokenless && live.leaseLive &&
    live.observedAt <= attempt.beganAt && live.leaseExpiresAt > attempt.beganAt &&
    destination.native?.id === seeded.id && destination.liveAfter?.id === seeded.id && destination.liveAfter.native && destination.liveAfter.tokenless && destination.liveAfter.leaseLive &&
    destination.liveAfter.observedAt >= attempt.endedAt && destination.invalid === 0 && destination.duplicateCalls === 0 && entries.length <= 1 &&
    destination.expected?.length === 1 && destination.expected[0] === attempt.marker &&
    commands.every(event => event.from?.id === attempt.source && event.to === seeded.id && event.data?.targetId === seeded.id &&
      event.data?.operation === "followUp" && event.data?.data?.marker === attempt.marker && event.data?.message === attempt.marker &&
      event.data?.destinationRemoteHost === attempt.actualOwner.remoteHost && event.data?.ownerIncarnation === attempt.actualOwner.ownerIncarnation);
  const authenticatedAck = event => event.verification === "bridge" && event.from?.id === seeded.id && event.to === attempt.source &&
    commandIds.includes(event.data?.commandId) && event.data?.targetId === seeded.id && event.data?.accepted === true &&
    event.data?.messageId === `ack:${attempt.marker}`;
  const delivered = !attempt.error && attempt.receipt?.acknowledged === true && attempt.receipt.routed === "mesh" &&
    attempt.receipt.messageId === `ack:${attempt.marker}` && commandIds.length === 1 && entries.length === 1 &&
    entries[0].marker === attempt.marker && entries[0].from === attempt.source && entries[0].targetId === seeded.id &&
    entries[0].commandId === commandIds[0] && entries[0].calls === 1 && entries[0].verification === "bridge" &&
    acks.length === 1 && authenticatedAck(acks[0]) &&
    (destination.ackEvents ?? []).filter(event => event.from?.id === seeded.id && event.to === attempt.source &&
      event.data?.commandId === commandIds[0] && event.data?.targetId === seeded.id && event.data?.accepted === true &&
      event.data?.messageId === `ack:${attempt.marker}`).length === 1;
  const refusal = !attempt.receipt && entries.length === 0 && !acks.some(event => event.data?.accepted === true) &&
    !(destination.ackEvents ?? []).some(event => commandIds.includes(event.data?.commandId) && event.data?.accepted === true) &&
    attempt.error?.source === "FabricControlPlane.request" &&
    // These are the public file-control route/lease/incarnation refusals, not any
    // named JS exception. In particular TypeError and generic ACK timeouts fail.
    ({ FabricBridgeStaleError: "FABRIC_BRIDGE_STALE", FabricPeerLeaseStaleError: "FABRIC_PEER_LEASE_STALE",
      FabricParticipantStaleError: "FABRIC_PARTICIPANT_STALE", FabricControlStaleIncarnationError: "FABRIC_CONTROL_STALE_INCARNATION",
      FabricControlIncarnationRequiredError: "FABRIC_CONTROL_INCARNATION_REQUIRED",
      FabricRelayDestinationUnavailableError: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" })[attempt.error.name] === attempt.error.code &&
    typeof attempt.error.code === "string" && typeof attempt.error.message === "string" &&
    (attempt.error.targetId === undefined ? attempt.error.message.includes(seeded.id) : attempt.error.targetId === seeded.id) &&
    !/RPC deadline|harness/i.test(attempt.error.message);
  check("historied-live-root-public-followup-outcome", baseline || Boolean(publicAttempt && (delivered || refusal)),
    baseline ? { skipped: "baseline-control" } : { outcome: delivered ? "delivered" : refusal ? "named-public-error" : "FAIL", attempt, live, destination });
  // Older pure-evaluator fixtures omit cleanup; explicit native cleanup evidence can
  // never be ignored, including a leader exit that preceded intentional shutdown.
  const cleanup = e.cleanup;
  check("owned-process-cleanup", cleanup === undefined || cleanup?.ok === true &&
    (cleanup.failures === undefined || Array.isArray(cleanup.failures) && cleanup.failures.length === 0) &&
    (cleanup.owned === undefined || Array.isArray(cleanup.owned) && cleanup.owned.every(child =>
      child.unexpectedExit !== true && child.spawnError == null && child.groupEmpty !== false)), cleanup);
  if (e.failure) check("execution", false, e.failure);
  return { ok: checks.every(check => check.status === "PASS"), releaseGate: !baseline, checks };
}

async function run(options, releases, evidence, scratch, signal) {
  const children = [], workers = [], measured = [];
  let rpcId = 0;
  const active = () => { if (signal.aborted) throw signal.reason; };
  const env = { ...process.env, XDG_RUNTIME_DIR: scratch, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" };
  // A private, short runtime avoids AF_UNIX path truncation even when TMPDIR is long.
  if (Buffer.byteLength(path.join(env.XDG_RUNTIME_DIR, "mesh-bridge/relay/relay-old-0.sock")) >= 104)
    throw new Error("private os.tmpdir path is too long for production AF_UNIX relay; use a shorter private TMPDIR");
  const killGroup = (owned, sig) => {
    if (!Number.isInteger(owned.child.pid) || owned.child.pid <= 0) return; // failed spawn owns no process group
    try { process.kill(-owned.child.pid, sig); } catch (error) { if (error.code !== "ESRCH") throw error; }
  };
  const groupMembers = owned => {
    if (!Number.isInteger(owned.child.pid) || owned.child.pid <= 0) return [];
    return fs.readdirSync("/proc").filter(name => /^\d+$/.test(name)).flatMap(name => {
      try {
        const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8"), fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        return Number(fields[2]) === owned.child.pid && !["Z", "X"].includes(fields[0]) ? [{ pid: Number(name), state: fields[0] }] : [];
      } catch (error) { if (["ENOENT", "ESRCH"].includes(error.code)) return []; throw error; }
    });
  };
  const own = (name, args, release) => {
    active();
    const child = spawn(process.execPath, args, { detached: true, stdio: ["pipe", "pipe", "pipe"],
      env: { ...env, PI_FABRIC_RELEASE_SHA: release.sha, PI_FABRIC_BUILD_SHA: release.sha } });
    const owned = { child, name, log: "", closing: false, pending: new Map(), linkedAt: null, closedAt: null };
    children.push(owned);
    const stderr = path.join(scratch, `${name}.stderr.log`), stdout = path.join(scratch, `${name}.stdout.log`);
    const output = (file, data) => {
      try { fs.appendFileSync(file, data, { mode: 0o600 }); }
      catch (error) { owned.ioError = error.message; killGroup(owned, "SIGKILL"); }
    };
    child.stderr.on("data", data => { output(stderr, data); owned.log = (owned.log + data).slice(-2_000_000); });
    child.stdout.on("data", data => output(stdout, data));
    const lifetime = setTimeout(() => killGroup(owned, "SIGKILL"), options.holdMs + 180_000);
    child.once("exit", () => { owned.unexpectedExit = !owned.closing; });
    owned.closed = new Promise(resolve => {
      child.once("close", (code, childSignal) => {
        clearTimeout(lifetime); killGroup(owned, "SIGKILL");
        owned.closedAt = Date.now(); owned.exit = { code, signal: childSignal }; owned.unexpectedExit ??= !owned.closing;
        for (const pending of owned.pending.values()) pending.reject(new Error(`${name} closed: ${code}/${childSignal}; ${owned.log.slice(-4000)}`));
        resolve();
      });
      child.once("error", error => { owned.spawnError = error.message; });
    });
    return owned;
  };
  const terminate = async (owned, hard = false) => {
    owned.closing = true;
    if (hard) killGroup(owned, "SIGKILL");
    else if (owned.closedAt === null) owned.child.kill("SIGTERM");
    const force = setTimeout(() => killGroup(owned, "SIGKILL"), 5_000);
    try { await owned.closed; } finally { clearTimeout(force); killGroup(owned, "SIGKILL"); }
    // Leader close is not proof that detached stdio descendants are gone. Inspect
    // the owned process group; zombies cannot write and are left for their reaper.
    const deadline = performance.now() + 2_000;
    let remaining;
    while ((remaining = groupMembers(owned)).length && performance.now() < deadline)
      await new Promise(resolve => setTimeout(resolve, 25));
    owned.groupEmpty = remaining.length === 0;
    if (!owned.groupEmpty) throw new Error(`${owned.name} surviving process group: ${JSON.stringify(remaining)}`);
  };
  const rpc = (worker, op, args = {}) => new Promise((resolve, reject) => {
    try { active(); } catch (error) { reject(error); return; }
    const id = ++rpcId;
    const finish = (error, value) => { clearTimeout(timer); signal.removeEventListener("abort", abort); worker.pending.delete(id); error ? reject(error) : resolve(value); };
    const abort = () => finish(signal.reason);
    // Special request gets transport headroom beyond the unchanged 20s + 15s public budget.
    const timer = setTimeout(() => finish(new Error(`${worker.name} ${op} RPC deadline`)), op === "historyFollowUp" ? 45_000 : 35_000);
    signal.addEventListener("abort", abort, { once: true });
    worker.pending.set(id, { resolve: value => finish(null, value), reject: error => finish(error) });
    worker.child.stdin.write(`${JSON.stringify({ id, op, ...args })}\n`, error => { if (error) finish(error); });
  });
  const startWorker = async (name, release, mesh, rootId, role) => {
    const worker = own(`worker-${name}`, [WORKER], release);
    workers.push(worker);
    let buffer = "";
    worker.child.stdin.on("error", () => undefined);
    worker.child.stdout.on("data", chunk => {
      buffer += chunk;
      for (;;) {
        const newline = buffer.indexOf("\n"); if (newline < 0) break;
        const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
        try {
          const response = JSON.parse(line), pending = worker.pending.get(response.id);
          if (pending) response.ok ? pending.resolve(response.result) : pending.reject(new Error(response.error));
        } catch (error) { for (const pending of worker.pending.values()) pending.reject(error); }
      }
    });
    const result = await rpc(worker, "start", { release: release.dir, mesh, rootId, name });
    Object.assign(worker, { native: result.native, mesh, release, role, lockProtocol: result.lockProtocol });
    if (result.stateBackend !== "sqlite") throw new Error(`${name} did not open production SQLite`);
    if (role !== "old" && result.lockProtocol !== 3) throw new Error(`${name} NEW runtime does not support lock protocol 3`);
    fs.writeFileSync(path.join(scratch, `${name}.registration.json`), JSON.stringify(result, null, 2), { mode: 0o600 });
    return { worker, result };
  };
  const bridge = async (name, target, phase) => {
    const owned = own(`${phase}-${name}`, [path.join(releases.candidate.dir, "bin/mesh-bridge"), "run", "--mesh", hubMesh,
      "--name", "hub", "--remote", name, "--cursor", path.join(scratch, `${phase}-${name}.cursor.json`),
      "--lock-protocol", "3", "--call-timeout-ms", "10000", "--", process.execPath,
      path.join(target.release.dir, "bin/mesh-bridge"), "agent", "--mesh", target.mesh, "--peer", "hub", "--lock-protocol", String(target.lockProtocol)], releases.candidate);
    if (phase === "measured") measured.push(owned);
    await new Promise((resolve, reject) => {
      const text = `linked hub <-> ${name}`;
      const finish = error => { clearTimeout(timer); signal.removeEventListener("abort", abort); owned.child.stderr.off("data", check); error ? reject(error) : resolve(); };
      const check = () => { if (owned.log.includes(text)) { owned.linkedAt = Date.now(); finish(); } };
      const abort = () => finish(signal.reason);
      const timer = setTimeout(() => finish(new Error(`${owned.name} link deadline: ${owned.log.slice(-4000)}`)), 30_000);
      signal.addEventListener("abort", abort, { once: true });
      owned.child.stderr.on("data", check);
      void owned.closed.then(() => finish(new Error(`${owned.name} closed: ${owned.log.slice(-4000)}`)));
      check();
    });
    return owned;
  };
  const summarize = (row, name) => ({ id: row.id, name, native: row.host?.remoteHost === undefined && row.lease != null,
    participant: Boolean(row.participant && row.row), leaseLive: row.lease?.expiresAt > Date.now(),
    ownerIncarnation: row.row?.ownerIncarnation, stateBackend: row.lease?.writer?.stateBackend, lockProtocol: row.lease?.writer?.lockProtocol,
    tokenless: Boolean(row.host && row.lease && row.host.incarnationToken === undefined && row.lease.incarnationToken === undefined),
    hostTokenHash: row.host?.incarnationToken ? hash(row.host.incarnationToken) : null,
    leaseTokenHash: row.lease?.incarnationToken ? hash(row.lease.incarnationToken) : null,
    remoteHost: row.host?.remoteHost, bridgeVia: row.host?.bridgeVia,
    observedAt: Date.now(), leaseExpiresAt: row.lease?.expiresAt,
    hostHash: row.host ? hash(JSON.stringify(row.host)) : null, leaseHash: row.lease ? hash(JSON.stringify(row.lease)) : null,
    rowHash: row.row ? hash(JSON.stringify(row.row)) : null,
    rawOwner: row.row ? { ownerHostId: row.row.ownerHostId, ownerIdentityId: row.row.ownerIdentityId,
      ownerIncarnation: row.row.ownerIncarnation, remoteHost: row.row.remoteHost } : null });
  const save = (name, data) => fs.writeFileSync(path.join(scratch, `${name}.json`), JSON.stringify(data, null, 2), { mode: 0o600 });
  const hubMesh = path.join(scratch, "hub");
  try {
    const hub = (await startWorker("hub", releases.candidate, hubMesh, `session:${randomUUID()}`, "hub"));
    const newer = (await startWorker("new", releases.candidate, path.join(scratch, "new"), `session:${randomUUID()}`, "new"));
    evidence.registrations.push(summarize(hub.result.readback, "hub"), summarize(newer.result.readback, "new"));
    const endpoints = [hub.worker, newer.worker], oldWorkers = [];
    for (let index = 0; index < releases.old.length; index++) {
      const name = `old-${index}`, release = releases.old[index];
      const old = await startWorker(name, release, path.join(scratch, name), `session:${randomUUID()}`, "old");
      evidence.registrations.push(summarize(old.result.readback, name));
      endpoints.push(old.worker); oldWorkers.push(old.worker);
    }
    evidence.nodes = endpoints.map(worker => ({ ...worker.native, role: worker.role, sha: worker.release.sha, lockProtocol: worker.lockProtocol,
      relayCapable: worker.release.relayCapability.supported, capabilityEvidence: worker.release.relayCapability }));
    evidence.unsupported = endpoints.flatMap(source => endpoints.filter(target => source !== target && source.role !== "hub" && target.role !== "hub" &&
      (!source.release.relayCapability.supported || !target.release.relayCapability.supported)).map(target => ({ source: source.native.id, target: target.native.id,
        reason: "unsupported (pre-existing)", lackingReleases: [source, target].filter(worker => !worker.release.relayCapability.supported)
          .map(worker => ({ sha: worker.release.sha, evidence: worker.release.relayCapability.evidence })) })));
    const legacy = oldWorkers.find(worker => evidence.registrations.find(row => row.id === worker.native.id)?.tokenless);
    if (!options.baselineControl && !legacy) throw new Error("no OLD release actually wrote a native tokenless host/lease; history case cannot be fabricated");
    let sentinel, regressionId;
    if (!options.baselineControl) {
      // A second genuine root on the SAME historical spoke. Never replace the fresh
      // primary topology node or add a fabricated release/unsupported-pair waiver.
      const started = await startWorker("history-sentinel", legacy.release, legacy.mesh, `session:${randomUUID()}`, "old");
      sentinel = started.worker; regressionId = sentinel.native.id;
      evidence.history.old = { ...summarize(started.result.readback, legacy.native.name), sha: legacy.release.sha };
      const [freshBefore] = await rpc(newer.worker, "presence", { ids: [legacy.native.id] });
      evidence.history.fresh = { id: legacy.native.id, before: freshBefore };
      save("fresh-primary-before-new-link", freshBefore);
      if ([freshBefore.host, freshBefore.lease, freshBefore.participant, freshBefore.row, freshBefore.owner].some(value => value !== null))
        throw new Error("fresh historical primary already has history on NEW");
      const seed = await startWorker("history-seed", releases.candidate, hubMesh, regressionId, "seed");
      if (!seed.result.readback.host?.incarnationToken) throw new Error("NEW release did not generate a native incarnationToken");
      const seedLink = await bridge("new", newer.worker, "seed");
      const [mirrored] = await rpc(newer.worker, "waitPresence", { ids: [regressionId], conditions: [{ remote: "hub", token: seed.result.readback.host.incarnationToken }] });
      evidence.history.seeded = summarize(mirrored, "new"); save("history-seeded", mirrored);
      await terminate(seedLink, true); // deliberate prehistory crash, never a measured-link restart
      seed.worker.closing = true;
      await rpc(seed.worker, "stop"); await terminate(seed.worker);
      await rpc(hub.worker, "waitPresence", { ids: [regressionId], conditions: [{ absent: true }] });
      const [preserved] = await rpc(newer.worker, "presence", { ids: [regressionId] });
      evidence.history.preserved = summarize(preserved, "new"); save("history-preserved", preserved);
      evidence.seedPhase = { hardStoppedBridgePid: seedLink.child.pid, exit: seedLink.exit, registrarClosed: true, measuredRestarts: 0 };
    }
    for (const old of oldWorkers) await bridge(old.native.name, old, "measured");
    if (!options.baselineControl) {
      const [oldAtHub] = await rpc(hub.worker, "waitPresence", { ids: [regressionId], conditions: [{ remote: legacy.native.name, tokenless: true }] });
      evidence.history.hub = summarize(oldAtHub, "hub"); save("old-tokenless-at-hub", oldAtHub);
      const [immediatelyBefore, freshBefore] = await rpc(newer.worker, "presence", { ids: [regressionId, legacy.native.id] });
      evidence.history.fresh.before = freshBefore; save("fresh-primary-before-new-link", freshBefore);
      if ([freshBefore.host, freshBefore.lease, freshBefore.participant, freshBefore.row, freshBefore.owner].some(value => value !== null))
        throw new Error("fresh historical primary gained history before measured NEW link");
      evidence.history.preserved = summarize(immediatelyBefore, "new"); save("history-before-regression", immediatelyBefore);
      if (!evidence.history.preserved.hostTokenHash || evidence.history.preserved.hostTokenHash !== evidence.history.seeded.hostTokenHash ||
          evidence.history.preserved.leaseTokenHash !== evidence.history.seeded.leaseTokenHash) throw new Error("persisted token mirror/lease history was erased before regression");
    }
    // This initial measured link, not a reconnect loop, encounters OLD tokenless presence
    // relayed through the real hub against the frozen NEW receiver's persisted authority.
    await bridge("new", newer.worker, "measured");
    const observeRoutes = async phase => {
      await Promise.all(endpoints.map(async source => {
        const targets = endpoints.filter(target => target !== source && (source.role === "hub" || source.release.relayCapability.supported || target.role === "hub"));
        const rows = await rpc(source, "waitPresence", { ids: targets.map(target => target.native.id),
          conditions: targets.map(target => ({ remote: target.native.name })) });
        save(`${phase}-${source.native.name}-presence`, rows);
        rows.forEach(row => evidence.routes.push({ phase, source: source.native.id, target: row.id,
          remoteHost: row.owner?.remoteHost, ownerIncarnation: row.owner?.ownerIncarnation, via: row.host?.bridgeVia }));
      }));
    };
    await observeRoutes("before");
    for (const link of measured) {
      const name = link.name.slice("measured-".length), dir = path.join(env.XDG_RUNTIME_DIR, "mesh-bridge/relay");
      const registration = JSON.parse(fs.readFileSync(path.join(dir, `relay-${name}.json`), "utf8"));
      const socket = fs.lstatSync(path.join(dir, `relay-${name}.sock`));
      link.relayAuthenticated = registration.pid === link.child.pid && registration.remoteName === name && registration.localName === "hub" &&
        registration.meshRoot === fs.realpathSync(hubMesh) && registration.bridgeScriptPath === fs.realpathSync(path.join(releases.candidate.dir, "bin/mesh-bridge")) &&
        socket.isSocket() && (socket.mode & 0o777) === 0o600 && socket.uid === process.getuid();
      save(`${name}-relay-registration`, registration);
      if (!link.relayAuthenticated) throw new Error(`production relay registration invalid: ${name}`);
    }
    const collectWarning = () => {
      const rawLines = measured.flatMap(link => fs.readFileSync(path.join(scratch, `${link.name}.stderr.log`), "utf8")
        .split("\n").map(line => ({ link: link.name, line }))).filter(({ link, line }) =>
          link === "measured-new" && line.includes(regressionId) && /skipped stale relayed root/i.test(line));
      evidence.history.warning = { name: "STALE_INCARNATION_HISTORY", source: "bridge", rootId: regressionId,
        tokenHash: evidence.history.seeded.hostTokenHash, occurrenceCount: rawLines.length, rawLines };
      save("stale-incarnation-warning", evidence.history.warning);
    };
    if (!options.baselineControl) collectWarning();
    evidence.plans = endpoints.flatMap(source => endpoints.filter(target => target !== source && (source.role === "hub" || target.role === "hub" ||
      source.release.relayCapability.supported && target.release.relayCapability.supported)).flatMap(target =>
      ["before", "after"].map(round => ({ source: source.native.id, target: target.native.id, round,
        marker: `${randomUUID()}:${source.native.name}->${target.native.name}:${round}` }))));
    await Promise.all(endpoints.map(worker => rpc(worker, "expect", { messages: evidence.plans.filter(plan => plan.target === worker.native.id).map(plan => ({ marker: plan.marker, from: plan.source })) })));
    const holdStarted = performance.now();
    evidence.hold.startedAt = Date.now();
    const sendRound = round => Promise.all(evidence.plans.filter(plan => plan.round === round).map(async plan => {
      const source = endpoints.find(worker => worker.native.id === plan.source), target = endpoints.find(worker => worker.native.id === plan.target);
      const result = await rpc(source, "send", { target: target.native, marker: plan.marker });
      evidence.sends.push({ ...result, source: plan.source, target: plan.target });
    }));
    let special;
    if (!options.baselineControl) {
      const marker = `${randomUUID()}:new->history-sentinel`;
      await rpc(sentinel, "expect", { messages: [{ marker, from: newer.worker.native.id }] });
      const [live] = await rpc(sentinel, "presence", { ids: [regressionId] });
      evidence.history.liveAtAttempt = summarize(live, legacy.native.name); save("sentinel-live-at-attempt", live);
      special = rpc(newer.worker, "historyFollowUp", { targetId: regressionId, marker }).then(async attempt => {
        evidence.history.attempt = attempt; save("history-public-followup", attempt);
        const [liveAfter] = await rpc(sentinel, "presence", { ids: [regressionId] });
        evidence.history.destination = { ...await rpc(sentinel, "tally"), liveAfter: summarize(liveAfter, legacy.native.name) };
        save("sentinel-after-attempt", { liveAfter, tally: evidence.history.destination });
      });
      // Observe rejection immediately; retain it for the ordered join below.
      special.catch(() => undefined);
    }
    await sendRound("before");
    await new Promise((resolve, reject) => {
      const abort = () => finish(signal.reason);
      const finish = error => { clearTimeout(timer); signal.removeEventListener("abort", abort); error ? reject(error) : resolve(); };
      const timer = setTimeout(() => finish(), Math.max(0, options.holdMs - (performance.now() - holdStarted)));
      signal.addEventListener("abort", abort, { once: true });
      for (const child of [...measured, ...endpoints, ...(sentinel ? [sentinel] : [])])
        void child.closed.then(() => finish(new Error(`early owned process close: ${child.name}`)));
    });
    await special;
    await observeRoutes("after");
    await sendRound("after");
    const snapshots = await Promise.all(endpoints.map(async worker => ({ id: worker.native.id, ...await rpc(worker, "tally") })));
    evidence.tallies = snapshots;
    if (!options.baselineControl) {
      const [after] = await rpc(newer.worker, "presence", { ids: [regressionId] });
      evidence.history.after = summarize(after, "new"); save("history-after-hold", after);
      const [liveAfter] = await rpc(sentinel, "presence", { ids: [regressionId] });
      evidence.history.destination = { ...await rpc(sentinel, "tally"), liveAfter: summarize(liveAfter, legacy.native.name) };
      save("sentinel-final-tally", evidence.history.destination);
      collectWarning();
    }
    evidence.hold.endedAt = Date.now();
    evidence.hold.elapsedMs = performance.now() - holdStarted;
    save("final-tallies", snapshots);
  } finally {
    const observedUntil = Date.now();
    evidence.links = measured.map(link => ({ name: link.name.slice("measured-".length), pid: link.child.pid, starts: 1, restartCount: 0,
      linkedAt: link.linkedAt, observedUntil: link.closedAt ?? observedUntil, closedAt: link.closedAt,
      relayAuthenticated: link.relayAuthenticated === true, exit: link.exit ?? null }));
    // Close native owners, then bounded bridge leaders and their process groups. No restart path.
    // Mark intentional shutdown before the stop RPC can let a registrar exit cleanly.
    for (const child of children) child.closing = true;
    const stopped = await Promise.allSettled(workers.filter(worker => worker.closedAt === null).map(worker => rpc(worker, "stop")));
    const terminated = await Promise.allSettled(children.map(child => terminate(child)));
    const failures = [...stopped, ...terminated].filter(result => result.status === "rejected").map(result => String(result.reason));
    failures.push(...children.filter(child => child.ioError).map(child => `${child.name} log write: ${child.ioError}`));
    failures.push(...children.filter(child => child.unexpectedExit || child.spawnError).map(child =>
      `${child.name} unexpected exit/spawn failure: ${child.spawnError ?? JSON.stringify(child.exit)}`));
    evidence.cleanup = { ok: failures.length === 0, failures, owned: children.map(child => ({ name: child.name, pid: child.child.pid,
      exit: child.exit, unexpectedExit: child.unexpectedExit, spawnError: child.spawnError, groupEmpty: child.groupEmpty })) };
    if (failures.length && !signal.aborted) evidence.failure ??= { message: "owned descendant cleanup failed", failures };
  }
}

// Resolve existing symlink ancestors (including a dangling final symlink) before
// allowing an external receipt. Invalid CLI inputs must not bypass this guard.
function canonicalDestination(input, depth = 0) {
  if (depth > 128) throw new Error("receipt path has too many symlink/parent components");
  const absolute = path.resolve(input);
  try { return fs.realpathSync(absolute); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  try {
    if (fs.lstatSync(absolute).isSymbolicLink())
      return canonicalDestination(path.resolve(path.dirname(absolute), fs.readlinkSync(absolute)), depth + 1);
  } catch (error) { if (error.code !== "ENOENT") throw error; }
  const parent = path.dirname(absolute);
  if (parent === absolute) throw new Error(`cannot resolve receipt path: ${input}`);
  return path.join(canonicalDestination(parent, depth + 1), path.basename(absolute));
}
function protectReleaseInputs(argv, destination) {
  const receipt = canonicalDestination(destination);
  for (let index = 0; index < argv.length; index++) {
    if (!["--new", "--old"].includes(argv[index]) || !argv[index + 1] || argv[index + 1].startsWith("--")) continue;
    const release = canonicalDestination(argv[index + 1]), relative = path.relative(release, receipt);
    if (relative === "" || relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
      throw new Error(`receipt destination must not modify an input release: ${destination}`);
  }
}

function parseAuthorityArgs(argv) {
  const options = { authoritySnapshot: true, passes: 3, timeoutMs: 30_000 };
  const names = { "--hub-writer": "hubWriter", "--hub-writer-sha": "hubWriterSha", "--work-writer": "workWriter",
    "--work-writer-sha": "workWriterSha", "--candidate-work": "candidateWork", "--candidate-work-sha": "candidateWorkSha",
    "--candidate-work-writer": "candidateWorkWriter", "--candidate-work-writer-sha": "candidateWorkWriterSha",
    "--capture-manifest": "captureManifest", "--capture-manifest-sha256": "captureManifestSha256",
    "--hub": "hub", "--work": "work", "--candidate": "candidate", "--hub-sha": "hubSha",
    "--work-sha": "workSha", "--candidate-sha": "candidateSha", "--hub-snapshot": "hubSnapshot",
    "--work-snapshot": "workSnapshot", "--receipt": "receipt", "--evidence-dir": "evidenceDir", "--passes": "passes", "--timeout-ms": "timeoutMs" };
  const seen = new Set();
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (seen.has(flag)) throw new Error(`duplicate ${flag}`);
    seen.add(flag);
    if (flag === "--authority-snapshot") continue;
    if (["--live-writers", "--candidate-both-sides", "--synthetic"].includes(flag)) {
      options[{ "--live-writers": "liveWriters", "--candidate-both-sides": "candidateBothSides", "--synthetic": "synthetic" }[flag]] = true;
      continue;
    }
    const key = names[flag], value = argv[++i];
    if (!key || !value || value.startsWith("--")) throw new Error("invalid authority CLI argument");
    options[key] = ["passes", "timeoutMs"].includes(key) ? Number(value) : value;
  }
  if (!["hub", "work", "hubSha", "workSha", "hubSnapshot", "workSnapshot"].every(key => options[key]))
    throw new Error("authority mode requires --hub --hub-sha --work --work-sha --hub-snapshot --work-snapshot");
  return options;
}

function authorityRelease(dir, sha) {
  if (!SHA.test(sha ?? "")) throw new Error("authority release requires full explicitly pinned SHA");
  dir = fs.realpathSync(dir);
  const pkg = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
  if (pkg.name !== "pi-fabric") throw new Error("authority release is not pi-fabric");
  const bridge = path.join(dir, "dist/mesh-bridge.js"), bin = path.join(dir, "bin/mesh-bridge");
  if (!fs.existsSync(bin) || !fs.statSync(path.join(dir, "dist/chunks")).isDirectory()) throw new Error("missing compiled authority release");
  let identitySource;
  for (const name of ["dist/build-identity.json", "build-identity.json", ".build-identity.json"]) {
    const file = path.join(dir, name);
    if (!fs.existsSync(file)) continue;
    const identity = JSON.parse(fs.readFileSync(file, "utf8"));
    if ((identity.sha ?? identity.commit ?? identity.gitSha ?? identity.releaseSha) !== sha) throw new Error("authority release identity mismatch");
    identitySource ??= name;
  }
  if (!identitySource) {
    try {
      const top = fs.realpathSync(execFileSync("git", ["-C", dir, "rev-parse", "--show-toplevel"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 5_000 }).trim());
      if (top === dir) {
        const head = execFileSync("git", ["-C", dir, "rev-parse", "HEAD"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 5_000 }).trim();
        if (head !== sha) throw new Error("authority release identity mismatch");
        identitySource = "git:HEAD";
      }
    } catch (error) { if (error.message === "authority release identity mismatch") throw error; }
  }
  if (!identitySource && SHA.test(path.basename(dir))) {
    if (path.basename(dir) !== sha) throw new Error("authority release identity mismatch");
    identitySource = "canonical-full-sha-artifact-directory";
  }
  if (!identitySource) throw new Error("authority release has no verifiable build identity");
  return { dir, sha, identitySource, bridgeSha256: hash(fs.readFileSync(bridge)),
    binSha256: hash(fs.readFileSync(bin)), version: pkg.version };
}

const PRIMARY_HUB_SHA = "9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5";
const PRIMARY_WORK_SHA = "0fc8ca5d706dfb85519719ebfcc12fadfd63696c";
const C42_SHA = "c42f7f34125abe2ebbe660758253c9ec4d7765dd";
const PRODUCER_SCOPE = "producer-boundary compatibility; principal session bootstrap and auth policy are not exercised";
const jsonBytes = input => typeof input === "string" ? fs.readFileSync(input) : Buffer.from(JSON.stringify(input));
const PARTIAL_LABEL = "partial diagnostic: participant/lease coverage missing; mixed-era (hub 16:30 live, work 12:40 snapshot)";

/** Input-derived, bounded context; never copy arbitrary manifest metadata into evidence. */
export function classifyAuthorityCoverage(snapshots) {
  const missingAuthority = [];
  for (const site of ["hub", "work"]) {
    const stateHosts = Array.isArray(snapshots[site]?.stateEntries)
      ? snapshots[site].stateEntries.filter(row => typeof row?.key === "string" && row.key.startsWith("topology/hosts/"))
      : [];
    const participantFiles = snapshots[site]?.participantFiles;
    const leaseFiles = snapshots[site]?.leaseFiles;
    const hasMatchingLiveLease = stateHosts.some(row => Array.isArray(leaseFiles) && leaseFiles.some(lease =>
      lease && row.value && lease.id === row.value.id && lease.rootId === row.value.rootId && lease.identityId === row.value.identity?.id &&
      Math.max(lease.expiresAt, Number.isFinite(lease.reloadUntil) ? lease.reloadUntil : Number.NEGATIVE_INFINITY) > snapshots[site].collectedAt));
    if (stateHosts.length > 0 && !hasMatchingLiveLease) {
      if (!Array.isArray(participantFiles) || participantFiles.length === 0) missingAuthority.push(`${site}.participantFiles`);
      missingAuthority.push(`${site}.leaseFiles`);
    }
  }
  return missingAuthority.length ? { partialDiagnostic: true, label: PARTIAL_LABEL, incidentFaithful: false,
    acceptanceEligible: false, missingAuthority, material: "mixed-era" } : undefined;
}

/** A diagnostic can never become acceptance merely because its arms appear RED/PASS. */
export function evaluateAuthorityAcceptance(result, synthetic = false) {
  return !synthetic && !result.diagnosticContext?.partialDiagnostic && result.arms.length === 2 &&
    result.arms[0].outcome === "RED" && result.arms[1].outcome === "PASS";
}

function readAuthorityInputs(options, result) {
  const snapshots = {};
  for (const site of ["hub", "work"]) {
    snapshots[site] = JSON.parse(jsonBytes(options[`${site}Snapshot`]));
    result.diagnosticContext = classifyAuthorityCoverage(snapshots);
  }
  for (const site of ["hub", "work"]) validateAuthoritySnapshot(snapshots[site], site);
  const referenceCollectedAt = Math.max(snapshots.hub.collectedAt, snapshots.work.collectedAt);
  result.captureAge = Object.fromEntries(["hub", "work"].map(site => {
    const snapshot = snapshots[site], expiries = snapshot.stateEntries.filter(row => row.key.startsWith("topology/hosts/")).map(row => row.value.expiresAt);
    return [site, { name: snapshot.name, collectedAt: snapshot.collectedAt, referenceCollectedAt,
      collectionAgeMs: referenceCollectedAt - snapshot.collectedAt, stateHostCount: expiries.length,
      hostsExpiredAtReference: expiries.filter(at => at <= referenceCollectedAt).length,
      youngestHostExpiryAgeMs: expiries.length ? referenceCollectedAt - Math.max(...expiries) : null,
      oldestHostExpiryAgeMs: expiries.length ? referenceCollectedAt - Math.min(...expiries) : null }];
  }));
  return snapshots;
}

/** Pin the original sanitized BYTES and external collector receipts, not reserialized objects.
 * The pinned operator manifest is the trust input, not an assertion of collector authenticity. */
export function validateCaptureProvenance(options, snapshots) {
  const bytes = jsonBytes(options.captureManifest);
  if (!/^[a-f0-9]{64}$/.test(options.captureManifestSha256 ?? "") || hash(bytes) !== options.captureManifestSha256)
    throw new Error("capture manifest byte hash missing or mismatched");
  const manifest = JSON.parse(bytes);
  if (manifest.schemaVersion !== 1 || !manifest.pairId || manifest.kind !== (options.synthetic ? "synthetic-fixture" : "operator-capture-pair"))
    throw new Error("capture manifest kind/pair identity mismatch");
  const diagnosticContext = classifyAuthorityCoverage(snapshots);
  if (manifest.diagnosticContext !== undefined) {
    const requested = manifest.diagnosticContext;
    if (!diagnosticContext || !requested || typeof requested !== "object" || Array.isArray(requested) ||
      Object.keys(requested).length !== Object.keys(diagnosticContext).length ||
      Object.entries(diagnosticContext).some(([key, value]) => JSON.stringify(requested[key]) !== JSON.stringify(value)))
      throw new Error("invalid bounded diagnostic context");
  }
  const sites = {};
  for (const site of ["hub", "work"]) {
    if (!options.synthetic && (typeof options[`${site}Snapshot`] !== "string" || typeof options.captureManifest !== "string"))
      throw new Error("REAL capture provenance requires original sanitized byte files and pinned operator manifest");
    const snapshot = snapshots[site], sha256 = hash(jsonBytes(options[`${site}Snapshot`])), row = manifest.sites?.[site];
    const binding = { site, name: snapshot.name, backend: snapshot.backend, collectedAt: snapshot.collectedAt, captureSha256: sha256 };
    if (!row || Object.entries(binding).some(([key, value]) => row[key] !== value))
      throw new Error(`capture manifest ${site} byte/metadata binding mismatch`);
    let receipt;
    if (options.synthetic) receipt = row.fixtureReceipt;
    else {
      if (typeof row.collectorReceipt?.path !== "string" || !/^[a-f0-9]{64}$/.test(row.collectorReceipt.sha256 ?? ""))
        throw new Error("missing immutable collector receipt binding");
      const receiptPath = path.resolve(path.dirname(options.captureManifest), row.collectorReceipt.path);
      const receiptBytes = fs.readFileSync(receiptPath);
      if (hash(receiptBytes) !== row.collectorReceipt.sha256) throw new Error("collector receipt byte hash mismatch");
      receipt = JSON.parse(receiptBytes);
    }
    if (!receipt || receipt.pairId !== manifest.pairId || Object.entries(binding).some(([key, value]) => receipt[key] !== value))
      throw new Error("collector receipt does not bind capture bytes/name/site/backend/time/pair");
    sites[site] = { ...binding, collectorReceipt: options.synthetic ? "SYNTHETIC fixture receipt" : row.collectorReceipt };
  }
  return { manifestSha256: hash(bytes), pairId: manifest.pairId, kind: manifest.kind, sites, diagnosticContext };
}

/** Fixed across arms; every address-like reservation in BOTH captures is excluded. */
export function createAuthorityWriterPlan(snapshots) {
  const reservations = new Set();
  const scan = value => {
    if (!value || typeof value !== "object") return;
    for (const [key, item] of Object.entries(value)) {
      if (["id", "rootId", "ownerHostId", "ownerIdentityId", "identityId", "name", "sessionId"].includes(key) && typeof item === "string") reservations.add(item);
      scan(item);
    }
  };
  scan(snapshots);
  return [["hub", "sandbox-principal"], ["hub", "sandbox-main"], ["work", "sandbox-main"]].map(([site, role], index) => {
    let rootId, name;
    do { rootId = `session:${randomUUID()}`; name = `canary-${index}-${randomUUID()}`; } while (reservations.has(rootId) || reservations.has(name));
    reservations.add(rootId); reservations.add(name);
    return { site, role, rootId, name };
  });
}

// Match the compiled host-leases identity fence. An unrelated lease is not an
// extension. Conservatively retain BOTH state and matching file/reload deadlines;
// a mirrored projection or a later checkpoint may not shorten captured authority.
function matchingExpiryLease(host, leases) {
  return leases.find(lease => lease.id === host.id && lease.rootId === host.rootId && lease.identityId === host.identity?.id &&
    (host.incarnationToken !== undefined ? lease.incarnationToken === host.incarnationToken :
      lease.startedAt === undefined || host.startedAt === undefined || lease.startedAt === host.startedAt)) ?? null;
}
function expiryDeadline(host, lease) {
  const values = [host?.expiresAt, ...(host?.reloadUntil === undefined ? [] : [host.reloadUntil]),
    ...(lease === null ? [] : [lease?.expiresAt, ...(lease?.reloadUntil === undefined ? [] : [lease.reloadUntil])])];
  return values.every(Number.isFinite) ? Math.max(...values) : NaN;
}
function provenCapturedExpiry(row) {
  const proof = row.expiryProof, original = proof?.original, before = row.authorityBefore ?? row.before;
  const captured = original?.value, host = before?.value;
  if (!captured || !host || !Number.isFinite(proof.deltaMs) || original.key !== before.key ||
      original.key !== `topology/hosts/${hash(captured.id ?? "")}` ||
      !["id", "rootId", "incarnationToken"].every(key => host[key] === captured[key]) || host.identity?.id !== captured.identity?.id ||
      host.startedAt !== captured.startedAt + proof.deltaMs ||
      !Object.hasOwn(proof, "originalLease") || !Object.hasOwn(proof, "beforeLease") ||
      !Object.hasOwn(proof, "afterLease") || !Object.hasOwn(proof, "afterHost")) return false;
  if (proof.afterHost !== null && (!proof.afterHost ||
      !["id", "rootId", "incarnationToken", "startedAt"].every(key => proof.afterHost[key] === host[key]) ||
      proof.afterHost.identity?.id !== host.identity?.id)) return false;
  const matches = (lease, value) => lease === null || matchingExpiryLease(value, [lease]) === lease;
  if (!matches(proof.originalLease, captured) || !matches(proof.beforeLease, host) || !matches(proof.afterLease, host)) return false;
  const deadline = Math.max(expiryDeadline(captured, proof.originalLease) + proof.deltaMs,
    expiryDeadline(host, proof.beforeLease), expiryDeadline(proof.afterHost ?? host, proof.afterLease));
  const at = row.kind === "compiled-compaction" ? row.beganAt : row.after?.observedAt;
  return Number.isFinite(deadline) && Number.isFinite(at) && deadline < at && row.expiresAt === deadline;
}

/** Pure fail-closed coverage check. A clock-only observation never establishes expiry. */
export function evaluateAuthorityWriterCoverage({ producers = [], plan = [], expiry = [], passes = [], inFlightAtMeasurementEnd = [], requestedPasses = 3 } = {}) {
  const renewals = plan.length > 0 && producers.length === plan.length && plan.every(expected => {
    const producer = producers.find(row => row.rootId === expected.rootId && row.site === expected.site);
    const leases = producer?.observations ?? [];
    const initial = leases[0]?.readback;
    return producer?.identityVerified === true && leases.length >= 3 && leases.every(row => row.readback?.host?.remoteHost === undefined &&
      row.readback?.host?.id === expected.rootId && row.readback?.lease?.incarnationToken === initial.lease.incarnationToken &&
      row.readback?.row?.ownerIncarnation === initial.row.ownerIncarnation && row.readback?.lease?.writer?.pid === producer.pid &&
      row.process?.nativeBirth === producer.process?.nativeBirth) &&
      leases[1].readback.lease.updatedAt > initial.lease.updatedAt && leases[2].readback.lease.updatedAt > leases[1].readback.lease.updatedAt &&
      leases.every(row => row.readback.lease.expiresAt > row.at);
  });
  const naturalExpiry = expiry.some(row => row.captured === true && row.compiled === true && provenCapturedExpiry(row) &&
    ((row.kind === "compiled-compaction" && row.applied === true && row.before && row.after === null &&
      row.expiryProof.afterHost === null && row.operation?.kind === "delete" && row.operation.key === row.before.key &&
      row.operation.ifVersion === row.before.version && row.at >= row.beganAt) ||
     (row.kind === "compiled-presence-expiry" && row.before?.present === true && row.after?.present === false &&
      row.before.observedAt < row.expiresAt && row.preservedExpiry === true)));
  const window = passes.slice(-requestedPasses), first = window[0]?.pass, last = window.at(-1)?.pass;
  const continuity = Number.isSafeInteger(requestedPasses) && requestedPasses >= 3 && window.length === requestedPasses &&
    window.every((pass, index) => Number.isSafeInteger(pass.pass) && pass.pass > 0 && (!index || pass.pass === window[index - 1].pass + 1) &&
      pass.remoteMirror === "fulfilled" && pass.wireComplete !== false && pass.inboundHosts > 0 && pass.outboundHosts > 0 && pass.allWriterOrigins && pass.bothMirrors) &&
    passes.filter(pass => pass.pass >= first && pass.pass <= last).length === requestedPasses &&
    !inFlightAtMeasurementEnd.some(pass => pass >= first && pass <= last);
  return { renewals, naturalExpiry, continuity, requestedPasses, observedPasses: passes.length,
    expiryCoverage: naturalExpiry ? "OBSERVED" : "INCOMPLETE/BLOCKED", eventReplay: false };
}

// Owned children use the same worker/RPC helper and compiled CLI as the original gate.
// Per-arm deadlines include setup; SIGTERM/SIGINT abort setup too. Each spawn enters
// ownership before any later await, including importer, producers and compiled readers.
async function runLiveAuthorityArm(options, arm, snapshots, plan, armRoot, signal) {
  const owned = [], workers = [], readers = {}, producers = [];
  const meshes = { hub: path.join(armRoot, "hub"), work: path.join(armRoot, "work") };
  const save = (name, value) => fs.writeFileSync(path.join(armRoot, name), JSON.stringify(sanitizeAuthorityEvidence(value)) + "\n", { mode: 0o600 });
  let rpcId = 0, hubProcess, observedBridgeClose = false, measuredWire = [];
  const active = () => { if (signal.aborted) throw signal.reason; };
  const members = pgid => fs.readdirSync("/proc").filter(name => /^\d+$/.test(name)).flatMap(name => {
    try { const value = nativeProcessEvidence(Number(name));
      const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8");
      return value.pgid === pgid && !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]) ? [value] : [];
    } catch (error) { if (["ENOENT", "ESRCH"].includes(error.code)) return []; throw error; }
  });
  const kill = (entry, sig) => { if (!entry.child.pid) return; try { process.kill(-entry.child.pid, sig); } catch (error) { if (error.code !== "ESRCH") throw error; } };
  const own = (name, args, release, backend, role) => {
    active();
    const env = { ...process.env, PI_FABRIC_RELEASE_SHA: release.sha, PI_FABRIC_BUILD_SHA: release.sha,
      PI_FABRIC_MESH_STATE_BACKEND: backend, XDG_RUNTIME_DIR: armRoot, PI_MIXED_CANARY_DEADLINE_AT: String(options.deadlineAt) };
    // Never inherit a real actor/parent/project identity into the bounded producer.
    for (const key of Object.keys(env)) if (/^PI_FABRIC_(?:ACTOR|PARENT|MAIN_AGENT|OWNER|PROJECT|ROLE|SESSION)/.test(key)) delete env[key];
    const child = spawn(process.execPath, args, { detached: true, stdio: ["pipe", "pipe", "pipe"], env });
    const entry = { child, name, role, release, pending: new Map(), stdout: "", stderr: "", closing: false, native: null };
    owned.push(entry);
    if (child.pid) { try { entry.native = nativeProcessEvidence(child.pid); } catch (error) { entry.birthError = error.message; } }
    child.stdin.on("error", () => undefined);
    child.stdout.on("data", chunk => { entry.stdout += chunk; if (entry.stdout.length > 4_000_000) { entry.ioError = "stdout budget"; kill(entry, "SIGKILL"); } });
    child.stderr.on("data", chunk => { entry.stderr += chunk; if (entry.stderr.length > 2_000_000) { entry.ioError = "stderr budget"; kill(entry, "SIGKILL"); } });
    entry.closed = new Promise(resolve => {
      child.once("error", error => { entry.spawnError = error.message; });
      child.once("close", (code, childSignal) => {
        entry.exit = { code, signal: childSignal }; entry.closedAt = Date.now(); entry.unexpectedExit = !entry.closing;
        for (const pending of entry.pending.values()) pending.reject(new Error(`${name} closed`));
        resolve(entry.exit);
      });
    });
    return entry;
  };
  const rpc = (entry, op, args = {}, cleanup = false) => new Promise((resolve, reject) => {
    if (!cleanup) { try { active(); } catch (error) { reject(error); return; } }
    const id = ++rpcId;
    const finish = (error, result) => { clearTimeout(timer); signal.removeEventListener("abort", abort); entry.pending.delete(id); error ? reject(error) : resolve(result); };
    const abort = () => finish(signal.reason);
    const timer = setTimeout(() => finish(new Error(`${entry.name} ${op} RPC deadline`)), cleanup ? 2_000 : 15_000);
    if (!cleanup) signal.addEventListener("abort", abort, { once: true });
    entry.pending.set(id, { resolve: value => finish(null, value), reject: error => finish(error) });
    entry.child.stdin.write(JSON.stringify({ id, op, ...args }) + "\n", error => { if (error) finish(error); });
  });
  const jsonWorker = entry => {
    let buffer = "";
    entry.child.stdout.on("data", chunk => {
      buffer += chunk;
      for (;;) {
        const index = buffer.indexOf("\n"); if (index < 0) break;
        const line = buffer.slice(0, index); buffer = buffer.slice(index + 1);
        try { const response = JSON.parse(line), pending = entry.pending.get(response.id);
          if (pending) response.ok ? pending.resolve(response.result) : pending.reject(new Error(response.error));
        } catch (error) { for (const pending of entry.pending.values()) pending.reject(error); }
      }
    });
    return entry;
  };
  const terminate = async entry => {
    entry.closing = true;
    kill(entry, "SIGTERM");
    const force = setTimeout(() => kill(entry, "SIGKILL"), 2_000);
    try { await entry.closed; } finally { clearTimeout(force); kill(entry, "SIGKILL"); }
    // A leader's close is not group teardown. Bounded verification is the safety net
    // after OS child-close events; zombies cannot heartbeat or hold product custody.
    const end = Date.now() + 1_000;
    while (entry.child.pid && members(entry.child.pid).length && Date.now() < end) await new Promise(resolve => setTimeout(resolve, 25));
    entry.groupEmpty = !entry.child.pid || members(entry.child.pid).length === 0;
  };
  const trace = path.join(armRoot, "wire.jsonl");
  const records = () => fs.readFileSync(trace, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
  try {
    const replayAt = Date.now(), referenceCollectedAt = Math.max(snapshots.hub.collectedAt, snapshots.work.collectedAt), deltaMs = replayAt - referenceCollectedAt;
    arm.clock = { referenceCollectedAt, replayAt, deltaMs, policy: "one global translation; no captured renewal or resurrection" };
    arm.seed = {};
    for (const site of ["hub", "work"]) {
      const importer = jsonWorker(own(`importer-${site}`, [WORKER], arm[site], snapshots[site].backend, "harness-seed-importer"));
      workers.push(importer);
      arm.seed[site] = await rpc(importer, "seed", { snapshot: snapshots[site], mesh: meshes[site], release: arm[site].dir,
        replayAt: snapshots[site].collectedAt + deltaMs });
      importer.closing = true; await rpc(importer, "stop"); await terminate(importer);
    }
    arm.seedVerifiedAt = Date.now();
    // Start native owners immediately after exact seed validation. No expiry pre-wait.
    for (const writer of plan) {
      const release = arm.writers[writer.site], producerTrace = path.join(armRoot, `${writer.name}.jsonl`);
      const process = jsonWorker(own(writer.name, [WORKER], release, snapshots[writer.site].backend, "product-native-producer"));
      workers.push(process);
      const registration = await rpc(process, "start", { producer: true, backend: snapshots[writer.site].backend, release: release.dir,
        mesh: meshes[writer.site], rootId: writer.rootId, name: writer.name, role: writer.role, evidencePath: producerTrace });
      const writerMetadata = registration.readback?.lease?.writer;
      const censusDir = path.join(meshes[writer.site], ".writer-census");
      const census = fs.existsSync(censusDir) ? fs.readdirSync(censusDir).filter(file => file.endsWith(".json")).map(file => {
        try { return JSON.parse(fs.readFileSync(path.join(censusDir, file), "utf8")); } catch { return null; }
      }) : [];
      const found = census.find(row => row?.pid === process.child.pid && row.host === writerMetadata?.host && row.startedAt === writerMetadata?.startedAt);
      const identityVerified = registration.process?.pid === process.child.pid && registration.process.nativeBirth === process.native.nativeBirth &&
        registration.releaseSha === release.sha && registration.buildSha === release.sha && writerMetadata?.releaseSha === release.sha &&
        writerMetadata.pid === process.child.pid && registration.stateBackend === snapshots[writer.site].backend && registration.lockProtocol === 3 &&
        writerMetadata.stateBackend === snapshots[writer.site].backend && writerMetadata.lockProtocol === 3 && Boolean(found) &&
        registration.readback.row?.role === writer.role;
      producers.push({ ...writer, pid: process.child.pid, process: process.native, release, identityVerified, census: found,
        registration, trace: producerTrace, observations: [] });
      if (!identityVerified) throw new Error(`native producer identity/backend/census/role proof incomplete: ${writer.name}`);
    }
    arm.producers = producers;
    fs.writeFileSync(trace, "", { mode: 0o600, flag: "wx" });
    const config = path.join(armRoot, "proxy.json");
    save("proxy.json", { hubRelease: arm.hub.dir, hubSha: arm.hub.sha, workRelease: arm.work.dir, workSha: arm.work.sha,
      hubMesh: meshes.hub, workMesh: meshes.work, hubName: snapshots.hub.name, workName: snapshots.work.name, trace });
    const cursor = path.join(armRoot, "cursor.json"), cursors = snapshots.hub.cursors.filter(row => row.local === snapshots.hub.name && row.remote === snapshots.work.name);
    if (cursors.length > 1) throw new Error("ambiguous captured bridge cursor");
    if (cursors[0]) save("cursor.json", cursors[0]);
    const args = [path.join(arm.hub.dir, "bin/mesh-bridge"), "run", "--mesh", meshes.hub, "--name", snapshots.hub.name,
      "--remote", snapshots.work.name, "--cursor", cursor, "--lock-protocol", "3", "--call-timeout-ms", "5000", "--log-level", "debug",
      "--", process.execPath, WORKER, "authority-proxy", config];
    arm.command = [process.execPath, ...args];
    hubProcess = own("hub-run", args, arm.hub, "sqlite", "product-hub-bridge");
    // Readers are harness-only actual compiled agents, independent of a fatal measured
    // link. They may observe natural expiry AFTER RED, without a continuity claim.
    for (const site of ["hub", "work"]) readers[site] = jsonWorker(own(`reader-${site}`,
      [path.join(arm[site].dir, "bin/mesh-bridge"), "agent", "--mesh", meshes[site], "--peer", snapshots[site].peer, "--lock-protocol", "3"],
      arm[site], snapshots[site].backend, "harness-compiled-presence-reader"));
    const capturedPresence = { hub: [], work: [] };
    const observe = async () => {
      for (const site of ["hub", "work"]) {
        const presence = await rpc(readers[site], "presence");
        const producer = workers.find(row => row.name === plan.find(writer => writer.site === site).name);
        const observation = await rpc(producer, "observe", { ids: [] });
        capturedPresence[site].push({ at: Date.now(), presence: sanitizeAuthorityEvidence(presence), authority: observation.authority });
      }
    };
    await observe();
    // Trace appends and owned lease-renewal files drive progress. A 1000ms safety
    // net covers coalesced fs events and natural wall expiry (which emits no event).
    await new Promise((resolve, reject) => {
      let busy = false, again = false, finished = false;
      const watchers = [];
      const finish = error => { if (finished) return; finished = true; clearInterval(safety); signal.removeEventListener("abort", abort);
        watchers.forEach(watcher => watcher.close()); error ? reject(error) : resolve(); };
      const abort = () => finish(signal.reason);
      const check = async () => {
        if (busy) { again = true; return; } if (finished) return; busy = true;
        try {
          await observe();
          const wire = records();
          const expiry = collectNaturalExpiry(snapshots, deltaMs, capturedPresence, producers, readers, arm);
          for (const producer of producers) producer.observations = readProducerTrace(producer.trace).filter(row => row.type === "native-lease-observation");
          const renewed = producers.every(row => row.observations.length >= 3);
          if (expiry.length && renewed && (hubProcess.closedAt || wire.filter(row => row.type === "response" && row.frame.ok).length >= (options.passes ?? 3) + 1)) finish();
        } catch (error) { finish(error); }
        finally { busy = false; if (again && !finished) { again = false; void check(); } }
      };
      const safety = setInterval(() => void check(), 1_000);
      signal.addEventListener("abort", abort, { once: true });
      for (const file of [trace, ...producers.map(row => row.trace)]) watchers.push(fs.watch(file, () => void check()));
      void hubProcess.closed.then(() => { observedBridgeClose = true; void check(); });
      void check();
    }).catch(error => { if (!signal.aborted) throw error; arm.deadlineReached = true; });
    arm.capturedPresence = capturedPresence;
    arm.expiry = collectNaturalExpiry(snapshots, deltaMs, capturedPresence, producers, readers, arm);
    arm.expiryObservationScope = hubProcess.closedAt ? "includes post-fatal compiled reader; NOT link continuity" : "live measured link";
  } catch (error) { arm.blocker = error.message; }
  finally {
    // Freeze the measured window BEFORE withdrawing any producer. Shutdown wakeups
    // remain in the raw trace, but are not fabricated empty measured passes.
    arm.measurementEndedAt = Date.now();
    arm.measuredBridgeExited = Boolean(hubProcess?.closedAt);
    measuredWire = fs.existsSync(trace) ? records() : [];
    // Keep native fatal/refusal evidence even if expiry or later setup/cleanup blocks acceptance.
    arm.nativeProcessInventory = owned.flatMap(row => row.child.pid ? members(row.child.pid).map(process => ({ owner: row.name, role: row.role, ...process })) : []);
    arm.producers = producers;
    if (hubProcess) { arm.nativeExit = hubProcess.exit ?? null; arm.stderrSha256 = hash(hubProcess.stderr); }
    for (const producer of producers) producer.observations = readProducerTrace(producer.trace).filter(row => row.type === "native-lease-observation");
    for (const row of workers) row.closing = true;
    const stopped = await Promise.allSettled(workers.filter(row => !row.closedAt).map(row => rpc(row, "stop", {}, true)));
    const terminated = await Promise.allSettled(owned.map(terminate));
    arm.cleanup = { ok: stopped.every(row => row.status === "fulfilled") && terminated.every(row => row.status === "fulfilled") &&
      owned.every(row => row.groupEmpty && !row.spawnError && !row.ioError && (row.role === "product-hub-bridge" || !row.unexpectedExit)),
      owned: owned.map(row => ({ name: row.name, role: row.role, ...row.native, exit: row.exit, groupEmpty: row.groupEmpty,
        spawnError: row.spawnError, ioError: row.ioError, unexpectedExit: row.unexpectedExit })),
      failures: [...stopped, ...terminated].filter(row => row.status === "rejected").map(row => String(row.reason)) };
    if (owned.some(row => row.stderr.includes("authority evidence rejected") || row.stdout.includes('"error":"authority evidence rejected"')))
      arm.blocker = "authority evidence rejected";
    for (const row of owned) { save(`${row.name}.stdout.json`, { output: row.role !== "product-hub-bridge" ? row.stdout.split("\n").filter(Boolean).map(line => {
      try { return JSON.parse(line); } catch { return { malformedOutputSha256: hash(line) }; }
    }) : { sha256: hash(row.stdout) } });
      // Opaque native diagnostics may echo authority; retain byte identity, not
      // unchecked text. Structured refusal attribution below retains the known error.
      save(`${row.name}.stderr.log`, { sha256: hash(row.stderr) }); }
  }
  const wire = measuredWire;
  arm.trace = trace; arm.wireSha256 = fs.existsSync(trace) ? hash(fs.readFileSync(trace)) : null;
  const transport = wire.find(row => row.type === "transport");
  arm.agent = wire.find(row => row.type === "agent-identity") ?? null;
  arm.transport = transport ?? null;
  const localFailure = hubProcess?.stderr.match(/stopped: (Bridge presence for [^\n]+ has an older incarnation than persisted authority)/)?.[1];
  const begun = wire.filter(row => row.type === "pass-start").map(row => row.pass);
  const numbers = begun.filter(pass => wire.some(row => row.type === "response" && row.pass === pass));
  arm.inFlightAtMeasurementEnd = begun.filter(pass => !numbers.includes(pass));
  arm.passes = numbers.map(pass => {
    const inbound = wire.find(row => row.type === "presence" && row.pass === pass), outbound = wire.find(row => row.type === "request" && row.pass === pass), response = wire.find(row => row.type === "response" && row.pass === pass);
    const localError = pass === numbers.at(-1) && localFailure && response?.frame.ok ? { source: "hub-cli-stderr", error: localFailure } : null;
    const has = (presence, ids) => ids.every(id => presence?.hosts.some(row => row.record.id === id) && presence?.participants.some(row => row.id === id));
    const hubIds = plan.filter(row => row.site === "hub").map(row => row.rootId), workIds = plan.filter(row => row.site === "work").map(row => row.rootId);
    const mirrorHas = (authority, origin, remote) => producers.filter(row => row.site === origin).every(producer => {
      const id = producer.rootId, expected = producer.registration.readback;
      const host = authority?.stateEntries.find(row => row.key.startsWith("topology/hosts/") && row.value.id === id)?.value;
      const participant = authority?.stateEntries.find(row => row.key.startsWith("topology/participants/") && row.value.id === id)?.value;
      const lease = authority?.leaseFiles.find(row => row.id === id);
      return host?.remoteHost === remote && host.rootId === id && host.identity.id === id &&
        host.incarnationToken === expected.host.incarnationToken && lease?.incarnationToken === expected.lease.incarnationToken &&
        participant?.ownerIncarnation === expected.row.ownerIncarnation && participant.ownerHostId === id;
    });
    return { pass, wireComplete: ["pass-start", "presence", "request", "response"].every(type => wire.filter(row => row.type === type && row.pass === pass).length === 1),
      inboundHosts: inbound?.presence.hosts.length ?? 0, outboundHosts: outbound?.presence.hosts.length ?? 0,
      allWriterOrigins: has(inbound?.presence, workIds) && has(outbound?.presence, hubIds),
      bothMirrors: response?.frame.ok === true &&
        // Mirror readbacks are authority rows, not native-only compiled presence.
        mirrorHas(response?.authorityAfter?.work ?? outbound?.authority, "hub", snapshots.hub.name) &&
        mirrorHas(inbound?.authority, "work", snapshots.work.name),
      remoteMirror: response?.frame.ok === true ? "fulfilled" : response?.frame.ok === false ? "rejected" : "missing",
      directions: [{ direction: "work-to-hub", rawError: localError, refusal: localError ? matchAuthorityRefusal(localError, inbound.predicates) : null },
        { direction: "hub-to-work", rawError: response?.frame.ok === false ? response.frame : null, refusal: response?.refusal ?? null }] };
  });
  arm.coverage = evaluateAuthorityWriterCoverage({ producers, plan, expiry: arm.expiry, passes: arm.passes,
    inFlightAtMeasurementEnd: arm.inFlightAtMeasurementEnd, requestedPasses: options.passes ?? 3 });
  const refusals = arm.passes.flatMap(pass => pass.directions.filter(row => row.rawError));
  arm.observedRefusal = refusals.length && refusals.every(row => row.refusal?.rootMatchesFirst) && arm.nativeExit?.code === 1 ? "MATCHED_NATIVE_RED" : null;
  const identities = producers.length === plan.length && producers.every(row => row.identityVerified) && transport?.workSha === arm.work.sha &&
    arm.agent?.census.some(row => row.pid === transport.childPid && row.releaseSha === arm.work.sha && row.stateBackend === "file" && row.lockProtocol === 3) &&
    arm.agent?.bridgeSha256 === arm.work.bridgeSha256 && arm.agent?.process.nativeBirth === transport.childNative.nativeBirth;
  arm.outcome = "BLOCKED";
  if (!arm.blocker && identities && arm.cleanup.ok && arm.coverage.renewals && arm.coverage.naturalExpiry) {
    if (arm.observedRefusal) arm.outcome = "RED";
    else if (!refusals.length && arm.coverage.continuity && !arm.measuredBridgeExited) arm.outcome = arm.name === "candidate-both-sides" ? "PASS" : "NON_REPRO";
    else arm.outcome = "NON_REPRO";
  }
  if (arm.outcome === "BLOCKED") arm.blocker ??= !arm.coverage.naturalExpiry ? "natural captured expiry coverage incomplete/BLOCKED" : "identity, renewal, native outcome or cleanup incomplete";
  arm.rolloutPermission = false;
  arm.maintenanceCoverage = "observed compiled directory batches and sampled authority window; not an exhaustive filesystem mutation journal";
}
function readProducerTrace(file) { return fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : []; }
export function collectNaturalExpiry(snapshots, deltaMs, observed, producers, readers, arm) {
  const evidence = [];
  for (const site of ["hub", "work"]) for (const entry of snapshots[site].stateEntries.filter(row => row.key.startsWith("topology/hosts/"))) {
    const originalLease = matchingExpiryLease(entry.value, snapshots[site].leaseFiles);
    const proof = (before, after) => ({ original: sanitizeAuthorityEvidence(entry), originalLease: sanitizeAuthorityEvidence(originalLease), deltaMs,
      beforeLease: matchingExpiryLease(before.value, before.authority.leaseFiles),
      afterLease: matchingExpiryLease(before.value, after.leaseFiles),
      afterHost: after.stateEntries.find(candidate => candidate.key === entry.key)?.value ?? null });
    const deadline = (before, expiryProof) => Math.max(expiryDeadline(entry.value, originalLease) + deltaMs,
      expiryDeadline(before.value, expiryProof.beforeLease), expiryDeadline(expiryProof.afterHost ?? before.value, expiryProof.afterLease));
    for (const producer of producers.filter(row => row.site === site)) for (const row of readProducerTrace(producer.trace).filter(row => row.type === "compiled-directory-batch")) {
      const before = row.before.stateEntries.find(candidate => candidate.key === entry.key);
      const operation = row.operations.find(op => op.kind === "delete" && op.key === entry.key);
      if (!before || !operation || operation.ifVersion !== before.version || !(row.at >= row.beganAt) ||
          !row.results.some(result => result.key === entry.key && result.applied) ||
          row.after.stateEntries.some(candidate => candidate.key === entry.key)) continue;
      const expiryProof = proof({ ...before, authority: row.before }, row.after);
      const candidate = { kind: "compiled-compaction", captured: true, compiled: true, site, rootId: entry.value.rootId,
        before, after: null, operation, applied: true, expiryProof, expiresAt: deadline(before, expiryProof), beganAt: row.beganAt,
        process: row.process, artifacts: producer.registration.artifacts, at: row.at };
      if (provenCapturedExpiry(candidate)) evidence.push(candidate);
    }
    if (entry.value.remoteHost !== undefined) continue;
    for (const before of observed[site]) {
      const host = before.presence.hosts.find(host => host.record.id === entry.value.id);
      const authorityBefore = before.authority?.stateEntries.find(candidate => candidate.key === entry.key);
      if (!host || !authorityBefore) continue;
      for (const after of observed[site].filter(row => row.at > before.at && row.authority && !row.presence.hosts.some(host => host.record.id === entry.value.id))) {
        const expiryProof = proof({ ...authorityBefore, authority: before.authority }, after.authority);
        const expiresAt = deadline(authorityBefore, expiryProof);
        const candidate = { kind: "compiled-presence-expiry", captured: true, compiled: true, site, rootId: entry.value.rootId,
          expiresAt, expiryProof, authorityBefore, preservedExpiry: host.expiresAt === expiresAt,
          before: { present: true, observedAt: before.at, host }, after: { present: false, observedAt: after.at },
          process: readers[site]?.native, observerRole: "harness-compiled-presence-reader", releaseSha: arm[site].sha,
          bridgeSha256: arm[site].bridgeSha256 };
        if (candidate.preservedExpiry && before.at < expiresAt && provenCapturedExpiry(candidate)) { evidence.push(candidate); break; }
      }
      if (evidence.some(row => row.site === site && row.rootId === entry.value.rootId)) break;
    }
  }
  return evidence;
}

export async function runLiveAuthorityCanary(options = {}) {
  const startedAt = Date.now(), controller = new AbortController();
  const result = { mode: "authority-snapshot", coverageSlice: "LIVE_NATIVE_WRITERS", scope: PRODUCER_SCOPE,
    releaseGate: false, rolloutPermission: false, acceptance: false, ok: false, outcome: "BLOCKED",
    evidenceKind: options.synthetic ? "SYNTHETIC" : "CAPTURED_AUTHORITY", arms: [], startedAt };
  let scratch, timer;
  const interrupt = () => controller.abort(new Error("authority canary cancelled"));
  const externalAbort = () => controller.abort(options.signal.reason ?? new Error("authority canary cancelled"));
  process.once("SIGINT", interrupt); process.once("SIGTERM", interrupt); options.signal?.addEventListener("abort", externalAbort, { once: true });
  try {
    if (process.platform !== "linux" || process.release.name !== "node" || Number(process.versions.node.split(".")[0]) < 24) throw new Error("authority replay requires Linux Node >=24");
    const timeoutMs = options.timeoutMs ?? 30_000, passes = options.passes ?? 3;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 60_000 || !Number.isSafeInteger(passes) || passes < 3 || passes > 10) throw new Error("invalid bounded writer canary deadline/passes");
    timer = setTimeout(() => controller.abort(new Error("absolute writer canary job deadline")), timeoutMs * 2 + 30_000);
    if (options.signal?.aborted) externalAbort();
    const snapshots = readAuthorityInputs(options, result);
    if (snapshots.hub.peer !== snapshots.work.name || snapshots.work.peer !== snapshots.hub.name) throw new Error("snapshot orientation mismatch");
    result.inputs = validateCaptureProvenance(options, snapshots);
    if (options.hubSha !== PRIMARY_HUB_SHA || options.workSha !== PRIMARY_WORK_SHA || options.hubWriterSha !== C42_SHA || options.workWriterSha !== C42_SHA)
      throw new Error("wrong primary four-role mix; required 9c hub/0fc work/c42 hub+c42 work writers");
    const hub = authorityRelease(options.hub, options.hubSha), work = authorityRelease(options.work, options.workSha);
    const writers = { hub: authorityRelease(options.hubWriter, options.hubWriterSha), work: authorityRelease(options.workWriter, options.workWriterSha) };
    const arms = [{ name: "primary-live", hub, work, writers }];
    if (options.candidate || options.candidateBothSides || options.candidateWork || options.candidateWorkWriter) {
      if (!options.candidateBothSides || !options.candidate || !options.candidateWork || !options.candidateWorkWriter ||
        !SHA.test(options.candidateSha ?? "") || options.candidateSha !== options.candidateWorkSha || options.candidateSha !== options.candidateWorkWriterSha ||
        [PRIMARY_HUB_SHA, PRIMARY_WORK_SHA, C42_SHA].includes(options.candidateSha)) throw new Error("candidate BLOCKED: BOTH RC2 bridge sides and RC2 work writer require the same new full SHA; hub-only candidate is not this mix");
      arms.push({ name: "candidate-both-sides", hub: authorityRelease(options.candidate, options.candidateSha), work: authorityRelease(options.candidateWork, options.candidateWorkSha),
        writers: { hub: writers.hub, work: authorityRelease(options.candidateWorkWriter, options.candidateWorkWriterSha) } });
    }
    const evidenceDir = fs.realpathSync(options.evidenceDir ?? os.tmpdir());
    for (const release of arms.flatMap(arm => [arm.hub, arm.work, arm.writers.hub, arm.writers.work]))
      if (evidenceDir === release.dir || evidenceDir.startsWith(release.dir + path.sep)) throw new Error("evidence must not modify release inputs");
    scratch = fs.mkdtempSync(path.join(evidenceDir, "live-authority-")); fs.chmodSync(scratch, 0o700); result.evidenceRoot = scratch;
    result.writerPlan = createAuthorityWriterPlan(snapshots);
    for (const descriptor of arms) {
      const arm = { ...descriptor, outcome: "BLOCKED", passes: [] }; result.arms.push(arm);
      const root = path.join(scratch, descriptor.name); fs.mkdirSync(root, { mode: 0o700 });
      const armSignal = AbortSignal.any([controller.signal, AbortSignal.timeout(timeoutMs)]);
      await runLiveAuthorityArm({ ...options, deadlineAt: Date.now() + timeoutMs + 5_000 }, arm, snapshots, result.writerPlan, root, armSignal);
      if (controller.signal.aborted) break;
    }
    result.primaryOutcome = result.arms[0]?.outcome;
    result.candidateOutcome = result.arms[1]?.outcome ?? "BLOCKED";
    result.candidateBlocker = result.arms.length < 2 ? "RC2 BOTH bridge sides/work-writer artifact input not supplied" : undefined;
    result.outcome = result.arms.length < 2 || result.arms.length !== arms.length || result.arms.some(arm => arm.outcome === "BLOCKED") ? "BLOCKED" :
      result.arms.some(arm => arm.outcome === "RED") ? "RED" : result.arms.at(-1)?.outcome ?? "BLOCKED";
    result.acceptance = evaluateAuthorityAcceptance(result, options.synthetic);
    result.ok = result.acceptance;
  } catch (error) { result.failure = { message: error.message }; }
  finally { clearTimeout(timer); process.off("SIGINT", interrupt); process.off("SIGTERM", interrupt); options.signal?.removeEventListener("abort", externalAbort); }
  result.finishedAt = Date.now(); result.elapsedMs = result.finishedAt - startedAt;
  return finishAuthorityEvidence(result, scratch);
}

function finishAuthorityEvidence(result, scratch) {
  if (result.diagnosticContext?.partialDiagnostic) { result.acceptance = false; result.ok = false; }
  let clean;
  try { clean = sanitizeAuthorityEvidence(result); }
  catch {
    // Rejection is an outcome, never a dropped frame followed by PASS. Teardown
    // already ran; retain only its boolean summary, not the rejected payload.
    clean = { mode: "authority-snapshot", scope: PRODUCER_SCOPE, releaseGate: false, rolloutPermission: false,
      acceptance: false, ok: false, outcome: "BLOCKED", evidenceRejected: true,
      coverageSlice: result.coverageSlice, evidenceKind: result.evidenceKind,
      diagnosticContext: result.diagnosticContext,
      startedAt: result.startedAt, finishedAt: result.finishedAt, elapsedMs: result.elapsedMs,
      failure: { message: "authority evidence rejected" }, arms: result.arms.map(arm => ({ outcome: "BLOCKED", cleanup: { ok: arm.cleanup?.ok === true } })) };
  }
  if (scratch) fs.writeFileSync(path.join(scratch, "receipt.json"), JSON.stringify(clean) + "\n", { mode: 0o600 });
  return clean;
}

/** Narrow regression reproduction only. Never grants release rollout permission. */
export async function runAuthorityCanary(options = {}) {
  if (options.liveWriters) return runLiveAuthorityCanary(options);
  const startedAt = Date.now();
  const result = { mode: "authority-snapshot", releaseGate: false, rolloutPermission: false, ok: false,
    outcome: "BLOCKED", acceptance: false, coverageSlice: "CAPTURED_ONLY_DIAGNOSTIC",
    scope: "producer-boundary compatibility; principal session bootstrap and auth policy are not exercised",
    evidenceKind: options.synthetic ? "SYNTHETIC" : "CAPTURED_AUTHORITY", startedAt, arms: [] };
  let scratch;
  try {
    if (process.platform !== "linux" || process.release.name !== "node" || Number(process.versions.node.split(".")[0]) < 24)
      throw new Error("authority replay requires Linux Node >=24");
    const passes = options.passes ?? 3, timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isSafeInteger(passes) || passes < 3 || passes > 10 || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 60_000)
      throw new Error("authority replay needs 3..10 passes and a 1000..60000ms deadline");
    const { hub: hubSnapshot, work: workSnapshot } = readAuthorityInputs(options, result);
    if (hubSnapshot.peer !== workSnapshot.name || workSnapshot.peer !== hubSnapshot.name)
      throw new Error("snapshot peer/name orientation mismatch");
    const hub = authorityRelease(options.hub, options.hubSha), work = authorityRelease(options.work, options.workSha);
    const provenance = validateCaptureProvenance(options, { hub: hubSnapshot, work: workSnapshot });
    result.captureProvenance = provenance;
    const releases = [{ name: "primary", hub }];
    if (options.candidate) releases.push({ name: "candidate-control", hub: authorityRelease(options.candidate, options.candidateSha) });
    const evidenceDir = fs.realpathSync(options.evidenceDir ?? os.tmpdir());
    for (const release of [hub, work, ...releases.map(arm => arm.hub)])
      if (evidenceDir === release.dir || evidenceDir.startsWith(release.dir + path.sep)) throw new Error("authority evidence must not modify release inputs");
    scratch = fs.mkdtempSync(path.join(evidenceDir, "authority-canary-")); fs.chmodSync(scratch, 0o700);
    result.evidenceRoot = scratch;
    result.inputs = { hub: { sha256: hash(JSON.stringify(hubSnapshot)), backend: hubSnapshot.backend, collectedAt: hubSnapshot.collectedAt },
      work: { sha256: hash(JSON.stringify(workSnapshot)), backend: workSnapshot.backend, collectedAt: workSnapshot.collectedAt } };
    // Each arm starts from exactly the same complete captures, never from another arm's output.
    for (const release of releases) {
      const armRoot = path.join(scratch, release.name); fs.mkdirSync(armRoot, { mode: 0o700 });
      const arm = { name: release.name, hub: release.hub, work, passes: [], outcome: "BLOCKED", cleanup: { ok: false } };
      result.arms.push(arm);
      const replayAt = Date.now(), hubMesh = path.join(armRoot, "hub"), workMesh = path.join(armRoot, "work");
      const referenceCollectedAt = Math.max(hubSnapshot.collectedAt, workSnapshot.collectedAt), deltaMs = replayAt - referenceCollectedAt;
      arm.clock = { referenceCollectedAt, replayAt, deltaMs, captureGapMs: Math.abs(hubSnapshot.collectedAt - workSnapshot.collectedAt),
        policy: "one global translation for both captures; no renewal or resurrection" };
      arm.seed = { hub: await seedAuthoritySnapshot(hubSnapshot, hubMesh, release.hub.dir, hubSnapshot.collectedAt + deltaMs),
        work: await seedAuthoritySnapshot(workSnapshot, workMesh, work.dir, workSnapshot.collectedAt + deltaMs) };
      arm.inventory = Object.fromEntries([["hub", hubSnapshot], ["work", workSnapshot]].map(([site, snapshot]) => [site, {
        stateHostCount: snapshot.stateEntries.filter(entry => entry.key.startsWith("topology/hosts/")).length,
        nativeHosts: snapshot.stateEntries.filter(entry => entry.key.startsWith("topology/hosts/") && entry.value.remoteHost === undefined)
          .map(({ value }) => ({ hostId: value.id, rootId: value.rootId, tokenHash: value.incarnationToken ?? null,
            stateRemainingMs: value.expiresAt - referenceCollectedAt,
            leaseRemainingMs: snapshot.leaseFiles.find(lease => lease.id === value.id)?.expiresAt - referenceCollectedAt }))
      }]));
      const trace = path.join(armRoot, "wire.jsonl"), config = path.join(armRoot, "proxy.json");
      fs.writeFileSync(trace, "", { mode: 0o600, flag: "wx" });
      fs.writeFileSync(config, JSON.stringify({ hubRelease: release.hub.dir, hubSha: release.hub.sha, workRelease: work.dir, workSha: work.sha, hubMesh, workMesh,
        hubName: hubSnapshot.name, workName: workSnapshot.name, trace }), { mode: 0o600, flag: "wx" });
      // Cursors retain their relative ordering and exact captured values, but no event log is
      // transplanted: this arm exercises presence only, not sequence/event delivery claims.
      const cursorPath = path.join(armRoot, "cursor.json");
      const cursors = hubSnapshot.cursors.filter(cursor => cursor.local === hubSnapshot.name && cursor.remote === workSnapshot.name);
      if (cursors.length > 1) throw new Error("ambiguous captured bridge cursor");
      if (cursors[0]) fs.writeFileSync(cursorPath, JSON.stringify(cursors[0]), { mode: 0o600, flag: "wx" });
      arm.cursor = { captured: cursors.length, eventReplay: false, path: cursorPath };
      const args = [path.join(release.hub.dir, "bin/mesh-bridge"), "run", "--mesh", hubMesh, "--name", hubSnapshot.name,
        "--remote", workSnapshot.name, "--cursor", cursorPath, "--lock-protocol", "3", "--call-timeout-ms", "5000",
        "--log-level", "debug", "--", process.execPath, WORKER, "authority-proxy", config];
      arm.command = [process.execPath, ...args];
      const child = spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite", XDG_RUNTIME_DIR: armRoot,
          PI_FABRIC_RELEASE_SHA: release.hub.sha, PI_FABRIC_BUILD_SHA: release.hub.sha } });
      let stderr = "", stdout = "", stopRequested = false, spawnError;
      child.stderr.on("data", chunk => { stderr += chunk; if (stderr.length > 2_000_000) child.kill("SIGTERM"); });
      child.stdout.on("data", chunk => { stdout += chunk; if (stdout.length > 2_000_000) child.kill("SIGTERM"); });
      child.once("error", error => { spawnError = error.message; });
      const records = () => fs.readFileSync(trace, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
      // Wire append is the event source; the one absolute timer bounds an unresponsive arm.
      const watcher = fs.watch(trace, () => {
        try {
          const completed = records().filter(row => row.type === "response" && row.frame.ok);
          if (completed.length >= passes && !stopRequested) { stopRequested = true; child.kill("SIGTERM"); }
        } catch { /* An append may not yet contain a complete frame; close-time parse is strict. */ }
      });
      const deadline = setTimeout(() => { stopRequested = true; arm.deadlineReached = true; child.kill("SIGTERM"); }, timeoutMs);
      const force = setTimeout(() => child.kill("SIGKILL"), timeoutMs + 10_000);
      const exit = await new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
      clearTimeout(deadline); clearTimeout(force); watcher.close();
      fs.writeFileSync(path.join(armRoot, "hub.stderr.log"), JSON.stringify({ sha256: hash(stderr) }) + "\n", { mode: 0o600 });
      fs.writeFileSync(path.join(armRoot, "hub.stdout.log"), JSON.stringify({ sha256: hash(stdout) }) + "\n", { mode: 0o600 });
      if (stderr.includes("authority evidence rejected")) arm.blocker = "authority evidence rejected";
      arm.exit = exit; arm.spawnError = spawnError;
      const wire = records();
      const transport = wire.find(row => row.type === "transport");
      const alive = pid => {
        if (!Number.isSafeInteger(pid)) return true;
        try { const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8"); return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]); }
        catch (error) { if (error.code === "ENOENT") return false; throw error; }
      };
      arm.cleanup = { ok: Boolean(transport && !alive(transport.pid) && !alive(transport.childPid)),
        hubPid: child.pid, transportPid: transport?.pid, agentPid: transport?.childPid,
        transportExit: wire.find(row => row.type === "transport-exit") ?? null };
      arm.trace = trace;
      arm.wireSha256 = hash(fs.readFileSync(trace));
      const localFailure = stderr.match(/stopped: (Bridge presence for [^\n]+ has an older incarnation than persisted authority)/)?.[1];
      const passNumbers = [...new Set(wire.filter(row => row.type === "presence").map(row => row.pass))];
      for (const pass of passNumbers) {
        const inbound = wire.find(row => row.type === "presence" && row.pass === pass);
        const outbound = wire.find(row => row.type === "request" && row.pass === pass);
        const response = wire.find(row => row.type === "response" && row.pass === pass);
        const localError = pass === passNumbers.at(-1) && localFailure && response?.frame.ok ? { source: "hub-cli-stderr", error: localFailure } : null;
        const localMatch = localError ? matchAuthorityRefusal(localError, inbound.predicates) : null;
        arm.passes.push({ pass, inboundHosts: inbound.presence.hosts.length, outboundHosts: outbound?.presence.hosts.length ?? null,
          directions: [{ direction: "work-to-hub", predicates: inbound.predicates, rawError: localError,
            refusal: localMatch }, { direction: "hub-to-work", predicates: outbound?.predicates ?? [],
            rawError: response?.frame.ok === false ? response.frame : null, refusal: response?.refusal ?? null }],
          remoteMirror: response?.frame.ok === true ? "fulfilled" : response?.frame.ok === false ? "rejected" : "missing" });
      }
      const refusals = arm.passes.flatMap(pass => pass.directions.filter(direction => direction.rawError));
      const matched = refusals.length > 0 && refusals.every(direction => direction.refusal?.rootMatchesFirst);
      const nonemptyExposure = arm.passes.some(pass => pass.inboundHosts > 0 || pass.outboundHosts > 0);
      arm.coverage = { requestedPasses: passes, observedPasses: arm.passes.length,
        continuouslyNonemptyBothWays: arm.passes.length >= passes && arm.passes.every(pass => pass.inboundHosts > 0 && pass.outboundHosts > 0),
        observedNativePopulationOnly: true, renewals: 0, eventReplay: false };
      if (!arm.blocker && arm.cleanup.ok && !spawnError && nonemptyExposure && matched && exit.code === 1) arm.outcome = "RED";
      else if (!arm.blocker && arm.cleanup.ok && !spawnError && nonemptyExposure && (arm.passes.length >= passes || arm.deadlineReached) && !refusals.length && stopRequested && exit.code === 0)
        arm.outcome = "NON_REPRO";
      else arm.blocker = "setup, population, pass completion, refusal attribution, native exit, or cleanup evidence incomplete";
      arm.boundedPassingOutcome = arm.outcome === "NON_REPRO" && arm.coverage.continuouslyNonemptyBothWays;
      arm.rolloutPermission = false;
    }
    result.primaryOutcome = result.arms[0].outcome;
    result.outcome = result.arms.some(arm => arm.outcome === "BLOCKED") ? "BLOCKED" :
      result.arms.some(arm => arm.outcome === "RED") ? "RED" : "NON_REPRO";
    result.diagnosticComplete = result.arms.every(arm => arm.boundedPassingOutcome);
    result.ok = false; // A zero-writer diagnostic can never satisfy four-role acceptance.
    result.limitedCoverage = result.arms.some(arm => arm.outcome === "NON_REPRO" && !arm.boundedPassingOutcome);
  } catch (error) { result.failure = { message: error.message }; }
  result.finishedAt = Date.now(); result.elapsedMs = result.finishedAt - startedAt;
  return finishAuthorityEvidence(result, scratch);
}

async function authorityMain() {
  let result;
  try {
    const argv = process.argv.slice(2), index = argv.indexOf("--receipt");
    const destination = index >= 0 ? argv[index + 1] : null;
    if (destination) protectAuthorityInputs(argv, destination);
    const options = parseArgs(argv);
    result = await runAuthorityCanary(options);
    if (destination) fs.writeFileSync(destination, JSON.stringify(result) + "\n", { mode: 0o600, flag: "wx" });
  } catch (error) { result = { ok: false, mode: "authority-snapshot", releaseGate: false, rolloutPermission: false, outcome: "BLOCKED", failure: { message: error.message } }; }
  process.stdout.write(JSON.stringify(result) + "\n");
  process.exitCode = result.ok ? 0 : result.outcome === "RED" ? 1 : 2;
}
function protectAuthorityInputs(argv, destination) {
  const canonical = canonicalDestination(destination);
  for (let index = 0; index < argv.length; index++) {
    if (!["--hub", "--work", "--candidate", "--candidate-work", "--hub-writer", "--work-writer", "--candidate-work-writer",
      "--hub-snapshot", "--work-snapshot", "--capture-manifest"].includes(argv[index])) continue;
    const input = canonicalDestination(argv[index + 1]);
    if (canonical === input || canonical.startsWith(input + path.sep)) throw new Error("authority receipt must not modify any replay input");
  }
}

async function main() {
  const startedAt = Date.now(), controller = new AbortController();
  let options, scratch, timer, receiptPath;
  const evidence = { mode: "release-gate", holdMs: MIN_HOLD, releases: [], nodes: [], registrations: [],
    routes: [], unsupported: [], links: [], restartCount: 0, plans: [], sends: [], tallies: [], hold: {}, history: {} };
  const onSignal = signal => controller.abort(new Error(`interrupted by ${signal}`));
  const sigint = () => onSignal("SIGINT"), sigterm = () => onSignal("SIGTERM");
  process.once("SIGINT", sigint); process.once("SIGTERM", sigterm);
  try {
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mx-")); fs.chmodSync(scratch, 0o700);
    const argv = process.argv.slice(2), receiptIndex = argv.indexOf("--receipt");
    if (receiptIndex >= 0 && argv[receiptIndex + 1] && !argv[receiptIndex + 1].startsWith("--")) {
      const destination = path.resolve(argv[receiptIndex + 1]);
      protectReleaseInputs(argv, destination);
      receiptPath = destination;
    }
    options = parseArgs(argv);
    evidence.mode = options.baselineControl ? "baseline-control" : "release-gate"; evidence.holdMs = options.holdMs;
    const releases = validateInputs(options);
    evidence.releases = [releases.candidate, ...releases.old];
    timer = setTimeout(() => controller.abort(new Error("absolute canary process deadline exceeded")), options.holdMs + 240_000);
    await run(options, releases, evidence, scratch, controller.signal);
  } catch (error) { evidence.failure = { message: error.message ?? String(error), stack: error.stack }; }
  finally { clearTimeout(timer); process.off("SIGINT", sigint); process.off("SIGTERM", sigterm); }
  const result = { ...evaluateReceipt(evidence), mode: evidence.mode, evidenceRoot: scratch ?? null,
    startedAt, finishedAt: Date.now(), elapsedMs: Date.now() - startedAt, evidence };
  const text = `${JSON.stringify(result)}\n`;
  try {
    if (scratch) fs.writeFileSync(path.join(scratch, "receipt.json"), text, { mode: 0o600 });
    if (receiptPath) {
      protectReleaseInputs(process.argv.slice(2), receiptPath);
      fs.writeFileSync(receiptPath, text, { mode: 0o600 });
    }
  } catch (error) {
    result.ok = false; result.checks.push({ name: "receipt-write", status: "FAIL", evidence: error.message });
    try { if (scratch) fs.writeFileSync(path.join(scratch, "receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 }); }
    catch (scratchError) { result.checks.push({ name: "scratch-receipt-write", status: "FAIL", evidence: scratchError.message }); }
  }
  process.stdout.write(`${JSON.stringify(result)}\n`);
  process.stderr.write(`mixed-release-canary: ${result.ok ? "PASS" : "FAIL"} (${result.mode}; releaseGate=${result.releaseGate}); evidence ${scratch ?? "unavailable"}\n`);
  process.exitCode = result.ok ? 0 : 1;
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href)
  await (process.argv.includes("--authority-snapshot") ? authorityMain() : main());
