#!/usr/bin/env node
// Real, private mixed-release mesh gate. All artifacts and raw logs are retained in scratch.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { pathToFileURL, fileURLToPath } from "node:url";

const SHA = /^[a-f0-9]{40}$/;
const hash = value => createHash("sha256").update(value).digest("hex");
const WORKER = fileURLToPath(new URL("./lib/mixed-release-worker.mjs", import.meta.url));
const MIN_HOLD = 60_000;

export function parseArgs(argv) {
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
 * history {seeded,preserved,old,hub,warning}. The CLI retains the underlying raw readbacks too.
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
  const h = e.history ?? {}, seeded = h.seeded ?? {}, preserved = h.preserved ?? {};
  const historyValid = seeded.id && seeded.id === preserved.id && seeded.id === h.old?.id && seeded.id === h.hub?.id &&
    seeded.hostTokenHash && seeded.hostTokenHash === seeded.leaseTokenHash && seeded.hostTokenHash === preserved.hostTokenHash &&
    preserved.hostTokenHash === preserved.leaseTokenHash && h.old.native && h.old.tokenless && h.hub.tokenless &&
    h.hub.remoteHost === h.old.name && h.warning?.name === "STALE_INCARNATION_HISTORY" &&
    ["bridge", "canary"].includes(h.warning?.source) && h.warning.rootId === seeded.id &&
    h.warning.tokenHash === seeded.hostTokenHash && nodes.some(node => node.role === "old" && node.id === seeded.id && node.name === h.old.name);
  check("stale-incarnation-history-warning-not-fatal", baseline ? true : Boolean(historyValid),
    baseline ? { skipped: "baseline-control is NOT the mixed-release release gate" } : h);
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
    const timer = setTimeout(() => finish(new Error(`${worker.name} ${op} RPC deadline`)), 35_000);
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
    remoteHost: row.host?.remoteHost, bridgeVia: row.host?.bridgeVia });
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
    const regressionId = legacy?.native.id;
    if (!options.baselineControl) {
      evidence.history.old = evidence.registrations.find(row => row.id === regressionId);
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
      const [immediatelyBefore] = await rpc(newer.worker, "presence", { ids: [regressionId] });
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
    if (!options.baselineControl) {
      const productionWarnings = measured.flatMap(link => link.log.split("\n").map(line => ({ link: link.name, line })))
        .filter(({ line }) => /warn|skip|ignor/i.test(line) && /incarnation/i.test(line));
      const productionWarning = productionWarnings.find(({ line }) => line.includes(regressionId));
      evidence.history.warning = { name: "STALE_INCARNATION_HISTORY", source: productionWarning ? "bridge" : "canary",
        productionWarningObserved: Boolean(productionWarning), rootId: regressionId, tokenHash: evidence.history.seeded.hostTokenHash,
        message: productionWarning?.line ?? "Canary warning: real persisted NEW token/lease history encountered a native OLD tokenless row via hub; production emitted no correlated named warning.",
        uncorrelatedProductionWarnings: productionWarnings.filter(value => value !== productionWarning).slice(0, 32),
        ...(productionWarning ? { link: productionWarning.link } : {}) };
      save("stale-incarnation-warning", evidence.history.warning);
    }
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
    await sendRound("before");
    await new Promise((resolve, reject) => {
      const abort = () => finish(signal.reason);
      const finish = error => { clearTimeout(timer); signal.removeEventListener("abort", abort); error ? reject(error) : resolve(); };
      const timer = setTimeout(() => finish(), Math.max(0, options.holdMs - (performance.now() - holdStarted)));
      signal.addEventListener("abort", abort, { once: true });
      for (const child of [...measured, ...endpoints]) void child.closed.then(() => finish(new Error(`early owned process close: ${child.name}`)));
    });
    await observeRoutes("after");
    await sendRound("after");
    const snapshots = await Promise.all(endpoints.map(async worker => ({ id: worker.native.id, ...await rpc(worker, "tally") })));
    evidence.tallies = snapshots;
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
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) await main();
