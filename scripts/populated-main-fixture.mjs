// Synthetic population planning and private-file safety. No runtime is imported here.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

export const sha256 = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
export const digest = text => createHash("sha256").update(text).digest("hex");
export const jsonBytes = value => Buffer.byteLength(JSON.stringify(value));
export const shape = (profile, name) => {
  const found = profile.semanticSnapshot.shapes.filter(row => row.shape === name);
  assert.equal(found.length, 1, `profile needs one semantic shape: ${name}`);
  return found[0];
};
const integer = (value, minimum, maximum, label) => {
  assert(Number.isSafeInteger(value) && value >= minimum && value <= maximum, `invalid ${label}`);
  return value;
};

export function populationPlan(profile, overrides = {}) {
  assert([2, 3, 4, 5].includes(profile.version), "profile version 2 through 5 required");
  assert.equal(profile.backend, "sqlite");
  assert.equal(profile.journalMode, "wal");
  const requested = profile.requestedWorkload, allocation = profile.operationalPopulationPlan;
  assert(requested && allocation && profile.tables && profile.semanticSnapshot);
  const mainCounts = allocation.mainActorCounts, residentCounts = allocation.residentActorCounts;
  assert(Array.isArray(mainCounts) && Array.isArray(residentCounts));
  integer(requested.mainProcesses, 1, 30, "Main count");
  integer(requested.residentProcesses, 1, 3, "resident count");
  assert.equal(mainCounts.length, requested.mainProcesses);
  assert.equal(residentCounts.length, requested.residentProcesses);
  for (const count of [...mainCounts, ...residentCounts]) integer(count, 1, 64, "actor allocation");
  const observedIdle = shape(profile, "actors/<id>/<id>").fields["status=idle"];
  assert.equal(mainCounts.concat(residentCounts).reduce((a, b) => a + b, 0), observedIdle);
  assert.equal(allocation.activeIdleActors, observedIdle);
  assert.equal(allocation.remainingActorStatusesAreHistoricalNotLive, true);
  assert.equal(allocation.networkAndPaidInference, false);
  const mains = overrides.mains ?? requested.mainProcesses, residents = overrides.residents ?? requested.residentProcesses;
  integer(mains, 1, mainCounts.length, "selected Mains");
  integer(residents, 1, residentCounts.length, "selected residents");
  const idleSeconds = overrides.idleSeconds ?? requested.idleSeconds;
  const trafficSeconds = overrides.trafficSeconds ?? requested.trafficSeconds;
  integer(idleSeconds, 1, 600, "idle duration"); integer(trafficSeconds, 2, 600, "traffic duration");
  const focused = Object.keys(overrides).length > 0;
  if (!focused) {
    assert.equal(idleSeconds, 600); assert.equal(trafficSeconds, 600);
    assert.equal(mains, 30); assert.equal(residents, 3);
  }
  const owners = [
    ...mainCounts.slice(0, mains).map((actors, i) => ({ label: `M${String(i).padStart(2, "0")}`, role: i === 0 ? "measuredMain" : "peerMain", actors })),
    ...residentCounts.slice(0, residents).map((actors, i) => ({ label: `R${String(i).padStart(2, "0")}`, role: "residentHost", actors })),
  ];
  const actors = owners.reduce((total, owner) => total + owner.actors, 0);
  const historicalActorStatuses = Object.fromEntries(Object.entries(shape(profile, "actors/<id>/<id>").fields)
    .filter(([key]) => key.startsWith("status=")).map(([key, value]) => [key.slice(7), value]));
  assert.equal(Object.values(historicalActorStatuses).reduce((a, b) => a + b, 0), shape(profile, "actors/<id>/<id>").rows);
  historicalActorStatuses.idle -= actors;
  assert(historicalActorStatuses.idle >= 0);
  const weights = profile.semanticSnapshot.recentChangesByShape;
  assert(Array.isArray(weights) && weights.every(row => Number.isSafeInteger(row.rows) && row.rows > 0));
  return { mains, residents, actors, owners, idleSeconds, trafficSeconds, focused, historicalActorStatuses,
    trafficWeights: weights.map(row => ({ ...row })),
    startupSeconds: focused ? 120 : 300, drainSeconds: focused ? 10 : 60, terminateSeconds: 30,
    shapeInterpretation: "Separate aggregate snapshots; exact preseed targets and native drift are reported independently." };
}

export function parseArguments(argv) {
  const result = { overrides: {} }, seen = new Set();
  const paths = { "--profile": "profile", "--import-root": "candidate", "--pi-cli": "cli", "--out": "out" };
  const numbers = { "--mains": "mains", "--residents": "residents", "--idle-seconds": "idleSeconds", "--traffic-seconds": "trafficSeconds" };
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === "--help" && argv.length === 1) return { help: true };
    assert(!flag.startsWith("--legacy-"), "OLD_MAIN_SCOPE_SUPERSEDED: current native Mains only; historical rows are static");
    assert(!seen.has(flag), `duplicate argument ${flag}`); seen.add(flag);
    assert(Object.hasOwn(paths, flag) || Object.hasOwn(numbers, flag), `unknown argument ${flag}`);
    const value = argv[++i]; assert(value && !value.startsWith("--"), `missing value for ${flag}`);
    if (Object.hasOwn(paths, flag)) result[paths[flag]] = path.resolve(value);
    else { assert(/^\d+$/.test(value), `invalid number for ${flag}`); result.overrides[numbers[flag]] = Number(value); }
  }
  for (const key of ["profile", "candidate", "cli", "out"]) assert(result[key], `${key} required`);
  assert(result.out.endsWith(".json"), "output must end in .json");
  return result;
}

export function beneath(parent, child) {
  const relative = path.relative(parent, child);
  return relative !== "" && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

export function noLinks(file) {
  const full = path.resolve(file), parsed = path.parse(full);
  let current = parsed.root;
  for (const component of full.slice(parsed.root.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, component);
    const stat = fs.lstatSync(current);
    assert(!stat.isSymbolicLink(), `symlink refused: ${current}`);
  }
  return full;
}

export function privateDirectory(directory) {
  noLinks(directory);
  const stat = fs.lstatSync(directory);
  assert(stat.isDirectory(), "private parent must be a directory");
  if (process.getuid) {
    assert.equal(stat.uid, process.getuid(), "private parent owner differs");
    assert.equal(stat.mode & 0o777, 0o700, "private parent must already be mode 0700");
  }
  return { directory: fs.realpathSync(directory), device: String(stat.dev), inode: String(stat.ino) };
}

export function temporaryParent() {
  assert(process.env.TMPDIR && path.isAbsolute(process.env.TMPDIR), "explicit private TMPDIR required");
  return privateDirectory(process.env.TMPDIR).directory;
}

export function freshJson(file, value) {
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
  try { fs.writeFileSync(fd, JSON.stringify(value, null, 2) + "\n"); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
}

export function prepareOutput(out, temporary = temporaryParent()) {
  assert(beneath(temporary, out), "evidence must be beneath private TMPDIR");
  const parent = privateDirectory(path.dirname(out));
  const prefix = out.slice(0, -5);
  const files = { summary: out, provenance: `${prefix}.provenance.json`, population: `${prefix}.population.json`,
    ranks: `${prefix}.ranks.json`, custody: `${prefix}.custody.json`, root: `${prefix}.run` };
  for (const file of Object.values(files)) assert(!fs.lstatSync(file, { throwIfNoEntry: false }), `fresh artifact required: ${file}`);
  // Reserve every companion before doing anything native. Never chmod a caller's parent.
  const descriptors = new Map();
  try {
    for (const [name, file] of Object.entries(files)) {
      if (name === "root") continue;
      const fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
      descriptors.set(name, { fd, stat: fs.fstatSync(fd), written: false });
    }
    fs.mkdirSync(files.root, { mode: 0o700 });
  } catch (error) {
    for (const { fd } of descriptors.values()) fs.closeSync(fd);
    throw error; // Retain partial reservations; never overwrite on retry.
  }
  return { files, parent, write(name, value) {
    const reservation = descriptors.get(name); assert(reservation && !reservation.written, "artifact already finalized");
    const stat = fs.lstatSync(files[name]);
    assert(!stat.isSymbolicLink() && stat.ino === reservation.stat.ino && stat.dev === reservation.stat.dev, "artifact inode changed");
    fs.writeFileSync(reservation.fd, JSON.stringify(value, null, 2) + "\n"); fs.fsyncSync(reservation.fd);
    reservation.written = true;
  }, close() { for (const reservation of descriptors.values()) fs.closeSync(reservation.fd); descriptors.clear(); } };
}

export function databaseIdentity(file) {
  noLinks(file);
  const stat = fs.statSync(file, { bigint: true }); assert(stat.isFile());
  return { file: fs.realpathSync(file), device: String(stat.dev), inode: String(stat.ino) };
}
export const sameDatabase = (a, b) => !!a && !!b && a.file === b.file && a.device === b.device && a.inode === b.inode;

export function pinFile(file) {
  noLinks(file); assert(fs.statSync(file).isFile(), "pin must name a regular file");
  return { file: fs.realpathSync(file), sha256: sha256(file) };
}
export function verifyPin(pin) {
  assert(pin && /^[a-f0-9]{64}$/.test(pin.sha256), "invalid dependency byte pin");
  assert.equal(pinFile(pin.file).sha256, pin.sha256, "frozen dependency bytes changed");
  return pin.file;
}

// This gate is supplied by the caller only after both dependency owners are quiescent.
// Merely finding helper files is never permission to read or import them.
export function readDependencyGate(file) {
  assert(file, "DEPENDENCY_GATE_REQUIRED: caller must supply POPULATED_MAIN_DEPENDENCY_GATE after interface freeze");
  noLinks(file);
  const stat = fs.statSync(file);
  assert(stat.isFile() && (!process.getuid || stat.uid === process.getuid()) && (stat.mode & 0o077) === 0, "private gate required");
  let gate = JSON.parse(fs.readFileSync(file, "utf8"));
  if (gate.sourceHashes) {
    assert(gate.checkedFiles > 0 && gate.terminalCommands?.length > 0 && gate.liveOwnedGroupMembers?.length === 0, "parent dependency custody verification missing");
    assert(gate.observerNativeState?.includes("completed") && gate.seederNativeState?.includes("completed"), "parent native completion verification missing");
    const base = path.dirname(path.dirname(path.resolve(file)));
    gate = { schema: 1, approved: true, interface: "populated-main-fixed-v1", head: gate.head,
      observer: { file: path.join(base, "scripts/main-batch-observer.mjs"), sha256: gate.sourceHashes["scripts/main-batch-observer.mjs"] },
      seeder: { file: path.join(base, "scripts/populated-mesh-state-fixture.mjs"), sha256: gate.sourceHashes["scripts/populated-mesh-state-fixture.mjs"] } };
  }
  assert.equal(gate.schema, 1); assert.equal(gate.approved, true);
  assert.equal(gate.interface, "populated-main-fixed-v1");
  for (const key of ["observer", "seeder"]) verifyPin(gate[key]);
  return { ...gate, receipt: pinFile(file) };
}

export async function loadDependencies(gate) {
  const observer = await import(pathToFileURL(verifyPin(gate.observer)).href);
  const seeder = await import(pathToFileURL(verifyPin(gate.seeder)).href);
  for (const name of ["createMainBatchObserver", "aggregateSites", "aggregateTransactions"]) assert.equal(typeof observer[name], "function", `observer API missing ${name}`);
  for (const name of ["buildStateSeed", "createPopulatedMesh", "inspectPopulatedMesh"]) assert.equal(typeof seeder[name], "function", `seeder API missing ${name}`);
  return { observer, seeder };
}

export function candidatePins(candidate, cli, sources = []) {
  const packageFile = path.join(candidate, "package.json"), pkg = JSON.parse(fs.readFileSync(packageFile, "utf8"));
  assert.equal(pkg.type, "module");
  const main = typeof pkg.exports?.["."]?.import === "string" ? pkg.exports["."].import : pkg.main;
  assert.equal(main, "./dist/index.js", "unsupported candidate extension entry");
  assert.equal(pkg.exports?.["./mesh"]?.import, "./dist/mesh.js", "unsupported public mesh entry");
  // These stable build entries are checked, not guessed chunk names or host fallbacks.
  const entries = [packageFile, path.join(candidate, main), path.join(candidate, "dist/mesh.js"),
    ...["dist/config.js", "src/config.ts"].filter(file => fs.existsSync(path.join(candidate, file))).map(file => path.join(candidate, file)),
    path.join(candidate, "dist/residency/host.js"), path.join(candidate, "dist/worker.js"),
    path.join(candidate, "bin/fabric-mesh-backend"), cli, ...sources];
  const walk = (directory, suffixes) => fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(item => {
    assert(!item.isSymbolicLink(), "candidate source/artifact symlinks refused");
    const file = path.join(directory, item.name);
    return item.isDirectory() ? walk(file, suffixes) : suffixes.some(suffix => item.name.endsWith(suffix)) ? [file] : [];
  });
  const compiled = walk(path.join(candidate, "dist"), [".js", ".mjs", ".cjs"]);
  const source = walk(path.join(candidate, "src"), [".ts"]);
  const files = [...new Set([...entries, ...compiled, ...source])].map(pinFile);
  return { version: pkg.version, compiledFiles: compiled.length, sourceFiles: source.length,
    manifestSha256: digest(JSON.stringify(files)), files };
}

export function sdkEntryFromCli(cli) {
  let directory = path.dirname(fs.realpathSync(cli));
  for (;;) {
    const file = path.join(directory, "package.json");
    if (fs.existsSync(file)) {
      const pkg = JSON.parse(fs.readFileSync(file, "utf8"));
      if (pkg.name?.endsWith("/pi-coding-agent")) {
        const entry = pkg.exports?.["."];
        const relative = typeof entry === "string" ? entry : entry?.import ?? entry?.default ?? pkg.main;
        assert(typeof relative === "string", "native SDK export not declared");
        const resolved = path.resolve(directory, relative);
        assert(beneath(directory, resolved));
        return { version: pkg.version, entry: pinFile(resolved), package: pinFile(file),
          sessionManager: pinFile(path.join(directory, "dist/core/session-manager.js")) };
      }
    }
    const parent = path.dirname(directory); assert.notEqual(parent, directory, "CLI is not inside a native SDK package"); directory = parent;
  }
}

export async function createNativeJournals(plan, root, project, sdk) {
  const { SessionManager } = await import(pathToFileURL(verifyPin(sdk.entry)).href);
  assert.equal(typeof SessionManager?.create, "function");
  const owners = [];
  for (const owner of plan.owners) {
    const sessionDir = path.join(root, owner.label, "sessions"); fs.mkdirSync(sessionDir, { recursive: true, mode: 0o700 });
    const manager = SessionManager.create(project, sessionDir);
    // A real synthetic user entry makes the supported manager persist its header.
    // It is stored history, NOT an executed turn or a forged assistant response.
    manager.appendMessage({ role: "user", content: "Synthetic cohort journal initialization; no work requested.", timestamp: Date.now() });
    const sessionFile = manager.getSessionFile(); assert(sessionFile && fs.statSync(sessionFile).isFile(), "native journal did not persist");
    const reopened = SessionManager.open(sessionFile, sessionDir, project);
    assert.equal(reopened.getSessionId(), manager.getSessionId(), "native journal reopening changed identity");
    const sessionId = reopened.getSessionId();
    owners.push({ ...owner, sessionDir, sessionFile, sessionId, rootId: `session:${sessionId}`, journalPin: pinFile(sessionFile),
      journalCreation: "native SessionManager.create/appendMessage(user)/open; no runtime started" });
  }
  assert.equal(new Set(owners.map(owner => owner.sessionId)).size, owners.length);
  return owners;
}

const syntheticId = (kind, index) => digest(`synthetic-cohort:${kind}:${index}`).slice(0, 32);
const rootForHistory = index => `session:archive-${syntheticId("root", index)}`;

export function actorRegistryRows(owners, project, now = Date.now()) {
  return owners.flatMap(owner => Array.from({ length: owner.actors }, (_, index) => ({
    id: syntheticId(owner.label, index), name: `fixture-${owner.label.toLowerCase()}-${index}`, rootId: owner.rootId,
    ownerSessionId: owner.sessionId, project, scope: "session", residency: owner.role === "residentHost" ? "durable" : "session",
    instructions: "Synthetic inert actor. Never infer. Only matched skip-filter events are permitted.",
    createdAt: now, updatedAt: now, status: "idle", runner: "pi", topics: [`fixture.cohort.${owner.label.toLowerCase()}`], events: [],
    delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: false,
    activationFilter: [{ id: "cohort-skip", topic: [`fixture.cohort.${owner.label.toLowerCase()}`], kind: ["fixture.history"],
      where: [{ path: "data.syntheticCohort", equals: true }] }],
    // messageHistory is a codec-owned byte-range reference, NEVER an array.
    // Empty inline messages are the supported format-1 input for a fresh actor.
    filteredCount: 0, messages: [],
  })));
}

export const canonicalDigest = value => digest(JSON.stringify(value, (_key, item) => item && typeof item === "object" && !Array.isArray(item)
  ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item));

export function semanticPopulation(profile, plan, owners, project, now = Date.now(), seedWriter) {
  assert.equal(owners.length, plan.owners.length);
  const actors = actorRegistryRows(owners, project, now), overlays = [], counts = {};
  const add = (category, key, value, pad = false, identity) => {
    overlays.push({ key, value, ...(pad ? { pad: true } : {}), ...(identity ? { identity } : {}) }); counts[category] = (counts[category] ?? 0) + 1;
  };
  const old = now - 30 * 86400000;
  for (const actor of actors) {
    const { instructions, messages: _messages, activationFilter: _filter, createdAt: _created, updatedAt: _updated, ...summary } = actor;
    add("actorsOperational", `actors/${actor.ownerSessionId}/${actor.id}`, { ...summary,
      instructionsDigest: digest(instructions), instructionsLength: instructions.length });
  }
  let archived = 0;
  const archivedActors = [];
  for (const [status, count] of Object.entries(plan.historicalActorStatuses)) {
    for (let i = 0; i < count; i++, archived++) {
      const id = syntheticId("archived-actor", archived), rootId = rootForHistory(archived);
      archivedActors.push({ id, rootId });
      add("actorsHistorical", `actors/${rootId.slice(8)}/${id}`, { id, rootId, name: `archived-${archived}`, status,
        ownerSessionId: rootId.slice(8), scope: "session", residency: "session", runner: "pi", topics: [], events: [],
        delivery: "mailbox", triggerTurn: false, coalesce: false, responseMode: "text", project: "synthetic-archive",
        instructionsDigest: digest("archived synthetic instructions"), instructionsLength: 31,
        fixtureArchive: { executable: false, capturedAt: old, historicalStatus: status } }, true);
    }
  }
  assert(archivedActors.length > 0);
  const alarms = ["actors/realarm/<id>/<x>", "actors/realarm/<id>"];
  for (const name of alarms) {
    const target = shape(profile, name), published = target.fields["state=published"];
    assert.equal(published + target.fields["state=pending"], target.rows);
    for (let i = 0; i < target.rows; i++) {
      const historical = archivedActors[i % archivedActors.length];
      // Distinct historical IDs avoid collisions with operational realarm slots.
      const id = syntheticId(name, i), suffix = name.endsWith("/<x>") ? "/actor-activation-overflow" : "";
      add("realarmHistorical", `actors/realarm/${id}${suffix}`, { at: old, owner: historical.rootId,
        since: old, slot: `archived-${i}`, state: i < published ? "published" : "pending" }, true);
    }
  }
  const participantShape = shape(profile, "topology/participants/<id>");
  const statuses = Object.entries(participantShape.fields).filter(([key]) => key.startsWith("status="))
    .flatMap(([key, n]) => Array(n).fill(key.slice(7)));
  const kinds = Object.entries(participantShape.fields).filter(([key]) => key.startsWith("kind="))
    .flatMap(([key, n]) => Array(n).fill(key.slice(5)));
  const historicalTargets = [], anomaly = profile.historicalMissingCapabilityRoots;
  for (let i = 0; i < participantShape.rows; i++) {
    let id = `archive:${syntheticId("participant", i)}`, identity;
    const rootId = rootForHistory(i), host = `archive-host:${syntheticId("host", i % shape(profile, "topology/hosts/<id>").rows)}`;
    let value = { format: 1, id, kind: kinds[i] ?? "agent", rootId,
      ownerHostId: host, ownerIdentityId: host, name: `Archived participant ${i}`, status: statuses[i] ?? "unknown",
      runner: "pi", transport: "host", capabilities: [], controlProtocol: "v1", livenessLeaseFiles: 1, startedAt: old, updatedAt: old,
      fixtureArchive: { executable: false, noLiveLease: true } };
    if (anomaly && historicalTargets.length < anomaly.count && value.kind === "root" && value.status === "idle") {
      const index = historicalTargets.length;
      id = rootId;
      // Unknown source names/types are not invented to hit a field count.
      // This conservative synthetic history has NO host-link or live ownership.
      value = { format: 1, id, kind: "root", rootId, name: `Archived missing-capability root ${index}`, status: "idle",
        runner: "pi", transport: "host", capabilities: [], controlProtocol: "v1",
        ownerHostId: `unmatched-historical-host:${syntheticId("unmatched-host", index)}`,
        ownerIdentityId: seedWriter?.id ?? "synthetic-archive-writer",
        startedAt: now - anomaly.payloadStartedAtAgeHours[index] * 3600000,
        updatedAt: now - anomaly.payloadUpdatedAtAgeHours * 3600000 };
      const fields = anomaly.laterSchemaSnapshot?.rowsInThisSnapshotOnly?.[index]?.fieldTypes ?? anomaly.schemaFields;
      if (profile.version >= 4) {
        assert(fields && anomaly.fieldCount === 23, "full historical schema required for v4/v5");
        const sessionId = `historical-${syntheticId("missing-session", index)}`, name = `Archived missing-capability root ${index}`;
        identity = { id, kind: "main", name, sessionId };
        value = { format: 1, id, kind: "root", rootId, name, status: "idle", runner: "pi", transport: "host",
          capabilities: [], controlProtocol: "v1", cwd: `/synthetic/history/${index}`, interactive: false,
          model: "synthetic/offline", thinking: "high", project: "synthetic-history", projectRoot: "/synthetic/history",
          sessionId, remoteHost: `synthetic-remote-${index}`, ownerIncarnation: `historical-${syntheticId("incarnation", index)}`,
          ownerHostId: `unmatched-historical-host:${syntheticId("unmatched-host", index)}`, ownerIdentityId: id,
          startedAt: value.startedAt, updatedAt: value.updatedAt };
        const type = item => Array.isArray(item) ? "list" : typeof item === "string" ? "str" : typeof item === "boolean" ? "bool" : Number.isSafeInteger(item) ? "int" : "unknown";
        assert.deepEqual(Object.fromEntries(Object.keys(value).sort().map(key => [key, type(value[key])])),
          Object.fromEntries(Object.keys(fields).sort().map(key => [key, fields[key]])), "historical schema names/types differ");
      }
      historicalTargets.push({ key: `topology/participants/${digest(id)}`, id, sessionId: value.sessionId,
        payloadSha256: digest(JSON.stringify(value)), payloadCanonicalSha256: canonicalDigest(value),
        actualFieldCount: Object.keys(value).length, sourceFieldCount: anomaly.fieldCount,
        sourceKvUpdatedAtAgeSeconds: anomaly.kvUpdatedAtAgeSeconds?.[index], schemaExact: !!fields && profile.version >= 4,
        sourceUpdatedByKnown: anomaly.updatedByKnown === true, seedWriter: identity ?? seedWriter, seedWriterEqualityExpected: !!(identity ?? seedWriter),
        staticHistory: true, parserAcceptance: "actual native parser acceptance must be observed at a phase boundary",
        assumptions: ["Anonymous source-shape values, not recovered production values or runtime-release proof.",
          "Unmatched synthetic owner host; remoteHost is historical, not a live bridge grant.",
          "No corresponding sessions row. Envelope commit times stay actual; no historical metadata ages are manufactured."] });
    }
    add("participantsHistorical", `topology/participants/${digest(id)}`, value, false, identity);
  }
  if (anomaly) assert.equal(historicalTargets.length, anomaly.count, "historical root allocation unavailable without changing kind/status marginals");
  for (let i = 0; i < shape(profile, "topology/hosts/<id>").rows; i++) {
    const id = `archive-host:${syntheticId("host", i)}`;
    add("hostsHistorical", `topology/hosts/${digest(id)}`, { format: 1, id, rootId: rootForHistory(i),
      identity: { id, name: "Archived synthetic host", kind: "agent" }, startedAt: old, updatedAt: old, expiresAt: old + 1 });
  }
  for (let i = 0; i < shape(profile, "topology/inbox/<id>").rows; i++) {
    const historicalEvent = digest(`archived-inbox-event:${i}`);
    add("inboxHistorical", `topology/inbox/${digest(rootForHistory(i)).slice(0, 32)}`, { after: 0,
      delivered: [historicalEvent], deliveredAt: [old], fallbackAt: [], fallbackThrough: 0, fallbacks: [] });
  }
  for (let i = 0; i < shape(profile, "topology/root-absence/<id>").rows; i++) {
    add("absenceHistorical", `topology/root-absence/${rootForHistory(i)}`, { rootId: rootForHistory(i), firstAbsentAt: old });
  }
  // Claims are not fabricated. Retained seen records name only nonexistent historical targets.
  for (let i = 0; i < shape(profile, "topology/control-seen/<id>").rows; i++) {
    const id = syntheticId("seen", i);
    add("controlSeenHistorical", `topology/control-seen/${id}`, { format: 1, commandId: id, targetId: `archive:${id}`,
      hostId: `archive-host:${id}`, expiresAt: old + 1, explicitDeadline: true });
  }
  const deliveryShape = shape(profile, "residency/deliveries/<id>/<id>");
  for (let i = 0; i < deliveryShape.rows; i++) {
    const rootId = rootForHistory(i), id = syntheticId("delivery", i);
    add("residencyDeliveriesHistorical", `residency/deliveries/${digest(rootId).slice(0, 32)}/${id}`, {
      format: 1, id, rootId, createdAt: old, message: "Archived synthetic delivery; no live recipient.", data: { fixtureArchive: true },
      from: { id: `archive:${id}`, name: "Archived sender", kind: "agent" }, delivery: "followUp", triggerTurn: false, source: "fixture-archive",
    }, true);
  }
  const sessionShape = shape(profile, "sessions/<id>");
  for (let i = 0; i < sessionShape.rows; i++) {
    const id = `archive-${syntheticId("session", i)}`;
    add("sessionsHistorical", `sessions/${id}`, { id, sessionId: id, kind: "peer", local: false, runner: "pi", transport: "host",
      name: `Archived session ${i}`, status: i < sessionShape.fields["status=idle"] ? "idle" : "running",
      pendingMessages: false, startedAt: old, updatedAt: old }, true);
  }
  const budgets = new Map(profile.tables.kv.prefixes.map(row => [row.prefix, row]));
  const budgetBallast = [];
  for (const [prefix, budget] of budgets) {
    const selected = overlays.filter(row => row.key.split("/")[0] === prefix), remaining = budget.rows - selected.length;
    const bytes = selected.reduce((n, row) => n + jsonBytes(row.value), 0);
    if (remaining > 0 && (budget.valueBytes - bytes) / remaining > 65536) {
      // The fixed helper pads archival objects only when filler count is zero.
      // Consume otherwise-oversized filler slots with explicit inert archive
      // budget rows; never pad an operational or historical target participant.
      for (let i = 0; i < remaining; i++) add("archivalBudgetBallast", `${prefix}/fixture-budget-${i}`,
        { fixtureArchive: { executable: false, purpose: "inert prefix byte budget" } }, true);
      budgetBallast.push({ prefix, rows: remaining, reason: "avoid oversized filler; permit archive-only padding" });
    }
  }
  const unique = new Set(overlays.map(row => row.key)); assert.equal(unique.size, overlays.length);
  for (const [prefix, budget] of budgets) {
    const selected = overlays.filter(row => row.key.split("/")[0] === prefix);
    assert(selected.length <= budget.rows, `semantic rows exceed base snapshot budget: ${prefix}`);
    assert(selected.reduce((n, row) => n + jsonBytes(row.value), 0) <= budget.valueBytes, `semantic bytes exceed base snapshot budget: ${prefix}`);
  }
  return { actors, overlays, manifest: { counts, nativeOwners: owners.map(({ label, rootId, sessionId, role, actors }) => ({ label, rootId, sessionId, role, actors })),
    authoritativePreseedGrants: 0, preseedQueuedActivations: 0, budgetBallast,
    historicalRowsExecutable: false, alarmPendingIsHistoricalOnly: true, historicalTargets,
    historicalTargetLimit: "v4/v5: static anonymous 23-field observed names/types and per-row Main authors; v2/v3 retain minimum-shape compatibility. Neither shape nor metadata fidelity proves native parser acceptance. Genuine puts determine envelope times, not source metadata ages. Boundary equality is a candidate, never per-commit proof. No old Mains or forced rewrites.",
    subscriptions: { target: shape(profile, "topology/subscriptions/<id>").rows, preseed: 0, setupViaNativeApi: true },
    historicalRootInboxLedgers: counts.inboxHistorical,
    registryFormat: "format:1 actors envelope, supported inline-instructions input schema; native loader republishes authority",
    limits: ["Marginals are not joint distributions or a replay.", "No historical host has a live lease, process identity, incarnation token or receiver grant.",
      "Historical rows can be reclaimed by production startup; preseed shape and native drift are separate.",
      "Later snapshot commit-outbox counts exceed the earlier mesh prefix budget; outbox is generated by native activity, not forged."] } };
}

export function writeActorRegistries(mesh, owners, actors) {
  const projectRoot = path.join(mesh, "actors"); fs.mkdirSync(projectRoot, { recursive: true, mode: 0o700 });
  freshJson(path.join(projectRoot, "actors.json"), { format: 1, actors: [] });
  for (const owner of owners) {
    const root = path.join(projectRoot, owner.sessionId); fs.mkdirSync(root, { mode: 0o700 });
    const rows = actors.filter(actor => actor.rootId === owner.rootId); assert.equal(rows.length, owner.actors);
    freshJson(path.join(root, "actors.json"), { format: 1, actors: rows });
    // No queue, process, slot-authority, issued-carrier or wake config is seeded.
  }
}

export const fabricSettings = () => ({ fullCodeMode: false, autoReload: false,
  mesh: { enabled: true, announce: true, persist: true, actorScope: "session", stateBackend: "sqlite", transport: "file", hostTransport: "file" },
  mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, entropy: { compile: false },
  speculation: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, components: [],
  approvals: { network: "deny" }, compaction: { engine: "pi" }, agents: { budgetUsd: 0 },
});

export function savedProgramCode() {
  return `
if (!input || typeof input.correlationId !== "string") throw new Error("correlationId required");
if (input.action === "subscribe") {
  const main = await agents.main();
  const ongoing = await agents.subscribe({ from: input.from, to: main.id, events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false });
  const once = await agents.subscribe({ from: input.from, to: main.id, events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false, once: true });
  return { correlationId: input.correlationId, main, subscriptions: [ongoing, once] };
}
if (input.action === "history") {
  const event = await mesh.publish({ topic: input.topic, kind: "fixture.history", text: input.correlationId,
    data: { syntheticCohort: true, correlationId: input.correlationId } });
  return { correlationId: input.correlationId, event };
}
if (input.action === "inbox") {
  const event = await mesh.publish({ topic: "fleet.fixture.cohort", kind: "answer", to: input.to, text: input.correlationId,
    data: { syntheticCohort: true, correlationId: input.correlationId } });
  return { correlationId: input.correlationId, event };
}
if (input.action === "followUp") {
  const receipt = await agents.followUp({ id: input.to, message: input.correlationId, data: { syntheticCohort: true, correlationId: input.correlationId } });
  return { correlationId: input.correlationId, receipt };
}
if (input.action === "unrelated") {
  const event = await mesh.publish({ topic: "fixture.cohort.unrelated", kind: "fixture.scan", text: input.correlationId,
    data: { syntheticCohort: true, correlationId: input.correlationId } });
  return { correlationId: input.correlationId, event };
}
if (input.action === "inspect") {
  const actors = await agents.actors();
  const histories = [];
  for (const actor of actors) histories.push({ actorId: actor.id, messages: await agents.messages({ id: actor.id, limit: 100 }) });
  return { correlationId: input.correlationId, main: await agents.main(), actors, histories,
    participants: await agents.participants({ scope: "project", includeStale: true }), subscriptions: await agents.subscriptions() };
}
throw new Error("unsupported cohort action");`;
}

export const bootstrapCode = () => `const saved = await programs.save({ name: "cohort-driver", code: ${JSON.stringify(savedProgramCode())} }); return { saved, main: await agents.main(), actors: await agents.actors() };`;

// More history events than addressed turns: real actor/history/presence work dominates.
// Weights come from the private profile; no direct KV operations are workload actions.
export function trafficSchedule(plan, owners) {
  const slots = plan.focused ? Math.max(owners.length + 1, Math.min(60, Math.floor(plan.trafficSeconds / 2))) : 60;
  const historyWeight = plan.trafficWeights.find(row => row.shape === "actors/<id>/<id>")?.rows ?? 0;
  const inboxWeight = plan.trafficWeights.find(row => row.shape === "topology/inbox/<id>")?.rows ?? 0;
  assert(historyWeight > 0 && inboxWeight > 0);
  const cadence = plan.trafficSeconds * 1000 / slots;
  const followUpEvery = Math.max(2, Math.min(5, Math.round(historyWeight / inboxWeight)));
  const mains = owners.filter(owner => owner.role !== "residentHost");
  return Array.from({ length: slots }, (_, slot) => {
    const historyOwner = owners[slot % owners.length], target = mains[slot % mains.length];
    // Leave the last full interval quiet for native acknowledgement and cursor commits.
    return { slot, offsetMs: slot * cadence, deadlineOffsetMs: Math.min((slot + 0.8) * cadence, plan.trafficSeconds * 1000 - 100),
      actions: [
        { action: "history", topic: `fixture.cohort.${historyOwner.label.toLowerCase()}`, owner: historyOwner.label, expectedFiltered: historyOwner.actors },
        ...(slot % followUpEvery === 0 ? [{ action: "inbox", to: target.rootId, owner: target.label },
          { action: "followUp", to: target.rootId, owner: target.label }] : []),
        ...(slot % 4 === 0 ? [{ action: "unrelated" }] : []),
      ].map((action, index) => ({ ...action, correlationId: `COHORT_TRAFFIC_${String(slot).padStart(3, "0")}_${index}` })) };
  }).filter(row => row.slot < slots - 1);
}

/** Common-clock barrier accounting; no observer snapshots belong inside a window. */
export function assertCommonWindow(receipts, phase, seconds, expectedLabels) {
  assert.equal(receipts.length, expectedLabels.length);
  assert.deepEqual([...receipts.map(row => row.label)].sort(), [...expectedLabels].sort());
  const windows = receipts.map(row => {
    const window = row.windows[phase]; assert(window?.start && window?.end, `missing ${phase} window`);
    assert(window.end.at >= window.start.at && window.end.monotonicMs - window.start.monotonicMs >= seconds * 1000, "short local window");
    return window;
  });
  const start = Math.max(...windows.map(window => window.start.at)), end = Math.min(...windows.map(window => window.end.at));
  assert(end - start >= seconds * 1000, "common overlap shorter than requested");
  return { start, end, overlapMs: end - start, windows };
}

export function requireReady(receipts, owners, sharedDatabase) {
  assert.equal(receipts.length, owners.length, "all native processes must be ready");
  assert.equal(new Set(receipts.map(row => row.pid)).size, owners.length, "native PIDs must be distinct");
  for (const owner of owners) {
    const receipt = receipts.find(row => row.label === owner.label);
    assert(receipt?.ready && receipt.native?.sessionId === owner.sessionId, `native identity not ready: ${owner.label}`);
    assert.equal(receipt.native.rootId, owner.rootId);
    assert.equal(receipt.actors.length, owner.actors, `actor registry not loaded: ${owner.label}`);
    assert(receipt.actors.every(actor => actor.status === "idle" && actor.rootId === owner.rootId && !actor.queued && !actor.preparing && !actor.inFlight));
    assert(receipt.publications.every(row => row.ownerHostId && row.ownerIdentityId && !row.stale), "publication needs current native lease");
    assert(receipt.publications.length >= owner.actors + (owner.role === "residentHost" ? 0 : 1));
    assert(receipt.stores.some(store => store.backend === "sqlite" && sameDatabase(store.databaseIdentity, sharedDatabase)), "shared DB identity mismatch");
    if (owner.role === "residentHost") assert(receipt.canConsumeMesh && receipt.noWakeConfig, "resident is not legitimately warm");
    else assert(receipt.native.trusted && receipt.native.mode === "rpc" && receipt.native.sessionFile === owner.sessionFile);
  }
  return true;
}

export function eventTarget(profile) {
  const target = profile.eventLogTarget ?? profile.eventLog ?? profile.events;
  const lines = target?.sourceLines ?? target?.lines, bytes = target?.eventBytes ?? target?.sourceBytes ?? target?.bytes;
  assert(Number.isSafeInteger(lines) && Number.isSafeInteger(bytes), "EVENT_PROFILE_REQUIRED: retained-log lines/bytes required");
  integer(lines, 1, 1000000, "retained event lines"); integer(bytes, lines * 128, 60000000, "retained event bytes");
  const lineBytes = target.eventLineBytes ?? Math.ceil(bytes / lines);
  integer(lineBytes, 512, 65535, "bounded event line bytes");
  return { lines, bytes, lineBytes };
}

export function selectCandidates(owners, candidate, legacyCandidate, legacyMains = 0) {
  assert(!legacyCandidate && legacyMains === 0, "OLD_MAIN_SCOPE_SUPERSEDED: current native Mains only");
  return owners.map(owner => ({ ...owner, candidate, legacy: false }));
}

export function validateChildExit(exit, stopping) {
  assert(exit && Number.isSafeInteger(exit.pid));
  if (!stopping) throw new Error(`native child exited before termination: ${exit.label} (${exit.code}/${exit.signal})`);
  assert.equal(exit.code, 0, "native child did not exit cleanly"); assert.equal(exit.signal, null, "native child needed escalation");
}
