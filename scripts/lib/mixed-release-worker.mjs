// Private JSONL worker for mixed-release-canary. No source imports or fabricated presence.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { pathToFileURL, fileURLToPath } from "node:url";

const hash = value => createHash("sha256").update(value).digest("hex");
const hostKey = id => `topology/hosts/${hash(id)}`;
const participantKey = id => `topology/participants/${hash(id)}`;
const MAX_BYTES = 256 * 1024, MAX_READ = 10_000;

/** Internal classes are named exports of split chunks, NOT dist/<source-path>.js. */
export async function loadReleaseSymbols(release) {
  const dir = path.join(release, "dist/chunks");
  const wanted = ["MeshStore", "SqliteStateStore", "ParticipantDirectory", "FabricControlPlane"];
  const found = new Map();
  for (const name of fs.readdirSync(dir).filter(name => name.endsWith(".js")).sort()) {
    const file = path.join(dir, name), text = fs.readFileSync(file, "utf8");
    const exports = [...text.matchAll(/export\s*\{([^}]+)\}/g)].map(match => match[1]).join(",");
    for (const symbol of wanted) {
      if (!new RegExp(`(?:var|let|const)\\s+${symbol}\\s*=\\s*class\\b|class\\s+${symbol}\\b`).test(text)) continue;
      const exported = exports.split(",").map(value => value.trim()).find(value => new RegExp(`^${symbol}(?:\\s+as\\s+\\w+)?$`).test(value));
      if (!exported) continue;
      if (found.has(symbol)) throw new Error(`ambiguous compiled ${symbol} in ${release}`);
      found.set(symbol, { file, key: exported.split(/\s+as\s+/)[1] ?? symbol, sha256: hash(text) });
    }
  }
  const symbols = {}, artifacts = {};
  for (const symbol of wanted) {
    const match = found.get(symbol);
    if (!match) throw new Error(`missing exported compiled class ${symbol} in ${release}`);
    symbols[symbol] = (await import(pathToFileURL(match.file).href))[match.key];
    if (typeof symbols[symbol] !== "function") throw new Error(`invalid compiled ${symbol}`);
    artifacts[symbol] = { path: match.file, sha256: match.sha256 };
  }
  return { ...symbols, artifacts };
}

async function main() {
  let mesh, directory, control, identity, native, artifacts, stopping;
  const expected = new Map(), entries = new Map(), sent = [];
  let invalid = 0, duplicateCalls = 0;
  const events = topic => mesh.read({ topic, after: 0, limit: MAX_READ });
  const presence = ids => ids.map(id => {
    const host = mesh.get(hostKey(id), { fresh: true })?.value ?? null;
    const leaseFile = path.join(mesh.root, "host-leases", `${hash(id).slice(0, 32)}.json`);
    let lease = null;
    try { lease = JSON.parse(fs.readFileSync(leaseFile, "utf8")); } catch (error) { if (error.code !== "ENOENT") throw error; }
    const participant = directory.get(id, Date.now(), { fresh: true }) ?? null;
    const row = mesh.get(participantKey(id), { fresh: true })?.value ?? null;
    const owner = directory.mirroredControlOwner(id, id, id) ?? null;
    return { id, host, lease, leaseFile, participant, row, owner };
  });
  const stop = () => stopping ??= (async () => {
    await control?.close();
    await directory?.close();
    mesh?.closeState();
  })();
  const dispatch = async input => {
    switch (input.op) {
      case "start": {
        if (mesh) throw new Error("worker already started");
        const loaded = await loadReleaseSymbols(input.release);
        artifacts = loaded.artifacts;
        loaded.SqliteStateStore.openSync(input.mesh, MAX_BYTES, MAX_READ, { initialize: "create" }).close();
        const storeOptions = { stateBackend: "sqlite", lockProtocol: 3, lockTimeoutMs: 20_000, staleLockMs: 60_000 };
        // The historical 41a API rejects protocol 3 before doing any I/O. Its own
        // bridge also only supports 1|2; do not pretend it spoke protocol 3.
        try { mesh = new loaded.MeshStore(input.mesh, MAX_BYTES, MAX_READ, storeOptions); }
        catch (error) {
          if (!/mesh\.lockProtocol must be 1 or 2/.test(error.message)) throw error;
          mesh = new loaded.MeshStore(input.mesh, MAX_BYTES, MAX_READ, { ...storeOptions, lockProtocol: 2 });
        }
        identity = { id: input.rootId, name: "main", kind: "main", sessionId: input.name };
        directory = new loaded.ParticipantDirectory(mesh, {
          enabled: true, hostId: identity.id, rootId: identity.id, identity,
          heartbeatMs: 1_000, leaseMs: 15_000, reapDeadHosts: false,
        });
        // There is no public directory.upsert in these releases. registerSource + start/refresh
        // executes its real fenced host/lease + root participant upsert path.
        const startedAt = Date.now();
        directory.registerSource(() => [directory.root({ id: identity.id, sessionId: input.name,
          status: "idle", startedAt, updatedAt: Date.now(), pendingMessages: 0 }, true, input.name)]);
        await directory.start();
        await directory.refresh();
        const [readback] = presence([identity.id]);
        if (!readback.host || !readback.lease || !readback.participant || !readback.row ||
            readback.host.remoteHost !== undefined || readback.lease.expiresAt <= Date.now() ||
            readback.row.ownerIncarnation !== directory.ownerIncarnation) throw new Error("native registration readback failed");
        native = { id: identity.id, name: input.name, ownerIncarnation: directory.ownerIncarnation };
        control = new loaded.FabricControlPlane(mesh, identity, {
          enabled: true, hostId: identity.id, ownerIncarnation: directory.ownerIncarnation,
          acknowledgementTimeoutMs: 20_000, bridgeTimeoutMs: 20_000,
          readMirroredOwner: (...args) => directory.mirroredControlOwner(...args),
        });
        control.start((command, from, _signal, verification) => {
          const marker = command.data?.marker, plan = expected.get(marker);
          if (!plan || verification !== "bridge" || from.id !== plan.from || command.targetId !== identity.id ||
              command.operation !== "followUp" || command.message !== marker) {
            invalid++;
            return { accepted: false, error: "unauthenticated or unexpected canary followUp", notRun: true };
          }
          const previous = entries.get(marker);
          if (previous) duplicateCalls++;
          entries.set(marker, { marker, from: from.id, targetId: command.targetId,
            commandId: command.commandId, verification, calls: (previous?.calls ?? 0) + 1, acceptedAt: Date.now() });
          return { accepted: true, messageId: `ack:${marker}`, triggered: true, result: { marker } };
        });
        return { native, readback, artifacts, lockProtocol: mesh.lockProtocol, stateBackend: mesh.stateBackend };
      }
      case "expect":
        for (const plan of input.messages) {
          if (expected.has(plan.marker)) throw new Error("duplicate expectation");
          expected.set(plan.marker, plan);
        }
        return { expected: expected.size };
      case "presence": return presence(input.ids);
      case "waitPresence": {
        // Filesystem notifications carry cross-process progress; a 2s bounded safety net
        // covers missed/coalesced WAL notifications. The timer never grants presence.
        const ready = rows => rows.every((row, index) => {
          const condition = input.conditions[index];
          if (condition.absent) return row.host === null && row.lease === null;
          return row.host && row.lease && row.participant && row.row &&
            (condition.remote === undefined || row.owner?.remoteHost === condition.remote) &&
            (condition.token === undefined || row.host.incarnationToken === condition.token && row.lease.incarnationToken === condition.token) &&
            (!condition.tokenless || row.host.incarnationToken === undefined && row.lease.incarnationToken === undefined);
        });
        return await new Promise((resolve, reject) => {
          let watcher, timer, safety, finished = false, last;
          const finish = (error, rows) => {
            if (finished) return;
            finished = true; watcher?.close(); clearTimeout(timer); clearInterval(safety);
            error ? reject(error) : resolve(rows);
          };
          const check = () => {
            try { last = presence(input.ids); if (ready(last)) finish(null, last); }
            catch (error) { finish(error); }
          };
          watcher = fs.watch(mesh.root, { recursive: true }, check);
          watcher.on("error", error => finish(error));
          timer = setTimeout(() => finish(new Error(`presence deadline: ${JSON.stringify(last)}`)), 25_000);
          safety = setInterval(check, 2_000);
          check();
        });
      }
      case "send": {
        const { target, marker } = input;
        const owner = directory.mirroredControlOwner(target.id, target.id, target.id);
        if (owner?.remoteHost !== target.name || directory.ownerIncarnation !== undefined && owner.ownerIncarnation !== target.ownerIncarnation)
          throw new Error(`no authentic mirrored owner for ${target.name}`);
        const receipt = await control.request(target.id, target.id, "followUp", {
          message: marker, data: { marker }, ownerIncarnation: owner.ownerIncarnation,
        }, target.id, { routedRemoteHost: owner.remoteHost, idempotencyKey: `mixed-release:${marker}`, timeoutMs: 20_000 });
        const commands = events("fabric.control.command").filter(event => event.from.id === identity.id &&
          event.to === target.id && event.data?.data?.marker === marker);
        const commandIds = [...new Set(commands.map(event => event.data.commandId))];
        const acks = events("fabric.control.ack").filter(event => commandIds.includes(event.data?.commandId) &&
          event.to === identity.id && event.from.id === target.id && event.verification === "bridge" &&
          event.data?.targetId === target.id && event.data?.messageId === `ack:${marker}` && event.data?.accepted === true);
        if (!receipt.acknowledged || receipt.routed !== "mesh" || receipt.messageId !== `ack:${marker}` ||
            commandIds.length !== 1 || acks.length !== 1) throw new Error(`uncorrelated or unauthenticated ACK: ${marker}`);
        const result = { marker, target: target.id, commandId: commandIds[0], receipt,
          ack: { id: acks[0].id, from: acks[0].from.id, to: acks[0].to, verification: acks[0].verification,
            commandId: acks[0].data.commandId, bridge: acks[0].data.bridge }, at: Date.now() };
        sent.push(result);
        return result;
      }
      case "historyFollowUp": {
        // No harness admission check: the public compiled API owns route/epoch fencing.
        const { targetId, marker } = input;
        const observed = presence([targetId])[0];
        const actualOwner = directory.mirroredControlOwner(targetId, targetId, targetId) ?? null;
        const beganAt = Date.now(), began = performance.now();
        let receipt, error;
        try {
          receipt = await control.request(targetId, targetId, "followUp", {
            message: marker, data: { marker }, ownerIncarnation: actualOwner?.ownerIncarnation,
          }, targetId, { routedRemoteHost: actualOwner?.remoteHost,
            idempotencyKey: `mixed-release-history:${marker}`, timeoutMs: 20_000 });
        } catch (caught) {
          // Serialize only the actual request rejection, never an RPC/precheck failure.
          error = { source: "FabricControlPlane.request", name: caught.name, code: caught.code,
            message: caught.message, stack: caught.stack, notRun: caught.notRun, notDelivered: caught.notDelivered };
        }
        const endedAt = Date.now(), elapsedMs = performance.now() - began;
        const commandEvents = events("fabric.control.command").filter(event => event.from.id === identity.id &&
          event.to === targetId && event.data?.data?.marker === marker);
        const ids = new Set(commandEvents.map(event => event.data.commandId));
        return { marker, source: identity.id, intendedTarget: targetId, actualOwner, observed,
          publicRequestInvoked: true, beganAt, endedAt, elapsedMs, timeoutMs: 20_000, ackGraceMs: 15_000,
          ...(error ? { error } : { receipt }), commandEvents,
          ackEvents: events("fabric.control.ack").filter(event => ids.has(event.data?.commandId)) };
      }
      case "tally": return { native, expected: [...expected.keys()], entries: [...entries.values()], invalid, duplicateCalls, sent,
        commandEvents: events("fabric.control.command"), ackEvents: events("fabric.control.ack") };
      case "stop": await stop(); return { stopped: true };
      default: throw new Error(`unknown worker operation: ${input.op}`);
    }
  };
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  lines.on("line", line => {
    let input;
    try { input = JSON.parse(line); } catch { process.stderr.write("invalid JSONL request\n"); return; }
    void dispatch(input).then(result => {
      process.stdout.write(`${JSON.stringify({ id: input.id, ok: true, result })}\n`);
      if (input.op === "stop") { lines.close(); process.stdin.destroy(); }
    }, error => process.stdout.write(`${JSON.stringify({ id: input.id, ok: false, error: error.stack ?? String(error) })}\n`));
  });
  lines.once("close", () => { void stop().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; }); });
  for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => {
    void stop().finally(() => { lines.close(); process.stdin.destroy(); });
  });
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; });
}
