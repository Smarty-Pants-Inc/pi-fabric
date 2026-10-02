// smarty-dev#2045: Dev1 -> actual Forge, using existing installed B65 artifacts/auth.
// Run with bun through forge-ssh-bridge.sh. Never creates a mesh, profile, key or sshd.
// REQUIRED JSON (absolute paths, existing meshes and host-owned agent profiles):
// { localMesh, remoteMesh, localFabric, remoteFabric, localAgentDir, remoteAgentDir,
//   localCwd, remoteCwd, evidenceRoot, remoteEvidenceRoot, bridgeKey, bridgeKnownHosts }
// localFabric/remoteFabric are the exact installed B65 dist/index.js paths. The
// local installation also supplies dist/mesh.js and bin/mesh-bridge. Both evidence
// roots must contain a .local path component; a unique run subdirectory is created.
// Owner/Light must preprepare each own cwd's .pi/fabric.json with mesh.root
// exactly equal to the corresponding absolute mesh path; this driver only reads it.
// OPTIONAL: localName="dev1", remoteName="forge", localPi="pi",
// remotePi="forge-pi", remoteNode="node", localNode="node",
// sshHost="forge-agent" (ordinary host-owned SSH profile, NOT the forced key),
// bridgeHost="paul@100.78.65.112", bridgePort=22,
// bridgeBin=<local install>/bin/mesh-bridge, model="cliproxyapi/gpt-6.1-sol".
// Ordinary SSH always uses BatchMode, strict host checking and a connect deadline.
// The bridge key must ALREADY force the installed remote mesh-bridge agent on
// remoteMesh with --peer <localName>. No auth file is opened, copied or linked here.
// For supplemental RYZEN2, set remoteName, sshHost, bridgeHost, remotePi and ALL
// remote paths explicitly. A supplemental PASS is not exact Forge acceptance.
// This is an intrusive opt-in proof: one fresh nice19 Pi per host, real mesh writes,
// one short-lived bridge; no existing bridge to Forge may already be running.
// Logs contain ONLY these fresh RPC sessions, never other agents' transcripts.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, execFileSync } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";

// BEGIN WORK INGRESS HELPER — extracted by .local/work-wake-probe.mjs without running this driver.
const workInboxIngress = (messages, expected) => {
  const xml = (s) => s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  for (const message of messages) {
    // Only the native custom message at the RPC message's top level counts.
    if (message?.role !== "custom" || message.customType !== "pi-fabric-inbox") continue;
    const text = typeof message.content === "string" ? message.content :
      Array.isArray(message.content) && message.content.every((c) => c.type === "text" && typeof c.text === "string")
        ? message.content.map((c) => c.text).join("") : "";
    const envelope = /^<fabric-inbox count="(\d+)">\nWork events addressed to you that no steer or follow-up delivered \(a shadow copy can repeat a message you already saw\):\n([\s\S]*)\n<\/fabric-inbox>$/.exec(text);
    if (!envelope) continue;
    // Bodies from rootInboxMessage are XML-escaped. Raw nested tags are never native events.
    const events = [...envelope[2].matchAll(/(?:^|\n)<event ([^<>\n]+)>([^<]*)<\/event>(?=\n|$)/g)];
    if (events.length !== Number(envelope[1]) || events.map((m) => m[0].replace(/^\n/, "")).join("\n") !== envelope[2]) continue;
    for (const [, header, body] of events) {
      const attrs = {}; let remainder = header;
      while (remainder) {
        const attr = /^([a-z_]+)=("(?:[^"\\]|\\.)*")(?: |$)/.exec(remainder);
        if (!attr || Object.hasOwn(attrs, attr[1])) break;
        try { attrs[attr[1]] = JSON.parse(attr[2]); } catch { break; }
        remainder = remainder.slice(attr[0].length);
      }
      if (remainder || typeof attrs.id !== "string" || !attrs.id || !/^\d+$/.test(attrs.sequence ?? "") || !Number.isFinite(Date.parse(attrs.at))) continue;
      if ((expected.id !== undefined && attrs.id !== expected.id) || attrs.from_id !== expected.from.id || attrs.from_name !== expected.from.name ||
          attrs.topic !== xml(expected.topic) || attrs.kind !== xml(expected.kind) || attrs.key !== expected.key || body !== xml(expected.text)) continue;
      return { role: message.role, customType: message.customType, eventId: attrs.id, header, senderId: attrs.from_id,
        topic: attrs.topic, kind: attrs.kind, key: attrs.key, body };
    }
  }
  return undefined;
};
// END WORK INGRESS HELPER

// BEGIN FIRST SEND VALIDATOR — inert probes extract this production predicate.
const assertFirstSend = (attempts, target, host, killedAt, boundMs = 10_000) => {
  const fail = (message) => { throw new Error(`FIRST postkill send to ${target} on ${host}: ${message}`); };
  if (!Array.isArray(attempts) || !attempts.length) fail("missing first attempt");
  const first = attempts[0]; // A later named lapse can never rescue this attempt.
  if (!first || Object.hasOwn(first, "delivered") || Object.hasOwn(first, "accepted") || typeof first.error !== "string") fail("accepted/delivered or missing native failure");
  const { requestAt, nativeStartedAt, nativeCompletedAt, completedAt, nativeMs } = first;
  if (![killedAt, requestAt, nativeStartedAt, nativeCompletedAt, completedAt, nativeMs, boundMs].every(Number.isFinite) ||
      boundMs !== 10_000 || requestAt < killedAt || completedAt < requestAt || nativeCompletedAt < nativeStartedAt ||
      nativeMs !== nativeCompletedAt - nativeStartedAt || nativeMs > boundMs)
    fail("missing/invalid timing or native operation over 10000ms");
  const error = first.error;
  // Trust only the full native router clause, never a target prefix or appended diagnostic.
  const preSendPrefix = `Unknown Fabric participant: ${target} (its lease mirrored from remote host ${host} lapsed`;
  const remainder = error.slice(preSendPrefix.length);
  const clause = /^(?: (\d+) s ago)?: the mesh bridge to that host is down, or the session has ended\)$/.exec(remainder);
  const preSend = error.startsWith(preSendPrefix) && clause !== null && clause[0] === remainder &&
    (clause[1] === undefined || Number.isFinite(Number(clause[1])));
  const pending = error.startsWith(`Fabric lease mirrored from remote host ${host} lapsed for ${target};`) && error.includes("mesh bridge");
  const deadline = error.startsWith(`Fabric mesh bridge to remote host ${host} is not responding for ${target};`);
  const unpublished = error === `Fabric mesh bridge routing to remote host ${host} is unavailable for ${target}; the routed owner could not be revalidated; this attempt was not published.`;
  if (!preSend && !unpublished && !pending && !deadline) fail("unnamed or wrong host/target bridge condition");
  if ((pending || deadline) && !error.includes("the outcome is unknown")) fail("pending/deadline failure lacks unknown-outcome warning");
  return { condition: unpublished ? "pre-send-unpublished" : preSend ? "pre-send-lapsed" : pending ? "pending-lapsed" : "ack-deadline",
    nativeTimestampDomain: "native-execution-host-local", nativeStartedAt, nativeCompletedAt, nativeMs,
    driverTimestampDomain: "local-driver", requestAfterKillMs: requestAt - killedAt,
    completionAfterKillMs: completedAt - killedAt, rpcModelDurationMs: completedAt - requestAt };
};
// END FIRST SEND VALIDATOR

// BEGIN POSTKILL LEDGER — bounded actual-store read, cross-checked against its fresh log suffix.
const validateLedgerEvent = (e, senderFindings = []) => {
  const object = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
  const string = (v) => typeof v === "string" && v.trim().length > 0;
  const optional = (v, key, test) => !Object.hasOwn(v, key) || test(v[key]);
  if (!object(e) || !string(e.id) || !string(e.topic) || !string(e.kind) ||
      !Number.isSafeInteger(e.sequence) || e.sequence <= 0 || !Number.isFinite(e.createdAt) ||
      !optional(e, "to", (v) => typeof v === "string") || !optional(e, "text", (v) => typeof v === "string"))
    throw new Error("malformed postkill ledger envelope");
  const validSender = object(e.from) && string(e.from.id) && string(e.from.name) &&
    ["main", "actor", "agent"].includes(e.from.kind) && optional(e.from, "sessionId", (v) => typeof v === "string");
  // Narrow authorization: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2625.
  // ops.owner carries this proof's pr.wake; a pr.wake kind is strict on any topic.
  const ownTopic = ["fabric.control.command", "fabric.control.ack", "pr.wake", "ops.owner",
    "fleet.work.smarty-dev.2045"].includes(e.topic) || e.kind === "pr.wake";
  if (!validSender) {
    if (ownTopic) throw new Error("malformed postkill ledger sender");
    // Metadata only: never preserve sender or body. Do not skip command/schema checks.
    senderFindings.push({ eventId: e.id, topic: e.topic });
  }
  if (e.topic === "fabric.control.command") {
    const d = e.data;
    if (!object(d) || d.version !== 1 || !string(d.commandId) || !string(d.targetId) || !string(d.replyTo) ||
        !["steer", "followUp", "stop", "ask", "cancel"].includes(d.operation) || e.kind !== d.operation || !string(e.to) ||
        !Number.isFinite(d.requestedAt) || !optional(d, "deadlineAt", Number.isFinite) ||
        !optional(d, "message", (v) => typeof v === "string") || !optional(d, "triggerTurn", (v) => typeof v === "boolean") ||
        !optional(d, "cancelCommandId", string) || !optional(d, "destinationRemoteHost", (v) => v === null || string(v)) ||
        (["steer", "followUp", "ask"].includes(d.operation) && typeof d.message !== "string") ||
        (d.operation === "cancel" && !string(d.cancelCommandId)) ||
        !optional(d, "binding", (v) => object(v) && optional(v, "model", (x) => typeof x === "string") &&
          optional(v, "thinking", (x) => typeof x === "string")))
      throw new Error("malformed postkill ledger command");
  }
  return e;
};
const captureLedger = (store) => {
  const file = path.join(store.root, "events.jsonl"), fd = fs.openSync(file, "r");
  try {
    const stat = fs.fstatSync(fd);
    const sameFile = (next) => ["dev", "ino", "size", "mtimeMs", "ctimeMs"].every((key) => next[key] === stat[key]);
    const recheck = () => {
      if (!sameFile(fs.fstatSync(fd)) || !sameFile(fs.statSync(file))) throw new Error("postkill ledger changed during baseline capture");
    };
    // Last committed whole record, not the sequence allocator: bounded even on a fleet log.
    const bytes = Math.min(stat.size, 4 * 1024 * 1024), buffer = Buffer.alloc(bytes);
    if (fs.readSync(fd, buffer, 0, bytes, stat.size - bytes) !== bytes) throw new Error("short postkill ledger baseline read");
    let after = 0; const senderFindings = [];
    if (bytes) {
      if (buffer[bytes - 1] !== 10) throw new Error("incomplete postkill ledger baseline record");
      const previous = buffer.lastIndexOf(10, bytes - 2);
      if (previous < 0 && bytes !== stat.size) throw new Error("postkill ledger baseline record exceeds 4MiB");
      const tail = buffer.subarray(previous + 1, bytes - 1), text = tail.toString("utf8");
      if (!text || !Buffer.from(text, "utf8").equals(tail)) throw new Error("malformed postkill ledger baseline record");
      after = validateLedgerEvent(JSON.parse(text), senderFindings).sequence;
    }
    recheck();
    return { file, device: stat.dev, inode: stat.ino, offset: stat.size, after, senderFindings };
  } finally { fs.closeSync(fd); }
};
const readLedger = (store, start, senderFindings = []) => {
  if (!Number.isSafeInteger(start.after) || start.after < 0 || !Number.isSafeInteger(start.offset) || start.offset < 0)
    throw new Error("invalid postkill ledger baseline");
  const fd = fs.openSync(start.file, "r");
  let events, actual;
  try {
    const stat = fs.fstatSync(fd), bytes = stat.size - start.offset;
    const sameFile = (next) => ["dev", "ino", "size", "mtimeMs", "ctimeMs"].every((key) => next[key] === stat[key]);
    const recheck = () => {
      if (!sameFile(fs.fstatSync(fd)) || !sameFile(fs.statSync(start.file)))
        throw new Error("postkill ledger changed during raw/store read");
    };
    if (stat.dev !== start.device || stat.ino !== start.inode || bytes < 0 || bytes > 4 * 1024 * 1024)
      throw new Error("postkill ledger rotated/truncated or exceeded 4MiB read bound");
    const buffer = Buffer.alloc(bytes);
    if (fs.readSync(fd, buffer, 0, bytes, start.offset) !== bytes) throw new Error("short postkill ledger read");
    const text = buffer.toString("utf8");
    if (!Buffer.from(text, "utf8").equals(buffer)) throw new Error("invalid UTF-8 postkill ledger");
    if (text && !text.endsWith("\n")) throw new Error("incomplete postkill ledger record");
    events = text ? text.slice(0, -1).split("\n").map((line) => {
      if (!line.trim()) throw new Error("blank postkill ledger record");
      return validateLedgerEvent(JSON.parse(line), senderFindings);
    }) : [];
    const ids = new Set(); let sequence = start.after;
    for (const e of events) {
      if (e.sequence <= sequence || ids.has(e.id)) throw new Error("postkill ledger stale/nonincreasing sequence or duplicate ID");
      sequence = e.sequence; ids.add(e.id);
    }
    recheck();
    const expected = events.filter((e) => e.sequence > start.after && e.topic === "fabric.control.command");
    actual = store.read({ after: start.after, topic: "fabric.control.command", limit: 500 });
    recheck();
    if (!Array.isArray(actual) || actual.length >= 500 || !isDeepStrictEqual(actual, expected))
      throw new Error("postkill actual-store ledger incomplete, changed, or saturated");
  } finally { fs.closeSync(fd); }
  return actual;
};
const assertPostkillLedger = (events, source, target, marker, host, condition, senderFindings = []) => {
  const fail = (why) => { throw new Error(`postkill ledger ${source} -> ${target}: ${why}`); };
  if (!Array.isArray(events)) fail("unreadable ledger");
  // Validate every typed command before attribution/marker filtering.
  for (const event of events) validateLedgerEvent(event);
  const own = events.filter((e) => e.topic === "fabric.control.command" && e.from?.id === source && e.data?.targetId === target);
  const sends = own.filter((e) => e.data?.operation === "steer" && e.data?.message === marker);
  const cancels = own.filter((e) => e.data?.operation === "cancel");
  const ids = [...new Set(sends.map((e) => e.data.commandId))];
  if (sends.length > 1 || ids.length > 1) fail("second steer publication/replay");
  const unpublished = condition === "pre-send-unpublished" || condition === "pre-send-lapsed";
  if (unpublished ? sends.length !== 0 : sends.length !== 1) fail("publication count disagrees with native failure");
  if (cancels.length > 1) fail("second cancellation publication");
  const send = sends[0];
  if (send && (typeof send.data.commandId !== "string" || !send.data.commandId || !send.to ||
      send.kind !== "steer" || send.data.destinationRemoteHost !== host)) fail("missing/wrong original destination binding");
  for (const cancel of cancels) {
    if (!send || cancel.kind !== "cancel" || cancel.data.cancelCommandId !== send.data.commandId ||
        cancel.to !== send.to || cancel.data.destinationRemoteHost !== send.data.destinationRemoteHost ||
        typeof cancel.data.commandId !== "string" || !cancel.data.commandId || cancel.data.commandId === send.data.commandId)
      fail("cancellation not bound to the same command/destination");
  }
  return { source, target, marker, host, condition, inspectedAt: Date.now(), examinedCommandRecords: events.length,
    unrelatedSenderFindings: { issue: "Smarty-Pants-Inc/smarty-dev#2625", count: senderFindings.length,
      events: senderFindings.map(({ eventId, topic }) => ({ eventId, topic })) },
    steerPublications: sends.length, commandIds: ids,
    steerEventIds: sends.map((e) => e.id), cancellationPublications: cancels.length,
    cancellations: cancels.map((e) => ({ eventId: e.id, commandId: e.data.commandId, cancelCommandId: e.data.cancelCommandId,
      to: e.to, destinationRemoteHost: e.data.destinationRemoteHost })),
    destination: send ? { to: send.to, destinationRemoteHost: send.data.destinationRemoteHost } : null };
};
// END POSTKILL LEDGER

// BEGIN EXCLUSIVE LEDGER — serialize only proof snapshots, never mutate mesh state.
const exclusiveLedger = async (store, operation, gate, wait) => {
  gate();
  const deadline = Date.now() + 5000;
  const check = () => {
    gate();
    if (Date.now() >= deadline) throw new Error("postkill ledger snapshot exceeded 5000ms");
  };
  // exclusive takes a synchronous callback. IO cannot be preempted: reject an
  // over-budget result after IO, and gate a delayed acquisition before any read.
  const value = await wait(store.exclusive(() => {
    check();
    const value = operation();
    check();
    return value;
  }));
  check();
  return value;
};
// END EXCLUSIVE LEDGER

const cleanupSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const quote = (s) => `'${String(s).replaceAll("'", "'\\''")}'`;
const shell = (args) => args.map(quote).join(" ");
const assert = (value, message) => { if (!value) throw new Error(message); };
const runId = `mesh-proof-${randomUUID()}`;
const children = new Set();
const results = { runId, startedAt: new Date().toISOString(), checks: {}, status: "FAIL" };
let cfg, evidence, remoteOwned, bridge, bridgeSsh, remoteBridge, piA, piB, ownershipMonitor;
let cleanupPromise;
const remoteOwners = new Map();
// BEGIN FORGE CANCELLATION — inert probes extract the production wiring.
let stopping = false;
const pending = new Set();
const gate = () => { assert(!stopping, "proof interrupted"); };
const stop = () => {
  stopping = true;
  for (const cancel of [...pending]) cancel(new Error("proof interrupted"));
};
const interrupt = () => {
  results.status = "FAIL";
  results.interrupted = true;
  process.exitCode = 1;
  results.failed ??= "proof interrupted by SIGTERM/SIGINT";
  stop(); save();
};
const cancelWait = (operation) => {
  gate();
  return new Promise((resolve, reject) => {
    const cancel = (error) => { pending.delete(cancel); reject(error); };
    pending.add(cancel);
    Promise.resolve(operation).then((value) => {
      pending.delete(cancel);
      try { gate(); resolve(value); } catch (error) { reject(error); }
    }, cancel);
  });
};
const sleep = (ms) => {
  gate();
  return new Promise((resolve, reject) => {
    const cancel = (error) => { clearTimeout(timer); pending.delete(cancel); reject(error); };
    const timer = setTimeout(() => { pending.delete(cancel); try { gate(); resolve(); } catch (error) { reject(error); } }, ms);
    pending.add(cancel);
  });
};
const finalStatus = (status, interrupted, errors) => status === "PASS" && !interrupted && errors.length === 0 ? "PASS" : "FAIL";
// END FORGE CANCELLATION
const save = () => { if (evidence) fs.writeFileSync(path.join(evidence, "results.json"), `${JSON.stringify(results, null, 2)}\n`, { mode: 0o600 }); };
const check = (name, value) => { gate(); results.checks[name] = value; save(); };
const waitFor = async (what, predicate, ms = 60_000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    assert(!stopping, "proof interrupted");
    const value = await cancelWait(predicate()); // Cancel now; never resume proof work after stop.
    gate();
    if (value) return value;
    await sleep(1000);
  }
  throw new Error(`deadline waiting for ${what}`);
};
const sshArgs = () => ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10",
  "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2", cfg.sshHost];
// BEGIN REMOTE HELPER — inert native probe extracts this block, not the driver.
const remoteBuffered = (command, cleanupOnly, deadline = Date.now() + 20_000) => {
  const timeoutMs = Math.min(20_000, deadline - Date.now());
  assert(timeoutMs > 0, "remote helper cleanup deadline exceeded");
  // execFile silently drops detached; spawn is also the regular child() mechanism.
  const transport = spawn("ssh", [...sshArgs(), command], { detached: true, stdio: ["ignore", "pipe", "pipe"] });
  children.add(transport); // Every real child remains in the existing cleanup ledger.
  let resolveReady, rejectReady, resolveClosed;
  const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  transport.proofClosed = new Promise((resolve) => { resolveClosed = resolve; });
  const operation = new Promise((resolve, reject) => {
    const chunks = { stdout: [], stderr: [] }, bytes = { stdout: 0, stderr: 0 };
    let settled = false, finalized = false, timer;
    const done = (error, value) => {
      if (settled) return;
      settled = true; pending.delete(cancel);
      transport.stdout?.removeListener("data", onStdout);
      transport.stderr?.removeListener("data", onStderr);
      // Wait settlement is NOT lifetime finalization or a group-death receipt.
      // Drain without collecting; retain errors, close and the direct-child bound.
      transport.stdout?.resume(); transport.stderr?.resume();
      chunks.stdout.length = 0; chunks.stderr.length = 0;
      if (error) { rejectReady(error); reject(error); } else resolve(value);
    };
    const finalize = () => {
      if (finalized) return;
      finalized = true; clearTimeout(timer); pending.delete(cancel);
      transport.removeListener("spawn", onSpawn);
      transport.removeListener("error", onError);
      transport.removeListener("close", onClose);
      transport.stdout?.removeListener("data", onStdout);
      transport.stderr?.removeListener("data", onStderr);
      transport.stdout?.removeListener("error", onStreamError);
      transport.stderr?.removeListener("error", onStreamError);
      chunks.stdout.length = 0; chunks.stderr.length = 0;
      resolveClosed(); // Actual child closure (or no-PID failed spawn), not group death.
    };
    const latch = (error) => {
      transport.proofError ??= error;
      results.status = "FAIL"; results.failed ??= String(error?.stack ?? error);
      done(error);
      stop(); // Also works during cleanupOnly: never resume ordinary proof waits.
      try { save(); } catch (saveError) { results.failed += `; failure save: ${saveError}`; }
    };
    const cancel = (error) => done(error);
    const collect = (stream, data) => {
      bytes[stream] += data.length;
      if (bytes[stream] > 1024 * 1024) {
        const error = new Error(`remote helper ${stream} maxBuffer exceeded`);
        error.code = "ERR_CHILD_PROCESS_STDIO_MAXBUFFER";
        done(error); // No numeric signal; tracked group is cleaned by cleanupOwned.
      } else chunks[stream].push(data);
    };
    const onStreamError = (error) => latch(error);
    const onStdout = (data) => collect("stdout", data);
    const onStderr = (data) => collect("stderr", data);
    const onError = (error) => {
      latch(error);
      // Failed spawn has no process/group to own or clean.
      if (transport.pid === undefined) { children.delete(transport); finalize(); }
    };
    const onSpawn = () => {
      try { transport.owned = ownLocal(transport.pid); }
      catch (error) { latch(error); return; }
      resolveReady();
      if (!cleanupOnly && !settled) {
        pending.add(cancel); // Ownership precedes all cancelable waits.
        if (stopping) cancel(new Error("proof interrupted"));
      }
    };
    const onClose = (code, signal) => {
      if (!settled) {
        const stdout = Buffer.concat(chunks.stdout).toString("utf8");
        const stderr = Buffer.concat(chunks.stderr).toString("utf8");
        if (code !== 0 || signal) {
          const error = new Error(`remote helper exited (${signal ?? code})${stderr ? `: ${stderr.trim()}` : ""}`);
          error.code = code; error.signal = signal; error.stdout = stdout; error.stderr = stderr;
          done(error);
        } else done(null, { stdout, stderr });
      }
      finalize();
    };
    transport.once("spawn", onSpawn);
    transport.on("error", onError);
    transport.once("close", onClose);
    transport.stdout?.on("data", onStdout); transport.stderr?.on("data", onStderr);
    transport.stdout?.on("error", onStreamError); transport.stderr?.on("error", onStreamError);
    timer = setTimeout(() => {
      const error = new Error(`remote helper deadline exceeded (${timeoutMs}ms)`);
      error.code = "ETIMEDOUT";
      // Only this directly owned ChildProcess, for its own deadline. Group death
      // is still verified later by the incarnation/pidfd cleanup. Capture
      // descendants before killing a leader whose children may retain pipes.
      try { if (transport.owned) ownedMembers(transport.owned); } catch (ownershipError) { error.cause = ownershipError; }
      try { transport.kill("SIGKILL"); } catch (killError) { error.cause = killError; }
      latch(error);
    }, timeoutMs);
  });
  // Ownership readiness can fail before remote() attaches its operation await.
  operation.catch(() => {});
  return { ready, operation };
};
const remote = async (command, cleanupOnly = false, deadline = Date.now() + 20_000) => {
  if (!cleanupOnly) gate();
  const { ready, operation } = remoteBuffered(command, cleanupOnly, deadline);
  await ready;
  // remoteBuffered already registers its own cancellation after ownership. A
  // second cancelWait would replace its specific error when latch() stops peers.
  const { stdout } = await operation;
  if (!cleanupOnly) gate();
  return stdout.trim();
};
// END REMOTE HELPER
// Process identity is PID + Linux start ticks, not a global name pattern. Forge
// is Linux. Return only mesh-bridge agents for this configured mesh, not raw ps.
const processScript = `const fs=require('fs'); const mesh=process.argv[1]; const rows=[];
for(const pid of fs.readdirSync('/proc').filter(p=>/^\\d+$/.test(p))) { try {
 const args=fs.readFileSync('/proc/'+pid+'/cmdline','utf8').split('\\0').filter(Boolean);
 const at=args.indexOf('--mesh');
 if(args.includes('agent') && args.some(a=>/mesh-bridge(?:\\.js)?$/.test(a)) && at>=0 && args[at+1]===mesh) {
 const stat=fs.readFileSync('/proc/'+pid+'/stat','utf8').split(') ')[1].split(' ');
 rows.push({pid:Number(pid),start:stat[19]}); }
} catch {} } console.log(JSON.stringify(rows));`;
const remoteAgents = async () => JSON.parse(await remote(shell([cfg.remoteNode, "-e", processScript, cfg.remoteMesh])));
// BEGIN FORGE LIFECYCLE — shared by local cleanup and serialized remote cleanup.
const processIdentity = (pid) => {
  try {
    const raw = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const s = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
    return { pid: Number(pid), state: s[0], parent: Number(s[1]), group: Number(s[2]), session: Number(s[3]), start: s[19] };
  } catch (error) { if (["ENOENT", "ESRCH"].includes(error.code)) return undefined; throw error; }
};
const ownedMembers = (owner) => {
  const all = fs.readdirSync("/proc").filter((p) => /^\d+$/.test(p)).map(processIdentity).filter(Boolean);
  const known = new Map(owner.known.map((p) => [p.pid, p]));
  const leader = all.find((p) => p.pid === owner.pid);
  for (const p of all) {
    const old = known.get(p.pid);
    if (old && (old.start !== p.start || old.group !== p.group || old.session !== p.session))
      throw new Error("owned PID reused or group/session changed");
  }
  if (leader && (leader.start !== owner.start || leader.group !== owner.group || leader.session !== owner.session))
    throw new Error("owner incarnation changed");
  const members = all.filter((p) => known.has(p.pid));
  for (let size = -1; size !== members.length;) {
    size = members.length;
    for (const p of all) {
      if (members.includes(p)) continue;
      const descendant = members.some((m) => m.pid === p.parent);
      const scoped = descendant || (!owner.tree &&
        p.group === owner.group && p.session === owner.session &&
        (leader || members.some((m) => m.group === owner.group && m.session === owner.session)));
      if (!scoped) continue;
      if (BigInt(p.start) < BigInt(owner.start)) throw new Error("pre-existing group member");
      members.push(p);
    }
  }
  if (!owner.tree && !leader && all.some((p) => p.group === owner.group && p.session === owner.session) && !members.length)
    throw new Error("unauthenticated orphan group");
  for (const p of members) known.set(p.pid, { ...p });
  owner.known = [...known.values()];
  return members.filter((p) => p.state !== "Z");
};
const ownLocal = (pid, tree = false) => {
  const identity = processIdentity(pid);
  if (!identity || (!tree && (identity.group !== pid || identity.session !== pid))) throw new Error("not an owned detached group");
  return { ...identity, tree, known: [identity] };
};
const alive = (p) => {
  const current = processIdentity(p.pid);
  if (current && (current.start !== p.start || current.group !== p.group || current.session !== p.session)) throw new Error("process incarnation changed");
  return current && current.state !== "Z";
};
// BEGIN OWNED PIDFD SIGNAL — self-contained for serialized remote lifecycle and probes.
const signalOwned = (member, name, deadline = Date.now() + 2000) => {
  const timeout = Math.min(2000, deadline - Date.now());
  if (timeout <= 0) throw new Error("owned signal deadline exceeded");
  const program = `import os, signal, sys, json
m = json.loads(sys.argv[1])
name = sys.argv[2]
fd = None
sent = False
try:
    if name not in ('SIGTERM', 'SIGKILL'):
        raise ValueError('unsupported owned signal')
    fd = os.pidfd_open(m['pid'])
    with open('/proc/%d/stat' % m['pid']) as f:
        fields = f.read().rsplit(') ', 1)[1].split()
    if fields[19] != m['start'] or int(fields[2]) != m['group'] or int(fields[3]) != m['session']:
        raise RuntimeError('owned process incarnation changed')
    if fields[0] != 'Z':
        signal.pidfd_send_signal(fd, getattr(signal, name))
        sent = True
except (ProcessLookupError, FileNotFoundError):
    pass
finally:
    if fd is not None:
        os.close(fd)
print(json.dumps(sent))
`;
  return JSON.parse(execFileSync("python3", ["-c", program, JSON.stringify(member), name],
    { encoding: "utf8", timeout, killSignal: "SIGKILL", maxBuffer: 64 * 1024 }));
};
// END OWNED PIDFD SIGNAL
const cleanupOwned = async (owner, deadline = Date.now() + 5000) => {
  const receipt = { pid: owner.pid, start: owner.start, signals: [], errors: [], dead: false };
  try {
    for (const [signal, ms] of [["SIGTERM", 2000], ["SIGKILL", 2000]]) {
      const members = ownedMembers(owner);
      if (!members.length) break;
      // Exhausting the polite phase must never skip the reserved SIGKILL phase.
      if (signal === "SIGTERM" && Date.now() >= deadline - 2000) continue;
      for (const p of members) {
        try {
          if (signalOwned(p, signal, signal === "SIGTERM" ? deadline - 2000 : deadline)) receipt.signals.push({ pid: p.pid, signal });
        } catch (error) { receipt.errors.push(String(error)); }
      }
      const until = Math.min(Date.now() + ms, signal === "SIGTERM" ? deadline - 2000 : deadline);
      while (ownedMembers(owner).length && Date.now() < until) await cleanupSleep(Math.min(50, until - Date.now()));
    }
    receipt.dead = ownedMembers(owner).length === 0;
    if (!receipt.dead) throw new Error("owned process/group survived cleanup");
  } catch (error) { receipt.errors.push(String(error?.stack ?? error)); }
  receipt.known = owner.known;
  return receipt;
};
// END FORGE LIFECYCLE
const remoteLifecycle = () => `const {execFileSync}=require("node:child_process"); const signalOwned=${signalOwned.toString()}; const processIdentity=${processIdentity.toString()}; const ownedMembers=${ownedMembers.toString()}; const ownLocal=${ownLocal.toString()}; const cleanupSleep=${cleanupSleep.toString()}; const cleanupOwned=${cleanupOwned.toString()};`;
const snapshotRemote = async (identity, tree = false, cleanupOnly = false, deadline = Date.now() + 20_000) => {
  const previous = remoteOwners.get(identity.pid);
  const program = `const fs=require('fs'); ${remoteLifecycle()}
const expected=${JSON.stringify(identity)}; const owner=${JSON.stringify(previous ?? null)} ?? ownLocal(expected.pid,${tree});
if(owner.start!==expected.start)throw Error('remote owner changed'); ownedMembers(owner); console.log(JSON.stringify(owner));`;
  const owner = JSON.parse(await remote(shell([cfg.remoteNode, "-e", program]), cleanupOnly, deadline));
  remoteOwners.set(owner.pid, owner); return owner;
};
const child = (exe, args, options, name) => {
  gate();
  results.commands ??= [];
  results.commands.push({ name, executable: exe, args, cwd: options.cwd }); save();
  const p = spawn(exe, args, { ...options, detached: true });
  children.add(p);
  p.on("error", (error) => { p.proofError = error; });
  p.stdin?.on("error", () => {});
  p.once("spawn", () => {
    try { p.owned = ownLocal(p.pid); }
    catch (error) { p.proofError = error; results.spawnErrors ??= []; results.spawnErrors.push({ name, error: error.message }); save(); }
  });
  return p;
};
// The fresh proof client is not a second owner of its parent's fleet lane.
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|HERDR|FABRIC|SMARTY_ROLE$)/.test(key)));
// ponytail: each proof call performs one explicit native provider operation, not
// general work-agent work. Keep context/profile/trust/mesh, but exclude unrelated
// skill reads: --tools allowlists extension tools too; --no-skills only disables discovery.
const piArgs = (fabric, sessionDir) => ["--mode", "rpc", "-ne", "-e", fabric, "--tools", "fabric_exec", "--no-skills", "--model", cfg.model, "--thinking", "medium", "--session-dir", sessionDir];
const rpc = (p, name) => {
  const listeners = new Set(), ingress = [], waits = new Set();
  let buffer = "", failure, promptCount = 0, runSequence = 0, activeRun;
  const fail = (error) => {
    failure ??= error;
    for (const cancel of [...waits]) cancel(failure);
  };
  const unavailable = () => failure ?? p.proofError ?? (stopping ? new Error("proof interrupted") :
    p.exitCode !== null || p.signalCode !== null ? new Error(`${name}: child exited`) : undefined);
  p.once("error", fail);
  p.once("spawn", () => { if (p.proofError) fail(p.proofError); });
  p.once("exit", (code, signal) => fail(new Error(`${name}: child exited (${signal ?? code})`)));
  const stdout = fs.createWriteStream(path.join(evidence, `${name}-rpc.jsonl`), { mode: 0o600 });
  const stderr = fs.createWriteStream(path.join(evidence, `${name}-stderr.log`), { mode: 0o600 });
  let stderrBuffer = "";
  p.stderr.on("data", (data) => {
    stderr.write(data);
    stderrBuffer = (stderrBuffer + data.toString()).slice(-8192);
    // Remote setsid leader is recorded before exec timeout; process start ticks
    // make later scoped cleanup safe even if the PID has been reused.
    const match = new RegExp(`PROOF_OWNED ${runId} (\\d+) (\\d+)`).exec(stderrBuffer);
    if (match) remoteOwned = { pid: Number(match[1]), start: match[2] };
  });
  const remember = (message) => {
    if (!message || ["assistant", "toolResult"].includes(message.role)) return;
    const text = typeof message.content === "string" ? message.content : (message.content ?? []).map((c) => c.text ?? "").join("");
    if (text.includes(runId)) ingress.push({ role: message.role, customType: message.customType, text });
  };
  p.stdout.on("data", (data) => {
    stdout.write(data); buffer += data.toString();
    let at;
    while ((at = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, at); buffer = buffer.slice(at + 1);
      let record; try { record = JSON.parse(line); } catch { continue; }
      // Native RPC has no run ID: number starts locally, keeping the run alive
      // through settled (agent_end precedes awaited native settle handlers).
      if (record.type === "agent_start") activeRun = ++runSequence;
      if (record.type === "message_end") remember(record.message);
      for (const listener of [...listeners]) listener(record);
      if (record.type === "agent_settled") activeRun = undefined;
    }
  });
  p.once("close", () => { fail(new Error(`${name}: child closed`)); stdout.end(); stderr.end(); });
  const request = (type, fields = {}, ms = 15_000) => new Promise((resolve, reject) => {
    const error = unavailable();
    if (error) return reject(error);
    const id = randomUUID();
    let settled = false;
    const done = (error, value) => {
      if (settled) return;
      settled = true; clearTimeout(timer); listeners.delete(on); waits.delete(done); pending.delete(done);
      error ? reject(error) : resolve(value);
    };
    const on = (record) => {
      if (!settled && record.type === "response" && record.id === id) {
        record.success === false ? done(new Error(`${name} ${type}: ${record.error}`)) : done(null, record.data);
      }
    };
    const timer = setTimeout(() => done(new Error(`${name} RPC ${type} deadline`)), ms);
    listeners.add(on); waits.add(done); pending.add(done);
    try {
      if (type === "prompt") promptCount++;
      p.stdin.write(`${JSON.stringify({ id, type, ...fields })}\n`, (error) => { if (error) done(error); });
    }
    catch (error) { done(error); }
  });
  const idle = async () => { const state = await request("get_state"); gate(); return !state.isStreaming; };
  const execute = async (code, ms = 120_000) => {
    const started = Date.now();
    await waitFor(`${name} idle`, idle, Math.min(ms, 60_000));
    gate();
    const token = randomUUID();
    const wrapped = `const value = await (async () => { ${code}\n})(); return { proofToken: ${JSON.stringify(token)}, value };`;
    const message = `Call fabric_exec exactly once with exactly the code below, byte for byte, resultFormat "json", timeoutMs ${Math.min(ms, 120_000)}. Do not call any other tool or communicate with any other root. Then reply only OK.\n${wrapped}`;
    return new Promise((resolve, reject) => {
      const error = unavailable();
      if (error) return reject(error);
      let owningRun, toolId, receipt, ended = false, settled = false;
      const done = (error, value) => {
        if (settled) return;
        settled = true; clearTimeout(timer); listeners.delete(on); waits.delete(done); pending.delete(done);
        error ? reject(error) : resolve(value);
      };
      const on = (record) => {
        if (settled) return;
        if ((record.type === "message_start" || record.type === "message_end") && record.message?.role === "user") {
          const content = record.message.content;
          const text = typeof content === "string" ? content : Array.isArray(content) && content.every((c) => c.type === "text")
            ? content.map((c) => c.text).join("") : undefined;
          if (text === message) {
            if (!activeRun || (owningRun !== undefined && owningRun !== activeRun)) return done(new Error(`${name}: prompt run identity mismatch`));
            owningRun = activeRun;
          }
        }
        // A queued prompt can follow an earlier run's settle, or a native inbox
        // reply with no tools. Neither owns this exact user prompt.
        if (owningRun === undefined) return;
        if (activeRun !== owningRun) return done(new Error(`${name}: owning run changed before settlement`));
        if (record.type === "agent_end") ended = true;
        if (record.type === "agent_settled") {
          if (record.outcome !== "completed" || !ended || !receipt) return done(new Error(`${name}: owning run settled without one completed execution`));
          results.toolExecutions ??= [];
          results.toolExecutions.push({ side: name, token, toolCallId: toolId, run: owningRun, isError: false }); save();
          return done(null, receipt.value);
        }
        if (record.type === "tool_execution_start") {
          if (ended || toolId !== undefined || record.toolName !== "fabric_exec" || record.args?.code !== wrapped ||
              typeof record.toolCallId !== "string" || !record.toolCallId) return done(new Error(`${name}: wrong or duplicate tool execution`));
          toolId = record.toolCallId;
          return;
        }
        if (record.type !== "tool_execution_end") return;
        if (ended || receipt || !toolId || record.toolCallId !== toolId || record.toolName !== "fabric_exec")
          return done(new Error(`${name}: wrong or duplicate tool execution end`));
        if (record.isError || record.result?.isError || record.result?.details?.success === false) return done(new Error(`${name}: fabric_exec failed`));
        const content = record.result?.content;
        const text = Array.isArray(content) ? content.filter((c) => c?.type === "text" && typeof c.text === "string").map((c) => c.text).join("\n") : "";
        let parsed;
        // resultFormat=json; permit host rendering around the JSON, not model prose.
        for (let a = text.indexOf("{"); a >= 0 && !parsed; a = text.indexOf("{", a + 1)) {
          for (let b = text.lastIndexOf("}"); b > a; b = text.lastIndexOf("}", b - 1)) {
            try { const v = JSON.parse(text.slice(a, b + 1)); if (v.proofToken === token) { parsed = v; break; } } catch {}
          }
        }
        if (!parsed) return done(new Error(`${name}: actual tool result lacks proof token / valid JSON`));
        receipt = parsed; // Commit exactly once only after the owning run settles.
      };
      const timer = setTimeout(() => done(new Error(`${name}: tool execution deadline`)), Math.max(1, ms - (Date.now() - started)));
      listeners.add(on); waits.add(done); pending.add(done);
      request("prompt", { message }, ms).catch((error) => done(error));
    });
  };
  const received = async (needle, delivery, senderId) => {
    // Explicit RPC reads ONLY this new session. Some native custom messages are
    // not emitted as message_end, so the stream alone is not proof of absence.
    const data = await request("get_messages");
    gate();
    for (const message of data?.messages ?? []) remember(message);
    for (const item of ingress.filter((m) => m.customType === "pi-fabric-agent-message")) {
      for (const match of item.text.matchAll(/(<fabric-agent-message\b[^>]*>)([\s\S]*?)<\/fabric-agent-message>/g)) {
        const header = match[1];
        if (match[2].includes(needle) && header.includes(`delivery="${delivery}"`) && header.includes(`from_id="${senderId}"`)) {
          return { role: item.role, customType: item.customType, header, senderId, needle };
        }
      }
    }
    return undefined;
  };
  const receivedWork = async (expected) => {
    // Same authoritative RPC path as received(): no model prompt, mesh read, or embedded tool output.
    const data = await request("get_messages");
    gate();
    return workInboxIngress(data?.messages ?? [], expected);
  };
  return { p, execute, idle, received, receivedWork, request, promptCount: () => promptCount };
};

const cleanup = () => cleanupPromise ??= (async () => {
  // One budget, below forge-ssh-bridge.sh's 30s TERM-to-KILL grace. Reserve the
  // last 5s for locally owned SSH groups created by remote cleanup itself.
  const deadline = Date.now() + 25_000, remoteDeadline = deadline - 5000;
  stop(); // Normal completion cancels proof waits, but is not a signal/failure.
  clearInterval(ownershipMonitor);
  const errors = [], receipts = [], reaped = new Set();
  const reapLocal = async (until) => {
    await Promise.all([...children].filter((p) => !reaped.has(p)).map(async (p) => {
      if (!p.owned) { reaped.add(p); errors.push(`missing ownership: ${p.pid}`); }
      else {
        const receipt = await cleanupOwned(p.owned, until);
        receipts.push({ remote: false, ...receipt }); errors.push(...receipt.errors);
        if (receipt.dead) reaped.add(p); // Retry a surviving initial group in the final reserve.
      }
      // Settlement is not closure. Bound pipe/close waits by the same budget;
      // an inherited SSH pipe must not keep cleanup past the wrapper's grace.
      if (p.proofClosed) {
        let timer;
        try {
          await Promise.race([p.proofClosed, new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`local helper close deadline: ${p.pid}`)), Math.max(1, until - Date.now()));
          })]);
        } catch (error) { errors.push(String(error)); }
        finally { clearTimeout(timer); }
      }
      if (p.proofError) errors.push(String(p.proofError));
    }));
  };
  // Capture authenticated descendants BEFORE EOF can remove their leader.
  for (const p of children) {
    try { if (p.owned) ownedMembers(p.owned); else throw p.proofError ?? new Error(`missing local ownership: ${p.pid}`); }
    catch (error) { errors.push(String(error)); }
  }
  for (const pi of [piA, piB]) {
    try { if (pi?.p.stdin && !pi.p.stdin.destroyed && !pi.p.stdin.writableEnded) pi.p.stdin.end(); }
    catch (error) { errors.push(String(error)); }
  }
  // Both Pis retain independent timeout --kill-after=10s 900s protection.
  await cleanupSleep(1500);
  // Never await remote SSH before reaping the bridge and existing local groups.
  await reapLocal(Math.min(remoteDeadline, Date.now() + 5000));
  for (const [identity, tree] of [[remoteOwned, false], [remoteBridge, true]]) {
    if (!identity || !cfg) continue;
    try { await snapshotRemote(identity, tree, true, remoteDeadline); }
    catch (error) { errors.push(`remote ownership: ${error.message}`); }
  }
  for (const owner of remoteOwners.values()) {
    const script = `const fs=require('fs'); ${remoteLifecycle()}
(async()=>{console.log(JSON.stringify(await cleanupOwned(${JSON.stringify(owner)})));})().catch(e=>{console.error(e);process.exitCode=1;});`;
    try {
      const receipt = JSON.parse(await remote(shell([cfg.remoteNode, "-e", script]), true, remoteDeadline));
      receipts.push({ remote: true, ...receipt }); errors.push(...receipt.errors);
      if (!receipt.dead) errors.push(`remote group not dead: ${owner.pid}`);
    } catch (error) { errors.push(`remote cleanup: ${error.message}`); }
  }
  await reapLocal(deadline);
  results.cleanup = { remoteOwned, receipts, errors }; save();
  assert(errors.length === 0, errors.join("; "));
})();
process.on("SIGTERM", interrupt);
process.on("SIGINT", interrupt);

try {
  cfg = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  for (const key of ["localMesh", "remoteMesh", "localFabric", "remoteFabric", "localAgentDir", "remoteAgentDir", "localCwd", "remoteCwd", "evidenceRoot", "remoteEvidenceRoot", "bridgeKey", "bridgeKnownHosts"]) {
    assert(typeof cfg[key] === "string" && path.isAbsolute(cfg[key]) && !cfg[key].includes("\n"), `required absolute path: ${key}`);
  }
  for (const key of ["evidenceRoot", "remoteEvidenceRoot"]) assert(cfg[key].split("/").includes(".local") && !cfg[key].startsWith("/tmp/"), `${key} must be under .local, not /tmp`);
  Object.assign(cfg, { localName: cfg.localName ?? "dev1", remoteName: cfg.remoteName ?? "forge",
    localPi: cfg.localPi ?? "pi", remotePi: cfg.remotePi ?? "forge-pi", localNode: cfg.localNode ?? "node", remoteNode: cfg.remoteNode ?? "node",
    sshHost: cfg.sshHost ?? "forge-agent", bridgeHost: cfg.bridgeHost ?? "paul@100.78.65.112", bridgePort: String(cfg.bridgePort ?? 22),
    model: cfg.model ?? "cliproxyapi/gpt-6.1-sol", bridgeBin: cfg.bridgeBin ?? path.resolve(path.dirname(cfg.localFabric), "../bin/mesh-bridge") });
  assert(!cfg.sshHost.startsWith("-") && !cfg.bridgeHost.startsWith("-"), "SSH host must not be an option");
  assert(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(cfg.localName) && /^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(cfg.remoteName) && cfg.localName !== cfg.remoteName, "distinct valid bridge names required");
  results.acceptanceScope = cfg.remoteName === "forge" ? "exact-forge" : `supplemental-${cfg.remoteName}`;
  evidence = path.join(cfg.evidenceRoot, runId); fs.mkdirSync(evidence, { recursive: true, mode: 0o700 });
  // Deliberately whitelist metadata; never persist arbitrary config or environment.
  results.configuration = Object.fromEntries(["localName", "remoteName", "localMesh", "remoteMesh", "localFabric", "remoteFabric", "localAgentDir", "remoteAgentDir", "localCwd", "remoteCwd", "localPi", "remotePi", "model", "sshHost", "bridgeHost", "bridgeBin", "bridgeKey", "bridgeKnownHosts"].map((k) => [k, cfg[k]])); save();
  for (const p of [cfg.localMesh, cfg.localAgentDir, cfg.localCwd]) assert(fs.statSync(p).isDirectory(), `missing existing directory: ${p}`);
  for (const p of [cfg.localFabric, cfg.bridgeBin, cfg.bridgeKey, cfg.bridgeKnownHosts]) assert(fs.statSync(p).isFile(), `missing existing file: ${p}`);
  const localMeshConfig = JSON.parse(fs.readFileSync(path.join(cfg.localCwd, ".pi/fabric.json"), "utf8"));
  assert(localMeshConfig?.mesh?.root === cfg.localMesh, "own local cwd .pi/fabric.json mesh.root must match localMesh exactly");
  check("localProjectMeshConfig", { path: path.join(cfg.localCwd, ".pi/fabric.json"), meshRoot: localMeshConfig.mesh.root });
  const remoteMeshCheck = `const fs=require('fs'),path=require('path');const file=path.join(process.argv[1],'.pi/fabric.json');const c=JSON.parse(fs.readFileSync(file,'utf8'));if(c.mesh?.root!==process.argv[2])throw Error('remote project mesh.root mismatch');console.log(JSON.stringify({path:file,meshRoot:c.mesh.root}));`;
  check("remoteProjectMeshConfig", JSON.parse(await remote(shell([cfg.remoteNode, "-e", remoteMeshCheck, cfg.remoteCwd, cfg.remoteMesh]))));
  ownershipMonitor = setInterval(() => {
    if (stopping) return;
    try { for (const p of children) if (p.owned) ownedMembers(p.owned); }
    catch (error) { results.status = "FAIL"; results.failed ??= String(error); stop(); save(); }
    // Remote groups are captured after launch and before kill/EOF; do not spawn
    // repeated SSH probes merely to monitor an idle proof.
  }, 250);
  const remoteRoot = path.join(cfg.remoteEvidenceRoot, runId);
  await remote([`test -d ${quote(cfg.remoteMesh)}`, `test -d ${quote(cfg.remoteAgentDir)}`, `test -d ${quote(cfg.remoteCwd)}`, `test -f ${quote(cfg.remoteFabric)}`, `umask 077; mkdir -p ${quote(remoteRoot)}`].join(" && "));
  const beforeRemote = await remoteAgents();
  assert(beforeRemote.length === 0, "existing bridge agent on selected remote mesh: cannot prove isolated bridge death");
  const { MeshStore } = await cancelWait(import(pathToFileURL(path.join(path.dirname(cfg.localFabric), "mesh.js")).href));
  gate();
  const store = new MeshStore(cfg.localMesh, 256 * 1024, 500, { lockTimeoutMs: 2000 });
  piA = rpc(child("timeout", ["--signal=TERM", "--kill-after=10s", "900s", "nice", "-n", "19", cfg.localPi, ...piArgs(cfg.localFabric, path.join(evidence, "sessions"))],
    { cwd: cfg.localCwd, env: { ...cleanEnv(), PI_CODING_AGENT_DIR: cfg.localAgentDir }, stdio: ["pipe", "pipe", "pipe"] }, `${cfg.localName} Pi`), cfg.localName);
  // ponytail: SSH's command may already lead a process group; setsid then forks.
  // --wait keeps the SSH command (and RPC stdin) alive until that child finishes.
  const remoteCommand = `cd ${quote(cfg.remoteCwd)} && exec ${shell(["setsid", "--wait", "sh", "-c",
    `printf 'PROOF_OWNED ${runId} %s %s\\n' "$$" "$(awk '{print $22}' /proc/$$/stat)" >&2; exec ${shell(["timeout", "--signal=TERM", "--kill-after=10s", "900s", "nice", "-n", "19", "env", `PI_CODING_AGENT_DIR=${cfg.remoteAgentDir}`, cfg.remotePi, ...piArgs(cfg.remoteFabric, path.join(remoteRoot, "sessions"))])}`])}`;
  piB = rpc(child("ssh", [...sshArgs(), remoteCommand], { stdio: ["pipe", "pipe", "pipe"] }, `${cfg.remoteName} Pi ordinary SSH`), cfg.remoteName);
  await waitFor("owned remote timeout leader", () => {
    assert(!piB.p.proofError && piB.p.exitCode === null && piB.p.signalCode === null, "remote Pi failed before reporting its owned leader");
    return remoteOwned;
  });
  await snapshotRemote(remoteOwned);
  // Initial RPC commands are queued while native session_start binds extensions.
  // Allow that one cold bootstrap its own bound; later requests retain the 15s bound.
  const states = await Promise.all([piA, piB].map((pi) => pi.request("get_state", {}, 120_000)));
  assert(states.every((s) => `${s.model?.provider}/${s.model?.id}` === cfg.model && typeof s.sessionId === "string"), "native RPC must select the requested model and fresh session");
  check("nativeRpcReady", states.map((s) => ({ sessionId: s.sessionId, model: `${s.model.provider}/${s.model.id}`, thinkingLevel: s.thinkingLevel })));
  const [a, b] = await Promise.all([piA, piB].map((pi) => pi.execute("const main = await agents.main(); return { id: main.id, local: main.local };")));
  assert(a.local === true && b.local === true && a.id === `session:${states[0].sessionId}` && b.id === `session:${states[1].sessionId}` && a.id !== b.id, "native agents.main must identify the two fresh RPC roots");
  results.roots = { [cfg.localName]: a.id, [cfg.remoteName]: b.id }; check("nativeRoots", results.roots);
  const peerCode = (target) => `const peers = await agents.peers(); return { ownTarget: peers.filter(p => p.id === ${JSON.stringify(target)}).map(p => ({id:p.id,host:p.host})), unrelatedCount: peers.filter(p => p.id !== ${JSON.stringify(a.id)} && p.id !== ${JSON.stringify(b.id)}).length };`;
  const baseline = await Promise.all([piA.execute(peerCode(b.id)), piB.execute(peerCode(a.id))]);
  assert(baseline.every((v) => v.ownTarget.length === 0), "own remote root visible before proof bridge: existing link interferes");
  check("noBridgeCounterexample", baseline); // No messages to unrelated peers.
  const bridgeLog = fs.openSync(path.join(evidence, "bridge.log"), "a", 0o600);
  bridge = child(cfg.localNode, [cfg.bridgeBin, "run", "--mesh", cfg.localMesh, "--name", cfg.localName, "--remote", cfg.remoteName, "--cursor", path.join(evidence, "cursor.json"),
    "--ssh", cfg.bridgeHost, "--ssh-key", cfg.bridgeKey, "--ssh-port", cfg.bridgePort, "--ssh-known-hosts", cfg.bridgeKnownHosts], { stdio: ["ignore", bridgeLog, bridgeLog] }, "bridge forced-command SSH");
  fs.closeSync(bridgeLog);
  bridgeSsh = await waitFor("exact bridge SSH child", async () => {
    assert(!bridge.proofError && bridge.exitCode === null, "bridge failed before linking");
    // ponytail: this Linux proof already uses /proc ownership; avoid spawning ps
    // (no-child exit races and process startup time are not transport failures).
    try {
      const pids = fs.readFileSync(`/proc/${bridge.pid}/task/${bridge.pid}/children`, "utf8").trim().split(/\s+/).filter(Boolean);
      const pid = pids.find((id) => fs.readFileSync(`/proc/${id}/comm`, "utf8").trim() === "ssh");
      return pid && ownLocal(Number(pid), true);
    } catch (error) { if (error.code === "ENOENT") return undefined; throw error; }
  });
  remoteBridge = await waitFor("new exact remote bridge agent", async () => {
    const rows = await remoteAgents(); assert(rows.length <= 1, "multiple selected remote bridge agents interfere"); return rows[0];
  });
  await snapshotRemote(remoteBridge, true);
  ownedMembers(bridge.owned);
  check("ownedTransport", { bridge: bridge.owned, ssh: bridgeSsh, remoteAgent: remoteBridge });
  const discovered = await Promise.all([[piA, b.id, cfg.remoteName], [piB, a.id, cfg.localName]].map(async ([pi, target, host]) =>
    waitFor(`${host} peer discovery`, async () => { const v = await pi.execute(peerCode(target)); return v.ownTarget.some((p) => p.host === host) && v; })));
  check("actualHostPeers", discovered);
  for (const [sender, receiver, target, direction] of [[piA, piB, b.id, `${cfg.localName}-${cfg.remoteName}`], [piB, piA, a.id, `${cfg.remoteName}-${cfg.localName}`]]) {
    for (const operation of ["steer", "followUp"]) {
      const needle = `${runId}-${direction}-${operation}`;
      const ack = await sender.execute(`return await agents.${operation}(${JSON.stringify(target)}, ${JSON.stringify(`${needle}: reply only OK; do not use tools.`)});`);
      assert(ack?.queued === true && ack.acknowledged === true && ack.routed === "mesh" && typeof ack.messageId === "string", `${direction} ${operation} lacks native acknowledgement`);
      check(`${direction}-${operation}`, { ack, ingress: await waitFor(`${direction} ${operation} native ingress`, () => receiver.received(needle, operation, sender === piA ? a.id : b.id), 120_000) });
    }
  }
  // Complete all previous turns before publishing: no remote owner prompt may
  // trigger before_agent_start reconciliation during this independent idle wake.
  await Promise.all([piA, piB].map((pi) => waitFor("idle before work wake", pi.idle, 120_000)));
  const work = { topic: "fleet.work.smarty-dev.2045", kind: "p0", to: b.id,
    text: `${runId}-work-wake: reply only OK; do not use tools or communicate with other roots.`,
    data: { ref: "smarty-dev#2045", key: `${runId}-work-${randomUUID()}`, runId } };
  const remotePrompts = piB.promptCount();
  const workWake = await piA.execute(`return await mesh.publish(${JSON.stringify(work)});`);
  assert(typeof workWake?.id === "string" && workWake.from?.id === a.id && workWake.to === b.id && workWake.topic === work.topic &&
    workWake.kind === work.kind && workWake.text === work.text && workWake.data?.key === work.data.key && workWake.data?.runId === runId,
    "native local work publication lacks exact fresh-root attribution / run marker");
  const expectedWork = { ...work, from: workWake.from, key: work.data.key };
  // >=120s is essential: actual RootInbox steer grace is 60s, plus idle timer and bridge latency.
  // No steer/followUp carries this unique key. RPC get_messages cannot start a model turn.
  const workIngress = await waitFor("remote idle Main native work inbox ingress", () => piB.receivedWork(expectedWork), 180_000);
  assert(piB.promptCount() === remotePrompts, "remote owner prompt occurred before independent work ingress assertion");
  check("factoryWorkIdleNativeIngress", { originalEventId: workWake.id, work, ingress: workIngress,
    remotePromptsBefore: remotePrompts, remotePromptsAtIngress: piB.promptCount(), noCorrespondingSteer: true });
  // Only AFTER native ingress has been asserted may Main be prompted to read its mesh.
  const workRead = await waitFor("remote Main native bridged work read", async () => {
    const events = await piB.execute(`const self=(await agents.main()).id; const events=await mesh.read({topic:${JSON.stringify(work.topic)},to:self,limit:500}); return events.filter(e=>e.data?.key===${JSON.stringify(work.data.key)}).map(e=>({id:e.id,topic:e.topic,kind:e.kind,to:e.to,from:e.from,text:e.text,key:e.data?.key,runId:e.data?.runId,bridge:e.data?.bridge}));`);
    return events.find((e) => e.id === workIngress.eventId && e.topic === work.topic && e.kind === work.kind && e.to === b.id &&
      e.from?.id === a.id && e.from?.name === workWake.from.name && e.from?.kind === workWake.from.kind && e.text === work.text &&
      e.key === work.data.key && e.runId === runId && e.bridge?.id === workWake.id && e.bridge.from === cfg.localName);
  }, 180_000);
  const correlatedIngress = await piB.receivedWork({ ...expectedWork, id: workRead.id });
  assert(correlatedIngress?.eventId === workRead.id, "native inbox remote event ID does not correlate with native bridged read");
  check("factoryWorkBridgedRead", { ...workRead, ingress: correlatedIngress });
  const prNeedle = `${runId}-pr.wake`, laneNeedle = `${runId}-lane-followUp`;
  gate();
  const prWake = await cancelWait(store.publish({ topic: "ops.owner", kind: "pr.wake", from: { id: "factory-host:owner", name: "factory-owner", kind: "main" }, to: b.id,
    text: prNeedle, data: { rootId: b.id, repo: "Smarty-Pants-Inc/smarty-dev", pr: 2045 } }));
  gate();
  const now = Date.now();
  const laneWake = await cancelWait(store.publish({ topic: "fabric.control.command", kind: "followUp", from: { id: "factory-host:stuck-work", name: "factory-stuck-work", kind: "main" }, to: b.id,
    text: laneNeedle, data: { version: 1, commandId: randomUUID().replaceAll("-", ""), targetId: b.id, operation: "followUp", replyTo: "factory-host:stuck-work",
      message: `${laneNeedle}: reply only OK; do not use tools.`, data: { ref: "smarty-dev#2045" }, requestedAt: now, deadlineAt: now + 120_000 } }));
  check("factoryLaneNativeIngress", { eventId: laneWake.id, ingress: await waitFor("factory lane followUp native ingress", () => piB.received(laneNeedle, "followUp", "factory-host:stuck-work"), 120_000) });
  const prRead = await waitFor("remote Main reads own factory pr.wake", async () => {
    const events = await piB.execute(`const self=(await agents.main()).id; const events=await mesh.read({topic:"ops.owner",to:self,limit:50}); return events.filter(e=>e.text===${JSON.stringify(prNeedle)}).map(e=>({id:e.id,kind:e.kind,to:e.to,bridge:e.data?.bridge}));`);
    return events.find((e) => e.kind === "pr.wake" && e.to === b.id && e.bridge?.id === prWake.id && e.bridge.from === cfg.localName);
  });
  check("factoryPrWakeRead", prRead);
  await Promise.all([piA, piB].map((pi) => waitFor("idle before bridge kill", pi.idle)));
  // Read only the actual configured source meshes; never another session/profile.
  const remoteLedger = async (start, marker, condition) => {
    assert(!stopping, "proof interrupted before remote ledger operation");
    const program = `import fs from "node:fs"; import path from "node:path"; import { isDeepStrictEqual } from "node:util";
import { MeshStore } from ${JSON.stringify(pathToFileURL(path.join(path.dirname(cfg.remoteFabric), "mesh.js")).href)};
const validateLedgerEvent = ${validateLedgerEvent.toString()};
const captureLedger = ${captureLedger.toString()}; const readLedger = ${readLedger.toString()};
const assertPostkillLedger = ${assertPostkillLedger.toString()};
const assert = ${assert.toString()};
let stopping = false; const pending = new Set();
const gate = ${gate.toString()}; const stop = ${stop.toString()}; const cancelWait = ${cancelWait.toString()};
const exclusiveLedger = ${exclusiveLedger.toString()};
process.on("SIGTERM", stop); process.on("SIGINT", stop);
const store = new MeshStore(${JSON.stringify(cfg.remoteMesh)}, 256 * 1024, 500, { lockTimeoutMs: 2000 });
const start = ${JSON.stringify(start ?? null)};
const senderFindings = [...(start?.senderFindings ?? [])];
const value = await exclusiveLedger(store, () => start ? assertPostkillLedger(readLedger(store, start, senderFindings), ${JSON.stringify(b.id)},
  ${JSON.stringify(a.id)}, ${JSON.stringify(marker ?? null)}, ${JSON.stringify(cfg.localName)}, ${JSON.stringify(condition ?? null)}, senderFindings) : captureLedger(store), gate, cancelWait);
console.log(JSON.stringify(value));`;
    const output = await remote(shell([cfg.remoteNode, "--input-type=module", "-e", program]));
    gate();
    return JSON.parse(output);
  };
  const ledgerStarts = { local: await exclusiveLedger(store, () => captureLedger(store), gate, cancelWait), remote: await remoteLedger() };
  gate(); // In particular, a canceled prekill SSH ledger must not reach SIGKILL.
  const postkillMarkers = { local: `${runId}-${cfg.localName}-to-${cfg.remoteName}-postkill-${randomUUID()}`,
    remote: `${runId}-${cfg.remoteName}-to-${cfg.localName}-postkill-${randomUUID()}` };
  gate();
  ownedMembers(bridge.owned);
  await snapshotRemote(remoteBridge, true);
  gate();
  assert(alive(bridge.owned), "bridge incarnation missing before kill");
  assert(bridge.kill("SIGKILL"), "bridge SIGKILL failed");
  const killedAt = Date.now(); results.bridgeKilledAt = new Date(killedAt).toISOString(); save();
  const firstSendProbe = async (pi, source, target, host, marker) => {
    const requestAt = Date.now();
    // The outer RPC/model budget is not the native send budget. No retry, no delay,
    // and no preceding peer read that could refresh away the pending-lapse path.
    const v = await pi.execute(`const nativeStartedAt = Date.now(); let outcome; try { outcome = { delivered: await agents.steer(${JSON.stringify(target)}, ${JSON.stringify(marker)} ) }; } catch(error) { outcome = { error: String(error?.message ?? error) }; } const nativeCompletedAt = Date.now(); return { nativeStartedAt, nativeCompletedAt, nativeMs: nativeCompletedAt - nativeStartedAt, ...outcome };`, 120_000);
    gate();
    const completedAt = Date.now();
    const attempts = [{ ...v, source, target, marker, nativeTimestampDomain: "native-execution-host-local",
      driverTimestampDomain: "local-driver", requestAt, completedAt, requestAfterKillMs: requestAt - killedAt,
      completionAfterKillMs: completedAt - killedAt, rpcModelDurationMs: completedAt - requestAt }];
    results.afterKill ??= {}; results.afterKill[host] = attempts; save();
    const validation = assertFirstSend(attempts, target, host, killedAt);
    check(`firstPostkillSend-${host}`, validation);
    return validation;
  };
  const probes = await Promise.allSettled([firstSendProbe(piA, a.id, b.id, cfg.remoteName, postkillMarkers.local), firstSendProbe(piB, b.id, a.id, cfg.localName, postkillMarkers.remote)]);
  assert(probes.every((p) => p.status === "fulfilled"), probes.filter((p) => p.status === "rejected").map((p) => p.reason.message).join("; "));
  check("bothFirstPostkillSendsNamedBounded", true);
  const afterDeath = await Promise.all([[piA, a.id, b.id], [piB, b.id, a.id]].map(async ([pi, ownId, target]) => {
    const v = await pi.execute(`const main=await agents.main(); const peers=await agents.peers(); return {main:{id:main.id,local:main.local},ownTarget:peers.filter(p=>p.id===${JSON.stringify(target)}).map(p=>({id:p.id,host:p.host})),unrelatedCount:peers.filter(p=>p.id!==${JSON.stringify(a.id)}&&p.id!==${JSON.stringify(b.id)}).length};`);
    assert(v.main.id === ownId && v.main.local === true && v.ownTarget.length === 0, "native main/peers must remain usable after bridge death without the lapsed own target");
    return v;
  }));
  check("nativeMainPeersAfterDeath", afterDeath);
  // First assert natural EOF reaping, before scoped cleanup could mask a leak.
  await waitFor("bridge/SSH owned group exits after SIGKILL", () => ownedMembers(bridge.owned).length === 0, 15_000);
  await waitFor("forced remote agent tree exits on transport EOF", async () => {
    const owner = await snapshotRemote(remoteBridge, true);
    gate();
    const program = `const fs=require('fs'); ${remoteLifecycle()} console.log(JSON.stringify(ownedMembers(${JSON.stringify(owner)}).length));`;
    return JSON.parse(await remote(shell([cfg.remoteNode, "-e", program]))) === 0;
  }, 20_000);
  check("transportGoneAfterKill", true);
  // Native settlement can precede best-effort publication/cancel completion. Observe
  // after two bounded 10s mesh-lock windows, without retrying either native send.
  const ledgerReadNotBefore = Math.max(...Object.values(results.afterKill).map((v) => v[0].completedAt)) + 20_000;
  await waitFor("bounded postkill ledger observation", () => Date.now() >= ledgerReadNotBefore, 25_000);
  assert(!stopping, "proof interrupted before ledger read");
  results.postkillLedgerReadNotBefore = ledgerReadNotBefore;
  const localCondition = results.checks[`firstPostkillSend-${cfg.remoteName}`].condition;
  const remoteCondition = results.checks[`firstPostkillSend-${cfg.localName}`].condition;
  const senderFindings = [...ledgerStarts.local.senderFindings];
  check("postkillSourceLedgers", {
    local: { window: ledgerStarts.local, ...await exclusiveLedger(store, () => assertPostkillLedger(readLedger(store, ledgerStarts.local, senderFindings),
      a.id, b.id, postkillMarkers.local, cfg.remoteName, localCondition, senderFindings), gate, cancelWait) },
    remote: { window: ledgerStarts.remote, ...await remoteLedger(ledgerStarts.remote, postkillMarkers.remote, remoteCondition) },
  });
  assert(!stopping, "proof interrupted during ledger read");
  results.status = "PASS";
} catch (error) {
  // Even invalid configuration gets a failure receipt, without touching auth.
  if (!evidence) { evidence = path.resolve(".local/forge-bridge-proof", runId); fs.mkdirSync(evidence, { recursive: true, mode: 0o700 }); }
  results.status = "FAIL"; results.failed = String(error?.stack ?? error); save(); console.error(results.failed);
} finally {
  try { await cleanup(); } catch (error) { results.status = "FAIL"; results.cleanupFailure = String(error?.stack ?? error); }
  results.status = finalStatus(results.status, results.interrupted, results.cleanup?.errors ?? ["cleanup missing"]);
  results.finishedAt = new Date().toISOString(); save();
  console.log(JSON.stringify({ status: results.status, acceptanceScope: results.acceptanceScope, evidence, checks: Object.keys(results.checks), failure: results.failed ?? results.cleanupFailure }));
  process.exitCode = results.status === "PASS" ? 0 : 1;
}
