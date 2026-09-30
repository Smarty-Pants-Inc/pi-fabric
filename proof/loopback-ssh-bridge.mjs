// smarty-dev#2045 lane C: two real Pis (RPC mode) on two scratch meshes, "dev1" (hub) and "forge",
// linked by pi-fabric#135's bin/mesh-bridge over REAL ssh (a private loopback sshd; see
// loopback-ssh-bridge.sh). Every agent step is a model turn that calls fabric_exec.
// Based on lane B's two-mesh-proof.mjs (pi-fabric#132 round-1 proof).
// usage: node loopback-ssh-bridge.mjs SCRATCH PI_FABRIC_DIST BRIDGE_BIN SSH_HOST SSH_KEY SSH_PORT KNOWN_HOSTS
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

// BEGIN LOOPBACK VALIDATORS (the scratch negative probe evaluates this exact block).
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const toolEvidence = (code, token) => {
  let start, end, value;
  const records = [];
  return {
    records,
    accept(record) {
      records.push(record);
      if (record.type === "tool_execution_start") {
        assert(!start && record.toolName === "fabric_exec" && record.args?.code === code && record.toolCallId,
          "missing/extra/wrong exact-program fabric_exec selection");
        start = record;
      }
      if (record.type !== "tool_execution_end") return;
      assert(start && !end && record.toolCallId === start.toolCallId && record.toolName === "fabric_exec", "wrong toolCallId/tool result");
      assert(!record.isError && !record.result?.isError && record.result?.details?.success !== false, "fabric_exec error");
      end = record;
      const text = (record.result?.content ?? []).filter((c) => c.type === "text").map((c) => c.text).join("\n");
      let parsed;
      for (let a = text.indexOf("{"); a >= 0 && !parsed; a = text.indexOf("{", a + 1)) {
        for (let b = text.lastIndexOf("}"); b > a; b = text.lastIndexOf("}", b - 1)) {
          try { const candidate = JSON.parse(text.slice(a, b + 1)); if (candidate.proofToken === token && Object.hasOwn(candidate, "value")) { parsed = candidate; break; } } catch {}
        }
      }
      assert(parsed, "actual tool result missing result token/value");
      value = parsed.value;
    },
    finish() { assert(start && end, "missing actual fabric_exec result"); return { value, toolCallId: start.toolCallId, token, records }; },
  };
};
const promptEvidence = (code, token, message) => {
  const evidence = toolEvidence(code, token);
  let sequence = 0, activeRun, owningRun, ended = false, settled = false;
  return {
    records: evidence.records,
    accept(record) {
      assert(!settled, "event after requested run settled");
      if (record.type === "agent_start") activeRun = ++sequence;
      if ((record.type === "message_start" || record.type === "message_end") && record.message?.role === "user") {
        const content = record.message.content;
        const text = typeof content === "string" ? content : Array.isArray(content) && content.every((c) => c.type === "text")
          ? content.map((c) => c.text).join("") : undefined;
        if (text === message) {
          assert(activeRun !== undefined && (owningRun === undefined || owningRun === activeRun), "prompt run identity mismatch");
          owningRun = activeRun;
        }
      }
      // Prior settlement and unrelated native no-tool replies do not own this prompt.
      if (owningRun === undefined) {
        if (record.type === "agent_settled") activeRun = undefined;
        return;
      }
      assert(activeRun === owningRun, "owning run changed before settlement");
      if (record.type === "tool_execution_start" || record.type === "tool_execution_end") assert(!ended, "tool execution after owning agent_end");
      evidence.accept(record);
      if (record.type === "agent_end") ended = true;
      if (record.type === "agent_settled") {
        assert(ended && record.outcome === "completed", "owning run settled without completed agent_end");
        const value = evidence.finish();
        settled = true;
        activeRun = undefined;
        return value;
      }
    },
  };
};
const assertTurn = (turn, target, host) => {
  assert(turn?.value?.peers?.some((p) => p.id === target && p.host === host), `missing native peer ${target} on ${host}`);
  for (const name of ["steer", "followUp"]) {
    const ack = turn.value[name];
    assert(ack?.queued === true && ack.routed === "mesh" && ack.acknowledged === true && typeof ack.messageId === "string" && ack.messageId.trim(), `${name}: missing mesh acknowledged/queued messageId`);
  }
  assert(turn.value.steer.messageId !== turn.value.followUp.messageId, "send IDs must be distinct");
};
const assertWake = (event, original, recipient) => {
  assert(event && event.topic === original.topic && event.kind === original.kind && event.to === recipient &&
    event.from?.id === original.from.id && event.text === original.text && event.data?.bridge?.id === original.id,
  `missing/wrong original wake ${original.id} for ${recipient}`);
};
const assertNative = (message, needle, customType, id, sender, delivery) => {
  const content = typeof message?.content === "string" ? message.content : (message?.content ?? []).map((c) => c.text ?? "").join("");
  assert(message?.role === "custom" && message.customType === customType && content.includes(needle), "missing native receiver message");
  if (customType === "pi-fabric-inbox") assert(message.details?.ids?.includes(id) && content.includes(`id="${id}"`), "missing native work inbox event ID");
  else {
    const items = message.details?.items ?? [message.details];
    const blocks = [...content.matchAll(/<fabric-agent-message\s+([^>]*)>([\s\S]*?)<\/fabric-agent-message>/g)];
    const xml = (text) => String(text).replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    assert(blocks.length === items.length, "native item/block count mismatch");
    assert(items.some((item, index) => {
      const [, attrs, body] = blocks[index];
      return typeof item?.id === "string" && item.id.trim() && (id === undefined || item.id === id) &&
        item.from?.id === sender && (delivery === undefined || item.delivery === delivery) &&
        attrs.split(/\s+/).includes(`from_id="${xml(item.from.id)}"`) && attrs.split(/\s+/).includes(`delivery="${xml(item.delivery)}"`) &&
        body.split("\n<data>")[0].includes(xml(needle));
    }), "wrong native same-item text/ID/sender/delivery");
  }
  return { customType, id, sender, content, details: message.details };
};
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
const assertNoSurvivors = (processes) => { assert(Object.keys(processes).length === 2 && Object.values(processes).every((v) => v === "none"), "bridge/SSH transport survivors"); };
// END LOOPBACK VALIDATORS

const [scratch, fabricDist, bridgeBin, sshHost, sshKey, sshPort, knownHosts] = process.argv.slice(2);
let MeshStore;
const fleetAgent = process.env.PI_CODING_AGENT_DIR;
const MODEL = process.env.PROOF_MODEL ?? "cliproxyapi/gpt-6.1-sol"; // smarty-dev#2236: Claude P0
const log = (...a) => console.log(new Date().toISOString(), ...a);
const cleanupSleep = (ms) => new Promise((r) => setTimeout(r, ms));
// BEGIN LOOPBACK CANCELLATION (extracted by the inert probe).
const cancellation = () => {
  let stopped;
  const pending = new Set();
  const gate = () => { if (stopped) throw stopped; };
  const subscribe = (reject) => {
    if (stopped) { reject(stopped); return () => {}; }
    pending.add(reject); return () => pending.delete(reject);
  };
  const stop = (error) => {
    if (stopped) return;
    stopped = error;
    for (const reject of [...pending]) reject(error);
    pending.clear();
  };
  const wait = (operation) => {
    gate();
    return new Promise((resolve, reject) => {
      const off = subscribe(reject);
      Promise.resolve(operation).then((value) => { off(); try { gate(); resolve(value); } catch (error) { reject(error); } }, (error) => { off(); reject(error); });
    });
  };
  return { gate, subscribe, stop, wait };
};
const installSignals = (host, fail) => {
  for (const signal of ["SIGTERM", "SIGINT"]) host.on(signal, () => fail(new Error(`proof canceled by ${signal}`)));
};
// END LOOPBACK CANCELLATION
const control = cancellation();
const sleep = (ms) => {
  control.gate();
  return new Promise((resolve, reject) => {
    let off = () => {};
    const timer = setTimeout(() => { off(); resolve(); }, ms);
    off = control.subscribe((error) => { clearTimeout(timer); off(); reject(error); });
  });
};
const results = { status: "RUNNING", startedAt: new Date().toISOString(), fabricDist, bridgeBin, sshHost };
const children = new Set();
// BEGIN LOOPBACK LIFECYCLE (also extracted by the owner-only probe).
const processIdentity = (pid) => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return { pid: Number(pid), state: fields[0], parent: Number(fields[1]), group: Number(fields[2]), session: Number(fields[3]), tick: fields[19] };
  } catch (error) { if (error.code === "ENOENT" || error.code === "ESRCH") return undefined; throw error; }
};
const ownChild = (child, label, requireCleanExit) => {
  const identity = processIdentity(child.pid);
  assert(identity && identity.group === child.pid && identity.session === child.pid, `${label}: not an owned detached group`);
  const owner = { child, label, identity, requireCleanExit, known: new Map([[child.pid, identity.tick]]), closed: false };
  child.once("error", (error) => { owner.error = String(error); });
  child.stdin?.on?.("error", (error) => { owner.error = `stdin: ${error}`; });
  child.once("close", (code, signal) => { owner.closed = true; owner.code = code; owner.signal = signal; });
  children.add(owner);
  return owner;
};
const ownSshd = () => {
  const pid = Number(fs.readFileSync(path.join(scratch, "../sshd.pid"), "utf8").trim());
  assert(Number.isSafeInteger(pid) && pid > 1, "invalid private sshd PID");
  const identity = processIdentity(pid);
  assert(identity, "private sshd missing");
  // OpenSSH may rewrite argv into a single listener process title.
  const args = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").split(/[\0\s]+/);
  assert(args.includes(path.resolve(scratch, "../sshd_config")), "not wrapper's private sshd");
  assert(processIdentity(pid)?.tick === identity.tick, "private sshd identity changed");
  const owner = { label: "private sshd/receiver", identity, tree: true, known: new Map([[pid, identity.tick]]) };
  children.add(owner); return owner;
};
const ownedMembers = (owner) => {
  const leader = processIdentity(owner.identity.pid);
  assert(!leader || (leader.tick === owner.identity.tick &&
    leader.group === owner.identity.group && leader.session === owner.identity.session), `${owner.label}: owner PID reused/group changed`);
  const all = fs.readdirSync("/proc").filter((name) => /^\d+$/.test(name)).map(processIdentity).filter(Boolean);
  owner.bindings ??= new Map([[owner.identity.pid, owner.identity]]);
  for (const p of all) if (owner.known.has(p.pid)) {
    const binding = owner.bindings.get(p.pid);
    assert(owner.known.get(p.pid) === p.tick && (!binding || (p.group === binding.group && p.session === binding.session)),
      `${owner.label}: known member PID reused/group changed`);
  }
  if (owner.tree) {
    // SSH receiver sessions have separate groups: follow authenticated ancestry only.
    const members = all.filter((p) => owner.known.get(p.pid) === p.tick);
    for (let size = -1; size !== members.length;) {
      size = members.length;
      for (const p of all) if (!members.includes(p) && members.some((parent) => parent.pid === p.parent)) {
        assert(BigInt(p.tick) >= BigInt(owner.identity.tick), "pre-existing sshd descendant");
        if (owner.known.has(p.pid)) assert(owner.known.get(p.pid) === p.tick, "sshd descendant PID reused");
        owner.known.set(p.pid, p.tick); owner.bindings.set(p.pid, p); members.push(p);
      }
    }
    return members.filter((p) => p.state !== "Z");
  }
  for (const p of all) if (owner.known.has(p.pid)) {
    assert(owner.known.get(p.pid) === p.tick && p.group === owner.identity.group && p.session === owner.identity.session,
      `${owner.label}: known member PID reused/group changed`);
  }
  const members = all.filter((p) => p.group === owner.identity.group && p.session === owner.identity.session);
  for (const p of members) {
    assert(BigInt(p.tick) >= BigInt(owner.identity.tick), `${owner.label}: pre-existing group member`);
    assert(leader || owner.known.get(p.pid) === p.tick || members.some((m) => owner.known.get(m.pid) === m.tick), `${owner.label}: unauthenticated orphan group`);
    if (owner.known.has(p.pid)) assert(owner.known.get(p.pid) === p.tick, `${owner.label}: member PID reused`);
    owner.known.set(p.pid, p.tick); owner.bindings.set(p.pid, p);
  }
  return members.filter((p) => p.state !== "Z");
};
// BEGIN PIDFD SIGNAL — self-contained production helper, extracted by native probes.
const signalOwned = (member, signalName) => {
  if (!Number.isSafeInteger(member.pid) || member.pid <= 1 || !/^\d+$/.test(member.tick ?? member.start ?? "") ||
      !Number.isSafeInteger(member.group) || !Number.isSafeInteger(member.session) ||
      !["SIGTERM", "SIGKILL"].includes(signalName)) throw new Error("invalid owned signal binding");
  const program = String.raw`import os, signal, sys
pid, start, group, session, name = sys.argv[1:]
pid, group, session = int(pid), int(group), int(session)
fd = None
sent = False
try:
    # Bind the kernel incarnation FIRST. Never fall back to a numeric kill.
    fd = os.pidfd_open(pid)
    with open(f'/proc/{pid}/stat') as stat:
        fields = stat.read().rsplit(') ', 1)[1].split()
    if fields[19] != start or int(fields[2]) != group or int(fields[3]) != session:
        raise RuntimeError('owned process incarnation/group/session changed')
    if fields[0] != 'Z':
        signal.pidfd_send_signal(fd, getattr(signal, name))
        sent = True
except (ProcessLookupError, FileNotFoundError):
    pass
finally:
    if fd is not None:
        os.close(fd)
print('sent' if sent else 'dead')
`;
  const result = execFileSync("python3", ["-c", program, String(member.pid), member.tick ?? member.start,
    String(member.group), String(member.session), signalName],
    { encoding: "utf8", timeout: 2_000, killSignal: "SIGKILL", maxBuffer: 16 * 1024 }).trim();
  if (!["sent", "dead"].includes(result)) throw new Error("invalid pidfd signal result");
  return result === "sent";
};
// END PIDFD SIGNAL
const cleanupChild = async (owner, graceMs = 5_000, termMs = 3_000, killMs = 2_000) => {
  const receipt = { label: owner.label, pid: owner.identity.pid, startTick: owner.identity.tick, errors: [], signals: [] };
  const waitDead = async (ms) => {
    const until = Date.now() + ms;
    do {
      if (ownedMembers(owner).length === 0 && (owner.tree || owner.closed)) return true;
      await cleanupSleep(Math.min(50, Math.max(1, until - Date.now())));
    } while (Date.now() < until);
    return ownedMembers(owner).length === 0 && (owner.tree || owner.closed);
  };
  try {
    ownedMembers(owner); // Capture descendants before EOF can reap the group leader.
    if (owner.child?.stdin && !owner.child.stdin.destroyed && !owner.child.stdin.writableEnded) owner.child.stdin.end();
    let dead = await waitDead(graceMs);
    for (const [signal, ms] of [["SIGTERM", termMs], ["SIGKILL", killMs]]) {
      if (dead) break;
      const members = ownedMembers(owner);
      for (const member of members) {
        if (signalOwned(member, signal)) receipt.signals.push({ pid: member.pid, signal });
      }
      dead = await waitDead(ms);
    }
    receipt.dead = dead;
    assert(dead, `${owner.label}: actual process/group survived cleanup`);
    assert(!owner.error, `${owner.label}: ${owner.error}`);
    if (owner.requireCleanExit) assert(owner.code === 0 && !owner.signal, `${owner.label}: unclean Pi exit ${owner.code}/${owner.signal}`);
  } catch (error) { receipt.errors.push(String(error?.stack ?? error)); }
  receipt.observed = [...owner.known].map(([pid, startTick]) => ({ pid, startTick }));
  receipt.code = owner.code; receipt.signal = owner.signal;
  return receipt;
};
const finalStatus = (proofPassed, cleanup) => proofPassed && cleanup.length > 0 && cleanup.every((r) => r.dead === true && r.errors.length === 0) ? "PASS" : "FAIL";
const rpcReady = (request) => request("get_state", 120_000);
// END LOOPBACK LIFECYCLE
const saveAfterKill = () => { if (results.bridgeKilledAt) fs.writeFileSync(path.join(scratch, "after-kill.json"), JSON.stringify(results, null, 2)); };
let finishing, ownershipMonitor;
const finish = (error) => finishing ??= (async () => {
  if (error) results.failed = String(error?.stack ?? error);
  control.stop(error ?? new Error("proof finishing"));
  results.cleanup = await Promise.all([...children].map((owner) => cleanupChild(owner)));
  clearInterval(ownershipMonitor);
  results.status = finalStatus(results.proofPassed === true && !results.failed, results.cleanup);
  results.finishedAt = new Date().toISOString();
  save(); saveAfterKill();
  if (results.status === "FAIL") console.error("FAILED", results.failed, results.cleanup);
  process.exit(results.status === "PASS" ? 0 : 1);
})();
const fail = (error) => { results.failed ??= String(error?.stack ?? error); control.stop(error); return finish(error); };
installSignals(process, fail);
process.on("unhandledRejection", fail);
process.on("uncaughtException", fail);
const save = () => fs.writeFileSync(path.join(scratch, "results.json"), JSON.stringify(results, null, 2));

const side = (name) => {
  control.gate();
  const root = path.join(scratch, name);
  const agentDir = path.join(root, "agent");
  const cwd = path.join(root, `${name}-work`);
  const mesh = path.join(root, "mesh");
  fs.mkdirSync(agentDir, { recursive: true });
  fs.mkdirSync(cwd, { recursive: true });
  // Credentials stay in the fleet agent dir: link, never copy.
  for (const file of ["auth.json", "models.json"]) fs.symlinkSync(path.join(fleetAgent, file), path.join(agentDir, file));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ packages: [], extensions: [] }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { root: mesh } }));
  return { name, root, agentDir, cwd, mesh, store: new MeshStore(mesh, 256 * 1024, 500, { lockTimeoutMs: 2000 }) };
};

const startPi = (s) => {
  control.gate();
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(PI_|HERDR|FABRIC|SMARTY_ROLE$)/.test(k)));
  env.PI_CODING_AGENT_DIR = s.agentDir;
  // ponytail: proof calls perform one explicit native provider operation, not
  // general work-agent work. Retain context/profile/trust/mesh; allowlist the
  // extension tool and suppress optional skill discovery, not context files.
  const child = spawn("timeout", ["--kill-after=10s", "900s", "nice", "-n", "19", "pi", "--mode", "rpc", "-ne", "-e", fabricDist, "--tools", "fabric_exec", "--no-skills", "--model", MODEL, "--thinking", "medium", "--session-dir", path.join(s.root, "sessions")],
    { cwd: s.cwd, env, stdio: ["pipe", "pipe", "pipe"], detached: true });
  const owner = ownChild(child, `${s.name} Pi`, true);
  const file = path.join(s.root, "rpc-stdout.jsonl");
  const out = fs.createWriteStream(file);
  child.stderr.pipe(fs.createWriteStream(path.join(s.root, "rpc-stderr.log")));
  const listeners = new Set();
  let buffer = "";
  child.stdout.on("data", (chunk) => {
    ownedMembers(owner); // Track actual Pi and descendants, not only the timeout wrapper.
    out.write(chunk);
    buffer += chunk.toString("utf8");
    let at;
    while ((at = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, at).replace(/\r$/, "");
      buffer = buffer.slice(at + 1);
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      for (const listener of listeners) listener(record);
    }
  });
  const request = (type, timeoutMs = 30_000) => new Promise((resolve, reject) => {
    control.gate();
    let completed = false, off = () => {};
    const id = randomUUID();
    const done = (error, value) => { if (completed) return; completed = true; off(); clearTimeout(timer); listeners.delete(on); child.off("close", dead); child.off("error", dead); error ? reject(error) : resolve(value); };
    const dead = () => done(new Error(`${s.name}: child died during ${type}`));
    const on = (record) => { if (record.type === "response" && record.id === id) done(record.success === false ? new Error(record.error) : null, record.data); };
    const timer = setTimeout(() => done(new Error(`${s.name}: ${type} timed out`)), timeoutMs);
    listeners.add(on);
    off = control.subscribe((error) => done(error));
    child.once("close", dead); child.once("error", dead);
    if (owner.closed || owner.error) return dead();
    child.stdin.write(JSON.stringify({ id, type }) + "\n", (error) => { if (error) done(error); });
  });
  const ready = rpcReady(request);
  const prompt = async (program, timeoutMs = 600_000) => {
    control.gate();
    await control.wait(ready); // Explicit cold extension binding budget, not a fixed sleep/30s guess.
    const deadline = Date.now() + timeoutMs;
    const remaining = () => { control.gate(); const ms = deadline - Date.now(); assert(ms > 0, `${s.name}: operation deadline`); return ms; };
    await waitFor(`${s.name} idle`, async () => !(await request("get_state", Math.min(30_000, remaining()))).isStreaming, Math.min(60_000, remaining()));
    const token = randomUUID();
    const code = `const value = await (async () => { ${program}\n})(); return { proofToken: ${JSON.stringify(token)}, value };`;
    const message = `Call fabric_exec exactly once with this exact code, resultFormat "json", and timeoutMs ${Math.min(timeoutMs, 180_000)}. Do not call any other tool. Then reply only OK.\n\n${code}`;
    const evidence = promptEvidence(code, token, message);
    results.toolExecutions ??= [];
    results.toolExecutions.push({ side: s.name, code, token, records: evidence.records });
    return new Promise((resolve, reject) => {
      const id = randomUUID();
      let completed = false, off = () => {};
      const done = (error, value) => { if (completed) return; completed = true; off(); clearTimeout(timer); listeners.delete(on); child.off("close", dead); child.off("error", dead); save(); error ? reject(error) : resolve(value); };
      const dead = () => done(new Error(`${s.name}: child died during requested run`));
      const on = (record) => {
        try {
          if (record.type === "response" && record.id === id && record.success === false) throw new Error(record.error);
          const value = evidence.accept(record);
          if (value !== undefined) done(null, value);
        } catch (error) { done(error); }
      };
      const timer = setTimeout(() => done(new Error(`${s.name}: prompt/tool timed out`)), remaining());
      listeners.add(on);
      off = control.subscribe((error) => done(error));
      child.once("close", dead); child.once("error", dead);
      if (owner.closed || owner.error) return dead();
      child.stdin.write(JSON.stringify({ id, type: "prompt", message }) + "\n", (error) => { if (error) done(error); });
    });
  };
  // The <fabric-agent-message ... delivery="..."> (or inbox) block that carried a needle into this session.
  const received = async (needle, customType, id, sender, delivery) => {
    control.gate();
    const raw = fs.readFileSync(file, "utf8");
    const messages = (await request("get_messages"))?.messages ?? [];
    control.gate();
    for (const message of messages) {
      try { return assertNative(message, needle, customType, id, sender, delivery); } catch {}
    }
    for (const line of raw.split("\n")) {
      if (!line.includes(needle)) continue;
      let record;
      try { record = JSON.parse(line); } catch { continue; }
      if (record.type !== "message_end" || !record.message || record.message.role === "assistant" || record.message.role === "toolResult") continue;
      const { content } = record.message;
      const text = typeof content === "string" ? content : (content ?? []).map((c) => c.text ?? "").join("");
      if (!text.includes(needle)) continue;
      try { return assertNative(record.message, needle, customType, id, sender, delivery); } catch {}
    }
    return undefined;
  };
  return { child, prompt, received, ready };
};

const roots = (s, mirrored) => s.store.listAll("topology/participants/", { fresh: true })
  .map((e) => e.value).filter((v) => v?.kind === "root" && (mirrored ? v.remoteHost : !v.remoteHost));
const waitFor = async (what, check, ms = 120_000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) { control.gate(); const v = await control.wait(check()); if (v) return v; await sleep(250); }
  throw new Error(`timed out waiting for ${what}`);
};
const run = (code) => { control.gate(); return code; };

try {
control.gate();
const sshdOwner = ownSshd();
ownershipMonitor = setInterval(() => {
  try { for (const owner of children) ownedMembers(owner); } catch (error) { fail(error); }
}, 500);
({ MeshStore } = await control.wait(import(path.join(path.dirname(fabricDist), "mesh.js"))));
const A = side("dev1"), B = side("forge");
const piA = startPi(A), piB = startPi(B);
await Promise.all([piA.ready, piB.ready]);
results.bootstrap = await Promise.all([piA, piB].map((pi) => pi.prompt(run("const main = await agents.main();\nreturn { id: main.id, local: main.local };"))));
const [rootA] = await waitFor("dev1 root", () => roots(A, false)[0] && roots(A, false));
const [rootB] = await waitFor("forge root", () => roots(B, false)[0] && roots(B, false));
results.roots = { dev1: rootA.id, forge: rootB.id };
for (const [index, root] of [rootA, rootB].entries()) assert(results.bootstrap[index]?.value?.id === root.id && results.bootstrap[index].value.local === true, "native Main bootstrap mismatch");
log("roots", rootA.id, rootB.id);

// The bridge: the hub side on dev1's mesh; the transport is the bridge's own ssh argv to the loopback
// sshd, whose forced command runs `mesh-bridge agent --mesh <forge mesh> --peer dev1`.
const bridgeLog = path.join(scratch, "bridge.log");
control.gate();
const bridge = spawn("node", [bridgeBin, "run", "--mesh", A.mesh, "--name", "dev1", "--remote", "forge",
  "--cursor", path.join(scratch, "bridge-cursor.json"), "--ssh", sshHost, "--ssh-key", sshKey, "--ssh-port", sshPort, "--ssh-known-hosts", knownHosts],
  { stdio: ["ignore", "inherit", fs.openSync(bridgeLog, "a")], detached: true });
const bridgeOwner = ownChild(bridge, "bridge", false);
await waitFor("forge root mirrored into dev1", () => roots(A, true).find((r) => r.id === rootB.id && r.remoteHost === "forge"));
await waitFor("dev1 root mirrored into forge", () => roots(B, true).find((r) => r.id === rootA.id && r.remoteHost === "dev1"));
results.transport = execFileSync("ps", ["-o", "pid,args", "--ppid", String(bridge.pid)], { encoding: "utf8" }).trim();
log("mirrors present both ways; transport:", results.transport);

// 1. Discovery with host set, then steer and followUp dev1 -> forge.
const tag = () => randomUUID().slice(0, 8);
const n = { s1: `LOOP-STEER-D2F-${tag()}`, f1: `LOOP-FOLLOWUP-D2F-${tag()}`, s2: `LOOP-STEER-F2D-${tag()}`, f2: `LOOP-FOLLOWUP-F2D-${tag()}` };
results.dev1Turn = await piA.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.s1}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootB.id)}, ${JSON.stringify(`${n.f1}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
assertTurn(results.dev1Turn, rootB.id, "forge");
results.forgeReceived = { steer: await waitFor("forge got the steer", () => piB.received(n.s1, "pi-fabric-agent-message", results.dev1Turn.value.steer.messageId, rootA.id, "steer"), 180_000), followUp: await waitFor("forge got the followUp", () => piB.received(n.f1, "pi-fabric-agent-message", results.dev1Turn.value.followUp.messageId, rootA.id, "followUp"), 240_000) };
log("forge received", JSON.stringify(results.forgeReceived));
await sleep(5_000);

// 2. The other direction: forge discovers dev1, steers and followUps it.
results.forgeTurn = await piB.prompt(run(
  `const peers = await agents.peers();\nconst steer = await agents.steer(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.s2}: reply with just OK`)});\nconst followUp = await agents.followUp(${JSON.stringify(rootA.id)}, ${JSON.stringify(`${n.f2}: reply with just OK`)});\nreturn { peers: peers.filter((p) => p.host !== undefined).map((p) => ({ id: p.id, host: p.host, label: p.label })), steer, followUp };`));
assertTurn(results.forgeTurn, rootA.id, "dev1");
results.dev1Received = { steer: await waitFor("dev1 got the steer", () => piA.received(n.s2, "pi-fabric-agent-message", results.forgeTurn.value.steer.messageId, rootB.id, "steer"), 180_000), followUp: await waitFor("dev1 got the followUp", () => piA.received(n.f2, "pi-fabric-agent-message", results.forgeTurn.value.followUp.messageId, rootB.id, "followUp"), 240_000) };
log("dev1 received", JSON.stringify(results.dev1Received));
await sleep(5_000);

// 3. Factory wakes, published on the dev1 hub mesh exactly as factory_host.py owner_wake does.
const pr = `LOOP-PRWAKE-${tag()}`, lane = `LOOP-LANEWAKE-${tag()}`, work = `LOOP-WORK-${tag()}`;
const now = Date.now();
// 3a. ops.owner pr.wake (factory-host:owner) addressed to the forge Main.
control.gate();
const prWake = await control.wait(A.store.publish({ topic: "ops.owner", kind: "pr.wake", from: { id: "factory-host:owner", name: "factory-owner", kind: "main" },
  to: rootB.id, text: `${pr}: PR smarty-dev#2045 needs its owner`, data: { rootId: rootB.id, repo: "Smarty-Pants-Inc/smarty-dev", pr: 2045 } }));
// 3b. The factory's lane wake: the followUp control command owner_wake publishes for a lane session.
control.gate();
const laneWake = await control.wait(A.store.publish({ topic: "fabric.control.command", kind: "followUp", from: { id: "factory-host:stuck-work", name: "factory-stuck-work", kind: "main" },
  to: rootB.id, text: `${lane}: reply with just OK`, data: { version: 1, commandId: randomUUID().replaceAll("-", ""), targetId: rootB.id, operation: "followUp",
    replyTo: "factory-host:stuck-work", message: `${lane}: reply with just OK`, data: { ref: "smarty-dev#2045" }, requestedAt: now, deadlineAt: now + 120_000 } }));
// 3c. A fleet.work event (the #754 shadow record / work inbox), kind p0, from the dev1 Main.
control.gate();
const workEvent = await control.wait(A.store.publish({ topic: "fleet.work.smarty-dev.2045", kind: "p0", from: { id: rootA.id, name: "main", kind: "main" },
  to: rootB.id, text: `${work}: reply with just OK`, data: { ref: "smarty-dev#2045", key: work } }));
results.wakesPublished = { prWake: prWake.id, laneWake: laneWake.id, work: workEvent.id };
const bridged = (id) => B.store.read({ after: 0, limit: 5_000 }).find((e) => e.data?.bridge?.id === id);
const onForge = {
  prWake: await waitFor("pr.wake on forge", () => bridged(prWake.id), 30_000),
  laneWake: await waitFor("lane wake on forge", () => bridged(laneWake.id), 30_000),
  work: await waitFor("fleet.work on forge", () => bridged(workEvent.id), 30_000),
};
for (const [key, original] of Object.entries({ prWake, laneWake, work: workEvent })) assertWake(onForge[key], original, rootB.id);
results.wakesOnForgeMesh = Object.fromEntries(Object.entries(onForge).map(([k, e]) => [k, { sequence: e.sequence, topic: e.topic, kind: e.kind, from: e.from.id, to: e.to, bridge: e.data.bridge }]));
log("wakes on forge mesh", JSON.stringify(results.wakesOnForgeMesh));
results.forgeWokeByLaneWake = await waitFor("forge Main woken by the lane followUp", () => piB.received(lane, "pi-fabric-agent-message", undefined, "factory-host:stuck-work", "followUp"), 180_000);
log("lane wake received", JSON.stringify(results.forgeWokeByLaneWake));
results.forgeWokeByWorkInbox = await waitFor("forge Main woken by fleet.work (root inbox)", () => piB.received(work, "pi-fabric-inbox", onForge.work.id), 240_000);
log("work inbox", JSON.stringify(results.forgeWokeByWorkInbox));
await sleep(5_000);
// The forge Main reads the pr.wake addressed to it (ops.owner has no Main handler; it is a supervisor's input).
results.forgeReadsPrWake = await piB.prompt(run(
  `const self = (await agents.main()).id;\nconst events = await mesh.read({ topic: "ops.owner", to: self, limit: 50 });\nreturn { self, events }; `));
assert(results.forgeReadsPrWake.value?.self === rootB.id, "pr.wake read wrong Main");
const readWake = results.forgeReadsPrWake.value?.events?.find((e) => e.data?.bridge?.id === prWake.id);
assertWake(readWake, prWake, rootB.id);
log("forge read pr.wake", JSON.stringify(readWake));
save();

// 4. Kill the bridge (SIGKILL: no clean withdrawal). Both Mains then try to reach the other side,
// exactly once per direction. A later named lapse must never rescue the first send.
control.gate();
ownedMembers(sshdOwner);
ownedMembers(bridgeOwner);
assert(processIdentity(bridge.pid)?.tick === bridgeOwner.identity.tick, "bridge identity changed before kill");
const ledgerStarts = { dev1ToForge: await exclusiveLedger(A.store, () => captureLedger(A.store), control.gate, control.wait),
  forgeToDev1: await exclusiveLedger(B.store, () => captureLedger(B.store), control.gate, control.wait) };
control.gate();
const postkillMarkers = { dev1ToForge: `LOOP-POSTKILL-D2F-${randomUUID()}`, forgeToDev1: `LOOP-POSTKILL-F2D-${randomUUID()}` };
assert(bridge.kill("SIGKILL"), "bridge SIGKILL failed");
const killedAt = Date.now();
results.bridgeKilledAt = new Date(killedAt).toISOString();
const firstSendProbe = async (pi, source, target, marker) => {
  const requestAt = Date.now();
  // No retry or pre-send discovery. Native timing excludes idle/model/settlement overhead.
  const turn = await pi.prompt(run(
    `const nativeStartedAt = Date.now(); let outcome; try { outcome = { delivered: await agents.steer(${JSON.stringify(target)}, ${JSON.stringify(marker)}) }; } catch (error) { outcome = { error: String(error?.message ?? error) }; } const nativeCompletedAt = Date.now(); return { nativeStartedAt, nativeCompletedAt, nativeMs: nativeCompletedAt - nativeStartedAt, ...outcome };`), 180_000);
  const completedAt = Date.now();
  return { ...turn, source, target, marker, value: [{ ...turn.value, nativeTimestampDomain: "native-execution-host-local",
    driverTimestampDomain: "local-driver", requestAt, completedAt, requestAfterKillMs: requestAt - killedAt,
    completionAfterKillMs: completedAt - killedAt, rpcModelDurationMs: completedAt - requestAt }] };
};
const after = await Promise.allSettled([firstSendProbe(piA, rootA.id, rootB.id, postkillMarkers.dev1ToForge), firstSendProbe(piB, rootB.id, rootA.id, postkillMarkers.forgeToDev1)]);
results.afterKill = Object.fromEntries(after.map((entry, i) => [["dev1ToForge", "forgeToDev1"][i], entry.status === "fulfilled" ? entry.value : { error: String(entry.reason?.stack ?? entry.reason) }]));
save(); saveAfterKill();
assert(after.every((entry) => entry.status === "fulfilled"), "required first post-kill prompt failed");
results.firstPostkillSends = {
  dev1ToForge: assertFirstSend(results.afterKill.dev1ToForge.value, rootB.id, "forge", killedAt),
  forgeToDev1: assertFirstSend(results.afterKill.forgeToDev1.value, rootA.id, "dev1", killedAt),
};
save(); saveAfterKill();
// The original roots must still work; do not replace them after transport loss.
results.nativeMainPeersAfterDeath = await Promise.all([[piA, rootA.id, rootB.id], [piB, rootB.id, rootA.id]].map(async ([pi, ownId, target]) => {
  const turn = await pi.prompt(run(`const main = await agents.main(); const peers = await agents.peers(); return { main: { id: main.id, local: main.local }, ownTarget: peers.filter(p => p.id === ${JSON.stringify(target)}).map(p => ({ id: p.id, host: p.host })) };`));
  assert(turn.value?.main?.id === ownId && turn.value.main.local === true && turn.value.ownTarget.length === 0, "original native main/peers unusable after bridge loss or lapsed target still visible");
  return turn;
}));
await waitFor("natural owned bridge/SSH group death", () => ownedMembers(bridgeOwner).length === 0, 15_000);
await sleep(3_000);
// pgrep exits 1 when nothing matches; any other failure is an error, not "none".
const regexEscape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
control.gate();
results.processesAfterKill = Object.fromEntries([["bridge or agent", `mesh-bridge (run|agent) --mesh ${regexEscape(A.mesh)}|agent --mesh ${regexEscape(B.mesh)}`], ["ssh transport", `(^|/)ssh .*${regexEscape(sshKey)}`]].map(([what, pattern]) => {
  try { return [what, execFileSync("pgrep", ["-af", "--", pattern], { encoding: "utf8" }).trim()]; }
  catch (error) { if (error.status === 1) return [what, "none"]; throw error; }
}));
results.bridgeLog = fs.readFileSync(bridgeLog, "utf8");
save(); saveAfterKill();
assertNoSurvivors(results.processesAfterKill);
// Native settlement can precede best-effort publication/cancel completion. Observe
// after two bounded 10s mesh-lock windows; native clocks and first-only sends are unchanged.
results.postkillLedgerReadNotBefore = Math.max(...Object.values(results.afterKill).map((v) => v.value[0].completedAt)) + 20_000;
await sleep(Math.max(0, results.postkillLedgerReadNotBefore - Date.now()));
results.postkillLedgers = {};
for (const [direction, store, source, target, host] of [["dev1ToForge", A.store, rootA.id, rootB.id, "forge"],
  ["forgeToDev1", B.store, rootB.id, rootA.id, "dev1"]]) {
  const senderFindings = [...ledgerStarts[direction].senderFindings];
  const value = await exclusiveLedger(store, () => assertPostkillLedger(readLedger(store, ledgerStarts[direction], senderFindings),
    source, target, postkillMarkers[direction], host, results.firstPostkillSends[direction].condition, senderFindings), control.gate, control.wait);
  control.gate();
  results.postkillLedgers[direction] = { window: ledgerStarts[direction], ...value };
}
save(); saveAfterKill();
log("RESULTS", JSON.stringify(results.afterKill, null, 2), results.processesAfterKill);
results.proofPassed = true;
} catch (error) { results.failed = String(error?.stack ?? error); }
finally { await finish(); }
