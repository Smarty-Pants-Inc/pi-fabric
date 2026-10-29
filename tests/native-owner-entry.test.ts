// Keep: real session-writer trust boundary + kernel/custody crash counterexamples.
// Linux-only kernel coverage. Explicit approved SDK + Node and a FRESH compiled
// candidate are prerequisites. Ordinary CI declares missing-input skips; REQUIRED=1 fails them. No model/SDK mocks.
// Parent MUST run via its live launcherTestTemp owner and archive the WHOLE root.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { createNativeActorOwner, type NativeActorOwner, type NativeOwnerCliShape } from "./helpers/native-actor-owner.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";
import { NATIVE_SDK_DIRECTORY, assertApprovedNativeSdk, nativeSdkTestPrerequisite } from "./helpers/native-sdk-no-model.js";

// Expected skip: missing Linux/approved SDK files/executable Node/env in ordinary CI only.
const nativeSkipReason = nativeSdkTestPrerequisite();
import { residentProcessAlive } from "../src/residency/process-identity.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const actorId = "88888888888848888888888888888888"; // synthetic test actor, never production
const owners: NativeActorOwner[] = [];
afterEach(async () => { for (const owner of owners.splice(0)) await owner.close(); }, 90_000);
const hash = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const realOwner = async (mainLaunch: "direct" | "launcher" = "direct"): Promise<NativeActorOwner> => {
  const sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST, nodeBinary = process.env.FABRIC_NATIVE_TEST_NODE;
  if (!sdkDirectory || !nodeBinary) throw new Error("F1(b) BLOCKED: explicit approved FABRIC_NATIVE_TEST_PI_SDK_DIST + FABRIC_NATIVE_TEST_NODE required");
  expect(sdkDirectory).toBe(NATIVE_SDK_DIRECTORY); assertApprovedNativeSdk(sdkDirectory);
  const owner = await createNativeActorOwner({ candidateRoot: repo, sdkDirectory, nodeBinary, actorId, installCandidate: true, mainLaunch });
  owners.push(owner); return owner;
};
const inode = (file: string) => { const stat = fs.lstatSync(file); return { dev: stat.dev, ino: stat.ino }; };
const snapshot = (owner: NativeActorOwner) => ({ session: fs.readFileSync(owner.sessionFile), pin: fs.readFileSync(owner.sessionPin),
  sessionInode: inode(owner.sessionFile), pinInode: inode(owner.sessionPin), file: fs.realpathSync(owner.sessionFile) });
const unchanged = (owner: NativeActorOwner, before: ReturnType<typeof snapshot>): void => {
  expect(snapshot(owner)).toEqual(before);
};
const fenceDescriptors = (owner: NativeActorOwner, pid: number): string[] => {
  const lock = inode(`${owner.sessionFile}.main.lock`);
  return fs.readdirSync(`/proc/${pid}/fd`).filter(name => {
    try { const stat = fs.statSync(`/proc/${pid}/fd/${name}`); return stat.dev === lock.dev && stat.ino === lock.ino; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
  });
};
const assertInstalled = (owner: NativeActorOwner): void => {
  const receipt = JSON.parse(fs.readFileSync(path.join(owner.root, "candidate-install.json"), "utf8")) as {
    candidateRoot: string; sourceCandidateRoot: string; files: { path: string; sha256: string; bytes: number }[];
  };
  expect(receipt.candidateRoot).toBe(owner.candidateRoot);
  expect(receipt.sourceCandidateRoot).toBe(owner.sourceCandidateRoot);
  expect(owner.candidateRoot).not.toBe(owner.sourceCandidateRoot);
  expect(owner.nativeOwnerEntry).toBe(path.join(owner.root, "candidate-install/dist/worker/native-owner-entry.js"));
  expect(fs.realpathSync(owner.nativeOwnerEntry)).toBe(owner.nativeOwnerEntry);
  expect(fs.lstatSync(owner.nativeOwnerEntry).isSymbolicLink()).toBe(false);
  for (const relative of ["dist/worker/native-owner-entry.js", "dist/worker/task-entry.js", "dist/residency/pi-entry.js", "dist/index.js"]) {
    const installed = receipt.files.find(file => file.path === relative);
    expect(installed).toBeDefined();
    expect(hash(path.join(owner.candidateRoot, relative))).toBe(installed!.sha256);
    expect(hash(path.join(owner.sourceCandidateRoot, relative))).toBe(installed!.sha256);
  }
  const generation = JSON.parse(fs.readFileSync(path.join(owner.root, "generation-evidence.json"), "utf8")) as Record<string, string>;
  expect(generation.nativeOwnerEntry).toBe(owner.nativeOwnerEntry);
  expect(generation.nativeOwnerEntrySha256).toBe(hash(owner.nativeOwnerEntry));
  expect(generation.nodeBinarySha256).toBe(hash(generation.nodeBinary!));
  expect(generation.sdkDirectory).toBe(NATIVE_SDK_DIRECTORY);
};
const noModel = (owner: NativeActorOwner): void => {
  const starts = owner.observations().filter(event => event.event === "session-start");
  expect(starts.length).toBeGreaterThanOrEqual(2);
  expect(starts.every(event => event.selectedModel === false && event.unconfigured === true)).toBe(true);
  expect(starts).toContainEqual(expect.objectContaining({ role: "main", pid: owner.mainPid(), projectTrusted: true,
    rootId: owner.pin.rootId, sessionFile: owner.sessionFile }));
  expect(starts.some(event => event.role === "resident")).toBe(true);
  expect(owner.observations().filter(event => String(event.event).startsWith("FORBIDDEN-"))).toEqual([]);
  expect(owner.callbacks()).toEqual([]);
};
const refusesAll = async (owner: NativeActorOwner, before: ReturnType<typeof snapshot>): Promise<void> => {
  const main = owner.mainIdentity()!, resident = owner.owner();
  for (const attempt of [() => owner.attemptSecondMain(), () => owner.attemptSessionAccess("verify"), () => owner.attemptSessionAccess("provision")]) {
    const result = await attempt();
    expect(result.pid).toBeGreaterThan(1); expect(result.code).not.toBeNull(); expect(result.code).not.toBe(0);
    expect(result.signal).toBeNull(); expect(result.stderr).toContain("NATIVE_OWNER_SESSION_BUSY");
    unchanged(owner, before);
    // Native loader delegates to the UNMODIFIED approved SDK. No SDK load at all
    // in refused processes, not just a zero model/extension callback count.
    expect(owner.sdkImports().filter(event => event.pid === result.pid)).toEqual([]);
    expect(owner.observations().filter(event => event.pid === result.pid)).toEqual([]);
    expect(owner.mainIdentity()).toEqual(main);
    expect((await owner.request("get_state")).success).toBe(true);
    expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
    expect(owner.owner().token).toBe(resident.token);
  }
};
const exactRestartAppend = (owner: NativeActorOwner, before: Buffer, started: number, ready: number): void => {
  const bytes = fs.readFileSync(owner.sessionFile);
  expect(bytes.subarray(0, before.length)).toEqual(before);
  expect(before.at(-1)).toBe(0x0a);
  const prior = before.toString("utf8").trimEnd().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
  expect(prior[0]).toMatchObject({ type: "session", id: owner.pin.sessionId, cwd: owner.cwd });
  expect(owner.binding.rootId).toBe(`session:${owner.pin.sessionId}`);
  const append = bytes.subarray(before.length), lines = append.toString("utf8").split("\n");
  expect(lines).toHaveLength(2); expect(lines[1]).toBe("");
  const entry = JSON.parse(lines[0]!) as Record<string, unknown>;
  expect(Object.keys(entry)).toEqual(["type", "id", "parentId", "timestamp", "thinkingLevel"]);
  expect(entry.type).toBe("thinking_level_change"); expect(entry.thinkingLevel).toBe("off");
  expect(entry.id).toMatch(/^(?:[0-9a-f]{8}|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/);
  expect(prior.some(value => value.id === entry.id)).toBe(false);
  expect(entry.parentId).toBe(prior.filter(value => value.type !== "session").at(-1)?.id ?? null);
  expect(typeof entry.timestamp).toBe("string");
  const timestamp = entry.timestamp as string;
  expect(timestamp).toBe(new Date(timestamp).toISOString());
  expect(Date.parse(timestamp)).toBeGreaterThanOrEqual(started); expect(Date.parse(timestamp)).toBeLessThanOrEqual(ready);
  expect(append).toEqual(Buffer.from(`${JSON.stringify(entry)}\n`));
};

describe.skipIf(!!nativeSkipReason)(`F1(b) actual candidate-installed native owner entry${nativeSkipReason ? ` [SKIP: ${nativeSkipReason}]` : ""}`, () => {
  it.each(["pre-ready", "sdk-append"] as const)("Q1 SIGKILL of actual Main at %s releases its proven fence; same pinned inode survives actual SDK parser/restart", async barrier => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
    await owner.start({ barrier }); // Returns at observed kernel T, explicitly NOT READY.
    const identity = owner.mainIdentity()!;
    expect(owner.mainLive()).toBe(true); expect(fenceDescriptors(owner, identity.pid)).toHaveLength(1);
    expect(fs.existsSync(path.join(owner.residencyRoot, "maintenance-ready.json"))).toBe(false);
    expect(owner.observations()).toEqual([]); expect(owner.callbacks()).toEqual([]);
    const receipt = JSON.parse(fs.readFileSync(path.join(owner.root, "startup-1.barrier.json"), "utf8"));
    expect(receipt.pid).toBe(identity.pid);
    let partial = Buffer.alloc(0);
    if (barrier === "pre-ready") {
      expect(receipt.event).toBe("before-first-sdk-load");
      expect(receipt.proof).toMatchObject(lock);
      expect(receipt.proof.info).toMatch(/^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s/m);
      unchanged(owner, before);
    } else {
      const call = JSON.parse(fs.readFileSync(path.join(owner.root, "startup-1.barrier.json.sdk-call.json"), "utf8"));
      expect(call.pid).toBe(identity.pid); expect(call.event).toBe("actual-sdk-appendFileSync-invocation");
      expect(call.stack).toContain(`${NATIVE_SDK_DIRECTORY}/core/session-manager.js`);
      expect(call.proof).toMatchObject(lock); expect(call.proof.info).toMatch(/FLOCK\s+ADVISORY\s+WRITE/);
      const requested = Buffer.from(call.dataBase64, "base64");
      const line = JSON.parse(requested.toString("utf8"));
      expect(Object.keys(line)).toEqual(["type", "id", "parentId", "timestamp", "thinkingLevel"]);
      expect(line).toMatchObject({ type: "thinking_level_change", thinkingLevel: "off" });
      expect(receipt).toMatchObject({ event: "real-libc-write-short-return", ...before.sessionInode,
        beforeSize: before.session.length, requested: requested.length });
      expect(receipt.written).toBeGreaterThan(0); expect(receipt.written).toBeLessThan(requested.length);
      expect(receipt.delegated).toBe(receipt.written);
      partial = requested.subarray(0, receipt.written);
      expect(fs.readFileSync(owner.sessionFile)).toEqual(Buffer.concat([before.session, partial]));
      expect(receipt.afterSize).toBe(before.session.length + partial.length);
      expect(partial.at(-1)).not.toBe(0x0a);
      expect(() => JSON.parse(partial.toString("utf8"))).toThrow();
    }
    const atBarrier = snapshot(owner);
    // While the actual stopped writer retains its OFD, no verify or Main can write.
    for (const attempt of [() => owner.attemptSecondMain(), () => owner.attemptSessionAccess("verify")]) {
      const result = await attempt(); expect(result.code).not.toBe(0); expect(result.signal).toBeNull();
      expect(result.stderr).toContain("NATIVE_OWNER_SESSION_BUSY");
      expect(owner.sdkImports().filter(value => value.pid === result.pid)).toEqual([]);
      unchanged(owner, atBarrier);
    }
    const startedNs = process.hrtime.bigint(), boundMs = 2000;
    await owner.crashMain(); // Exact recorded ChildProcess, positive death + closed stdout/stderr.
    const death = JSON.parse(fs.readFileSync(path.join(owner.root, "main-1.death.json"), "utf8"));
    const closed = JSON.parse(fs.readFileSync(path.join(owner.root, "main-1.close.json"), "utf8"));
    expect(death).toMatchObject(identity); expect(["absent", "reused", "zombie"]).toContain(death.evidence);
    expect(closed).toMatchObject({ pid: identity.pid, signal: "SIGKILL", stdoutClosed: true, stderrClosed: true });
    const verify = await owner.attemptSessionAccess("verify", { startedNs, boundMs });
    expect(verify.code).toBe(0); expect(JSON.parse(verify.stdout)).toEqual(owner.pin);
    const releaseVerifiedMs = Number(process.hrtime.bigint() - startedNs) / 1_000_000;
    expect(releaseVerifiedMs).toBeLessThanOrEqual(boundMs);
    const parsedBytes = fs.readFileSync(owner.sessionFile);
    // Real 031c parser skips the malformed final record and repairs ONLY its
    // missing newline. This pre-hook SDK write is a named residual, not Fabric.
    expect(parsedBytes).toEqual(Buffer.concat([before.session, partial, ...(partial.length ? [Buffer.from("\n")] : [])]));
    const start = Date.now(); await owner.start(); const ready = Date.now();
    expect(owner.mainPid()).not.toBe(identity.pid); expect(fenceDescriptors(owner, owner.mainPid()!)).toHaveLength(1);
    expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
    expect(inode(owner.sessionFile)).toEqual(before.sessionInode); expect(inode(owner.sessionPin)).toEqual(before.pinInode);
    expect(fs.readFileSync(owner.sessionPin)).toEqual(before.pin); expect(owner.binding.rootId).toBe(owner.pin.rootId);
    if (!partial.length) exactRestartAppend(owner, before.session, start, ready);
    else {
      const bytes = fs.readFileSync(owner.sessionFile);
      expect(bytes.subarray(0, parsedBytes.length)).toEqual(parsedBytes);
      const lines = bytes.subarray(parsedBytes.length).toString("utf8").split("\n");
      expect(lines).toHaveLength(2); expect(lines[1]).toBe("");
      const line = JSON.parse(lines[0]!);
      expect(Object.keys(line)).toEqual(["type", "id", "parentId", "timestamp", "thinkingLevel"]);
      expect(line).toMatchObject({ type: "thinking_level_change", thinkingLevel: "off",
        parentId: before.session.toString("utf8").trimEnd().split("\n")
          .map(value => JSON.parse(value) as Record<string, unknown>)
          .filter(value => value.type !== "session").at(-1)?.id ?? null });
      expect(Date.parse(line.timestamp)).toBeGreaterThanOrEqual(start); expect(Date.parse(line.timestamp)).toBeLessThanOrEqual(ready);
    }
    noModel(owner);
    fs.writeFileSync(path.join(owner.root, "q1-crash-restart.json"), JSON.stringify({ barrier, identity, death, closed, lock,
      pin: owner.pin, beforeBytes: before.session.length, partialBytes: partial.length, releaseVerifiedMs, boundMs,
      verification: verify, restartPid: owner.mainPid(), start, ready }), { mode: 0o600, flag: "wx" });
  }, 180_000);

  it("fences the ACTUAL executable before all SDK/session work; kernel Main death alone permits same-file restart", async () => {
    const owner = await realOwner(); assertInstalled(owner); await owner.start();
    const identity = owner.mainIdentity()!, resident = owner.owner(), before = snapshot(owner);
    // The launch preload runs before argv rewriting: this is the installed
    // executable itself, NOT the argv fixture/source module impersonating it.
    expect(owner.ownedProcesses()).toContainEqual(expect.objectContaining({ pid: identity.pid, started: identity.started,
      argv: expect.arrayContaining([owner.nativeOwnerEntry, NATIVE_SDK_DIRECTORY]) }));
    expect(owner.sdkImports().some(event => event.pid === identity.pid && event.url.endsWith("/core/session-manager.js"))).toBe(true);
    const descriptors = fenceDescriptors(owner, identity.pid);
    expect(descriptors).toHaveLength(1);
    const flags = /^flags:\s+([0-7]+)$/m.exec(fs.readFileSync(`/proc/${identity.pid}/fdinfo/${descriptors[0]}`, "utf8"));
    expect(flags).not.toBeNull(); expect(Number.parseInt(flags![1]!, 8) & 0o2000000).not.toBe(0); // CLOEXEC
    expect(fenceDescriptors(owner, resident.pid)).toEqual([]); // actual resident skip, no inheritance
    await refusesAll(owner, before); noModel(owner);
    await owner.crashMain();
    expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
    expect(owner.owner().token).toBe(resident.token); unchanged(owner, before);
    const death = JSON.parse(fs.readFileSync(path.join(owner.root, "main-1.death.json"), "utf8")) as Record<string, unknown>;
    expect(death).toMatchObject(identity); expect(["absent", "zombie", "reused"]).toContain(death.evidence);
    const verify = await owner.attemptSessionAccess("verify");
    expect(verify.code).toBe(0); expect(verify.signal).toBeNull(); expect(JSON.parse(verify.stdout)).toEqual(owner.pin);
    unchanged(owner, before);
    const start = Date.now(); await owner.start(); const ready = Date.now();
    expect(owner.mainPid()).not.toBe(identity.pid); expect(owner.owner().pid).toBe(resident.pid);
    expect(owner.owner().token).toBe(resident.token); expect(fenceDescriptors(owner, resident.pid)).toEqual([]);
    expect(inode(owner.sessionFile)).toEqual(before.sessionInode); expect(fs.readFileSync(owner.sessionPin)).toEqual(before.pin);
    exactRestartAppend(owner, before.session, start, ready); noModel(owner);
    expect(owner.observations().filter(event => event.event === "module-loaded")).toHaveLength(1);
  }, 180_000);

  it("launcher SIGKILL preserves one writer in both death orders; a new launcher acquires only after verified old Main death", async () => {
    for (const order of ["old-main-first", "contender-first"] as const) {
      const owner = await realOwner("launcher"); assertInstalled(owner); await owner.start();
      const identity = owner.mainIdentity()!, resident = owner.owner(), before = snapshot(owner);
      const lock = inode(`${owner.sessionFile}.main.lock`), pin = { ...owner.pin }, rootId = owner.binding.rootId;
      // Regression deadline for natural EOF + positive Main death/stdio + actual
      // fenced verifier. Not an N6 production/hung-Main or host-load guarantee.
      const boundMs = 2000;
      let step = 0;
      const trace = (event: string, evidence: unknown): void => fs.appendFileSync(path.join(owner.root, "launcher-handover.jsonl"),
        JSON.stringify({ order, step: ++step, at: Date.now(), monotonicNs: process.hrtime.bigint().toString(), event, evidence }) + "\n", { mode: 0o600 });
      const custody = JSON.parse(fs.readFileSync(path.join(owner.root, "launcher-1.custody.json"), "utf8")) as Record<string, unknown>;
      expect(custody).toMatchObject({ mainPid: identity.pid, mainStarted: identity.started, stdinCustody: "launcher-owned-pipe",
        entry: owner.nativeOwnerEntry, entrySha256: hash(owner.nativeOwnerEntry) });
      expect(custody.launcherPid).not.toBe(identity.pid);
      expect(owner.ownedProcesses()).toContainEqual(expect.objectContaining({ pid: identity.pid, started: identity.started,
        argv: expect.arrayContaining([owner.nativeOwnerEntry, NATIVE_SDK_DIRECTORY]) }));
      expect(fenceDescriptors(owner, identity.pid)).toHaveLength(1);
      expect(fenceDescriptors(owner, resident.pid)).toEqual([]); noModel(owner);
      trace("old-writer-ready", { identity, custody, lock, pin, rootId, sessionSha256: hash(owner.sessionFile), pinSha256: hash(owner.sessionPin) });
      // TEST SCHEDULING only: stop this exact launch-recorded Main to observe
      // contender-first deterministically. No parent-held substitute stdin pipe.
      const holdStartedNs = order === "contender-first" ? process.hrtime.bigint() : undefined;
      if (order === "contender-first") await owner.pauseMain();
      const launcherSignalAt = Date.now(), launcherSignalNs = process.hrtime.bigint();
      let startedAt = launcherSignalAt, startedNs = launcherSignalNs, startEvent = "before-launcher-SIGKILL";
      let holdMs = 0, launcherEofHeldMs = 0;
      await owner.crashLauncher(order === "old-main-first" ? { startedNs, boundMs } : undefined); // NOT Main death or RPC liveness
      const receipt = JSON.parse(fs.readFileSync(path.join(owner.root, "launcher-1.main-observation.json"), "utf8")) as Record<string, unknown>;
      expect(receipt).toMatchObject({ ...identity, launcherPid: custody.launcherPid, launcherSignal: "SIGKILL", stdinCustody: "launcher-owned-pipe" });
      trace("launcher-SIGKILL-terminal", receipt);
      if (order === "contender-first") {
        const scheduling = fs.readFileSync(path.join(owner.root, "main-1.test-scheduling.jsonl"), "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
        expect(scheduling).toContainEqual(expect.objectContaining({ kind: "TEST_SCHEDULING", ...identity, signal: "SIGSTOP", state: "T" }));
        expect(receipt).toMatchObject({ mainDeath: "live", mainBirthReadback: identity.started });
        for (const attempt of [() => owner.attemptSecondMain(), () => owner.attemptSessionAccess("verify"), () => owner.attemptSessionAccess("provision")]) {
          expect(owner.mainLive()).toBe(true); expect(owner.mainIdentity()).toEqual(identity);
          expect(fenceDescriptors(owner, identity.pid)).toHaveLength(1);
          expect(fs.existsSync(path.join(owner.root, "main-1.death.json"))).toBe(false);
          expect(fs.existsSync(path.join(owner.root, "launcher-1.close.json"))).toBe(false);
          const priorLaunches = new Set(owner.ownedProcesses().map(value => `${value.pid}:${value.started}`));
          const result = await attempt();
          expect(result.pid).toBeGreaterThan(1); expect(result.code).not.toBeNull(); expect(result.code).not.toBe(0);
          expect(result.signal).toBeNull(); expect(result.stderr).toContain("NATIVE_OWNER_SESSION_BUSY");
          const launched = owner.ownedProcesses().filter(value => !priorLaunches.has(`${value.pid}:${value.started}`));
          expect(launched.some(value => value.pid === result.pid)).toBe(true);
          if ("launcherPid" in result) {
            expect(result.launcherPid).toBeGreaterThan(1); expect(result.launcherPid).not.toBe(custody.launcherPid);
            expect(result.pid).not.toBe(result.launcherPid); expect(result.launcherCode).not.toBe(0);
            expect(launched).toContainEqual(expect.objectContaining({ pid: result.pid, started: result.started,
              argv: expect.arrayContaining([owner.nativeOwnerEntry, NATIVE_SDK_DIRECTORY]) }));
          }
          const pids = new Set(launched.map(value => value.pid));
          expect(owner.sdkImports().filter(event => pids.has(event.pid))).toEqual([]);
          expect(owner.observations().filter(event => pids.has(event.pid as number))).toEqual([]);
          expect(owner.mainLive()).toBe(true); expect(fenceDescriptors(owner, identity.pid)).toHaveLength(1);
          expect(fs.existsSync(path.join(owner.root, "main-1.death.json"))).toBe(false);
          unchanged(owner, before);
          expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
          expect(owner.owner().token).toBe(resident.token); expect(fenceDescriptors(owner, resident.pid)).toEqual([]);
          trace("live-old-holder-refused-contender", { identity, result, launched, lock });
        }
        // Deliberate TEST_SCHEDULING hold is separate: qualify only from BEFORE
        // SIGCONT. Natural order never resets its pre-SIGKILL clock.
        startedAt = Date.now(); startedNs = process.hrtime.bigint(); startEvent = "before-Main-SIGCONT";
        holdMs = Number(startedNs - holdStartedNs!) / 1_000_000;
        launcherEofHeldMs = Number(startedNs - launcherSignalNs) / 1_000_000;
        owner.resumeMain(); // EOF can now request ordinary SDK shutdown; SIGCONT is NOT death evidence
      }
      // Natural 031c EOF shutdown in BOTH orders. No crashMain/RPC/death forgery.
      // Every wait subtracts ALL time since the signal/resume, not a new 30s join.
      const deadline = { startedNs, boundMs };
      const joined = await owner.joinMainDeath(deadline);
      const death = JSON.parse(fs.readFileSync(path.join(owner.root, "main-1.death.json"), "utf8")) as Record<string, unknown>;
      expect(death).toMatchObject(identity); expect(["absent", "zombie", "reused"]).toContain(death.evidence);
      const closed = JSON.parse(fs.readFileSync(path.join(owner.root, "launcher-1.close.json"), "utf8")) as Record<string, unknown>;
      expect(closed).toMatchObject({ pid: custody.launcherPid, signal: "SIGKILL", stdoutClosed: true, stderrClosed: true });
      expect(owner.mainIdentity()).toBeUndefined(); unchanged(owner, before);
      expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
      expect(owner.owner().token).toBe(resident.token); expect(fenceDescriptors(owner, resident.pid)).toEqual([]);
      trace("old-Main-positive-death-and-stdio-close", { death, closed, lock });
      const verify = await owner.attemptSessionAccess("verify", deadline); expect(verify.code).toBe(0); expect(verify.signal).toBeNull();
      expect(JSON.parse(verify.stdout)).toEqual(pin); unchanged(owner, before);
      // Includes actual verifier exit/stdio, successful product flock + pinned
      // SDK readback, and the pre-restart byte checks. No empty-fence inference.
      const releaseVerifiedAt = Date.now(), releaseVerifiedNs = process.hrtime.bigint();
      const elapsedMs = (at: bigint): number => Number(at - startedNs) / 1_000_000;
      const stdioClosedMs = elapsedMs(joined.stdioClosedNs), positiveDeathMs = elapsedMs(joined.positiveDeathNs);
      const releaseVerifiedMs = elapsedMs(releaseVerifiedNs);
      expect(stdioClosedMs).toBeGreaterThanOrEqual(0); expect(stdioClosedMs).toBeLessThanOrEqual(boundMs);
      expect(positiveDeathMs).toBeGreaterThanOrEqual(stdioClosedMs); expect(positiveDeathMs).toBeLessThanOrEqual(boundMs);
      expect(releaseVerifiedMs).toBeGreaterThanOrEqual(positiveDeathMs); expect(releaseVerifiedMs).toBeLessThanOrEqual(boundMs);
      trace("old-Main-EOF-bound-and-product-fence-verified", { clock: "process.hrtime.bigint", startEvent, startedAt,
        startedNs: startedNs.toString(), launcherSignalAt, launcherSignalNs: launcherSignalNs.toString(),
        holdMs, launcherEofHeldMs, holdStartedNs: holdStartedNs?.toString(), boundMs,
        stdioClosedNs: joined.stdioClosedNs.toString(), stdioClosedMs, positiveDeathNs: joined.positiveDeathNs.toString(), positiveDeathMs,
        releaseVerifiedAt, releaseVerifiedNs: releaseVerifiedNs.toString(), releaseVerifiedMs, identity, death, closed, verify, lock, pin, rootId });
      const start = Date.now(); await owner.start(); const ready = Date.now();
      const replacement = owner.mainIdentity()!;
      const newCustody = JSON.parse(fs.readFileSync(path.join(owner.root, "launcher-2.custody.json"), "utf8")) as Record<string, unknown>;
      expect(newCustody).toMatchObject({ mainPid: replacement.pid, mainStarted: replacement.started, stdinCustody: "launcher-owned-pipe",
        entry: owner.nativeOwnerEntry, entrySha256: hash(owner.nativeOwnerEntry) });
      expect(newCustody.launcherPid).not.toBe(custody.launcherPid); expect(newCustody.launcherPid).not.toBe(replacement.pid);
      expect(replacement.pid).not.toBe(identity.pid); expect(fenceDescriptors(owner, replacement.pid)).toHaveLength(1);
      expect(owner.ownedProcesses()).toContainEqual(expect.objectContaining({ ...replacement,
        argv: expect.arrayContaining([owner.nativeOwnerEntry, NATIVE_SDK_DIRECTORY]) }));
      expect(owner.sdkImports().some(event => event.pid === replacement.pid && event.url.endsWith("/core/session-manager.js"))).toBe(true);
      expect(owner.owner().pid).toBe(resident.pid); expect(owner.owner().token).toBe(resident.token);
      expect(fenceDescriptors(owner, resident.pid)).toEqual([]); expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      expect(fs.realpathSync(owner.sessionFile)).toBe(before.file); expect(inode(owner.sessionFile)).toEqual(before.sessionInode);
      expect(inode(owner.sessionPin)).toEqual(before.pinInode); expect(fs.readFileSync(owner.sessionPin)).toEqual(before.pin);
      expect(owner.pin).toEqual(pin); expect(owner.binding.rootId).toBe(rootId);
      exactRestartAppend(owner, before.session, start, ready); noModel(owner);
      expect(owner.observations().filter(event => event.event === "module-loaded")).toHaveLength(1);
      trace("replacement-only-after-old-death-exact-append", { death, replacement, newCustody, lock, pin, rootId, start, ready,
        unchangedPrefixBytes: before.session.length, appendedBytes: fs.statSync(owner.sessionFile).size - before.session.length,
        sessionSha256: hash(owner.sessionFile), pinSha256: hash(owner.sessionPin) });
    }
  }, 180_000);

  it("rejects an unlocked SAME-inode fd before SDK open and rechecks an old holder's actual SDK open/append after public lock replacement", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner);
    const unlocked = await owner.attemptFenceProof("unlocked-proof");
    expect(unlocked.code).not.toBe(0); expect(unlocked.signal).toBeNull();
    expect(unlocked.stderr).toContain("NATIVE_OWNER_FENCE_REQUIRED");
    expect(unlocked.stderr).toContain("no whole-file WRITE FLOCK");
    expect(owner.sdkImports().filter(value => value.pid === unlocked.pid)).toEqual([]); unchanged(owner, before);
    const replaced = await owner.attemptFenceProof("replaced-holder");
    expect(replaced.code).toBe(0); expect(replaced.signal).toBeNull();
    expect(JSON.parse(replaced.stdout).refusals).toEqual(["open", "append"].map(operation => expect.objectContaining({
      operation, name: "NativeOwnerFenceError", code: "NATIVE_OWNER_FENCE_REQUIRED" })));
    expect(owner.sdkImports().some(value => value.pid === replaced.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
    unchanged(owner, before);
    // Do NOT claim a fresh same-UID/new-inode contender is excluded. The retained
    // old holder's next real I/O refuses; check-to-write TOCTOU remains explicit.
  }, 180_000);

  it("missing-lock native config selection refuses bare task before SDK work without reading or changing native bytes; ordinary custom session still opens", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = `${owner.sessionFile}.main.lock`;
    fs.renameSync(lock, `${lock}.retained-missing-lock-control`);
    expect(fs.existsSync(lock)).toBe(false);
    for (const attempt of [() => owner.attemptUnfencedRoute("bare-task"), () => owner.attemptSecondMain()]) {
      const result = await attempt();
      expect(result.code).toBe(1); expect(result.signal).toBeNull();
      expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
      expect(owner.sdkImports().filter(value => value.pid === result.pid)).toEqual([]);
      expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
      unchanged(owner, before); expect(fs.existsSync(lock)).toBe(false);
    }
    const ordinary = await owner.attemptOrdinaryTask();
    expect(ordinary.code).toBe(0); expect(ordinary.signal).toBeNull();
    expect(ordinary.stderr).not.toContain("NATIVE_OWNER_FENCE_REQUIRED");
    expect(owner.sdkImports().some(value => value.pid === ordinary.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
    unchanged(owner, before); expect(owner.callbacks()).toEqual([]);
    expect(owner.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
    expect(fs.existsSync(lock)).toBe(false); // Never create a new anchor to hide the missing-lock case.
  }, 180_000);

  it.each(["unseeded-empty-context", "seeded-control"] as const)("REAL plain SDK CLI WITH candidate Fabric must refuse native identity/input/Fabric append AND preserve clean-tail bytes: %s", async contextCase => {
    const owner = await realOwner(); assertInstalled(owner);
    const beforeSeed = snapshot(owner);
    const bare = await owner.attemptUnfencedRoute("bare-task");
    expect(bare.code).not.toBe(0); expect(bare.signal).toBeNull();
    expect(bare.stderr).toContain("NATIVE_OWNER_FENCE_REQUIRED");
    expect(owner.sdkImports().filter(value => value.pid === bare.pid)).toEqual([]); unchanged(owner, beforeSeed);
    const inspected = await owner.attemptFenceProof("inspect-cli-session");
    expect(inspected.code).toBe(0); expect(inspected.signal).toBeNull();
    expect(JSON.parse(inspected.stdout)).toEqual({ sessionId: owner.pin.sessionId, sessionFile: owner.sessionFile, messages: 0 });
    expect(owner.sdkImports().some(value => value.pid === inspected.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
    unchanged(owner, beforeSeed); // Actual holder positive; inspection did not seed history.
    if (contextCase === "seeded-control") {
      const seeded = await owner.attemptFenceProof("clean-cli-session");
      expect(seeded.code).toBe(0); expect(JSON.parse(seeded.stdout)).toMatchObject({ sessionId: owner.pin.sessionId, sessionFile: owner.sessionFile });
      expect(JSON.parse(seeded.stdout).messages).toBeGreaterThan(0);
    }
    const before = snapshot(owner);
    expect(before.session.at(-1)).toBe(0x0a);
    const files = (directory: string): unknown[] => fs.existsSync(directory) ? fs.readdirSync(directory).sort().flatMap(name => {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      return stat.isDirectory() ? files(file) : [{ file, sha256: hash(file), bytes: stat.size }];
    }) : [];
    const meshBefore = files(owner.meshRoot);
    const result = await owner.attemptUnfencedRoute("plain-cli");
    const after = fs.readFileSync(owner.sessionFile);
    // Retain the unseeded counterexample EVEN IF unchanged() fails. Pinned SDK
    // core/sdk.js can append thinking metadata for empty context before hooks;
    // that is NOT torn-newline repair or an authorized new residual ruling.
    fs.writeFileSync(path.join(owner.root, "real-plain-cli-refusal.json"), JSON.stringify({ contextCase, result,
      cleanTailBytes: before.session.length, beforeBase64: before.session.toString("base64"), afterBase64: after.toString("base64"),
      byteEquality: after.equals(before.session), sessionSha256: hash(owner.sessionFile), meshBefore, meshAfter: files(owner.meshRoot),
      boundary: "upstream open/read accepted; typed terminal refusal/no identity/no Fabric append AND clean-tail byte equality required" }), { mode: 0o600, flag: "wx" });
    expect(result.code).toBe(1); expect(result.signal).toBeNull();
    expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
    expect(result.stderr).not.toMatch(/role.*(?:refus|invalid)|model.*policy.*(?:refus|denied)/i);
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
    // Actual candidate Fabric must have executed: only its product bootstrap
    // produces that terminal typed refusal; no fixture native-fence guard runs.
    expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
    expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
    expect(files(owner.meshRoot)).toEqual(meshBefore);
    expect(owner.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
    expect(fs.existsSync(path.join(owner.residencyRoot, "owner.json"))).toBe(false);
    // The seeded control is NOT sole proof of the real native empty-context
    // starting state. Both cases keep the identical unchanged-byte requirement.
  }, 180_000);

  it.each([
    "continue-native", "short-continue-native", "resume-native", "long-resume-native", "duplicate-native",
    "relative-native", "dotdot-native", "symlink-native", "unknown-native", "inline-session-native",
    "ordinary-continue", "ordinary-resume", "duplicate-ordinary", "unknown-ordinary",
  ] satisfies NativeOwnerCliShape[])("P1 REAL plain SDK CLI configured fail-closed refusal preserves header-only native bytes: %s", async shape => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
    const header = before.session.toString("utf8").split("\n");
    expect(header).toHaveLength(2); expect(header[1]).toBe("");
    expect(JSON.parse(header[0]!)).toMatchObject({ type: "session", id: owner.pin.sessionId, cwd: owner.cwd });
    expect(owner.mainIdentity()).toBeUndefined(); // No native fence holder or seeded context.
    const files = (directory: string): unknown[] => fs.existsSync(directory) ? fs.readdirSync(directory).sort().flatMap(name => {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      return stat.isDirectory() ? files(file) : [{ file, sha256: hash(file), bytes: stat.size }];
    }) : [];
    const meshBefore = files(owner.meshRoot);
    const result = await owner.attemptRealCli(shape);
    const after = fs.readFileSync(owner.sessionFile);
    fs.writeFileSync(path.join(owner.root, "p1-real-cli-refusal.json"), JSON.stringify({ shape, result,
      beforeBase64: before.session.toString("base64"), afterBase64: after.toString("base64"), byteEquality: after.equals(before.session),
      before, after: snapshot(owner), lock, meshBefore, meshAfter: files(owner.meshRoot) }), { mode: 0o600, flag: "wx" });
    expect(result.code).toBe(1); expect(result.signal).toBeNull();
    expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
    expect(result.stderr).not.toMatch(/role.*(?:refus|invalid)|model.*policy.*(?:refus|denied)/i);
    expect(result.stdout).toBe(""); // Queued negative input never reaches RPC/admission.
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
    expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]); // Includes session_start and input.
    expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
    expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
    expect(files(owner.meshRoot)).toEqual(meshBefore);
    expect(owner.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
    expect(fs.existsSync(path.join(owner.residencyRoot, "owner.json"))).toBe(false);
    expect(owner.mainIdentity()).toBeUndefined();
  }, 180_000);

  it("P1 REAL plain SDK CLI unsupported short option -z exits before selection and preserves header-only native bytes", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
    const header = before.session.toString("utf8").split("\n");
    expect(header).toHaveLength(2); expect(header[1]).toBe("");
    expect(JSON.parse(header[0]!)).toMatchObject({ type: "session", id: owner.pin.sessionId, cwd: owner.cwd });
    expect(owner.mainIdentity()).toBeUndefined(); // No native Main holder or seeded context.
    const files = (directory: string): unknown[] => fs.existsSync(directory) ? fs.readdirSync(directory).sort().flatMap(name => {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      return stat.isDirectory() ? files(file) : [{ file, sha256: hash(file), bytes: stat.size }];
    }) : [];
    const meshBefore = files(owner.meshRoot);
    const result = await owner.attemptRealCli("short-unknown-native");
    expect(result.code).toBe(1); expect(result.signal).toBeNull();
    expect(result.stderr).toContain("Unknown option: -z");
    expect(result.stderr).not.toContain("NativeOwnerFenceError"); // The pinned SDK exits before Fabric registration.
    expect(result.stdout).toBe(""); // Queued negative input never reaches RPC/admission.
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
    expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
    expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
    expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
    expect(files(owner.meshRoot)).toEqual(meshBefore);
    expect(owner.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
    expect(fs.existsSync(path.join(owner.residencyRoot, "owner.json"))).toBe(false);
    expect(owner.mainIdentity()).toBeUndefined();
  }, 180_000);

  it.each(["ordinary-relative", "ordinary-symlink", "ordinary-flag-value"] satisfies NativeOwnerCliShape[])(
    "P1 REAL plain SDK CLI configured single ordinary selection reaches model-free RPC: %s", async shape => {
      const owner = await realOwner(); assertInstalled(owner);
      const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
      const result = await owner.attemptRealCli(shape), ordinary = result.ordinary!;
      expect(result.code).toBe(0); expect(result.signal).toBeNull();
      expect(result.stderr).not.toContain("NATIVE_OWNER_FENCE_REQUIRED");
      expect(ordinary.sessionId).not.toBe(owner.pin.sessionId);
      expect(fs.realpathSync(ordinary.sessionFile)).not.toBe(owner.sessionFile);
      const records = result.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line));
      expect(records).toContainEqual(expect.objectContaining({ type: "response", id: "ordinary-readback", success: true,
        data: expect.objectContaining({ sessionId: ordinary.sessionId }) }));
      expect(owner.observations().filter(value => value.pid === result.pid && value.event === "session-start")).toEqual([
        expect.objectContaining({ selectedModel: false, unconfigured: true, rootId: ordinary.rootId }),
      ]);
      expect(owner.observations().filter(value => value.pid === result.pid &&
        (String(value.event).startsWith("FORBIDDEN-") || value.event === "input-handled"))).toEqual([]);
      expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
      expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      expect(owner.observations().some(value => value.rootId === owner.pin.rootId)).toBe(false);
      // --name's value is literally --frobnicate in the flag-value case. The
      // SDK consumes it as data; treating that value as an unknown flag is wrong.
    }, 180_000);

  it.each(["ordinary-new", "ordinary-new-parent-symlink", "task-new", "task-new-parent-symlink",
    "task-new-owner-host-profile"] satisfies NativeOwnerCliShape[])(
    "P2 actual configured SDK opens a NEW ordinary session leaf without prior header or history: %s", async shape => {
      const owner = await realOwner(); assertInstalled(owner);
      const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
      const result = await owner.attemptRealCli(shape);
      expect(result.missingAtLaunch).toBe(true); expect(result.selectedFile).toBeDefined();
      expect(result.code).toBe(0); expect(result.signal).toBeNull();
      expect(result.stderr).not.toContain("NATIVE_OWNER_FENCE_REQUIRED");
      const ordinary = result.ordinary!; expect(ordinary).toBeDefined();
      expect(ordinary.sessionId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
      expect(ordinary.sessionId).not.toBe(owner.pin.sessionId);
      expect(ordinary.sessionFile).toBe(result.selectedFile);
      const selectedParent = fs.realpathSync(path.dirname(ordinary.sessionFile));
      expect(path.join(selectedParent, path.basename(ordinary.sessionFile))).not.toBe(owner.sessionFile);
      expect(fs.lstatSync(path.dirname(ordinary.sessionFile)).isSymbolicLink()).toBe(shape.endsWith("parent-symlink"));
      const records = result.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line));
      expect(records).toContainEqual(expect.objectContaining({ type: "response", id: "ordinary-readback", success: true,
        data: expect.objectContaining({ sessionId: ordinary.sessionId, sessionFile: ordinary.sessionFile }) }));
      expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/core/session-manager.js"))).toBe(true);
      if (!shape.startsWith("task-")) expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
      expect(owner.observations().filter(value => value.pid === result.pid && value.event === "session-start")).toEqual([
        expect.objectContaining({ selectedModel: false, unconfigured: true, rootId: ordinary.rootId, sessionFile: ordinary.sessionFile }),
      ]);
      expect(owner.observations().filter(value => String(value.event).startsWith("FORBIDDEN-") || value.event === "input-handled")).toEqual([]);
      expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
      expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      // Missing NEW leaves need not be flushed without a turn. The real SDK's
      // RPC/session_start identity and selected path, not a fixture header, prove admission.
    }, 180_000);

  it.each(["task-new-missing-parent", "task-new-missing-pin"] satisfies NativeOwnerCliShape[])(
    "P2 bare task selection failures are typed and occur before SDK load: %s", async shape => {
      const owner = await realOwner(); assertInstalled(owner);
      const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
      const result = await owner.attemptRealCli(shape);
      expect(result.missingAtLaunch).toBe(true);
      expect(result.code).toBe(1); expect(result.signal).toBeNull();
      expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
      expect(result.stdout).toBe("");
      expect(owner.sdkImports().filter(value => value.pid === result.pid)).toEqual([]);
      expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
      expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
      expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      expect(fs.existsSync(result.selectedFile!)).toBe(false);
    }, 180_000);

  it.each(["hardlink-native", "task-hardlink-native"] satisfies NativeOwnerCliShape[])(
    "P3 same-dev+ino native hardlink alias is typed-refused without native SDK writes: %s", async shape => {
      const owner = await realOwner(); assertInstalled(owner);
      const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
      const result = await owner.attemptRealCli(shape);
      expect(result.code).toBe(1); expect(result.signal).toBeNull();
      expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
      expect(result.stderr).toContain("native session inode alias"); expect(result.stdout).toBe("");
      if (shape.startsWith("task-")) expect(owner.sdkImports().filter(value => value.pid === result.pid)).toEqual([]);
      else expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
      expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
      expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
      expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      expect(fs.lstatSync(owner.sessionFile).nlink).toBe(2);
      const alias = fs.readdirSync(owner.cwd).find(name => name.startsWith(`real-cli-${shape}-`) && name.endsWith(".jsonl"));
      expect(alias).toBeDefined(); expect(inode(path.join(owner.cwd, alias!))).toEqual(before.sessionInode);
      expect(fs.realpathSync(path.join(owner.cwd, alias!))).not.toBe(owner.sessionFile);
    }, 180_000);

  it.each(["task-prefix-ordinary", "cli-prefix-ordinary"] satisfies NativeOwnerCliShape[])(
    "P3.1 actual SDK parser opens distinct ordinary native-ID-prefix control: %s", async shape => {
      const owner = await realOwner(); assertInstalled(owner);
      const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
      const result = await owner.attemptRealCli(shape), ordinary = result.ordinary!;
      expect(ordinary.sessionId.slice(0, 8)).toBe(owner.pin.sessionId.slice(0, 8));
      expect(ordinary.sessionId).not.toBe(owner.pin.sessionId);
      expect(result.code).toBe(0); expect(result.signal).toBeNull();
      expect(result.stderr).not.toContain("NATIVE_OWNER_FENCE_REQUIRED");
      const records = result.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line));
      expect(records).toContainEqual(expect.objectContaining({ type: "response", id: "ordinary-readback", success: true,
        data: expect.objectContaining({ sessionId: ordinary.sessionId, sessionFile: ordinary.sessionFile }) }));
      expect(owner.observations().filter(value => value.pid === result.pid && value.event === "session-start")).toEqual([
        expect.objectContaining({ selectedModel: false, unconfigured: true, rootId: ordinary.rootId, sessionFile: ordinary.sessionFile }),
      ]);
      expect(owner.observations().filter(value => String(value.event).startsWith("FORBIDDEN-") || value.event === "input-handled")).toEqual([]);
      unchanged(owner, before); expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
      const exact = await owner.attemptRealCli("task-exact-native-id");
      expect(exact.code).toBe(1); expect(exact.signal).toBeNull();
      expect(exact.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
      expect(owner.sdkImports().filter(value => value.pid === exact.pid)).toEqual([]);
      expect(owner.observations().filter(value => value.pid === exact.pid)).toEqual([]);
      expect(owner.callbacks()).toEqual([]); unchanged(owner, before);
    }, 180_000);

  it("P2.1 REAL plain SDK CLI --name residual is exactly one canonical SDK session_info before typed registration refusal", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = inode(`${owner.sessionFile}.main.lock`);
    expect(before.session.toString("utf8").split("\n")).toHaveLength(2);
    expect(before.session.at(-1)).toBe(0x0a);
    const files = (directory: string): unknown[] => fs.existsSync(directory) ? fs.readdirSync(directory).sort().flatMap(name => {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      return stat.isDirectory() ? files(file) : [{ file, sha256: hash(file), bytes: stat.size }];
    }) : [];
    const meshBefore = files(owner.meshRoot);
    const result = await owner.attemptRealCli("name-native"), after = fs.readFileSync(owner.sessionFile);
    fs.writeFileSync(path.join(owner.root, "p2-name-residual.json"), JSON.stringify({ result,
      beforeBase64: before.session.toString("base64"), afterBase64: after.toString("base64"),
      meshBefore, meshAfter: files(owner.meshRoot) }), { mode: 0o600, flag: "wx" });
    expect(result.code).toBe(1); expect(result.signal).toBeNull();
    expect(result.stderr).toContain("NativeOwnerFenceError: NATIVE_OWNER_FENCE_REQUIRED");
    expect(result.stdout).toBe("");
    expect(after.subarray(0, before.session.length)).toEqual(before.session);
    const append = after.subarray(before.session.length), lines = append.toString("utf8").split("\n");
    expect(lines).toHaveLength(2); expect(lines[1]).toBe("");
    const entry = JSON.parse(lines[0]!) as Record<string, unknown>;
    expect(Object.keys(entry)).toEqual(["type", "id", "parentId", "timestamp", "name"]);
    expect(entry).toMatchObject({ type: "session_info", parentId: null, name: "native-name-residual" });
    expect(entry.id).toMatch(/^[0-9a-f]{8}$/); expect(entry.id).not.toBe(owner.pin.sessionId);
    expect(typeof entry.timestamp).toBe("string");
    expect(entry.timestamp).toBe(new Date(entry.timestamp as string).toISOString());
    expect(Date.parse(entry.timestamp as string)).toBeGreaterThanOrEqual(result.startedAt);
    expect(Date.parse(entry.timestamp as string)).toBeLessThanOrEqual(result.closedAt);
    expect(append).toEqual(Buffer.from(`${JSON.stringify(entry)}\n`)); // Full prefix + exactly one LF-terminated SDK record.
    expect(inode(owner.sessionFile)).toEqual(before.sessionInode); expect(inode(owner.sessionPin)).toEqual(before.pinInode);
    expect(fs.readFileSync(owner.sessionPin)).toEqual(before.pin); expect(fs.realpathSync(owner.sessionFile)).toBe(before.file);
    expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(lock);
    expect(owner.sdkImports().some(value => value.pid === result.pid && value.url.endsWith("/cli.js"))).toBe(true);
    expect(owner.observations().filter(value => value.pid === result.pid)).toEqual([]);
    expect(owner.callbacks()).toEqual([]); expect(files(owner.meshRoot)).toEqual(meshBefore);
    expect(owner.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
    expect(fs.existsSync(path.join(owner.residencyRoot, "owner.json"))).toBe(false);
  }, 180_000);

  it("keeps the pinned native file outside REAL ordinary SDK listing/resume/continue/bare-ID resolution, with an ordinary positive control", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), proof = await owner.probeDiscovery();
    expect(proof).toMatchObject({ sessionDir: null, envSessionDir: null, pathPinChanged: false, schemaChanged: false, migrationPerformed: false });
    const globalRoot = proof.globalRoot as string, ordinaryFile = proof.ordinaryFile as string, ordinaryId = proof.ordinaryId as string;
    expect(globalRoot).toBe(path.join(owner.root, "profile", "sessions"));
    expect(path.relative(globalRoot, owner.sessionFile).startsWith(`..${path.sep}`)).toBe(true);
    expect(proof.native).toEqual(owner.pin);
    expect(proof.samples).toHaveLength(2);
    for (const sample of proof.samples as Array<Record<string, unknown>>) {
      expect(sample.listed).toEqual([{ id: ordinaryId, path: ordinaryFile }]);
      expect(sample.all).toEqual([{ id: ordinaryId, path: ordinaryFile }]);
      expect(sample.nativeExact).toBeNull(); expect(sample.ordinaryExact).toBe(ordinaryFile);
      expect(sample.continued).toBe(ordinaryFile); expect(sample.bareOrdinary).toBe(ordinaryFile);
    }
    expect(proof.bareNative).toMatchObject({ code: 1, signal: null });
    expect((proof.bareNative as { stderr: string }).stderr).toContain(`No session found matching '${owner.pin.sessionId}'`);
    expect(proof.resume).toMatchObject({ code: 0, signal: null });
    expect(proof.resumeSelection).toEqual({ sessionId: ordinaryId, sessionFile: ordinaryFile });
    expect((proof.resume as { stdout: string }).stdout).toContain('"selectedViaActualPicker": true');
    unchanged(owner, before); expect(owner.callbacks()).toEqual([]);
    expect(owner.observations()).toEqual([]); // resolver-only; not a user/model/Fabric turn
  }, 180_000);

  it("fails closed on unsafe fence mode/symlink and multiply-linked native file, with no SDK loading or pin/session mutation", async () => {
    const owner = await realOwner(); assertInstalled(owner);
    const before = snapshot(owner), lock = `${owner.sessionFile}.main.lock`;
    const refuse = async (reason: RegExp): Promise<void> => {
      for (const attempt of [() => owner.attemptSecondMain(), () => owner.attemptSessionAccess("verify")]) {
        const result = await attempt(); expect(result.code).not.toBe(0); expect(result.signal).toBeNull(); expect(result.stderr).toMatch(reason);
        expect(owner.sdkImports().filter(event => event.pid === result.pid)).toEqual([]); unchanged(owner, before);
      }
    };
    fs.chmodSync(lock, 0o640); await refuse(/unsafe|Unsafe/); fs.chmodSync(lock, 0o600);
    fs.renameSync(lock, `${lock}.retained-original`);
    fs.symlinkSync(`${lock}.retained-original`, lock);
    await refuse(/ELOOP|symlink|unsafe/i);
    // Retain all negative evidence, rather than deleting even this test symlink.
    fs.renameSync(lock, `${lock}.retained-symlink`); fs.renameSync(`${lock}.retained-original`, lock);
    fs.linkSync(owner.sessionFile, path.join(owner.root, "retained-session-hardlink.jsonl"));
    await refuse(/unsafe|Unsafe/);
    expect(fs.lstatSync(owner.sessionFile).nlink).toBe(2);
  }, 180_000);
});

describe("restart parent expectation regression", () => {
  const sessionId = "11111111-1111-4111-8111-111111111111";
  const appendTimestamp = "2026-10-26T00:00:00.000Z";
  const fixtureTimestamp = "2026-10-25T23:59:30.000Z";
  const headerTimestamp = "2026-10-25T23:59:00.000Z";
  const staleEntryId = "00000001";
  const latestEntryId = "00000002";
  const entryHistory: Record<string, unknown>[] = [
    { type: "thinking_level_change", id: staleEntryId, parentId: null, timestamp: fixtureTimestamp, thinkingLevel: "off" },
    { type: "thinking_level_change", id: latestEntryId, parentId: staleEntryId, timestamp: fixtureTimestamp, thinkingLevel: "off" },
  ];

  // These are assertion-only byte fixtures under the parent-owned private temp
  // guard. They are never seeded into genuine native history or used as
  // custody proof; no SDK, SessionManager, native owner, or Main is opened.
  const assertionBytes = (
    entries: readonly Record<string, unknown>[],
    parentId: string | null | undefined,
    includeParent: boolean,
  ): { owner: NativeActorOwner; before: Buffer; started: number; ready: number } => {
    const temp = residentDrainTemp("restart-parent-expectation-");
    const cwd = path.join(temp.root, "service");
    fs.mkdirSync(cwd, { mode: 0o700 });
    const sessionFile = path.join(temp.root, "assertion-bytes.jsonl");
    const header = { type: "session", version: 3, id: sessionId, timestamp: headerTimestamp, cwd };
    const before = Buffer.from(`${[header, ...entries].map(value => JSON.stringify(value)).join("\n")}\n`);
    const appended: Record<string, unknown> = {
      type: "thinking_level_change",
      id: entries.length ? "00000003" : "00000001",
      ...(includeParent ? { parentId } : {}),
      timestamp: appendTimestamp,
      thinkingLevel: "off",
    };
    fs.writeFileSync(sessionFile, Buffer.concat([before, Buffer.from(`${JSON.stringify(appended)}\n`)]),
      { mode: 0o600, flag: "wx" });
    temp.assertNoOutsideDeletion();
    // The actual assertion reads only these fields; this is not a fake native
    // owner, SessionManager, SDK, Main, or source of custody evidence.
    const owner = {
      sessionFile,
      pin: { sessionId },
      cwd,
      binding: { rootId: `session:${sessionId}` },
    } as NativeActorOwner;
    const started = Date.parse(appendTimestamp);
    return { owner, before, started, ready: started };
  };

  it("accepts a null parent for header-only assertion bytes", () => {
    const fixture = assertionBytes([], null, true);
    expect(() => exactRestartAppend(fixture.owner, fixture.before, fixture.started, fixture.ready)).not.toThrow();
  });

  it.each([
    { label: "header ID", parentId: sessionId, includeParent: true },
    { label: "arbitrary ID", parentId: "deadbeef", includeParent: true },
    { label: "missing parent", parentId: undefined, includeParent: false },
  ])("rejects $label for header-only assertion bytes", ({ parentId, includeParent }) => {
    const fixture = assertionBytes([], parentId, includeParent);
    expect(() => exactRestartAppend(fixture.owner, fixture.before, fixture.started, fixture.ready)).toThrow();
  });

  it("accepts the exact latest non-header entry ID for nonempty assertion bytes", () => {
    const fixture = assertionBytes(entryHistory, latestEntryId, true);
    expect(() => exactRestartAppend(fixture.owner, fixture.before, fixture.started, fixture.ready)).not.toThrow();
  });

  it.each([
    { label: "null", parentId: null, includeParent: true },
    { label: "stale entry ID", parentId: staleEntryId, includeParent: true },
    { label: "header ID", parentId: sessionId, includeParent: true },
    { label: "missing parent", parentId: undefined, includeParent: false },
  ])("rejects $label for nonempty assertion bytes", ({ parentId, includeParent }) => {
    const fixture = assertionBytes(entryHistory, parentId, includeParent);
    expect(() => exactRestartAppend(fixture.owner, fixture.before, fixture.started, fixture.ready)).toThrow();
  });
});
