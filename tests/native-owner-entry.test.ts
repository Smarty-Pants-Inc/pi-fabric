// Keep: real session-writer trust boundary + kernel/custody crash counterexamples.
// Linux-only kernel coverage. Explicit approved SDK + Node and a FRESH compiled
// candidate are prerequisites. Ordinary CI declares missing-input skips; REQUIRED=1 fails them. No model/SDK mocks.
// Parent MUST run via its live launcherTestTemp owner and archive the WHOLE root.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { createNativeActorOwner, type NativeActorOwner } from "./helpers/native-actor-owner.js";
import { NATIVE_SDK_DIRECTORY, assertApprovedNativeSdk, nativeSdkTestPrerequisite } from "./helpers/native-sdk-no-model.js";

// Expected skip: missing Linux/approved SDK files/executable Node/env in ordinary CI only.
const nativeSkipReason = nativeSdkTestPrerequisite();
import { residentProcessAlive } from "../src/residency/process-identity.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const actorId = "88888888888848888888888888888888"; // synthetic test actor, never production
const owners: NativeActorOwner[] = [];
afterEach(async () => { for (const owner of owners.splice(0)) await owner.close(); });
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
  expect(entry.parentId).toBe(prior.at(-1)!.id);
  expect(typeof entry.timestamp).toBe("string");
  const timestamp = entry.timestamp as string;
  expect(timestamp).toBe(new Date(timestamp).toISOString());
  expect(Date.parse(timestamp)).toBeGreaterThanOrEqual(started); expect(Date.parse(timestamp)).toBeLessThanOrEqual(ready);
  expect(append).toEqual(Buffer.from(`${JSON.stringify(entry)}\n`));
};

describe.skipIf(!!nativeSkipReason)(`F1(b) actual candidate-installed native owner entry${nativeSkipReason ? ` [SKIP: ${nativeSkipReason}]` : ""}`, () => {
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
      // Frozen Step1 wall traces conservatively enclose terminal + verified
      // release by 324ms. This is a 1676ms margin, NOT a fresh measurement.
      const measuredUpperMs = 324, marginMs = 1676, boundMs = 2000;
      expect(measuredUpperMs + marginMs).toBe(boundMs);
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
        holdMs, launcherEofHeldMs, holdStartedNs: holdStartedNs?.toString(), measuredUpperMs, marginMs, boundMs,
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
