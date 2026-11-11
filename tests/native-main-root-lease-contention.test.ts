/** #19998 B host proof: no fake clock/platform, no test-driver message retries.
 * Keep reason: incarnation trust boundary + data safety after a native Main restart.
 * Run ONLY under the parent's owned smarty-reap / smarty-test-env scope.
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { hostLeasePath, readHostLeases, type FabricHostLease } from "../src/topology/host-leases.js";
import { delay, mainRootProofHarness, nonceCopies, outcomes, type ProofEvent, type Ready, until } from "./helpers/main-root-proof-harness.js";
import { startTime } from "./helpers/owned-processes.js";

const fixture = mainRootProofHarness(import.meta.url);
type Custody = { gatePath: string; gateOwner?: string; sharedOwner?: string; sharedIncarnation?: string;
  physical?: { machineId: string; bootId: string } };
const assertHeld = (event: ProofEvent, main: Ready, held: ProofEvent) => {
  const custody = event.custody as Custody;
  expect(event.pid).toBe(main.pid);
  expect(event.incarnation).toBe(main.incarnation);
  expect(startTime(main.pid)).toBe(main.incarnation);
  expect(custody.sharedIncarnation).toBe(held.incarnation);
  expect(custody.sharedOwner).toBe(held.owner);
  const fields = custody.gateOwner?.split("\n");
  expect(fields?.[1]).toBe(String(main.pid));
  expect(fields?.[3]).toBe(main.incarnation);
  expect(custody.physical?.machineId).toBeTruthy();
  expect(custody.physical?.bootId).toBeTruthy();
  expect(fields?.[4]).toBe(custody.physical?.machineId);
  expect(fields?.[5]).toBe(custody.physical?.bootId);
};

// Native start-time proof is currently Linux-specific: do not silently call a
// ps string / mocked platform equivalent proof. Other platforms are not this gate.
describe.skipIf(process.platform !== "linux")("Main root FILE lease and exact-session followUp host proof (#19998 B)", () => {
  it("keeps root FILE liveness while REAL shared custody blocks confirmation beyond the 15 s TTL", async () => {
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", undefined);
    const f = await fixture();
    const main = await f.startMain();
    const beforeNonce = `root-hold-before-${randomUUID()}`;
    await f.send(main.ready.id, beforeNonce);
    await until(() => nonceCopies(main.ready.sessionFile, beforeNonce).length === 1, "healthy native path never delivered");
    const holder = f.start("holder");
    const held = await holder.next("held");
    expect(held.pid).toBe(holder.process.pid);
    expect(held.incarnation).toBe(startTime(held.pid));
    expect(String(held.owner).split("\n")[1]).toBe(String(held.pid));
    expect(String(held.owner)).toContain(held.incarnation);
    main.child.send("snapshot");
    const before = await main.child.next("snapshot") as Ready;
    const stateBytes = fs.readFileSync(path.join(f.meshRoot, "state.json"), "utf8");
    main.child.send("change");
    await main.child.next("refresh-started");
    // One actual followUp waits on shared publication. Nothing in the driver resends it.
    const nonce = `root-hold-during-${randomUUID()}`;
    const sent = f.send(main.ready.id, nonce);
    const samples: Array<{ at: number; lease?: FabricHostLease | undefined }> = [];
    const end = held.at + 17_000; // Real wall time, strictly beyond the original fixed root TTL.
    while (Date.now() < end) {
      samples.push({ at: Date.now(), lease: readHostLeases(f.meshRoot).get(main.ready.id) });
      await delay(Math.min(250, Math.max(1, end - Date.now())));
    }
    main.child.send("snapshot");
    const during = await main.child.next("snapshot") as Ready;
    const unchanged = fs.readFileSync(path.join(f.meshRoot, "state.json"), "utf8") === stateBytes;
    const custodyOwnerUnchanged = fs.readFileSync(path.join(f.meshRoot, ".lock", "owner"), "utf8") === held.owner;
    const custodyProcessUnchanged = startTime(held.pid) === held.incarnation;
    f.observations.push({ kind: "held-custody", held, before, during, samples, unchanged,
      custodyOwnerUnchanged, custodyProcessUnchanged });
    // Release before assertions so RED still leaves terminal custody and delivery evidence.
    holder.send("release");
    const released = await holder.next("released");
    await holder.closed;
    f.observations.push({ kind: "custody-released", released, durationMs: released.at - held.at });
    await sent;
    await until(() => nonceCopies(main.ready.sessionFile, nonce).length === 1, "held publication was not eventually consumed", 12_000);
    expect(released.at - held.at).toBeGreaterThan(15_000);
    expect(custodyOwnerUnchanged && custodyProcessUnchanged).toBe(true);
    expect(during.confirmedAt).toBe(before.confirmedAt); // FILE freshness is NOT shared confirmation.
    expect(unchanged).toBe(true);
    expect(during.lease.incarnationToken).toBe(before.lease.incarnationToken);
    expect(samples.every(sample => sample.lease && sample.lease.expiresAt >= sample.at &&
      sample.lease.session !== undefined && sample.lease.session.expiresAt >= sample.at),
      "ROOT FILE LEASE LAPSED while a live native Main waited on shared custody").toBe(true);
    expect(during.lease.expiresAt).toBeGreaterThan(during.at);
    for (const value of [beforeNonce, nonce]) {
      expect(nonceCopies(main.ready.sessionFile, value)).toHaveLength(1);
      const event = f.commands().find(item => (item.data as { message?: string }).message === value)!;
      expect(outcomes(f.meshRoot).filter(item => item.eventId === event.id && item.outcome === "delivered")).toHaveLength(1);
    }
  }, 65_000);

  it("A-1 contains a fired held-path EIO and latches a replaced token without killing the native root", async () => {
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", undefined);
    const f = await fixture();
    const main = await f.startMain(undefined, { instrumentLeaseProof: true });
    // Reach the changed publication's real host gate BEFORE shared custody is held.
    // An already-running idle confirmation can finish normally; no timer-phase race.
    main.child.send("arm-publication-barrier");
    const barrierArmed = await main.child.next("publication-barrier-armed");
    main.child.send("change");
    await main.child.next("refresh-started");
    const barrierReady = await main.child.next("publication-barrier-ready");
    const barrierCustody = barrierReady.custody as Custody;
    const gateOwner = barrierCustody.gateOwner?.split("\n");
    expect(barrierReady.pid).toBe(main.ready.pid);
    expect(barrierReady.incarnation).toBe(main.ready.incarnation);
    expect(startTime(main.ready.pid)).toBe(main.ready.incarnation);
    expect(barrierReady.gate).toBeDefined();
    expect(gateOwner?.[1]).toBe(String(main.ready.pid));
    expect(gateOwner?.[3]).toBe(main.ready.incarnation);
    expect(barrierCustody.physical?.machineId).toBeTruthy();
    expect(barrierCustody.physical?.bootId).toBeTruthy();
    expect(gateOwner?.[4]).toBe(barrierCustody.physical?.machineId);
    expect(gateOwner?.[5]).toBe(barrierCustody.physical?.bootId);
    expect(barrierCustody.sharedOwner).toBeUndefined();
    f.observations.push({ kind: "A-1-publication-barrier-ready", barrierArmed, barrierReady });
    const holder = f.start("holder");
    await f.assertRuntime(holder);
    const held = await holder.next("held");
    let released: ProofEvent | undefined;
    try {
      main.child.send("release-publication-barrier");
      const barrierReleased = await main.child.next("publication-barrier-released");
      assertHeld(barrierReleased, main.ready, held);
      expect(barrierReleased.gate).toBe(barrierReady.gate);
      await until(() => main.child.events.some(event => event.event === "publication-attempt-start" &&
        (event.custody as Custody).sharedOwner === held.owner), "publication never entered its real held host gate");
      const attemptStarted = main.child.events.find(event => event.event === "publication-attempt-start" &&
        (event.custody as Custody).sharedOwner === held.owner)!;
      assertHeld(attemptStarted, main.ready, held);
      expect(attemptStarted.gate).toBe(barrierReady.gate);
      expect(held.at).toBeGreaterThanOrEqual(barrierReady.at);
      f.observations.push({ kind: "A-1-publication-barrier-released", held, barrierReleased, attemptStarted });
      main.child.send("arm-held-eio");
      const armed = await main.child.next("held-write-fault-armed", 3_000);
      assertHeld(armed, main.ready, held);
      const fired = await main.child.next("held-write-fault-fired", 4_000);
      const caught = await main.child.next("heartbeat-catch-observed", 3_000);
      assertHeld(fired, main.ready, held);
      assertHeld(caught, main.ready, held);
      expect(fired.code).toBe("EIO");
      expect(String(fired.stack)).toContain("commitHostLeaseUnderCustody");
      expect(caught.sameInjectedError).toBe(true);
      expect(caught.attempt).toBe(fired.attempt);
      expect(main.child.events.filter(event => event.event === "held-write-fault-fired")).toHaveLength(1);
      expect(main.child.events.some(event => event.event === "publication-attempt-error" && event.attempt === fired.attempt)).toBe(false);
      main.child.send("admission");
      const afterFault = await main.child.next("admission");
      expect(afterFault.canConsumeMesh).toBe(false);
      expect((afterFault.admission as Ready["admission"]).controlAdmissionReason).toContain("lease-proof injected held-path EIO");
      expect(afterFault.heartbeatArmed).toBe(true);
      // The same native process, same still-pending publication, no release/restart.
      main.child.send("replace-held-token");
      const replaced = await main.child.next("held-token-replaced");
      assertHeld(replaced, main.ready, held);
      const successor = replaced.successor as FabricHostLease;
      expect(successor.incarnationToken).not.toBe(main.ready.lease.incarnationToken);
      await until(() => main.child.events.some(event => event.event === "held-cas-read" &&
        event.token === successor.incarnationToken && event.attempt === fired.attempt),
      "no held-path CAS actually observed the successor token", 4_000);
      const cas = main.child.events.find(event => event.event === "held-cas-read" && event.token === successor.incarnationToken)!;
      assertHeld(cas, main.ready, held);
      const supersessionCatch = await main.child.next("heartbeat-supersession-observed", 3_000);
      assertHeld(supersessionCatch, main.ready, held);
      expect(supersessionCatch.attempt).toBe(fired.attempt);
      main.child.send("probe-superseded");
      const firstProbe = await main.child.next("superseded-probe", 3_000);
      expect(firstProbe.code).toBe("FABRIC_HOST_LEASE_SUPERSEDED");
      expect(firstProbe.heartbeatArmed).toBe(false);
      expect(firstProbe.bytes).toBe(replaced.bytes);
      await delay(2_500); // More than two unchanged real heartbeat periods, not a fake clock.
      main.child.send("admission");
      const latched = await main.child.next("admission");
      expect(latched.canConsumeMesh).toBe(false);
      expect(latched.heartbeatArmed).toBe(false);
      expect(fs.readFileSync(hostLeasePath(f.meshRoot, main.ready.id), "utf8")).toBe(replaced.bytes);
      expect(startTime(main.ready.pid)).toBe(main.ready.incarnation);
      expect(fs.readFileSync(path.join(f.meshRoot, ".lock", "owner"), "utf8")).toBe(held.owner);
      f.observations.push({ kind: "A-1-held-fault-and-token", held, armed, fired, caught, afterFault, replaced, cas, firstProbe, latched });
      holder.send("release");
      released = await holder.next("released");
      await holder.closed;
      await main.child.next("refresh-error");
      main.child.send("probe-superseded");
      const finalProbe = await main.child.next("superseded-probe");
      expect(finalProbe.code).toBe("FABRIC_HOST_LEASE_SUPERSEDED");
      expect(finalProbe.bytes).toBe(replaced.bytes);
      expect(startTime(main.ready.pid)).toBe(main.ready.incarnation);
      expect(main.child.events.filter(event => ["fatal", "uncaught-observed", "superseded-probe-unexpected-success"].includes(event.event))).toEqual([]);
      f.observations.push({ kind: "A-1-after-release", released, finalProbe });
    } finally {
      if (!released && !holder.exit) {
        holder.send("release");
        await holder.next("released");
        await holder.closed;
      }
    }
  }, 45_000);

  it("A-2/A-3 keeps independent native routing live through 17s custody and a real 5s publication timeout/retry", async () => {
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", undefined);
    const f = await fixture();
    const main = await f.startMain(undefined, { lockTimeoutMs: 5_000, instrumentLeaseProof: true });
    const beforeNonce = `root-retry-before-${randomUUID()}`;
    await f.send(main.ready.id, beforeNonce);
    await until(() => nonceCopies(main.ready.sessionFile, beforeNonce).length === 1, "short-timeout healthy path never delivered");
    const reader = f.start("reader", main.ready.id);
    await f.assertRuntime(reader);
    const readerReady = await reader.next("reader-ready");
    const holder = f.start("holder");
    await f.assertRuntime(holder);
    const held = await holder.next("held");
    const reads: ProofEvent[] = [readerReady];
    const admissions: ProofEvent[] = [];
    const samples: Array<{ at: number; lease?: FabricHostLease | undefined }> = [];
    let released: ProofEvent | undefined;
    try {
      main.child.send("snapshot");
      const before = await main.child.next("snapshot") as Ready;
      const stateBytes = fs.readFileSync(path.join(f.meshRoot, "state.json"), "utf8");
      main.child.send("change");
      await main.child.next("refresh-started");
      const nonce = `root-retry-during-${randomUUID()}`;
      const sent = f.send(main.ready.id, nonce); // Once sent; no retry in this driver.
      const end = held.at + 17_000;
      let nextRead = held.at;
      while (Date.now() < end) {
        samples.push({ at: Date.now(), lease: readHostLeases(f.meshRoot).get(main.ready.id) });
        if (Date.now() >= nextRead) {
          reader.send("read");
          reads.push(await reader.next("reader-view", 3_000));
          main.child.send("admission");
          admissions.push(await main.child.next("admission", 3_000));
          nextRead = Date.now() + 1_000;
        }
        await delay(Math.min(250, Math.max(1, end - Date.now())));
      }
      // Explicit independent view AFTER the original 15s TTL, before release.
      reader.send("read");
      reads.push(await reader.next("reader-view"));
      main.child.send("snapshot");
      const during = await main.child.next("snapshot") as Ready;
      const unchanged = fs.readFileSync(path.join(f.meshRoot, "state.json"), "utf8") === stateBytes;
      const ownerUnchanged = fs.readFileSync(path.join(f.meshRoot, ".lock", "owner"), "utf8") === held.owner;
      const processUnchanged = startTime(held.pid) === held.incarnation;
      const heldEvents = main.child.events.filter(event => event.at >= held.at && event.at <= during.at);
      f.observations.push({ kind: "A-2/A-3-held", held, before, during, samples, reads, admissions,
        unchanged, ownerUnchanged, processUnchanged, heldEvents });
      holder.send("release");
      released = await holder.next("released");
      await holder.closed;
      await sent;
      await until(() => nonceCopies(main.ready.sessionFile, nonce).length === 1, "publication retry never delivered after release", 12_000);
      reader.send("close");
      const readerFinal = await reader.next("reader-final");
      const readerTerminal = await reader.next("native-terminal");
      const readerExit = await reader.closed;
      f.observations.push({ kind: "A-2/A-3-released", released, readerFinal, readerTerminal, readerExit });
      expect(readerExit).toMatchObject({ code: 0, signal: null });
      expect(startTime(reader.process.pid!)).toBe("");
      expect(readerTerminal.code).toBe(0);
      expect(released.at - held.at).toBeGreaterThanOrEqual(17_000);
      expect(ownerUnchanged && processUnchanged && unchanged).toBe(true);
      expect(during.confirmedAt).toBe(before.confirmedAt);
      expect(samples.every(sample => sample.lease && sample.lease.expiresAt >= sample.at &&
        sample.lease.session !== undefined && sample.lease.session.expiresAt >= sample.at)).toBe(true);
      expect(during.lease.incarnationToken).toBe(before.lease.incarnationToken);
      expect(during.lease.expiresAt).toBeGreaterThan(during.at);
      for (const view of [...reads, readerFinal]) {
        expect(view.target).toMatchObject({ id: main.ready.id, kind: "root", stale: false, local: false });
        expect(view.routingUnavailable).toBeUndefined();
        expect(view.heartbeatArmed).toBe(false);
        expect(view.ownLeasePresent).toBe(false);
        expect(view.policy).toMatchObject({ value: { version: 1, hostLeases: "files" } });
        const census = (view.census as Array<{ name: string; bytes: string }>).map(item => JSON.parse(item.bytes));
        expect(census.some(record => record.pid === main.ready.pid && record.startedAt === main.ready.processStartedAt &&
          record.stateBackend === "file" && record.lockProtocol === 2)).toBe(true);
        expect(census.some(record => record.pid === reader.process.pid)).toBe(true);
      }
      expect(reads.some(view => view.at > held.at + 15_000)).toBe(true);
      const overdue = admissions.filter(event => event.at - before.confirmedAt >= 2_000);
      expect(overdue.length).toBeGreaterThan(5);
      for (const event of overdue) {
        expect(event.canConsumeMesh).toBe(false);
        expect(event.confirmedAt).toBe(before.confirmedAt);
        expect((event.admission as Ready["admission"]).controlAdmission).toBe("gated");
      }
      const starts = heldEvents.filter(event => event.event === "publication-attempt-start");
      const failure = heldEvents.find(event => event.event === "publication-attempt-error");
      expect(failure, "need an actual failed publication, not an elapsed-time inference").toBeDefined();
      expect(String(failure!.error)).toMatch(/timed out.*mesh lock|mesh.*lock.*timed out|timeout/i);
      const first = starts.find(event => event.attempt === failure!.attempt)!;
      const retry = starts.find(event => event.at > failure!.at && event.attempt !== failure!.attempt);
      expect(first.lockTimeoutMs).toBe(5_000);
      expect(failure!.at - first.at).toBeGreaterThanOrEqual(4_900);
      expect(retry, "need an actual new publication attempt inside held custody").toBeDefined();
      assertHeld(first, main.ready, held);
      assertHeld(failure!, main.ready, held);
      assertHeld(retry!, main.ready, held);
      const writes = heldEvents.filter(event => event.event === "lease-write");
      const inside = writes.filter(event => event.attempt === first.attempt);
      expect(new Set(inside.map(event => (event.lease as FabricHostLease).updatedAt)).size).toBeGreaterThanOrEqual(3);
      for (const write of inside) assertHeld(write, main.ready, held);
      const unwound = heldEvents.find(event => event.event === "host-gate-released" && event.gate === first.gate);
      expect(unwound).toBeDefined();
      expect((unwound!.custody as Custody).gateOwner).toBeUndefined();
      const between = writes.find(event => event.attempt === undefined && event.at >= unwound!.at &&
        event.at <= retry!.at && event.gate !== first.gate && event.gate !== retry!.gate &&
        (event.lease as FabricHostLease).updatedAt > (inside[0]!.lease as FabricHostLease).updatedAt);
      expect(between, "FILE must advance after the failed fence releases and before the retry fence").toBeDefined();
      assertHeld(between!, main.ready, held);
      expect(main.child.events.some(event => event.event === "refresh-error" && event.at >= held.at && event.at < released!.at)).toBe(true);
      for (const value of [beforeNonce, nonce]) {
        expect(nonceCopies(main.ready.sessionFile, value)).toHaveLength(1);
        const event = f.commands().find(item => (item.data as { message?: string }).message === value)!;
        expect(outcomes(f.meshRoot).filter(item => item.eventId === event.id && item.outcome === "delivered")).toHaveLength(1);
      }
    } finally {
      if (!released && !holder.exit) {
        holder.send("release");
        await holder.next("released");
        await holder.closed;
      }
    }
  }, 65_000);
});
