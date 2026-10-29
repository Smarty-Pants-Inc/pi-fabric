import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { B1ErrorCode, RevokeGenerationRequest } from "./contract.js";
import type { B1Fact, B1Harness, B1HarnessFactory, B1MountFault, B1TeardownItem } from "./harness.js";

// Independent consumer-side fixture encoding. Not imported from the implementation.
function encode(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "number") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item: unknown) => encode(item)).join(",")}]`;
  if (typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
    .map(([key, item]: [string, unknown]) => `${JSON.stringify(key)}:${encode(item)}`).join(",")}}`;
  throw new Error("Unsupported conformance fixture value");
}
const hash = (bytes: string): string => createHash("sha256").update(bytes, "utf8").digest("hex");
const rehash = (request: RevokeGenerationRequest): RevokeGenerationRequest => {
  const { requestSha256: _hash, deadlineUnixMs: _deadline, ...members } = request;
  return { ...request, requestSha256: hash(encode(members)) };
};
async function failure(call: () => unknown, code: B1ErrorCode, disposition: "refused" | "held"): Promise<unknown> {
  let caught: unknown;
  try { await call(); } catch (error) { caught = error; }
  expect(caught).toBeInstanceOf(Error);
  if (!(caught instanceof Error)) throw new Error(`Expected an Error carrying ${code}`);
  expect(caught.message.length).toBeGreaterThan(0);
  if (!("code" in caught) || !("disposition" in caught) || !("evidence" in caught)) {
    throw new Error("B1 failure lost its public code/disposition/evidence");
  }
  expect(caught.code).toBe(code); expect(caught.disposition).toBe(disposition);
  expect(Array.isArray(caught.evidence)).toBe(true);
  if (!Array.isArray(caught.evidence)) throw new Error("Evidence must be an array");
  expect(caught.evidence.length).toBeGreaterThan(0);
  const references: readonly unknown[] = caught.evidence;
  for (const ref of references) {
    if (typeof ref !== "object" || ref === null || !("uri" in ref) || !("sha256" in ref)) {
      throw new Error("Failure evidence must preserve reference shape");
    }
    expect(typeof ref.uri).toBe("string"); expect(typeof ref.sha256).toBe("string");
  }
  return caught;
}
const withheld = (h: B1Harness): void => {
  expect(h.observe()).toMatchObject({ operation: { state: "held" }, attachGrant: null,
    retainedReceipt: null, receiverReceipt: null, receiverScheduled: false, receiverMounted: false });
};
const complete = async (h: B1Harness) => {
  h.completeRetirement(); const request = h.revokeRequest();
  const receipt = await h.ports.revokeGeneration(request);
  h.attachReservedReceiver();
  return { request, receipt, mount: await h.ports.currentMountGeneration(h.mountRequest()) };
};

/** Callable shared observed-B cases. Each case owns a fresh harness and finally closes it.
 * Keep: data safety and the B1 trust boundary. No real verifier/cluster/crash acceptance. */
export function runConformance(factory: B1HarnessFactory): void {
  const test = (name: string, run: (h: B1Harness) => void | Promise<void>): void => {
    it(name, async () => {
      const harness = await factory.create();
      try { await run(harness); } finally { harness.close(); }
    });
  };
  describe(`l7-b1-fence-v1 / observed B / ${factory.name}`, () => {
    test("declares only candidate-test observed B on both ports; no invented A fields", async h => {
      const { receipt, mount } = await complete(h);
      for (const result of [receipt, mount]) {
        expect(result.assurance).toMatchObject({ profile: "b1-v1-observed", staleDriverOperations: "not-prevented",
          lateBlockWrites: "not-guaranteed", qualification: { state: "candidate-test" } });
        expect("driverFenceEpoch" in result.assurance).toBe(false);
        expect("backendBarrier" in result.assurance).toBe(false);
        if (result.assurance.qualification.state !== "candidate-test") throw new Error("Fake cannot qualify production");
        expect(h.readEvidence(result.assurance.qualification.plan)).toMatchObject({
          trustDomain: "test-only", kind: "fabric.b1.qualification", version: 1,
        });
      }
      expect(h.observe()).toMatchObject({ profile: "b1-v1-observed", trustDomain: "test-only" });
    });

    test("A policy refuses before operation reservation, backend effects, or mount observation", async h => {
      h.completeRetirement(); const before = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash({ ...h.revokeRequest(), requiredProfile: "r3-storage-fence" })),
        "B1_POLICY_UNSATISFIED", "refused");
      expect(h.observe()).toEqual(before);
      await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), requiredProfile: "r3-storage-fence" }),
        "B1_POLICY_UNSATISFIED", "refused");
      expect(h.observe()).toEqual(before);
      await complete(h); // The same fixture actually succeeds under its declared B policy.
      const mounted = h.observe();
      await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), requiredProfile: "r3-storage-fence" }),
        "B1_POLICY_UNSATISFIED", "refused");
      expect(h.observe()).toEqual(mounted);
    });

    test("unknown profile fails closed rather than selecting an A-mode or B default", async h => {
      const request = h.revokeRequest();
      Object.defineProperty(request, "requiredProfile", { value: "unknown-profile", enumerable: true });
      const before = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash(request)), "B1_UNSUPPORTED_PROFILE", "refused");
      expect(h.observe()).toEqual(before);
    });

    test("valid-looking caller evidence cannot create inventory/exit facts or open the successor gate", async h => {
      const request = h.revokeRequest();
      const before = h.observe();
      await failure(() => h.attachReservedReceiver(), "B1_TEARDOWN_INCOMPLETE", "held");
      expect(h.observe()).toEqual(before);
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
      withheld(h);
      expect(h.observe().actualPredecessors).toEqual(request.predecessor.generations);
      expect(h.observe().actualPodUIDs.length).toBeGreaterThan(request.predecessor.generations.length);
      h.completeRetirement();
      const receipt = await h.ports.revokeGeneration(request);
      expect(receipt.exitedPodUIDs).toEqual(h.observe().actualPodUIDs);
      expect(h.observe()).toMatchObject({ receiverScheduled: false, receiverMounted: false,
        attachGrant: { receiver: request.reservedReceiver, acquisitionId: request.reservedAcquisitionId } });
      h.attachReservedReceiver();
      expect((await h.ports.currentMountGeneration(h.mountRequest())).self).toEqual(request.reservedReceiver);
    });

    const factCases: readonly { fact: B1Fact; code: B1ErrorCode }[] = [
      { fact: "inventoryComplete", code: "B1_PREDECESSOR_SET_INCOMPLETE" },
      { fact: "restartDisabled", code: "B1_RESTART_NOT_DISABLED" },
      { fact: "killIssued", code: "B1_KILL_NOT_CONFIRMED" },
      { fact: "workloadsExited", code: "B1_TASKS_STILL_ALIVE" },
      { fact: "oldPathRetired", code: "B1_TEARDOWN_INCOMPLETE" },
      { fact: "oldReplicasExited", code: "B1_TEARDOWN_INCOMPLETE" },
      { fact: "oldIoDrained", code: "B1_TEARDOWN_INCOMPLETE" },
      { fact: "driverOutcomeKnown", code: "B1_DRIVER_OUTCOME_UNKNOWN" },
      { fact: "nodeReachable", code: "B1_NODE_UNREACHABLE" },
      { fact: "coverageContinuous", code: "B1_TEARDOWN_INCOMPLETE" },
    ];
    for (const { fact, code } of factCases) test(`independent ${fact}=false holds even with complete caller/T1–T8 evidence`, async h => {
      h.completeRetirement(); h.setFact(fact, false); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), code, "held");
      withheld(h);
      const held = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash({ ...request, operationId: `${request.operationId}:replacement` })),
        "B1_OPERATION_CONFLICT", "refused");
      expect(h.observe()).toEqual(held);
      h.setFact(fact, true);
      const receipt = await h.ports.revokeGeneration({ ...request, deadlineUnixMs: request.deadlineUnixMs + 1_000 });
      expect(receipt.operationId).toBe(request.operationId);
      h.attachReservedReceiver();
      expect((await h.ports.currentMountGeneration(h.mountRequest())).acquisitionId).toBe(request.reservedAcquisitionId);
    });

    const teardownItems: readonly B1TeardownItem[] = ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8"];
    for (const item of teardownItems) test(`${item} is conjunctive, not a success-shaped summary flag`, async h => {
      h.completeRetirement(); h.setTeardown(item, false); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held"); withheld(h);
      h.setTeardown(item, true);
      const receipt = await h.ports.revokeGeneration(request);
      expect(receipt.teardown).toHaveLength(8);
      const observed = receipt.teardown.map(ref => {
        const payload = h.readEvidence(ref);
        expect(hash(encode(payload))).toBe(ref.sha256);
        return payload;
      });
      expect(observed).toEqual(teardownItems.map(id => expect.objectContaining({ kind: "fabric.b1.teardown", version: 1,
        trustDomain: "test-only", data: expect.objectContaining({ item: id, operationId: request.operationId,
          exitedPodUIDs: h.observe().actualPodUIDs }) })));
    });

    test("restart disable precedes kill submission; kill confirmation precedes retirement request", async h => {
      h.setFact("inventoryComplete", true); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), "B1_RESTART_NOT_DISABLED", "held");
      expect(h.observe().operation).toMatchObject({ restartRevocationPersisted: true, killSubmitted: false, retirementRequested: false });
      h.setFact("restartDisabled", true);
      await failure(() => h.ports.revokeGeneration(request), "B1_KILL_NOT_CONFIRMED", "held");
      expect(h.observe().operation).toMatchObject({ killSubmitted: true, retirementRequested: false });
      h.setFact("killIssued", true);
      await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
      expect(h.observe().operation).toMatchObject({ retirementRequested: true });
      h.completeRetirement(); await h.ports.revokeGeneration(request);
    });

    test("held Error preserves operation, current durable-state reference and independently resolvable evidence", async h => {
      const request = h.revokeRequest();
      const error = await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
      expect(error).toMatchObject({ operationId: request.operationId,
        durableState: { resourceVersion: h.observe().controllerRevision }, evidence: [request.predecessor.enumeration] });
      expect(h.readEvidence(request.predecessor.enumeration)).toMatchObject({ kind: "fabric.b1.inventory",
        trustDomain: "test-only", data: { podUIDs: h.observe().actualPodUIDs } });
      withheld(h);
    });

    test("failed/ungranted candidates cannot disappear from the predecessor inventory", async h => {
      h.completeRetirement(); const original = h.revokeRequest();
      const request = rehash({ ...original, predecessor: { ...original.predecessor,
        podUIDs: original.predecessor.generations.map(g => g.receiver.podUID) } });
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held"); withheld(h);
      // Repairing semantic bytes is not a same-operation retry or an override.
      await failure(() => h.ports.revokeGeneration(original), "B1_OPERATION_CONFLICT", "refused"); withheld(h);
    });

    test("a caller cannot fabricate restart/kill evidence refs to supply model facts", async h => {
      h.completeRetirement(); const original = h.revokeRequest();
      const request = rehash({ ...original, predecessor: { ...original.predecessor,
        restartDisabled: { uri: "caller://claims-safe", sha256: "a".repeat(64) },
        killIssued: { uri: "caller://claims-killed", sha256: "b".repeat(64) } } });
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held"); withheld(h);
    });

    const changedMembers: readonly { name: string; change: (r: RevokeGenerationRequest) => RevokeGenerationRequest }[] = [
      { name: "receiver", change: r => ({ ...r, reservedReceiver: { ...r.reservedReceiver, podUID: "changed" } }) },
      { name: "profile", change: r => ({ ...r, requiredProfile: "r3-storage-fence" }) },
      { name: "original reservation revision", change: r => ({ ...r, expectedControllerRevision: "999" }) },
      { name: "acquisition", change: r => ({ ...r, reservedAcquisitionId: "changed" }) },
      { name: "storage", change: r => ({ ...r, storage: { ...r.storage, filesystemUUID: "changed" } }) },
      { name: "predecessors", change: r => ({ ...r, predecessor: { ...r.predecessor, podUIDs: [] } }) },
    ];
    for (const { name, change } of changedMembers) test(`changed ${name} conflicts on held and completed replay without effects`, async h => {
      h.completeRetirement(); h.setFact("workloadsExited", false); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
      const held = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash(change(request))), "B1_OPERATION_CONFLICT", "refused");
      expect(h.observe()).toEqual(held);
      h.setFact("workloadsExited", true); await h.ports.revokeGeneration(request); const done = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash(change(request))), "B1_OPERATION_CONFLICT", "refused");
      expect(h.observe()).toEqual(done);
    });

    test("hash excludes itself/deadline, includes original reservation revision, and progress does not repin it", async h => {
      const request = h.revokeRequest();
      expect(request.requestSha256).toBe(rehash(request).requestSha256);
      const before = h.observe();
      await failure(() => h.ports.revokeGeneration({ ...request, requestSha256: "0".repeat(64) }), "B1_INVALID_ARGUMENT", "refused");
      expect(h.observe()).toEqual(before);
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
      const held = h.observe();
      expect(held.controllerRevision).not.toBe(request.expectedControllerRevision);
      expect(held.operation).toMatchObject({ reservationRevision: request.expectedControllerRevision });
      expect(rehash({ ...request, deadlineUnixMs: request.deadlineUnixMs + 10_000 }).requestSha256).toBe(request.requestSha256);
      expect(rehash({ ...request, expectedControllerRevision: held.controllerRevision }).requestSha256).not.toBe(request.requestSha256);
      h.completeRetirement();
      const receipt = await h.ports.revokeGeneration({ ...request, deadlineUnixMs: request.deadlineUnixMs + 10_000 });
      expect(receipt.requestSha256).toBe(request.requestSha256);
      expect(h.observe().operation?.reservationRevision).toBe(request.expectedControllerRevision);
    });

    test("expired invocation holds its slot; extended deadline progresses, completed readback ignores elapsed deadline", async h => {
      h.completeRetirement(); const request = { ...h.revokeRequest(), deadlineUnixMs: 0 };
      await failure(() => h.ports.revokeGeneration(request), "B1_DEADLINE_HELD", "held"); withheld(h);
      expect(h.observe().operation).toMatchObject({ killSubmitted: false, retirementRequested: false });
      const result = await h.ports.revokeGeneration({ ...request, deadlineUnixMs: h.revokeRequest().deadlineUnixMs });
      const done = h.observe();
      expect(await h.ports.revokeGeneration(request)).toEqual(result);
      expect(h.observe()).toEqual(done);
    });

    test("retained payload bytes/hash exclude record envelope and replies cannot mutate the retained receipt", async h => {
      const { request, receipt } = await complete(h);
      const retained = h.observe().retainedReceipt;
      if (!retained) throw new Error("Expected exact retained payload bytes");
      const { record, ...payload } = receipt;
      expect(encode(payload)).toBe(retained.payloadBytes);
      expect(hash(retained.payloadBytes)).toBe(record.sha256);
      expect(retained.payloadBytes.includes('"record":')).toBe(false);
      expect(hash(encode(receipt))).not.toBe(record.sha256);
      const pristine = structuredClone(receipt), before = h.observe();
      Object.defineProperty(receipt.storage, "sessionId", { value: "caller-mutated" });
      Object.defineProperty(receipt.record, "sha256", { value: "caller-mutated" });
      Object.defineProperty(receipt.assurance, "profile", { value: "r3-storage-fence" });
      expect(await h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 })).toEqual(pristine);
      expect(h.observe()).toEqual(before);
    });

    test("lost commit ACK and client reopen preserve operation, receipt bytes and grant without repeated effects", async h => {
      h.completeRetirement(); h.loseNextRevokeAck(); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
      const committed = h.observe();
      expect(committed).toMatchObject({ operation: { state: "committed" }, receiverScheduled: false });
      expect(committed.attachGrant).not.toBeNull(); expect(committed.retainedReceipt).not.toBeNull();
      const oldPorts = h.ports; h.reopen(); expect(h.observe()).toEqual(committed);
      await failure(() => oldPorts.revokeGeneration(request), "B1_OBSERVER_UNAVAILABLE", "held");
      const receipt = await h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 });
      expect(receipt.record).toEqual(committed.retainedReceipt?.record);
      expect(h.observe()).toEqual(committed);
      h.attachReservedReceiver(); const attached = h.observe(); h.reopen();
      expect(h.observe()).toEqual(attached);
      const mount = await h.ports.currentMountGeneration(h.mountRequest());
      expect(mount.acquisitionId).toBe(request.reservedAcquisitionId); expect(h.observe()).toEqual(attached);
    });

    test("new/superseded operation cannot borrow a committed reservation or touch the receiver", async h => {
      const { request } = await complete(h); const before = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash({ ...request, operationId: `${request.operationId}:new` })),
        "B1_STALE_GENERATION", "refused");
      expect(h.observe()).toEqual(before);
    });

    test("first invocation requires actual storage, receiver, acquisition and reservation identity", async h => {
      const original = h.revokeRequest(), before = h.observe();
      for (const [request, code] of [
        [rehash({ ...original, storage: { ...original.storage, pvcUID: "other" } }), "B1_STORAGE_IDENTITY_MISMATCH"],
        [rehash({ ...original, reservedReceiver: { ...original.reservedReceiver, podUID: "other" } }), "B1_NOT_AUTHORIZED"],
        [rehash({ ...original, reservedAcquisitionId: "other" }), "B1_NOT_AUTHORIZED"],
        [rehash({ ...original, expectedControllerRevision: "0" }), "B1_STALE_GENERATION"],
      ] satisfies readonly (readonly [RevokeGenerationRequest, B1ErrorCode])[]) {
        await failure(() => h.ports.revokeGeneration(request), code, "refused"); expect(h.observe()).toEqual(before);
      }
    });

    test("immediate same-node/node-name reuse is refused before handover effects", async h => {
      const original = h.revokeRequest(), old = original.predecessor.generations[0];
      if (!old) throw new Error("Required fixture needs a real predecessor");
      const before = h.observe();
      for (const receiver of [{ ...original.reservedReceiver, nodeUID: old.receiver.nodeUID },
        { ...original.reservedReceiver, nodeName: old.receiver.nodeName, nodeUID: "recreated-node-uid" }]) {
        await failure(() => h.ports.revokeGeneration(rehash({ ...original, reservedReceiver: receiver })),
          "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused"); expect(h.observe()).toEqual(before);
      }
    });

    test("empty REQUIRED inventory is held even with valid hashes and complete retirement flags", async h => {
      h.completeRetirement(); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(rehash({ ...request, predecessor: { ...request.predecessor,
        generations: [], podUIDs: [] } })), "B1_PREDECESSOR_SET_INCOMPLETE", "held"); withheld(h);
    });
    test("NEW without its independently sealed fixture anchor is held", async h => {
      h.provisionNewVolume(false); h.completeRetirement(); const request = h.revokeRequest();
      expect(request.predecessor.podUIDs).toEqual([]);
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held"); withheld(h);
    });
    test("sealed NEW alone permits empty predecessors, never reinitialization of an owned operation", async h => {
      h.provisionNewVolume(true); const { receipt } = await complete(h);
      expect(receipt.revoked).toEqual([]); expect(receipt.exitedPodUIDs).toEqual([]);
      const before = h.observe();
      await failure(() => h.provisionNewVolume(true), "B1_OPERATION_CONFLICT", "refused"); expect(h.observe()).toEqual(before);
    });

    test("ReceiverReceipt is produced only by fixture controller after actual attach, not by revoke or a request builder", async h => {
      h.completeRetirement(); const before = h.observe();
      h.revokeRequest(); h.mountRequest(); expect(h.observe()).toEqual(before);
      const request = h.revokeRequest(), receipt = await h.ports.revokeGeneration(request);
      const excluded = h.observe();
      expect(excluded).toMatchObject({ receiverReceipt: null, receiverReceiptPayloadBytes: null,
        receiverScheduled: false, receiverMounted: false });
      await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_MOUNT_NOT_FOUND", "refused");
      expect(h.observe()).toEqual(excluded);
      h.attachReservedReceiver(); const attached = h.observe(), receiver = attached.receiverReceipt;
      if (!receiver || !attached.receiverReceiptPayloadBytes) throw new Error("Actual fixture controller attach must publish a receiver receipt");
      const mount = await h.ports.currentMountGeneration(h.mountRequest());
      expect(receiver).toMatchObject({ kind: "fabric.b1.receiver", storage: mount.storage, receiver: mount.self,
        acquisitionId: mount.acquisitionId, engine: mount.engine, bootstrapExecutionId: mount.actualMount.bootstrapExecutionId,
        revokeReceipt: receipt.record, attachObservation: mount.attachObservation });
      const { record, ...payload } = receiver;
      expect(encode(payload)).toBe(attached.receiverReceiptPayloadBytes);
      expect(hash(attached.receiverReceiptPayloadBytes)).toBe(record.sha256);
      const pristineObservation = structuredClone(attached);
      Object.defineProperty(receiver.receiver, "podUID", { value: "caller-mutated-old-self-receipt" });
      expect(h.observe()).toEqual(pristineObservation);
    });

    test("post-S6 observation drift blocks both attach and current mount without borrowing old proof", async h => {
      h.completeRetirement(); const request = h.revokeRequest(); await h.ports.revokeGeneration(request);
      h.setFact("oldPathRetired", false); const excluded = h.observe();
      await failure(() => h.attachReservedReceiver(), "B1_UNEXPECTED_ATTACHMENT", "held"); expect(h.observe()).toEqual(excluded);
      h.setFact("oldPathRetired", true); h.attachReservedReceiver();
      await h.ports.currentMountGeneration(h.mountRequest());
      h.setFact("coverageContinuous", false); const changed = h.observe();
      await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_UNEXPECTED_ATTACHMENT", "held");
      expect(h.observe()).toEqual(changed);
    });

    test("mount challenge/deadline validation remains read-only on the actual mounted fixture", async h => {
      await complete(h); const before = h.observe(), request = h.mountRequest();
      await failure(() => h.ports.currentMountGeneration({ ...request, challengeNonce: "" }), "B1_INVALID_ARGUMENT", "refused");
      await failure(() => h.ports.currentMountGeneration({ ...request, deadlineUnixMs: 0 }), "B1_DEADLINE_HELD", "held");
      expect(h.observe()).toEqual(before);
      expect((await h.ports.currentMountGeneration(h.mountRequest())).self).toEqual(h.revokeRequest().reservedReceiver);
    });

    test("post-cut replicas are independently required before actual receiver attach", async h => {
      h.completeRetirement(); h.setFact("newReplicasAfterCut", false);
      await h.ports.revokeGeneration(h.revokeRequest()); const before = h.observe();
      await failure(() => h.attachReservedReceiver(), "B1_MOUNT_LINEAGE_UNKNOWN", "held"); expect(h.observe()).toEqual(before);
      h.setFact("newReplicasAfterCut", true); h.attachReservedReceiver();
      expect((await h.ports.currentMountGeneration(h.mountRequest())).engine).not.toEqual(h.observe().actualPredecessors[0]?.engine);
    });

    test("mount read traverses actual topology, authenticates caller target, and fresh nonce is not an engine echo", async h => {
      const { request, receipt, mount } = await complete(h); const before = h.observe();
      const first = h.mountRequest(), second = h.mountRequest();
      expect(first.challengeNonce).not.toBe(second.challengeNonce);
      expect("engine" in first).toBe(false); expect("expectedEngineUUID" in first).toBe(false);
      const one = await h.ports.currentMountGeneration(first), two = await h.ports.currentMountGeneration(second);
      expect(one.challengeNonce).toBe(first.challengeNonce); expect(two.challengeNonce).toBe(second.challengeNonce);
      expect(one.engine).toEqual(two.engine); expect(one.engine).toEqual(mount.engine);
      expect(one.self).toEqual(request.reservedReceiver); expect(one.storage).toEqual(request.storage);
      expect(one.engine).not.toEqual(request.predecessor.generations[0]?.engine);
      expect(one.observation.sha256).not.toBe(two.observation.sha256);
      const observation = h.readEvidence(one.observation);
      expect(hash(encode(observation))).toBe(one.observation.sha256);
      expect(hash(encode(h.readEvidence(one.attachObservation)))).toBe(one.attachObservation.sha256);
      expect(observation).toMatchObject({ kind: "fabric.b1.mount-observation", trustDomain: "test-only", version: 1,
        data: { challengeNonce: first.challengeNonce, nativeEpoch: { kind: "absent" },
          lockFd: { role: "lock", mountId: one.actualMount.mountId },
          generationFd: { role: "generation", mountId: one.actualMount.mountId } } });
      expect(h.readEvidence(one.attachObservation)).toMatchObject({ kind: "fabric.b1.attach-observation",
        data: { revokeReceiptHash: receipt.record.sha256, engine: one.engine, nativeEpoch: { kind: "absent" } } });
      expect(h.observe()).toEqual(before);
      await failure(() => h.ports.currentMountGeneration({ ...first, target: { ...first.target, bootstrapHandle: "caller-pid" } }),
        "B1_NOT_AUTHORIZED", "refused");
      await failure(() => h.ports.currentMountGeneration({ ...first, target: { ...first.target, protectedRoot: "/other" } }),
        "B1_MOUNT_NOT_FOUND", "refused");
      expect(h.observe()).toEqual(before);
    });

    const mountFaults: readonly { fault: B1MountFault; code: B1ErrorCode; disposition: "refused" | "held" }[] = [
      { fault: "wrong-self", code: "B1_SELF_IDENTITY_MISMATCH", disposition: "refused" },
      { fault: "wrong-storage", code: "B1_STORAGE_IDENTITY_MISMATCH", disposition: "refused" },
      { fault: "wrong-bootstrap", code: "B1_SELF_IDENTITY_MISMATCH", disposition: "refused" },
      { fault: "wrong-root", code: "B1_SELF_IDENTITY_MISMATCH", disposition: "refused" },
      { fault: "lineage-unknown", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "lock-off-volume", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "generation-off-volume", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "lock-fd-missing", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "generation-fd-missing", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "stale-observation", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "observer-unavailable", code: "B1_OBSERVER_UNAVAILABLE", disposition: "held" },
      { fault: "engine-changed", code: "B1_MOUNT_CHANGED", disposition: "refused" },
      { fault: "device-reused", code: "B1_MOUNT_CHANGED", disposition: "refused" },
      { fault: "old-replica", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
    ];
    for (const { fault, code, disposition } of mountFaults) test(`independently observed ${fault} refuses/holds without custody or storage mutation`, async h => {
      const { mount } = await complete(h); expect(mount.custody.bothFilesOnBoundMount).toBe(true);
      h.setMountFault(fault); const before = h.observe();
      await failure(() => h.ports.currentMountGeneration(h.mountRequest()), code, disposition);
      expect(h.observe()).toEqual(before);
    });

    test("canonical counters retain bigint precision, and mount read never increments/reclaims custody", async h => {
      await complete(h); h.setCustodyValue("9007199254740993"); const before = h.observe();
      expect((await h.ports.currentMountGeneration(h.mountRequest())).custody.value).toBe("9007199254740993");
      expect(h.observe()).toEqual(before);
      h.incrementCustody(); const advanced = h.observe(); expect(advanced.custodyValue).toBe("9007199254740994");
      expect((await h.ports.currentMountGeneration(h.mountRequest())).custody.value).toBe("9007199254740994");
      expect(h.observe()).toEqual(advanced);
      h.setCustodyValue("0"); const zero = h.observe();
      expect((await h.ports.currentMountGeneration(h.mountRequest())).custody.value).toBe("0");
      expect(h.observe()).toEqual(zero);
    });
    for (const value of ["", "00", "+1", "-1", "1.0", "1e3", " 1", "1\n"]) {
      test(`noncanonical counter ${JSON.stringify(value)} refuses without repair/reset`, async h => {
        await complete(h); h.setCustodyValue(value); const before = h.observe();
        await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_GENERATION_INVALID", "refused");
        expect(h.observe()).toEqual(before);
      });
    }

    for (const action of ["detach", "reattach-old", "old-write"] satisfies readonly ("detach" | "reattach-old" | "old-write")[]) {
      test(`B raw stale ${action} really changes model state: strict R3 property NOT provided, mount is contained`, async h => {
        const { request, receipt } = await complete(h), before = h.observe();
        h.executeRawStaleAction(action); const after = h.observe();
        expect(after.storageSnapshot).not.toBe(before.storageSnapshot); expect(after.contained).toBe(true);
        if (action === "detach") expect(after.actualAttachment).toBe("none");
        if (action === "reattach-old") expect(after.actualAttachment).toBe("predecessor");
        if (action === "old-write") expect(after.applicationBytes).not.toBe(before.applicationBytes);
        await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_MOUNT_CHANGED", "refused");
        expect(await h.ports.revokeGeneration(request)).toEqual(receipt); // Historical bytes, not a new capability.
        await failure(() => h.attachReservedReceiver(), "B1_STALE_GENERATION", "refused");
        expect(h.observe()).toEqual(after);
      });
    }

    test("factories isolate state; close is idempotent and cannot erase retained evidence", async h => {
      const { request } = await complete(h); const before = h.observe();
      const other = await factory.create();
      try {
        expect(other.revokeRequest().storage).not.toEqual(request.storage);
        expect(other.observe()).toMatchObject({ operation: null, attachGrant: null, receiverMounted: false });
        other.completeRetirement(); await other.ports.revokeGeneration(other.revokeRequest());
        expect(h.observe()).toEqual(before);
      } finally { other.close(); }
      h.close(); h.close(); expect(h.observe()).toEqual(before);
      await failure(() => h.ports.revokeGeneration(request), "B1_OBSERVER_UNAVAILABLE", "held");
      expect(h.observe()).toEqual(before);
    });
  });
}
