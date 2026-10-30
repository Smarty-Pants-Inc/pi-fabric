import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { B1ErrorCode, CurrentMountGenerationRequest, ReceiverIdentity, RevokeGenerationRequest } from "./contract.js";
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
export type B1FieldPath = readonly string[];
/** Enumerate the materialized typed request/evidence shape, including collection
 * membership AND every member field. No selected-field allow-list. Array indexes
 * become '*' and are expanded against ALL elements of each actual handover. */
export function fenceFieldPaths(value: unknown, path: B1FieldPath = []): B1FieldPath[] {
  if (Array.isArray(value)) {
    const children = value.flatMap((entry: unknown) => fenceFieldPaths(entry, [...path, "*"]));
    return [path, ...new Map((children.length ? children : [[...path, "*"]]).map(p => [p.join("."), p])).values()];
  }
  if (value !== null && typeof value === "object") {
    return Object.entries(value).flatMap(([key, child]) => fenceFieldPaths(child, [...path, key]));
  }
  return [path];
}
export function fenceFieldValue(value: unknown, path: B1FieldPath): unknown {
  for (const key of path) {
    if (value === null || typeof value !== "object") return undefined;
    value = (value as Record<string, unknown>)[key];
  }
  return value;
}
function concreteFields(value: unknown, path: B1FieldPath, prefix: B1FieldPath = []): B1FieldPath[] {
  const [head, ...tail] = path;
  if (head === undefined) return [prefix];
  if (head === "*") {
    if (!Array.isArray(value)) return [];
    // No fictitious absent element: verify an empty primitive collection's
    // membership with an added member. The compiler-API audit checks the shape.
    if (!value.length && !tail.length) return [prefix];
    return value.flatMap((entry: unknown, i: number) => concreteFields(entry, tail, [...prefix, String(i)]));
  }
  return concreteFields(fenceFieldValue(value, [head]), tail, [...prefix, head]);
}
function replaceField<T>(value: T, path: B1FieldPath, replacement: unknown): T {
  const result = structuredClone(value), parent = fenceFieldValue(result, path.slice(0, -1));
  const key = path.at(-1);
  if (!key || !parent || typeof parent !== "object") throw new Error(`Missing generated field ${path.join(".")}`);
  (parent as Record<string, unknown>)[key] = structuredClone(replacement);
  return result;
}
/** Equal previous values (schema constants / same-volume identity / recyclable
 * names) are valid: not a stale-evidence negative. Exercise a wrong value instead;
 * the equality is an executable equivalence proof, never a claimed stale kill. */
function wrongField(value: unknown): unknown {
  if (Array.isArray(value)) return value.length ? [] : ["test-only-unretained-member"];
  if (typeof value === "boolean") return !value;
  if (typeof value === "number") return 0;
  if (typeof value === "string") return value === "" ? "test-only-wrong" : "";
  return "test-only-wrong";
}

export async function runConformance(factory: B1HarnessFactory,
  options: { readonly include?: (name: string) => boolean } = {}): Promise<void> {
  // Registration enumerates real typed values. No source parser, fake import,
  // private-state access, hand-maintained field list or test-side result mutation.
  const schemaHarness = await factory.create();
  let fieldShapes: { request: RevokeGenerationRequest; mountRequest: CurrentMountGenerationRequest;
    inputs: ReturnType<B1Harness["fenceInputs"]> };
  try {
    await complete(schemaHarness);
    fieldShapes = { request: schemaHarness.revokeRequest(), mountRequest: schemaHarness.mountRequest(),
      inputs: schemaHarness.fenceInputs() };
  } finally { schemaHarness.close(); }
  const test = (name: string, run: (h: B1Harness) => void | Promise<void>): void => {
    if (options.include && !options.include(name)) return;
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

    // Design §8.1.6: fail-closed decoding is required independently on BOTH ports.
    test("unknown mount profile fails closed on an actually mounted receiver", async h => {
      await complete(h); const request = h.mountRequest(), before = h.observe();
      Object.defineProperty(request, "requiredProfile", { value: "unknown-profile", enumerable: true });
      await failure(() => h.ports.currentMountGeneration(request), "B1_UNSUPPORTED_PROFILE", "refused");
      expect(h.observe()).toEqual(before);
    });
    for (const version of [0, 2]) {
      test(`revoke version ${version} is invalid before any effects (§8.1.6)`, async h => {
        h.completeRetirement(); const request = h.revokeRequest(), before = h.observe();
        Object.defineProperty(request, "version", { value: version, enumerable: true });
        await failure(() => h.ports.revokeGeneration(rehash(request)), "B1_INVALID_ARGUMENT", "refused");
        expect(h.observe()).toEqual(before);
      });
      test(`mount version ${version} is invalid and read-only (§8.1.6)`, async h => {
        await complete(h); const request = h.mountRequest(), before = h.observe();
        Object.defineProperty(request, "version", { value: version, enumerable: true });
        await failure(() => h.ports.currentMountGeneration(request), "B1_INVALID_ARGUMENT", "refused");
        expect(h.observe()).toEqual(before);
      });
    }
    for (const operationId of ["", " \t"]) test(`empty operationId ${JSON.stringify(operationId)} is invalid (§8.1.6)`, async h => {
      h.completeRetirement(); const before = h.observe();
      await failure(() => h.ports.revokeGeneration(rehash({ ...h.revokeRequest(), operationId })), "B1_INVALID_ARGUMENT", "refused");
      expect(h.observe()).toEqual(before);
    });
    for (const [label, deadlineUnixMs] of [["NaN", NaN], ["negative", -1], ["fractional", 0.5],
      ["infinite", Infinity], ["unsafe integer", Number.MAX_SAFE_INTEGER + 1]] as const) {
      test(`revoke ${label} deadline is invalid, not a liveness bypass (§8.1.1/6/7)`, async h => {
        h.completeRetirement(); const before = h.observe();
        await failure(() => h.ports.revokeGeneration({ ...h.revokeRequest(), deadlineUnixMs }), "B1_INVALID_ARGUMENT", "refused");
        expect(h.observe()).toEqual(before);
      });
      test(`mount ${label} deadline is invalid and read-only (§8.1.6/7)`, async h => {
        await complete(h); const before = h.observe();
        await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), deadlineUnixMs }), "B1_INVALID_ARGUMENT", "refused");
        expect(h.observe()).toEqual(before);
      });
    }

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

    test("revoke receipt binds exact storage, reservation and revoked inventory (§8.1.3/§8.3)", async h => {
      h.completeRetirement(); const request = h.revokeRequest(), actual = h.observe();
      expect(actual.actualPredecessors.length).toBeGreaterThan(0);
      const receipt = await h.ports.revokeGeneration(request);
      expect(receipt).toMatchObject({ version: 1, kind: "fabric.b1.revoke", operationId: request.operationId,
        requestSha256: request.requestSha256 });
      expect(receipt.storage).toEqual(request.storage);
      expect(receipt.reservedReceiver).toEqual(request.reservedReceiver);
      expect(receipt.reservedAcquisitionId).toBe(request.reservedAcquisitionId);
      expect(receipt.revoked).toEqual(actual.actualPredecessors); // Exact list, not subset/duplicated summary.
      expect(receipt.revoked).toEqual(request.predecessor.generations);
      expect(receipt.exitedPodUIDs).toEqual(actual.actualPodUIDs);
      expect(h.observe().attachGrant).toEqual({ receiver: receipt.reservedReceiver,
        acquisitionId: receipt.reservedAcquisitionId, revokeReceiptHash: receipt.record.sha256 });
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

    // Design §8.1.5/§9 #2: facts must be re-observed AFTER effects, not only on entry.
    for (const { fact, code } of [
      { fact: "killIssued", code: "B1_KILL_NOT_CONFIRMED" },
      { fact: "restartDisabled", code: "B1_RESTART_NOT_DISABLED" },
      { fact: "nodeReachable", code: "B1_NODE_UNREACHABLE" },
    ] satisfies readonly { fact: B1Fact; code: B1ErrorCode }[]) {
      test(`post-effect ${fact} regression is rechecked before commit (§8.1.5/§9 #2)`, async h => {
        h.completeRetirement(); h.setFact("workloadsExited", false); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
        expect(h.observe().operation).toMatchObject({ restartRevocationPersisted: true, killSubmitted: true, retirementRequested: true });
        h.setFact("workloadsExited", true); h.setFact(fact, false); const changed = h.observe();
        await failure(() => h.ports.revokeGeneration(request), code, "held"); withheld(h);
        expect(h.observe()).toEqual(changed); // No new effects/receipt while the regression is present.
        h.setFact(fact, true);
        expect((await h.ports.revokeGeneration(request)).operationId).toBe(request.operationId);
      });
    }
    test("task discovered after kill invalidates immutable coverage and retains the slot (§8.1.5/§9 #2)", async h => {
      h.completeRetirement(); h.setFact("workloadsExited", false); const request = h.revokeRequest();
      await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
      const tasks = h.observe().actualWorkloadTaskIds;
      h.discoverWorkloadTask(); h.setFact("workloadsExited", true); const discovered = h.observe();
      expect(discovered.actualWorkloadTaskIds.length).toBeGreaterThan(tasks.length);
      expect(h.revokeRequest().predecessor).toEqual(request.predecessor); // Never rewrite hashed evidence in place.
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
      expect(h.observe()).toEqual(discovered); withheld(h);
      h.completeRetirement(); const claimedComplete = h.observe(); // A boolean cannot repair old immutable coverage.
      await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
      expect(h.observe()).toEqual(claimedComplete);
      await failure(() => h.ports.revokeGeneration(rehash({ ...request, operationId: `${request.operationId}:replacement` })),
        "B1_OPERATION_CONFLICT", "refused"); expect(h.observe()).toEqual(claimedComplete);
      h.retireFailedCandidateAndReserve({ ...request.reservedReceiver, podUID: `${request.reservedReceiver.podUID}:fresh`,
        nodeName: "fresh-after-task", nodeUID: "fresh-after-task-uid" });
      const transferred = h.observe();
      await failure(() => h.ports.revokeGeneration(request), "B1_STALE_GENERATION", "refused");
      expect(h.observe()).toEqual(transferred);
      h.completeRetirement(); const next = h.revokeRequest();
      expect(next.operationId).not.toBe(request.operationId); expect(next.reservedAcquisitionId).not.toBe(request.reservedAcquisitionId);
      expect(next.predecessor.podUIDs).toContain(request.reservedReceiver.podUID);
      expect((await h.ports.revokeGeneration(next)).reservedReceiver).toEqual(next.reservedReceiver);
    });

    // Design §4.1/§9 #3: exact HELD retries must not resubmit already-recorded effects.
    for (const { fact, code } of [
      { fact: "restartDisabled", code: "B1_RESTART_NOT_DISABLED" },
      { fact: "killIssued", code: "B1_KILL_NOT_CONFIRMED" },
      { fact: "oldPathRetired", code: "B1_TEARDOWN_INCOMPLETE" },
      { fact: "workloadsExited", code: "B1_TASKS_STILL_ALIVE" },
    ] satisfies readonly { fact: B1Fact; code: B1ErrorCode }[]) {
      test(`exact held retry at ${fact} leaves the complete observed state unchanged (§4.1/§9 #3)`, async h => {
        h.completeRetirement(); h.setFact(fact, false); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), code, "held"); const held = h.observe();
        await failure(() => h.ports.revokeGeneration(request), code, "held"); expect(h.observe()).toEqual(held);
        h.reopen();
        await failure(() => h.ports.revokeGeneration({ ...request, deadlineUnixMs: request.deadlineUnixMs + 10_000 }), code, "held");
        expect(h.observe()).toEqual(held); withheld(h);
        h.setFact(fact, true); await h.ports.revokeGeneration(request);
      });
    }

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
        // §8.1.2 requires equality, not merely revision >= the completed S1 reservation.
        [rehash({ ...original, expectedControllerRevision: (BigInt(original.expectedControllerRevision) + 1n).toString() }), "B1_STALE_GENERATION"],
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

    // Design §4.1/§9 #4: exercise real multi-handover histories through the ports.
    const freshNode = (h: B1Harness, name: string): ReceiverIdentity => ({ nodeName: name,
      nodeUID: `${h.revokeRequest().storage.identityRecordUID}:${name}:uid`,
      podUID: `${h.revokeRequest().storage.identityRecordUID}:${name}:pod` });
    test("B→C→B historical-node quarantine refuses non-immediate reuse (§4.1/§9 #4)", async h => {
      const first = h.revokeRequest(), b = first.predecessor.generations[0]?.receiver;
      if (!b) throw new Error("Required fixture needs initial B");
      const c = (await complete(h)).mount;
      h.reserveNextReceiver({ ...b, podUID: `${b.podUID}:new-pod` }); h.completeRetirement();
      const request = h.revokeRequest(), before = h.observe();
      expect(request.predecessor.generations.map(g => g.receiver)).toEqual([c.self]);
      expect(before.quarantinedNodeNames).toContain(b.nodeName);
      await failure(() => h.ports.revokeGeneration(request), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
      expect(h.observe()).toEqual(before);
      expect(before).toMatchObject({ operation: null, attachGrant: null, receiverMounted: false });
    });
    test("historical node-name recreation does not erase its ticket key (§4.1/§9 #4)", async h => {
      const b = h.revokeRequest().predecessor.generations[0]?.receiver;
      if (!b) throw new Error("Required fixture needs initial B");
      await complete(h);
      h.reserveNextReceiver({ ...b, podUID: `${b.podUID}:recreated`, nodeUID: `${b.nodeUID}:recreated` });
      h.completeRetirement(); const before = h.observe();
      await failure(() => h.ports.revokeGeneration(h.revokeRequest()), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
      expect(h.observe()).toEqual(before);
    });
    for (const size of [2, 3]) test(`N=${size} pool allows exactly N−1 fresh handovers then holds admission (§4.1/§9 #4)`, async h => {
      const initial = h.revokeRequest(), b = initial.predecessor.generations[0]?.receiver;
      if (!b) throw new Error("Required fixture needs an initial node key");
      const pool = [b, initial.reservedReceiver, ...(size === 3 ? [freshNode(h, "third-node")] : [])];
      let successes = 0;
      for (let i = 1; i < size; i++) {
        const node = pool[i]; if (!node) throw new Error("Missing finite-pool node");
        if (i > 1) h.reserveNextReceiver(node);
        const { request, receipt, mount } = await complete(h); successes++;
        expect(mount.self.nodeName).toBe(node.nodeName);
        expect(receipt.reservedAcquisitionId).toBe(request.reservedAcquisitionId);
        expect(mount.acquisitionId).toBe(request.reservedAcquisitionId);
      }
      expect(successes).toBe(size - 1);
      h.reserveNextReceiver({ ...b, podUID: `${b.podUID}:pool-exhausted` }); h.completeRetirement();
      const request = h.revokeRequest(), before = h.observe();
      // Every finite-pool key is either historical or the immediate predecessor.
      for (const node of pool) {
        await failure(() => h.ports.revokeGeneration(rehash({ ...request, reservedReceiver: { ...node,
          podUID: `${node.podUID}:retry` } })), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
        expect(h.observe()).toEqual(before);
      }
      expect(before.attachGrant).toBeNull(); expect(before.receiverScheduled).toBe(false);
    });
    test("failed candidate consumes its historical key without a fictitious generation (§4.1/§8.2/§9 #4)", async h => {
      h.completeRetirement(); const failed = h.revokeRequest();
      await h.ports.revokeGeneration(failed);
      h.failNextReceiverAttach();
      await failure(() => h.attachReservedReceiver(), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
      const held = h.observe();
      expect(held).toMatchObject({ candidateAttachSubmitted: true, receiverScheduled: true,
        receiverMounted: false, receiverReceipt: null, operation: { state: "committed" } });
      expect(held.attachGrant).not.toBeNull(); // The failed raw attempt was AFTER S6, not an impossible early attach.
      await failure(() => h.retireFailedCandidateAndReserve(freshNode(h, "third-node")), "B1_OPERATION_CONFLICT", "refused");
      expect(h.observe()).toEqual(held); // Unknown retirement cannot transfer or release the slot.
      h.completeRetirement(); h.retireFailedCandidateAndReserve(freshNode(h, "third-node"));
      const next = h.revokeRequest();
      expect(next.predecessor.podUIDs).toContain(failed.reservedReceiver.podUID);
      expect(next.predecessor.generations.some(g => g.receiver.podUID === failed.reservedReceiver.podUID)).toBe(false);
      expect(h.observe().retiredCandidatePodUIDs).toContain(failed.reservedReceiver.podUID);
      expect(h.observe().quarantinedNodeNames).toContain(failed.reservedReceiver.nodeName);
      await complete(h);
      h.reserveNextReceiver({ ...failed.reservedReceiver, podUID: `${failed.reservedReceiver.podUID}:new`,
        nodeUID: `${failed.reservedReceiver.nodeUID}:recreated` }); h.completeRetirement(); const before = h.observe();
      await failure(() => h.ports.revokeGeneration(h.revokeRequest()), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
      expect(h.observe()).toEqual(before);
    });

    test("N=2 failed attach consumes the last fresh key without a successful handover (§4.1/§9 #4)", async h => {
      h.completeRetirement(); const request = h.revokeRequest(), b = request.predecessor.generations[0]?.receiver;
      if (!b) throw new Error("Required fixture needs initial B");
      await h.ports.revokeGeneration(request); h.failNextReceiverAttach();
      await failure(() => h.attachReservedReceiver(), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
      h.completeRetirement(); h.retireFailedCandidateAndReserve({ ...b, podUID: `${b.podUID}:new` });
      h.completeRetirement(); const next = h.revokeRequest(), before = h.observe();
      for (const receiver of [b, request.reservedReceiver]) {
        await failure(() => h.ports.revokeGeneration(rehash({ ...next, reservedReceiver: receiver })),
          "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused"); expect(h.observe()).toEqual(before);
      }
      expect(before).toMatchObject({ receiverScheduled: false, receiverMounted: false, attachGrant: null });
    });
    test("earlier receipt readback never grants a later candidate after slot transfer (§8.1.3/§8.3)", async h => {
      const { request, receipt } = await complete(h);
      h.reserveNextReceiver(freshNode(h, "third-node")); const reserved = h.observe(); h.reopen();
      expect(await h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 })).toEqual(receipt);
      expect(h.observe()).toEqual(reserved);
      await failure(() => h.attachReservedReceiver(), "B1_TEARDOWN_INCOMPLETE", "held");
      expect(h.observe()).toEqual(reserved);
      const { mount } = await complete(h);
      expect(mount.acquisitionId).not.toBe(request.reservedAcquisitionId);
      expect(mount.self.nodeName).toBe("third-node");
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

    // §4.2 S8/S10, §5/§8.2: actual acquisition/attach lineage and each independent FD.
    const mountFaults: readonly { fault: B1MountFault; code: B1ErrorCode; disposition: "refused" | "held" }[] = [
      { fault: "stale-receiver-receipt", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "stale-acquisition", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "foreign-acquisition", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "attach-observation-mismatch", code: "B1_MOUNT_LINEAGE_UNKNOWN", disposition: "held" },
      { fault: "lock-wrong-namespace", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "generation-wrong-namespace", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "lock-wrong-filesystem", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "generation-wrong-filesystem", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
      { fault: "same-metadata-inode", code: "B1_METADATA_OFF_VOLUME", disposition: "refused" },
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

    for (const role of ["lockRelativePath", "generationRelativePath"] as const) {
      test(`mount target ${role} must match enrollment (§4.2 S8/§5/§8.2)`, async h => {
        await complete(h); const request = h.mountRequest(), before = h.observe();
        await failure(() => h.ports.currentMountGeneration({ ...request, target: { ...request.target, [role]: "elsewhere" } }),
          "B1_MOUNT_NOT_FOUND", "refused");
        expect(h.observe()).toEqual(before);
      });
    }
    test("mount custody and device reply bind independent record bytes and FD evidence (§6/§8.2)", async h => {
      const { request } = await complete(h);
      let previousHash: string | undefined, lockInode: string | undefined;
      for (let i = 0; i < 2; i++) {
        const before = h.observe(), mount = await h.ports.currentMountGeneration(h.mountRequest());
        expect(mount.custody.acquisitionId).toBe(request.reservedAcquisitionId);
        expect(mount.acquisitionId).toBe(request.reservedAcquisitionId);
        const independent: unknown = JSON.parse(before.custodyRecordPayloadBytes);
        expect(independent).toMatchObject({ storage: request.storage,
          acquisitionId: request.reservedAcquisitionId, value: before.custodyValue });
        expect(mount.custody.recordSha256).toBe(hash(before.custodyRecordPayloadBytes));
        expect(h.readEvidence(mount.observation)).toMatchObject({ data: {
          actualMount: mount.actualMount, acquisitionId: mount.acquisitionId,
          lockFd: { role: "lock", inode: mount.custody.lockInode, deviceLifetimeId: mount.actualMount.deviceLifetimeId },
          generationFd: { role: "generation", deviceLifetimeId: mount.actualMount.deviceLifetimeId },
        } });
        expect(h.readEvidence(mount.attachObservation)).toMatchObject({ data: { actual: {
          deviceLifetimeId: mount.actualMount.deviceLifetimeId,
        } } });
        if (i) { expect(mount.custody.recordSha256).not.toBe(previousHash); expect(mount.custody.lockInode).toBe(lockInode); }
        previousHash = mount.custody.recordSha256; lockInode = mount.custody.lockInode;
        expect(h.observe()).toEqual(before);
        if (!i) h.incrementCustody(); // Fixture M2 mutation, never performed by the port.
      }
    });
    test("reused nonce cannot authenticate a changed observation (§8.1.8/§8.2)", async h => {
      await complete(h); const request = h.mountRequest();
      const first = await h.ports.currentMountGeneration(request), originalEvidence = h.readEvidence(first.observation);
      h.incrementCustody(); const changed = h.observe();
      await failure(() => h.ports.currentMountGeneration(request), "B1_INVALID_ARGUMENT", "refused");
      expect(h.observe()).toEqual(changed); expect(h.readEvidence(first.observation)).toEqual(originalEvidence);
      const fresh = await h.ports.currentMountGeneration(h.mountRequest());
      expect(fresh.custody.value).toBe(changed.custodyValue);
      expect(fresh.observation.sha256).not.toBe(first.observation.sha256); expect(h.observe()).toEqual(changed);
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

    // V3: quantify the fence over handovers, not just the initial fixture. Each
    // negative runs on an isolated current handover after a real successful prefix.
    // Prefix replies are consumer-held oracles, not reconstructed from observe().
    const finishHandover = async (h: B1Harness) => {
      const done = await complete(h);
      for (const ref of done.receipt.teardown) expect(h.readEvidence(ref)).toMatchObject({ data: {
        cut: done.receipt.record.resourceVersion, retiredGenerations: done.receipt.revoked,
        exitedPodUIDs: done.receipt.exitedPodUIDs, storage: done.request.storage,
        operationId: done.request.operationId, acquisitionId: done.request.reservedAcquisitionId,
      } });
      expect(done.receipt).toMatchObject({ storage: done.request.storage, reservedReceiver: done.request.reservedReceiver,
        reservedAcquisitionId: done.request.reservedAcquisitionId, revoked: done.request.predecessor.generations,
        exitedPodUIDs: done.request.predecessor.podUIDs });
      h.incrementCustody(); // Fixture M2: distinct n, so stale predecessor n is observable.
      const mountRequest = h.mountRequest(), before = h.observe();
      const mount = await h.ports.currentMountGeneration(mountRequest);
      expect(h.observe()).toEqual(before);
      const receiver = before.receiverReceipt;
      if (!receiver) throw new Error("S7 must retain its own receiver receipt");
      expect(receiver).toMatchObject({ receiver: mount.self, acquisitionId: mount.acquisitionId,
        engine: mount.engine, bootstrapExecutionId: mount.actualMount.bootstrapExecutionId,
        attachObservation: mount.attachObservation, revokeReceipt: done.receipt.record });
      expect(mount.engine.nodeUID).toBe(mount.self.nodeUID);
      expect(mount).toMatchObject({ storage: done.request.storage, self: done.request.reservedReceiver,
        acquisitionId: done.request.reservedAcquisitionId, custody: { acquisitionId: done.request.reservedAcquisitionId,
          value: before.custodyValue, recordSha256: hash(before.custodyRecordPayloadBytes) } });
      expect(h.readEvidence(mount.observation)).toMatchObject({ data: { challengeNonce: mountRequest.challengeNonce,
        storage: mount.storage, self: mount.self, acquisitionId: mount.acquisitionId, actualMount: mount.actualMount,
        custody: mount.custody, attachObservation: mount.attachObservation, nativeEpoch: { kind: "absent" },
        lockFd: { role: "lock", inode: mount.custody.lockInode, deviceLifetimeId: mount.actualMount.deviceLifetimeId },
        generationFd: { role: "generation", deviceLifetimeId: mount.actualMount.deviceLifetimeId } } });
      expect(h.readEvidence(mount.attachObservation)).toMatchObject({ data: { storage: mount.storage,
        receiver: mount.self, acquisitionId: mount.acquisitionId, engine: mount.engine, actual: mount.actualMount,
        revokeReceiptHash: done.receipt.record.sha256, cut: done.receipt.record.resourceVersion } });
      for (const ref of [mount.observation, mount.attachObservation]) expect(hash(encode(h.readEvidence(ref)))).toBe(ref.sha256);
      return { ...done, mount, mountRequest, receiver };
    };
    const atHandover = async (h: B1Harness, number: number) => {
      const history: Awaited<ReturnType<typeof finishHandover>>[] = [];
      for (let i = 1; i < number; i++) {
        history.push(await finishHandover(h));
        h.reserveNextReceiver(freshNode(h, `handover-${i + 1}`));
      }
      return history;
    };
    const noGrantAtPorts = async (h: B1Harness): Promise<void> => {
      withheld(h); const held = h.observe();
      await failure(() => h.attachReservedReceiver(), "B1_TEARDOWN_INCOMPLETE", "held");
      await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_MOUNT_NOT_FOUND", "refused");
      expect(h.observe()).toEqual(held);
    };
    const noEffects = (h: B1Harness): void => {
      expect(h.observe().operation).toMatchObject({ restartRevocationPersisted: false,
        killSubmitted: false, retirementRequested: false });
    };

    for (const handover of [1, 2, 3]) {
      const prefix = `handover ${handover}/3`;
      test(`${prefix}: no observations borrowed from an earlier handover (§4.2 S1–S6/§9 #1–2)`, async h => {
        await atHandover(h, handover); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
        noEffects(h); await noGrantAtPorts(h);
        await finishHandover(h); // Positive control: this very operation can later finish.
      });
      test(`${prefix}: fresh facts cannot borrow earlier T1–T8 (§4.3/§9 #1)`, async h => {
        await atHandover(h, handover);
        for (const { fact } of factCases) h.setFact(fact, true);
        await failure(() => h.ports.revokeGeneration(h.revokeRequest()), "B1_TEARDOWN_INCOMPLETE", "held");
        await noGrantAtPorts(h); await finishHandover(h);
      });
      for (const { fact, code } of factCases) {
        test(`${prefix}: independent ${fact} gates this handover (§4.2 S3–S6/§4.3)`, async h => {
          await atHandover(h, handover); h.completeRetirement(); h.setFact(fact, false);
          const request = h.revokeRequest();
          await failure(() => h.ports.revokeGeneration(request), code, "held");
          if (fact === "inventoryComplete" || fact === "nodeReachable") noEffects(h);
          if (fact === "restartDisabled" || fact === "killIssued") {
            expect(h.observe().operation?.retirementRequested).toBe(false);
          }
          await noGrantAtPorts(h); const held = h.observe(); h.reopen();
          await failure(() => h.ports.revokeGeneration({ ...request, deadlineUnixMs: request.deadlineUnixMs + 1_000 }), code, "held");
          expect(h.observe()).toEqual(held);
          h.setFact(fact, true); await finishHandover(h);
        });
        test(`${prefix}: post-effect ${fact} regression rechecked before commit (§4.2 S5→S6/§8.1.5)`, async h => {
          await atHandover(h, handover); h.completeRetirement(); h.setTeardown("T8", false);
          const request = h.revokeRequest();
          await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
          expect(h.observe().operation).toMatchObject({ restartRevocationPersisted: true,
            killSubmitted: true, retirementRequested: true });
          h.setTeardown("T8", true); h.setFact(fact, false); const regressed = h.observe();
          await failure(() => h.ports.revokeGeneration(request), code, "held");
          expect(h.observe()).toEqual(regressed); await noGrantAtPorts(h);
          h.reopen();
          await failure(() => h.ports.revokeGeneration(request), code, "held");
          expect(h.observe()).toEqual(regressed);
          h.setFact(fact, true); await finishHandover(h);
        });
      }
      for (const item of teardownItems) {
        test(`${prefix}: ${item} is fresh and conjunctive (§4.3/§9 #1)`, async h => {
          await atHandover(h, handover); h.completeRetirement(); h.setTeardown(item, false);
          const request = h.revokeRequest();
          await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
          await noGrantAtPorts(h); const held = h.observe();
          await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
          expect(h.observe()).toEqual(held);
          h.setTeardown(item, true); const { receipt } = await finishHandover(h);
          expect(receipt.teardown).toHaveLength(8);
          for (const ref of receipt.teardown) expect(h.readEvidence(ref)).toMatchObject({ data: {
            storage: request.storage, operationId: request.operationId, acquisitionId: request.reservedAcquisitionId,
          } });
        });
      }
      for (const item of teardownItems.filter(item => item !== "T8")) {
        test(`${prefix}: post-effect ${item} regression invalidates the cut (§4.2 S5→S6/§4.3)`, async h => {
          await atHandover(h, handover); h.completeRetirement(); h.setTeardown("T8", false);
          const request = h.revokeRequest();
          await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
          h.setTeardown("T8", true); h.setTeardown(item, false); const changed = h.observe();
          await failure(() => h.ports.revokeGeneration(request), "B1_TEARDOWN_INCOMPLETE", "held");
          expect(h.observe()).toEqual(changed); await noGrantAtPorts(h);
          h.setTeardown(item, true); await finishHandover(h);
        });
      }
      test(`${prefix}: deadline/inventory/node/latch/kill checks precede effects (§4.2 S3→S4/§9 #2)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 }), "B1_DEADLINE_HELD", "held");
        noEffects(h); await noGrantAtPorts(h);
        h.setFact("inventoryComplete", false);
        await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
        noEffects(h); await noGrantAtPorts(h);
        h.setFact("inventoryComplete", true); h.setFact("nodeReachable", false);
        await failure(() => h.ports.revokeGeneration(request), "B1_NODE_UNREACHABLE", "held");
        noEffects(h); await noGrantAtPorts(h);
        h.setFact("nodeReachable", true); h.setFact("restartDisabled", false);
        await failure(() => h.ports.revokeGeneration(request), "B1_RESTART_NOT_DISABLED", "held");
        expect(h.observe().operation).toMatchObject({ restartRevocationPersisted: true, killSubmitted: false, retirementRequested: false });
        h.setFact("restartDisabled", true); h.setFact("killIssued", false);
        await failure(() => h.ports.revokeGeneration(request), "B1_KILL_NOT_CONFIRMED", "held");
        expect(h.observe().operation).toMatchObject({ killSubmitted: true, retirementRequested: false });
        h.setFact("killIssued", true); await finishHandover(h);
      });

      test(`${prefix}: changed historical replay and stale S1/a refuse at the port (§8.1.1–3)`, async h => {
        const history = await atHandover(h, handover); h.completeRetirement();
        const request = h.revokeRequest(), before = h.observe();
        for (const old of history) {
          for (const { change } of changedMembers) {
            await failure(() => h.ports.revokeGeneration(rehash(change(old.request))), "B1_OPERATION_CONFLICT", "refused");
            expect(h.observe()).toEqual(before);
          }
          await failure(() => h.ports.revokeGeneration(rehash({ ...request,
            expectedControllerRevision: old.request.expectedControllerRevision })), "B1_STALE_GENERATION", "refused");
          expect(h.observe()).toEqual(before);
          await failure(() => h.ports.revokeGeneration(rehash({ ...request,
            reservedAcquisitionId: old.mount.acquisitionId })), "B1_NOT_AUTHORIZED", "refused");
          expect(h.observe()).toEqual(before);
          expect(await h.ports.revokeGeneration({ ...old.request, deadlineUnixMs: 0 })).toEqual(old.receipt);
          expect(h.observe()).toEqual(before); // Historical readback cannot re-publish its grant.
        }
        await finishHandover(h);
      });
      for (const { fault, code, disposition } of mountFaults) {
        test(`${prefix}: actual mount ${fault} fails closed (§4.2 S8/S10/§5/§8.2/§9 #8)`, async h => {
          await atHandover(h, handover); await finishHandover(h);
          h.setMountFault(fault); const before = h.observe();
          await failure(() => h.ports.currentMountGeneration(h.mountRequest()), code, disposition);
          expect(h.observe()).toEqual(before);
        });
      }
      test(`${prefix}: discovered task holds the immutable slot until positive-drain transfer (§8.1.5/§9 #2)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); h.setFact("workloadsExited", false);
        const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
        h.discoverWorkloadTask(); h.completeRetirement(); const changed = h.observe();
        await failure(() => h.ports.revokeGeneration(request), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
        expect(h.observe()).toEqual(changed); await noGrantAtPorts(h);
        await failure(() => h.ports.revokeGeneration(rehash({ ...request, operationId: `${request.operationId}:replacement` })),
          "B1_OPERATION_CONFLICT", "refused");
        expect(h.observe()).toEqual(changed);
        h.retireFailedCandidateAndReserve(freshNode(h, "drained-new-task")); const transferred = h.observe(); h.reopen();
        await failure(() => h.ports.revokeGeneration(request), "B1_STALE_GENERATION", "refused");
        expect(h.observe()).toEqual(transferred); await finishHandover(h);
      });
      test(`${prefix}: mounted receiver cannot escape inventory as a failed candidate (§8.1.5/§9 #5)`, async h => {
        await atHandover(h, handover); const { mount } = await finishHandover(h), before = h.observe();
        await failure(() => h.retireFailedCandidateAndReserve(freshNode(h, "unsafe-slot-release")), "B1_OPERATION_CONFLICT", "refused");
        expect(h.observe()).toEqual(before);
        expect((await h.ports.currentMountGeneration(h.mountRequest())).acquisitionId).toBe(mount.acquisitionId);
        h.reserveNextReceiver(freshNode(h, "safe-next"));
        const { receipt } = await finishHandover(h);
        expect(receipt.revoked).toHaveLength(1);
        expect(receipt.revoked[0]).toMatchObject({ acquisitionId: mount.acquisitionId, receiver: mount.self,
          engine: mount.engine, applicationGeneration: mount.custody.value });
      });
      test(`${prefix}: reopened client still refuses a historical node through revoke (§4.1/§9 #4)`, async h => {
        const initial = h.revokeRequest().predecessor.generations[0]?.receiver;
        if (!initial) throw new Error("Required fixture needs an initial predecessor");
        const history = await atHandover(h, handover), current = await finishHandover(h);
        h.reserveNextReceiver({ ...initial, nodeUID: `${initial.nodeUID}:recreated`, podUID: `${initial.podUID}:again` });
        h.completeRetirement(); h.reopen(); const before = h.observe(), request = h.revokeRequest();
        for (const node of [initial, ...history.map(done => done.mount.self), current.mount.self]) {
          await failure(() => h.ports.revokeGeneration(rehash({ ...request, reservedReceiver: { ...node,
            nodeUID: `${node.nodeUID}:recreated`, podUID: `${node.podUID}:again` } })), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
          expect(h.observe()).toEqual(before);
        }
      });
      for (const action of ["detach", "reattach-old", "old-write"] as const) {
        test(`${prefix}: reopen preserves ${action} containment and closes slot transfers (§4.2 S11/§8.1.5)`, async h => {
          await atHandover(h, handover); const { request, receipt } = await finishHandover(h);
          h.executeRawStaleAction(action); h.reopen(); const before = h.observe();
          await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_MOUNT_CHANGED", "refused");
          expect(h.observe()).toEqual(before);
          await failure(() => h.reserveNextReceiver(freshNode(h, "after-containment")), "B1_OPERATION_CONFLICT", "refused");
          expect(h.observe()).toEqual(before);
          await failure(() => h.retireFailedCandidateAndReserve(freshNode(h, "after-containment")), "B1_OPERATION_CONFLICT", "refused");
          expect(h.observe()).toEqual(before);
          await failure(() => h.attachReservedReceiver(), "B1_STALE_GENERATION", "refused");
          await failure(() => h.ports.revokeGeneration(rehash({ ...request, operationId: `${request.operationId}:new` })),
            "B1_STALE_GENERATION", "refused");
          expect(await h.ports.revokeGeneration(request)).toEqual(receipt);
          expect(h.observe()).toEqual(before);
        });
      }
      test(`${prefix}: retired failed candidate stays in the next exclusion receipt (§8.2/§9 #4–5)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); const failed = h.revokeRequest();
        await h.ports.revokeGeneration(failed); h.failNextReceiverAttach();
        await failure(() => h.attachReservedReceiver(), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
        h.completeRetirement(); h.retireFailedCandidateAndReserve(freshNode(h, "after-failed-attach"));
        const { request, receipt } = await finishHandover(h);
        expect(receipt.exitedPodUIDs).toContain(failed.reservedReceiver.podUID);
        expect(receipt.exitedPodUIDs).toEqual(request.predecessor.podUIDs);
        expect(receipt.revoked.some(g => g.receiver.podUID === failed.reservedReceiver.podUID)).toBe(false);
        h.reserveNextReceiver({ ...failed.reservedReceiver, podUID: `${failed.reservedReceiver.podUID}:again` });
        h.completeRetirement(); h.reopen(); const before = h.observe();
        await failure(() => h.ports.revokeGeneration(h.revokeRequest()), "B1_SAME_NODE_REUSE_UNQUALIFIED", "refused");
        expect(h.observe()).toEqual(before);
      });
      test(`${prefix}: unsubmitted gated candidate consumes no raw key (§4.1/§8.2)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); const gated = h.revokeRequest();
        await h.ports.revokeGeneration(gated); // Deliberately no S7 submission.
        h.retireFailedCandidateAndReserve(freshNode(h, "after-gated"));
        const next = await finishHandover(h);
        expect(next.receipt.exitedPodUIDs).toContain(gated.reservedReceiver.podUID);
        h.reserveNextReceiver({ ...gated.reservedReceiver, podUID: `${gated.reservedReceiver.podUID}:fresh` });
        h.reopen(); const reused = await finishHandover(h);
        expect(reused.mount.self.nodeName).toBe(gated.reservedReceiver.nodeName);
      });
      test(`${prefix}: policy and first-invocation identity checks have no effects (§8.1.2/6)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); const original = h.revokeRequest(), before = h.observe();
        for (const [request, code] of [
          [rehash({ ...original, requiredProfile: "r3-storage-fence" }), "B1_POLICY_UNSATISFIED"],
          [rehash({ ...original, storage: { ...original.storage, pvcUID: "foreign-storage" } }), "B1_STORAGE_IDENTITY_MISMATCH"],
          [rehash({ ...original, reservedReceiver: { ...original.reservedReceiver, podUID: "foreign-self" } }), "B1_NOT_AUTHORIZED"],
          [rehash({ ...original, reservedAcquisitionId: "foreign-acquisition" }), "B1_NOT_AUTHORIZED"],
          [rehash({ ...original, expectedControllerRevision: "0" }), "B1_STALE_GENERATION"],
          [{ ...original, requestSha256: "0".repeat(64) }, "B1_INVALID_ARGUMENT"],
        ] satisfies readonly (readonly [RevokeGenerationRequest, B1ErrorCode])[]) {
          await failure(() => h.ports.revokeGeneration(request), code, "refused"); expect(h.observe()).toEqual(before);
        }
        const unknown = structuredClone(original);
        Object.defineProperty(unknown, "requiredProfile", { value: "unknown", enumerable: true });
        await failure(() => h.ports.revokeGeneration(rehash(unknown)), "B1_UNSUPPORTED_PROFILE", "refused");
        expect(h.observe()).toEqual(before);
        await finishHandover(h); const attached = h.observe();
        await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), requiredProfile: "r3-storage-fence" }),
          "B1_POLICY_UNSATISFIED", "refused");
        expect(h.observe()).toEqual(attached);
      });
      test(`${prefix}: held/completed semantic conflicts and exact replays retain bytes (§8.1.1–4/§9 #3)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); h.setFact("workloadsExited", false);
        const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
        const held = h.observe();
        for (const { change } of changedMembers) {
          await failure(() => h.ports.revokeGeneration(rehash(change(request))), "B1_OPERATION_CONFLICT", "refused");
          expect(h.observe()).toEqual(held);
        }
        h.reopen();
        await failure(() => h.ports.revokeGeneration(request), "B1_TASKS_STILL_ALIVE", "held");
        expect(h.observe()).toEqual(held);
        const { receipt } = await finishHandover(h), committed = h.observe();
        const { record, ...payload } = receipt;
        expect(hash(encode(payload))).toBe(record.sha256);
        expect(committed.retainedReceipt?.payloadBytes).toBe(encode(payload));
        for (const { change } of changedMembers) {
          await failure(() => h.ports.revokeGeneration(rehash(change(request))), "B1_OPERATION_CONFLICT", "refused");
          expect(h.observe()).toEqual(committed);
        }
        const pristine = structuredClone(receipt);
        Object.defineProperty(receipt.storage, "sessionId", { value: "caller-mutated" });
        expect(await h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 })).toEqual(pristine);
        expect(h.observe()).toEqual(committed);
      });
      test(`${prefix}: lost commit ACK and reopen preserve exactly one grant (§8.1.3/§9 #3)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); h.loseNextRevokeAck(); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(request), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
        const committed = h.observe();
        expect(committed).toMatchObject({ operation: { state: "committed" }, receiverScheduled: false, receiverMounted: false });
        expect(committed.attachGrant).not.toBeNull(); expect(committed.retainedReceipt).not.toBeNull();
        const oldPorts = h.ports; h.reopen();
        await failure(() => oldPorts.revokeGeneration(request), "B1_OBSERVER_UNAVAILABLE", "held");
        const receipt = await h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 });
        expect(receipt.record).toEqual(committed.retainedReceipt?.record); expect(h.observe()).toEqual(committed);
        h.attachReservedReceiver();
        expect((await h.ports.currentMountGeneration(h.mountRequest())).acquisitionId).toBe(request.reservedAcquisitionId);
      });
      test(`${prefix}: post-S6 facts are rechecked at attach and mount (§4.2 S6–S11/§4.3)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); await h.ports.revokeGeneration(h.revokeRequest());
        for (const { fact } of factCases) {
          h.setFact(fact, false); const before = h.observe();
          await failure(() => h.attachReservedReceiver(), "B1_UNEXPECTED_ATTACHMENT", "held");
          expect(h.observe()).toEqual(before); h.setFact(fact, true);
        }
        for (const item of teardownItems) {
          h.setTeardown(item, false); const before = h.observe();
          await failure(() => h.attachReservedReceiver(), "B1_UNEXPECTED_ATTACHMENT", "held");
          expect(h.observe()).toEqual(before); h.setTeardown(item, true);
        }
        h.setFact("newReplicasAfterCut", false); const preCut = h.observe();
        await failure(() => h.attachReservedReceiver(), "B1_MOUNT_LINEAGE_UNKNOWN", "held");
        expect(h.observe()).toEqual(preCut); h.setFact("newReplicasAfterCut", true);
        h.attachReservedReceiver();
        for (const { fact } of factCases) {
          h.setFact(fact, false); const before = h.observe();
          await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_UNEXPECTED_ATTACHMENT", "held");
          expect(h.observe()).toEqual(before); h.setFact(fact, true);
        }
        for (const item of teardownItems) {
          h.setTeardown(item, false); const before = h.observe();
          await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_UNEXPECTED_ATTACHMENT", "held");
          expect(h.observe()).toEqual(before); h.setTeardown(item, true);
        }
        expect((await h.ports.currentMountGeneration(h.mountRequest())).self).toEqual(h.revokeRequest().reservedReceiver);
      });
      test(`${prefix}: malformed calls fail closed on both ports (§8.1.6–8)`, async h => {
        await atHandover(h, handover); h.completeRetirement(); const original = h.revokeRequest(), before = h.observe();
        for (const value of [0, 2, "1"]) {
          const version = structuredClone(original);
          Object.defineProperty(version, "version", { value, enumerable: true });
          await failure(() => h.ports.revokeGeneration(rehash(version)), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(before);
        }
        for (const operationId of ["", " \t"]) {
          await failure(() => h.ports.revokeGeneration(rehash({ ...original, operationId })), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(before);
        }
        const deadlines = [NaN, -1, 0.5, Infinity, Number.MAX_SAFE_INTEGER + 1];
        for (const deadlineUnixMs of deadlines) {
          await failure(() => h.ports.revokeGeneration({ ...original, deadlineUnixMs }), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(before);
        }
        await finishHandover(h); const mounted = h.observe();
        for (const value of [0, 2, "1"]) {
          const version = h.mountRequest(); Object.defineProperty(version, "version", { value, enumerable: true });
          await failure(() => h.ports.currentMountGeneration(version), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(mounted);
        }
        for (const deadlineUnixMs of deadlines) {
          await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), deadlineUnixMs }), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(mounted);
        }
        for (const challengeNonce of ["", " \t"]) {
          await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), challengeNonce }), "B1_INVALID_ARGUMENT", "refused");
          expect(h.observe()).toEqual(mounted);
        }
        const unknown = h.mountRequest();
        Object.defineProperty(unknown, "requiredProfile", { value: "unknown", enumerable: true });
        await failure(() => h.ports.currentMountGeneration(unknown), "B1_UNSUPPORTED_PROFILE", "refused");
        await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), deadlineUnixMs: 0 }), "B1_DEADLINE_HELD", "held");
        for (const key of ["protectedRoot", "lockRelativePath", "generationRelativePath"] as const) {
          const request = h.mountRequest();
          await failure(() => h.ports.currentMountGeneration({ ...request, target: { ...request.target, [key]: "foreign" } }),
            "B1_MOUNT_NOT_FOUND", "refused"); expect(h.observe()).toEqual(mounted);
        }
        await failure(() => h.ports.currentMountGeneration({ ...h.mountRequest(), target: {
          ...h.mountRequest().target, bootstrapHandle: "foreign" } }), "B1_NOT_AUTHORIZED", "refused");
        expect(h.observe()).toEqual(mounted);
        const value = mounted.custodyValue;
        for (const invalid of ["", "00", "+1", "-1", "1.0", "1e3", " 1", "1\n"]) {
          h.setCustodyValue(invalid); const corrupt = h.observe();
          await failure(() => h.ports.currentMountGeneration(h.mountRequest()), "B1_GENERATION_INVALID", "refused");
          expect(h.observe()).toEqual(corrupt);
        }
        h.setCustodyValue(value);
        expect((await h.ports.currentMountGeneration(h.mountRequest())).custody.value).toBe(value);
        expect(h.observe()).toEqual(mounted);
      });
    }

    test("three fresh handovers bind a/u/bootstrap/device/evidence and revoke the real predecessor (§4.1/§4.2 S7/§8.2)", async h => {
      const history: Awaited<ReturnType<typeof finishHandover>>[] = [];
      for (let i = 1; i <= 3; i++) {
        if (i > 1) h.reserveNextReceiver(freshNode(h, `fresh-lineage-${i}`));
        const request = h.revokeRequest(), prior = history.at(-1);
        if (prior) {
          expect(request.predecessor.since).toEqual({ kind: "receipt", record: prior.receiver.record });
          expect(request.predecessor.generations).toHaveLength(1);
          expect(request.predecessor.generations[0]).toMatchObject({ acquisitionId: prior.mount.acquisitionId,
            receiver: prior.mount.self, engine: prior.mount.engine, applicationGeneration: prior.mount.custody.value });
        }
        for (const ref of [request.predecessor.restartDisabled, request.predecessor.killIssued]) {
          expect(h.readEvidence(ref)).toMatchObject({ data: { storage: request.storage, operationId: request.operationId } });
        }
        expect(h.readEvidence(request.predecessor.enumeration)).toMatchObject({ data: { storage: request.storage,
          operationId: request.operationId, acquisitionId: request.reservedAcquisitionId,
          generations: request.predecessor.generations, podUIDs: request.predecessor.podUIDs,
          since: request.predecessor.since.record } });
        const done = await finishHandover(h), after = h.observe();
        if (prior) {
          expect(done.receipt.revoked).toHaveLength(1);
          expect(done.receipt.revoked[0]).toMatchObject({ acquisitionId: prior.mount.acquisitionId,
            receiver: prior.mount.self, engine: prior.mount.engine, applicationGeneration: prior.mount.custody.value });
        }
        for (const old of history) {
          expect(done.mount.acquisitionId).not.toBe(old.mount.acquisitionId);
          expect(done.mount.engine).not.toEqual(old.mount.engine);
          expect(done.mount.engine.processUUID).not.toBe(old.mount.engine.processUUID);
          expect(done.mountRequest.target.bootstrapHandle).not.toBe(old.mountRequest.target.bootstrapHandle);
          expect(done.mount.actualMount.bootstrapExecutionId).not.toBe(old.mount.actualMount.bootstrapExecutionId);
          expect(done.mount.actualMount.deviceLifetimeId).not.toBe(old.mount.actualMount.deviceLifetimeId);
          for (const key of ["enumeration", "restartDisabled", "killIssued"] as const) {
            expect(request.predecessor[key]).not.toEqual(old.request.predecessor[key]);
          }
          await failure(() => h.ports.currentMountGeneration({ ...old.mountRequest,
            challengeNonce: h.mountRequest().challengeNonce }), "B1_NOT_AUTHORIZED", "refused");
          expect(h.observe()).toEqual(after);
        }
        history.push(done);
      }
    });

    // An old but genuine S1/S2/S3 record is not current coverage. Once that bad
    // request is retained, repair requires §8.1.5's positive-drain slot transfer.
    for (const member of ["since", "enumeration", "restartDisabled", "killIssued"] as const) {
      test(`three later handovers reject stale ${member} evidence at revoke (§4.2 S1–S3/§8.1.5)`, async h => {
        let prior = await finishHandover(h);
        for (let i = 2; i <= 4; i++) {
          h.reserveNextReceiver(freshNode(h, `stale-evidence-${i}`)); h.completeRetirement();
          const request = h.revokeRequest();
          const stale = rehash({ ...request, predecessor: { ...request.predecessor, [member]: prior.request.predecessor[member] } });
          await failure(() => h.ports.revokeGeneration(stale), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
          noEffects(h); await noGrantAtPorts(h); const held = h.observe();
          await failure(() => h.ports.revokeGeneration(request), "B1_OPERATION_CONFLICT", "refused");
          expect(h.observe()).toEqual(held);
          h.retireFailedCandidateAndReserve(freshNode(h, `drained-evidence-${i}`));
          prior = await finishHandover(h);
        }
      });
    }

    // V4: generated field table, crossed with every handover and entry path.
    // The registration snapshot supplies SHAPE only. All oracle values below come
    // from the consumer's previous replies/current request and public controls.
    const snapshots = (h: B1Harness, mountRequest: CurrentMountGenerationRequest) => ({
      request: h.revokeRequest(), mountRequest, inputs: h.fenceInputs(),
    });
    type EntryPath = "normal" | "failed-transfer" | "reopen";
    const prepareFields = async (h: B1Harness, number: number, entry: EntryPath) => {
      let previous: ReturnType<typeof snapshots> | undefined;
      for (let i = 1; i < number; i++) {
        const done = await finishHandover(h); previous = snapshots(h, done.mountRequest);
        h.reserveNextReceiver(freshNode(h, `generated-${i + 1}`));
      }
      if (!previous) {
        // The initial fixture's real predecessor has no modeled revoke invocation.
        // The genuine, already-closed registration fixture supplies H0 values;
        // it is NOT counted as a same-volume earlier successful handover.
        previous = structuredClone(fieldShapes);
      }
      if (entry === "failed-transfer") {
        h.completeRetirement(); const failedRequest = h.revokeRequest();
        const failedReceipt = await h.ports.revokeGeneration(failedRequest);
        const beforeAttach = h.observe();
        await failure(() => h.reserveNextReceiver(freshNode(h, "never-attached-normal-transfer")), "B1_OPERATION_CONFLICT", "refused");
        expect(h.observe()).toEqual(beforeAttach);
        h.failNextReceiverAttach();
        await failure(() => h.attachReservedReceiver(), "B1_DRIVER_OUTCOME_UNKNOWN", "held");
        const beforeDrain = h.observe();
        await failure(() => h.retireFailedCandidateAndReserve(freshNode(h, "undrained-transfer")), "B1_OPERATION_CONFLICT", "refused");
        expect(h.observe()).toEqual(beforeDrain);
        h.completeRetirement();
        const failedInputs = h.fenceInputs();
        h.retireFailedCandidateAndReserve(freshNode(h, "generated-after-failure"));
        const transferred = h.observe();
        expect(h.revokeRequest().predecessor.since).toEqual({ kind: "receipt", record: failedReceipt.record });
        expect(await h.ports.revokeGeneration({ ...failedRequest, deadlineUnixMs: 0 })).toEqual(failedReceipt);
        expect(h.observe()).toEqual(transferred);
        previous = { request: failedRequest, mountRequest: previous.mountRequest,
          inputs: { ...failedInputs, mount: previous.inputs.mount, receiverReceipt: previous.inputs.receiverReceipt } };
      }
      if (entry === "reopen") h.reopen(); // Before the FIRST invocation of this operation.
      return previous;
    };
    const refusal = async (h: B1Harness, call: () => Promise<unknown>, mount = false) => {
      const request = h.revokeRequest(), before = h.observe();
      let error: unknown;
      try { await call(); } catch (caught) { error = caught; }
      expect(error).toBeInstanceOf(Error);
      expect(error).toMatchObject({ code: expect.stringMatching(/^B1_/),
        disposition: expect.stringMatching(/^(refused|held)$/), evidence: [request.predecessor.enumeration] });
      if (h.observe().operation) expect(error).toMatchObject({ operationId: request.operationId,
        durableState: { resourceVersion: h.observe().controllerRevision } });
      if (mount) expect(h.observe()).toEqual(before);
      else expect(h.observe()).toMatchObject({ attachGrant: null, retainedReceipt: null,
        receiverScheduled: false, receiverMounted: false });
    };
    const domains = Object.keys(fieldShapes) as (keyof typeof fieldShapes)[];
    for (const number of [1, 2, 3]) for (const entry of ["normal", "failed-transfer", "reopen"] as const) {
      const prefix = `generated N=${number} path=${entry}`;
      for (const domain of domains) for (const path of fenceFieldPaths(fieldShapes[domain])) {
        const label = `${domain}.${path.join(".")}`;
        test(`${prefix} field=${label}: previous evidence or unequal-value negative`, async h => {
          const firstPrevious = await prepareFields(h, number, entry);
          const isMount = domain === "mountRequest" || (domain === "inputs" && ["mount", "custodyValue", "receiverReceipt"].includes(path[0] ?? ""));
          if (isMount) await finishHandover(h); else h.completeRetirement();
          const firstCurrent = snapshots(h, h.mountRequest());
          const fields = concreteFields(firstCurrent[domain], path);
          // A changed semantic request/ref needs a fresh operation per element:
          // its retained conflict must not masquerade as a field-level refusal.
          // Read-only mount faults and source observations outside the semantic
          // request are restored and can reuse the same held operation safely.
          const needsFresh = domain === "request" || (domain === "inputs" && ["inventory", "restart", "kill"].includes(path[0] ?? ""));
          for (let index = 0; index < fields.length; index++) {
            const fresh = index !== 0 && needsFresh;
            const live = fresh ? await factory.create() : h;
            try {
              const previous = fresh ? await prepareFields(live, number, entry) : firstPrevious;
              if (fresh) { if (isMount) await finishHandover(live); else live.completeRetirement(); }
              const current = fresh ? snapshots(live, live.mountRequest()) : firstCurrent;
              const concrete = concreteFields(current[domain], path)[index];
              if (!concrete) throw new Error("Generated collection shape changed unexpectedly");
              const value = fenceFieldValue(current[domain], concrete);
              const oldValue = fenceFieldValue(previous[domain], concrete);
              const equal = oldValue !== undefined && encode(value) === encode(oldValue);
              let replacement = oldValue !== undefined && !equal ? oldValue : wrongField(value);
              // An older, still-live deadline is permitted, and a never-issued
              // foreign nonce is fresh. Neither is stale authority by itself.
              if (concrete.at(-1) === "deadlineUnixMs") replacement = 0;
              // Counter rollback/high-water is an M2 concern. B1 must report a
              // canonical actual value, not reject an otherwise canonical old n.
              if (domain === "inputs" && concrete[0] === "custodyValue") replacement = "";
              if (domain === "mountRequest" && concrete[0] === "challengeNonce" && number === 1) replacement = "";
              if (equal) expect(oldValue).toEqual(value); // Explicit equal-value equivalence.
              if (domain === "request") {
                let changed = replaceField(current.request, concrete, replacement);
                if (concrete[0] !== "requestSha256") changed = rehash(changed);
                await refusal(live, () => live.ports.revokeGeneration(changed));
              } else if (domain === "mountRequest") {
                const changed = replaceField(current.mountRequest, concrete, replacement);
                await refusal(live, () => live.ports.currentMountGeneration(changed), true);
              } else {
                live.injectFenceField(concrete, replacement);
                expect(fenceFieldValue(live.fenceInputs(), concrete)).toEqual(replacement);
                await refusal(live, () => isMount ? live.ports.currentMountGeneration(live.mountRequest())
                  : live.ports.revokeGeneration(live.revokeRequest()), isMount);
                if (!needsFresh) live.injectFenceField(concrete, value);
              }
            } finally { if (live !== h) live.close(); }
          }
        });
      }
      for (const fact of Object.keys(schemaHarness.observe().facts) as B1Fact[]) {
        test(`${prefix} fresh-fact=${fact}: withhold only this observation, never completeRetirement first`, async h => {
          await prepareFields(h, number, entry);
          for (const key of Object.keys(h.observe().facts) as B1Fact[]) if (key !== fact) h.setFact(key, true);
          for (const item of Object.keys(h.observe().teardown) as B1TeardownItem[]) h.setTeardown(item, true);
          const resetFalse = ["inventoryComplete", "restartDisabled", "killIssued", "workloadsExited",
            "oldPathRetired", "oldReplicasExited", "oldIoDrained"].includes(fact);
          if (!resetFalse) h.setFact(fact, false);
          // The decisive assertion is the port refusal below, not a reset-flag assertion.
          if (fact === "newReplicasAfterCut") {
            await h.ports.revokeGeneration(h.revokeRequest());
            await failure(() => h.attachReservedReceiver(), "B1_MOUNT_LINEAGE_UNKNOWN", "held");
          } else {
            const code = factCases.find(item => item.fact === fact)?.code;
            if (!code) throw new Error(`Missing typed failure expectation for ${fact}`);
            await failure(() => h.ports.revokeGeneration(h.revokeRequest()), code, "held");
            withheld(h);
            if (fact === "inventoryComplete" || fact === "nodeReachable") noEffects(h);
            if (fact === "restartDisabled") expect(h.observe().operation).toMatchObject({ killSubmitted: false, retirementRequested: false });
            if (fact === "killIssued") expect(h.observe().operation?.retirementRequested).toBe(false);
          }
        });
      }
      for (const item of Object.keys(schemaHarness.observe().teardown) as B1TeardownItem[]) {
        test(`${prefix} fresh-teardown=${item}: all other items and T8 observed, this item NOT carried`, async h => {
          await prepareFields(h, number, entry);
          for (const key of Object.keys(h.observe().facts) as B1Fact[]) h.setFact(key, true);
          for (const other of Object.keys(h.observe().teardown) as B1TeardownItem[]) if (other !== item) h.setTeardown(other, true);
          // Require refusal through the port even if a broken fixture carried true.
          await failure(() => h.ports.revokeGeneration(h.revokeRequest()), "B1_TEARDOWN_INCOMPLETE", "held"); withheld(h);
        });
      }
      test(`${prefix}: wrong predecessor generations ALONE refuse before effects`, async h => {
        const previous = await prepareFields(h, number, entry); h.completeRetirement(); const request = h.revokeRequest();
        const generations = previous.request.predecessor.generations;
        const wrong = encode(generations) !== encode(request.predecessor.generations) ? generations : [];
        await failure(() => h.ports.revokeGeneration(rehash({ ...request,
          predecessor: { ...request.predecessor, generations: wrong } })), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
        noEffects(h); withheld(h);
      });
      test(`${prefix}: dropped predecessor Pods ALONE refuse before effects`, async h => {
        await prepareFields(h, number, entry); h.completeRetirement(); const request = h.revokeRequest();
        await failure(() => h.ports.revokeGeneration(rehash({ ...request,
          predecessor: { ...request.predecessor, podUIDs: [] } })), "B1_PREDECESSOR_SET_INCOMPLETE", "held");
        noEffects(h); withheld(h);
      });
      test(`${prefix}: higher nonmatching S1 revision refuses without state change`, async h => {
        await prepareFields(h, number, entry); h.completeRetirement(); const request = h.revokeRequest(), before = h.observe();
        await failure(() => h.ports.revokeGeneration(rehash({ ...request,
          expectedControllerRevision: (BigInt(request.expectedControllerRevision) + 1n).toString() })), "B1_STALE_GENERATION", "refused");
        expect(h.observe()).toEqual(before);
      });
      for (const gate of ["restartDisabled", "workloadsExited"] as const) {
        test(`${prefix}: expired retry of held ${gate} refuses and leaves ALL state unchanged`, async h => {
          await prepareFields(h, number, entry); h.completeRetirement(); h.setFact(gate, false);
          const request = h.revokeRequest();
          await failure(() => h.ports.revokeGeneration(request), gate === "restartDisabled" ? "B1_RESTART_NOT_DISABLED" : "B1_TASKS_STILL_ALIVE", "held");
          h.setFact(gate, true); const before = h.observe();
          await failure(() => h.ports.revokeGeneration({ ...request, deadlineUnixMs: 0 }), "B1_DEADLINE_HELD", "held");
          expect(h.observe()).toEqual(before); withheld(h);
          expect((await h.ports.revokeGeneration(request)).operationId).toBe(request.operationId);
        });
      }
      test(`${prefix}: complete current evidence is a positive control with exact inventory history`, async h => {
        const previous = await prepareFields(h, number, entry);
        const request = h.revokeRequest(), before = h.observe();
        expect(h.readEvidence(request.predecessor.enumeration)).toMatchObject({ data: {
          quarantinedNodeNames: before.quarantinedNodeNames, retiredCandidatePodUIDs: before.retiredCandidatePodUIDs,
          generations: request.predecessor.generations, podUIDs: request.predecessor.podUIDs,
          workloadTasks: before.actualWorkloadTaskIds, operationId: request.operationId,
        } });
        const done = await finishHandover(h);
        if (entry !== "failed-transfer") expect(done.receipt.revoked[0]?.ticketId).not.toBe(previous.request.predecessor.generations[0]?.ticketId);
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
