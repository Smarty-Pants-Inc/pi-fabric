// Keep: #7991 caller/error/cancellation trust boundary. A safe Node JS exchange
// fixture exercises the real client transport, not actor execution or SQLite durability.
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import {
  RESIDENT_HOST_FORMAT, RESIDENT_COMMANDS, ResidentCommandUnsupportedError, ResidentOutcomeUnknownError,
  ResidentSlotError, assertResidentCommandSupported, prepareResidentCreationCommand,
  readResidentRequestDecision, registerResidentCancellation, residentCreateMayBeProtected,
  residentHostId, residentRoot, residentRequestExpiredOutcome, residentSlotErrorFields, residentSlotResponseError,
  type ResidentActorCaller, type ResidentCommand, type ResidentCommandResponse, type ResidentHostOwner,
} from "../src/residency/protocol.js";
import { cancellationError, preserveCancellationOutcome } from "../src/async-settlement.js";
import type { FabricResidentOutcomeReceipt } from "../src/runtime/kernel.js";

const roots: string[] = [];
const children: Array<{ child: ChildProcessWithoutNullStreams; exited: Promise<unknown> }> = [];
const claim = { key: "factory/review-slots/v1/repo/1/base", ifVersion: 17 };
const caller: ResidentActorCaller = { identity: { id: "main", name: "Main", kind: "main", sessionId: "session" }, hostId: "main-host" };
const request = { name: "review", instructions: "Review.", residency: "durable" as const,
  topics: ["factory.pull.repo"], slotClaim: claim, idempotencyKey: "claim-17-review" };
const command = (): Extract<ResidentCommand, { operation: "createActor" }> => ({
  format: RESIDENT_HOST_FORMAT, operation: "createActor", requestId: "request", rootId: "main",
  request, idempotencyKey: request.idempotencyKey, createdAt: Date.now(),
});
const temporary = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-wire-"));
  roots.push(root);
  return root;
};

type FixtureEvent = { event: string; command?: ResidentCommand };
async function fixture(mode: string) {
  const root = temporary();
  const residencyRoot = residentRoot(root, "main");
  // Deliberately Node JS only. The fixture has a finite process deadline and exits
  // cooperatively through stdin; teardown never sends a signal.
  const child = spawn("node", [fileURLToPath(new URL("./fixtures/slot-wire-resident.mjs", import.meta.url)),
    residencyRoot, residentHostId("main"), mode], { stdio: ["pipe", "pipe", "pipe"], env: process.env });
  const exited = once(child, "exit");
  children.push({ child, exited });
  const pending: FixtureEvent[] = [];
  let notify: (() => void) | undefined;
  let output = "";
  let stderr = "";
  child.stderr.on("data", data => { stderr += String(data); });
  child.stdout.on("data", data => {
    output += String(data);
    for (;;) {
      const end = output.indexOf("\n");
      if (end < 0) break;
      pending.push(JSON.parse(output.slice(0, end)) as FixtureEvent);
      output = output.slice(end + 1);
    }
    notify?.();
  });
  child.once("exit", () => notify?.());
  const next = async (event: string): Promise<FixtureEvent> => {
    for (;;) {
      const index = pending.findIndex(value => value.event === event);
      if (index >= 0) return pending.splice(index, 1)[0]!;
      if (child.exitCode !== null) throw new Error(`fixture exited before ${event}: ${stderr}`);
      await new Promise<void>(resolve => { notify = resolve; });
      notify = undefined;
    }
  };
  await next("ready");
  return { client: new ResidentActorClient(root, "main", 2_000), next, residencyRoot };
}

afterEach(async () => {
  for (const { child, exited } of children.splice(0)) {
    child.stdin.end("done\n");
    const [code, signal] = await exited as [number | null, string | null];
    expect({ code, signal }).toEqual({ code: 0, signal: null });
  }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("native resident slot transport", () => {
  it("forwards claim, authenticated caller and envelope retry identity unchanged", async () => {
    const { client, next } = await fixture("success");
    const actor = await client.createActor(request, undefined, caller);
    const picked = (await next("picked")).command as Extract<ResidentCommand, { operation: "createActor" }>;
    expect(actor.id).toBe("fixture-original-actor");
    expect(picked.request.slotClaim).toEqual(claim);
    expect(picked.idempotencyKey).toBe(request.idempotencyKey);
    expect(picked.request.idempotencyKey).toBeUndefined();
    expect(picked.caller).toEqual(caller);
  });

  it("queries the selected loaded host's status over the negotiated read-only command", async () => {
    const { client, next } = await fixture("success");
    expect(await client.slotStatus()).toMatchObject({ protocol: "slot-admission-v1", enforcement: "unenforced",
      reason: "existing-authority", fixtureHost: residentHostId("main") });
    expect((await next("picked")).command?.operation).toBe("slotStatus");
  });

  it("rehydrates a known no-birth refusal without converting it to uncertainty", async () => {
    const { client, next, residencyRoot } = await fixture("refusal");
    const signal = new AbortController();
    await expect(client.createActor(request, signal.signal, caller)).rejects.toMatchObject({
      name: "ActorSlotClaimRefusedError", code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "stale",
      claimKey: claim.key, claimVersion: claim.ifVersion,
    });
    const picked = (await next("picked")).command!;
    expect(readResidentRequestDecision(residencyRoot, picked.requestId)).toBeUndefined();
    const reason = new Error("later cancellation");
    expect(cancellationError(signal.signal, reason)).toBe(reason);
  });

  it("preserves a known native caller refusal without inventing a birth", async () => {
    const { client } = await fixture("forbidden");
    const controller = new AbortController();
    await expect(client.createActor(request, controller.signal, caller)).rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
    const reason = new Error("later cancellation");
    expect(cancellationError(controller.signal, reason)).toBe(reason);
  });

  it("retains distinct post-commit uncertainty details and the tool outcome ledger", async () => {
    const { client } = await fixture("uncertain");
    const signal = new AbortController();
    await expect(client.createActor(request, signal.signal, caller)).rejects.toMatchObject({
      name: "ActorSlotCreateUncertainError", code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN",
      actorId: "fixture-original-actor", claimKey: claim.key, claimVersion: 17, requestKey: request.idempotencyKey,
    });
    const result = preserveCancellationOutcome({ value: "handled", terminationReason: "completed" as const,
      residentOutcomes: [] as FabricResidentOutcomeReceipt[] }, signal.signal, false);
    expect(result.residentOutcomes).toEqual([expect.objectContaining({ state: "unknown", id: "fixture-original-actor" })]);
  });

  it("post-dispatch cancellation is uncertain even when abandonment wins and no acceptance file exists", async () => {
    const { client, next, residencyRoot } = await fixture("cancel");
    const controller = new AbortController();
    const result = client.createActor(request, controller.signal, caller).catch(error => error);
    const picked = (await next("picked")).command!;
    controller.abort(new Error("cancel after dispatch"));
    const error = await result;
    expect(error).toBeInstanceOf(ResidentOutcomeUnknownError);
    expect(error).toMatchObject({ claimKey: claim.key, claimVersion: 17, requestKey: request.idempotencyKey });
    expect(readResidentRequestDecision(residencyRoot, picked.requestId)?.state).toBe("abandoned");
    expect(error.message).toContain("Do not retry or reassign");
  });

  it("pre-dispatch cancellation publishes nothing", async () => {
    const { client, residencyRoot } = await fixture("success");
    const controller = new AbortController();
    controller.abort(new Error("cancel before dispatch"));
    await expect(client.createActor(request, controller.signal, caller)).rejects.toThrow("cancel before dispatch");
    expect(fs.readdirSync(path.join(residencyRoot, "requests"))).toEqual([]);
  });
});

describe("slot error and cancellation contract", () => {
  it("allowlists native error details and preserves hard durability errors", () => {
    const fields = residentSlotErrorFields({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "foreign-root",
      actorId: "actor", claimKey: claim.key, claimVersion: 17, requestKey: "retry", secret: "not transported" });
    expect(fields).toEqual({ errorCode: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", slotError: {
      reason: "foreign-root", actorId: "actor", claimKey: claim.key, claimVersion: 17, requestKey: "retry",
    } });
    expect(residentSlotErrorFields(new Error("plain"))).toEqual({});
    const response: ResidentCommandResponse = { format: 1, requestId: "request", ok: false,
      errorCode: "FABRIC_MESH_STATE_NATS_NOT_DURABLE", error: "periodic sync is not durable", completedAt: 1 };
    expect(residentSlotResponseError(response)).toMatchObject({
      code: "FABRIC_MESH_STATE_NATS_NOT_DURABLE", name: "MeshStateNatsNotDurableError",
    });
    expect(residentSlotResponseError({ ...response, errorCode: "FABRIC_MODEL_DENIED" })).toBeUndefined();
    expect(new ResidentSlotError("FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", "sync failed", { actorId: "actor", claimVersion: 17 }).message)
      .toContain("actorId=actor, claimVersion=17");
  });

  it("requires explicit negotiated slotStatus support", () => {
    expect(RESIDENT_COMMANDS).toContain("slotStatus");
    expect(() => assertResidentCommandSupported({ commands: ["createActor"] } as unknown as ResidentHostOwner, "slotStatus"))
      .toThrow(ResidentCommandUnsupportedError);
  });

  it("does not send a token to a loaded host predating native slot support", () => {
    expect(() => prepareResidentCreationCommand({ creationIdempotency: 1, commands: ["createActor"] } as unknown as ResidentHostOwner, command()))
      .toThrow(/no token-bearing create request was dispatched/);
  });

  it("never invents an omitted protected retry key", () => {
    const owner = { creationIdempotency: 1, commands: ["createActor", "slotStatus"] } as unknown as ResidentHostOwner;
    const unkeyed = command();
    delete unkeyed.idempotencyKey;
    expect(prepareResidentCreationCommand(owner, unkeyed)).not.toHaveProperty("idempotencyKey");
    const ordinary = { ...unkeyed, request: { name: "ordinary", instructions: "Work." } };
    expect(prepareResidentCreationCommand(owner, ordinary)).toMatchObject({ idempotencyKey: expect.any(String) });
  });

  it("cannot infer no effects from an abandoned or collected protected request fence", () => {
    const root = temporary();
    const controller = new AbortController();
    const create = command();
    let dispatched = false;
    registerResidentCancellation(controller.signal, root, create, () => dispatched);
    const reason = new Error("cancel");
    expect(cancellationError(controller.signal, reason)).toBe(reason);
    expect(readResidentRequestDecision(root, create.requestId)).toBeUndefined();
    dispatched = true;
    expect(cancellationError(controller.signal, reason)).toBeInstanceOf(ResidentOutcomeUnknownError);
    expect(readResidentRequestDecision(root, create.requestId)?.state).toBe("abandoned");
    expect(residentRequestExpiredOutcome(root, create)).toBeInstanceOf(ResidentOutcomeUnknownError);
    const { slotClaim: _token, ...withoutClaim } = create.request;
    expect(residentCreateMayBeProtected({ ...create, request: withoutClaim })).toBe(true);
  });
});
