// Keep: trust boundary/data safety (#1199). Exercise the public provider with
// metadata-free ordinary adapters and native commit/refusal callbacks. No worker,
// real actor deletion, host startup or unguarded fixture child is involved.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { cancellationError, preserveCancellationOutcome } from "../src/async-settlement.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const claim = { key: "factory/review-slots/v1/example/repo/1/base", ifVersion: 7 };
const identity = { id: "main", name: "Main", kind: "main" as const, sessionId: "session" };
const context = (signal?: AbortSignal) => ({
  cwd: process.cwd(), signal, parentToolCallId: "outer", nestedToolCallId: "nested",
  extensionContext: { modelRegistry: { getAvailable: () => [], find: () => undefined } },
  update() {}, activity() {},
}) as unknown as FabricInvocationContext;
type Args = ConstructorParameters<typeof AgentsProvider>;
type CreateOptions = { beforeCommit(id: string, nativeBoundary?: boolean): void; checkActive(): void; onCommit(id: string): void };
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agents-slot-provider-safe-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: root });
  managers.push(manager);
  const create = vi.fn(async (request: FabricActorRequest, _options: CreateOptions) => ({ ...request, id: "actor" }));
  // mesh is deliberately absent. Ordinary adapters need no slot authority.
  const actors = { create, identity };
  const participants = { scheduleRefresh() {}, async refresh() {} };
  const provider = new AgentsProvider(manager, actors as unknown as Args[1], {} as Args[2],
    { id: "main", local: true } as Args[3], participants as unknown as Args[4], undefined,
    {} as Args[6], () => false, undefined, false);
  return { provider, create, actors, participants };
};
beforeEach(() => {
  for (const name of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET", "PI_FABRIC_WRITE_POLICY"])
    vi.stubEnv(name, undefined);
});
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("safe actor provider admission boundary", () => {
  it.each(["create", "createActor"])("%s ordinary local creation requires neither mesh nor participant identity", async action => {
    const { provider, create } = setup();
    await expect(provider.invoke(action, { name: "ordinary", instructions: "Work.", residency: "session" }, context()))
      .resolves.toMatchObject({ id: "actor" });
    expect(create).toHaveBeenCalledWith(expect.objectContaining({ name: "ordinary" }),
      expect.objectContaining({ beforeCommit: expect.any(Function), checkActive: expect.any(Function), onCommit: expect.any(Function) }));
  });

  it("ordinary durable child forwarding preserves the two-argument call without self or identity", async () => {
    const { provider, actors } = setup();
    Reflect.deleteProperty(actors, "identity");
    const createActor = vi.fn(async (request: FabricActorRequest) => ({ ...request, id: "resident" }));
    vi.spyOn(ResidentActorClient, "fromEnv").mockReturnValue({ createActor } as unknown as ResidentActorClient);
    const signal = new AbortController().signal;
    await expect(provider.invoke("create", { name: "ordinary", instructions: "Work.", residency: "durable" }, context(signal)))
      .resolves.toMatchObject({ id: "resident" });
    expect(createActor).toHaveBeenCalledWith(expect.objectContaining({ name: "ordinary", residency: "durable" }), signal);
  });

  it("claim-bearing durable forwarding binds the caller and preserves claim/scope rather than request-forged metadata", async () => {
    const { provider, participants } = setup();
    Object.assign(participants, { self: () => ({ ownerHostId: "authenticated-owner" }) });
    const createActor = vi.fn(async (request: FabricActorRequest) => ({ ...request, id: "resident" }));
    vi.spyOn(ResidentActorClient, "fromEnv").mockReturnValue({ createActor } as unknown as ResidentActorClient);
    const signal = new AbortController().signal;
    await provider.invoke("create", { name: "review", instructions: "Review.", residency: "durable", scope: "project",
      topics: ["pull.example.repo"], slotClaim: claim, idempotencyKey: "claim-7-review",
      caller: { identity: { id: "forged" }, hostId: "forged" } }, context(signal));
    expect(createActor).toHaveBeenCalledWith(expect.objectContaining({ scope: "project", topics: ["pull.example.repo"],
      slotClaim: claim, idempotencyKey: "claim-7-review" }), signal,
      { identity, hostId: "authenticated-owner" });
  });

  it("does not downgrade claim-bearing durable creation when trusted caller metadata is unavailable", async () => {
    const { provider } = setup();
    const createActor = vi.fn();
    vi.spyOn(ResidentActorClient, "fromEnv").mockReturnValue({ createActor } as unknown as ResidentActorClient);
    await expect(provider.invoke("create", { name: "review", instructions: "Review.", residency: "durable",
      topics: ["pull.example.repo"], slotClaim: claim, idempotencyKey: "claim-7-review" }, context())).rejects.toThrow();
    expect(createActor).not.toHaveBeenCalled();
  });

  it.each(["required", "authority-unavailable"])("preserves the native %s refusal without a provider policy lookup or false uncertainty", async reason => {
    const { provider, create } = setup();
    const refused = Object.assign(new Error(reason), { code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason });
    create.mockRejectedValueOnce(refused);
    const signal = new AbortController().signal;
    await expect(provider.invoke("create", { name: "review", instructions: "Review.", topics: ["pull.example.repo"],
      ...(reason === "required" ? {} : { slotClaim: claim, idempotencyKey: "retry" }) }, context(signal))).rejects.toBe(refused);
    const cancelled = new Error("cancelled");
    expect(cancellationError(signal, cancelled)).toBe(cancelled);
  });

  it.each([undefined, false])("does not invent a commit for an ordinary claimed create (native boundary=%s)", async nativeBoundary => {
    const { provider, create } = setup();
    const controller = new AbortController();
    const cancelled = new Error("cancelled before ordinary materialization");
    let boundaryOutcome: Error | undefined;
    create.mockImplementationOnce(async (request, options) => {
      options.beforeCommit("never-created", nativeBoundary);
      controller.abort(cancelled);
      boundaryOutcome = cancellationError(controller.signal, cancelled);
      options.checkActive();
      throw new Error("Cancelled ordinary create reached materialization");
    });
    const failure = await provider.invoke("create", { name: "ordinary", instructions: "Work.",
      topics: ["ordinary.topic"], slotClaim: claim, idempotencyKey: "irrelevant-claim" }, context(controller.signal))
      .catch((error: unknown) => error);
    expect(boundaryOutcome).toBe(cancelled);
    expect(failure).toBe(cancelled);
    expect(preserveCancellationOutcome({ value: undefined, terminationReason: "aborted", error: cancelled.message },
      controller.signal)).not.toHaveProperty("residentOutcomes");
  });

  it("observes a real native boundary before its cancellation check without prematurely committing the ID", async () => {
    const { provider, create } = setup();
    const controller = new AbortController();
    const cancelled = new Error("cancelled at native callback entry");
    create.mockImplementationOnce(async (_request, options) => {
      controller.abort(cancelled);
      // Models entry from the native SQL callback, not presence of a claim.
      options.beforeCommit("selected-original", true);
      throw new Error("Native cancellation check did not fence the create");
    });
    const failure = await provider.invoke("create", { name: "review", instructions: "Review.",
      topics: ["pull.example.repo"], slotClaim: claim, idempotencyKey: "retry" }, context(controller.signal))
      .catch((error: unknown) => error);
    expect(failure).toMatchObject({ name: "ResidentOutcomeUnknownError", residentOutcome: { state: "unknown" } });
    expect((failure as { residentOutcome: unknown }).residentOutcome).not.toHaveProperty("id");
  });

  it.each([false, true])("preserves commit/cancellation outcomes (actual native boundary=%s)", async nativeBoundary => {
    const { provider, create } = setup();
    const controller = new AbortController();
    const cancelled = new Error("cancelled");
    create.mockImplementationOnce(async (request, options) => {
      // This adapter explicitly models the actual SQL callback only in the native case.
      options.beforeCommit("actor", nativeBoundary);
      const atBoundary = cancellationError(controller.signal, cancelled);
      if (nativeBoundary) expect(atBoundary).toMatchObject({ name: "ResidentOutcomeUnknownError",
        residentOutcome: { state: "committed", id: "actor", ownerHostId: "main" } });
      else expect(atBoundary).toBe(cancelled);
      options.onCommit("actor");
      expect(cancellationError(controller.signal, cancelled)).toMatchObject({ name: "ResidentOutcomeUnknownError",
        residentOutcome: { state: "committed", id: "actor", ownerHostId: "main" } });
      return { ...request, id: "actor" };
    });
    await provider.invoke("create", { name: "review", instructions: "Review.",
      ...(nativeBoundary ? { slotClaim: claim, idempotencyKey: "retry", topics: ["pull.example.repo"] } : {}) }, context(controller.signal));
  });
});
