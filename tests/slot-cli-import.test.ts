import { describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { registerFabricCommand } from "../src/commands/fabric.js";
import type { FabricState } from "../src/fabric-state.js";

// Keep reason: #7991 trust boundary. Exercise the registered dispatcher, not
// parser/source-text surrogates; the fake actor port must receive only validated,
// explicit per-materialization authority. Never launch a real actor or worker.
const claimKey = "factory/review-slots/v1/acme/7/base";
const claimFlags = `--slot-claim ${claimKey} --slot-version 12 --idempotency-key acme-review-7`;

function commandHarness() {
  type Command = { handler: (args: string, context: ExtensionContext) => Promise<void> };
  const commands = new Map<string, Command>();
  const definition = Object.freeze({ id: "template-1", name: "review-template" });
  const templateRequest = Object.freeze({
    name: definition.name,
    instructions: "Review the change.",
    topics: Object.freeze(["review/input"]),
  });
  const resolve = vi.fn((key: string) =>
    key === definition.id || key === definition.name ? definition : undefined);
  const toRequest = vi.fn((_definition: typeof definition, as?: string) =>
    as ? { ...templateRequest, name: as } : templateRequest);
  const create = vi.fn(async (request: Record<string, unknown>) => ({ id: "actor-1", name: String(request.name) }));
  const remove = vi.fn();
  const mutateTemplate = vi.fn();
  const put = vi.fn();
  const notify = vi.fn();
  const ensure = vi.fn(async () => undefined);
  const state = {
    ensure,
    globalActors: { resolve, toRequest, create: mutateTemplate, update: mutateTemplate },
    actors: { create, remove },
    mesh: { put, delete: put },
  } as unknown as FabricState;
  const pi = {
    registerCommand: (name: string, command: Command) => { commands.set(name, command); },
  } as unknown as ExtensionAPI;
  registerFabricCommand(pi, {
    state,
    fabricUi: {} as never,
    capturedTools: {} as never,
    applyFabricMode: vi.fn(),
    suspendToolCapture: vi.fn(),
  });
  const command = commands.get("fabric");
  if (!command) throw new Error("fabric command was not registered");
  const context = { ui: { notify } } as unknown as ExtensionContext;
  return {
    dispatch: (args: string) => command.handler(args, context),
    create, notify, resolve, toRequest, ensure, templateRequest, definition,
    remove, mutateTemplate, put,
  };
}

describe("/fabric import slot admission flags", () => {
  it("passes explicit claim, revision, and retry identity through the registered command", async () => {
    const harness = commandHarness();
    await harness.dispatch(`import review-template as imported ${claimFlags}`);

    expect(harness.create.mock.calls.map(([request]) => request)).toEqual([{
      name: "imported",
      instructions: "Review the change.",
      topics: ["review/input"],
      slotClaim: { key: claimKey, ifVersion: 12 },
      idempotencyKey: "acme-review-7",
    }]);
    expect(harness.notify).toHaveBeenLastCalledWith('Imported global actor "review-template" as imported', "info");
    expect(harness.ensure).toHaveBeenCalledOnce();
  });

  it.each([
    ["review-template", undefined],
    ["template-1", undefined],
    ["review-template as ordinary", "ordinary"],
  ])("keeps ordinary name/id/as imports unchanged: %s", async (args, as) => {
    const harness = commandHarness();
    await harness.dispatch(`import ${args}`);
    expect(harness.create.mock.calls.map(([request]) => request)).toEqual([{
      ...harness.templateRequest, name: as ?? "review-template",
    }]);
    expect(harness.toRequest).toHaveBeenCalledWith(harness.definition, as);
  });

  it("consumes flag values separately from rename syntax and supports interleaved flags", async () => {
    const harness = commandHarness();
    await harness.dispatch("import --idempotency-key as review-template --slot-version 12 as renamed --slot-claim claim/as");
    expect(harness.create.mock.calls[0]?.[0]).toEqual({
      ...harness.templateRequest,
      name: "renamed",
      slotClaim: { key: "claim/as", ifVersion: 12 },
      idempotencyKey: "as",
    });
  });

  it("accepts the maximum safe revision and bounded key lengths without normalization", async () => {
    const harness = commandHarness();
    const key = "k".repeat(256);
    await harness.dispatch(`import review-template --slot-claim ${key} --slot-version 9007199254740991 --idempotency-key ${key}`);
    expect(harness.create.mock.calls[0]?.[0]).toMatchObject({
      slotClaim: { key, ifVersion: Number.MAX_SAFE_INTEGER }, idempotencyKey: key,
    });
  });

  it.each([
    ["--slot-claim claim", "--slot-claim and --slot-version must be supplied together"],
    ["--slot-version 12", "--slot-claim and --slot-version must be supplied together"],
    ["--slot-claim claim --slot-version 12", "Protected import requires --idempotency-key"],
    ["--slot-claim", "--slot-claim requires a value"],
    ["--slot-version", "--slot-version requires a value"],
    ["--idempotency-key", "--idempotency-key requires a value"],
    ["--slot-claim --slot-version 12", "--slot-claim requires a value"],
    ["--slot-version --idempotency-key retry", "--slot-version requires a value"],
    ["--idempotency-key --slot-claim claim", "--idempotency-key requires a value"],
    [`${claimFlags} --slot-claim other`, "--slot-claim must be supplied only once"],
    [`${claimFlags} --slot-version 13`, "--slot-version must be supplied only once"],
    [`${claimFlags} --idempotency-key other`, "--idempotency-key must be supplied only once"],
    ["--slot-cliam claim", "Unknown import option: --slot-cliam"],
    ["--slot-claim=claim", "Unknown import option: --slot-claim=claim"],
    ["as", "Usage: /fabric import"],
  ])("refuses malformed/incomplete flags before resolving or creating: %s", async (flags, message) => {
    const harness = commandHarness();
    await harness.dispatch(`import review-template ${flags}`);
    expect(harness.create.mock.calls).toEqual([]);
    expect(harness.resolve).not.toHaveBeenCalled();
    expect(harness.notify).toHaveBeenLastCalledWith(expect.stringContaining(message), "error");
  });

  it.each(["0", "-1", "1.5", "1e2", "0x10", "NaN", "Infinity", "9007199254740992", "01", "+1", "1junk"])(
    "rejects invalid revision %s before create",
    async (revision) => {
      const harness = commandHarness();
      await harness.dispatch(`import review-template --slot-claim claim --slot-version ${revision} --idempotency-key retry`);
      expect(harness.create.mock.calls).toEqual([]);
      expect(harness.notify).toHaveBeenLastCalledWith("--slot-version must be a positive safe integer", "error");
    },
  );

  it.each(["--slot-claim", "--idempotency-key"])("rejects oversized and control-character stable keys for %s", async (flag) => {
    for (const key of ["k".repeat(257), "bad\0key", "bad\x7fkey"]) {
      const harness = commandHarness();
      const flags = flag === "--slot-claim"
        ? `--slot-claim ${key} --slot-version 12 --idempotency-key retry`
        : `--slot-claim claim --slot-version 12 --idempotency-key ${key}`;
      await harness.dispatch(`import review-template ${flags}`);
      expect(harness.create.mock.calls).toEqual([]);
      expect(harness.notify).toHaveBeenLastCalledWith(expect.stringContaining(`${flag} must be a nonempty stable key`), "error");
    }
  });

  it("keeps tokens out of reusable templates and later ordinary imports; exact retries stay unchanged", async () => {
    const harness = commandHarness();
    await harness.dispatch(`import review-template ${claimFlags}`);
    await harness.dispatch(`import review-template ${claimFlags}`);
    await harness.dispatch("import review-template");
    const requests = harness.create.mock.calls.map(([request]) => request);
    expect(requests[0]).toEqual(requests[1]);
    expect(requests[0]).not.toBe(harness.templateRequest);
    expect(harness.templateRequest).toEqual({
      name: "review-template", instructions: "Review the change.", topics: ["review/input"],
    });
    expect(requests[2]).toEqual(harness.templateRequest);
    expect(requests[2]).not.toHaveProperty("slotClaim");
    expect(requests[2]).not.toHaveProperty("idempotencyKey");
    expect(harness.mutateTemplate).not.toHaveBeenCalled();
  });

  it("supports an explicit ordinary-import retry key without inventing a claim", async () => {
    const harness = commandHarness();
    await harness.dispatch("import review-template --idempotency-key ordinary-retry");
    expect(harness.create.mock.calls[0]?.[0]).toEqual({ ...harness.templateRequest, idempotencyKey: "ordinary-retry" });
  });

  it.each([
    "FABRIC_ACTOR_SLOT_CLAIM_REFUSED: stale claim",
    "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN: reconcile actor-1",
  ])("reports native errors without retry, token refresh, or compensation: %s", async (message) => {
    const harness = commandHarness();
    harness.create.mockRejectedValueOnce(new Error(message));
    await harness.dispatch(`import review-template ${claimFlags}`);
    expect(harness.create).toHaveBeenCalledOnce();
    expect(harness.create.mock.calls[0]?.[0]).toMatchObject({
      slotClaim: { key: claimKey, ifVersion: 12 }, idempotencyKey: "acme-review-7",
    });
    expect(harness.notify).toHaveBeenLastCalledWith(message, "error");
    expect(harness.remove).not.toHaveBeenCalled();
    expect(harness.put).not.toHaveBeenCalled();
    expect(harness.mutateTemplate).not.toHaveBeenCalled();
  });

  it("shows the syntax without creating when no template is supplied", async () => {
    const harness = commandHarness();
    await harness.dispatch("import");
    expect(harness.create.mock.calls).toEqual([]);
    expect(harness.notify).toHaveBeenLastCalledWith(expect.stringContaining("--slot-claim KEY --slot-version N --idempotency-key KEY"), "warning");
  });

  it("keeps the unknown-template error and never creates", async () => {
    const harness = commandHarness();
    await harness.dispatch(`import unknown ${claimFlags}`);
    expect(harness.create.mock.calls).toEqual([]);
    expect(harness.notify).toHaveBeenLastCalledWith("Unknown global actor: unknown", "error");
  });
});
