import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { DecisionStore, type DecisionRecord } from "../src/decisions/store.js";
import { requestHeadlessApproval, routeChildQuestion } from "../src/decisions/host.js";
import { ApprovalController } from "../src/core/approval-controller.js";
import type { ResolvedFabricAction } from "../src/core/action-registry.js";
import type { AgentChildQuestionRequest } from "../src/agents/types.js";

const roots: string[] = [];
const main: MeshIdentity = { id: "session:main", name: "main", kind: "main", sessionId: "main" };
const human = { answeredBy: "alice", via: "cli" };

const decisionStore = (): DecisionStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-decision-host-"));
  roots.push(root);
  return new DecisionStore(new MeshStore(root, 64 * 1024, 500), main);
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const waitForOpen = async (store: DecisionStore): Promise<DecisionRecord> => {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const [open] = await store.list({ status: "open" });
    if (open) return open;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("no open decision appeared");
};

const request = (
  question: Record<string, unknown>,
  signal = new AbortController().signal,
): AgentChildQuestionRequest & { onDecision: ReturnType<typeof vi.fn<(id: string) => void>> } => ({
  runId: "run-1",
  name: "worker",
  question: { requestId: "q1", timeout: 60_000, ...question },
  signal,
  onDecision: vi.fn<(id: string) => void>(),
});

const action: ResolvedFabricAction = {
  ref: "demo.write",
  provider: "demo",
  name: "write",
  description: "Write data",
  inputSchema: {},
  risk: "write",
};

describe("routeChildQuestion", () => {
  it("asks through the parent's interactive UI, labelled with the child name", async () => {
    const select = vi.fn(async () => "Beta");
    const confirm = vi.fn(async () => true);
    const input = vi.fn(async () => undefined);
    const context = { hasUI: true, ui: { select, confirm, input } } as unknown as ExtensionContext;
    expect(await routeChildQuestion(request({ method: "select", title: "Pick", options: ["Alpha", "Beta"] }), { context }))
      .toEqual({ value: "Beta" });
    expect(select).toHaveBeenCalledWith("worker: Pick", ["Alpha", "Beta"], expect.objectContaining({ timeout: 60_000 }));
    expect(await routeChildQuestion(request({ method: "confirm", title: "Sure?", message: "m" }), { context }))
      .toEqual({ confirmed: true });
    expect(await routeChildQuestion(request({ method: "input", title: "Name" }), { context })).toEqual({ cancelled: true });
  });

  it("raises a root-held decision without UI and maps the answer back", async () => {
    const store = decisionStore();
    const context = { hasUI: false } as ExtensionContext;
    const routed = request({ method: "select", title: "Pick", message: "why", options: ["Alpha", "Beta"] });
    const pending = routeChildQuestion(routed, { context, store });
    const open = await waitForOpen(store);
    expect(open).toMatchObject({
      kind: "question",
      holder: "root",
      title: "worker: Pick",
      body: "why",
      raisedBy: { participantId: "run-1", runId: "run-1" },
      options: [{ id: "o1", label: "Alpha" }, { id: "o2", label: "Beta" }],
    });
    expect(routed.onDecision).toHaveBeenCalledWith(open.id);
    await store.answer(open.id, { optionId: "o2" }, human);
    expect(await pending).toEqual({ value: "Beta" });
  });

  it("maps text and confirm decisions and cancels on decision cancel", async () => {
    const store = decisionStore();
    const context = { hasUI: false } as ExtensionContext;
    const text = routeChildQuestion(request({ method: "input", title: "Name" }), { context, store });
    await store.answer((await waitForOpen(store)).id, { text: "Ada" }, human);
    expect(await text).toEqual({ value: "Ada" });
    const confirm = routeChildQuestion(request({ method: "confirm", title: "Ok?" }), { context, store });
    await store.answer((await waitForOpen(store)).id, { optionId: "no" }, human);
    expect(await confirm).toEqual({ confirmed: false });
    const cancelled = routeChildQuestion(request({ method: "editor", title: "Draft" }), { context, store });
    await store.cancel((await waitForOpen(store)).id, human);
    expect(await cancelled).toEqual({ cancelled: true });
  });

  it("cancels the decision when the child run settles", async () => {
    const store = decisionStore();
    const controller = new AbortController();
    const pending = routeChildQuestion(
      request({ method: "input", title: "Slow" }, controller.signal),
      { context: { hasUI: false } as ExtensionContext, store },
    );
    const open = await waitForOpen(store);
    controller.abort(new Error("settled"));
    await expect(pending).rejects.toThrow("settled");
    expect((await store.get(open.id))?.status).toBe("cancelled");
  });

  it("fails closed without UI or store, and on malformed requests", async () => {
    const context = { hasUI: false } as ExtensionContext;
    expect(await routeChildQuestion(request({ method: "input", title: "x" }), { context })).toEqual({ cancelled: true });
    expect(await routeChildQuestion(request({ method: "notify" }), { context, store: decisionStore() })).toEqual({ cancelled: true });
    expect(await routeChildQuestion(request({ method: "select", options: [] }), { context, store: decisionStore() }))
      .toEqual({ cancelled: true });
  });
});

describe("headless approvals", () => {
  it("approves only on an explicit approve answer", async () => {
    const store = decisionStore();
    const approved = requestHeadlessApproval(store, action, { reason: "because" });
    const open = await waitForOpen(store);
    expect(open).toMatchObject({ kind: "approval", holder: "user", title: "demo.write requests write access" });
    expect(open.body).toBe("Write data\n\nbecause");
    await store.answer(open.id, { optionId: "approve" }, human);
    expect(await approved).toBe(true);

    const denied = requestHeadlessApproval(store, action);
    await store.answer((await waitForOpen(store)).id, { optionId: "deny" }, human);
    expect(await denied).toBe(false);
  });

  it("denies on expiry", async () => {
    const store = decisionStore();
    expect(await requestHeadlessApproval(store, action, { timeoutMs: 1_000 })).toBe(false);
    const [expired] = await store.list({ status: "expired" });
    expect(expired?.kind).toBe("approval");
  });

  it("ApprovalController uses the decision path only when approvals.headless is decision", async () => {
    const policies = { read: "allow", write: "ask", execute: "ask", network: "ask", agent: "ask" } as const;
    const headless = vi.fn(async () => true);
    const noUi = { hasUI: false } as ExtensionContext;
    await expect(new ApprovalController(policies, noUi, undefined, undefined, undefined, undefined, headless).approve(action))
      .rejects.toThrow(/no interactive UI/);
    expect(headless).not.toHaveBeenCalled();
    const decisionPolicies = { ...policies, headless: "decision" as const };
    const signal = new AbortController().signal;
    await new ApprovalController(decisionPolicies, noUi, undefined, undefined, undefined, undefined, headless).approve(action, {}, signal);
    expect(headless).toHaveBeenCalledWith(action, undefined, signal);
    const denying = vi.fn(async () => false);
    await expect(new ApprovalController(decisionPolicies, noUi, undefined, undefined, undefined, undefined, denying).approve(action))
      .rejects.toThrow(/denied, cancelled, or expired/);
    // No handler wired: still fail closed.
    await expect(new ApprovalController(decisionPolicies, noUi).approve(action)).rejects.toThrow(/no interactive UI/);
  });
});

describe("/fabric decisions", () => {
  it("lists open decisions and answers the picked one through native dialogs", async () => {
    const { openFabricDecisions } = await import("../src/decisions/command.js");
    const store = decisionStore();
    const approval = await store.raise({
      kind: "approval",
      title: "demo.write requests write access",
      options: [{ id: "approve", label: "Approve once" }, { id: "deny", label: "Deny" }],
    });
    const select = vi.fn()
      .mockImplementationOnce(async (_title: string, labels: string[]) => labels[0])
      .mockImplementationOnce(async () => "Approve once");
    const notify = vi.fn();
    const context = { hasUI: true, ui: { select, notify } } as unknown as ExtensionContext;
    await openFabricDecisions(store.mesh, context);
    expect(select.mock.calls[1]![1]).toEqual(["Approve once", "Deny", "Cancel this decision"]);
    expect(await store.get(approval.id)).toMatchObject({ status: "answered", answer: { optionId: "approve", via: "tui" } });

    const text = await store.raise({ title: "Why?" });
    const input = vi.fn(async () => "because");
    await openFabricDecisions(store.mesh, { hasUI: true, ui: { input, notify } } as unknown as ExtensionContext, text.id.slice(-6));
    expect((await store.get(text.id))?.answer?.text).toBe("because");
    await openFabricDecisions(store.mesh, context);
    expect(notify).toHaveBeenLastCalledWith("No open Fabric decisions", "info");
  });
});
