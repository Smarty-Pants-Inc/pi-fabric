import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runDecisionsCli } from "../src/cli/decisions.js";
import { openFabricDecisions } from "../src/decisions/command.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import {
  applyDecisionDeadline,
  DECISION_PREFIX,
  DecisionStore,
  DECISIONS_TOPIC,
  defaultEscalationChain,
  type DecisionRecord,
} from "../src/decisions/store.js";
import { DecisionsProvider } from "../src/providers/decisions-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const roots: string[] = [];
const main: MeshIdentity = { id: "session:main", name: "main", kind: "main", sessionId: "main" };
const lead: MeshIdentity = { id: "run-lead", name: "lead", kind: "agent", sessionId: "lead" };
const worker: MeshIdentity = { id: "run-worker", name: "worker", kind: "agent", sessionId: "worker" };
const HOP = 60_000;

const meshAt = (root?: string): MeshStore => {
  const directory = root ?? fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-escalation-"));
  if (!root) roots.push(directory);
  return new MeshStore(directory, 64 * 1024, 500);
};

const context = (parentToolCallId: string): FabricInvocationContext => ({
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId,
  nestedToolCallId: `${parentToolCallId}:n`,
  extensionContext: {} as FabricInvocationContext["extensionContext"],
  update: () => {},
});

const clock = (start = 10_000_000) => {
  const state = { now: start };
  return { state, now: () => state.now };
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("decision escalation chains", () => {
  it("defaults the chain upward from the holder and the first deadline to one hop", async () => {
    expect(defaultEscalationChain(`supervisor:${lead.id}`)).toEqual([`supervisor:${lead.id}`, "root", "user"]);
    expect(defaultEscalationChain("root")).toEqual(["root", "user"]);
    expect(defaultEscalationChain("user")).toEqual(["user"]);
    const { state, now } = clock();
    const store = new DecisionStore(meshAt(), main, now);
    const raised = await store.raise({ title: "Climb", holder: `supervisor:${lead.id}`, onExpire: "escalate" });
    expect(raised).toMatchObject({
      holder: `supervisor:${lead.id}`,
      deadline: state.now + 600_000,
      escalation: { chain: [`supervisor:${lead.id}`, "root", "user"], hop: 0, hopTimeoutMs: 600_000, onFinal: "cancel" },
    });
    const explicit = await store.raise({ title: "Soon", holder: "root", onExpire: "escalate", timeoutMs: 5_000 });
    expect(explicit.deadline).toBe(state.now + 5_000);
    expect(explicit.escalation?.chain).toEqual(["root", "user"]);
  });

  it("validates explicit chains and escalation options (fail closed)", async () => {
    const store = new DecisionStore(meshAt(), main);
    const options = [{ id: "keep", label: "Keep" }, { id: "drop", label: "Drop" }];
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { chain: ["user", "root"] } }))
      .rejects.toThrow(/start with the holder/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { chain: ["root", "user", "root"] } }))
      .rejects.toThrow(/repeats/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { chain: [] } }))
      .rejects.toThrow(/1\.\.8/);
    const nine = ["root", ...Array.from({ length: 8 }, (_, index) => `supervisor:s${index}`)];
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { chain: nine } }))
      .rejects.toThrow(/1\.\.8/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { chain: ["root", "admin"] } }))
      .rejects.toThrow(/holder/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { hopTimeoutMs: 999 } }))
      .rejects.toThrow(/hopTimeoutMs/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { hopTimeoutMs: 8 * 86_400_000 } }))
      .rejects.toThrow(/hopTimeoutMs/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { onFinal: "default" } }))
      .rejects.toThrow(/onFinal "default" requires defaultOptionId/);
    await expect(store.raise({ title: "t", holder: "root", onExpire: "escalate", escalation: { extra: 1 } }))
      .rejects.toThrow(/Unknown decision escalation field/);
    await expect(store.raise({ title: "t", holder: "root", escalation: {} })).rejects.toThrow(/requires onExpire "escalate"/);
    await expect(store.raise({ title: "t", onExpire: "later" })).rejects.toThrow(/onExpire/);
    const valid = await store.raise({
      title: "ok",
      holder: `supervisor:${worker.id}`,
      options,
      defaultOptionId: "keep",
      onExpire: "escalate",
      escalation: { chain: [`supervisor:${worker.id}`, `supervisor:${lead.id}`, "user"], hopTimeoutMs: 2_000, onFinal: "default" },
    });
    expect(valid.escalation).toEqual({
      chain: [`supervisor:${worker.id}`, `supervisor:${lead.id}`, "user"], hop: 0, hopTimeoutMs: 2_000, onFinal: "default",
    });
  });

  it("moves one hop on expiry, keeps the decision open, and posts a wake event", async () => {
    const { state, now } = clock();
    const store = new DecisionStore(meshAt(), main, now);
    const raised = await store.raise({
      title: "Hop",
      holder: `supervisor:${worker.id}`,
      onExpire: "escalate",
      escalation: { hopTimeoutMs: HOP },
    });
    const firstDeadline = raised.deadline!;
    state.now = firstDeadline + 10;
    const moved = await store.get(raised.id);
    expect(moved).toMatchObject({
      status: "open",
      holder: "root",
      deadline: firstDeadline + HOP,
      escalation: { hop: 1 },
      history: [{ holder: `supervisor:${worker.id}`, until: firstDeadline, reason: "expired" }],
    });
    expect((await store.list({ holder: "root" })).map((record) => record.id)).toEqual([raised.id]);
    expect(await store.list({ holder: `supervisor:${worker.id}` })).toEqual([]);
    const events = store.mesh.read({ topic: DECISIONS_TOPIC });
    expect(events.map((event) => event.kind)).toEqual(["decision.raised", "decision.escalated"]);
    expect(events[1]?.data).toMatchObject({ id: raised.id, holder: "root", status: "open", hop: 1 });
  });

  it("applies several elapsed hops in one step and then onFinal cancel", async () => {
    const { state, now } = clock();
    const store = new DecisionStore(meshAt(), main, now);
    const raised = await store.raise({
      title: "Late reader",
      holder: `supervisor:${worker.id}`,
      onExpire: "escalate",
      escalation: { hopTimeoutMs: HOP },
    });
    state.now = raised.deadline! + HOP + 5;
    const moved = await store.get(raised.id);
    expect(moved).toMatchObject({ status: "open", holder: "user", escalation: { hop: 2 }, deadline: raised.deadline! + 2 * HOP });
    expect(moved?.history?.map((entry) => entry.holder)).toEqual([`supervisor:${worker.id}`, "root"]);
    expect(store.mesh.read({ topic: DECISIONS_TOPIC }).map((event) => event.kind))
      .toEqual(["decision.raised", "decision.escalated"]);

    state.now = raised.deadline! + 10 * HOP;
    const final = await store.get(raised.id);
    expect(final).toMatchObject({ status: "expired", holder: "user", escalation: { hop: 2 } });
    expect(final?.answer).toBeUndefined();
    await expect(store.answer(raised.id, { text: "late" }, { answeredBy: "u", via: "cli" })).rejects.toThrow(/expired/);
  });

  it("resolves past the last hop with onFinal default straight from the first holder", async () => {
    const { state, now } = clock();
    const store = new DecisionStore(meshAt(), main, now);
    const raised = await store.raise({
      title: "Default at the end",
      holder: "root",
      options: [{ id: "keep", label: "Keep" }, { id: "drop", label: "Drop" }],
      defaultOptionId: "keep",
      onExpire: "escalate",
      escalation: { hopTimeoutMs: HOP, onFinal: "default" },
    });
    state.now = raised.deadline! + 5 * HOP;
    const final = await store.get(raised.id);
    expect(final).toMatchObject({
      status: "expired",
      holder: "user",
      answer: { optionId: "keep", answeredBy: "deadline", via: "expiry" },
      history: [{ holder: "root", reason: "expired" }],
    });
    expect(store.mesh.read({ topic: DECISIONS_TOPIC }).map((event) => event.kind))
      .toEqual(["decision.raised", "decision.expired"]);
  });

  it("lets exactly one of two racing movers win", async () => {
    const { state, now } = clock();
    const mesh = meshAt();
    const first = new DecisionStore(mesh, main, now);
    const second = new DecisionStore(new MeshStore(mesh.root, 64 * 1024, 500), lead, now);
    const raised = await first.raise({ title: "Race", holder: "root", onExpire: "escalate", escalation: { hopTimeoutMs: HOP } });
    state.now = raised.deadline! + 1;
    const [left, right] = await Promise.all([first.get(raised.id), second.get(raised.id)]);
    expect(left).toEqual(right);
    expect(left).toMatchObject({ holder: "user", escalation: { hop: 1 } });
    expect(left?.history).toHaveLength(1);
    expect(mesh.get(DECISION_PREFIX + raised.id)?.version).toBe(2);
    expect(mesh.read({ topic: DECISIONS_TOPIC }).filter((event) => event.kind === "decision.escalated")).toHaveLength(1);
  });

  it("keeps old records without escalation fields working", async () => {
    const { state, now } = clock();
    const mesh = meshAt();
    const store = new DecisionStore(mesh, main, now);
    const legacy: Record<string, unknown> = {
      id: "dec_legacy000001",
      kind: "question",
      title: "Old",
      input: "text",
      raisedBy: { participantId: main.id },
      holder: "root",
      createdAt: state.now,
      deadline: state.now + 1_000,
      onExpire: "cancel",
      status: "open",
    };
    await mesh.put({ key: DECISION_PREFIX + legacy.id, value: legacy, identity: main, ifVersion: 0 });
    expect(await store.get(legacy.id as string)).toEqual(legacy);
    state.now += 2_000;
    const expired = await store.get(legacy.id as string);
    expect(expired).toMatchObject({ status: "expired", holder: "root" });
    expect(expired?.history).toBeUndefined();
    expect(expired?.escalation).toBeUndefined();
    // A malformed chain escalates nowhere: it expires as a plain cancel.
    const broken = { ...legacy, onExpire: "escalate", escalation: { chain: "root", hop: 0 } } as unknown as DecisionRecord;
    expect(applyDecisionDeadline(broken, state.now + 10_000)).toMatchObject({ status: "expired", holder: "root" });
  });
});

describe("decisions.escalate", () => {
  it("lets the current holder pass the decision up and records the reason", async () => {
    const { state, now } = clock();
    const mesh = meshAt();
    const root = new DecisionsProvider(new DecisionStore(mesh, main, now), main);
    const workerProvider = new DecisionsProvider(new DecisionStore(mesh, worker, now), worker);
    const { id } = await root.invoke("raise", {
      title: "Needs judgement",
      holder: `supervisor:${worker.id}`,
      onExpire: "escalate",
      escalation: { hopTimeoutMs: HOP },
    }, context("raise")) as { id: string };
    state.now += 100;
    const moved = await workerProvider.invoke("escalate", { id, reason: "outside my lane" }, context("w1")) as DecisionRecord;
    expect(moved).toMatchObject({
      status: "open",
      holder: "root",
      deadline: state.now + HOP,
      escalation: { hop: 1 },
      history: [{ holder: `supervisor:${worker.id}`, until: state.now, reason: "escalated", text: "outside my lane", by: worker.id }],
    });
    // The previous holder lost authority.
    await expect(workerProvider.invoke("answer", { id, text: "late" }, context("w2"))).rejects.toThrow(/held by root/);
    await expect(workerProvider.invoke("escalate", { id }, context("w3"))).rejects.toThrow(/held by root/);
    // Root escalates to the user; the user is the last hop.
    expect(await root.invoke("escalate", { id }, context("r1"))).toMatchObject({ holder: "user", escalation: { hop: 2 } });
    await expect(root.invoke("escalate", { id }, context("r2"))).rejects.toThrow(/held by the user/);
    await expect(root.invoke("answer", { id, text: "x" }, context("r3"))).rejects.toThrow(/held by the user/);
    expect(mesh.read({ topic: DECISIONS_TOPIC }).filter((event) => event.kind === "decision.escalated")).toHaveLength(2);
  });

  it("refuses non-holders, self-raised escalation, and decisions with no next hop", async () => {
    const mesh = meshAt();
    const root = new DecisionsProvider(new DecisionStore(mesh, main), main);
    const leadProvider = new DecisionsProvider(new DecisionStore(mesh, lead), lead);
    const { id } = await root.invoke("raise", {
      title: "Root first", holder: "root", onExpire: "escalate",
    }, context("a")) as { id: string };
    await expect(leadProvider.invoke("escalate", { id }, context("b"))).rejects.toThrow(/held by root/);
    await expect(root.invoke("escalate", { id }, context("a"))).rejects.toThrow(/same program/);
    const plain = await root.invoke("raise", { title: "Plain", holder: "root" }, context("c")) as { id: string };
    await expect(root.invoke("escalate", { id: plain.id }, context("d"))).rejects.toThrow(/no further holder/);
    const single = await root.invoke("raise", {
      title: "Single", holder: "root", onExpire: "escalate", escalation: { chain: ["root"] },
    }, context("e")) as { id: string };
    await expect(root.invoke("escalate", { id: single.id }, context("f"))).rejects.toThrow(/no further holder/);
    await expect(root.invoke("escalate", { id, reason: "x".repeat(501) }, context("g"))).rejects.toThrow(/Invalid decisions.escalate/);
    expect((await root.invoke("list", { holder: "root", status: "open" }, context("h")) as DecisionRecord[]).map((record) => record.id).sort())
      .toEqual([id, plain.id, single.id].sort());
  });

  it("checks authority against the record after an expiry moved it", async () => {
    const { state, now } = clock();
    const mesh = meshAt();
    const leadProvider = new DecisionsProvider(new DecisionStore(mesh, lead, now), lead);
    const root = new DecisionsProvider(new DecisionStore(mesh, main, now), main);
    const { id } = await root.invoke("raise", {
      title: "Lead first",
      holder: `supervisor:${lead.id}`,
      onExpire: "escalate",
      escalation: { hopTimeoutMs: HOP },
    }, context("a")) as { id: string };
    state.now += HOP + 1;
    await expect(leadProvider.invoke("answer", { id, text: "too late" }, context("b"))).rejects.toThrow(/held by root/);
    expect(await root.invoke("answer", { id, text: "root decides" }, context("c")))
      .toMatchObject({ status: "answered", holder: "root", answer: { text: "root decides" } });
  });
});

describe("escalation surfaces", () => {
  it("shows hop and chain in the CLI list and the /fabric decisions picker", async () => {
    const mesh = meshAt();
    const store = new DecisionStore(mesh, main);
    const raised = await store.raise({ title: "Chained", holder: `supervisor:${lead.id}`, onExpire: "escalate" });
    let stdout = "";
    const code = await runDecisionsCli(["list", "--root", mesh.root], {
      stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
      stderr: { write: () => true },
      env: {},
    });
    expect(code).toBe(0);
    expect(stdout).toContain(`${raised.id}  open  question  holder=supervisor:${lead.id} hop 1/3 chain=supervisor:${lead.id}>root>user`);

    const notices: string[] = [];
    await openFabricDecisions(mesh, {
      hasUI: false,
      ui: { notify: (message: string) => notices.push(message) },
    } as never);
    expect(notices[0]).toContain(`supervisor:${lead.id} (hop 1/3: supervisor:${lead.id} > root > user)`);
  });
});
