import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { DecisionStore, DECISIONS_TOPIC, MAX_OPEN_DECISIONS } from "../src/decisions/store.js";
import { DecisionsProvider } from "../src/providers/decisions-provider.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const roots: string[] = [];
const main: MeshIdentity = { id: "session:main", name: "main", kind: "main", sessionId: "main" };
const child: MeshIdentity = { id: "run-child", name: "child", kind: "agent", sessionId: "child" };

const meshAt = (root?: string): MeshStore => {
  const directory = root ?? fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-decisions-"));
  if (!root) roots.push(directory);
  return new MeshStore(directory, 64 * 1024, 500);
};

const context = (parentToolCallId = "call-1", signal?: AbortSignal): FabricInvocationContext => ({
  cwd: process.cwd(),
  signal,
  parentToolCallId,
  nestedToolCallId: `${parentToolCallId}:n`,
  extensionContext: {} as FabricInvocationContext["extensionContext"],
  update: () => {},
});

afterEach(() => {
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("DecisionStore", () => {
  it("raises, lists, and answers through open -> answered", async () => {
    const store = new DecisionStore(meshAt(), main);
    const raised = await store.raise({ title: "Ship it?", input: "confirm" });
    expect(raised).toMatchObject({ status: "open", holder: "user", kind: "question", input: "confirm" });
    expect(raised.options?.map((option) => option.id)).toEqual(["yes", "no"]);
    expect((await store.list({ status: "open" })).map((record) => record.id)).toEqual([raised.id]);
    const answered = await store.answer(raised.id, { optionId: "yes" }, { answeredBy: "alice", via: "cli" });
    expect(answered).toMatchObject({ status: "answered", answer: { optionId: "yes", answeredBy: "alice", via: "cli" } });
    await expect(store.answer(raised.id, { optionId: "no" }, { answeredBy: "bob", via: "cli" }))
      .rejects.toThrow(/answered, not open/);
    await expect(store.cancel(raised.id, { answeredBy: "bob", via: "cli" })).rejects.toThrow(/not open/);
    const events = store.mesh.read({ topic: DECISIONS_TOPIC });
    expect(events.map((event) => event.kind)).toEqual(["decision.raised", "decision.answered"]);
  });

  it("lets exactly one of two racing answerers win across store instances", async () => {
    const mesh = meshAt();
    const first = new DecisionStore(mesh, main);
    const second = new DecisionStore(new MeshStore(mesh.root, 64 * 1024, 500), main);
    const raised = await first.raise({ title: "Pick", options: [{ id: "a", label: "A" }, { id: "b", label: "B" }] });
    const results = await Promise.allSettled([
      first.answer(raised.id, { optionId: "a" }, { answeredBy: "one", via: "tui" }),
      second.answer(raised.id, { optionId: "b" }, { answeredBy: "two", via: "cli" }),
    ]);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
    const final = await first.get(raised.id);
    const winner = results.find((result) => result.status === "fulfilled") as PromiseFulfilledResult<{ answer: { optionId: string } }>;
    expect(final?.answer?.optionId).toBe(winner.value.answer.optionId);
  });

  it("validates answers against options and input kind", async () => {
    const store = new DecisionStore(meshAt(), main);
    const select = await store.raise({ title: "Pick", options: [{ id: "a", label: "A" }] });
    await expect(store.answer(select.id, { optionId: "zzz" }, { answeredBy: "u", via: "cli" })).rejects.toThrow(/needs optionId/);
    await expect(store.answer(select.id, { text: "free" }, { answeredBy: "u", via: "cli" })).rejects.toThrow(/needs optionId/);
    const text = await store.raise({ title: "Why?" });
    await expect(store.answer(text.id, {}, { answeredBy: "u", via: "cli" })).rejects.toThrow(/text answer/);
    await expect(store.answer(text.id, { optionId: "a" }, { answeredBy: "u", via: "cli" })).rejects.toThrow(/no options/);
    expect((await store.answer(text.id, { text: "because" }, { answeredBy: "u", via: "cli" })).answer?.text).toBe("because");
  });

  it("rejects malformed raises (fail closed)", async () => {
    const store = new DecisionStore(meshAt(), main);
    await expect(store.raise({ title: "" })).rejects.toThrow(/title/);
    await expect(store.raise({ title: "x".repeat(201) })).rejects.toThrow(/200/);
    await expect(store.raise({ title: "t", body: "x".repeat(4_001) })).rejects.toThrow(/4000/);
    await expect(store.raise({ title: "t", input: "select" })).rejects.toThrow(/requires options/);
    await expect(store.raise({ title: "t", options: Array.from({ length: 13 }, (_, i) => ({ id: `o${i}`, label: "x" })) }))
      .rejects.toThrow(/1\.\.12/);
    await expect(store.raise({ title: "t", options: [{ id: "a", label: "A" }, { id: "a", label: "B" }] })).rejects.toThrow(/Duplicate/);
    await expect(store.raise({ title: "t", holder: "admin" })).rejects.toThrow(/holder/);
    await expect(store.raise({ title: "t", deadline: Date.now() - 1 })).rejects.toThrow(/future/);
    await expect(store.raise({ title: "t", timeoutMs: 31 * 86_400_000 })).rejects.toThrow(/timeoutMs/);
    await expect(store.raise({ title: "t", options: [{ id: "a", label: "A" }], onExpire: "default", defaultOptionId: "a" }))
      .rejects.toThrow(/requires defaultOptionId and a deadline/);
    await expect(store.raise({ title: "t", options: [{ id: "a", label: "A" }], defaultOptionId: "b" })).rejects.toThrow(/defaultOptionId/);
    expect(await store.list({ status: "open" })).toEqual([]);
  });

  it("expires lazily on read, to cancel or to the default option", async () => {
    let now = 1_000_000;
    const store = new DecisionStore(meshAt(), main, () => now);
    const cancel = await store.raise({ title: "Soon", timeoutMs: 1_000 });
    const fallback = await store.raise({
      title: "Default",
      options: [{ id: "keep", label: "Keep" }, { id: "drop", label: "Drop" }],
      timeoutMs: 2_000,
      onExpire: "default",
      defaultOptionId: "keep",
    });
    now += 1_500;
    expect((await store.get(cancel.id))?.status).toBe("expired");
    expect((await store.get(cancel.id))?.answer).toBeUndefined();
    expect((await store.get(fallback.id))?.status).toBe("open");
    now += 1_000;
    const listed = await store.list({ status: "expired" });
    expect(listed.map((record) => record.id).sort()).toEqual([cancel.id, fallback.id].sort());
    expect(listed.find((record) => record.id === fallback.id)?.answer).toMatchObject({ optionId: "keep", via: "expiry" });
    await expect(store.answer(fallback.id, { optionId: "drop" }, { answeredBy: "late", via: "cli" })).rejects.toThrow(/expired/);
  });

  it("wait resolves on answer, returns the open record on timeout, and honours abort", async () => {
    const store = new DecisionStore(meshAt(), main);
    const raised = await store.raise({ title: "Wait for me" });
    const timedOut = await store.wait(raised.id, { timeoutMs: 30, pollMs: 10 });
    expect(timedOut.status).toBe("open");
    const waiting = store.wait(raised.id, { pollMs: 10 });
    setTimeout(() => void store.answer(raised.id, { text: "done" }, { answeredBy: "u", via: "cli" }), 30);
    expect((await waiting).answer?.text).toBe("done");
    const other = await store.raise({ title: "Abort me" });
    const controller = new AbortController();
    const aborted = store.wait(other.id, { signal: controller.signal, pollMs: 10 });
    controller.abort(new Error("stop"));
    await expect(aborted).rejects.toThrow("stop");
  });

  it("bounds the number of open decisions", async () => {
    const store = new DecisionStore(meshAt(), main);
    for (let index = 0; index < MAX_OPEN_DECISIONS; index += 1) await store.raise({ title: `q${index}` });
    await expect(store.raise({ title: "one too many" })).rejects.toThrow(/Too many open decisions/);
  }, 30_000);
});

describe("DecisionsProvider", () => {
  it("refuses self-answer within the same program and user-held answers", async () => {
    const mesh = meshAt();
    const provider = new DecisionsProvider(new DecisionStore(mesh, main), main);
    const { id } = await provider.invoke("raise", { title: "Approve?", holder: "root", input: "confirm" }, context("call-1")) as { id: string };
    await expect(provider.invoke("answer", { id, optionId: "yes" }, context("call-1"))).rejects.toThrow(/same program/);
    const answered = await provider.invoke("answer", { id, optionId: "yes" }, context("call-2"));
    expect(answered).toMatchObject({ status: "answered", answer: { via: "program", answeredBy: main.id } });

    const userHeld = await provider.invoke("raise", { title: "Human only", input: "confirm" }, context("call-3")) as { id: string };
    await expect(provider.invoke("answer", { id: userHeld.id, optionId: "yes" }, context("call-4"))).rejects.toThrow(/held by the user/);
  });

  it("enforces root and supervisor holders", async () => {
    const mesh = meshAt();
    const rootProvider = new DecisionsProvider(new DecisionStore(mesh, main), main);
    const childProvider = new DecisionsProvider(new DecisionStore(mesh, child), child);
    const rootHeld = await rootProvider.invoke("raise", { title: "Root", holder: "root" }, context("a")) as { id: string };
    await expect(childProvider.invoke("answer", { id: rootHeld.id, text: "x" }, context("b"))).rejects.toThrow(/held by root/);
    const supervised = await rootProvider.invoke("raise", { title: "Sup", holder: `supervisor:${child.id}` }, context("c")) as { id: string };
    expect(await childProvider.invoke("answer", { id: supervised.id, text: "ok" }, context("d"))).toMatchObject({ status: "answered" });
    // The raiser may always cancel its own decision.
    const own = await childProvider.invoke("raise", { title: "Mine" }, context("e")) as { id: string };
    expect(await childProvider.invoke("cancel", { id: own.id }, context("e"))).toMatchObject({ status: "cancelled" });
    await expect(childProvider.invoke("cancel", { id: rootHeld.id }, context("f"))).rejects.toThrow(/held by root/);
  });

  it("validates arguments and lists by status", async () => {
    const provider = new DecisionsProvider(new DecisionStore(meshAt(), main), main);
    await expect(provider.invoke("raise", { title: "t", extra: 1 }, context())).rejects.toThrow(/Invalid decisions.raise/);
    await expect(provider.invoke("answer", { id: "nope" }, context())).rejects.toThrow(/Invalid decision id|Invalid decisions.answer/);
    await provider.invoke("raise", { title: "listed" }, context());
    expect(await provider.invoke("list", { status: "open" }, context())).toHaveLength(1);
    expect(await provider.invoke("list", { status: "answered" }, context())).toHaveLength(0);
  });

  it("keeps decision records out of reach of mesh.put/delete", async () => {
    const mesh = meshAt();
    const meshProvider = new MeshProvider(mesh, main, { list: () => [] } as never);
    await expect(meshProvider.invoke("put", { key: "decisions/dec_forged0000", value: {} }, context()))
      .rejects.toThrow(/reserved/);
    await expect(meshProvider.invoke("delete", { key: "decisions/dec_forged0000" }, context()))
      .rejects.toThrow(/reserved/);
  });
});
