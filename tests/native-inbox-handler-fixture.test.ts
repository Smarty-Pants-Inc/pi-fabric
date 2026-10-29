import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
// @ts-expect-error -- plain .mjs fixture also loaded by future native resident compatibility tests.
import { createNativeInboxHandler, handlerKey } from "./fixtures/native-inbox-handler.mjs";

// Keep reason: application trust boundary and durable data safety (DESIGN N9/N10).
// These exercise the real on-disk SQLite transaction, not Fabric/NATS admission.
type Operation = "add" | "list" | "resolve";
type Item = { id: string; text: string; resolved: boolean };
type Reply = { v: 1; requestId: string; ok: boolean; status: string;
  result?: { item?: Item; items?: Item[] }; error?: { code: string } };
type Pin = { link: string; sourceMachine: string; senderId: string; seat: string };
type Context = { actorId: string; source: string; sender: { id: string; kind: string; sessionId: string };
  sourceMachine: string; link: string; commandId: string; sourceCommandId: string; sourceEventId: string;
  deadlineAt: number; signal: AbortSignal };
type Readback = { items: number; operations: number; addEffects: number; resolveEffects: number };
type Handler = ((request: unknown, context: unknown) => Promise<{ data: Reply }>)
  & { close(): void; readback(): Readback };
const create = createNativeInboxHandler as (options: { databasePath: string; seats: readonly Pin[] }) => Handler;
const ALICE: Pin = { link: "fixture-link", sourceMachine: "pod-alice",
  senderId: "session:342aaaaa-aaaa-4000-8000-000000000001", seat: "alice" };
const BOB: Pin = { link: "fixture-link", sourceMachine: "pod-bob",
  senderId: "session:342bbbbb-bbbb-4000-8000-000000000002", seat: "bob" };
const context = (pin = ALICE, signal = new AbortController().signal): Context => ({
  actorId: "34200000000000000000000000000001", source: "nats-bridge",
  sender: { id: pin.senderId, kind: "main", sessionId: pin.senderId.slice(8) },
  sourceMachine: pin.sourceMachine, link: pin.link, commandId: randomUUID(),
  sourceCommandId: randomUUID(), sourceEventId: randomUUID(), deadlineAt: Date.now() + 60_000, signal,
});
const request = (operation: Operation, opKey: string, args: unknown, requestId: string = randomUUID()) => ({
  message: handlerKey as string, data: { v: 1, requestId, operation, opKey, args },
});
async function call(h: Handler, pin: Pin, operation: Operation, opKey: string, args: unknown,
  requestId: string = randomUUID()): Promise<Reply> {
  return (await h(request(operation, opKey, args, requestId), context(pin))).data;
}
function returnedItem(reply: Reply): Item {
  expect(reply.ok).toBe(true);
  expect(reply.result?.item).toBeDefined();
  return reply.result!.item!;
}
const denied = (reply: Reply, requestId: string, status: string) => {
  expect(reply).toEqual({ v: 1, requestId, ok: false, status, error: { code: status } });
};
const roots: string[] = [];
const handles: Handler[] = [];
afterEach(() => {
  for (const h of handles.splice(0)) h.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function fixture(seats: readonly Pin[] = [ALICE, BOB]) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-inbox-fixture-"));
  roots.push(root);
  const databasePath = path.join(root, "inbox.db");
  const open = () => {
    const h = create({ databasePath, seats }); handles.push(h); return h;
  };
  return { databasePath, open, handler: open() };
}
const empty = { items: 0, operations: 0, addEffects: 0, resolveEffects: 0 };

// An independent SQLite reader observes persisted state without using fixture counters.
function observed(databasePath: string) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try {
    return {
      items: db.prepare("SELECT seat, text, resolved FROM fixture_items ORDER BY seat, text").all(),
      effects: db.prepare("SELECT seat, operation, COUNT(*) AS n FROM fixture_effects GROUP BY seat, operation ORDER BY seat, operation").all(),
      operations: db.prepare("SELECT COUNT(*) AS n FROM fixture_operations").get()!.n,
    };
  } finally { db.close(); }
}

describe("durable native inbox TEST handler", () => {
  it("lets two pinned seats independently add/list/resolve with equal cross-seat and cross-operation keys", async () => {
    const { handler: h, databasePath } = fixture();
    expect(handlerKey).toBe("smarty-inbox.rpc.v1");
    const a = returnedItem(await call(h, ALICE, "add", "shared", { text: "Alice private" }));
    const b = returnedItem(await call(h, BOB, "add", "shared", { text: "Bob private" }));
    expect(a.id).not.toBe(b.id);
    for (const [pin, own] of [[ALICE, a], [BOB, b]] as const) {
      const listed = await call(h, pin, "list", "shared", {});
      expect(listed).toMatchObject({ ok: true, status: "listed", result: { items: [own] } });
      expect(returnedItem(await call(h, pin, "resolve", "shared", { itemId: own.id }))).toEqual({ ...own, resolved: true });
      expect((await call(h, pin, "list", "fresh", {})).result).toEqual({ items: [{ ...own, resolved: true }] });
    }
    expect(h.readback()).toEqual({ items: 2, operations: 8, addEffects: 2, resolveEffects: 2 });
    expect(observed(databasePath)).toEqual({
      items: [{ seat: "alice", text: "Alice private", resolved: 1 }, { seat: "bob", text: "Bob private", resolved: 1 }],
      effects: [{ seat: "alice", operation: "add", n: 1 }, { seat: "alice", operation: "resolve", n: 1 },
        { seat: "bob", operation: "add", n: 1 }, { seat: "bob", operation: "resolve", n: 1 }], operations: 8,
    });
  });

  it("refuses guessed IDs before cached resolve lookup and never returns another seat's cached list/item", async () => {
    const { handler: h } = fixture();
    const a = returnedItem(await call(h, ALICE, "add", "shared-add", { text: "Alice secret" }));
    await call(h, ALICE, "resolve", "shared-resolve", { itemId: a.id });
    await call(h, ALICE, "list", "shared-list", {});
    expect((await call(h, BOB, "list", "shared-list", {})).result).toEqual({ items: [] });
    const before = h.readback();
    denied(await call(h, BOB, "resolve", "shared-resolve", { itemId: a.id }, "fish-alice-cache"), "fish-alice-cache", "not-found");
    denied(await call(h, BOB, "resolve", "shared-resolve", { itemId: randomUUID() }, "guess-missing"), "guess-missing", "not-found");
    expect(h.readback()).toEqual(before);
    const b = returnedItem(await call(h, BOB, "add", "shared-add", { text: "Bob secret" }));
    expect(b.id).not.toBe(a.id);
    await call(h, BOB, "resolve", "shared-resolve", { itemId: b.id });
    const afterOwnResolve = h.readback();
    // Even Bob's own successful cached key must not bypass ownership or reveal a conflict.
    denied(await call(h, BOB, "resolve", "shared-resolve", { itemId: a.id }, "fish-bob-cache"), "fish-bob-cache", "not-found");
    denied(await call(h, ALICE, "resolve", "shared-resolve", { itemId: b.id }, "fish-reverse"), "fish-reverse", "not-found");
    expect(h.readback()).toEqual(afterOwnResolve);
    expect((await call(h, BOB, "list", "shared-list", {})).result).toEqual({ items: [] }); // stable old Bob snapshot
    expect((await call(h, BOB, "list", "fresh", {})).result).toEqual({ items: [{ ...b, resolved: true }] });
  });

  it("coalesces parallel retries across two database connections into one durable effect, with current requestIds", async () => {
    const { handler: first, open, databasePath } = fixture();
    const second = open();
    const ids = Array.from({ length: 24 }, (_, i) => `parallel-request-${i}`);
    const replies = await Promise.all(ids.map((id, i) => call(i % 2 ? first : second,
      ALICE, "add", "one-business-key", { text: "One persisted item" }, id)));
    const stable = returnedItem(replies[0]!);
    replies.forEach((reply, i) => expect(reply).toEqual({
      v: 1, requestId: ids[i], ok: true, status: "added", result: { item: stable },
    }));
    expect(first.readback()).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
    expect(second.readback()).toEqual(first.readback());
    expect(observed(databasePath)).toEqual({ items: [{ seat: "alice", text: "One persisted item", resolved: 0 }],
      effects: [{ seat: "alice", operation: "add", n: 1 }], operations: 1 });
    first.close(); second.close();
    const reopened = open();
    expect(await call(reopened, ALICE, "add", "one-business-key", { text: "One persisted item" }, "reopened-retry")).toEqual({
      v: 1, requestId: "reopened-retry", ok: true, status: "added", result: { item: stable },
    });
    expect(reopened.readback()).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
  });

  it("preserves stable add/list/resolve results and effect counts after every connection closes and reopens", async () => {
    const { handler: first, open, databasePath } = fixture();
    const added = await call(first, ALICE, "add", "persist-add", { text: "Survives reopen" }, "old-add");
    const original = returnedItem(added);
    const listed = await call(first, ALICE, "list", "persist-list", {}, "old-list");
    const resolved = await call(first, ALICE, "resolve", "persist-resolve", { itemId: original.id }, "old-resolve");
    first.close();
    const h = open();
    const retry = await call(h, ALICE, "add", "persist-add", { text: "Survives reopen" }, "new-add");
    expect(retry).toEqual({ ...added, requestId: "new-add" }); // original unresolved result, not current row
    expect(await call(h, ALICE, "list", "persist-list", {}, "new-list")).toEqual({ ...listed, requestId: "new-list" });
    expect(await call(h, ALICE, "resolve", "persist-resolve", { itemId: original.id }, "new-resolve")).toEqual({ ...resolved, requestId: "new-resolve" });
    expect(h.readback()).toEqual({ items: 1, operations: 3, addEffects: 1, resolveEffects: 1 });
    expect(observed(databasePath)).toEqual({ items: [{ seat: "alice", text: "Survives reopen", resolved: 1 }],
      effects: [{ seat: "alice", operation: "add", n: 1 }, { seat: "alice", operation: "resolve", n: 1 }], operations: 3 });
    // A new key may acknowledge an already-resolved item, but cannot cause a second transition.
    expect(returnedItem(await call(h, ALICE, "resolve", "new-resolve-key", { itemId: original.id })).resolved).toBe(true);
    expect(h.readback()).toEqual({ items: 1, operations: 4, addEffects: 1, resolveEffects: 1 });
  });

  it("conflicts on different semantic args under the same key, including after reopen, without another mutation", async () => {
    const { handler: first, open } = fixture();
    const a = returnedItem(await call(first, ALICE, "add", "same-key", { text: "original" }));
    first.close();
    const h = open();
    denied(await call(h, ALICE, "add", "same-key", { text: "changed" }, "conflicting-add"), "conflicting-add", "conflict");
    // Envelope order, current requestId and new transport-command IDs are not semantic args.
    const reordered = await h({ data: { args: { text: "original" }, opKey: "same-key", operation: "add",
      requestId: "reordered", v: 1 }, message: handlerKey }, context());
    expect(reordered.data).toMatchObject({ requestId: "reordered", ok: true, result: { item: a } });
    expect(h.readback()).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
    const b = returnedItem(await call(h, ALICE, "add", "second-key", { text: "second" }));
    await call(h, ALICE, "resolve", "same-key", { itemId: a.id });
    const before = h.readback();
    denied(await call(h, ALICE, "resolve", "same-key", { itemId: b.id }, "conflicting-resolve"), "conflicting-resolve", "conflict");
    expect(h.readback()).toEqual(before);
    expect((await call(h, ALICE, "list", "fresh", {})).result?.items).toEqual(
      [a, b].map((value) => ({ ...value, resolved: value.id === a.id })).sort((x, y) => x.id < y.id ? -1 : 1));
  });

  it("rejects identity smuggling and malformed closed data/args without any application effects", async () => {
    const { handler: h } = fixture();
    const base = request("add", "forged", { text: "must never persist" }, "safe-correlation");
    for (const key of ["seat", "identity", "sender", "sourceMachine", "link", "principal", "from", "context", "handlerKey", "__proto__"]) {
      for (const bad of [
        { ...base, data: { ...base.data, [key]: BOB.seat } },
        { ...base, data: { ...base.data, args: { text: "must never persist", [key]: BOB.seat } } },
      ]) denied((await h(bad, context())).data, "safe-correlation", "invalid-request");
    }
    for (const patch of [{ v: 2 }, { operation: "read" }, { opKey: "" }, { args: null }, { args: [] },
      { args: { text: "" } }, { args: { text: 42 } }, { args: { text: "x".repeat(4097) } }]) {
      denied((await h({ ...base, data: { ...base.data, ...patch } }, context())).data, "safe-correlation", "invalid-request");
    }
    denied((await h({ ...base, message: "wrong.rpc" }, context())).data, "safe-correlation", "invalid-request");
    denied(await call(h, ALICE, "list", "forged-list", { seat: "bob" }, "bad-list"), "bad-list", "invalid-request");
    denied(await call(h, ALICE, "resolve", "forged-resolve", { itemId: randomUUID(), seat: "bob" }, "bad-resolve"), "bad-resolve", "invalid-request");
    expect(h.readback()).toEqual(empty);
  });

  it("refuses unknown, contradictory, malformed or payload-like contexts with zero effects", async () => {
    const { handler: h } = fixture();
    const good = context();
    const badContexts: unknown[] = [undefined, null, {},
      { ...good, source: "local" }, { ...good, link: "other-link" }, { ...good, sourceMachine: BOB.sourceMachine },
      { ...good, sender: { ...good.sender, id: BOB.senderId, sessionId: BOB.senderId.slice(8) } },
      { ...good, sender: { ...good.sender, kind: "agent" } },
      { ...good, sender: { ...good.sender, sessionId: BOB.senderId.slice(8) } },
      { ...good, sender: { ...good.sender, id: "session:untrusted" } },
      { ...good, sender: { ...good.sender, id: null } },
      { ...good, actorId: "not-pinned-actor-shape" }, { ...good, actorId: [good.actorId] }, { ...good, commandId: "" },
      { ...good, sourceCommandId: "" }, { ...good, sourceEventId: "" }, { ...good, deadlineAt: NaN },
      { ...good, signal: { aborted: false } }, { ...good, seat: "bob" }, { ...good, identity: BOB },
    ];
    for (const bad of badContexts) {
      denied((await h(request("add", "forged-context", { text: "no" }, "context-refused"), bad)).data, "context-refused", "unauthorized");
    }
    expect(h.readback()).toEqual(empty);
  });

  it("copies trusted seat pins and rejects duplicate identities or a non-durable database path", async () => {
    const pins = [{ ...ALICE }, { ...BOB }];
    const { handler: h, databasePath } = fixture(pins);
    pins[0]!.seat = "bob";
    pins[0]!.sourceMachine = "changed-after-open";
    const a = returnedItem(await call(h, ALICE, "add", "pin-copy", { text: "Still Alice" }));
    expect((await call(h, ALICE, "list", "alice-list", {})).result).toEqual({ items: [a] });
    expect((await call(h, BOB, "list", "bob-list", {})).result).toEqual({ items: [] });
    expect(() => create({ databasePath, seats: [ALICE, { ...ALICE, seat: "bob" }] })).toThrow("Duplicate inbox fixture identity");
    expect(() => create({ databasePath: ":memory:", seats: [ALICE] })).toThrow("Invalid inbox fixture configuration");
  });

  it("rejects pre-entry abort/expiry, but does not pretend a post-commit abort rolled back", async () => {
    const { handler: h } = fixture();
    const controller = new AbortController();
    controller.abort(new Error("private abort reason"));
    denied((await h(request("add", "abort", { text: "no" }, "aborted"), context(ALICE, controller.signal))).data, "aborted", "aborted");
    denied((await h(request("add", "expired", { text: "no" }, "expired"), { ...context(), deadlineAt: Date.now() - 1 })).data, "expired", "expired");
    expect(h.readback()).toEqual(empty);
    const afterCommit = new AbortController();
    const pendingObservation = h(request("add", "committed", { text: "Committed" }, "committed"), context(ALICE, afterCommit.signal));
    afterCommit.abort(); // synchronous SQLite commit is complete; caller has not awaited its reply yet
    expect(returnedItem((await pendingObservation).data).text).toBe("Committed");
    expect(h.readback()).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
    h.close();
    h.close();
    denied((await h(request("list", "closed", {}, "closed"), context())).data, "closed", "closed");
  });

  it("rolls back the actual item and trigger journal if saving the stable result fails", async () => {
    const { handler: h, databasePath } = fixture();
    const injector = new DatabaseSync(databasePath);
    try {
      injector.exec(`CREATE TRIGGER reject_saved_result BEFORE INSERT ON fixture_operations BEGIN
        SELECT RAISE(ABORT, 'private SQLite diagnostic must not escape'); END`);
      denied(await call(h, ALICE, "add", "atomic", { text: "Atomic" }, "failed-save"), "failed-save", "storage-error");
      expect(h.readback()).toEqual(empty);
      expect(observed(databasePath)).toEqual({ items: [], effects: [], operations: 0 });
      injector.exec("DROP TRIGGER reject_saved_result");
      const retry = await call(h, ALICE, "add", "atomic", { text: "Atomic" }, "retry-save");
      expect(returnedItem(retry).text).toBe("Atomic");
      expect(h.readback()).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
    } finally { injector.close(); }
  });

  it("uses a generic safe error only when no usable requestId is available", async () => {
    const { handler: h } = fixture();
    for (const value of [undefined, null, "", " ", 7, {}, "x".repeat(257)]) {
      const bad = { ...request("add", "no-id", { text: "private" }), data: { v: 1, requestId: value } };
      await expect(h(bad, context())).rejects.toThrow(/^Invalid inbox fixture request$/);
    }
    await expect(h(null, context())).rejects.toThrow(/^Invalid inbox fixture request$/);
    expect(h.readback()).toEqual(empty);
  });
});
