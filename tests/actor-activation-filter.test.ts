import fs from "node:fs";
import os from "node:os";
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import * as predicate from "../src/actors/predicate.js";
import {
  ACTOR_ACTIVATION_GENERATION,
  normalizeStoredActorActivationReservation,
  type ActorActivationFilterReservationRequest,
  activationFilterSkip,
  normalizeActorActivationFilter,
  normalizeActorActivationReservation,
  normalizeActorActivationObservation,
  activationPrInvalidReason,
  ACTOR_ACTIVATION_RESERVATION_MAX_TTL_MS,
  type ActorActivationFilterReservation,
  type ActorActivationFilterObservation,
  type FabricActorActivationFilter,
} from "../src/actors/activation-filter.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Real queued-item envelopes from fleet run records (smarty-dev#1579 item 4).
const fixture = JSON.parse(fs.readFileSync(path.resolve("tests/fixtures/activation-filter-envelopes.json"), "utf8")) as {
  holdLabel: Record<string, unknown>;
  envelopes: Record<string, { source: string; payload: Record<string, any> }>;
};
const envelope = (name: string) => structuredClone(fixture.envelopes[name]!);
const hold = fixture.holdLabel;
const bug = { ...hold, id: 1, node_id: "LA_bug", name: "bug", color: "d73a4a", description: "Something is wrong" };

// The GitHub webhook body carries the item's labels; the ingress projection may add them.
const withLabels = (name: string, labels: unknown[], carrier: "issue" | "pull_request" = "issue") => {
  const item = envelope(name);
  item.payload.data.payload[carrier] = { number: item.payload.data.payload.number, labels };
  return item;
};
const unlabel = (label: Record<string, unknown>, labels: unknown[]) => {
  const item = withLabels("issues.unlabeled", labels);
  item.payload.data.payload.label = label;
  return item;
};
const skip = (filter: FabricActorActivationFilter, item: { source: string; payload: unknown }) =>
  activationFilterSkip(normalizeActorActivationFilter(filter), item.source, item.payload);

const BOTH: FabricActorActivationFilter = ["hold", "never-message-events"];

describe("activation filter rules on real envelopes", () => {
  it("keeps absent filter telemetry absent after a current-host reload for rollback", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "untouched", instructions: "x", topics: ["github.demo"] });
    const registryPath = path.join(first.root, "actors", "actors.json");
    const before = JSON.parse(fs.readFileSync(registryPath, "utf8")) as { actors: Array<Record<string, unknown>> };
    const beforeRow = before.actors.find(row => row.id === actor.id)!;
    expect(beforeRow).not.toHaveProperty("filterSkipped");
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();

    const second = setup(first.root);
    expect(second.actors.status(actor.id).filterSkipped).toEqual({ count: 0, lastKey: null, lastTopic: null, lastAt: null });
    await waitFor(() => {
      const row = (JSON.parse(fs.readFileSync(registryPath, "utf8")) as { actors: Array<Record<string, unknown>> }).actors.find(value => value.id === actor.id);
      return row !== undefined && typeof row.updatedAt === "number" && row.updatedAt > (beforeRow.updatedAt as number);
    });
    const after = JSON.parse(fs.readFileSync(registryPath, "utf8")) as { actors: Array<Record<string, unknown>> };
    const afterRow = after.actors.find(row => row.id === actor.id)!;
    expect(afterRow).not.toHaveProperty("filterSkipped");
    const stable = (row: Record<string, unknown>) => {
      const { status: _status, updatedAt: _updatedAt, messages: _messages, ...settings } = row;
      return settings;
    };
    expect(stable(afterRow)).toEqual(stable(beforeRow));
  });
  it("R7 never-message-events skips the five observed silent event types", () => {
    expect(skip(BOTH, envelope("issues.field_added"))).toBe("never-message-events/issues.field_added");
    expect(skip(BOTH, envelope("issues.typed"))).toBe("never-message-events/issues.typed");
    expect(skip(BOTH, envelope("issue_comment.deleted"))).toBe("never-message-events/issue_comment.deleted");
    expect(skip(BOTH, envelope("host:tool_error"))).toBe("never-message-events/host:tool_error");
    expect(skip(BOTH, envelope("ops.owner:actions.minutes"))).toBe("never-message-events/ops.owner:actions.minutes");
  });

  it("R7 delivers the other event types: a comment, an unlabel, another owner notice", () => {
    for (const name of ["issue_comment.created", "issues.unlabeled", "ops.owner:idle.wait"]) {
      expect(skip(BOTH, envelope(name)), name).toBeUndefined();
    }
    // The same action on another event type is not the rule's event: a deleted issue, a typed comment.
    const deletedIssue = envelope("issue_comment.deleted");
    deletedIssue.payload.data.event = "issues";
    expect(skip(BOTH, deletedIssue)).toBeUndefined();
    // actions.minutes on another topic, and a tool_error published on the mesh, are not the rule's.
    const otherTopic = envelope("ops.owner:actions.minutes");
    otherTopic.payload.topic = "ops.org-ask";
    expect(skip(BOTH, { source: "mesh:ops.org-ask", payload: otherTopic.payload })).toBeUndefined();
    expect(skip(BOTH, { source: "mesh:tool_error", payload: { topic: "tool_error", kind: "tool_error" } })).toBeUndefined();
  });

  it("R1 hold skips a GitHub event on an item labelled hold, from issue or pull_request labels", () => {
    expect(skip(["hold"], withLabels("issue_comment.created", [bug, hold]))).toBe("hold");
    expect(skip(["hold"], withLabels("issue_comment.created", [hold], "pull_request"))).toBe("hold");
    expect(skip(["hold"], withLabels("issues.field_added", [hold]))).toBe("hold");
    // Unlabelling another label while hold stays is still held.
    expect(skip(["hold"], unlabel(bug, [hold]))).toBe("hold");
  });

  it("R1 always delivers the event that removes hold", () => {
    // GitHub sends the labels after the change; a stale or duplicated list must not hide the wake.
    expect(skip(BOTH, unlabel(hold, []))).toBeUndefined();
    expect(skip(BOTH, unlabel(hold, [hold, bug]))).toBeUndefined();
  });

  // review/astra F1 on #106: a missing field in the exception must not turn into a skip.
  it("R1 delivers an unlabel event whose label, label name or action is missing (unsure means deliver)", () => {
    const noLabel = withLabels("issues.unlabeled", [hold]);                // stale list, which label unknown
    expect(noLabel.payload.data.payload.label).toBeUndefined();
    expect(skip(BOTH, noLabel)).toBeUndefined();
    expect(skip(BOTH, unlabel({ id: 1, color: "B60205" }, [hold]))).toBeUndefined();   // label without a name
    const noAction = withLabels("issue_comment.created", [hold]);
    delete noAction.payload.data.payload.action;
    expect(skip(BOTH, noAction)).toBeUndefined();
    // Counterexamples the fix must keep: a known other action or a known other label rules the
    // exception out, though a comment carries no label field.
    expect(skip(BOTH, withLabels("issue_comment.created", [hold]))).toBe("hold");
    expect(skip(BOTH, unlabel(bug, [hold]))).toBe("hold");
  });

  it("custom unless: skips only when some exception predicate is known false", () => {
    const filter: FabricActorActivationFilter = [{
      id: "quiet", topic: ["ops.owner"],
      unless: [{ path: "data.urgent", equals: true }, { path: "data.key", in: ["a", "b"] }],
    }];
    const owner = (data: Record<string, unknown>) => ({ source: "mesh:ops.owner", payload: { topic: "ops.owner", kind: "x", data } });
    expect(skip(filter, owner({ urgent: false, key: "a" }))).toBe("quiet");        // urgent known false
    expect(skip(filter, owner({ urgent: true, key: "c" }))).toBe("quiet");         // key known not in the list
    expect(skip(filter, owner({ urgent: true, key: "a" }))).toBeUndefined();       // the exception holds
    expect(skip(filter, owner({ urgent: true }))).toBeUndefined();                 // key missing: unsure
    expect(skip(filter, owner({}))).toBeUndefined();                               // both missing
    expect(skip(filter, owner({ key: "z" }))).toBe("quiet");                       // key known false is enough
    // A missing where field is not a match either.
    const where: FabricActorActivationFilter = [{ id: "w", topic: ["ops.owner"], where: [{ path: "data.key", equals: "a" }] }];
    expect(skip(where, owner({}))).toBeUndefined();
    expect(skip(where, owner({ key: "a" }))).toBe("w");
  });

  it("R1 delivers when the labels field is missing (the projected payload today) or has no hold", () => {
    const projected = envelope("issue_comment.created");
    expect(projected.payload.data.payload.issue).toBeUndefined();
    expect(skip(["hold"], projected)).toBeUndefined();
    expect(skip(["hold"], withLabels("issue_comment.created", []))).toBeUndefined();
    expect(skip(["hold"], withLabels("issue_comment.created", [bug]))).toBeUndefined();
    // A hold label on a non-GitHub topic is not a GitHub event.
    const owner = envelope("ops.owner:idle.wait");
    owner.payload.data.payload = { issue: { labels: [hold] } };
    expect(skip(["hold"], owner)).toBeUndefined();
    // No filter, or an empty one, delivers everything.
    expect(activationFilterSkip(undefined, "host:tool_error", {})).toBeUndefined();
    expect(activationFilterSkip([], "host:tool_error", {})).toBeUndefined();
  });

  it("custom rules: a missing field never matches; in and exists; source prefixes", () => {
    const filter: FabricActorActivationFilter = [{
      id: "bot-edit", topic: ["github.*"], where: [
        { path: "data.payload.action", in: ["edited", "typed"] },
        { path: "data.payload.sender.login", equals: "smarty-fleet-write[bot]" },
      ],
    }];
    expect(skip(filter, envelope("issues.typed"))).toBe("bot-edit");
    expect(skip(filter, envelope("issue_comment.deleted"))).toBeUndefined();          // no sender field
    const exists: FabricActorActivationFilter = [{ id: "snap", source: ["mesh:github.*"], where: [{ path: "data.payload.issueSnapshot", exists: true }] }];
    expect(skip(exists, envelope("issues.typed"))).toBe("snap");
    expect(skip(exists, envelope("issue_comment.created"))).toBeUndefined();
  });

  // smarty-dev#2004: the factory projection carries label names, the author and the Owner line.
  it("hold reads projected label names; owned set and bots off the allow list", () => {
    const projected = (payload: Record<string, unknown>) => {
      const item = envelope("issue_comment.created");
      Object.assign(item.payload.data.payload, payload);
      return item;
    };
    expect(skip(["hold"], projected({ labels: ["bug", "hold"] }))).toBe("hold");
    expect(skip(["hold"], projected({ labels: ["bug"] }))).toBeUndefined();
    expect(skip(["hold"], projected({ labels: [] }))).toBeUndefined();
    const owned: FabricActorActivationFilter = [{ id: "not-owned", topic: ["github.*"], unless: [{ path: "data.payload.owners", in: ["fabric-v2"] }] }];
    expect(skip(owned, projected({ owners: ["dev-lead"] }))).toBe("not-owned");
    expect(skip(owned, projected({ owners: ["other", "fabric-v2"] }))).toBeUndefined();
    expect(skip(owned, projected({}))).toBeUndefined();                               // no Owner line: unsure
    const bots: FabricActorActivationFilter = [{
      id: "bot-not-allowed", topic: ["github.*"], where: [{ path: "data.payload.author.type", equals: "Bot" }],
      unless: [{ path: "data.payload.author.login", in: ["smarty-fleet-write[bot]"] }],
    }];
    expect(skip(bots, projected({ author: { login: "mergify[bot]", type: "Bot" } }))).toBe("bot-not-allowed");
    expect(skip(bots, projected({ author: { login: "smarty-fleet-write[bot]", type: "Bot" } }))).toBeUndefined();
    expect(skip(bots, projected({ author: { login: "paul", type: "User" } }))).toBeUndefined();
    expect(skip(bots, projected({ author: { type: "Bot" } }))).toBeUndefined();        // login missing: unsure
  });
});

describe("activation filter validation", () => {
  const invalid: Array<[string, unknown]> = [
    ["not a list", "hold"],
    ["unknown preset", ["holds"]],
    ["a rule that names nothing", [{ id: "all" }]],
    ["a wildcard-only rule", [{ id: "all", topic: ["*"] }]],
    ["exists false", [{ id: "x", topic: ["a"], where: [{ path: "data.x", exists: false }] }]],
    ["two operators", [{ id: "x", topic: ["a"], where: [{ path: "data.x", equals: 1, in: [1] }] }]],
    ["no operator", [{ id: "x", topic: ["a"], where: [{ path: "data.x" }] }]],
    ["a bad path", [{ id: "x", topic: ["a"], where: [{ path: "data..x", equals: 1 }] }]],
    ["an object value", [{ id: "x", topic: ["a"], where: [{ path: "data.x", equals: { a: 1 } }] }]],
    ["an unknown field", [{ id: "x", topic: ["a"], reply: "hi" }]],
    ["a bad id", [{ id: "", topic: ["a"] }]],
    ["an empty topic list", [{ id: "x", topic: [] }]],
    ["a duplicate id", [{ id: "hold", topic: ["a"] }, "hold"]],
  ];
  it.each(invalid)("rejects %s", (_name, value) => {
    expect(() => normalizeActorActivationFilter(value)).toThrow(/activationFilter/);
  });
});

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-activation-filter-")),
  actorOptions: NonNullable<ConstructorParameters<typeof ActorManager>[6]> = {}, maxConcurrent = DEFAULT_FABRIC_CONFIG.agents.maxConcurrent) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, ...actorOptions,
  });
  closers.push(async () => { await actors.close(); await agents.close(); });
  return { root, mesh, agents, actors };
};
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
// The fake worker writes one directory per spawned run: the proof of a model run.
const runDirs = (root: string, actorId: string) => {
  const dir = path.join(root, "actors", actorId, "runs");
  return fs.existsSync(dir) ? fs.readdirSync(dir) : [];
};
const runTasks = (root: string, actorId: string) =>
  runDirs(root, actorId).map((run) => path.join(root, "actors", actorId, "runs", run, "task.txt"))
    .filter((file) => fs.existsSync(file))
    .map((file) => ({ file, mtime: fs.statSync(file).mtimeMs, text: fs.readFileSync(file, "utf8") }))
    .sort((a, b) => a.mtime - b.mtime).map((task) => task.text);
// Publish a real envelope's event on a test topic (a real run record's topic is per repository).
const publish = (mesh: MeshStore, item: { payload: Record<string, any> }, topic = item.payload.topic) =>
  mesh.publish({ topic, kind: item.payload.kind, from, ...(item.payload.text ? { text: item.payload.text } : {}), data: item.payload.data });
const filtered = (actors: ActorManager, id: string) =>
  actors.messages(id).filter((message) => message.reason?.startsWith("filtered: "));

describe("actor activation filter in ActorManager", () => {
  it("skips matching events with no model run, logs and counts each skip, and runs the rest", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({
      name: "supervisor", instructions: "Supervise.", topics: ["github.demo", "ops.owner"], events: ["tool_error"],
      responseMode: "directive", coalesce: false, activationFilter: BOTH,
    });
    expect(actor.activationFilter).toEqual(BOTH);
    await publish(mesh, withLabels("issue_comment.created", [hold]), "github.demo");
    await publish(mesh, unlabel(hold, []), "github.demo");
    await publish(mesh, envelope("issues.typed"), "github.demo");
    await publish(mesh, envelope("ops.owner:actions.minutes"));
    actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
    await publish(mesh, envelope("issue_comment.created"), "github.demo");
    await waitFor(() => filtered(actors, actor.id).length === 4 && actors.messages(actor.id).filter((m) => m.direction === "out").length === 2);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(runDirs(root, actor.id)).toHaveLength(2);                      // the unlabel and the comment
    const tasks = runTasks(root, actor.id);
    expect(tasks[0]).toContain('"action": "unlabeled"');
    expect(tasks[1]).toContain('"action": "created"');
    // The host event is queued at once; mesh events arrive on the next poll.
    expect(filtered(actors, actor.id).map((m) => [m.direction, m.source, m.reason])).toEqual([
      ["in", "host:tool_error", "filtered: never-message-events/host:tool_error"],
      ["in", "mesh:github.demo", "filtered: hold"],
      ["in", "mesh:github.demo", "filtered: never-message-events/issues.typed"],
      ["in", "mesh:ops.owner", "filtered: never-message-events/ops.owner:actions.minutes"],
    ]);
    const status = actors.status(actor.id);
    expect(status.filterSkipped).toMatchObject({ count: 4, lastTopic: "ops.owner", lastKey: expect.any(String), lastAt: status.lastFilteredAt });
    expect(status.filteredCount).toBe(4);
    expect(status.lastFilteredAt).toBeGreaterThan(0);
    expect(status.queued).toBe(0);
  }, 30_000);

  it("never filters a caller's direct message, and keeps coalescing and queue order", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({
      name: "reviewer", instructions: "Review.", topics: ["github.demo"], coalesce: false,
      coalesceKey: "payload.number", activationFilter: ["hold"],
    });
    await mesh.publish({ topic: "github.demo", from, text: "LIVE_WITH_PROGRESS" });           // keeps the actor busy
    await waitFor(() => actors.status(actor.id).status === "running");
    const event = (number: number, marker: string, labels: unknown[]) =>
      mesh.publish({ topic: "github.demo", kind: "github.webhook", from, data: {
        event: "issue_comment", payload: { action: "created", number, marker, issue: { number, labels } },
      } });
    await event(1, "one-a", []);
    await event(2, "two-held", [hold]);
    await event(3, "three", []);
    await event(1, "one-b", [bug]);                    // replaces one-a in its place
    await event(2, "two-released", []);                // the held one never queued: the release queues
    await event(4, "four-held", [hold]);
    await waitFor(() => actors.status(actor.id).queued === 3);
    await waitFor(() => actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0, 20_000);
    const markers = runTasks(root, actor.id).map((task) => task.match(/"marker": "([^"]+)"/)?.[1]).filter(Boolean);
    expect(markers).toEqual(["one-b", "three", "two-released"]);
    expect(filtered(actors, actor.id).map((m) => m.reason)).toEqual(["filtered: hold", "filtered: hold"]);
    expect(actors.status(actor.id).filterSkipped).toMatchObject({ count: 2, lastTopic: "github.demo", lastKey: JSON.stringify(["mesh", "github.demo", 4]) });
    expect(actors.status(actor.id).filteredCount).toBe(2);
    // A direct message is never filtered, even by a rule its fields match.
    await actors.setActivationFilter(actor.id, [{ id: "any-message", where: [{ path: "message", exists: true }] }]);
    expect(actors.status(actor.id).filterSkipped).toEqual({ count: 0, lastKey: null, lastTopic: null, lastAt: null });
    const runs = runDirs(root, actor.id).length;
    actors.tell(actor.id, "direct work");
    await waitFor(() => runDirs(root, actor.id).length === runs + 1);
    expect(filtered(actors, actor.id)).toHaveLength(2);
  }, 40_000);

  // smarty-dev#2004: a skipped event must not replace a queued one by coalescing and take it along.
  it("filters on arrival: an edit never replaces the queued comment it would skip", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({
      name: "supervisor", instructions: "Supervise.", topics: ["github.demo"], coalesce: false,
      coalesceKey: "payload.number", activationFilter: [{ id: "edited", topic: ["github.*"], where: [{ path: "data.payload.action", equals: "edited" }] }],
    });
    await mesh.publish({ topic: "github.demo", from, text: "LIVE_WITH_PROGRESS" });           // keeps the actor busy
    await waitFor(() => actors.status(actor.id).status === "running");
    const comment = (number: number, action: string, marker: string) =>
      mesh.publish({ topic: "github.demo", kind: "github.webhook", from, data: { event: "issue_comment", payload: { action, number, marker } } });
    await comment(7, "created", "seven-created");
    await comment(7, "edited", "seven-edited");        // the reported sequence: skipped, the creation stays
    await comment(8, "created", "eight-a");
    await comment(8, "created", "eight-b");            // counterexample: a delivered event still coalesces
    await waitFor(() => filtered(actors, actor.id).length === 1 && actors.status(actor.id).queued === 2);
    await waitFor(() => actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0, 20_000);
    const markers = runTasks(root, actor.id).map((task) => task.match(/"marker": "([^"]+)"/)?.[1]).filter(Boolean);
    expect(markers).toEqual(["seven-created", "eight-b"]);
    expect(filtered(actors, actor.id).map((m) => [m.source, m.reason])).toEqual([["mesh:github.demo", "filtered: edited"]]);
    expect(actors.status(actor.id).filteredCount).toBe(1);
  }, 40_000);

  it("rejects an invalid filter at create and set time, and sets, persists and clears a valid one", async () => {
    const { root, mesh, agents, actors } = setup();
    await expect(actors.create({ name: "bad", instructions: "x", topics: ["a"], activationFilter: ["nope" as never] }))
      .rejects.toThrow(/Unknown activationFilter preset: nope/);
    expect(actors.list().find((actor) => actor.name === "bad")).toBeUndefined();
    const actor = await actors.create({ name: "watcher", instructions: "Watch.", topics: ["github.demo"], coalesce: false });
    expect(actor.activationFilter).toBeUndefined();
    await expect(actors.setActivationFilter(actor.id, [{ id: "all" } as never])).rejects.toThrow(/name a source, topic, kind or where/);
    expect(actors.status(actor.id).activationFilter).toBeUndefined();
    const set = await actors.setActivationFilter(actor.id, ["never-message-events"]);
    expect(set.activationFilter).toEqual(["never-message-events"]);
    const rejected = await publish(mesh, envelope("issues.typed"), "github.demo");
    await waitFor(() => filtered(actors, actor.id).length === 1);
    expect(runDirs(root, actor.id)).toHaveLength(0);
    const skipped = actors.status(actor.id).filterSkipped;
    expect(skipped).toMatchObject({ count: 1, lastTopic: "github.demo", lastKey: rejected.id, lastAt: expect.any(Number) });
    // A restart keeps the filter and the count.
    await actors.close();
    closers.length = 0;
    await agents.close();
    const again = setup(root).actors;
    const reloaded = again.status(actor.id);
    expect(reloaded.activationFilter).toEqual(["never-message-events"]);
    expect(reloaded.filteredCount).toBe(1);
    expect(reloaded.filterSkipped).toEqual(skipped);
    const cleared = await again.setActivationFilter(actor.id, null);
    expect(cleared.activationFilter).toBeUndefined();
    expect(cleared.filteredCount).toBe(1);
    expect(cleared.filterSkipped).toEqual({ count: 0, lastKey: null, lastTopic: null, lastAt: null });
    expect(again.messages(actor.id)).toContainEqual(expect.objectContaining({ reason: "activationFilter cleared: explicit" }));
  }, 30_000);

  it("coalesces a burst of host skips into one soft registry write without fsync", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "burst", instructions: "x", events: ["tool_error"], activationFilter: BOTH });
    const writes = vi.spyOn(ActorRegistryStore.prototype, "write");
    const syncs = vi.spyOn(fs, "fsyncSync");
    try {
      for (let i = 0; i < 30; i++) actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
      expect(actors.status(actor.id).filterSkipped).toMatchObject({ count: 30, lastKey: "host:tool_error", lastTopic: "tool_error" });
      expect(writes).not.toHaveBeenCalled();
      await waitFor(() => {
        const record = JSON.parse(fs.readFileSync(path.join(root, "actors/actors.json"), "utf8")).actors.find((a: { id: string }) => a.id === actor.id);
        return record.filterSkipped?.count === 30;
      });
      expect(writes).toHaveBeenCalledTimes(1);
      expect(writes.mock.calls[0]?.[1]).toMatchObject({ durable: false });
      expect(syncs).not.toHaveBeenCalled();
    } finally { writes.mockRestore(); syncs.mockRestore(); }
  });

  it("expires on the next host event, resets telemetry, and audits the clear", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "expiry", instructions: "x", events: ["tool_error"] });
    await actors.setActivationFilter(actor.id, BOTH);
    actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
    expect(actors.status(actor.id).filterSkipped.count).toBe(1);
    await actors.setActivationFilter(actor.id, BOTH, undefined, Date.now() - 1);
    actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
    expect(actors.status(actor.id).activationFilter).toBeUndefined();
    expect(actors.status(actor.id).filterSkipped).toEqual({ count: 0, lastKey: null, lastTopic: null, lastAt: null });
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ reason: "activationFilter cleared: expired" }));
  });

  it("persists expiry across a restart and clears on an idle poll without new events", async () => {
    const { root, actors, agents } = setup();
    const actor = await actors.create({ name: "restart-expiry", instructions: "x", topics: ["github.demo"] });
    const expiresAt = Date.now() + 500;
    await actors.setActivationFilter(actor.id, BOTH, undefined, expiresAt);
    await actors.close();
    closers.length = 0;
    await agents.close();
    const again = setup(root).actors;
    expect(again.status(actor.id).activationFilterExpiresAt).toBe(expiresAt);
    await waitFor(() => again.status(actor.id).activationFilter === undefined);
    expect(Date.now()).toBeGreaterThanOrEqual(expiresAt);
    expect(again.status(actor.id).filterSkipped.count).toBe(0);
    expect(again.messages(actor.id).filter(m => m.reason === "activationFilter cleared: expired")).toHaveLength(1);
    await expect(again.setActivationFilter(actor.id, BOTH, undefined, NaN)).rejects.toThrow("expiresAt");
    await again.setActivationFilter(actor.id, BOTH, undefined, Date.now() + 10_000);
    const reset = await again.setActivationFilter(actor.id, []);
    expect(reset.activationFilterExpiresAt).toBeUndefined();
    expect(reset.filterSkipped.count).toBe(0);
  });
});

const HEAD_A = "a".repeat(40);
const HEAD_B = "b".repeat(40);
const p0Request = (changes: Partial<ActorActivationFilterReservationRequest> = {}): ActorActivationFilterReservationRequest => {
  const createdAt = Date.now();
  return { repository: "smarty/demo", pr: 7, head: HEAD_A, createdAt, expiresAt: createdAt + 60_000,
    requiredSecurity: ["security", "dependencies"], ...changes };
};
interface HeldP0 { reservation: ActorActivationFilterReservation; token: string }
/** Reserve through the owning manager: it issues the generation and the one-time capability. */
const reserveP0 = async (actors: ActorManager, id: string, request: ActorActivationFilterReservationRequest,
  filter: FabricActorActivationFilter = BOTH, reservationToken?: string): Promise<HeldP0> => {
  const info = await actors.setActivationFilter(id, filter, undefined, undefined, request, undefined, reservationToken);
  expect(info.activationFilterReservation).toMatchObject({ ...request, generation: expect.stringMatching(ACTOR_ACTIVATION_GENERATION) });
  expect(info.activationFilterReservationToken).toEqual(expect.any(String));
  return { reservation: info.activationFilterReservation!, token: info.activationFilterReservationToken! };
};
const identityOf = ({ repository, pr, head, generation }: ActorActivationFilterReservation) => ({ repository, pr, head, generation });
const observeP0 = (actors: ActorManager, id: string, held: HeldP0,
  evidence: Partial<ActorActivationFilterObservation>, token: string | null = held.token) =>
  actors.setActivationFilter(id, undefined, undefined, undefined, undefined, { ...identityOf(held.reservation), ...evidence }, token ?? undefined);
const prEvent = (head: string, extra: Record<string, unknown> = {}) => ({
  event: "pull_request", payload: { repository: { full_name: "smarty/demo" }, number: 7,
    pull_request: { number: 7, head: { sha: head } }, ...extra },
});

// smarty-dev#4440: P0 state is native, exact-identity fenced and independently bounded.
describe("native P0 activation reservations", () => {
  it("rejects unbounded, expired, abbreviated, caller-generated and forged verdict reservations", async () => {
    const request = p0Request();
    expect(normalizeActorActivationReservation({ ...request, repository: "Smarty/Demo" })).toEqual(request);
    for (const changes of [{ head: "abc" }, { pr: 0 }, { repository: "demo" },
      { createdAt: request.createdAt + 100_000 }, { expiresAt: request.createdAt },
      { expiresAt: request.createdAt + ACTOR_ACTIVATION_RESERVATION_MAX_TTL_MS + 1 }]) {
      expect(() => normalizeActorActivationReservation({ ...request, ...changes }, request.createdAt)).toThrow();
    }
    expect(() => normalizeActorActivationReservation(request, request.expiresAt)).toThrow("hard TTL");
    // The generation is manager-issued: a caller can never pick (or reuse) one.
    expect(() => normalizeActorActivationReservation({ ...request, generation: randomUUID() })).toThrow("manager-issued");
    expect(() => normalizeActorActivationObservation({ ...request, generation: randomUUID() })).toThrow("lifecycle evidence");
    expect(() => normalizeActorActivationObservation({ ...request, createdAt: request.createdAt, reviewTerminal: true })).toThrow("generation");
    const { actors } = setup();
    const actor = await actors.create({ name: "bounded", instructions: "x" });
    await expect(actors.setActivationFilter(actor.id, [], undefined, undefined, request)).rejects.toThrow("non-empty");
    await expect(actors.setActivationFilter(actor.id, BOTH, undefined, request.expiresAt + 1, request)).rejects.toThrow("matching expiresAt");
    await expect(actors.setActivationFilter(actor.id, BOTH, undefined, undefined, { ...request, reviewTerminal: true } as never)).rejects.toThrow("observation");
    const held = await reserveP0(actors, actor.id, request);
    await expect(observeP0(actors, actor.id, held, { runId: "run", runStatus: "running" as never })).rejects.toThrow("terminal run evidence");
  });

  it("security P1: accepts lifecycle evidence only with the manager-issued capability, never from ordinary callers", async () => {
    const { actors, root } = setup();
    const actor = await actors.create({ name: "provenance", instructions: "x" });
    const other = await actors.create({ name: "other", instructions: "x" });
    const held = await reserveP0(actors, actor.id, p0Request({ requiredSecurity: [] }));
    const foreign = await reserveP0(actors, other.id, p0Request({ requiredSecurity: [] }));
    // Status reads never disclose the capability or its digest.
    expect(actors.status(actor.id).activationFilterReservationToken).toBeUndefined();
    expect(JSON.stringify(actors.status(actor.id))).not.toContain(held.token);
    expect(JSON.stringify(actors.list())).not.toMatch(/TokenSha256/);
    for (const token of [null, "", "forged", foreign.token, held.token + "x"]) {
      for (const evidence of [{ reviewTerminal: true as const }, { prState: "closed" as const }, { currentHead: HEAD_B }, { runId: "run", runStatus: "completed" as const }]) {
        await expect(observeP0(actors, actor.id, held, evidence, token)).rejects.toThrow("not authorized");
      }
    }
    expect(actors.status(actor.id).activationFilterReservation).toEqual(held.reservation);
    expect(actors.status(actor.id).activationFilter).toEqual(BOTH);
    // The registry keeps only the SHA-256 digest; the raw token is never persisted.
    const registry = fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8");
    expect(registry).not.toContain(held.token);
    expect(registry).toContain(createHash("sha256").update(held.token).digest("hex"));
    const receipt = await observeP0(actors, actor.id, held, { reviewTerminal: true });
    expect(receipt.activationFilterRelease).toMatchObject({ reason: "verdicts-terminal", reservation: { ...held.reservation, reviewTerminal: true } });
    // A retained receipt is republished only for its capability holder too.
    await expect(observeP0(actors, actor.id, held, { reviewTerminal: true }, foreign.token)).rejects.toThrow("not authorized");
    await expect(observeP0(actors, actor.id, held, { reviewTerminal: true })).resolves.toMatchObject({ activationFilterRelease: receipt.activationFilterRelease });
  });

  it("security P1: refuses unscoped replacement or clear while reserved; the capability holder's release retains evidence", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "guarded", instructions: "x" });
    const held = await reserveP0(actors, actor.id, p0Request());
    await observeP0(actors, actor.id, held, { reviewTerminal: true });
    const permissive: FabricActorActivationFilter = [{ id: "unrelated", topic: ["other.topic"] }];
    for (const filter of [permissive, null, []] as const) {
      await expect(actors.setActivationFilter(actor.id, filter as FabricActorActivationFilter | null)).rejects.toThrow("held by P0 reservation");
      await expect(actors.setActivationFilter(actor.id, filter as FabricActorActivationFilter | null, undefined, undefined, undefined, undefined, "forged")).rejects.toThrow("held by P0 reservation");
    }
    await expect(actors.setActivationFilter(actor.id, BOTH, undefined, undefined, p0Request())).rejects.toThrow("held by P0 reservation");
    expect(actors.status(actor.id)).toMatchObject({ activationFilter: BOTH, activationFilterReservation: { ...held.reservation, reviewTerminal: true } });
    expect(actors.messages(actor.id).filter(message => message.source === "actor:activation-filter")).toEqual([]);
    // Authorized replacement: predecessor released as "replaced" with its accumulated evidence and one audit.
    const replaced = await actors.setActivationFilter(actor.id, permissive, undefined, undefined, undefined, undefined, held.token);
    expect(replaced).toMatchObject({ activationFilter: permissive, activationFilterRelease: { reason: "replaced", reservation: { ...held.reservation, reviewTerminal: true } } });
    expect(replaced.activationFilterReservation).toBeUndefined();
    expect(actors.messages(actor.id).filter(message => message.reason === "activationFilter cleared: replaced")).toHaveLength(1);
    // An elapsed reservation never blocks the next ordinary set: it is released as expired first.
    const now = Date.now();
    const short = await reserveP0(actors, actor.id, p0Request({ createdAt: now, expiresAt: now + 200 }));
    await expect(actors.setActivationFilter(actor.id, null)).rejects.toThrow("held by P0 reservation");
    await waitFor(() => Date.now() >= short.reservation.expiresAt);
    const cleared = await actors.setActivationFilter(actor.id, null);
    expect(cleared.activationFilterRelease).toMatchObject({ reason: "expired", reservation: short.reservation });
  });

  it("reproduces s2: three ordinary heads skipped until exact review AND every required security verdict", async () => {
    const { actors, mesh, root } = setup();
    const actor = await actors.create({ name: "s2", instructions: "x", topics: ["github.demo"], coalesceKey: "payload.number" });
    const request = p0Request();
    const held = await reserveP0(actors, actor.id, request, [{ id: "p0", topic: ["github.demo"] }]);
    for (const head of [HEAD_B, "c".repeat(40), "d".repeat(40)]) {
      await mesh.publish({ topic: "github.demo", from, data: prEvent(head) });
    }
    await waitFor(() => actors.status(actor.id).filterSkipped.count === 3);
    expect(runDirs(root, actor.id)).toHaveLength(0);
    expect(actors.status(actor.id).filterSkipped).toMatchObject({ count: 3,
      lastKey: JSON.stringify(["mesh", "github.demo", 7]), lastAt: expect.any(Number) });
    // Older head, another PR/repository, or another generation cannot clear.
    for (const stale of [{ head: HEAD_B }, { pr: 8 }, { repository: "smarty/other" }, { generation: randomUUID() }]) {
      await observeP0(actors, actor.id, held, { ...stale, reviewTerminal: true, securityTerminal: ["security", "dependencies"] });
      expect(actors.status(actor.id).activationFilterReservation).toEqual(held.reservation);
    }
    await expect(observeP0(actors, actor.id, held, { securityTerminal: ["unknown"] })).rejects.toThrow("required security");
    await observeP0(actors, actor.id, held, { securityTerminal: ["security"] });
    await observeP0(actors, actor.id, held, { reviewTerminal: true });
    expect(actors.status(actor.id).activationFilterReservation).toMatchObject({ reviewTerminal: true, securityTerminal: ["security"] });
    const receipt = await observeP0(actors, actor.id, held, { securityTerminal: ["dependencies"] });
    expect(receipt.activationFilter).toBeUndefined();
    expect(receipt.activationFilterRelease).toMatchObject({ reason: "verdicts-terminal", reservation: {
      ...held.reservation, reviewTerminal: true, securityTerminal: ["security", "dependencies"] } });
    await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_B) });
    await waitFor(() => runDirs(root, actor.id).length === 1 && actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id).filterSkipped.count).toBe(0);
    expect(actors.status(actor.id).filteredCount).toBe(3);
  });

  it("security P2: a same-millisecond successor at the same head gets a fresh generation that delayed evidence cannot clear", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "generation", instructions: "x" });
    const request = p0Request({ requiredSecurity: [] });
    const first = await reserveP0(actors, actor.id, request);
    // Identical caller input (same createdAt): replaced by the capability holder.
    const second = await reserveP0(actors, actor.id, request, BOTH, first.token);
    expect(second.reservation.generation).not.toBe(first.reservation.generation);
    expect(second.token).not.toBe(first.token);
    expect(actors.status(actor.id).activationFilterRelease).toMatchObject({ reason: "replaced", reservation: first.reservation });
    // Delayed predecessor evidence, even with the predecessor's own capability, is inert.
    for (const evidence of [{ reviewTerminal: true as const }, { prState: "merged" as const }, { currentHead: HEAD_B }]) {
      await observeP0(actors, actor.id, first, evidence);
      expect(actors.status(actor.id).activationFilterReservation).toEqual(second.reservation);
    }
    // Predecessor capability cannot authorize evidence for the successor generation either.
    await expect(observeP0(actors, actor.id, second, { reviewTerminal: true }, first.token)).rejects.toThrow("not authorized");
    expect(actors.status(actor.id).activationFilter).toEqual(BOTH);
    // Many same-millisecond issues never reuse a generation.
    const generations = new Set([first.reservation.generation, second.reservation.generation]);
    let token = second.token;
    for (let i = 0; i < 20; i++) {
      const next = await reserveP0(actors, actor.id, request, BOTH, token);
      generations.add(next.reservation.generation);
      token = next.token;
    }
    expect(generations.size).toBe(22);
  });

  it("reloads the generation and capability digest across restart; a reservation missing them is never honoured", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "restart-capability", instructions: "x" });
    const held = await reserveP0(first.actors, actor.id, p0Request({ requiredSecurity: [] }));
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();
    const again = setup(first.root).actors;
    expect(again.status(actor.id).activationFilterReservation).toEqual(held.reservation);
    await expect(observeP0(again, actor.id, held, { reviewTerminal: true }, "forged")).rejects.toThrow("not authorized");
    await expect(observeP0(again, actor.id, held, { reviewTerminal: true })).resolves.toMatchObject({ activationFilterRelease: { reason: "verdicts-terminal" } });
    // A hand-edited/legacy stored reservation without its manager-issued generation is rejected.
    const { generation: _generation, ...legacy } = held.reservation;
    expect(() => normalizeStoredActorActivationReservation(legacy)).toThrow("generation");
    expect(normalizeStoredActorActivationReservation(held.reservation)).toEqual(held.reservation);
  });

  it.each(["closed", "merged", "head-changed"] as const)("drops queued PR work on %s, but keeps unrelated PRs eligible", async change => {
    const { actors, mesh, root } = setup();
    const actor = await actors.create({ name: "queued-pr", instructions: "x", topics: ["github.demo"], coalesce: false });
    const held = await reserveP0(actors, actor.id, p0Request());
    await mesh.publish({ topic: "github.demo", from, text: "LIVE_WITH_PROGRESS" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A) });
    await waitFor(() => actors.status(actor.id).queued === 1);
    const receipt = await observeP0(actors, actor.id, held, change === "head-changed" ? { currentHead: HEAD_B } : { prState: change });
    expect(receipt.activationFilterRelease?.reason).toBe(change === "head-changed" ? "head-changed" : "pr-closed");
    await waitFor(() => actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0);
    expect(runDirs(root, actor.id)).toHaveLength(1); // only the earlier unrelated run
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ stale: true, reason: change === "head-changed" ? "PR head changed" : "PR closed or merged" }));
    await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A, { number: 8, pull_request: { number: 8, head: { sha: HEAD_A } } }) });
    await waitFor(() => runDirs(root, actor.id).length === 2);
  });

  it("review P2: a late old-head event never coalesces over queued current-head work", async () => {
    const { actors, mesh, root } = setup();
    const actor = await actors.create({ name: "coalesce-pr", instructions: "x", topics: ["github.demo", "busy.demo"], coalesceKey: "payload.number" });
    const held = await reserveP0(actors, actor.id, p0Request(), [{ id: "none", topic: ["never.demo"] }]);
    const releasePath = path.join(root, "release-busy");
    await mesh.publish({ topic: "busy.demo", from, text: "LIVE_WITH_PROGRESS", data: { fakeWorkerReleasePath: releasePath } });
    await waitFor(() => actors.status(actor.id).status === "running");
    await observeP0(actors, actor.id, held, { currentHead: HEAD_B });
    await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_B, { marker: "CURRENT_HEAD_WORK" }) });
    await waitFor(() => actors.status(actor.id).queued === 1);
    // The delayed old-head webhook shares the coalesce key: it must not replace the queued item.
    await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A, { marker: "LATE_OLD_HEAD" }) });
    await waitFor(() => actors.messages(actor.id).some(message => message.stale && message.reason === "PR head changed"));
    expect(actors.status(actor.id).queued).toBe(1);
    fs.writeFileSync(releasePath, "release");
    await waitFor(() => runDirs(root, actor.id).length === 2 && actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0);
    const tasks = runTasks(root, actor.id);
    expect(tasks.some(task => task.includes("CURRENT_HEAD_WORK"))).toBe(true);
    expect(tasks.some(task => task.includes("LATE_OLD_HEAD"))).toBe(false);
    expect(actors.messages(actor.id).filter(message => message.stale)).toHaveLength(1);
  });

  it("drops a result when the current PR head changes while its run is in flight", async () => {
    const { actors, mesh, root } = setup();
    const actor = await actors.create({ name: "inflight-pr", instructions: "x", topics: ["github.demo"] });
    const held = await reserveP0(actors, actor.id, p0Request());
    const releasePath = path.join(root, "release-worker");
    await mesh.publish({ topic: "github.demo", from, text: "LIVE_WITH_PROGRESS", data: prEvent(HEAD_A, { fakeWorkerReleasePath: releasePath }) });
    await waitFor(() => actors.status(actor.id).status === "running");
    await observeP0(actors, actor.id, held, { currentHead: HEAD_B });
    fs.writeFileSync(releasePath, "release");
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ stale: true, reason: "PR head changed", runId: expect.any(String) }));
    expect(actors.messages(actor.id).filter(message => message.direction === "out" && message.text)).toEqual([]);
  });

  it("security P1: rechecks PR freshness after an asynchronous validWhile, before delivering the result", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "late-valid-while", instructions: "x", topics: ["github.demo"],
      validWhile: { version: 1, source: "() => true" } });
    const held = await reserveP0(actors, actor.id, p0Request());
    let calls = 0, evaluating = false, finish!: () => void;
    const gate = new Promise<void>(resolve => { finish = resolve; });
    const evaluate = vi.spyOn(predicate, "evaluateActorValidWhile").mockImplementation(async () => {
      // The first call is the before-run check; the second is before delivery.
      if (++calls === 2) { evaluating = true; await gate; }
      return { valid: true };
    });
    try {
      await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A) });
      await waitFor(() => evaluating);
      await observeP0(actors, actor.id, held, { currentHead: HEAD_B });
      finish();
      await waitFor(() => actors.status(actor.id).status === "idle");
      expect(calls).toBe(2);
      expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ stale: true, reason: "PR head changed", runId: expect.any(String) }));
      expect(actors.messages(actor.id).filter(message => message.direction === "out" && message.text)).toEqual([]);
    } finally { finish(); evaluate.mockRestore(); }
  });

  it("rechecks PR freshness after asynchronous model preparation, before allocating a run", async () => {
    let stalled = false, preparing = false, finish!: () => void;
    const binding = new Promise<void>(resolve => { finish = resolve; });
    const { actors, agents, mesh } = setup(undefined, { resolvePiModel: model => {
      if (!stalled) return model;
      preparing = true;
      return binding.then(() => model);
    } });
    const actor = await actors.create({ name: "late-preparation", instructions: "x", topics: ["github.demo"], model: "provider/test" });
    const held = await reserveP0(actors, actor.id, p0Request());
    const run = vi.spyOn(agents, "run");
    try {
      stalled = true;
      await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A) });
      await waitFor(() => preparing);
      await observeP0(actors, actor.id, held, { currentHead: HEAD_B });
      finish();
      await waitFor(() => actors.status(actor.id).status === "idle");
      expect(run).not.toHaveBeenCalled();
      expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ stale: true, reason: "PR head changed" }));
    } finally { stalled = false; finish(); run.mockRestore(); }
  });

  it("rechecks PR freshness after waiting for a native admission permit, before launching a worker", async () => {
    const { actors, agents, mesh, root } = setup(undefined, {}, 1);
    const releasePath = path.join(root, "release-permit");
    const blocker = await agents.spawn({ task: `LIVE_WITH_PROGRESS ${JSON.stringify({ fakeWorkerReleasePath: releasePath })}` });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      const actor = await actors.create({ name: "late-admission", instructions: "x", topics: ["github.demo"] });
      const held = await reserveP0(actors, actor.id, p0Request());
      await mesh.publish({ topic: "github.demo", from, data: prEvent(HEAD_A) });
      await waitFor(() => actors.status(actor.id).status === "waiting");
      await observeP0(actors, actor.id, held, { prState: "closed" });
      fs.writeFileSync(releasePath, "release");
      await agents.wait(blocker.id);
      await waitFor(() => actors.status(actor.id).status === "idle");
      expect(launch).not.toHaveBeenCalled();
      expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ stale: true, reason: "PR closed or merged" }));
    } finally { fs.writeFileSync(releasePath, "release"); launch.mockRestore(); }
  });

  it("validates known PR identity without network or registry reads and never guesses missing identity", () => {
    const reservation = { ...p0Request(), generation: randomUUID() };
    expect(activationPrInvalidReason(reservation, undefined, "mesh:github.demo", { data: prEvent(HEAD_B) })).toBe("PR head changed");
    expect(activationPrInvalidReason(reservation, undefined, "mesh:github.demo", { data: prEvent(HEAD_A) })).toBeUndefined();
    expect(activationPrInvalidReason(reservation, undefined, "mesh:github.demo", { data: { payload: { number: 7 } } })).toBeUndefined();
    expect(activationPrInvalidReason(reservation, undefined, "host:tool_error", { data: prEvent(HEAD_B) })).toBeUndefined();
  });

  it.each(["completed", "failed", "stopped", "timed_out", "throw"] as const)("releases a matching native run on %s", async outcome => {
    const { actors, agents } = setup();
    const actor = await actors.create({ name: "terminal", instructions: "x", responseMode: "directive" });
    let finish!: () => void;
    const done = new Promise<void>(resolve => { finish = resolve; });
    const runId = `terminal-${outcome}`;
    const run = vi.spyOn(agents, "run").mockImplementation(async (_request, _signal, launched) => {
      launched?.({ id: runId, status: "running" } as AgentHandleInfo);
      await done;
      if (outcome === "throw") throw new Error("transport failed after launch");
      return { id: runId, status: outcome, text: "", toolCalls: 0, turns: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } as AgentRunResult;
    });
    try {
      actors.tell(actor.id, "run");
      await waitFor(() => actors.status(actor.id).inFlightRun?.id === runId);
      const held = await reserveP0(actors, actor.id, p0Request({ runId }));
      finish();
      await waitFor(() => actors.status(actor.id).status === "idle");
      expect(actors.status(actor.id).activationFilterReservation).toBeUndefined();
      expect(actors.status(actor.id).activationFilterRelease).toMatchObject({ reason: "run-terminal", reservation: held.reservation, runStatus: outcome === "throw" ? "failed" : outcome });
    } finally { finish(); run.mockRestore(); }
  });

  it("polls external run claims, ignores non-terminal/unknown and wrong-run evidence, and releases timeout", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({ name: "external-claim", instructions: "x" });
    const status = vi.spyOn(agents, "status").mockImplementation(() => { throw new Error("unknown run"); });
    try {
      const held = await reserveP0(actors, actor.id, p0Request({ runId: "external" }));
      await observeP0(actors, actor.id, held, { runId: "old", runStatus: "completed" });
      expect(actors.status(actor.id).activationFilterReservation?.runId).toBe("external");
      for (const nonterminal of ["running", "queued"] as const) {
        status.mockReturnValue({ id: "external", status: nonterminal } as AgentHandleInfo);
        await new Promise(resolve => setTimeout(resolve, 60));
        expect(actors.status(actor.id).activationFilterReservation).toBeDefined();
      }
      status.mockReturnValue({ id: "external", status: "timed_out" } as AgentHandleInfo);
      await waitFor(() => actors.status(actor.id).activationFilterReservation === undefined);
      expect(actors.status(actor.id).activationFilterRelease).toMatchObject({ reason: "run-terminal", runStatus: "timed_out" });
    } finally { status.mockRestore(); }
  });

  it("retries an uncertain clear with retained exact evidence and one release audit", async () => {
    const { actors, root } = setup();
    const actor = await actors.create({ name: "retry", instructions: "x" });
    const held = await reserveP0(actors, actor.id, p0Request({ requiredSecurity: [] }));
    const write = vi.spyOn(ActorRegistryStore.prototype, "write").mockImplementationOnce(() => { throw new Error("disk unavailable"); });
    try {
      await expect(observeP0(actors, actor.id, held, { reviewTerminal: true })).rejects.toThrow("disk unavailable");
      const receipt = actors.status(actor.id).activationFilterRelease;
      expect(receipt).toMatchObject({ reason: "verdicts-terminal", observation: { reviewTerminal: true } });
      write.mockRestore();
      const retry = await observeP0(actors, actor.id, held, { reviewTerminal: true });
      expect(retry.activationFilterRelease).toEqual(receipt);
      expect(actors.messages(actor.id).filter(message => message.reason === "activationFilter cleared: verdicts-terminal")).toHaveLength(1);
      const row = JSON.parse(fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8")).actors.find((row: { id: string }) => row.id === actor.id);
      expect(row.activationFilterRelease).toEqual(receipt);
      expect(row.activationFilterReservation).toBeUndefined();
    } finally { write.mockRestore(); }
  });

  it("rolls back an issued reservation whose commit failed, so no unreleasable claim is left behind", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "issue-rollback", instructions: "x" });
    const write = vi.spyOn(ActorRegistryStore.prototype, "write").mockImplementationOnce(() => { throw new Error("disk unavailable"); });
    try {
      await expect(actors.setActivationFilter(actor.id, BOTH, undefined, undefined, p0Request())).rejects.toThrow("disk unavailable");
      expect(actors.status(actor.id).activationFilterReservation).toBeUndefined();
      expect(actors.status(actor.id).activationFilter).toBeUndefined();
      write.mockRestore();
      await reserveP0(actors, actor.id, p0Request());
    } finally { write.mockRestore(); }
  });

  it.each([false, true])("enforces the reservation deadline after restart (expired while offline: %s)", async expiredWhileOffline => {
    const first = setup();
    const actor = await first.actors.create({ name: "restart-p0", instructions: "x" });
    const held = await reserveP0(first.actors, actor.id, p0Request({ expiresAt: Date.now() + 500 }));
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();
    if (expiredWhileOffline) await waitFor(() => Date.now() >= held.reservation.expiresAt);
    const again = setup(first.root).actors;
    expect(again.status(actor.id).activationFilterReservation).toEqual(held.reservation);
    await waitFor(() => again.status(actor.id).activationFilterReservation === undefined);
    expect(Date.now()).toBeGreaterThanOrEqual(held.reservation.expiresAt);
    expect(again.status(actor.id).activationFilterRelease).toMatchObject({ reason: "expired", reservation: held.reservation });
    expect(again.status(actor.id).activationFilterExpiresAt).toBeUndefined();
  });

  it("expires before a synchronous event dispatch, with no additional per-event registry lock", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "hot-p0", instructions: "x", events: ["tool_error"] });
    const held = await reserveP0(actors, actor.id, p0Request());
    const lock = vi.spyOn(ActorRegistryStore.prototype, "withLock");
    const now = vi.spyOn(Date, "now");
    try {
      for (let i = 0; i < 30; i++) actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
      expect(actors.status(actor.id).filterSkipped.count).toBe(30);
      expect(lock).not.toHaveBeenCalled();
      now.mockReturnValue(held.reservation.expiresAt);
      actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
      expect(actors.status(actor.id).activationFilterReservation).toBeUndefined();
      expect(actors.status(actor.id).activationFilterRelease?.reason).toBe("expired");
      expect(actors.status(actor.id).filterSkipped.count).toBe(0);
    } finally { now.mockRestore(); lock.mockRestore(); }
  });
});

// review/astra F2 on #106: an unreadable stored filter must never drop or rewrite its actor.
describe("an unreadable stored activation filter", () => {
  const badFilter = ["hold", { id: "future", topic: ["github.*"], someNewField: 1 }];
  const recordOf = (file: string, id: string) =>
    JSON.stringify((JSON.parse(fs.readFileSync(file, "utf8")).actors as Array<{ id: string }>).find((a) => a.id === id));

  it("keeps a global template byte for byte, disables only its filter, and survives other saves", () => {
    const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-activation-filter-global-"));
    roots.push(agentDir);
    const first = new GlobalActorRegistry(agentDir, 64 * 1024);
    const template = first.create({ name: "sup", instructions: "Supervise.", topics: ["github.demo"], activationFilter: ["hold"] });
    const file = path.join(agentDir, "fabric", "actors", "global-actors.json");
    const stored = JSON.parse(fs.readFileSync(file, "utf8"));
    stored.actors[0].activationFilter = badFilter;                          // another version wrote it
    fs.writeFileSync(file, JSON.stringify(stored, null, 2));
    const before = recordOf(file, template.id);
    const emitWarning = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    try {
      const registry = new GlobalActorRegistry(agentDir, 64 * 1024);
      const listed = registry.resolve("sup")!;
      expect(listed.activationFilterError).toMatch(/unknown field someNewField/);
      expect(registry.toRequest(listed)).not.toHaveProperty("activationFilter");    // import delivers everything
      registry.create({ name: "other", instructions: "Other." });                   // saves the registry
      // Every stored field and value is unchanged. (Key order is the registry's own: any load and
      // save writes templates in its load order, with or without a filter.)
      expect(JSON.parse(recordOf(file, template.id))).toStrictEqual(JSON.parse(before));
      expect(JSON.parse(recordOf(file, template.id)).activationFilter).toEqual(badFilter);
      expect(new GlobalActorRegistry(agentDir, 64 * 1024).list().map((t) => t.name).sort()).toEqual(["other", "sup"]);
      // Updating the template itself keeps the stored filter; setting a valid one repairs it.
      const renamed = registry.update("sup", { instructions: "Supervise more." });
      expect(renamed.activationFilterError).toBeDefined();
      expect(JSON.parse(recordOf(file, template.id)).activationFilter).toEqual(badFilter);
      const repaired = registry.update("sup", { activationFilter: ["never-message-events"] });
      expect(repaired.activationFilterError).toBeUndefined();
      expect(repaired.activationFilter).toEqual(["never-message-events"]);
      expect(emitWarning.mock.calls.map(([message]) => String(message))
        .filter((message) => message.includes("unreadable activationFilter") && message.includes(template.id))).toHaveLength(1);
    } finally {
      emitWarning.mockRestore();
    }
  });

  it("keeps a project actor and its stored filter, delivers every event, and shows the error", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "sup", instructions: "Supervise.", topics: ["github.demo"], coalesce: false, activationFilter: ["hold"] });
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();
    const file = path.join(first.root, "actors", "actors.json");
    const stored = JSON.parse(fs.readFileSync(file, "utf8"));
    stored.actors.find((a: { id: string }) => a.id === actor.id).activationFilter = badFilter;
    fs.writeFileSync(file, JSON.stringify(stored));
    const { root, mesh, actors } = setup(first.root);
    const status = actors.status(actor.id);
    expect(status.activationFilterError).toMatch(/unknown field someNewField/);
    expect(status.activationFilter).toBeUndefined();
    await publish(mesh, envelope("issues.typed"), "github.demo");
    await publish(mesh, withLabels("issue_comment.created", [hold]), "github.demo");
    await waitFor(() => runDirs(root, actor.id).length === 2);             // nothing filtered
    expect(filtered(actors, actor.id)).toHaveLength(0);
    await actors.create({ name: "other", instructions: "Other.", topics: ["x"] });   // saves actors.json
    expect(JSON.parse(recordOf(file, actor.id)).activationFilter).toEqual(badFilter);
    const repaired = await actors.setActivationFilter(actor.id, ["hold"]);
    expect(repaired.activationFilterError).toBeUndefined();
    expect(JSON.parse(recordOf(file, actor.id)).activationFilter).toEqual(["hold"]);
  }, 30_000);
});
