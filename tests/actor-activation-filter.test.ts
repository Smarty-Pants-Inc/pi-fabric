import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import {
  activationFilterSkip,
  normalizeActorActivationFilter,
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
const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-activation-filter-"))) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
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
    // The optimistic path stages via prepare(), then atomically renames once.
    const writes = vi.spyOn(ActorRegistryStore.prototype, "prepare");
    const registryPath = path.join(root, "actors/actors.json");
    const renames = vi.spyOn(fs, "renameSync");
    const syncs = vi.spyOn(fs, "fsyncSync");
    try {
      for (let i = 0; i < 30; i++) actors.dispatchHostEvent("tool_error", envelope("host:tool_error").payload);
      expect(actors.status(actor.id).filterSkipped).toMatchObject({ count: 30, lastKey: "host:tool_error", lastTopic: "tool_error" });
      expect(writes).not.toHaveBeenCalled();
      expect(renames.mock.calls.filter(([, target]) => String(target) === registryPath)).toHaveLength(0);
      await waitFor(() => {
        const record = JSON.parse(fs.readFileSync(path.join(root, "actors/actors.json"), "utf8")).actors.find((a: { id: string }) => a.id === actor.id);
        return record.filterSkipped?.count === 30;
      });
      expect(writes).toHaveBeenCalledTimes(1);
      expect(writes.mock.calls[0]?.[1]).toMatchObject({ durable: false });
      expect(syncs).not.toHaveBeenCalled();
      expect(renames.mock.calls.filter(([, target]) => String(target) === registryPath)).toHaveLength(1);
    } finally { writes.mockRestore(); renames.mockRestore(); syncs.mockRestore(); }
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
