import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import {
  activationFilterSkip,
  normalizeActorActivationFilter,
  type FabricActorActivationFilter,
} from "../src/actors/activation-filter.js";
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
    await event(2, "two-released", []);                // replaces the held one: the newest state wins
    await event(4, "four-held", [hold]);
    await waitFor(() => actors.status(actor.id).queued === 4);
    await waitFor(() => actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0, 20_000);
    const markers = runTasks(root, actor.id).map((task) => task.match(/"marker": "([^"]+)"/)?.[1]).filter(Boolean);
    expect(markers).toEqual(["one-b", "two-released", "three"]);
    expect(filtered(actors, actor.id).map((m) => m.reason)).toEqual(["filtered: hold"]);
    expect(actors.status(actor.id).filteredCount).toBe(1);
    // A direct message is never filtered, even by a rule its fields match.
    await actors.setActivationFilter(actor.id, [{ id: "any-message", where: [{ path: "message", exists: true }] }]);
    const runs = runDirs(root, actor.id).length;
    actors.tell(actor.id, "direct work");
    await waitFor(() => runDirs(root, actor.id).length === runs + 1);
    expect(filtered(actors, actor.id)).toHaveLength(1);
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
    await publish(mesh, envelope("issues.typed"), "github.demo");
    await waitFor(() => filtered(actors, actor.id).length === 1);
    expect(runDirs(root, actor.id)).toHaveLength(0);
    // A restart keeps the filter and the count.
    await actors.close();
    closers.length = 0;
    await agents.close();
    const again = setup(root).actors;
    const reloaded = again.status(actor.id);
    expect(reloaded.activationFilter).toEqual(["never-message-events"]);
    expect(reloaded.filteredCount).toBe(1);
    const cleared = await again.setActivationFilter(actor.id, null);
    expect(cleared.activationFilter).toBeUndefined();
    expect(cleared.filteredCount).toBe(1);
  }, 30_000);
});
