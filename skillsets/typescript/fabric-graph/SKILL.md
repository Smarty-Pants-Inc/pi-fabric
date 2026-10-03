---
name: fabric-graph
description: Builds a step graph from plain Fabric programs. In-session graphs are ordinary code; resumable graphs are a durable actor that checkpoints each node in the mesh, schedules its next step, and parks human nodes on decisions. Use for multi-step flows that must survive restarts or wait for a person.
disable-model-invocation: true
---

# Fabric Graph

A graph is program code, not a framework. Pick the smallest form that fits:

- **In-session graph**: one `fabric_exec` program. Nodes are calls; edges are `if`, loops, `workflow.parallel`, and budgets. Use it when the whole graph finishes in this turn.
- **Resumable graph**: a durable actor that runs one node per mailbox message, checkpoints each node with `mesh.put` CAS, and wakes itself for the next node. Use it when a node waits on a person or a schedule, or the run must survive Main and resident-host restarts.

Nodes that are reviewed, reusable steps can be saved programs: `programs.run({ ref, input, requirePromoted: true })` runs one nested with the caller's capabilities.

## In-session graph

```ts
const target = π.target;
await workflow.configure({ name: `Graph · ${target}` });
await phase("Checks");
const [types, tests] = await workflow.parallel([
  () => pi.bash({ command: "bun run typecheck", settle: true }),
  () => pi.bash({ command: `bunx vitest run ${target}`, settle: true }),
]);
if (!types.ok || !tests.ok) {
  return { status: "failed", node: "checks", types: types.ok, tests: tests.ok };
}
await phase("Review");
const review = await agent<string>(`Review the change to ${target} for correctness. Reply APPROVE or a list of blockers.`);
return { status: review.startsWith("APPROVE") ? "success" : "partial", review };
```

Pass the test target as `strings.target`. A failed node returns its evidence; do not rerun passing nodes.

## Resumable graph

Seed the run once, then create a durable actor that owns it. The state record is the single source of truth: `runs/<run>/graph` holds `{ node, status, results, decisionId? }` with its mesh version.

```ts
const run = π.run;
const key = `runs/${run}/graph`;
await mesh.put({
  key,
  value: { node: "fetch", status: "ready", results: {} },
  ifVersion: 0,
});
const actor = await agents.create({
  name: `graph-${run}`,
  runner: "pi",
  residency: "durable",
  topics: [`graph.${run}`],
  instructions: [
    `You own the resumable graph ${key}. Nodes in order: fetch, test, approve, release.`,
    "On every message, run exactly one fabric_exec program that:",
    `1. reads ${key} with mesh.get and stops if status is "done" or "failed";`,
    "2. runs the current node only, writes its result and the next node with mesh.put({ ifVersion: <the version you read> }), and stops on a CAS conflict;",
    "3. for approve, raises decisions.raise({ title, input: \"confirm\" }) once, stores decisionId in the record, and waits with decisions.wait({ id, timeoutMs: 60000 }); while still open, schedules mesh.publish({ topic, kind: \"graph.tick\", afterMs: 600000, key }) and stops;",
    "4. after a successful node, wakes itself with agents.tell to its own id from mesh.self().",
    "Every node must be idempotent: before acting, check the recorded result; an interrupted spawn or shell run is indeterminate, so verify its effect before repeating it.",
  ].join("\n"),
});
await agents.tell({ id: actor.id, message: `Start graph ${key}.` });
return { run, key, actor: actor.id, topic: `graph.${run}` };
```

Pass the run key as `strings.run`. The actor's node program follows this shape:

```ts
const key = `runs/${π.run}/graph`;
type Graph = { node: string; status: string; results: Record<string, unknown>; decisionId?: string };
const entry = await mesh.get<Graph>({ key });
if (!entry || entry.value.status === "done" || entry.value.status === "failed") return { status: entry?.value.status ?? "missing" };
const graph = entry.value;
const advance = async (node: string, result: unknown, status = "ready") => {
  await mesh.put({ key, value: { ...graph, node, status, results: { ...graph.results, [graph.node]: result } }, ifVersion: entry.version });
  if (status === "ready") await agents.tell({ id: (await mesh.self()).id, message: `Continue ${key}.` });
};
if (graph.node === "approve") {
  let decisionId = graph.decisionId;
  if (!decisionId) {
    decisionId = (await decisions.raise({ title: `Release ${π.run}?`, input: "confirm" })).id;
    await mesh.put({ key, value: { ...graph, decisionId }, ifVersion: entry.version });
    return { status: "waiting", decisionId };
  }
  const decision = await decisions.wait({ id: decisionId, timeoutMs: 60_000 });
  if (decision.status === "open") {
    await mesh.publish({ topic: `graph.${π.run}`, kind: "graph.tick", afterMs: 600_000, key: `graph-${π.run}` });
    return { status: "waiting", decisionId };
  }
  const approved = decision.status === "answered" && decision.answer?.optionId === "yes";
  await advance(approved ? "release" : "approve", decision.status, approved ? "ready" : "failed");
  return { status: approved ? "approved" : "rejected" };
}
const result = await programs.run({ ref: `graph-${graph.node}`, input: { run: π.run }, requirePromoted: true });
await advance(graph.node === "fetch" ? "test" : graph.node === "test" ? "approve" : "done", result, graph.node === "release" ? "done" : "ready");
return { status: "advanced", from: graph.node };
```

Here each work node is a promoted saved program named `graph-<node>`; plain inline code works the same way.

## Rules

- One node per wake. The mesh record decides what runs next; mailbox text never does.
- Every write is `mesh.put` with the version you read. A conflict means another wake advanced the graph: stop.
- Human nodes park on `decisions.raise`/`decisions.wait` and recheck through a scheduled `mesh.publish` with a stable `key`, so a restart never raises a second decision.
- Nodes are idempotent. Interrupted spawns are indeterminate after a restart: verify effects (files, commits, tickets) before repeating a node, and record the evidence in `results`.
- Mesh events never run programs by themselves. An event may carry `data.fabricProgram = { ref, input }`; the subscribed actor decides to call `programs.run`.

## Completion criterion

Complete when the in-session program returns `success`, `partial`, or `failed` with per-node evidence, or when the resumable graph is seeded, its durable actor is created and told to start, and you report the run key, state key, actor id, and topic.
