import fs from "node:fs";
import { ModuleKind, ScriptTarget, transpileModule } from "typescript";
import { expect, it } from "vitest";
import { MontyRuntime } from "../src/runtime/monty-runtime.js";
import { availablePythonBackends } from "./fixtures/python-backends.js";

type Host = (ref: string, args: Record<string, any>) => Promise<any>;
const execute = async (kernel: "typescript" | "python", host: Host) => {
  const text = fs.readFileSync(`skillsets/${kernel}/fabric-graph/SKILL.md`, "utf8");
  const pattern = kernel === "typescript" ? /```ts\r?\n([\s\S]*?)\r?\n```/g : /```python\r?\n([\s\S]*?)\r?\n```/g;
  const code = [...text.matchAll(pattern)][2]![1]!;
  if (kernel === "python") {
    const result = await new MontyRuntime().execute(code, host, { timeoutMs: 5000, memoryLimitBytes: 64 * 1024 * 1024, strings: { run: "release-test" } });
    expect(result.terminationReason, result.error).toBe("completed");
    return result.value;
  }
  const javascript = transpileModule(code, { compilerOptions: { target: ScriptTarget.ES2022, module: ModuleKind.None } }).outputText;
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const proxy = (provider: string) => new Proxy({}, { get: (_target, name) => (args: Record<string, any>) => host(`${provider}.${String(name)}`, args) });
  return new AsyncFunction("π", "mesh", "decisions", "agents", "programs", javascript)({ run: "release-test" }, proxy("mesh"), proxy("decisions"), proxy("agents"), proxy("programs"));
};

const fixture = (answered: boolean) => {
  let entry = { key: "runs/release-test/graph", version: 1, value: { node: "approve", status: "ready", results: {} } as Record<string, any> };
  const raised: string[] = [], writes: number[] = [], wakes: string[] = [], ticks: Record<string, any>[] = [];
  const host: Host = async (ref, args) => {
    if (ref === "fabric.$call") { ref = args.ref; args = args.args ?? {}; }
    if (ref === "mesh.get") return structuredClone(entry);
    if (ref === "mesh.put") {
      expect(args.ifVersion).toBe(entry.version);
      writes.push(args.ifVersion);
      entry = { ...entry, version: entry.version + 1, value: args.value };
      return structuredClone(entry);
    }
    if (ref === "decisions.raise") { raised.push("dec-first"); return { id: "dec-first" }; }
    if (ref === "decisions.wait") {
      expect(args.id).toBe("dec-first");
      return answered ? { status: "answered", answer: { optionId: "yes" } } : { status: "open" };
    }
    if (ref === "mesh.publish") { ticks.push(args); return { id: "tick-first" }; }
    if (ref === "mesh.self") return { id: "actor" };
    if (ref === "agents.tell") { wakes.push(args.id); return {}; }
    throw new Error(`Unexpected action ${ref}`);
  };
  return { host, raised, writes, wakes, ticks, state: () => entry, answer: () => { answered = true; } };
};

for (const kernel of ["typescript", "python"] as const) {
  it.skipIf(kernel === "python" && !availablePythonBackends.monty)(`${kernel}: answering the first approval advances immediately with the updated CAS version`, async () => {
    const h = fixture(true);
    expect(await execute(kernel, h.host)).toEqual({ status: "approved" });
    expect(h.raised).toHaveLength(1);
    expect(h.writes).toEqual([1, 2]);
    expect(h.state().value.node).toBe("release");
    expect(h.wakes).toEqual(["actor"]);
  });
  it.skipIf(kernel === "python" && !availablePythonBackends.monty)(`${kernel}: an unanswered first approval arms its own wake, then advances after an answer`, async () => {
    const h = fixture(false);
    expect(await execute(kernel, h.host)).toEqual({ status: "waiting", decisionId: "dec-first" });
    expect(h.ticks).toEqual([{ topic: "graph.release-test", kind: "graph.tick", afterMs: 600000, key: "graph-release-test" }]);
    h.answer();
    // Deliver the actor's scheduled tick, not an unrelated/manual mailbox nudge.
    expect(await execute(kernel, h.host)).toEqual({ status: "approved" });
    expect(h.raised).toHaveLength(1);
    expect(h.state().value.node).toBe("release");
    expect(h.wakes).toEqual(["actor"]);
  });
}
