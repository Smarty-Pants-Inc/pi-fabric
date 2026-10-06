import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorRegistryPayloads } from "../src/actors/registry-payloads.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "registry-hold-time-")); roots.push(root);
  const actorRoot = path.join(root, "actors");
  const store = new ActorRegistryStore(actorRoot);
  const rows = Array.from({ length: 50 }, (_, index) => ({
    id: index.toString(16).padStart(32, "0"), name: `review-${index}`, rootId: "session:hold",
    residency: "session", instructions: "p".repeat(20_000), messages: [], createdAt: 1, updatedAt: 1, status: "idle",
  }));
  store.write(rows, { durable: true });
  return { root, actorRoot, store, rows, lock: path.join(actorRoot, "actors.json.lock") };
};
const burn = (ms: number) => { const end = performance.now() + ms; while (performance.now() < end) { /* CPU-starved codec */ } };

// CPU bounds, not wall-clock scheduling assertions: a nice-19 process can be
// descheduled at any instruction. Structural assertions also catch slow codecs
// even on a host too busy to make a meaningful wall-time measurement.
describe("#4383 bounded registry holds", () => {
  it("keeps slow 1 MB parse/serialize and all 50 payload preparations outside every hold", async () => {
    const { store, lock, actorRoot } = fixture();
    expect(fs.statSync(path.join(actorRoot, "actors.json")).size).toBeGreaterThan(1_000_000);
    let holding = false;
    const cpu: number[] = [];
    const acquire = store.withLock.bind(store);
    vi.spyOn(store, "withLock").mockImplementation(operation => acquire(() => {
      const start = process.cpuUsage(); holding = true;
      try { return operation(); }
      finally { holding = false; const used = process.cpuUsage(start); cpu.push((used.user + used.system) / 1_000); }
    }));
    const parse = JSON.parse, stringify = JSON.stringify;
    let slowParses = 0, slowEncodes = 0;
    vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
      if (text.length > 100_000) { expect(holding).toBe(false); expect(fs.existsSync(lock)).toBe(false); slowParses++; burn(60); }
      return parse(text, reviver);
    });
    vi.spyOn(JSON, "stringify").mockImplementation(((value: unknown, replacer?: never, space?: string | number) => {
      if (typeof value === "object" && value !== null && "instructions" in value &&
        typeof value.instructions === "string" && value.instructions.length >= 20_000) {
        expect(holding).toBe(false); slowEncodes++; burn(10);
      }
      return stringify(value, replacer, space);
    }) as typeof JSON.stringify);
    const compact = ActorRegistryPayloads.prototype.compact;
    vi.spyOn(ActorRegistryPayloads.prototype, "compact").mockImplementation(function (this: ActorRegistryPayloads, ...args) {
      expect(holding).toBe(false);
      return compact.apply(this, args);
    });
    for (let round = 0; round < 4; round++) await store.update(current => ({
      actors: current.map((row, index) => index === round ? { ...row, nice: round + 1 } : row), durable: true, value: true,
    }));
    expect(slowParses).toBeGreaterThan(0);
    expect(slowEncodes).toBeGreaterThan(0);
    expect(cpu).toHaveLength(4);
    expect(Math.max(...cpu)).toBeLessThan(50);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("reuses unchanged actor encodings instead of compacting/re-serializing their personas", async () => {
    const { store } = fixture();
    await store.withLock(() => store.write(store.records().map((row, index) => ({ ...row,
      messages: [{ id: `prior-${index}`, direction: "in", text: "keep" }],
    }))));
    await store.update(current => ({ actors: current, value: true })); // prime the encoding cache
    const compact = vi.spyOn(ActorRegistryPayloads.prototype, "compact");
    const stringify = JSON.stringify;
    const personas: string[] = [];
    vi.spyOn(JSON, "stringify").mockImplementation(((value: unknown, replacer?: never, space?: string | number) => {
      if (typeof value === "object" && value !== null && "instructions" in value) personas.push(String((value as Record<string, unknown>).id));
      return stringify(value, replacer, space);
    }) as typeof JSON.stringify);
    await store.update(current => ({ actors: current.map((row, index) => {
      // Model the manager's lazy history selection (no inline messages), with
      // different field order from a mixed-release writer. Neither is a change.
      const { messages: _stub, ...selected } = row;
      const serializedShape = Object.fromEntries(Object.entries(selected).reverse());
      return index === 17 ? { ...serializedShape, nice: 7 } : serializedShape;
    }), value: true }));
    expect(compact).toHaveBeenCalledTimes(1);
    expect(personas).toEqual(["11".padStart(32, "0")]);
  });

  it("re-selects after a foreign generation wins and never publishes speculative history heads", async () => {
    const { store, actorRoot } = fixture();
    const other = new ActorRegistryStore(actorRoot);
    const acquire = store.withLock.bind(store);
    let selections = 0, raced = false;
    vi.spyOn(store, "withLock").mockImplementation(async operation => {
      if (!raced) {
        raced = true;
        expect(fs.existsSync(path.join(actorRoot, "0".repeat(32), "registry", "messages-head.json"))).toBe(false);
        await other.withLock(() => other.write(other.records().map((row, index) => index === 49 ? { ...row, futureField: "keep" } : row)));
      }
      return acquire(operation);
    });
    await store.update(current => {
      selections++;
      const message = { id: "accepted", direction: "in", text: "once" };
      return { actors: current.map((row, index) => index === 0 ? { ...row, messages: [message], registryMessageAppend: [message] } : row), value: true };
    });
    expect(selections).toBe(2);
    expect(store.records().at(-1)?.futureField).toBe("keep");
    expect(store.messages(store.records()[0]!)).toEqual([{ id: "accepted", direction: "in", text: "once" }]);
    const transactions = fs.readFileSync(path.join(actorRoot, "0".repeat(32), "registry", "messages.jsonl"), "utf8").trim().split("\n");
    expect(transactions).toHaveLength(1);
    expect(fs.readdirSync(actorRoot).filter(file => file.endsWith(".prepared"))).toEqual([]);
  });

  it("real concurrent setters over a 50-actor registry complete inside the 5 s acquisition window", async () => {
    const { root, actorRoot, store } = fixture();
    const identity = { id: "session:hold", name: "Main", kind: "main" as const, sessionId: "hold" };
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    const manager = new ActorManager("hold", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot, rootId: identity.id, persistent: true, claimResidency: "session", reapDeadSessionPresence: false,
    });
    try {
      const parse = JSON.parse, stringify = JSON.stringify;
      vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
        if (text.length > 100_000) { expect(fs.existsSync(path.join(actorRoot, "actors.json.lock"))).toBe(false); burn(15); }
        return parse(text, reviver);
      });
      vi.spyOn(JSON, "stringify").mockImplementation(((value: unknown, replacer?: never, space?: string | number) => {
        if (typeof value === "object" && value !== null && "instructions" in value &&
          typeof value.instructions === "string" && value.instructions.length >= 20_000) {
          expect(fs.existsSync(path.join(actorRoot, "actors.json.lock"))).toBe(false); burn(2);
        }
        return stringify(value, replacer, space);
      }) as typeof JSON.stringify);
      const started = performance.now();
      const ids = store.records().slice(0, 8).map(row => row.id);
      await Promise.all(ids.map((id, index) => manager.setNice(id, index + 1)));
      expect(performance.now() - started).toBeLessThan(5_000);
      for (const [index, id] of ids.entries()) expect(store.records().find(row => row.id === id)?.nice).toBe(index + 1);
    } finally { await manager.close(); await agents.close(); }
  }, 15_000);
});
