import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { FabricActorRequest, GlobalActorDefinition } from "../src/actors/types.js";

// Keep reason: native-to-model downgrade is a trust boundary; unrelated template
// writes must also preserve quarantined data in the actual registry file.
const roots: string[] = [];
const maxBytes = 64 * 1024;
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-native-global-"));
  roots.push(root);
  const file = path.join(root, "fabric", "actors", "global-actors.json");
  return { root, file, registry: new GlobalActorRegistry(root, maxBytes) };
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const modelRequest: FabricActorRequest = {
  name: "reviewer",
  instructions: "Review the change without inheriting history.",
  events: ["turn_end"],
  topics: ["team.review"],
  delivery: "steer",
  responseMode: "directive",
  triggerTurn: false,
  coalesce: true,
};

const rejectedMarkers: Array<[string, Record<string, unknown>]> = [
  ["native sentinel", {
    execution: { kind: "native", version: 1, handlerKey: "smarty-inbox.rpc.v1" },
    runner: "native",
    instructions: null,
    residency: "durable",
    events: [],
    topics: [],
    delivery: "mailbox",
    responseMode: "text",
    triggerTurn: false,
    coalesce: false,
  }],
  ["native execution with a model runner", { execution: { kind: "native", version: 1, handlerKey: "rpc.v1" }, runner: "pi" }],
  ["unknown execution kind", { execution: { kind: "future", version: 1 }, runner: "pi" }],
  ["unknown execution version", { execution: { kind: "native", version: 2, handlerKey: "rpc.v2" } }],
  ["model-looking execution", { execution: { kind: "model" } }],
  ["empty execution", { execution: {} }],
  ["undefined execution", { execution: undefined }],
  ["null execution", { execution: null }],
  ["boolean execution", { execution: false }],
  ["number execution", { execution: 0 }],
  ["string execution", { execution: "native" }],
  ["array execution", { execution: [] }],
  ["reserved runner alone", { runner: "native" }],
];

const refusal = "Global actor templates do not support execution markers or the native runner";

describe("GlobalActorRegistry native template guard", () => {
  it.each(rejectedMarkers)("refuses %s in create, overwrite, update and toRequest without changing the file", (_label, marker) => {
    const { registry, file } = setup();
    const original = registry.create(modelRequest);
    const before = fs.readFileSync(file, "utf8");
    const input = { ...modelRequest, ...marker } as unknown as FabricActorRequest;
    expect(() => registry.create(input)).toThrow(refusal);
    expect(() => registry.create(input, true)).toThrow(refusal);
    expect(() => registry.update(original.id, { instructions: "Must not replace the model.", ...marker } as unknown as Parameters<GlobalActorRegistry["update"]>[1]))
      .toThrow(refusal);
    const definition = { ...original, ...marker } as unknown as GlobalActorDefinition;
    expect(() => registry.toRequest(definition)).toThrow(refusal);
    expect(() => registry.toRequest(definition, "renamed")).toThrow(refusal);
    expect(registry.list()).toEqual([original]);
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("rejects an own non-enumerable undefined execution property before reconstruction", () => {
    const { registry, file } = setup();
    const original = registry.create(modelRequest);
    const before = fs.readFileSync(file, "utf8");
    const mark = <T extends object>(value: T): T => Object.defineProperty(value, "execution", { value: undefined });
    expect(() => registry.create(mark({ ...modelRequest }))).toThrow(refusal);
    expect(() => registry.update(original.id, mark({ instructions: "Must not be applied." }))).toThrow(refusal);
    expect(() => registry.toRequest(mark({ ...original }))).toThrow(refusal);
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it.each(["pi", "pi-durable", "claude"] as const)("keeps ordinary %s model create/update/list/import roundtrips unchanged", (runner) => {
    const { registry, root } = setup();
    const created = registry.create({ ...modelRequest, runner, model: "provider/model", thinking: "high", extensions: false });
    const updated = registry.update(created.id, { instructions: "Updated model instructions.", topics: ["team.updated"] });
    expect(updated).toMatchObject({ id: created.id, createdAt: created.createdAt, runner, model: "provider/model", thinking: "high", extensions: false });
    const reloaded = new GlobalActorRegistry(root, maxBytes);
    expect(reloaded.list()).toEqual([updated]);
    expect(reloaded.resolve(updated.name)).toEqual(updated);
    const request = reloaded.toRequest(updated, "imported-reviewer");
    expect(request).toEqual({
      ...modelRequest,
      name: "imported-reviewer",
      instructions: "Updated model instructions.",
      topics: ["team.updated"],
      residency: "session",
      runner,
      model: "provider/model",
      thinking: "high",
      extensions: false,
    });
    const imported = reloaded.create(request);
    expect(imported.id).not.toBe(created.id);
    expect(new GlobalActorRegistry(root, maxBytes).list()).toEqual([updated, imported]);
  });
});

// Undefined has no JSON representation; it is covered by direct API tests above.
const quarantinedRows = (): Record<string, unknown>[] => [
  ...rejectedMarkers
    .filter(([, marker]) => !("execution" in marker) || marker.execution !== undefined)
    .map(([, marker], index) => ({
      id: (index + 1).toString(16).padStart(32, "0"),
      name: `quarantined-${index}`,
      instructions: "Do not normalize this row to Pi.",
      createdAt: 1,
      updatedAt: 2,
      quarantineFixture: true,
      opaque: { text: "Keep \"quoted\" bytes, \\paths, \r\n whitespace and café.", nested: [null, false, { future: "value" }] },
      ...marker,
    })),
  // Quarantine must happen even before legacy identity/instructions validation.
  { id: null, name: 17, instructions: null, execution: { kind: "future" }, quarantineFixture: true, opaque: { keep: "invalid model shape" } },
];

const writeRows = (file: string, actors: unknown[]): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify({ format: 1, actors }, null, 2));
};

const readRows = (file: string): Record<string, unknown>[] =>
  (JSON.parse(fs.readFileSync(file, "utf8")) as { actors: Record<string, unknown>[] }).actors;

describe("GlobalActorRegistry on-disk execution quarantine", () => {
  it("never lists, resolves or updates native/unknown execution rows or the reserved runner as model templates", () => {
    const { root, file } = setup();
    const rows = quarantinedRows();
    writeRows(file, rows);
    const before = fs.readFileSync(file, "utf8");
    const registry = new GlobalActorRegistry(root, maxBytes);
    expect(registry.list()).toEqual([]);
    for (const row of rows) {
      if (typeof row.id !== "string" || typeof row.name !== "string") continue;
      expect(registry.resolve(row.id)).toBeUndefined();
      expect(registry.resolve(row.name)).toBeUndefined();
      const id = row.id;
      expect(() => registry.update(id, { instructions: "Must not revive a native template." })).toThrow("Unknown global actor");
      expect(registry.remove(row.id)).toEqual({ removed: false });
      expect(() => registry.toRequest(row as unknown as GlobalActorDefinition)).toThrow(refusal);
    }
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("retains every raw quarantined row and nested field across unrelated creates, updates and reloads", () => {
    const { root, file, registry } = setup();
    const original = registry.create(modelRequest);
    const rows = quarantinedRows();
    // Simulate another session writing markers after this instance cached its model.
    writeRows(file, [...rows, original]);
    const preserved = () => {
      const stored = readRows(file);
      const quarantined = stored.filter((row) => row.quarantineFixture === true);
      expect(quarantined).toEqual(rows);
      // Compare serialized opaque values too; pretty-print framing is not the row data.
      expect(JSON.stringify(quarantined)).toBe(JSON.stringify(rows));
      expect(stored).toHaveLength(rows.length + 2);
      const reloaded = new GlobalActorRegistry(root, maxBytes);
      expect(reloaded.list().map((row) => row.name).sort()).toEqual(["reviewer", "unrelated"]);
      expect(reloaded.resolve(original.id)?.runner).toBe("pi");
    };
    registry.create({ ...modelRequest, name: "unrelated" });
    preserved();
    new GlobalActorRegistry(root, maxBytes).update(original.id, { instructions: "Updated ordinary model." });
    preserved();
    registry.create({ ...modelRequest, instructions: "Overwritten ordinary model." }, true);
    preserved();
    expect(registry.resolve(original.id)?.instructions).toBe("Overwritten ordinary model.");
  });
});
