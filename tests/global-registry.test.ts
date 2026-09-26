import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { FabricActorRequest } from "../src/actors/types.js";

const dirs: string[] = [];

const setup = () => {
  const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-global-actors-"));
  dirs.push(agentDir);
  const registry = new GlobalActorRegistry(agentDir, 64 * 1024);
  return { agentDir, registry };
};

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

const baseRequest: FabricActorRequest = {
  name: "reviewer",
  instructions: "Review code for security defects and reply concisely.",
  events: ["turn_end"],
  topics: ["team.review"],
  delivery: "steer" as const,
  responseMode: "directive" as const,
  triggerTurn: false,
  coalesce: true,
};

describe("GlobalActorRegistry", () => {
  it("round-trips inference policy without history and keeps same-ID updates", () => {
    const { registry, agentDir } = setup();
    const original = registry.create({ ...baseRequest, inferenceContext: "activation", extensions: false });
    expect(registry.update(original.id, { instructions: "Updated" })).toMatchObject({ id: original.id, inferenceContext: "activation" });
    const restored = new GlobalActorRegistry(agentDir, 64 * 1024);
    expect(restored.toRequest(restored.resolve(original.id)!)).toMatchObject({ inferenceContext: "activation", extensions: false });
    expect(restored.toRequest(original)).not.toHaveProperty("sessionFile");
    expect(restored.toRequest(original)).not.toHaveProperty("messages");
    expect(restored.update(original.id, { inferenceContext: "full-history" })).toMatchObject({ id: original.id, inferenceContext: "full-history" });
    expect(() => restored.update(original.id, { inferenceContext: "invalid" as "activation" })).toThrow(/inference context/);
    expect(() => restored.update(original.id, { inferenceContext: null as unknown as "activation" })).toThrow(/inference context/);
    expect(() => restored.create({ ...baseRequest, name: "claude", runner: "claude", inferenceContext: "activation" })).toThrow(/Pi runner/);
  });

  it("creates, lists, and resolves templates by id, prefix, and name", () => {
    const { registry } = setup();
    expect(registry.list()).toEqual([]);

    const created = registry.create(baseRequest);
    expect(created.id).toMatch(/^[a-f0-9]{32}$/);
    expect(created.name).toBe("reviewer");
    expect(created.events).toEqual(["turn_end"]);
    expect(created.delivery).toBe("steer");
    expect(created.runner).toBe("pi");
    expect(created.model).toBeUndefined();

    expect(registry.list()).toHaveLength(1);
    expect(registry.resolve(created.id)?.name).toBe("reviewer");
    expect(registry.resolve(created.id.slice(0, 8))?.name).toBe("reviewer");
    expect(registry.resolve("reviewer")?.id).toBe(created.id);
    expect(registry.resolve("missing")).toBeUndefined();
  });

  it("lists and reloads a guarded template with validWhile as serialized source", () => {
    const { agentDir, registry } = setup();
    const validWhile = { version: 1 as const, source: "({ activation }) => activation.kind !== \"mesh\"" };
    registry.create({ ...baseRequest, validWhile });
    for (const listed of [registry.list(), new GlobalActorRegistry(agentDir, 64 * 1024).list()]) {
      expect(listed).toHaveLength(1);
      expect(listed[0]!.validWhile).toEqual(validWhile);
      expect(typeof listed[0]!.validWhile).toBe("object");
    }
  });

  it("persists across instances in the same agent dir", () => {
    const { agentDir, registry } = setup();
    registry.create({ ...baseRequest, extensions: false });
    const reloaded = new GlobalActorRegistry(agentDir, 64 * 1024);
    expect(reloaded.list()).toHaveLength(1);
    expect(reloaded.resolve("reviewer")?.instructions).toBe(baseRequest.instructions);
    expect(reloaded.resolve("reviewer")?.extensions).toBe(false);
  });

  it("rejects duplicate names without overwrite and replaces with it", () => {
    const { registry } = setup();
    registry.create(baseRequest);
    expect(() => registry.create(baseRequest)).toThrow(/already exists/);

    const replaced = registry.create(
      { ...baseRequest, instructions: "Updated instructions." },
      true,
    );
    expect(replaced.instructions).toBe("Updated instructions.");
    expect(replaced.id).toBe(registry.resolve("reviewer")?.id);
    expect(registry.list()).toHaveLength(1);
  });

  it("applies partial patches via update and revalidates", () => {
    const { registry } = setup();
    const created = registry.create(baseRequest);
    const patched = registry.update(created.id, { instructions: "Be brief." });
    expect(patched.instructions).toBe("Be brief.");
    expect(patched.name).toBe("reviewer");
    expect(patched.events).toEqual(["turn_end"]);

    const fabricDisabled = registry.update(created.id, { extensions: false });
    expect(fabricDisabled.extensions).toBe(false);
    expect(registry.update(created.id, { instructions: "Stay brief." }).extensions).toBe(false);

    expect(() => registry.update(created.id, { instructions: "   " })).toThrow(/empty/);
    expect(() => registry.update(created.id, { name: "bad name!" })).toThrow(/Invalid/);
  });

  it("requires explicit active delivery intent and rejects impossible policies", () => {
    const { registry } = setup();
    const { triggerTurn: _triggerTurn, ...ambiguous } = baseRequest;
    expect(() => registry.create(ambiguous)).toThrow(/requires explicit triggerTurn/);
    expect(() =>
      registry.create({ ...baseRequest, delivery: "mailbox", triggerTurn: true }),
    ).toThrow(/never starts Main/);
  });

  it("updates template delivery policies", () => {
    const { registry } = setup();
    const created = registry.create(baseRequest);
    const active = registry.update(created.id, { delivery: "followUp", triggerTurn: true });
    expect(active).toMatchObject({ delivery: "followUp", triggerTurn: true });
    expect(() =>
      registry.update(created.id, { delivery: "nextTurn", triggerTurn: true }),
    ).toThrow(/never starts Main/);
    expect(registry.resolve(created.id)).toMatchObject({ delivery: "followUp", triggerTurn: true });
  });

  it("removes templates", () => {
    const { registry } = setup();
    const created = registry.create(baseRequest);
    expect(registry.remove(created.id)).toEqual({ removed: true });
    expect(registry.list()).toEqual([]);
    expect(registry.remove(created.id)).toEqual({ removed: false });
  });

  it("validates names, instructions, events, topics, and sizes", () => {
    const { registry } = setup();
    expect(() => registry.create({ ...baseRequest, name: "" })).toThrow(/Invalid/);
    expect(() => registry.create({ ...baseRequest, name: "9bad name!" })).toThrow(/Invalid/);
    expect(() => registry.create({ ...baseRequest, instructions: "  " })).toThrow(/empty/);
    expect(() =>
      registry.create({ ...baseRequest, events: ["bogus" as never] }),
    ).toThrow(/Unsupported/);
    expect(() =>
      registry.create({ ...baseRequest, topics: ["bad topic!"] }),
    ).toThrow(/Invalid/);
    const big = "x".repeat(64 * 1024 + 1);
    expect(() => registry.create({ ...baseRequest, instructions: big })).toThrow(/exceed/);
  });

  it("strips identity and timestamps in toRequest and supports renaming", () => {
    const { registry } = setup();
    const created = registry.create({
      ...baseRequest,
      runner: "claude",
      model: "claude/haiku",
      extensions: false,
    });
    const request = registry.toRequest(created);
    expect(request).not.toHaveProperty("id");
    expect(request).not.toHaveProperty("createdAt");
    expect(request).not.toHaveProperty("updatedAt");
    expect(request.name).toBe("reviewer");
    expect(request.runner).toBe("claude");
    expect(request.model).toBe("claude/haiku");
    expect(request.extensions).toBe(false);

    const renamed = registry.toRequest(created, "reviewer-2");
    expect(renamed.name).toBe("reviewer-2");
  });


  it("normalizes impossible legacy trigger settings while keeping active modes passive by default", () => {
    const { agentDir } = setup();
    const registryPath = path.join(agentDir, "fabric", "actors", "global-actors.json");
    fs.mkdirSync(path.dirname(registryPath), { recursive: true });
    fs.writeFileSync(
      registryPath,
      JSON.stringify({
        format: 1,
        actors: [
          {
            id: "a".repeat(32),
            name: "legacy-mailbox",
            instructions: "Legacy.",
            delivery: "mailbox",
            triggerTurn: true,
            createdAt: 1,
          },
          {
            id: "b".repeat(32),
            name: "legacy-steer",
            instructions: "Legacy.",
            delivery: "steer",
            createdAt: 1,
          },
        ],
      }),
    );

    const reloaded = new GlobalActorRegistry(agentDir, 64 * 1024);
    expect(reloaded.resolve("legacy-mailbox")).toMatchObject({
      delivery: "mailbox",
      triggerTurn: false,
    });
    expect(reloaded.resolve("legacy-steer")).toMatchObject({
      delivery: "steer",
      triggerTurn: false,
    });
  });

  it("throws when a query matches multiple templates", () => {
    const { registry } = setup();
    registry.create(baseRequest);
    registry.create({ ...baseRequest, name: "reviewer-2" });
    // An empty query matches every template's id prefix, so two templates
    // are ambiguous. (Random 32-hex ids rarely share a longer prefix, so the
    // empty query deterministically exercises the ambiguity branch.)
    expect(() => registry.resolve("")).toThrow(/Ambiguous/);
  });
});

// smarty-dev#918 cross-session tests.
const crossRoots: string[] = [];
afterEach(() => {
  for (const root of crossRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const agentDir = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-global-"));
  crossRoots.push(root);
  return root;
};
const template = (name: string) => ({ name, instructions: `Be ${name}.` });

// smarty-dev#918: each session read the registry once at startup and wrote its whole copy back,
// so a write erased templates other sessions had added since and revived removed ones.
describe("GlobalActorRegistry across sessions", () => {
  it("sees another session's template, and a write keeps it", () => {
    const dir = agentDir();
    const first = new GlobalActorRegistry(dir, 64 * 1024);
    const second = new GlobalActorRegistry(dir, 64 * 1024);        // both loaded an empty registry
    const a = second.create(template("alpha"));
    expect(first.resolve("alpha")?.id).toBe(a.id);                   // read reloads
    first.create(template("beta"));                                  // write keeps alpha
    expect(new GlobalActorRegistry(dir, 64 * 1024).list().map((t) => t.name).sort()).toEqual(["alpha", "beta"]);
  });

  it("does not bring back a template another session removed", () => {
    const dir = agentDir();
    const seed = new GlobalActorRegistry(dir, 64 * 1024);
    seed.create(template("stale"));
    const holder = new GlobalActorRegistry(dir, 64 * 1024);          // loaded with "stale"
    expect(new GlobalActorRegistry(dir, 64 * 1024).remove("stale")).toEqual({ removed: true });
    holder.create(template("fresh"));
    expect(new GlobalActorRegistry(dir, 64 * 1024).list().map((t) => t.name)).toEqual(["fresh"]);
  });

  it("updates and removes against the current file", () => {
    const dir = agentDir();
    const first = new GlobalActorRegistry(dir, 64 * 1024);
    const second = new GlobalActorRegistry(dir, 64 * 1024);
    const created = second.create(template("gamma"));
    expect(first.update(created.id, { instructions: "Changed." }).instructions).toBe("Changed.");
    expect(second.resolve("gamma")?.instructions).toBe("Changed.");
    expect(second.remove(created.id)).toEqual({ removed: true });
    expect(first.list()).toEqual([]);
  });

  // review/astra F1 on #77: a session that replaced the file right after this one's save must
  // not be hidden by a fingerprint taken after the rename.
  it("sees a replacement made right after its own save, and a later write keeps it", () => {
    const dir = agentDir();
    const first = new GlobalActorRegistry(dir, 64 * 1024);
    const rename = fs.renameSync;
    let replaced = false;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (!replaced && String(to).endsWith("global-actors.json")) {
        replaced = true;
        new GlobalActorRegistry(dir, 64 * 1024).create(template("beta"));   // another session, at once
      }
    });
    first.create(template("alpha"));
    spy.mockRestore();
    expect(first.list().map((t) => t.name).sort()).toEqual(["alpha", "beta"]);
    first.create(template("later"));
    expect(new GlobalActorRegistry(dir, 64 * 1024).list().map((t) => t.name).sort()).toEqual(["alpha", "beta", "later"]);
  });

  // review/astra F2 on #77: a failed reload must not look like an empty registry.
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
    "keeps its snapshot when the file cannot be read, refuses to write, and loses nothing after",
    () => {
      const dir = agentDir();
      const first = new GlobalActorRegistry(dir, 64 * 1024);
      first.create(template("alpha"));
      expect(first.list()).toHaveLength(1);
      new GlobalActorRegistry(dir, 64 * 1024).create(template("beta"));   // another session changes it
      const file = path.join(dir, "fabric", "actors", "global-actors.json");
      fs.chmodSync(file, 0o000);
      try {
        expect(first.list().map((t) => t.name)).toEqual(["alpha"]);       // the previous snapshot
        expect(() => first.create(template("gamma"))).toThrow(/could not be read, so it was not changed/);
      } finally {
        fs.chmodSync(file, 0o600);
      }
      first.create(template("gamma"));
      expect(new GlobalActorRegistry(dir, 64 * 1024).list().map((t) => t.name).sort()).toEqual(["alpha", "beta", "gamma"]);
    },
  );
});
