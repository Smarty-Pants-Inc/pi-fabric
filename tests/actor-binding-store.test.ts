import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ActorBindingStore } from "../src/actors/binding-store.js";

const roots: string[] = [];

const setup = (rootId?: string): { first: ActorBindingStore; second: ActorBindingStore } => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bindings-"));
  roots.push(root);
  return {
    first: new ActorBindingStore("session:shared", root, rootId),
    second: new ActorBindingStore("session:shared", root, rootId),
  };
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorBindingStore", () => {
  it("prunes only exact selected IDs with explicit dead-root ownership under the overlay lock", async () => {
    const { first, second } = setup("session:dead");
    const at = path.dirname(path.dirname(first.filePath!));
    const live = new ActorBindingStore(first.sessionId, at, "session:live");
    const legacy = new ActorBindingStore(first.sessionId, at);
    await first.setModel("actor:a", "provider/dead"); await first.setThinking("actor:a", "high");
    await first.setThinking("actor:durable", "high");
    await live.setModel("actor:b", "provider/live"); await live.setThinking("actor:b", "low");
    await legacy.setModel("actor:legacy", "provider/legacy");
    const ids = new Set(["actor:a", "actor:b", "actor:legacy"]);
    const before = JSON.parse(fs.readFileSync(first.filePath!, "utf8")).bindings;
    expect(first.pruneCandidates("session:dead", ids)).toEqual(["actor:a"]);
    expect(await first.prune("session:dead", ids, async commit => {
      expect(fs.existsSync(`${first.filePath}.lock`)).toBe(true);
      await Promise.resolve(); expect(fs.existsSync(`${first.filePath}.lock`)).toBe(true);
      return commit();
    })).toBe(1);
    expect(second.get("actor:a")).toBeUndefined();
    const after = JSON.parse(fs.readFileSync(first.filePath!, "utf8")).bindings;
    delete before["actor:a"]; expect(after).toEqual(before);
    expect(live.get("actor:b")).toMatchObject({ model: "provider/live", thinking: "low", rootId: "session:live" });
    await expect(first.setThinking("actor:b", "high")).rejects.toThrow(/another root/);
  });
  it("preserves legacy ambiguity and refuses conflicting binding session identity", async () => {
    const { first } = setup(); await first.setThinking("actor:a", "high");
    expect(first.pruneCandidates("session:dead", new Set(["actor:a"]))).toEqual([]);
    const file = JSON.parse(fs.readFileSync(first.filePath!, "utf8")); file.sessionId = "not-this-storage-session";
    fs.writeFileSync(first.filePath!, JSON.stringify(file)); const before = fs.readFileSync(first.filePath!, "utf8");
    await expect(first.prune("session:dead", new Set(["actor:a"]), async commit => commit())).rejects.toThrow(/binding ownership/);
    expect(fs.readFileSync(first.filePath!, "utf8")).toBe(before);
  });

  it("merges unrelated writes from stale stores under one session lock", async () => {
    const { first, second } = setup();

    await first.setModel("actor:a", "provider/model-a");
    await second.setThinking("actor:b", "high");

    expect(first.get("actor:a")).toMatchObject({ model: "provider/model-a" });
    expect(first.get("actor:b")).toMatchObject({ thinking: "high" });
    expect(second.get("actor:a")).toMatchObject({ model: "provider/model-a" });
  });

  it("deletes a binding from the latest file instead of a stale snapshot", async () => {
    const { first, second } = setup();
    await first.setModel("actor:a", "provider/model-a");
    await first.setModel("actor:b", "provider/model-b");

    await second.delete("actor:a");

    expect(first.get("actor:a")).toBeUndefined();
    expect(first.get("actor:b")).toMatchObject({ model: "provider/model-b" });
  });
});
