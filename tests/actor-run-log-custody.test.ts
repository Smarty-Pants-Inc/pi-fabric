import fs from "node:fs";
import { constants, zstdCompressSync } from "node:zlib";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-run-custody-"));
  roots.push(root);
  const source = path.join(root, "source");
  fs.mkdirSync(source, { mode: 0o700 });
  const id = "a".repeat(32);
  fs.writeFileSync(path.join(source, "events.jsonl.zst"), zstdCompressSync(Buffer.from("{\"type\":\"message_end\"}\n"), { params: { [constants.ZSTD_c_checksumFlag]: 1 } }), { mode: 0o600 });
  fs.writeFileSync(path.join(source, "status.json"), JSON.stringify({ id, status: "completed", finishedAt: 1 }), { mode: 0o600 });
  const actor = { id: "actor", sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: id };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 60_000 });
  return { root, source, actor, id, store };
};

describe("compressed actor run-log custody", () => {
  it("copies compressed events and status durably, then verifies a source-less rejoin", async () => {
    const { root, source, actor, id, store } = fixture();
    await store.retainRun(actor, id, source);
    const archive = path.join(root, "actor", "runs", id);
    expect(fs.readFileSync(path.join(archive, "events.jsonl.zst"))).toEqual(fs.readFileSync(path.join(source, "events.jsonl.zst")));
    expect(fs.readFileSync(path.join(archive, "status.json"), "utf8")).toBe(fs.readFileSync(path.join(source, "status.json"), "utf8"));
    fs.rmSync(source, { recursive: true, force: true });
    await expect(store.retainRun(actor, id, undefined)).resolves.toBeUndefined();
  });

  it("fails closed on a mandatory archive alias and leaves the compressed source intact", async () => {
    const { root, source, actor, id, store } = fixture();
    const archive = path.join(root, "actor", "runs", id);
    fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
    const victim = path.join(root, "victim-status.json");
    fs.writeFileSync(victim, "victim", { mode: 0o600 });
    fs.symlinkSync(victim, path.join(archive, "status.json"));
    await expect(store.retainRun(actor, id, source)).rejects.toThrow(/Unsafe actor run .*status\.json/);
    expect(fs.readFileSync(path.join(source, "events.jsonl.zst"))).toEqual(zstdCompressSync(Buffer.from("{\"type\":\"message_end\"}\n"), { params: { [constants.ZSTD_c_checksumFlag]: 1 } }));
    expect(fs.readFileSync(victim, "utf8")).toBe("victim");
  });
});
