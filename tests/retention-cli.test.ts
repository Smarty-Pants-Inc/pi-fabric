import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";
import { lockFile } from "../src/residency/file-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const snapshot = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? snapshot(file) : stat.isSymbolicLink() ? fs.readlinkSync(file) : fs.readFileSync(file, "utf8")];
});
const make = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-sweep-")); roots.push(root);
  const host = path.join(root, "residency", "host");
  write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
  write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:proof", residencyRoot: host, meshRoot: root }));
  const actor = path.join(root, "actors", "project", "actor");
  write(path.join(root, "actors", "project", "actors.json"), JSON.stringify({ actors: [{ id: "actor", rootId: "session:proof", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" }] }));
  const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
  for (const run of [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive"), path.join(actor, "runs", "latest")]) {
    write(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1 }));
    write(path.join(run, "events.jsonl"), log);
    write(path.join(run, "reply.json"), '{"text":"keep"}');
  }
  write(path.join(actor, "session.jsonl.20260927T150000000Z.bak"), "old");
  write(path.join(actor, "session.jsonl.20260928T150000000Z.bak"), "new");
  write(path.join(actor, "session.jsonl"), "live");
  return { root, host, actor };
};

describe.skipIf(process.platform !== "linux")("offline retained mesh sweep", () => {
  it("lists exact bytes with a whole-tree no-op dry run, then applies under existing native fences", async () => {
    const { root, actor, host } = make(); const before = snapshot(root);
    const plan = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(snapshot(root)).toEqual(before);
    expect(plan.skipped).toEqual([]);
    expect(plan.changes).toHaveLength(3);
    expect(plan.bytesBefore).toBeGreaterThan(plan.bytesAfter);
    const applied = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false });
    expect(applied.changes).toEqual(plan.changes);
    for (const run of [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive")]) {
      expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(201);
      expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"keep"}');
    }
    expect(fs.readFileSync(path.join(actor, "runs", "latest", "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(401);
    expect(fs.readdirSync(actor).filter(name => name.endsWith(".bak"))).toEqual(["session.jsonl.20260928T150000000Z.bak"]);
  });

  it("does not displace a kernel-fenced holder even with stale dead-PID diagnostics", async () => {
    const { root, host } = make(); const fd = await lockFile(path.join(host, "host.lock"), 0, true);
    try {
      const before = snapshot(root);
      expect((await sweepMeshRetention(root, { dryRun: false })).changes).toEqual([]);
      expect(snapshot(root)).toEqual(before);
    } finally { fs.closeSync(fd); }
  });

  it("refuses ambiguous identities and linked actor-reference trees without changing them", async () => {
    const { root, host } = make(); const second = path.join(root, "residency", "second");
    write(path.join(second, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(second, "config.json"), JSON.stringify({ format: 1, rootId: "session:proof", residencyRoot: second, meshRoot: root }));
    let before = snapshot(root);
    expect((await sweepMeshRetention(root, { dryRun: false })).changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    fs.symlinkSync(path.join(root, "actors", "project"), path.join(root, "actors", "linked"), "dir"); before = snapshot(root);
    await expect(sweepMeshRetention(root, { dryRun: false })).rejects.toThrow(/uncertain/);
    expect(snapshot(root)).toEqual(before);
    expect(fs.existsSync(path.join(host, "runs", "terminal"))).toBe(true);
  });

  it.each(["live", "missing lock", "unknown lock", "unknown owner", "unknown registry", "unknown config"])("never changes a mesh with %s custody", async kind => {
    const { root, host } = make();
    if (kind === "live") write(path.join(host, "host.lock"), JSON.stringify({ pid: process.pid }));
    if (kind === "missing lock") fs.unlinkSync(path.join(host, "host.lock"));
    if (kind === "unknown lock") write(path.join(host, "host.lock"), "{");
    if (kind === "unknown owner") write(path.join(host, "owner.json"), "{}");
    if (kind === "unknown config") write(path.join(host, "config.json"), "{}");
    if (kind === "unknown registry") write(path.join(root, "actors", "project", "actors.json"), "{");
    const before = snapshot(root);
    const result = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false });
    expect(result.changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    expect(result.skipped.length).toBeGreaterThan(0);
  });
});
