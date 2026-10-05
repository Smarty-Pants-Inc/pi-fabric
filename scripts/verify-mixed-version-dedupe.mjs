import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const repo = process.cwd();
const oldCommit = "81c0f9f0";
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mixed-dedupe-"));
const root = path.join(scratch, "mesh");
const archive = path.join(scratch, "archive");
const oldTree = path.join(scratch, "old");
fs.mkdirSync(root, { recursive: true });
fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir: archive }));
const from = { id: "session:mixed-version", name: "proof", kind: "main", sessionId: "mixed-version" };
const packet = { topic: "mesh.mixed-version", from, dedupeKey: "mixed-version-dedupe", text: "exact-once" };
const current = await import(pathToFileURL(path.join(repo, "dist", "mesh.js")).href);
const crashCode = "const { MeshStore } = await import(process.argv[1]); const store = new MeshStore(process.argv[2], 1024, 500, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 }); await store.publish(JSON.parse(process.argv[3]));";
try {
  const worktree = spawnSync("git", ["worktree", "add", "--detach", oldTree, oldCommit], { cwd: repo, encoding: "utf8" });
  if (worktree.status !== 0) throw new Error(worktree.stderr || "git worktree add failed");
  const link = spawnSync("ln", ["-s", path.join(repo, "node_modules"), path.join(oldTree, "node_modules")], { encoding: "utf8" });
  if (link.status !== 0) throw new Error(link.stderr || "node_modules link failed");
  const built = spawnSync("bun", ["run", "build"], { cwd: oldTree, env: { ...process.env }, stdio: "inherit" });
  if (built.status !== 0) throw new Error("old-version build failed");
  const old = await import(pathToFileURL(path.join(oldTree, "dist", "mesh.js")).href);
  const crashed = spawnSync(process.execPath, ["--input-type=module", "-e", crashCode, pathToFileURL(path.join(repo, "dist", "mesh.js")).href, root, JSON.stringify(packet)], {
    cwd: repo, env: { ...process.env, PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND: "1" }, encoding: "utf8",
  });
  if (crashed.signal !== "SIGKILL") throw new Error(`crash hook exited unexpectedly: ${crashed.status} ${crashed.stderr}`);
  const oldStore = new old.MeshStore(root, 1024, 500, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
  const original = JSON.parse(fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim());
  const intentName = fs.readdirSync(path.join(root, "event-receipts")).find(name => name.endsWith(".pending.json"));
  if (!intentName) throw new Error("crash did not leave a durable intent");
  for (let index = 0; index < 12; index++) await oldStore.publish({ topic: "mesh.mixed-version", from, text: "old-" + index + "x".repeat(500) });
  const retained = fs.readFileSync(path.join(root, "events.jsonl"), "utf8");
  if (retained.includes(original.id)) throw new Error("old compaction did not drop the original event");
  if (!fs.existsSync(path.join(root, "event-receipts", intentName))) throw new Error("old compactor unexpectedly settled the intent");
  const retryStore = new current.MeshStore(root, 1024, 500, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
  const recovered = await retryStore.publish(packet);
  if (JSON.stringify(recovered) !== JSON.stringify(original)) throw new Error("retry did not return the exact original event");
  const all = retryStore.read({ after: 0, limit: 500 }).filter((event) => event.dedupeKey === packet.dedupeKey);
  if (all.length !== 1 || all[0].id !== recovered.id) throw new Error(`expected exactly one recovered event, found ${all.length}`);
  console.log(JSON.stringify({ oldCommit, recoveredId: recovered.id, sequence: recovered.sequence, dedupeEvents: all.length, originalDroppedByOldCompactor: true, pendingSurvivedOldCompactor: true, liveBytes: fs.statSync(path.join(root, "events.jsonl")).size }));
} finally {
  spawnSync("git", ["worktree", "remove", "--force", oldTree], { cwd: repo, stdio: "ignore" });
  fs.rmSync(scratch, { recursive: true, force: true });
}
