import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorLogStore } from "../src/actors/log-store.js";

// smarty-dev#9016: a host started from a login shell runs under umask 002 (Ubuntu USERGROUPS_ENAB + pam_umask). Run
// directories it created were 0775, and verified retention (pi-fabric#850) refuses group-writable trees, so those
// runs were never collected (2000 on Ryzen 1). Run directories and archived nested runs are now 0700 under any umask.
const roots: string[] = [];
let umask: number | undefined;
afterEach(() => {
  if (umask !== undefined) { process.umask(umask); umask = undefined; }
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const mode = (file: string) => fs.statSync(file).mode & 0o777;

it.skipIf(process.platform === "win32")("creates a run directory 0700 under umask 002 through the real AgentManager spawn", async () => {
  const root = tmp("fabric-run-mode-");
  umask = process.umask(0o002);
  vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => false, waitForClose: async () => {}, stop: async () => {},
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  try {
    const handle = await manager.spawn({ task: "run directory mode", transport: "process", extensions: false });
    expect(mode(manager.runDirectory(handle.id)!)).toBe(0o700);
  } finally { await manager.close(); }
});

it.skipIf(process.platform === "win32")("archives an actor run's nested tree with 0700 directories", async () => {
  const root = tmp("fabric-archive-mode-");
  const source = path.join(root, "source");
  const nested = path.join(source, "nested", "child", "nested", "grandchild");
  fs.mkdirSync(nested, { recursive: true, mode: 0o775 });
  for (const directory of [path.join(source, "nested"), path.join(source, "nested", "child"), path.join(source, "nested", "child", "nested"), nested]) fs.chmodSync(directory, 0o775);
  fs.writeFileSync(path.join(nested, "status.json"), "{}", { mode: 0o644 });
  fs.writeFileSync(path.join(source, "status.json"), "{}");
  const sessionFile = path.join(root, "actor", "session.jsonl");
  fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 7 * 24 * 60 * 60 * 1000 });
  await store.retainRun({ sessionFile }, "run-1", source);
  const archived = path.join(root, "actor", "runs", "run-1", "nested");
  for (const directory of [archived, path.join(archived, "child"), path.join(archived, "child", "nested"), path.join(archived, "child", "nested", "grandchild")]) {
    expect(mode(directory), directory).toBe(0o700);
  }
  expect(mode(path.join(archived, "child", "nested", "grandchild", "status.json"))).toBe(0o644);
});
