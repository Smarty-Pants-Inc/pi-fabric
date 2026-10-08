import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { StateProjector } from "../src/mesh/state-projector.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Lane L3 of smarty-dev#6477: a projector killed inside an apply transaction loses only that
// transaction; a restarted projector resumes from the last committed generation and converges.
const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { StateProjector } = await jiti.import("./src/mesh/state-projector.ts");
const [root, kind, killAtText] = process.argv.slice(1);
const killAt = Number(killAtText);
const projector = await StateProjector.open({ root, owner: "child", leaseMs: 400, verifyMs: 0, statusMs: 0,
  beforeCommit: (info) => {
    if (info.kind !== kind || info.index !== killAt) return;
    fs.writeSync(1, "dying\\n");
    process.kill(process.pid, "SIGKILL");
  } });
await projector.tick();
fs.writeSync(1, "survived\\n");
`;

interface ChildResult { code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }

const runChild = (root: string, kind: "record" | "snapshot", killAt: number): Promise<ChildResult> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, root, kind, String(killAt)],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
});

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const projectors: StateProjector[] = [];

afterEach(async () => {
  for (const projector of projectors.splice(0)) await projector.stop();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-projector-crash-${label}-`));
  roots.push(root);
  return root;
};

const openProjector = async (root: string, owner: string): Promise<StateProjector> => {
  const projector = await StateProjector.open({ root, owner, verifyMs: 0, statusMs: 0 });
  projectors.push(projector);
  return projector;
};

const meta = (root: string, name: string): unknown => {
  const raw = openNodeSqlite(path.join(root, "state.db"));
  try { return raw.prepare("SELECT value FROM meta WHERE name = ?").get(name)?.value; }
  finally { raw.close(); }
};

const expectInStep = async (root: string): Promise<void> => {
  const file = JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")) as {
    entries: Record<string, unknown>; versions?: Record<string, number>; tombstoneOrder?: string[]; highWater: number;
  };
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
  try {
    const projected = store.exportState();
    expect(projected.entries).toEqual(file.entries);
    expect(projected.versions).toEqual(file.versions ?? {});
    expect(projected.tombstoneOrder).toEqual(file.tombstoneOrder ?? []);
    expect(projected.highWater).toBe(file.highWater);
  } finally { store.close(); }
};

const write = async (store: MeshStore, count: number): Promise<void> => {
  for (let step = 0; step < count; step += 1) {
    if (step % 3 === 2) await store.delete({ key: `k/${step - 2}` });
    else await store.put({ key: `k/${step}`, value: { step }, identity });
  }
};

const waitForLease = async (projector: StateProjector): Promise<void> => {
  const deadline = Date.now() + 5_000;
  while ((await projector.tick()).role !== "active") {
    if (Date.now() > deadline) throw new Error("the crashed projector's lease never expired");
    await new Promise(resolve => setTimeout(resolve, 50));
  }
};

describe("state projector crash", () => {
  it("SIGKILL inside a generation's transaction: restart resumes at the last committed generation", async () => {
    const root = tempRoot("record");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 2 });
    await file.put({ key: "k/base", value: 0, identity });
    const seed = await openProjector(root, "seed");
    const seeded = await seed.tick();
    await seed.stop();
    await write(file, 7);

    // Killed while applying the 4th of 7 generations, after three committed.
    const child = await runChild(root, "record", 3);
    expect(child.stderr).toBe("");
    expect(child.signal).toBe("SIGKILL");
    expect(child.stdout).toBe("dying\n");
    expect(Number(meta(root, "commit_no"))).toBe(seeded.commit + 3);
    expect(JSON.parse(String(meta(root, "projector.lease")))).toMatchObject({ owner: "child" });

    // The crashed holder's lease blocks a new projector until it expires; then it converges.
    const restarted = await openProjector(root, "restarted");
    expect((await restarted.tick()).role).toBe("standby");
    await waitForLease(restarted);
    expect(restarted.status()).toMatchObject({ role: "active", appliedGenerations: 4, fullResyncs: 0, commit: seeded.commit + 7 });
    await expectInStep(root);
    expect(await restarted.verify()).toEqual([]);
  }, 60_000);

  it("SIGKILL inside the initial full import leaves an empty database that a restart imports", async () => {
    const root = tempRoot("snapshot");
    const file = new MeshStore(root, 64 * 1024, 1_000, { maxStateTombstones: 2 });
    await write(file, 9);
    const child = await runChild(root, "snapshot", 0);
    expect(child.signal).toBe("SIGKILL");
    expect(Number(meta(root, "commit_no"))).toBe(0);
    expect(meta(root, "projector.progress")).toBeUndefined();

    const restarted = await openProjector(root, "restarted");
    await waitForLease(restarted);
    expect(restarted.status()).toMatchObject({ role: "active", fullResyncs: 1, lastResync: { reason: "initial" } });
    await expectInStep(root);
    await write(file, 3);
    expect(await restarted.tick()).toMatchObject({ appliedGenerations: 3, fullResyncs: 1 });
    await expectInStep(root);
  }, 60_000);
});
