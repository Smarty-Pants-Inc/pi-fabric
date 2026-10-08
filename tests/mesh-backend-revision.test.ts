import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBatchConflictError, MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshStateUnsupportedError, SqliteStateStore } from "../src/mesh/state-sqlite.js";

const identity: MeshIdentity = { id: "mesh-backend-test", name: "mesh-backend-test", kind: "agent" };
const roots: string[] = [];
const temporary = (kind: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-backend-${kind}-`)); roots.push(root); return root; };
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

// L2a implementations can be added without changing these cross-lane probes. File is mandatory;
// SQLite is included only when this runtime supports node:sqlite. Shadow is explicitly reported as
// unavailable on this interface-only base (it will be picked up when a factory is introduced).
type RevisionBackend = { kind: string; get(key: string): { value: unknown; version: number } | undefined; put(input: { key: string; value: unknown; identity: MeshIdentity; ifVersion?: number }): Promise<unknown>; close(): void };
async function openBackends(): Promise<Array<{ kind: string; open: () => Promise<RevisionBackend> }>> {
  const kinds: Array<{ kind: string; open: () => Promise<RevisionBackend> }> = [{ kind: "file", open: async () => { const store = new MeshStore(temporary("file"), 64 * 1024, 100); return { kind: "file", get: key => store.get(key), put: input => store.put(input), close: () => undefined }; } }];
  try {
    const probeRoot = temporary("sqlite-probe");
    const probe = await SqliteStateStore.open(probeRoot, 64 * 1024, 100, { lockTimeoutMs: 1_000 });
    probe.close();
    kinds.push({ kind: "sqlite", open: async () => { const store = await SqliteStateStore.open(temporary("sqlite"), 64 * 1024, 100, { lockTimeoutMs: 10_000 }); return { kind: "sqlite", get: key => store.get(key), put: input => store.put(input), close: () => store.close() }; } });
  } catch (error) { if (!(error instanceof MeshStateUnsupportedError) && (error as NodeJS.ErrnoException).code !== "ERR_UNKNOWN_BUILTIN_MODULE") throw error; }
  return kinds;
}

const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const [{ MeshStore }, { SqliteStateStore }, { MeshBatchConflictError }] = await Promise.all([jiti.import("./src/mesh/store.ts"), jiti.import("./src/mesh/state-sqlite.ts"), jiti.import("./src/mesh/state-file.ts")]);
const [kind, root, worker] = process.argv.slice(1);
const identity = { id: "worker-" + worker, name: "worker", kind: "agent" };
const store = kind === "file" ? new MeshStore(root, 65536, 100) : await SqliteStateStore.open(root, 65536, 100, { lockTimeoutMs: 30000 });
const key = "fence/revision";
const initial = store.get(key)?.version ?? 0;
for (let n = 0; n < 30; n++) {
  for (;;) { const entry = store.get(key); const version = entry?.version ?? 0; try { await store.put({ key, value: Number(entry?.value ?? 0) + 1, identity, ifVersion: version }); break; } catch (error) { if (!(error instanceof MeshBatchConflictError)) throw error; } }
}
let staleRejected = false;
try { await store.put({ key, value: -1, identity, ifVersion: initial }); } catch (error) { if (!(error instanceof MeshBatchConflictError)) throw error; staleRejected = true; }
if (!staleRejected) throw new Error("stale revision was accepted");
store.close?.();
`;

function runWorker(kind: string, root: string, worker: number): Promise<{ code: number | null; signal: NodeJS.Signals | null; output: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, kind, root, String(worker)], { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let output = ""; child.stdout.on("data", chunk => { output += chunk; }); child.stderr.on("data", chunk => { output += chunk; });
    const timer = setTimeout(() => child.kill("SIGKILL"), 50_000);
    child.once("error", reject); child.once("close", (code, signal) => { clearTimeout(timer); resolve({ code, signal, output }); });
  });
}

describe("mesh backend Linux cross-process revision fence (R5)", () => {
  it("never loses concurrent CAS increments and never accepts a stale revision", async () => {
    const backends = await openBackends();
    for (const backend of backends) {
      const root = temporary(`${backend.kind}-writers`);
      const store = await backend.open();
      try { await store.put({ key: "fence/revision", value: 0, identity }); } finally { store.close(); }
      const outcomes = await Promise.all(Array.from({ length: 4 }, (_, worker) => runWorker(backend.kind, root, worker)));
      for (const result of outcomes) expect(result, `${backend.kind}: ${result.output}`).toMatchObject({ code: 0, signal: null });
      const verify = await backend.open();
      try { expect(verify.get("fence/revision")).toMatchObject({ value: 120 }); } finally { verify.close(); }
    }
    // Shadow is dynamically optional; keep the reason visible in test output on this L2a interface base.
    if (!backends.some(backend => backend.kind === "shadow")) console.info("SKIP shadow: no shadow StateBackend implementation is present");
  }, 80_000);
});
