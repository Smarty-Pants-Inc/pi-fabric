import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openAsyncMeshStateStore } from "../src/mesh/state-async.js";

const servers = process.env.FABRIC_NATS_TEST_SERVERS;
const deadline = async <T>(promise: Promise<T>, ms: number): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Worker deadline spent")), ms); })]); }
  finally { clearTimeout(timer); }
};
for (const kind of ["file", "sqlite", "nats-kv"] as const) {
  describe.skipIf(kind === "nats-kv" && !servers)(`${kind} eight independent CAS processes`, () => {
    it("has no lost updates or reused successful fencing revisions", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-eight-process-"));
      const store = await openAsyncMeshStateStore(root, kind === "nats-kv"
        ? { backend: kind, nats: { servers: servers!, experimentalNatsKv: true } } : { backend: kind });
      const workers: ReturnType<typeof startWorker>[] = [];
      function startWorker() {
        const child = spawn(process.env.FABRIC_TEST_BUN ?? "bun", ["scripts/state-cas-worker.ts", kind, root, "counter/shared", "25"],
          { cwd: process.cwd(), env: process.env, stdio: ["pipe", "pipe", "pipe"] });
        let stdout = "", stderr = "";
        let announce!: () => void;
        let refuse!: (error: Error) => void;
        const ready = new Promise<void>((resolve, reject) => { announce = resolve; refuse = reject; });
        ready.catch(() => undefined);
        const done = new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
          child.once("error", error => { refuse(error); reject(error); });
          child.once("close", code => { refuse(new Error(`Worker closed (${code}) before READY: ${stderr}`)); resolve({ code, stdout, stderr }); });
        });
        done.catch(() => undefined);
        child.stdout.on("data", data => { stdout += data; if (stdout.includes("READY\n")) announce(); });
        child.stderr.on("data", data => { stderr += data; });
        return { child, ready, done };
      }
      try {
        await store.put({ key: "counter/shared", value: 0, identity: { id: "parent", name: "parent", kind: "agent" } });
        for (let n = 0; n < 8; n++) workers.push(startWorker());
        await deadline(Promise.all(workers.map(worker => worker.ready)), 30_000);
        for (const worker of workers) worker.child.stdin.end("start\n");
        const results = await deadline(Promise.all(workers.map(worker => worker.done)), 45_000);
        for (const result of results) expect(result.code, result.stderr).toBe(0);
        const parsed = results.map(result => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { pid: number; count: number; conflicts: number; revisions: number[] });
        expect(new Set(parsed.map(result => result.pid)).size).toBe(8);
        expect(parsed.reduce((sum, result) => sum + result.count, 0)).toBe(200);
        expect(new Set(parsed.flatMap(result => result.revisions)).size).toBe(200);
        expect((await store.get("counter/shared"))?.value).toBe(200);
        const evidence = process.env.FABRIC_NATS_EVIDENCE_DIR;
        if (evidence) fs.writeFileSync(path.join(evidence, `${kind}-eight-process.json`), JSON.stringify({ kind, final: await store.get("counter/shared"), workers: parsed }, null, 2) + "\n");
      } finally {
        for (const worker of workers) if (worker.child.exitCode === null) worker.child.kill("SIGTERM");
        await Promise.allSettled(workers.map(worker => worker.done));
        await store.close(); fs.rmSync(root, { recursive: true, force: true });
      }
    }, 90_000);
  });
}
