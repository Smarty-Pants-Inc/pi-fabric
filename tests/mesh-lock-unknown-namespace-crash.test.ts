import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { currentOwnerIdentity, lockTokenLocality, ownerLocalityTag, SHORT_LOCK_MAX_HOLD_MS } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

// PR #864 P1: a holder whose PID namespace is unknown dies holding the mesh lock. Its locality
// stays foreign (ESRCH is not proof there), so only its heartbeat, the lock's creation time,
// can release it: a new process must acquire once the stale threshold has passed.
let root: string;
let bundle: string;
beforeAll(async () => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-unknown-ns-crash-"));
  bundle = path.join(root, "probe.mjs");
  await build({ entryPoints: ["tests/fixtures/mesh-lock-unknown-namespace-crash.ts"], outfile: bundle, bundle: true,
    platform: "node", format: "esm", packages: "external" });
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "dir");
});
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));
afterEach(() => { vi.restoreAllMocks(); });

const probe = (mesh: string, ...args: string[]): Promise<{ code: number | null; signal: string | null; output: string }> =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [bundle, mesh, ...args], { env: process.env });
    let output = "";
    child.stdout.on("data", chunk => { output += chunk; });
    child.stderr.on("data", chunk => { output += chunk; });
    const timeout = setTimeout(() => child.kill("SIGKILL"), 15_000);
    child.on("error", reject);
    child.on("close", (code, signal) => { clearTimeout(timeout); resolve({ code, signal, output }); });
  });

describe.skipIf(process.platform !== "linux")("mesh lock: unknown-namespace holder crash", () => {
  it("is refused while its heartbeat is fresh and acquired by a new process after the stale threshold", async () => {
    const mesh = path.join(root, "mesh");
    const held = await probe(mesh, "hold");
    expect(held.signal, held.output).toBe("SIGKILL");
    const owner = JSON.parse(/HELD (.*)/.exec(held.output)?.[1] ?? "null") as string;
    const [token, pidText] = owner.split("\n");
    // The holder's token names its namespace as unknown; to this reader it is foreign.
    expect(token).toMatch(/^[0-9a-f-]{36}-[0-9a-f]{6}-{6}[0-9a-f-]{6}$/);
    expect(lockTokenLocality(token)).toBe("foreign");
    // The holder is really dead: the local probe says ESRCH, which must not decide anything.
    expect(() => process.kill(Number(pidText), 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    const ownerPath = path.join(mesh, ".lock", "owner");

    const fresh = await probe(mesh, "acquire", "0");
    expect(fresh.output, fresh.output).toContain("REFUSED FABRIC_MESH_LOCK_TIMEOUT");
    expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);

    const stale = await probe(mesh, "acquire", String(SHORT_LOCK_MAX_HOLD_MS + 5_000));
    expect(stale.output, stale.output).toContain("ACQUIRED");
    expect(stale.code, stale.output).toBe(0);
    expect(fs.readdirSync(mesh).filter(name => name.startsWith(".lock.dead."))).toHaveLength(1);
    expect(fs.existsSync(ownerPath)).toBe(false);
  }, 60_000);

  it("two concurrent recoverers of one stale unknown-namespace owner reap it once; the paused one fails closed", async () => {
    const mesh = path.join(root, "race");
    const lockPath = path.join(mesh, ".lock");
    const store = new MeshStore(mesh, 64 * 1024, 100, { lockTimeoutMs: 500 });
    const self = currentOwnerIdentity();
    const token = `${randomUUID()}-${ownerLocalityTag({ hostname: self.hostname, ...(self.bootId === undefined ? {} : { bootId: self.bootId }) })}`;
    const deadPid = spawnSync(process.execPath, ["-e", ""]).pid!;
    const stale = `${token}\n${deadPid}\n${Date.now() - SHORT_LOCK_MAX_HOLD_MS - 5_000}\n`;
    fs.mkdirSync(lockPath, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), stale);
    const rename = fs.renameSync;
    let paused = false;
    let other: SpawnSyncReturns<string> | undefined;
    let pausedError: NodeJS.ErrnoException | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (!paused && String(from) === lockPath) {
        // This recoverer judged the stale owner dead and passed its inode/owner re-read. While it
        // is paused at the rename, another process recovers the same owner and takes the lock.
        paused = true;
        other = spawnSync(process.execPath, [bundle, mesh, "hold", "1500"], { encoding: "utf8", timeout: 30_000 });
        try { rename(from, to); } catch (error) { pausedError = error as NodeJS.ErrnoException; throw error; }
        return;
      }
      rename(from, to);
    });
    const error = await store.exclusive(() => "stolen").catch((caught: unknown) => caught);
    vi.restoreAllMocks();
    const output = `${other?.stdout ?? ""}${other?.stderr ?? ""}`;
    expect(other?.signal, output).toBe("SIGKILL");
    const successor = JSON.parse(/HELD (.*)/.exec(other?.stdout ?? "")?.[1] ?? "null") as string;
    expect(successor.startsWith(token)).toBe(false);
    // The paused rename met the identity-bound fence and failed closed: the successor keeps its lock.
    expect(["ENOTEMPTY", "EEXIST"]).toContain(pausedError?.code);
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(successor);
    const fences = fs.readdirSync(mesh).filter(name => name.startsWith(".lock.dead."));
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(mesh, fences[0]!, "owner"), "utf8")).toBe(stale);
  }, 60_000);
});
