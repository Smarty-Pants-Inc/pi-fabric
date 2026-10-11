import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { lockTokenLocality, SHORT_LOCK_MAX_HOLD_MS } from "../src/core/atomic-write.js";

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
});
