import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "successor", name: "successor", kind: "main" };
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("stopped foreign writers and delayed protocol-2 publication", () => {
  it.each([[1, "state"], [2, "state"], [1, "event"], [2, "event"], [2, "staged"]] as const)(
    "protocol %s phase %s denies recovery while stopped, then preserves acknowledged successor state",
    async (lockProtocol, phase) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-paused-foreign-"));
      roots.push(root);
      const store = new MeshStore(root, 65536, 100, { lockProtocol, lockTimeoutMs: 100 });
      await store.put({ key: "state/baseline", value: "acknowledged", identity });
      const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-paused-foreign.mjs"), root, String(lockProtocol), phase],
        { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "", stderr = "";
      child.stdout.on("data", chunk => { stdout += chunk; });
      child.stderr.on("data", chunk => { stderr += chunk; });
      const closed = new Promise<number | null>((resolve, reject) => {
        child.once("error", reject); child.once("close", resolve);
      });
      closed.catch(() => undefined);
      let stopped = false;
      const wait = (name: string) => vi.waitFor(() => expect(fs.existsSync(path.join(root, `${name}.ready`))).toBe(true),
        { timeout: 10_000, interval: 20 });
      const go = (name: string) => fs.writeFileSync(path.join(root, `${name}.go`), "");
      try {
        if (phase === "staged") {
          await wait("before");
          expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
          // A real successor may commit before private publication; its state must
          // be read only after the delayed initializer actually acquires canonical.
          await store.put({ key: "state/before-publication", value: "acknowledged", identity });
          go("before");
        }
        await wait("after");
        process.kill(child.pid!, "SIGSTOP"); stopped = true;
        const ownerPath = path.join(root, ".lock", "owner");
        const owner = fs.readFileSync(ownerPath, "utf8");
        const directory = fs.statSync(path.dirname(ownerPath));
        expect(Date.now() - Math.max(Number(owner.split("\n")[2]), fs.statSync(ownerPath).mtimeMs)).toBeGreaterThan(120_000);
        expect(owner.split("\n")[4]).not.toBe(fs.readlinkSync("/proc/self/ns/pid"));
        // Clock and actual receipt mtime are old, not just a fabricated canonical
        // replacement token. This contender uses the production put/commit path.
        const attempt = phase === "event"
          ? store.publish({ topic: "probe.resume", kind: "probe", from: identity, data: "must not acknowledge yet" })
          : store.put({ key: "state/successor", value: "must not acknowledge yet", identity });
        await expect(attempt).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", message: expect.stringContaining("foreign pid namespace") });
        expect(store.get("state/successor")).toBeUndefined();
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
        expect(fs.statSync(path.dirname(ownerPath)).ino).toBe(directory.ino);
        expect(fs.readdirSync(root).some(name => name.startsWith(".lock.dead."))).toBe(false);
        go("after"); process.kill(child.pid!, "SIGCONT"); stopped = false;
        expect(await closed, stderr).toBe(0);
        expect(JSON.parse(stdout.trim())).toMatchObject({ committed: true });
        await store.put({ key: "state/successor", value: "acknowledged after resume", identity });
        expect(store.get("state/baseline")?.value).toBe("acknowledged");
        if (phase === "event") {
          await store.publish({ topic: "probe.resume", kind: "probe", from: identity, data: "acknowledged after resume" });
          const events = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n")
            .map(line => JSON.parse(line) as { sequence: number; data: string });
          expect(events.map(event => event.sequence)).toEqual([1, 2]);
          expect(events.map(event => event.data)).toEqual(["resumed", "acknowledged after resume"]);
        } else expect(store.get("state/holder")?.value).toBe("resumed");
        expect(store.get("state/successor")?.value).toBe("acknowledged after resume");
        if (phase === "staged") expect(store.get("state/before-publication")?.value).toBe("acknowledged");
      } finally {
        go("before"); go("after");
        if (stopped) process.kill(child.pid!, "SIGCONT");
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        await closed;
      }
    }, 20_000,
  );

  it("protocol 2 rejects a canonical replacement at publication before entering", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-v2-canonical-")); roots.push(root);
    const store = new MeshStore(root, 65536, 100, { lockProtocol: 2 });
    const lock = path.join(root, ".lock");
    const rename = fs.renameSync.bind(fs);
    const successor = `successor\n${process.pid}\n${Date.now()}\n`;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to) === lock) {
        rename(lock, `${lock}.detached-by-test`);
        fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), successor);
      }
    });
    const operation = vi.fn();
    await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
  });
});
