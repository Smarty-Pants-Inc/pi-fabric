import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshCustodyUnrecoverableError, acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("ownerless stale-mtime release review", () => {
  it("does not reclaim a live release just because the directory mtime predates owner unlink", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-live-ownerless-mtime-")); roots.push(root);
    const lock = path.join(root, "custody.lock");
    const holder = spawn(process.execPath, ["--input-type=module", "-e", `
      import fs from "node:fs";
      import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      const root = process.argv.at(-1);
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { acquireMeshCustodyLock } = await jiti.import("./src/mesh/custody-lock.ts");
      const lock = root + "/custody.lock";
      const unlock = await acquireMeshCustodyLock(root, 1000, { hostQualified: true, ownIncarnation: undefined });
      const rmdir = fs.rmdirSync;
      let paused = false;
      fs.rmdirSync = function (directory, ...args) {
        if (!paused && String(directory) === lock) {
          paused = true;
          process.send({ phase: "owner-unlinked-before-rmdir", pid: process.pid });
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2500);
        }
        return rmdir.call(this, directory, ...args);
      };
      process.send({ phase: "held", pid: process.pid });
      unlock();
      process.send({ phase: "released" });
    `, root], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
    let stderr = "";
    holder.stderr!.on("data", chunk => { stderr += chunk; });
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      holder.once("error", reject);
      holder.once("close", (code, signal) => resolve({ code, signal }));
    });
    exited.catch(() => undefined);
    const phases = new Set<string>();
    let successorRelease: (() => void) | undefined;
    holder.on("message", message => { phases.add((message as { phase?: string }).phase ?? ""); });
    const waitFor = (phase: string): Promise<void> => new Promise((resolve, reject) => {
      if (phases.has(phase)) { resolve(); return; }
      const timer = setTimeout(() => reject(new Error(`holder did not reach ${phase}: ${stderr}`)), 10_000);
      const onMessage = (message: unknown) => {
        if ((message as { phase?: string }).phase === phase) { clearTimeout(timer); resolve(); }
      };
      holder.on("message", onMessage);
      holder.once("close", () => { clearTimeout(timer); reject(new Error(`holder exited before ${phase}: ${stderr}`)); });
    });
    try {
      await waitFor("held");
      await waitFor("owner-unlinked-before-rmdir");
      expect(holder.exitCode).toBeNull();
      expect(fs.existsSync(lock)).toBe(true);
      expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
      expect(() => process.kill(holder.pid!, 0)).not.toThrow();

      // Model an unchanged/coarse directory mtime: owner unlink did not refresh the parent's mtime.
      const old = new Date(Date.now() - 2 * 30_000);
      fs.utimesSync(lock, old, old);
      let observedError: unknown;
      try {
        successorRelease = await acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined });
      } catch (error) { observedError = error; }
      expect(observedError).toBeInstanceOf(MeshCustodyUnrecoverableError);
      expect(successorRelease).toBeUndefined();
      expect(holder.exitCode).toBeNull();
      expect(fs.existsSync(lock)).toBe(true);
      expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
    } finally {
      successorRelease?.();
      const result = await Promise.race([exited, new Promise<{ code: null; signal: null }>(resolve => setTimeout(() => resolve({ code: null, signal: null }), 3_000))]);
      if (result.code === null && result.signal === null) holder.kill("SIGKILL");
      await exited;
    }
  }, 15_000);
});
