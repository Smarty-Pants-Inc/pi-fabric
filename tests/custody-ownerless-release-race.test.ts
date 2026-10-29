import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshCustodyUnrecoverableError, acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("custody ownerless release interleaving", () => {
  it("keeps an ownerless canonical directory busy while its live holder is between unlink and rmdir", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-ownerless-release-")); roots.push(root);
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
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1200);
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
    const messages: Array<{ phase: string; pid: number }> = [];
    holder.on("message", message => { messages.push(message as { phase: string; pid: number }); });
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`holder did not reach release seam: ${stderr}`)), 10_000);
        const onMessage = (message: unknown) => {
          if ((message as { phase?: string }).phase === "owner-unlinked-before-rmdir") {
            clearTimeout(timer); resolve();
          }
        };
        holder.on("message", onMessage);
        holder.once("close", () => { clearTimeout(timer); reject(new Error(`holder exited early: ${stderr}`)); });
      });
      expect(holder.exitCode).toBeNull();
      expect(fs.existsSync(lock)).toBe(true);
      expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);

      await expect(acquireMeshCustodyLock(root, 250, { hostQualified: true, ownIncarnation: undefined }))
        .rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
      expect(holder.exitCode).toBeNull();
      expect(() => process.kill(holder.pid!, 0)).not.toThrow();
      expect(messages.some(message => message.phase === "owner-unlinked-before-rmdir" && message.pid === holder.pid)).toBe(true);
    } finally {
      const result = await Promise.race([exited, new Promise<{ code: null; signal: null }>(resolve => setTimeout(() => resolve({ code: null, signal: null }), 3_000))]);
      if (result.code === null && result.signal === null) holder.kill("SIGKILL");
      await exited;
    }
  }, 15_000);

  it("refuses an old ownerless directory pending operator recovery instead of inferring grace", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-ownerless-stale-")); roots.push(root);
    const lock = path.join(root, "custody.lock"); fs.mkdirSync(lock);
    const old = new Date(Date.now() - 2 * 30_000);
    fs.utimesSync(lock, old, old);

    await expect(acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined }))
      .rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.readdirSync(lock)).toEqual([]);
  });

  it("does not rmdir a successor published after the stale ownerless inode check", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-ownerless-successor-")); roots.push(root);
    const lock = path.join(root, "custody.lock");
    fs.mkdirSync(lock);
    const old = new Date(Date.now() - 2 * 30_000);
    fs.utimesSync(lock, old, old);
    await expect(acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined }))
      .rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.readdirSync(lock)).toEqual([]);
  });
});
