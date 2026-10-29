import fs from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";

const roots: string[] = [];
const children: ChildProcess[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await new Promise<void>(resolve => child.exitCode !== null || child.signalCode !== null ? resolve() : child.once("close", () => resolve()));
  }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const rootDir = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "custody-ownerless-recovery-"));
  fs.chmodSync(root, 0o700);
  roots.push(root);
  return root;
};
const invoke = async (root: string): Promise<{ code: number; stdout: string; stderr: string }> => {
  let stdout = "", stderr = "";
  const code = await main(["lock-recover", "--root", root, "--lock", "custody", "--ownerless"], {
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
  });
  return { code, stdout, stderr };
};

const startPausedRelease = (root: string, pauseMs: number): ChildProcess => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", `
    import fs from "node:fs";
    import { createJiti } from "jiti";
    import { pathToFileURL } from "node:url";
    const root = process.argv.at(-1);
    const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
    const { acquireMeshCustodyLock } = await jiti.import("./src/mesh/custody-lock.ts");
    const lock = root + "/custody.lock";
    const release = await acquireMeshCustodyLock(root, 1000, { hostQualified: true, ownIncarnation: undefined });
    const realRmdir = fs.rmdirSync;
    let paused = false;
    fs.rmdirSync = function (name, ...args) {
      if (!paused && String(name) === lock) {
        paused = true;
        process.send({ phase: "owner-unlinked-before-rmdir", pid: process.pid });
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ${pauseMs});
      }
      return realRmdir.call(this, name, ...args);
    };
    process.on("message", message => {
      if (message === "again") {
        try { release(); process.send({ phase: "old-release-again" }); }
        catch (error) { process.send({ phase: "old-release-threw", error: String(error) }); }
      }
      if (message === "exit") process.exit(0);
    });
    process.send({ phase: "held", pid: process.pid });
    release();
    process.send({ phase: "released" });
  `, root], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  children.push(child);
  return child;
};

const waitPhase = (child: ChildProcess, phase: string): Promise<Record<string, unknown>> => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error(`timed out waiting for ${phase}`)), 10_000);
  const onMessage = (message: unknown) => {
    if ((message as { phase?: string })?.phase !== phase) return;
    clearTimeout(timer);
    child.off("message", onMessage);
    resolve(message as Record<string, unknown>);
  };
  child.on("message", onMessage);
  child.once("close", code => { clearTimeout(timer); reject(new Error(`holder exited ${code} before ${phase}`)); });
});

const waitClose = (child: ChildProcess): Promise<void> => new Promise(resolve => {
  if (child.exitCode !== null || child.signalCode !== null) resolve();
  else child.once("close", () => resolve());
});

describe("ownerless custody recovery command", () => {
  it("refuses a live holder paused after owner unlink; its resumed and repeated release cannot remove a successor", async () => {
    const root = rootDir();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    const holder = startPausedRelease(root, 3_500);
    await waitPhase(holder, "owner-unlinked-before-rmdir");
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
    expect(fs.readdirSync(lock)).toEqual([]);

    const result = await invoke(root);
    expect(result.code).toBe(3);
    expect(result.stderr).toContain("release holder pid");
    expect(result.stderr).toContain("is still live; nothing was recovered");
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.readdirSync(lock)).toEqual([]);

    await waitPhase(holder, "released");
    expect(fs.existsSync(lock)).toBe(false);
    const releaseNew = await acquireMeshCustodyLock(root, 1_000, { hostQualified: true, ownIncarnation: undefined });
    const successor = fs.readFileSync(path.join(lock, "owner"), "utf8");
    holder.send("again");
    await waitPhase(holder, "old-release-again");
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
    releaseNew();
    holder.send("exit");
    await waitClose(holder);
  }, 15_000);

  it("refuses atomically if a new owner is published after the final identity check", async () => {
    const root = rootDir();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    const holder = startPausedRelease(root, 20_000);
    await waitPhase(holder, "owner-unlinked-before-rmdir");
    holder.kill("SIGKILL");
    await waitClose(holder);
    const successor = `successor\n${process.pid}\n${Date.now()}\n`;
    const realRmdir = fs.rmdirSync.bind(fs);
    let published = false;
    vi.spyOn(fs, "rmdirSync").mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (!published && String(target) === lock) {
        published = true;
        const staging = `${lock}.staging`;
        fs.mkdirSync(staging, { mode: 0o700 });
        fs.writeFileSync(path.join(staging, "owner"), successor, { mode: 0o600 });
        fs.renameSync(staging, lock); // model a normal acquirer publishing its complete staged owner
      }
      return realRmdir(target);
    }) as typeof fs.rmdirSync);

    const result = await invoke(root);
    expect(result.code).toBe(3);
    expect(result.stderr).toContain("could not be atomically removed");
    expect(published).toBe(true);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
  }, 15_000);

  it("recovers a genuinely dead release receipt after grace and permits reacquisition", async () => {
    const root = rootDir();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    const holder = startPausedRelease(root, 20_000);
    await waitPhase(holder, "owner-unlinked-before-rmdir");
    holder.kill("SIGKILL");
    await waitClose(holder);
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
    expect(fs.readdirSync(lock)).toEqual([]);

    const result = await invoke(root);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`lock-recover done (ownerless): ${lock} removed after 2000 ms grace`);
    expect(fs.existsSync(lock)).toBe(false);
    const audit = fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8");
    expect(JSON.parse(audit.trim())).toMatchObject({ mode: "ownerless", lock, releasePid: holder.pid });
    const release = await acquireMeshCustodyLock(root, 1_000, { hostQualified: true, ownIncarnation: undefined });
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(true);
    release();
    expect(fs.existsSync(lock)).toBe(false);
  }, 15_000);

  it("writes one principal-visible alarm with the exact lock path and recovery command", async () => {
    vi.stubEnv("SMARTY_ROLE", "org-agent-test");
    const root = rootDir();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    fs.mkdirSync(lock, { mode: 0o700 });
    const output: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array) => {
      output.push(String(chunk)); return true;
    }) as typeof process.stderr.write);
    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(acquireMeshCustodyLock(root, 0, { hostQualified: true, ownIncarnation: undefined }))
        .rejects.toMatchObject({ code: "FABRIC_MESH_CUSTODY_UNRECOVERABLE" });
    }
    const command = `fabric-mesh-backend lock-recover --root ${root} --lock custody --ownerless`;
    expect(output).toEqual([`[pi-fabric] ALARM FABRIC_MESH_CUSTODY_UNRECOVERABLE: Fabric custody-unrecoverable: ${lock} has no valid owner pid; recovery refused; recover a confirmed dead release with: ${command}\n`]);
  });
});
