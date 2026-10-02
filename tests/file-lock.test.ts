import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { withExclusiveFileLock, withExclusiveFileLockAsync } from "../src/core/file-lock.js";

const directories: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
const fixture = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-file-lock-"));
  directories.push(directory);
  return { directory, lock: path.join(directory, "current.lock"), options: {
    directory, lockName: "current.lock", timeoutMessage: "test lock timeout", attempts: 3, delayMs: 1,
  } };
};

describe.each(["sync", "async"] as const)("shared %s file-lock recovery", mode => {
  const run = (options: ReturnType<typeof fixture>["options"], operation: () => string) => mode === "sync"
    ? Promise.resolve().then(() => withExclusiveFileLock(options, operation))
    : withExclusiveFileLockAsync(options, operation);

  it("honors a live reaper marker before admitting any operation", async () => {
    const { lock, options } = fixture();
    const marker = `${lock}.reap-${process.pid}-test`;
    fs.mkdirSync(marker);
    const operation = vi.fn(() => "admitted");
    await expect(run(options, operation)).rejects.toThrow("test lock timeout");
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(marker)).toBe(true);
    fs.rmdirSync(marker);
    await expect(run(options, operation)).resolves.toBe("admitted");
  });

  it("restores and reaps a stale lock left inside a dead reaper marker", async () => {
    const { lock, options } = fixture();
    const marker = `${lock}.reap-999999999-test`;
    fs.mkdirSync(path.join(marker, "lock"), { recursive: true });
    fs.writeFileSync(path.join(marker, "lock", "owner"), `stale\n999999999\n${Date.now() - 60_000}\n`);
    const kill = process.kill;
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === 999999999) throw Object.assign(new Error("synthetic dead PID"), { code: "ESRCH" });
      return kill(pid, signal);
    });
    await expect(run(options, () => "admitted")).resolves.toBe("admitted");
    expect(fs.existsSync(marker)).toBe(false);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("recovers after exhausted marker sealing retries and one-shot removal failure", async () => {
    const { lock, options } = fixture();
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `stale\n999999999\n${Date.now() - 60_000}\n`);
    const kill = process.kill;
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === 999999999) throw Object.assign(new Error("synthetic dead PID"), { code: "ESRCH" });
      return kill(pid, signal);
    });
    const rename = fs.renameSync;
    let sealFailures = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (sealFailures < 8 && String(target).includes(".reap-done-")) {
        sealFailures++;
        throw Object.assign(new Error("transient seal failure"), { code: "EACCES" });
      }
      rename(source, target);
    });
    const remove = fs.rmdirSync;
    let removalFailed = false;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory, ...args) => {
      if (!removalFailed && String(directory).includes(".reap-")) {
        removalFailed = true;
        throw Object.assign(new Error("transient marker removal failure"), { code: "EACCES" });
      }
      remove(directory, ...args);
    });
    await expect(run(options, () => "admitted")).resolves.toBe("admitted");
    expect(sealFailures).toBe(8);
    expect(removalFailed).toBe(true);
    expect(fs.readdirSync(options.directory).filter(name => name.includes(".reap-"))).toEqual([]);
  });

  it("recovers an aged empty owner obstructing a dead reaper's claim", async () => {
    const { lock, options } = fixture();
    const marker = `${lock}.reap-999999999-obstructed`;
    fs.mkdirSync(path.join(marker, "lock"), { recursive: true });
    fs.writeFileSync(path.join(marker, "lock", "owner"), `stale\n999999999\n${Date.now() - 60_000}\n`);
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), "");
    const past = new Date(Date.now() - 60_000);
    fs.utimesSync(lock, past, past);
    const kill = process.kill;
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === 999999999) throw Object.assign(new Error("synthetic dead PID"), { code: "ESRCH" });
      return kill(pid, signal);
    });
    await expect(run(options, () => "admitted")).resolves.toBe("admitted");
    expect(fs.existsSync(marker)).toBe(false);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("never reaps an owner whose liveness is unknown", async () => {
    const { lock, options } = fixture();
    fs.mkdirSync(lock);
    const bytes = `unknown\n12345\n${Date.now() - 60_000}\n`;
    fs.writeFileSync(path.join(lock, "owner"), bytes);
    vi.spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("permission denied"), { code: "EPERM" });
    });
    await expect(run(options, () => "must not admit")).rejects.toThrow("test lock timeout");
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(bytes);
  });

  it("withdraws a provisional token if a reaper appears during owner publication", async () => {
    const { lock, options } = fixture();
    const owner = path.join(lock, "owner");
    const marker = `${lock}.reap-${process.pid}-postwrite-test`;
    if (mode === "sync") {
      const write = fs.writeFileSync;
      vi.spyOn(fs, "writeFileSync").mockImplementation((...args) => {
        write(...args);
        if (String(args[0]) === owner) fs.mkdirSync(marker);
      });
    } else {
      const write = fs.promises.writeFile;
      vi.spyOn(fs.promises, "writeFile").mockImplementation(async (...args) => {
        await write(...args);
        if (String(args[0]) === owner) fs.mkdirSync(marker);
      });
    }
    const operation = vi.fn(() => "must not admit");
    await expect(run(options, operation)).rejects.toThrow("test lock timeout");
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lock)).toBe(false);
    expect(fs.existsSync(marker)).toBe(true);
  });
});
