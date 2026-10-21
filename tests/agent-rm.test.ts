import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { removeTree } from "../src/agents/rm.js";
import { runSessionTraceArchivePath } from "../src/storage/retention.js";

describe("removeTree", () => {
  it("removes a directory tree", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-"));
    fs.mkdirSync(path.join(dir, "nested", "deep"), { recursive: true });
    fs.writeFileSync(path.join(dir, "nested", "deep", "file.txt"), "x");
    await removeTree(dir);
    expect(fs.existsSync(dir)).toBe(false);
  });

  it("archives raw traces before recursive cleanup", async () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-traces-"));
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-mesh-"));
    const directory = path.join(parent, "run");
    fs.mkdirSync(directory);
    const traces = new Map([["session.jsonl", "raw session\n"], ["route-session.jsonl", "raw route\n"], ["events.jsonl", "raw events\n"]]);
    const mtimes = new Map<string, number>();
    for (const [name, content] of traces) {
      const file = path.join(directory, name);
      fs.writeFileSync(file, content);
      fs.utimesSync(file, new Date(1_000), new Date(1_000));
      mtimes.set(name, fs.statSync(file).mtimeMs);
    }
    await removeTree(directory, undefined, meshRoot);
    expect(fs.existsSync(directory)).toBe(false);
    const archive = runSessionTraceArchivePath(meshRoot, directory);
    for (const [name, content] of traces) {
      const file = path.join(archive, name);
      expect(fs.readFileSync(file, "utf8")).toBe(content);
      expect(fs.statSync(file).mtimeMs).toBe(mtimes.get(name));
    }
    await expect(removeTree(archive, undefined, meshRoot)).rejects.toThrow("durable session trace archives are excluded from cleanup");
    expect(fs.existsSync(path.join(archive, "session.jsonl"))).toBe(true);
    fs.rmSync(parent, { recursive: true, force: true });
    fs.rmSync(meshRoot, { recursive: true, force: true });
  });

  it("refuses recursive cleanup when trace archival fails", async () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-trace-failure-"));
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-mesh-failure-"));
    const directory = path.join(parent, "run");
    fs.mkdirSync(directory);
    for (const name of ["session.jsonl", "route-session.jsonl", "events.jsonl"]) fs.writeFileSync(path.join(directory, name), `raw ${name}\n`);
    const failedSource = path.join(directory, "route-session.jsonl");
    const realRename = fs.renameSync;
    const rename = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === failedSource) throw Object.assign(new Error("injected archive failure"), { code: "EIO" });
      return realRename(from, to);
    });
    let deleteCalls = 0;
    await expect(removeTree(directory, async (target, options) => {
      deleteCalls++;
      return fs.promises.rm(target, options);
    }, meshRoot)).rejects.toThrow("raw session trace archive failed");
    expect(deleteCalls).toBe(0);
    expect(fs.existsSync(directory)).toBe(true);
    expect(fs.readFileSync(failedSource, "utf8")).toBe("raw route-session.jsonl\n");
    rename.mockRestore();
    await removeTree(directory, undefined, meshRoot);
    expect(fs.existsSync(directory)).toBe(false);
    fs.rmSync(parent, { recursive: true, force: true });
    fs.rmSync(meshRoot, { recursive: true, force: true });
  });

  it.each(["ENOTEMPTY", "EBUSY"])("retries on transient %s and then succeeds", async (code) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-"));
    fs.mkdirSync(path.join(dir, "nested"), { recursive: true });
    fs.writeFileSync(path.join(dir, "nested", "file.txt"), "x");
    let calls = 0;
    await removeTree(dir, async (target, options) => {
      calls++;
      if (calls < 3) {
        const err = new Error(`transient removal failure: ${code}`) as NodeJS.ErrnoException;
        err.code = code;
        throw err;
      }
      return fs.promises.rm(target, options);
    });
    expect(calls).toBe(3);
    expect(fs.existsSync(dir)).toBe(false);
  });

  it("rethrows non-retryable errors without retrying", async () => {
    let calls = 0;
    await expect(
      removeTree("ignored", async () => {
        calls++;
        const err = new Error("permission denied") as NodeJS.ErrnoException;
        err.code = "EACCES";
        throw err;
      }),
    ).rejects.toThrow("permission denied");
    expect(calls).toBe(1);
  });
});
