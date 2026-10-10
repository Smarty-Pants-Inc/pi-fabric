import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readFileRetrying, writeJsonAtomic } from "../src/core/atomic-write.js";
import { updateRunRecord, writeRunRecord } from "../src/worker/run-record.js";
import type { AgentRunRecord } from "../src/agents/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const targetFile = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-windows-retry-"));
  roots.push(root);
  return path.join(root, "status.json");
};
const failure = (code: string) => Object.assign(new Error(`Windows sharing conflict: ${code}`), { code });

describe("bounded Windows sharing-conflict retries", () => {
  it.each(["EPERM", "EBUSY"])("atomically replaces a host record after transient %s", code => {
    const file = targetFile();
    writeJsonAtomic(file, { text: "previous" });
    const rename = fs.renameSync.bind(fs);
    let attempts = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual({ text: "previous" });
      expect(JSON.parse(fs.readFileSync(source, "utf8"))).toEqual({ text: "retained 🦄" });
      if (++attempts <= 2) throw failure(code);
      rename(source, target);
    });
    writeJsonAtomic(file, { text: "retained 🦄" }, { renameRetryDelayMs: 0 });
    expect(attempts).toBe(3);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual({ text: "retained 🦄" });
    expect(fs.readdirSync(path.dirname(file))).toEqual(["status.json"]);
  });

  it.each(["EPERM", "EBUSY", "EIO"])("bounds failed host replacements for %s without damaging the checkpoint", code => {
    const file = targetFile();
    writeJsonAtomic(file, { text: "checkpoint" });
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => { throw failure(code); });
    expect(() => writeJsonAtomic(file, { text: "unpublished" }, { renameRetryDelayMs: 0 })).toThrow(code);
    expect(rename).toHaveBeenCalledTimes(code === "EIO" ? 1 : 8);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual({ text: "checkpoint" });
    expect(fs.readdirSync(path.dirname(file))).toEqual(["status.json"]);
  });

  it.each(["EPERM", "EBUSY"])("reads the durable checkpoint after transient %s", code => {
    const file = targetFile();
    writeJsonAtomic(file, { text: "retained 🦄" });
    const read = fs.readFileSync.bind(fs);
    let attempts = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args) => {
      if (++attempts <= 2) throw failure(code);
      return Reflect.apply(read, fs, args);
    });
    expect(JSON.parse(readFileRetrying(file, 5, 0))).toEqual({ text: "retained 🦄" });
    expect(attempts).toBe(3);
  });

  it.each(["EPERM", "EBUSY", "EIO"])("bounds unavailable checkpoint reads for %s", code => {
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(() => { throw failure(code); });
    expect(() => readFileRetrying("unavailable", 5, 0)).toThrow(code);
    expect(read).toHaveBeenCalledTimes(code === "EIO" ? 1 : 5);
  });

  it.each(["EPERM", "EBUSY"])("retries the plain-Node worker's streamed checkpoint after %s", code => {
    const file = targetFile();
    const record: AgentRunRecord = { id: "checkpoint", name: "worker", task: "task", status: "running", runner: "pi", transport: "process", cwd: os.tmpdir(),
      startedAt: 1, updatedAt: 1, turns: 1, toolCalls: 1, text: "previous", lastCompleteText: "previous",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, logFile: path.join(path.dirname(file), "events.jsonl") };
    writeRunRecord(file, record);
    const rename = fs.renameSync.bind(fs);
    let attempts = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      expect(JSON.parse(fs.readFileSync(file, "utf8")).text).toBe("previous");
      if (++attempts <= 2) throw failure(code);
      rename(source, target);
    });
    updateRunRecord(file, { ...record, text: "retained 🦄", partialText: "retained 🦄" });
    expect(attempts).toBe(3);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ status: "running", text: "retained 🦄", partialText: "retained 🦄", lastCompleteText: "previous" });
  });
});
