import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { FabricLifecyclePublishRequest } from "../src/lifecycle/types.js";

// Keep reason: data safety at the real manager -> process worker boundary. The
// fixture wrapper appends like src/worker.ts (the old generic fake truncates).
const roots: string[] = [];
const managers: AgentManager[] = [];
const hash = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const row = (event: string, data: object) => JSON.stringify({ version: 1, event, occurredAt: 1234, data }) + "\n";
const usage = (input: number) => ({ runId: "fixture", name: "fixture", runner: "pi", depth: 1, cumulativeTokens: input,
  input, output: 0, cacheRead: 0, cacheWrite: 0, cost: input / 1000 });
const prefix = Buffer.from(row("pi.turn_end", { attempt: 1 }) + row("tokens.usage", usage(7)) +
  "malformed\u0000\r\n" + "x".repeat(70_000) + "\n", "utf8");
const suffix = Buffer.concat([Buffer.from(row("pi.agent_end", { attempt: 1, willRetry: false }) +
  row("tokens.usage", usage(13)) + '{"version":1,"event":"tokens.usage","data":', "utf8"), Buffer.from([0xf0, 0x9f])]);
const original = Buffer.concat([prefix, suffix]);

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

function fixture(kind: "startup" | "resume", options: { blockArchive?: boolean; empty?: boolean } = {}) {
  // Match core fixture isolation, without touching any live host or worktree.
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID", "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lifecycle-archive-"));
  roots.push(root);
  const worker = path.join(root, "worker.mjs");
  const target = pathToFileURL(path.resolve(kind === "startup" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs")).href;
  fs.writeFileSync(worker, `
    import fs from 'node:fs'; import path from 'node:path';
    const args = new Map();
    for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
    const journal = args.get('lifecycle-file'), directory = path.dirname(journal);
    const counter = path.join(directory, 'fixture-attempts');
    const attempt = fs.existsSync(counter) ? Number(fs.readFileSync(counter, 'utf8')) + 1 : 1;
    fs.writeFileSync(counter, String(attempt));
    if (attempt === 1 && ${!options.empty}) {
      fs.appendFileSync(journal, Buffer.from(${JSON.stringify(prefix.toString("base64"))}, 'base64'));
      // Native exit can publish bytes after terminal status and the first drain.
      process.on('exit', () => fs.appendFileSync(journal, Buffer.from(${JSON.stringify(suffix.toString("base64"))}, 'base64')));
      ${options.blockArchive ? "fs.writeFileSync(path.join(directory, 'session-archive'), 'blocked');" : ""}
    }
    if (attempt > 1 && ${!options.empty}) {
      // Capture the exact state AT replacement launch, not only after settlement.
      fs.copyFileSync(journal, ${JSON.stringify(path.join(root, "at-replacement.raw"))});
      const manifest = path.join(directory, 'session-archive', 'MANIFEST.jsonl');
      if (fs.existsSync(manifest)) fs.copyFileSync(manifest, ${JSON.stringify(path.join(root, "at-replacement.manifest"))});
      fs.appendFileSync(journal, ${JSON.stringify(row("pi.turn_end", { attempt: 2 }) + row("tokens.usage", usage(11)))});
    }
    await import(${JSON.stringify(target)});
  `);
  const events: FabricLifecyclePublishRequest[] = [];
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    workerPath: worker, runRoot: path.join(root, "runs"), onLifecycle: event => events.push(event),
  });
  managers.push(manager);
  return { manager, root, events };
}

function receipts(directory: string): Array<{ phase: string; from: string; to: string; sha256: string; bytes: number }> {
  const file = path.join(directory, "session-archive", "MANIFEST.jsonl");
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
}

describe("AgentManager lifecycle archive before retry", () => {
  it.each(["startup", "resume"] as const)("archives full raw bytes before actual public %s replacement and never replays old events", async kind => {
    const { manager, root, events } = fixture(kind);
    const result = await manager.run({ task: kind === "startup" ? "Recover startup" : "RESUME_AFTER_STOP", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.text).toBe(kind === "startup" ? "startup retry recovered" : "resumed attempt 2");
    if (kind === "resume") {
      expect(result.turns).toBe(6);
      expect(result.toolCalls).toBe(4);
      expect(result.usage.input).toBe(110);
    }
    const directory = manager.runDirectory(result.id)!;
    const archive = receipts(directory);
    expect(archive).toHaveLength(1);
    expect(archive[0]).toMatchObject({ phase: "COPY-READBACK", from: path.join(directory, "lifecycle.jsonl"), bytes: original.length, sha256: hash(original) });
    const readback = fs.readFileSync(archive[0]!.to);
    expect(readback.equals(original)).toBe(true);
    expect(hash(readback)).toBe(archive[0]!.sha256);
    console.info(JSON.stringify({ proof: "full-raw-byte-readback-before-replacement", kind, bytes: original.length,
      originalSha256: hash(original), independentArchiveSha256: hash(readback), exactBytesEqual: readback.equals(original) }));
    expect(fs.readFileSync(path.join(root, "at-replacement.manifest"), "utf8")).toContain(archive[0]!.sha256);
    expect(fs.readFileSync(path.join(root, "at-replacement.raw")).subarray(0, original.length).equals(original)).toBe(true);
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).subarray(0, original.length).equals(original)).toBe(true);
    expect(events.filter(event => event.event === "pi.turn_end").map(event => event.data)).toEqual([{ attempt: 1 }, { attempt: 2 }]);
    expect(events.filter(event => event.event === "pi.agent_end")).toHaveLength(1);
    expect(events.filter(event => event.event === "tokens.usage").map(event => (event.data as { input: number }).input)).toEqual([7, 13, 11]);
    expect(fs.readFileSync(path.join(directory, "fixture-attempts"), "utf8")).toBe("2");
  }, 30_000);

  it.each(["startup", "resume"] as const)("archive failure preserves exact raw bytes and refuses %s replacement", async kind => {
    const { manager } = fixture(kind, { blockArchive: true });
    const result = await manager.run({ task: kind === "startup" ? "Recover startup" : "RESUME_AFTER_STOP", transport: "process" });
    expect(result.status).not.toBe("completed");
    expect(result.error).toContain("relaunch failed:");
    expect(result.error).toContain("Trace archive");
    const directory = manager.runDirectory(result.id)!;
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).equals(original)).toBe(true);
    expect(fs.readFileSync(path.join(directory, "fixture-attempts"), "utf8")).toBe("1");
  }, 30_000);

  it("independent archive readback corruption refuses replacement and retains the raw source", async () => {
    const { manager } = fixture("startup");
    const open = fs.openSync;
    const read = fs.readSync;
    let archiveReader: number | undefined;
    let corrupted = false;
    vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).includes(`${path.sep}session-archive${path.sep}`) &&
          typeof args[1] === "number" && !(args[1] & fs.constants.O_WRONLY)) archiveReader = fd;
      return fd;
    });
    vi.spyOn(fs, "readSync").mockImplementation((...args: Parameters<typeof fs.readSync>) => {
      const count = read(...args);
      if (!corrupted && args[0] === archiveReader && count > 0) {
        const bytes = args[1] as Buffer;
        bytes[0] = bytes[0]! ^ 0xff;
        corrupted = true;
      }
      return count;
    });
    const result = await manager.run({ task: "Recover startup", transport: "process" });
    expect(corrupted).toBe(true);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Trace archive readback verification failed");
    const directory = manager.runDirectory(result.id)!;
    expect(receipts(directory)).toEqual([]);
    expect(fs.readFileSync(path.join(directory, "fixture-attempts"), "utf8")).toBe("1");
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).equals(original)).toBe(true);
  }, 30_000);

  it("never unlinks or overwrites a late canonical journal occupant after the verified copy", async () => {
    const { manager, root } = fixture("startup");
    const append = fs.appendFileSync;
    const late = Buffer.from(row("pi.turn_end", { late: true }) + "late raw bytes\u0000\n");
    let swapped = false;
    vi.spyOn(fs, "appendFileSync").mockImplementation((...args: Parameters<typeof fs.appendFileSync>) => {
      if (!swapped && String(args[0]).endsWith(`${path.sep}lifecycle.jsonl`) && args[1] === "\n") {
        swapped = true;
        fs.renameSync(String(args[0]), path.join(root, "superseded.raw"));
        fs.writeFileSync(String(args[0]), late);
      }
      return append(...args);
    });
    const result = await manager.run({ task: "Recover startup", transport: "process" });
    expect(swapped).toBe(true);
    expect(result.status).toBe("completed");
    const directory = manager.runDirectory(result.id)!;
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).subarray(0, late.length).equals(late)).toBe(true);
    expect(fs.readFileSync(path.join(root, "superseded.raw")).equals(original)).toBe(true);
    expect(fs.readFileSync(receipts(directory)[0]!.to).equals(original)).toBe(true);
  }, 30_000);

  it("allows ordinary retry without a lifecycle journal", async () => {
    const { manager } = fixture("startup", { empty: true });
    const result = await manager.run({ task: "Recover startup", transport: "process" });
    expect(result.status).toBe("completed");
    expect(receipts(manager.runDirectory(result.id)!)).toEqual([]);
  }, 30_000);

  it("does not archive or retry an ordinary deterministic failure", async () => {
    const { manager } = fixture("startup");
    const result = await manager.run({ task: "Reject startup", transport: "process" });
    expect(result.status).toBe("failed");
    expect(result.error).toBe("provider rejected the prompt");
    const directory = manager.runDirectory(result.id)!;
    expect(receipts(directory)).toEqual([]);
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).equals(original)).toBe(true);
  }, 30_000);

  it("operator cancellation retains the journal without a replacement", async () => {
    const { manager, events } = fixture("resume");
    const handle = await manager.spawn({ task: "HANG until stopped", transport: "process" });
    await vi.waitFor(() => expect(events.some(event => event.event === "pi.turn_end")).toBe(true), { timeout: 10_000 });
    await manager.stop(handle.id);
    expect((await manager.wait(handle.id)).status).toBe("stopped");
    const directory = manager.runDirectory(handle.id)!;
    expect(receipts(directory)).toEqual([]);
    expect(fs.readFileSync(path.join(directory, "fixture-attempts"), "utf8")).toBe("1");
    expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).subarray(0, prefix.length).equals(prefix)).toBe(true);
  }, 30_000);

  it("never archives or replaces a worker whose stop did not take effect", async () => {
    const { manager } = fixture("resume");
    const launch = ProcessTransport.prototype.launch;
    let first: Awaited<ReturnType<typeof launch>> | undefined;
    let launches = 0;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      first = handle;
      launches++;
      let stopRequested = false;
      return { ...handle, isAlive: async () => stopRequested ? handle.isAlive() : false, stop: async () => { stopRequested = true; } };
    });
    try {
      const handle = await manager.spawn({ task: "HANG until stopped", transport: "process" });
      await vi.waitFor(() => expect(manager.status(handle.id).status).toBe("failed"), { timeout: 25_000 });
      expect(manager.status(handle.id)).toMatchObject({ error: expect.stringContaining("did not stop, so it was not relaunched") });
      const directory = manager.runDirectory(handle.id)!;
      expect(launches).toBe(1);
      expect(receipts(directory)).toEqual([]);
      expect(fs.readFileSync(path.join(directory, "lifecycle.jsonl")).equals(prefix)).toBe(true);
      expect(await first!.isAlive()).toBe(true);
      await first!.stop();
      await manager.stop(handle.id);
    } finally { spy.mockRestore(); await first?.stop(); }
  }, 45_000);
});
