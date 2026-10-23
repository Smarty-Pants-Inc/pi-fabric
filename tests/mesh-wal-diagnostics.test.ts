import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { MeshLockTimeoutError, meshDeferredSummary } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { StoreBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";
import { MeshStateWalCapError, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { formatWalReaders, walReaderPids } from "../src/mesh/wal-readers.js";

type BridgeErrorFrame = { id?: number; ok?: boolean; code?: string; error?: string };

const roots: string[] = [];
const stores: SqliteStateStore[] = [];
const children: ChildProcessWithoutNullStreams[] = [];

const privateRoot = (label: string, track = true): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-wal-diagnostics-${label}-`));
  fs.chmodSync(root, 0o700);
  if (track) roots.push(root);
  return root;
};

const childAlive = (child: ChildProcessWithoutNullStreams): boolean => child.exitCode === null && child.signalCode === null;

const stopChild = async (child: ChildProcessWithoutNullStreams): Promise<void> => {
  if (!childAlive(child)) return;
  const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
  child.stdin.end();
  let timer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([closed, new Promise<void>((resolve) => { timer = setTimeout(resolve, 2_000); })]);
  if (timer !== undefined) clearTimeout(timer);
  if (childAlive(child)) child.kill("SIGKILL");
  await closed;
};

afterEach(async () => {
  for (const child of children.splice(0)) await stopChild(child);
  for (const store of stores.splice(0)) store.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const waitForReady = (child: ChildProcessWithoutNullStreams): Promise<void> => new Promise((resolve, reject) => {
  let buffer = "";
  let timer: ReturnType<typeof setTimeout> | undefined;
  const cleanup = (): void => {
    if (timer !== undefined) clearTimeout(timer);
    child.stdout.off("data", onData);
    child.off("close", onClose);
  };
  const onData = (chunk: Buffer): void => {
    buffer += chunk.toString("utf8");
    if (buffer.split("\n").includes("READY")) { cleanup(); resolve(); }
  };
  const onClose = (): void => { cleanup(); reject(new Error("SQLite fixture child exited before readiness")); };
  timer = setTimeout(() => { cleanup(); reject(new Error("SQLite fixture child did not become ready")); }, 5_000);
  child.stdout.on("data", onData);
  child.once("close", onClose);
});

const nextFrame = (output: PassThrough): Promise<BridgeErrorFrame> => new Promise((resolve, reject) => {
  let buffer = "";
  let timer: ReturnType<typeof setTimeout> | undefined;
  const onData = (chunk: Buffer): void => {
    buffer += chunk.toString("utf8");
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    if (timer !== undefined) clearTimeout(timer);
    output.off("data", onData);
    resolve(JSON.parse(buffer.slice(0, newline)) as BridgeErrorFrame);
  };
  timer = setTimeout(() => {
    output.off("data", onData);
    reject(new Error("serveBridgeAgent did not return its diagnostic frame"));
  }, 3_000);
  output.on("data", onData);
});

/** Injects an already-produced error only at the StoreBridgeSide method boundary. */
const serveProducedError = async (error: Error): Promise<BridgeErrorFrame> => {
  const root = privateRoot("rpc", false);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
  const side = new StoreBridgeSide(store, "diagnostic-peer");
  side.latestSequence = async () => { throw error; };
  const input = new PassThrough();
  const output = new PassThrough();
  const serving = serveBridgeAgent(side, input, output);
  try {
    const response = nextFrame(output);
    input.write(`${JSON.stringify({ id: 1, op: "latestSequence" })}\n`);
    const frame = await response;
    input.end();
    await serving;
    return frame;
  } finally {
    input.destroy();
    output.destroy();
    store.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  }
};

const containsAny = (text: string, values: readonly string[]): boolean => values.some((value) => text.includes(value));

const DATABASE_CHILD = `
import { DatabaseSync } from "node:sqlite";
const [readerFile, writerFile] = process.argv.slice(1, 3);
const reader = new DatabaseSync(readerFile, { timeout: 0 });
reader.exec("BEGIN");
reader.prepare("SELECT count(*) AS n FROM kv").get();
const writer = new DatabaseSync(writerFile, { timeout: 0 });
writer.exec("BEGIN IMMEDIATE");
process.stdout.write("READY\\n");
process.stdin.once("end", () => {
  try { reader.exec("ROLLBACK"); } catch {}
  try { writer.exec("ROLLBACK"); } catch {}
  reader.close();
  writer.close();
});
process.stdin.resume();
`;

const identity: MeshIdentity = { id: "wal-diagnostics-agent", name: "wal-diagnostics-agent", kind: "agent" };
const walCapBytes = 1024 * 1024;

const openSqlite = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 100, {
    initialize: "create",
    checkpointLog: "off",
    checkpointBytes: 0,
    emergencyCheckpointBytes: 1024 * 1024 * 1024,
    walResetBytes: 0,
    ...options,
  });
  stores.push(store);
  return store;
};

describe("WAL diagnostic producer safety (smarty-dev#16197)", () => {
  it("drops legacy free text from the constructor message", () => {
    const secret = "SYNTHETIC_LEGACY_WAL_READER_TEXT";
    const error = new MeshStateWalCapError(120, 96, `pid 7 (node ${secret}) slot 0`, 1);
    expect(error.message.includes(secret)).toBe(false);
  });

  it("drops legacy free text from JSON.stringify(error)", () => {
    const secret = "SYNTHETIC_LEGACY_WAL_READER_TEXT";
    const error = new MeshStateWalCapError(120, 96, `pid 7 (node ${secret}) slot 0`, 1);
    expect(JSON.stringify(error).includes(secret)).toBe(false);
  });

  it("drops legacy free text from a real serveBridgeAgent RPC frame", async () => {
    const secret = "SYNTHETIC_LEGACY_WAL_READER_TEXT";
    const error = new MeshStateWalCapError(120, 96, `pid 7 (node ${secret}) slot 0`, 1);
    const frame = await serveProducedError(error);
    expect(frame.ok).toBe(false);
    expect(frame.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect(typeof frame.error).toBe("string");
    expect(frame.error!.includes(secret)).toBe(false);
    expect(JSON.stringify(frame).includes(secret)).toBe(false);
    expect(frame.error).toContain("120 bytes, above the 96-byte cap; 1 WAL reader(s)");
  });

  it("preserves finite positive fractional WAL caps in constructor and summary", () => {
    const error = new MeshStateWalCapError(120, 96.5, [], 0);
    expect(error.capBytes).toBe(96.5);
    expect(meshDeferredSummary(error)).toContain("120 bytes, above the 96.5-byte cap; 0 WAL reader(s)");
    const invalidCount = new MeshStateWalCapError(120, 96.5, [], 1.5);
    expect(invalidCount.readerCount).toBeUndefined();
    expect(meshDeferredSummary(invalidCount)).toContain("unknown WAL reader(s)");
  });

  it.each([
    { label: "NaN WAL bytes", walBytes: Number.NaN, capBytes: 96 },
    { label: "infinite WAL bytes", walBytes: Number.POSITIVE_INFINITY, capBytes: 96 },
    { label: "string WAL bytes", walBytes: "120", capBytes: 96 },
    { label: "NaN cap", walBytes: 120, capBytes: Number.NaN },
    { label: "infinite cap", walBytes: 120, capBytes: Number.POSITIVE_INFINITY },
    { label: "string cap", walBytes: 120, capBytes: "96" },
  ])("does not surface invalid numeric WAL fields: $label", ({ walBytes, capBytes }) => {
    const error = new MeshStateWalCapError(walBytes as never, capBytes as never, [], 1);
    const summary = meshDeferredSummary(error);
    expect(summary).toContain("sizes not available");
    expect([error.message, JSON.stringify(error), summary].some((text) => /NaN|Infinity|undefined|120 bytes/.test(text))).toBe(false);
  });

  it("allows only safe pid/comm reader facts and discards surplus or malformed metadata", () => {
    const secret = "SYNTHETIC_READER_METADATA_SECRET";
    const hostileFacts = [
      { pid: 1234, slot: 2, comm: "node", cmd: `node --token=${secret}`, argv: [`--token=${secret}`],
        env: { API_KEY: secret }, state: "S", ageMs: 180_000, surplus: secret },
      { pid: 0, slot: 0, comm: secret },
      { pid: -12, slot: 1, comm: secret },
      { pid: 1.5, slot: 2, comm: secret },
      { pid: 1235, slot: 3, comm: `tool-${secret}-overlong` },
      { pid: 1236, slot: 4, comm: `tool\n${secret}` },
    ] as never;
    const formatted = formatWalReaders(hostileFacts);
    expect(/^pid 1234 \(node\); pid 1235; pid 1236$/.test(formatted)).toBe(true);
    expect(formatted.includes(secret)).toBe(false);
    expect(formatted.includes("slot 2")).toBe(false);
    expect(formatted.includes("argv")).toBe(false);
    expect(formatted.includes("env")).toBe(false);

    const error = new MeshStateWalCapError(120, 96, hostileFacts, 1);
    const surfaces = [error.message, JSON.stringify(error)];
    expect(surfaces.some((surface) => surface.includes(secret))).toBe(false);
    expect(surfaces.some((surface) => surface.includes("slot 2"))).toBe(false);
  });

  it("does not serialize invalid pids or overlong/control comm text", () => {
    const secret = "RAWSECRET";
    const invalidFacts = [
      { pid: 0, slot: 0, comm: secret },
      { pid: Number.NaN, slot: 1, comm: secret },
      { pid: 44, slot: 2, comm: `${secret}${"x".repeat(16)}` },
      { pid: 45, slot: 3, comm: `${secret}\runsafe` },
      { pid: 46, slot: 4, comm: `${secret}\nunsafe` },
    ] as never;
    const formatted = formatWalReaders(invalidFacts);
    const error = new MeshStateWalCapError(120, 96, invalidFacts, 5);
    expect(formatted.includes(secret)).toBe(false);
    expect(error.message.includes(secret)).toBe(false);
    expect(JSON.stringify(error).includes(secret)).toBe(false);
  });

  it("preserves the ordinary SQLITE_BUSY reader-inspection diagnostic", () => {
    const error = Object.assign(new Error("Could not inspect WAL readers: database is busy"), { code: "SQLITE_BUSY" });
    expect(error.message).toBe("Could not inspect WAL readers: database is busy");
  });
});

describe("actual SQLite WAL and writer-timeout producers", () => {
  describe.skipIf(process.platform !== "linux")("Linux /proc reader evidence", () => {
    it("produces safe diagnostics from a real child WAL reader and real SQLite writer contention", async () => {
      const root = privateRoot("sqlite");
      const walRoot = path.join(root, "wal-state");
      const lockRoot = path.join(root, "writer-state");
      const walStore = await openSqlite(walRoot, { walHardCapBytes: walCapBytes });
      const lockStore = await openSqlite(lockRoot, { walHardCapBytes: 1024 * 1024, lockTimeoutMs: 180 });
      await walStore.put({ key: "diagnostics/before-reader", value: { payload: "initial WAL frame" }, identity });
      expect(walStore.walBytes()).toBeGreaterThan(0);
      const argvSecret = "SYNTHETIC_WAL_ARGV_SECRET";
      const envSecret = "SYNTHETIC_WAL_ENV_SECRET";
      const child = spawn(process.execPath,
        ["--input-type=module", "-e", DATABASE_CHILD, walStore.file, lockStore.file, argvSecret], {
          cwd: process.cwd(),
          env: { PATH: process.env.PATH ?? "/usr/bin:/bin", TMPDIR: os.tmpdir(), WAL_DIAGNOSTIC_SYNTHETIC_SECRET: envSecret },
          stdio: ["pipe", "pipe", "pipe"],
        });
      children.push(child);
      child.stderr.resume();
      await waitForReady(child);
      const pid = child.pid!;
      const procArgv = fs.readFileSync(`/proc/${pid}/cmdline`);
      const procEnv = fs.readFileSync(`/proc/${pid}/environ`);
      expect(procArgv.includes(Buffer.from(argvSecret))).toBe(true);
      expect(procEnv.includes(Buffer.from(envSecret))).toBe(true);
      const nativeComm = fs.readFileSync(`/proc/${pid}/comm`, "utf8").trim();
      expect(/^[\x20-\x7e]{1,15}$/.test(nativeComm)).toBe(true);

      const actualReaders = walReaderPids(walStore.file);
      const childReaders = actualReaders.filter((reader) => reader.pid === pid);
      expect(childReaders.length).toBeGreaterThan(0);
      expect(childReaders.every((reader) => reader.comm === nativeComm && Number.isSafeInteger(reader.pid) && reader.pid > 0)).toBe(true);
      for (const reader of childReaders) {
        expect(reader.slot).toBeGreaterThanOrEqual(0);
        expect(reader.slot).toBeLessThanOrEqual(4);
        expect(reader).not.toHaveProperty("cmd");
        expect(reader).not.toHaveProperty("state");
        expect(reader).not.toHaveProperty("ageMs");
      }
      expect(formatWalReaders(childReaders).split("; ").every((line) => line === `pid ${pid} (${nativeComm})`)).toBe(true);
      expect(walStore.walBytes()).toBeLessThanOrEqual(walCapBytes);

      let walRefusal: unknown;
      let committedWrites = 0;
      for (let index = 0; index < 200; index += 1) {
        try {
          await walStore.put({ key: `diagnostics/${index}`, value: { index, payload: "x".repeat(8 * 1024) }, identity });
          committedWrites += 1;
        } catch (error) {
          walRefusal = error;
          break;
        }
      }
      expect(committedWrites).toBeGreaterThan(0);
      expect(walRefusal).toBeInstanceOf(MeshStateWalCapError);
      const walError = walRefusal as MeshStateWalCapError;
      const producedWalBytes = walError.walBytes;
      const producedCapBytes = walError.capBytes;
      const producedReaderCount = walError.readerCount;
      expect(typeof producedWalBytes).toBe("number");
      expect(typeof producedCapBytes).toBe("number");
      expect(typeof producedReaderCount).toBe("number");
      if (typeof producedWalBytes !== "number" || typeof producedCapBytes !== "number" || typeof producedReaderCount !== "number") {
        throw new Error("real WAL-cap producer omitted validated numeric diagnostics");
      }
      expect(producedWalBytes).toBeGreaterThan(producedCapBytes);
      expect(producedCapBytes).toBe(walCapBytes);
      expect(producedReaderCount).toBeGreaterThan(0);
      expect(walStore.walBytes()).toBeGreaterThan(walCapBytes);

      const synthetic = [argvSecret, envSecret];
      const walJson = JSON.stringify(walError);
      expect(containsAny(walError.message, synthetic)).toBe(false);
      expect(containsAny(walJson, synthetic)).toBe(false);
      expect(walError.message.includes("slot ")).toBe(false);
      expect(walJson.includes('"slot"')).toBe(false);
      expect(walJson.includes('"state"')).toBe(false);
      expect(walJson.includes('"ageMs"')).toBe(false);
      expect(walJson.includes('"cmd"')).toBe(false);
      expect(walJson.includes('"argv"')).toBe(false);
      expect(walJson.includes('"env"')).toBe(false);

      const walFrame = await serveProducedError(walError);
      expect(walFrame.ok).toBe(false);
      expect(walFrame.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
      expect(typeof walFrame.error).toBe("string");
      expect(walFrame.error).toContain(`${producedWalBytes} bytes, above the ${producedCapBytes}-byte cap; ${producedReaderCount} WAL reader(s)`);
      expect.soft(containsAny(walFrame.error!, synthetic)).toBe(false);
      expect.soft(containsAny(JSON.stringify(walFrame), synthetic)).toBe(false);
      expect(walFrame.error!.includes(`pid ${pid}`)).toBe(false);
      expect(walFrame.error!.includes("slot ")).toBe(false);

      const lockRefusal = await lockStore.put({ key: "diagnostics/contended", value: "writer timeout", identity })
        .then(() => undefined, (error: unknown) => error);
      expect(lockRefusal).toBeInstanceOf(MeshLockTimeoutError);
      const lockError = lockRefusal as MeshLockTimeoutError;
      expect(lockError.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
      expect(containsAny(lockError.message, synthetic)).toBe(false);
      expect(containsAny(JSON.stringify(lockError), synthetic)).toBe(false);
      const lockFrame = await serveProducedError(lockError);
      expect(lockFrame.ok).toBe(false);
      expect(lockFrame.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
      expect(lockFrame.error).toBe(lockError.message);
      expect.soft(containsAny(lockFrame.error!, synthetic)).toBe(false);
      expect.soft(containsAny(JSON.stringify(lockFrame), synthetic)).toBe(false);
    }, 30_000);
  });
});

describe("custody timeout holder diagnostics", () => {
  it("does not echo an invalid PID line through the timeout, JSON, or RPC frame", async () => {
    const root = privateRoot("custody-invalid-pid");
    const secret = "SYNTHETIC_CUSTODY_OWNER_PID_SECRET";
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    fs.mkdirSync(lock, { mode: 0o700 });
    fs.writeFileSync(path.join(lock, "owner"), `fixture-token\n${secret}\n${Date.now()}\n`, { mode: 0o600 });

    const refused = await acquireMeshCustodyLock(root, 0, { ownIncarnation: undefined })
      .then(() => undefined, (error: unknown) => error);
    expect(refused).toBeInstanceOf(MeshLockTimeoutError);
    const error = refused as MeshLockTimeoutError;
    expect.soft(error.message.includes(secret)).toBe(false);
    expect.soft(error.message.includes("owner pid unreadable")).toBe(true);
    const json = JSON.stringify(error);
    expect.soft(json.includes(secret)).toBe(false);
    expect.soft(json.includes("owner pid unreadable")).toBe(true);

    const frame = await serveProducedError(error);
    expect(frame.ok).toBe(false);
    expect(frame.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect.soft(frame.error!.includes(secret)).toBe(false);
    expect.soft(containsAny(JSON.stringify(frame), [secret])).toBe(false);
    expect.soft(frame.error).toContain("owner pid unreadable");
  });

  it("continues to report a positive integer holder PID", async () => {
    const root = privateRoot("custody-valid-pid");
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    fs.mkdirSync(lock, { mode: 0o700 });
    fs.writeFileSync(path.join(lock, "owner"), `fixture-token\n${process.pid}\n${Date.now()}\n`, { mode: 0o600 });

    const refused = await acquireMeshCustodyLock(root, 0, { ownIncarnation: undefined })
      .then(() => undefined, (error: unknown) => error);
    expect(refused).toBeInstanceOf(MeshLockTimeoutError);
    const error = refused as MeshLockTimeoutError;
    expect(error.message.includes(`held by pid ${process.pid}`)).toBe(true);
    expect(JSON.stringify(error).includes(`held by pid ${process.pid}`)).toBe(true);
  });
});
