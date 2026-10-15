import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
// Type ONLY: importing this fixture must not import the production deletion graph at collection time.
import type { WindowsMeshCapability } from "../../src/storage/windows-mesh-release.js";

/** Hosted Windows T8: real external Win32 handles; no Bun/.ts children, deleters or mocked native outcomes. */
export const ACCESS = { READ_DATA: 1, READ_ATTRIBUTES: 0x80, DELETE: 0x10000, LIST_DIRECTORY: 1 } as const;
export const SHARE = { READ: 1, WRITE: 2, DELETE: 4, ALL: 7 } as const;
export type ChildReceipt = { code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string; error?: string };
type Ready = {
  pid: number; access: number; share: number; volume: string; index: string; directory: boolean;
  baselineHandles: number; heldHandles: number;
};
export type HeldHandle = { pid: number; ready: Ready; done: Promise<ChildReceipt>; close(): Promise<ChildReceipt> };
export type NativeRun = { child: ChildProcessWithoutNullStreams; done: Promise<ChildReceipt> };
export type Disposition = {
  pid: number; sequence: number; api: "extended" | "classic"; forced: boolean; ok: boolean; error: number;
  volume: string; index: string; directory: boolean;
};
type Owned = NativeRun & {
  kind: "compiler" | "handle" | "native"; closed: boolean; firstLine: Promise<string | null>; ready?: Ready;
  gateKey?: string;
};
const OUTPUT_LIMIT = 16 * 1024;
const FIXTURE_DEADLINE_MS = 15_000;
const checkpointChildren = new Map<string, Owned>();
const hostedOnly = (): void => {
  if (process.platform !== "win32" || process.env.GITHUB_ACTIONS !== "true") {
    throw new Error("T8 external handles require hosted Windows (real NTFS handles and bounded native processes)");
  }
};
const safeInteger = (value: unknown, min: number, max = Number.MAX_SAFE_INTEGER): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= min && value <= max;
const hexId = (value: unknown): value is string => typeof value === "string" && /^[0-9a-f]{1,16}$/.test(value);
const object = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("expected a JSON object");
  return value as Record<string, unknown>;
};
const gateKey = (dir: string, name: string): string => {
  if (!/^[a-z][a-z0-9-]{0,63}$/.test(name)) throw new Error("invalid checkpoint name");
  return path.join(path.resolve(dir), `${name}.paused`);
};
const childEnv = (): NodeJS.ProcessEnv => Object.fromEntries(Object.entries(process.env).filter(([key]) => {
  const upper = key.toUpperCase();
  return !upper.startsWith("TEST_") && !upper.includes("_TEST_");
}));
const receiptError = (receipt: ChildReceipt): string =>
  JSON.stringify({ code: receipt.code, signal: receipt.signal, error: receipt.error, stdout: receipt.stdout, stderr: receipt.stderr });
const assertClosedFixture = (receipt: ChildReceipt): ChildReceipt => {
  if (receipt.code !== 0 || receipt.signal !== null || receipt.error || receipt.stderr) throw new Error(`external handle did not close cleanly: ${receiptError(receipt)}`);
  return receipt;
};
const within = async <T>(promise: Promise<T>, ms: number, message: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), ms); })]);
  } finally { if (timer) clearTimeout(timer); }
};

/** Extract, but NEVER rewrite, the caller's real framed request deadline. Native validates every authority field. */
const requestDeadline = (input: Buffer, mode: "release" | "owner" | "debris"): number => {
  const magic = mode === "release" ? "FMRQ1" : mode === "owner" ? "FMRC1" : "FMRD1";
  if (input.length > 65536 || input.subarray(0, 5).toString("ascii") !== magic) throw new Error("invalid native request framing");
  const fields: Buffer[] = [];
  let at = 5;
  while (at < input.length) {
    const header = input.subarray(at, at + 8).toString("ascii");
    if (!/^[0-9a-f]{8}$/.test(header)) throw new Error("invalid native field length");
    const length = Number.parseInt(header, 16);
    at += 8;
    if (length > input.length - at || fields.length >= 451) throw new Error("invalid native field bounds");
    fields.push(input.subarray(at, at + length));
    at += length;
  }
  const text = fields[mode === "debris" ? 1 : 9]?.toString("ascii") ?? "";
  const deadline = Number(text);
  if (!/^[0-9]{1,5}$/.test(text) || !safeInteger(deadline, 1, 30_000)) throw new Error("native request needs an independent deadline of 1..30000 ms");
  return deadline;
};

export class WindowsHandleHarness {
  private readonly children: Owned[] = [];
  private closing?: Promise<ChildReceipt[]>;
  private constructor(private readonly directory: string) {}

  /** Compile ONLY the external-handle fixture, in this test's own root and caches. No product helper build. */
  static async create(root: string): Promise<WindowsHandleHarness> {
    hostedOnly();
    const zig = process.env.FABRIC_WIN_ZIG;
    if (!zig) throw new Error("hosted T8 fixture requires existing FABRIC_WIN_ZIG; no installer or PATH compiler fallback");
    const directory = fs.mkdtempSync(path.join(fs.realpathSync.native(root), "win32-handles-"));
    const harness = new WindowsHandleHarness(directory);
    const env = childEnv();
    env.ZIG_LOCAL_CACHE_DIR = path.join(directory, "zig-local-cache");
    env.ZIG_GLOBAL_CACHE_DIR = path.join(directory, "zig-global-cache");
    env.TMP = env.TEMP = env.TMPDIR = directory;
    try {
      const compiler = harness.start("compiler", zig, [
        "cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-municode",
        fileURLToPath(new URL("./mesh-win32-handles.c", import.meta.url)), "-o", harness.executable,
      ], 120_000, env);
      compiler.child.stdin.end();
      const receipt = await compiler.done;
      if (receipt.code !== 0 || receipt.signal !== null || receipt.error || !fs.statSync(harness.executable).isFile()) {
        throw new Error(`external Win32 fixture compile failed (existing FABRIC_WIN_ZIG required): ${receiptError(receipt)}`);
      }
      return harness;
    } catch (error) {
      // create has not returned: it must still own and join any compiler it started before rejecting.
      await harness.close();
      throw error;
    }
  }
  private get executable(): string { return path.join(this.directory, "mesh-win32-handles.exe"); }
  private active(): void {
    hostedOnly();
    if (this.closing) throw new Error("WindowsHandleHarness is already closing");
  }

  /** One registry for EVERY child, including failed spawns and the compiler, before the first await/assertion. */
  private start(kind: Owned["kind"], file: string, args: string[], timeout: number, env: NodeJS.ProcessEnv): Owned {
    const child = spawn(file, args, { stdio: "pipe", shell: false, windowsHide: true, timeout, cwd: this.directory, env });
    let finish!: (receipt: ChildReceipt) => void;
    let first!: (line: string | null) => void;
    const entry: Owned = {
      child, kind, closed: false, done: new Promise(resolve => { finish = resolve; }),
      firstLine: new Promise(resolve => { first = resolve; }),
    };
    this.children.push(entry);
    let stdout: Buffer = Buffer.alloc(0), stderr: Buffer = Buffer.alloc(0);
    let firstSent = false;
    const errors: string[] = [];
    const note = (message: string): void => { if (errors.length < 4) errors.push(message.slice(0, 1024)); };
    const capture = (old: Buffer, chunk: Buffer, stream: string): Buffer => {
      if (old.length + chunk.length > OUTPUT_LIMIT && !errors.some(error => error === `${stream} exceeded ${OUTPUT_LIMIT} bytes`)) {
        note(`${stream} exceeded ${OUTPUT_LIMIT} bytes`);
      }
      return old.length >= OUTPUT_LIMIT ? old : Buffer.concat([old, chunk.subarray(0, OUTPUT_LIMIT - old.length)]);
    };
    child.stdout.on("data", (chunk: Buffer) => {
      stdout = capture(stdout, chunk, "stdout");
      const newline = stdout.indexOf(10);
      if (!firstSent && newline >= 0) { firstSent = true; first(stdout.subarray(0, newline).toString("utf8")); }
    });
    child.stderr.on("data", (chunk: Buffer) => { stderr = capture(stderr, chunk, "stderr"); });
    child.on("error", error => { note(`spawn/process: ${error.message}`); });
    child.stdin.on("error", error => { note(`stdin: ${error.message}`); });
    child.stdout.on("error", error => { note(`stdout: ${error.message}`); });
    child.stderr.on("error", error => { note(`stderr: ${error.message}`); });
    child.on("close", (code, signal) => {
      entry.closed = true;
      if (!firstSent) first(null);
      if (entry.ready) {
        try {
          const lines = stdout.toString("utf8").trimEnd().split("\n");
          if (lines.length !== 2) throw new Error("expected exactly ready + closed census lines");
          const terminal = object(JSON.parse(lines[1]!));
          if (terminal.event !== "closed" || terminal.pid !== entry.ready.pid ||
              terminal.baselineHandles !== entry.ready.baselineHandles || terminal.heldHandles !== entry.ready.heldHandles ||
              terminal.closedHandles !== entry.ready.baselineHandles) throw new Error("last-close handle census did not restore baseline");
        } catch (error) { note(`fixture receipt: ${String(error)}`); }
      }
      finish({ code, signal, stdout: stdout.toString("utf8"), stderr: stderr.toString("utf8"), ...(errors.length ? { error: errors.join("; ") } : {}) });
    });
    return entry;
  }

  async hold(file: string, access: number, share: number): Promise<HeldHandle> {
    this.active();
    if (!safeInteger(access, 0, 0xffffffff) || !safeInteger(share, 0, SHARE.ALL)) throw new Error("invalid Win32 access/share mask");
    const entry = this.start("handle", this.executable,
      [path.toNamespacedPath(path.resolve(file)), access.toString(16), share.toString(16), String(FIXTURE_DEADLINE_MS)],
      19_000, childEnv());
    try {
      const line = await within(entry.firstLine, 8000, "external handle fixture did not become ready within 8 seconds");
      if (line === null) throw new Error(`external handle fixture exited before ready: ${receiptError(await entry.done)}`);
      const ready = object(JSON.parse(line));
      if (ready.event !== "ready" || !safeInteger(ready.pid, 1) || ready.pid !== entry.child.pid || ready.access !== access ||
          ready.share !== share || !hexId(ready.volume) || !hexId(ready.index) || typeof ready.directory !== "boolean" ||
          !safeInteger(ready.baselineHandles, 1) || !safeInteger(ready.heldHandles, 2) ||
          ready.heldHandles !== ready.baselineHandles + 1 || entry.closed) {
        throw new Error(`invalid real handle readiness/census: ${line}`);
      }
      entry.ready = {
        pid: ready.pid, access, share, volume: ready.volume, index: ready.index, directory: ready.directory,
        baselineHandles: ready.baselineHandles, heldHandles: ready.heldHandles,
      };
      return {
        pid: entry.ready.pid, ready: entry.ready, done: entry.done,
        close: async () => {
          try { if (!entry.closed && !entry.child.stdin.writableEnded) entry.child.stdin.end(); }
          catch (error) { await entry.done; throw error; }
          return assertClosedFixture(await entry.done);
        },
      };
    } catch (error) {
      let closeError: unknown;
      try { if (!entry.closed && !entry.child.stdin.writableEnded) entry.child.stdin.end(); }
      catch (failure) { closeError = failure; }
      const receipt = await entry.done; // never abandon a failed or timed-out readiness child
      throw new Error(`${String(error)}; stdin close=${String(closeError)}; terminal receipt: ${receiptError(receipt)}`);
    }
  }

  native(capability: WindowsMeshCapability, mode: "release" | "owner" | "debris", input: Buffer,
    checkpoint?: { name: string; dir: string }): NativeRun {
    this.active();
    const deadline = requestDeadline(input, mode);
    const stat = fs.lstatSync(capability.absolutePath);
    if (capability.meshAbi !== 1 || !path.isAbsolute(capability.absolutePath) || !stat.isFile() || stat.isSymbolicLink() ||
        createHash("sha256").update(fs.readFileSync(capability.absolutePath)).digest("hex") !== capability.digest) {
      throw new Error("native capability is not the captured digest-verified executable");
    }
    const env = childEnv();
    let key: string | undefined;
    if (checkpoint) {
      key = gateKey(checkpoint.dir, checkpoint.name);
      if (checkpointChildren.has(key)) throw new Error("checkpoint gate already belongs to an owned child; use a fresh gate");
      env.FABRIC_WIN_MESH_TEST_CHECKPOINT = checkpoint.name;
      env.FABRIC_WIN_MESH_TEST_DIR = path.resolve(checkpoint.dir);
    }
    const verb = mode === "release" ? "--mesh-retire-v1" : mode === "owner" ? "--mesh-recover-owner-v1" : "--mesh-recover-debris-v1";
    const entry = this.start("native", capability.absolutePath, [verb], deadline + 1000, env);
    if (key) { entry.gateKey = key; checkpointChildren.set(key, entry); }
    entry.child.stdin.end(input);
    return { child: entry.child, done: entry.done };
  }

  /** EOF every external fixture, terminate ONLY owned pending native children, and join ALL before reporting failure.
   * Includes the already joined compiler receipt. Root cleanup belongs to the test, strictly AFTER this resolves.
   * Native nonzero exits/signals can be the crash/refusal under test; fixture nonzero exits are NEVER a pass. */
  close(): Promise<ChildReceipt[]> {
    if (this.closing) return this.closing;
    this.closing = (async () => {
      const teardownErrors: unknown[] = [];
      for (const entry of this.children) {
        if (entry.closed) continue;
        try {
          if (entry.kind === "native") entry.child.kill();
          else if (!entry.child.stdin.writableEnded) entry.child.stdin.end();
        } catch (error) { teardownErrors.push(error); } // still join every other child
      }
      const receipts = await Promise.all(this.children.map(entry => entry.done));
      for (const entry of this.children) {
        if (entry.gateKey && checkpointChildren.get(entry.gateKey) === entry) checkpointChildren.delete(entry.gateKey);
      }
      receipts.forEach((receipt, index) => {
        const entry = this.children[index]!;
        if (entry.kind === "handle") {
          try { assertClosedFixture(receipt); } catch (error) { teardownErrors.push(error); }
        } else if (receipt.error) teardownErrors.push(new Error(`owned child I/O/spawn failure: ${receiptError(receipt)}`));
      });
      if (teardownErrors.length) throw new AggregateError(teardownErrors, `Windows handle harness teardown failed; receipts=${JSON.stringify(receipts)}`);
      return receipts;
    })();
    return this.closing;
  }
}

/** Bounded test-only polling safety net (8 s). An owned native early exit fails instead of impersonating a pause.
 * Public synchronous-route tests can also use this with their existing explicit checkpoint, racing their own done. */
export const waitCheckpoint = async (dir: string, name: string): Promise<void> => {
  const file = gateKey(dir, name);
  const end = performance.now() + 8000;
  for (;;) {
    const entry = checkpointChildren.get(file);
    if (entry?.closed) throw new Error(`native child exited before checkpoint observation: ${receiptError(await entry.done)}`);
    try {
      const pid = fs.readFileSync(file, "ascii");
      if (!/^[1-9][0-9]*\n$/.test(pid) || (entry && Number(pid) !== entry.child.pid)) throw new Error("checkpoint marker has wrong/malformed pid");
      return;
    } catch (error) {
      // The native marker is exclusive until its pid write + CloseHandle complete; existence alone is not readiness.
      if (!["ENOENT", "EBUSY", "EACCES", "EPERM"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
    }
    if (performance.now() >= end) throw new Error(`checkpoint not reached within 8 seconds: ${name}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

export const proceedCheckpoint = (dir: string, mode?: "classic" | "trace"): void => {
  if (mode !== undefined && mode !== "classic" && mode !== "trace") throw new Error("invalid disposition test command");
  fs.writeFileSync(path.join(dir, "go"), mode === "classic" ? "classic-disposition\n" : mode === "trace" ? "trace-disposition\n" : "");
};

/** Missing, truncated, oversized or malformed traces fail loudly. Trace success is not proof of last-close deletion. */
export const readDispositions = (dir: string): Disposition[] => {
  const file = path.join(dir, "disposition.jsonl");
  const size = fs.statSync(file).size;
  if (size < 1 || size > 64 * 384) throw new Error("missing/oversized native disposition trace");
  const text = fs.readFileSync(file, "utf8");
  if (!text.endsWith("\n")) throw new Error("truncated native disposition trace");
  const lines = text.slice(0, -1).split("\n");
  if (lines.length > 64) throw new Error("native disposition trace exceeds 64 calls; use one fresh gate per run");
  const sequence = new Map<number, number>();
  return lines.map(line => {
    const item = object(JSON.parse(line));
    if (!safeInteger(item.pid, 1) || !safeInteger(item.sequence, 1, 64) || (item.api !== "extended" && item.api !== "classic") ||
        typeof item.forced !== "boolean" || typeof item.ok !== "boolean" || !safeInteger(item.error, 0, 0xffffffff) ||
        !hexId(item.volume) || !hexId(item.index) || typeof item.directory !== "boolean" ||
        (item.forced && item.api !== "classic") || (item.ok && item.error !== 0) ||
        item.sequence !== (sequence.get(item.pid) ?? 0) + 1) throw new Error(`invalid native disposition evidence: ${line}`);
    sequence.set(item.pid, item.sequence);
    return item as unknown as Disposition;
  });
};
