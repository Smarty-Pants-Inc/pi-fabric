// Keep reason: real kernel/process writer-exclusion boundary and data safety.
// Contract-only RED: the production module is absent at this prep baseline.
// BUILD WITH CHANGES is accepted; this task remains tests-only.
// No helper below implements/substitutes acquirePortableVolumeGuard.
// Expected skips: Linux OFD behavior off Linux/unverified ABI; real PID namespace
// or test-only seccomp denied by the host (each row reports its concrete reason).
// Kernel custody is NOT B1 evidence or permission to read history/reclaim locks.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { transportHostLeasePredecessorExited } from "../src/topology/host-leases.js";

type Guard = { assertCurrent(): void; close(): void };
type Acquire = (file: string) => Promise<Guard>;
type Probe = { status: string; dev: string; ino: string; pid?: number; type?: number;
  writeType?: number; start?: number; len?: number; errno?: number; reason?: string };
type Proof = { pid: number; pidStartTime: string; state: string; pidNamespace: string };
type Message = { event: string; child?: Message; outer?: Proof; pid?: number; pidStartTime?: string;
  state?: string; dev?: string; ino?: string; pidNamespace?: string; code?: number;
  exitStatus?: number; exitKind?: number;
  assertRefused?: boolean; sandboxProof?: Probe; error?: string };
const probeFile = fileURLToPath(new URL("./fixtures/portable-ofd-probe.py", import.meta.url));
const workerFile = fileURLToPath(new URL("./fixtures/portable-ofd-worker.ts", import.meta.url));
const target = fileURLToPath(new URL("../src/recovery/exclude-writer.ts", import.meta.url));
const python = "/usr/bin/python3";
const environment = () => ({ ...process.env, PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C",
  PYTHONDONTWRITEBYTECODE: "1", JITI_FS_CACHE: "0", NODE_DISABLE_COMPILE_CACHE: "1" });
const supportedLinux = process.platform === "linux" && ["x64", "arm64"].includes(process.arch);

async function production(): Promise<Acquire> {
  let module: Record<string, unknown>;
  try {
    // Nonliteral path keeps the intentionally missing module out of static TS
    // resolution. It MUST still fail at runtime; never use a fixture fallback.
    module = await import(/* @vite-ignore */ target) as Record<string, unknown>;
  } catch (cause) {
    throw new Error(`IMPORT-LEVEL RED: ${target} unavailable; candidate kernel/process behavior NOT EXERCISED`, { cause });
  }
  if (typeof module.acquirePortableVolumeGuard !== "function") {
    throw new Error("IMPORT-LEVEL RED: required acquirePortableVolumeGuard export absent; behavior NOT EXERCISED");
  }
  return module.acquirePortableVolumeGuard as Acquire;
}

function probe(command: string, file?: string, inherited?: number): Probe {
  const result = spawnSync(python, ["-I", "-B", probeFile, command, ...(file === undefined ? [] : [file])], {
    encoding: "utf8", timeout: 10_000, maxBuffer: 64 * 1024, env: environment(),
    stdio: inherited === undefined ? ["ignore", "pipe", "pipe"] : ["ignore", "pipe", "pipe", inherited],
  });
  if (result.error || result.status !== 0) {
    throw new Error(`real OFD oracle ${command} failed: ${result.error?.message ?? result.stderr} ${result.stdout}`);
  }
  return JSON.parse(result.stdout) as Probe;
}

const fixtures: { root: string; guards: Guard[]; supervisors: Supervisor[] }[] = [];
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "portable-ofd-"));
  const file = path.join(root, "volume.guard");
  const bytes = Buffer.from("fixed pre-existing volume guard; never unlink to unlock\n");
  fs.writeFileSync(file, bytes, { mode: 0o600, flag: "wx" });
  const item = { root, guards: [] as Guard[], supervisors: [] as Supervisor[] };
  fixtures.push(item);
  const initial = fs.statSync(file, { bigint: true });
  const take = async (acquire: Acquire, name = file): Promise<Guard> => {
    const guard = await acquire(name);
    item.guards.push(guard);
    return guard;
  };
  const unchanged = () => {
    const current = fs.lstatSync(file, { bigint: true });
    expect([current.dev, current.ino]).toEqual([initial.dev, initial.ino]);
    expect(fs.readFileSync(file)).toEqual(bytes);
  };
  const supervise = (options: { namespace?: boolean; sandbox?: boolean } = {}) => {
    const supervisor = new Supervisor(file, options);
    item.supervisors.push(supervisor);
    return supervisor;
  };
  return { ...item, file, bytes, initial, take, unchanged, supervise };
}

afterEach(async () => {
  for (const item of fixtures.splice(0)) {
    // All exact owned processes exit/reap before deleting ONLY this mkdtemp tree.
    try {
      for (const supervisor of item.supervisors) await supervisor.stop();
    } finally {
      for (const guard of item.guards) guard.close();
    }
    fs.rmSync(item.root, { recursive: true, force: true });
  }
});

class Supervisor {
  readonly child: ChildProcessWithoutNullStreams;
  readonly messages: Message[] = [];
  readonly closed: Promise<void>;
  private ended = false;
  private failure: Error | undefined;
  private pending: { resolve(message: Message): void; reject(error: Error): void } | undefined;
  private stderr = "";
  constructor(file: string, options: { namespace?: boolean; sandbox?: boolean }) {
    const launch = "import {createJiti} from 'jiti'; await createJiti(import.meta.url,{fsCache:false}).import(process.argv[1]);";
    const runtime = [process.execPath, "--input-type=module", "-e", launch, workerFile, file,
      ...(options.sandbox ? ["deny-ofd"] : [])];
    const argv = options.namespace
      ? ["/usr/bin/unshare", "--user", "--map-root-user", "--pid", "--fork", "--kill-child=KILL", ...runtime]
      : runtime;
    this.child = spawn(python, ["-I", "-B", probeFile,
      options.sandbox ? "supervise-deny-ofd" : "supervise", ...argv],
    { cwd: fileURLToPath(new URL("..", import.meta.url)),
      env: { ...environment(), TMPDIR: path.dirname(file), TMP: path.dirname(file), TEMP: path.dirname(file) },
      stdio: ["pipe", "pipe", "pipe"] });
    this.child.stdin.on("error", error => { this.fail(error); });
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (text: string) => { this.stderr = (this.stderr + text).slice(-16_384); });
    createInterface({ input: this.child.stdout }).on("line", line => {
      let message: Message;
      try { message = JSON.parse(line) as Message; }
      catch { this.fail(new Error(`non-JSON supervisor message: ${line}`)); return; }
      if (this.pending) { const waiting = this.pending; this.pending = undefined; waiting.resolve(message); }
      else this.messages.push(message);
    });
    this.closed = new Promise(resolve => {
      this.child.once("error", error => { this.fail(error); });
      this.child.once("close", (code, signal) => {
        this.ended = true;
        this.fail(new Error(`supervisor closed (${code}/${signal}): ${this.stderr}`));
        resolve();
      });
    });
  }
  private fail(error: Error): void {
    this.failure = error;
    if (this.pending) { const waiting = this.pending; this.pending = undefined; waiting.reject(error); }
  }
  next(): Promise<Message> {
    const message = this.messages.shift();
    if (message) return Promise.resolve(message);
    if (this.failure) return Promise.reject(this.failure);
    if (this.pending) return Promise.reject(new Error("overlapping readiness wait"));
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending = undefined;
        reject(new Error(`bounded supervisor readiness timed out: ${this.stderr}`));
      }, 10_000);
      this.pending = {
        resolve: value => { clearTimeout(timeout); resolve(value); },
        reject: error => { clearTimeout(timeout); reject(error); },
      };
    });
  }
  async control(command: string): Promise<Message> {
    if (this.ended) throw this.failure ?? new Error("supervisor already ended");
    this.child.stdin.write(`${command}\n`);
    return this.next();
  }
  async stop(): Promise<void> {
    if (this.ended) return;
    this.child.stdin.end("QUIT\n"); // Python finally kills/reaps its exact child.
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([this.closed, new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error("owned supervisor failed to reap/exit")), 10_000);
      })]);
    } finally { if (timeout) clearTimeout(timeout); }
  }
}

function expectIdentity(result: Probe, stat: fs.BigIntStats): void {
  expect([result.dev, result.ino]).toEqual([String(stat.dev), String(stat.ino)]);
}
function expectHeld(file: string, stat: fs.BigIntStats): void {
  const query = probe("query", file);
  expect(query).toMatchObject({ status: "conflict", type: query.writeType, pid: -1, start: 0, len: 0 });
  expectIdentity(query, stat);
  for (const kind of ["ofd", "posix"]) {
    const result = probe(`try-${kind}`, file);
    expect(result.status, `independent fresh-open ${kind} contender`).toBe("blocked");
    expectIdentity(result, stat);
  }
}
function matchingParentDescriptors(stat: fs.BigIntStats): number[] {
  return fs.readdirSync("/proc/self/fd").flatMap(name => {
    const fd = Number(name);
    try {
      const candidate = fs.fstatSync(fd, { bigint: true });
      return candidate.dev === stat.dev && candidate.ino === stat.ino ? [fd] : [];
    } catch { return []; } // readdir's own already-closed fd, not lock proof.
  });
}
function directChildren(): number[] {
  const pids = new Set<number>();
  for (const thread of fs.readdirSync("/proc/self/task")) {
    let text: string;
    try { text = fs.readFileSync(`/proc/self/task/${thread}/children`, "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
    for (const textPid of text.trim().split(/\s+/).filter(Boolean)) pids.add(Number(textPid));
  }
  return [...pids].sort((a, b) => a - b);
}

it("requires the provisional real production export (absence is IMPORT-LEVEL RED, never a skip)", async () => {
  expect(typeof await production()).toBe("function");
});

// This control can pass with no candidate module. It calibrates the oracle ONLY;
// it must never be reported as candidate M2(c) behavior or recovery authority.
describe.skipIf(!supportedLinux)("fixture calibration ONLY: real F_OFD_SETLK", () => {
  it("helper exits while the parent description still excludes fresh OFD/POSIX opens", () => {
    const s = fixture();
    const fd = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    try {
      const locked = probe("set-inherited", undefined, fd); // spawnSync has waited for helper exit.
      expect(locked.status).toBe("held");
      expectIdentity(locked, s.initial);
      expect(probe("query-inherited", undefined, fd).status).toBe("unlocked"); // same description is reentrant
      expect(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8")).toMatch(/OFDLCK\s+ADVISORY\s+WRITE\s+-1\s/);
      expectHeld(s.file, s.initial);
      const unrelated = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
      fs.closeSync(unrelated); // same inode, DIFFERENT open file description
      expectHeld(s.file, s.initial);
    } finally { fs.closeSync(fd); }
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    s.unchanged();
  });

  it.each(["flock", "posix"] as const)("known-mutant calibration: %s helper is NOT an OFD guard", kind => {
    const s = fixture(), fd = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    try {
      expect(probe(`set-inherited-${kind}`, undefined, fd).status).toBe("held");
      // flock remains in a different domain; POSIX dies with the helper. Both
      // are rejected by exactly the oracle used against the production guard.
      expect(probe("query", s.file).status).toBe("unlocked");
      expect(probe("try-ofd", s.file).status).toBe("acquired");
      expect(() => expectHeld(s.file, s.initial)).toThrow();
      expect(probe(`try-${kind}`, s.file).status).toBe(kind === "flock" ? "blocked" : "acquired");
    } finally { fs.closeSync(fd); }
    s.unchanged();
  });
});

describe.skipIf(!supportedLinux)("candidate M2(c) portable volume OFD guard", () => {
  it("K1: is genuine parent-held OFDLCK, not flock/POSIX or a resident-helper lock", async () => {
    const acquire = await production();
    const s = fixture(), before = directChildren();
    const guard = await s.take(acquire);
    expect(guard.assertCurrent()).toBeUndefined();
    expect(directChildren(), "lock acquisition helper must have exited and been reaped").toEqual(before);
    const descriptions = matchingParentDescriptors(s.initial);
    expect(descriptions.length, "guard must retain an fd in this parent, not just a live helper").toBeGreaterThan(0);
    expect(descriptions.some(fd => /OFDLCK\s+ADVISORY\s+WRITE\s+-1\s/.test(
      fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8")))).toBe(true);
    expectHeld(s.file, s.initial);
    expect(descriptions.some(fd => probe("query-inherited", undefined, fd).status === "unlocked")).toBe(true);
    // On local Linux flock and record locks are independent. This corroborates
    // F_OFD_GETLK/fdinfo; a filesystem merging the domains is not silently passed.
    expect(probe("try-flock", s.file).status).toBe("acquired");
    const other = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    fs.closeSync(other);
    guard.assertCurrent();
    expectHeld(s.file, s.initial);
    guard.close();
    expect(() => guard.assertCurrent()).toThrow();
    expect(() => guard.close()).not.toThrow();
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    expect(probe("try-posix", s.file).status).toBe("acquired");
    s.unchanged();
  });

  it("assertCurrent detects descriptor close/reuse even for the same inode", async () => {
    const acquire = await production(), s = fixture(), guard = await s.take(acquire);
    const descriptions = matchingParentDescriptors(s.initial).filter(fd =>
      /OFDLCK\s+ADVISORY\s+WRITE\s+-1\s/.test(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8")));
    expect(descriptions.length).toBeGreaterThan(0);
    const firstLocked = Math.min(...descriptions), fillers: number[] = [];
    let reused: number | undefined;
    try {
      // Acquisition releases its ancestor directory pins, leaving holes BELOW the
      // retained lock fd. Fill them while that fd is still live. Linux open takes
      // the lowest free number; one allocation above firstLocked proves saturation.
      // At most firstLocked lower numbers exist, so this loop is strictly bounded.
      for (let count = 0; count <= firstLocked; count++) {
        const filler = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
        fillers.push(filler);
        if (filler > firstLocked) break;
      }
      expect(fillers.at(-1), "all lower fd holes must be occupied before closing the lock fd").toBeGreaterThan(firstLocked);
      for (const fd of descriptions) fs.closeSync(fd); // exact owned temp-file fds, a deliberate test fault
      reused = fs.openSync(s.file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
      const replacement = reused;
      expect(descriptions).toContain(reused); // same fd number AND same inode; fstat alone is insufficient
      expect(reused).toBe(firstLocked);
      const stat = fs.fstatSync(reused, { bigint: true });
      expect([stat.dev, stat.ino]).toEqual([s.initial.dev, s.initial.ino]);
      expect(probe("try-ofd", s.file).status).toBe("acquired");
      expect(() => guard.assertCurrent()).toThrow();
      guard.close();
      expect(() => fs.fstatSync(replacement), "close must not close a reused, unowned description").not.toThrow();
    } finally {
      if (reused !== undefined) {
        try { fs.closeSync(reused); } catch { /* already reported above if candidate closed it */ }
      }
      for (const filler of fillers) fs.closeSync(filler);
    }
    s.unchanged();
  });

  it("K3: a second real process/fresh guard open refuses; final close admits it", async () => {
    const acquire = await production(), s = fixture();
    const guard = await s.take(acquire);
    const denied = s.supervise(), rejected = await denied.next();
    expect(rejected).toMatchObject({ event: "CHILD", child: { event: "REFUSED" } });
    expectHeld(s.file, s.initial);
    await denied.stop();
    guard.close();
    const successor = s.supervise(), ready = await successor.next();
    expect(ready).toMatchObject({ event: "CHILD", child: { event: "READY", dev: String(s.initial.dev), ino: String(s.initial.ino) } });
    expectHeld(s.file, s.initial);
    const closed = await successor.control("CLOSE");
    expect(closed).toMatchObject({ event: "CHILD", child: { event: "CLOSED", assertRefused: true } });
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    s.unchanged();
  }, 30_000);

  it("K2: STOP keeps exclusion; unreaped Z releases kernel fds but is NOT local predecessor-exit proof", async () => {
    await production(); // No raw lock may stand in for the missing candidate here.
    const s = fixture(), worker = s.supervise(), ready = await worker.next();
    expect(ready).toMatchObject({ event: "CHILD", child: { event: "READY" } });
    const original = ready.outer!;
    expect(original.pid).not.toBe(process.pid);
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    const proof = { pid: original.pid, pidStartTime: original.pidStartTime, bootId };
    expect(transportHostLeasePredecessorExited(proof)).toBe(false);
    expectHeld(s.file, s.initial);
    const stopped = await worker.control("STOP");
    expect(stopped).toMatchObject({ event: "STOPPED", state: "T", pid: proof.pid, pidStartTime: proof.pidStartTime });
    expect(transportHostLeasePredecessorExited(proof)).toBe(false);
    expectHeld(s.file, s.initial);
    const zombie = await worker.control("KILL");
    expect(zombie).toMatchObject({ event: "ZOMBIE", state: "Z", pid: proof.pid, pidStartTime: proof.pidStartTime });
    // Supervisor uses waitid(WNOWAIT), not Popen.poll()/wait(), until explicit REAP.
    expect(fs.existsSync(`/proc/${proof.pid}/stat`)).toBe(true);
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    expect(probe("try-posix", s.file).status).toBe("acquired");
    expect(transportHostLeasePredecessorExited(proof), "a zombie is not positive local proof").toBe(false);
    // A new kernel guard is allowed while Z remains. This grants ONLY a lock,
    // never permission for history, B1, gen updates, or foreign-owner reclaim.
    const guard = await s.take(await production());
    guard.assertCurrent();
    expectHeld(s.file, s.initial);
    guard.close();
    expect(await worker.control("REAP")).toMatchObject({ event: "REAPED", pid: proof.pid });
    expect(transportHostLeasePredecessorExited(proof)).toBe(true);
    s.unchanged();
  }, 30_000);

  it("kernel-only SIGTERM handler exit releases the guard (NOT Main cancel/settlement proof)", async () => {
    await production();
    const s = fixture(), worker = s.supervise(), ready = await worker.next();
    expect(ready).toMatchObject({ event: "CHILD", child: { event: "READY" } });
    expectHeld(s.file, s.initial);
    const terminated = await worker.control("TERM");
    expect(terminated).toMatchObject({ event: "TERMINATED", state: "Z", exitStatus: 0,
      pid: ready.outer!.pid, pidStartTime: ready.outer!.pidStartTime });
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    expect(probe("try-posix", s.file).status).toBe("acquired");
    expect(await worker.control("REAP")).toMatchObject({ event: "REAPED", code: 0 });
    s.unchanged();
  }, 30_000);

  it.each(["inode", "symlink", "ancestor"] as const)("assertCurrent refuses and latches after %s path replacement", async kind => {
    const acquire = await production(), s = fixture();
    const directory = path.join(s.root, "session");
    fs.mkdirSync(directory);
    const file = path.join(directory, "fixed.guard");
    fs.writeFileSync(file, s.bytes, { mode: 0o600, flag: "wx" });
    const original = fs.statSync(file, { bigint: true });
    const guard = await s.take(acquire, file);
    guard.assertCurrent();
    const retained = path.join(s.root, "retained-original");
    if (kind === "ancestor") {
      fs.renameSync(directory, retained);
      fs.mkdirSync(directory);
      fs.writeFileSync(file, s.bytes, { mode: 0o600, flag: "wx" });
    } else {
      fs.renameSync(file, retained);
      if (kind === "symlink") fs.symlinkSync(retained, file);
      else fs.writeFileSync(file, s.bytes, { mode: 0o600, flag: "wx" });
    }
    expect(() => guard.assertCurrent()).toThrow();
    const heldName = kind === "ancestor" ? path.join(retained, "fixed.guard") : retained;
    expectHeld(heldName, original); // Refusal may not unlock/remove the old inode.
    const replacement = path.join(s.root, "retained-replacement");
    fs.renameSync(kind === "ancestor" ? directory : file, replacement);
    fs.renameSync(retained, kind === "ancestor" ? directory : file);
    expect(() => guard.assertCurrent(), "restoring the path must not revive a revoked guard").toThrow();
    guard.close();
    expect(probe("try-ofd", file).status).toBe("acquired");
    expect(fs.readFileSync(file)).toEqual(s.bytes);
  });

  it.each(["final-symlink", "ancestor-symlink", "directory"] as const)("refuses %s at acquisition without weaker-lock fallback", async kind => {
    const acquire = await production(), s = fixture();
    const alias = path.join(s.root, "alias");
    let name: string;
    if (kind === "final-symlink") { fs.symlinkSync(s.file, alias); name = alias; }
    else if (kind === "ancestor-symlink") {
      const real = path.join(s.root, "real"); fs.mkdirSync(real);
      fs.writeFileSync(path.join(real, "guard"), s.bytes, { mode: 0o600, flag: "wx" });
      fs.symlinkSync(real, alias); name = path.join(alias, "guard");
    } else { fs.mkdirSync(alias); name = alias; }
    await expect(s.take(acquire, name)).rejects.toThrow();
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    s.unchanged();
  });

  it("refuses real kernel EINVAL for OFD commands, never substitutes flock/F_SETLK", async context => {
    const s = fixture(), capability = probe("sandbox-capability", s.file);
    if (capability.status === "expected-skip") context.skip(`expected skip: test-only seccomp unavailable: ${capability.reason}`);
    expect(capability).toMatchObject({ status: "unsupported", errno: 22 });
    await production();
    const worker = s.supervise({ sandbox: true }), result = await worker.next();
    expect(result).toMatchObject({ event: "CHILD", child: { event: "REFUSED",
      sandboxProof: { status: "unsupported", errno: 22 } } });
    await worker.stop();
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    s.unchanged();
  }, 30_000);

  it("K3: real different PID namespace still contends on the same existing inode", async context => {
    const s = fixture(), capability = probe("namespace-capability");
    if (capability.status === "expected-skip") context.skip(`expected skip: real user/PID namespace unavailable: ${capability.reason}`);
    expect(capability.status).toBe("supported");
    const acquire = await production();
    const selfNamespace = fs.readlinkSync("/proc/self/ns/pid");
    const guard = await s.take(acquire);
    const denied = s.supervise({ namespace: true }), rejected = await denied.next();
    expect(rejected).toMatchObject({ event: "CHILD", child: { event: "REFUSED" } });
    expect(rejected.child!.pidNamespace).toMatch(/^pid:\[\d+\]$/);
    expect(rejected.child!.pidNamespace).not.toBe(selfNamespace);
    expectHeld(s.file, s.initial);
    await denied.stop();
    guard.close();
    const successor = s.supervise({ namespace: true }), ready = await successor.next();
    expect(ready).toMatchObject({ event: "CHILD", child: { event: "READY", dev: String(s.initial.dev), ino: String(s.initial.ino) } });
    expect(ready.child!.pidNamespace).not.toBe(selfNamespace);
    expectHeld(s.file, s.initial);
    await successor.stop();
    expect(probe("try-ofd", s.file).status).toBe("acquired");
    s.unchanged();
  }, 30_000);
});

it.skipIf(process.platform === "linux")("unsupported platform refuses, with no flock/POSIX fallback", async () => {
  const acquire = await production(), s = fixture();
  await expect(s.take(acquire)).rejects.toThrow();
  s.unchanged();
});
