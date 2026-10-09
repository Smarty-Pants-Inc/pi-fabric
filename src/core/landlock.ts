import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import { loadedFabricRoot } from "./agent-dir.js";

export interface LandlockSettings {
  mode: "off" | "enforce";
  /** Host-only kill switch. Project settings cannot override it. */
  disabled: boolean;
  /** Root-policy-only per-command escape grant. Absent/false means denied. */
  allowEscape?: boolean;
}

const ESCAPE = /^\s*PI_FABRIC_LANDLOCK_ESCAPE=1[ \t]+/;
let warnedEscapeDenied = false;
export const landlockCommand = (command: string, allowEscape = false): { escape: boolean; command: string } => {
  const requested = ESCAPE.test(command);
  if (requested && allowEscape !== true && !warnedEscapeDenied) {
    warnedEscapeDenied = true;
    console.warn("[pi-fabric] PI_FABRIC_LANDLOCK_ESCAPE ignored: no valid root policy grants executor.landlock.allowEscape: true; command remains confined");
  }
  return { escape: requested && allowEscape === true, command: command.replace(ESCAPE, "") };
};

/** No git process/hooks: only the lane's actual .git/commondir metadata. */
const gitCommonDir = (cwd: string): string | undefined => {
  for (let current = cwd; ; current = path.dirname(current)) {
    const marker = path.join(current, ".git");
    try {
      const stat = fs.statSync(marker);
      let git = stat.isDirectory() ? marker : path.resolve(current,
        fs.readFileSync(marker, "utf8").match(/^gitdir: (.+)\s*$/)?.[1] ?? "");
      if (!stat.isDirectory() && git === current) return undefined;
      const common = path.join(git, "commondir");
      if (fs.existsSync(common)) git = path.resolve(git, fs.readFileSync(common, "utf8").trim());
      return fs.realpathSync(git);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    if (path.dirname(current) === current) return undefined;
  }
};

interface Grant { path: string; reason: string }
interface RolePolicy { default: Grant[]; roles: Record<string, Grant[]> }
/** A write root bound to the directory identity approved by the trusted host. */
export interface PinnedGrant { real: string; dev: bigint; ino: bigint }

const unsafe = (value: string): boolean => /[\n\r\0]/.test(value);
const within = (child: string, parent: string): boolean =>
  child === parent || child.startsWith(parent.endsWith(path.sep) ? parent : parent + path.sep);
const pinPath = (expanded: string): PinnedGrant | undefined => {
  if (!path.isAbsolute(expanded) || unsafe(expanded)) throw new Error("Invalid Landlock write grant");
  let real: string;
  try { real = fs.realpathSync(expanded); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  if (unsafe(real) || real === "/") throw new Error("Unsafe Landlock write grant");
  const stat = fs.statSync(real, { bigint: true });
  return { real, dev: stat.dev, ino: stat.ino };
};
const sameGrant = (a: PinnedGrant, b: PinnedGrant): boolean =>
  a.real === b.real && a.dev === b.dev && a.ino === b.ino;
const LEDGER = ".custody";
const SWEEP_LIMIT = 16;
interface Ledger {
  host: number; hostStart: number | undefined; since: number;
  dev: string; ino: string; groups: number[]; unconfirmed: boolean;
}

const code = (error: unknown): string | undefined => (error as NodeJS.ErrnoException).code;
/** Process start in clock ticks since boot (field 22 of /proc/<pid>/stat). */
const startTicks = (pid: number): number | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]);
  } catch { return undefined; }
};
/** Now, in the same unit, minus a one-second margin (USER_HZ is 100 on Linux). */
const bootTicks = (): number =>
  Math.floor(Number(fs.readFileSync("/proc/uptime", "utf8").split(" ")[0]) * 100) - 100;
/** Only ESRCH confirms a process group is gone; EPERM or success means it may live. */
const groupGone = (pgid: number): boolean => {
  try { process.kill(-pgid, 0); return false; }
  catch (error) { return code(error) === "ESRCH"; }
};

/**
 * S2: a resolved shell result proves only that the shell exited. The generated TMPDIR
 * is releasable only with confirmed quiescence: every launched process group is gone
 * (kill(-pgid, 0) == ESRCH) AND no process of ours can still be a confined descendant
 * or a holder. The native helper sets no_new_privs, which every confined descendant
 * inherits and cannot clear (also across setsid): any same-uid no_new_privs process
 * started since the confinement began is a possible descendant that escaped its group,
 * so it holds. Processes without that marker are provably not confined descendants;
 * they still hold when they inherit the TMPDIR or have cwd/root/an fd under it.
 * Inspection failures other than "exited" are unresolved, never absence.
 */
const quiescent = (exported: string, real: string, since: number, groups: Iterable<number>): boolean => {
  for (const pgid of groups) if (!groupGone(pgid)) return false;
  let pids: string[];
  try { pids = fs.readdirSync("/proc").filter(name => /^\d+$/.test(name) && Number(name) !== process.pid); }
  catch { return false; }
  const marker = Buffer.from(`\0TMPDIR=${exported}\0`);
  const uid = process.getuid!();
  const under = (link: string): boolean => {
    try { const target = fs.readlinkSync(link); return within(target, real) || within(target, exported); }
    catch (error) { if (code(error) === "ENOENT") return false; throw error; }
  };
  for (const pid of pids) {
    const base = `/proc/${pid}`;
    try {
      if (fs.statSync(base).uid !== uid) continue; // a confined child cannot change uid (no_new_privs)
      const status = fs.readFileSync(`${base}/status`, "utf8");
      const nnp = /^NoNewPrivs:\s*(\d)/m.exec(status)?.[1];
      if (nnp === undefined) return false;
      if (nnp === "1") {
        const started = startTicks(Number(pid));
        if (started === undefined) { if (fs.existsSync(base)) return false; continue; }
        if (started >= since) return false; // possible confined descendant, in or out of its group
      }
      try {
        if (Buffer.concat([Buffer.from("\0"), fs.readFileSync(`${base}/environ`)]).includes(marker)) return false;
        if (under(`${base}/cwd`) || under(`${base}/root`)) return false;
        for (const fd of fs.readdirSync(`${base}/fd`)) if (under(`${base}/fd/${fd}`)) return false;
      } catch (error) {
        // Not a confined descendant (no marker above): an unreadable unrelated process
        // (e.g. a non-dumpable agent) cannot hold a pathname-only future use for us.
        if (nnp === "1" && code(error) !== "ENOENT" && code(error) !== "ESRCH") return false;
      }
    } catch (error) {
      if (code(error) === "ENOENT" || code(error) === "ESRCH") continue; // exited while scanning
      return false; // unreadable state is unresolved
    }
  }
  return true;
};

/**
 * Next-session sweep: bounded, and it applies the same two checks to temps retained by an
 * ended session. A live owner, an unconfirmed launch, or any doubt keeps the directory.
 */
const sweep = (parent: string, own: string): void => {
  let names: string[];
  try { names = fs.readdirSync(parent).filter(name => /^pi-fabric-landlock-[^/]+\.custody$/.test(name)); }
  catch { return; }
  for (const name of names.slice(0, SWEEP_LIMIT)) {
    const ledger = path.join(parent, name);
    const dir = ledger.slice(0, -LEDGER.length);
    if (dir === own) continue;
    try {
      const fd = fs.openSync(ledger, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      let record: Ledger;
      try {
        if (fs.fstatSync(fd).uid !== process.getuid!()) continue;
        record = JSON.parse(fs.readFileSync(fd, "utf8")) as Ledger;
      } finally { fs.closeSync(fd); }
      if (record.unconfirmed || !Array.isArray(record.groups) || typeof record.since !== "number") continue;
      if (record.hostStart === undefined || startTicks(record.host) === record.hostStart) continue; // owner alive
      let stat: fs.BigIntStats | undefined;
      try { stat = fs.lstatSync(dir, { bigint: true }); }
      catch (error) { if (code(error) === "ENOENT") { fs.rmSync(ledger, { force: true }); continue; } throw error; }
      if (stat.isSymbolicLink() || !stat.isDirectory() || stat.uid !== BigInt(process.getuid!())
        || String(stat.dev) !== record.dev || String(stat.ino) !== record.ino) continue;
      if (!quiescent(dir, fs.realpathSync(dir), record.since, record.groups.map(Number))) continue;
      fs.rmSync(dir, { recursive: true, force: true });
      fs.rmSync(ledger, { force: true });
    } catch { /* unknown: keep */ }
  }
};

type Launch = (command: string, cwd: string, options: Parameters<BashOperations["exec"]>[2],
  onGroup: (pgid: number) => void) => Promise<{ exitCode: number | null }>;
const launchers = new WeakMap<BashOperations, Launch>();
const foreground = new Set<number>();
let exitHook = false;

/**
 * Pi's local shell backend semantics (detached = setsid, so the shell leads a new process
 * group whose id is its pid; timeout/abort kill the whole group; exit + idle stdio grace),
 * but the host keeps the kernel-reported group id instead of trusting a pid file.
 */
export const groupOperations = (shell: string, args: string[]): BashOperations => {
  const launch: Launch = async (command, cwd, { onData, signal, timeout, env }, onGroup) => {
    if (timeout !== undefined && (!Number.isFinite(timeout) || timeout <= 0)) {
      throw new Error("Invalid timeout: must be a finite number of seconds");
    }
    if (signal?.aborted) throw new Error("aborted");
    try { await fs.promises.access(cwd); }
    catch { throw new Error(`Working directory does not exist: ${cwd}\nCannot execute bash commands.`); }
    // S3/F3: cancellation during the awaited preparation is honoured; never spawn.
    if (signal?.aborted) throw new Error("aborted");
    if (!exitHook) {
      exitHook = true; // as Pi does for its tracked detached children
      process.once("exit", () => { for (const pgid of foreground) try { process.kill(-pgid, "SIGKILL"); } catch { /* gone */ } });
    }
    const child = spawn(shell, [...args, command], { cwd, detached: true, env, stdio: ["ignore", "pipe", "pipe"] });
    const pgid = child.pid;
    // S5: kill/exit custody is established before any fallible observer (ledger) I/O.
    if (pgid) foreground.add(pgid);
    const kill = (): void => {
      if (!pgid) return;
      try { process.kill(-pgid, "SIGKILL"); } catch { try { process.kill(pgid, "SIGKILL"); } catch { /* gone */ } }
    };
    let timedOut = false;
    const timer = timeout === undefined ? undefined : setTimeout(() => { timedOut = true; kill(); }, timeout * 1000);
    if (signal) {
      signal.addEventListener("abort", kill, { once: true });
      if (signal.aborted) kill(); // AbortSignal does not replay an earlier abort
    }
    try {
      const exited = new Promise<number | null>((resolve, reject) => {
        let exited = false; let status: number | null = null; let open = 2; let settled = false;
        let grace: NodeJS.Timeout | undefined;
        const done = (): void => {
          if (settled) return;
          settled = true; clearTimeout(grace);
          child.stdout.destroy(); child.stderr.destroy();
          resolve(status);
        };
        const idle = (): void => { clearTimeout(grace); grace = setTimeout(done, 100); };
        for (const stream of [child.stdout, child.stderr]) {
          stream.on("data", (data: Buffer) => { onData(data); if (exited) idle(); });
          stream.once("end", () => { if (--open === 0 && exited) done(); });
        }
        child.once("error", error => { if (!settled) { settled = true; clearTimeout(grace); reject(error); } });
        child.once("exit", exitStatus => { exited = true; status = exitStatus; if (open === 0) done(); else idle(); });
        child.once("close", exitStatus => { status ??= exitStatus; done(); });
      });
      if (pgid) {
        try { onGroup(pgid); }
        catch (error) {
          // S5: custody persistence failed after spawn: kill the started group and confirm
          // its exit before reporting failure. The group stays in the in-memory set.
          kill();
          await exited.catch(() => undefined);
          throw error;
        }
      }
      const exitCode = await exited;
      if (signal?.aborted) throw new Error("aborted");
      if (timedOut) throw new Error(`timeout:${timeout}`);
      const signalCode = child.signalCode;
      return { exitCode: exitCode ?? (signalCode ? 128 + (os.constants.signals[signalCode] ?? 0) : 1) };
    } finally {
      if (pgid) foreground.delete(pgid);
      clearTimeout(timer);
      signal?.removeEventListener("abort", kill);
    }
  };
  const operations: BashOperations = { exec: (command, cwd, options) => launch(command, cwd, options, () => {}) };
  launchers.set(operations, launch);
  return operations;
};

/** Loaded only for the first enforced local bash call, never at registration. */
export class LandlockBashConfinement {
  readonly helperPath: string;
  readonly #policy: RolePolicy;
  readonly #role = (process.env.SMARTY_ROLE ?? "main").split("@")[0]!;
  readonly #tmpdir: string;
  readonly #tmpPin: PinnedGrant;
  readonly #ownsTmp: boolean;
  readonly #git: string | undefined;
  readonly #agentRun = process.env.PI_FABRIC_AGENT_RUN_DIR;
  /** S1: identities pinned by the trusted host before any confined command ran. */
  readonly #pins = new Map<string, PinnedGrant | undefined>();
  /** S2: confined/escaped operations whose exit is not confirmed by the operations API. */
  #pending = 0;
  /** Process groups of every launched command; release needs each one gone (ESRCH). */
  readonly #groups = new Set<number>();
  /** A launch whose process group is unknown (escape, foreign operations): never auto-delete. */
  #unconfirmed = false;
  readonly #since = bootTicks();
  #closed = false;
  /** Cleanup failed: kept for the next session's sweep; no further in-process attempts. */
  #retained = false;
  #recheck: NodeJS.Timeout | undefined;

  constructor(readonly cwd: string) {
    const root = loadedFabricRoot(import.meta.url);
    if (!root) throw new Error("Cannot locate Fabric Landlock package; refusing unconfined execution");
    this.helperPath = path.join(root, "dist/native/fabric-landlock");
    fs.accessSync(this.helperPath, fs.constants.X_OK);
    this.#policy = JSON.parse(fs.readFileSync(path.join(root, "config/landlock-roles.json"), "utf8")) as RolePolicy;
    this.#git = gitCommonDir(cwd);
    const supplied = process.env.TMPDIR;
    let stat: fs.Stats | undefined;
    try { if (supplied && path.isAbsolute(supplied)) stat = fs.lstatSync(supplied); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    const privateTmp = !!stat && stat.isDirectory() && !stat.isSymbolicLink()
      && stat.uid === process.getuid!() && (stat.mode & 0o077) === 0
      && !unsafe(supplied!) && fs.realpathSync(supplied!) === path.resolve(supplied!)
      && !["/", "/tmp", "/var/tmp", os.homedir()].includes(fs.realpathSync(supplied!));
    this.#ownsTmp = !privateTmp;
    this.#tmpdir = privateTmp ? supplied! : fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-landlock-"));
    if (this.#ownsTmp) fs.chmodSync(this.#tmpdir, 0o700);
    this.#tmpPin = pinPath(this.#tmpdir)!;
    if (this.#ownsTmp) {
      this.#ledger(fs.constants.O_EXCL);
      sweep(path.dirname(this.#tmpdir), this.#tmpdir); // retained temps of earlier sessions
    }
    // Pin every session-stable grant now, from trusted host state. Later calls
    // never re-credit a pathname a confined command may have replaced.
    for (const entry of this.#entries()) {
      if (entry === "$RUN_DIR") continue;
      const expanded = this.#expand(entry);
      this.#pins.set(entry, expanded === undefined ? undefined : pinPath(expanded));
    }
  }

  /** Release the generated temp only with confirmed quiescence; otherwise retain it. */
  close(): void {
    this.#closed = true;
    this.#release();
  }

  /**
   * S4/F4: cleanup is housekeeping. A failure (e.g. EACCES on a non-writable nested
   * directory) must never escape into close/settlement or a timer callback, where it
   * would crash the live Pi process. Retain the directory and its custody ledger.
   */
  #release(): void {
    try { this.#tryRelease(); }
    catch {
      clearInterval(this.#recheck); this.#recheck = undefined;
      this.#retained = true;
    }
  }

  #tryRelease(): void {
    if (this.#retained || !this.#closed || !this.#ownsTmp || this.#pending > 0) return;
    // Unknown process-group custody: never delete (the ledger also bars the sweep).
    if (this.#unconfirmed) { clearInterval(this.#recheck); this.#recheck = undefined; return; }
    // Identity discipline for cleanup too: remove only the directory we created.
    let stat: fs.BigIntStats | undefined;
    try { stat = fs.lstatSync(this.#tmpdir, { bigint: true }); } catch { /* already gone */ }
    if (!stat || stat.isSymbolicLink() || stat.dev !== this.#tmpPin.dev || stat.ino !== this.#tmpPin.ino) {
      clearInterval(this.#recheck); this.#recheck = undefined; return;
    }
    // A resolved shell is not quiescence: retain until every process group is gone and
    // no possible descendant/holder remains; re-check while Pi lives, else the next
    // session's bounded sweep applies the same checks from the ledger.
    if (!quiescent(this.#tmpdir, this.#tmpPin.real, this.#since, this.#groups)) {
      this.#recheck ??= setInterval(() => this.#release(), 1000).unref();
      return;
    }
    clearInterval(this.#recheck); this.#recheck = undefined;
    fs.rmSync(this.#tmpdir, { recursive: true, force: true });
    fs.rmSync(`${this.#tmpdir}${LEDGER}`, { force: true });
  }

  /** Settlement ends launch custody; deletion still needs confirmed quiescence. */
  #custody<T>(operation: Promise<T>): Promise<T> {
    this.#pending++;
    const settle = (): void => { this.#pending--; this.#release(); };
    return operation.then(result => { settle(); return result; }, error => { settle(); throw error; });
  }

  #launch(ops: BashOperations, command: string, cwd: string,
    options: Parameters<BashOperations["exec"]>[2], escape: boolean): Promise<{ exitCode: number | null }> {
    const launch = launchers.get(ops);
    // An unconfined escape can leave its group without the no_new_privs marker, and
    // foreign operations do not report their group: neither is confirmable.
    if (!launch || escape) this.#unknown();
    if (!launch) return ops.exec(command, cwd, options);
    return launch(command, cwd, options, pgid => {
      for (const known of this.#groups) if (groupGone(known)) this.#groups.delete(known); // bounded ledger
      this.#groups.add(pgid);
      this.#ledger();
    });
  }

  #unknown(): void {
    if (this.#unconfirmed) return;
    this.#unconfirmed = true;
    this.#ledger();
  }

  /** Host-only custody record next to the temp, for the next session's sweep. */
  #ledger(create = 0): void {
    if (!this.#ownsTmp) return;
    const record: Ledger = {
      host: process.pid, hostStart: startTicks(process.pid), since: this.#since,
      dev: String(this.#tmpPin.dev), ino: String(this.#tmpPin.ino),
      groups: [...this.#groups], unconfirmed: this.#unconfirmed,
    };
    const fd = fs.openSync(`${this.#tmpdir}${LEDGER}`, fs.constants.O_WRONLY | fs.constants.O_CREAT
      | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW | create, 0o600);
    try { fs.writeFileSync(fd, JSON.stringify(record)); } finally { fs.closeSync(fd); }
  }

  /**
   * S2: launch custody taken before awaited middleware/spawn preparation. The caller
   * releases it only when the tool call settled; an inner operation that started keeps
   * its own custody, and an unknown exit retains the temp.
   */
  hold(): () => void {
    this.#pending++;
    let held = true;
    return () => {
      if (!held) return;
      held = false;
      this.#pending--;
      this.#release();
    };
  }

  get pendingOperations(): number { return this.#pending; }
  get tmpdir(): string { return this.#tmpdir; }

  #entries(): string[] {
    return [...this.#policy.default, ...(this.#policy.roles[this.#role] ?? [])].map(grant => grant.path);
  }

  #expand(entry: string, runDir?: string): string | undefined {
    const values: Record<string, string | undefined> = {
      $CWD: this.cwd, $TMPDIR: this.#tmpdir, $RUN_DIR: runDir,
      $AGENT_RUN_DIR: this.#agentRun, $GIT_COMMON_DIR: this.#git,
    };
    return entry.startsWith("$") ? values[entry]
      : entry.startsWith("~/") ? path.join(os.homedir(), entry.slice(2)) : entry;
  }

  #grants(runDir: string): PinnedGrant[] {
    const grants: PinnedGrant[] = [];
    for (const entry of this.#entries()) {
      if (entry === "$RUN_DIR") {
        // Fresh host-created run directory: no symlinked component, owned by us.
        const pinned = pinPath(runDir);
        if (!pinned) continue;
        const stat = fs.lstatSync(runDir);
        if (pinned.real !== path.resolve(runDir) || !stat.isDirectory() || stat.uid !== process.getuid!()) {
          throw new Error("Landlock run directory is not an owned real directory; refusing");
        }
        grants.push(pinned);
        continue;
      }
      const expanded = this.#expand(entry);
      if (expanded === undefined) continue;
      const pinned = this.#pins.get(entry);
      const current = pinPath(expanded);
      // Absent when the trusted host pinned grants: omitted for this confinement's
      // lifetime. A later-created root (or a dangling alias whose target a confined
      // command created) is never admitted; provision it and start a new session.
      if (!pinned) continue;
      if (!current) continue; // Removed: grant nothing (fail closed).
      if (!sameGrant(pinned, current)) {
        throw new Error(`Landlock write grant ${entry} changed identity since it was approved (${pinned.real}); refusing. Restore it or start a new session.`);
      }
      grants.push(pinned);
    }
    const seen = new Set<string>();
    return grants.filter(grant => !seen.has(grant.real) && !!seen.add(grant.real));
  }

  operations(confined: BashOperations, unconfined: BashOperations, shell: string,
    runDir: string, escape: boolean, originalCommand: string): BashOperations {
    const grants = this.#grants(runDir);
    const lines = grants.map(({ dev, ino, real }) => `${dev}:${ino}:${real}`);
    return { exec: async (command, cwd, options) => {
      // Fence: no launch after close, even from delayed middleware.
      if (this.#closed) throw new Error("Landlock confinement is closed; refusing a late launch");
      // Escape logging is mandatory and happens before spawn. Do not log command
      // text (it may contain secrets); record a digest and nested tool correlation.
      const auditDir = path.join(this.cwd, ".pi");
      fs.mkdirSync(auditDir, { recursive: true });
      const auditPath = path.join(auditDir, "landlock-audit.jsonl");
      // Pin the directory so a concurrently renamed .pi cannot redirect this
      // host write. Never follow audit symlinks, hard links, FIFOs or devices.
      const directoryFd = fs.openSync(auditDir, fs.constants.O_RDONLY
        | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
      try {
        const auditFd = fs.openSync(`/proc/self/fd/${directoryFd}/landlock-audit.jsonl`,
          fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT
          | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK, 0o600);
        try {
          const stat = fs.fstatSync(auditFd);
          if (!stat.isFile() || stat.nlink !== 1 || stat.uid !== process.getuid!()) {
            throw new Error("Landlock audit must be an owned regular file with one link");
          }
          fs.writeFileSync(auditFd, JSON.stringify({
            at: new Date().toISOString(), event: escape ? "escape" : "enforce",
            role: this.#role, cwd, runDir,
            commandSha256: createHash("sha256").update(originalCommand).digest("hex"),
            ...(escape ? {} : { writes: grants.map(grant => grant.real) }),
          }) + "\n");
        } finally { fs.closeSync(auditFd); }
      } finally { fs.closeSync(directoryFd); }
      const env: NodeJS.ProcessEnv = { ...options.env, TMPDIR: this.#tmpdir };
      delete env.PI_FABRIC_LANDLOCK_ESCAPE;
      delete env.PI_FABRIC_LANDLOCK_SHELL;
      delete env.PI_FABRIC_LANDLOCK_WRITES;
      if (escape) {
        options.onData(Buffer.from(`[Landlock escape: unconfined command; recorded in ${auditPath}]\n`));
        return this.#custody(this.#launch(unconfined, command, cwd, { ...options, env }, true));
      }
      env.PI_FABRIC_LANDLOCK_SHELL = shell;
      // dev:ino:path — the helper binds each rule to this identity, not the name.
      env.PI_FABRIC_LANDLOCK_WRITES = lines.join("\n");
      return this.#custody(this.#launch(confined, command, cwd, { ...options, env }, false));
    } };
  }
}
