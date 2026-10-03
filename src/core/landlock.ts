import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import { loadedFabricRoot } from "./agent-dir.js";

export interface LandlockSettings {
  mode: "off" | "enforce";
  /** Host-only kill switch. Project settings cannot override it. */
  disabled: boolean;
}

const ESCAPE = /^\s*PI_FABRIC_LANDLOCK_ESCAPE=1[ \t]+/;
export const landlockCommand = (command: string): { escape: boolean; command: string } => ({
  escape: ESCAPE.test(command), command: command.replace(ESCAPE, ""),
});

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
  #unresolved = false;
  #closed = false;

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
    // Pin every session-stable grant now, from trusted host state. Later calls
    // never re-credit a pathname a confined command may have replaced.
    for (const entry of this.#entries()) {
      if (entry === "$RUN_DIR") continue;
      const expanded = this.#expand(entry);
      this.#pins.set(entry, expanded === undefined ? undefined : pinPath(expanded));
    }
  }

  /** Release the generated temp only after every associated shell exit is confirmed. */
  close(): void {
    this.#closed = true;
    this.#release();
  }

  #release(): void {
    if (!this.#closed || !this.#ownsTmp || this.#pending > 0 || this.#unresolved) return;
    // Identity discipline for cleanup too: remove only the directory we created.
    let stat: fs.BigIntStats;
    try { stat = fs.lstatSync(this.#tmpdir, { bigint: true }); } catch { return; }
    if (stat.isSymbolicLink() || stat.dev !== this.#tmpPin.dev || stat.ino !== this.#tmpPin.ino) return;
    fs.rmSync(this.#tmpdir, { recursive: true, force: true });
  }

  /** Only a resolved operations result confirms exit; a rejection retains the temp. */
  #custody<T>(operation: Promise<T>): Promise<T> {
    this.#pending++;
    return operation.then(result => {
      this.#pending--;
      this.#release();
      return result;
    }, error => {
      this.#pending--;
      this.#unresolved = true; // exit unknown: never delete under a possibly live child
      throw error;
    });
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
        return this.#custody(unconfined.exec(command, cwd, { ...options, env }));
      }
      env.PI_FABRIC_LANDLOCK_SHELL = shell;
      // dev:ino:path — the helper binds each rule to this identity, not the name.
      env.PI_FABRIC_LANDLOCK_WRITES = lines.join("\n");
      return this.#custody(confined.exec(command, cwd, { ...options, env }));
    } };
  }
}
