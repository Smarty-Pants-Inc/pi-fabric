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

/** Loaded only for the first enforced local bash call, never at registration. */
export class LandlockBashConfinement {
  readonly helperPath: string;
  readonly #policy: RolePolicy;
  readonly #role = (process.env.SMARTY_ROLE ?? "main").split("@")[0]!;
  readonly #tmpdir: string;
  readonly #ownsTmp: boolean;
  readonly #git: string | undefined;
  readonly #agentRun = process.env.PI_FABRIC_AGENT_RUN_DIR;

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
      && !["/", "/tmp", "/var/tmp", os.homedir()].includes(fs.realpathSync(supplied!));
    this.#ownsTmp = !privateTmp;
    this.#tmpdir = privateTmp ? supplied! : fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-landlock-"));
    if (this.#ownsTmp) fs.chmodSync(this.#tmpdir, 0o700);
  }

  close(): void {
    if (this.#ownsTmp) fs.rmSync(this.#tmpdir, { recursive: true, force: true });
  }

  #grants(runDir: string): string[] {
    const values: Record<string, string | undefined> = {
      $CWD: this.cwd, $TMPDIR: this.#tmpdir, $RUN_DIR: runDir,
      $AGENT_RUN_DIR: this.#agentRun, $GIT_COMMON_DIR: this.#git,
    };
    const entries = [...this.#policy.default, ...(this.#policy.roles[this.#role] ?? [])];
    return [...new Set(entries.flatMap(({ path: entry }) => {
      const expanded = entry.startsWith("$") ? values[entry]
        : entry.startsWith("~/") ? path.join(os.homedir(), entry.slice(2)) : entry;
      // Absent optional cache/device grants are not broadened to their parent.
      if (expanded === undefined || !fs.existsSync(expanded)) return [];
      if (!path.isAbsolute(expanded) || /[\n\r\0]/.test(expanded)) throw new Error("Invalid Landlock write grant");
      const real = fs.realpathSync(expanded);
      if (/[\n\r\0]/.test(real) || real === "/") throw new Error("Unsafe Landlock write grant");
      return [real];
    }))];
  }

  operations(confined: BashOperations, unconfined: BashOperations, shell: string,
    runDir: string, escape: boolean, originalCommand: string): BashOperations {
    const grants = this.#grants(runDir);
    return { exec: async (command, cwd, options) => {
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
            ...(escape ? {} : { writes: grants }),
          }) + "\n");
        } finally { fs.closeSync(auditFd); }
      } finally { fs.closeSync(directoryFd); }
      const env: NodeJS.ProcessEnv = { ...options.env, TMPDIR: this.#tmpdir };
      delete env.PI_FABRIC_LANDLOCK_ESCAPE;
      delete env.PI_FABRIC_LANDLOCK_SHELL;
      delete env.PI_FABRIC_LANDLOCK_WRITES;
      if (escape) {
        options.onData(Buffer.from(`[Landlock escape: unconfined command; recorded in ${auditPath}]\n`));
        return unconfined.exec(command, cwd, { ...options, env });
      }
      env.PI_FABRIC_LANDLOCK_SHELL = shell;
      env.PI_FABRIC_LANDLOCK_WRITES = grants.join("\n");
      return confined.exec(command, cwd, { ...options, env });
    } };
  }
}
