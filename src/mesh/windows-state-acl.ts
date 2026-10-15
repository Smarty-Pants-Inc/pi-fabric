/**
 * smarty-dev#8920: the Windows half of the mesh state gate (state-gate.ts). Node and libuv expose no Windows
 * security descriptor API (fs.chmod maps only the read-only attribute; fs.access does not evaluate ACLs), so the
 * gate reads native descriptors through ONE pinned Windows PowerShell child per gate call (windows-powershell.ts):
 * the root, its state files and flag directory, and every lexical and real ancestor, in one structured snapshot.
 * Loaded only through state-gate.ts, which is not in the startup graph.
 *
 * Policy (SIDs, never localized names; deny ACEs never cancel an unsafe grant):
 * - root, state.db, -wal, -shm, -journal, flag directory: owned by the current user (TokenUser), or by BUILTIN\Administrators
 *   only when that is this token's default owner (an elevated run creates them so); any other default owner, and
 *   Administrators on a non-elevated run, refuses. A root this process just made private must be TokenUser's. Grants
 *   outside the current user, SYSTEM, Administrators or TrustedInstaller may contain only READ_CONTROL, SYNCHRONIZE
 *   and FILE_READ_ATTRIBUTES, including inherit-only grants. Only CREATOR OWNER inherit-only is excepted: every
 *   other ACE must still pass, so no foreign user can add a child or write. CREATOR GROUP has no exception.
 * - ancestors: owned by one of those trusted SIDs; a foreign grant that applies to the ancestor itself may add
 *   files or subdirectories (the sticky analogue: C:\ grants Authenticated Users AD) and nothing else, so nobody
 *   else can delete, rename, re-ACL or take over a component. Inherit-only grants (CREATOR OWNER among them) are
 *   skipped on ordinary ancestors because they are not the ancestor's own access. A directory immediately used to
 *   create a missing component is stricter: foreign inheritable write/full/modify/delete grants are refused before
 *   that component is made visible; CREATOR OWNER remains the explicit safe inheritance exception.
 * - a root this process just created (a non-recursive mkdir that succeeded) gets, through the built-in icacls, an
 *   explicit owner (the current user) and a protected DACL: current user and SYSTEM full control, (OI)(CI); it is read
 *   back in the same child. An existing root is never repaired or adopted.
 * Existing-path reads retain warn-once/fail-open for missing tooling, timeout or global script failure (issue step 5;
 * nonzero/timeout residual: smarty-dev#13459). A newly created root MUST have its private ACL set: every protect
 * runner failure refuses. Output overflow (incomplete evidence) and malformed successful output always refuse.
 */
import childProcess from "node:child_process";
import path from "node:path";
import { childProcessEnvironment } from "../core/atomic-write.js";
import { windowsSecurityPowerShell } from "../storage/windows-powershell.js";

const TRUSTED_SIDS = new Set([
  "S-1-5-18", // LocalSystem
  "S-1-5-32-544", // Built-in Administrators
  "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", // TrustedInstaller
]);
const ADMINISTRATORS = "S-1-5-32-544";
// Strict targets permit foreign metadata access only: READ_CONTROL, SYNCHRONIZE, FILE_READ_ATTRIBUTES.
const STRICT_METADATA_RIGHTS = 0x00120080;
// Ancestors: read data, read EA, execute/traverse, read attributes, READ_CONTROL, SYNCHRONIZE, GENERIC_READ, GENERIC_EXECUTE.
const READ_ONLY_RIGHTS = 0xa01200a9;
const PRIVATE_PATH_ADVICE = "use a private project folder, or set mesh.root/PI_FABRIC_MESH_ROOT to a private path";
// FILE_ADD_FILE (write data) and FILE_ADD_SUBDIRECTORY (append data) on a directory.
const ADD_CHILD_RIGHTS = 0x6;
const INHERIT_ONLY = 0x08;
const SE_DACL_PROTECTED = 0x1000;
const FILE_ATTRIBUTE_DIRECTORY = 0x10;
const FILE_ATTRIBUTE_REPARSE_POINT = 0x400;
const SID = /^S-1-\d+(?:-\d+)+$/;

// Fixed script; the request travels as JSON in the child environment, never as PowerShell source. Raw descriptors
// keep null DACLs and unsupported ACE types visible. Each path reports its own failure, so an unreadable component
// refuses instead of looking like a tooling gap.
const SNAPSHOT_ACLS = String.raw`
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
try {
  $request = ConvertFrom-Json -InputObject $env:PI_FABRIC_STATE_ACL
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
  $user = $identity.User
  $protect = $null
  if ($request.protect) {
    try {
      $target = [string]$request.protect
      # Do not change an ACL through a reparse point swapped in after the caller's lexical preflight.
      $attributes = [System.IO.File]::GetAttributes($target)
      if (($attributes -band 1024) -ne 0 -or ($attributes -band 16) -eq 0) { throw 'not a plain directory' }
      $before = (Get-Acl -LiteralPath $target).GetOwner([System.Security.Principal.SecurityIdentifier]).Value
      $elevated = $before -eq 'S-1-5-32-544' -and $identity.Owner.Value -eq 'S-1-5-32-544'
      if ($before -ne $user.Value -and -not $elevated) { throw "owner $before" }
      # The built-in icacls from the kernel's system directory (not PATH, not an environment variable); the path and
      # SIDs are separate arguments, never command text.
      $icacls = [System.IO.Path]::Combine([System.Environment]::SystemDirectory, 'icacls.exe')
      $null = & $icacls $target '/setowner' ('*' + $user.Value) '/Q'
      if ($LASTEXITCODE -ne 0) { throw "icacls /setowner exited $LASTEXITCODE" }
      $null = & $icacls $target '/inheritance:r' '/grant:r' ('*' + $user.Value + ':(OI)(CI)F') '*S-1-5-18:(OI)(CI)F' '/Q'
      if ($LASTEXITCODE -ne 0) { throw "icacls /inheritance:r /grant:r exited $LASTEXITCODE" }
      $protect = 'ok'
    } catch { $protect = 'failed: ' + $_.Exception.Message }
  }
  # Existence comes from GetAttributes alone: only FileNotFound/DirectoryNotFound mean missing. Anything else
  # (access denied included) is that entry's error, so it refuses; it never fails the whole run or looks absent.
  $entries = @(foreach ($p in $request.paths) {
    try {
      $attributes = $null
      try { $attributes = [System.IO.File]::GetAttributes($p) }
      catch [System.IO.FileNotFoundException], [System.IO.DirectoryNotFoundException] { $attributes = $null }
      if ($null -eq $attributes) { @{ path = $p; missing = $true } } else {
      $raw = [System.Security.AccessControl.RawSecurityDescriptor]::new((Get-Acl -LiteralPath $p).GetSecurityDescriptorBinaryForm(), 0)
      $dacl = $null
      if ($null -ne $raw.DiscretionaryAcl) {
        $dacl = @(foreach ($ace in $raw.DiscretionaryAcl) {
          $sid = $null
          $mask = $null
          if ($ace -is [System.Security.AccessControl.KnownAce]) {
            $sid = $ace.SecurityIdentifier.Value
            $mask = [uint32]([long]$ace.AccessMask -band 4294967295)
          }
          @{ type = [int]$ace.AceType; flags = [int]$ace.AceFlags; sid = $sid; mask = $mask }
        })
      }
      @{ path = $p; attributes = [int]$attributes; owner = $raw.Owner.Value; control = [int]$raw.ControlFlags; dacl = $dacl }
      }
    } catch {
      $message = 'unreadable'
      try { $message = [string]$_.Exception.Message } catch { }
      @{ path = $p; error = $message }
    }
  })
  @{ userSid = $user.Value; ownerSid = $identity.Owner.Value; protect = $protect; entries = $entries } | ConvertTo-Json -Compress -Depth 8
} catch {
  [Console]::Error.WriteLine($_.Exception.Message)
  exit 1
}
`;

export interface WindowsStateAclRequest {
  /** The state root, resolved; `real` is its realpath (the real ancestor chain). */
  root: string;
  real: string;
  /**
   * What `root` is: the state root (default), or a single private path judged by itself (assertPrivatePath's direct
   * callers: the flag directory, a state file), with its parent chain as ancestors. `ancestor` is an ordinary
   * existing ancestor; `creation-parent` is the immediate parent preflighted before a missing component is created.
   */
  role?: "root" | "directory" | "file" | "ancestor" | "creation-parent";
  /** Strict targets inside the root; a missing one passes (as in assertPrivatePath). */
  files: string[];
  directories: string[];
  /** Set the private ACL first: only for a root this process just created. Every runner failure then refuses. */
  protect?: boolean;
}

/** Runs the child with the request in PI_FABRIC_STATE_ACL and returns its stdout; throws when it cannot run. */
export type WindowsAclRunner = (request: string) => string;

const runPowerShell: WindowsAclRunner = (request) => {
  const command = windowsSecurityPowerShell(SNAPSHOT_ACLS, { ...process.env, PI_FABRIC_STATE_ACL: request });
  return childProcess.execFileSync(command.file, command.args, {
    env: childProcessEnvironment(command.env),
    encoding: "utf8", windowsHide: true, timeout: 20_000, maxBuffer: 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
};

let warned = false;
const warnUnverified = (root: string, error: unknown): void => {
  if (warned) return;
  warned = true;
  const reason = error instanceof Error ? error.message.split("\n")[0] : String(error);
  process.emitWarning(`[pi-fabric] Fabric mesh SQLite state could not read Windows ACLs (${reason}); `
    + `${root} and its ancestors are opened without an ACL check (smarty-dev#8920).`, { code: "FABRIC_MESH_STATE_ACL_UNVERIFIED" });
};

const record = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const uint32 = (value: unknown): value is number => typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffffffff;

const ancestorsOf = (dir: string): string[] => {
  const chain: string[] = [];
  for (let at = path.win32.dirname(dir); ; at = path.win32.dirname(at)) {
    chain.push(at);
    if (at === path.win32.dirname(at)) return chain;
  }
};

type Role = "ancestor" | "root" | "directory" | "file" | "creation-parent";

/**
 * The refusal for this root, or undefined when it may be opened (including the warned tooling gap).
 * One child process per call; nothing is cached across calls.
 */
export const windowsStateAclRefusal = (request: WindowsStateAclRequest, run: WindowsAclRunner = runPowerShell): string | undefined => {
  const root = path.win32.resolve(request.root);
  // One entry per component, first role wins; NTFS names compare case-insensitively.
  const seen = new Set<string>();
  const paths = new Map<string, Role>();
  const add = (target: string, role: Role) => {
    const key = target.toLowerCase();
    if (!seen.has(key)) { seen.add(key); paths.set(target, role); }
  };
  const rootRole: Role = request.role ?? "root";
  if (request.protect && rootRole !== "root") throw new Error("only a state root is ever made private");
  add(root, rootRole);
  for (const dir of [...ancestorsOf(root), ...ancestorsOf(path.win32.resolve(request.real))]) add(dir, "ancestor");
  for (const dir of request.directories) add(path.win32.resolve(dir), "directory");
  for (const file of request.files) add(path.win32.resolve(file), "file");
  let output: string;
  try { output = run(JSON.stringify({ protect: request.protect ? root : null, paths: [...paths.keys()] })); }
  catch (error) {
    // A read-only compatibility fallback cannot prove that a new root was made private. Never create state in it
    // when the setter did not run or failed, even if missing tooling would be accepted for an existing-root read.
    if (request.protect) {
      const reason = error instanceof Error ? error.message.split("\n")[0] : String(error);
      return `${root} was created but its private ACL could not be set (native ACL reader failed: ${reason})`;
    }
    // execFileSync reports either stream overflowing maxBuffer as ENOBUFS (Node 24 spawnSync) or the explicit
    // max-buffer code. The tool ran but the snapshot is incomplete: this is not the accepted tooling gap.
    if (record(error) && (error.code === "ENOBUFS" || error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER")) {
      return `${root} has an incomplete Windows ACL snapshot (native reader output exceeded its buffer limit)`;
    }
    warnUnverified(root, error);
    return undefined;
  }
  let snapshot: unknown;
  try { snapshot = JSON.parse(output); } catch { return `${root} has an unreadable Windows ACL snapshot`; }
  if (!record(snapshot) || typeof snapshot.userSid !== "string" || !SID.test(snapshot.userSid) || !Array.isArray(snapshot.entries)
    || snapshot.entries.length !== paths.size) return `${root} has an invalid Windows ACL snapshot`;
  const user = snapshot.userSid;
  // TokenOwner: the default owner of what this token creates. An elevated administrator's is BUILTIN\Administrators,
  // so a root, state.db (fs.open), SQLite's -wal/-shm and the flag directory made by an elevated run come out owned
  // by it. Exactly that case is accepted on every strict target (lead ruling, smarty-dev#8920): owner Administrators
  // AND this token's default owner Administrators. Any other default owner (a group a token may name as owner, whose
  // other members could write) is not, nor Administrators on a non-elevated run. A root this call made private must
  // still read back owned by the current user: the protect step set that owner explicitly.
  if (typeof snapshot.ownerSid !== "string" || !SID.test(snapshot.ownerSid)) return `${root} has an invalid Windows ACL snapshot`;
  const tokenOwner = snapshot.ownerSid;
  if (request.protect && snapshot.protect !== "ok") {
    return `${root} was created but its private ACL could not be set (${String(snapshot.protect)})`;
  }
  const trusted = (sid: string) => sid === user || TRUSTED_SIDS.has(sid);
  for (const [index, [target, role]] of [...paths].entries()) {
    const entry: unknown = snapshot.entries[index];
    if (!record(entry) || entry.path !== target) return `${target} has an invalid Windows ACL snapshot entry`;
    const strict = role !== "ancestor" && role !== "creation-parent";
    if (entry.missing === true) {
      if (role === "file" || role === "directory") continue;
      return `${target} vanished while its Windows ACL was read`;
    }
    if (typeof entry.error === "string") return `${target} has an unreadable Windows security descriptor (${entry.error})`;
    if (!uint32(entry.attributes) || !uint32(entry.control)) return `${target} has an invalid Windows ACL snapshot entry`;
    const directory = (entry.attributes & FILE_ATTRIBUTE_DIRECTORY) !== 0;
    if ((entry.attributes & FILE_ATTRIBUTE_REPARSE_POINT) !== 0) {
      // An ancestor junction or symbolic link: its descriptor as read here is ambiguous (the link's or its target's),
      // so it is refused rather than guessed (lead ruling, smarty-dev#8920). No-follow native reads are out of scope.
      return strict ? `${target} is a reparse point`
        : `${target}, an ancestor of ${root}, is a reparse point (junction or symbolic link); `
          + "set the mesh state root to a path with no junctions or symbolic links in it";
    }
    if (strict && directory !== (role !== "file")) return `${target} is not a ${role === "file" ? "regular file" : "directory"}`;
    if (typeof entry.owner !== "string" || !SID.test(entry.owner)) return `${target} has an unproven owner`;
    const protectedRoot = role === "root" && request.protect === true;
    const owned = role === "ancestor" || role === "creation-parent" ? trusted(entry.owner)
      : protectedRoot ? entry.owner === user
      : entry.owner === user || (entry.owner === ADMINISTRATORS && tokenOwner === ADMINISTRATORS);
    if (!owned) {
      return `${target} is owned by ${entry.owner}, not ${role === "ancestor" || role === "creation-parent" ? "the current user, SYSTEM, Administrators or TrustedInstaller"
        : protectedRoot ? `the current user ${user}, which it was just given` : `the current user ${user} (or Administrators on an elevated run)`}`;
    }
    if (role === "root" && request.protect && (entry.control & SE_DACL_PROTECTED) === 0) return `${target} still inherits its ACL after it was made private`;
    if (!Array.isArray(entry.dacl)) return `${target} has a null or unproven DACL (everyone has full control)`;
    for (const ace of entry.dacl as unknown[]) {
      if (!record(ace) || (ace.type !== 0 && ace.type !== 1) || !uint32(ace.flags) || (ace.flags & ~0x1f) !== 0
        || typeof ace.sid !== "string" || !SID.test(ace.sid) || !uint32(ace.mask)) return `${target} has an unsupported or unproven ACE`;
      if (ace.type === 1 || trusted(ace.sid) || (role === "creation-parent" && ace.sid === "S-1-3-0")) continue;
      // CO IO expands to the child's trusted creator. Skip only this ACE, never the remaining DACL: every other
      // foreign allow is metadata-only on strict targets, proving no foreign add-child/write regardless of ACE order
      // or deny ACEs. Every other inherit-only grant is judged too, including CREATOR GROUP. Ordinary ancestors skip
      // IO access; a creation parent instead refuses foreign inheritable writes before a new component is visible.
      if (strict && ace.sid === "S-1-3-0" && (ace.flags & INHERIT_ONLY) !== 0) continue;
      const inheritable = (ace.flags & 0x0b) !== 0; // OBJECT_INHERIT | CONTAINER_INHERIT | INHERIT_ONLY
      if (role === "ancestor" && (ace.flags & INHERIT_ONLY) !== 0) continue;
      if (role === "creation-parent" && inheritable && (ace.mask & ~READ_ONLY_RIGHTS) !== 0) {
        return `${target}, the immediate creation parent of ${root}, grants ${ace.sid} inheritable write, delete or permission rights (mask 0x${ace.mask.toString(16)}); ${PRIVATE_PATH_ADVICE}`;
      }
      const allowed = strict ? STRICT_METADATA_RIGHTS : READ_ONLY_RIGHTS | ADD_CHILD_RIGHTS;
      if ((ace.mask & ~allowed) !== 0) {
        return `${target}${strict ? "" : `, an ancestor of ${root},`} grants ${ace.sid} ${strict ? "read, " : ""}write, delete or permission rights (mask 0x${ace.mask.toString(16)}); ${PRIVATE_PATH_ADVICE}`;
      }
    }
  }
  return undefined;
};
