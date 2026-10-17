/**
 * smarty-dev#8920: the Windows half of the mesh state gate (state-gate.ts). Node and libuv expose no Windows
 * security descriptor API (fs.chmod maps only the read-only attribute; fs.access does not evaluate ACLs), so the
 * gate reads native descriptors through ONE pinned Windows PowerShell child per uncached gate call (windows-powershell.ts):
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
 * - ancestors: owned by one of those trusted SIDs, or by LocalService (S-1-5-19) or NetworkService (S-1-5-20)
 *   as owner-only exceptions; these service SIDs are never trusted as DACL trustees. A foreign grant that applies
 *   to the ancestor itself may add
 *   files or subdirectories (the sticky analogue: C:\ grants Authenticated Users AD) and nothing else, so nobody
 *   else can delete, rename, re-ACL or take over a component. Inherit-only grants (CREATOR OWNER among them) are
 *   skipped on ordinary ancestors because they are not the ancestor's own access. A directory immediately used to
 *   create a missing component is stricter: foreign inheritable write/full/modify/delete grants are refused before
 *   that component is made visible; CREATOR OWNER remains the explicit safe inheritance exception. Owner admission
 *   is not isolation from a service: an admitted owner can WRITE_DAC and re-ACL its ancestor (R-SERVICE-OWNER-REACL;
 *   lead-owned follow-up issue/link pending).
 * - a root this process just created (a non-recursive mkdir that succeeded) gets, through the built-in icacls, an
 *   explicit owner (the current user) and a protected DACL: current user and SYSTEM full control, (OI)(CI); it is read
 *   back in the same child. An existing root is never repaired or adopted.
 * Existing-path reads retain warn-once/fail-open for missing tooling, timeout or global script failure (issue step 5;
 * nonzero/timeout residual: smarty-dev#13459). A newly created root MUST have its private ACL set: every protect
 * runner failure refuses. Output overflow (incomplete evidence) and malformed successful output always refuse.
 */
import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { childProcessEnvironment } from "../core/atomic-write.js";
import { windowsSecurityPowerShell } from "../storage/windows-powershell.js";

const TRUSTED_SIDS = new Set([
  "S-1-5-18", // LocalSystem
  "S-1-5-32-544", // Built-in Administrators
  "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", // TrustedInstaller
]);
// Owner-only ancestor admission. Keep this separate from TRUSTED_SIDS: these SIDs never become trusted trustees.
const SERVICE_OWNER_SIDS = new Set([
  "S-1-5-19", // LocalService
  "S-1-5-20", // NetworkService
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
  /** Require the native snapshot to prove that state.db is present (no-create/post-create gate). */
  requireDatabase?: boolean;
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
const malformedAceRefusal = (target: string, ace: unknown, ordinal: number): string => {
  const context = [`ordinal ${ordinal}`];
  if (record(ace) && typeof ace.sid === "string" && SID.test(ace.sid)) context.push(`trustee ${ace.sid}`);
  if (record(ace) && uint32(ace.mask)) context.push(`mask 0x${ace.mask.toString(16)}`);
  return `${target} has an unsupported or unproven ACE (${context.join(", ")})`;
};

const ancestorsOf = (dir: string): string[] => {
  const chain: string[] = [];
  for (let at = path.win32.dirname(dir); ; at = path.win32.dirname(at)) {
    chain.push(at);
    if (at === path.win32.dirname(at)) return chain;
  }
};

type Role = "ancestor" | "root" | "directory" | "file" | "creation-parent";

type AdmissionStatus = "present" | "absent" | "invalid" | "unreadable";
type AdmissionIdentity = {
  root?: string; db?: string; guard?: string;
  rootStatus?: AdmissionStatus;
  dbStatus?: AdmissionStatus;
  invalidAncestor?: string;
};
type RootAdmission = { plan: string; identity: AdmissionIdentity };
// Process-local verified root admissions only. One exact plan per root, bounded by roots, not by caller plans.
const rootAdmissions = new Map<string, RootAdmission>();
const MAX_ROOT_ADMISSIONS = 64;
const fileIdentity = (stat: fs.BigIntStats): string | undefined =>
  typeof stat.dev === "bigint" && stat.dev >= 0n && typeof stat.ino === "bigint" && stat.ino > 0n
    ? `${stat.dev}:${stat.ino}` : undefined;

// No-follow, volume-down guard of every lexical component (including the real ancestor plan). Optional children
// contribute types/reparse checks, not identities: later SQLite sidecars/flags inherit the admitted strict root.
// Unknown IDs or unreadable paths disable reuse, never substitute a new refusal for the native policy.
// Keep positive missing/type observations separate: a known target cannot vanish during a successful proof.
const admissionIdentity = (root: string, db: string, paths: Map<string, Role>): AdmissionIdentity => {
  const components = new Map<string, { target: string; role: Role }>();
  for (const [target, role] of paths) components.set(target.toLowerCase(), { target, role });
  for (const target of paths.keys()) {
    for (const ancestor of ancestorsOf(target)) {
      const key = ancestor.toLowerCase();
      if (!components.has(key)) components.set(key, { target: ancestor, role: "ancestor" });
    }
  }
  const ordered = [...components.values()].sort((a, b) => a.target.split("\\").length - b.target.split("\\").length);
  const identity: AdmissionIdentity = {};
  const ancestors: [string, string][] = [];
  let proven = true;
  for (const { target, role } of ordered) {
    const key = target.toLowerCase();
    const status = key === root.toLowerCase() ? "rootStatus" : key === db.toLowerCase() ? "dbStatus" : undefined;
    let stat: fs.BigIntStats | undefined;
    try { stat = fs.lstatSync(target, { bigint: true, throwIfNoEntry: false }); }
    catch {
      if (status !== undefined) identity[status] = "unreadable";
      proven = false;
      continue;
    }
    if (!stat) {
      if (status !== undefined) identity[status] = "absent";
      if (role !== "file" && role !== "directory") proven = false;
      // A missing ancestor must not hide the required root/DB observations below it.
      continue;
    }
    // Node's Windows lstat identifies symbolic links AND junctions without following the final component.
    if (stat.isSymbolicLink() || (role === "file" ? !stat.isFile() : !stat.isDirectory())) {
      if (status !== undefined) identity[status] = "invalid";
      if (role === "ancestor" || key === root.toLowerCase()) identity.invalidAncestor = target;
      proven = false;
      continue;
    }
    if (status !== undefined) identity[status] = "present";
    const id = fileIdentity(stat);
    if (id !== undefined && key === root.toLowerCase()) identity.root = id;
    if (id !== undefined && key === db.toLowerCase()) identity.db = id;
    if (role === "ancestor") {
      if (id === undefined) proven = false;
      else ancestors.push([key, id]);
    }
  }
  if (proven) identity.guard = JSON.stringify(ancestors);
  return identity;
};

type NativePresence = "present" | "absent";
type AdmissionDecision =
  | { kind: "cache" }
  | { kind: "fresh" }
  | { kind: "refuse"; target: "root" | "db"; reason: string };

// Matrix for the two security-critical targets. Native presence is the middle observation: an absent DB is
// admissible only for the pre-create proof when the after observation confirms absence and no positive before
// presence was seen. Unknown IDs and unreadable lstats remain full-proof/uncached, never a new refusal by themselves.
const admissionDecision = (before: AdmissionIdentity, native: { root: NativePresence; db: NativePresence },
  after: AdmissionIdentity, requireDatabase: boolean): AdmissionDecision => {
  if (before.invalidAncestor !== undefined || after.invalidAncestor !== undefined) {
    return { kind: "refuse", target: "root", reason: `${before.invalidAncestor ?? after.invalidAncestor} changed type while ACL was read` };
  }
  if (native.root === "absent") return { kind: "refuse", target: "root", reason: "the root is missing" };
  const targets = ["root", "db"] as const;
  for (const target of targets) {
    const beforeStatus = before[`${target}Status`] ?? "unreadable";
    const afterStatus = after[`${target}Status`] ?? "unreadable";
    const beforeId = before[target];
    const afterId = after[target];
    if (beforeStatus === "invalid" || afterStatus === "invalid") {
      return { kind: "refuse", target, reason: "it changed to the wrong type or a link" };
    }
    if (beforeId !== undefined && afterId !== undefined && beforeId !== afterId) {
      return { kind: "refuse", target, reason: "its identity changed while ACL was read" };
    }
    if (target === "db" && native.db === "absent") {
      if (beforeStatus === "present" || afterStatus !== "absent") {
        return { kind: "refuse", target, reason: "state.db presence was not covered by the native snapshot" };
      }
      if (requireDatabase) {
        return { kind: "refuse", target, reason: "the native snapshot did not prove state.db is present" };
      }
    } else {
      // A positive before observation that disappears, or a positive native observation that is absent after it,
      // is a replacement race. Conversely, a target absent before native proof may not appear without a create path.
      if (beforeStatus === "absent" && native[target] === "present") {
        return { kind: "refuse", target, reason: "it appeared during ACL proof without a create path" };
      }
      if (native[target] === "present" && afterStatus === "absent") {
        return { kind: "refuse", target, reason: "it disappeared while ACL was read" };
      }
      if (beforeStatus === "present" && afterStatus === "absent") {
        return { kind: "refuse", target, reason: "it disappeared while ACL was read" };
      }
      if (beforeStatus === "absent" && afterStatus === "present") {
        return { kind: "refuse", target, reason: "it appeared while ACL was read without a create path" };
      }
    }
  }
  const cacheable = targets.every(target => before[`${target}Status`] === "present"
    && after[`${target}Status`] === "present" && before[target] !== undefined && before[target] === after[target])
    && native.root === "present" && native.db === "present";
  return cacheable ? { kind: "cache" } : { kind: "fresh" };
};

/**
 * The refusal for this root, or undefined when it may be opened (including the warned tooling gap).
 * Default-runner Windows root admissions reuse only a complete verified proof with unchanged root/DB identities
 * and fresh component guards. Same-identity ACL edits by the trusted authority class are intentionally not re-read.
 * Other roles, protect/readback, injected runners, and unproven identities retain fresh native reads.
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
  const defaultWindows = process.platform === "win32" && run === runPowerShell;
  const rootKey = root.toLowerCase();
  // Only direct children inherit this root's proof. Unusual external/nested target plans stay fresh.
  const cacheable = defaultWindows && rootRole === "root" && !request.protect
    && [...request.files, ...request.directories].every(target =>
      path.win32.dirname(path.win32.resolve(target)).toLowerCase() === rootKey);
  const db = request.files[0] === undefined ? undefined : path.win32.resolve(request.files[0]);
  const plan = cacheable && db !== undefined && paths.get(db) === "file"
    ? JSON.stringify([rootKey, path.win32.resolve(request.real).toLowerCase(),
      [...paths].map(([target, role]) => [target.toLowerCase(), role]),
      request.files.map(file => path.win32.resolve(file).toLowerCase()),
      request.directories.map(dir => path.win32.resolve(dir).toLowerCase())]) : undefined;
  const before = plan !== undefined ? admissionIdentity(root, db!, paths) : undefined;
  const cached = cacheable ? rootAdmissions.get(rootKey) : undefined;
  if (cached && cached.plan === plan && before?.root !== undefined && before.db !== undefined
    && before.guard !== undefined && before.root === cached.identity.root && before.db === cached.identity.db
    && before.guard === cached.identity.guard) {
    rootAdmissions.delete(rootKey);
    rootAdmissions.set(rootKey, cached);
    return undefined;
  }
  // A miss must not leave an older proof available after a failed replacement proof or a protect operation.
  if (defaultWindows && rootRole === "root") rootAdmissions.delete(rootKey);
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
    const ancestorRole = role === "ancestor" || role === "creation-parent";
    if (entry.missing === true) {
      // Optional sidecars/flags may be absent. The admission matrix below separately reconciles state.db's
      // native absence with positive before/after presence, and requires it on no-create/post-create gates.
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
    const diagnostics: string[] = [];
    let ownerFailure = false;
    const owner = typeof entry.owner === "string" && SID.test(entry.owner) ? entry.owner : undefined;
    const ownerProven = owner !== undefined;
    if (!ownerProven) {
      const reason = `${target} has an unproven owner`;
      if (strict) return reason;
      diagnostics.push(reason);
      ownerFailure = true;
    }
    const protectedRoot = role === "root" && request.protect === true;
    const owned = owner !== undefined && (ancestorRole ? trusted(owner) || SERVICE_OWNER_SIDS.has(owner)
      : protectedRoot ? owner === user
      : owner === user || (owner === ADMINISTRATORS && tokenOwner === ADMINISTRATORS));
    if (!owned) {
      const ownerReason = owner !== undefined
        ? `${target} is owned by ${owner}, not ${ancestorRole ? "the current user, SYSTEM, Administrators or TrustedInstaller"
          : protectedRoot ? `the current user ${user}, which it was just given` : `the current user ${user} (or Administrators on an elevated run)`}`
        : undefined;
      if (strict) return ownerReason!;
      const reason = ownerReason && owner !== undefined ? `${ownerReason}; owner: ${owner}` : ownerReason;
      if (!ownerFailure && reason) diagnostics.push(reason);
      ownerFailure = true;
    }
    if (role === "root" && request.protect && (entry.control & SE_DACL_PROTECTED) === 0) return `${target} still inherits its ACL after it was made private`;
    if (!Array.isArray(entry.dacl)) {
      const reason = `${target} has a null or unproven DACL (everyone has full control)`;
      if (strict) return reason;
      diagnostics.push(reason);
    } else {
      for (const [ordinal, ace] of (entry.dacl as unknown[]).entries()) {
        if (!record(ace) || (ace.type !== 0 && ace.type !== 1) || !uint32(ace.flags) || (ace.flags & ~0x1f) !== 0
          || typeof ace.sid !== "string" || !SID.test(ace.sid) || !uint32(ace.mask)) {
          const reason = malformedAceRefusal(target, ace, ordinal);
          if (strict) return `${target} has an unsupported or unproven ACE`;
          diagnostics.push(reason);
          continue;
        }
        if (ace.type === 1 || trusted(ace.sid) || (role === "creation-parent" && ace.sid === "S-1-3-0")) continue;
        // CO IO expands to the child's trusted creator. Skip only this ACE, never the remaining DACL: every other
        // foreign allow is metadata-only on strict targets, proving no foreign add-child/write regardless of ACE order
        // or deny ACEs. Every other inherit-only grant is judged too, including CREATOR GROUP. Ordinary ancestors skip
        // IO access; a creation parent instead refuses foreign inheritable writes before a new component is visible.
        if (strict && ace.sid === "S-1-3-0" && (ace.flags & INHERIT_ONLY) !== 0) continue;
        const inheritable = (ace.flags & 0x0b) !== 0; // OBJECT_INHERIT | CONTAINER_INHERIT | INHERIT_ONLY
        if (role === "ancestor" && (ace.flags & INHERIT_ONLY) !== 0) continue;
        if (role === "creation-parent" && inheritable && (ace.mask & ~READ_ONLY_RIGHTS) !== 0) {
          const reason = `${target}, the immediate creation parent of ${root}, grants ${ace.sid} inheritable write, delete or permission rights (mask 0x${ace.mask.toString(16)}); ${PRIVATE_PATH_ADVICE}`;
          if (strict) return reason;
          diagnostics.push(reason);
          continue;
        }
        const allowed = strict ? STRICT_METADATA_RIGHTS : READ_ONLY_RIGHTS | ADD_CHILD_RIGHTS;
        if ((ace.mask & ~allowed) !== 0) {
          const reason = `${target}${strict ? "" : `, an ancestor of ${root},`} grants ${ace.sid} ${strict ? "read, " : ""}write, delete or permission rights (mask 0x${ace.mask.toString(16)}); ${PRIVATE_PATH_ADVICE}`;
          if (strict) return reason;
          diagnostics.push(reason);
        }
      }
    }
    if (diagnostics.length > 0) {
      if (ancestorRole && !ownerFailure && owner !== undefined) diagnostics.push(`${target} owner: ${owner}`);
      return diagnostics.join("; ");
    }
  }
  // Only this bottom-of-reader path is a complete structured policy success. In particular the warned
  // undefined return above must never publish an admission. Capture again rather than blessing a replacement
  // with the descriptor read from its predecessor; unknown IDs simply leave this full read uncached.
  if (plan !== undefined && before !== undefined) {
    const after = admissionIdentity(root, db!, paths);
    const entries = snapshot.entries;
    const entryAt = (target: string): unknown => entries[[...paths.keys()].indexOf(target)];
    const nativePresence = (entry: unknown): NativePresence => record(entry) && entry.missing === true ? "absent" : "present";
    const decision = admissionDecision(before, {
      root: nativePresence(entryAt(root)),
      db: nativePresence(entryAt(db!)),
    }, after, request.requireDatabase === true);
    if (decision.kind === "refuse") return `${decision.target === "root" ? root : db} ${decision.reason}`;
    if (decision.kind === "cache" && before.guard !== undefined && after.guard === before.guard) {
      rootAdmissions.set(rootKey, { plan, identity: after });
      if (rootAdmissions.size > MAX_ROOT_ADMISSIONS) rootAdmissions.delete(rootAdmissions.keys().next().value!);
    }
  }
  return undefined;
};
