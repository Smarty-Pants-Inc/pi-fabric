import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as custody from "../src/agents/worktree-manager.js";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import * as windowsCommands from "../src/storage/windows-powershell-command.js";
import { addCloneFirstWorktree } from "../src/agents/clone-first-worktree.js";

// Keep reason: data safety at a filesystem/Git boundary. No fixture deletion.
// Local Git is recorded, never invoked. Hosted runners exercise the real tools.
const ownedFixtures: string[] = [];
const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "external-custody-")));
  ownedFixtures.push(root);
  console.info(`retained external custody fixture: ${root}`);
  return root;
};
const mkdir = (directory: string) => { fs.mkdirSync(directory, { recursive: true, mode: 0o700 }); return directory; };
const marker = (directory: string, worktree = false) => {
  mkdir(directory);
  if (worktree) fs.writeFileSync(path.join(directory, ".git"), "gitdir: synthetic-registration\n");
  else mkdir(path.join(directory, ".git"));
  return directory;
};
const sourceAt = (root: string) => {
  const source = mkdir(path.join(root, "source"));
  fs.writeFileSync(path.join(source, "raw.JSONL"), Buffer.from([0, 255, 13, 10, 123]), { mode: 0o600 });
  return source;
};
const outside = (destination: string, roots: string[]) => {
  const actual = fs.realpathSync(destination);
  for (const root of roots) {
    const relative = path.relative(fs.realpathSync(root), actual);
    expect(relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative), `${actual} remains inside ${root}`).toBe(true);
  }
};
const result = (fn: () => unknown) => { try { fn(); } catch (error) { return error as custody.CleanupCustodyDeferredError; } throw new Error("expected deferred cleanup"); };
// Metadata seam models a source mount with no external same-device candidate.
// Actual payload I/O, hashing, receipt writes and fsyncs remain real.
const crossDevice = (source: string, wrongOwner = false) => {
  const lstat = fs.lstatSync, rename = fs.renameSync;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean; throwIfNoEntry?: boolean }) => {
    const stat = lstat(file, options as { bigint: false; throwIfNoEntry: false });
    if (wrongOwner && String(file) === path.join(source, "raw.JSONL") && options?.bigint && stat) {
      return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: BigInt(stat.uid) + 1n });
    }
    return String(file) === source && !options?.bigint && stat ? Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { dev: stat.dev + 1 }) : stat;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (String(from) === source) throw Object.assign(new Error("EXDEV mounted source"), { code: "EXDEV" });
    rename(from, to);
  });
};
afterEach(() => {
  vi.restoreAllMocks();
  const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  for (const root of ownedFixtures.splice(0)) {
    if (!evidence) continue;
    const target = path.join(evidence, path.basename(root));
    fs.mkdirSync(target, { recursive: true, mode: 0o700 });
    const manifest: unknown[] = []; let count = 0, bytes = 0;
    const exportOwned = (file: string, relative: string): void => {
      if (++count > 4096) throw new Error("Owned custody evidence entry bound exceeded");
      const stat = fs.lstatSync(file, { bigint: true });
      const type = stat.isSymbolicLink() ? "symlink" : stat.isDirectory() ? "directory" : stat.isFile() ? "file" : "special";
      manifest.push({ path: relative, type, dev: String(stat.dev), ino: String(stat.ino), mode: String(stat.mode), size: String(stat.size),
        ...(stat.isSymbolicLink() ? { link: fs.readlinkSync(file) } : {}) });
      if (type === "directory") {
        fs.mkdirSync(path.join(target, "raw", relative), { recursive: true, mode: 0o700 });
        for (const name of fs.readdirSync(file).sort()) exportOwned(path.join(file, name), path.join(relative, name));
      } else if (type === "file") {
        bytes += Number(stat.size); if (bytes > 64 * 1024 * 1024) throw new Error("Owned custody evidence byte bound exceeded");
        fs.writeFileSync(path.join(target, "raw", relative), fs.readFileSync(file), { mode: 0o600, flag: "wx" });
      }
    };
    exportOwned(root, "");
    fs.writeFileSync(path.join(target, "manifest.json"), JSON.stringify({ root, TMPDIR: process.env.TMPDIR,
      umask: process.umask(), userBus: process.env.DBUS_SESSION_BUS_ADDRESS ?? null, runtimeDirectory: process.env.XDG_RUNTIME_DIR ?? null, manifest }, null, 2));
  }
});

describe("external custody outside every Git root", () => {
  it.each([false, true])("escapes nested repository/worktree markers including enclosing root (worktree=%s)", worktree => {
    const root = fixture(), outer = marker(path.join(root, "outer")), inner = marker(path.join(outer, "nested"), worktree);
    const source = sourceAt(inner), ino = fs.lstatSync(source).ino;
    const row = custody.retainCleanupRoot(source, path.join(inner, "archive"), "nested")!;
    outside(row.destination, [outer, inner]);
    expect(fs.lstatSync(row.destination).ino).toBe(ino);
    expect(JSON.parse(fs.readFileSync(row.receipt, "utf8"))).toMatchObject({ state: "moved", destination: row.destination });
  });

  it("escapes a conventional bare root and a proposed destination that is itself Git", () => {
    const root = fixture(), bare = mkdir(path.join(root, "bare"));
    fs.writeFileSync(path.join(bare, "HEAD"), "ref: refs/heads/main\n"); mkdir(path.join(bare, "objects")); mkdir(path.join(bare, "refs"));
    const proposed = marker(path.join(bare, "archive")), source = sourceAt(bare);
    const row = custody.retainCleanupRoot(source, proposed, "bare")!;
    outside(row.destination, [bare, proposed]);
  });

  it("rejects a proposed external archive's own independent Git root, not just source prefixes", () => {
    const root = fixture(), sourceRepo = marker(path.join(root, "source-repo")), proposedRepo = marker(path.join(root, "unrelated-external-repo"));
    const source = sourceAt(sourceRepo), row = custody.retainCleanupRoot(source, proposedRepo, "external-git")!;
    outside(row.destination, [sourceRepo, proposedRepo]);
    expect(fs.readdirSync(proposedRepo)).toEqual([".git"]);
  });

  it("resolves an archive symlink physically before checking enclosing Git roots", () => {
    const root = fixture(), repo = marker(path.join(root, "repo")), source = sourceAt(repo);
    const target = mkdir(path.join(repo, "inside")), alias = path.join(root, "alias");
    fs.symlinkSync(target, alias, process.platform === "win32" ? "junction" : "dir");
    const row = custody.retainCleanupRoot(source, path.join(alias, "archive"), "alias")!;
    outside(row.destination, [repo]);
    expect(fs.readdirSync(target)).toEqual([]);
  });

  it("refuses physical containment, dangling archive aliases, and ambiguous dot-dot without moving originals", () => {
    const root = fixture(), source = sourceAt(root), alias = path.join(root, "alias");
    fs.symlinkSync(source, alias, process.platform === "win32" ? "junction" : "dir");
    expect(() => custody.retainCleanupRoot(source, path.join(alias, "archive"), "inside")).toThrow(/physically outside/);
    const absent = path.join(root, "absent");
    fs.symlinkSync(path.join(root, "missing"), absent, process.platform === "win32" ? "junction" : "dir");
    expect(() => custody.retainCleanupRoot(source, path.join(absent, "held"), "dangling")).toThrow();
    expect(() => custody.retainCleanupRoot(source, `${alias}${path.sep}..${path.sep}held`, "ambiguous")).toThrow();
    expect(fs.readFileSync(path.join(source, "raw.JSONL"))).toEqual(Buffer.from([0, 255, 13, 10, 123]));
  });

  it("same-device safe sibling wins over a different-device requested area; allocation is still fresh/private", () => {
    const root = fixture(), source = sourceAt(root), requested = mkdir(path.join(root, "remote"));
    const stat = fs.statSync;
    vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      const value = stat(file, options as { bigint: false; throwIfNoEntry: false });
      return String(file) === requested && value ? Object.assign(Object.create(Object.getPrototypeOf(value)), value, { dev: value.dev + 1 }) : value;
    }) as typeof fs.statSync);
    const row = custody.retainCleanupRoot(source, requested, "same-device")!;
    expect(path.relative(requested, row.destination).startsWith("..")).toBe(true);
    if (process.platform !== "win32") expect(fs.statSync(path.dirname(row.destination)).mode & 0o777).toBe(0o700);
    expect(fs.readdirSync(requested)).toEqual([]);
  });

  it("permission refusal before rename preserves original bytes and creates no custody success", () => {
    const root = fixture(), source = sourceAt(root), ino = fs.lstatSync(source).ino;
    const mkdirReal = fs.mkdirSync;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (String(file).endsWith("denied")) throw Object.assign(new Error("archive access denied"), { code: "EACCES" });
      return mkdirReal(file, options as fs.MakeDirectoryOptions);
    }) as typeof fs.mkdirSync);
    expect(() => custody.retainCleanupRoot(source, path.join(root, "denied"), "denied")).toThrow(/access denied/);
    expect(fs.lstatSync(source).ino).toBe(ino);
  });

  it("missing optional roots in the same batch do not create phantom containers or receipts", () => {
    const root = fixture(), source = sourceAt(root), archive = path.join(root, "archive");
    const rows = [path.join(root, "missing-before"), source, path.join(root, "missing-after")]
      .map(file => custody.retainCleanupRoot(file, archive, "batch")).filter(Boolean);
    expect(rows).toHaveLength(1); expect(fs.readdirSync(archive)).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(rows[0]!.receipt, "utf8")).state).toBe("moved");
  });

  it("case aliases stay external on native Windows; POSIX case-distinct paths remain distinct", () => {
    const root = fixture(), repo = marker(path.join(root, "CaseRepo")), source = sourceAt(repo);
    const spelling = process.platform === "win32" ? repo.toUpperCase() : repo;
    const row = custody.retainCleanupRoot(source, path.join(spelling, "held"), "case")!;
    outside(row.destination, [repo]);
    if (process.platform !== "win32") {
      const distinct = mkdir(path.join(root, "caserepo")); expect(fs.realpathSync(distinct)).not.toBe(fs.realpathSync(repo));
    }
  });

  it("clone/new-worktree, add-A, clean-fdx and forced worktree removal cannot reach external archive", async () => {
    const root = fixture(), repo = mkdir(path.join(root, "repo")), worktree = path.join(repo, "nested-worktree"), clone = path.join(root, "new-clone");
    const actualGit = hostedOnly("external custody real Git destruction proof");
    const calls: string[][] = [];
    const git = (cwd: string, args: string[]) => {
      calls.push(args);
      return actualGit ? execFileSync("git", args, { cwd, encoding: "utf8", env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1" } }) : "";
    };
    if (actualGit) {
      git(repo, ["init"]); fs.writeFileSync(path.join(repo, "tracked.txt"), "base\n"); git(repo, ["add", "tracked.txt"]);
      git(repo, ["-c", "user.name=Custody fixture", "-c", "user.email=custody@example.invalid", "commit", "-m", "fixture"]);
      git(repo, ["worktree", "add", "-b", "held-fixture", worktree]);
    } else { marker(repo); marker(worktree, true); }
    const source = sourceAt(worktree), ino = fs.lstatSync(source).ino;
    const row = custody.retainCleanupRoot(source, path.join(worktree, "held"), "git-boundary")!;
    outside(row.destination, [repo, worktree]);
    const receipt = fs.readFileSync(row.receipt), bytes = fs.readFileSync(path.join(row.destination, "raw.JSONL"));
    if (actualGit) await addCloneFirstWorktree({ gitRoot: repo, dest: clone, branch: { flag: "-b", name: "new-clone-fixture" }, startPoint: "HEAD" });
    else git(repo, ["clone-first-worktree", repo, clone]);
    const staged = git(repo, ["add", "-A"]); expect(staged).toBe("");
    if (actualGit) {
      expect(git(repo, ["diff", "--cached", "--name-only"])).not.toMatch(/cleanup-|custody|trace-archive/);
      expect(fs.existsSync(path.join(clone, path.basename(path.dirname(row.destination))))).toBe(false);
      expect(fs.readdirSync(clone).filter(name => name !== ".git").sort()).not.toContain(".pi-fabric-trace-archive");
    }
    git(repo, ["clean", "-fdx"]); git(repo, ["worktree", "remove", "--force", worktree]);
    expect(calls).toContainEqual(["add", "-A"]); expect(calls).toContainEqual(["clean", "-fdx"]);
    expect(fs.readFileSync(row.receipt)).toEqual(receipt); expect(fs.readFileSync(path.join(row.destination, "raw.JSONL"))).toEqual(bytes);
    expect(fs.lstatSync(row.destination).ino).toBe(ino);
    console.info(actualGit ? "actual hosted Git proof" : "recorded local Git calls only; actual hosted coverage pending");
  });
});

describe("custody protection and no-inventory rename", () => {
  it("same-filesystem rename never inventories payloads or applies shadow entry/byte limits", () => {
    const root = fixture(), source = sourceAt(root), archive = path.join(root, "archive");
    const read = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("payload inventory forbidden on rename path"); });
    const row = custody.retainCleanupRoot(source, archive, "rename-no-inventory")!;
    expect(read).not.toHaveBeenCalled(); expect(fs.readFileSync(path.join(row.destination, "raw.JSONL"))).toEqual(Buffer.from([0, 255, 13, 10, 123]));
  });

  it("fresh-container protection failure is visible before rename, with original inode and bytes intact", () => {
    const root = fixture(), source = sourceAt(root), ino = fs.lstatSync(source).ino;
    if (process.platform === "win32") vi.spyOn(windowsCommands, "windowsSecurityPowerShell").mockImplementation(() => { throw new Error("DACL setup refused"); });
    else vi.spyOn(fs, "chmodSync").mockImplementation(() => { throw new Error("mode setup refused"); });
    const rename = vi.spyOn(fs, "renameSync");
    expect(() => custody.retainCleanupRoot(source, path.join(root, "archive"), "protection")).toThrow(/setup refused/);
    expect(rename.mock.calls.some(([from]) => String(from) === source)).toBe(false); expect(fs.lstatSync(source).ino).toBe(ino);
    expect(fs.readFileSync(path.join(source, "raw.JSONL"))).toEqual(Buffer.from([0, 255, 13, 10, 123]));
  });

  it("independently checks fresh-container privacy with native DACLs on Windows and owner/mode on POSIX", () => {
    const root = fixture(), source = sourceAt(root), row = custody.retainCleanupRoot(source, path.join(root, "archive"), "privacy")!;
    const container = path.dirname(row.destination);
    if (process.platform !== "win32") {
      const stat = fs.lstatSync(container); expect(stat.uid).toBe(process.getuid!()); expect(stat.mode & 0o777).toBe(0o700); return;
    }
    const command = windowsCommands.windowsSecurityPowerShell(String.raw`
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$acl = Get-Acl -LiteralPath $env:PI_TEST_CUSTODY_CONTAINER
$raw = [System.Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
$aces = @(foreach ($ace in $raw.DiscretionaryAcl) { @{ sid=$ace.SecurityIdentifier.Value; type=[int]$ace.AceType; flags=[int]$ace.AceFlags; mask=$ace.AccessMask } })
@{ user=$user; owner=$raw.Owner.Value; protected=$acl.AreAccessRulesProtected; aces=$aces } | ConvertTo-Json -Depth 6 -Compress
`, { ...process.env, PI_TEST_CUSTODY_CONTAINER: container });
    const data = JSON.parse(execFileSync(command.file, command.args, { env: command.env, encoding: "utf8", timeout: 15_000, windowsHide: true }));
    expect(data.protected).toBe(true); expect(data.owner).toBe(data.user); expect(data.aces).toHaveLength(2);
    for (const ace of data.aces) { expect([data.user, "S-1-5-18"]).toContain(ace.sid); expect(ace.type).toBe(0); expect(ace.flags).toBe(3); expect(ace.mask & 0x1f01ff).toBe(0x1f01ff); }
  });
});

describe("true cross-device shadow retains original indefinitely", () => {
  it("returns structured not-cleaned status; independently verified copy is not inode custody and late appends remain original", () => {
    const root = fixture(), source = sourceAt(root), archive = path.join(root, "archive"), ino = fs.lstatSync(source).ino;
    crossDevice(source);
    const warn = vi.spyOn(console, "warn"), fd = fs.openSync(path.join(source, "raw.JSONL"), "a");
    try {
      const failure = result(() => custody.retainCleanupRoot(source, archive, "cross-device"));
      expect(failure.code).toBe("CLEANUP_CUSTODY_DEFERRED"); expect("retainedRoot" in failure).toBe(false);
      expect(failure.cleanupStatus).toMatchObject({ originalRetained: true, source });
      if (process.platform !== "linux") { expect(failure.cleanupStatus.state).toBe("copy-failed"); expect(String(failure.cause)).toMatch(/ACL/); return; }
      expect(failure.cleanupStatus.state).toBe("held-in-place");
      const status = failure.cleanupStatus, receipt = JSON.parse(fs.readFileSync(status.receipt, "utf8"));
      expect(receipt).toMatchObject({ state: "held-in-place", originalRetained: true, verifiedCopyPath: status.destination,
        manifest: expect.arrayContaining([{ path: "raw.JSONL", sha256: createHash("sha256").update(Buffer.from([0, 255, 13, 10, 123])).digest("hex") }]) });
      expect(fs.lstatSync(source).ino).toBe(ino); expect(fs.lstatSync(status.destination).ino).not.toBe(ino);
      expect(fs.readFileSync(path.join(status.destination, "raw.JSONL"))).toEqual(fs.readFileSync(path.join(source, "raw.JSONL")));
      expect(fs.statSync(path.join(status.destination, "raw.JSONL")).mode & 0o777).toBe(0o600);
      fs.writeSync(fd, "late-original-tail");
      expect(fs.readFileSync(path.join(source, "raw.JSONL")).subarray(5).toString()).toBe("late-original-tail");
      expect(fs.readFileSync(path.join(status.destination, "raw.JSONL"))).toEqual(Buffer.from([0, 255, 13, 10, 123]));
      expect(warn.mock.calls.flat().join("\n")).toMatch(/NOT cleaned/);
    } finally { fs.closeSync(fd); }
  });

  it.each(["source-eio", "readback-eio", "corrupt-readback", "source-change", "mode", "owner", "hardlink", "symlink", "fifo"])("unsafe/refused copy keeps original and immutable preparation (%s)", fault => {
    const root = fixture(), source = sourceAt(root), file = path.join(source, "raw.JSONL"), ino = fs.lstatSync(source).ino;
    const original = fs.readFileSync(file); crossDevice(source, fault === "owner");
    if (fault === "mode" && process.platform !== "win32") fs.chmodSync(file, 0o622);
    if (fault === "hardlink") fs.linkSync(file, path.join(source, "second"));
    if (fault === "symlink") fs.symlinkSync(source, path.join(source, "link"), process.platform === "win32" ? "junction" : "dir");
    if (fault === "fifo" && process.platform !== "win32") execFileSync("mkfifo", [path.join(source, "pipe")]);
    const open = fs.openSync, read = fs.readSync; const readbacks = new Set<number>(), inputs = new Set<number>(); let changed = false;
    vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) => {
      const fd = open(name, flags, mode);
      if (String(name).includes(`${path.sep}tree${path.sep}`) && flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) readbacks.add(fd);
      if (String(name) === file && flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NOATIME)) inputs.add(fd);
      return fd;
    });
    const close = fs.closeSync;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { readbacks.delete(fd); inputs.delete(fd); close(fd); });
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number | null) => {
      if (inputs.has(fd) && fault === "source-eio") throw Object.assign(new Error("source copy read EIO"), { code: "EIO" });
      if (readbacks.has(fd)) {
        if (fault === "readback-eio") throw Object.assign(new Error("independent readback EIO"), { code: "EIO" });
        if (fault === "source-change" && !changed) { fs.appendFileSync(file, "late"); changed = true; }
      }
      const count = read(fd, buffer, offset, length, position);
      if (readbacks.has(fd) && fault === "corrupt-readback" && count) buffer[offset] = buffer[offset]! ^ 1;
      return count;
    }) as typeof fs.readSync);
    const failure = result(() => custody.retainCleanupRoot(source, path.join(root, "archive"), fault));
    expect(failure.cleanupStatus).toMatchObject({ state: "copy-failed", originalRetained: true, source });
    expect(failure.cleanupStatus.verifiedCopyPath).toBeUndefined(); expect(fs.lstatSync(source).ino).toBe(ino);
    expect(fs.readFileSync(file)).toEqual(changed ? Buffer.concat([original, Buffer.from("late")]) : original);
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(failure.cleanupStatus.receipt), "custody.prepared.json"), "utf8")).state).toBe("prepared");
    expect(JSON.parse(fs.readFileSync(failure.cleanupStatus.receipt, "utf8")).state).toBe("copy-failed");
  });
});
