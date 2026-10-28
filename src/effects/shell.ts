import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { spawn } from "node:child_process";
import { effectBytesHash, inspectEffectPath, MAX_EFFECT_FILE_BYTES, resolveEffectPath } from "./file.js";
import type { EffectAdapterControls } from "./failpoints.js";

const FORMAT = "fabric-read-sha256-v1";
const BINARY = "/usr/bin/python3";
const TIMEOUT_MS = 5_000;
const MAX_OUTPUT_BYTES = 4_096;
const HASH = /^[a-f0-9]{64}$/;

/** Trusted manifest profile, supplied by the host, never by model arguments. */
export interface CertifiedShellProfile {
  readonly kind: "read-sha256-v1";
  readonly path: string;
  readonly sha256: string;
}

export type ShellEffectProfile = CertifiedShellProfile;

export interface PreparedShellEffect {
  readonly format: typeof FORMAT;
  readonly workspaceRoot: string;
  readonly path: string;
  readonly sha256: string;
  readonly command: string;
  readonly integrity: string;
}

// This is the entire child program. No script from the repository, caller code,
// shell, startup files, inherited Python environment, networking or child jobs.
// Both the Node preflight and Python's dirfd traversal reject aliases/nonregular
// files. Exclusive workspace access is still a prerequisite, not established by
// these checks. A read-only computation is safe to retry, not proof of one run.
const READ_HASH_PROGRAM = `import hashlib, os, stat, sys
root, rel, expected = sys.argv[1:]
flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
fd = os.open(root, flags | os.O_DIRECTORY)
try:
    parts = rel.split('/')
    for part in parts[:-1]:
        nxt = os.open(part, flags | os.O_DIRECTORY, dir_fd=fd)
        os.close(fd)
        fd = nxt
    filefd = os.open(parts[-1], flags, dir_fd=fd)
    try:
        before = os.fstat(filefd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > ${MAX_EFFECT_FILE_BYTES}:
            raise ValueError('unsafe input file')
        h = hashlib.sha256()
        total = 0
        while True:
            block = os.read(filefd, 65536)
            if not block:
                break
            total += len(block)
            if total > ${MAX_EFFECT_FILE_BYTES}:
                raise ValueError('input exceeds bound')
            h.update(block)
        after = os.fstat(filefd)
        entry = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
        fields = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if fields(before) != fields(after) or fields(after) != fields(entry):
            raise ValueError('input changed during read')
        digest = h.hexdigest()
        if digest != expected:
            raise ValueError('input hash mismatch')
        print(digest + '  ' + rel)
    finally:
        os.close(filefd)
finally:
    os.close(fd)
`;

function fail(message: string): never {
  throw new Error(`EFFECT_SHELL_UNSUPPORTED: ${message}`);
}

/** Model-visible command grammar, NOT text sent to a shell. Exact bytes only. */
export function shellEffectCommand(path: string, sha256: string): string {
  if (!HASH.test(sha256) || !path || path.length > 1_024 || /[\x00-\x1f\x7f]/.test(path)) fail("invalid read/hash profile");
  return `read-sha256 ${JSON.stringify(path)} ${sha256}`;
}

function seal(workspaceRoot: string, path: string, sha256: string): PreparedShellEffect {
  const core = { format: FORMAT, workspaceRoot, path, sha256, command: shellEffectCommand(path, sha256) };
  return Object.freeze({ ...core, integrity: effectBytesHash(JSON.stringify(core)) }) as PreparedShellEffect;
}

function checkedPrepared(input: PreparedShellEffect): PreparedShellEffect {
  if (!input || input.format !== FORMAT || typeof input.sha256 !== "string") fail("unsupported/corrupt prepared read");
  const target = resolveEffectPath(input.workspaceRoot, input.path);
  if (target.path !== input.path) fail("non-canonical stored path");
  const clean = seal(target.workspaceRoot, target.path, input.sha256);
  if (input.command !== clean.command || input.integrity !== clean.integrity) fail("corrupt stored read");
  return clean;
}

export async function prepareShellEffect(workspaceRoot: string, args: Record<string, unknown>, profile: CertifiedShellProfile): Promise<PreparedShellEffect> {
  if (!profile || profile.kind !== "read-sha256-v1" || typeof profile.sha256 !== "string") fail("only the fixed read-sha256-v1 profile is certified");
  if (Object.keys(args).length !== 1 || typeof args.command !== "string" || !Object.hasOwn(args, "command")) fail("only command is supported; cwd/env/timeout/operations are host-controlled");
  const target = resolveEffectPath(workspaceRoot, profile.path);
  const prepared = seal(target.workspaceRoot, target.path, profile.sha256);
  if (args.command !== prepared.command) fail("command must exactly match the trusted read/hash profile");
  const input = await inspectEffectPath(prepared.workspaceRoot, prepared.path);
  if (!input.state.exists || input.state.sha256 !== prepared.sha256) fail("input does not match the trusted hash");
  return prepared;
}

export async function executeShellEffect(prepared: PreparedShellEffect, controls: EffectAdapterControls): Promise<AgentToolResult<unknown>> {
  const stored = checkedPrepared(prepared);
  await controls.assertLive();
  const input = await inspectEffectPath(stored.workspaceRoot, stored.path);
  if (!input.state.exists || input.state.sha256 !== stored.sha256) fail("input changed before execution");
  await controls.assertLive();
  const output = await new Promise<string>((resolve, reject) => {
    const child = spawn(BINARY, ["-I", "-B", "-c", READ_HASH_PROGRAM, stored.workspaceRoot, stored.path, stored.sha256], {
      cwd: stored.workspaceRoot,
      shell: false,
      env: { PATH: "/usr/bin:/bin", LANG: "C.UTF-8", LC_ALL: "C.UTF-8" },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let size = 0;
    const stdout: Buffer[] = [], stderr: Buffer[] = [];
    let failure: Error | undefined;
    const stop = (error: Error) => { failure ??= error; child.kill("SIGKILL"); };
    // Child close/error events drive completion; this one deadline is solely a
    // bounded safety net for a stuck fixed read program/filesystem.
    const deadline = setTimeout(() => stop(new Error("EFFECT_SHELL_TIMEOUT: fixed read exceeded 5000ms")), TIMEOUT_MS);
    const collect = (chunks: Buffer[]) => (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_OUTPUT_BYTES) stop(new Error("EFFECT_SHELL_OUTPUT_LIMIT: fixed read exceeded 4096 bytes"));
      else chunks.push(chunk);
    };
    child.stdout.on("data", collect(stdout));
    child.stderr.on("data", collect(stderr));
    child.once("error", (error) => { failure ??= error; });
    child.once("close", (code, signal) => {
      clearTimeout(deadline);
      if (failure) reject(failure);
      else if (code !== 0) reject(new Error(`EFFECT_SHELL_FAILED: code=${code}, signal=${signal ?? "none"}; ${Buffer.concat(stderr).toString("utf8")}`));
      else resolve(Buffer.concat(stdout).toString("utf8"));
    });
  });
  const expectedOutput = `${stored.sha256}  ${stored.path}\n`;
  if (output !== expectedOutput) fail("unexpected fixed-program output");
  await controls.failpoint?.("destination-committed", { path: input.absolutePath, sha256: stored.sha256, readOnly: true });
  return { content: [{ type: "text", text: output }], details: { exitCode: 0, readOnly: true, profile: "read-sha256-v1" } };
}
