import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import fabricWriteGuard from "../src/agents/write-guard.js";

const originalPolicy = process.env.PI_FABRIC_WRITE_POLICY;
const originalTmpDir = process.env.TMPDIR;
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  if (originalPolicy === undefined) delete process.env.PI_FABRIC_WRITE_POLICY;
  else process.env.PI_FABRIC_WRITE_POLICY = originalPolicy;
  if (originalTmpDir === undefined) delete process.env.TMPDIR;
  else process.env.TMPDIR = originalTmpDir;
});

const tempRoot = (prefix: string): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  roots.push(root);
  return root;
};

type Hook = (event: { toolName: string; input: unknown }, context: { cwd: string }) => { block: boolean; reason: string } | undefined;
const hookFor = (policy: { readOnly: boolean; writableRoots: string[]; shell: "deny" | "unconfined" }, extensionTmpDir?: string): Hook => {
  if (extensionTmpDir === undefined) delete process.env.TMPDIR;
  else process.env.TMPDIR = extensionTmpDir;
  process.env.PI_FABRIC_WRITE_POLICY = JSON.stringify(policy);
  let hook: Hook | undefined;
  fabricWriteGuard({ on: (_event: string, handler: Hook) => { hook = handler; } } as unknown as ExtensionAPI);
  if (!hook) throw new Error("write guard did not register");
  return hook;
};

const write = (hook: Hook, target: string, cwd: string) => hook({ toolName: "write", input: { path: target } }, { cwd });

describe("write guard private TMPDIR exception", () => {
  it("allows call-time and extension-load private scratch but refuses a sibling", () => {
    const base = tempRoot("write-guard-private-");
    const allowed = path.join(base, "allowed");
    const temp = path.join(base, "session-tmp");
    const sibling = path.join(base, "sibling");
    const captured = tempRoot("write-guard-captured-");
    fs.mkdirSync(allowed);
    fs.mkdirSync(temp, { mode: 0o700 });
    fs.mkdirSync(sibling);
    const hook = hookFor({ readOnly: false, writableRoots: [allowed], shell: "unconfined" }, captured);

    process.env.TMPDIR = temp;
    expect(write(hook, path.join(temp, "x", "claim.json"), base)).toBeUndefined();
    expect(write(hook, path.join(captured, "findings.md"), base)).toBeUndefined();
    expect(write(hook, path.join(sibling, "claim.json"), base)?.reason).toMatch(/outside writable roots/);
  });

  it("does not grant a TMPDIR with mode 0755", () => {
    const base = tempRoot("write-guard-nonprivate-");
    const allowed = path.join(base, "allowed");
    const temp = path.join(base, "session-tmp");
    const captured = tempRoot("write-guard-captured-");
    fs.mkdirSync(allowed);
    fs.mkdirSync(temp, { mode: 0o700 });
    fs.chmodSync(temp, 0o755);
    const hook = hookFor({ readOnly: false, writableRoots: [allowed], shell: "unconfined" }, captured);

    process.env.TMPDIR = temp;
    expect(write(hook, path.join(temp, "claim.json"), base)?.reason).toMatch(/outside writable roots/);
  });

  it("does not add temporary roots for shell: deny", () => {
    const base = tempRoot("write-guard-shell-deny-");
    const allowed = path.join(base, "allowed");
    const temp = path.join(base, "session-tmp");
    fs.mkdirSync(allowed);
    fs.mkdirSync(temp, { mode: 0o700 });
    const hook = hookFor({ readOnly: false, writableRoots: [allowed], shell: "deny" }, temp);

    process.env.TMPDIR = temp;
    expect(write(hook, path.join(temp, "claim.json"), base)?.reason).toMatch(/outside writable roots/);
  });

  it("keeps read-only policy refused despite a private TMPDIR", () => {
    const base = tempRoot("write-guard-readonly-");
    const temp = path.join(base, "session-tmp");
    fs.mkdirSync(temp, { mode: 0o700 });
    const hook = hookFor({ readOnly: true, writableRoots: [], shell: "unconfined" }, temp);

    expect(write(hook, path.join(temp, "claim.json"), base)?.reason).toMatch(/read-only/);
  });

  it("refuses a symlink from private TMPDIR to an outside target", () => {
    const base = tempRoot("write-guard-symlink-");
    const allowed = path.join(base, "allowed");
    const temp = path.join(base, "session-tmp");
    const outside = path.join(base, "outside");
    fs.mkdirSync(allowed);
    fs.mkdirSync(temp, { mode: 0o700 });
    fs.mkdirSync(outside);
    fs.symlinkSync(outside, path.join(temp, "escape"), "dir");
    const hook = hookFor({ readOnly: false, writableRoots: [allowed], shell: "unconfined" }, temp);

    expect(write(hook, path.join(temp, "escape", "claim.json"), base)?.reason).toMatch(/outside writable roots/);
  });
});
