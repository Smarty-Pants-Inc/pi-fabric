import childProcess from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { checkedWindowsAclReply, createWindowsAclInspector, windowsAclInspectorMode, type WindowsAclInspector } from "../src/storage/windows-acl-inspector.js";
import { prepareRunRoot } from "../src/storage/run-scratch.js";
import * as windowsRoots from "../src/storage/windows-temp-root.js";
import { posixDataRoot } from "../src/storage/temp-root.js";

const roots: string[] = [], inspectors: WindowsAclInspector[] = [];
afterEach(() => {
  for (const inspector of inspectors.splice(0)) inspector.close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = Buffer.alloc(32, 7);
const reply = (nonce: string, payload: string, ok = true, signingKey = key): string => JSON.stringify({
  nonce, ok, payload,
  mac: createHmac("sha256", signingKey).update(`${nonce}\n${ok ? "ok" : "error"}\n${payload}`).digest("hex"),
});

// Real mailbox/receipt code on Linux; substitute only native PowerShell IO.
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "acl-inspector-test-"));
  roots.push(root);
  const write = fs.writeFileSync, rename = fs.renameSync;
  let mailbox = "", signingKey = key, source = "", calls = 0;
  let respond = true, tamper = false;
  const child = Object.assign(new EventEmitter(), { pid: 123, exitCode: null, killed: false,
    unref: vi.fn(), kill: vi.fn(() => { child.killed = true; return true; }) });
  const spawn = vi.spyOn(childProcess, "spawn").mockImplementation((file, args, options) => {
    const env = (options as childProcess.SpawnOptions).env!;
    mailbox = env.PI_FABRIC_ACL_MAILBOX!;
    signingKey = Buffer.from(env.PI_FABRIC_ACL_REPLY_KEY!, "base64");
    source = Buffer.from((args as string[])[4]!, "base64").toString("utf16le");
    write(path.join(mailbox, "reply.json"), reply(env.PI_FABRIC_ACL_READY_NONCE!, "ready", true, signingKey));
    expect(file).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    return child as unknown as childProcess.ChildProcess;
  });
  vi.spyOn(fs, "renameSync").mockImplementation((from, file) => {
    rename(from, file);
    if (String(file) === path.join(mailbox, "request.json") && respond) {
      const request = JSON.parse(fs.readFileSync(String(file), "utf8"));
      fs.unlinkSync(String(file)); // the native server takes one request, once
      calls++;
      const payload = JSON.stringify({ paths: JSON.parse(request.chain), fresh: calls });
      write(path.join(mailbox, "reply.json"), reply(request.nonce, payload, true, tamper ? key : signingKey));
    }
  });
  const inspector = createWindowsAclInspector("'fresh snapshot'", { SystemRoot: "C:\\Windows", PSModulePath: "foreign" }, root);
  inspectors.push(inspector);
  return { inspector, spawn, child, mailbox, source, get calls() { return calls; },
    noReply() { respond = false; }, tamper() { tamper = true; } };
};

describe("Windows ACL inspector host selection", () => {
  it.each(["linux", "darwin", "freebsd", "aix"] as const)("keeps %s on the bounded one-shot seam", platform => {
    expect(windowsAclInspectorMode(platform)).toBe("oneshot");
  });
  it("selects a warm native inspector only on Windows, independent of runner/CI/TEMP selectors", () => {
    expect(windowsAclInspectorMode("win32")).toBe("resident");
  });
  it("does no native work or allocation just by importing the module", async () => {
    const spawn = vi.spyOn(childProcess, "spawn"), mkdir = vi.spyOn(fs, "mkdtempSync");
    vi.resetModules();
    await import("../src/storage/windows-acl-inspector.js");
    expect(spawn).not.toHaveBeenCalled();
    expect(mkdir).not.toHaveBeenCalled();
  });
});

describe.skipIf(process.platform === "win32")("Windows root orchestration on a strict POSIX native seam", () => {
  const withRoot = (run: (root: string, validate: MockInstance<typeof windowsRoots.windowsDataRoot>) => void) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "acl-root-orchestration-")); roots.push(root);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!, mask = process.umask(0);
    const validate = vi.spyOn(windowsRoots, "windowsDataRoot").mockImplementation(directory => {
      if (directory.includes(`${path.sep}..${path.sep}`)) throw new Error("ambiguous Windows path component");
      return posixDataRoot(directory);
    });
    try { Object.defineProperty(process, "platform", { value: "win32" }); run(root, validate); }
    finally { Object.defineProperty(process, "platform", platform); process.umask(mask); }
  };
  it("validates a canonical existing root exactly once per public call, not from a cached decision", () => withRoot((root, validate) => {
    expect(prepareRunRoot(root)).toBe(root);
    expect(prepareRunRoot(root)).toBe(root);
    expect(validate).toHaveBeenCalledTimes(2);
  }));
  it("checks the existing parent and each newly created directory, with mode 0700 even under umask 0", () => withRoot((root, validate) => {
    const target = path.join(root, "runs", "new");
    expect(prepareRunRoot(target)).toBe(target);
    expect(validate.mock.calls.map(call => call[0])).toEqual([root, path.join(root, "runs"), target]);
    expect(fs.statSync(path.join(root, "runs")).mode & 0o777).toBe(0o700);
    expect(fs.statSync(target).mode & 0o777).toBe(0o700);
  }));
  it("does not normalize away the caller's rejected lexical spelling", () => withRoot(root => {
    const raw = root + path.sep + "ignored" + path.sep + ".." + path.sep + "run";
    expect(() => prepareRunRoot(raw)).toThrow(/ambiguous Windows/);
  }));
  it("refuses an existing writable parent before creation and never repairs its permissions", () => withRoot(root => {
    const parent = path.join(root, "foreign"); fs.mkdirSync(parent, { mode: 0o777 });
    const target = path.join(parent, "must-not-create");
    expect(() => prepareRunRoot(target)).toThrow(/writable/);
    expect(fs.existsSync(target)).toBe(false);
    expect(fs.statSync(parent).mode & 0o777).toBe(0o777);
  }));
});

describe("authenticated fresh native ACL receipts", () => {
  it("accepts a bound signed snapshot including Unicode, quotes and newlines", () => {
    const payload = '{"path":"日本語\\\\quote\'", "acl":[]}\n';
    expect(checkedWindowsAclReply(reply("one", payload), "one", key)).toBe(payload);
  });
  it.each([
    ["wrong nonce", reply("old", "snapshot")],
    ["wrong MAC", reply("one", "snapshot", true, Buffer.alloc(32, 8))],
    ["native error", reply("one", "failure", false)],
    ["bad JSON", "not json"], ["null", "null"], ["missing fields", "{}"],
    ["oversized", "x".repeat(1024 * 1024 + 1)],
  ])("rejects %s, not an ACL exemption", (_name, text) => {
    expect(() => checkedWindowsAclReply(text, "one", key)).toThrow();
  });
  it("rejects modifying the success flag or payload of a signed receipt", () => {
    const value = JSON.parse(reply("one", "old"));
    expect(() => checkedWindowsAclReply(JSON.stringify({ ...value, payload: "safe" }), "one", key)).toThrow();
    expect(() => checkedWindowsAclReply(JSON.stringify({ ...value, ok: false }), "one", key)).toThrow();
  });
});

describe("owned warm inspector lifecycle", () => {
  it("receives fresh authenticated replies from a real asynchronous peer while the caller is synchronous", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "acl-inspector-peer-"));
    roots.push(root);
    const spawn = childProcess.spawn;
    let exited!: Promise<void>;
    vi.spyOn(childProcess, "spawn").mockImplementation((_file, _args, options) => {
      // Replace ONLY PowerShell's native ACL producer on Linux. Exercise real
      // cross-process publication, waiting, nonce/MAC validation and retirement.
      const peer = spawn(process.execPath, ["-e", `
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const dir = process.env.PI_FABRIC_ACL_MAILBOX, key = Buffer.from(process.env.PI_FABRIC_ACL_REPLY_KEY, 'base64');
let count = 0;
function reply(nonce, payload) {
  const mac = crypto.createHmac('sha256', key).update(nonce+'\\n'+'ok'+'\\n'+payload).digest('hex');
  fs.writeFileSync(path.join(dir, 'pending.json'), JSON.stringify({nonce,ok:true,payload,mac}));
  fs.renameSync(path.join(dir, 'pending.json'), path.join(dir, 'reply.json'));
}
reply(process.env.PI_FABRIC_ACL_READY_NONCE, 'ready');
setInterval(() => {
  const file = path.join(dir, 'request.json');
  if (!fs.existsSync(file)) return;
  const request = JSON.parse(fs.readFileSync(file, 'utf8'));
  fs.unlinkSync(file);
  reply(request.nonce, JSON.stringify({fresh:++count,chain:JSON.parse(request.chain)}));
}, 2);
`], options as childProcess.SpawnOptions);
      exited = new Promise(resolve => peer.once("exit", () => resolve()));
      return peer;
    });
    const inspector = createWindowsAclInspector("native seam", { ...process.env, SystemRoot: "C:\\Windows" }, root);
    inspectors.push(inspector);
    try {
      for (let fresh = 1; fresh <= 6; fresh++) {
        expect(JSON.parse(inspector.inspect('["Z:\\\\tmp"]'))).toEqual({ fresh, chain: ["Z:\\tmp"] });
      }
    } finally {
      inspector.close();
      await exited; // no spawned work survives this test
    }
  });
  it("starts one pinned PowerShell, inspects again on every call and closes exactly once", () => {
    const f = fixture(), chain = JSON.stringify(["Z:\\", "Z:\\tmp\\日本語"]);
    expect(JSON.parse(f.inspector.inspect(chain))).toEqual({ paths: JSON.parse(chain), fresh: 1 });
    expect(JSON.parse(f.inspector.inspect(chain))).toEqual({ paths: JSON.parse(chain), fresh: 2 });
    expect(f.spawn).toHaveBeenCalledOnce();
    expect(f.spawn.mock.calls[0]![2]).toMatchObject({ windowsHide: true, stdio: "ignore", env: {
      PSModulePath: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules", PI_FABRIC_ACL_OWNER_PID: String(process.pid),
    } });
    expect(f.source).toContain("while (-not $owner.HasExited)");
    expect(f.source).toContain("SetAccessRuleProtection($true, $false)");
    expect(f.source).toContain("$prior.GetOwner");
    expect(f.source).toContain("$adminOwner = $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)");
    expect(f.source).toContain("if ($adminOwner) { $allowedOwners += 'S-1-5-32-544' }");
    expect(f.source).toContain("WindowsBuiltInRole]::Administrator");
    expect(f.source).toContain("$prior.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -notin $allowedOwners");
    expect(f.source).not.toContain(chain);
    f.inspector.close(); f.inspector.close();
    expect(f.child.kill).toHaveBeenCalledOnce();
    expect(f.inspector.closed).toBe(true);
    expect(fs.existsSync(f.mailbox)).toBe(false);
    expect(() => f.inspector.inspect(chain)).toThrow(/closed/);
  });
  it("fails closed on a forged reply and retires its inspector without returning data", () => {
    const f = fixture(); f.tamper();
    expect(() => f.inspector.inspect('["Z:\\\\tmp"]')).toThrow(/Unconfirmed/);
    expect(f.inspector.closed).toBe(true);
    expect(f.child.kill).toHaveBeenCalledOnce();
  });
  it("bounds an absent response and does not let wall-clock mocks disable the deadline", () => {
    const f = fixture(); f.noReply();
    vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValue(16_000);
    vi.spyOn(Date, "now").mockReturnValue(0);
    expect(() => f.inspector.inspect("[]")).toThrow(/timed out/);
    expect(f.child.kill).toHaveBeenCalledOnce();
  });
  it("does not write into or prune a replaced mailbox", () => {
    const f = fixture();
    fs.renameSync(f.mailbox, f.mailbox + "-old");
    fs.mkdirSync(f.mailbox);
    fs.writeFileSync(path.join(f.mailbox, "request.json"), "foreign");
    expect(() => f.inspector.inspect("[]")).toThrow(/replaced/);
    expect(fs.readFileSync(path.join(f.mailbox, "request.json"), "utf8")).toBe("foreign");
    expect(f.child.kill).toHaveBeenCalledOnce();
  });
  it("does not recursively prune unknown mailbox contents", () => {
    const f = fixture();
    fs.writeFileSync(path.join(f.mailbox, "foreign.txt"), "keep");
    f.inspector.close();
    expect(fs.readFileSync(path.join(f.mailbox, "foreign.txt"), "utf8")).toBe("keep");
  });
});
