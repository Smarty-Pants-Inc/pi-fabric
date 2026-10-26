import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

// smarty-dev#16966: Node authorizes path strings, not a proc-fd path's writable target. Exercise the
// actual source in real permission-model children: no permission, platform, or filesystem mocks.
// Keep reason: the permission boundary and successor-record safety on the measured release failure.
// c42f7f34's releaseFenceLock returns void: assert filesystem effects, not newer release receipts.
const CHILD = String.raw`
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
const [moduleUrl, root, scenario] = process.argv.slice(1);
const { pinFenceLock, releaseFenceLock, holdMeshFenceSync, MeshLock, acquireMeshCustodyLock } = await import(moduleUrl);
const lock = path.join(root, ".lock");
const record = "owner-token\n" + process.pid + "\n1\n";
const successor = "successor-token\n" + process.pid + "\n2\n";
const publish = (directory, owner) => {
  fs.mkdirSync(directory, { mode: 0o700 });
  fs.writeFileSync(path.join(directory, "owner"), owner, { flag: "wx", mode: 0o600 });
};

if (scenario !== "pinned") {
  assert.equal(typeof process.permission?.has, "function", "must run under Node --permission");
  const probe = path.join(root, "permission-probe");
  publish(probe, record);
  const fd = fs.openSync(probe, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
  try {
    const realOwner = path.join(probe, "owner");
    const procOwner = "/proc/self/fd/" + fd + "/owner";
    assert.equal(process.permission.has("fs.write", realOwner), true);
    assert.equal(process.permission.has("fs.read", procOwner), true);
    assert.equal(process.permission.has("fs.write", procOwner), false);
    assert.equal(fs.readFileSync(procOwner, "utf8"), record);
    assert.throws(() => fs.unlinkSync(procOwner), error =>
      error.code === "ERR_ACCESS_DENIED" && error.permission === "FileSystemWrite");
    assert.equal(fs.readFileSync(realOwner, "utf8"), record, "denied unlink must leave the owner");
  } finally { fs.closeSync(fd); }
  // Positive control: the same write through its real, explicitly allowed path succeeds.
  fs.unlinkSync(path.join(probe, "owner"));
  fs.rmdirSync(probe);
}

if (scenario === "own") {
  publish(lock, record);
  const ours = fs.statSync(lock);
  const pin = pinFenceLock(lock, record);
  releaseFenceLock(lock, record, {}, pin);
  assert.equal(fs.existsSync(lock), false, "release must remove its own directory");
  assert.equal(pin, undefined, "a denied proc-fd write must select the existing unpinned protocol");
  // A denied pin may have opened the directory before checking permission. It must close that fd,
  // even though the caller now has no numeric pin to close. Count only fds on this exact inode.
  const leaked = fs.readdirSync("/proc/self/fd").filter(name => {
    try {
      const stat = fs.fstatSync(Number(name));
      return stat.isDirectory() && stat.dev === ours.dev && stat.ino === ours.ino;
    } catch (error) {
      if (error.code === "EBADF") return false; // readdir's own, already closed directory fd
      throw error;
    }
  });
  assert.deepEqual(leaked, [], "permission-denied pin leaked a directory descriptor");
} else if (scenario === "fence") {
  const records = [];
  for (let round = 0; round < 2; round++) {
    // Zero budget is one actual acquire attempt, never a sleep/recovery-based eventual success.
    const value = holdMeshFenceSync(root, 0, () => {
      records.push(fs.readFileSync(path.join(lock, "owner"), "utf8"));
      assert.equal(fs.readFileSync(path.join(root, "custody.lock", "owner"), "utf8").split("\n")[1], String(process.pid));
      return round;
    });
    assert.equal(value, round);
    assert.equal(fs.existsSync(lock), false, "mesh lock stranded after release");
    assert.equal(fs.existsSync(path.join(root, "custody.lock")), false, "custody lock stranded after release");
    assert.deepEqual(fs.readdirSync(root), []);
  }
  assert.equal(new Set(records).size, 2, "the second section must acquire a fresh owner record");
} else if (scenario === "mesh") {
  for (const lockProtocol of [1, 2]) {
    const mesh = new MeshLock(root, { lockProtocol, lockTimeoutMs: 100, lockHeldEventMs: 0 }, () => {});
    // Prepare protocol 2 outside the zero-budget acquire, just as its real short-try caller must.
    await mesh.ownIncarnation;
    const records = [];
    for (let round = 0; round < 2; round++) {
      const value = await mesh.withLockAcrossAwait(async () => {
        const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
        assert.equal(owner.split("\n")[1], String(process.pid));
        records.push(owner);
        await Promise.resolve();
        assert.equal(fs.readFileSync(path.join(lock, "owner"), "utf8"), owner);
        return round;
      }, 0);
      assert.equal(value, round);
      assert.equal(fs.existsSync(lock), false, "async MeshLock release stranded its directory");
      assert.deepEqual(fs.readdirSync(root), []);
    }
    assert.equal(new Set(records).size, 2, "async MeshLock must immediately acquire a fresh record");
  }
} else if (scenario === "custody") {
  const custody = path.join(root, "custody.lock");
  const records = [];
  for (let round = 0; round < 2; round++) {
    const release = await acquireMeshCustodyLock(root, 0, { ownIncarnation: undefined });
    const owner = fs.readFileSync(path.join(custody, "owner"), "utf8");
    assert.equal(owner.split("\n")[1], String(process.pid));
    records.push(owner);
    release();
    assert.equal(fs.existsSync(custody), false, "async custody release stranded its directory");
    assert.deepEqual(fs.readdirSync(root), []);
  }
  assert.equal(new Set(records).size, 2, "async custody must immediately acquire a fresh record");
} else if (scenario === "successor") {
  publish(lock, record);
  const pin = pinFenceLock(lock, record);
  const receipt = lock + ".dead.receipt";
  let taken = false;
  releaseFenceLock(lock, record, {
    afterCheck: () => {
      // The existing fallback's hard interleaving: a recoverer replaces the directory after the
      // ownership check. Taking a foreign owner INSIDE the new directory must restore it there.
      fs.renameSync(lock, receipt);
      publish(lock, successor);
    },
    afterTake: () => {
      taken = true;
      assert.deepEqual(fs.readdirSync(lock), ["owner.released.owner-token"]);
      assert.equal(fs.readFileSync(path.join(lock, "owner.released.owner-token"), "utf8"), successor);
    },
  }, pin);
  assert.equal(pin, undefined);
  assert.equal(taken, true, "must exercise the real unpinned take-and-restore path");
  assert.deepEqual(fs.readdirSync(lock), ["owner"]);
  assert.equal(fs.readFileSync(path.join(lock, "owner"), "utf8"), successor);
  assert.equal(fs.readFileSync(path.join(receipt, "owner"), "utf8"), record);
  assert.deepEqual(fs.readdirSync(root).sort(), [".lock", ".lock.dead.receipt"]);
} else if (scenario === "pinned") {
  publish(lock, record);
  const pin = pinFenceLock(lock, record);
  assert.equal(typeof pin, "number", "unrestricted Linux must retain inode-pinned release");
  assert.ok(pin >= 0);
  assert.equal(fs.fstatSync(pin).ino, fs.statSync(lock).ino);
  assert.equal(fs.readFileSync("/proc/self/fd/" + pin + "/owner", "utf8"), record);
  releaseFenceLock(lock, record, {}, pin);
  assert.equal(fs.existsSync(lock), false);
  assert.throws(() => fs.fstatSync(pin), { code: "EBADF" });
} else {
  throw new Error("unknown scenario: " + scenario);
}
process.stdout.write(JSON.stringify({ scenario, ok: true }) + "\n");
`;

type Scenario = "own" | "fence" | "mesh" | "custody" | "successor" | "pinned";

// /proc/self/fd is Linux-only, matching the existing mesh fence and pinned-release suites.
describe.runIf(process.platform === "linux")("fence locks under Node's permission model (smarty-dev#16966)", () => {
  const roots: string[] = [];
  let bundleDir: string;
  let bundle: string;

  beforeAll(() => {
    bundleDir = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fence-permission-bundle-"));
    bundle = path.join(bundleDir, "fence-lock.mjs");
    buildSync({ stdin: { contents: [
      'export { pinFenceLock, releaseFenceLock, holdMeshFenceSync } from "./src/mesh/fence-lock.ts";',
      'export { MeshLock } from "./src/mesh/mesh-lock.ts";',
      'export { acquireMeshCustodyLock } from "./src/mesh/custody-lock.ts";',
    ].join("\n"), resolveDir: process.cwd() }, bundle: true, platform: "node", format: "esm", outfile: bundle,
      banner: { js: 'import { createRequire as __cr } from "node:module"; const require = __cr(import.meta.url);' } });
  });
  afterEach(() => {
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  afterAll(() => { if (bundleDir) fs.rmSync(bundleDir, { recursive: true, force: true }); });

  const run = (scenario: Scenario): string => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fence-permission-"));
    roots.push(parent);
    // Advisory MeshLock tickets live beside the mesh root. Keep both inside the one allowed parent.
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root, { mode: 0o700 });
    const flags = scenario === "pinned" ? [] : ["--permission", `--allow-fs-write=${parent}`, "--allow-fs-read=*"];
    const child = spawnSync(process.execPath, [...flags, "--input-type=module", "-e", CHILD, pathToFileURL(bundle).href, root, scenario], {
      encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL",
      // Inherit NODE_OPTIONS: retain the test runner's private-TMPDIR deletion guard in children.
      env: { ...process.env, PI_FABRIC_LOCK_STATS: "0" },
    });
    const diagnostic = `${scenario}: ${child.error?.message ?? ""}\n${child.stderr}\n${child.stdout}`;
    expect(child.error, diagnostic).toBeUndefined();
    expect(child.signal, diagnostic).toBeNull();
    expect(child.status, diagnostic).toBe(0);
    expect(child.stdout.trim(), diagnostic).toBe(JSON.stringify({ scenario, ok: true }));
    return root;
  };

  it("queries genuine write permission, proves proc-fd unlink denial, and releases without leaking a pin", () => {
    expect(fs.readdirSync(run("own"))).toEqual([]);
  });

  it("releases both fence directories so a second real zero-budget acquire immediately succeeds", () => {
    expect(fs.readdirSync(run("fence"))).toEqual([]);
  });

  it("releases real async MeshLock protocols 1/2 and immediately reacquires with zero budget", () => {
    expect(fs.readdirSync(run("mesh"))).toEqual([]);
  });

  it("releases a real async custody lock and immediately reacquires with zero budget", () => {
    expect(fs.readdirSync(run("custody"))).toEqual([]);
  });

  it("keeps a successor's record in its directory when fallback release races replacement", () => {
    const root = run("successor");
    expect(fs.readdirSync(path.join(root, ".lock"))).toEqual(["owner"]);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toMatch(/^successor-token\n\d+\n2\n$/);
    expect(fs.readFileSync(path.join(root, ".lock.dead.receipt", "owner"), "utf8")).toMatch(/^owner-token\n\d+\n1\n$/);
  });

  it("retains the real Linux directory pin and closes it after release without --permission", () => {
    expect(fs.readdirSync(run("pinned"))).toEqual([]);
  });
});
