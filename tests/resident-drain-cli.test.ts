import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { main, type ResidentDrainReceipt } from "../src/resident-drain-cli.js";
import { residentHostId, residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import type { ResidentChannelOwnerProbe } from "../src/actors-cli.js";

// Keep reason: data safety + OS-user/release/process trust boundaries. These
// fixtures exercise the operator entrypoint but never signal/stop a native child.
// Deliberately retained on disk: no cleanup/deletion runs in this suite.
function fixture() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-drain-preflight-")));
  const meshRoot = path.join(root, "mesh");
  const rootId = "test-root";
  const directory = residentRoot(meshRoot, rootId);
  const expected = path.join(root, "old");
  const target = path.join(root, "new");
  fs.mkdirSync(path.join(expected, "dist/residency"), { recursive: true });
  fs.mkdirSync(target);
  fs.writeFileSync(path.join(expected, "dist/index.js"), "// retained fixture\n");
  fs.writeFileSync(path.join(expected, "dist/residency/pi-entry.js"), "// retained fixture\n");
  fs.mkdirSync(path.join(directory, "requests"), { recursive: true });
  fs.mkdirSync(path.join(directory, "responses"));
  fs.mkdirSync(path.join(directory, "actors"));
  fs.writeFileSync(path.join(directory, "requests/pending.json"), '{"accepted":"keep"}\n');
  fs.writeFileSync(path.join(directory, "actors/queue.json"), '["keep queued work"]\n');
  fs.writeFileSync(path.join(directory, "watchdog-custody.json"), '{"custody":"keep"}\n');
  const config: Record<string, unknown> = { format: 1, rootId, meshRoot, residencyRoot: directory,
    // Desired release can differ: it is NOT evidence of the loaded release.
    fabricExtensionPath: path.join(target, "dist/index.js") };
  const birth = processStartTime(process.pid);
  expect(birth).toMatch(/^\d+$/);
  const owner: Record<string, unknown> = { format: 1, hostId: residentHostId(rootId), token: "fixture-owner",
    pid: process.pid, processStartTime: birth, releaseRoot: expected,
    fabricExtensionPath: path.join(expected, "dist/index.js"), configDigest: "a".repeat(64),
    handover: { abi: "fabric-resident-1", launcher: { pid: process.pid, processStartTime: birth,
      token: "fixture-launcher", entry: path.join(expected, "dist/residency/pi-entry.js"),
      runtime: fs.realpathSync(process.execPath) } } };
  const save = () => {
    fs.writeFileSync(path.join(directory, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(directory, "owner.json"), JSON.stringify(owner));
  };
  save();
  const argv = ["--resident", directory, "--mesh-root", meshRoot, "--expected-release", expected, "--target-release", target];
  return { root, rootId, directory, meshRoot, expected, target, config, owner, save, argv };
}

// Capture names, contents, inode, birth/mtime/ctime and modes without modifying the
// fixture. Reads may update atime, which is deliberately not an asserted mutation.
function snapshot(root: string): unknown[] {
  const entries: unknown[] = [];
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      entries.push([path.relative(root, file), stat.ino, stat.mode, stat.mtimeMs, stat.ctimeMs,
        stat.isSymbolicLink() ? fs.readlinkSync(file) : stat.isFile() ? fs.readFileSync(file).toString("base64") : null]);
      if (stat.isDirectory()) walk(file);
    }
  };
  walk(root);
  return entries;
}

async function invoke(argv: string[], probe?: ResidentChannelOwnerProbe) {
  const stdout: string[] = [];
  const exit = await main(argv, { out: text => stdout.push(text) }, probe);
  expect(stdout).toHaveLength(1);
  const receipt = JSON.parse(stdout[0]!) as ResidentDrainReceipt;
  expect(receipt).toMatchObject({ phase: "preflight", unchanged: true, transitionAttempted: false,
    rollback: { status: "not-needed", reason: "no-transition-attempted" } });
  expect(receipt.elapsedMs).toBeGreaterThanOrEqual(0);
  expect(Number.isFinite(receipt.elapsedMs)).toBe(true);
  return { exit, receipt };
}

async function refusal(f: ReturnType<typeof fixture>, code: string, argv = f.argv, probe?: ResidentChannelOwnerProbe) {
  const before = snapshot(f.root);
  const result = await invoke(argv, probe);
  expect(result.exit).not.toBe(0);
  expect(result.receipt).toMatchObject({ ok: false, outcome: "refused", code, resident: argv[1], expected: f.expected, target: f.target });
  expect(result.receipt.reason.length).toBeGreaterThan(0);
  expect(snapshot(f.root)).toEqual(before);
  return result.receipt;
}

describe.skipIf(process.platform !== "linux")("fabric-resident-drain read-only preflight", () => {
  it("allows only JSON help to succeed, without a mesh default", async () => {
    const { exit, receipt } = await invoke(["--help"]);
    expect(exit).toBe(0);
    expect(receipt).toMatchObject({ ok: true, outcome: "help", code: "HELP", resident: null, expected: null, target: null });
    expect(receipt.help).toContain("setActivationFilter skips/consumes");
    expect(receipt.help).toContain("no actor stop/control");
    expect(receipt.help).toContain("not that the concurrent\nservice is frozen");
    expect(receipt.observed).toBeUndefined();
    expect((await invoke(["-h"])).exit).toBe(0);
  });

  it("requires every flag and refuses unknown, duplicate and bypass flags", async () => {
    expect((await invoke([])).receipt).toMatchObject({ ok: false, code: "INVALID_ARGUMENTS", resident: null });
    const f = fixture();
    for (let index = 0; index < f.argv.length; index += 2) {
      const argv = f.argv.filter((_, position) => position !== index && position !== index + 1);
      expect((await invoke(argv)).receipt.code).toBe("INVALID_ARGUMENTS");
    }
    for (const extra of [["--force"], ["--dry-run"], ["--help"], ["--resident", f.directory]]) {
      await refusal(f, "INVALID_ARGUMENTS", [...f.argv, ...extra]);
    }
  });

  it("refuses an expected release mismatch from either immutable owner field", async () => {
    for (const field of ["releaseRoot", "fabricExtensionPath"]) {
      const f = fixture();
      f.owner[field] = field === "releaseRoot" ? f.target : path.join(f.target, "dist/index.js");
      f.save();
      await refusal(f, "EXPECTED_RELEASE_MISMATCH");
    }
  });

  it("identifies legacy loaded-path mismatch before refusing missing attestation", async () => {
    const f = fixture();
    delete f.owner.releaseRoot;
    delete f.owner.configDigest;
    delete f.owner.handover;
    f.owner.fabricExtensionPath = path.join(f.target, "dist/index.js");
    f.save();
    await refusal(f, "EXPECTED_RELEASE_MISMATCH");
    f.owner.fabricExtensionPath = path.join(f.expected, "dist/index.js");
    f.save();
    const receipt = await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    expect(receipt.observed?.loadedRelease).toEqual({ releaseRoot: null, fabricExtensionPath: f.owner.fabricExtensionPath });
  });

  it("refuses each absent attestation field even when mutable config claims the old release", async () => {
    for (const field of ["releaseRoot", "fabricExtensionPath", "configDigest", "handover"]) {
      const f = fixture();
      delete f.owner[field];
      f.config.fabricExtensionPath = path.join(f.expected, "dist/index.js");
      f.save();
      await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    }
  });

  it("refuses a valid birth-bound attested root precisely at the absent admission protocol", async () => {
    const f = fixture();
    const receipt = await refusal(f, "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE");
    expect(receipt.observed).toEqual({ rootId: f.rootId, hostId: f.owner.hostId, pid: process.pid,
      processStartTime: f.owner.processStartTime, token: f.owner.token,
      loadedRelease: { releaseRoot: f.expected, fabricExtensionPath: path.join(f.expected, "dist/index.js") } });
    expect(receipt.reason).toContain("Structural launch identity is available");
    expect(receipt.reason).toContain("digest not recomputed/validated");
    expect(receipt.reason).toContain("closes admission and checkpoints accepted work");
    expect(receipt.reason).toContain("setActivationFilter skips/consumes rather than holds");
    expect(receipt.reason).toContain("pauseForRelease is internal");
    expect(receipt.reason).toContain("releaseChange is unconditionally deferred");
    expect(processStartTime(process.pid)).toBe(f.owner.processStartTime);
  });

  it("accepts an unambiguous resident id/prefix but still refuses admission", async () => {
    const f = fixture();
    for (const selector of [path.basename(f.directory), path.basename(f.directory).slice(0, 12)]) {
      const argv = [...f.argv]; argv[1] = selector;
      const before = snapshot(f.root);
      const { exit, receipt } = await invoke(argv);
      expect(exit).toBe(1);
      expect(receipt).toMatchObject({ code: "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", resident: f.directory });
      expect(snapshot(f.root)).toEqual(before);
    }
  });

  it("rejects missing, malformed, stale and invalid resident births/PIDs", async () => {
    for (const birth of [undefined, "", "invalid"]) {
      const f = fixture(); f.owner.processStartTime = birth; f.save();
      await refusal(f, "RESIDENT_BIRTH_MISSING");
    }
    for (const patch of [{ processStartTime: "0" }, { pid: 0 }, { pid: -1 }, { pid: "123" }, { pid: 1.5 }]) {
      const f = fixture(); Object.assign(f.owner, patch); f.save();
      await refusal(f, "RESIDENT_BIRTH_MISMATCH");
    }
  });

  it("rejects absent, malformed and stale handover launcher identity", async () => {
    for (const patch of [{ launcher: undefined }, { launcher: {} }, { abi: "future-abi" }]) {
      const f = fixture(); Object.assign(f.owner.handover as object, patch); f.save();
      await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    }
    const f = fixture();
    const handover = f.owner.handover as { launcher: Record<string, unknown> };
    handover.launcher.processStartTime = "0"; f.save();
    await refusal(f, "LAUNCHER_BIRTH_MISMATCH");
  });

  it("refuses malformed digest and unverifiable immutable launch files", async () => {
    const f = fixture(); f.owner.configDigest = "not-a-digest"; f.save();
    await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    const other = fixture();
    (other.owner.handover as { launcher: Record<string, unknown> }).launcher.runtime = path.join(other.root, "absent-runtime");
    other.save(); await refusal(other, "MISSING_LAUNCH_ATTESTATION");
  });

  it("rejects selectors outside the mesh, symlink selectors, ambiguity and unknown IDs", async () => {
    const f = fixture();
    fs.mkdirSync(path.join(f.meshRoot, "residency/abcdef01"));
    fs.mkdirSync(path.join(f.meshRoot, "residency/abcdef02"));
    fs.symlinkSync(f.directory, path.join(f.meshRoot, "residency/linked"));
    for (const selector of [f.expected, path.join(f.meshRoot, "residency/linked"), "abc", "not-hex", "ffff"]) {
      const argv = [...f.argv]; argv[1] = selector;
      await refusal(f, "RESIDENT_SELECTOR_INVALID", argv);
    }
  });

  it("rejects config root, owner host, residency and explicit mesh mismatches", async () => {
    for (const patch of [{ rootId: "different-root" }, { meshRoot: path.join(os.tmpdir(), "missing-drain-mesh") },
      { residencyRoot: os.tmpdir() }]) {
      const f = fixture(); Object.assign(f.config, patch); f.save();
      await refusal(f, "RESIDENT_IDENTITY_MISMATCH");
    }
    const f = fixture(); f.owner.hostId = residentHostId("other-root"); f.save();
    await refusal(f, "RESIDENT_IDENTITY_MISMATCH");
    const other = fixture();
    const aliasMesh = path.join(other.root, "other-mesh"); fs.mkdirSync(aliasMesh);
    // Same residency parent through a symlink must not hide the config mesh mismatch.
    fs.symlinkSync(path.join(other.meshRoot, "residency"), path.join(aliasMesh, "residency"));
    const argv = [...other.argv]; argv[3] = aliasMesh;
    await refusal(other, "RESIDENT_IDENTITY_MISMATCH", argv);
  });

  it("enforces channel OS-user ownership and refuses a symlink owner file", async () => {
    const f = fixture();
    await refusal(f, "RESIDENT_CHANNEL_UNOWNED", f.argv, { platform: "linux", getuid: () => fs.statSync(f.directory).uid + 1 });
    // Move only our fixture owner aside; no deletion, native kill or live-root access.
    const ownerPath = path.join(f.directory, "owner.json");
    fs.renameSync(ownerPath, path.join(f.directory, "retained-owner.json"));
    fs.symlinkSync(path.join(f.directory, "retained-owner.json"), ownerPath);
    await refusal(f, "RESIDENT_CHANNEL_UNOWNED");
  });

  it("refuses malformed resident JSON and missing explicit mesh", async () => {
    const f = fixture(); fs.writeFileSync(path.join(f.directory, "owner.json"), "{");
    const receipt = await refusal(f, "RESIDENT_STATE_INVALID");
    expect(receipt.observed).toBeUndefined();
    const other = fixture(); const argv = [...other.argv]; argv[3] = path.join(other.root, "missing-mesh");
    await refusal(other, "INVALID_MESH_ROOT", argv);
  });

  it("requires real absolute release roots, not mutable symlink selectors", async () => {
    const f = fixture(); const alias = path.join(f.root, "old-alias"); fs.symlinkSync(f.expected, alias);
    for (const [index, value] of [[5, alias], [5, "relative-old"], [7, path.join(f.root, "missing-target")]] as const) {
      const argv = [...f.argv]; argv[index] = value;
      const before = snapshot(f.root);
      const { exit, receipt } = await invoke(argv);
      expect(exit).toBe(1); expect(receipt.code).toBe("INVALID_RELEASE_ROOT");
      expect(snapshot(f.root)).toEqual(before);
    }
  });
});
