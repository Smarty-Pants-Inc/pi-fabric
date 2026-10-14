import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ensureResidentGenerations, recordLauncherGenerationExit, recordResidentGenerationError, recordResidentGenerationExit, readResidentGenerationError, readResidentGenerationExit,
  residentGenerationErrorPath, residentGenerationExitPath, residentGenerationsPath } from "../src/residency/generation-exit.js";
import { processHarness } from "./helpers/process-harness.js";
import { fixture } from "./helpers/dead-root-fixture.js";
import { ResidencyClient } from "../src/residency/client.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";

const recordError = (generation: string, error: string) => ({ format: 1 as const, generation, writer: "launcher" as const,
  error, occurredAt: Date.now(), pid: process.pid });
const recordExit = (generation: string) => ({ format: 1 as const, generation, writer: "host" as const,
  reason: "idle-exit", at: Date.now(), pid: process.pid });

// Decision C: these deterministic format/platform checks complement, rather than claim to replace, the Ryzen 1
// idle-resident exit check (#7950); see .local/fencing-result.md for the acceptance ledger.
describe("generation-private startup errors", () => {
  it.skipIf(process.platform !== "linux")("keeps error and exit record validators disjoint and preserves invalid existing bytes", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-error-shape-"));
    try {
      ensureResidentGenerations(root);
      const generation = randomUUID();
      const errorBytes = JSON.stringify(recordError(generation, "startup failed"));
      fs.writeFileSync(residentGenerationExitPath(root, generation), errorBytes, { mode: 0o600 });
      expect(recordResidentGenerationExit(root, recordExit(generation))).toMatchObject({ outcome: "invalid" });
      expect(readResidentGenerationExit(root, generation)).toBeUndefined();
      expect(fs.readFileSync(residentGenerationExitPath(root, generation), "utf8")).toBe(errorBytes);

      const other = randomUUID();
      const exitBytes = JSON.stringify(recordExit(other));
      fs.writeFileSync(residentGenerationErrorPath(root, other), exitBytes, { mode: 0o600 });
      expect(recordResidentGenerationError(root, recordError(other, "startup failed"))).toMatchObject({ outcome: "invalid" });
      expect(fs.readFileSync(residentGenerationErrorPath(root, other), "utf8")).toBe(exitBytes);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("fails closed without throwing or writing when Windows lacks its no-follow publication primitives", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-error-windows-"));
    try {
      ensureResidentGenerations(root);
      const { O_NOFOLLOW: _noFollow, ...constants } = fs.constants;
      void _noFollow;
      const generation = randomUUID();
      const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
      try {
        expect(() => recordResidentGenerationError(root, recordError(generation, "no-follow unavailable"), constants)).not.toThrow();
        expect(() => recordResidentGenerationExit(root, recordExit(generation), constants)).not.toThrow();
        expect(() => recordLauncherGenerationExit(root, recordExit(generation), constants)).not.toThrow();
        expect(fs.readdirSync(path.join(root, "generations"))).toEqual([]);
      } finally { platform.mockRestore(); }
      expect(fs.readdirSync(path.join(root, "generations"))).toEqual([]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});

// Decision C: Darwin injection exercises the filesystem boundary on Linux without borrowing /proc support.
// Native Darwin is unproven here; the per-case keep reasons and installed E2E are in .local/r2-writer-result.md.
describe.skipIf(process.platform !== "linux" && process.platform !== "darwin")("Darwin generation-private startup errors", () => {
  const fixtures: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of fixtures.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const setup = () => {
    const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-error-darwin-"));
    fixtures.push(fixtureRoot);
    const root = path.join(fixtureRoot, "resident");
    fs.mkdirSync(root, { mode: 0o700 });
    ensureResidentGenerations(root);
    const generation = randomUUID();
    const record = { ...recordError(generation, "Fabric resident host config is incomplete"), writer: "host" as const };
    return { fixtureRoot, root, generation, record };
  };
  const quiet = () => vi.spyOn(process.stderr, "write").mockImplementation(() => true);

  it("round-trips the original host error without any /proc/self/fd access", () => {
    const f = setup();
    const procAccesses: string[] = [];
    const withoutProc = <T extends (...args: never[]) => unknown>(operation: T): T =>
      ((...args: unknown[]) => {
        for (const arg of args) {
          if (typeof arg === "string" && arg.startsWith("/proc/self/fd")) {
            procAccesses.push(arg);
            throw new Error("Darwin must not use /proc/self/fd");
          }
        }
        return operation(...args as never[]);
      }) as unknown as T;
    const { openSync, statSync, lstatSync, readdirSync, linkSync, unlinkSync, rmSync, readFileSync } = fs;
    vi.spyOn(fs, "openSync").mockImplementation(withoutProc(openSync));
    vi.spyOn(fs, "statSync").mockImplementation(withoutProc(statSync));
    vi.spyOn(fs, "lstatSync").mockImplementation(withoutProc(lstatSync));
    vi.spyOn(fs, "readdirSync").mockImplementation(withoutProc(readdirSync));
    vi.spyOn(fs, "linkSync").mockImplementation(withoutProc(linkSync));
    vi.spyOn(fs, "unlinkSync").mockImplementation(withoutProc(unlinkSync));
    vi.spyOn(fs, "rmSync").mockImplementation(withoutProc(rmSync));
    vi.spyOn(fs, "readFileSync").mockImplementation(withoutProc(readFileSync));
    expect(recordResidentGenerationError(f.root, f.record, fs.constants, "darwin")).toEqual({ outcome: "written" });
    expect(readResidentGenerationError(f.root, f.generation, "darwin")).toEqual(f.record);
    expect(fs.statSync(residentGenerationErrorPath(f.root, f.generation)).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(residentGenerationsPath(f.root))).toEqual([`${f.generation}.error.json`]);
    expect(procAccesses).toEqual([]);
  });

  it("a repeated Darwin writer leaves the original host error bytes unchanged", () => {
    const f = setup();
    expect(recordResidentGenerationError(f.root, f.record, fs.constants, "darwin")).toEqual({ outcome: "written" });
    const original = fs.readFileSync(residentGenerationErrorPath(f.root, f.generation));
    expect(recordResidentGenerationError(f.root, recordError(f.generation, "later launcher error"), fs.constants, "darwin"))
      .toEqual({ outcome: "exists" });
    expect(fs.readFileSync(residentGenerationErrorPath(f.root, f.generation))).toEqual(original);
    expect(readResidentGenerationError(f.root, f.generation, "darwin")).toEqual(f.record);
  });

  it("Darwin refuses symlinked generations without writing to the target", () => {
    const f = setup();
    const target = path.join(f.fixtureRoot, "target-generations");
    fs.renameSync(residentGenerationsPath(f.root), target);
    fs.symlinkSync(target, residentGenerationsPath(f.root));
    quiet();
    expect(recordResidentGenerationError(f.root, f.record, fs.constants, "darwin")).toMatchObject({ outcome: "unavailable" });
    expect(fs.readdirSync(target)).toEqual([]);
  });

  it("Darwin refuses a symlinked record for both write and read and leaves its target unchanged", () => {
    const f = setup();
    const target = path.join(f.fixtureRoot, "target-error.json");
    const bytes = JSON.stringify(f.record);
    fs.writeFileSync(target, bytes, { mode: 0o600 });
    fs.symlinkSync(target, residentGenerationErrorPath(f.root, f.generation));
    quiet();
    expect(recordResidentGenerationError(f.root, recordError(f.generation, "must not replace"), fs.constants, "darwin"))
      .toMatchObject({ outcome: "unavailable" });
    expect(readResidentGenerationError(f.root, f.generation, "darwin")).toBeUndefined();
    expect(fs.readFileSync(target, "utf8")).toBe(bytes);
    expect(fs.lstatSync(residentGenerationErrorPath(f.root, f.generation)).isSymbolicLink()).toBe(true);
  });

  it.each(["root", "generations"] as const)("Darwin refuses a post-write %s identity mismatch without path cleanup", which => {
    const f = setup();
    const held = path.join(f.fixtureRoot, `held-${which}`);
    const replacement = JSON.stringify(recordError(f.generation, "replacement directory sentinel"));
    const fsync = fs.fsyncSync;
    let swapped = false;
    // Swap after the complete file write, while the directory fd is still held. A valid replacement record
    // makes this pass as "written" if the identity recheck is removed; neither directory's record may be deleted.
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (!swapped && fs.fstatSync(fd).isDirectory()) {
        swapped = true;
        const fullPath = which === "root" ? f.root : residentGenerationsPath(f.root);
        fs.renameSync(fullPath, held);
        fs.mkdirSync(fullPath, { mode: 0o700 });
        if (which === "root") ensureResidentGenerations(f.root);
        fs.writeFileSync(residentGenerationErrorPath(f.root, f.generation), replacement, { mode: 0o600 });
      }
      fsync(fd);
    });
    quiet();
    expect(recordResidentGenerationError(f.root, f.record, fs.constants, "darwin"))
      .toMatchObject({ outcome: "unavailable", reason: expect.stringContaining("directory identity changed") });
    expect(swapped).toBe(true);
    const heldRecord = which === "root" ? residentGenerationErrorPath(held, f.generation) : path.join(held, `${f.generation}.error.json`);
    expect(JSON.parse(fs.readFileSync(heldRecord, "utf8"))).toEqual(f.record);
    expect(fs.readFileSync(residentGenerationErrorPath(f.root, f.generation), "utf8")).toBe(replacement);
  });

  it.each(["root", "generations"] as const)("Darwin refuses a non-private %s directory before creating a record", which => {
    const f = setup();
    fs.chmodSync(which === "root" ? f.root : residentGenerationsPath(f.root), 0o770);
    quiet();
    expect(recordResidentGenerationError(f.root, f.record, fs.constants, "darwin"))
      .toMatchObject({ outcome: "unavailable", reason: expect.stringContaining("not a private directory owned by this user") });
    expect(fs.readdirSync(residentGenerationsPath(f.root))).toEqual([]);
  });
});

// Real client -> compiled launcher -> host startup refusal: host failure text must survive generation attribution.
it.skipIf(process.platform !== "linux" || !fs.existsSync(path.resolve("dist/residency/launcher.js")))(
  "surfaces the original host startup failure instead of timeout or stale diagnostics", { timeout: 30_000 }, async () => {
    const f = await fixture();
    let client: ResidencyClient | undefined;
    try {
      await f.host.close();
      // A real native Pi loads pi-entry and reaches the host's config validator.
      // An unsupported format must fail before ownership, with its original error intact.
      f.config.piBinary = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
      f.config.format = 2 as 1;
      const generations = path.join(f.config.residencyRoot, "generations");
      const sentinel = Buffer.from('{"legacy":"must remain untouched"}');
      const staleGeneration = randomUUID();
      const staleBytes = Buffer.from(JSON.stringify(recordError(staleGeneration, "stale startup error")));
      fs.writeFileSync(path.join(f.config.residencyRoot, "error.json"), sentinel);
      ensureResidentGenerations(f.config.residencyRoot);
      fs.writeFileSync(residentGenerationErrorPath(f.config.residencyRoot, staleGeneration), staleBytes);
      client = new ResidencyClient({ config: f.config, mesh: f.host.mesh, participants: f.host.participants,
        mainAgent: { local: false } as FabricMainAgentTarget,
        hostPath: path.resolve("dist/residency/launcher.js"), startupTimeoutMs: 10_000 });
      let failure: unknown;
      try { await client.ensureHost(); } catch (error) { failure = error; }
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).message).toMatch(/^Fabric resident host failed to start:/);
      const errors = fs.readdirSync(generations).filter(name => name.endsWith(".error.json"));
      const current = errors.map(name => JSON.parse(fs.readFileSync(path.join(generations, name), "utf8")))
        .find(record => record.writer === "host" && record.error !== "stale startup error");
      expect(current).toBeDefined();
      expect((failure as Error).message).toBe(`Fabric resident host failed to start: ${current.error}`);
      expect(current.writer).toBe("host");
      expect(current.error).toBe("Fabric resident host config is incomplete");
      expect(fs.readFileSync(path.join(f.config.residencyRoot, "error.json"))).toEqual(sentinel);
      expect(fs.readFileSync(residentGenerationErrorPath(f.config.residencyRoot, staleGeneration))).toEqual(staleBytes);
    } finally { await client?.close(); await f.close(); }
  });

// Process acceptance: the predecessor is the real launcher running supervise() and a real child; only its final
// no-replace error publication is paused. A successor publishes its own record before the predecessor resumes.
it.skipIf(process.platform !== "linux")("a paused real predecessor launcher cannot replace or prune a successor error", { timeout: 20_000 }, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-error-race-"));
  const generation = randomUUID();
  const configPath = path.join(root, "config.json");
  const childPath = path.join(root, "failing-child.mjs");
  fs.writeFileSync(childPath, "console.error('fixture failure 23'); process.exit(23);\n");
  fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: childPath }));
  const legacyPath = path.join(root, "error.json");
  const legacyBytes = Buffer.from('{"legacy":"unchanged"}');
  fs.writeFileSync(legacyPath, legacyBytes);
  const harness = processHarness();
  try {
    const predecessor = harness.launcherError(configPath, generation);
    await predecessor.next("paused", 10_000);
    const successorGeneration = randomUUID();
    const ownerPath = path.join(root, "owner.json");
    const successorOwner = JSON.stringify({ token: successorGeneration, pid: process.pid, startedAt: Date.now() });
    fs.writeFileSync(ownerPath, successorOwner, { mode: 0o600 });
    const successor = recordError(successorGeneration, "successor startup error");
    expect(recordResidentGenerationError(root, successor)).toEqual({ outcome: "written" });
    const successorPath = residentGenerationErrorPath(root, successorGeneration);
    const successorBytes = fs.readFileSync(successorPath, "utf8");
    fs.writeFileSync(path.join(root, `resume-${generation}`), "resume\n", { mode: 0o600 });
    await predecessor.next("done", 10_000);
    expect(fs.readFileSync(successorPath, "utf8")).toBe(successorBytes);
    expect(fs.readFileSync(ownerPath, "utf8")).toBe(successorOwner);
    expect(fs.readFileSync(legacyPath)).toEqual(legacyBytes);
    expect(JSON.parse(fs.readFileSync(residentGenerationErrorPath(root, generation), "utf8")))
      .toMatchObject({ generation, writer: "launcher", error: expect.stringContaining("23") });
  } finally {
    await harness.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
