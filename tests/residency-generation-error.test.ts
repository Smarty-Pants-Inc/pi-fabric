import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ensureResidentGenerations, recordLauncherGenerationExit, recordResidentGenerationError, recordResidentGenerationExit, readResidentGenerationExit,
  residentGenerationErrorPath, residentGenerationExitPath } from "../src/residency/generation-exit.js";
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
