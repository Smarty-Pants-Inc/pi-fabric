import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { recordResidentGenerationExit, residentGenerationExitPath, residentGenerationsPath,
  type ResidentGenerationExit } from "../src/residency/generation-exit.js";
import { processHarness } from "./helpers/process-harness.js";

// smarty-dev#7950 (pi-fabric#902 review P1-1, P1-2): the record goes only into the verified generations/ dir of the
// verified residency root, through pinned fds, and is published complete (temp file, fsync, no-replace link); an
// existing record is validated.
describe.skipIf(process.platform !== "linux")("resident generation exit record", () => {
  const roots: string[] = [];
  afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-generation-exit-"));
    roots.push(root);
    fs.mkdirSync(residentGenerationsPath(root), { mode: 0o700 });
    const generation = randomUUID();
    const record = (writer: ResidentGenerationExit["writer"]): ResidentGenerationExit =>
      ({ format: 1, generation, writer, reason: writer === "host" ? "idle-exit" : "unreported", pid: process.pid, at: Date.now() });
    const outcome = (writer: ResidentGenerationExit["writer"]) => recordResidentGenerationExit(root, record(writer));
    const read = () => JSON.parse(fs.readFileSync(residentGenerationExitPath(root, generation), "utf8")) as ResidentGenerationExit;
    return { root, generation, record, outcome, read, entries: () => fs.readdirSync(residentGenerationsPath(root)).sort() };
  };
  const quiet = () => vi.spyOn(process.stderr, "write").mockImplementation(() => true);

  it("writes one complete record; a second writer gets exists and leaves it unchanged; no temp file stays", () => {
    const f = setup();
    expect(f.outcome("host")).toEqual({ outcome: "written" });
    expect(f.outcome("launcher")).toEqual({ outcome: "exists" });
    expect(f.read()).toMatchObject({ writer: "host", reason: "idle-exit" });
    expect(f.entries()).toEqual([`${f.generation}.exit.json`]);
    expect(fs.statSync(residentGenerationExitPath(f.root, f.generation)).mode & 0o777).toBe(0o600);
  });

  it.each(["generations", "root"] as const)("a group- or world-writable %s dir refuses with no record and one stderr line", which => {
    for (const mode of ["770", "707", "777"]) {
      const f = setup();
      fs.chmodSync(which === "root" ? f.root : residentGenerationsPath(f.root), Number.parseInt(mode, 8));
      const stderr = quiet();
      expect(f.outcome("launcher")).toMatchObject({ outcome: "unavailable", reason: expect.stringContaining("not a private directory") });
      expect(f.entries()).toEqual([]);
      expect(stderr).toHaveBeenCalledTimes(1);
      stderr.mockRestore();
    }
  });

  it("a generations/ symlink refuses with no record", () => {
    const f = setup();
    const elsewhere = path.join(f.root, "elsewhere");
    fs.renameSync(residentGenerationsPath(f.root), elsewhere);
    fs.symlinkSync(elsewhere, residentGenerationsPath(f.root));
    quiet();
    expect(f.outcome("launcher").outcome).toBe("unavailable");
    expect(fs.readdirSync(elsewhere)).toEqual([]);
  });

  it("a generations/ dir swapped after the check cannot receive the record: it lands in the verified dir", () => {
    const f = setup();
    const verified = path.join(f.root, "verified-generations");
    const open = fs.openSync;
    let swapped = false;
    // After both dir checks, at the temp file's open: the path generations/ now names another (private) directory.
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (!swapped && String(file).endsWith(".tmp")) {
        swapped = true;
        fs.renameSync(residentGenerationsPath(f.root), verified);
        fs.mkdirSync(residentGenerationsPath(f.root), { mode: 0o700 });
      }
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    expect(f.outcome("host")).toEqual({ outcome: "written" });
    expect(swapped).toBe(true);
    expect(fs.readdirSync(residentGenerationsPath(f.root))).toEqual([]);
    expect(fs.readdirSync(verified)).toEqual([`${f.generation}.exit.json`]);
  });

  it.each([
    ["empty", ""],
    ["partial JSON", "{\"format\":1,\"gener"],
    ["another generation's record", JSON.stringify({ format: 1, generation: randomUUID(), writer: "host", reason: "idle-exit", pid: 1, at: 1 })],
  ])("an existing %s record is invalid, with its reason; it is left as it was", (_name, bytes) => {
    const f = setup();
    fs.writeFileSync(residentGenerationExitPath(f.root, f.generation), bytes, { mode: 0o600 });
    quiet();
    expect(f.outcome("launcher")).toMatchObject({ outcome: "invalid", reason: expect.stringMatching(/incomplete|unreadable/) });
    expect(fs.readFileSync(residentGenerationExitPath(f.root, f.generation), "utf8")).toBe(bytes);
    expect(f.entries()).toEqual([`${f.generation}.exit.json`]);
  });

  it("a host killed between its temp write and the link publishes nothing; the launcher's record goes in", async () => {
    const f = setup();
    const h = processHarness();
    try {
      const host = h.generationExit(f.root, f.generation, "before-link");
      await host.next("paused");
      expect(await host.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      expect(fs.existsSync(residentGenerationExitPath(f.root, f.generation))).toBe(false);
      expect(f.outcome("launcher")).toEqual({ outcome: "written" });
      expect(f.read()).toMatchObject({ writer: "launcher", reason: "unreported" });
    } finally { await h.close(); }
  }, 60_000);

  it("a host killed after the link has published its complete record; the launcher's write is a no-op", async () => {
    const f = setup();
    const h = processHarness();
    try {
      const host = h.generationExit(f.root, f.generation, "after-link");
      await host.next("paused");
      expect(await host.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      expect(f.read()).toMatchObject({ writer: "host", reason: "idle-exit", pid: host.pid });
      expect(f.outcome("launcher")).toEqual({ outcome: "exists" });
      expect(f.read()).toMatchObject({ writer: "host", pid: host.pid });
    } finally { await h.close(); }
  }, 60_000);
});
