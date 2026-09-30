import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { inspectStaleMain, readReleaseCatalog, StaleMainGuard } from "../src/lifecycle/stale-main.js";

const roots: string[] = [];
afterEach(() => roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true })));
const fixture = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-metadata-"));
  roots.push(base);
  const releases = path.join(base, "releases");
  const labels = ["z-loaded", "m-critical", "a-active"];
  for (const [index, label] of labels.entries()) {
    const root = path.join(releases, label);
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    fs.writeFileSync(path.join(base, `${label}.receipt.json`), JSON.stringify({ commit: label, installedAt: new Date(Date.UTC(2026, 8, 30, index)).toISOString() }));
  }
  const settings = path.join(base, "settings.json");
  fs.writeFileSync(settings, JSON.stringify({ packages: [path.join(releases, "a-active")] }));
  fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ version: 1, releases: {} }));
  return { base, releases, loaded: path.join(releases, "z-loaded"), settings };
};

describe("release safety metadata", () => {
  it.each(["manifest.json", "install-receipt.json"])("reads optional safetyCritical from %s and orders by receipts, not SHA/mtime", name => {
    const f = fixture();
    fs.writeFileSync(path.join(f.releases, "m-critical", name), JSON.stringify({ safetyCritical: true }));
    const result = inspectStaleMain(f.loaded, f.settings);
    expect(result).toMatchObject({ refused: true, criticalReleases: ["m-critical"] });
    expect(result?.reason).toContain("/fabric-release-reload");
  });

  it("reads optional receipt flags and retains critical metadata after pruning", () => {
    const f = fixture();
    const receipt = path.join(f.base, "m-critical.receipt.json");
    const before = JSON.parse(fs.readFileSync(receipt, "utf8"));
    fs.writeFileSync(receipt, JSON.stringify({ ...before, safetyCritical: true }));
    fs.writeFileSync(path.join(f.base, "releases-safety.json"), JSON.stringify({ releases: { "m-critical": { safetyCritical: false } } }));
    fs.rmSync(path.join(f.releases, "m-critical"), { recursive: true });
    expect(inspectStaleMain(f.loaded, f.settings)?.criticalReleases).toEqual(["m-critical"]);
    expect(JSON.parse(fs.readFileSync(receipt, "utf8"))).toEqual({ ...before, safetyCritical: true });
  });

  it("fails closed on missing, invalid and tied chronology, without changing equality", () => {
    const f = fixture();
    fs.unlinkSync(path.join(f.base, "z-loaded.receipt.json"));
    expect(inspectStaleMain(f.loaded, f.settings)).toMatchObject({ refused: true, reason: expect.stringContaining("missing install/activation time") });
    fs.writeFileSync(path.join(f.base, "z-loaded.receipt.json"), JSON.stringify({ installedAt: "invalid" }));
    expect(inspectStaleMain(f.loaded, f.settings)?.reason).toContain("Invalid installedAt");
    fs.writeFileSync(path.join(f.base, "z-loaded.receipt.json"), fs.readFileSync(path.join(f.base, "a-active.receipt.json"), "utf8").replaceAll("a-active", "z-loaded"));
    expect(inspectStaleMain(f.loaded, f.settings)?.reason).toContain("tied");
    expect(inspectStaleMain(path.join(f.releases, "a-active"), f.settings)).toBeUndefined();
  });

  it("uses activation times only when install times are absent and sees policy edits on next admission", () => {
    const f = fixture();
    for (const label of ["z-loaded", "m-critical", "a-active"]) {
      const file = path.join(f.base, `${label}.receipt.json`);
      const row = JSON.parse(fs.readFileSync(file, "utf8"));
      fs.writeFileSync(file, JSON.stringify({ commit: label, activatedAt: row.installedAt }));
    }
    expect(inspectStaleMain(f.loaded, f.settings)?.refused).toBe(false);
    fs.writeFileSync(path.join(f.base, "releases-safety.json"), JSON.stringify({ releases: { "m-critical": { safetyCritical: true } } }));
    expect(inspectStaleMain(f.loaded, f.settings)?.refused).toBe(true);
  });

  it("deduplicates across manager/guard replacements per Main and active release", () => {
    const f = fixture();
    const id = randomUUID();
    const publish = vi.fn();
    new StaleMainGuard(f.loaded, id, f.settings, publish).check();
    new StaleMainGuard(f.loaded, id, f.settings, publish).check();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ loaded: "z-loaded", active: "a-active", refused: false }));
  });

  it("claims one ops notice across process-local state replacement", () => {
    const f = fixture();
    const id = randomUUID();
    const first = vi.fn();
    new StaleMainGuard(f.loaded, id, f.settings, first).check();
    delete (globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.stale-main.reported")];
    const resident = vi.fn();
    new StaleMainGuard(f.loaded, id, f.settings, resident).check();
    expect(first).toHaveBeenCalledOnce();
    expect(resident).not.toHaveBeenCalled();
    expect(fs.readdirSync(path.join(f.base, "fabric", "stale-main-events"))).toHaveLength(1);
  });

  it("refuses a critical release tied with the loaded boundary", () => {
    const f = fixture();
    const loaded = JSON.parse(fs.readFileSync(path.join(f.base, "z-loaded.receipt.json"), "utf8"));
    fs.writeFileSync(path.join(f.base, "m-critical.receipt.json"), JSON.stringify({ commit: "m-critical", installedAt: loaded.installedAt, safetyCritical: true }));
    expect(inspectStaleMain(f.loaded, f.settings)?.reason).toContain("order is ambiguous");
  });
  it("ships the mandated B66 and B68 safety marks", () => {
    const policy = JSON.parse(fs.readFileSync(path.resolve("releases-safety.json"), "utf8"));
    expect(policy.releases["e17d78b82377e623fa1d71336baf9b01eb47f731"].safetyCritical).toBe(true);
    expect(policy.releases["e4cbf5b5be29384a4acca999c3b31560b7054c03"].safetyCritical).toBe(true);
    const f = fixture();
    expect(readReleaseCatalog(f.releases).has("z-loaded")).toBe(true);
  });
});
