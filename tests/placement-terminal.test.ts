import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  assertPlacementTerminalRecord,
  makePlacementTerminalRecord,
  PlacementTerminalWriteError,
  placementTerminalPath,
  writePlacementTerminalFile,
  type PlacementTerminalRecord,
} from "../src/placement/terminal.js";

const temporaryRoots: string[] = [];
const temporaryRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "placement-terminal-test-"));
  temporaryRoots.push(root);
  return root;
};

afterEach(() => {
  for (const root of temporaryRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const record = (): PlacementTerminalRecord =>
  makePlacementTerminalRecord("run-123", "finished", { at: 1_700_000_000_000, exitCode: 0 });

describe("Placement terminal record validation", () => {
  it("builds a canonical timestamped record and rejects invalid fields or extra keys", () => {
    const valid = record();
    expect(valid).toEqual({
      placementRunId: "run-123", state: "finished", at: "2023-11-14T22:13:20.000Z", exitCode: 0,
    });
    expect(() => assertPlacementTerminalRecord(valid)).not.toThrow();
    expect(() => makePlacementTerminalRecord("../run", "finished", { at: 1 })).toThrow(PlacementTerminalWriteError);
    expect(() => makePlacementTerminalRecord("run-123", "failed", { at: 1, exitCode: 256 })).toThrow(PlacementTerminalWriteError);
    expect(() => makePlacementTerminalRecord("run-123", "failed", { at: 1, reason: "bad\nreason" })).toThrow(PlacementTerminalWriteError);
    expect(() => assertPlacementTerminalRecord({ ...valid, unexpected: true })).toThrow(PlacementTerminalWriteError);
    expect(() => assertPlacementTerminalRecord({ ...valid, at: "2023-11-14T22:13:20Z" })).toThrow(PlacementTerminalWriteError);
  });
});

describe("FILE placement terminal receipts", () => {
  it("persists an exact record idempotently and rejects a conflicting replay", () => {
    const meshRoot = path.join(temporaryRoot(), "mesh");
    const hostId = "host:test";
    const first = record();
    const file = placementTerminalPath(meshRoot, hostId, first.placementRunId);

    writePlacementTerminalFile(meshRoot, hostId, first);
    const firstBytes = fs.readFileSync(file, "utf8");
    expect(JSON.parse(firstBytes)).toEqual(first);
    if (process.platform !== "win32") {
      expect(fs.statSync(file).mode & 0o777).toBe(0o600);
      expect(fs.statSync(path.dirname(file)).mode & 0o777).toBe(0o700);
    }
    expect(fs.readdirSync(path.dirname(file))).toEqual([path.basename(file)]);

    expect(() => writePlacementTerminalFile(meshRoot, hostId, first)).not.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe(firstBytes);

    const conflicting: PlacementTerminalRecord = { ...first, state: "failed", exitCode: 1 };
    expect(() => writePlacementTerminalFile(meshRoot, hostId, conflicting)).toThrow(PlacementTerminalWriteError);
    expect(fs.readFileSync(file, "utf8")).toBe(firstBytes);
  });

  it("does not create a receipt for an invalid record", () => {
    const meshRoot = path.join(temporaryRoot(), "mesh");
    const invalid = { ...record(), state: "unknown" } as unknown as PlacementTerminalRecord;
    expect(() => writePlacementTerminalFile(meshRoot, "host:test", invalid)).toThrow(PlacementTerminalWriteError);
    expect(fs.existsSync(path.join(meshRoot, "host-leases"))).toBe(false);
  });
});
