import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { residentAttestationPath, writeResidentAttestation, type AttemptLaunchReceipt } from "../src/residency/attempt-containment.js";
import { parseResidentAttemptAttestation } from "../src/residency/protocol.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-attestation-test-"));
  roots.push(root);
  return root;
};

describe("resident launch attestation lead record", () => {
  it.skipIf(process.platform !== "linux")("creates one exclusive mode-0600 exact receipt and preserves it unchanged", () => {
    const root = fixture();
    const receipt: AttemptLaunchReceipt = { attemptId: "test-attempt_1", scope: "/user.slice/test.scope", cgroupPath: "/user.slice/test.scope", launchedInScope: true, at: Date.now() };
    const file = writeResidentAttestation(root, receipt);
    expect(file).toBe(path.join(root, "attempt-launch-test-attempt_1.json"));
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    const record = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(record).toEqual(receipt);
    expect(parseResidentAttemptAttestation(record, JSON.parse(JSON.stringify(record)), receipt.attemptId, receipt.scope)).toEqual(receipt);
    const reordered = { at: receipt.at, scope: receipt.scope, launchedInScope: true, cgroupPath: receipt.cgroupPath, attemptId: receipt.attemptId };
    expect(parseResidentAttemptAttestation(record, reordered, receipt.attemptId, receipt.scope)).toBe(record);
    expect(() => writeResidentAttestation(root, { ...receipt, at: receipt.at + 1 })).toThrow();
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(record);
  });

  it("rejects record/env mismatch, extra fields, malformed legacy values and traversal before path creation", () => {
    const root = fixture();
    const receipt: AttemptLaunchReceipt = { attemptId: "valid-id", scope: "/user.slice/test.scope", cgroupPath: "/user.slice/test.scope", launchedInScope: true, at: Date.now() };
    const valid = { ...receipt };
    expect(() => parseResidentAttemptAttestation(valid, { ...valid, at: valid.at + 1 }, receipt.attemptId, receipt.scope)).toThrow(/mismatch/);
    expect(() => parseResidentAttemptAttestation({ ...valid, ignored: true }, { ...valid, ignored: true }, receipt.attemptId, receipt.scope)).toThrow(/mismatch/);
    expect(() => parseResidentAttemptAttestation({ attemptId: receipt.attemptId, scope: receipt.scope }, undefined, receipt.attemptId, receipt.scope)).toThrow();
    expect(() => residentAttestationPath(root, "../escape")).toThrow(/invalid attempt identity/);
    expect(fs.readdirSync(root)).toEqual([]);
  });
});
