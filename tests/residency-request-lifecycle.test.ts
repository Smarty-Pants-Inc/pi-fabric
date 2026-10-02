import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  abandonResidentRequest,
  commitResidentRequest,
  readResidentRequestDecision,
  sleepUnlessAborted,
  type ResidentCommand,
} from "../src/residency/protocol.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("abandonResidentRequest", () => {
  it("removes the abandoned request and its late response, leaving siblings alone", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-abandon-"));
    const requests = path.join(root, "requests");
    const responses = path.join(root, "responses");
    fs.mkdirSync(requests, { recursive: true });
    fs.mkdirSync(responses, { recursive: true });
    fs.writeFileSync(path.join(requests, "gone.json"), "{}");
    fs.writeFileSync(path.join(responses, "gone.json"), "{}");
    fs.writeFileSync(path.join(requests, "kept.json"), "{}");

    try {
      abandonResidentRequest(requests, responses, "gone");
      expect(readResidentRequestDecision(root, "gone")).toEqual({ requestId: "gone", state: "abandoned" });
      expect(fs.existsSync(path.join(requests, "gone.json"))).toBe(false);
      expect(fs.existsSync(path.join(responses, "gone.json"))).toBe(false);
      expect(fs.existsSync(path.join(requests, "kept.json"))).toBe(true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("creates the fence even with missing exchange files or directories", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-abandon-"));
    try {
      expect(() =>
        abandonResidentRequest(path.join(root, "no-requests"), path.join(root, "no-responses"), "absent"),
      ).not.toThrow();
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["spawnBound", "createActor", "removeActor", "foreground", "cleanup"] as const)("fences %s with the same immutable decision", (operation) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-fence-"));
    const command = { requestId: "r", operation } as ResidentCommand;
    try {
      abandonResidentRequest(path.join(root, "requests"), path.join(root, "responses"), "r");
      expect(() => commitResidentRequest(root, command, "known", "owner")).toThrow(/abandoned before commit/);
      expect(readResidentRequestDecision(root, "r")?.state).toBe("abandoned");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("retains the committed known ID and exchange files rather than claiming abandonment", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-fence-"));
    const requests = path.join(root, "requests");
    const responses = path.join(root, "responses");
    fs.mkdirSync(requests); fs.mkdirSync(responses);
    fs.writeFileSync(path.join(requests, "r.json"), "{}");
    fs.writeFileSync(path.join(responses, "r.json"), "{}");
    const command = { requestId: "r", operation: "spawnBound" } as ResidentCommand;
    try {
      commitResidentRequest(root, command, "known", "owner");
      expect(abandonResidentRequest(requests, responses, "r")).toEqual({
        requestId: "r", state: "committed", operation: "spawnBound", id: "known", ownerHostId: "owner",
      });
      expect(fs.existsSync(path.join(requests, "r.json"))).toBe(true);
      expect(fs.existsSync(path.join(responses, "r.json"))).toBe(true);
      expect(() => commitResidentRequest(root, command, "second", "owner")).toThrow(/already committed/);
      expect(readResidentRequestDecision(root, "r")?.id).toBe("known");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("publishes a complete tombstone before deletion and fails closed if the fence cannot be written", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-fence-"));
    const requests = path.join(root, "requests");
    fs.mkdirSync(requests);
    const request = path.join(requests, "r.json");
    fs.writeFileSync(request, "{}");
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((...args) => {
      if (args[0] === request) expect(readResidentRequestDecision(root, "r")?.state).toBe("abandoned");
      return remove(...args);
    });
    try {
      abandonResidentRequest(requests, path.join(root, "responses"), "r");
      fs.writeFileSync(path.join(requests, "failed.json"), "{}");
      vi.spyOn(fs, "linkSync").mockImplementation(() => { throw Object.assign(new Error("no atomic fence"), { code: "EPERM" }); });
      expect(() => abandonResidentRequest(requests, path.join(root, "responses"), "failed")).toThrow("no atomic fence");
      expect(fs.existsSync(path.join(requests, "failed.json"))).toBe(true);
      expect(readResidentRequestDecision(root, "failed")).toBeUndefined();
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("sleepUnlessAborted", () => {
  it("resolves after the interval without a signal", async () => {
    vi.useFakeTimers();
    const pending = sleepUnlessAborted(100);
    vi.advanceTimersByTime(100);
    await pending;
  });

  it("rejects immediately for an already-aborted signal", async () => {
    const controller = new AbortController();
    controller.abort(new Error("stop"));
    await expect(sleepUnlessAborted(1_000, controller.signal)).rejects.toThrow("stop");
  });

  it("wakes without waiting out the interval when aborted mid-sleep", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const pending = sleepUnlessAborted(30_000, controller.signal);
    controller.abort(new Error("mid-sleep stop"));
    await expect(pending).rejects.toThrow("mid-sleep stop");
  });
});
