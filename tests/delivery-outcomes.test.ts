import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { appendDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("durable delivery outcomes", () => {
  it("appends delivered, superseded, failed, and unknown as JSONL records", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-outcomes-")); roots.push(root);
    const send = { eventId: "event-1", to: "session:target", from: "session:sender", mode: "followUp" as const };
    for (const outcome of ["delivered", "superseded", "failed", "unknown"] as const) {
      appendDeliveryOutcome(root, send, outcome, `reason-${outcome}`, 1_735_689_600_000);
    }
    const file = fs.readdirSync(path.join(root, "delivery-outcomes"))[0]!;
    const rows = fs.readFileSync(path.join(root, "delivery-outcomes", file), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows.map(row => row.outcome)).toEqual(["delivered", "superseded", "failed", "unknown"]);
    expect(rows[0]).toMatchObject({ eventId: "event-1", to: "session:target", from: "session:sender", mode: "followUp", reason: "reason-delivered" });
  });

  it("dedupes a final receipt after a fresh-module restart and across midnight, but permits unknown then delivered", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-restart-")); roots.push(root);
    const send = { eventId: "restart", to: "session:t", from: "session:f", mode: "steer" as const };
    appendDeliveryOutcome(root, send, "unknown", "timed out", Date.parse("2025-01-01T23:59:00Z"));
    vi.resetModules();
    const restarted = await import("../src/mesh/delivery-outcomes.js");
    restarted.appendDeliveryOutcome(root, send, "unknown", "same observation", Date.parse("2025-01-02T00:01:00Z"));
    restarted.appendDeliveryOutcome(root, send, "delivered", "consumed", Date.parse("2025-01-02T00:02:00Z"));
    restarted.appendDeliveryOutcome(root, send, "delivered", "confirmed again", Date.parse("2025-01-02T00:03:00Z"));
    const directory = path.join(root, "delivery-outcomes");
    const records = fs.readdirSync(directory).flatMap(file => fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(records.map(row => row.outcome)).toEqual(["unknown", "delivered"]);
  });

  it("does not append a second line when a post-write fsync failure is retried", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-fsync-")); roots.push(root);
    const send = { eventId: "barrier", to: "t", from: "f", mode: "publish" as const };
    const sync = fs.fsyncSync.bind(fs); let failed = false;
    const spy = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (!failed && fs.fstatSync(fd).isFile()) { failed = true; throw new Error("barrier failed"); }
      sync(fd);
    });
    try { expect(() => appendDeliveryOutcome(root, send, "delivered", "consumed")).toThrow("barrier failed"); }
    finally { spy.mockRestore(); }
    appendDeliveryOutcome(root, send, "delivered", "retry");
    const directory = path.join(root, "delivery-outcomes");
    expect(fs.readFileSync(path.join(directory, fs.readdirSync(directory)[0]!), "utf8").trim().split("\n")).toHaveLength(1);
  });

  it("fsyncs a dedup receipt through a writable append handle (Windows rejects a read-only fsync)", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-receipt-")); roots.push(root);
    const send = { eventId: "receipt", to: "t", from: "f", mode: "followUp" as const };
    appendDeliveryOutcome(root, send, "failed", "expired");
    const open = fs.openSync.bind(fs); const sync = fs.fsyncSync.bind(fs);
    const flags = new Map<number, number>(); const synced: number[] = [];
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation((file, mode, permissions) => {
      const fd = open(file, mode, permissions);
      if (String(file).endsWith(".jsonl")) flags.set(fd, mode as number);
      return fd;
    });
    const syncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const mode = flags.get(fd);
      if (mode !== undefined && fs.fstatSync(fd).isFile()) synced.push(mode);
      sync(fd);
    });
    try { appendDeliveryOutcome(root, send, "failed", "expired again"); }
    finally { openSpy.mockRestore(); syncSpy.mockRestore(); }
    expect(synced).toHaveLength(1);
    expect(synced[0]! & (fs.constants.O_WRONLY | fs.constants.O_RDWR)).not.toBe(0);
    expect(synced[0]! & fs.constants.O_APPEND).toBe(fs.constants.O_APPEND);
    expect(synced[0]! & fs.constants.O_TRUNC).toBe(0);
    const directory = path.join(root, "delivery-outcomes");
    expect(fs.readFileSync(path.join(directory, fs.readdirSync(directory)[0]!), "utf8").trim().split("\n")).toHaveLength(1);
  });

  it("opens with O_APPEND and does not acquire the mesh lock", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-append-")); roots.push(root);
    fs.mkdirSync(path.join(root, ".lock"));
    fs.writeFileSync(path.join(root, ".lock", "owner"), "held by a different mesh writer");
    const open = fs.openSync.bind(fs); const flags: number[] = [];
    const spy = vi.spyOn(fs, "openSync").mockImplementation((file, mode, permissions) => {
      if (String(file).endsWith(".jsonl")) flags.push(mode as number);
      return open(file, mode, permissions);
    });
    try { appendDeliveryOutcome(root, { eventId: "e", to: "t", from: "f", mode: "steer" }, "delivered", "consumed"); }
    finally { spy.mockRestore(); }
    expect(flags).toHaveLength(1);
    expect(flags[0]! & fs.constants.O_APPEND).toBe(fs.constants.O_APPEND);
    expect(flags[0]! & fs.constants.O_TRUNC).toBe(0);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe("held by a different mesh writer");
  });

  it("removes daily partitions older than seven UTC days", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-retention-")); roots.push(root);
    const directory = path.join(root, "delivery-outcomes"); fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "20250101.jsonl"), "old\n");
    appendDeliveryOutcome(root, { eventId: "e", to: "t", from: "f", mode: "publish" }, "unknown", "late", Date.parse("2025-01-10T00:00:00Z"));
    expect(fs.existsSync(path.join(directory, "20250101.jsonl"))).toBe(false);
  });
});
