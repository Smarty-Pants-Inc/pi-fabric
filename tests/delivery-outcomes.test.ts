import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
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

  it("removes daily partitions older than seven UTC days", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-retention-")); roots.push(root);
    const directory = path.join(root, "delivery-outcomes"); fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "20250101.jsonl"), "old\n");
    appendDeliveryOutcome(root, { eventId: "e", to: "t", from: "f", mode: "publish" }, "unknown", "late", Date.parse("2025-01-10T00:00:00Z"));
    expect(fs.existsSync(path.join(directory, "20250101.jsonl"))).toBe(false);
  });
});
