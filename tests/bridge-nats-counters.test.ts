import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  NATS_LINK_COUNTER_NAMES,
  NATS_LINK_COUNTERS_FILE,
  openNatsLinkCounters,
  readNatsLinkCounters,
  type NatsLinkCounterName,
} from "../src/mesh/bridge-nats-counters.js";

const roots: string[] = [];
afterEach(() => {
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-link-counters-"));
  roots.push(root);
  return root;
};

describe("nats-link counters", () => {
  it("writes a snapshot at start, flushes at most once per interval, and on close", () => {
    vi.useFakeTimers();
    const dir = scratch();
    let clock = Date.parse("2026-10-25T06:00:00Z");
    const counters = openNatsLinkCounters(dir, "v2", { flushMs: 1_000, now: () => clock });
    const first = readNatsLinkCounters(dir)!;
    expect(first).toMatchObject({ v: 1, link: "v2", pid: process.pid, since: "2026-10-25T06:00:00.000Z" });
    expect(Object.values(first.counters).every((value) => value === 0)).toBe(true);
    // POSIX only: Windows has no owner-only file modes (nats-link runs only on Linux and macOS).
    if (process.platform !== "win32") expect(fs.statSync(path.join(dir, NATS_LINK_COUNTERS_FILE)).mode & 0o777).toBe(0o600);

    clock += 5;
    counters.add("republish.accepted");
    counters.add("republish.accepted");
    counters.add("relay.toNats", 3);
    // Nothing written before the interval.
    expect(readNatsLinkCounters(dir)!.counters["republish.accepted"]).toBe(0);
    vi.advanceTimersByTime(1_000);
    const flushed = readNatsLinkCounters(dir)!;
    expect(flushed.counters).toMatchObject({ "republish.accepted": 2, "relay.toNats": 3, "control.toNats": 0 });
    expect(flushed.last["republish.accepted"]).toBe("2026-10-25T06:00:00.005Z");

    counters.add("control.refused");
    counters.close();
    expect(readNatsLinkCounters(dir)!.counters["control.refused"]).toBe(1);
    counters.add("control.refused");
    expect(counters.get("control.refused")).toBe(1);
  });

  it("continues across restarts of the same link and starts over for another link", () => {
    const dir = scratch();
    const one = openNatsLinkCounters(dir, "v2", { now: () => Date.parse("2026-10-25T06:00:00Z") });
    one.add("republish.duplicate", 4);
    one.close();
    const two = openNatsLinkCounters(dir, "v2", { now: () => Date.parse("2026-10-25T07:00:00Z") });
    two.add("republish.duplicate");
    two.close();
    expect(readNatsLinkCounters(dir)).toMatchObject({ since: "2026-10-25T06:00:00.000Z", startedAt: "2026-10-25T07:00:00.000Z",
      counters: { "republish.duplicate": 5 } });
    const other = openNatsLinkCounters(dir, "v3");
    other.close();
    expect(readNatsLinkCounters(dir)).toMatchObject({ link: "v3", counters: { "republish.duplicate": 0 } });
  });

  it("refuses unknown names and bad amounts, and reads only a valid snapshot", () => {
    const dir = scratch();
    const counters = openNatsLinkCounters(dir, "v2");
    expect(() => counters.add("republish.everything" as NatsLinkCounterName)).toThrow(/unknown nats-link counter/);
    expect(() => counters.add("relay.toFile", -1)).toThrow(/non-negative/);
    counters.close();
    const file = path.join(dir, NATS_LINK_COUNTERS_FILE);
    for (const bad of ["{", "null", JSON.stringify({ v: 2 }),
      JSON.stringify({ ...readNatsLinkCounters(dir), counters: { "relay.toFile": -3 } })]) {
      fs.writeFileSync(file, bad);
      expect(readNatsLinkCounters(dir), bad).toBeUndefined();
    }
    expect(readNatsLinkCounters(path.join(dir, "absent"))).toBeUndefined();
    expect(NATS_LINK_COUNTER_NAMES).toContain("republish.accepted");
  });
});
