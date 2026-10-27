// Keep: the recorded aggregate-renewal incident loses underlying codes at the link logger.
// Exercise real renewal failures over fake KV, plus bounded/redacted diagnostics and minute windows.
import { afterEach, describe, expect, it, vi } from "vitest";
import { natsLinkPortErrorLogger } from "../src/mesh/bridge-nats-link.js";
import { NatsKvMirror, NatsPresenceReader } from "../src/mesh/bridge-nats-presence.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";

const MESSAGE = "NATS bridge mirror renewal failed";
const aggregate = (...errors: unknown[]) => new AggregateError(errors, MESSAGE);
const coded = (code: unknown) => Object.assign(new Error("private message /secret/path participant:private"), { code });
const codes = (line: string) => JSON.parse(line.match(/codes=(\{.*\})/)![1]!);
const summaries = (lines: string[]) => lines.filter(line => line.startsWith("NATS renewal summary "));
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("link renewal failure diagnostics", () => {
  it("carries code/class counts without values, ids, messages or stacks", () => {
    const lines: string[] = [];
    const report = natsLinkPortErrorLogger(line => lines.push(line), () => 0);
    report(aggregate(coded("KV_TIMEOUT"), coded("KV_TIMEOUT"), coded("CAS_CONFLICT"), new TypeError("private"), coded(10071)));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain(`NATS port: ${MESSAGE}`);
    expect(codes(lines[0]!)).toEqual({ KV_TIMEOUT: 2, CAS_CONFLICT: 1, TypeError: 1, "10071": 1 });
    expect(lines[0]).toContain("omittedErrors=0");
    expect(lines.join("\n")).not.toMatch(/private|secret|participant:|stack/);
  });

  it("rate limits aggregate-only summaries, includes the triggering failure, and resets", () => {
    const lines: string[] = [];
    let now = 500;
    const report = natsLinkPortErrorLogger(line => lines.push(line), () => now);
    report(aggregate(coded("KV_TIMEOUT"), coded("KV_TIMEOUT")));
    now += 59_999;
    report(aggregate(coded("CAS_CONFLICT")));
    expect(summaries(lines)).toEqual([]);
    now++;
    report(aggregate(coded("KV_TIMEOUT")));
    expect(summaries(lines)).toHaveLength(1);
    expect(summaries(lines)[0]).toContain("direction=toKV scope=aggregate-only windowMs=60000 aggregateFailures=3");
    expect(codes(summaries(lines)[0]!)).toEqual({ KV_TIMEOUT: 3, CAS_CONFLICT: 1 });
    expect(summaries(lines)[0]).not.toMatch(/attempts=|successes=|maxElapsedMs=|minRemainingAuthorityMs=/);
    report(aggregate(coded("CAS_CONFLICT")));
    now += 60_000;
    report(aggregate(coded("CAS_CONFLICT")));
    expect(summaries(lines)).toHaveLength(2);
    expect(summaries(lines)[1]).toContain("aggregateFailures=2");
    expect(codes(summaries(lines)[1]!)).toEqual({ CAS_CONFLICT: 2 });
  });

  it("bounds detail and summary cardinality and inspected children", () => {
    const lines: string[] = [];
    let now = 0;
    const report = natsLinkPortErrorLogger(line => lines.push(line), () => now);
    const errors = Array.from({ length: 1_000 }, (_, i) => coded(`CODE_${i}`));
    report(aggregate(...errors));
    expect(Object.keys(codes(lines[0]!))).toHaveLength(17);
    expect(codes(lines[0]!).OTHER_ERRORS).toBe(112);
    expect(Object.values(codes(lines[0]!)).reduce<number>((sum, count) => sum + Number(count), 0)).toBe(128);
    expect(lines[0]).toContain("omittedErrors=872");
    now = 60_000;
    report(aggregate(...errors));
    const summary = summaries(lines)[0]!;
    expect(Object.keys(codes(summary))).toHaveLength(17);
    expect(codes(summary).OTHER_ERRORS).toBe(224);
    expect(summary).toContain("omittedErrors=1744");
    expect(summary.length).toBeLessThan(2_000);
  });

  it("rejects unsafe/oversized tokens and tolerates throwing accessors and non-errors", () => {
    const lines: string[] = [];
    const report = natsLinkPortErrorLogger(line => lines.push(line), () => 0);
    const throwing = Object.defineProperty({}, "code", { get() { throw new Error("private"); } });
    const polluted = Object.assign(Object.create(null), { code: "__proto__" });
    report(aggregate(coded("private/path\nforged"), coded("x".repeat(65)),
      { code: "bad:value", name: "Private\nmessage" }, throwing, null, "private", polluted));
    expect(codes(lines[0]!)).toEqual({ Error: 2, UNKNOWN_ERROR: 5 });
    expect(lines.join("\n")).not.toMatch(/private|Private|forged|proto|xxxxx/);
  });

  it("leaves unrelated errors alone and never assigns them to renewal", () => {
    const lines: string[] = [];
    let now = 0;
    const report = natsLinkPortErrorLogger(line => lines.push(line), () => now);
    report(aggregate(coded("KV_TIMEOUT")));
    now = 60_000;
    report(new Error("unrelated watch failure"));
    report(new AggregateError([coded("OTHER")], "unrelated aggregate"));
    report("unrelated string");
    expect(lines.slice(1)).toEqual(["NATS port: unrelated watch failure", "NATS port: unrelated aggregate", "NATS port: unrelated string"]);
    expect(summaries(lines)).toEqual([]);
    report(aggregate(coded("CAS_CONFLICT")));
    expect(summaries(lines)[0]).toContain("aggregateFailures=2");
    expect(codes(summaries(lines)[0]!)).toEqual({ KV_TIMEOUT: 1, CAS_CONFLICT: 1 });
  });

  it("preserves codes from the real mirror timer's multi-failure aggregate", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const world = kvWorld();
    const base = world.transport("bridge-machine", "bridge-host");
    const leases = base.kv("leases"), presence = base.kv("presence");
    const transport = { ...base, kv: (bucket: "presence" | "leases") => bucket === "leases" ? leases : presence };
    const lines: string[] = [];
    const report = natsLinkPortErrorLogger(line => lines.push(line), Date.now);
    const reader = new NatsPresenceReader({ transport, now: Date.now, monotonic: Date.now });
    const mirror = new NatsKvMirror({ transport, reader, now: Date.now, monotonic: Date.now, onError: report,
      processProof: { pid: 42, pidStartTime: "420", bootId: "test-boot" } });
    try {
      reader.start();
      await reader.whenReady();
      const roots = ["session:root-a", "session:root-b"].map(id => ({ record: {
        format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
        ownerIncarnation: "inc-1", name: id, status: "running", runner: "pi", transport: "host",
        capabilities: ["steer"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now(),
      } satisfies FabricParticipantRecord, until: Infinity }));
      expect((await mirror.replace("roots", roots, true)).refused).toEqual([]);
      vi.spyOn(leases, "put").mockImplementation(async () => { throw coded("KV_TIMEOUT"); });
      await vi.advanceTimersByTimeAsync(5_000);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toContain(MESSAGE);
      expect(codes(lines[0]!)).toEqual({ KV_TIMEOUT: 2 });
      // Diagnostics do not delete claims or extend their liveness after the last successful write.
      expect(mirror.holds("session:root-a")).toBe(true);
      await vi.advanceTimersByTimeAsync(8_001);
      expect(mirror.holds("session:root-a")).toBe(false);
    } finally { await mirror.fence(); await reader.close(); }
  });
});
