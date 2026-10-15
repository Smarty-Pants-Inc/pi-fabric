import { mkdirSync, mkdtempSync } from "node:fs";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { beforeAll, describe, expect, it } from "vitest";
import { FabricActivityStore, MAX_ACTIVITY_PAYLOAD_BYTES } from "../src/activity/store.js";
import type { FabricActivityRun } from "../src/activity/types.js";

// Keep reason: P0 #10720, retained provider payloads and idle projection caches.
const repo = fileURLToPath(new URL("../", import.meta.url));
let probe: string;
beforeAll(() => {
  const output = path.join(repo, ".local", "out");
  mkdirSync(output, { recursive: true });
  const directory = mkdtempSync(path.join(output, "activity-preview-heap-"));
  probe = path.join(directory, "probe.mjs");
  buildSync({
    entryPoints: [fileURLToPath(new URL("./fixtures/preview-heap-probe.ts", import.meta.url))],
    outfile: probe, bundle: true, platform: "node", format: "esm", packages: "external",
  });
  // No deletion: preserve the child fixture/evidence for parent-hosted cleanup.
}, 15_000);

const payloadBytes = (runs: FabricActivityRun[]): number => {
  let bytes = 0;
  for (const run of runs) {
    for (const row of [...run.calls, ...run.items, ...run.events]) {
      const record = row as unknown as Record<string, unknown>;
      for (const key of ["args", "result", "preview", "data"]) {
        if (record[key] === undefined) continue;
        const json = JSON.stringify(record[key]);
        bytes += Math.max(json.length * 2, Buffer.byteLength(json, "utf8"));
      }
    }
  }
  return bytes;
};

describe("activity global payload budget (#10720)", () => {
  it("bounds all runs together, evicts oldest heavy payloads, and preserves call outcomes", () => {
    const store = new FabricActivityStore();
    const read = store.createRunView();
    for (let runIndex = 0; runIndex < 4; runIndex++) {
      const runId = `run-${runIndex}`;
      store.start(runId, { name: `Run ${runIndex}` });
      for (let index = 0; index < 10; index++) {
        const callId = `call-${index}`;
        store.beginCall(runId, { callId, ref: "agents.run", args: { task: `Task ${index}`, blob: "界".repeat(19_000) } });
        store.finishCall(runId, callId, {
          success: index !== 0,
          result: { id: `agent-${runIndex}-${index}`, status: index === 0 ? "failed" : "completed", error: index === 0 ? "boom" : undefined,
            output: "界".repeat(19_000), usage: { input: 2, output: 3, cost: 0.01 }, toolCalls: 7 },
          preview: { blob: "界".repeat(19_000) },
        });
        read(true);
      }
    }
    const runs = store.runs();
    expect(payloadBytes(runs)).toBeLessThanOrEqual(MAX_ACTIVITY_PAYLOAD_BYTES);
    expect(runs.reduce((count, run) => count + run.calls.length, 0)).toBe(40);
    const oldest = store.get("run-0")!.calls[0]!;
    expect(oldest).not.toHaveProperty("args");
    expect(oldest).not.toHaveProperty("result");
    expect(oldest).not.toHaveProperty("preview");
    expect(oldest).toMatchObject({ id: "call-0", label: "agents.run · Task 0", status: "failed", error: "boom",
      entityId: "agent-0-0", entityKind: "agent", metrics: { tokens: 5, toolCalls: 7, cost: 0.01 } });
    const newest = store.get("run-3")!.calls.at(-1)!;
    expect(newest.args).toBeDefined();
    expect(newest.result).toBeDefined();
    expect(newest.preview).toBeDefined();
    expect(newest.detail).toBe("界".repeat(120));
    expect(read(true)).toEqual(store.runs());
    expect(read()).toEqual(store.runSummaries());
  });

  it("shares the budget with item/event data rather than only limiting call count", () => {
    const store = new FabricActivityStore();
    store.start("data");
    store.beginCall("data", { callId: "old", ref: "pi.read", args: { blob: "a".repeat(60_000) } });
    for (let index = 0; index < 200; index++) {
      store.upsertItem("data", { id: `item-${index}`, label: `Item ${index}`, data: { blob: "b".repeat(7_000) } });
      store.event("data", { message: `Event ${index}`, data: { blob: "c".repeat(7_000) } });
    }
    const run = store.get("data")!;
    expect(payloadBytes([run])).toBeLessThanOrEqual(MAX_ACTIVITY_PAYLOAD_BYTES);
    expect(run.calls[0]).not.toHaveProperty("args");
    expect(run.items).toHaveLength(200);
    expect(run.events).toHaveLength(200);
    expect(run.items[0]).toMatchObject({ id: "item-0", label: "Item 0", status: "running" });
    expect(run.items[0]).not.toHaveProperty("data");
    expect(run.events[0]).not.toHaveProperty("data");
    expect(run.items.at(-1)?.data).toEqual({ blob: "b".repeat(7_000) });
    expect(run.events.at(-1)?.data).toEqual({ blob: "c".repeat(7_000) });
  });

  it("releases accounting on replacement/reset and does not accumulate repeated updates", () => {
    const store = new FabricActivityStore();
    const fill = (): void => {
      store.start("replace");
      for (let index = 0; index < 10; index++) {
        store.beginCall("replace", { callId: `call-${index}`, ref: "pi.read", args: { blob: "x".repeat(60_000) } });
      }
    };
    fill();
    for (let index = 0; index < 30; index++) store.updateCallArgs("replace", "call-0", { blob: "y".repeat(60_000) });
    expect(store.get("replace")!.calls.every(call => call.args !== undefined)).toBe(true);
    fill();
    expect(store.get("replace")!.calls.every(call => call.args !== undefined)).toBe(true);
    store.reset();
    fill();
    expect(store.get("replace")!.calls.every(call => call.args !== undefined)).toBe(true);
    expect(payloadBytes(store.runs())).toBeLessThanOrEqual(MAX_ACTIVITY_PAYLOAD_BYTES);
  });

  it("keeps exact native JSON behavior under budget, including bigint and hooks", () => {
    const store = new FabricActivityStore();
    store.start("exact");
    const value = { small: [undefined, NaN, "\ud800😀"], big: 12n, wrapped: new String("small") };
    const expected = JSON.parse(JSON.stringify(value, (_key, nested) => typeof nested === "bigint" ? String(nested) : nested)) as unknown;
    store.beginCall("exact", { callId: "call", ref: "pi.read", args: { path: "/tmp/small" } });
    store.finishCall("exact", "call", { success: true, result: value });
    expect(store.get("exact")!.calls[0]!.result).toEqual(expected);
    let calls = 0;
    store.upsertItem("exact", { id: "hook", label: "Hook", data: { toJSON() { calls++; return undefined; } } });
    expect(calls).toBe(1);
    expect(store.get("exact")!.items[0]).not.toHaveProperty("data");
    const text = "\ud800😀\\\"".repeat(25_000);
    const json = JSON.stringify({ text });
    store.finishCall("exact", "call", { success: true, result: { text } });
    expect(store.get("exact")!.calls[0]!.result).toEqual({
      fabricTruncated: true, originalChars: json.length, preview: json.slice(0, 63_900),
    });
  });

  it("drops old detailed previews on call restart and tracks payload eviction in reader views", () => {
    const store = new FabricActivityStore();
    store.start("live");
    store.beginCall("live", { callId: "call", ref: "pi.read", args: { path: "old" } });
    store.finishCall("live", "call", { success: true, result: { output: "old result" }, preview: { text: "old preview" } });
    const read = store.createRunView();
    const old = read(true)[0]!.calls[0]!;
    store.beginCall("live", { callId: "call", ref: "pi.read", args: { path: "new" } });
    const current = read(true)[0]!.calls[0]!;
    expect(current.args).toEqual({ path: "new" });
    expect(current).not.toHaveProperty("result");
    expect(current).not.toHaveProperty("preview");
    expect(old.preview).toEqual({ text: "old preview" });
    expect(read(true)).toEqual(store.runs());
  });

  it.each(["activity-small", "activity-full", "activity-obsolete"])(
    "%s bounds 100 unique 4 MiB inputs and releases obsolete projection values", mode => {
      const child = spawnSync(process.execPath, ["--expose-gc", probe, mode], {
        cwd: repo, encoding: "utf8", timeout: 14_000, maxBuffer: 1024 * 1024,
      });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr || child.stdout).toBe(0);
      const receipt = JSON.parse(child.stdout) as { count: number; heapDelta: number; heapCap: number; payloadBytes: number };
      expect(receipt.count).toBe(100);
      expect(receipt.heapDelta).toBeLessThan(receipt.heapCap);
      expect(receipt.payloadBytes).toBeLessThanOrEqual(MAX_ACTIVITY_PAYLOAD_BYTES);
      console.info("activity heap", receipt);
    }, 15_000,
  );
});
