// P0 #10720: actual retained heap through public preview/store paths, not source assertions.
// This fixture has no filesystem cleanup/deletion operations.
import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  boundedPreviewValue, boundedResult, failedResultError,
  previewArgs, previewResult, truncateString,
} from "../../src/core/action-result.js";
import { headlineArg } from "../../src/core/call-preview.js";
import { detachedPreview } from "../../src/core/preview.js";
import { FabricActivityStore, MAX_ACTIVITY_PAYLOAD_BYTES } from "../../src/activity/store.js";

const PAYLOAD_CHARS = 4 * 1024 * 1024;
const COUNT = 100;
const mode = process.argv[2];
const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc;
assert.equal(typeof gc, "function", "run with --expose-gc");
const retained: unknown[] = [];
const refs: WeakRef<object>[] = [];
const store = new FabricActivityStore();
const view = store.createRunView();
const payload = (index: number): string => {
  // Flat, unique parents: neither repeat's ropes nor accidental deduplication
  // should make a sliced-string retention regression look green.
  const buffer = Buffer.alloc(PAYLOAD_CHARS, 97 + index % 26);
  buffer.write(`unique-${index}:`);
  // JSON's V8-owned string reproduces the runtime failure; an external
  // Buffer-decoded string can make V8 copy slices and hide the regression.
  return JSON.stringify(buffer.toString("utf8")).slice(1, -1);
};
const collect = async (): Promise<void> => {
  // WeakRef targets are kept until a turn ends. Fixed GC turns, no polling.
  for (let round = 0; round < 6; round++) { await nextTurn(); gc!(); }
  await nextTurn();
};
const captureLastArgs = (): void => {
  const call = view(true)[0]!.calls.at(-1)!;
  assert(call.args);
  refs.push(new WeakRef(call.args));
};
const accountPayloads = (): number => {
  let bytes = 0;
  for (const run of store.runs()) {
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
const append = (index: number): void => {
  const text = payload(index);
  switch (mode) {
    case "retained-slice-control":
      retained.push(text.slice(128, 224));
      break;
    case "action-string":
      retained.push(previewArgs("pi.read", { path: text }), previewResult(text));
      retained.push(truncateString(text, 96), headlineArg({ query: text }, 96));
      // Short slices must detach too, even when no ellipsis is needed.
      retained.push(detachedPreview(text.slice(128, 224), 128));
      retained.push(truncateString(text.slice(128, 224), 128));
      break;
    case "action-json": {
      const result = boundedResult({ text }, 400);
      assert.equal(result.chars, PAYLOAD_CHARS + 11);
      assert.equal(result.truncated, true);
      retained.push(result.value, boundedPreviewValue({ text }, 512));
      break;
    }
    case "action-nested-args":
      retained.push(previewArgs("pi.read", { nested: { text } }));
      break;
    case "action-nested-result":
      retained.push(previewResult({ nested: { text } }));
      break;
    case "action-failed":
      retained.push(failedResultError({ status: "failed", error: text }));
      break;
    case "activity-small":
      store.upsertItem("heap", { id: `item-${index}`, label: text, detail: text, data: { text } });
      store.event("heap", { message: text, data: { text } });
      view();
      break;
    case "activity-full":
      store.beginCall("heap", { callId: `call-${index}`, ref: "pi.read", args: { path: text } });
      store.finishCall("heap", `call-${index}`, {
        success: true, result: { output: text }, preview: { text },
      });
      view(true);
      break;
    case "activity-obsolete":
      store.beginCall("heap", { callId: `call-${index}`, ref: "pi.read", args: { path: text } });
      captureLastArgs();
      break;
    default: throw new Error(`Unknown probe: ${mode}`);
  }
};

// Warm imports, projection machinery and all measured paths before baseline.
store.start("heap");
append(-1);
retained.length = 0;
refs.length = 0;
store.reset();
store.start("heap");
view();
await collect();
const baseline = process.memoryUsage().heapUsed;
for (let index = 0; index < COUNT; index++) append(index);
let payloadBytes = 0;
if (mode === "activity-full" || mode === "activity-small") {
  payloadBytes = accountPayloads();
  assert(payloadBytes <= MAX_ACTIVITY_PAYLOAD_BYTES, `retained payload bytes: ${payloadBytes}`);
  assert.equal(store.get("heap")!.calls.length, mode === "activity-full" ? COUNT : 0);
}
if (mode === "activity-obsolete") {
  for (let index = 0; index < COUNT; index++) {
    store.updateCallArgs("heap", `call-${index}`, { path: `small-${index}` });
  }
  // Do NOT refresh this reader: a paused projection must release obsolete data.
}
await collect();
const heapDelta = process.memoryUsage().heapUsed - baseline;
const heapCap = (mode === "retained-slice-control" ? 512 : mode === "activity-full" ? 12 : 5) * 1024 * 1024;
assert(heapDelta < heapCap, `${mode}: retained ${heapDelta} bytes, cap ${heapCap}`);
if (mode === "retained-slice-control") {
  assert(heapDelta > 100 * 1024 * 1024, `negative control did not retain its parents: ${heapDelta}`);
}
if (mode === "activity-obsolete") {
  assert.equal(refs.filter(ref => ref.deref() !== undefined).length, 0, "paused reader retained replaced args");
  const captureRun = (): WeakRef<object> => new WeakRef(view(true)[0]!);
  const oldRun = captureRun();
  store.reset();
  await collect();
  assert.equal(oldRun.deref(), undefined, "paused reader retained reset run");
  assert.deepEqual(view(true), []);
}
// Keep the measured roots demonstrably live through the measurement.
assert.equal(mode?.startsWith("action-") || mode === "retained-slice-control" ? retained.length > 0 : store.revision() > 0, true);
console.log(JSON.stringify({ mode, count: COUNT, payloadChars: PAYLOAD_CHARS, heapDelta, heapCap, payloadBytes }));
