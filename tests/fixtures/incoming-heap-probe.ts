import { registerIncomingMessageRenderers } from "../../src/ui/incoming-messages.js";

type Renderer = (message: unknown, options: { expanded: boolean; outputPad: number }, theme: unknown) =>
  { render?: (width: number) => string[] } | undefined;

const HISTORY_COUNT = 100;
const PARENT_BYTES = 4 * 1024 * 1024;
const RETAINED_CAP_BYTES = 5 * 1024 * 1024;
const CONTROL_FLOOR_BYTES = 350 * 1024 * 1024;
const theme = { fg: (_name: string, text: string) => text };
const agentEntry = (id: string, key: string, payload = "") => ({
  type: "custom_message", id, customType: "pi-fabric-agent-message", content: payload,
  details: { items: [{ from: { id: "peer" }, data: { key, topic: "heap.probe" } }] },
});
const inbox = (id: string, key: string) => ({
  role: "custom", customType: "pi-fabric-inbox", display: true, timestamp: 1,
  content: `<event id="${id}" from_id="peer" from_name="Peer" key="${key}" topic="heap.probe">${key}</event>`,
  details: {},
});
const historyId = (index: number): string => `history-${String(index).padStart(24, "0")}`;

// Each independent ASCII JSON record is exactly 4 MiB, owned by V8's JSON.stringify.
// Do not substitute Buffer.toString(): external strings can make this falsely green.
const ownedFrame = (index: number): string => {
  const id = historyId(index);
  const overhead = JSON.stringify({ id, largeMessage: "" }).length;
  return JSON.stringify({ id, largeMessage: "x".repeat(PARENT_BYTES - overhead) });
};
const slicedId = (parent: string): string => parent.slice(7, 7 + 32); // {"id":"...

const gc = globalThis.gc;
if (typeof gc !== "function" || process.release.name !== "node" || !process.versions.v8 || process.versions.bun) {
  throw new Error("probe requires actual Node/V8 with --expose-gc");
}
const collect = async (): Promise<void> => {
  // WeakRefs are kept alive until the current job ends. Cross a native event-loop
  // boundary before each forced collection; never dereference between these turns.
  for (let attempt = 0; attempt < 12; attempt++) {
    await new Promise<void>(resolve => setImmediate(resolve));
    gc();
  }
};

export const runIncomingHeapProbe = () => {
  let branch: unknown[] = [];
  let headerVersion = 3;
  let currentSessionId = "heap-probe";
  let renderer: Renderer | undefined;
  let start: ((event: unknown, context: unknown) => void) | undefined;
  registerIncomingMessageRenderers({
    on: (_name: string, callback: (event: unknown, context: unknown) => void) => { start = callback; },
    registerMessageRenderer: (type: string, callback: Renderer) => {
      if (type === "pi-fabric-inbox") renderer = callback;
    },
  } as never, () => "collapsed");
  start!({}, { sessionManager: {
    getBranch: () => branch,
    getHeader: () => ({ version: headerVersion }),
    getSessionId: () => currentSessionId,
  } });
  const render = (message: unknown): string[] => renderer!(message, { expanded: false, outputPad: 0 }, theme)?.render?.(200) ?? [];

  const first = agentEntry("entry-1", "old");
  branch = [first];
  render(inbox("old-copy", "old"));
  const oldSuppressed = render(inbox("old-copy-2", "old")).length === 0;

  const appended = agentEntry("entry-2", "new");
  branch = [first, appended];
  const appendSuppressed = render(inbox("new-copy", "new")).length === 0;

  const replacement = agentEntry("replacement", "replacement");
  branch = [replacement, appended]; // Same length/last ID, changed earlier prefix.
  const sameLengthResetShown = render(inbox("old-after-fork", "old")).length > 0;
  branch = [agentEntry("replacement", "replacement"), agentEntry("entry-2", "new")];
  const stableIdsSuppress = render(inbox("new-after-reload", "new")).length === 0;
  branch = [];
  const shrinkResetShown = render(inbox("new-after-shrink", "new")).length > 0;

  branch = [agentEntry("reused-id", "before-version")];
  render(inbox("before-version-copy", "before-version"));
  headerVersion++;
  branch = [agentEntry("reused-id", "after-version")];
  const versionResetShown = render(inbox("before-version-visible", "before-version")).length > 0;
  currentSessionId = "next-session";
  branch = [agentEntry("reused-id", "after-session")];
  const sessionResetShown = render(inbox("after-version-visible", "after-version")).length > 0;

  return {
    oldSuppressed, appendSuppressed, sameLengthResetShown,
    stableIdsSuppress, shrinkResetShown, versionResetShown, sessionResetShown,
    renderer,
    populateHistory: () => {
      // Keep these owners in a separate synchronous frame, gone before collecting.
      let history = Array.from({ length: HISTORY_COUNT }, (_, index) => {
        const parent = ownedFrame(index);
        return agentEntry(slicedId(parent), `history-${index}`, parent);
      });
      const slicedIdProbe = history.every((entry, index) =>
        entry.content.length === PARENT_BYTES && entry.id.length === 32 && entry.id === historyId(index));
      branch = history;
      render(inbox("history-probe", "not-delivered")); // Actual registered inbox/index path.
      const historySuppressed = render(inbox("history-delivered-copy", "history-99")).length === 0;
      const references = [new WeakRef(history), ...history.map(entry => new WeakRef(entry))];
      // NO subsequent index update: resetting via render([]) would hide #seen's leak.
      branch = [];
      history = [];
      return { references, slicedIdProbe, historySuppressed };
    },
    showAfterGc: () => render(inbox("after-release", "not-delivered")),
  };
};

const main = async (): Promise<void> => {
  if (process.argv[2] === "--slice-control") {
    await collect();
    const before = process.memoryUsage().heapUsed;
    const holders: string[] = [];
    for (let index = 0; index < HISTORY_COUNT; index++) {
      // The same exact parents/ID slices as the real index probe, deliberately NOT detached.
      holders.push(slicedId(ownedFrame(index)));
    }
    await collect();
    const delta = process.memoryUsage().heapUsed - before;
    process.stdout.write(JSON.stringify({
      runtime: "node-v8", delta, held: holders.length, parentBytes: PARENT_BYTES,
      uniqueIds: new Set(holders).size === HISTORY_COUNT,
      slicedIdProbe: holders.every((id, index) => id.length === 32 && id === historyId(index)),
      retainsParents: delta > CONTROL_FLOOR_BYTES,
    }) + "\n");
    return;
  }

  // Warm real registration, rendering and behavior before recording the retained delta.
  const result = runIncomingHeapProbe();
  await collect();
  const before = process.memoryUsage().heapUsed;
  const history = result.populateHistory();
  await collect();
  const retainedBytes = process.memoryUsage().heapUsed - before;
  // Measure BEFORE calling the renderer again; the populated index stays live throughout.
  const branchCollected = history.references[0]!.deref() === undefined;
  const entriesCollected = history.references.slice(1).every(reference => reference.deref() === undefined);
  process.stdout.write(JSON.stringify({
    runtime: "node-v8", historyCount: HISTORY_COUNT, parentBytes: PARENT_BYTES,
    retainedCapBytes: RETAINED_CAP_BYTES, retainedBytes,
    heapUnderCap: retainedBytes < RETAINED_CAP_BYTES,
    oldSuppressed: result.oldSuppressed,
    appendSuppressed: result.appendSuppressed,
    sameLengthResetShown: result.sameLengthResetShown,
    stableIdsSuppress: result.stableIdsSuppress,
    shrinkResetShown: result.shrinkResetShown,
    versionResetShown: result.versionResetShown,
    sessionResetShown: result.sessionResetShown,
    slicedIdProbe: history.slicedIdProbe,
    historySuppressed: history.historySuppressed,
    weakRefCount: history.references.length,
    branchCollected, entriesCollected,
    collected: branchCollected && entriesCollected,
    rendererLive: typeof result.renderer === "function",
    usableAfterGc: result.showAfterGc().length > 0,
  }) + "\n");
};

await main();
