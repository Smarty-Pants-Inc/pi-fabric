// Golden mesh event lines (smarty-dev#6729): the same publications through publish, publishBatch
// and the bridge side, normalized only in the per-run id and createdAt. Used by
// tests/mesh-publish-batch-hold.test.ts; the golden there was produced by main (200e021d) before
// the envelope was encoded outside the lock.
import fs from "node:fs";
import path from "node:path";

const from = { id: "session:golden", name: "gold \u00e9", kind: "main", sessionId: "s-1" };
const remote = { id: "session:remote-golden", name: "far", kind: "main" };
const bridgeEvent = (id, extra = {}) => ({ topic: "fleet.work.batch", kind: "ask", from: remote, to: "session:local-golden",
  text: "bridged " + id, data: { body: "\u00fc\u2713 \"q\" \\ \u2028", n: [1, 2.5, null], bridge: { from: "untrusted", id } }, ...extra });

export const normalizeLine = line => line
  .replace(/^\{"id":"[0-9a-f-]{36}"/, '{"id":"<id>"')
  .replace(/"createdAt":\d+/g, '"createdAt":0');

export const publishGoldenEvents = async ({ MeshStore, StoreBridgeSide }, root) => {
  const mesh = new MeshStore(root, 65536, 500);
  await mesh.publish({ topic: "golden.plain", from, text: "hello" });
  await mesh.publish({ topic: "golden.keyed", kind: "  note  ", from, dedupeKey: "golden-key-\u00fc", to: "session:x",
    text: "\u00fcn\u00efc\u00f8d\u00e9 \u2713 \u2028 \"q\" \\ \u{1F600}", data: { z: 1, a: [1, "two", { nested: null }], "\u00e9": "x" },
    principal: { id: "p-1", binding: "voice-call" }, durable: true });
  await mesh.publish({ topic: "golden.bridgeish", from, data: { bridge: { from: "old", id: "b-1" }, body: "x" } });
  await mesh.publish({ topic: "golden.stamp", kind: "", from, data: createdAt => ({ createdAt, s: "stamp" }) });
  await mesh.publish({ topic: "golden.null", from, text: "", data: null });
  // A slow disk can end a batch at its 50 ms work bound: publish the rest, as the bridge does.
  const batch = [
    { topic: "golden.batch", from, text: "one", data: { k: "v" } },
    { topic: "golden.batch", from, dedupeKey: "golden-batch-key", text: "two", data: [1, { "\u00e9": "\u00e8" }], durable: true },
    { topic: "golden.batch", from: { ...from, verified: "bridge" }, text: "three", principal: { id: "p-2", binding: "herdr-client" } },
    { topic: "golden.batch", from, data: createdAt => ({ k: "stamp", createdAt }) },
  ];
  for (let index = 0; index < batch.length;) index += (await mesh.publishBatch(batch.slice(index))).length;
  const side = new StoreBridgeSide(mesh, "forge");
  side.holds = () => true;
  const bridged = [{ event: bridgeEvent("b-2"), held: [remote.id] }, { event: bridgeEvent("b-3", { principal: { id: "p-3", binding: "org-agent" } }) }];
  for (let index = 0; index < bridged.length;) index += (await side.publishBatch(bridged.slice(index))).length;
  await side.publish(bridgeEvent("b-4"), [remote.id]);
  return fs.readFileSync(path.join(root, "events.jsonl"), "utf8").split("\n").filter(Boolean);
};
