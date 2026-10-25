import { parentPort, threadId, workerData } from "node:worker_threads";
import { MeshStore } from "../../src/mesh/store.js";
import { StoreBridgeSide } from "../../src/mesh/bridge.js";
import { serveBridgeAgentOnPort } from "../../src/mesh/bridge-agent.js";

if (!parentPort) throw new Error("Tail admission fixture requires a worker port");
const control = new Int32Array(workerData.control);
// The real worker blocks only after the requested operation enters its service.
// Shared-memory entry/release makes the test independent of IPC scheduling speed.
const gate = index => {
  if (Atomics.compareExchange(control, index, 1, 2) !== 1) return;
  parentPort.postMessage(JSON.stringify({ notification: "fixture-gate", kind: String(index) }));
  while (Atomics.load(control, index) === 2) Atomics.wait(control, index, 2);
  Atomics.store(control, index, 0);
};
const store = new MeshStore(workerData.root, 64 * 1024, 1, { stateBackend: "sqlite" });
const side = new StoreBridgeSide(store, "hub", () => Date.now(), true);
for (const text of ["page-one", "page-two"]) {
  await store.publish({ topic: "fleet.work.test", kind: "probe", to: "hub", text, durable: true,
    from: { id: "agent:tail-admission", name: "tail admission", kind: "agent" } });
}
if (workerData.failHello) {
  const prepare = store.prepareLock.bind(store);
  let fail = true;
  store.prepareLock = async (...args) => {
    if (fail) { fail = false; throw new Error("Injected first hello preparation failure"); }
    return prepare(...args);
  };
}
const tail = side.tail.bind(side);
side.tail = async (...args) => {
  const ordinal = Atomics.add(control, 2, 1) + 1;
  gate(0);
  if (workerData.failTail) throw new Error("Injected worker tail failure");
  return { ...await tail(...args), threadId, ordinal };
};
const presence = side.presence.bind(side);
side.presence = async (...args) => { gate(1); return presence(...args); };
await serveBridgeAgentOnPort(side, parentPort);
