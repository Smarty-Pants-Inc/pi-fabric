import { parentPort, workerData } from "node:worker_threads";
import { serveBridgeAgentOnPort, type BridgeAgentWorkerData } from "./bridge-agent.js";
import { StoreBridgeSide } from "./bridge.js";
import { MeshStore } from "./store.js";

if (!parentPort) throw new Error("Bridge agent worker requires a parent port");
const options = workerData as BridgeAgentWorkerData;
const store = new MeshStore(options.root, options.maxEventBytes, options.maxReadEvents, options.storeOptions);
let side: StoreBridgeSide;
try { side = new StoreBridgeSide(store, options.peer, Date.now, true); }
catch (error) { store.closeState(); parentPort.close(); throw error; }
await serveBridgeAgentOnPort(side, parentPort,
  options.debug ? message => { process.stderr.write(`mesh-bridge: ${message}\n`); } : undefined);
