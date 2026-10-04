// A separate process leaves a visible consumption receipt after rejecting its namespace barrier.
import fs from "node:fs";
import path from "node:path";
import { ResidencyClient } from "../../src/residency/client.js";
import { MeshStore } from "../../src/mesh/store.js";
import type { ResidentHostConfig } from "../../src/residency/protocol.js";
import type { FabricParticipantSource } from "../../src/topology/types.js";
import type { FabricMainAgentTarget } from "../../src/main-agent.js";
const config = JSON.parse(fs.readFileSync(process.argv[2]!, "utf8")) as ResidentHostConfig, id = process.argv[3]!;
const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
fs.openSync = ((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; }) as typeof fs.openSync;
fs.fsyncSync = fd => { if (files.get(fd) === path.join(config.residencyRoot, "agents")) throw new Error("consumption namespace unavailable"); sync(fd); };
const client = new ResidencyClient({ config, mesh: new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents),
  // waitAgent's saved-result/metadata path does not use these services; no host is started.
  participants: {} as FabricParticipantSource, mainAgent: { local: false } as FabricMainAgentTarget });
try { await client.waitAgent(id); throw new Error("expected consumption failure"); }
catch (error) { if (!(error instanceof Error) || error.message !== "consumption namespace unavailable") throw error; console.log(error.message); }
await client.close();
