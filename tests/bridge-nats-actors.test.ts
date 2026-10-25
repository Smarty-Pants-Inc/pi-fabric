// L1 (smarty-dev#15310, N1): which file actors the A' bridge may hold in NATS presence. Only native,
// live, v1 actors with an owner incarnation are listed; the listing is read-only.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createFileActorDirectory, readFileActorsForMirror, type NatsFileActorDirectory } from "../src/mesh/bridge-nats-actors.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantInfo, FabricParticipantListOptions } from "../src/topology/types.js";

const ROOT = "session:11111111-1111-4111-8111-111111111111";
const temps: string[] = [];
afterEach(() => { for (const dir of temps.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

function actor(overrides: Partial<FabricParticipantInfo> = {}): FabricParticipantInfo {
  return { format: 1, id: "0123456789abcdef0123456789abcdef", kind: "actor", rootId: ROOT, ownerHostId: "resident:0123456789abcdef01234567",
    ownerIdentityId: ROOT, ownerIncarnation: "inc-1", parentId: ROOT, name: "fleet-supervisor", status: "idle", runner: "pi",
    transport: "host", capabilities: ["steer", "followUp"], controlProtocol: "v1", startedAt: 1, updatedAt: 2,
    local: false, stale: false, ...overrides };
}

describe("readFileActorsForMirror", () => {
  it("asks for a fresh project-wide actor listing and keeps only native, live, v1 actors", () => {
    const calls: Array<{ options?: FabricParticipantListOptions; now?: number }> = [];
    const keep = actor();
    const directory: NatsFileActorDirectory = {
      list(options, now) {
        calls.push({ ...(options ? { options } : {}), ...(now !== undefined ? { now } : {}) });
        return [
          keep,
          actor({ id: "stale", stale: true }),
          actor({ id: "bridge-stale", bridgeStale: true }),
          actor({ id: "mirrored", remoteHost: "spoke" }),
          actor({ id: "relayed", remoteHost: "spoke", bridgeVia: "hub" }),
          actor({ id: "nats-view", machine: "v2-org" }),
          actor({ id: "stopped", status: "stopped" }),
          actor({ id: "legacy", controlProtocol: "legacy" }),
          (({ ownerIncarnation: _incarnation, ...rest }) => rest)(actor({ id: "no-incarnation" })),
          { ...actor({ id: "a-root" }), kind: "root" },
        ];
      },
    };
    const read = readFileActorsForMirror(directory, () => 5_000);
    expect(calls).toEqual([{ options: { scope: "project", kinds: ["actor"], fresh: true }, now: 5_000 }]);
    expect(read.sampledAt).toBe(5_000);
    const { local: _local, stale: _stale, ...record } = keep;
    expect(read.actors).toEqual([record]);
    expect(read.actors[0]).not.toHaveProperty("local");
  });

  it("lets a listing failure propagate: unreadable is not absence", () => {
    const directory: NatsFileActorDirectory = { list() { throw new Error("mesh busy"); } };
    expect(() => readFileActorsForMirror(directory)).toThrow("mesh busy");
  });
});

describe("createFileActorDirectory", () => {
  it("lists a real mesh read-only without publishing the bridge as a participant", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-actors-"));
    temps.push(dir);
    const store = new MeshStore(path.join(dir, "mesh"), 65_536, 1_000);
    const directory = await createFileActorDirectory(store, "v2");
    expect(readFileActorsForMirror(directory).actors).toEqual([]);
    const participants = path.join(dir, "mesh", "participants");
    expect(fs.existsSync(participants) ? fs.readdirSync(participants) : []).toEqual([]);
    expect(store.listAll("topology/")).toEqual([]);
  });
});
