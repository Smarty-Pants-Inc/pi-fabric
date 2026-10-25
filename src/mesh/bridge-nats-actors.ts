// A' bridge, file actors into NATS presence (smarty-dev#15310, lane L1, N1).
//
// Ryzen 1 actors (supervisors, review-astra) stay on the file mesh; an actor's owner runtime never
// crosses (APRIME2 fact 3). For a v2 Main to address one, or to verify one as a sender, the link
// holds an allow-listed actor's presence row (plus its owner host's lease) in KV under the bridge
// machine. This module only READS the hub's participant directory: the live, native, v1 actors it
// lists right now. NatsBridgePresence.mirrorActors applies the allow-list, bounds each row's
// authority by that read (or its owner host's sample) and writes through the shared KV mirror.
import type { MeshStore } from "./store.js";
import type { FabricParticipantInfo, FabricParticipantListOptions, FabricParticipantRecord } from "../topology/types.js";
// Static: loaded only behind the lazy bridge-nats-link entry (the build forbids unstable dynamic chunks).
import { ParticipantDirectory } from "../topology/participant-directory.js";

// Same set as participant-view KV_TERMINAL.
const TERMINAL = new Set(["completed", "failed", "cancelled", "stopped", "timed_out", "done", "aborted"]);

/** The read side of a file participant directory (ParticipantDirectory.list). */
export interface NatsFileActorDirectory {
  list(options?: FabricParticipantListOptions, now?: number): FabricParticipantInfo[];
}

export interface NatsFileActorRead {
  /** Native, live, v1 actors with an owner incarnation, as listed (allow-listing happens at mirror time). */
  readonly actors: FabricParticipantRecord[];
  /** Wall clock of the listing: the moment it proved these actors live. */
  readonly sampledAt: number;
}

/**
 * One fresh listing. A throw (an unreadable or busy mesh) is not absence: the caller keeps the
 * previous claims, which then lapse on their own deadlines, and never withdraws on it.
 */
export const readFileActorsForMirror = (directory: NatsFileActorDirectory, now: () => number = Date.now): NatsFileActorRead => {
  const sampledAt = now();
  const listed = directory.list({ scope: "project", kinds: ["actor"], fresh: true }, sampledAt);
  const actors: FabricParticipantRecord[] = [];
  for (const info of listed) {
    if (info.kind !== "actor" || info.stale || info.bridgeStale || info.remoteHost !== undefined || info.bridgeVia !== undefined ||
      info.machine !== undefined || TERMINAL.has(info.status) || info.controlProtocol !== "v1" || typeof info.ownerIncarnation !== "string" ||
      info.ownerIncarnation.length === 0) continue;
    const { local: _local, stale: _stale, machine: _machine, bridgeStale: _bridgeStale, ...record } = info;
    actors.push(record);
  }
  return { actors, sampledAt };
};

/**
 * A read-only directory over the hub mesh, built the way fabric-participants builds its listing:
 * an identity no participant has, no heartbeat, no host reaping. It never publishes this link.
 */
export const createFileActorDirectory = async (store: MeshStore, link: string): Promise<NatsFileActorDirectory> => {
  const id = `nats-bridge-actors:${link}:${process.pid}`;
  return new ParticipantDirectory(store, {
    enabled: true, hostId: id, rootId: id, identity: { id, name: "nats-bridge-actors", kind: "main" }, reapDeadHosts: false,
  });
};
