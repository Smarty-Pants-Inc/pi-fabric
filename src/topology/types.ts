import type { FabricAgentRunner, FabricAgentTransport } from "../config.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { AgentUsage } from "../agents/types.js";

export type FabricParticipantKind = "root" | "agent" | "actor";
export type FabricParticipantResidency = "session" | "durable";
export type FabricParticipantScope = "local" | "lineage" | "project";
export type FabricParticipantCapability =
  | "steer"
  | "followUp"
  | "stop"
  | "ask"
  | "actor-bindings"
  | "attach"
  | "fabric";

export interface FabricParticipantRecord {
  format: 1;
  id: string;
  kind: FabricParticipantKind;
  rootId: string;
  ownerHostId: string;
  ownerIdentityId: string;
  parentId?: string;
  name: string;
  /**
   * Project-scoped Linear-style label (e.g. "PQS-2") minted once per root
   * participant via the mesh peer sequence. Never reused after a peer leaves.
   */
  label?: string;
  status: string;
  residency?: FabricParticipantResidency;
  runner: FabricAgentRunner;
  transport: FabricAgentTransport | "host";
  capabilities: FabricParticipantCapability[];
  cwd?: string;
  sessionId?: string;
  model?: string;
  thinking?: string;
  startedAt: number;
  updatedAt: number;
  finishedAt?: number;
  pendingMessages?: boolean;
  currentTool?: string;
  turns?: number;
  toolCalls?: number;
  usage?: AgentUsage;
  actorQueued?: number;
  actorMessages?: number;
  controlProtocol: "v1" | "legacy";
}

export interface FabricParticipantInfo extends FabricParticipantRecord {
  local: boolean;
  stale: boolean;
}

export interface FabricHostRecord {
  format: 1;
  id: string;
  rootId: string;
  identity: MeshIdentity;
  startedAt: number;
  updatedAt: number;
  expiresAt: number;
}

export interface FabricParticipantListOptions {
  scope?: FabricParticipantScope;
  kinds?: FabricParticipantKind[];
  includeStale?: boolean;
  /** Read the current mesh state, not a recent cached parse (for protocol decisions). */
  fresh?: boolean;
}

export interface FabricPeerInfo {
  id: string;
  name: string;
  /** Minted peer label when the owning host publishes one. */
  label?: string;
  kind: "peer";
  status: "idle" | "running";
  runner: "pi";
  transport: "host";
  cwd: string;
  sessionId: string;
  model?: string;
  thinking?: string;
  startedAt: number;
  updatedAt: number;
  pendingMessages: boolean;
  local: false;
}

export interface FabricParticipantSource {
  list(options?: FabricParticipantListOptions, now?: number): FabricParticipantInfo[];
  get(id: string, now?: number, options?: { fresh?: boolean }): FabricParticipantInfo | undefined;
  self(now?: number): FabricParticipantInfo;
  /** All live root Pi session agents, including the current lineage root. */
  sessions?(now?: number): FabricParticipantInfo[];
  peers(now?: number): FabricPeerInfo[];
  /** The reason peer visibility is unknown (a stalled mesh writer), or undefined when healthy. */
  writeStalled?(now?: number): Error | undefined;
  /** When this host last committed its heartbeat through the mesh. */
  confirmedAt?(): number;
  /**
   * The record of a participant that get() no longer lists because its owner host's lease
   * lapsed, and how long ago (Infinity when that host is gone or was replaced). Undefined
   * when the participant is live or has no record on this mesh root.
   */
  lastKnown?(id: string, now?: number): { participant: FabricParticipantInfo; lapsedMs: number } | undefined;
  refresh(): Promise<void>;
  scheduleRefresh(): void;
}
