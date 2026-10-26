import type { FabricActorInfo } from "../actors/types.js";
import type { FabricAgentRunner, FabricAgentTransport } from "../config.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { AgentPlacementKind, AgentUsage } from "../agents/types.js";
import type { FabricHostLease } from "./host-leases.js";

/** `provider`: work a Fabric provider registered through `context.participants`. */
export type FabricParticipantKind = "root" | "agent" | "actor" | "provider";
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
  /**
   * Fresh control-owner activation, including same-process extension reloads.
   * Requesters copy it onto control commands so the owner refuses commands
   * meant for an earlier activation. Absent on older records.
   */
  ownerIncarnation?: string;
  parentId?: string;
  name: string;
  /**
   * Project-scoped Linear-style label (e.g. "PQS-2") minted once per root
   * participant via the mesh peer sequence. Never reused after a peer leaves.
   */
  label?: string;
  /** Pod-injected sandbox correlation or Placement API binding for process/remote runs. */
  placementRunId?: string;
  /** The root's fleet role, for example "project-agent" (smarty-dev#784). */
  role?: string;
  /** The Herdr pane a root Main runs in (smarty-dev#6758); display only, never authority. */
  herdrPane?: string;
  /** The checkout that owns the root's git common directory (smarty-dev#784). */
  project?: string;
  /** Runtime project root, distinct from the shared checkout of linked worktrees. */
  projectRoot?: string;
  /** Normalized origin identity, portable across checkout paths and hosts. */
  repository?: string;
  /** False for print/JSON roots: discoverable observers, never message or lead targets. */
  interactive?: boolean;
  /** Reserved remote Main setter advertisement; false until a native Pi commit guard exists.
   * Kept outside capabilities: existing format-1 readers reject unknown capability strings. */
  mainBindings?: boolean;
  /** Outside capabilities so old format-1 parsers can read the advertisement. */
  livenessLeaseFiles?: 1;
  status: string;
  /** Authenticated, capability-free terminal history of a session actor. */
  stopReason?: "owner-root-gone";
  ownerRootGoneAt?: number;
  ownerRootGoneBy?: string;
  settledAt?: number;
  retiredAt?: number;
  retiredBy?: string;
  /** Root records: the writing Main process (pid, host, Linux /proc start ticks), so an operator tool can
   * tell a live Main from a reused pid (smarty-dev#7817). Absent on older writers: identity unavailable. */
  mainProcess?: { pid: number; host: string; startTime?: string };
  /** Fixed expiry of a Main reload handoff; never a grace period for an exited session. */
  reloadUntil?: number;
  residency?: FabricParticipantResidency;
  /** Absent only for `provider` participants, which have no agent runner. */
  runner?: FabricAgentRunner;
  /** Registering provider name for `provider` participants. */
  provider?: string;
  transport: FabricAgentTransport | "host" | "hosted";
  capabilities: FabricParticipantCapability[];
  cwd?: string;
  sessionId?: string;
  /** Placement API execution metadata, present only for enabled process/remote runs. */
  placementKind?: AgentPlacementKind;
  placementHost?: string;
  expectedSeconds?: number;
  /** Services that this Main session accepts over Fabric's NATS session channel. */
  services?: Array<{ name: string; version: number }>;
  model?: string;
  thinking?: string;
  startedAt: number;
  updatedAt: number;
  finishedAt?: number;
  /** Advertised only after the durable task final-answer boundary, never a turn/status alone. */
  finalAnswerReceiptId?: string;
  pendingMessages?: boolean;
  currentTool?: string;
  turns?: number;
  toolCalls?: number;
  usage?: AgentUsage;
  /** Registry lineage selected by an actor publisher; not an authority grant. */
  actorOwnershipToken?: string;
  actorQueued?: number;
  actorMessages?: number;
  /** Accepted activation preparation or AgentManager admission receipt, before worker launch. */
  actorPreparing?: FabricActorInfo["preparing"];
  /** The actor's in-flight run (smarty-dev#2184 item 8). */
  actorRun?: { id: string; startedAt: number };
  /** The actor's removal, pending behind its in-flight run (smarty-dev#2184 item 8). */
  actorRemoval?: { requestedAt: number; runId?: string; runStartedAt?: number };
  controlProtocol: "v1" | "legacy";
  /**
   * Set on a root that the mesh bridge mirrors from another host's mesh (smarty-dev#2004):
   * the name of that host. A routing target only, never a local owner.
   */
  remoteHost?: string;
  /** Trusted hub link carrying a re-export; remoteHost still names the origin. */
  bridgeVia?: string;
}

/** KV lease payload. Process identity is evidence for the separate local successor gate, not TTL takeover. */
export interface FabricKvHostLease extends FabricHostLease {
  incarnation: string;
  pid: number;
  pidStartTime: string;
  bootId: string;
  /**
   * Set while a lease that expired in KV and was created again awaits its owner's root re-check
   * (smarty-dev#15310): it still holds the host against other machines, but routes nothing.
   */
  rootRecheck?: true;
}

export interface FabricParticipantInfo extends FabricParticipantRecord {
  /** KV-only projection from the authenticated key, never a presence JSON authority field. */
  machine?: string;
  local: boolean;
  stale: boolean;
  /** Retained remote routing attribution; transport freshness is unknown, not owner death. */
  bridgeStale?: true;
}

export interface FabricHostRecord {
  format: 1;
  livenessLeaseFiles?: 1;
  id: string;
  rootId: string;
  identity: MeshIdentity;
  /** UUID fence for token-capable host incarnations; start time is metadata. */
  incarnationToken?: string;
  /** Locally observed tokens superseded by this mirror; bounded and absent on legacy records. */
  bridgeSupersededTokens?: string[];
  /** Root-local first-seen ordinals for retained superseded tokens; absent on legacy records. */
  bridgeSupersededTokenSeen?: Record<string, number>;
  startedAt: number;
  updatedAt: number;
  expiresAt: number;
  /** Set on a host lease that the mesh bridge mirrors from another host's mesh (smarty-dev#2004). */
  remoteHost?: string;
  /** Trusted hub link carrying a re-export; remoteHost still names the origin. */
  bridgeVia?: string;
  /** This projection requires a matching source-clock mirror lease; no legacy expiry fallback. */
  mirrorLease?: 1;
}

export interface FabricParticipantListOptions {
  scope?: FabricParticipantScope;
  kinds?: FabricParticipantKind[];
  includeStale?: boolean;
  /** Read the current mesh state, not a recent cached parse (for protocol decisions). */
  fresh?: boolean;
  /** Display/background observation only; never use for routing or ownership decisions. */
  background?: boolean;
}

export interface FabricPeerInfo {
  id: string;
  name: string;
  /** Minted peer label when the owning host publishes one. */
  label?: string;
  role?: string;
  project?: string;
  /** Normalized origin identity, portable across checkout paths and hosts. */
  repository?: string;
  /** False for print/JSON roots: discoverable observers, never message or lead targets. */
  interactive?: boolean;
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
  /** The remote host of a peer mirrored by the mesh bridge (smarty-dev#2004). */
  host?: string;
}

export type FabricControlAdmission =
  | { controlAdmission: "admitted" }
  | { controlAdmission: "gated"; controlAdmissionReason: string };

export interface FabricParticipantSource {
  list(options?: FabricParticipantListOptions, now?: number): FabricParticipantInfo[];
  get(id: string, now?: number, options?: { fresh?: boolean }): FabricParticipantInfo | undefined;
  /**
   * Whether this host publishes the participant (`main` names the lineage root), from memory.
   * False means get(id) is not local, so a caller can pass over the id without a mesh read.
   */
  publishes?(id: string): boolean;
  /** Lease-independent lineage test shared by adoption and delivery; only positive closure proves death. */
  lineageAlive?(rootId: string, now?: number): boolean;
  self(now?: number): FabricParticipantInfo;
  /** All live root Pi session agents, including the current lineage root. */
  sessions?(now?: number): FabricParticipantInfo[];
  peers(now?: number): FabricPeerInfo[];
  /** The reason peer visibility is unknown (a stalled mesh writer), or undefined when healthy. */
  writeStalled?(now?: number): Error | undefined;
  /** Effective local control gate, not routing-read freshness or ordinary Main activity. */
  controlAdmission?(now?: number): FabricControlAdmission;
  /** Why a negative routing lookup is not authoritative (unjoined, overdue or failed view). */
  routingUnavailable?(now?: number): string | undefined;
  /** Wait for a briefly late native lease (file) or remote mirror (notifications).
   * false means too old/ended; signal cancels a mirror wait with its exact reason. */
  resolveRoutingLease?(id: string, signal?: AbortSignal): Promise<boolean>;
  /** Bound retained-root delivery by routing grace, never by lineage absence alone. */
  retainedRouteAllowed?(id: string): boolean;
  /** One short, lock-bounded canonical read; does not renew or confirm a host lease. */
  refreshRoutingView?(): Promise<void>;
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
