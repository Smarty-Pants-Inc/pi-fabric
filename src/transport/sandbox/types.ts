import type { AgentUsage } from "../../agents/types.js";

export const SANDBOX_TERMINAL_BUCKET = "fabric_placement_terminal";
export const SANDBOX_PLACEMENT_WAIT_MS = 120_000;
export const SANDBOX_READY_TIMEOUT_MS = 90_000;

export type SandboxPlacementStatus = "refused" | "delayed" | "admitted";
export type SandboxTerminalState = "finished" | "failed" | "never-started";
export type SandboxRunStatus = "completed" | "failed" | "stopped" | "timed_out";
export type SandboxComplexity = "simple" | "normal" | "complex" | "delicate";

/** Exact v1 intent fields. Prompt text is never part of this value. */
export interface SandboxSpawnIntent {
  v: 1;
  runId: string;
  kind: "sandbox";
  org: string;
  parentSession: string;
  role: string;
  complexity: SandboxComplexity;
  model: string;
  thinking: string;
  needs: string[];
  repository: string;
  cwd: string;
  expectedSeconds: number;
  timeoutSeconds: number;
  deadline: string | null;
}

export interface SandboxSpawnResult {
  runId: string;
  status: SandboxPlacementStatus;
  code: string;
  detail: string;
  retryAfterSeconds: number | null;
  host: string | null;
  claimRef: { namespace: string; name: string; uid: string } | null;
  claimUID: string | null;
  podUID: string | null;
}

/** Exact sixteen-field binding response. parentSession is internal and is not on this wire type. */
export interface SandboxBinding {
  runId: string;
  status: SandboxPlacementStatus;
  claimRef: { namespace: string; name: string; uid: string } | null;
  claimUID: string | null;
  podUID: string | null;
  podName: string | null;
  host: string | null;
  org: string | null;
  role: string | null;
  model: string | null;
  provider: string | null;
  thinking: string | null;
  expectedSeconds: number | null;
  deadline: string | null;
  executionDeadline: string | null;
  admittedAt: string | null;
}

/** The PlacementClient implementation is injected by the integration owner. */
export interface SandboxPlacement {
  requestSpawn(intent: SandboxSpawnIntent, signal?: AbortSignal): Promise<SandboxSpawnResult>;
  watchSpawn(runId: string, signal?: AbortSignal): AsyncIterable<SandboxSpawnResult>;
  getBinding(runId: string, signal?: AbortSignal): Promise<SandboxBinding>;
}

export interface SandboxParticipant {
  id: string;
  placementRunId: string;
}

export interface SandboxTaskResult {
  runId: string;
  participantId: string;
  status: SandboxRunStatus;
  text: string;
  exitCode?: number;
  error?: string;
  usage?: AgentUsage;
  turns?: number;
  toolCalls?: number;
}

/** The owning runtime matches the child's PI_FABRIC_RUN_ID presence to the runId, never Pod/Claim names, then uses followUp and the child's return path. */
export interface SandboxMesh {
  waitForPresence(runId: string, options: { timeoutMs: number; signal?: AbortSignal }): Promise<SandboxParticipant>;
  deliverTask(childSession: string, text: string): Promise<void>;
  awaitResult(childSession: string, deadline: number): Promise<SandboxTaskResult>;
  stop?(childSession: string, runId: string): Promise<{ confirmed: boolean; reason?: string }>;
}

export interface SandboxTerminalRecord {
  placementRunId: string;
  state: SandboxTerminalState;
  at: string;
  exitCode?: number;
  reason?: string;
}

/** Backed by the dedicated NATS KV bucket fabric_placement_terminal in the lead's integration. */
export interface SandboxTerminalKv {
  put(key: string, value: SandboxTerminalRecord): Promise<unknown>;
}

export interface SandboxTransportConfig {
  enabled: boolean;
  /** Retained as deployment selection metadata; v1 SpawnIntent currently has no template field. */
  template: string;
  readyTimeoutMs: number;
}

export interface SandboxTransportDependencies {
  placement?: SandboxPlacement;
  mesh?: SandboxMesh;
  terminal?: SandboxTerminalKv;
  /** The parent host's configured control carrier; sandbox requires NATS presence and KV. */
  meshTransport?: "file" | "nats";
  /** Trusted server-side broker context. Never sourced from task text or request fields. */
  org?: string;
  /** Deterministic test seams; production leaves these unset. */
  now?: () => number;
  sleep?: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  resolveRepository?: (cwd: string) => { repository: string; cwd: string };
}

export const writeSandboxTerminalRecord = async (
  store: SandboxTerminalKv | undefined,
  record: SandboxTerminalRecord,
): Promise<void> => {
  if (!store) throw new Error("Sandbox placement terminal KV is not configured");
  await store.put(record.placementRunId, record);
};
