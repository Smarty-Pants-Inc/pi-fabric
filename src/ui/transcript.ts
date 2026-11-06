import type { FabricAgentRunner } from "../config.js";
import { AgentTranscriptReader } from "./transcript-reader.js";

type FabricTranscriptEntryStatus = "running" | "completed" | "failed";

export interface FabricTranscriptEntry {
  id: string;
  kind: "user" | "assistant" | "tool" | "error" | "status";
  label: string;
  text?: string;
  status?: FabricTranscriptEntryStatus;
  toolName?: string;
  args?: Record<string, unknown>;
  result?: unknown;
  parentId?: string;
  depth?: number;
  /** Count only when the original stderr chunk contains warnings exclusively. */
  warningCount?: number;
}

export interface FabricAgentTranscript {
  entries: FabricTranscriptEntry[];
  /** Kept for compatibility; true means older pages are available. */
  truncated: boolean;
  hasMore?: boolean;
  hasNewer?: boolean;
  updatedAt?: number;
}

export interface FabricTranscriptSource {
  id: string;
  status: string;
  logFile?: string;
}

export interface FabricAgentToolPreviewNode {
  id: string;
  name: string;
  status?: string;
  runner?: FabricAgentRunner;
  owner?: "agent" | "actor";
  /** Most recent tool the agent was observed running, when known. */
  currentTool?: string;
  text?: string;
  tools: FabricTranscriptEntry[];
  /** Descendant previews, one branch per spawned nested agent run. */
  agents?: FabricAgentToolPreviewNode[];
  /** True when descendant previews were cut by the preview tree budget. */
  agentsTruncated?: boolean;
}

export interface FabricAgentToolPreview extends FabricAgentToolPreviewNode {
  kind: "fabric-agent-tools";
}

export {
  projectAgentTranscript,
  isFabricAgentToolPreview,
  recentTranscriptTools,
} from "../util.js";

export { AgentTranscriptReader };
