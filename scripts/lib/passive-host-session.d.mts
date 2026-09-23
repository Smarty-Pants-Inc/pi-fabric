import type { Agent } from "@earendil-works/pi-agent-core";
import type { AgentSession, SessionManager } from "@earendil-works/pi-coding-agent";

export interface PassiveHostSession {
  sendCustomMessage: AgentSession["sendCustomMessage"];
  flushPending(): void;
  dispose(): void;
}

export function createPassiveHostSession(
  agent: Agent,
  sessionManager: SessionManager,
): PassiveHostSession;
