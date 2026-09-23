// Passive custom-message host for Prewalk delivery: the real AgentSession
// prototype methods run against a real pi-agent-core agent, so a
// `triggerTurn: false` send defers into the pending list while a run is active
// and the turn_end subscription appends it after the send turn's tool results
// — the same deferred path the production host takes. Only the state the real
// constructor installs is wired here; see
// node_modules/@earendil-works/pi-coding-agent/dist/core/agent-session.js
// (sendCustomMessage, _handleAgentEvent -> _flushPendingCustomMessages).
import { AgentSession } from "@earendil-works/pi-coding-agent";

export function createPassiveHostSession(agent, sessionManager) {
  // This shim drives real AgentSession internals, and the peer range allows a
  // newer host than the pinned one. Fail here, beside the cause, instead of
  // inside a turn_end subscriber when an upgrade renames a member.
  for (const member of ["sendCustomMessage", "_flushPendingCustomMessages"]) {
    if (typeof AgentSession.prototype[member] !== "function") {
      throw new Error(
        `passive host shim requires AgentSession.prototype.${member}, which the installed @earendil-works/pi-coding-agent does not provide; update scripts/lib/passive-host-session.mjs for the new host API`,
      );
    }
  }
  const session = Object.create(AgentSession.prototype);
  session.agent = agent;
  session.sessionManager = sessionManager;
  session._pendingCustomMessages = [];
  // Pi 0.87 refreshes finalized transcript projections when it flushes a
  // passive message. Match the constructor's message-to-entry identity map.
  session._entryIdsByMessage = new WeakMap();
  session._isAgentRunActive = true;
  session._emit = () => {};
  const unsubscribe = agent.subscribe((event) => {
    if (event.type === "turn_end") session._flushPendingCustomMessages();
  });
  return {
    sendCustomMessage: (message, options) => session.sendCustomMessage(message, options),
    // Append anything still pending without waiting for a turn boundary.
    flushPending: () => session._flushPendingCustomMessages(),
    dispose: unsubscribe,
  };
}
