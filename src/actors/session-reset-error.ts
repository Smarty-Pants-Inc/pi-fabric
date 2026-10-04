const RESET_CANCELLED_CODE = "ACTOR_SESSION_RESET_CANCELLED" as const;

/** A terminal stop cancelled a requested rotation before its activation boundary. */
export class ActorSessionResetCancelledError extends Error {
  readonly code = RESET_CANCELLED_CODE;
  constructor(readonly id: string, message = `Fabric actor ${id} session reset cancelled by terminal stop`, readonly requestId?: string) {
    // Guest bridges may retain only name/message; preserve the terminal code and
    // exchange identity without depending on custom Error properties.
    super(`${message.startsWith(`${RESET_CANCELLED_CODE}:`) ? message : `${RESET_CANCELLED_CODE}: ${message}`}${requestId ? ` (requestId=${requestId}, actorId=${id})` : ""}`);
    this.name = "ActorSessionResetCancelledError";
  }
}
