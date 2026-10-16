// src/topology/participant-view.ts (A2 owns; A1 imports only this)
export interface ParticipantRoute { machine: string; ownerHostId: string; ownerIdentityId: string; ownerIncarnation: string;
  capabilities: readonly string[]; live: boolean }
export interface ParticipantView {
  /** Throws DirectoryNotReadyError before the presence watch is ready. undefined = known absent. */
  resolve(participantId: string): ParticipantRoute | undefined;
  /** Waits (bounded by signal) for ready, or for a put/gone on that id. */
  whenChanged(participantId: string, signal: AbortSignal): Promise<void>;
  /** The host lease is live by the local monotonic window (§4.2). */
  leaseLive(): boolean;
}
