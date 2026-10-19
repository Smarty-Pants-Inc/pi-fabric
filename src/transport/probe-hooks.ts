import { performance } from "node:perf_hooks";
import type { TransportEnvelope, TransportEnvelopeKind } from "./types.js";

/** Process-local, opt-in observations for the Path B acceptance probe. No observer is installed by the runtime. */
export type FabricPathBProbeEvent =
  | {
      readonly type: "transport-puback" | "file-append";
      readonly atNs: string;
      readonly carrier: "nats" | "file";
      readonly envelopeId: string;
      readonly commandId?: string;
      readonly kind: TransportEnvelopeKind;
      readonly operation: string;
      readonly targetId: string;
      readonly sequence: number;
      readonly duplicate: boolean;
    }
  | {
      readonly type: "inbox-ack-confirmed";
      readonly atNs: string;
      readonly carrier: "nats" | "file";
      readonly envelopeId: string;
      readonly commandId?: string;
      readonly kind: TransportEnvelopeKind;
      readonly operation: string;
      readonly targetId: string;
      readonly sequence: number;
      readonly attempt: number;
    }
  | {
      readonly type: "main-followup-journal-commit";
      readonly atNs: string;
      readonly commandId: string;
      readonly messageId: string;
    };

export type FabricPathBProbeObserver = (event: FabricPathBProbeEvent) => void;

let observer: FabricPathBProbeObserver | undefined;

/** Install one worker-local observer; the returned disposer restores the previous observer. */
export const observeFabricPathBProbe = (next: FabricPathBProbeObserver): (() => void) => {
  const previous = observer;
  observer = next;
  return () => {
    if (observer === next) observer = previous;
  };
};

export const hasFabricPathBProbeObserver = (): boolean => observer !== undefined;

/** Same-host stage timestamp in the host epoch domain; never used for cross-host arithmetic. */
export const fabricPathBProbeEpochNs = (): string =>
  (BigInt(Math.round(performance.timeOrigin * 1_000_000)) + BigInt(Math.round(performance.now() * 1_000_000))).toString();

/** A no-op unless a probe explicitly installs an observer. Observer failures never affect runtime work. */
export const recordFabricPathBProbe = (event: FabricPathBProbeEvent): boolean => {
  const current = observer;
  if (!current) return false;
  try { current(Object.freeze({ ...event })); } catch { /* probe evidence is observational only */ }
  return true;
};

/** Extract only correlation metadata; never pass command text or arbitrary user data to observers. */
export const transportProbeCommandId = (envelope: TransportEnvelope): string | undefined => {
  const body: unknown = envelope.body;
  if (body === null || typeof body !== "object" || Array.isArray(body)) return undefined;
  const id = (body as Record<string, unknown>).commandId;
  return typeof id === "string" ? id : undefined;
};
