/**
 * The `records` section of .pi/fabric.json (smarty-dev#754). Off unless enabled. Fabric reaches
 * the org's records service over its socket; database access, role policy and archive admission
 * are that service's configuration (C10), never a caller's.
 */
export interface FabricRecordsConfig {
  enabled: boolean;
  /** The org's records service socket, for example /run/<org>-records/records.sock. */
  socket?: string;
  /** An operator-issued credential (the importer or the mirror) instead of registering this participant. */
  credentialFile?: string;
  /** The records relay's credential (0600, the org user's): with it, this process publishes nudges and raises alarms. */
  relayCredentialFile?: string;
  /** Who gets ops.records alarms (a participant id or name); unaddressed when absent. */
  alarmTo?: string;
  watchdogMs: number;
  consumerLagSeconds: number;
}

export const DEFAULT_RECORDS_CONFIG: FabricRecordsConfig = {
  enabled: false,
  watchdogMs: 60_000,
  consumerLagSeconds: 120,
};

const object = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown): string | undefined => typeof value === "string" && value.trim() ? value.trim() : undefined;
const integer = (value: unknown, fallback: number, min: number, max: number): number =>
  typeof value === "number" && Number.isSafeInteger(value) ? Math.min(max, Math.max(min, value)) : fallback;

export const normalizeRecordsConfig = (input: unknown): FabricRecordsConfig => {
  const raw = object(input);
  return {
    enabled: raw.enabled === true,
    ...(text(raw.socket) ? { socket: text(raw.socket)! } : {}),
    ...(text(raw.credentialFile) ? { credentialFile: text(raw.credentialFile)! } : {}),
    ...(text(raw.relayCredentialFile) ? { relayCredentialFile: text(raw.relayCredentialFile)! } : {}),
    ...(text(raw.alarmTo) ? { alarmTo: text(raw.alarmTo)! } : {}),
    watchdogMs: integer(raw.watchdogMs, DEFAULT_RECORDS_CONFIG.watchdogMs, 1_000, 3_600_000),
    consumerLagSeconds: integer(raw.consumerLagSeconds, DEFAULT_RECORDS_CONFIG.consumerLagSeconds, 1, 86_400),
  };
};
