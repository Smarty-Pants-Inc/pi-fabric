/** The `records` section of .pi/fabric.json (smarty-dev#754). Off unless enabled. */
export interface FabricRecordsArchiveTarget {
  name: string;
  /** The full argv of `wal-g ... wal-verify integrity --json` for this target (no shell). */
  command: string[];
  env?: Record<string, string>;
  timeoutMs?: number;
}

export interface FabricRecordsConfig {
  enabled: boolean;
  /** The org this database belongs to (one database per org, C13). */
  org?: string;
  /** This Node's origin id; defaults to the host name. */
  origin?: string;
  /** libpq connection fields; a Unix socket directory goes in host. No password in config. */
  connection: { host?: string; port?: number; database?: string; user?: string };
  /** Apply migrations at first use (needs a role that may create tables and roles). */
  migrate: boolean;
  /** The GitHub mirror's outbox rows; off writes none (§7 "mirror off"). */
  mirror: { enabled: boolean; repos?: string[] };
  /** Participant ids with the importer role (C13): they may set author, with data.via. */
  importers: string[];
  /** Participant ids with the mirror role: they may append record.mirror. */
  mirrors: string[];
  /** C2 degraded-mode admission; off without targets. */
  admission: { targets: FabricRecordsArchiveTarget[]; alarmSeconds: number; refuseSeconds: number; refreshMs: number; segmentSize?: number };
  /** Where the status the factory check reads is written; defaults under the mesh root. */
  statusFile?: string;
  /** Who gets ops.records alarms (a participant id or name); unaddressed when absent. */
  alarmTo?: string;
  watchdogMs: number;
  consumerLagSeconds: number;
}

export const DEFAULT_RECORDS_CONFIG: FabricRecordsConfig = {
  enabled: false,
  connection: {},
  migrate: true,
  mirror: { enabled: false },
  importers: [],
  mirrors: [],
  admission: { targets: [], alarmSeconds: 120, refuseSeconds: 300, refreshMs: 30_000 },
  watchdogMs: 60_000,
  consumerLagSeconds: 120,
};

const object = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown): string | undefined => typeof value === "string" && value.trim() ? value.trim() : undefined;
const strings = (value: unknown): string[] => Array.isArray(value) ? value.map(text).filter((item): item is string => item !== undefined) : [];
const integer = (value: unknown, fallback: number, min: number, max: number): number =>
  typeof value === "number" && Number.isSafeInteger(value) ? Math.min(max, Math.max(min, value)) : fallback;

export const normalizeRecordsConfig = (input: unknown): FabricRecordsConfig => {
  const raw = object(input);
  const connection = object(raw.connection);
  const mirror = object(raw.mirror);
  const admission = object(raw.admission);
  const defaults = DEFAULT_RECORDS_CONFIG;
  const targets = (Array.isArray(admission.targets) ? admission.targets : []).flatMap((entry): FabricRecordsArchiveTarget[] => {
    const target = object(entry);
    const name = text(target.name);
    const command = strings(target.command);
    if (!name || command.length === 0) return [];
    const env = Object.fromEntries(Object.entries(object(target.env)).filter((pair): pair is [string, string] => typeof pair[1] === "string"));
    return [{ name, command, ...(Object.keys(env).length ? { env } : {}), ...(typeof target.timeoutMs === "number" ? { timeoutMs: integer(target.timeoutMs, 60_000, 1_000, 600_000) } : {}) }];
  });
  const alarmSeconds = integer(admission.alarmSeconds, defaults.admission.alarmSeconds, 1, 86_400);
  return {
    enabled: raw.enabled === true,
    ...(text(raw.org) ? { org: text(raw.org)! } : {}),
    ...(text(raw.origin) ? { origin: text(raw.origin)! } : {}),
    connection: {
      ...(text(connection.host) ? { host: text(connection.host)! } : {}),
      ...(typeof connection.port === "number" ? { port: integer(connection.port, 5432, 1, 65_535) } : {}),
      ...(text(connection.database) ? { database: text(connection.database)! } : {}),
      ...(text(connection.user) ? { user: text(connection.user)! } : {}),
    },
    migrate: raw.migrate !== false,
    mirror: { enabled: mirror.enabled === true, ...(Array.isArray(mirror.repos) ? { repos: strings(mirror.repos) } : {}) },
    importers: strings(raw.importers),
    mirrors: strings(raw.mirrors),
    admission: {
      targets,
      alarmSeconds,
      refuseSeconds: Math.max(alarmSeconds, integer(admission.refuseSeconds, defaults.admission.refuseSeconds, 1, 86_400)),
      refreshMs: integer(admission.refreshMs, defaults.admission.refreshMs, 1_000, 3_600_000),
      ...(typeof admission.segmentSize === "number" ? { segmentSize: integer(admission.segmentSize, 16 * 1024 * 1024, 1024 * 1024, 1024 * 1024 * 1024) } : {}),
    },
    ...(text(raw.statusFile) ? { statusFile: text(raw.statusFile)! } : {}),
    ...(text(raw.alarmTo) ? { alarmTo: text(raw.alarmTo)! } : {}),
    watchdogMs: integer(raw.watchdogMs, defaults.watchdogMs, 1_000, 3_600_000),
    consumerLagSeconds: integer(raw.consumerLagSeconds, defaults.consumerLagSeconds, 1, 86_400),
  };
};
