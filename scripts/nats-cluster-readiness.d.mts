export const READINESS_TIMEOUT_MS: number;
export const READINESS_POLL_MS: number;

export interface NatsReadinessNode {
  name: string;
  monitorUrl: string;
}

export interface NatsReadinessState {
  name: string;
  monitorUrl: string;
  serverId: string;
  serverName: string;
  leader: string | null;
  syncAlways: boolean;
  routeCount: number | null;
  peers: string[];
  diagnostics: Record<string, unknown>;
  problems: string[];
}

export interface NatsReadinessOptions {
  fetch?: typeof globalThis.fetch;
  now?: () => number;
  timeoutMs?: number;
}

export function waitForNatsCluster(
  nodes: NatsReadinessNode[],
  options?: NatsReadinessOptions,
): Promise<{ nodes: NatsReadinessState[]; metaLeader: string | undefined }>;

export function runReadinessCli(
  args: string[],
  options?: NatsReadinessOptions & { stdout?: (line: string) => void; stderr?: (line: string) => void },
): Promise<number>;
