export interface PlacementApiConfig {
  enabled: true;
  url: string;
  requestTimeoutMs: number;
  watchTimeoutMs: number;
}

const optionKeys = new Set(["enabled", "url", "requestTimeoutMs", "watchTimeoutMs"]);

const positiveInteger = (value: unknown, fallback: number, key: string): number => {
  if (value === undefined) return fallback;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 1 && value <= 2_147_483_647) return value;
  throw new Error(`Invalid agents.placementApi.${key}`);
};

const tailnetIpv4 = (hostname: string): boolean => {
  const octets = hostname.split(".");
  return octets.length === 4 && octets.every(part => /^(?:0|[1-9][0-9]{0,2})$/.test(part) && Number(part) <= 255)
    && Number(octets[0]) === 100 && Number(octets[1]) >= 64 && Number(octets[1]) <= 127;
};

const allowedUrl = (value: string): string | undefined => {
  try {
    const url = new URL(value);
    if (url.username || url.password || url.pathname !== "/" || url.search || url.hash || url.protocol !== "http:") return;
    if (url.origin === "http://smarty-placement.smarty-island.svc:8080") return url.origin;
    if (url.port === "30880" && tailnetIpv4(url.hostname)) return url.origin;
  } catch { /* Use one fixed error below; never echo config input. */ }
  return;
};

/** Normalize the host-owned opt-in; workspaces must strip this key before merge. */
export const normalizePlacementApiConfig = (value: unknown): PlacementApiConfig | undefined => {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid agents.placementApi configuration");
  const input = value as Record<string, unknown>;
  if (Object.keys(input).some(key => !optionKeys.has(key))) throw new Error("Invalid agents.placementApi configuration");
  if (input.enabled === false) return undefined;
  if (input.enabled !== true) throw new Error("agents.placementApi.enabled must be true when Placement is configured");
  const url = typeof input.url === "string" ? allowedUrl(input.url) : undefined;
  if (!url) throw new Error("Invalid agents.placementApi configuration");
  return {
    enabled: true,
    url,
    requestTimeoutMs: positiveInteger(input.requestTimeoutMs, 5_000, "requestTimeoutMs"),
    watchTimeoutMs: positiveInteger(input.watchTimeoutMs, 30_000, "watchTimeoutMs"),
  };
};
