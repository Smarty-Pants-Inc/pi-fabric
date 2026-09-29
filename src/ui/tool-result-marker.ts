// Keep this wire-shape guard dependency-free: both transcript readers use it.
export const isCompactToolResult = (value: unknown): boolean => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const result = value as Record<string, unknown>;
  const keys = Object.keys(result);
  return keys.length === 2 && keys.includes("elided") && keys.includes("bytes") &&
    result.elided === true && typeof result.bytes === "number" &&
    Number.isInteger(result.bytes) && result.bytes >= 0;
};
