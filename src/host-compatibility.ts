import { existsSync, readFileSync, realpathSync } from "node:fs";
import path from "node:path";

export const MINIMUM_PI_HOST_VERSION = "0.99.0";

const PI_HOST_PACKAGE_NAMES = new Set([
  "@earendil-works/pi-coding-agent",
  "@mariozechner/pi-coding-agent",
]);

interface ParsedVersion {
  numbers: [number, number, number];
  prerelease?: string;
}

const parseVersion = (value: string): ParsedVersion | undefined => {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?/.exec(value.trim());
  if (!match) return undefined;
  return {
    numbers: [Number(match[1]), Number(match[2]), Number(match[3])],
    ...(match[4] ? { prerelease: match[4] } : {}),
  };
};

export const compareVersions = (left: string, right: string): number | undefined => {
  const a = parseVersion(left);
  const b = parseVersion(right);
  if (!a || !b) return undefined;
  for (let index = 0; index < a.numbers.length; index++) {
    const delta = a.numbers[index]! - b.numbers[index]!;
    if (delta !== 0) return Math.sign(delta);
  }
  if (a.prerelease && !b.prerelease) return -1;
  if (!a.prerelease && b.prerelease) return 1;
  if (a.prerelease === b.prerelease) return 0;
  return (a.prerelease ?? "").localeCompare(b.prerelease ?? "");
};

export const detectPiHostVersion = (
  cliPath: string | undefined = process.argv[1],
): string | undefined => {
  if (!cliPath) return undefined;
  let directory: string;
  try {
    directory = path.dirname(realpathSync(cliPath));
  } catch {
    return undefined;
  }
  while (true) {
    const manifestPath = path.join(directory, "package.json");
    if (existsSync(manifestPath)) {
      try {
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
          name?: unknown;
          version?: unknown;
        };
        if (
          typeof manifest.name === "string" &&
          PI_HOST_PACKAGE_NAMES.has(manifest.name) &&
          typeof manifest.version === "string"
        ) {
          return manifest.version;
        }
      } catch {
        // Keep walking when a parent package manifest is unreadable.
      }
    }
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

export const piHostCompatibilityWarning = (
  version: string | undefined = detectPiHostVersion(),
): string | undefined => {
  if (!version) return undefined;
  const comparison = compareVersions(version, MINIMUM_PI_HOST_VERSION);
  if (comparison === undefined || comparison >= 0) return undefined;
  return "Pi Fabric requires Pi >= " + MINIMUM_PI_HOST_VERSION + "; detected " + version + ". Native tool loadouts and nested execution require Pi 0.99. Upgrade Pi before using Fabric.";
};

/** Pi 0.87.0 added agent_before_settle, where the followUp drain hands its messages back to Pi. */
export const FOLLOW_UP_DRAIN_MIN_PI_VERSION = "0.87.0";

/**
 * Whether this Pi host can run the followUp drain (smarty-dev#1495). An older host keeps Pi's own
 * followUp queue: without agent_before_settle, a held followUp could only restart Main after the
 * run, which a cancel there must prevent. An unknown host (an SDK embedding) is assumed current.
 */
export const followUpDrainSupported = (version: string | undefined = detectPiHostVersion()): boolean =>
  version === undefined || (compareVersions(version, FOLLOW_UP_DRAIN_MIN_PI_VERSION) ?? 1) >= 0;
