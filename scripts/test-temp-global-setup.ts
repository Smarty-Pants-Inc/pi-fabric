import { cleanupIsolatedTestTemp } from "./test-temp.js";

// Registered once on the root config. Vitest calls teardown on pass and failure. Config and
// global setup share inode-pinned callbacks on process; runner-owned Windows paths are untouched.
export default function setup(): () => void {
  return function teardown(): void {
    cleanupIsolatedTestTemp();
  };
}
