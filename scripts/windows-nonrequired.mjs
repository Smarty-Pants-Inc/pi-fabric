// Temporary hosted-Windows exception through 2026-11-28 UTC; the test still runs in its own job.
// https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20553
// https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20555
export const WINDOWS_NONREQUIRED_FULL_NAME = "live cleanup move-aside custody retainRuns leaves run and TMPDIR untouched but LIVE host still retains metadata and saved result";

/**
 * Vitest testNamePattern matches full names, not just the individual test title.
 * @param {string} platform
 * @param {Record<string, string | undefined>} env
 * @param {number} now UTC epoch milliseconds
 * @returns {string | undefined}
 */
export function windowsNonrequiredPattern(platform, env, now) {
  const exact = WINDOWS_NONREQUIRED_FULL_NAME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  if (env.FABRIC_TEST_WINDOWS_NONREQUIRED === "1") return `^${exact}$`;
  if (platform === "win32" && env.GITHUB_ACTIONS === "true"
    && env.RUNNER_ENVIRONMENT === "github-hosted" && now < Date.parse("2026-11-29T00:00:00Z")) {
    return `^(?!${exact}$).*$`;
  }
  return undefined;
}
