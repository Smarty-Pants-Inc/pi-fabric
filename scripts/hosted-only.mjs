// Test-only authorization, shared by Vitest collection and the inherited Node deletion guard.
import { posix, win32 } from "node:path";

// Some suites mock node:fs before collection. Authorization needs the runner's real
// filesystem, not a suite's mocked fixture catalog (Node >=24 is the package floor).
const fs = process.getBuiltinModule("node:fs");

// isolateTestFleetEnvironment scrubs SMARTY_ROLE for attribution tests. Carry only its veto,
// not the role/identity, through config reloads, workers and NODE_OPTIONS children.
export const fleetHostMarker = "PI_TEST_FLEET_HOST";
export const hostedAdmissionMarker = "PI_TEST_HOSTED_ONLY_PARENT_ADMITTED";
export function rememberFleetHost(env = process.env) {
  if (env.SMARTY_ROLE !== undefined) env[fleetHostMarker] = "1";
}

function runnerDirectory(directory) {
  try {
    return fs.statSync(directory).isDirectory() ? fs.realpathSync(directory) : undefined;
  } catch { return undefined; }
}

const absolute = path => posix.isAbsolute(path) || win32.isAbsolute(path);
const fleetPath = path => /^(?:\/home\/paul|\/srv\/scratch)(?:\/|$)/i.test(path.replaceAll("\\", "/"));

/** Pure with a supplied directory probe. Flags/opt-ins alone are never authorization.
 * This is an accidental-execution guard, not attestation against a caller controlling all env/filesystem inputs. */
export function isHostedOnlyAllowed(env, directory = runnerDirectory) {
  // The test setup records original fleet identity before role scrubbing. A worker's new task role
  // is tolerated only when its parent explicitly passed this already-admitted decision.
  if (env[fleetHostMarker] !== undefined) return false;
  if (env.SMARTY_ROLE !== undefined && env[hostedAdmissionMarker] !== "1") return false;
  if (env.GITHUB_ACTIONS !== "true" || env.RUNNER_ENVIRONMENT !== "github-hosted") return false;
  const temp = env.RUNNER_TEMP;
  if (!temp || !absolute(temp) || fleetPath(temp)) return false;
  const physical = directory(temp);
  return typeof physical === "string" && absolute(physical) && !fleetPath(physical);
}

const refused = new Set();
export function hostedOnly(caseName, env = process.env) {
  const allowed = isHostedOnlyAllowed(env);
  if (!allowed && !refused.has(caseName)) {
    refused.add(caseName);
    console.warn(`hosted-only (${caseName}): push and let CI run it`);
  }
  return allowed;
}
