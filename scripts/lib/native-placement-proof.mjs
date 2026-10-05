import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const mainHostnames = new Set(["dev1", "dev1.smartypants.ai", "ryzen1", "ryzen1.smartypants.ai"]);
const routeName = value => String(value).toLowerCase().split(".")[0].replace(/-agent$/, "");
const isMainRoute = value => ["dev1", "ryzen1"].includes(routeName(value));

// The host-owned map must not turn Main into a work target, even when the
// caller itself has the approved hostname. Check keys AND SSH alias routes.
export function assertWorkHostRoutes(aliases) {
  assert(aliases && typeof aliases === "object" && !Array.isArray(aliases), "work-hosts.json must be a host/alias map");
  const routes = Object.entries(aliases).flat();
  assert(!routes.some(isMainRoute), "Main dev1/ryzen1 must not appear as a work-host route");
}

export function assertMainProofCaller(hostname, aliases) {
  assertWorkHostRoutes(aliases);
  const routes = Object.entries(aliases).flat();
  assert(!routes.some(route => routeName(route) === routeName(hostname)), "SSH proof caller must not be a work host");
  assert(mainHostnames.has(hostname.toLowerCase()), "SSH evidence is a Ryzen 1 owner action only (dev1.smartypants.ai or ryzen1)");
}

// Do not read, serialize, copy, log, or pass the host models file on argv.
// Only the installed SDK consumes this private scratch-profile symlink.
export function linkProofModels(profile, hostProfile, enabled) {
  if (!enabled) return null;
  const source = path.resolve(hostProfile, "models.json");
  assert(fs.existsSync(source) && fs.statSync(source).isFile(), "PROOF_MODELS_FROM_PROFILE requires a host profile models.json file");
  const destination = path.join(profile, "models.json");
  fs.symlinkSync(source, destination);
  return destination;
}
