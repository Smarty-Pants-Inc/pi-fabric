#!/usr/bin/env node
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const env = { ...process.env, BEND_NO_TELEMETRY: "1" };
const bend = process.env.BEND_BIN || "bend";
if (execFileSync(bend, ["version"], { encoding: "utf8", env }).trim() !== "bend 2.0.26") throw new Error("Bend 2.0.26 required");
const inputs = JSON.parse(readFileSync(join(root, "src/verified/generated/manifest.json"), "utf8")).inputs;
const originals = Object.fromEntries(Object.keys(inputs).filter((path) => path.endsWith(".bend")).map((path) => [path, readFileSync(join(root, path), "utf8")]));
const mutations = [
  ["storage CAS bypass", "proofs/storage-plans.bend", "equal(version, actual)", "True{}"],
  ["storage deny all", "proofs/storage-kernel.bend", "C.reduce(request, slot)", "C.Conflict{}"],
  ["storage wrong key", "proofs/storage-plans.bend", "PutNext{key, value, identity, next(version), next(highWater)}", "PutNext{Nil{}, value, identity, next(version), next(highWater)}"],
  ["storage wrong payload", "proofs/storage-plans.bend", "PutNext{key, value, identity, next(version), next(highWater)}", "PutNext{key, Nil{}, identity, next(version), next(highWater)}"],
  ["storage wrong identity", "proofs/storage-plans.bend", "PutNext{key, value, identity, next(version), next(highWater)}", "PutNext{key, value, Nil{}, next(version), next(highWater)}"],
  ["storage put revision reuse", "proofs/storage-plans.bend", "PutNext{key, value, identity, next(version), next(highWater)}", "PutNext{key, value, identity, version, next(highWater)}"],
  ["storage delete revision reuse", "proofs/storage-plans.bend", "DeleteNext{key, next(version), next(highWater)}", "DeleteNext{key, version, next(highWater)}"],
  ["storage clock reuse", "proofs/storage-plans.bend", "PutNext{key, value, identity, next(version), next(highWater)}", "PutNext{key, value, identity, next(version), highWater}"],
  ["storage ignores eviction clock", "proofs/storage-plans.bend", "case False{}:\n      highWater", "case False{}:\n      version"],
  ["storage ignores clock exhaustion", "proofs/storage-plans.bend", "room(version) && room(highWater)", "room(version)"],
  ["storage overflow bypass", "proofs/storage-plans.bend", "room(version) && room(highWater)", "True{}"],
  ["storage lost carry", "proofs/storage-plans.bend", "Revision{1n+high, 0n}", "Revision{high, 0n}"],
  ["storage reset low limb", "proofs/storage-plans.bend", "Revision{high, 1n+low}", "Revision{high, 0n}"],
  ["storage missing-delete refusal", "proofs/storage-plans.bend", "case Delete{} False{}:\n      Unchanged{}", "case Delete{} False{}:\n      Conflict{}"],

  ["lifecycle staged admission denied", "proofs/lifecycle.bend", "case _:\n      cleanup || Bool.not(revoked)", "case Staged{}:\n      False{}\n    case _:\n      cleanup || Bool.not(revoked)"],
  ["lifecycle retiring admission denied", "proofs/lifecycle.bend", "case _:\n      cleanup || Bool.not(revoked)", "case Retiring{}:\n      cleanup\n    case _:\n      cleanup || Bool.not(revoked)"],
  ["lifecycle failed cleanup denied", "proofs/lifecycle.bend", "case Failed{}:\n      cleanup", "case Failed{}:\n      False{}"],
  ["lifecycle activation loses work", "proofs/lifecycle.bend", "Life{Active{}, owner, holds, calls, revoked}, Granted{}", "Life{Active{}, owner, holds, 0n, revoked}, Granted{}"],
  ["lifecycle quarantine loses work", "proofs/lifecycle.bend", "Life{Failed{}, False{}, holds, calls, True{}}, Granted{}", "Life{Failed{}, False{}, holds, 0n, True{}}, Granted{}"],
  ["lifecycle completes failed close", "proofs/lifecycle.bend", "case Complete{} _:\n      Outcome{life, Denied{}}", "case Complete{} _:\n      Outcome{life, Granted{}}"],

  ["authority widening", "proofs/authority-state.bend", "select(R.covered(candidate, grants, True{}), candidate)", "select(True{}, candidate)"],
  ["authority deny all", "proofs/authority-state.bend", "select(R.covered(candidate, grants, True{}), candidate)", "Released{}"],
  ["authority subset direction", "proofs/authority-state.bend", "R.covered(candidate, grants, True{})", "R.covered(grants, candidate, True{})"],
  ["authority release reuse", "proofs/authority-state.bend", "def release(authority: Authority) -> Authority:\n  Released{}", "def release(authority: Authority) -> Authority:\n  authority"],
  ["authority wrapper widening", "proofs/authority-kernel.bend", "A.derive(parent, candidate)", "A.issue(candidate)"],
  ["authority wrapper release reuse", "proofs/authority-kernel.bend", "A.release(authority)", "authority"],
  ["lifecycle uncounted work", "proofs/lifecycle.bend", "Life{phase, owner, holds, 1n+calls, revoked}, Granted{}", "Life{phase, owner, holds, calls, revoked}, Granted{}"],
  ["lifecycle close under lease", "proofs/lifecycle.bend", "case Close{} Life{Retiring{}, False{}, 0n, 0n, revoked}:", "case Close{} Life{Retiring{}, False{}, holds, 0n, revoked}:"],
  ["lifecycle close under work", "proofs/lifecycle.bend", "case Close{} Life{Retiring{}, False{}, 0n, 0n, revoked}:", "case Close{} Life{Retiring{}, False{}, 0n, calls, revoked}:"],
  ["lifecycle revocation drops work", "proofs/lifecycle.bend", "Life{retiring(phase), False{}, holds, calls, True{}}", "Life{retiring(phase), False{}, holds, 0n, True{}}"],
  ["lifecycle owner release drops holds", "proofs/lifecycle.bend", "Life{phase, False{}, holds, calls, revoked}, Granted{}", "Life{phase, False{}, 0n, calls, revoked}, Granted{}"],
  ["lifecycle close not reserved", "proofs/lifecycle.bend", "Life{Closing{}, False{}, 0n, 0n, revoked}, StartClose{}", "Life{Retiring{}, False{}, 0n, 0n, revoked}, StartClose{}"],
  ["lifecycle revoked admission", "proofs/lifecycle.bend", "cleanup || Bool.not(revoked)", "True{}"],
  ["lifecycle wrapper bypass", "proofs/lifecycle-kernel.bend", "L.bindingStep(life, event)", "L.Outcome{life, L.Granted{}}"],

  ["state authority bypass", "proofs/state-plans.bend", "R.nameEqual(granted, key, True{})", "True{}"],
  ["state stale revision bypass", "proofs/state-plans.bend", "&& R.nameEqual(expected, observed, True{})", "&& True{}"],
  ["state cancellation bypass", "proofs/state-plans.bend", "Bool.not(stopped)", "True{}"],
  ["state revocation bypass", "proofs/state-plans.bend", "active && (", "True{} && ("],
  ["state payload substitution", "proofs/state-plans.bend", "Write{key, expected, value}", "Write{key, expected, Nil{}}"],
  ["state target substitution", "proofs/state-plans.bend", "Write{key, expected, value}", "Write{expected, expected, value}"],
  ["state deny all", "proofs/state-plans.bend", "Write{key, expected, value}", "Denied{}"],
  ["state wrapper ignores cancellation", "proofs/state-kernel.bend", "C.writePlan(grant, key, expected, observed, value, stopped)", "C.writePlan(grant, key, expected, observed, value, False{})"],

  ["provider ticket reuse", "proofs/provider-plans.bend", "Outcome{Ticket{S.revoke(grant), expected, payload}", "Outcome{Ticket{grant, expected, payload}"],
  ["provider descriptor bypass", "proofs/provider-plans.bend", "S.writePlan(grant, key, expected, observed, payload, stopped)", "S.writePlan(grant, key, expected, expected, payload, stopped)"],
  ["provider identity substitution", "proofs/provider-plans.bend", "S.writePlan(grant, key, expected, observed, payload, stopped)", "S.writePlan(grant, Nil{}, expected, observed, payload, stopped)"],
  ["provider payload substitution", "proofs/provider-plans.bend", "S.writePlan(grant, key, expected, observed, payload, stopped)", "S.writePlan(grant, key, expected, observed, Nil{}, stopped)"],
  ["provider deny all", "proofs/provider-plans.bend", "S.writePlan(grant, key, expected, observed, payload, stopped)", "S.Denied{}"],
  ["provider wrapper cancellation bypass", "proofs/provider-kernel.bend", "P.take(ticket, key, observed, stopped)", "P.take(ticket, key, observed, False{})"],
  ["provider wrapper revocation bypass", "proofs/provider-kernel.bend", "P.revoke(ticket)", "ticket"],
  ["provider consumed payload corruption", "proofs/provider-plans.bend", "Outcome{Ticket{S.revoke(grant), expected, payload}", "Outcome{Ticket{S.revoke(grant), expected, Nil{}}"],

  ["deny valid sources", "proofs/resources.bend", "select(nonempty(names) && identitiesValid(names, True{}), names)", "Unknown{}"],
  ["lost original resource", "proofs/resources.bend", "covered(original, candidate, True{}) && covered(candidate, original, True{})", "True{} && covered(candidate, original, True{})"],
  ["injected resource", "proofs/resources.bend", "covered(original, candidate, True{}) && covered(candidate, original, True{})", "covered(original, candidate, True{}) && True{}"],
  ["truncated identity", "proofs/resources.bend", "case Nil{} Con{h, t}:\n      False{}", "case Nil{} Con{h, t}:\n      equal"],
  ["oversized candidate", "proofs/resources.bend", "bounded(candidate, 64n)", "bounded(candidate, 65n)"],
  ["overlong identity", "proofs/resources.bend", "nameBounded(name, 256n)", "nameBounded(name, 257n)"],
  ["forgotten unknown", "proofs/resources.bend", "case Unknown{}:\n      Unknown{}", "case Unknown{}:\n      Exact{Nil{}}"],
  ["dropped effect group", "proofs/resources.bend", "groups(t, ys, found || against(ys, h, False{}))", "groups(t, ys, found)"],
  ["deny every exact footprint", "proofs/resources.bend", "case True{}:\n      Exact{candidate}", "case True{}:\n      Unknown{}"],
  ["invalid source admitted", "proofs/resources.bend", "select(nonempty(names) && identitiesValid(names, True{}), names)", "select(True{}, names)"],
  ["open law", "PROOF.bend", "def Laws.canonical_identity(plan, changed, accepted):\n  {==}", ""],
  ["footprint overflow", "proofs/kernel.bend", "all(footprintConditions(count, limit, valid))", "True{}"],
  ["stale lifecycle publication", "proofs/kernel.bend", "[Bool.not(retired), epoch, owner, Bool.not(closed)]", "[Bool.not(retired), True{}, owner, Bool.not(closed)]"],
  ["crossing tool pair", "proofs/kernel.bend", "Bool.not(paired) || Nat.is_le(boundary, first) || Nat.is_lt(last, boundary)", "True{}"],
  ["false chunk completion", "proofs/kernel.bend", "Bool.not(Bool.xor(complete, Nat.is_eq(end, total)))", "True{}"],
  ["canonical mutation", "proofs/kernel.bend", "all(normalizationConditions(canonical, plan, changed, accepted))", "True{}"],
  ["uncommitted visibility", "proofs/kernel.bend", "committed || (pending && marker)", "committed || pending"],
  ["invalid head version", "proofs/kernel.bend", "all([sequence, version, committed || (pending && marker)])", "all([sequence, True{}, committed || (pending && marker)])"],
  ["reusable certificate", "proofs/kernel.bend", "(active, False{})", "(active, True{})"],
];
const temp = mkdtempSync(join(tmpdir(), "fabric-bend-negative-"));
try {
  mkdirSync(join(temp, "proofs"));
  for (const [name, path, before, after] of mutations) {
    if (!originals[path].includes(before)) throw new Error(`Mutation anchor disappeared: ${name}`);
    for (const [file, source] of Object.entries(originals)) {
      writeFileSync(join(temp, file), file === path ? source.replace(before, after) : source);
    }
    const result = spawnSync(bend, [join(temp, "PROOF.bend"), "--check-only"], { encoding: "utf8", env, timeout: 30_000 });
    const output = (result.stdout ?? "") + (result.stderr ?? "");
    if (result.error || result.signal || result.status !== 1 || !/Error:/.test(output) || /unknown:|a declared constructor|syntax/i.test(output)) {
      throw new Error(`Expected a proof rejection for ${name}, got ${result.status}: ${result.error ?? output}`);
    }
  }
  console.log(`${mutations.length} negative proof probes rejected invalid kernels/open laws without changing the specification.`);
} finally {
  rmSync(temp, { recursive: true, force: true });
}
