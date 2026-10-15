// Keep: trust-boundary contention and process-crash recovery (#7991), real multiprocess SQLite.
// No worker, TypeScript child, Bun child, signals, process-tree control or polling.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { readHostPolicy } from "../src/host-policy.js";
import { initializeFreshSlotAuthority, slotPhysicalHostIdentity, slotRoutesDigest, type SlotAuthorityBinding } from "../src/actors/slot-authority.js";
import { slotAdmissionFor, SLOT_ADMISSION_PREFIX } from "../src/actors/slot-admission.js";

vi.mock("../src/host-policy.js", () => ({ readHostPolicy: vi.fn() }));
const roots: string[] = [], meshes: MeshStore[] = [];
const childSettlements: Promise<unknown>[] = [];
const caller = { id: "session:process-main", name: "Main", kind: "main" as const };
const routes = [{ repository: "acme/demo", topic: "github.demo.pulls", maxSlot: 4 }];
const key = "factory/review-slots/v1/acme/demo/1/base";
const names = ["demo-review-astra", "demo-security-astra"];
const openMesh = (root: string) => { const mesh = new MeshStore(root, 64 * 1024, 10_000, { stateBackend: "sqlite" }); meshes.push(mesh); return mesh; };
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-process-")); roots.push(root);
  const plan: SlotAuthorityBinding = { protocol: "slot-admission-v1", meshRoot: path.join(root, "mesh"),
    registryRoots: [path.join(root, "actors")], rootIds: [caller.id], authorityGeneration: "process-authority-7991", hubRootId: "same-hub",
    physicalHostIdentity: slotPhysicalHostIdentity(), routesDigest: slotRoutesDigest(routes) };
  let document = { slotAdmission: { protocol: "slot-admission-v1", routes, authorities: [plan] } };
  vi.mocked(readHostPolicy).mockImplementation(() => ({ status: "valid", document }));
  const binding = await initializeFreshSlotAuthority(plan.meshRoot);
  document = { ...document, slotAdmission: { ...document.slotAdmission, authorities: [binding] } };
  const policyFile = path.join(root, "fixture-policy.json"); fs.writeFileSync(policyFile, JSON.stringify(document));
  const mesh = openMesh(binding.meshRoot);
  const claim = await mesh.put({ key, identity: caller, value: { version: 1, repository: "acme/demo", slot: 1, generation: "base",
    owner: "process-fixture", hubRootId: binding.hubRootId, names, claimedBy: caller.id } });
  const bundle = path.join(root, "core-fixture.mjs");
  const sourceRoot = fileURLToPath(new URL("../src", import.meta.url));
  await build({ stdin: { contents: 'export { MeshStore } from "./mesh/store.js"; export { slotAdmissionFor } from "./actors/slot-admission.js";',
    resolveDir: sourceRoot, loader: "ts" }, outfile: bundle, bundle: true, format: "esm", platform: "node", target: "node24",
    plugins: [{ name: "installer-policy-fixture", setup(build) {
      build.onResolve({ filter: /host-policy\.js$/ }, args => args.importer.endsWith("/actors/slot-authority.ts")
        ? { path: "installer-policy-fixture", namespace: "fixture" } : undefined);
      build.onLoad({ filter: /.*/, namespace: "fixture" }, () => ({ contents:
        'import fs from "node:fs"; export const readHostPolicy = () => ({status:"valid",document:JSON.parse(fs.readFileSync(process.env.SLOT_FIXTURE_POLICY,"utf8"))});' }));
    } }] });
  const input = (actorId: string, requestKey: string, role = 0, revision = claim.version) => {
    const request = { name: names[role]!, instructions: "Fully resolved process fixture instructions", topics: [routes[0]!.topic],
      residency: "durable", scope: "session", idempotencyKey: requestKey, slotClaim: { key, ifVersion: revision } };
    return { actorId, request, spec: { ...request, id: actorId, rootId: caller.id, principalScope: { kind: "root", id: caller.id },
      createdAt: 1, updatedAt: 1, sessionFile: path.join(root, actorId, "session.jsonl"), messages: [] }, registryRoot: binding.registryRoots[0]!, caller };
  };
  const child = (value: ReturnType<typeof input>, mode = "admit") => {
    const inputFile = path.join(root, `${value.actorId}.input.json`), receipt = path.join(root, `${value.actorId}.receipt.json`);
    fs.writeFileSync(inputFile, JSON.stringify(value));
    const processChild = spawn(process.execPath, [fileURLToPath(new URL("./fixtures/slot-admission-process.mjs", import.meta.url)),
      bundle, binding.meshRoot, inputFile, mode, receipt], { env: { ...process.env, SLOT_FIXTURE_POLICY: policyFile }, stdio: ["ignore", "pipe", "pipe", "ipc"] });
    let stderr = "", stdout = "", result: { ok: boolean; code?: string; reason?: string; value?: { actorId: string } } | undefined;
    processChild.stderr!.on("data", data => { stderr = (stderr + String(data)).slice(-16_384); });
    processChild.stdout!.on("data", data => { stdout = (stdout + String(data)).slice(-16_384); });
    let readyResolve!: () => void;
    const ready = new Promise<void>(resolve => { readyResolve = resolve; });
    processChild.on("message", (message: { phase?: string; result?: typeof result }) => {
      if (message.phase === "ready") readyResolve();
      if (message.phase === "result") result = message.result;
    });
    const done = new Promise<{ code: number | null; result: typeof result; stderr: string; stdout: string }>((resolve, reject) => {
      processChild.once("error", error => { readyResolve(); reject(error); });
      processChild.once("exit", code => { readyResolve(); resolve({ code, result, stderr, stdout }); });
    });
    childSettlements.push(done);
    return { ready, done, receipt, go: () => processChild.send("go") };
  };
  return { mesh, binding, claim, input, child };
};
afterEach(async () => {
  // Even on an assertion failure, await the fixture's finite self-exit before touching its files.
  await Promise.allSettled(childSettlements.splice(0));
  for (const mesh of meshes.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 30_000);

it("multiprocess: concurrent same-role births serialize to one durable permanent ID", async () => {
  const f = await fixture(), a = f.child(f.input("actor-a", "request-a")), b = f.child(f.input("actor-b", "request-b"));
  await Promise.all([a.ready, b.ready]); a.go(); b.go();
  const receipts = await Promise.all([a.done, b.done]);
  expect(receipts.map(receipt => receipt.code)).toEqual([0, 0]);
  expect(receipts.filter(receipt => receipt.result?.ok)).toHaveLength(1);
  expect(receipts.filter(receipt => receipt.result?.code === "FABRIC_ACTOR_SLOT_CLAIM_REFUSED")).toHaveLength(1);
  const replay = await slotAdmissionFor(openMesh(f.mesh.root)).recoverable(f.binding.registryRoots[0]!);
  expect(replay).toHaveLength(1);
  expect(replay[0]!.actorId).toBe(receipts.find(receipt => receipt.result?.ok)!.result!.value!.actorId);
}, 30_000);

it("multiprocess: claim replacement before admission is stale; no accepted receipt or intent escapes", async () => {
  const f = await fixture(), actor = f.child(f.input("actor-a", "request-a"));
  await actor.ready;
  await f.mesh.delete({ key, ifVersion: f.claim.version });
  const recreated = await f.mesh.put({ key, value: f.claim.value, identity: caller });
  expect(recreated.version).toBeGreaterThan(f.claim.version);
  actor.go();
  expect(await actor.done).toMatchObject({ code: 0, result: { ok: false, reason: "stale" } });
  expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
}, 30_000);

it("multiprocess: birth before claim replacement retains exact original ID and blocks mixed partner", async () => {
  const f = await fixture(), review = f.child(f.input("actor-review", "review-request"));
  await review.ready; review.go();
  expect(await review.done).toMatchObject({ code: 0, result: { ok: true, value: { actorId: "actor-review" } } });
  const replaced = await f.mesh.put({ key, value: f.claim.value, identity: caller });
  const security = f.child(f.input("actor-security", "security-request", 1, replaced.version));
  await security.ready; security.go();
  expect(await security.done).toMatchObject({ code: 0, result: { ok: false, reason: "occupied" } });
  const replay = await slotAdmissionFor(openMesh(f.mesh.root)).recoverable(f.binding.registryRoots[0]!);
  expect(replay).toMatchObject([{ actorId: "actor-review", state: "admitted" }]);
}, 30_000);

it.each([["exit-before-commit", 42, 0], ["exit-before-sync", 43, 1]] as const)(
  "process-crash window %s: native commit, not acceptance or registry existence, determines replay", async (mode, code, count) => {
    const f = await fixture(), actor = f.child(f.input("actor-crash", "crash-request"), mode);
    await actor.ready; actor.go();
    expect(await actor.done).toMatchObject({ code });
    expect(JSON.parse(fs.readFileSync(actor.receipt, "utf8"))).toMatchObject({ actorId: "actor-crash" });
    const receiver = slotAdmissionFor(openMesh(f.mesh.root));
    const recovered = await receiver.recoverable(f.binding.registryRoots[0]!);
    expect(recovered).toHaveLength(count);
    if (count) expect(recovered[0]).toMatchObject({ actorId: "actor-crash", state: "admitted" });
    expect(fs.existsSync(path.join(f.binding.registryRoots[0]!, "actors.json"))).toBe(false);
  }, 30_000);
