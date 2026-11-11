/** Shared native #19998 B host-proof wiring. No test-driver message retries.
 * Run ONLY under the parent's owned smarty-reap / smarty-test-env scope.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, vi } from "vitest";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../../src/mesh/store.js";
import type { DeliveryOutcomeRecord } from "../../src/mesh/delivery-outcomes.js";
import { AgentsProvider } from "../../src/providers/agents-provider.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { LIVENESS_POLICY_KEY, type FabricHostLease } from "../../src/topology/host-leases.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { launchLog, same, startTime, stopAllOwned } from "./owned-processes.js";

export type ProofEvent = { event: string; pid: number; incarnation: string; at: number; [key: string]: unknown };
export type Ready = ProofEvent & { id: string; sessionId: string; sessionFile: string; processStartedAt: number;
  ownerIncarnation: string; confirmedAt: number; lease: FabricHostLease;
  heartbeatArmed: boolean; admission: { controlAdmission: string; controlAdmissionReason?: string } };
type Exit = { code: number | null; signal: NodeJS.Signals | null; at: number };
export type NativeChild = { process: ChildProcess; events: ProofEvent[]; stderr: string; exit?: Exit;
  next(event: string, timeoutMs?: number): Promise<ProofEvent>; send(line: string): void; closed: Promise<Exit> };
export type MainProofOptions = { lockTimeoutMs?: number; instrumentLeaseProof?: boolean };
export const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
// Mesh append/watch and native stdout/exit are normal event sources. This bounded
// 25 ms observation fallback is only for assertions on externally written files.
export const until = async (predicate: () => boolean, description: string, timeoutMs = 8_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(description);
    await delay(25);
  }
};
const jsonLines = (file: string): Array<Record<string, unknown>> => {
  try { return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
export const outcomes = (root: string): DeliveryOutcomeRecord[] => {
  const dir = path.join(root, "delivery-outcomes");
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter(name => /^\d{8}\.jsonl$/.test(name))
    .flatMap(name => jsonLines(path.join(dir, name))) as unknown as DeliveryOutcomeRecord[];
};
const nativeMessages = (file: string) => jsonLines(file).filter(entry =>
  entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message");
export const nonceCopies = (file: string, nonce: string) => nativeMessages(file).filter(entry => String(entry.content).includes(nonce));
const sha256 = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");

/** Register the original per-suite cleanup and bind receipts to the actual calling test. */
export function mainRootProofHarness(sourceTestUrl: string) {
  const active: Array<{ close(testState: string): Promise<void> }> = [];

  afterEach(async context => {
    try { for (const f of active.splice(0).reverse()) await f.close(context.task.result?.state ?? "unknown"); }
    finally { vi.unstubAllEnvs(); }
  });

  async function fixture(acknowledgementTimeoutMs = 5_000) {
    const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-followup-host-")));
    const meshRoot = path.join(scratch, "mesh");
    fs.mkdirSync(meshRoot);
    // An ESM bundle in scratch must resolve the already-installed host SDK. No install.
    fs.symlinkSync(path.resolve("node_modules"), path.join(scratch, "node_modules"), process.platform === "win32" ? "junction" : "dir");
    const module = path.join(scratch, "standin.mjs");
    buildSync({ entryPoints: [path.resolve("tests/fixtures/main-root-lease-followup-standin.ts")],
      bundle: true, packages: "external", platform: "node", format: "esm", outfile: module,
      banner: { js: 'import { createRequire as proofCreateRequire } from "node:module"; const require = proofCreateRequire(import.meta.url);' } });
    const sources = [
      path.relative(process.cwd(), fileURLToPath(import.meta.url)),
      path.relative(process.cwd(), fileURLToPath(sourceTestUrl)),
      "tests/fixtures/main-root-lease-followup-standin.ts", "tests/helpers/owned-processes.ts",
      "src/core/atomic-write.ts", "src/core/session-receipts.ts", "src/main-agent.ts",
      "src/mesh/custody-lock.ts", "src/mesh/mesh-lock.ts", "src/mesh/state-file.ts",
      "src/mesh/state-backend.ts", "src/mesh/event-log.ts", "src/mesh/store.ts",
      "src/mesh/writer-census.ts", "src/mesh/delivery-outcomes.ts",
      "src/providers/agents-provider.ts", "src/providers/agents-message-router.ts",
      "src/topology/control-plane.ts", "src/topology/host-leases.ts", "src/topology/host-lease-lock.ts",
      "src/topology/participant-directory.ts", "src/topology/types.ts",
    ].map(file => ({ file, sha256: sha256(file) }));
    const sourceManifestSha256 = createHash("sha256").update(JSON.stringify(sources)).digest("hex");
    const compiledFixtureSha256 = sha256(module);
    const sdk = ["@earendil-works/pi-coding-agent", "@earendil-works/pi-ai"].map(name => {
      const file = fs.realpathSync(fileURLToPath(import.meta.resolve(name)));
      let directory = path.dirname(file);
      for (;;) {
        const metadata = path.join(directory, "package.json");
        if (fs.existsSync(metadata)) {
          const pkg = JSON.parse(fs.readFileSync(metadata, "utf8"));
          if (pkg.name === name) return { name, file, sha256: sha256(file), metadata,
            metadataSha256: sha256(metadata), version: pkg.version as string,
            runtimeFiles: [path.join(path.dirname(file), name.endsWith("pi-coding-agent")
              ? "core/session-manager.js" : "providers/faux.js")].map(file => ({ file, sha256: sha256(file) })) };
        }
        const parent = path.dirname(directory);
        if (parent === directory) throw new Error(`SDK package metadata missing: ${name}`);
        directory = parent;
      }
    });
    const log = launchLog(scratch);
    const children: NativeChild[] = [];
    const nonces = new Map<string, { target: string; calls: number; result?: unknown }>();
    const observations: Record<string, unknown>[] = [];
    const identity: MeshIdentity = { id: `session:${randomUUID()}`, name: "proof-sender", kind: "main" };
    const mesh = new MeshStore(meshRoot, 65_536, 1_000, {
      stateBackend: "file", lockProtocol: 2, lockTimeoutMs: 35_000, staleLockMs: 90_000, readCacheMs: 0,
    });
    const observer = new ParticipantDirectory(mesh, {
      enabled: true, identity, rootId: identity.id, hostId: identity.id, reapDeadHosts: false,
    });
    const plane = new FabricControlPlane(mesh, identity, {
      enabled: true, hostId: identity.id, ownerIncarnation: observer.ownerIncarnation,
      pollMs: 20, acknowledgementTimeoutMs,
    });
    const local = new MainAgentController({} as ExtensionAPI, identity.id, true, scratch, identity.id.slice(8));
    // The real provider constructs the real router. Only unrelated task/actor lookup
    // ports are minimal: a session:<uuid> can never take those fallback paths.
    const provider = new AgentsProvider(
      { cwd: scratch, status: () => { throw new Error("Unknown Fabric agent"); } } as never,
      { identity, mesh, validateDirectMessage: () => {}, status: () => { throw new Error("Unknown Fabric actor"); } } as never,
      {} as never, local, observer, plane, {} as never, () => false, undefined, false,
    );
    const commands = (): MeshEvent[] => mesh.read({ topic: "fabric.control.command", limit: 1_000 });
    const acks = (): MeshEvent[] => mesh.read({ topic: "fabric.control.ack", limit: 1_000 });
    const start = (mode: "main" | "holder" | "reader", sessionFile?: string, options: MainProofOptions = {}): NativeChild => {
      const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("PI_FABRIC_")));
      const child = spawn(process.execPath, [module, mode, scratch, meshRoot, sessionFile ?? "", JSON.stringify(options)], {
        cwd: process.cwd(), env: { ...env, ...log.env, PI_FABRIC_MESH_STATE_BACKEND: "file", PI_CODING_AGENT_DIR: scratch,
          HOST_LEASE_PROOF_SOURCE_MANIFEST: sourceManifestSha256, HOST_LEASE_PROOF_COMPILED: compiledFixtureSha256 },
        stdio: ["pipe", "pipe", "pipe"], timeout: 90_000, killSignal: "SIGKILL",
      });
      const changed = new EventEmitter();
      const consumed = new Set<ProofEvent>();
      const state: NativeChild = { process: child, events: [], stderr: "",
        closed: new Promise<Exit>((resolve, reject) => {
          child.once("error", error => { changed.emit("changed"); reject(error); });
          child.once("close", (code, signal) => {
            state.exit = { code, signal, at: Date.now() };
            changed.emit("changed"); resolve(state.exit);
          });
        }),
        send: line => { child.stdin!.write(line + "\n"); },
        next: async (event, timeoutMs = 15_000) => {
          const deadline = Date.now() + timeoutMs;
          for (;;) {
            const found = state.events.find(item => item.event === event && !consumed.has(item));
            if (found) { consumed.add(found); return found; }
            if (state.exit) throw new Error(`Native ${child.pid} exited before ${event}: ${state.stderr}`);
            const left = deadline - Date.now();
            if (left <= 0) throw new Error(`Native ${child.pid}: missing ${event}: ${state.stderr}`);
            await new Promise<void>(resolve => {
              const wake = () => { clearTimeout(timer); changed.off("changed", wake); resolve(); };
              const timer = setTimeout(wake, left);
              changed.once("changed", wake);
            });
          }
        },
      };
      void state.closed.catch(() => {});
      let buffered = "";
      child.stdout!.on("data", chunk => {
        buffered += String(chunk);
        for (let end = buffered.indexOf("\n"); end >= 0; end = buffered.indexOf("\n")) {
          const line = buffered.slice(0, end); buffered = buffered.slice(end + 1);
          try { state.events.push(JSON.parse(line) as ProofEvent); }
          catch { state.stderr += `[stdout] ${line}\n`; }
        }
        changed.emit("changed");
      });
      child.stderr!.on("data", chunk => { state.stderr += String(chunk); });
      children.push(state);
      return state;
    };
    const stop = async (child: NativeChild) => {
      if (!child.exit) {
        const owned = log.owned().find(item => item.pid === child.process.pid);
        if (!owned) throw new Error(`No launch ownership receipt for ${child.process.pid}`);
        expect(same(owned)).toBe(true);
        await stopAllOwned([owned], 100, 2_000);
      }
      await Promise.race([child.closed, delay(3_000).then(() => { throw new Error("Owned native close not confirmed"); })]);
      expect(startTime(child.process.pid!)).toBe("");
    };
    const assertRuntime = async (child: NativeChild) => {
      const receipt = await child.next("runtime-receipt");
      expect(receipt.pid).toBe(child.process.pid);
      expect(receipt.incarnation).toBe(startTime(receipt.pid));
      expect(receipt.sourceManifestSha256).toBe(sourceManifestSha256);
      expect(receipt.compiledFixtureSha256).toBe(compiledFixtureSha256);
      expect(receipt.sdk).toEqual(sdk);
      expect(log.owned().some(item => item.pid === receipt.pid && item.started === receipt.incarnation)).toBe(true);
      return receipt;
    };
    const startMain = async (sessionFile?: string, options: MainProofOptions = {}) => {
      const child = start("main", sessionFile, options);
      const ready = await child.next("ready") as Ready;
      await assertRuntime(child);
      expect(ready.pid).toBe(child.process.pid);
      expect(ready.incarnation).toBe(startTime(ready.pid));
      expect(ready.ownerIncarnation).toBeTruthy();
      expect(ready.lease.expiresAt).toBeGreaterThan(ready.at);
      return { child, ready };
    };
    const send = (target: string, nonce: string) => {
      // This guard forbids the tempting test-driver repair, including same-key retries.
      if (nonces.has(nonce)) throw new Error(`Test-driver replay forbidden: ${nonce}`);
      const receipt: { target: string; calls: number; result?: unknown } = { target, calls: 1 };
      nonces.set(nonce, receipt);
      return provider.routeMessage(target, nonce, { nonce }, "followUp", undefined, { idempotencyKey: nonce })
        .then(value => { receipt.result = { ok: true, value }; return receipt.result; }, error => {
          receipt.result = { ok: false, code: error.code, message: error.message }; return receipt.result;
        });
    };
    const f = { scratch, meshRoot, mesh, observer, plane, start, startMain, assertRuntime, stop, send, commands, acks, observations,
      async close(testState: string) {
        let cleanupError: unknown;
        try {
          await stopAllOwned(log.owned(), 300, 2_000);
          await Promise.all(children.map(child => Promise.race([child.closed, delay(3_000).then(() => {
            throw new Error(`No native close event for ${child.process.pid}`);
          })])));
        } catch (error) { cleanupError = String(error); }
        await plane.close();
        await observer.close();
        const allCommands = commands(), allAcks = acks(), finalOutcomes = outcomes(meshRoot);
        const sessions = [...new Set(children.flatMap(child => child.events.flatMap(event =>
          typeof event.sessionFile === "string" ? [event.sessionFile] : [])))];
        const sourcesAfter = sources.map(({ file }) => ({ file, sha256: sha256(file) }));
        const sourceStable = JSON.stringify(sourcesAfter) === JSON.stringify(sources);
        const summary = {
          kind: "main-root-lease-followup-host-proof", version: 1, testState, scratch, cleanupError,
          sources, sourcesAfter, sourceStable, sourceManifestSha256, compiledFixtureSha256, sdk,
          launches: log.owned(),
          processes: children.map(child => ({ pid: child.process.pid, exit: child.exit,
            nativeStillSame: log.owned().filter(entry => entry.pid === child.process.pid).some(same),
            events: child.events, stderr: child.stderr })),
          observations, controlCommands: allCommands, controlAcknowledgements: allAcks,
          nonces: [...nonces].map(([nonce, request]) => {
            const sends = allCommands.filter(event => (event.data as { message?: string }).message === nonce);
            const receipts = sessions.flatMap(file => nonceCopies(file, nonce).map(entry => ({ file, entry,
              sessionId: jsonLines(file).find(line => line.type === "session")?.id })));
            const sendIds = new Set(sends.map(event => event.id));
            const commandIds = new Set(sends.map(event => (event.data as { commandId: string }).commandId));
            const terminal = finalOutcomes.filter(item => sendIds.has(item.eventId));
            const exact = receipts.filter(receipt => `session:${receipt.sessionId}` === request.target);
            return { nonce, ...request, nativeReceipts: receipts, outcomes: terminal,
              controlReceipts: allAcks.filter(event => commandIds.has((event.data as { commandId: string }).commandId)),
              disposition: receipts.length > exact.length ? "WRONG_SESSION" : receipts.length > 1 ? "DUPLICATED" :
                exact.length === 1 && terminal.some(item => item.outcome === "delivered") ? "native-delivered-confirmed" :
                  exact.length === 1 ? "NATIVE_ENTRY_WITHOUT_FINAL_DURABILITY_RECEIPT" : "NOT_DELIVERED_NO_DURABLE_PENDING_PROOF", sends };
          }),
        };
        fs.writeFileSync(path.join(scratch, "proof-summary.json"), JSON.stringify(summary, null, 2) + "\n");
        console.info("HOST_PROOF_JSON " + JSON.stringify(summary));
        mesh.closeState();
        // Retain native sessions, launch/exit, command/ACK and custody diagnostics for
        // the parent's pin. No test deletes shared data or even its own proof artifacts.
        if (cleanupError) throw new Error(`Host proof teardown unconfirmed: ${cleanupError}; receipts: ${scratch}`);
        if (!sourceStable) throw new Error(`Host proof source changed during native run; receipts: ${scratch}`);
      },
    };
    active.push(f); // Register teardown before the first native process can start.
    await mesh.put({ key: LIVENESS_POLICY_KEY, identity,
      value: { version: 1, hostLeases: "files" } });
    plane.start(() => ({ accepted: false }));
    return f;
  }

  return fixture;
}
