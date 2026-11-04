import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { expect, it } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { sameActorSpec } from "../src/main-drain/actor-restore.js";
import { clearBinding } from "../src/main-drain/binding.js";
import { MainDrainControl } from "../src/main-drain/control.js";
import { mainInboxDrainGate, reopenMainInbox } from "../src/main-drain/gate.js";
import { releaseNativeInput } from "../src/main-drain/native-port.js";
import { MainDrainServer } from "../src/main-drain/server.js";
import { MeshStore } from "../src/mesh/store.js";
import { createSeat, ROOT, runSeam, SID } from "./seat-drain-fixture.js";

const linux = process.platform === "linux" && typeof process.getuid === "function";

// Keep: data safety; reproduces pi-fabric#1810 judge F1 at the shipped CLI / owner-socket boundary.
// Native Pi admission remains a fixture, with busy/idle driven by a bounded real Node child (not a Pi turn).
it.skipIf(!linux)("compiled drain routes a session supervisor through either directory manager, preserving scope on undo and cutover", async () => {
  for (const defaultScope of ["project", "session"] as const) {
    for (const recovery of ["undo", "cutover"] as const) {
      const seat = await createSeat();
      const identity = { id: ROOT, name: "Main", kind: "main" as const, sessionId: SID };
      const agents = new AgentManager(seat.base, { ...DEFAULT_FABRIC_CONFIG.agents, model: "test/project", thinking: "low" },
        { runRoot: path.join(seat.base, "runs") });
      const mesh = new MeshStore(seat.meshRoot, 256 * 1024, 100);
      const roots = { project: path.join(seat.meshRoot, "actors", "project"), session: path.join(seat.meshRoot, "actors", SID) };
      const directory = () => new ActorDirectory([SID, identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
        persistent: true, rootId: ROOT, claimResidency: "session", reapDeadSessionPresence: false,
      }], roots, defaultScope);
      let actors = directory();
      seat.host.actors = () => actors;
      let busy: ReturnType<typeof spawn> | undefined;
      let busyEnded: Promise<unknown> | undefined;
      try {
        // Use the production build, not installedSeam's test-local esbuild bundle.
        const exe = path.join(seat.base, "fabric-seat-drain");
        const installed = spawnSync("node", [path.resolve("dist/seat-drain/fabric-seat-drain.cjs"), "--install", "--out", exe], { encoding: "utf8" });
        expect(installed.status, installed.stderr).toBe(0);
        expect(JSON.parse(installed.stdout)).toMatchObject({ owner: "fabric-v2", interpreter: { rootOwned: true } });
        const actor = await actors.create({ name: "supervisor", scope: "session", residency: "session",
          instructions: "Supervise this Main and preserve the complete configuration across drain.",
          responseMode: "directive", events: ["agent_settled"], delivery: "steer", triggerTurn: true, tools: ["read"], nice: 3,
          model: "test/project", modelReason: "Project default", thinking: "low" });
        await actors.setModel(actor.id, "test/live-override", "session", undefined, "Saved live override #18491");
        await actors.setThinking(actor.id, "high", "session");
        const definition = actors.definition(actor.id);
        expect(actors.status(actor.id)).toMatchObject({ status: "idle", scope: "session", supervisorFor: ROOT });
        expect(actors.drainQuiescence(actor.id)).toEqual([]);
        expect(actors.drainWorker(actor.id)).toBeNull();
        const trace = JSON.parse(fs.readFileSync(path.join(seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
        seat.native.state.sessionFile = trace; seat.native.state.leafId = "leaf";
        const traceBefore = fs.readFileSync(trace);
        const phase = async (name: string, inputs: Record<string, unknown> = {}) => {
          const result = await runSeam(seat, exe, name, seat.request(name, inputs));
          expect({ code: result.code, stderr: result.stderr }).toEqual({ code: 0, stderr: "" });
          return { ...result, receipt: JSON.parse(result.stdout) };
        };
        seat.native.state.busy = true; seat.native.state.idle = false;
        busy = spawn("node", ["-e", "process.stdout.write('busy\\n'); setTimeout(() => {}, 400)"], { stdio: ["ignore", "pipe", "pipe"] });
        busyEnded = once(busy, "close").then(([code]) => {
          seat.native.state.busy = false; seat.native.state.idle = true;
          expect(code).toBe(0);
        });
        await once(busy.stdout!, "data");
        const drained = await phase("drain");
        await busyEnded;
        const outcome = JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-drain-outcome.json"), "utf8"));
        expect(drained.receipt, JSON.stringify({ defaultScope, recovery, outcome }))
          .toMatchObject({ result: "pass", observed: { childrenTerminal: true, inputsFenced: true } });
        expect(actors.status(actor.id)).toMatchObject({ status: "stopped", scope: "session" });
        const savedFile = path.join(seat.runDir, `fabric-v2-drain-actor-${actor.id}.json`);
        const saved = JSON.parse(fs.readFileSync(savedFile, "utf8"));
        expect(saved).toMatchObject({ schema: "fabric-v2-drain-actor-v1", id: actor.id, rootId: ROOT,
          spec: { name: "supervisor", rootId: ROOT, residency: "session", scope: "session", instructions: definition.instructions,
            model: "test/project", modelReason: "Project default", thinking: "low" } });
        expect(saved.spec.scope).toBe("session");
        expect(saved.spec.resolvedBinding).toEqual({ modelSource: "explicit", model: "test/live-override",
          modelReason: "Saved live override #18491", thinking: "high" });
        if (recovery === "undo") {
          expect((await phase("drain-undo", { attempt: 1, drainReceiptSha256: null })).receipt)
            .toMatchObject({ result: "pass", observed: { inputsFenced: false, admissionRestored: true } });
        } else {
          fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
          fs.writeFileSync(path.join(seat.runDir, "receipts", "drain.json"), drained.stdout);
          fs.writeFileSync(path.join(seat.runDir, "receipts", "fence.json"), JSON.stringify({ schema: "d3-seam-receipt-v1", run: seat.run,
            phase: "fence", owner: "synthetic-fence-owner", result: "pass", observed: {} }));
          expect((await phase("drain-restore")).receipt.result).toBe("refused");
          await seat.server.close(); await actors.close();
          releaseNativeInput(ROOT); reopenMainInbox(ROOT); clearBinding(ROOT);
          ((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.main.drain-slot.v1")] as Map<string, unknown>).delete(ROOT);
          actors = directory();
          seat.server = new MainDrainServer(new MainDrainControl(seat.host));
          expect(await seat.server.start()).toBe("listening");
          expect((await phase("drain-restore")).receipt).toMatchObject({ result: "pass", observed: { actorsRestored: true } });
          expect((await phase("drain-restore")).receipt.result).toBe("pass");
        }
        const restored = actors.listOwned().filter(row => row.name === "supervisor");
        expect(restored).toHaveLength(1);
        expect(restored[0]).toMatchObject({ status: "idle", scope: "session", supervisorFor: ROOT,
          model: "test/live-override", thinking: "high", modelReason: "Saved live override #18491" });
        expect(restored[0]!.id).not.toBe(actor.id);
        expect(actors.definition(restored[0]!.id)).toEqual(definition);
        expect(sameActorSpec(saved.spec, actors.drainSpec(restored[0]!.id))).toBe(true);
        const sessionRows = JSON.parse(fs.readFileSync(path.join(roots.session, "actors.json"), "utf8")).actors;
        expect(sessionRows.find((row: { id: string }) => row.id === restored[0]!.id)?.status).toBe("idle");
        const projectFile = path.join(roots.project, "actors.json");
        expect(fs.existsSync(projectFile) ? JSON.parse(fs.readFileSync(projectFile, "utf8")).actors : []).toEqual([]);
        expect(mainInboxDrainGate(ROOT)).toBeUndefined();
        expect(seat.native.state.fenced).toBe(false);
        expect(fs.readFileSync(trace)).toEqual(traceBefore);
        console.info("directory-drain evidence", JSON.stringify({ defaultScope, recovery,
          drainResult: drained.receipt.result, savedScope: saved.spec.scope, restoredScope: restored[0]!.scope,
          restoredModel: restored[0]!.model, restoredThinking: restored[0]!.thinking,
          sameSpec: sameActorSpec(saved.spec, actors.drainSpec(restored[0]!.id)),
          traceSha256: createHash("sha256").update(fs.readFileSync(trace)).digest("hex"),
          traceBeforeSha256: createHash("sha256").update(traceBefore).digest("hex") }));
      } finally {
        if (busyEnded) await busyEnded;
        await actors.close(); await agents.close(); await seat.close();
      }
    }
  }
}, 120_000);
