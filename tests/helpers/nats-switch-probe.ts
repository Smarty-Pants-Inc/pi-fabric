// Thin fixture mapping only. All send IDs, metrics, journal reads and stage
// extraction belong to the accepted shared probe. Lifecycle traces are entry-only.
import fs from "node:fs";
import path from "node:path";
import { installControlProbe } from "./nats-path-b-probe.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import type { SwitchProbeContext, SwitchSend } from "./nats-switch-runtime.js";

export type SwitchRoleSnapshot = ReturnType<Awaited<ReturnType<typeof installControlProbe>>["snapshot"]>;
export async function installSwitchProbe(input: SwitchProbeContext) {
  const role = input.options.probeRole ?? (input.options.name.endsWith("owner") ? "owner" : "sender");
  const hostId = `session:${input.options.sessionId}`;
  const traceFile = path.join(input.options.root, "checkpoint-trace.jsonl");
  const traces: Array<Record<string, unknown>> = [];
  let observationFault: unknown;
  const record = (body: Record<string, unknown>) => {
    const row = { at: Date.now(), ...body };
    traces.push(row);
    try {
      fs.appendFileSync(traceFile, `${JSON.stringify(row)}\n`, { mode: 0o600 });
      process.send?.({ event: "checkpoint-trace", result: row }, error => { if (error) observationFault ??= error; });
    } catch (error) { observationFault ??= error; }
  };
  const prototype = FabricControlPlane.prototype;
  const originalCheckpoint = prototype.checkpointForRelease;
  const originalReceipt = prototype.releaseCheckpointReceipt;
  const actualReceivers = new Set<FabricControlPlane>();
  let unsubscribeProviders: (() => void) | undefined;
  const unsubscribeCatalog = input.runtime.componentCatalog.subscribe(event => {
    try {
      if (event.name !== "fabric.provider.agents" || !event.current || unsubscribeProviders) return;
      unsubscribeProviders = input.runtime.registry.subscribeProviderChanges(change => {
        try {
          if (change.type !== "staged" && change.type !== "activated" || change.binding.name !== "agents") return;
          const control = (change.binding.provider as unknown as { control?: FabricControlPlane }).control;
          if (!control) throw new Error("actual agents provider has no control receiver");
          actualReceivers.add(control);
        } catch (error) { observationFault ??= error; }
      });
    } catch (error) { observationFault ??= error; }
  });
  let checkpoints = 0, readbacks = 0;
  const entry = (receiver: FabricControlPlane, entryKind: string, sequence: number) => {
    // Count before touching metadata; sticky faults invalidate negative claims.
    try {
      record({ stage: "entry", entryKind, sequence, hostId: receiver.options.hostId,
        identityId: receiver.identity.id, identityKind: receiver.identity.kind, incarnation: receiver.incarnation,
        meshRoot: receiver.mesh.root, transport: receiver.transportKind, stack: new Error(entryKind).stack });
    } catch (error) { observationFault ??= error; }
  };
  const checkpoint: typeof originalCheckpoint = function (this: FabricControlPlane, ...args) {
    entry(this, "checkpointForRelease", ++checkpoints);
    return Reflect.apply(originalCheckpoint, this, args);
  };
  const receipt: typeof originalReceipt = function (this: FabricControlPlane, ...args) {
    entry(this, "releaseCheckpointReceipt", ++readbacks);
    return Reflect.apply(originalReceipt, this, args);
  };
  prototype.checkpointForRelease = checkpoint;
  prototype.releaseCheckpointReceipt = receipt;
  const coverage = (afterShutdown = false) => {
    const actual = [...actualReceivers];
    const covered = !observationFault && actual.length === 1 && actual.every(receiver =>
      receiver.options.hostId === hostId && receiver.identity.id === hostId && receiver.mesh.root === input.options.meshRoot &&
      receiver.transportKind === input.options.config.mesh.transport &&
      (afterShutdown || receiver.mesh === input.runtime.mesh) && Object.getPrototypeOf(receiver) === prototype &&
      receiver.checkpointForRelease === checkpoint && receiver.releaseCheckpointReceipt === receipt) &&
      prototype.checkpointForRelease === checkpoint && prototype.releaseCheckpointReceipt === receipt;
    record({ stage: "coverage", covered, hostId, throughShutdown: afterShutdown,
      actualProviderReceiverCount: actual.length, fault: observationFault instanceof Error ? observationFault.message : observationFault ? String(observationFault) : null });
    if (!covered || observationFault) throw new Error("checkpoint trace actual receiver/prototype/lifetime coverage UNKNOWN");
    return { covered, installedBeforeInitialize: true, throughShutdown: afterShutdown, actualProviderReceiverMatched: true,
      receiverCount: actual.length, hostId, identityId: actual[0]!.identity.id, incarnation: actual[0]!.incarnation,
      meshRoot: actual[0]!.mesh.root, transport: actual[0]!.transportKind,
      entries: checkpoints, receiptReadbackEntries: readbacks, traces: [...traces] };
  };
  let probe: Awaited<ReturnType<typeof installControlProbe>>;
  try {
    probe = await installControlProbe({ runtime: input.runtime, context: input.context, invoke: input.invoke,
      role, evidenceFile: path.join(input.options.root, "control-probe-evidence.jsonl") });
  } catch (error) {
    prototype.checkpointForRelease = originalCheckpoint; prototype.releaseCheckpointReceipt = originalReceipt;
    unsubscribeCatalog(); unsubscribeProviders?.(); throw error;
  }
  return { ...probe,
    afterInitialize: () => coverage(),
    lifecycleSnapshot: (afterShutdown = false) => coverage(afterShutdown),
    async send(request: SwitchSend | Parameters<typeof probe.send>[0]) {
      if ("probeId" in request) return probe.send(request);
      if (request.ref !== "agents.followUp" && request.ref !== "agents.steer") throw new Error("switch probe requires followUp/steer");
      if (typeof request.args.id !== "string" || typeof request.args.message !== "string") throw new Error("switch probe requires target/message");
      return probe.send({ probeId: request.id, operation: request.ref === "agents.followUp" ? "followUp" : "steer",
        targetId: request.args.id, message: request.args.message, ...(request.args.data === undefined ? {} : { data: request.args.data }) });
    },
    async close() {
      // Fixture invokes this only after joined shutdown, never before lifecycle.
      try { await probe.close(); }
      finally {
        unsubscribeCatalog(); unsubscribeProviders?.();
        if (prototype.checkpointForRelease === checkpoint) prototype.checkpointForRelease = originalCheckpoint;
        if (prototype.releaseCheckpointReceipt === receipt) prototype.releaseCheckpointReceipt = originalReceipt;
      }
    },
  };
}
