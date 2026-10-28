// Keep: B5's durable outbox/receiver trust boundary and data-safety crash window.
// Offline native-boundary regression only: no real Pi turn, external effect, live NATS,
// AgentsProvider startup hook, cluster qualification, or 200-session qualification.
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

type WorkerEvent = { kind: string; [key: string]: unknown };
type WorkerExit = { code: number | null; signal: NodeJS.Signals | null };
type Waiter = {
  kind: string;
  resolve: (event: WorkerEvent) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};
type WorkerHandle = {
  child: ChildProcess;
  events: WorkerEvent[];
  stderr: () => string;
  waitFor(kind: string, timeoutMs?: number): Promise<WorkerEvent>;
  send(command: Record<string, unknown>): void;
  kill(): Promise<WorkerExit>;
  close(): Promise<WorkerExit>;
};

const workerFile = path.resolve("tests/fixtures/scripted-input-worker.ts");
const roots: string[] = [];
const workers: WorkerHandle[] = [];
const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "scripted-input-process-"));
  roots.push(root);
  return root;
};

const startWorker = (mode: string, input: Record<string, unknown>): WorkerHandle => {
  const child = spawn("bun", ["run", workerFile, mode, JSON.stringify(input)], {
    cwd: process.cwd(),
    stdio: ["pipe", "pipe", "pipe"],
  });
  const events: WorkerEvent[] = [];
  const waiters = new Set<Waiter>();
  let buffered = "";
  let stderr = "";
  let startupError: Error | undefined;
  const closed = new Promise<WorkerExit>((resolve) => {
    child.once("close", (code, signal) => {
      resolve({ code, signal });
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error(`worker exited before ${waiter.kind} (code=${code}, signal=${signal})\n${stderr}`));
      }
      waiters.clear();
    });
    child.once("error", (error) => {
      startupError = error;
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error(`worker spawn failed: ${error.message}\n${stderr}`));
      }
      waiters.clear();
    });
  });
  const dispatch = (event: WorkerEvent): void => {
    events.push(event);
    for (const waiter of waiters) {
      if (waiter.kind !== event.kind) continue;
      clearTimeout(waiter.timer);
      waiters.delete(waiter);
      waiter.resolve(event);
      break;
    }
  };
  child.stdout!.on("data", (chunk: Buffer) => {
    buffered += chunk.toString("utf8");
    for (let end = buffered.indexOf("\n"); end >= 0; end = buffered.indexOf("\n")) {
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 1);
      try { dispatch(JSON.parse(line) as WorkerEvent); }
      catch { stderr += `[stdout] ${line}\n`; }
    }
  });
  child.stderr!.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString("utf8")).slice(-32_768); });
  const handle: WorkerHandle = {
    child,
    events,
    stderr: () => stderr,
    waitFor: (kind, timeoutMs = 20_000) => {
      const existing = events.findIndex((event) => event.kind === kind);
      if (existing >= 0) return Promise.resolve(events.splice(existing, 1)[0]!);
      if (startupError) return Promise.reject(new Error(`worker spawn failed: ${startupError.message}\n${stderr}`));
      if (child.exitCode !== null || child.signalCode !== null) {
        return Promise.reject(new Error(`worker exited before ${kind} (code=${child.exitCode}, signal=${child.signalCode})\n${stderr}`));
      }
      return new Promise((resolve, reject) => {
        const waiter: Waiter = {
          kind,
          resolve,
          reject,
          timer: setTimeout(() => {
            waiters.delete(waiter);
            reject(new Error(`worker did not emit ${kind} within ${timeoutMs} ms\n${stderr}`));
          }, timeoutMs),
        };
        waiters.add(waiter);
      });
    },
    send: (command) => {
      if (child.stdin!.destroyed) throw new Error("worker stdin is closed");
      child.stdin!.write(`${JSON.stringify(command)}\n`);
    },
    kill: async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      return closed;
    },
    close: async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.stdin!.end(`${JSON.stringify({ command: "exit" })}\n`);
      }
      return closed;
    },
  };
  workers.push(handle);
  return handle;
};

const sessionFacts = (sessionFile: string, deliveryId: string, inputId: string) => {
  // Native persisted SessionManager bytes, not child-process counters or a copied journal.
  const entries = fs.readFileSync(sessionFile, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line) as {
    customType?: string;
    details?: Record<string, any>;
  });
  const carriers = entries.filter((entry) => entry.customType === "pi-fabric-agent-message" &&
    entry.details?.deliveryId === deliveryId);
  const outcomes = entries.filter((entry) => entry.customType === "pi-fabric-scripted-input-result" &&
    entry.details?.inputId === inputId);
  return { entries, carriers, outcomes };
};

const deliveryRequest = (item: Record<string, any>) => ({
  from: item.from,
  message: item.message,
  data: item.data,
  delivery: item.delivery,
  ...(item.priority === undefined ? {} : { priority: item.priority }),
  ...(item.triggerTurn === undefined ? {} : { triggerTurn: item.triggerTurn }),
});

afterEach(async () => {
  for (const worker of workers.splice(0)) await worker.kill();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("scripted input native process recovery", () => {
  it("recovers the prepared offer across sender/receiver SIGKILL, physical move, and ACK without a duplicate carrier or outcome", async () => {
    const root = tempRoot();
    const senderRoot = path.join(root, "sender-state");
    const receiverRoot = path.join(root, "retained-receiver-state");
    const sessionId = `scripted-session-${Date.now()}`;
    const targetId = `session:${sessionId}`;
    const from = { id: "scripted-driver", name: "Scripted driver", kind: "actor" };
    const inputId = `round-${sessionId}`;
    const deadlineAt = Date.now() + 10 * 60_000;
    const message = "Process the original scripted input once";
    const delivery = "followUp";
    const triggerTurn = false;
    const data = {
      payload: { round: inputId, operation: "record-completion" },
      scriptedInput: { version: 1, inputId, sessionId, deadlineAt },
    };

    // Crash-window regression path: durably prepare, then remain before publish so the
    // parent can kill the actual sender process without a publish or driver reconnect.
    const firstSender = startWorker("prepare-and-wait", { root: senderRoot, from, targetId, message, data, delivery, triggerTurn });
    const preparedEvent = await firstSender.waitFor("prepared");
    const prepared = preparedEvent.prepared as Record<string, any>;
    expect(prepared).toMatchObject({ message, deadlineAt, delivery, triggerTurn, data: { scriptedInput: { version: 1, inputId, sessionId, deadlineAt } } });
    const senderKilled = await firstSender.kill();
    expect(senderKilled.signal).toBe("SIGKILL");
    expect(firstSender.events.some((event) => event.kind === "published")).toBe(false);

    // A fresh sender process reads the immutable prepared bytes; there is no driver prompt or
    // new prepare call here. A published but unacknowledged item must remain in this outbox.
    const sender = startWorker("resume-sender", { root: senderRoot, from, sessionId });
    const pendingEvent = await sender.waitFor("pending");
    const pending = pendingEvent.items as Array<Record<string, any>>;
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      ...prepared,
      from,
      targetId,
      published: false,
    });
    const originalOffer = pending[0]!;
    sender.send({ command: "publish", inputId });
    const publishedEvent = await sender.waitFor("published");
    expect(publishedEvent.item).toMatchObject({ ...originalOffer, published: true });

    // The real MainAgentController calls this fixture's Pi host adapter. It appends the native
    // carrier and one offline deterministic processing result through the real SessionManager,
    // confirms both with withConfirmedSessionFile, then blocks before admitScriptedInput replies.
    const firstReceiver = startWorker("admit", {
      root: receiverRoot,
      sessionId,
      physicalMachineId: "physical-machine-before",
      request: deliveryRequest(originalOffer),
      holdAfterNativeAppend: true,
    });
    const append = await firstReceiver.waitFor("carrier-confirmed");
    const sessionFile = String(append.sessionFile);
    expect(append.physicalMachineId).toBe("physical-machine-before");
    expect(firstReceiver.events.some((event) => event.kind === "admission-result")).toBe(false);
    const killedReceiver = await firstReceiver.kill();
    expect(killedReceiver.signal).toBe("SIGKILL");
    expect(firstReceiver.events.some((event) => event.kind === "admission-result")).toBe(false);

    const inputMetadata = (originalOffer.data as typeof data).scriptedInput;
    const mainJournal = path.join(receiverRoot, "mesh", "main-followups", `${sessionId}.json`);
    expect(fs.readFileSync(mainJournal, "utf8")).toContain(String(originalOffer.idempotencyKey));
    let facts = sessionFacts(sessionFile, String(originalOffer.idempotencyKey), inputMetadata.inputId);
    expect(facts.carriers).toHaveLength(1);
    expect(facts.outcomes).toHaveLength(1);
    expect(facts.carriers[0]!.details?.data?.scriptedInput).toEqual(inputMetadata);
    expect(facts.outcomes[0]!.details).toMatchObject({
      inputId: inputMetadata.inputId,
      sessionId,
      deadlineAt: inputMetadata.deadlineAt,
      delivery,
      triggerTurn,
      outcome: "completed",
    });

    // The durable publish receipt is not an application ACK: it remains pending until native
    // Main admission returns after the receiver restart.
    sender.send({ command: "snapshot" });
    const stillPending = await sender.waitFor("pending-snapshot");
    expect(stillPending.items).toMatchObject([{ ...originalOffer, published: true }]);

    const receiver = startWorker("admit", {
      root: receiverRoot,
      sessionId,
      sessionFile,
      physicalMachineId: "physical-machine-after",
      request: deliveryRequest(originalOffer),
      holdAfterNativeAppend: false,
    });
    const admission = await receiver.waitFor("admission-result");
    expect(admission.physicalMachineId).toBe("physical-machine-after");
    expect(admission.sessionFile).toBe(sessionFile);
    expect(admission.result).toMatchObject({ queued: true, duplicate: true });
    facts = sessionFacts(sessionFile, String(originalOffer.idempotencyKey), inputMetadata.inputId);
    expect(facts.carriers).toHaveLength(1);
    expect(facts.outcomes).toHaveLength(1);
    expect(facts.carriers[0]!.details?.data?.scriptedInput).toEqual(inputMetadata);
    expect(facts.outcomes[0]!.details?.deadlineAt).toBe(deadlineAt);

    sender.send({ command: "acknowledge", result: admission.result });
    const acknowledged = await sender.waitFor("acknowledged");
    expect(acknowledged.inputId).toBe(inputMetadata.inputId);
    sender.send({ command: "exit" });
    expect((await sender.close()).code).toBe(0);

    // Sender restart after native ACK has no pending input. An additional receiver retry using
    // the original offer is also a duplicate at Main, with the persisted carrier/outcome unchanged.
    const restartedSender = startWorker("list-pending", { root: senderRoot, from });
    expect((await restartedSender.waitFor("pending")).items).toEqual([]);
    expect((await restartedSender.close()).code).toBe(0);

    const replay = startWorker("admit", {
      root: receiverRoot,
      sessionId,
      sessionFile,
      physicalMachineId: "physical-machine-after-ack",
      request: deliveryRequest(originalOffer),
      holdAfterNativeAppend: false,
    });
    expect((await replay.waitFor("admission-result")).result).toMatchObject({ queued: true, duplicate: true });
    facts = sessionFacts(sessionFile, String(originalOffer.idempotencyKey), inputMetadata.inputId);
    expect(facts.carriers).toHaveLength(1);
    expect(facts.outcomes).toHaveLength(1);
    expect(facts.carriers[0]!.details?.deliveryId).toBe(originalOffer.idempotencyKey);
    expect(facts.outcomes[0]!.details?.deadlineAt).toBe(deadlineAt);
  }, 60_000);
});
