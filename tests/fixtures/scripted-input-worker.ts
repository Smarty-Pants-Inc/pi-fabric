import { createInterface } from "node:readline";
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../../src/main-agent.js";
import { withConfirmedSessionFile } from "../../src/core/session-receipts.js";
import {
  acknowledgeScriptedInput,
  admitScriptedInput,
  listPendingScriptedInputs,
  markScriptedInputPublished,
  prepareScriptedInput,
} from "../../src/recovery/input-admission.js";

const mode = process.argv[2];
const input = JSON.parse(process.argv[3] ?? "{}") as Record<string, any>;
const lines = createInterface({ input: process.stdin });
const commands: Array<Record<string, any>> = [];
const commandWaiters: Array<(command: Record<string, any>) => void> = [];
lines.on("line", (line) => {
  try {
    const command = JSON.parse(line) as Record<string, any>;
    const waiter = commandWaiters.shift();
    if (waiter) waiter(command);
    else commands.push(command);
  } catch { /* Invalid fixture commands do not become product input. */ }
});
const nextCommand = (): Promise<Record<string, any>> => {
  const queued = commands.shift();
  if (queued) return Promise.resolve(queued);
  return new Promise((resolve) => commandWaiters.push(resolve));
};
const emit = (event: Record<string, unknown>): void => {
  fs.writeSync(1, `${JSON.stringify(event)}\n`);
};
const finish = (): void => { lines.close(); };

const confirmCustomEntry = (
  sessionFile: string,
  customType: string,
  matches: (entry: Record<string, any>) => boolean,
): Record<string, any> => {
  let receipt: Record<string, any> | undefined;
  const confirmed = withConfirmedSessionFile(sessionFile, (fd) => {
    const entries = fs.readFileSync(fd, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line) as Record<string, any>);
    const found = entries.filter((entry) => entry.customType === customType && matches(entry));
    if (found.length !== 1) throw new Error(`expected one confirmed ${customType} entry, found ${found.length}`);
    receipt = found[0];
  });
  if (!confirmed || !receipt) throw new Error(`native SessionManager has no confirmed ${customType} receipt`);
  return receipt;
};

const run = async (): Promise<void> => {
  if (mode === "prepare-and-wait") {
    const prepared = await prepareScriptedInput({
      root: input.root,
      from: input.from,
      targetId: input.targetId,
      message: input.message,
      data: input.data,
      delivery: input.delivery,
      ...(input.priority === undefined ? {} : { priority: input.priority }),
      ...(input.triggerTurn === undefined ? {} : { triggerTurn: input.triggerTurn }),
    });
    emit({ kind: "prepared", prepared });
    await nextCommand(); // Only the parent can allow publication; the crash case kills us here.
    finish();
    return;
  }

  if (mode === "list-pending") {
    const items = await listPendingScriptedInputs({ root: input.root, from: input.from });
    emit({ kind: "pending", items });
    finish();
    return;
  }

  if (mode === "resume-sender") {
    const query = { root: input.root, from: input.from };
    const items = await listPendingScriptedInputs(query) as unknown as Array<Record<string, any>>;
    emit({ kind: "pending", items });
    for (;;) {
      const command = await nextCommand();
      if (command.command === "publish") {
        const item = items.find((candidate: Record<string, any>) => candidate.data?.scriptedInput?.inputId === command.inputId);
        if (!item) throw new Error(`prepared scripted input ${String(command.inputId)} is not pending`);
        await markScriptedInputPublished({
          root: input.root,
          from: input.from,
          sessionId: item.data.scriptedInput.sessionId,
          inputId: item.data.scriptedInput.inputId,
        });
        emit({ kind: "published", item: { ...item, published: true } });
        continue;
      }
      if (command.command === "snapshot") {
        emit({ kind: "pending-snapshot", items: await listPendingScriptedInputs(query) });
        continue;
      }
      if (command.command === "acknowledge") {
        const item = items[0];
        if (!item) throw new Error("cannot acknowledge without the prepared pending input");
        await acknowledgeScriptedInput({
          root: input.root,
          from: input.from,
          sessionId: item.data.scriptedInput.sessionId,
          inputId: item.data.scriptedInput.inputId,
          result: command.result,
        });
        emit({ kind: "acknowledged", inputId: item.data.scriptedInput.inputId });
        continue;
      }
      if (command.command === "exit") { finish(); return; }
    }
  }

  if (mode === "admit") {
    const sessionDirectory = path.join(input.root, "sessions");
    fs.mkdirSync(sessionDirectory, { recursive: true, mode: 0o700 });
    const manager = input.sessionFile
      ? SessionManager.open(input.sessionFile, sessionDirectory, input.root)
      : SessionManager.create(input.root, sessionDirectory);
    const handlers = new Map<string, Array<(event: unknown, context: ExtensionContext) => void>>();
    const pi = {
      on: (name: string, handler: (event: unknown, context: ExtensionContext) => void) => {
        handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      },
      // Offline native-boundary regression only: no real Pi turn, external effect, or live NATS.
      // This is the real MainAgentController -> host boundary. The deterministic fixture records
      // one processing result per handoff and deliberately contains no deduper.
      sendMessage: (message: { content?: string; details: Record<string, any> }, options?: { deliverAs?: string; triggerTurn?: boolean }) => {
        manager.appendCustomMessageEntry("pi-fabric-agent-message", message.content ?? "scripted input", true, message.details);
        manager.appendMessage(fauxAssistantMessage("native scripted input carrier appended"));
        const sessionFile = manager.getSessionFile();
        if (!sessionFile) throw new Error("native Main did not create its SessionManager file");
        const carrier = confirmCustomEntry(sessionFile, "pi-fabric-agent-message", (entry) =>
          entry.details?.deliveryId === message.details.deliveryId);

        const metadata = carrier.details?.data?.scriptedInput;
        if (!metadata || typeof metadata.inputId !== "string") throw new Error("native carrier has no scripted input identity");
        manager.appendCustomMessageEntry("pi-fabric-scripted-input-result", "completed", true, {
          inputId: metadata.inputId,
          sessionId: metadata.sessionId,
          deadlineAt: metadata.deadlineAt,
          outcome: "completed",
          delivery: options?.deliverAs,
          triggerTurn: options?.triggerTurn,
          deliveryId: carrier.details.deliveryId,
        });
        manager.appendMessage(fauxAssistantMessage("offline scripted processing result appended"));
        confirmCustomEntry(sessionFile, "pi-fabric-scripted-input-result", (entry) =>
          entry.details?.inputId === metadata.inputId && entry.details?.deliveryId === carrier.details.deliveryId);

        if (input.holdAfterNativeAppend === true) {
          emit({
            kind: "carrier-confirmed",
            physicalMachineId: input.physicalMachineId,
            sessionFile,
            details: message.details,
          });
          // A local IPC failpoint: the parent SIGKILLs this actual process after native namespace
          // confirmation and before MainAgentController.deliverAgent / admitScriptedInput returns.
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 60_000);
        }
      },
    } as unknown as ExtensionAPI;
    const context = {
      isIdle: () => true,
      hasPendingMessages: () => false,
      sessionManager: manager,
    } as unknown as ExtensionContext;
    const targetId = `session:${input.sessionId}`;
    const main = new MainAgentController(pi, targetId, true, input.root, input.sessionId);
    const journal = path.join(input.root, "mesh", "main-followups", `${input.sessionId}.json`);
    main.attachFollowUpDrain(context, 60_000, journal);
    try {
      const result = await admitScriptedInput({
        root: input.root,
        sessionId: input.sessionId,
        mainAgent: main,
        request: input.request,
      });
      emit({
        kind: "admission-result",
        physicalMachineId: input.physicalMachineId,
        sessionFile: manager.getSessionFile(),
        result,
      });
    } finally {
      main.closeFollowUpDrain();
    }
    finish();
    return;
  }

  throw new Error(`unknown worker mode: ${String(mode)}`);
};

await run();
