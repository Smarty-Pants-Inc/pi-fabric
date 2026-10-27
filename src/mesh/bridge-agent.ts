import { Readable, Writable } from "node:stream";
import type { MessagePort, Worker } from "node:worker_threads";
import { BRIDGE_PROTOCOL_VERSION, bridgeRpcLines, bridgeTailBoundary, serveBridgeAgent, type BridgeRead, type RpcRequest, type StoreBridgeSide } from "./bridge.js";
import type { MeshStoreOptions } from "./store.js";

export interface BridgeAgentWorkerData {
  root: string;
  peer: string;
  maxEventBytes: number;
  maxReadEvents: number;
  storeOptions: Pick<MeshStoreOptions, "lockProtocol" | "maxStateBytes">;
  debug?: boolean;
}

/** Worker adapter, also usable by a test-only bootstrap that instruments its own store.
 * Receipt times come from the owning stdio thread, never from peer-controlled RPC arguments. */
export const serveBridgeAgentOnPort = async (side: StoreBridgeSide, port: MessagePort,
  debug?: (message: string) => void): Promise<void> => {
  const receipts: number[] = [];
  const input = new Readable({ read() {} });
  const output = new Writable({ write(chunk, _encoding, done) {
    try { port.postMessage(String(chunk).trimEnd()); done(); }
    catch (error) { done(error as Error); }
  } });
  const receive = (message: { line?: string; receivedAt?: number; end?: boolean }): void => {
    if (message.end) { input.push(null); return; }
    if (typeof message.line !== "string" || !Number.isFinite(message.receivedAt)) {
      input.destroy(new Error("Invalid bridge worker request"));
      return;
    }
    receipts.push(message.receivedAt!);
    input.push(`${message.line}\n`);
  };
  const closed = (): void => { input.push(null); };
  port.on("message", receive);
  port.on("close", closed);
  try { await serveBridgeAgent(side, input, output, debug, () => receipts.shift()!); }
  finally {
    port.off("message", receive);
    port.off("close", closed);
    input.destroy();
    output.end();
    side.store.closeState();
    port.close();
  }
};

type Reply = { id?: unknown; ok?: boolean; result?: unknown; notification?: string; kind?: string };
type Mailbox = { after: number; offset?: number; generation: number; reply?: Reply };
const noProgress = (slot: Mailbox): boolean => {
  const page = slot.reply?.ok === true ? slot.reply.result as BridgeRead : undefined;
  return !!page && Array.isArray(page.events) && !page.events.length && !page.skipped?.length &&
    page.through === slot.after && page.offset === slot.offset;
};
const STOP_MS = 2_000;
// Completion is the normal path; this timer only bounds an idle admission that stalls.
const TAIL_RESPONSE_GRACE_MS = 50;

/** The stdio owner does no store/filesystem work. At most one exact-cursor tail page is
 * pending or ready; another boundary cannot evict it. All real operations reuse worker RPC. */
export const serveBridgeAgentWorker = (worker: Worker, input: Readable, output: Writable): Promise<void> =>
  new Promise((resolve, reject) => {
    let next = 1;
    let mailbox: Mailbox | undefined;
    let changes = false;
    let busyTail = false;
    let generation = 0;
    let stopping = false;
    let settled = false;
    let failure: Error | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let tailTimer: ReturnType<typeof setTimeout> | undefined;
    const pending = new Map<number, (response: Reply) => void>();
    const settle = (): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      if (tailTimer !== undefined) clearTimeout(tailTimer);
      pending.clear();
      mailbox = undefined;
      output.end();
      if (failure) reject(failure); else resolve();
    };
    const stop = (error?: Error): void => {
      failure ??= error;
      if (stopping) return;
      stopping = true;
      if (tailTimer !== undefined) clearTimeout(tailTimer);
      input.destroy();
      if (error) output.end(); // Fail remote pending calls; never replay an ambiguous publication.
      try { worker.postMessage({ end: true }); } catch { /* Already exiting. */ }
      timer = setTimeout(() => { void worker.terminate().then(settle, settle); }, STOP_MS);
    };
    const reply = (response: unknown): void => {
      if (stopping || output.destroyed || output.writableEnded) return;
      try { output.write(`${JSON.stringify(response)}\n`); }
      catch (error) { stop(error as Error); }
    };
    const forward = (request: RpcRequest, receivedAt: number, done: (response: Reply) => void): void => {
      const id = next++;
      pending.set(id, done);
      try { worker.postMessage({ line: JSON.stringify({ ...request, id }), receivedAt }); }
      catch (error) { stop(error as Error); }
    };
    worker.on("message", (line: unknown) => {
      if (stopping) return;
      let response: Reply;
      try {
        if (typeof line !== "string") throw new Error("Invalid bridge worker response");
        response = JSON.parse(line) as Reply;
        if (!response || typeof response !== "object") throw new Error("Invalid bridge worker response");
      } catch (error) { stop(error as Error); return; }
      if (response.notification) {
        if (response.notification === "change" && response.kind === "events") {
          generation++;
          // A real append after an empty read must not be consumed by that stale page.
          // Nonempty/advancing prefixes remain pinned, including across compaction.
          if (mailbox && noProgress(mailbox)) mailbox = undefined;
        }
        reply(response);
        return;
      }
      const done = typeof response.id === "number" ? pending.get(response.id) : undefined;
      if (!done) { stop(new Error("Unexpected bridge worker response")); return; }
      pending.delete(response.id as number);
      done(response);
    });
    const workerFailed = (error: unknown): void => stop(error instanceof Error ? error : new Error(String(error)));
    worker.on("error", workerFailed);
    worker.on("messageerror", workerFailed);
    worker.on("exit", (code) => {
      if (!stopping) stop(new Error(`Bridge worker exited unexpectedly (${code})`));
      settle();
    });
    output.on("error", (error: NodeJS.ErrnoException) => stop(error.code === "EPIPE" || error.code === "ERR_STREAM_DESTROYED" ? undefined : error));
    output.on("close", () => stop());
    output.on("finish", () => stop());
    bridgeRpcLines(input, (line) => {
      if (stopping) return;
      const receivedAt = Date.now();
      let request: RpcRequest;
      try {
        request = JSON.parse(line) as RpcRequest;
        if (!request || typeof request !== "object") throw new Error("Malformed bridge request");
      } catch { reply({ id: null, ok: false, error: "Malformed bridge request" }); return; }
      if (request.op === "ping") {
        reply({ id: request.id, ok: true, result: { busy: pending.size > 0 } });
        return;
      }
      if (request.op === "tail") {
        let boundary: { after: number; offset?: number };
        try { boundary = bridgeTailBoundary(request.args); }
        catch (error) { reply({ id: request.id, ok: false, error: (error as Error).message }); return; }
        // Legacy hubs still wait for an ordinary completed worker page. Never move
        // their reads onto the stdio thread, even while the worker is occupied.
        if (!busyTail) {
          forward(request, receivedAt, response => reply({ ...response, id: request.id }));
          return;
        }
        if (mailbox?.after === boundary.after && mailbox.offset === boundary.offset && mailbox.reply) {
          const completed = mailbox.reply;
          mailbox = undefined; // Empty pages too: an append can never leave an indefinitely cached empty.
          reply({ ...completed, id: request.id });
          return;
        }
        const respondBusy = (): void => {
          const busy: BridgeRead = { events: [], through: boundary.after,
            ...(boundary.offset === undefined ? {} : { offset: boundary.offset }), busy: true };
          reply({ id: request.id, ok: true, result: busy });
        };
        if (mailbox) { respondBusy(); return; }
        // Observe pending work BEFORE forwarding this read. A genuinely idle worker
        // gets one RPC to return its page, not an unconditional busy round trip.
        let waiting = pending.size === 0;
        const slot: Mailbox = { ...boundary, generation };
        mailbox = slot;
        forward(request, receivedAt, (response) => {
          if (waiting) {
            waiting = false;
            if (tailTimer !== undefined) clearTimeout(tailTimer);
            tailTimer = undefined;
            mailbox = undefined;
            reply({ ...response, id: request.id });
            return; // Direct completion must not manufacture a wake or cached page.
          }
          slot.reply = response;
          if (slot.generation !== generation && noProgress(slot)) mailbox = undefined;
          if (changes) reply({ notification: "change", kind: "events" });
        });
        if (waiting) {
          tailTimer = setTimeout(() => {
            tailTimer = undefined;
            waiting = false;
            respondBusy(); // Keep the in-flight read for the existing retained drain.
          }, TAIL_RESPONSE_GRACE_MS);
        } else respondBusy();
        return;
      }
      forward(request, receivedAt, (response) => {
        const hello = request.op === "hello" && response.ok === true && response.result &&
          typeof response.result === "object" && !Array.isArray(response.result)
          ? response.result as Record<string, unknown> : undefined;
        // Only a successful, supported worker hello can grant a capability. A failed
        // prepareLock/bootstrap must not turn a legacy tail into an unexpected busy.
        const validHello = hello?.tail === true && (hello.version === 1 || hello.version === BRIDGE_PROTOCOL_VERSION);
        if (validHello) {
          const args = request.args && typeof request.args === "object" && !Array.isArray(request.args)
            ? request.args as Record<string, unknown> : undefined;
          busyTail = args?.busyTail === true;
          if (args?.changes === true) changes = true;
        }
        reply({ ...response, id: request.id,
          ...(validHello ? { result: { ...hello, responsivePing: true, ...(busyTail ? { busyTail: true } : {}) } } : {}) });
      });
    }, stop);
    if (output.destroyed || output.writableEnded) stop();
  });
