import fs from "node:fs";
import { vi } from "vitest";

/** Observe actual SDK callback returns while a separate native worker is paused.
 * No fake feed or host-result transport: only the receiver's scheduling changes. */
export async function captureMontyTransport(onReturned: () => void, onAck?: (ack: (idDelta?: number, responseDelta?: number) => void) => void, holdResponse = false) {
  const native = await import("@pydantic/monty/node");
  const create = native.Monty.create.bind(native.Monty);
  let pid: number | undefined;
  let returned!: () => void;
  const responseReturned = new Promise<void>(resolve => { returned = resolve; });
  let transport: { callMethod(name: string, args: unknown[], kwargs: Record<string, unknown>): unknown } | undefined;
  let response: { id: number; responseId: number } | undefined;
  let replay: ((name: string, args: unknown[], kwargs: Record<string, unknown>) => unknown) | undefined;
  const spy = vi.spyOn(native.Monty, "create").mockImplementation(async opts => {
    const pool = await create(opts);
    const checkout = pool.checkout.bind(pool);
    vi.spyOn(pool, "checkout").mockImplementation(async opts => {
      const session = await checkout(opts);
      pid = session.workerPid!;
      const feed = session.feedRun.bind(session);
      vi.spyOn(session, "feedRun").mockImplementation((code, opts) => {
        if (opts?.inputs?.__fabric_transport) {
          transport = opts.inputs.__fabric_transport as typeof transport;
          const call = transport!.callMethod.bind(transport);
          replay = call;
          vi.spyOn(transport!, "callMethod").mockImplementation((name, args, kwargs) => {
            if (name === "response_ack" && onAck) {
              onAck((idDelta = 0, responseDelta = 0) => { call(name, [], { ...kwargs, id: Number(kwargs.id) + idDelta, responseId: Number(kwargs.responseId) + responseDelta }); });
              return null;
            }
            return call(name, args, kwargs);
          });
        }
        return feed(code, opts);
      });
      const sdk = session as unknown as { native: { resolveFutures(results: { value?: unknown }[], onPrint: unknown): Promise<unknown> } };
      const resolve = sdk.native.resolveFutures.bind(sdk.native);
      vi.spyOn(sdk.native, "resolveFutures").mockImplementation(async (results, onPrint) => {
        if (holdResponse) {
          process.kill(pid!, "SIGSTOP");
          for (let attempt = 0; !/\) T /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8")); attempt++) {
            if (attempt >= 500) throw new Error("Monty receiver did not stop");
            await new Promise(resolve => setTimeout(resolve, 10));
          }
        }
        for (const { value } of results) {
          if (value instanceof Map && value.has("responseId")) response = { id: value.get("id") as number, responseId: value.get("responseId") as number };
        }
        // The SDK has received the callback return and prepared its future
        // resolution. Sending to a SIGSTOPped native receiver is not admission.
        onReturned(); returned();
        // NAPI may synchronously wait for the worker lock when submitting the
        // next turn. Yield so the test can expire/close the stopped receiver.
        await new Promise<void>(resolve => setImmediate(resolve));
        return resolve(results, onPrint);
      });
      return session;
    });
    return pool;
  });
  return {
    responseReturned,
    closeReceiver() { if (pid !== undefined) { try { process.kill(pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } } },
    staleAck() { if (replay && response) replay("response_ack", [], response); },
    restore() { spy.mockRestore(); },
  };
}
