import { fork, type ChildProcess } from "node:child_process";
import { PassThrough, Writable } from "node:stream";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { buildSync } from "esbuild";
import { BRIDGE_PROTOCOL_VERSION, MeshBridge, RemoteBridgeSide, StoreBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const children: ChildProcess[] = [];
const stores: MeshStore[] = [];
const cleanups: Array<() => void> = [];
afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await waitForChildClose(child, 1_000).catch(() => undefined);
    }
  }
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-stop-"));
  roots.push(root);
  return root;
};

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
};

const withTimeout = async <T>(promise: Promise<T>, timeoutMs: number, what: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out waiting for ${what}`)), timeoutMs);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};

const waitForMessage = (child: ChildProcess, type: string, timeoutMs = 3_000): Promise<any> => new Promise((resolve, reject) => {
  const finish = (action: () => void): void => {
    clearTimeout(timer);
    child.off("message", onMessage);
    child.off("close", onClose);
    action();
  };
  const onMessage = (message: any): void => {
    if (message?.type === "fixture-error") finish(() => reject(new Error(message.error)));
    else if (message?.type === type) finish(() => resolve(message));
  };
  const onClose = (code: number | null, signal: NodeJS.Signals | null): void => {
    finish(() => reject(new Error(`Agent closed before ${type}: code=${code}, signal=${signal}`)));
  };
  const timer = setTimeout(() => finish(() => reject(new Error(`Timed out waiting for agent ${type}`))), timeoutMs);
  child.on("message", onMessage);
  child.once("close", onClose);
});

const waitForAgentOutcome = (child: ChildProcess, timeoutMs: number): Promise<
  { type: "served" } | { type: "exit"; code: number | null; signal: NodeJS.Signals | null }
> => new Promise((resolve, reject) => {
  const finish = (action: () => void): void => {
    clearTimeout(timer);
    child.off("message", onMessage);
    child.off("close", onClose);
    action();
  };
  const onMessage = (message: any): void => {
    if (message?.type === "fixture-error") finish(() => reject(new Error(message.error)));
    else if (message?.type === "served") finish(() => resolve({ type: "served" }));
  };
  const onClose = (code: number | null, signal: NodeJS.Signals | null): void => {
    finish(() => resolve({ type: "exit", code, signal }));
  };
  const timer = setTimeout(() => finish(() => reject(new Error("Timed out waiting for remote agent shutdown"))), timeoutMs);
  child.on("message", onMessage);
  child.once("close", onClose);
});

const waitForChildClose = (child: ChildProcess, timeoutMs: number): Promise<{
  code: number | null; signal: NodeJS.Signals | null;
}> => new Promise((resolve, reject) => {
  if (child.exitCode !== null || child.signalCode !== null) {
    resolve({ code: child.exitCode, signal: child.signalCode });
    return;
  }
  const finish = (action: () => void): void => {
    clearTimeout(timer);
    child.off("close", onClose);
    action();
  };
  const onClose = (code: number | null, signal: NodeJS.Signals | null): void => {
    finish(() => resolve({ code, signal }));
  };
  const timer = setTimeout(() => finish(() => reject(new Error(`Agent did not exit within ${timeoutMs} ms`))), timeoutMs);
  child.once("close", onClose);
});

const waitForStreamClose = (stream: NodeJS.ReadableStream, timeoutMs: number): Promise<void> => new Promise((resolve, reject) => {
  const finish = (action: () => void): void => {
    clearTimeout(timer);
    stream.off("close", onClose);
    action();
  };
  const onClose = (): void => finish(resolve);
  const timer = setTimeout(() => finish(() => reject(new Error("Timed out closing the remote stdout reader"))), timeoutMs);
  stream.once("close", onClose);
});

describe.skipIf(process.platform === "win32")("mesh bridge agent shutdown (smarty-dev#14612)", () => {
  it("exits 0 when stop closes the real stdout pipe during a queued store notification flush", async () => {
    const root = scratch();
    const agentRoot = path.join(root, "agent-mesh");
    fs.mkdirSync(agentRoot);
    // The child bundle leaves package imports external, as the other native bridge fixtures do.
    fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "dir");
    const program = path.join(root, "bridge-agent.mjs");
    const source = String.raw`
      import fs from "node:fs";
      import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
      import { StoreBridgeSide, serveBridgeAgent } from ${JSON.stringify(path.resolve("src/mesh/bridge.ts"))};

      const root = process.argv[2];
      const send = (message, callback) => { if (process.connected) process.send(message, callback); };
      const store = new MeshStore(root, 64 * 1024, 2);
      const side = new StoreBridgeSide(store, "hub");

      // Hold only the real StoreBridgeSide notification flush scheduled synchronously from an
      // fs.watch callback. The state change itself below is a real MeshStore.publish; this seam
      // lets the parent close stdout before that already-queued callback writes its frame.
      const originalWatch = fs.watch.bind(fs);
      let insideWatchCallback = false;
      fs.watch = (target, options, listener) => {
        const callback = typeof options === "function" ? options : listener;
        if (typeof callback !== "function") return originalWatch(target, options, listener);
        const wrapped = (...args) => {
          const wasInside = insideWatchCallback;
          insideWatchCallback = true;
          try { callback(...args); } finally { insideWatchCallback = wasInside; }
        };
        return typeof options === "function"
          ? originalWatch(target, wrapped)
          : originalWatch(target, options, wrapped);
      };
      const nativeSetImmediate = globalThis.setImmediate.bind(globalThis);
      const nativeClearImmediate = globalThis.clearImmediate.bind(globalThis);
      const heldHandles = new WeakSet();
      let armed = false;
      let heldFlush;
      globalThis.setImmediate = (callback, ...args) => {
        if (armed && insideWatchCallback && !heldFlush) {
          const handle = { ref() { return this; }, unref() { return this; }, hasRef() { return true; } };
          heldFlush = { callback, args, handle, cancelled: false };
          heldHandles.add(handle);
          send({ type: "flush-queued" });
          return handle;
        }
        return nativeSetImmediate(callback, ...args);
      };
      globalThis.clearImmediate = handle => {
        if (handle && heldHandles.has(handle)) {
          if (heldFlush?.handle === handle) heldFlush.cancelled = true;
          return;
        }
        return nativeClearImmediate(handle);
      };

      process.on("message", message => {
        if (message?.type === "publish") {
          armed = true;
          void store.publish({ topic: "mesh-bridge-stop.test", kind: "probe",
            from: { id: "agent:test", name: "agent", kind: "main" } })
            .then(() => send({ type: "published" }), error => send({ type: "fixture-error", error: String(error) }));
        } else if (message?.type === "release-flush") {
          if (!heldFlush || heldFlush.cancelled) {
            send({ type: "fixture-error", error: "The real subscription flush was not held" });
            return;
          }
          const pending = heldFlush;
          nativeSetImmediate(() => { if (!pending.cancelled) pending.callback(...pending.args); });
        }
      });

      const serving = serveBridgeAgent(side, process.stdin, process.stdout);
      send({ type: "ready" });
      void serving.then(() => {
        store.closeState();
        send({ type: "served" }, () => { if (process.connected) process.disconnect(); });
      });
    `;
    buildSync({ stdin: { contents: source, resolveDir: process.cwd() }, outfile: program,
      bundle: true, platform: "node", format: "esm", packages: "external" });

    const child = fork(program, [agentRoot], { execArgv: [], stdio: ["pipe", "pipe", "pipe", "ipc"] });
    children.push(child);
    const agentReady = waitForMessage(child, "ready");
    let stderr = "";
    child.stderr!.setEncoding("utf8");
    child.stderr!.on("data", chunk => { stderr += chunk; });
    const frames: any[] = [];
    let buffered = "";
    child.stdout!.on("data", chunk => {
      buffered += chunk;
      let newline: number;
      while ((newline = buffered.indexOf("\n")) >= 0) {
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        if (line) frames.push(JSON.parse(line));
      }
    });

    const input = new PassThrough();
    cleanups.push(() => input.destroy());
    input.pipe(child.stdin!);
    const originalEnd = input.end.bind(input);
    const endRequested = deferred<void>();
    let inputEndReleased = false;
    input.end = ((..._args: unknown[]) => { endRequested.resolve(); return input; }) as typeof input.end;
    const releaseInputEnd = (): void => {
      if (inputEndReleased) return;
      inputEndReleased = true;
      originalEnd();
    };

    const hub = new MeshStore(path.join(root, "hub-mesh"), 64 * 1024, 2);
    stores.push(hub);
    const remote = new RemoteBridgeSide(child.stdout!, input);
    cleanups.push(() => remote.close());
    const bridge = new MeshBridge({ localName: "hub", remoteName: "agent", local: new StoreBridgeSide(hub, "agent"),
      remote, cursorPath: path.join(root, "bridge-cursor.json"), stopMs: 500 });

    await agentReady;
    await withTimeout(remote.hello(true), 3_000, "initial hello reply");
    expect(frames).toContainEqual(expect.objectContaining({ id: 1, ok: true,
      result: expect.objectContaining({ version: BRIDGE_PROTOCOL_VERSION, changes: true }) }));

    const queued = waitForMessage(child, "flush-queued");
    const published = waitForMessage(child, "published");
    child.send({ type: "publish" });
    await Promise.all([queued, published]);

    const stopping = bridge.stop();
    await endRequested.promise;
    await stopping;
    // Model the remote transport reader disappearing during stop, but hold stdin EOF so the
    // queued native notification callback gets its deterministic turn first. On EPIPE the fixed
    // serveBridgeAgent must close/dispose itself; only then does the parent release input EOF.
    const stdoutClosed = waitForStreamClose(child.stdout!, 1_000);
    child.stdout!.destroy();
    await stdoutClosed;

    const outcomePromise = waitForAgentOutcome(child, 3_000);
    child.send({ type: "release-flush" });
    const outcome = await outcomePromise;
    let exit: { code: number | null; signal: NodeJS.Signals | null };
    if (outcome.type === "served") {
      releaseInputEnd();
      exit = await waitForChildClose(child, 2_000);
    } else exit = { code: outcome.code, signal: outcome.signal };
    expect({ served: outcome.type === "served", exit,
      stderrHasPipeError: /EPIPE|ERR_STREAM_DESTROYED/.test(stderr) }, stderr)
      .toEqual({ served: true, exit: { code: 0, signal: null }, stderrHasPipeError: false });
  }, 20_000);

  it("treats ERR_STREAM_DESTROYED from stdout as agent EOF and retains an earlier reply", async () => {
    const root = scratch();
    const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 2);
    stores.push(store);
    const input = new PassThrough();
    cleanups.push(() => input.destroy());
    const frames: any[] = [];
    const errors: NodeJS.ErrnoException[] = [];
    let failWrites = false;
    const firstReply = deferred<void>();
    const output = new Writable({
      autoDestroy: false,
      write(chunk, _encoding, callback) {
        if (failWrites) {
          callback(Object.assign(new Error("stdout was destroyed"), { code: "ERR_STREAM_DESTROYED" }));
          return;
        }
        frames.push(JSON.parse(String(chunk)));
        firstReply.resolve();
        callback();
      },
    });
    output.on("error", error => errors.push(error as NodeJS.ErrnoException));
    cleanups.push(() => output.destroy());
    const serving = serveBridgeAgent(new StoreBridgeSide(store, "hub"), input, output);

    try {
      input.write(JSON.stringify({ id: 1, op: "hello", args: { version: BRIDGE_PROTOCOL_VERSION, changes: true } }) + "\n");
      await withTimeout(firstReply.promise, 2_000, "initial reply");
      expect(frames).toContainEqual(expect.objectContaining({ id: 1, ok: true }));

      failWrites = true;
      const streamError = new Promise<NodeJS.ErrnoException>(resolve => output.once("error", error => resolve(error as NodeJS.ErrnoException)));
      input.write(JSON.stringify({ id: 2, op: "latestSequence" }) + "\n");
      await expect(withTimeout(streamError, 2_000, "ERR_STREAM_DESTROYED write")).resolves.toMatchObject({ code: "ERR_STREAM_DESTROYED" });
      await expect(withTimeout(serving, 1_000, "agent EOF after stdout error")).resolves.toBeUndefined();
      expect(errors.map(error => error.code)).toContain("ERR_STREAM_DESTROYED");
    } finally {
      input.end();
      output.destroy();
      await withTimeout(serving, 1_000, "agent cleanup").catch(() => undefined);
    }
  }, 8_000);

  it("keeps an unread reply buffered across EOF and ignores reentrant and late notifications", async () => {
    const store = new MeshStore(path.join(scratch(), "mesh"), 64 * 1024, 2);
    stores.push(store);
    const side = new StoreBridgeSide(store, "hub");
    const input = new PassThrough();
    const output = new PassThrough({ highWaterMark: 1 });
    cleanups.push(() => { input.destroy(); output.destroy(); });
    const subscribe = side.subscribeChanges.bind(side);
    let notify!: (kind: "events") => void;
    side.subscribeChanges = (changed, failed) => {
      notify = changed;
      const unsubscribe = subscribe(changed, failed);
      return () => { changed("events"); unsubscribe(); };
    };
    const readable = new Promise<void>(resolve => output.once("readable", () => resolve()));
    const serving = serveBridgeAgent(side, input, output);
    input.write(JSON.stringify({ id: 1, op: "hello", args: { version: BRIDGE_PROTOCOL_VERSION, changes: true } }) + "\n");
    await withTimeout(readable, 2_000, "buffered hello reply");
    const bufferedBytes = output.readableLength;
    expect(bufferedBytes).toBeGreaterThan(0);
    input.end();
    await withTimeout(serving, 1_000, "stdin EOF");
    notify("events");
    expect(output.destroyed).toBe(false);
    expect(output.readableLength).toBe(bufferedBytes);
    expect(JSON.parse(String(output.read()))).toMatchObject({ id: 1, ok: true });
  });

  it("does not suppress an unrelated stdout error", async () => {
    const store = new MeshStore(path.join(scratch(), "mesh"), 64 * 1024, 2);
    stores.push(store);
    const input = new PassThrough();
    const output = new PassThrough();
    cleanups.push(() => { input.destroy(); output.destroy(); });
    const serving = serveBridgeAgent(new StoreBridgeSide(store, "hub"), input, output);
    const error = Object.assign(new Error("unrelated stdout failure"), { code: "EIO" });
    expect(() => output.emit("error", error)).toThrow(error);
    input.end();
    await withTimeout(serving, 1_000, "stdin EOF after unrelated error");
  });
});
