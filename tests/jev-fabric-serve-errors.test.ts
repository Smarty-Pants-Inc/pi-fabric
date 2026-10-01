import { EventEmitter } from "node:events";
import { PassThrough, Writable } from "node:stream";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn: mocks.spawn }));
import { JevFabricServe, JevFabricServeError } from "../src/jev-fabric/serve.js";

afterEach(() => vi.restoreAllMocks());
const fixture = () => {
  const writes: Array<(error?: Error | null) => void> = [];
  const stdin = new Writable({ write(_chunk, _encoding, callback) { writes.push(callback); } });
  const child = Object.assign(new EventEmitter(), { stdin, stdout: new PassThrough(), stderr: new PassThrough(),
    kill: vi.fn(() => { queueMicrotask(() => child.emit("close", null)); return true; }) });
  mocks.spawn.mockReturnValueOnce(child as unknown as ChildProcessWithoutNullStreams);
  const opening = JevFabricServe.open("test-backend", { home: "/unused", cwd: "/unused", timeoutMs: 1000 });
  return { child, writes, opening, banner: async () => {
    child.stdout.write('{"ready":{"protocol":2,"version":"test"}}\n');
    return opening;
  } };
};
const pipeError = (code: string) => Object.assign(new Error(`write ${code}`), { code });

describe("Jev backend stdin failure boundaries", () => {
  it.each(["EPIPE", "ECONNRESET", "ERR_STREAM_DESTROYED"])("contains %s for all pending requests and confirms isolated teardown", async code => {
    const f = fixture();
    expect(f.child.stdin.listenerCount("error")).toBe(1); // present even before banner
    const serve = await f.banner();
    const signal = new AbortController().signal;
    const remove = vi.spyOn(signal, "removeEventListener");
    const first = serve.request("pending", {}, signal).catch(error => error);
    const second = serve.request("pending", {}, signal).catch(error => error);
    f.child.stdin.destroy(pipeError(code));
    const [a, b] = await Promise.all([first, second]);
    expect(a).toBeInstanceOf(JevFabricServeError);
    expect(a).toMatchObject({ code: null, cause: { code } });
    expect(b).toBe(a);
    expect(remove).toHaveBeenCalledTimes(2);
    await expect(serve.request("future")).rejects.toBe(a);
    const closed = serve.close();
    expect(serve.close()).toBe(closed);
    await closed;
    expect(f.child.kill).toHaveBeenCalledOnce();
  });

  it("rejects a pre-banner pipe failure only after confirmed backend exit", async () => {
    const f = fixture();
    const outcome = f.opening.catch(error => error);
    f.child.stdin.destroy(pipeError("ECONNRESET"));
    expect(await outcome).toMatchObject({ name: "JevFabricServeError", cause: { code: "ECONNRESET" } });
    expect(f.child.kill).toHaveBeenCalledOnce();
  });

  it.each(["callback", "throw"])("contains a write %s failure even without a stream event", async kind => {
    const f = fixture();
    const serve = await f.banner();
    const error = pipeError("ERR_STREAM_DESTROYED");
    if (kind === "throw") vi.spyOn(f.child.stdin, "write").mockImplementation(() => { throw error; });
    const outcome = serve.request("pending").catch(value => value);
    if (kind === "callback") f.writes[0]!(error);
    expect(await outcome).toMatchObject({ name: "JevFabricServeError", cause: error });
    await serve.close();
    expect(f.child.kill).toHaveBeenCalledOnce();
  });

  it("contains late stdin errors and synchronous end failures during owner shutdown", async () => {
    const f = fixture();
    const serve = await f.banner();
    const pending = serve.request("pending").catch(error => error);
    vi.spyOn(f.child.stdin, "end").mockImplementation(() => { throw pipeError("EPIPE"); });
    const closed = serve.close();
    expect(await pending).toMatchObject({ name: "JevFabricServeError", cause: { code: "EPIPE" } });
    expect(() => f.child.stdin.emit("error", pipeError("ECONNRESET"))).not.toThrow();
    await closed;
    expect(f.child.kill).toHaveBeenCalledOnce();
  });
});
