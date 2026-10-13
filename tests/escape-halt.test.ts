import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installFabricEscapeHalt } from "../src/ui/escape-halt.js";

function harness() {
  vi.useFakeTimers();
  let input: ((data: string) => unknown) | undefined;
  const unsubscribe = vi.fn();
  const notify = vi.fn();
  const context = {
    mode: "tui",
    ui: {
      notify,
      onTerminalInput: (handler: (data: string) => unknown) => { input = handler; return unsubscribe; },
    },
  } as unknown as ExtensionContext;
  const options = {
    enabled: vi.fn(() => true),
    ownsInput: vi.fn(() => false),
    halted: vi.fn(() => false),
    halt: vi.fn(() => 2),
  };
  const dispose = installFabricEscapeHalt(context, options);
  return { context, options, dispose, notify, unsubscribe, input: (data: string) => input?.(data) };
}

afterEach(() => vi.useRealTimers());

describe("Fabric Escape ownership", () => {
  it("observes native Escape without consuming it and halts once after debounce", () => {
    const h = harness();
    expect(h.input("\x1b")).toBeUndefined();
    expect(h.options.halt).not.toHaveBeenCalled();
    vi.advanceTimersByTime(60);
    expect(h.options.halt).toHaveBeenCalledOnce();
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("halted 2 background workers"), "warning");
    h.dispose();
  });

  it("never treats closing a Fabric view as a native interrupt", () => {
    const h = harness();
    h.options.ownsInput.mockReturnValue(true);
    h.input("\x1b");
    // Pi now delivers this same key to the focused view, which closes it.
    h.options.ownsInput.mockReturnValue(false);
    vi.advanceTimersByTime(100);
    expect(h.options.halt).not.toHaveBeenCalled();
    expect(h.notify).not.toHaveBeenCalled();
    h.dispose();
  });

  it("drops a pending interrupt when a conversation opens during the debounce", () => {
    const h = harness();
    h.input("\x1b");
    h.options.ownsInput.mockReturnValue(true);
    vi.advanceTimersByTime(100);
    expect(h.options.halt).not.toHaveBeenCalled();
    h.dispose();
  });

  it("does not confuse complete or split arrow sequences with Escape", () => {
    const h = harness();
    h.input("\x1b[A");
    vi.advanceTimersByTime(100);
    h.input("\x1b");
    vi.advanceTimersByTime(30);
    h.input("[B");
    vi.advanceTimersByTime(100);
    expect(h.options.halt).not.toHaveBeenCalled();
    h.dispose();
  });

  it("cancels delayed work on teardown, not just the input subscription", () => {
    const h = harness();
    h.input("\x1b");
    h.dispose();
    vi.advanceTimersByTime(100);
    expect(h.unsubscribe).toHaveBeenCalledOnce();
    expect(h.options.halt).not.toHaveBeenCalled();
  });

  it("respects disabled and already-halted states without noisy notifications", () => {
    const h = harness();
    h.options.enabled.mockReturnValue(false);
    h.input("\x1b");
    vi.advanceTimersByTime(100);
    h.options.enabled.mockReturnValue(true);
    h.options.halted.mockReturnValue(true);
    h.input("\x1b");
    vi.advanceTimersByTime(100);
    expect(h.options.halt).not.toHaveBeenCalled();
    h.options.halted.mockReturnValue(false);
    h.options.halt.mockReturnValue(0);
    h.input("\x1b");
    vi.advanceTimersByTime(100);
    expect(h.notify).not.toHaveBeenCalled();
    h.dispose();
  });

  it("warns the person who pressed Escape when the halt was not saved", () => {
    const h = harness();
    h.options.halt.mockImplementation(() => { throw new Error("queue storage unavailable"); });
    h.input("\x1b");
    vi.advanceTimersByTime(60);
    expect(h.notify).toHaveBeenCalledWith(
      "Fabric: halt not saved; work may resume on restart: queue storage unavailable", "warning",
    );
    h.dispose();
  });

  it("does not attach terminal input in RPC mode", () => {
    const onTerminalInput = vi.fn();
    const context = { mode: "rpc", ui: { onTerminalInput } } as unknown as ExtensionContext;
    installFabricEscapeHalt(context, {
      enabled: () => true, ownsInput: () => false, halted: () => false, halt: () => 0,
    })();
    expect(onTerminalInput).not.toHaveBeenCalled();
  });
});

// smarty-dev#8881: AgentSession.dispose() invalidates the captured ctx without session_shutdown.
// Every ctx getter then throws Pi's stale-ctx error; the TUI (and its input listener) can outlive it.
describe("Fabric Escape after session disposal", () => {
  function disposable() {
    vi.useFakeTimers();
    let stale = false;
    const touched: string[] = [];
    let input: ((data: string) => unknown) | undefined;
    const unsubscribe = vi.fn();
    const notify = vi.fn();
    const ui = { notify, onTerminalInput: (handler: (data: string) => unknown) => { input = handler; return unsubscribe; } };
    const context = {} as ExtensionContext;
    for (const [key, value] of Object.entries({ mode: "tui", hasUI: true, ui })) {
      Object.defineProperty(context, key, { get() {
        if (stale) { touched.push(key); throw new Error("This extension ctx is stale after session replacement or reload."); }
        return value;
      } });
    }
    const options = { enabled: vi.fn(() => true), ownsInput: vi.fn(() => false), halted: vi.fn(() => false), halt: vi.fn(() => 2) };
    const remove = installFabricEscapeHalt(context, options);
    return { options, notify, unsubscribe, remove, touched, input: (data: string) => input?.(data), dispose: () => { stale = true; } };
  }

  it("a pending Escape halts nothing and touches no stale UI once the session is disposed", () => {
    const h = disposable();
    h.input("\x1b"); // live at the key press: enabled/ownsInput are read synchronously
    vi.clearAllMocks();
    h.dispose();
    expect(() => vi.advanceTimersByTime(100)).not.toThrow();
    for (const option of Object.values(h.options)) expect(option).not.toHaveBeenCalled();
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.touched.filter(key => key !== "hasUI")).toEqual([]);
    expect(h.unsubscribe).toHaveBeenCalledOnce(); // retired locally
    h.remove();
    expect(h.unsubscribe).toHaveBeenCalledOnce();
  });

  it("Escape reaching a surviving TUI listener after disposal arms nothing", () => {
    const h = disposable();
    h.dispose();
    expect(h.input("\x1b")).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(100);
    for (const option of Object.values(h.options)) expect(option).not.toHaveBeenCalled();
    expect(h.unsubscribe).toHaveBeenCalledOnce();
  });

  it("a live session still halts after the debounce", () => {
    const h = disposable();
    h.input("\x1b");
    vi.advanceTimersByTime(60);
    expect(h.options.halt).toHaveBeenCalledOnce();
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("halted 2 background workers"), "warning");
    h.remove();
  });
});
