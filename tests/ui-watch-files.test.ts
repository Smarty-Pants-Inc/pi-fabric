import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { watchUiFiles } from "../src/ui/watch-files.js";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const cleanup of cleanups.splice(0)) cleanup(); vi.restoreAllMocks(); });

describe("UI filesystem dirty hints", () => {
  it("watches the signal nonpersistently and survives creation, atomic replacement, deletion and recreation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "ui-watch-"));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const signal = path.join(root, "state.read-signal.json");
    const observed = vi.fn();
    const watch = vi.spyOn(fs, "watch");
    const stop = watchUiFiles(root, ["state.read-signal.json"], observed);
    cleanups.unshift(stop);
    const replace = (value: string) => { fs.writeFileSync(signal + ".tmp", value); fs.renameSync(signal + ".tmp", signal); };
    for (const operation of [() => replace("first"), () => replace("second"), () => fs.unlinkSync(signal), () => replace("third")]) {
      observed.mockClear(); operation();
      await vi.waitFor(() => expect(observed).toHaveBeenCalled(), { timeout: 1000, interval: 5 });
    }
    expect(watch.mock.calls.some(([file]) => file === signal)).toBe(true);
    for (const [, options] of watch.mock.calls) expect(options).toEqual({ persistent: false });
    stop(); observed.mockClear(); replace("after-stop");
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(observed).not.toHaveBeenCalled();
  });

  it("ignores unrelated mesh-root files and tolerates an unavailable root", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "ui-watch-unrelated-"));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const observed = vi.fn();
    const stop = watchUiFiles(root, ["state.read-signal.json"], observed);
    cleanups.unshift(stop);
    fs.writeFileSync(path.join(root, "unrelated"), "metadata");
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(observed).not.toHaveBeenCalled();
    expect(() => watchUiFiles(path.join(root, "absent"), ["signal"], observed)()).not.toThrow();
  });
});
