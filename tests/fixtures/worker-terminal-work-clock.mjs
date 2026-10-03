// The fixture still imports the actual built worker at its original URL, so its
// relative imports and the manager's real deadline remain unchanged. Only the
// synchronous terminal-compaction call uses a frozen work clock.
import fs from "node:fs";
import { registerHooks } from "node:module";

export const importWorkerWithFrozenTerminalClock = async (workerUrl, receiptFile) => {
  const hooks = registerHooks({
    load(url, context, nextLoad) {
      const loaded = nextLoad(url, context);
      if (url !== workerUrl) return loaded;
      const source = typeof loaded.source === "string" ? loaded.source : Buffer.from(loaded.source).toString("utf8");
      // Bundling can suffix the local function name, but this boundary must stay
      // unique. Fail loudly if the built worker no longer exposes it.
      const boundary = /const logCompaction = (compactTerminalRunLog\d*\(options\.logFile, record\.status\));/g;
      if ([...source.matchAll(boundary)].length !== 1) throw new Error("Expected one worker terminal-compaction boundary");
      const scoped = source.replace(boundary, (_match, call) => `const logCompaction = (() => {
        const clock = globalThis.performance;
        const descriptor = Object.getOwnPropertyDescriptor(clock, "now");
        const originalNow = clock.now;
        let calls = 0;
        Object.defineProperty(clock, "now", { configurable: true, value: () => { calls++; return 0; } });
        try { return ${call}; }
        finally {
          if (descriptor) Object.defineProperty(clock, "now", descriptor);
          else delete clock.now;
          globalThis[Symbol.for("pi-fabric.test.terminal-work-clock-receipt")]({ calls, restored: clock.now === originalNow });
        }
      })();`);
      return { ...loaded, source: scoped };
    },
  });
  const receiptKey = Symbol.for("pi-fabric.test.terminal-work-clock-receipt");
  globalThis[receiptKey] = (receipt) => {
    fs.writeFileSync(receiptFile, JSON.stringify(receipt));
    delete globalThis[receiptKey];
    hooks.deregister();
  };
  await import(workerUrl);
};
