// Idle wake counter (smarty-dev#7978): `node --require scripts/idle-wake-preload.cjs ...`.
// Counts each Timeout and Immediate callback run (async_hooks `before`) per creation callsite: the
// first stack frame outside node: internals and this file, captured once when the resource is made.
// fs.watchFile polls inside libuv (a StatWatcher), which async_hooks `before` does not see: each
// watched file counts 1000/interval wakes per second while it is watched, under its watchFile callsite.
// SIGUSR2 starts the window (earlier wakes are warm-up); SIGUSR1 writes the counts to IDLE_WAKE_OUT.
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { createHook } = require("node:async_hooks");
const { syncBuiltinESMExports } = require("node:module");

const out = process.env.IDLE_WAKE_OUT;
const callsites = new Map(); // asyncId -> "type callsite"
const counts = new Map();
const statWatchers = new Map(); // resolved file -> { callsite, interval, from, until }
const statWatcherIds = new Set(); // live STATWATCHER async resources, the cross-check
let began = 0;

function callsite() {
  const limit = Error.stackTraceLimit;
  Error.stackTraceLimit = 40;
  const stack = new Error().stack ?? "";
  Error.stackTraceLimit = limit;
  for (const line of stack.split("\n").slice(1)) {
    if (line.includes("node:") || line.includes(__filename)) continue;
    const frame = /at (?:async )?(.*?) \((.*):(\d+):\d+\)$/.exec(line) ?? /at ()(.*):(\d+):\d+$/.exec(line);
    if (!frame) continue;
    const file = frame[2].replace(/^file:\/\//, "");
    const root = process.env.IDLE_WAKE_ROOT ?? process.cwd();
    // "file:line function": the allowlist matches file and function, so a line shift needs no edit.
    return `${file.startsWith(root + "/") ? file.slice(root.length + 1) : file}:${frame[3]} ${frame[1] || "<anonymous>"}`;
  }
  return "node:internal";
}

createHook({
  init(asyncId, type) {
    if (type === "Timeout" || type === "Immediate") callsites.set(asyncId, `${type} ${callsite()}`);
    else if (type === "STATWATCHER") statWatcherIds.add(asyncId);
  },
  before(asyncId) {
    if (!began) return;
    const key = callsites.get(asyncId);
    if (key) counts.set(key, (counts.get(key) ?? 0) + 1);
  },
  destroy(asyncId) { callsites.delete(asyncId); statWatcherIds.delete(asyncId); },
}).enable();

// One poll per file, at the first watcher's interval (Node's default 5007 ms), as fs.watchFile does.
const { watchFile, unwatchFile } = fs;
const watchedListeners = new Map(); // resolved file -> listener count
fs.watchFile = function (file, options, listener) {
  const key = path.resolve(String(file));
  if (!statWatchers.get(key) || statWatchers.get(key).until) {
    const interval = (typeof options === "object" && options?.interval) || 5007;
    statWatchers.set(key, { callsite: callsite(), interval, from: Date.now(), until: 0 });
    watchedListeners.set(key, 0);
  }
  if (typeof (listener ?? options) === "function") watchedListeners.set(key, watchedListeners.get(key) + 1);
  return watchFile.apply(this, arguments);
};
fs.unwatchFile = function (file, listener) {
  const key = path.resolve(String(file));
  const left = typeof listener === "function" ? (watchedListeners.get(key) ?? 1) - 1 : 0;
  watchedListeners.set(key, left);
  if (left <= 0 && statWatchers.get(key)) statWatchers.get(key).until ||= Date.now();
  return unwatchFile.apply(this, arguments);
};
syncBuiltinESMExports();

process.on("SIGUSR2", () => { counts.clear(); began = Date.now(); });
process.on("SIGUSR1", () => {
  if (!out || !began) return;
  const seconds = (Date.now() - began) / 1000;
  const now = Date.now();
  for (const watcher of statWatchers.values()) {
    const polled = (watcher.until || now) - Math.max(watcher.from, began);
    if (polled > 0) counts.set(`StatWatcher ${watcher.callsite}`,
      (counts.get(`StatWatcher ${watcher.callsite}`) ?? 0) + Math.floor(polled / watcher.interval));
  }
  const rows = [...counts].map(([key, wakes]) => {
    const [type, site] = [key.slice(0, key.indexOf(" ")), key.slice(key.indexOf(" ") + 1)];
    return { type, callsite: site, wakes, perSecond: wakes / seconds };
  }).sort((a, b) => b.wakes - a.wakes);
  const watchedFiles = [...statWatchers.values()].filter(watcher => !watcher.until).length;
  fs.writeFileSync(out + ".tmp", JSON.stringify({ pid: process.pid, seconds, rows,
    statWatchers: { watchedFiles, liveResources: statWatcherIds.size } }));
  fs.renameSync(out + ".tmp", out);
});
