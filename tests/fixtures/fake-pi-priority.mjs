#!/usr/bin/env node
// smarty-dev#1579 probe: record every thread's niceness, a descendant's, and the IO
// priority, after Node has started its runtime threads; then act as the fake Pi.
import fs from "node:fs";
import { spawnSync } from "node:child_process";

const out = process.env.FABRIC_PRIORITY_PROBE;
if (out) {
  await new Promise((resolve) => setTimeout(resolve, 100));
  const niceOf = (stat) => Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[16]);
  const threads = fs.readdirSync("/proc/self/task").map((tid) => niceOf(fs.readFileSync(`/proc/self/task/${tid}/stat`, "utf8")));
  const descendant = niceOf(spawnSync("cat", ["/proc/self/stat"], { encoding: "utf8" }).stdout);
  const ionice = spawnSync("ionice", ["-p", String(process.pid)], { encoding: "utf8" });
  fs.writeFileSync(out, JSON.stringify({ threads, descendant, ionice: ionice.error ? null : ionice.stdout.trim() }));
}
await import("./fake-pi-rpc.mjs");
