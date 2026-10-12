import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { transformSync } from "esbuild";
import { expect, it } from "vitest";

it("collects discarded host cards, including pending refreshes, but refreshes live cards (#2177)", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-display-gc-"));
  try {
    const source = fileURLToPath(new URL("../src/ui/tool-display.ts", import.meta.url));
    const module = transformSync(fs.readFileSync(source, "utf8"), { loader: "ts", format: "esm" }).code;
    const script = path.join(root, "probe.mjs");
    fs.writeFileSync(script, module + `
const controller = new FabricToolDisplayController();
const refs = [];
function register(id) {
  const card = { state: {}, payload: new Array(1000).fill(id), calls: 0 };
  controller.observe(id, "call", () => { card.calls++; }, card.state);
  controller.observe(id, "result", () => { card.calls++; }, card.state);
  refs.push(new WeakRef(card));
  return card;
}
const alive = register("alive");
for (let i = 0; i < 100; i++) register("discarded-" + i);
controller.refresh();
// Clear turn-local WeakRef kept objects; the GC immediate precedes the drain.
await new Promise(resolve => setTimeout(resolve, 0));
for (let i = 0; i < 12; i++) {
  global.gc();
  await new Promise(resolve => setImmediate(resolve));
}
const collected = refs.slice(1).filter(ref => !ref.deref()).length;
controller.refresh();
for (let i = 0; i < 40; i++) await new Promise(resolve => setImmediate(resolve));
if (collected !== 100) throw new Error("retained discarded host cards: " + (100-collected));
if (alive.calls < 1) throw new Error("live card stopped refreshing");
controller.clear();
const before = alive.calls;
controller.refresh();
await new Promise(resolve => setImmediate(resolve));
if (alive.calls !== before) throw new Error("clear did not detach live card");
console.log(JSON.stringify({ collected, liveCalls: alive.calls }));
`);
    const result = spawnSync(process.execPath, ["--expose-gc", script], { encoding: "utf8", timeout: 15_000 });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr || result.stdout).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ collected: 100 });
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
