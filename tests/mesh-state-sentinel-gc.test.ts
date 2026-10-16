import { spawnSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { expect, it } from "vitest";

it("does not retain importing runtimes through the file-read retry sentinel (#10720)", () => {
  const source = fileURLToPath(new URL("../src/mesh/state-file.ts", import.meta.url));
  const bundle = buildSync({
    stdin: { contents: `export { FILE_READ_CHANGED } from ${JSON.stringify(source)};`, resolveDir: dirname(source), loader: "ts" },
    bundle: true, write: false, platform: "node", format: "esm", target: "node24",
  }).outputFiles[0]!.text;
  // Evaluate the real declaration while a Main-like receiver is on the stack. Keep the
  // imported modules alive, just as the module cache does across native new_session.
  // No StateFile is constructed and no files are written or removed by the probe.
  const script = `
import vm from "node:vm";
const source = ${JSON.stringify(bundle)};
const modules = [], refs = [];
const imports = new Map();
async function link(specifier) {
  if (!imports.has(specifier)) {
    const exports = await import(specifier);
    const module = new vm.SyntheticModule(Object.keys(exports), function () {
      for (const [key, value] of Object.entries(exports)) this.setExport(key, value);
    });
    imports.set(specifier, module);
  }
  return imports.get(specifier);
}
class Runtime {
  constructor(id) { this.payload = new Array(131072).fill(id); }
  evaluate(module) { return module.evaluate(); }
}
async function load(id, text = source) {
  const runtime = new Runtime(id);
  const module = new vm.SourceTextModule(text);
  await module.link(link);
  await runtime.evaluate(module);
  modules.push(module);
  return new WeakRef(runtime);
}
async function collect() {
  // WeakRef targets are kept for the current job; collect in later jobs, not the
  // same job that constructs or dereferences them.
  await new Promise(resolve => setImmediate(resolve));
  for (let i = 0; i < 12; i++) {
    global.gc();
    await new Promise(resolve => setImmediate(resolve));
  }
}
const baseline = await load("unformatted", 'export const sentinel = new Error("control");');
const live = new Runtime("live");
for (let i = 0; i < 32; i++) refs.push(await load(i));
await collect();
const retained = refs.filter(ref => ref.deref()).length;
if (!baseline.deref()) throw new Error("unformatted-stack control did not retain its receiver");
if (retained !== 0) throw new Error("retired importing runtimes retained: " + retained);
if (live.payload.length !== 131072) throw new Error("live runtime lost its state");
for (const module of modules.slice(1)) {
  const sentinel = module.namespace.FILE_READ_CHANGED;
  if (!(sentinel instanceof Error)) throw new Error("retry sentinel changed type");
  if (!sentinel.stack.includes(sentinel.message)) throw new Error("retry diagnostic lost");
  try { throw sentinel; } catch (error) {
    if (error !== module.namespace.FILE_READ_CHANGED) throw new Error("retry identity changed");
  }
}
console.log(JSON.stringify({ collected: refs.length - retained, baselineRetained: true, identityPreserved: true }));
`;
  const result = spawnSync(process.execPath, ["--expose-gc", "--experimental-vm-modules", "--input-type=module"], {
    input: script, encoding: "utf8", timeout: 30_000,
    env: { ...process.env, PI_FABRIC_LOCK_STATS: "0", PI_FABRIC_COMMIT_STATS: "" },
  });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr || result.stdout).toBe(0);
  expect(JSON.parse(result.stdout)).toEqual({ collected: 32, baselineRetained: true, identityPreserved: true });
}, 45_000);
