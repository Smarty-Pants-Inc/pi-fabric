import { spawnSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { expect, it } from "vitest";

it("does not scan bundled runners while capture is off, skips workers, and still captures after enabling (#10720)", () => {
  const source = fileURLToPath(new URL("../src/capture/interceptor.ts", import.meta.url));
  const bundle = buildSync({
    stdin: { contents: `export { installRegisteredToolCapture } from ${JSON.stringify(source)};`, resolveDir: dirname(source), loader: "ts" },
    bundle: true, write: false, platform: "node", format: "esm", target: "node24", packages: "external",
  }).outputFiles[0]!.text;
  // Only read-only discovery runs. The fixture creates private files, never deletes them.
  const script = bundle + `
{
const path = (await import("node:path")).default;
const {pathToFileURL} = await import("node:url");
const {mkdtempSync, mkdirSync, writeFileSync} = await import("node:fs");
const {tmpdir} = await import("node:os");
const root = mkdtempSync(path.join(tmpdir(), "fabric-capture-imports-"));
mkdirSync(path.join(root, "dist", "bundle", "chunks"), {recursive:true});
writeFileSync(path.join(root, "dist", "index.js"), "export class ExtensionRunner { getAllRegisteredTools() { return []; } }");
writeFileSync(path.join(root, "package.json"), JSON.stringify({name:"@earendil-works/pi-coding-agent",type:"module"}));
const chunks = path.join(root, "dist", "bundle", "chunks");
writeFileSync(path.join(chunks, "image-resize-worker.js"), 'globalThis.workerImported=true; throw new Error("parentPort required");');
writeFileSync(path.join(chunks, "runner.js"), 'globalThis.runnerImported=true; export class ExtensionRunner { getAllRegisteredTools() { return globalThis.fixtureTools; } }');
writeFileSync(path.join(chunks, "failed.js"), '/* getAllRegisteredTools candidate */ throw new Error("candidate import failed");');
process.env.PI_PACKAGE_DIR = root;
const warnings=[];
console.warn = message => warnings.push(message);
const anchor = {name:"fabric_exec"};
globalThis.fixtureTools = [{definition:anchor,sourceInfo:{path:"/fixture/fabric.js"}}];
let captures=0;
const catalog = {clear(){},replace(tools,runner,policy){ if(policy.enabled) captures++; }};
const off = {enabled:false,hideFromModel:false,keepVisible:[],defaultRisk:"read",risks:{}};
const controller = await installRegisteredToolCapture({anchorDefinition:anchor,catalog,initialPolicy:off});
if(globalThis.runnerImported || globalThis.workerImported || warnings.length) throw new Error("disabled capture scanned chunks");
await controller.setPolicy({...off,enabled:true});
if(!globalThis.runnerImported) throw new Error("enable did not discover bundled runner");
if(globalThis.workerImported) throw new Error("worker-only chunk evaluated on Main");
if(warnings.length !== 1 || typeof warnings[0] !== "string" || !warnings[0].includes("candidate import failed")) throw new Error("import failure was not a plain diagnostic");
const {ExtensionRunner} = await import(pathToFileURL(path.join(chunks,"runner.js")).href);
new ExtensionRunner().getAllRegisteredTools();
if(captures !== 1) throw new Error("valid bundled runner was not patched");
controller.dispose();
new ExtensionRunner().getAllRegisteredTools();
if(captures !== 1) throw new Error("dispose left a capture listener");
console.log(JSON.stringify({disabledScan:false,workerImported:false,enabledCapture:true,diagnosticString:true}));
}
`;
  const result = spawnSync(process.execPath, ["--input-type=module"], {
    input: script, encoding: "utf8", timeout: 30_000,
    env: { ...process.env, PI_FABRIC_LOCK_STATS: "0", PI_FABRIC_COMMIT_STATS: "" },
  });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr || result.stdout).toBe(0);
  expect(JSON.parse(result.stdout)).toEqual({ disabledScan: false, workerImported: false, enabledCapture: true, diagnosticString: true });
}, 45_000);
