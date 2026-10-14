// Collected only by scripts/probe-test-temp-cleanup.mjs, never by the normal *.test.ts glob.
import fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

it("holds a real temporary tree until the controller releases or stops the run", async () => {
  const mode = process.env.TEMPFIX_PROBE_MODE;
  const evidence = process.env.TEMPFIX_PROBE_EVIDENCE!;
  const runRoot = tmpdir();
  const fixtureDir = fs.mkdtempSync(join(runRoot, "lifecycle-fixture-"));
  fs.mkdirSync(join(fixtureDir, "node_modules", "fixture"), { recursive: true });
  for (let index = 0; index < 13; index++) {
    fs.writeFileSync(join(fixtureDir, "node_modules", "fixture", `${index}.js`), `export const value = ${index};\n`);
  }
  const released = new Promise<void>((resolve, reject) => {
    const watcher = fs.watch(evidence, (_event, name) => {
      if (String(name) === "release.json") { watcher.close(); resolve(); }
    });
    watcher.once("error", reject);
    // Writing is permitted; an outside-root rename would correctly be refused by the guard.
    const ready = { runRoot, fixtureDir, pid: process.pid };
    fs.writeFileSync(join(evidence, "ready.json"), JSON.stringify(ready));
    console.log(`TEMPFIX_READY ${JSON.stringify(ready)}`);
  });
  await released;
  if (mode === "fail") throw new Error("Intentional lifecycle failure after the tree exists");
  expect(mode).toBe("normal");
}, 60_000);
