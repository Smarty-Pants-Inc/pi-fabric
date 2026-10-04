// Publish one real command response, fail retirement, then exit without close.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
const { ResidentHost } = await import(pathToFileURL(path.resolve("dist/residency/host.js")).href);
const config = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const evidence = process.argv[3];
const host = new ResidentHost(config);
const rm = fs.rmSync.bind(fs);
let failed = false;
fs.rmSync = (file, options) => {
  if (String(file).startsWith(path.join(config.residencyRoot, "processing") + path.sep) && !failed) {
    failed = true;
    const response = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "responses", path.basename(String(file))), "utf8"));
    fs.writeFileSync(path.join(evidence, "original.json"), JSON.stringify(response));
    // Let the owned retry boundary catch the actual retirement error before death.
    setImmediate(() => process.exit(0));
    throw new Error("fixture processing retirement unavailable");
  }
  return rm(file, options);
};
try {
  await host.start();
  const create = host.actors.create.bind(host.actors);
  host.actors.create = (...args) => {
    fs.appendFileSync(path.join(evidence, "mutations.txt"), "createActor\n");
    return create(...args);
  };
  fs.writeFileSync(path.join(evidence, "ready"), String(process.pid));
} catch (error) {
  console.error(error);
  await host.close();
  process.exit(1);
}
