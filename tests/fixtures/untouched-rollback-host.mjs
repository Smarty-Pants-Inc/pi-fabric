// An actual current bundle writes, restores, and saves an untouched registry before rollback.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const { ResidentHost } = await import(pathToFileURL(process.argv[2]).href);
const config = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
let host = new ResidentHost(config);
try {
  await host.start();
  const actor = await host.actors.create({ name: "untouched-rollback", instructions: "Original", residency: "durable", topics: ["github.demo"] }, { asRegistryOwner: true });
  await host.close();
  host = new ResidentHost(config);
  await host.start();
  await new Promise(resolve => setTimeout(resolve, 100));
  if (host.actors.status(actor.id).filterSkipped.count !== 0) throw new Error("Unexpected public telemetry");
  await host.close();
} catch (error) {
  console.error(error);
  await host.close();
  process.exitCode = 1;
}
