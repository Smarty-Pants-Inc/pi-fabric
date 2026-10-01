// Run the actual installed pre-setter bundle, not a compatibility mock.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const { ResidentHost } = await import(pathToFileURL(process.argv[2]).href);
const config = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const host = new ResidentHost(config);
let actor;
const waitFor = async (predicate) => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Legacy actor did not reach queued state");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
process.on("message", async (message) => {
  if (message !== "close") return;
  try {
    // Explicitly stop only this fixture actor: a progress-making turn otherwise
    // survives close until the legacy host's 30s shutdown grace expires.
    if (actor) {
      try { await host.actors.stop(actor.id); } catch { /* A destructive legacy command may already have removed it. */ }
    }
    // Legacy stop aborts observation but can detach a progress-making worker.
    // Join the fixture agent manager too, before waiting on the host drain.
    await host.agents.close();
    await host.close();
    process.disconnect();
    process.exit(0);
  }
  catch (error) { console.error(error); process.exitCode = 1; process.disconnect(); }
});
try {
  if (process.argv[4] === "recover") {
    const registry = JSON.parse(fs.readFileSync(config.actorRoot + "/actors.json", "utf8"));
    actor = registry.actors[0];
    await host.start();
    process.send({ actor: host.actors.listOwned().find(candidate => candidate.id === actor.id) });
  } else {
    await host.start();
    actor = await host.actors.create({ name: "mixed-release", instructions: "Original", residency: "durable", scope: "project", coalesce: false }, { asRegistryOwner: true });
    host.actors.tell(actor.id, "HANG_WITH_PROGRESS");
    await waitFor(() => host.actors.status(actor.id).status === "running");
    host.actors.tell(actor.id, "queued-mailbox-one");
    host.actors.tell(actor.id, "queued-mailbox-two");
    await waitFor(() => host.actors.status(actor.id).queued === 2);
    process.send({ actor });
  }
} catch (error) {
  console.error(error);
  await host.close();
  process.exitCode = 1;
  process.disconnect();
}
