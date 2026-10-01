// The current host AND its requesting client die without close/abandonment.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
const { ResidentHost } = await import(pathToFileURL(process.argv[2]).href);
const { ResidentActorClient } = await import(pathToFileURL(path.join(path.dirname(process.argv[2]), "actor-client.js")).href);
const config = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const operation = process.argv[4];
const host = new ResidentHost(config);
const waitFor = async predicate => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Current actor did not reach queued state");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
try {
  await host.start();
  const actor = await host.actors.create({ name: "crash-rollback", instructions: "Original", residency: "durable", scope: "project", coalesce: false }, { asRegistryOwner: true });
  host.actors.tell(actor.id, "HANG_WITH_PROGRESS");
  await waitFor(() => host.actors.status(actor.id).status === "running" && host.agents.listForUi().some(agent => agent.toolCalls > 0));
  host.actors.tell(actor.id, "queued-mailbox-one");
  host.actors.tell(actor.id, "queued-mailbox-two");
  await waitFor(() => host.actors.status(actor.id).queued === 2);
  const client = new ResidentActorClient(config.meshRoot, config.rootId);
  const caller = { identity: { id: config.rootId, kind: "main", sessionId: config.sessionId, name: "Main" }, hostId: config.rootId };
  // These calls write synchronously before their first await. No event-loop turn
  // lets the owner claim the request before SIGKILL kills both owner and client.
  void (operation === "actorStatus" ? client.actorStatus(actor.id) :
    operation === "actors" ? client.actors() : client.setActor(
      operation === "setInstructions" ? { operation, id: actor.id, instructions: "Changed" } :
      operation === "setModel" ? { operation, id: actor.id, model: "fixture/visible", scope: "project" } :
      operation === "setThinking" ? { operation, id: actor.id, thinking: "high", scope: "project" } :
      operation === "setActivationFilter" ? { operation, id: actor.id, activationFilter: null } :
      { operation, id: actor.id, tools: ["read"] }, undefined, caller));
  const requests = fs.readdirSync(path.join(config.residencyRoot, "requests"));
  if (requests.length !== 1 || fs.readdirSync(path.join(config.residencyRoot, "processing")).length !== 0) {
    throw new Error("Crash must leave exactly one unclaimed request");
  }
  process.kill(process.pid, "SIGKILL");
} catch (error) {
  console.error(error);
  await host.agents?.close();
  await host.close();
  process.exit(1);
}
