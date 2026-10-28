// A fresh resident process: never pre-register the test adapter in this host.
import fs from "node:fs";
import { ResidentHost } from "../../src/residency/host.js";
import type { ResidentHostConfig } from "../../src/residency/protocol.js";
const config = JSON.parse(fs.readFileSync(process.argv[2]!, "utf8")) as ResidentHostConfig;
const host = new ResidentHost(config);
try {
  await host.start();
  process.send?.({ ready: true, pid: process.pid });
} catch (error) {
  process.send?.({ error: String(error) });
  process.exit(1);
}
process.on("message", async (message: { id: number; operation: string; actorId?: string }) => {
  try {
    if (message.operation === "close") {
      await host.close();
      process.send?.({ id: message.id, result: true });
      process.disconnect?.();
    } else if (message.operation === "status") {
      process.send?.({ id: message.id, result: host.actors.status(message.actorId!) });
    }
  } catch (error) { process.send?.({ id: message.id, error: String(error) }); }
});
