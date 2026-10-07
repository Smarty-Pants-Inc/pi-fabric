#!/usr/bin/env node
import { parseDurableWorkerOptions } from "./worker-options.js";

try {
  // This process-only entry is never imported by extension registration.
  const options = parseDurableWorkerOptions(process.argv.slice(2));
  const { runDurableWorker } = await import("./worker-host.js");
  await runDurableWorker(options);
} catch (error) {
  process.stderr.write(`Durable Pi worker failed: ${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
}
