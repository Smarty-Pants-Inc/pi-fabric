#!/usr/bin/env node
// smarty-dev#6207: load the real -e sender hook from this child's argv, with this child's env,
// and report the header map it writes onto an outbound request; then behave as the fake Pi.
import { pathToFileURL } from "node:url";

const argv = process.argv.slice(2);
const hookPath = argv.find((arg, index) => argv[index - 1] === "-e" && arg.replaceAll("\\", "/").endsWith("/guards/sender-headers.js"));
let headers = null;
if (hookPath) {
  const { default: hook } = await import(pathToFileURL(hookPath).href);
  let handler;
  hook({ on: (name, fn) => { if (name === "before_provider_headers") handler = fn; }, getSessionName: () => undefined });
  headers = { "X-Session-Id": "unmodified" };
  await handler?.({ type: "before_provider_headers", headers });
}
process.stdout.write(JSON.stringify({ type: "fake_sender_headers", hookLoaded: Boolean(hookPath), headers }) + "\n");
await import("./fake-pi-model.mjs");
