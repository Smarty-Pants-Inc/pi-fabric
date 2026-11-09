import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

const commands = ["queue", "requeue", "dequeue", "refresh"] as const;
const allowedSenders = [
  "sender-permission >= write",
  "sender = smarty-fleet-write[bot]",
  "sender = smarty-fleet-write-leads[bot]",
  "sender = smarty-fleet-write-lanes[bot]",
  "sender = smarty-fleet-write-versehq[bot]",
  "sender = smarty-poc-org[bot]",
];

// These commands affect merge-queue state. Keep write permission and only the explicitly trusted bot
// identities; admitting arbitrary senders would widen this authorization boundary.
const config = parse(fs.readFileSync(fileURLToPath(new URL("../.mergify.yml", import.meta.url)), "utf8")) as {
  commands_restrictions: Record<typeof commands[number], { conditions: Array<{ or: string[] }> }>;
};

describe("Mergify command restrictions", () => {
  it.each(commands)("allows only the writer permission and named bots for %s", (command) => {
    expect(config.commands_restrictions[command]).toEqual({
      conditions: [{ or: allowedSenders }],
    });
  });
});
