import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { followUpFile, releaseFollowUpPayload, settleFollowUp } from "../src/agents/follow-up-delivery.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("follow-up prompt payload retention", () => {
  it.each(["queued", "delivered", "cancelled"] as const)("keeps exact payload bytes after %s", state => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-follow-up-payload-"));
    roots.push(root);
    const run = path.join(root, "run");
    const id = "11111111-1111-4111-8111-111111111111";
    const admission = followUpFile(run, id);
    fs.mkdirSync(path.dirname(admission), { recursive: true });
    fs.writeFileSync(admission, JSON.stringify({ messageId: id, deadlineAt: 1234 }));
    const payload = path.join(run, "deliveries", `${id}.json`);
    const bytes = Buffer.from(JSON.stringify({ message: "retain this prompt π", delivery: "followUp", images: [{ data: "cGl4ZWxz" }] }));
    fs.mkdirSync(path.dirname(payload), { recursive: true });
    fs.writeFileSync(payload, bytes);
    if (state !== "queued") settleFollowUp(admission, state);

    releaseFollowUpPayload(admission);

    expect(fs.readFileSync(payload)).toEqual(bytes);
  });
});
