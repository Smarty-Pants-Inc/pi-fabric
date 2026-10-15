import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { PassThrough } from "node:stream";
import readline from "node:readline";
import { describe, expect, it } from "vitest";
import { installTerminalControlFrameFence } from "../src/worker/terminal-boundary.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { markTerminalControlForwarded, readTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

// Keep reason: child RPC ingress/data-safety boundary. Real stream + JSONL
// consumer, production receipt/filter/ACKs. Retain all fixtures, never rm/unlink.
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-frame-fence-"));
  const runId = randomUUID();
  const directory = path.join(root, runId);
  fs.mkdirSync(directory);
  const input = new PassThrough();
  installTerminalControlFrameFence(input, directory, runId);
  const downstream: Array<Record<string, unknown>> = [];
  const reader = readline.createInterface({ input, crlfDelay: Infinity });
  reader.on("line", line => { downstream.push(JSON.parse(line)); });
  const tracked = () => {
    const id = randomUUID();
    trackTerminalControl(directory, { id, delivery: "steer" });
    markTerminalControlForwarded(directory, id);
    return id;
  };
  return { directory, runId, input, reader, downstream, tracked };
};

describe("production terminal control frame fence", () => {
  it("passes prefinal setmode, then discards split-UTF8/multi-frame controls with durable steer ACKs and no downstream handlers", async () => {
    const s = setup();
    const prefinal = { type: "set_steering_mode", mode: "one-at-a-time" };
    const receivedPrefinal = once(s.reader, "line");
    s.input.write(JSON.stringify(prefinal) + "\n");
    await receivedPrefinal;
    expect(s.downstream).toEqual([prefinal]);

    const held = s.tracked();
    const late = s.tracked();
    const eof = s.tracked();
    // The first frame starts BEFORE final and finishes AFTER final. The split
    // bisects a four-byte UTF8 character, so a chunk-local parser would be wrong.
    const frame = Buffer.from(JSON.stringify({ type: "steer", id: held, message: "WRITE 🧭" }) + "\n");
    const split = frame.indexOf(Buffer.from("🧭")) + 1;
    expect(split).toBeGreaterThan(0);
    s.input.write(frame.subarray(0, split));
    expect(s.downstream).toEqual([prefinal]);
    expect(readTerminalControl(s.directory, held)?.state).toBe("queued");
    saveFinalAnswerReceipt(s.directory, s.runId, "done");

    const controls = [
      { type: "prompt", id: late, message: `/fabric-delivery ${late}` },
      { type: "set_steering_mode", mode: "all" },
      { type: "set_follow_up_mode", mode: "all" },
      { type: "compact" },
      { type: "get_state" },
      { type: "set_model", provider: "fake", modelId: "must-not-run" },
      { type: "set_thinking_level", level: "high" },
    ];
    s.input.write(Buffer.concat([frame.subarray(split), Buffer.from(controls.map(command => JSON.stringify(command) + "\n").join(""))]));
    expect(readTerminalControl(s.directory, held)).toMatchObject({ state: "refused", forwarded: true });
    expect(readTerminalControl(s.directory, late)).toMatchObject({ state: "refused", forwarded: true });
    expect(s.downstream).toEqual([prefinal]);

    // A valid last frame without LF is still buffered until actual stream EOF.
    s.input.write(JSON.stringify({ type: "steer", id: eof, message: "WRITE EOF_SENTINEL" }));
    expect(readTerminalControl(s.directory, eof)?.state).toBe("queued");
    const closed = once(s.reader, "close");
    s.input.end();
    await closed;
    expect(readTerminalControl(s.directory, eof)).toMatchObject({ state: "refused", forwarded: true });
    expect(s.downstream).toEqual([prefinal]);
  });

  it("drops a malformed EOF partial without downstream action or an invented control ACK", async () => {
    const s = setup();
    const id = s.tracked();
    saveFinalAnswerReceipt(s.directory, s.runId, "done");
    // Truncated JSON is not identity evidence; neither handlers nor guessed ACKs.
    s.input.write(`{"type":"steer","id":"${id}","message":"incomplete`);
    const closed = once(s.reader, "close");
    s.input.end();
    await closed;
    expect(s.downstream).toEqual([]);
    expect(readTerminalControl(s.directory, id)).toMatchObject({ state: "queued", forwarded: true });
  });
});
