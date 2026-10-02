// Measurement, not a timing-sensitive CI assertion. Run with Bun after a change:
// bun scripts/benchmark-message-identifiers.ts
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { outgoingMessageNotice } from "../src/providers/message-id-notice.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const bytes = 2 * 1024 * 1024;
const noise = "The command reports ordinary test output and continued work. ";
const history = noise.repeat(Math.ceil(bytes / noise.length)).slice(0, bytes);
const entries = Array.from({ length: 500 }, (_, i) => ({
  id: String(i), parentId: i ? String(i - 1) : null, type: "message",
  message: { role: "toolResult", content: [{ type: "text", text: history.slice(Math.floor(i * bytes / 500), Math.floor((i + 1) * bytes / 500)) }] },
}));
let accesses = 0;
const sessionManager = {
  getSessionId: () => "benchmark", getLeafId: () => { accesses++; return "499"; },
  getEntry: (id: string) => { accesses++; return entries[Number(id)]; },
} as unknown as ExtensionContext["sessionManager"];
const context = { extensionContext: { sessionManager } } as FabricInvocationContext;
const ids = "session:01a0cd9c-7c24-72d5-ae80-03d729baf901 01a0cd9c-7c24-72d5-ae80-03d729baf902 aaaa1111bbbb2222cccc3333dddd4444 bbbb1111cccc2222dddd3333eeee4444 commit 1234567890abcdef1234567890abcdef12345678 head abc1234 `def567890abc` #issuecomment-1234567890 pid 987654 ";
const noIds = noise.repeat(200).slice(0, 10 * 1024);
const withIds = (ids + noIds).slice(0, 10 * 1024);
const atLimit = (kind: "pid" | "actor") => {
  const identifiers = Array.from({ length: 128 }, (_, i) => kind === "pid"
    ? `pid ${987650000 + i}` : `actor:${i.toString(16).padStart(32, "a")}`).join("; ");
  return (identifiers + noIds).slice(0, 10 * 1024);
};
const first = performance.now();
const result = await outgoingMessageNotice(withIds, context);
const firstUseMs = performance.now() - first;
if (!result.notice?.startsWith("unverified ids: ") || result.notice.includes("check failed")) throw new Error("Benchmark did not exercise history checks");

const measure = async (name: string, text: string) => {
  for (let i = 0; i < 20; i++) await outgoingMessageNotice(text, context);
  accesses = 0;
  const times: number[] = [];
  const cpuStart = process.cpuUsage();
  for (let i = 0; i < 100; i++) {
    const start = performance.now();
    await outgoingMessageNotice(text, context);
    times.push(performance.now() - start);
  }
  const cpu = process.cpuUsage(cpuStart);
  times.sort((a, b) => a - b);
  return { name, messageBytes: Buffer.byteLength(text), historyBytes: bytes, samples: times.length,
    medianMs: +times[50]!.toFixed(4), p95Ms: +times[95]!.toFixed(4), maxMs: +times[99]!.toFixed(4),
    cpuMsPerCheck: +((cpu.user + cpu.system) / 1000 / times.length).toFixed(4), historyApiCalls: accesses };
};
console.log(JSON.stringify({ firstUseMs: +firstUseMs.toFixed(4), reports: [
  await measure("no identifiers / no history access", noIds),
  await measure("nine identifiers / 500 entries / 2 MiB", withIds),
  await measure("128 PIDs / 500 entries / 2 MiB", atLimit("pid")),
  await measure("128 actors / 500 entries / 2 MiB", atLimit("actor")),
] }, null, 2));
