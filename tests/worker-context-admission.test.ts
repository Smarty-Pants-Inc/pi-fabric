import { fileURLToPath } from "node:url";
import { expect, it, vi } from "vitest";
import { ActorContextAdmission, loadActorInputEstimator } from "../src/worker/context-admission.js";
const fixture = (window = 272_000) => {
  const sent: Record<string, unknown>[] = [];
  const ready = vi.fn(); const fail = vi.fn(); const compact = vi.fn();
  const admission = new ActorContextAdmission("actor-run", "current event", "actor persona", value => Math.ceil(value.length / 4), {
    send: frame => sent.push(frame), ready, fail, compact,
  });
  const reply = (data: unknown, success = true) => {
    const frame = sent.at(-1)!;
    expect(admission.observe({ type: "response", id: frame.id, command: frame.type, success, data, error: success ? undefined : "compaction refused" })).toBe(true);
  };
  const start = () => { admission.start(); reply({ model: { contextWindow: window }, isStreaming: false, isCompacting: false }); };
  return { admission, sent, ready, fail, compact, reply, start };
};
it("280k > 272k compacts before admitting the activation, then measures the new context", () => {
  const f = fixture(); f.start(); f.reply({ messages: [{ role: "user", content: "x".repeat(280_000 * 4) }] });
  expect(f.sent.map(frame => frame.type)).toEqual(["get_state", "get_messages", "compact"]);
  expect(f.ready).not.toHaveBeenCalled(); expect(f.fail).not.toHaveBeenCalled(); expect(f.compact).toHaveBeenCalledTimes(1);
  f.reply({ summary: "retained objectives" }); expect(f.sent.at(-1)?.type).toBe("get_messages");
  f.reply({ messages: [{ role: "user", content: "retained objectives" }] });
  expect(f.ready).toHaveBeenCalledTimes(1); expect(f.fail).not.toHaveBeenCalled();
});
it("small context dispatches without compaction", () => {
  const f = fixture(); f.start(); f.reply({ messages: [] });
  expect(f.ready).toHaveBeenCalledTimes(1); expect(f.compact).not.toHaveBeenCalled();
});
it.each(["still oversized", "compact error"])("%s refuses once, never retries oversized inference", mode => {
  const f = fixture(); f.start(); f.reply({ messages: [{ content: "x".repeat(280_000 * 4) }] });
  if (mode === "compact error") f.reply(undefined, false);
  else { f.reply({ summary: "summary" }); f.reply({ messages: [{ content: "x".repeat(280_000 * 4) }] }); }
  expect(f.ready).not.toHaveBeenCalled(); expect(f.fail).toHaveBeenCalledTimes(1);
  expect(f.sent.filter(frame => frame.type === "compact")).toHaveLength(1);
});
it("loads the pure estimator from selected native launcher ancestry, not a worker peer", async () => {
  const estimate = await loadActorInputEstimator(fileURLToPath(new URL("../node_modules/@earendil-works/pi-coding-agent/dist/cli.js", import.meta.url)));
  expect(estimate).toBeTypeOf("function"); expect(estimate!("x".repeat(280_000 * 4))).toBeGreaterThan(272_000);
  expect(await loadActorInputEstimator("/missing/opaque-launcher")).toBeUndefined();
});
it("correlates responses and refuses non-idle model state", () => {
  const f = fixture(); f.admission.start();
  expect(f.admission.observe({ type: "response", command: "get_state", id: "other", success: true })).toBe(false);
  f.reply({ model: { contextWindow: 272_000 }, isStreaming: true, isCompacting: false });
  expect(f.fail).toHaveBeenCalledTimes(1); expect(f.ready).not.toHaveBeenCalled();
});
