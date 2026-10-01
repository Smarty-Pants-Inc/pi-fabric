import type { BeforeAgentStartEventResult } from "@earendil-works/pi-coding-agent";

/** Run the whole ordered hook chain, not whichever handler happened to register first.
 * Like Pi, later hooks see prompt replacements and messages accumulate separately.
 * Unlike the host's error reporting, let exceptions fail the test immediately.
 */
export const emitBeforeAgentStart = async <Event extends { systemPrompt: string }>(
  handlers: ReadonlyMap<string, readonly ((event: unknown, context: unknown) => unknown)[]>,
  event: Event,
  context: unknown,
) => {
  const hooks = handlers.get("before_agent_start");
  if (!hooks?.length) throw new Error("before_agent_start handler was not registered");
  let systemPrompt = event.systemPrompt;
  const messages: NonNullable<BeforeAgentStartEventResult["message"]>[] = [];
  for (const hook of hooks) {
    const result = await hook({ ...event, systemPrompt }, context) as BeforeAgentStartEventResult | undefined;
    if (result?.message) messages.push(result.message);
    if (result?.systemPrompt !== undefined) systemPrompt = result.systemPrompt;
  }
  return { systemPrompt, messages };
};
