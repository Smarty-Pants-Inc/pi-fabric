import type { BeforeAgentStartEventResult } from "@earendil-works/pi-coding-agent";

/** Run the whole ordered hook chain, not whichever handler registered first.
 * Like Pi, later hooks see section mutations rendered into the prompt, or a
 * forced replacement; messages accumulate separately. Exceptions fail the test.
 */
export const emitBeforeAgentStart = async <Event extends { systemPrompt: string; systemPromptOptions?: unknown }>(
  handlers: ReadonlyMap<string, readonly ((event: unknown, context: unknown) => unknown)[]>,
  event: Event,
  context: unknown,
) => {
  const hooks = handlers.get("before_agent_start");
  if (!hooks?.length) throw new Error("before_agent_start handler was not registered");
  const options = event.systemPromptOptions as { sections?: Record<string, string> } | undefined;
  const systemPromptOptions = { ...options, sections: { ...options?.sections } };
  let systemPrompt = event.systemPrompt;
  let forced: string | undefined;
  const messages: NonNullable<BeforeAgentStartEventResult["message"]>[] = [];
  for (const hook of hooks) {
    const result = await hook({ ...event, systemPrompt, systemPromptOptions }, context) as BeforeAgentStartEventResult | undefined;
    if (result?.message) messages.push(result.message);
    if (result?.systemPrompt !== undefined) forced = result.systemPrompt;
    systemPrompt = forced ?? [event.systemPrompt, ...Object.entries(systemPromptOptions.sections)
      .filter(([, content]) => content).map(([name, content]) => `<${name}>\n${content}\n</${name}>`)].join("\n\n");
  }
  return { systemPrompt, systemPromptOptions, messages };
};
