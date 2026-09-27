/**
 * Closes the actors alongside the steps that close around them. A control handler (a remote
 * agents.ask) can wait on a running actor turn, and only the actors' close grace ends that turn;
 * awaiting the control drain first held a session's exit for a whole turn (smarty-dev#1113).
 */
export const closeWithActors = async (
  actors: { close(): Promise<void> } | undefined,
  ...steps: Array<() => Promise<unknown> | undefined>
): Promise<void> => {
  const actorsClosed = actors?.close();
  try {
    for (const step of steps) await step();
  } finally {
    await actorsClosed;
  }
};
