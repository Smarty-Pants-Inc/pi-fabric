import { expect, vi } from "vitest";
import { controlOwnerIncarnation } from "../../src/topology/control-plane.js";
import type { FabricParticipantSource } from "../../src/topology/types.js";

/** Direct transport fixtures capture the target owner's published activation, never the sender's. */
export const liveControlOwnerIncarnation = (
  participants: Pick<FabricParticipantSource, "get">,
  targetId: string,
): Promise<string> => vi.waitFor(() => {
  const target = participants.get(targetId, undefined, { fresh: true });
  expect(target, `live control owner for ${targetId}`).toBeDefined();
  return controlOwnerIncarnation(target!);
}, { timeout: 5_000, interval: 20 });
