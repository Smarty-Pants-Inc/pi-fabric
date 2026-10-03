/** A transient ownership fence, never a terminal handler refusal. */
export class MeshConsumptionPausedError extends Error {
  constructor() { super("Resident host lease unavailable for mesh consumption"); }
}

export const assertMeshConsumption = (canConsume?: () => boolean): void => {
  if (canConsume?.() === false) throw new MeshConsumptionPausedError();
};
