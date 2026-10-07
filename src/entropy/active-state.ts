// Registration-only pointer. Normalization execution is loaded with the runtime.
import type { CompiledSurfaceFile } from "./compiled-surface.js";
const state: { file: CompiledSurfaceFile | undefined; enabled: boolean } = { file: undefined, enabled: false };
export const setActiveCompiledSurface = (file: CompiledSurfaceFile | undefined, enable = file !== undefined): void => {
  state.file = file;
  state.enabled = enable;
};
export const clearActiveCompiledSurface = (): void => { state.file = undefined; state.enabled = false; };
export const activeCompiledSurface = (): Readonly<typeof state> => state;
