// The last jev-fabric resolution, for settings and status surfaces. Kept free of
// imports so the lazy UI graph can read it without loading the resolver.
let status: string | undefined;

export const setJevFabricStatus = (value: string): void => { status = value; };

/** Undefined until the first durable or session use resolves a binary. */
export const jevFabricStatus = (): string | undefined => status;
