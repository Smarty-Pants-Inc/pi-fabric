import { loadHostSdk } from "./host-sdk.js";

/**
 * The running host's SDK for lazy runtime modules that use its values
 * synchronously. Compiled Fabric never imports the package by name at runtime
 * (see loadHostSdk); awaiting here keeps every importer's own scope
 * synchronous. Never reach this module from the startup graph.
 */
export const PiCodingAgent = await loadHostSdk();
