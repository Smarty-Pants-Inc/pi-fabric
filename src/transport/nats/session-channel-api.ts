import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { SessionChannel, SessionChannelOpenOptions } from "./session-channel.js";

export interface FabricSessionChannelApi {
  open(options: SessionChannelOpenOptions): Promise<SessionChannel>;
}

export const SESSION_CHANNEL_API_GET = "fabric:session-channel:get";
export const SESSION_CHANNEL_API_READY = "fabric:session-channel:ready";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isSessionChannelApi = (value: unknown): value is FabricSessionChannelApi =>
  isRecord(value) && typeof value.open === "function";

/** Register the Fabric responder, announce readiness, and expose the API on Fabric's own pi object. */
export function registerSessionChannelApi(
  pi: Pick<ExtensionAPI, "events">,
  api: FabricSessionChannelApi,
  ownApi?: ExtensionAPI,
): () => void {
  const unsubscribe = pi.events.on(SESSION_CHANNEL_API_GET, (value) => {
    if (!isRecord(value) || typeof value.resolve !== "function") return;
    (value.resolve as (result: FabricSessionChannelApi) => void)(api);
  });
  if (ownApi) {
    const host = ownApi as ExtensionAPI & { fabric?: Record<string, unknown> };
    const previous = isRecord(host.fabric) ? host.fabric : {};
    host.fabric = Object.freeze({ ...previous, sessionChannel: api });
  }
  pi.events.emit(SESSION_CHANNEL_API_READY, { sessionChannel: api });
  return unsubscribe;
}

/** Obtain Fabric's session-channel API regardless of which extension factory loads first. */
export function requestSessionChannelApi(pi: Pick<ExtensionAPI, "events">): Promise<FabricSessionChannelApi> {
  return new Promise((resolve) => {
    let settled = false;
    let unsubscribeReady: (() => void) | undefined;
    const accept = (value: unknown): void => {
      if (settled || !isSessionChannelApi(value)) return;
      settled = true;
      unsubscribeReady?.();
      resolve(value);
    };
    unsubscribeReady = pi.events.on(SESSION_CHANNEL_API_READY, (value) => {
      if (isRecord(value)) accept(value.sessionChannel);
    });
    pi.events.emit(SESSION_CHANNEL_API_GET, { resolve: accept });
  });
}
