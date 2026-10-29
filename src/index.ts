import { createFabricExtension } from "./extension.js";
export { FABRIC_MANAGED_HOST_VERSION } from "./extension.js";
export type { FabricManagedHostOptions } from "./managed-host.js";
export default createFabricExtension(import.meta.url);

export * from "./audit/index.js";
export * from "./entropy/index.js";
export * from "./protocol.js";
export { registerSessionRecoveryAdapters, SESSION_RECOVERY_ADAPTERS_GET } from "./recovery/l7-interfaces.js";
export type {
  PortableIdentityEvidence, PortableSessionBinding, PortableIdentityAdapter,
  SessionRecoveryActivation, TransportHostLeasePredecessorSnapshot,
  RecoveryAcceptedInput, RecoveryInputSnapshot, ContinuationRequest, ContinuationAdmission,
  RecoveryInputAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult, RecoveryDurabilityAdapter,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult,
  RecoveryNativeEntryLocation, RecoveryNativeFlushReceipt,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
  SettledRecoveryEffect, RecoveryEffectsSettlement, RecoveryToolResult,
  RecoveryToolResultSource, RecoveryToolResultRead, RecoveryToolResultTransform, RecoveryMaterializedResultReceipt,
  RecoveryEffectAdapter, SessionRecoveryAdapters,
} from "./recovery/l7-interfaces.js";
export { requestSessionChannelApi, SESSION_CHANNEL_API_GET, SESSION_CHANNEL_API_READY } from "./transport/nats/session-channel-api.js";
export type { FabricSessionChannelApi } from "./transport/nats/session-channel-api.js";
export { FabricModelDeniedError } from "./core/model-policy.js";
export { FabricModelNotServedError } from "./core/model-served-error.js";
export type {
  CallerIdentity,
  SessionChannel,
  SessionChannelEvent,
  SessionChannelOpenOptions,
  SessionChannelReply,
  SessionChannelRequest,
  SessionChannelIdentity,
} from "./transport/nats/session-channel.js";
