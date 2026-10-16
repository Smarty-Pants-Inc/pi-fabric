import { JetStreamApiError } from "@nats-io/jetstream";
import { PermissionViolationError, AuthorizationError } from "@nats-io/transport-node";
import {
  TransportCapacityError,
  TransportDeniedError,
  TransportInvalidError,
  TransportUncertainError,
} from "../types.js";

/** Classify only definite broker refusals; unknown publish failures may already be stored. */
export function publishFailure(error: unknown, id: string, resolvedBody: unknown): Error {
  let cause = error;
  for (let depth = 0; cause && depth < 6; depth += 1) {
    if (cause instanceof PermissionViolationError || cause instanceof AuthorizationError) {
      return new TransportDeniedError("NATS publish permission denied", { cause: error });
    }
    if (cause instanceof JetStreamApiError) {
      const api = cause.apiError();
      if ([10002, 10023, 10028, 10047, 10053].includes(api.err_code) ||
        (api.err_code === 10077 && /maximum (?:bytes|messages)(?: per subject)? exceeded/.test(api.description))) {
        return new TransportCapacityError("NATS stream capacity exceeded", { cause: error });
      }
      if ([10054, 10060].includes(api.err_code)) {
        return new TransportInvalidError("NATS publish rejected by stream policy", { cause: error });
      }
      break;
    }
    cause = cause instanceof Error ? cause.cause : undefined;
  }
  return new TransportUncertainError(id, resolvedBody, { cause: error });
}
