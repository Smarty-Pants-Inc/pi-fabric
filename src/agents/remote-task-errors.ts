export const REMOTE_TASK_ERROR_CODES = [
  "FABRIC_REMOTE_TASK_NO_OWNER", "FABRIC_REMOTE_TASK_BRIDGE_STALE",
  "FABRIC_REMOTE_TASK_POLICY_REFUSED", "FABRIC_REMOTE_TASK_LOST",
] as const;
export type RemoteTaskErrorCode = typeof REMOTE_TASK_ERROR_CODES[number];
export const isRemoteTaskErrorCode = (code: unknown): code is RemoteTaskErrorCode =>
  typeof code === "string" && (REMOTE_TASK_ERROR_CODES as readonly string[]).includes(code);
export class RemoteTaskError extends Error {
  constructor(readonly code: RemoteTaskErrorCode, message: string, options?: ErrorOptions) {
    super(`${code}: ${message}`, options);
    this.name = "RemoteTaskError";
  }
}
