import fs from "node:fs";
import net from "node:net";
import path from "node:path";

/**
 * The Node's Jev gateway (smarty-dev#1470; smarty-knowledge-3 org-context/jev-gateway/serve.mjs) as a transport.
 * Protocol: one JSON request line, one JSON reply line, per connection.
 *   { "op": "systemone", "use", "body": "<exact Jev JSON body>", "expiresAt" } -> { "ok": true, "status", "body" }
 *   anything refused                                                      -> { "ok": false, "error", "unsettled"? }
 * The gateway holds the only TypeSafe key, applies the use's validator, budget and rate limit, meters the
 * request, and projects the answer to `{ model, answers: { <id>: { noul } } }` (Noul only; model is the
 * gateway's pin or "other"). Fabric adds no credential on this path.
 */
export interface JevGatewayReply { status: number; body: string }
export interface JevGatewayTransport {
  systemone(use: string, body: string, signal: AbortSignal, expiresAt: number): Promise<JevGatewayReply>;
}

export class JevGatewayError extends Error {
  constructor(message: string, readonly unsettled = false) {
    super(message);
    this.name = "JevGatewayError";
  }
}

const MAX_REPLY_BYTES = 1_048_576;
const uid = (): number | undefined => process.getuid?.();

/** Mirrors the gateway client's socket trust (socket-path.mjs, #507 item 8), read-only:
 * no symlink on the path; the directory is a real directory owned by this uid (or root) and
 * writable by nobody else; the socket is a socket owned by this uid with mode 0600 or 0660.
 * Same-uid races between the check and the connect remain (as in the gateway's own client). */
export function checkGatewaySocket(socket: string): string {
  if (!path.isAbsolute(socket)) throw new JevGatewayError("Jev gateway socket must be an absolute path");
  const resolved = path.resolve(socket);
  const self = uid();
  for (let up = path.dirname(resolved); ; up = path.dirname(up)) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(up); } catch { throw new JevGatewayError("Jev gateway socket directory is unavailable"); }
    if (stat.isSymbolicLink() || !stat.isDirectory()) throw new JevGatewayError("Jev gateway socket path traverses a symlink or non-directory");
    if (process.platform !== "win32" && up === path.dirname(resolved)) {
      if ((stat.uid !== self && stat.uid !== 0) || (stat.mode & 0o022) !== 0) {
        throw new JevGatewayError("Jev gateway socket directory must be owned by this user and not group- or world-writable");
      }
    }
    if (up === path.dirname(up)) break;
  }
  let stat: fs.Stats;
  try { stat = fs.lstatSync(resolved); } catch { throw new JevGatewayError("Jev gateway socket does not exist"); }
  if (stat.isSymbolicLink() || !stat.isSocket()) throw new JevGatewayError("Jev gateway socket is not a socket");
  if (process.platform !== "win32") {
    const mode = stat.mode & 0o7777;
    if (stat.uid !== self || (mode !== 0o600 && mode !== 0o660)) throw new JevGatewayError("Jev gateway socket must be owned by this user with mode 0600 or 0660");
  }
  return resolved;
}

/** One connection per call; the socket is re-checked before every connect. Nothing is retried.
 * `expiresAt` (epoch ms) bounds ALL of the work, connect, write and read alike: it is sent to the gateway
 * as the server-side lifetime of the request, and at that instant the socket is destroyed and the call
 * rejects (unsettled once the line was written). A caller's abort does the same earlier. */
export function createGatewaySocketTransport(socket: string, connect: (file: string) => net.Socket = file => net.connect(file)): JevGatewayTransport {
  return {
    systemone: (use, body, signal, expiresAt) => new Promise<JevGatewayReply>((resolve, reject) => {
      if (signal.aborted) { reject(new JevGatewayError("Jev gateway request cancelled")); return; }
      const remaining = Math.floor(expiresAt - Date.now());
      if (!Number.isFinite(remaining) || remaining <= 0) { reject(new JevGatewayError("Jev gateway request timed out before sending")); return; }
      let checked: string;
      try { checked = checkGatewaySocket(socket); } catch (error) { reject(error); return; }
      let conn: net.Socket;
      try { conn = connect(checked); } catch { reject(new JevGatewayError("Jev gateway socket failed")); return; }
      let done = false;
      let sent = false;
      let buffer = "";
      const finish = (error: Error | undefined, reply?: JevGatewayReply): void => {
        if (done) return;
        done = true;
        clearTimeout(deadline);
        signal.removeEventListener("abort", onAbort);
        conn.destroy();
        if (error) reject(error); else resolve(reply!);
      };
      const onAbort = (): void => finish(new JevGatewayError("Jev gateway request cancelled or timed out", sent));
      // One timer for connect, write and read: nothing outlives the caller's deadline.
      const deadline = setTimeout(() => finish(new JevGatewayError("Jev gateway request timed out", sent)), remaining);
      signal.addEventListener("abort", onAbort, { once: true });
      conn.setEncoding("utf8");
      conn.on("error", () => finish(new JevGatewayError("Jev gateway socket failed", sent)));
      conn.on("close", () => finish(new JevGatewayError("Jev gateway closed without a reply", sent)));
      conn.on("data", (chunk: string) => {
        buffer += chunk;
        if (buffer.length > MAX_REPLY_BYTES) { finish(new JevGatewayError("Jev gateway reply too large", true)); return; }
        const at = buffer.indexOf("\n");
        if (at < 0) return;
        let reply: unknown;
        try { reply = JSON.parse(buffer.slice(0, at)); } catch { finish(new JevGatewayError("Jev gateway reply unreadable", true)); return; }
        const value = reply as { ok?: unknown; status?: unknown; body?: unknown; error?: unknown; unsettled?: unknown } | null;
        if (value?.ok === true && Number.isInteger(value.status) && typeof value.body === "string") {
          finish(undefined, { status: value.status as number, body: value.body });
        } else if (value?.ok === false && typeof value.error === "string") {
          // Refusals are the gateway's own sanitized text (cap, rate, unknown use, validator); bound it anyway.
          finish(new JevGatewayError(`Jev gateway refused: ${value.error.slice(0, 200)}`, value.unsettled === true));
        } else finish(new JevGatewayError("Jev gateway reply misshaped", true));
      });
      conn.on("connect", () => {
        if (done) return;
        sent = true;
        conn.write(`${JSON.stringify({ op: "systemone", use, body, expiresAt })}\n`, error => {
          if (error) finish(new JevGatewayError("Jev gateway socket failed", true));
        });
      });
    }),
  };
}
