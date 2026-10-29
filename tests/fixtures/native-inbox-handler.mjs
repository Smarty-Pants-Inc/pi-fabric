// TEST ONLY: an application-owned transaction behind the frozen native actor RPC.
// No registration, transport, model calls or external effects live here.
import { createHash, randomUUID } from "node:crypto";
import { isAbsolute } from "node:path";
import { DatabaseSync } from "node:sqlite";

export const handlerKey = "smarty-inbox.rpc.v1";
const SESSION = /^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const record = (value) => value !== null && typeof value === "object"
  && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const fields = (value, keys) => record(value) && Reflect.ownKeys(value).length === keys.length
  && keys.every((key) => Object.hasOwn(value, key));
const token = (value) => typeof value === "string" && value.length > 0 && value.length <= 256
  && value.trim().length > 0 && !/[\u0000-\u001f\u007f]/.test(value);
const identityKey = (link, machine, sender) => JSON.stringify([link, machine, sender]);
const failure = (requestId, status) => ({ data: { v: 1, requestId, ok: false, status, error: { code: status } } });
const success = (requestId, operation, result) => ({ data: {
  v: 1, requestId, ok: true, status: { add: "added", list: "listed", resolve: "resolved" }[operation], result,
} });
const item = (row) => ({ id: row.id, text: row.text, resolved: row.resolved === 1 });

/**
 * seats: [{ link, sourceMachine, senderId: "session:<uuid>", seat }].
 * The copied pins are trusted test configuration; context is supplied by the host,
 * not cryptographically authenticated by this fixture. Never expose this callback
 * as an unguarded public endpoint. databasePath must name a test-owned on-disk DB.
 *
 * Closed args: add {text: nonblank string <=4096}, list {}, resolve {itemId}.
 * Success results: {item:{id,text,resolved}} or {items:[...]}. All statuses are
 * fixture-only. Even list snapshots dedupe by (seat,operation,opKey); use a new
 * opKey for a fresh view. Cached results never store an old requestId envelope.
 * The returned function has test-only close() and constant-size readback() APIs.
 */
export function createNativeInboxHandler({ databasePath, seats }) {
  if (typeof databasePath !== "string" || !isAbsolute(databasePath) || !Array.isArray(seats) || seats.length === 0) {
    throw new Error("Invalid inbox fixture configuration");
  }
  const pins = new Map();
  for (const pin of seats) {
    if (!fields(pin, ["link", "sourceMachine", "senderId", "seat"])
      || ![pin.link, pin.sourceMachine, pin.senderId, pin.seat].every(token) || !SESSION.test(pin.senderId)) {
      throw new Error("Invalid inbox fixture configuration");
    }
    const key = identityKey(pin.link, pin.sourceMachine, pin.senderId);
    if (pins.has(key)) throw new Error("Duplicate inbox fixture identity");
    pins.set(key, pin.seat);
  }

  const db = new DatabaseSync(databasePath, { timeout: 1000 });
  try {
    db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      PRAGMA foreign_keys = ON;
      PRAGMA busy_timeout = 1000;
      CREATE TABLE IF NOT EXISTS fixture_items (
        id TEXT PRIMARY KEY, seat TEXT NOT NULL, text TEXT NOT NULL,
        resolved INTEGER NOT NULL CHECK (resolved IN (0, 1))
      ) STRICT;
      CREATE TABLE IF NOT EXISTS fixture_operations (
        seat TEXT NOT NULL, operation TEXT NOT NULL CHECK (operation IN ('add','list','resolve')),
        op_key TEXT NOT NULL, args_digest TEXT NOT NULL, result_json TEXT NOT NULL,
        PRIMARY KEY (seat, operation, op_key)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS fixture_effects (
        seq INTEGER PRIMARY KEY, seat TEXT NOT NULL, operation TEXT NOT NULL,
        item_id TEXT NOT NULL REFERENCES fixture_items(id)
      ) STRICT;
      CREATE TRIGGER IF NOT EXISTS fixture_added AFTER INSERT ON fixture_items BEGIN
        INSERT INTO fixture_effects(seat, operation, item_id) VALUES (NEW.seat, 'add', NEW.id);
      END;
      CREATE TRIGGER IF NOT EXISTS fixture_resolved AFTER UPDATE OF resolved ON fixture_items
        WHEN OLD.resolved != NEW.resolved BEGIN
        INSERT INTO fixture_effects(seat, operation, item_id) VALUES (NEW.seat, 'resolve', NEW.id);
      END;
    `);
  } catch (error) { db.close(); throw error; }

  const owned = db.prepare("SELECT id, text, resolved FROM fixture_items WHERE seat = ? AND id = ?");
  const cached = db.prepare("SELECT args_digest, result_json FROM fixture_operations WHERE seat = ? AND operation = ? AND op_key = ?");
  const save = db.prepare("INSERT INTO fixture_operations(seat, operation, op_key, args_digest, result_json) VALUES (?, ?, ?, ?, ?)");
  const add = db.prepare("INSERT INTO fixture_items(id, seat, text, resolved) VALUES (?, ?, ?, 0)");
  const resolve = db.prepare("UPDATE fixture_items SET resolved = 1 WHERE seat = ? AND id = ? AND resolved = 0");
  const list = db.prepare("SELECT id, text, resolved FROM fixture_items WHERE seat = ? ORDER BY id");
  const counts = db.prepare(`SELECT
    (SELECT COUNT(*) FROM fixture_items) AS items,
    (SELECT COUNT(*) FROM fixture_operations) AS operations,
    (SELECT COUNT(*) FROM fixture_effects WHERE operation = 'add') AS addEffects,
    (SELECT COUNT(*) FROM fixture_effects WHERE operation = 'resolve') AS resolveEffects`);
  let closed = false;

  function authorizedSeat(context) {
    if (!fields(context, ["actorId", "source", "sender", "sourceMachine", "link", "commandId", "sourceCommandId", "sourceEventId", "deadlineAt", "signal"])
      || context.source !== "nats-bridge" || typeof context.actorId !== "string" || !/^[0-9a-f]{32}$/.test(context.actorId)
      || !fields(context.sender, ["id", "kind", "sessionId"]) || context.sender.kind !== "main"
      || !token(context.sender.id) || !SESSION.test(context.sender.id) || context.sender.sessionId !== context.sender.id.slice(8)
      || ![context.link, context.sourceMachine, context.commandId, context.sourceCommandId, context.sourceEventId].every(token)
      || !Number.isSafeInteger(context.deadlineAt) || context.deadlineAt <= 0
      || !(context.signal instanceof AbortSignal)) return undefined;
    return pins.get(identityKey(context.link, context.sourceMachine, context.sender.id));
  }

  const handle = async (request, context) => {
    const requestId = record(request) && record(request.data) ? request.data.requestId : undefined;
    if (!token(requestId)) throw new Error("Invalid inbox fixture request");
    if (!fields(request, ["message", "data"]) || request.message !== handlerKey
      || !fields(request.data, ["v", "requestId", "operation", "opKey", "args"])) return failure(requestId, "invalid-request");
    const { v, operation, opKey, args } = request.data;
    if (v !== 1 || !["add", "list", "resolve"].includes(operation) || !token(opKey)) return failure(requestId, "invalid-request");
    // Fixed primitive schemas also define the canonical semantic argument encoding.
    let semanticArgs;
    if (operation === "add" && fields(args, ["text"]) && typeof args.text === "string"
      && args.text.trim().length > 0 && args.text.length <= 4096) semanticArgs = { text: args.text };
    if (operation === "list" && fields(args, [])) semanticArgs = {};
    if (operation === "resolve" && fields(args, ["itemId"]) && token(args.itemId)) semanticArgs = { itemId: args.itemId };
    if (!semanticArgs) return failure(requestId, "invalid-request");
    const seat = authorizedSeat(context);
    if (seat === undefined) return failure(requestId, "unauthorized");
    if (closed) return failure(requestId, "closed");
    const digest = createHash("sha256").update(JSON.stringify(semanticArgs)).digest("hex");
    // No await between these admission checks and BEGIN. No post-commit abort can
    // turn a committed application effect into a claimed rollback/not-run result.
    if (context.signal.aborted) return failure(requestId, "aborted");
    if (context.deadlineAt <= Date.now()) return failure(requestId, "expired");
    try {
      db.exec("BEGIN IMMEDIATE");
      let reply;
      // Membership precedes cache lookup, including for conflicting/reused opKeys.
      const target = operation === "resolve" ? owned.get(seat, semanticArgs.itemId) : undefined;
      if (operation === "resolve" && !target) reply = failure(requestId, "not-found");
      else {
        const previous = cached.get(seat, operation, opKey);
        if (previous) reply = previous.args_digest === digest
          ? success(requestId, operation, JSON.parse(previous.result_json)) : failure(requestId, "conflict");
        else {
          let result;
          if (operation === "add") {
            const id = randomUUID();
            add.run(id, seat, semanticArgs.text);
            result = { item: { id, text: semanticArgs.text, resolved: false } };
          } else if (operation === "list") result = { items: list.all(seat).map(item) };
          else {
            resolve.run(seat, semanticArgs.itemId);
            result = { item: { ...item(target), resolved: true } };
          }
          save.run(seat, operation, opKey, digest, JSON.stringify(result));
          reply = success(requestId, operation, result);
        }
      }
      db.exec("COMMIT");
      return reply;
    } catch {
      try { if (db.isTransaction) db.exec("ROLLBACK"); }
      catch { try { db.close(); } catch {} closed = true; }
      return failure(requestId, "storage-error");
    }
  };
  handle.readback = () => {
    if (closed) throw new Error("Inbox fixture closed");
    return { ...counts.get() }; // Counts of real rows and trigger-recorded mutations, not JS calls.
  };
  handle.close = () => { if (!closed) { db.close(); closed = true; } };
  return handle;
}
