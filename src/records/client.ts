import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import { writeJsonAtomic } from "../core/atomic-write.js";
import net from "node:net";
import path from "node:path";
import { errorFromWire, LineReader, type WireResponse } from "./protocol.js";
import { MAX_ANCHORS, wireAnchors, type RecordsAnchor, type RecordsVerifyResult } from "./chain.js";
import { RecordsArgumentError } from "./kinds.js";
/** A verify request stays well inside the 1 MiB line (MAX_ANCHORS {seq, hash} anchors are ~0.85 MB). */
const REQUEST_BUDGET_BYTES = 960 * 1024;
import type {
  ClaimedPublication, ConsumerLag, ConsumerState, PageArgs, RecordEnvelope, RecordReceipt, RecordsBackend, RecordsCallOptions,
  RecordsGetPart, RecordsGetResult, RecordsListResult, RecordsOps, RecordsPage, RecordsPrincipal,
} from "./store.js";

/** A principal's credential as the client keeps it: 0600, in the agent's own directory. */
export interface RecordsCredential { id: string; token: string }
/** What is saved before registering: the enrollment nonce, and then the token too. */
interface SavedEnrollment { id: string; nonce?: string; token?: string }

export interface RemoteRecordsOptions {
  socket: string;
  /** This process's participant: the id it registers, or the id in `credentialFile`. */
  identity: { id: string; name?: string };
  /** Where registered tokens are kept (one file per principal). */
  credentialDir: string;
  /** An operator-issued credential (the importer or the mirror); used instead of registering. */
  credentialFile?: string;
  /** Per-call deadline; a call that outlives it is cancelled on the service too. */
  timeoutMs?: number;
}

const credentialPath = (dir: string, id: string) => path.join(dir, `${createHash("sha256").update(id).digest("hex").slice(0, 32)}.json`);

/**
 * The records service's client (C10): Fabric's `records.*` and the inbox, relay and watchdog
 * reach the org's record only through it. The service derives the principal from the token;
 * `principal` arguments are ignored here. An aborted call is cancelled on the service, and
 * closing the client cancels every call in flight.
 */
export class RemoteRecords implements RecordsBackend, RecordsOps {
  org = "";
  origin = "";
  #socket: net.Socket | undefined;
  /** The socket still connecting, owned here so close() destroys it too. */
  #pendingSocket: net.Socket | undefined;
  #connecting: Promise<net.Socket> | undefined;
  /** The client's lifetime: close() aborts it, and every call observes it. */
  readonly #life = new AbortController();
  #next = 1;
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: unknown) => void }>();
  #credential: RecordsCredential | undefined;
  #closed = false;

  constructor(readonly options: RemoteRecordsOptions) {}

  /** Connect, learn the org and origin, and load or obtain this principal's credential. */
  async open(signal?: AbortSignal): Promise<this> {
    const hello = await this.#call("hello", {}, signal, false) as { org: string; origin: string };
    this.org = hello.org;
    this.origin = hello.origin;
    this.#credential = await this.#loadCredential(signal);
    return this;
  }

  get principalId(): string { return this.#credential?.id ?? this.options.identity.id; }

  /**
   * This participant's credential. A saved token the service still knows is reused. Otherwise
   * the client registers with an enrollment nonce it saved (0600) before sending, so a response
   * lost after the service committed is recovered by registering again with the same nonce.
   */
  async #loadCredential(signal?: AbortSignal): Promise<RecordsCredential> {
    if (this.options.credentialFile) return readCredential(this.options.credentialFile);
    const file = credentialPath(this.options.credentialDir, this.options.identity.id);
    let saved: SavedEnrollment | undefined;
    try {
      const value = JSON.parse(fs.readFileSync(file, "utf8")) as SavedEnrollment;
      if (value.id === this.options.identity.id) saved = value;
    } catch { /* not enrolled yet */ }
    if (saved?.token) {
      const known = await this.#call("whoami", {}, signal, true, saved.token).then(() => true, (error: { code?: string }) => {
        if (error.code === "RECORD_UNAUTHENTICATED") return false;
        throw error;
      });
      if (known) return { id: saved.id, token: saved.token };
    }
    const nonce = saved?.nonce ?? randomBytes(32).toString("base64url");
    const save = (value: SavedEnrollment) => {
      writeJsonAtomic(file, value, { newline: true, durable: true });
    };
    // The nonce is on disk before the service can commit anything for it.
    if (!saved?.nonce) save({ id: this.options.identity.id, nonce });
    const issued = await this.#call("register", { id: this.options.identity.id, name: this.options.identity.name, nonce }, signal, false) as RecordsCredential;
    save({ id: issued.id, nonce, token: issued.token });
    return issued;
  }

  #connect(): Promise<net.Socket> {
    if (this.#closed) return Promise.reject(new Error("records client closed"));
    if (this.#socket && !this.#socket.destroyed) return Promise.resolve(this.#socket);
    this.#connecting ??= new Promise<net.Socket>((resolve, reject) => {
      const socket = net.connect(this.options.socket);
      this.#pendingSocket = socket;
      socket.setEncoding("utf8");
      const reader = new LineReader((line) => {
        let response: WireResponse;
        try { response = JSON.parse(line) as WireResponse; } catch { socket.destroy(); return; }
        const waiter = this.#pending.get(response.id);
        if (!waiter) return;
        this.#pending.delete(response.id);
        if (response.ok) waiter.resolve(response.result);
        else waiter.reject(errorFromWire(response.error));
      }, () => socket.destroy());
      socket.on("data", (chunk: string) => reader.push(chunk));
      socket.once("connect", () => {
        this.#pendingSocket = undefined;
        this.#connecting = undefined;
        // Closed while connecting: this socket never carries a call.
        if (this.#closed) { socket.destroy(); reject(new Error("records client closed")); return; }
        this.#socket = socket;
        resolve(socket);
      });
      socket.once("error", (error) => { if (this.#pendingSocket === socket) this.#pendingSocket = undefined; this.#connecting = undefined; reject(Object.assign(new Error(`records service unreachable at ${this.options.socket}: ${error.message}`), { code: "RECORD_SERVICE_UNREACHABLE", retryable: true })); });
      socket.on("close", () => {
        if (this.#socket === socket) this.#socket = undefined;
        if (this.#pendingSocket === socket) {
          this.#pendingSocket = undefined;
          this.#connecting = undefined;
          reject(new Error("records client closed"));
        }
        // The outcome of a call in flight is unknown; a retry with the same key settles it (C3).
        for (const [id, waiter] of this.#pending) {
          this.#pending.delete(id);
          waiter.reject(Object.assign(new Error("records service connection closed; the outcome is unknown: retry with the same key"), { code: "RECORD_SERVICE_UNREACHABLE", retryable: true }));
        }
      });
    });
    return this.#connecting;
  }

  async #call(method: string, args: unknown, signal?: AbortSignal, authenticated = true, tokenOverride?: string): Promise<unknown> {
    signal?.throwIfAborted();
    this.#life.signal.throwIfAborted();
    const socket = await this.#connect();
    signal?.throwIfAborted();
    // Recheck after every wait: a call never starts on a closed client.
    this.#life.signal.throwIfAborted();
    if (socket.destroyed) throw Object.assign(new Error("records service connection closed"), { code: "RECORD_SERVICE_UNREACHABLE", retryable: true });
    const id = this.#next++;
    const token = authenticated ? tokenOverride ?? this.#credential?.token : undefined;
    if (authenticated && !token) throw new Error("records client is not open");
    const deadline = AbortSignal.timeout(this.options.timeoutMs ?? 120_000);
    const stop = AbortSignal.any([...(signal ? [signal] : []), deadline, this.#life.signal]);
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        if (!this.#pending.delete(id)) return;
        socket.write(`${JSON.stringify({ id: this.#next++, method: "cancel", args: { target: id } })}\n`);
        reject(stop.reason);
      };
      stop.addEventListener("abort", onAbort, { once: true });
      this.#pending.set(id, {
        resolve: (value) => { stop.removeEventListener("abort", onAbort); resolve(value); },
        reject: (error) => { stop.removeEventListener("abort", onAbort); reject(error); },
      });
      socket.write(`${JSON.stringify({ id, method, ...(token ? { token } : {}), args })}\n`);
    });
  }

  append(_principal: RecordsPrincipal, args: unknown, options: RecordsCallOptions = {}): Promise<RecordReceipt> {
    return this.#call("append", { args }, options.signal) as Promise<RecordReceipt>;
  }
  read(_principal: RecordsPrincipal, args: unknown, options: RecordsCallOptions = {}): Promise<RecordsPage> {
    return this.#call("read", { args }, options.signal) as Promise<RecordsPage>;
  }
  get(_principal: RecordsPrincipal, args: unknown, options: RecordsCallOptions = {}): Promise<RecordsGetResult> {
    return this.#call("get", { args }, options.signal) as Promise<RecordsGetResult>;
  }
  fold(_principal: RecordsPrincipal, args: unknown, options: RecordsCallOptions = {}): Promise<RecordsGetPart> {
    return this.#call("fold", { args }, options.signal) as Promise<RecordsGetPart>;
  }
  list(_principal: RecordsPrincipal, args: unknown, options: RecordsCallOptions = {}): Promise<RecordsListResult> {
    return this.#call("list", { args }, options.signal) as Promise<RecordsListResult>;
  }
  anchor(_principal: RecordsPrincipal, args: unknown = {}, options: RecordsCallOptions = {}): Promise<RecordsAnchor> {
    return this.#call("anchor", { args }, options.signal) as Promise<RecordsAnchor>;
  }
  verify(_principal: RecordsPrincipal, args: unknown = {}, options: RecordsCallOptions = {}): Promise<RecordsVerifyResult> {
    // Only {seq, hash} goes on the wire, so MAX_ANCHORS of them fit one request line (#118 F2).
    const raw = (args ?? {}) as Record<string, unknown>;
    const sent = typeof raw === "object" && raw !== null && "anchors" in raw ? { ...raw, anchors: wireAnchors(raw.anchors) } : args;
    if (Buffer.byteLength(JSON.stringify(sent)) > REQUEST_BUDGET_BYTES) {
      return Promise.reject(new RecordsArgumentError(`records.verify: the anchors exceed ${REQUEST_BUDGET_BYTES} bytes; send at most ${MAX_ANCHORS} anchors of {seq, hash}`));
    }
    return this.#call("verify", { args: sent }, options.signal) as Promise<RecordsVerifyResult>;
  }
  status(signal?: AbortSignal): Promise<Record<string, unknown>> {
    return this.#call("status", {}, signal) as Promise<Record<string, unknown>>;
  }
  page(args: PageArgs, signal?: AbortSignal): Promise<RecordsPage> {
    return this.#call("page", { ...args, ...(args.to ? { to: [...args.to] } : {}) }, signal) as Promise<RecordsPage>;
  }
  byIds(ids: readonly string[], signal?: AbortSignal): Promise<RecordEnvelope[]> {
    return this.#call("byIds", { ids: [...ids] }, signal) as Promise<RecordEnvelope[]>;
  }
  openConsumer(_consumer: string, names: readonly string[], signal?: AbortSignal): Promise<ConsumerState> {
    return this.#call("openConsumer", { names: [...names] }, signal) as Promise<ConsumerState>;
  }
  async saveConsumer(_consumer: string, after: number, pending: ConsumerState["pending"], signal?: AbortSignal): Promise<void> {
    await this.#call("saveConsumer", { after, pending }, signal);
  }
  claimPublications(limit: number, signal?: AbortSignal): Promise<{ claims: ClaimedPublication[]; more: boolean }> {
    return this.#call("claimPublications", { limit }, signal) as Promise<{ claims: ClaimedPublication[]; more: boolean }>;
  }
  ackPublication(claim: Pick<ClaimedPublication, "claimId" | "recordId">, meshSequence: number, signal?: AbortSignal): Promise<boolean> {
    return this.#call("ackPublication", { claimId: claim.claimId, recordId: claim.recordId, meshSequence }, signal) as Promise<boolean>;
  }
  async failPublication(claim: Pick<ClaimedPublication, "claimId" | "recordId">, error: string, signal?: AbortSignal): Promise<void> {
    await this.#call("failPublication", { claimId: claim.claimId, recordId: claim.recordId, error }, signal);
  }
  async releasePublications(claimId: string, recordIds: readonly string[], signal?: AbortSignal): Promise<void> {
    await this.#call("releasePublications", { claimId, recordIds: [...recordIds] }, signal);
  }
  unpublished(signal?: AbortSignal): Promise<number> {
    return this.#call("unpublished", {}, signal) as Promise<number>;
  }
  lagging(lagMs: number, _now: number, signal?: AbortSignal): Promise<ConsumerLag[]> {
    return this.#call("lagging", { lagMs }, signal) as Promise<ConsumerLag[]>;
  }
  claimAlarm(key: string, _now: number, realarmMs: number, signal?: AbortSignal): Promise<boolean> {
    return this.#call("claimAlarm", { key, realarmMs }, signal) as Promise<boolean>;
  }

  /** Cancel every call in flight (the service rolls them back) and disconnect. */
  close(): void {
    this.#closed = true;
    this.#life.abort(new Error("records client closed"));
    this.#pendingSocket?.destroy();
    this.#socket?.destroy();
  }
}

export const readCredential = (file: string): RecordsCredential => {
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<RecordsCredential>;
  if (typeof value.id !== "string" || typeof value.token !== "string") throw new Error(`records credential ${file} needs id and token`);
  return { id: value.id, token: value.token };
};
