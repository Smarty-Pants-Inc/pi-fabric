import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { requireNamespaceBarrier, syncDirectoryChain } from "../core/atomic-write.js";
import { EffectContractError } from "./contracts.js";
import type {
  EffectAttemptIdentity, EffectIntent, EffectPermission, EffectRecord, EffectTransition,
  EffectTransitionInput, RecoveryOwner,
} from "./contracts.js";
import { canonicalEncode, intentBindingDigest, mintEffectKey, sha256Bytes, validateIntent } from "./slots.js";
import { maintenanceRepairKey, validateMaintenanceRepairBody, validateStoredMaintenanceRepair } from "./repair-records.js";
import type { MaintenanceRepairRecordBody, StoredMaintenanceRepair } from "./repair-records.js";

/** Synchronous ledger operations, valid only in their owning withSession callback.
 * The callback may await a sink without releasing the session's serialization. */
export interface EffectStoreTransaction {
  bind(intent: EffectIntent): EffectRecord;
  read(key: string): EffectRecord;
  list(): readonly EffectRecord[];
  append(key: string, input: EffectTransitionInput): EffectRecord;
  readMaintenanceRepair(originalInputId: string): StoredMaintenanceRepair | undefined;
  listMaintenanceRepairs(): readonly StoredMaintenanceRepair[];
  publishMaintenanceRepair(candidate: MaintenanceRepairRecordBody): StoredMaintenanceRepair;
}

const KEY = /^[0-9a-f]{64}$/;
const validKey = (value: unknown): value is string => typeof value === "string" && value.length === 64 && KEY.test(value);
const TRANSITION_FILE = /^\d{12}\.json$/;
const PUBLICATION_WITNESS = /^\.published-(intent\.json|receipt\.json|\d{12}\.json)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.tmp$/;
const REPAIRS_DIRECTORY = "repairs-v1";
const MAX_SEQUENCE = 999_999_999_999;
const UNCERTAIN = "EFFECT_STORE_UNCERTAIN";
const INPUT_FIELDS = new Set([
  "state", "toolCallId", "parentToolCallId", "attempt", "at", "recoveryGeneration",
  "outcome", "result", "resultBytesHash", "receipt", "reason",
]);
const TRANSITION_FIELDS = new Set([...INPUT_FIELDS, "format", "version", "key", "sequence", "previousDigest", "digest"]);
const ATTEMPT_FIELDS = ["podUID", "bootId", "ownerIncarnation", "authorityRevision"] as const;

interface SessionScope { active: boolean; usable: boolean; failure?: EffectContractError }
// These are in-process serialization and a failed-durability admission latch, NOT
// a lease service. B3 excludes old writers and qualifies settlement after restart.
const sessionTails = new Map<string, Promise<void>>();
const admissionFailures = new Map<string, EffectContractError>();
const scopes = new AsyncLocalStorage<ReadonlyMap<string, SessionScope>>();

const codeOf = (error: unknown): unknown =>
  typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
const own = (value: object, field: string): boolean => Object.prototype.hasOwnProperty.call(value, field);
const sameInode = (a: fs.Stats, b: fs.Stats): boolean => a.dev === b.dev && a.ino === b.ino;
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const sequenceFile = (sequence: number): string => `${String(sequence).padStart(12, "0")}.json`;

const freeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};

const uncertainty = (key: string | undefined, error: unknown): EffectContractError =>
  new EffectContractError(UNCERTAIN,
    `Effect ledger${key === undefined ? "" : ` ${key}`} is uncertain: ${error instanceof Error ? error.message : String(error)}`,
    key === undefined ? undefined : { key, state: "UNCERTAIN" });

const invalidTransition = (message: string): never => {
  throw new EffectContractError("EFFECT_INVALID_TRANSITION", message);
};

const checkAttempt = (attempt: unknown): attempt is EffectAttemptIdentity =>
  object(attempt) && Object.keys(attempt).length === ATTEMPT_FIELDS.length && ATTEMPT_FIELDS.every(field => text(attempt[field]));

/** Validation is shared by publication and replay; replay additionally requires the stored hash. */
const checkTransitionInput = (input: EffectTransitionInput, stored: boolean): void => {
  if (!object(input) || !text(input.toolCallId) || !checkAttempt(input.attempt) ||
      !text(input.at) || !Number.isFinite(Date.parse(input.at))) {
    invalidTransition("A transition requires a physical call, attempt identity and timestamp");
  }
  if (own(input, "parentToolCallId") && !text(input.parentToolCallId)) invalidTransition("Invalid parent call alias");
  if (own(input, "reason") && !text(input.reason)) invalidTransition("Invalid transition reason");
  if (own(input, "recoveryGeneration")) {
    if (!text(input.recoveryGeneration)) invalidTransition("Recovery generation must be a nonempty Unicode identifier");
    canonicalEncode(input.recoveryGeneration);
  }
  // A durable intent is itself INTENT. No synthetic tail is required or admitted.
  switch (input.state) {
    case "RESULT":
      if (input.outcome !== "committed" && input.outcome !== "not-committed") {
        invalidTransition("RESULT requires a definitive outcome");
      }
      if (input.outcome === "committed" && !own(input, "result")) {
        invalidTransition("A committed RESULT requires its original result");
      }
      break;
    case "UNCERTAIN":
      if (own(input, "outcome") && input.outcome !== "uncertain") invalidTransition("UNCERTAIN cannot claim a definitive outcome");
      break;
    case "ATTEMPT":
    case "ALIAS":
      if (own(input, "outcome") || own(input, "result") || own(input, "resultBytesHash")) {
        invalidTransition(`${input.state} cannot claim an outcome or result`);
      }
      break;
    default:
      invalidTransition("Only ATTEMPT, ALIAS, RESULT and UNCERTAIN may follow an intent");
  }
  if (own(input, "result")) {
    const digest = sha256Bytes(canonicalEncode(input.result));
    if ((stored || own(input, "resultBytesHash")) && input.resultBytesHash !== digest) {
      invalidTransition("Result bytes do not match resultBytesHash");
    }
  } else if (own(input, "resultBytesHash")) {
    invalidTransition("A result hash without the original result is not a result record");
  }
};

const originalStatus = (record: EffectRecord): NonNullable<EffectContractError["original"]> => {
  const result = record.transitions.find(transition => transition.state === "RESULT");
  if (result) return { key: record.intent.key, state: "RESULT", ...(own(result, "result") ? { result: result.result! } : {}) };
  const disposition = [...record.transitions].reverse().find(transition => transition.state === "UNCERTAIN");
  const state = disposition?.state ?? record.transitions.at(-1)?.state ?? "INTENT";
  return { key: record.intent.key, state, ...(disposition && own(disposition, "result") ? { result: disposition.result! } : {}) };
};

/** Retained-volume immutable intents and hash-chained, immutable transitions.
 * No identity discovery, lease acquisition, index, cleanup or destination execution. */
export class EffectStore {
  readonly owner: RecoveryOwner;
  readonly #root: string;
  readonly #effects: string;
  readonly #orgId: string;
  readonly #logicalSessionId: string;
  readonly #sessionFile: string;
  #rootIdentity: fs.Stats | undefined;

  constructor(owner: RecoveryOwner) {
    if (!owner || !text(owner.orgId) || !text(owner.logicalSessionId) ||
        !text(owner.sessionRoot) || !path.isAbsolute(owner.sessionRoot) ||
        path.normalize(owner.sessionRoot) !== owner.sessionRoot || owner.sessionRoot.includes("\0") ||
        owner.sessionRoot.includes("\\") || path.parse(owner.sessionRoot).root === owner.sessionRoot ||
        typeof owner.assertLive !== "function" || !owner.signal) {
      throw new EffectContractError("EFFECT_INVALID_OWNER", "An exact owner and absolute, preprovisioned session root are required");
    }
    canonicalEncode([owner.orgId, owner.logicalSessionId, owner.sessionRoot]);
    this.owner = owner;
    this.#root = owner.sessionRoot;
    this.#effects = path.join(this.#root, "effects");
    this.#orgId = owner.orgId;
    this.#logicalSessionId = owner.logicalSessionId;
    this.#sessionFile = owner.sessionFile;
    // Never mkdir the supplied root. Missing retained state is not a fresh session.
    this.#rootIdentity = this.#io(undefined, () => this.#directory(this.#root));
  }

  async withSession<T>(
    permission: EffectPermission,
    operation: (transaction: EffectStoreTransaction) => Promise<T> | T,
  ): Promise<T> {
    if (permission !== "new-intent" && permission !== "settle") {
      throw new EffectContractError("EFFECT_PERMISSION_DENIED", "Unknown effect-store permission");
    }
    if (scopes.getStore()?.get(this.#root)?.active) {
      throw new EffectContractError("EFFECT_SESSION_REENTRANT", "Nested operations on the same session would deadlock");
    }
    const previous = sessionTails.get(this.#root);
    let release!: () => void;
    const tail = new Promise<void>(resolve => { release = resolve; });
    sessionTails.set(this.#root, tail);
    await previous;
    const scope: SessionScope = { active: true, usable: true };
    try {
      // Acquire authority AFTER entering the queue, not while waiting on its predecessor.
      await this.owner.assertLive(permission);
      // A bind can append an ALIAS, and all transition writes require settlement authority.
      if (permission === "new-intent") await this.owner.assertLive("settle");
      requireNamespaceBarrier(platform => new EffectContractError(UNCERTAIN,
        `Effect-store namespace durability is unavailable on ${platform}`));
      this.#checkOwner(permission === "new-intent");
      if (permission === "new-intent") this.#assertAdmissionOpen();
      this.#io(undefined, () => this.#directory(this.#root));
      const guard = (binding = false): void => {
        if (!scope.active || !scope.usable || scopes.getStore()?.get(this.#root) !== scope) {
          throw new EffectContractError("EFFECT_TRANSACTION_CLOSED", "Effect operations require their active withSession callback");
        }
        if (scope.failure) throw scope.failure;
        this.#checkOwner(binding);
        if (binding) {
          if (permission !== "new-intent") {
            throw new EffectContractError("EFFECT_PERMISSION_DENIED", "Binding requires new-intent permission");
          }
          this.#assertAdmissionOpen();
        }
      };
      const transaction: EffectStoreTransaction = Object.freeze({
        bind: (intent: EffectIntent) => { guard(true); return this.#bind(intent); },
        read: (key: string) => { guard(); return this.#read(key); },
        list: () => { guard(); return this.#list(); },
        append: (key: string, input: EffectTransitionInput) => { guard(); return this.#append(key, input); },
        readMaintenanceRepair: (originalInputId: string) => { guard(); return this.#readMaintenanceRepair(originalInputId); },
        listMaintenanceRepairs: () => { guard(); return this.#listMaintenanceRepairs(); },
        publishMaintenanceRepair: (candidate: MaintenanceRepairRecordBody) => { guard(); return this.#publishMaintenanceRepair(candidate); },
      });
      const context = new Map(scopes.getStore());
      context.set(this.#root, scope);
      return await scopes.run(context, async () => {
        let value: T;
        try { value = await operation(transaction); }
        finally { scope.usable = false; } // No escaped writes during post-callback qualification either.
        const failure = admissionFailures.get(this.#root);
        if (permission === "settle" && failure && !scope.failure) {
          // A callback merely returning is not a settlement receipt. Requalify
          // authority and the ENTIRE ledger, including fresh namespace barriers.
          await this.owner.assertLive("settle");
          this.#checkOwner(false);
          const records = this.#list();
          if (records.every(record => record.transitions.some(transition => transition.state === "RESULT")) &&
              admissionFailures.get(this.#root) === failure) {
            admissionFailures.delete(this.#root);
          }
        }
        return value;
      });
    } finally {
      scope.active = false;
      release();
      if (sessionTails.get(this.#root) === tail) sessionTails.delete(this.#root);
    }
  }

  #checkOwner(newIntent: boolean): void {
    if (this.owner.orgId !== this.#orgId || this.owner.logicalSessionId !== this.#logicalSessionId ||
        this.owner.sessionRoot !== this.#root || (newIntent && this.owner.signal.aborted)) {
      throw new EffectContractError("EFFECT_OWNER_STALE", "Effect-store owner identity changed or new-intent authority was aborted");
    }
    // Only B3 decides whether an aborted owner still has settlement permission.
  }

  #assertAdmissionOpen(): void {
    const failure = admissionFailures.get(this.#root);
    if (failure) {
      throw new EffectContractError("EFFECT_ADMISSION_CLOSED",
        `New effects are closed until qualified settlement: ${failure.message}`, failure.original);
    }
  }

  #io<T>(key: string | undefined, operation: () => T): T {
    try { return operation(); }
    catch (error) {
      const failure = error instanceof EffectContractError && error.code === UNCERTAIN ? error : uncertainty(key, error);
      admissionFailures.set(this.#root, failure);
      const scope = scopes.getStore()?.get(this.#root);
      if (scope?.active) scope.failure ??= failure;
      // Even a caught failure cannot permit more operations in this pass. Keep
      // intent/temp evidence intact; only a later qualified settlement can reopen.
      throw failure;
    }
  }

  #keyDirectory(key: string): string {
    if (!validKey(key)) {
      throw new EffectContractError("EFFECT_UNSAFE_PATH", "An effect key must be exactly 64 lowercase hexadecimal characters");
    }
    return path.join(this.#effects, key);
  }

  /** Inspect every ancestor, not realpath's collapsed endpoint. No symlinks, including in roots. */
  #directory(directory: string): fs.Stats {
    let current = path.parse(directory).root;
    let stat = fs.lstatSync(current);
    const components = directory.slice(current.length).split(path.sep).filter(Boolean);
    for (let index = 0; ; index++) {
      if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`Unsafe effect directory: ${current}`);
      if (current === this.#root && this.#rootIdentity && !sameInode(stat, this.#rootIdentity)) {
        throw new Error("Retained session root was replaced");
      }
      if ((current === this.#effects || current.startsWith(`${this.#effects}${path.sep}`)) && (stat.mode & 0o7777) !== 0o700) {
        throw new Error(`Effect directory must have mode 0700: ${current}`);
      }
      if (index === components.length) return stat;
      current = path.join(current, components[index]!);
      stat = fs.lstatSync(current);
    }
  }

  #mkdir(directory: string): boolean {
    this.#directory(path.dirname(directory));
    try { fs.mkdirSync(directory, { mode: 0o700 }); }
    catch (error) {
      if (codeOf(error) !== "EEXIST") throw error;
      this.#directory(directory);
      return false;
    }
    // Set exact modes even with a restrictive provisioning umask, only on our new directory.
    fs.chmodSync(directory, 0o700);
    this.#directory(directory);
    syncDirectoryChain(directory);
    return true;
  }

  #regular(file: string): fs.Stats {
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink() || !stat.isFile() || (stat.mode & 0o7777) !== 0o600) {
      throw new Error(`Effect record must be a regular mode-0600 file: ${file}`);
    }
    return stat;
  }

  #canonicalFile<T>(file: string): { value: T; bytes: string } {
    const parent = this.#directory(path.dirname(file));
    const before = this.#regular(file);
    const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || !sameInode(before, opened)) throw new Error("Effect record changed during open");
      const raw = fs.readFileSync(fd);
      const value: unknown = JSON.parse(raw.toString("utf8"));
      const bytes = canonicalEncode(value);
      if (!raw.equals(Buffer.from(bytes, "utf8"))) throw new Error("Effect record is not canonical UTF-8 JSON");
      const after = fs.fstatSync(fd);
      if (!sameInode(opened, after) || opened.size !== after.size || opened.mtimeMs !== after.mtimeMs ||
          opened.ctimeMs !== after.ctimeMs || !sameInode(after, this.#regular(file)) ||
          !sameInode(parent, this.#directory(path.dirname(file)))) {
        throw new Error("Effect record changed during read");
      }
      return { value: value as T, bytes };
    } finally { fs.closeSync(fd); }
  }

  #evidence(directory: string, filename: string, expected: "intent.json" | "transition" | "receipt.json"): void {
    const evidence = this.#regular(path.join(directory, filename));
    const witness = PUBLICATION_WITNESS.exec(filename);
    if (!witness) return; // A failed/unpublished stage is never authority for an effect.
    const target = witness[1]!;
    if ((expected === "transition" ? !TRANSITION_FILE.test(target) : target !== expected) ||
        !sameInode(evidence, this.#regular(path.join(directory, target)))) {
      throw new Error("A retained publication witness no longer matches its committed record");
    }
  }

  /** Retain ALL staging files, including successful hard-link publications. A second,
   * immutable publication witness makes deletion of an acknowledged tail detectable
   * without a mutable frontier. Unpublished stages never authorize an effect.
   * No overwrite, unlink, exit cleanup hook or in-place fallback. */
  #publish(file: string, bytes: string): boolean {
    const directory = path.dirname(file);
    const parent = this.#directory(directory);
    const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
    const fd = fs.openSync(temporary,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try {
      fs.fchmodSync(fd, 0o600);
      fs.writeFileSync(fd, bytes, { encoding: "utf8" });
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    if (!sameInode(parent, this.#directory(directory))) throw new Error("Effect namespace changed during staging");
    syncDirectoryChain(directory);
    try { fs.linkSync(temporary, file); }
    catch (error) {
      if (codeOf(error) === "EEXIST") return false;
      throw error;
    }
    // Create the witness only AFTER no-clobber publication. If it exists but its
    // canonical name is missing on replay, that is corruption, not an unstarted tail.
    fs.linkSync(temporary, path.join(directory, `.published-${path.basename(file)}-${randomUUID()}.tmp`));
    syncDirectoryChain(directory);
    if (!sameInode(parent, this.#directory(directory))) throw new Error("Effect namespace changed during publication");
    return true;
  }

  #verifyIntent(intent: EffectIntent, key: string): void {
    validateIntent(intent);
    if (intent.key !== key || mintEffectKey(intent.keyTuple) !== key ||
        intent.keyTuple[1] !== this.#orgId || intent.keyTuple[2] !== this.#logicalSessionId ||
        intent.keyTuple[3] !== intent.acceptedInput.id || intent.keyTuple[4] !== intent.slot.id ||
        intent.keyTuple[5] !== intent.slot.kind) {
      throw new EffectContractError("EFFECT_INTENT_IDENTITY", "Intent key, tuple, input, slot and owner must match exactly");
    }
    const payload = Buffer.from(intent.payload.bytes, "base64");
    if (intent.payload.encoding !== "base64" || payload.toString("base64") !== intent.payload.bytes ||
        sha256Bytes(payload) !== intent.payload.digest) {
      throw new EffectContractError("EFFECT_INTENT_PAYLOAD", "Intent payload bytes do not match their digest");
    }
  }

  #read(key: string): EffectRecord {
    const directory = this.#keyDirectory(key);
    return this.#io(key, () => {
      const identity = this.#directory(directory);
      const entries = fs.readdirSync(directory);
      for (const entry of entries) {
        if (entry === "intent.json" || entry === "transitions") continue;
        if (!entry.endsWith(".tmp")) throw new Error(`Unexpected committed effect filename: ${entry}`);
        this.#evidence(directory, entry, "intent.json");
      }
      const { value: intent, bytes } = this.#canonicalFile<EffectIntent>(path.join(directory, "intent.json"));
      this.#verifyIntent(intent, key);
      const intentDigest = sha256Bytes(bytes);
      const transitionDirectory = path.join(directory, "transitions");
      const transitionIdentity = this.#directory(transitionDirectory);
      const filenames = fs.readdirSync(transitionDirectory).sort();
      const transitions: EffectTransition[] = [];
      let previousDigest = intentDigest;
      let terminal = false;
      for (const filename of filenames) {
        const file = path.join(transitionDirectory, filename);
        if (filename.endsWith(".tmp")) { this.#evidence(transitionDirectory, filename, "transition"); continue; }
        const sequence = transitions.length + 1;
        if (!TRANSITION_FILE.test(filename) || filename !== sequenceFile(sequence)) {
          throw new Error(`Broken sequence or unexpected committed transition filename: ${filename}`);
        }
        const { value: transition } = this.#canonicalFile<EffectTransition>(file);
        if (!object(transition) || Object.keys(transition).some(field => !TRANSITION_FIELDS.has(field)) ||
            transition.format !== "fabric.effect-transition" || transition.version !== 1 ||
            transition.key !== key || transition.sequence !== sequence || transition.previousDigest !== previousDigest ||
            !validKey(transition.digest)) {
          throw new Error("Invalid transition envelope or broken digest chain");
        }
        checkTransitionInput(transition, true);
        const { digest, ...body } = transition;
        if (sha256Bytes(canonicalEncode(body)) !== digest) throw new Error("Transition digest does not match its bytes");
        if (terminal && transition.state !== "ALIAS") throw new Error("A durable RESULT was changed by a later transition");
        terminal ||= transition.state === "RESULT";
        transitions.push(transition);
        previousDigest = digest;
      }
      // A previous publisher may have lost its acknowledgement at the namespace
      // barrier. Reconfirm names before exposing even an already-present RESULT.
      syncDirectoryChain(transitionDirectory);
      if (!sameInode(identity, this.#directory(directory)) ||
          !sameInode(transitionIdentity, this.#directory(transitionDirectory)) ||
          canonicalEncode(entries.sort()) !== canonicalEncode(fs.readdirSync(directory).sort()) ||
          canonicalEncode(filenames) !== canonicalEncode(fs.readdirSync(transitionDirectory).sort())) {
        throw new Error("Effect namespace changed during chain verification");
      }
      return freeze({ intent, intentDigest, transitions });
    });
  }

  /** A crash BEFORE intent publication leaves only staging evidence. Such bytes
   * never authorize a sink, and are not a missing acknowledged record: every
   * acknowledged publication has both the canonical name and a witness. */
  #unpublished(directory: string): boolean {
    this.#directory(directory);
    const entries = fs.readdirSync(directory);
    if (entries.includes("intent.json") || entries.some(name => name.startsWith(".published-"))) return false;
    for (const entry of entries) {
      if (entry === "transitions") {
        const transitions = path.join(directory, entry);
        this.#directory(transitions);
        for (const name of fs.readdirSync(transitions)) {
          if (!name.endsWith(".tmp") || name.startsWith(".published-")) return false;
          this.#regular(path.join(transitions, name));
        }
      } else {
        if (!entry.endsWith(".tmp")) return false;
        this.#regular(path.join(directory, entry));
      }
    }
    return true;
  }

  #list(): readonly EffectRecord[] {
    return this.#io(undefined, () => {
      this.#directory(this.#root);
      try { fs.lstatSync(this.#effects); }
      catch (error) {
        // A not-yet-created effects namespace is empty. Within it, only
        // provably unpublished staging is ignored; acknowledged gaps are errors.
        if (codeOf(error) === "ENOENT") {
          syncDirectoryChain(this.#root);
          return Object.freeze([]);
        }
        throw error;
      }
      const identity = this.#directory(this.#effects);
      const entries = fs.readdirSync(this.#effects).sort();
      const records: EffectRecord[] = [];
      for (const key of entries) {
        if (key === REPAIRS_DIRECTORY) { this.#listMaintenanceRepairs(); continue; }
        if (!validKey(key)) throw new Error(`Unexpected effect namespace entry: ${key}`);
        if (!this.#unpublished(this.#keyDirectory(key))) records.push(this.#read(key));
      }
      syncDirectoryChain(this.#effects);
      if (!sameInode(identity, this.#directory(this.#effects)) ||
          canonicalEncode(entries) !== canonicalEncode(fs.readdirSync(this.#effects).sort())) {
        throw new Error("Effect namespace changed during enumeration");
      }
      return Object.freeze(records);
    });
  }

  #repairOwner(): Pick<RecoveryOwner, "orgId" | "logicalSessionId" | "sessionFile"> {
    if (this.owner.sessionFile !== this.#sessionFile) {
      throw new EffectContractError("EFFECT_OWNER_STALE", "Repair native file binding changed");
    }
    return { orgId: this.#orgId, logicalSessionId: this.#logicalSessionId, sessionFile: this.#sessionFile };
  }

  /** A saved frontier identifies authenticated immutable prefixes, not the current
   * global revision. New-publication exact key/tail checks also belong to the
   * engine's SAME transaction; later aliases/keys do not erase historical receipts. */
  #verifyRepairFrontier(candidate: MaintenanceRepairRecordBody): void {
    for (const entry of candidate.frontier) {
      const record = this.#read(entry.key);
      const digest = entry.sequence === 0 ? record.intentDigest : record.transitions[entry.sequence - 1]?.digest;
      if (record.intent.acceptedInput.id !== candidate.view.originalInputId ||
          record.intent.acceptedInput.digest !== candidate.originalPayloadSha256 || digest !== entry.digest) {
        throw new EffectContractError("EFFECT_REPAIR_CONFLICT", "Repair frontier does not match its original's retained immutable prefix");
      }
    }
  }

  /** No recursion through #list: effect enumeration validates this reserved
   * namespace too, including when deciding whether a storage latch may clear. */
  #listMaintenanceRepairs(): readonly StoredMaintenanceRepair[] {
    const owner = this.#repairOwner();
    return this.#io(undefined, () => {
      this.#directory(this.#root);
      try { fs.lstatSync(this.#effects); }
      catch (error) {
        if (codeOf(error) !== "ENOENT") throw error;
        syncDirectoryChain(this.#root);
        return Object.freeze([]);
      }
      const effectsIdentity = this.#directory(this.#effects);
      const effectNames = fs.readdirSync(this.#effects).sort();
      const records: StoredMaintenanceRepair[] = [];
      if (effectNames.includes(REPAIRS_DIRECTORY)) {
        const namespace = path.join(this.#effects, REPAIRS_DIRECTORY);
        const identity = this.#directory(namespace);
        const keys = fs.readdirSync(namespace).sort();
        for (const key of keys) {
          if (!validKey(key)) throw new Error(`Unexpected repair namespace entry: ${key}`);
          const directory = path.join(namespace, key);
          const receiptIdentity = this.#directory(directory);
          const names = fs.readdirSync(directory).sort();
          for (const name of names) {
            if (name === "receipt.json") continue;
            if (!name.endsWith(".tmp") || (name.startsWith(".published-") && !PUBLICATION_WITNESS.test(name))) {
              throw new Error(`Unexpected committed repair filename: ${name}`);
            }
            this.#evidence(directory, name, "receipt.json");
          }
          // Ordinary unpublished stages confer no authority. A retained witness
          // with no exact canonical target already failed in #evidence above.
          if (names.includes("receipt.json")) {
            const file = path.join(directory, "receipt.json");
            const committed = this.#regular(file);
            const { value: record } = this.#canonicalFile<StoredMaintenanceRepair>(file);
            validateStoredMaintenanceRepair(record, owner);
            if (maintenanceRepairKey(this.#orgId, this.#logicalSessionId, record.view.originalInputId) !== key) {
              throw new Error("Repair original does not match its retained directory key");
            }
            this.#verifyRepairFrontier(record);
            if (!names.some(name => PUBLICATION_WITNESS.exec(name)?.[1] === "receipt.json")) {
              // Recover the precise cut after canonical no-clobber publication but
              // before its witness. Never infer authority from plain stages, and
              // never acknowledge a canonical receipt whose later deletion would
              // look unpublished. This adds NO new stage on replay.
              if (!sameInode(committed, this.#regular(file))) throw new Error("Repair changed before witness recovery");
              const witness = `.published-receipt.json-${randomUUID()}.tmp`;
              fs.linkSync(file, path.join(directory, witness));
              this.#evidence(directory, witness, "receipt.json");
              names.push(witness);
              names.sort();
            }
            records.push(freeze(record));
          }
          syncDirectoryChain(directory);
          if (!sameInode(receiptIdentity, this.#directory(directory)) ||
              canonicalEncode(names) !== canonicalEncode(fs.readdirSync(directory).sort())) {
            throw new Error("Repair namespace changed during receipt verification");
          }
        }
        syncDirectoryChain(namespace);
        if (!sameInode(identity, this.#directory(namespace)) ||
            canonicalEncode(keys) !== canonicalEncode(fs.readdirSync(namespace).sort())) {
          throw new Error("Repair namespace changed during enumeration");
        }
      }
      syncDirectoryChain(this.#effects);
      if (!sameInode(effectsIdentity, this.#directory(this.#effects)) ||
          canonicalEncode(effectNames) !== canonicalEncode(fs.readdirSync(this.#effects).sort())) {
        throw new Error("Effect namespace changed during repair enumeration");
      }
      return Object.freeze(records);
    });
  }

  #readMaintenanceRepair(originalInputId: string): StoredMaintenanceRepair | undefined {
    // Validate the original before deriving a path; absence never mkdirs a namespace.
    maintenanceRepairKey(this.#orgId, this.#logicalSessionId, originalInputId);
    return this.#listMaintenanceRepairs().find(record => record.view.originalInputId === originalInputId);
  }

  #publishMaintenanceRepair(input: MaintenanceRepairRecordBody): StoredMaintenanceRepair {
    // Snapshot first; invalid caller data/conflicting candidates are NOT storage
    // failures and cannot introduce the store's global admission latch.
    const candidate: unknown = JSON.parse(canonicalEncode(input));
    validateMaintenanceRepairBody(candidate, this.#repairOwner());
    const record: StoredMaintenanceRepair = {
      ...candidate, recordSha256: sha256Bytes(canonicalEncode(candidate)),
    };
    const bytes = canonicalEncode(record);
    const existing = this.#readMaintenanceRepair(candidate.view.originalInputId);
    if (existing) {
      if (canonicalEncode(existing) !== bytes) {
        throw new EffectContractError("EFFECT_REPAIR_CONFLICT", "An original already has a different immutable repair receipt");
      }
      // The exact existing canonical file was reread and namespace-barriered above.
      // Do NOT publish another retained stage on an ordinary exact retry.
      return existing;
    }
    this.#verifyRepairFrontier(candidate);
    const key = maintenanceRepairKey(this.#orgId, this.#logicalSessionId, candidate.view.originalInputId);
    this.#io(undefined, () => {
      this.#mkdir(this.#effects);
      const namespace = path.join(this.#effects, REPAIRS_DIRECTORY);
      this.#mkdir(namespace);
      const directory = path.join(namespace, key);
      this.#mkdir(directory);
      this.#publish(path.join(directory, "receipt.json"), bytes);
    });
    const published = this.#readMaintenanceRepair(candidate.view.originalInputId);
    if (!published) return this.#io(undefined, () => { throw new Error("Published repair receipt is missing"); });
    if (canonicalEncode(published) !== bytes) {
      throw new EffectContractError("EFFECT_REPAIR_CONFLICT", "Repair receipt publication lost to a different immutable candidate");
    }
    return published;
  }

  #bind(intent: EffectIntent): EffectRecord {
    // Snapshot through canonical JSON; a caller cannot mutate a bound request or a returned record.
    const bytes = canonicalEncode(intent);
    const candidate = JSON.parse(bytes) as EffectIntent;
    const directory = this.#keyDirectory(candidate.key);
    this.#verifyIntent(candidate, candidate.key);
    const existing = (): EffectRecord => {
      const record = this.#read(candidate.key);
      if (intentBindingDigest(record.intent) !== intentBindingDigest(candidate)) {
        throw new EffectContractError("EFFECT_SLOT_CONFLICT", "Effect slot is already bound to a different immutable request", originalStatus(record));
      }
      const sameAlias = (call: { toolCallId: string; parentToolCallId?: string }): boolean =>
        call.toolCallId === candidate.tool.originalToolCallId && call.parentToolCallId === candidate.tool.parentToolCallId;
      if (sameAlias({ toolCallId: record.intent.tool.originalToolCallId,
        ...(record.intent.tool.parentToolCallId === undefined ? {} : { parentToolCallId: record.intent.tool.parentToolCallId }) }) ||
          record.transitions.some(sameAlias)) return record;
      return this.#append(candidate.key, {
        state: "ALIAS", toolCallId: candidate.tool.originalToolCallId,
        ...(candidate.tool.parentToolCallId === undefined ? {} : { parentToolCallId: candidate.tool.parentToolCallId }),
        attempt: candidate.originAttempt, at: new Date().toISOString(),
      });
    };
    const published = this.#io(candidate.key, () => {
      this.#mkdir(this.#effects);
      // Only a provably unpublished stage may be completed using a NEW validated
      // binding. Never infer authority from temp contents, or replace an intent.
      if (!this.#mkdir(directory) && !this.#unpublished(directory)) return false;
      this.#mkdir(path.join(directory, "transitions"));
      return this.#publish(path.join(directory, "intent.json"), bytes);
    });
    return published ? this.#read(candidate.key) : existing();
  }

  #append(key: string, input: EffectTransitionInput): EffectRecord {
    const directory = this.#keyDirectory(key);
    const candidate = JSON.parse(canonicalEncode(input)) as EffectTransitionInput;
    if (!object(candidate) || Object.keys(candidate).some(field => !INPUT_FIELDS.has(field))) {
      invalidTransition("Unexpected transition input field");
    }
    checkTransitionInput(candidate, false);
    const record = this.#read(key);
    if (record.transitions.some(transition => transition.state === "RESULT") && candidate.state !== "ALIAS") {
      throw new EffectContractError("EFFECT_RESULT_IMMUTABLE", "Only physical aliases may follow a durable RESULT", originalStatus(record));
    }
    const sequence = record.transitions.length + 1;
    if (sequence > MAX_SEQUENCE) throw uncertainty(key, "Transition sequence exhausted");
    const body = {
      ...candidate,
      ...(own(candidate, "result") ? { resultBytesHash: sha256Bytes(canonicalEncode(candidate.result)) } : {}),
      format: "fabric.effect-transition" as const, version: 1 as const, key, sequence,
      previousDigest: record.transitions.at(-1)?.digest ?? record.intentDigest,
    };
    const transition: EffectTransition = { ...body, digest: sha256Bytes(canonicalEncode(body)) };
    this.#io(key, () => {
      if (!this.#publish(path.join(directory, "transitions", sequenceFile(sequence)), canonicalEncode(transition))) {
        throw new Error("Transition publication collided with another writer; staging evidence retained");
      }
    });
    return this.#read(key);
  }
}
