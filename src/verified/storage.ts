import {
  storageTransition,
  type StoragePlan,
  type StorageRequest,
  type StorageRevision,
  type StorageText,
} from "./generated/storage-kernel.js";
import type { MeshIdentity } from "../mesh/store.js";

// Bend Nat arithmetic is capped at 2^48-1. Canonical radix-2^32 limbs
// represent the entire JS safe-integer range without crossing that runtime cap.
const RADIX = 4294967296n;

const encode = (text: string): StorageText => {
  let result: StorageText = { $: "Nil" };
  for (let index = text.length - 1; index >= 0; index--) {
    result = { $: "Con", head: BigInt(text.charCodeAt(index)), tail: result };
  }
  return result;
};

const decode = (text: StorageText): string => {
  const units: string[] = [];
  for (let node = text; node.$ === "Con"; node = node.tail) {
    if (node.head < 0n || node.head > 65535n) throw new Error("Invalid storage code unit");
    units.push(String.fromCharCode(Number(node.head)));
  }
  return units.join("");
};

export const storageRevision = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("Invalid Fabric mesh revision: expected a nonnegative safe integer");
  }
  return value;
};

const encodeRevision = (value: number): StorageRevision => {
  const exact = BigInt(storageRevision(value));
  return { $: "Revision", high: exact / RADIX, low: exact % RADIX };
};

const decodeRevision = (value: StorageRevision): number => {
  if (value.high < 0n || value.high > 2097151n || value.low < 0n || value.low >= RADIX) {
    throw new Error("Invalid verified storage revision limbs");
  }
  // Representation conversion only; CAS, overflow and successor/carry decisions
  // are performed by the executable proved reducer, not reproduced here.
  return storageRevision(Number(value.high * RADIX + value.low));
};

const serialize = (value: unknown): string => {
  const json = JSON.stringify(value);
  if (json === undefined) throw new Error("Mesh values must be JSON-serializable");
  return json;
};

export type StorageTransition =
  | { kind: "put"; key: string; value: unknown; identity: MeshIdentity; version: number; highWater: number }
  | { kind: "delete"; key: string; version: number; highWater: number }
  | { kind: "unchanged" };

export interface CapturedStorageRequest {
  readonly key: string;
  readonly transition: (present: boolean, actualVersion: number, highWater: number) => StorageTransition;
}

const capture = (
  key: string,
  ifVersion: number | undefined,
  payload?: { value: string; identity: string },
): CapturedStorageRequest => {
  const expected = ifVersion === undefined ? undefined : storageRevision(ifVersion);
  // The wire graph and JSON strings never escape this closure. Caller-owned
  // objects (including nested identity/value) are not retained across lock wait.
  const request: StorageRequest = {
    $: "Request",
    key: encode(key),
    expected: expected === undefined ? { $: "Any" } : { $: "At", version: encodeRevision(expected) },
    change: payload === undefined ? { $: "Delete" } : {
      $: "Put", value: encode(payload.value), identity: encode(payload.identity),
    },
  };
  return Object.freeze({
    key,
    transition(present: boolean, actualVersion: number, highWater: number): StorageTransition {
      const actual = storageRevision(actualVersion);
      const clock = storageRevision(highWater);
      if (clock < actual) throw new Error("Inconsistent Fabric mesh high-water revision");
      const plan: StoragePlan = storageTransition(
        request, { $: "Slot", present, version: encodeRevision(actual), highWater: encodeRevision(clock) },
      );
      switch (plan.$) {
        case "Conflict":
          throw new Error(`Mesh compare-and-swap failed for ${key}: expected version ${expected}, found ${actual}`);
        case "Exhausted":
          throw new Error(`Fabric mesh revision exhausted for ${key}`);
        case "Unchanged":
          return { kind: "unchanged" };
        case "PutNext":
          return {
            kind: "put", key: decode(plan.key), value: JSON.parse(decode(plan.value)),
            identity: JSON.parse(decode(plan.identity)) as MeshIdentity,
            version: decodeRevision(plan.version), highWater: decodeRevision(plan.highWater),
          };
        case "DeleteNext":
          return { kind: "delete", key: decode(plan.key), version: decodeRevision(plan.version), highWater: decodeRevision(plan.highWater) };
      }
    },
  });
};

export const captureStoragePut = (input: {
  key: string; ifVersion?: number | undefined; value: unknown; identity: MeshIdentity;
}, maxValueBytes = Number.POSITIVE_INFINITY): CapturedStorageRequest => {
  const { key, ifVersion, value, identity } = input;
  // Capture scalars before serialization can invoke user-defined toJSON hooks.
  if (ifVersion !== undefined) storageRevision(ifVersion);
  const identityJson = serialize(identity);
  const valueJson = serialize(value);
  if (Buffer.byteLength(valueJson, "utf8") > maxValueBytes) {
    throw new Error(`Mesh state value exceeds ${maxValueBytes} bytes`);
  }
  return capture(key, ifVersion, { value: valueJson, identity: identityJson });
};

export const captureStorageDelete = (input: {
  key: string; ifVersion?: number | undefined;
}): CapturedStorageRequest => {
  const { key, ifVersion } = input;
  return capture(key, ifVersion);
};
