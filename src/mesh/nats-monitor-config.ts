// Pure selection of a nats-server's JetStream `sync_always` policy from its monitor bodies (#13112). No imports:
// shared by the product adapter (state-backend-nats-client.ts) and the CI readiness gate
// (scripts/nats-cluster-readiness.mjs), which Node >= 24 loads as TypeScript directly; keep it erasable syntax only.
//
// nats-server 2.14.7 can return a valid `/varz` whose `jetstream` omits `config` while the same node's
// `/jsz?raft=true` reports top-level `config.sync_always`. Contract:
//   - a boolean `/varz` `jetstream.config.sync_always` is authoritative (an explicit `false` is never overridden);
//   - otherwise (absent, null, or not a boolean) a boolean top-level `/jsz` `config.sync_always` is used;
//   - otherwise `undefined` (the caller refuses; it never assumes durability).
// Only these exact snake_case paths are read.

const field = (value: unknown, key: string): unknown =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>)[key] : undefined;

export const selectNatsSyncAlways = (varz: unknown, jsz?: unknown): boolean | undefined => {
  const fromVarz = field(field(field(varz, "jetstream"), "config"), "sync_always");
  if (typeof fromVarz === "boolean") return fromVarz;
  const fromJsz = field(field(jsz, "config"), "sync_always");
  return typeof fromJsz === "boolean" ? fromJsz : undefined;
};
