// Pure selection of a nats-server's JetStream `sync_always` policy from its monitor bodies (#13112). No imports:
// shared by the product adapter (state-backend-nats-client.ts) and the CI readiness gate
// (scripts/nats-cluster-readiness.mjs), which Node >= 24 loads as TypeScript directly; keep it erasable syntax only.
//
// nats-server 2.14.7 can return a valid `/varz` whose `jetstream` omits `config` while the same node's
// `/jsz?raft=true` reports top-level `config.sync_always`. Contract (callers always read both sources):
//   - an explicit `false` in EITHER `/varz` `jetstream.config.sync_always` or top-level `/jsz` `config.sync_always`
//     vetoes: the result is `false`, and neither source overrides the other;
//   - otherwise `true` iff at least one source is boolean `true`;
//   - otherwise `undefined` (the caller refuses; it never assumes durability).
// Only these exact snake_case paths are read; absent, null, or non-boolean values are not trusted (ignored).

const field = (value: unknown, key: string): unknown =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>)[key] : undefined;

export const selectNatsSyncAlways = (varz: unknown, jsz?: unknown): boolean | undefined => {
  const fromVarz = field(field(field(varz, "jetstream"), "config"), "sync_always");
  const fromJsz = field(field(jsz, "config"), "sync_always");
  if (fromVarz === false || fromJsz === false) return false;
  return fromVarz === true || fromJsz === true ? true : undefined;
};
