# Sandbox transport intent

Sandbox Placement receives a compact JSON intent, not the task prompt. Ordinary callers continue to omit optional fields; the client does not add defaults, preserving the legacy serialized intent and run-ID replay identity.

`agents.spawn` may explicitly supply `branch` and `sessionImport`; neither accepts `null`. Omit unused fields.

- `branch`: 1–255 ASCII characters from `[A-Za-z0-9._/-]`, starting with an alphanumeric, without the `refs/heads/` prefix. Git branch restrictions apply: no `//`, `..`, `/.`, component ending in `.lock`, or trailing `/` or `.`. Branch-only metadata is allowed.
- `sessionImport`: exactly `{ session, manifestSha256 }`. `session` is the lowercase 36-character UUID from the Pi session header. `manifestSha256` is exactly 64 lowercase hexadecimal characters, the digest of the capsule's `manifest.json` bytes. It is a digest, not a path or URL.

An import requires sandbox transport, a branch, and a `lead` or `lane` profile. When an import is explicitly requested without a profile, it defaults to `lane`; ordinary spawns keep their existing `task` default. The client validates the values before making the Placement request and counts their serialized bytes against the 4096-byte intent limit. Explicit optional fields are sent unchanged; omitted fields remain absent.

An old broker's HTTP 400 `INTENT_INVALID` response to a request carrying either field becomes a typed `SandboxSessionImportUnsupportedError` with code `PLACEMENT_SESSION_IMPORT_UNSUPPORTED`. The client never retries that refusal or strips fields/profile to downgrade a move into a fresh session, even if an earlier request cached a legacy profile fallback. Requests without either field retain existing error and profile-fallback handling.
