DESIGN (design-pair, challenge record: docs/designs/nats-auth-7504-challenge-r1.md; round 2 skipped for time)

# NATS authentication: one login per issued launch and principal, one account per org

smarty-dev#7504 / #8031 R1 (goals G1, G2). Owner: fabric-store. Status: DESIGN DRAFT, revision 1 with Amendment A (10-13, section 9); implementation and named security approval remain pending. Section 9 is docs only, not implemented or measured.
Code read: main `fs-main-44da` (this tree) and `feat/sd-9690-reader-proof` @6fadd911 (`../fs-nats2`, cited as `b:`).
Measured: the challenger's local nats-server 2.14.7 ACL probe (static password, not JWT; challenge record, "ACL call-path").

## Round 1 answers

- **C1 (blocker) ADOPT.** No agent user holds `$JS.API.STREAM.PURGE`. Phase A is gone: the compaction move (agents
  publish snapshots, a maintainer job purges) is gate G-2, before the first fleet root switches. The #907 rule
  becomes an authenticated role matrix; T2 on the deployed cluster is the deploy check. Sections 1.4, 4, 6.
- **C2 (blocker) ADOPT-PARTLY.** Section 2.2 lists every launch path. Only an issuer (Ryzen 1, or the self-hosted
  machine) mints. Each launch from the issuer gets its own user; a child on the same host inherits its parent's
  user. No mint daemon and no host secret, so paths without an issuer cannot select NATS in R1 (tested, T11).
- **C3 (blocker) ADOPT.** Section 3 separates FLEET custody (1Password, minted on Ryzen 1) from SELF-HOSTED custody
  (local operator in `~/.config/fabric/nats`, the user's own trust). Same code path, different custody; a
  deliberate, documented difference with its own threat boundary. `init --local` refuses on a fleet host.
- **C4 (major) ADOPT-PARTLY.** The agent role binds to exact streams with a `mesh:<hex>` user tag and
  `{{tag(mesh)}}` templates. Principal and maintainer stay org-wide on purpose. Template behavior is not yet
  measured; if T2 fails, R1 does not fall back silently (D2). Sections 1.1, 1.4.
- **C5 (major) ADOPT.** The branch's `create: false` open contract is gate G-1. Only the maintainer creates,
  updates or deletes streams; a new root gets its stream from a maintainer provision step. T12. Sections 1.3, 4.
- **C6 (major) ADOPT.** Fabric reads `exp` from the JWT it holds and closes its NATS roots at `exp − 1 h` with a
  "relaunch" error. Children and the resident host share that cutoff. T5 now tests a live connection past `exp`
  and a reconnect after it. Renewal stays phase C (D6). Sections 1.4, 2.2, 6.
- **C7 (major) ADOPT.** `revoke` says `complete` only when every configured server confirms the new account JWT;
  else `partial`, the stale nodes, exit 2. The stale window is defined per case; T4 adds a stopped node and a
  route-partitioned, client-reachable node. Section 2.4.
- **C8 (major) ADOPT.** Fabric reads and closes FD 3 before any tool or child runs; the wrapper sets `ulimit -c 0`;
  the same-UID boundary is written down. T10 checks FDs and core policy without reading credential bytes. 2.3.
- **C9 (major) ADOPT-PARTLY.** All prior-art and license claims now say `unverified:`; pin and inspect is gate G-7.
  A host-level user is not equal to per-launch revocation (Section 8). The encoder choice stays open (D4).

## 1. Decision and identity model

We use NATS decentralized auth. One operator signs one account per org. Each issued launch and each principal device
gets its own user (an nkey pair and a user JWT). All servers use the built-in **full** resolver. Revocations go out
as a new account JWT on `$SYS.REQ.CLAIMS.UPDATE`. On the fleet, Ryzen 1 is the only issuer; it sends creds on the
ssh stdin or a local pipe, and the process keeps them in memory: **no new secret file on a lane host.**
Self-hosting runs the same code; only key custody differs (Section 3).

| entity | name | private key lives (FLEET) | signs |
|---|---|---|---|
| operator | `smarty-fleet` | identity key: 1Password only (offline use) | operator JWT (in each server config) |
| operator signing key | - | 1Password; used on Ryzen 1 | account JWTs (create, limits, revocations) |
| system account | `SYS` | 1Password; its user runs on Ryzen 1 only | - |
| org account | `smarty-pants` (first), `playful` (later) | identity key: 1Password (recovery) | - |
| scoped signing keys | per account: `agent`, `principal`, `maintainer` | 1Password; in memory on Ryzen 1 only | user JWTs of that role only |
| user | one per issued launch, principal device, or maintainer job | process memory (principal: 1Password) | - |

### 1.1 Identities

- **Agent launch.** User JWT `name` = `agent/<host>/<lane>/<session-id>`. Tags: `host:`, `lane:`, `session:`,
  `release:`, `role:agent`, one `mesh:<32 hex>` per mesh root that the launch may open (the hex of
  `natsStreamName`, `b:src/mesh/state-backend-nats.ts:1057-1063`), and `parent:<session-id>` for a remote task.
  The user public key (`U…`) is the identity in server logs and `connz?auth=1`.
- **Principal:** `principal/<person>/<device>`. **Maintainer:** `maintainer/<org>/<job>/<run-id>`, Ryzen 1 only.
- **Per-org isolation** is the account boundary: no exports or imports; account JetStream limits (disk, streams,
  consumers), so one org cannot fill a node.
- **What it does NOT give.** All writers of one root share one stream, and any writer can forge a self-consistent
  snapshot (docs/mesh-backends.md, "Snapshot integrity"). A child shares its parent's user (2.2), so NATS logs name
  the parent launch, not the child.

### 1.2 Finding: the documented ACL does not match anything

docs/mesh-backends.md:77-79 allows `FABRIC_MESH_*.state`. In NATS, `*` "can not match a substring within a token"
(nats.docs `nats-concepts/subjects.md`): these are literal subjects. The probe shows `*.state` allows a publish to
any root. So a role limits **verbs**; only a per-user exact name limits **streams** (1.4). G-4 fixes that doc.

### 1.3 What the adapter calls (least-privilege input)

- Read path (`b:src/mesh/state-backend-nats-client.ts:28-119`, `b:src/mesh/reader-proof.ts:591`): `$JS.API.INFO`,
  `$SYS.REQ.USER.INFO`, `$JS.API.STREAM.INFO.<S>`, `$JS.API.STREAM.MSG.GET.<S>` (no `DIRECT.GET`), and the ordered
  pull consumer `$JS.API.CONSUMER.{CREATE,MSG.NEXT,DELETE,INFO}.<S>.<c>`. The challenger traced it: none missing.
- Write path: CAS publish to `<S>.state`, `<S>.snapshot` (`b:…-client.ts:97-107`).
- Purge after a snapshot (`b:…state-backend-nats.ts:684`): maintainer only after G-2.
- Create, update, delete (`b:src/mesh/backend-migration-nats.ts:293,372-374,572`, `b:…-client.ts:73-84`):
  maintainer only. Branch open does not create (`b:…state-backend-nats.ts:482-485`); main does
  (`src/mesh/state-backend-nats.ts:454`). These ACLs need the branch (G-1, C5).

### 1.4 Permissions per role (in the scoped signing key; templates expand per user)

`<B>` = `FABRIC_MESH_{{tag(mesh)}}`, the user's bound stream names. `<CUS>` = the configured custody stream (9.2); it is never a `<B>`.
`<LIVE>` = the configured liveness storage's stream (for a KV bucket, its underlying stream) (9.2); it is never a `<B>`. The
exact names behind `<CUS>` and `<LIVE>` are open item O-2; this note does not invent them.

| role | publish allow | subscribe allow |
|---|---|---|
| `agent` | `<B>.state`, `<B>.snapshot`, `$JS.API.INFO`, `$JS.API.STREAM.INFO.<B>`, `$JS.API.STREAM.MSG.GET.<B>`, `$JS.API.CONSUMER.{CREATE,MSG.NEXT,DELETE,INFO}.<B>.>`, `$SYS.REQ.USER.INFO` | `_INBOX.{{subject()}}.>` |
| `principal` | `$JS.API.INFO`, `$JS.API.STREAM.INFO.*`, `$JS.API.STREAM.MSG.GET.*`, the 4 consumer verbs `.*.>`, `$JS.API.STREAM.NAMES`, `$SYS.REQ.USER.INFO` | `_INBOX.{{subject()}}.>` |
| `maintainer` | `*.state`, `*.snapshot`, `$JS.API.INFO`, `$JS.API.STREAM.{INFO,MSG.GET,CREATE,UPDATE,DELETE,PURGE}.*`, `$JS.API.STREAM.NAMES`, the 4 consumer verbs `.*.>`, `$SYS.REQ.USER.INFO` | `_INBOX.{{subject()}}.>` |

Explicit publish **deny** (in each scoped signing key; deny wins over every allow above, so it also overrides the
maintainer's `PURGE.*`):

| role | publish deny |
|---|---|
| every role (agent, principal, maintainer) | `$JS.API.DIRECT.GET.<CUS>`, `$JS.API.DIRECT.GET.<CUS>.>` (9.2) |
| every role (agent, principal, maintainer) | `$JS.API.STREAM.PURGE.<CUS>`, `$JS.API.STREAM.PURGE.<LIVE>` (9.2, amendment review F1) |
| every role (agent, principal, maintainer) | `$JS.API.STREAM.DELETE.<CUS>`, `$JS.API.STREAM.DELETE.<LIVE>` (same record-erasure boundary) |

- **Root binding (C4).** Stream names are one token, so a prefix rule is impossible; the tag gives exact names. An
  agent that opens a root it was not minted for gets `Permissions Violation`; Fabric maps it to a clear error.
  `unverified:` substring expansion (`FABRIC_MESH_{{tag(mesh)}}`) and one subject per tag value; T2 measures both.
  Fallback: tag the whole stream name (works only if the server keeps tag case). Else D2.
- **Denied to every role** (allow lists deny the rest): `CONSUMER.DURABLE.CREATE`, the unnamed one-token
  `CONSUMER.CREATE.<S>` (the probe refused it; nats.js 3.4.0 uses the named form), `STREAM.MSG.DELETE`,
  `$JS.API.ACCOUNT.>`, other `$SYS.>` (no explicit deny: deny wins and would block USER.INFO), `$JS.ACK`/`$JS.FC`
  (pull consumers need neither), and subscribe outside the own inbox. No allow list holds `$JS.API.DIRECT.GET`;
  on `<CUS>` the explicit deny (table) also keeps a later allow from opening it. Purge and stream deletion on `<CUS>`
  and `<LIVE>` are explicitly denied to every role; maintainer mesh-root operations stay allowed; agents purge nothing. No role holds debug-attach
  subjects (9.3).
- **Private inbox.** The client sets `inboxPrefix: "_INBOX.<own U key>"` (gate G-3).
- **Compaction (gate G-2, C1).** Agent `compact()` publishes the snapshot and does not purge. One maintainer job per
  org on Ryzen 1 (hourly timer) mints a 1 h maintainer user, reads each root's latest snapshot, checks it as a
  reader does, and purges below its first sequence. A bad writer can still forge a snapshot and so cause loss, but
  only in the roots it is bound to.
- **Later sets (not designed here):** event log (`origin/feat/nats-event-log`) and KV leases
  (`origin/feat/nats-kv-leases`) each need their own bound allow entries. Custody and liveness storage (9.2) have the
  explicit deny above; their allow entries are open item O-2.
- **Expiry.** Agent `exp` = 7 days; Fabric's own cutoff at `exp − 1 h` (C6). Maintainer: 2 h migration, 1 h
  compaction. Principal: 90 days. `iat` 60 s in the past; no `nbf`. `SYS` serves only the resolver and the tools.

## 2. Issuance, launch graph, delivery, revocation

### 2.1 Who mints, rotation

- The issuer runs `fabric nats mint --role agent --org <org> --mesh <hex>…` under `smarty-credentials run --cache
  nats-mint-<org>` (agent signing key only): a fresh nkey pair and user JWT in memory (`@nats-io/nkeys`, encoder D4).
  Maintainer jobs use `nats-maint-<org>`; revoke and account push use `nats-admin`. No daemon; keys only on Ryzen 1.
- Users are not rotated: a new launch gets a new user. A signing key rotates: add the new key to the account JWT,
  mint with it, remove the old key after the longest live user expires (7 days).

### 2.2 Launch graph (C2)

**Rule.** Only an issuer mints. Each launch FROM the issuer gets its own user. A process started below it on the
same host **inherits its parent's user**: the parent writes the creds it holds into a new pipe on the child's FD 3.
No path uses a host-wide credential. A path without an issuer cannot select NATS: the NATS root refuses to open
with a new error `FABRIC_MESH_STATE_NATS_NO_CREDS`.

| launch path | creds source | user | test |
|---|---|---|---|
| lead or worktree Pi on Ryzen 1, started by the fleet launcher | launcher mints; local pipe | own | T11a |
| Pi started by hand on Ryzen 1 | none, unless started as `fabric nats run -- pi …` (mints) | refused / own | T11b |
| remote lane Pi, ssh from Ryzen 1 | launcher mints; ssh stdin; remote wrapper gives FD 3 | own | T11c |
| remote task (placement launcher on Ryzen 1, e.g. `smarty-task-ryzen2`) | launcher mints with `parent:`; ssh stdin | own | T11d |
| remote placement from a Main on a lane host | no issuer there | refused | T11e |
| local `process` task agent spawned by any Pi | parent's pipe | inherited | T11f |
| actors, actor activations, in-process children | the parent process's connection | inherited | T11g |
| durable resident host (hidden, per root) | the Main that starts it passes its pipe | inherited from that Main | T11h |
| `/reload` | creds stay in one process-global slot | unchanged | T11i |
| restart/resume (`pi --session`, relaunch) through the launcher | launcher mints again | own, new U key | T11a, T11c |
| restart/resume on a lane host without Ryzen 1 | none | refused (Q3) | T11j |
| mesh-bridge, hub and remote side; any other service | none in R1 | refuses a NATS root (D5) | T11k |
| maintainer jobs (migration, provision, compaction) | the job mints on Ryzen 1 | own, maintainer | T11l |
| principal device (m5) | `op run` with the principal creds item | own | T2 |

- Inherited means: NATS logs show the parent; revoking the parent cuts its children; a child has the parent's
  cutoff. The seed is in more processes, but on one host and one UID, already one trust boundary (docs/agents.md).
- The resident host keeps the user of the Main that started it. At its cutoff, or on revocation, it closes its NATS
  roots and exits as on host exit; the next durable request from a Main with valid creds starts a new one, which
  resumes the suspended actors (T11h).

### 2.3 Delivery and in-process handling (C8)

- Creds (JWT plus seed, the `.creds` text) go over the ssh stdin the launcher already uses, or a local pipe, to FD 3.
  `FABRIC_NATS_CREDS_FD=3` carries only the number. Never in argv, the environment, a file, a log, the mesh, an error.
- Fabric reads FD 3 once in its extension factory, before any tool, extension command or child runs, then closes
  it. It keeps the bytes in one process-global slot and gives them only to `credsAuthenticator` and child pipes.
- The wrapper sets `ulimit -c 0` before it starts Pi. T10 reads `/proc/<pid>/limits`, `core_pattern` and the FD list
  of Pi and of a child. It reads no credential bytes and prints none.
- **Same-UID boundary.** All code in the Pi process can read the seed. A same-UID process can read its memory where
  `ptrace_scope` allows. This is the same exposure as the lane-host model-gateway key in that process. We accept it.

### 2.4 Revocation (C7)

- `fabric nats revoke <U key | session:<id> | host:<h> | *>` on Ryzen 1 adds the keys to `revocations`, re-signs
  the account JWT, sends it on `$SYS.REQ.CLAIMS.UPDATE`, then asks each configured server for the JWT it holds.
- Output `complete` only when every server holds the new JWT. Else `partial`, the stale servers, exit 2.
- **Stale window.** Up node: target 5 s (T4; `unverified:` that the server closes live connections on the update).
  Stopped node: it starts with its old resolver dir, so it accepts the revoked user until it syncs, at most
  `interval` (2 min) after it joins the cluster. Partitioned but client-reachable node: no bound while the
  partition lasts. Response: put that node in lame-duck mode over ssh. With R3 streams (Q6) a minority node has no
  stream quorum, so the revoked user cannot read or write mesh state there; with R1 streams it can.
  *Amended (9.1):* R3 is the fleet configuration. One-machine configuration (`nodes: 1`): only the up-node case
  applies while the node runs. A node stopped during `revoke` restarts with its old resolver dir and has no peer to
  sync from, so it accepts the revoked user until `revoke` is re-run against it (`partial`, exit 2, until then; O-5).
  Not measured.
- A session that ends normally needs no revocation: its seed dies with its process tree.

## 3. Fleet and self-hosted custody (C3)

- **FLEET** (Smarty hosts): operator, account and signing keys in 1Password (Smarty Development), in memory on
  Ryzen 1 only. Issuer: the Ryzen 1 launcher. Only Ryzen 1 processes with the profile can mint. Fleet policy.
- **SELF-HOSTED** (a person's own machines): `fabric nats init --local` writes them to `~/.config/fabric/nats/`
  (dir 0700, files 0600, owner the user). Issuer: the local launcher. Any process of that Unix user can mint. The
  user's own trust.
- **Same code path:** `fabric nats mint`, the pipe, the full resolver, the permissions and the client.
- This is a deliberate, documented difference, not a fleet file class. On a self-hosted install any same-UID process
  (an agent's bash tool too) can read the keys and mint any user. There, per-launch users give attribution,
  revocation and org and root separation against mistakes, not against a hostile same-UID process. The self-hosting
  docs say so. Option, not required: run the issuer as a separate Unix user.
- `init --local` refuses when the Fabric setting `nats.custody` is `fleet` (fleet enrollment sets it). The fleet
  launcher refuses a local operator.
- `init --local` writes the server config (`operator`, `system_account`, `resolver: { type: full }`,
  `resolver_preload` of `SYS`). Embedded: Fabric starts `nats-server` as a child, bound to `127.0.0.1` unless the
  user sets a listen address. One node, no external service. It renders `bus: {nodes: 1, replicas: 1}` with the fleet's adapter,
  accounts, users and creds (9.1). No-auth loopback stays only in unit tests.

## 4. Ordering gates (all before the first fleet root switches to NATS)

- **G-1** The branch's `create: false` open contract is merged (C5). T12. **G-3** Private inbox prefix in the client.
- **G-2** Agent compaction without purge, plus the maintainer compaction job (C1). T13.
- **G-4** docs/mesh-backends.md "Required nats-server configuration (pi-fabric#907)" is replaced by the role matrix
  of 1.4 in the same PR. The new rule: only agent users bound to that root, and maintainers, publish to its state
  subjects; only maintainers purge or delete mesh-root streams (never `<CUS>` or `<LIVE>`, 9.2), create or update.
  T2 on the deployed cluster is the deploy check.
- **G-5** T2 proves root binding, or fabric-store decides D2. **G-6** Q6 (replicas 3) is done. *Amended (9.1):* the
  fleet configuration selects replicas 3, the adapter takes replicas from the configuration and checks existing
  streams and buckets against it (smarty-dev#11373); a one-machine install selects 1.
- **G-7** Prior art pinned and inspected, encoder chosen (D4), security pass passed. **G-8** All T11 cases pass.
- **G-9** (9.2, before any engine keeps `owner.<agent>` in NATS) `<CUS>` has `allow_direct: false`, no expiry and the
  configured replicas, checked at open; a mismatch is a hard open failure (no ownership decision and no owner write
  until a maintainer corrects it; replicas are never lowered automatically); liveness storage `<LIVE>` is separate
  and its replicas are checked the same way; purge and stream deletion on `<CUS>` and `<LIVE>` are denied to every role (1.4); the engine
  mapping (O-2) is written; T14 passes.

## 5. Failure modes

| failure | effect | response |
|---|---|---|
| resolver dir or account JWT lost | that node (or all) rejects the org's users; data intact | peer sync within `interval`; re-push from 1Password |
| leaked seed | holder acts as that launch, in its roots, until `exp` | revoke the U key; 2.4 windows |
| lane host compromised | its live users, writes in their bound roots | revoke `host:<h>`; no signing key on the host |
| NATS node compromised | that node's traffic and JetStream data; cannot mint | remove from the cluster; rotate its TLS cert |
| Ryzen 1 compromised | attacker mints any user, re-signs accounts | rotate all signing keys; operator identity key re-issues |
| signing key leaked | account key: users of that role; operator key: forged accounts | remove the key and relaunch; or new operator JWT, reload |
| compaction job stopped | streams grow to the account limit, then writes fail | alert on job age; run the job by hand |
| revoke `partial` | stale node accepts the user (2.4) | lame duck the node; re-run revoke |
| credential cutoff reached | the process and its children lose NATS | relaunch through the launcher |
| liveness key lapses (9.2) | ownership does not change | a successor starts only after the #8639 confirmed-termination handoff |
| stream or bucket differs from configured replicas, or `<CUS>` from `allow_direct: false` or no expiry | open fails with an error naming the field; no ownership decision and no write to it; never lowered automatically | a maintainer corrects the stream, then the open is retried |
| a maintainer job sends `STREAM.PURGE` or `STREAM.DELETE` for `<CUS>` or `<LIVE>` | `Permissions Violation` (explicit deny, 1.4); owner records and history intact | none; mesh-root operations are unaffected |

## 6. Test plan

Cluster tests: Ryzen 2, Intel 1, qs32729, after the tailnet ACL change (docs/mesh-backends.md, "Network
prerequisite"); monitoring from Ryzen 1 on 8222. No test server binds a non-loopback address off the cluster.

| test | checks |
|---|---|
| T1 `nats-auth-cluster-form` | fleet configuration `{nodes: 3, replicas: 3}`: 3 nodes route in operator mode; `/varz` cluster size 3; `/accountz` shows `SYS` and the org; streams and buckets the adapter opens report replicas 3; T3: no creds, connect refused |
| T2 `nats-auth-role-matrix` | per role, every verb of 1.3-1.4 passes or gets `Permissions Violation`; agent: other root refused, purge refused, 2 `mesh:` tags both work; every role: `DIRECT.GET` on `<CUS>` refused, `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused (maintainer included), no attach subjects (9.2, 9.3); control: maintainer purge and deletion of a mesh-root `<B>` pass |
| T4 `nats-auth-revoke-propagation` | live connections on all nodes close and reconnect fails within 5 s; stopped node; partitioned node gives `partial` |
| T5 `nats-auth-expiry-skew` | a live connection past `exp`; reconnect after `exp` refused; Fabric cutoff at `exp − 1 h`; +30 s skew connects |
| T6 `nats-auth-org-isolation` | a `playful-test` user sees no Smarty stream in `STREAM.NAMES` and cannot get its messages |
| T7 `nats-auth-reader-proof-bind` | a proof signed by reader A is refused as reader B's proof |
| T8 `nats-auth-single-node` (CI) | `init --local` renders `{nodes: 1, replicas: 1}`; one loopback `nats-server`; the same adapter and the conformance suite over creds; a stream or bucket whose replicas differ from the configuration fails to open, gets no write and is not lowered. Not in CI today (9.1) |
| T9 `nats-auth-1password-down` | a launch with the cached profile mints; revoke reports the outage |
| T10 `nats-auth-fd-core` | FD 3 closed in Pi before tools; absent in children; core limit 0; no seed pattern in logs (count only) |
| T11 `nats-auth-launch-graph` | cases a-l of 2.2: each gets its stated user or refuses with `FABRIC_MESH_STATE_NATS_NO_CREDS` |
| T12 `nats-auth-root-lifecycle` | new root (provision, then agent open), migrated root, migration rerun; agent open of a missing stream refuses |
| T13 `nats-auth-compaction` | agent compaction publishes without purge; the job purges below the snapshot |
| T14 `nats-auth-custody-leader` | `<CUS>` info: `allow_direct: false`, no expiry, configured replicas; ownership reads use `STREAM.MSG.GET`; a CAS write on a stale revision fails; a lapsed liveness key leaves `owner.<agent>` unchanged; maintainer `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused while maintainer mesh-root operations pass (control); a `<CUS>` whose replicas, `allow_direct` or expiry differ from the configuration fails to open, no ownership read decides and no owner write is sent, and nothing is lowered (needs O-2) |

## 7. The reader-proof gap (smarty-dev#9834)

Today a proof answers the stream nonce (`b:src/mesh/reader-proof.ts:50-51`); any NATS reader can compute it for any
reader name. With per-launch users, the proof adds the reader's U key and an nkey signature over the challenge. The
gate checks the signature, that the org's `agent` key signed the user JWT, and that its `host:`/`release:` tags match
the proof. This stops copying and forging across launches. It does NOT prove the code: a changed process that holds
the seed can still sign, and children share the parent's key. The gate's output says so.

## 8. Options considered

- **Chosen: JWT + full resolver.** Per-launch id, org isolation, revocation in one push, the same config on one
  machine. Cost: `SYS` creds on Ryzen 1, a resolver dir on each node. Fallback: MEMORY resolver (ssh and reload).
- **No:** users/passwords or nkey users in the server config (a reload per launch; password file class); a token or
  user per host (one stolen host credential is every session on that host: no per-launch revocation, C9); auth
  callout (an external service on the connect path); Synadia Control Plane or Cloud (commercial, hosted).

## Prior art

`unverified:` (no network; G-7 pins and inspects): NATS docs JWT resolver (nats-io/nats.docs, Apache-2.0), our model;
nsc (nats-io/nsc, Apache-2.0), keys in an on-disk keystore, so setup tool and encoder oracle, not the minter; Helm
chart (nats-io/k8s) and NACK (nats-io/nack), same config shape and creds split. Checked locally by the challenger:
`@nats-io/jetstream`, `@nats-io/transport-node` 3.4.0 (Apache-2.0), `@nats-io/nkeys` 2.0.3.

## Open questions

- **Q3.** Local relaunch without Ryzen 1: fail closed (now), or a per-host bootstrap creds file (a new class).
- **Q4.** Cache the `nats-admin` profile for revocation during a 1Password outage, or not.
- **Q5.** Route auth on 6222: TLS plus the tailnet ACL only, or also a route password.
- **Q6.** Streams use `num_replicas: 1` (`b:…-client.ts:66,79`). R3 is needed for the claims and for the 2.4 bound.
  *Amended (9.1):* R3 is the fleet configuration; one machine selects 1. Main `c9b9a1d4` also hard-codes 1.

## Open disagreements

- **D1 (C2)** Children inherit the parent's user; a per-child user needs an issuer reachable from lane hosts (a daemon
  or a host key). Recommend: accept inheritance for R1; per-child users in phase C.
- **D2 (C4)** If T2 cannot bind exact streams. Recommend: block the fleet switch, try the whole-name tag, and only
  then ask Paul to accept org-wide agent trust as a recorded trade-off.
- **D3 (C3)** Self-hosted signing keys on the user's disk. Recommend: accept as a documented self-hosting mode.
- **D4 (C9)** Encoder: own ~80 lines vs `@nats-io/jwt` 0.0.10-5 (pre-1.0) vs a Go helper on nats-io/jwt/v2.
  Recommend: own encoder, tested against `nsc describe` and nats-server, after G-7 inspects `@nats-io/jwt`.
- **D5 (C2)** mesh-bridge carries no NATS root in R1. Recommend: accept; a `bridge` identity needs its own design.
- **D6 (C6, C7)** Hard 7-day cutoff, and no bound for a partitioned node. Recommend: accept for R1 with the
  lame-duck runbook and Q6; renewal in phase C.

## Approvals needed

- **F1 (secret, fleet).** NATS node TLS key `~/.local/share/smarty/nats/tls/<host>.key` on Ryzen 2, Intel 1,
  qs32729 only. File 0600, dir 0700, owner `paul`. Writer: `tailscale cert` on that host (never sent). Rotation:
  90 days, host timer; deleted at decommission. Approver: chief-of-staff; FYI and veto to Paul.
- **F2-F4 (not secret).** Resolver dir `~/.local/share/smarty/nats/jwt/` (3 nodes, dir 0700, writer nats-server);
  server config `~/.local/share/smarty/nats/nats-server.conf` (3 nodes, 0644, operator JWT and `SYS` public key);
  `~/.config/systemd/user/fabric-nats-compact@<org>.{service,timer}` (Ryzen 1, 0644). Owner `paul`; the deploy step
  from Ryzen 1 writes the last two; the config changes on operator rotation. Host record only.
- **S1 (self-hosting, never on a fleet host).** `~/.config/fabric/nats/` on the user's machine, dir 0700, files
  0600, owner the user, writer `fabric nats init --local`, rotation `fabric nats rotate --local`. Approver:
  fabric-store, as a product mode.
- **No new file class for agent creds on any lane host.**
- **1Password items**, vault Smarty Development: `NATS — operator identity key (smarty fleet)`,
  `NATS — operator signing key (smarty fleet)`, `NATS — system account user (smarty fleet)`,
  `NATS — account identity key (org smarty-pants)`, `NATS — agent signing key (org smarty-pants)`,
  `NATS — principal signing key (org smarty-pants)`, `NATS — maintainer signing key (org smarty-pants)`,
  `NATS — principal user paul-m5 (org smarty-pants)`. Later, vault Playful Development: the same four account items
  for `org playful`. Profiles (Ryzen 1 only): `nats-mint-smarty-pants`, `nats-maint-smarty-pants`, `nats-admin`.
- **Security pass `nats-auth-7504-security`**, by a reviewer who is not the designer or the challenger, before any
  fleet root switches: this note, the encoder, the permission templates, the #907 text change (G-4), and the
  T2, T4, T5, T10 and T11 results.

## fabric-store decisions on revision 1 (10-12 05:20Z)

The design is accepted as the R1 basis. A named security pass on this note runs next, in place of the skipped challenge round 2. No fleet traffic and no 1Password items are created before it passes.

- **D1 accepted.** Children inherit the parent's user in R1; per-child users move to phase C.
- **D2 accepted.** If T2 can't bind exact streams: block the fleet switch, try the whole-name tag, and only then take the org-wide trust trade-off to Paul.
- **D3 accepted.** Self-hosted keys on the user's own disk, as a documented product mode (S1, approver fabric-store; Paul's own words: "scale all the way down to a single machine"). It is never a fleet file class.
- **D4: the encoder is decided at G-7, leaning toward the recommendation.** The own encoder (~80 lines over `@nats-io/nkeys`) wins only if its output matches `nsc describe` and nats-server accepts it, in a test. The `ponytail:` comment says why `@nats-io/jwt` (pre-1.0) is not used. If G-7 finds `@nats-io/jwt` sound and maintained, it is used instead.
- **D5 accepted.** mesh-bridge carries no NATS root in R1.
- **D6 accepted.** A hard 7-day cutoff, the lame-duck runbook, and replicas 3 (Q6). Renewal moves to phase C.
  *Amended 10-13 (9.1):* replicas 3 is the fleet's configuration; a one-machine install is `{nodes: 1, replicas: 1}`.
- **Q3:** fail closed, as written now. No per-host bootstrap creds file.
- **Q4:** no cache for `nats-admin`. During a 1Password outage, the response to a leak is lame duck or stopping the node over ssh (runbook row). A cached operator-signing-key file on Ryzen 1 is a worse risk than a delayed revoke.
- **Q5:** routes use **mutual TLS with known-URL checking** (`verify_cert_and_check_known_urls` on the route block) plus the tailnet ACL, which limits 6222 to the 3 nodes. There is no route password, so there is no new secret on the nodes beyond F1. T1 adds one more check: a 4th tailnet host with a valid certificate cannot join as a route.
- **Q6:** replicas 3 are required before the fleet switch (G-6, unchanged).
  *Amended 10-13 (9.1):* for the fleet configuration only. It does not block a one-machine install and is not a
  runtime branch.

**Phasing, for the S8 date (logins ready for the direct-route S8 run by 10-13 12:00Z):**
- **Phase 1, for S8:**
  - `fabric nats mint`;
  - the server config generator (operator mode, full resolver, route mTLS);
  - `init --local` with an embedded single node;
  - T1, T2, T4, T5, T6, T8.
  - S8 uses minted test users only, never a fleet root.
- **Phase 2, before any fleet root switches:**
  - FD 3 delivery and the launch graph (T10, T11);
  - G-1, G-2 (the maintainer compaction job), G-3, G-4;
  - the reader-proof binding (section 7, #9834).

## 9. Amendment A (10-13, #958): three agreed #8031 constraints

Docs only. Nothing in this section is implemented, tested or measured unless it says so. It adds three constraints
from the agreed #8031 addendum (engine-lead to fabric-store on #7504, posted 10-12 06:10Z). It does not change the
auth model, the roles, key custody or the decisions above, except where they now say "amended". The three problems:
replication is a configuration value; ownership is a CAS record while liveness is an expiring signal; human debug
attach is its own authentication, not NATS.

### 9.1 One machine is a cluster of one, by configuration

- The configuration names the topology: the fleet selects `bus: {nodes: 3, replicas: 3}`, one machine
  `bus: {nodes: 1, replicas: 1}`. Both use the same adapter, operator/account/user model, creds delivery and role
  matrix (1.4). Goal G2 applies on one host too.
- No code branches on host count at runtime. Rendering the server config from `bus` is fine.
- Replicas are never lowered at runtime. The adapter checks each existing stream and bucket against the configured
  replicas; a mismatch is a hard open failure, not an automatic change: nothing is written to that stream or bucket
  until it is corrected. Agents hold no `STREAM.UPDATE` (1.4); a correction is a maintainer step (O-3, resolved).
  Replicas are never reduced automatically.
- Replicas 3 (Q6, G-6, D6) is the fleet's selected configuration, not an unconditional barrier. It does not block a
  one-machine install (section 3). Its part in the 2.4 stale-window bound applies to the fleet configuration.
- **Implementation and CI gap (smarty-dev#11373).** Current main
  `c9b9a1d44cb723589bcbd54d46ed7fc7814c5993`: `src/mesh/state-backend-nats-client.ts:59` creates streams with
  `num_replicas: 1` hard-coded; lines 62-64 add missing subjects and do not check replicas.
  `.github/workflows/test.yml:194-232` (`nats-state-conformance`) runs one standalone loopback nats-server 2.14.7
  with JetStream, no cluster and no auth. So neither the fleet R3 configuration nor T1/T8 is covered today. Read
  with `git show`; nothing was run.

### 9.2 Custody is a CAS record; liveness only signals

- `owner.<agent>` = `{engine, epoch, state}`, written by compare-and-set on its last revision, with no TTL. It
  lives in a configured custody stream `<CUS>` with no expiry (no max age, no per-message TTL). No TTL bucket may
  hold or expire an owner record.
- A separate liveness key with a TTL lives in separately configured storage. It only signals. A lapsed liveness key
  never authorizes a successor; only the confirmed-termination handoff of smarty-dev#8639 does.
- Reads that decide ownership go to the stream leader: `<CUS>` has `allow_direct: false`, every role has the
  explicit `DIRECT.GET` deny of 1.4, and ownership reads use `$JS.API.STREAM.MSG.GET.<CUS>`.
- Do not assume the leader path by default. The NATS KV docs (9.5) show a KV bucket created with Direct Get on (the KV
  default, per the prior read). So
  `<CUS>` sets `allow_direct: false` explicitly and the adapter checks it in `STREAM.INFO` at open, as for
  replicas. A mismatch in replicas, `allow_direct` or expiry is a hard open failure: no ownership read decides and
  no `owner.<agent>` write goes out until a maintainer corrects `<CUS>`; nothing is changed automatically. Which request a nats.js 3.4.0 KV `get` sends: not measured.
- `<CUS>` and liveness storage are not a mesh root's state stream `<B>`. The root binding, compaction and purge rules
  for `<B>` do not cover them. `<LIVE>` (1.4) names the liveness storage's stream. Agents hold no purge anywhere.
  The maintainer purges mesh-root streams `<B>` (G-2), but every role, maintainer included, has an explicit deny on
  `$JS.API.STREAM.PURGE.<CUS>` and `$JS.API.STREAM.PURGE.<LIVE>` (1.4). The same explicit deny covers
  `$JS.API.STREAM.DELETE.<CUS>` and `$JS.API.STREAM.DELETE.<LIVE>`: deleting the stream would erase the same
  owner records and CAS history. Mesh-root stream lifecycle authority is unchanged.
- **Not fixed here:** the engine's exact bucket or stream names, subjects, and which role writes `owner.<agent>`.
  The #8031 addendum does not give that mapping and this note does not invent it: O-2, gate G-9, test T14.

### 9.3 Human debug attach is not NATS

- Attach goes to the owner's socket through upstream `pi-server`. Each operation carries an SSH signature over the
  op and a digest of its payload. It uses no NATS creds.
- `watch` comes now. `drive` comes only after agents run under their own Unix users (org decision 10-12).
- No role in 1.4 has attach subjects: agent users need none, and principal users are for the mesh only (T2).
- The attach transport, its keys and its tests are outside this note.

### 9.4 Open items

- **O-1** smarty-dev#11373: replicas from the
  configuration, checks of existing streams and buckets, and a clustered, authenticated CI job.
- **O-2** The engine's custody and liveness mapping (names, subjects, CAS writer role) and the 1.4 allow entries for
  it, including subject-space isolation from the maintainer's `*.state` / `*.snapshot` mesh grants. Gate G-9.
- **O-3 resolved** (amendment review F2): a replicas, `allow_direct` or expiry mismatch is a hard open failure, with
  no ownership decision or write until a maintainer corrects it; never lowered automatically (9.1, 9.2, G-9, T8, T14).
- **O-4 resolved** (amendment review F1): purge and stream deletion on `<CUS>` and `<LIVE>` are explicitly denied
  to every role, maintainer included; mesh-root lifecycle operations stay allowed (1.4, G-4, T2, T14).
- **O-5** One-node revoke after a stop (2.4): re-run `revoke`; not measured.
- **O-6** The phase of G-9 and T14 relative to the phase 1 / phase 2 split above.
- **O-7** This amendment is not a security pass. Whether `nats-auth-7504-security` also covers section 9: lead
  decides.

### 9.5 Prior art

- Read 10-13 18:02Z (HTTP 200, text checked): NATS docs
  [KV under the hood](https://docs.nats.io/learn/key-value/under-the-hood): the example bucket shows
  `Direct Get: true`; a direct get goes to `$JS.API.DIRECT.GET.<stream>.<subject>`.
  [etcd API](https://etcd.io/docs/v3.5/learning/api/): transactions as compare-and-swap; a key may carry a lease ID
  (0 = none).
- Read by the prior writer 17:56Z, not re-read: [KV TTL and limits](https://docs.nats.io/learn/key-value/ttl-and-limits)
  (per-key TTL needs server 2.11+ and limit markers).
- The difference: an etcd key attached to a lease goes away when the lease expires (not re-checked here). We keep
  the owner record free of any TTL and treat the TTL key as a signal, because a successor needs confirmed
  termination (#8639). `unverified:` fencing-token write-ups such as Kleppmann's "How to do distributed locking"
  (not fetched).
