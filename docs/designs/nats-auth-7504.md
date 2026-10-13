DESIGN (design-pair, challenge record: docs/designs/nats-auth-7504-challenge-r1.md; round 2 skipped for time)

# NATS authentication: one login per issued launch and principal, one account per org

smarty-dev#7504 / #8031 R1 (goals G1, G2). Owner: fabric-store. Status: DESIGN DRAFT, revision 1 with Amendment A (10-13, section 9) and Amendment B (10-13, section 10); implementation and named security approval remain pending. Sections 9 and 10 are docs only, not implemented or measured.
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

## Round 2 answers (Amendment B, 10-13, #958)

Basis: the full texts of two #958 reviews (staged copy read by this writer) and the lead's decisions on them:
Astra, https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6152830182 (one P1), and security,
https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6152848625 (`SECURITY_REVIEW: FAIL`; P1, P1, P2).
The lead's decisions match all four findings; no extra finding is claimed. B5 is a lead clarification, not a review
finding. Section 10 has the detail. Docs only: nothing is implemented, tested or measured.

- **B1 (Astra P1, c6152830182) ADOPT.** No normal role (agent, principal, maintainer) can change the configuration
  of `<CUS>` or `<LIVE>`: each normal scoped signing key denies `STREAM.CREATE` and `STREAM.UPDATE` on them, besides
  the existing `PURGE`/`DELETE` deny. A separate break-glass user, minted only by an explicit
  `fabric nats mint --break-glass`, expires after 1 h and may only create or update `<CUS>`/`<LIVE>`; its mint logs
  public metadata only. The runbook stops the engines that use them first (#8639 confirmed stop), changes, rechecks,
  restarts; engines recheck `<CUS>` on every reconnect and refuse a mismatch, so no already-open engine continues under
  a changed `<CUS>` configuration (the review's point). T14 adds the refusal. Sections 1, 1.4,
  2.1, 4, 5, 6, 9.1, 9.2, 10.1.
- **B2 (security P1, c6152848625) ACCEPTED as a security gap, smarty-dev#TBD-UID.** Fleet agents under one UID on
  one host are one trust domain. Per-launch creds give network authentication, org isolation and root binding, not
  same-host isolation. Fixes: a per-host broker under a separate UID, or per-lane Unix users. Measure first:
  `ptrace_scope` and `PR_SET_DUMPABLE=0` (not implemented, not a fix). C8's FD and core guards stay. Sections 1.1,
  2.2, 2.3, 10.2.
- **B3 (security P1, c6152848625) ADOPT.** `revoke` writes the new account JWT durably (write, fsync, rename,
  parent-dir fsync) into
  each node's local resolver dir, also when its server is down. Every node start, for `init --local` and for each
  node of a 3-node configuration, accepts no clients until its resolver account JWT has the SHA-256 of the latest
  account JWT the issuer signed. Closes O-5 by design (not measured). T16. Sections 2.4, 3, 4, 5, 6, 9.4, 10.3.
- **B4 (security P2, c6152848625) ADOPT.** Rotation is per signing key: a retiring key stays in the account JWT until
  the longest credential it signed expires (principal 90 days, agent 7 days, maintainer 2 h, break-glass 1 h), or
  until every user it signed is reissued and revoked. T15. Sections 2.1, 5, 6, 10.4.
- **B5 (lead clarification, 10-13) ADOPT.** A separate SYS-only user, signed by the `SYS` account identity, never by
  an org role key, publishes only `$SYS.REQ.CLAIMS.UPDATE` (no lookup or wildcard publish) and subscribes only to
  its own inbox; 1 h; only for an explicit operator `revoke` or `--sys` use, never in a
  launch environment. Org users cannot publish resolver or admin `$SYS.REQ` subjects (`$SYS.REQ.USER.INFO` stays).
  Encoder D4 decided; quotas and `<CUS>`/`<LIVE>` configuration are required caller inputs. Sections 1, 1.4, 2.1,
  6, D4, 10.5, 10.6.

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
| system account | `SYS` | identity key: 1Password; in memory on Ryzen 1 only | the SYS-only user (10.5), never an org user |
| org account | `smarty-pants` (first), `playful` (later) | identity key: 1Password (recovery) | - |
| scoped signing keys | per account: `agent`, `principal`, `maintainer`, `break-glass` (10.1) | 1Password; in memory on Ryzen 1 only | user JWTs of that role only |
| user | one per issued launch, principal device, maintainer job, or break-glass change (10.1) | process memory (principal: 1Password) | - |

### 1.1 Identities

- **Agent launch.** User JWT `name` = `agent/<host>/<lane>/<session-id>`. Tags: `host:`, `lane:`, `session:`,
  `release:`, `role:agent`, one `mesh:<32 hex>` per mesh root that the launch may open (the hex of
  `natsStreamName`, `b:src/mesh/state-backend-nats.ts:1057-1063`), and `parent:<session-id>` for a remote task.
  The user public key (`U…`) is the identity in server logs and `connz?auth=1`.
- **Principal:** `principal/<person>/<device>`. **Maintainer:** `maintainer/<org>/<job>/<run-id>`, Ryzen 1 only.
  **Break-glass:** `break-glass/<org>/<person>/<run-id>`, Ryzen 1 only, 1 h (10.1).
- **Per-org isolation** is the account boundary: no exports or imports; account JetStream limits (disk, streams,
  consumers), so one org cannot fill a node.
- **What it does NOT give.** All writers of one root share one stream, and any writer can forge a self-consistent
  snapshot (docs/mesh-backends.md, "Snapshot integrity"). A child shares its parent's user (2.2), so NATS logs name
  the parent launch, not the child. Nor does it give same-host isolation: one UID on one host is one trust domain
  (accepted security gap smarty-dev#TBD-UID, 10.2).

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
| `break-glass` (10.1; not a normal role) | `$JS.API.INFO`, `$JS.API.STREAM.{INFO,CREATE,UPDATE}.<CUS>`, `$JS.API.STREAM.{INFO,CREATE,UPDATE}.<LIVE>`, `$SYS.REQ.USER.INFO`; nothing else (no `MSG.GET`, no mesh root, no state subject) | `_INBOX.{{subject()}}.>` |

The SYS-only user is separate from this org-role table, signed by the SYS account identity (10.5):

| account / user | publish allow | subscribe allow |
|---|---|---|
| `SYS` / explicit operator user, 1 h | `$SYS.REQ.CLAIMS.UPDATE` only; no lookup, wildcard or other admin subject | `_INBOX.<own U key>.>` |

Explicit publish **deny**. An org user's permissions come only from the scoped signing key that signed it. A deny wins over
every allow **of the same user**, so it overrides the maintainer's `PURGE.*`, `DELETE.*`, `CREATE.*` and `UPDATE.*`
for `<CUS>` and `<LIVE>`. A deny in a normal role's key does not reach a break-glass user: that user has its own key
with its own allow and deny lists (10.1). So the normal-role deny and the break-glass allow do not contradict.

| role | publish deny |
|---|---|
| every role (agent, principal, maintainer, break-glass) | `$JS.API.DIRECT.GET.<CUS>`, `$JS.API.DIRECT.GET.<CUS>.>` (9.2) |
| every role (agent, principal, maintainer, break-glass) | `$JS.API.STREAM.PURGE.<CUS>`, `$JS.API.STREAM.PURGE.<LIVE>` (9.2, amendment review F1) |
| every role (agent, principal, maintainer, break-glass) | `$JS.API.STREAM.DELETE.<CUS>`, `$JS.API.STREAM.DELETE.<LIVE>` (same record-erasure boundary) |
| every normal role (agent, principal, maintainer) | `$JS.API.STREAM.CREATE.<CUS>`, `$JS.API.STREAM.CREATE.<LIVE>`, `$JS.API.STREAM.UPDATE.<CUS>`, `$JS.API.STREAM.UPDATE.<LIVE>` (10.1, Astra P1) |

- **Normal role boundaries (10.1).** Agents use their bound roots; principals read; the maintainer runs mesh-root
  `<B>` lifecycle (create, update, delete, purge) and compaction. No normal role changes custody or liveness storage.
  The configuration-changing verbs on `<CUS>`/`<LIVE>` are `CREATE`, `UPDATE` and `DELETE` (`PURGE` erases records).
  Only the maintainer's `.*` allow reaches them today; the deny is in every normal key anyway, so a later allow cannot
  reopen it. Other `$JS.API.STREAM.*` verbs (for example `RESTORE`) are in no allow list, break-glass included, and
  stay denied.

- **Root binding (C4).** Stream names are one token, so a prefix rule is impossible; the tag gives exact names. An
  agent that opens a root it was not minted for gets `Permissions Violation`; Fabric maps it to a clear error.
  `unverified:` substring expansion (`FABRIC_MESH_{{tag(mesh)}}`) and one subject per tag value; T2 measures both.
  Fallback: tag the whole stream name (works only if the server keeps tag case). Else D2.
- **Denied to every role** (allow lists deny the rest): `CONSUMER.DURABLE.CREATE`, the unnamed one-token
  `CONSUMER.CREATE.<S>` (the probe refused it; nats.js 3.4.0 uses the named form), `STREAM.MSG.DELETE`,
  `$JS.API.ACCOUNT.>`, other `$SYS.>` (no explicit deny: deny wins and would block USER.INFO), `$JS.ACK`/`$JS.FC`
  (pull consumers need neither), and subscribe outside the own inbox. No allow list holds `$JS.API.DIRECT.GET`;
  on `<CUS>` the explicit deny (table) also keeps a later allow from opening it. Purge and stream deletion on `<CUS>`
  and `<LIVE>` are explicitly denied to every role, break-glass included; create and update on them are denied to
  every normal role; maintainer mesh-root operations stay allowed; agents purge nothing. No role, break-glass
  included, holds debug-attach subjects (9.3).
- **Private inbox.** The client sets `inboxPrefix: "_INBOX.<own U key>"` (gate G-3).
- **Compaction (gate G-2, C1).** Agent `compact()` publishes the snapshot and does not purge. One maintainer job per
  org on Ryzen 1 (hourly timer) mints a 1 h maintainer user, reads each root's latest snapshot, checks it as a
  reader does, and purges below its first sequence. A bad writer can still forge a snapshot and so cause loss, but
  only in the roots it is bound to.
- **Later sets (not designed here):** event log (`origin/feat/nats-event-log`) and KV leases
  (`origin/feat/nats-kv-leases`) each need their own bound allow entries. Custody and liveness storage (9.2) have the
  explicit deny above; their allow entries are open item O-2.
- **Expiry.** Agent `exp` = 7 days; Fabric's own cutoff at `exp − 1 h` (C6). Maintainer: 2 h migration, 1 h
  compaction. Principal: 90 days. Break-glass: 1 h (10.1). `iat` 60 s in the past; no `nbf`. `SYS` serves only the resolver and the tools, through the SYS-only user
  (10.5): 1 h.

## 2. Issuance, launch graph, delivery, revocation

### 2.1 Who mints, rotation

- The issuer runs `fabric nats mint --role agent --org <org> --mesh <hex>…` under `smarty-credentials run --cache
  nats-mint-<org>` (agent signing key only): a fresh nkey pair and user JWT in memory (`@nats-io/nkeys`, encoder D4).
  Maintainer jobs use `nats-maint-<org>`; revoke and account push use `nats-admin` and a 1 h SYS-only user (10.5). A break-glass user comes only
  from an explicit `fabric nats mint --break-glass --org <org>` under `nats-breakglass-<org>` (no cache, as Q4),
  after explicit prior approval and the 10.1 runbook; no launcher, job, timer or deployment mints it. No daemon; keys
  only on Ryzen 1.
- Users are not rotated: a new launch gets a new user. **Signing keys rotate one key at a time (10.4):** add the new
  key to the account JWT and mint with it; remove the retiring key only after the longest credential **that key**
  signed expires (agent 7 days, principal 90 days, maintainer 2 h, break-glass 1 h), or after every user it signed
  was reissued with the new key and revoked. There is no single 7-day rule for all keys. T15.

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
  cutoff. The seed is in more processes, but on one host and one UID, already one trust domain (docs/agents.md):
  an accepted security gap, not isolation (10.2, smarty-dev#TBD-UID).
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
- **Same-UID boundary (accepted security gap, smarty-dev#TBD-UID, 10.2).** All code in the Pi process can read the
  seed. A same-UID process can read its memory where `ptrace_scope` allows. This is the same exposure as the
  lane-host model-gateway key in that process. The lead accepts it as a recorded security gap, not as isolation; the
  FD and core guards above stay. Measuring `ptrace_scope` and `PR_SET_DUMPABLE=0` comes first (10.2); neither is
  implemented here, and neither removes same-process or every same-UID path.

### 2.4 Revocation (C7)

- `fabric nats revoke <U key | session:<id> | host:<h> | *>` on Ryzen 1 adds the keys to `revocations`, re-signs
  the account JWT, records it as the latest (10.3), writes it durably into each configured node's local resolver dir
  (write, fsync, rename, parent-dir fsync; also when that node's server is down; 10.3), broadcasts that exact JWT on
  `$SYS.REQ.CLAIMS.UPDATE`, then collects the per-server update acknowledgements on its own reply inbox.
- Output `complete` only when every configured server ID acknowledges the exact broadcast JWT successfully.
  Correlate replies to that push's unique inbox and require every configured ID; an error, missing or unknown
  acknowledgement cannot satisfy it. Else `partial`, naming unconfirmed servers, exit 2. The server saves the
  broadcast claim before acknowledging it (10.5). This is not a per-server hash or bytes read-back: that is a known
  limit of the standard v2.14.7 API, accepted by the lead (10.5).
- **Stale window.** Up node: target 5 s (T4; `unverified:` that the server closes live connections on the update).
  Stopped node: *amended (10.3)*: its start accepts no clients until its resolver account JWT matches the latest,
  so it never accepts the revoked user after a restart; nothing relies on peer sync. It still cannot confirm, so
  `revoke` stays `partial` for it (C7). Partitioned but client-reachable node: no bound while the
  partition lasts. Response: put that node in lame-duck mode over ssh. With R3 streams (Q6) a minority node has no
  stream quorum, so the revoked user cannot read or write mesh state there; with R1 streams it can.
  *Amended (9.1):* R3 is the fleet configuration. One-machine configuration (`nodes: 1`): only the up-node case
  applies while the node runs. *Amended (10.3):* a node stopped during `revoke` gets the new account JWT in its local
  resolver dir from `revoke` itself, and its start accepts no clients until that file matches the latest, so it
  never accepts the revoked user after a restart (O-5 resolved by design, T16). Not measured.
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
  `resolver_preload` of `SYS`). Embedded: Fabric starts `nats-server` as a child (only after the 10.3 start check), bound to `127.0.0.1` unless the
  user sets a listen address. One node, no external service. It renders `bus: {nodes: 1, replicas: 1}` with the fleet's adapter,
  accounts, users and creds (9.1). No-auth loopback stays only in unit tests.

## 4. Ordering gates (all before the first fleet root switches to NATS)

- **G-1** The branch's `create: false` open contract is merged (C5). T12. **G-3** Private inbox prefix in the client.
- **G-2** Agent compaction without purge, plus the maintainer compaction job (C1). T13.
- **G-4** docs/mesh-backends.md "Required nats-server configuration (pi-fabric#907)" is replaced by the role matrix
  of 1.4 in the same PR. The new rule: only agent users bound to that root, and maintainers, publish to its state
  subjects; only maintainers purge, delete, create or update mesh-root streams; nobody purges or deletes `<CUS>` or
  `<LIVE>` (9.2), and only the break-glass user creates or updates them (10.1).
  T2 on the deployed cluster is the deploy check.
- **G-5** T2 proves root binding, or fabric-store decides D2. **G-6** Q6 (replicas 3) is done. *Amended (9.1):* the
  fleet configuration selects replicas 3, the adapter takes replicas from the configuration and checks existing
  streams and buckets against it (smarty-dev#11373); a one-machine install selects 1.
- **G-7** Prior art pinned and inspected, encoder chosen (D4), security pass passed. **G-8** All T11 cases pass.
- **G-9** (9.2, before any engine keeps `owner.<agent>` in NATS) `<CUS>` has `allow_direct: false`, no expiry and the
  configured replicas, checked at open and, for `<CUS>`, again on every reconnect; a mismatch is a hard open failure
  (no ownership decision and no owner write until it is corrected through the break-glass runbook, 10.1; replicas are
  never lowered automatically); liveness storage `<LIVE>` is separate and its replicas are checked the same way;
  purge and stream deletion on `<CUS>` and `<LIVE>` are denied to every role, and create and update to every normal
  role (1.4); the engine mapping (O-2) is written; T14 passes.
- **G-10** (10.3, 10.4) Every configured node start runs the resolver start check (T16); `revoke` writes resolver
  dirs durably; signing-key rotation follows 10.4 (T15).

## 5. Failure modes

| failure | effect | response |
|---|---|---|
| resolver dir or account JWT lost, or older than the latest | that node's start accepts no clients (10.3); data intact | write the issuer's latest account JWT into its resolver dir (re-run `revoke` or push), then start |
| leaked seed | holder acts as that launch, in its roots, until `exp` | revoke the U key; 2.4 windows |
| lane host compromised | its live users, writes in their bound roots | revoke `host:<h>`; no signing key on the host |
| NATS node compromised | that node's traffic and JetStream data; cannot mint | remove from the cluster; rotate its TLS cert |
| Ryzen 1 compromised | attacker mints any user, re-signs accounts | rotate all signing keys; operator identity key re-issues |
| signing key leaked | account key: users of that role; operator key: forged accounts | remove the key at once (this cuts every user it signed; reissue principals, 10.4) and relaunch; or new operator JWT, reload |
| compaction job stopped | streams grow to the account limit, then writes fail | alert on job age; run the job by hand |
| revoke `partial` | an unconfirmed live node may still accept the user (2.4); a stopped node's start gate refuses stale claims (10.3) | lame duck an unconfirmed live node; re-run revoke |
| credential cutoff reached | the process and its children lose NATS | relaunch through the launcher |
| liveness key lapses (9.2) | ownership does not change | a successor starts only after the #8639 confirmed-termination handoff |
| stream or bucket differs from configured replicas, or `<CUS>` from `allow_direct: false` or no expiry | open fails with an error naming the field; no ownership decision and no write to it; never lowered automatically | a maintainer corrects a mesh-root stream or bucket; `<CUS>`/`<LIVE>` only through the break-glass runbook (10.1); then the open is retried |
| a maintainer job sends `STREAM.PURGE` or `STREAM.DELETE` for `<CUS>` or `<LIVE>` | `Permissions Violation` (explicit deny, 1.4); owner records and history intact | none; mesh-root operations are unaffected |
| a normal role sends `STREAM.CREATE` or `STREAM.UPDATE` for `<CUS>` or `<LIVE>` | `Permissions Violation` (explicit deny, 1.4) | none; a needed change goes through the break-glass runbook (10.1) |
| `<CUS>` changed while an engine was disconnected | the engine's reconnect recheck fails; no ownership decision and no write | stop, correct and restart per 10.1 |

## 6. Test plan

Cluster tests: Ryzen 2, Intel 1, qs32729, after the tailnet ACL change (docs/mesh-backends.md, "Network
prerequisite"); monitoring from Ryzen 1 on 8222. No test server binds a non-loopback address off the cluster.

| test | checks |
|---|---|
| T1 `nats-auth-cluster-form` | fleet configuration `{nodes: 3, replicas: 3}`: 3 nodes route in operator mode; `/varz` cluster size 3; `/accountz` shows `SYS` and the org; streams and buckets the adapter opens report replicas 3; T3: no creds, connect refused |
| T2 `nats-auth-role-matrix` | per role, every verb of 1.3-1.4 passes or gets `Permissions Violation`; agent: other root refused, purge refused, 2 `mesh:` tags both work; every role: `DIRECT.GET` on `<CUS>` refused, `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused (maintainer included), no attach subjects (9.2, 9.3); control: maintainer purge and deletion of a mesh-root `<B>` pass; every normal role: `STREAM.CREATE` and `STREAM.UPDATE` on `<CUS>` and `<LIVE>` refused; break-glass: `STREAM.INFO`, `CREATE` and `UPDATE` on `<CUS>`/`<LIVE>` pass, everything else refused (`DIRECT.GET`, `MSG.GET`, purge, delete, mesh roots, state subjects, attach) (10.1); every org-account user: resolver and admin `$SYS.REQ` subjects refused while `$SYS.REQ.USER.INFO` passes; SYS-only user: its exact subjects pass, other `$SYS.REQ` subjects and any non-own inbox subscribe refused (10.5) |
| T4 `nats-auth-revoke-propagation` | every configured server ID must acknowledge the exact broadcast JWT before `complete`; a missing or error acknowledgement yields `partial`; live connections on all nodes close and reconnect fails within 5 s; stopped node: `partial`, its resolver file written, refused after restart (T16); partitioned node gives `partial` |
| T5 `nats-auth-expiry-skew` | a live connection past `exp`; reconnect after `exp` refused; Fabric cutoff at `exp − 1 h`; +30 s skew connects; a break-glass user's `exp` is 1 h after its mint (10.1) |
| T6 `nats-auth-org-isolation` | a `playful-test` user sees no Smarty stream in `STREAM.NAMES` and cannot get its messages |
| T7 `nats-auth-reader-proof-bind` | a proof signed by reader A is refused as reader B's proof |
| T8 `nats-auth-single-node` (CI) | `init --local` renders `{nodes: 1, replicas: 1}`; one loopback `nats-server`; the same adapter and the conformance suite over creds; a stream or bucket whose replicas differ from the configuration fails to open, gets no write and is not lowered. Not in CI today (9.1) |
| T9 `nats-auth-1password-down` | a launch with the cached profile mints; revoke reports the outage |
| T10 `nats-auth-fd-core` | FD 3 closed in Pi before tools; absent in children; core limit 0; no seed pattern in logs (count only) |
| T11 `nats-auth-launch-graph` | cases a-l of 2.2: each gets its stated user or refuses with `FABRIC_MESH_STATE_NATS_NO_CREDS` |
| T12 `nats-auth-root-lifecycle` | new root (provision, then agent open), migrated root, migration rerun; agent open of a missing stream refuses |
| T13 `nats-auth-compaction` | agent compaction publishes without purge; the job purges below the snapshot |
| T14 `nats-auth-custody-leader` | `<CUS>` info: `allow_direct: false`, no expiry, configured replicas; ownership reads use `STREAM.MSG.GET`; a CAS write on a stale revision fails; a lapsed liveness key leaves `owner.<agent>` unchanged; maintainer `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused while maintainer mesh-root operations pass (control); a `<CUS>` whose replicas, `allow_direct` or expiry differ from the configuration fails to open, no ownership read decides and no owner write is sent, and nothing is lowered; normal maintainer `STREAM.UPDATE` (and `CREATE`) on `<CUS>` and on `<LIVE>` refused while maintainer mesh-root operations pass (control); after a `<CUS>` change made while an engine is disconnected, its reconnect recheck refuses (10.1) (needs O-2) |
| T15 `nats-auth-key-rotation` | for each scoped key (agent, principal, maintainer, break-glass): add a new key and mint with it; a user signed by the retiring key still connects until its own `exp` (principal 90 days, run with a short test `exp`); the key is removed only after that, or after its users were reissued and revoked; after removal a user signed by the old key is refused and new-key users are unaffected (10.4) |
| T16 `nats-auth-revoke-restart` | one node (`{nodes: 1}`): stop, `revoke` a user, start: the node accepts no client until its resolver account JWT hash matches the latest, and the revoked user's connect is refused before any mesh request; a stale or missing resolver JWT keeps the node from accepting clients; fleet configuration: the same check on each of the 3 node starts (10.3) |

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
  `NATS — break-glass signing key (org smarty-pants)` (10.1),
  `NATS — principal user paul-m5 (org smarty-pants)`. Later, vault Playful Development: the same five account items
  for `org playful`. Profiles (Ryzen 1 only): `nats-mint-smarty-pants`, `nats-maint-smarty-pants`, `nats-admin`,
  `nats-breakglass-smarty-pants` (no cache).
- **Security pass `nats-auth-7504-security`**, by a reviewer who is not the designer or the challenger, before any
  fleet root switches: this note, the encoder, the permission templates, the #907 text change (G-4), and the
  T2, T4, T5, T10 and T11 results.

## fabric-store decisions on revision 1 (10-12 05:20Z)

The design is accepted as the R1 basis. A named security pass on this note runs next, in place of the skipped challenge round 2. No fleet traffic and no 1Password items are created before it passes.

- **D1 accepted.** Children inherit the parent's user in R1; per-child users move to phase C.
- **D2 accepted.** If T2 can't bind exact streams: block the fleet switch, try the whole-name tag, and only then take the org-wide trust trade-off to Paul.
- **D3 accepted.** Self-hosted keys on the user's own disk, as a documented product mode (S1, approver fabric-store; Paul's own words: "scale all the way down to a single machine"). It is never a fleet file class.
- **D4: the encoder is decided at G-7, leaning toward the recommendation.** The own encoder (~80 lines over `@nats-io/nkeys`) wins only if its output matches `nsc describe` and nats-server accepts it, in a test. The `ponytail:` comment says why `@nats-io/jwt` (pre-1.0) is not used. If G-7 finds `@nats-io/jwt` sound and maintained, it is used instead.
  *Amended 10-13 (Amendment B, 10.6, lead):* decided: a small own encoder over `@nats-io/nkeys` 2.0.3, no
  `@nats-io/jwt`; the oracle is a nats-server we host in the test accepting its output; an `nsc` cross-check is
  unconfirmed.
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
  until it is corrected. Agents hold no `STREAM.UPDATE` (1.4); a correction is a maintainer step for mesh-root
  streams and buckets, and a break-glass step for `<CUS>` and `<LIVE>` (O-3, resolved; 10.1).
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
  no `owner.<agent>` write goes out until `<CUS>` is corrected through the break-glass runbook (10.1); the engine
  repeats the check on every reconnect; nothing is changed automatically. Which request a nats.js 3.4.0 KV `get` sends: not measured.
- `<CUS>` and liveness storage are not a mesh root's state stream `<B>`. The root binding, compaction and purge rules
  for `<B>` do not cover them. `<LIVE>` (1.4) names the liveness storage's stream. Agents hold no purge anywhere.
  The maintainer purges mesh-root streams `<B>` (G-2), but every role, maintainer included, has an explicit deny on
  `$JS.API.STREAM.PURGE.<CUS>` and `$JS.API.STREAM.PURGE.<LIVE>` (1.4). The same explicit deny covers
  `$JS.API.STREAM.DELETE.<CUS>` and `$JS.API.STREAM.DELETE.<LIVE>`: deleting the stream would erase the same
  owner records and CAS history. Mesh-root stream lifecycle authority is unchanged. *Amended (10.1):* no normal role
  creates or updates `<CUS>` or `<LIVE>` either; only the break-glass user does.
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
  *Amended (10.1):* for `<CUS>` and `<LIVE>` the correction is the break-glass runbook.
- **O-4 resolved** (amendment review F1): purge and stream deletion on `<CUS>` and `<LIVE>` are explicitly denied
  to every role, maintainer included; mesh-root lifecycle operations stay allowed (1.4, G-4, T2, T14).
- **O-5 resolved by design** (Amendment B, 10.3): `revoke` writes the account JWT durably into each node's local
  resolver dir, and every node start accepts no clients until it matches the latest; T16. A design decision, not a
  measurement; nothing is measured.
- **O-6** The phase of G-9 and T14 relative to the phase 1 / phase 2 split above; also the phase of G-10, T15, T16
  and the break-glass mint (section 10).
- **O-7** Neither amendment is a security pass. Whether `nats-auth-7504-security` also covers sections 9 and 10:
  lead decides.

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

## 10. Amendment B (10-13, #958 round 2): update deny, break-glass, same-UID gap, durable revoke, per-key rotation, SYS-only user

Docs only: nothing in this section is implemented, tested or measured. Basis: the full Astra review
(https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6152830182) and security review
(https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6152848625), and the lead's decisions on them;
10.5 and 10.6 are a lead clarification. No finding beyond those is claimed. It does not change the auth model, the
key custody model, the credential cutoff (C6) or the revocation policy, except as stated. A delta design review and
security review of the frozen text follow; this note does not claim them.

### 10.1 Custody and liveness configuration: normal roles deny, break-glass changes (Astra P1)

- Problem: the maintainer's `$JS.API.STREAM.UPDATE.*` (and `CREATE.*`) reached `<CUS>` and `<LIVE>`. An update could
  add a max age or TTL and expire owner CAS history; the open-time check (G-9) did not stop an already-open engine.
- Normal roles: every normal scoped signing key (agent, principal, maintainer) denies `STREAM.CREATE` and
  `STREAM.UPDATE` on `<CUS>` and `<LIVE>`, besides the kept `PURGE`, `DELETE` and `DIRECT.GET` denies (1.4).
  `CREATE` is denied too, so no normal role can choose the configuration of a missing `<CUS>`/`<LIVE>`. Maintainer
  mesh-root `<B>` lifecycle and compaction are unchanged.
- Break-glass role: a fourth scoped signing key per account, `break-glass`, in the existing org signing custody
  (1Password item, in memory on Ryzen 1 only; no new key file). Allow: `STREAM.INFO`, `STREAM.CREATE` and
  `STREAM.UPDATE` on `<CUS>` and `<LIVE>`, `$JS.API.INFO`, `$SYS.REQ.USER.INFO`, its own inbox. It keeps the
  every-role denies: no `DIRECT.GET` on `<CUS>`, no purge or delete of `<CUS>`/`<LIVE>`. No `MSG.GET`, no mesh
  root, no state subject, no attach (9.3), no agent grant. 1.4 says why the normal-role deny does not block it.
- Mint: only an explicit `fabric nats mint --break-glass --org <org>` run by a person after explicit prior approval
  of that change; never a launcher, job, timer or deployment. `exp` 1 h. The mint logs public metadata only (user
  public key, name, org, signing key public key, `iat`, `exp`, the user JWT's SHA-256, approval reference); never the
  seed or the user JWT. The stop-first runbook below is what covers engines that are already open.
  Creds are held as in 2.3 (memory, no file).
- Minimum runbook for any `<CUS>`/`<LIVE>` configuration change, first creation included:
  1. Get the approval.
  2. Stop every engine that uses that `<CUS>`/`<LIVE>` before the change, and verify the stop with the existing
     #8639 confirmed-termination handoff. No new protocol.
  3. Mint the break-glass user and make the change.
  4. Recheck the configuration with `STREAM.INFO` against the configured values (9.1, 9.2, G-9).
  5. Restart the engines; the break-glass user expires within 1 h.
- Engines check `<CUS>` (`allow_direct: false`, no expiry, configured replicas) at open and again on every
  reconnect, and refuse a mismatch: no ownership decision, no owner write (9.2).
- Tests: T2, T5, T14.

### 10.2 Same-UID seed exposure: accepted security gap (security P1)

- Lead decision: ACCEPTED as a security gap, smarty-dev#TBD-UID (number pending). All fleet agents under one UID on
  one host are one trust domain. That is already true for the file mesh, registries and session files.
- Per-launch creds give network authentication, org isolation and root binding (1.1, 1.4). They do not isolate a
  launch from another same-UID process on the same host, and this note no longer treats them as that.
- Fixes, not designed here: a per-host broker under a separate UID that keeps the seeds; or per-lane Unix users.
- Measure first, cheap: `ptrace_scope` on the fleet hosts and `PR_SET_DUMPABLE=0` in the credential-bearing
  process. To be measured and evaluated; not implemented and not claimed. Neither removes the gap: code in the same
  process still reads the seed, and other same-UID paths remain.
- Unchanged: FD 3 closed before tools, `ulimit -c 0`, no creds in argv, environment, files or logs (2.3, C8, T10).
  Accepting the gap does not remove these guards.

### 10.3 Revocation survives a stopped node (security P1; resolves O-5 by design)

- The latest record: every mint or `revoke` that signs a new account JWT records that signed JWT and its SHA-256 as
  the org's current account claim. Only this issuer record is authoritative. A hash cached on a node, from a peer or
  from an earlier start is not: it could match an old resolver file.
- Persisting: the record and each resolver file are written to a temporary file, fsynced, renamed over the target,
  and the parent directory is fsynced.
- `revoke` writes the new account JWT this way into the local resolver dir of every configured node, also when that
  node's server is down (fleet: over the existing ssh path from Ryzen 1; self-hosted: the local dir). Then the
  `$SYS.REQ.CLAIMS.UPDATE` push and confirmation of 2.4.
- Start check, the same code for `init --local` and for each node of the 3-node configuration: before
  `nats-server` accepts any client, the start wrapper compares the SHA-256 of each org account JWT in the node's
  resolver dir with the issuer's current record. Missing, different or unreadable file, or a latest record that is
  unknown or cannot be obtained: the node accepts no clients (fails closed). Hash, not `iat`: it names exactly one
  JWT; two re-signs in the same second share `iat`.
- No new daemon or transport: the existing issuer, its control files and the runbook supply the record. The exact
  path and how a node start binds to it are an unimplemented gate (O-9).
- No claim rests on peer sync after a restart.
- C7 still holds: `complete` only when every configured server ID acknowledges the exact broadcast JWT; a down
  node, error or missing acknowledgement makes it `partial`, with that node named, and the output also says whether
  its resolver file was written. The change: after a restart that node accepts no stale clients. A host down during `revoke` gets no
  write; its start check fails until the latest JWT is written to it (re-run `revoke` or push).
- Unchanged: the credential cutoff (C6), the revocation policy, the partitioned-node case (2.4, lame duck).
- Not fixed here: the exact path of the issuer's record and the node-start binding to it (when the start cannot get
  it, it fails closed): O-9, an implementation gate. `unverified:` the resolver dir layout (one file per account public key); T16 checks.
- Test T16; T4 adds the stopped-node result.

### 10.4 Rotation per signing key (security P2)

- Each scoped signing key rotates on its own: agent, principal, maintainer, break-glass. Add the new key to the
  account JWT and mint with it; remove the retiring key only after the longest credential it signed expires: agent
  7 days, principal 90 days, maintainer 2 h, break-glass 1 h.
- Alternative: reissue every user the retiring key signed with the new key, revoke the old users, then remove the key.
- Removing a key early cuts every user it signed: that is the leaked-key response (section 5), not a rotation.
- Test T15. This replaces the single 7-day rule in 2.1.

### 10.5 SYS-only user (lead clarification)

- A separate SYS-only user does resolver push and revocation. The `SYS` account identity signs it (1Password, in
  memory on Ryzen 1; self-hosted: the S1 directory). No org role key ever signs it, and the encoder never signs a
  `SYS` user with an org key. No new credential file class.
- Publish allow: **only `$SYS.REQ.CLAIMS.UPDATE`**; no lookup, no `$SYS.REQ.>` or any wildcard publish grant.
  The full resolver validates and saves the exact broadcast JWT, then returns a response naming its server.
  `revoke` correlates successful update acknowledgements to that push and every configured server ID (2.4).
- Source checked 10-13 19:16Z: nats-server v2.14.7
  [update handler](https://github.com/nats-io/nats-server/blob/v2.14.7/server/accounts.go#L4547-L4564) and
  [response](https://github.com/nats-io/nats-server/blob/v2.14.7/server/accounts.go#L4284-L4340).
  The [raw account lookup](https://github.com/nats-io/nats-server/blob/v2.14.7/server/accounts.go#L4566-L4590)
  returns JWT bytes without a server identity. A per-server `CLAIMS.LOOKUP` is not a registered
  [server service](https://github.com/nats-io/nats-server/blob/v2.14.7/server/events.go#L1267-L1352).
  **Known limit, accepted by the lead:** update acknowledgement, not per-server JWT hash or bytes read-back.
  No new read-back mechanism is designed or required here; O-10 is resolved.
- Subscribe allow: only `_INBOX.<own U key>.>`.
- `exp` 1 h. Minted only for an explicit operator `fabric nats revoke`, account push, or `--sys` use; never put in
  a launch environment, never given to Pi, an agent or a job.
- Org users (every role, break-glass included) publish no resolver or admin `$SYS.REQ` subject; the existing
  `$SYS.REQ.USER.INFO` allow stays. T2.

### 10.6 Encoder and required caller inputs (lead clarification)

- D4 decided: a small own encoder over `@nats-io/nkeys` 2.0.3; no `@nats-io/jwt`. Oracle: a nats-server we host in
  the test accepting its output; an `nsc` cross-check is unconfirmed.
- Org account JetStream limits (disk, streams, consumers) are required, caller-configured, finite values; no
  unlimited default.
- `<CUS>` and `<LIVE>` names and configuration are required caller configuration; the names are distinct from each
  other and from every bound mesh stream `<B>`; nothing defaults them. The 1.4 denies are written with those
  configured literal names. This does not resolve O-2: the engine mapping stays open.

### 10.7 Open items after Amendment B

- **O-5 resolved by design** (10.3), not measured.
- Still open: O-1, O-2 (engine mapping and its allow subjects), O-6 (phase, including section 10), O-7
  (security pass scope, including section 10).
- **O-8** Who approves a break-glass mint and where its public audit record is kept. Not invented here.
- **O-9** Exact path of the issuer's current account-JWT record (not a secret) and how each node start binds to it,
  through the existing issuer, control files and runbook. Implementation gate; no new daemon or transport.
- **O-10 resolved** (10.5): SYS publish allow is only `$SYS.REQ.CLAIMS.UPDATE`. Every configured server ID must
  acknowledge the exact broadcast JWT for `complete`; there is no per-server read-back, a known accepted limit.
- smarty-dev#TBD-UID: the lead replaces the issue number.
