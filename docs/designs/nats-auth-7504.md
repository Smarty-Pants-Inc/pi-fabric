DESIGN (design-pair, challenge record: docs/designs/nats-auth-7504-challenge-r1.md; round 2 skipped for time)

# NATS authentication: one login per issued launch and principal, one account per org

smarty-dev#7504 / #8031 R1 (goals G1, G2). Owner: fabric-store. Status: DESIGN DRAFT, revision 1 with Amendment A (10-13, section 9), Amendment B (10-13, section 10) and Amendment C (10-13, section 11); implementation and named security approval remain pending. Sections 9, 10 and 11 are docs only, not implemented or measured.
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
  *Amended (11.1):* renewal of 1 h JWTs comes before the fleet switch (slice 2+); Fabric fails closed at
  `exp − 5 min` without a renewal.
- **C7 (major) ADOPT.** `revoke` says `complete` only when every configured server confirms the new account JWT;
  else `partial`, the stale nodes, exit 2. The stale window is defined per case; T4 adds a stopped node and a
  route-partitioned, client-reachable node. Section 2.4. *Amended (11.1):* the bound is stated on every node,
  partitioned ones included: **1 h**, the user JWT `exp`, enforced by the node itself (T17).
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
- **B2 (security P1, c6152848625) ACCEPTED as a security gap, smarty-dev#11465.** Fleet agents under one UID on
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

## Round 3 answers (Amendment C, 10-13, #958)

Basis: Astra round 2, https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6154244752 (five P1s), and
the lead's decisions on it. Section 11 has the detail. Docs only: nothing is implemented, tested or measured.

- **Partition revocation (P1) ADOPT.** Every user JWT has `exp` at most 1 h after it is signed; the node itself closes a
  connection at `exp` and refuses an expired JWT at connect (nats-server source cited in 11.1), so the bound needs
  no route and no issuer: **1 h**. Live clients renew through a 15 min issuer job; no renewal means fail closed.
  T17 observes a revoked user lose access on a partitioned, client-reachable node. Sections 1.4, 2.4, 11.1.
- **Same-UID seed exposure (P1).** Accepted by org's delegate (chief-of-staff), smarty-dev#11465 comment
  6154729908; recorded in 10.2. No other change.
- **Custody and liveness boundary (P1) ADOPT; O-2 resolved.** `<CUS>` = KV bucket `FABRIC_CUSTODY` (stream
  `KV_FABRIC_CUSTODY`), `<LIVE>` = KV bucket `FABRIC_LEASES` (stream `KV_FABRIC_LEASES`, the lease prototype's
  name); the only `owner.<agent>` writer is a new `custodian` role (slice 2+), each user scoped to the agents
  handed to its engine (#8639); exact allows and denies; a
  token-count proof that these subjects are disjoint from mesh-root subjects. T2 and T14 use the values. 1.4, 11.2.
- **Break-glass approval (P1) ADOPT; O-8 resolved.** Paul approves through a recorded `PAUL:` inbox item; the
  approver cannot mint; the audit record is a GitHub comment on the tracking issue, linked in the mint log; mint
  refuses a missing or mismatched reference. T18. Section 11.3.
- **Claim-record trust anchor (P1) ADOPT; O-9 resolved.** The issuer signs a claim record with the operator signing
  key; each node verifies it against its own operator JWT, so the transport need not be trusted. The start fails
  closed when the issuer is unavailable, with a logged one-start operator override, and it rechecks a record that
  changes during startup. T16 covers both profiles. Section 11.4.

Slice 1 (fs-fence's unmerged slice 1 branch, cited at 48fe892f) keeps its fixed scope: four scoped keys, issuance
metadata returned per credential (no durable record or inventory), a per-key retirement guard over a caller-complete
inventory, POSIX mint checks of the real file mode and UID before secret bytes are written, and win32 refusing to
write seeds (smarty-dev#11482). Everything section 11 needs beyond that is marked **slice 2+**.

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
  (accepted security gap smarty-dev#11465, 10.2).

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
`<LIVE>` = the configured liveness storage's stream (for a KV bucket, its underlying stream) (9.2); it is never a `<B>`.
*Amended (11.2):* the fleet values are `<CUS>` = `KV_FABRIC_CUSTODY` (bucket `FABRIC_CUSTODY`, data subjects
`$KV.FABRIC_CUSTODY.owner.<agent key>`) and `<LIVE>` = `KV_FABRIC_LEASES` (bucket `FABRIC_LEASES`, data subjects
`$KV.FABRIC_LEASES.<host key>`). Every role below also subscribes to `fabric.auth.renew.{{subject()}}` (its own
renewals, 11.1), except break-glass, and the maintainer also publishes `fabric.auth.renew.*` (slice 2+). The
`custodian` role and its grants are in 11.2.

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
| agent, principal, maintainer, break-glass (11.2) | `$KV.FABRIC_CUSTODY.>`, `$KV.FABRIC_LEASES.>` (only the `custodian` role writes custody or liveness keys) |

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
  compaction. Principal: 90 days. Break-glass: 1 h (10.1). *Amended (11.1, slice 2+):* every user JWT has `exp` at
  most 1 h after it is signed (agent, principal, maintainer, custodian; break-glass and the SYS-only user stay 1 h, not
  renewed). Live users renew; Fabric fails closed at `exp − 5 min` without a renewal. This replaces the 7-day,
  90-day and `exp − 1 h` values above. `iat` 60 s in the past; no `nbf`. `SYS` serves only the resolver and the tools, through the SYS-only user
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
  an accepted security gap, not isolation (10.2, smarty-dev#11465).
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
- **Same-UID boundary (accepted security gap, smarty-dev#11465, 10.2).** All code in the Pi process can read the
  seed. A same-UID process can read its memory where `ptrace_scope` allows. This is the same exposure as the
  lane-host model-gateway key in that process. Org's delegate (chief-of-staff) accepts it as a recorded security gap
  (smarty-dev#11465 comment 6154729908), not as isolation; the
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
  `revoke` stays `partial` for it (C7). Partitioned but client-reachable node: *amended (11.1):* bounded at
  **1 h**. The revoked user's last JWT was signed before the revocation and expires within 1 h; the node closes its
  connections at `exp` and refuses it at connect by itself, with no route and no issuer, and the issuer never
  renews a revoked user. Faster manual response: put that node in lame-duck mode over ssh. With R3 streams (Q6) a minority node has no
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
  dirs durably; signing-key rotation follows 10.4 (T15). *Amended (11.4):* the check verifies the signed claim
  record.
- **G-11** (11.1) Every user JWT has `exp` at most 1 h after it is signed, the issuance ledger, the per-org issuer
  lock and the renewal job run,
  and clients renew and fail closed (T17, T19). The break-glass approval checks (11.3, T18) pass before any
  break-glass user is minted for a fleet account.
- **G-12** (11.2, smarty-dev#8639) The #8639 confirmed-termination handoff and its admission proof are in place
  before any fleet root switches and before any engine uses a fleet owner record (`owner.<agent key>` in
  `KV_FABRIC_CUSTODY`). While the prior owner is live or its termination is unconfirmed, no successor engine starts
  and the issuer grants no `custody:` tag. T14's ACL and CAS checks do not prove the handoff. smarty-dev#8639 is
  open: until it lands with its acceptance test, no fleet root switches to NATS. T20.

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
| no renewal by `exp − 5 min` (renewal job stopped, issuer down, partition, revoked) (11.1) | Fabric closes its NATS roots with `FABRIC_MESH_STATE_NATS_CREDS_EXPIRED`; the node closes the connection at `exp` and refuses the expired JWT | restart the renewal job, or reconnect to a majority node; relaunch through the launcher |
| issuer or claim record unavailable at a node start (11.4) | the node starts no client listener (fails closed) | restore the issuer path; or a logged one-start `--claim-override` (11.4) |
| break-glass reference missing or mismatched (11.3) | mint exits 2; no user is minted | get a matching `PAUL:` approval and audit comment |

## 6. Test plan

Cluster tests: Ryzen 2, Intel 1, qs32729, after the tailnet ACL change (docs/mesh-backends.md, "Network
prerequisite"); monitoring from Ryzen 1 on 8222. No test server binds a non-loopback address off the cluster.

| test | checks |
|---|---|
| T1 `nats-auth-cluster-form` | fleet configuration `{nodes: 3, replicas: 3}`: 3 nodes route in operator mode; `/varz` cluster size 3; `/accountz` shows `SYS` and the org; streams and buckets the adapter opens report replicas 3; T3: no creds, connect refused |
| T2 `nats-auth-role-matrix` | values of 11.2: `<CUS>` = `KV_FABRIC_CUSTODY`, `<LIVE>` = `KV_FABRIC_LEASES`. Agent, principal, maintainer and break-glass: publish to `$KV.FABRIC_CUSTODY.owner.<agent key>` and to `$KV.FABRIC_LEASES.<host key>` refused. Custodian (11.2): CAS write to its tagged `$KV.FABRIC_CUSTODY.owner.<agent key>` passes and to another agent's key is refused (a read of that agent's record passes, as documented), its own `$KV.FABRIC_LEASES.<host key>`, `STREAM.INFO` and `STREAM.MSG.GET` on both streams and the lease watch pass; another host's lease key, any `FABRIC_MESH_<hex>.state` or `.snapshot` publish, `CREATE`, `UPDATE`, `PURGE`, `DELETE` and `DIRECT.GET` on both streams refused. Every renewing user: subscribe to its own `fabric.auth.renew.<U key>` passes, another user's refused; only the maintainer publishes `fabric.auth.renew.*`. Then, per role, every verb of 1.3-1.4 passes or gets `Permissions Violation`; agent: other root refused, purge refused, 2 `mesh:` tags both work; every role: `DIRECT.GET` on `<CUS>` refused, `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused (maintainer included), no attach subjects (9.2, 9.3); control: maintainer purge and deletion of a mesh-root `<B>` pass; every normal role: `STREAM.CREATE` and `STREAM.UPDATE` on `<CUS>` and `<LIVE>` refused; break-glass: `STREAM.INFO`, `CREATE` and `UPDATE` on `<CUS>`/`<LIVE>` pass, everything else refused (`DIRECT.GET`, `MSG.GET`, purge, delete, mesh roots, state subjects, attach) (10.1); every org-account user: resolver and admin `$SYS.REQ` subjects refused while `$SYS.REQ.USER.INFO` passes; SYS-only user: its exact subjects pass, other `$SYS.REQ` subjects and any non-own inbox subscribe refused (10.5) |
| T4 `nats-auth-revoke-propagation` | every configured server ID must acknowledge the exact broadcast JWT before `complete`; a missing or error acknowledgement yields `partial`; live connections on all nodes close and reconnect fails within 5 s; stopped node: `partial`, its resolver file written, refused after restart (T16); partitioned node gives `partial`, and the revoked user loses access there within the JWT `exp` (T17) |
| T5 `nats-auth-expiry-skew` | a live connection past `exp` is closed with `User Authentication Expired`; reconnect after `exp` refused; without a renewal Fabric fails closed at `exp − 5 min` (11.1; replaces the `exp − 1 h` cutoff); +30 s skew connects; a break-glass user's `exp` is 1 h after its mint (10.1) |
| T6 `nats-auth-org-isolation` | a `playful-test` user sees no Smarty stream in `STREAM.NAMES` and cannot get its messages |
| T7 `nats-auth-reader-proof-bind` | a proof signed by reader A is refused as reader B's proof |
| T8 `nats-auth-single-node` (CI) | `init --local` renders `{nodes: 1, replicas: 1}`; one loopback `nats-server`; the same adapter and the conformance suite over creds; a stream or bucket whose replicas differ from the configuration fails to open, gets no write and is not lowered. Not in CI today (9.1) |
| T9 `nats-auth-1password-down` | a launch with the cached profile mints; revoke reports the outage |
| T10 `nats-auth-fd-core` | FD 3 closed in Pi before tools; absent in children; core limit 0; no seed pattern in logs (count only) |
| T11 `nats-auth-launch-graph` | cases a-l of 2.2: each gets its stated user or refuses with `FABRIC_MESH_STATE_NATS_NO_CREDS` |
| T12 `nats-auth-root-lifecycle` | new root (provision, then agent open), migrated root, migration rerun; agent open of a missing stream refuses |
| T13 `nats-auth-compaction` | agent compaction publishes without purge; the job purges below the snapshot |
| T14 `nats-auth-custody-leader` | values of 11.2. `STREAM.INFO.KV_FABRIC_CUSTODY`: `allow_direct: false`, no `max_age`, no per-message TTL, configured replicas; `STREAM.INFO.KV_FABRIC_LEASES`: every field of the 11.2 table (file, configured replicas, history 1, `max_age` and `subject_delete_marker_ttl` = the maximum lease, `allow_msg_ttl: true`, `allow_direct: false`, no mirror or source), and a bucket that differs in any of them fails to open; both stream names are what `@nats-io/kv` creates for the buckets; ownership reads use `$JS.API.STREAM.MSG.GET.KV_FABRIC_CUSTODY`; a custodian CAS write to `$KV.FABRIC_CUSTODY.owner.<agent key>` on a stale revision fails; a lapsed `$KV.FABRIC_LEASES.<host key>` leaves `owner.<agent key>` unchanged; a stream whose subjects overlap `$KV.FABRIC_CUSTODY.>` or a `FABRIC_MESH_<hex>` subject is refused at create; maintainer `STREAM.PURGE` and `STREAM.DELETE` on `<CUS>` and on `<LIVE>` refused while maintainer mesh-root operations pass (control); a `<CUS>` whose replicas, `allow_direct` or expiry differ from the configuration fails to open, no ownership read decides and no owner write is sent, and nothing is lowered; normal maintainer `STREAM.UPDATE` (and `CREATE`) on `<CUS>` and on `<LIVE>` refused while maintainer mesh-root operations pass (control); after a `<CUS>` change made while an engine is disconnected, its reconnect recheck refuses (10.1) |
| T15 `nats-auth-key-rotation` | for each scoped key (agent, principal, maintainer, break-glass): add a new key and mint with it; a user signed by the retiring key still connects until its own `exp` (at most 1 h after slice 2+, 11.1; slice 1's longer defaults run with a short test `exp`); the key is removed only after that, or after its users were reissued and revoked; after removal a user signed by the old key is refused and new-key users are unaffected (10.4) |
| T16 `nats-auth-revoke-restart` | one node (`{nodes: 1}`): stop, `revoke` a user, start: the node accepts no client until its resolver account JWT hash matches the latest, and the revoked user's connect is refused before any mesh request; a stale or missing resolver JWT keeps the node from accepting clients; fleet configuration: the same check on each of the 3 node starts (10.3). *Amended (11.4)*, in both profiles (`{nodes: 1}` with the local record file; `{nodes: 3}` with the per-node SSH pull from Ryzen 1): a valid record starts the node; a record with a bad signature, another operator or account, `exp` in the past, or a `seq` below the node's highest seen is refused; the issuer unreachable or the record missing: no client listener; `--claim-override --approval <item> --reason`, with a matching unreserved Paul approval naming this node, account, `seq` and the caller's principal, starts only with the newest verified record and its matching resolver file and logs who, when, why and the record's `seq` and age; a record that changes between the two fetches is checked again, three changes fail closed; `--claim-override` without an approval, with an unanswered or mismatched approval (other node, account, `seq` or operator principal), run from a session whose principal is not the one the approval names, or with an already reserved approval is refused and the node starts no listener; after an override start and a power cut (or reboot) the same approval is still reserved and a second override with it is refused |
| T17 `nats-auth-partition-expiry` | `{nodes: 3, replicas: 3}`, test lifetime 120 s, renewal every 30 s (the same code with short values). An R3 stream has no quorum on a minority node (2.4), so the access observed is what the node itself serves: a test-only R1 stream `FABRIC_MESH_<test hex>` placed on node C (bound to U and W by their `mesh:` tag), and `$SYS.REQ.USER.INFO`, which C answers locally. U, W and X are raw nats.js clients with creds (no Fabric cutoff), so the server's own expiry is what is observed; Fabric's `exp − 5 min` cutoff is T19's. Users U and W connect to node C, X to node A. Cut C's routes (6222) while its client port stays reachable. `revoke U`: `partial`, naming C. Through U's real client on C: before `exp`, U's `USER.INFO`, its publish to that stream's `.state` and its `$JS.API.STREAM.MSG.GET` on it succeed (the window); at U's `exp` plus at most 30 s, C closes U's connection with `User Authentication Expired`, those three calls fail, a reconnect to C with that JWT is refused, and no renewal for U arrives. Control: C also closes W at its `exp` (no renewal crosses the partition); X on A keeps access over 3 renewals; after the routes heal W renews and U stays refused on every node (11.1) |
| T18 `nats-auth-breakglass-approval` | each case exits 2 and mints nothing: no `--approval`; unknown item; item not answered or answered `ignore`; answered by a person other than Paul; answered over 24 h ago; item already reserved; org, stream or config SHA-256 differs (one byte changed in `--config`); mint run from a session whose principal is Paul (a command-line name is ignored); no `--audit`; the audit comment is on another issue, by another author, created before Paul's answer, or lacks the item id or the SHA-256. Two concurrent mints with one item: exactly one mints. A crash injected after the reservation and before the signature: the item stays reserved, no credential exists, a retry is refused. Positive: everything matches: one 1 h user; the mint record holds the comment snapshot (id, author, `created_at`, body SHA-256); after an edit or deletion of that comment the runbook's audit check reports the mismatch (11.3) |
| T19 `nats-auth-renewal` | a renewed JWT has the same `sub`, name, tags and permissions, a later `iat` and `exp`, and `exp − iat` at most 3660 s; a JWT for another key, from a key outside the account, with other permissions, an older `iat` (a replay of an earlier valid JWT), `exp − iat` over 3660 s (slice 1's 7-day form included), or `exp` over 1 h 90 s ahead is ignored; a `revoke` that runs while the renewal job is signing: no JWT for the revoked key is signed after R, and every JWT signed for it is refused by an up node after the push; the client reconnects once in the last 20 min and continues; with the job stopped (test lifetime 10 min), Fabric fails closed at `exp − 5 min` and the node closes at `exp`; a revoked user gets no renewal; a principal's 1Password `jwt` field is updated and its seed field is not touched (11.1) |
| T20 `nats-auth-custody-handoff` | the issuer grants a `custody:<agent key>` tag only to the unique current engine: (a) first placement grants one tag to one engine; (b) a second engine that asks for the same agent while the first is live or its termination is unconfirmed gets no tag, and its CAS write to `$KV.FABRIC_CUSTODY.owner.<agent key>` gets `Permissions Violation`; (c) after the #8639 confirmed-termination handoff the successor gets the tag and the predecessor's custodian user is revoked, so its write is refused; (d) an unconfirmed termination (timeout, unknown) grants nothing. The handoff proof is smarty-dev#8639's own acceptance test (https://github.com/Smarty-Pants-Inc/smarty-dev/issues/8639) (G-12, 11.2) |

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
  lame-duck runbook and Q6; renewal in phase C. *Amended (11.1):* closed. A 1 h user JWT with renewal comes before
  the fleet switch (slice 2+, G-11); the partitioned-node bound is 1 h.

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
- **F5 (host access, 11.4).** One SSH key per NATS node (Ryzen 2, Intel 1, qs32729), forced command
  `fabric nats claim-record --print`, accepted from that node's tailnet address in Ryzen 1's `authorized_keys`;
  the private key 0600 on the node, owner `paul`. It can only print signed, non-secret claim records. Approver: as F1.
- **F6 (not secret, 11.1, 11.4).** `~/.config/systemd/user/fabric-nats-renew@<org>.{service,timer}` (Ryzen 1, 0644);
  claim records `~/.local/share/smarty/nats/claims/<A key>.json` (Ryzen 1) and `claims-seen/` beside each node's
  resolver dir. The renewal job writes the `jwt` field of each principal 1Password item (never the seed field).
  Owner `paul`. Host record only.
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
  *Amended (11.2):* O-2 is resolved: `FABRIC_CUSTODY`/`KV_FABRIC_CUSTODY`, `FABRIC_LEASES`/`KV_FABRIC_LEASES`, the
  `custodian` writer role, its grants and the disjointness proof.

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
  *Resolved (11.2).*
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
  of that change; never a launcher, job, timer or deployment. *Amended (11.3):* the approver is Paul through a
  recorded `PAUL:` inbox item, the approver cannot mint, and mint checks the approval and its GitHub audit comment
  before it signs. `exp` 1 h. The mint logs public metadata only (user
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

- ACCEPTED as a security gap, accepted by org's delegate (chief-of-staff), smarty-dev#11465 comment 6154729908
  (https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11465#issuecomment-6154729908). All fleet agents under one UID on
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
  it, it fails closed): O-9, an implementation gate. *Amended (11.4):* resolved by the signed claim record. `unverified:` the resolver dir layout (one file per account public key); T16 checks.
- Test T16; T4 adds the stopped-node result.

### 10.4 Rotation per signing key (security P2)

- Each scoped signing key rotates on its own: agent, principal, maintainer, break-glass. Add the new key to the
  account JWT and mint with it; remove the retiring key only after the longest credential it signed expires: agent
  7 days, principal 90 days, maintainer 2 h, break-glass 1 h.
- Alternative: reissue every user the retiring key signed with the new key, revoke the old users, then remove the key.
- Removing a key early cuts every user it signed: that is the leaked-key response (section 5), not a rotation.
- Test T15. This replaces the single 7-day rule in 2.1.
- *Amended (11.1, slice 2+):* with every user JWT at most 1 h, the longest credential a key signed expires within
  1 h of its last mint (break-glass and SYS: 1 h). Slice 1's per-key retirement guard needs a caller-complete
  inventory (11.1).

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
- Resolved: the same-UID security-gap issue is smarty-dev#11465.

## 11. Amendment C (10-13, #958 round 3): bounded partition revocation, custody names, break-glass approval, claim-record trust anchor

Docs only: nothing in this section is implemented, tested or measured. Basis: Astra round 2
(https://github.com/Smarty-Pants-Inc/pi-fabric/pull/958#issuecomment-6154244752) and the lead's decisions on it.
Its same-UID finding is answered in 10.2 (org's delegate acceptance), not here. Source reads are cited with their
time; NATS built-ins come first (user JWT `exp`, account `revocations`, KV CAS), bespoke parts are named.

Slice 1 (fs-fence's unmerged slice 1 branch, cited at 48fe892f) keeps its fixed scope: four scoped keys, issuance
metadata returned per credential (U key, role, signer, `issuedAt`, expiry, digest: `src/mesh/nats-auth/mint.ts`,
at 48fe892f ~L375-389, and the CLI output, `cli.ts` ~L232; no durable record or inventory is written), a per-key
retirement guard, POSIX mint checks of the real file mode and UID before secret bytes are written, and win32
refusing to write seeds (smarty-dev#11482). Rotate, inventory, reissue and revoke are later slices. Everything below beyond slice 1 is **slice 2+**, and no fleet root switches before it (G-9, G-10, G-11, G-12).

### 11.1 Revocation bound on every node: a 1 h user JWT (Astra P1, partition; C7)

- **Mechanism (NATS built-in).** Every user JWT the issuer signs has `exp` at most 1 h after the moment it is
  signed: agent, principal, maintainer and custodian (11.2). Break-glass and the SYS-only user were already 1 h and
  are not renewed. For CLI-minted user credentials (SYS-only user included), `iat` is the signing time minus 60 s
  (1.4): slice 1's `src/mesh/nats-auth/jwt.ts` has a default backdate of 60 s (at 48fe892f ~L109; the user encoder
  ~L436, the SYS user encoder ~L457), and `mint.ts` freezes the signing time once and passes no override (~L358-365).
  Operator and account JWTs use `iat` = now (`jwt.ts` ~L322, ~L418). So on the mint path `exp − iat` is at most
  3600 + 60 = 3660 s. That is the mint path's cap, not a wire-token cap: the low-level encoders accept an explicit
  backdate of up to 300 s, and the renewal check (`exp − iat` at most 3660 s, below) refuses any wider token, so it
  fails closed. Every bound below counts from the signing time. **Bound: 1 h**, plus the node's
  clock error. Fleet nodes and the issuer keep clock error at most 30 s (NTP; T5 checks a +30 s skew), so the
  worst case is 1 h 30 s.
- **The node enforces it alone.** Read 10-13 20:30Z from nats-server v2.14.7 and nats-io/jwt v2.8.2 (the version in
  that server's `go.mod`):
  - At connect, an expired JWT is refused. `server/auth.go` L885-896 decodes the user JWT and refuses it when
    `vr.IsBlocking(true)`. `ClaimsData.Validate` adds the time check `claim is expired` (jwt `v2/claims.go`
    L285-292), and `IsBlocking(true)` counts time checks as blocking (`v2/validation.go` L80-91).
  - An open connection is closed at `exp`. After authentication the server calls `c.setExpiration(juc.Claims(),
    validFor)` (`auth.go` L1166), which arms a timer for `exp` (`server/client.go` L1344-1361). When it fires,
    `authExpired` sends `User Authentication Expired` and closes the connection (`client.go` L2507-2510).
  - Both run on the node. They need no route, no resolver push and no issuer, so a partitioned node applies them.
- **Revocation and renewal.** An account revocation covers the user JWTs of that key whose `iat` is at or before the
  revocation time (`server/accounts.go` L4008-4019: `seconds >= issuedAt`). A JWT signed later for the same key is
  not covered. So the issuer never signs a JWT for a revoked key, fenced by one lock: every signature of a user JWT
  (mint, renewal) and every `revoke` run under the issuer's per-org lock (a `flock` on the issuer's ledger file on
  Ryzen 1). Under that lock, renewal re-reads the ledger and the account's `revocations` immediately before it signs
  each JWT; `revoke` removes the user, writes a durable tombstone (U key, R) and records the revocation time R
  before it releases the lock. So every JWT for that key was signed before R (its `iat` is then before R, covered by
  the revocation) or is never signed. T19 runs the revoke-during-renewal race.
- **Stated bound.** A user revoked at time R holds at most a JWT signed before R, so its `exp` is at most R + 1 h.
  On every node (up, stopped, or partitioned and still client-reachable), that user's connections close and its
  connects fail by R + 1 h plus the node's clock error (at most 30 s). An up node closes them on the account push, target 5 s (2.4);
  1 h is the worst case.
- **Refresh path (slice 2+).**
  - The issuer keeps a ledger of live users: public key, role, claims, host, session. Mint adds to it; `revoke` and
    a normal session end remove from it.
  - A maintainer job on Ryzen 1, `fabric-nats-renew@<org>.timer`, runs every 15 min. It mints its own 1 h maintainer
    user. For each ledger user that is not revoked it signs a new user JWT with the same public key, name, tags and
    permissions, and a new `iat`, `exp` and `jti`. It needs no seed. It publishes that JWT on
    `fabric.auth.renew.<U key>`.
  - Each renewing user subscribes only to its own `fabric.auth.renew.{{subject()}}` (1.4). The JWT is not a secret:
    without the seed's signature over the server nonce it authenticates nothing.
  - Fabric keeps a received JWT only if it has its own `sub`, an `iss` among its account's scoped keys, the same
    name, tags and permissions, an `iat` later than the JWT it holds, `exp − iat` at most 3660 s, and `exp` at most
    1 h 90 s after the client's own clock; it drops anything else. An older JWT replayed on the renewal subject, or
    a long one (slice 1's 7-day and 90-day forms included), is refused. If a reconnect with the new JWT fails
    authentication, Fabric keeps the previous JWT until its own `exp`. The client reads the JWT through a
    function: nats.js 3.4.0 `jwtAuthenticator(ajwt: string | (() => string), seed)` calls it on every connect and
    reconnect (`@nats-io/nats-core/lib/authenticator.js` L91-97, read in this tree's `node_modules`).
  - The server keeps the old JWT's timer on an open connection. When the connection's JWT has less than 20 min left
    and a newer one is held, Fabric calls `nc.reconnect()` (nats.js 3.4.0 `lib/core.d.ts` L425-440), once per JWT.
  - Children, actors and the resident host share the parent's connection or pipe (2.2), so they share its renewals.
  - Principals: the job also writes each principal's current JWT into the `jwt` field of that principal's
    1Password item (never the seed field), so `op run` starts with a JWT under 15 min old. A live principal client
    renews like an agent.
- **Refresh failure fails closed.** No newer JWT by `exp − 5 min` (job stopped, issuer down, Ryzen 1 cut off, a
  partition between the client's node and the cluster, or the user revoked): Fabric closes its NATS roots with
  `FABRIC_MESH_STATE_NATS_CREDS_EXPIRED` ("credentials not renewed; relaunch through the launcher"). The node closes
  the connection at `exp` and refuses that JWT afterwards. There is no fallback credential. Cost: in a partition
  longer than 1 h, the unrevoked users of the partitioned node lose it too; they can reconnect to a majority node in
  their server list, where renewals arrive.
- **Replaced.** The C6 cutoff at `exp − 1 h` of a 7-day JWT and the 90-day principal JWT (1.4, D6). Slice 1's
  lifetimes (the TTL table in `src/mesh/nats-auth/mint.ts`, at 48fe892f ~L81-89: agent 7 days, principal 90 days,
  maintainer compaction 1 h, maintainer migration 2 h, break-glass 1 h, SYS 1 h) stay for slice 1's test users. The 1 h default, the ledger, the renewal
  job and client renewal are slice 2+ (G-11).
- **Rotation (10.4).** With every renewed JWT at most 1 h old, a retiring key's longest credential ends within 1 h
  of its last signature. Slice 1's per-key retirement guard decides from a caller-complete inventory
  (`src/mesh/nats-auth/retirement.ts`, at 48fe892f ~L7-14), and slice 1 writes none; the issuance ledger (slice 2+,
  above) supplies it.
- Lame duck (2.4) stays a faster manual containment; it is no longer the bound. Tests T5, T17, T19.
- C7: ADOPT, bound stated: 1 h.

### 11.2 Custody and liveness names, the single CAS writer, subject isolation (Astra P1, custody; resolves O-2)

Names taken from code (none invented where code has one):

- Mesh roots: stream `FABRIC_MESH_<32 hex>` (`natsStreamName`, `src/mesh/state-backend-nats.ts:1001-1004`), data
  subjects `<stream>.state` and `<stream>.snapshot` (`:1041`).
- Liveness: the NATS KV lease prototype on `origin/feat/nats-kv-leases` @7bbd74e4 (`src/topology/nats-kv-leases.ts`,
  `docs/nats-kv-leases.md`; owner fabric-v2; not wired). The bucket is a configured option; its documented value is
  `FABRIC_LEASES`. The key is the SHA-256 hex of the host id (`keyFor`, `:37`); writes publish to
  `$KV.<bucket>.<key>` (`:210`); reads go to the leader (`allow_direct: false`).
- Custody `owner.<agent>`: no NATS code exists yet (the #8031 engine). Its names follow the same KV pattern.

Fleet values (required configuration, 10.6; the code still sets no default):

| | `<CUS>` (custody) | `<LIVE>` (liveness) |
|---|---|---|
| KV bucket | `FABRIC_CUSTODY` | `FABRIC_LEASES` |
| stream | `KV_FABRIC_CUSTODY` | `KV_FABRIC_LEASES` |
| stream subjects | `$KV.FABRIC_CUSTODY.>` | `$KV.FABRIC_LEASES.>` |
| data subject | `$KV.FABRIC_CUSTODY.owner.<agent key>`, `<agent key>` = SHA-256 hex of the agent id | `$KV.FABRIC_LEASES.<host key>`, `<host key>` = SHA-256 hex of the host id |
| configuration | file, replicas from `bus`, no `max_age`, no per-message TTL, `allow_direct: false`, no mirror or source (9.2) | as the prototype requires: file, replicas from `bus`, history 1, `max_age` = the maximum lease, `allow_msg_ttl: true`, `subject_delete_marker_ttl` = the maximum lease, `allow_direct: false`, no mirror or source (`docs/nats-kv-leases.md`) |

Per org account. `unverified:` that `@nats-io/kv` 3.4.0 names a bucket's stream `KV_<bucket>`; T14 reads the names.

**Single CAS writer: the `custodian` role (slice 2+).** A fifth scoped signing key per account, added with the
10.4 add-then-mint step; slice 1's four keys do not change. The issuer mints one custodian user per engine launch:
name `custodian/<host>/<engine-session>`, tag `lease:<host key>`, and one tag `custody:<agent key>` per agent that
engine holds; 1 h with renewal (11.1). A custodian user writes only its tagged `owner.<agent key>` keys, always by
CAS on the last revision (KV update with the expected last subject sequence). The issuer adds a `custody:` tag for
an agent only to a newly minted custodian user (renewal keeps tags unchanged, 11.1), and it issues and transfers
the tag only through the proven #8639 confirmed-termination handoff (G-12, T20). At the agent's first placement it
grants the tag only to the unique current engine, after it confirms that no other engine holds or held a live tag
for that agent. On a transfer it grants the tag only after it has recorded the handoff from the previous engine, and
in that handoff it revokes the previous engine's custodian user if it still holds that tag. While the previous
owner is live or its termination is unconfirmed, it grants nothing. So NATS refuses a write by an engine that was
never handed the agent. No engine uses a fleet owner record before G-12 passes. Agents, principals, maintainers and
break-glass users write no custody or liveness key (1.4 deny).

Reads are not scoped per agent: `$JS.API.STREAM.MSG.GET.<stream>` carries the wanted subject in its request body,
so a subject permission cannot narrow it. Every custodian (and every principal and maintainer, 1.4) can read every
`owner.<agent key>` record of its org. The record holds `{engine, epoch, state}`, no secret; a read revision gives
no write without the tag.

| role | publish allow | subscribe allow |
|---|---|---|
| `custodian` (slice 2+) | `$KV.FABRIC_CUSTODY.owner.{{tag(custody)}}` (one subject per `custody:` tag), `$KV.FABRIC_LEASES.{{tag(lease)}}`, `$JS.API.INFO`, `$JS.API.STREAM.INFO.KV_FABRIC_CUSTODY`, `$JS.API.STREAM.INFO.KV_FABRIC_LEASES`, `$JS.API.STREAM.MSG.GET.KV_FABRIC_CUSTODY`, `$JS.API.STREAM.MSG.GET.KV_FABRIC_LEASES`, the 4 consumer verbs `.KV_FABRIC_LEASES.>` (lease watch), `$SYS.REQ.USER.INFO` | `_INBOX.{{subject()}}.>`, `fabric.auth.renew.{{subject()}}` |

- Custodian denies: the every-role and every-normal-role denies of 1.4 with the values above (`DIRECT.GET` on
  `KV_FABRIC_CUSTODY`; `CREATE`, `UPDATE`, `PURGE`, `DELETE` on both streams). It has no mesh-root grant.
- Other roles: explicit publish deny on `$KV.FABRIC_CUSTODY.>` and `$KV.FABRIC_LEASES.>` (1.4), besides the
  existing denies.
- `{{tag(lease)}}` and `{{tag(custody)}}` are whole tokens here, so they need no substring expansion (1.4's open
  template question does not apply); a user with two `custody:` tags gets two subjects, as for `mesh:` (1.4). T2
  checks both. If T2 fails, NATS cannot scope custody writes and G-9 blocks (D2 applies).

**Isolation proof.**

1. A NATS `*` matches exactly one token and `>` matches one or more trailing tokens (nats.docs
   `nats-concepts/subjects.md`, 1.2).
2. Mesh-root data subjects have exactly 2 tokens, the first `FABRIC_MESH_<32 hex>`. Custody data subjects have 4
   tokens and liveness data subjects 3, the first `$KV`. Renewal subjects have 4 tokens, the first `fabric`.
3. The grants that reach mesh-root data are the agent's `<B>.state` and `<B>.snapshot` (`<B>` =
   `FABRIC_MESH_{{tag(mesh)}}`) and the maintainer's `*.state` and `*.snapshot`: all exactly 2 tokens. A 2-token
   pattern matches no 3- or 4-token subject, so no mesh grant reaches a custody, liveness or renewal subject.
4. The custodian's data grants start with `$KV.FABRIC_CUSTODY.` or `$KV.FABRIC_LEASES.`, so they match no subject
   whose first token is `FABRIC_MESH_<hex>`.
5. JS API subjects carry the stream name as one token. `<B>` always starts with `FABRIC_MESH_`, so an agent's
   `$JS.API...<B>` grant never names `KV_FABRIC_CUSTODY` or `KV_FABRIC_LEASES`. The custodian's JS API grants name
   only those two streams. The maintainer's and principal's `.*` JS API grants do reach them: the explicit 1.4
   denies remove `CREATE`, `UPDATE`, `PURGE`, `DELETE` and `DIRECT.GET`, leaving `INFO` and `MSG.GET` reads.
6. Stream capture: the subject sets `$KV.FABRIC_CUSTODY.>`, `$KV.FABRIC_LEASES.>` and
   `FABRIC_MESH_<hex>.{state,snapshot}` are disjoint by (2). `unverified:` that JetStream refuses a stream whose
   subjects overlap another stream's in the account; T14 checks it.

### 11.3 Break-glass approval and audit (Astra P1, break-glass; resolves O-8)

- **Approver: the product owner, Paul, only.** The approval is an item in Paul's inbox (to `paul`, kind `risk`)
  whose title starts `PAUL: break-glass <org> <stream> <config sha256>`. It names the exact target: org, stream
  (`KV_FABRIC_CUSTODY` or `KV_FABRIC_LEASES`) and the SHA-256 of the requested stream configuration file. Paul
  answers `accept`. The item id is the approval reference.
- **Separation of duties.** The minter is the Fabric principal of the session that runs mint, read from that
  session's participant record (owner principal and `session:<id>`), never from a command-line name; the user name
  `break-glass/<org>/<principal>/<run-id>` (1.1) and the mint record carry it. Mint refuses when that principal is
  the approver (Paul), so Paul approves and an agent session mints. Residual: on one UID a same-UID process can
  impersonate another session; that is the accepted same-UID trust domain of 10.2 (smarty-dev#11465), and the
  approval itself stays Paul's own inbox answer.
- **Durable audit record.** Before the mint, a comment on the tracking issue the item names (default
  smarty-dev#7504), posted by the fleet write App, with the item id, org, stream, config SHA-256, minter and planned
  window. Mint takes `--approval <item id> --audit <comment URL> --config <file>`. Its append-only mint record
  (slice 2+: slice 1 returns issuance metadata and writes no durable record; one JSON line per mint, fsynced, in
  the issuer's state dir) keeps a snapshot of the audit comment: repository, issue,
  comment id, author, `created_at`, and the SHA-256 of the comment body as read at mint, with the item id, target,
  config SHA-256, minter and mint time. A later edit or deletion of the comment no longer matches that snapshot,
  so the audit check of the runbook detects it. After runbook step 4 (recheck) the operator posts the
  `STREAM.INFO` result as a reply on the same issue.
- **Mint refuses** (exit 2, nothing signed) unless all of these hold:
  1. The item exists, its recipient is `paul`, it is answered `accept` by Paul (`unverified:` that the inbox
     records who answered; T18 checks), and the answer is under 24 h old.
  2. The item's org, stream and config SHA-256 equal `--org`, the target stream and the SHA-256 of `--config`.
  3. The session principal (from its participant record, never a command-line name) is not the approver.
  4. `--audit` resolves, through a GitHub REST read with the fleet App, to a comment on the named issue, by the
     fleet write App, created after Paul's answer and before this mint, that carries the same item id and SHA-256.
  5. (slice 2+) The approval is reserved atomically before anything is signed: mint creates `consumed/<item id>` in the
     issuer's state dir with an exclusive create (`O_EXCL`), fsyncs it and its directory, and refuses when it
     exists. Two concurrent mints cannot both pass. A crash after the reservation leaves the item consumed and no
     credential (the creds live only in that process's memory, 2.3), so it fails closed and a new approval is
     needed.
  Then it signs, writes the mint record and gives the creds to the operator's process.
- The break-glass permissions (10.1) and the stop, recheck and restart runbook stay; the approval is runbook step 1.
- These checks are slice 2+. Until they land, no break-glass user is minted for a fleet account (G-11). Test T18.

### 11.4 The claim record: trust anchor of the node start check (Astra P1, trust anchor; resolves O-9)

- **Record.** Per org account: `{format: 1, operator: <O key>, account: <A key>, seq: <n>, jwtSha256: <hex>,
  iat: <s>, exp: <s>}`, with a detached nkey signature by the operator signing key that signs that account's JWTs.
  `seq` increases by one with every account JWT the issuer signs; `exp` = `iat` + 1 h.
- **Owner and writer.** fabric-store owns the format and the code. Only the issuer writes it: on the fleet, Ryzen 1
  `fabric nats` under the `nats-admin` profile; self-hosted, the local issuer (S1). Ryzen 1 path:
  `~/.local/share/smarty/nats/claims/<A key>.json`; self-hosted: `~/.config/fabric/nats/claims/<A key>.json`.
  Written as in 10.3 (temp file, fsync, rename, directory fsync). Not a secret.
- **Signing.** Every account JWT signed (mint, `revoke`, push) gets a new record with `seq + 1`. Each renewal job run
  (11.1) re-signs the current record with a new `iat` and `exp`, so a current record is never more than 15 min old.
- **Integrity: the read need not be trusted.** The node start wrapper verifies the signature against the signing
  keys of the operator JWT in the node's own server config (F3, already on every node), then checks `operator`,
  `account`, `exp` later than now, and `seq` at least the highest seq this node verified before. That highest seq is
  kept in `claims-seen/<A key>` beside the resolver dir, written durably. A forged, altered, expired or rolled-back
  record fails. Then the 10.3 check: the SHA-256 of the node's resolver account JWT equals `jwtSha256`.
- **Transport.** Fleet: the start wrapper pulls the record from Ryzen 1 over SSH with that node's forced-command key
  (`fabric nats claim-record --print`, F5). Self-hosted single node: a local file read. A hostile or stale transport
  can only withhold the record (the start fails closed) or replay a signed record under 1 h old with a seq at least
  the node's last. Such a replay can admit only a user revoked within the last hour, whose JWT 11.1 already
  expires within 1 h.
- **Issuer unavailable at start: fail closed.** The node starts no client listener. Explicit operator override, for
  one start: `fabric nats node start --claim-override --approval <item id> --reason "<text>"`. It needs Paul's
  approval, checked as in 11.3 (an inbox item `PAUL: claim-override <node> <account> <seq> <operator principal>`,
  answered `accept` by Paul within 24 h, reserved once with an exclusive create in the node's `claims-seen/` dir;
  the reservation file and that directory are fsynced before the server starts, so a power loss cannot make it
  reusable).
  The caller's session principal (as in 11.3) must be the operator the approval names, and it is not Paul.
  Same-UID impersonation is the accepted 10.2 residual. It starts only with the
  node's newest verified record (expired allowed) and the resolver file that matches it. Before it starts the server
  it logs the approval, the session principal that ran it, the reason, and the record's seq and age to the node
  journal; the runbook posts the same on smarty-dev#7504.
  Exposure: users revoked while their current JWT is still valid, at most 1 h (11.1).
- **Changes during startup.** The wrapper fetches and verifies the record (seq N) and checks the resolver file, then
  fetches again just before it executes `nats-server`. If the seq moved, it repeats the whole check against the new
  record (`revoke` writes the new resolver file into this node's dir first, 10.3). Three moves in a row fail closed.
  A change after `nats-server` starts is the up-node case (2.4): the `$SYS.REQ.CLAIMS.UPDATE` push reaches it, and
  `revoke` waits for its acknowledgement.
- Slice 2+, for every start in both profiles (G-10). Slice 1's embedded single node serves only slice 1's test
  users and is not a T16 profile; from slice 2+, `init --local` and each fleet node start through this check, and
  T16 tests exactly those starts.

### 11.5 Open items after Amendment C

- Resolved: O-2 (11.2), O-8 (11.3), O-9 (11.4); C7 bound stated (11.1). The same-UID gap is accepted by org's
  delegate (10.2).
- Still open: O-1; O-6 (phases, now including section 11's slice 2+ items); O-7 (security pass scope, now including
  section 11).
- smarty-dev#8639 (confirmed-termination handoff) is open and blocks G-12 and the fleet switch (T20).
- `unverified:` items, each with its test: `@nats-io/kv` stream names and KV `get` with `allow_direct: false`
  (T14); a non-inbox `{{subject()}}` and the whole-token `{{tag(lease)}}` (T2); the inbox records who answered (T18);
  JetStream refuses overlapping stream subjects (T14); SSH from each node to Ryzen 1 (T16).
