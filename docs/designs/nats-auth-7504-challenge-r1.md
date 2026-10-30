# NATS authentication — challenger pass, round 1

Inputs read: `docs/designs/nats-auth-7504.md`; main HEAD `44da0a80d38c72d720fd39fe3e6921b768659818`; `../fs-nats2` branch `feat/sd-9690-reader-proof` HEAD `6fadd911c8f2db99864f203d110c3fff5c78613d`; `docs/mesh-backends.md`; and the Smarty credentials skill. This is a design challenge, not a security-pass approval.

**Counts:** 3 blocker, 6 major, 0 minor.

## Challenges

### C1 — blocker — Phase A conflicts with the existing #907 deployment gate

**Counterexample:** A compromised agent can invoke `STREAM.PURGE` on any stream in its account, not only its own root. The design grants every agent both `*.state`/`*.snapshot` publication and `$JS.API.STREAM.PURGE.*` in phase A. That directly contradicts the required #907 NATS configuration: only the fleet user may publish to state subjects or purge; the adapter's deployment requirements say those rules are mandatory. `#9834` does not make this safe: the design itself says a modified process holding the seed can still sign a proof. Purge is a destructive account-wide verb, not a proof-bound operation.

**Evidence:** `docs/designs/nats-auth-7504.md:63-76,154-164`; `docs/mesh-backends.md:64-96`; `b:src/mesh/state-backend-nats.ts:684` calls purge for compaction; `b:src/mesh/state-backend-nats.ts:73` defines purge as removing every message below a sequence.

**Suggested fix:** Do not deploy phase A under the current #907 gate. Land phase B before enabling the fleet backend, and replace/update the #907 deployment check with an explicit authenticated-role matrix that proves each role's real authority. If agent publication is retained, the gate must explicitly authorize it and test cross-root effects; editing the prose alone is not enough. Keep purge out of agent JWTs.

### C2 — blocker — The Ryzen 1 mint-and-SSH path does not cover the real launch graph

**Counterexample:** The design describes a Ryzen 1 launcher that streams creds over SSH to a remote lane. Fabric also launches ordinary local `process` children when no external launcher applies; actor/nested/inherited/durable runs stay local; durable actors are owned and restarted by a resident host; and local Pi reload/resume does not necessarily re-run the original remote launcher. A lane-host task agent can spawn its own task locally. The mesh bridge is another uncovered service path: it runs under a service manager on the hub, its remote side is a forced SSH command, and the service manager restarts it from a cursor file; neither side is a Pi launch through the described mint wrapper, and the role table has no bridge identity. None of those paths has a Ryzen 1 mint step, and the work hosts have no 1Password. The proposed fail-closed behavior therefore makes valid child/resident/bridge/restart paths unable to open NATS, while any bypass that reuses a host credential breaks the one-user-per-agent goal.

**Evidence:** `docs/designs/nats-auth-7504.md:90-98`; `docs/agents.md:125-153` (remote placement is only a Main task path; several run classes stay local); `docs/agents.md:494-529` (durable actors and resident ownership/restart); `docs/agents.md:894-896` (local child transport); `docs/mesh-bridge.md:164-171,214-216` (service-manager bridge, forced-SSH remote end, restart). These are distinct from the design's single SSH delivery path.

**Suggested fix:** Before implementation, enumerate each supported owner/transport/start/restart path and route it through an authorized issuer delivery step, with a fail-closed test for each. If that cannot be done without a daemon or a new host secret, explicitly narrow the rollout so those paths cannot select NATS; do not imply all agents are covered.

### C3 — blocker — One-machine mode stores signing authority outside the mandated vault boundary

**Counterexample:** `init --local` writes the operator, SYS, account, and role signing keys under `~/.config/fabric/nats/`. That is a new on-host signing-secret class and is not the stated custody model (signing material in Smarty Development on Ryzen 1 only). If the machine also runs agents as the same Unix user, those processes/tools can read the operator and mint arbitrary identities; mode 0600 does not isolate same-UID code. “Only configuration differs” is therefore false for custody and authority.

**Evidence:** `docs/designs/nats-auth-7504.md:110-121`; credential rule `smarty-credentials/SKILL.md`, sections “Lane-host model-gateway client keys” and “Never expose or restore” (additional lane secret files require approval; the existing exception is narrowly scoped).

**Suggested fix:** Treat local self-hosting as a separately approved trust mode with an explicit signer-custody exception and threat boundary, or use the same approved issuer/custody path. Do not present locally generated operator files as satisfying the fleet constraint by default.

### C4 — major — “Per-org isolation” also grants cross-root stream access inside that org

**Counterexample:** The adapter has one stream per canonical mesh root, but every user in an org receives token-wide `STREAM.INFO.*`, `STREAM.MSG.GET.*`, consumer API wildcards, and (phase A) `PURGE.*`, plus `*.state` publication. A user that knows another root's stream name can read, publish to, create consumers on, and purge that root's stream. This is broader than the documented caveat that agents of one mesh root share that root's stream. If roots have different project/access boundaries, the account is not the only relevant boundary.

**Evidence:** `b:src/mesh/state-backend-nats.ts:1057-1063` hashes the canonical root into a distinct one-token stream name; `docs/designs/nats-auth-7504.md:27,63-65`; local NATS 2.14.7 ACL probe (fake password auth, exact role permission patterns): two different `FABRIC_MESH_*` stream-info requests each got JetStream API `404` rather than a permissions violation; publish to the other name's `.state` and `.snapshot` subjects succeeded; `STREAM.PURGE.<other-name>` returned API `404` rather than a permission violation. No real foreign stream was present, so the probe confirms authorization matching, not deletion of stored data.

**Suggested fix:** Either state and obtain acceptance that all users of one org are trusted for *all* org roots, or generate exact per-user stream permissions from a trusted root tag when minting (stream names are single NATS tokens, so prefix ACLs do not help). If exact stream binding cannot be made reliable, this is a trust-boundary trade-off, not “moot” merely because roots share an account.

### C5 — major — The permissions only work with the branch's migrated-stream open contract

**Counterexample:** The role tables omit stream create/update for agents, correctly only if agent `open` uses the branch's `create: false` path and migration/maintainer creates the stream first. Main's current open path calls `ensureStream`, which can create or update; a user with the proposed agent permissions then fails to open a fresh root. Granting create/update to all agents would widen their authority and undermine the separation.

**Evidence:** `docs/designs/nats-auth-7504.md:56`; main's current behavior is called out in the design; `b:src/mesh/state-backend-nats.ts:482-485` refuses missing streams when `create: false`; `b:src/mesh/state-backend-nats-client.ts:73-84` implements `ensureStream` creation/update; `b:src/mesh/backend-migration-nats.ts:293,372-374` creates the staged stream.

**Suggested fix:** Make integration order a hard acceptance condition: merge/use the migration-created stream contract before these ACLs, then test new root, migrated root, and rerun paths. Keep create/update/delete only on the migration maintainer role.

### C6 — major — Seven-day expiry has no lifecycle path for long-lived processes

**Counterexample:** A Pi/resident/actor can outlive seven days, or lose its NATS route after expiry. There is no renewal service; a reconnect then presents an expired user JWT and is rejected. A still-live client may instead remain connected beyond `exp` unless NATS closes it; the design's claim that the connection is dropped at `exp` is only a planned T5, not evidence. A process reload also needs a fresh credential source, not just an old FD that was consumed at initial launch.

**Evidence:** `docs/designs/nats-auth-7504.md:81-83,97-98,146-148`; `docs/agents.md:523-529` durable workers/actors continue after the initiating TUI exits and are resident-owned.

**Suggested fix:** Choose a supported lifecycle: enforce and test a maximum process/session lifetime shorter than `exp` and always relaunch through issuance, or add a bounded renewal/restart protocol that works for resident and remote processes. Test expiration of a live connection and a disconnect/reconnect after expiry; do not infer either from JWT validation at initial connect.

### C7 — major — Revocation is not yet shown complete under partial cluster failure

**Counterexample:** The proposal claims all live connections close within five seconds when all nodes are up, but calls this behavior `unverified`; T4 is a plan, not a result. During a partition or when a node is down, the push cannot get three replies. The design then permits up to the resolver interval (two minutes) after reconnect before convergence. A stale resolver may therefore continue accepting a revoked user during that window, and a connected isolated server may continue serving it. The current text does not define the behavior if the server is reachable by clients but not by the claims-update path.

**Evidence:** `docs/designs/nats-auth-7504.md:101-108,147`; the test table contains no result. No JWT resolver/revocation experiment was run in this pass.

**Suggested fix:** Report revocation as partial until every live server has acknowledged the new account JWT. Test live connection closure, reconnect denial, a stopped node with persistent resolver state, and a route-partitioned but client-reachable node. Define a maximum stale-authorization window and make the status/output distinguish “all revoked” from “some nodes updated.”

### C8 — major — “No file on disk” omits crash-dump and same-process exposure

**Counterexample:** Sending `.creds` over stdin and closing FD 3 avoids an intentional credential file and does not put the bytes in `FABRIC_NATS_CREDS_FD` (which is only the descriptor number). But `credsAuthenticator` parses the seed and keeps auth callbacks over it for reconnects; a core dump can persist process memory. Any tool/code running in that Pi process can also read FD 3 until it is closed. The design has not shown coredump policy, descriptor inheritance/closure timing, or what same-UID tools can inspect. Therefore “no file on disk” is an unproven operational claim, not a property of pipe transport alone.

**Evidence:** `docs/designs/nats-auth-7504.md:93-96`; `node_modules/@nats-io/nats-core/lib/authenticator.js:106-130` parses the creds and returns JWT/NKey callbacks; credentials skill, “Never expose or restore.”

**Suggested fix:** Keep secret bytes out of argv/env/logs; mark the descriptor close-on-exec and close it before extensions/tools start; set and verify a no-core-dump policy for the credential-bearing process and document the same-UID threat boundary. Probe the actual launcher and Pi process table/FD lifecycle without printing credential bytes.

### C9 — major — Prior-art and license claims are not verified by this pass

**Counterexample:** My permitted local search covered the design, repo docs/source, and installed NATS client packages. It found `@nats-io/jetstream` and `@nats-io/transport-node` 3.4.0 marked Apache-2.0, plus `@nats-io/nkeys` 2.0.3. It did not find local sources or pinned artifacts for `nsc`, the NATS Helm chart, NACK, or the auth-callout examples; the design's citations and license labels therefore remain **unverified** here. The design also proposes a bespoke JWT encoder (~80 lines) while citing `nsc` as an oracle, but does not compare a maintained official JWT encoder or prove that `nsc` cannot be used safely for in-memory minting. Per-host users with a session tag are simpler but are not equally safe for individual compromise/revocation: one stolen host credential can impersonate all sessions on that host. Auth callout remains out under the no-external-service constraint.

**Evidence:** `docs/designs/nats-auth-7504.md:88-92,176-190`; installed package metadata: `../fs-nats2/node_modules/@nats-io/jetstream/package.json`, `transport-node/package.json`, and `nkeys/package.json`. No external host was contacted, as required.

**Suggested fix:** Before choosing or licensing the encoder/control tooling, pin and inspect the upstream source/license for each cited candidate (or explicitly mark it unverified in the decision). Prefer a maintained official JWT implementation or the official nsc path if it can meet the no-disk/short-lived issuance boundary; otherwise record why it cannot. Do not treat a host-level user as equivalent to per-agent revocation.

## ACL call-path check

For the cited branch and nats.js 3.4.0 on the supported server floor, I found no missing JetStream operation in the proposed `agent`/`principal` read path **provided** the migrated-stream contract in C5 is in place:

- `jetstreamManager()` requests `$JS.API.INFO`; adapter asks `$SYS.REQ.USER.INFO`; both appear in the ACL.
- `streamInfo` uses `$JS.API.STREAM.INFO.<S>`; `getMessage` and `lastBySubject` use `$JS.API.STREAM.MSG.GET.<S>`. The adapter does not use `$JS.API.DIRECT.GET`; `allow_direct` is not required for these calls.
- `js.consumers.get(stream, options)` selects the ordered **pull** consumer path. nats.js creates a named consumer using `$JS.API.CONSUMER.CREATE.<S>.<name>`, pulls via `$JS.API.CONSUMER.MSG.NEXT.<S>.<name>`, queries info on recovery, and deletes it on stop. Replies/idle heartbeats use the client's reply inbox; pull mode does not need `$JS.FC` or `$JS.ACK`. The proposed own-user inbox prefix must be set on the NATS connection as designed.
- Local NATS 2.14.7 probe: named consumer-create subject returned API `404` (permission matched; stream absent); the one-token unnamed `CONSUMER.CREATE.<S>` request got a permission/request error under the table's two-token `CREATE.*.*` allow. This is compatible with the branch because nats.js 3.4.0 uses the new named create API, supported since 2.9, and the adapter floor is 2.11. Recheck if the client/server floor changes.
- Main's existing stream-create/update behavior is not covered by these agent permissions; see C5. Phase A's `PURGE.*` grant and cross-root breadth are not least privilege; see C1/C4.

Probe output (throwaway static-password ACL, not JWT/resolver testing):

```text
root-A stream-info=404
root-B stream-info=404
state=published
snapshot=published
purge-other-stream=API-response:404
consumer-create-named=API-response:404
consumer-create-unnamed=transport-or-permission-error:RequestError
```

## Probe cleanup handoff

The local server probe was started as PID `3626203`. I accidentally omitted a loopback-only `host` setting, so NATS logged a bind on `0.0.0.0:43934` (monitor `0.0.0.0:42050`). I only connected to `127.0.0.1`; I did not contact another host. The fleet shell guard refused my exact `kill 3626203` attempt because this process has no Herdr-pane ancestry, and also refused the server's `--signal` control form. (Resolved: fabric-store stopped it at 05:04Z; tracked on smarty-dev#8266.) Its config and data are under `/srv/scratch/paul/smarty-pi-tmp/pi-d8dbb921aab6/p.L71mmv/` and use only fake probe credentials.

## Constraints added after round 1 (10-13, #958)

This record of round 1 is unchanged above. After it, the design note gained section 9: three constraints from the
agreed #8031 addendum. They were not part of this challenge and this pass did not test them. In short:

- One machine is a cluster of one by configuration (`bus: {nodes: 1, replicas: 1}`), with the same adapter,
  accounts, users and creds; replicas 3 is the fleet's configuration, not an unconditional barrier. Replicas are
  never lowered at runtime. Current main hard-codes `num_replicas: 1` and its CI runs one standalone server
  (smarty-dev#11373).
- `owner.<agent>` is a CAS record with no TTL; a TTL liveness key only signals; only #8639's confirmed-termination
  handoff starts a successor. Ownership reads go to the leader: custody storage sets `allow_direct: false`, and
  every role has an explicit `DIRECT.GET` deny on it. The ACL call-path note above (the root adapter uses no
  `DIRECT.GET`) still holds for root streams; custody storage is a separate stream.
- Human debug attach uses no NATS creds; no role gets attach subjects.

Effect on the findings: C7's stale window gains a one-node case (design note 2.4, not measured). C1's "no agent
purge" now also covers custody and liveness storage. The other findings are unchanged. Open implementation gates are
in the design note, 9.4. The amendment challenge below covers the draft conflicts; no named security pass is claimed.

## Amendment review round 1 (10-13, #958): findings and resolutions

An independent reviewer (Luna) read section 9 against the #8031 addendum and found two material contradictions.
The author fixed both in the design note. This is not a final pass: a new delta review of the fix comes next.
C1-C9 above are unchanged.

- **F1 (P1) Maintainer purge reached custody and liveness storage.** The maintainer's `$JS.API.STREAM.PURGE.*`
  matched `<CUS>` and liveness storage, so a purge could erase owner records and their CAS history.
  **Resolution:** a new `<LIVE>` placeholder names the liveness storage's stream (exact names stay O-2). Every role,
  maintainer included, has an explicit publish deny on `$JS.API.STREAM.PURGE.<CUS>` and `.<LIVE>`, in a separate
  deny table in 1.4. The parent also applies the same denial to `STREAM.DELETE` on `<CUS>` and `<LIVE>`: deleting
  those streams would erase the same records. Mesh-root lifecycle authority stays; agents purge nothing. T2 and
  T14 require both verbs refused on custody/liveness plus permitted mesh-root controls. Closes O-4.
- **F2 (P1) A config mismatch was only reported.** G-9 required a check at open, but the failure row said
  "reported" and O-3 left "report and continue" open. **Resolution:** a replicas, `allow_direct` or expiry mismatch
  is a hard open failure: no ownership decision and no write until a maintainer corrects it; replicas are never
  lowered automatically. G-9, the failure table, T8, T14, 9.1 and 9.2 now say the same. Closes O-3.

Still open, as pre-enable gates (not results): O-1 (smarty-dev#11373 implementation and clustered CI), O-2 (custody
and liveness mapping, CAS writer role, allow entries), O-5 (one-node revoke after a stop, not measured), O-6 (phase
of G-9/T14, lead decides), O-7 (scope of the named security pass, lead decides).

## Round 2 (10-13, #958): Amendment B answers and limits

The records above are unchanged. Basis: the full texts of two #958 reviews (staged copy) and the lead's decisions on
them, which match all four findings: Astra,
[review comment](../../REFERENCES.md#source-9) (P1), and security,
[security comment](../../REFERENCES.md#source-10) (`SECURITY_REVIEW: FAIL`; P1, P1,
P2). This record adds no finding and claims no review pass. Design note: "Round 2 answers" and section 10.

- **B1 ADOPT (Astra P1).** Normal maintainer `STREAM.UPDATE` could add an expiry to `<CUS>`, and an open engine
  would continue. Now every normal role denies `STREAM.CREATE` and `STREAM.UPDATE` on `<CUS>` and `<LIVE>` (the
  purge and delete deny stays); a separate 1 h break-glass user, minted only by an explicit
  `fabric nats mint --break-glass` after approval, makes such changes; the runbook stops the engines first (#8639),
  rechecks, restarts; engines recheck `<CUS>` on every reconnect and refuse a mismatch. T2, T14.
- **B2 ACCEPTED as security gap smarty-dev#11465 (security P1, same UID).** One UID on one host is one trust
  domain; per-launch creds are network authentication, org isolation and root binding, not same-host isolation.
  Fixes: per-host broker under a separate UID, or per-lane Unix users. `ptrace_scope` and `PR_SET_DUMPABLE=0` are
  to be measured first, not implemented, not a fix. C8's guards stay.
- **B3 ADOPT (security P1, restart with an old JWT).** `revoke` writes the account JWT durably into each node's
  resolver dir; every node start (`init --local` and each of 3 nodes) accepts no clients until its JWT hash matches
  the issuer's latest. Effect on C7: `partial` still names a node that cannot confirm, but a stopped node no longer
  accepts the revoked user after a restart. O-5 resolved by design, not measured. T4, T16.
- **B4 ADOPT (security P2, rotation).** Per-key rotation; the retiring key stays until the longest credential it
  signed expires (principal 90 days), or its users are reissued and revoked first. T15.
- **B5 (lead clarification, not a review finding).** A SYS-only user (1 h, publish only
  `$SYS.REQ.CLAIMS.UPDATE`, own inbox, never in a launch environment); org users get no resolver or admin
  `$SYS.REQ` beyond `$SYS.REQ.USER.INFO`; D4 decided (own encoder over `@nats-io/nkeys` 2.0.3); finite quotas
  and `<CUS>`/`<LIVE>` configuration are required caller inputs. Every configured server ID must acknowledge
  the exact broadcast JWT for `complete`; missing or error acknowledgements yield `partial` (T4), and the start
  gate covers a node that was down. Source-confirmed nats-server v2.14.7 has no per-server JWT read-back;
  the lead accepts this known limit. No LOOKUP grant or new read-back mechanism. O-10 resolved (10.5).

Limits: docs only; nothing implemented, tested or measured; the mint later follows Amendment B. Still open: O-1, O-2,
O-6, O-7, and new O-8 (break-glass approver and audit record) and O-9 (fleet location of the latest-JWT record). The
same-UID gap is filed as smarty-dev#11465. A delta design review and security review of the frozen text come next; neither is
claimed here.
