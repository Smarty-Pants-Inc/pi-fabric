# Remote capability tasks — #11362 part 1 / #11170

Why: moving lead Mains to work hosts must not remove their ability to delegate corpus/GitHub work. This removes a fleet-placement P0 while keeping private data and App access on Ryzen 1.

## Reused path

Use the existing **resident host** as the capability owner: it already owns process admission, native task custody, terminal results, and durable control-command claims. A new named coordinator would duplicate those mechanisms. Each live resident advertises host-only `agents.hostCapabilities` on its existing host record. Workspace configuration cannot grant capabilities or relax remote admission.

The work-host provider selects a live capable resident from the mirrored directory. It sends `fabric.control.command` / `spawn`, addressed to that owner's exact host id, with a captured owner incarnation, the exact caller session id, and an idempotency key. The bridge authenticates inbound identity from mirrored presence, not request text. The owner uses the normal AgentManager process launch and write/model/tool admission. No SSH or second transport is added.

The caller persists its intent/route before publication and keeps a proxy handle. Status, bounded wait/result, and stop use addressed control commands. Completion uses the same control path back to the exact spawning Main, is durably journaled on that side, and enters the normal completion inbox. The resident retains an outbox until the receiving Main acknowledges durable receipt. Completion control keys include the current receiving activation: existing control receipts preserve their claiming epoch, while the caller's completion journal deduplicates the same task outcome across reloads.

## Security rule

Only a verified live root Main with the host-bound `project-agent` or `org-agent` role may request a remote spawn. Tasks, actors, worktree Mains, forged roles/return addresses, and unverified events are refused before process admission. Receiver revalidates sender identity, root, session, and bridge attribution at the mutation fence. Return addresses come from that verified root, never a role/name/cwd lookup.

Remote launch accepts only public one-shot settings; no actor/session seeds, startup probes, runtime paths, placement commands, or inherited host authority. It forces process/local execution on the capable host. An explicit absolute cwd must exist there; omission allocates a fresh TMPDIR. Remote write policy defaults to read-only with shell denied; host-only `agents.remoteSpawnPolicy` can authorize an existing writable root, shell, and tool ceiling. Requests can narrow but never widen it. This extra explicit boundary is necessary because a foreign lead does not inherit the resident's ambient privileges. Fabric-v2 selected this explicit ceiling (Decision A). F9 will provision the named `ryzen1-github-cli` deployment profile with shell `unconfined`, existing host-local writable roots and a tool ceiling under a named security review; `.local/out/pr.md` has the exact proposed JSON. Model and tool admission also run through the local manager.

## Persistence/failure semantics

Caller intent, owner claim/allocated id, terminal results, and completion outbox survive restarts. Same-key retries keep their original route and input digest and cannot launch twice. An owner restart with an interrupted claim is an explicit lost/outcome-unknown typed error, not permission to replay. Completed native outcomes remain available. Errors distinguish no capable owner, stale bridge, policy refusal, and owner-restart loss.

## Acceptance ledger

- Host-only capability/policy config, empty capabilities by default; workspace cannot override.
- Real bridge: root A lead calls spawn(needs corpus), root B process executes, exact result/normal completion inbox returns to A.
- Proxy status/wait/result/stop; observation timeout leaves task running.
- Authenticated lead-only receiver, caller/session spoof rejection, task/actor/worktree refusal.
- No write/shell/model/tool widening; selected-host cwd/input existence; local capable and reserved local counterexamples remain local.
- Same-key unchanged request deduplicates; changed request refuses; caller restart recovers; owner interrupted claim refuses replay; terminal result survives.
- Targeted tests, typecheck, fresh build and artifact verification; no full suite.

## Cuts / remaining installation proof

No new transport, launcher, secret copying, capability-host provisioning, cross-host file copy, Main migration, remote actor creation, branch seeds, recursive remote spawning, automatic restart/replay, transcript mirroring, or additional task controls. Existing mesh event/frame budgets apply. Deployment/configuration belongs to fabric-v2's F9 install. The named user E2E is **a work-host lead Main spawns needs:[github] and gets the answer back**, owner **fabric-v2**, **after the F9 install**. Local two-root proof uses deterministic process input instead of the private corpus or live GitHub credentials.
