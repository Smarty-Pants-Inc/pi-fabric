# Real SSH mesh-bridge proof (#2045)

`loopback-ssh-bridge.sh FABRIC_CHECKOUT [EVIDENCE_DIR]` is the historical one-host proof with two scratch meshes and a private loopback sshd. It needs a built Fabric checkout. Its scratch directory stays under the lane's `.local/`.

`forge-ssh-bridge.sh CONFIG.json` is the opt-in real-host proof. Run it on Dev1 against the installed artifacts, **not** a candidate build. It starts one fresh Pi on each host, each at nice 19, and one temporary bridge. It never sends messages to unrelated agents. Evidence is local `results.json`, both fresh RPC streams, and the bridge log. Exit 0 requires all assertions and cleanup to pass.

Before running:

- The remote host owner installs the same accepted Pi/Fabric artifacts and supplies the actual paths and host-owned Pi profile. Do not copy Dev1 credentials.
- Prepare dedicated, non-Git proof working directories on both hosts. A fresh Pi inside a lane's Git branch can steal that lane's factory PR wake. Keep the stack/org context and host-owned profile, but do not inherit the lane owner's role. Their `.pi/fabric.json` must select the intended existing host mesh. Accept only those prepared resources with a one-time scoped trust decision if needed; do not change global trust. An environment variable does not select a Fabric mesh.
- Pin the existing dedicated bridge public key to `command="node <installed-fabric>/bin/mesh-bridge agent --mesh <remote-mesh> --peer dev1",restrict` in the non-root remote account. Only the host owner changes that slot.
- Keep independently verified host keys; use strict batch SSH. The ordinary SSH alias launches the remote Pi; the forced-command key transports the bridge. These are different routes.
- Do not run while another bridge to the same remote mesh exists. Killing the proof bridge must not disrupt an installed fleet link.
- Keep the config and evidence under your lane's `.local/`. See the driver header for required and optional config fields. Host-specific paths and raw logs do not belong in Git.

The proof checks native discovery, acknowledged **and attributed received** steer/followUp in both directions, factory-shaped `ops.owner` `pr.wake` read by the remote Main, factory lane-wake followUp ingress, and a directed `fleet.work.*` event's bridged arrival and attributed native root-inbox ingress. The work-inbox check waits through the native 60-second steer grace; an acknowledgement or a model echo is not ingress. A `pr.wake` alone has no Main handler; use the factory lane-wake followUp for an immediate native turn, not a claim that `ops.owner` starts Main. It then SIGKILLs its own bridge and requires the named mirrored-host lease-lapsed error on both sides under a finite deadline. A send in the live-lease window can first get the bounded acknowledgement-timeout error; that is not delivery and not the final lapse assertion.

A run on ryzen2 is supplemental evidence for the mission's second host. It does **not** satisfy #2045's explicit Dev1↔Forge acceptance requirement. Report the tested host and installed version without renaming a host to make an assertion pass.

Builds and suites run through the fleet batch tool on the available compute host (currently `FORGE_RUN_HOST=ryzen2-batch …/lightweight-fleet/.local/forge/forge-run …`); only the real-Pi mesh proof runs on Dev1. No whole-suite run is needed for these proof scripts.
