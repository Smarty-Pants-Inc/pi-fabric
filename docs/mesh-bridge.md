# Mesh bridge (v1)

`bin/mesh-bridge` links the Fabric mesh of this host (the hub, Dev1) to the mesh of one remote host
over an ssh stdio transport (smarty-dev#2004). Each host keeps its own mesh; the bridge republishes
existing events and mirrors root presence. There is no new store or protocol.

## What crosses

- `fabric.control.command` and `fabric.control.ack`, `fleet.work.*`, and `ops.owner` with kind
  `pr.wake`, when the recipient (`to`) is a live root or host native to the other side.
- Presence: each side's live native hosts and root participants go into the other side's state at
  the same keys, with `remoteHost: <side name>` and the source identity as `updatedBy`. A mirrored
  lease is `min(source expiry, now + 15 s)` and is renewed every 5 s. When the bridge stops, its
  mirrors lapse within 15 s. On a clean stop or a transport failure it deletes them at once.
- Nothing else. This excludes `github.*`, actor output, state and cache keys, and agent and actor
  participants.

## Rules

- Each bridged event keeps its `from`, `to`, `kind` and `text`. It carries
  `data.bridge = {from: <side>, id: <original event id>}`, and an event with a `bridge` field is
  never forwarded again.
- Per direction the cursor is the last source sequence handled. It is saved in the cursor file,
  and also after each forwarded event with the destination sequence of that event. On restart the
  bridge skips the source ids already bridged after that mark, so a crash does not forward an
  event twice.
- The remote is not trusted. An event from it crosses only when all of these are true:
  - its sender is a live native participant or host identity of the remote;
  - no hub record, live or not, uses that id;
  - its recipient is native to the hub.
- A mirror never replaces a native record or another bridge's mirror. It writes by
  compare-and-swap only.
- The agent (the remote end) serves only the bridge operations. It applies the allow-list and
  stamps its pinned `--peer` name on everything it writes, whatever the hub sends.

## Running it

On the remote, pin the agent to the bridge's dedicated key in `~/.ssh/authorized_keys`:

    command="node /path/to/pi-fabric/bin/mesh-bridge agent --mesh /path/to/project/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 AAAA... mesh-bridge@dev1

On the hub, run one bridge per remote host as the fleet user, under the service manager:

    mesh-bridge run --mesh ~/proj/.pi/fabric/mesh --name dev1 --remote forge \
      --cursor ~/.local/state/mesh-bridge/forge.json --ssh forge --ssh-key ~/.ssh/mesh-bridge

The bridge exits with status 1 when the transport closes. The service manager restarts it, and it
resumes from the cursor file. `-- COMMAND...` replaces ssh with any stdio transport (tests use a
local agent process).
