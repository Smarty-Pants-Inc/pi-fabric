# Participants CLI

`fabric-participants` prints the Fabric participant directory of a mesh as JSON, for readers
outside Pi (smarty-knowledge-3#395; first reader: knowledge-live, smarty-knowledge-3#400). It lists through `ParticipantDirectory.list()`, the code
`agents.members` uses, so it applies the same participant liveness policy (state or file
leases) and includes roots the [mesh bridge](mesh-bridge.md) mirrors from other hosts.

```sh
fabric-participants [--json] [--mesh DIR] [--include-stale] [--kind root|agent|actor]...
# the same, in the form the fleet names it:
fabric-participants participants --json
```

It opens the mesh read-only: it takes no mesh lock, writes no lease, event or state, and
never creates the mesh directory.

## Flags

| Flag | Effect |
| --- | --- |
| `--json` | Accepted for clarity. JSON is the only output format. |
| `--mesh DIR` | The mesh root. Default: the root a Pi started in the current directory uses: `PI_FABRIC_MESH_ROOT`; else `mesh.root` from `<project>/.pi/fabric.json`, else from `$PI_CODING_AGENT_DIR/fabric.json` (default `~/.pi/agent`), relative to the project root (`PI_FABRIC_PROJECT_ROOT` or the current directory); else `<project>/.pi/fabric/mesh`. The project file is read without Pi's project-trust check: a reader that must not trust its current directory passes `--mesh`. |
| `--include-stale` | Also list participants whose owner host lease lapsed (`stale: true`). Default: live only. |
| `--kind K` | Only participants of kind `root`, `agent` or `actor`. Repeat it, or give a comma list. |

## Output

Standard output is one JSON array of `FabricParticipantInfo` objects
([src/topology/types.ts](../src/topology/types.ts)), sorted by `startedAt`, then `name`, then `id`.
An empty mesh prints `[]`.

| Field | Always | Meaning |
| --- | --- | --- |
| `id` | yes | Participant id. A root is `session:<session id>`. |
| `kind` | yes | `root` (a Pi session), `agent` or `actor`. |
| `rootId` | yes | The root of its lineage. |
| `name`, `status`, `runner`, `transport`, `capabilities` | yes | As the owner published them. `status` of a root: `idle`, `running` or `stopping`. |
| `startedAt`, `updatedAt` | yes | Epoch milliseconds. |
| `local` | yes | Always `false`: the CLI is not a host on the mesh. |
| `stale` | yes | `true` only with `--include-stale`, for a lapsed owner lease. |
| `label`, `role`, `project`, `cwd`, `sessionId`, `model`, `thinking`, `parentId`, `pendingMessages` | no | Present when the owner published them. |
| `remoteHost` | no | Set on a root the mesh bridge mirrors from another host: that host's name. Absent on this mesh's own participants. |

Other fields of `FabricParticipantRecord` (`ownerHostId`, `ownerIdentityId`, `controlProtocol`,
activity counters) can also appear. Readers must ignore fields they do not know.

## Exit status

| Code | Meaning |
| --- | --- |
| 0 | The array is on standard output. |
| 1 | An unexpected error, for example a `state.json` it cannot read. |
| 2 | A named error on standard error: `FABRIC_MESH_MISSING` (the mesh directory does not exist), `FABRIC_MESH_UNREADABLE` (`state.json` does not parse) or `FABRIC_USAGE` (a bad flag). |
