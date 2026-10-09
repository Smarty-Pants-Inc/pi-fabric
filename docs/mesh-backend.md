# Mesh backend switch: the reader readiness gate

`fabric-mesh-backend cutover` (and `import`, the same fenced section) switches a mesh root from
`state.json` to `state.db`. Every program that reads the mesh root must be able to read the new
store before the switch goes live. smarty-dev#7815: the #6477 switch went live while the installed
factory could not read the new store (`KeyError 'entries'`), and the GitHub event forwarder
crash-looped for hours. The gate stops that.

## The rule

A switch that starts from `backend=file` (or a legacy root without `state.db`) lists every
registered reader, `<mesh>/readers/<name>.json`. It refuses, exit 3, nothing changed, when one is:

- **missing**: its `backends` does not contain `sqlite`;
- **stale**: its `provedEpoch` is lower than the mesh backend epoch now (`fabric-mesh-backend status`);
- **invalid**: the file does not parse or does not match the format below.

`--accept-unready NAME,...` lets the switch go ahead for the named readers only. Each switch appends
one line to `<mesh>/backend-switches.jsonl`:

```json
{"at":"2026-10-09T17:00:00.000Z","command":"cutover","backend":"sqlite","epoch":1,"previousEpoch":0,
 "converged":false,"readersReady":["bridge"],"acceptedUnready":[{"name":"factory","reason":"missing sqlite (has file)"}]}
```

A root with no reader file passes, with the warning `no reader registered`. A rerun after a crash
(`backend=importing` or `sqlite`) is not gated: it converges the switch that already started.
`rollback` is not gated: it is the way back.

## The proof file

`<mesh>/readers/<name>.json`, one per reader, written atomically (temp file, then rename):

| Field         | Type      | Meaning |
|---------------|-----------|---------|
| `name`        | string    | Equal to the file name without `.json`. `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. |
| `version`     | string    | The reader's installed version (release SHA or package version). |
| `backends`    | string[]  | Backends this reader read for real: `file`, `sqlite`. |
| `provedAt`    | string    | ISO 8601 UTC time of the proof. |
| `provedEpoch` | number    | The mesh backend epoch when the proof ran: `meta.epoch` in `state.db`, 0 without one. |

```json
{"name": "factory", "version": "4f2c9e1", "backends": ["file", "sqlite"], "provedAt": "2026-10-09T16:58:12Z", "provedEpoch": 0}
```

A reader registers itself by writing its file with the backends it can read now (for example
`["file"]`). Then the switch knows the reader exists and refuses until it proves `sqlite`.

## Writing a proof

A Fabric reader (Pi sessions, mesh-bridge, actors) runs, as the installed release:

```sh
fabric-mesh-backend reader-proof --root <mesh> --name <name> --backend sqlite [--reader-version V]
```

It copies `state.json` to a scratch directory, imports it there with the cutover's own code, lists
every entry through the installed store read path, and checks the count. On success it writes the
proof file. A root already cut over is read in place. The live root is never changed.

A reader in another language (the Python factory) writes the file itself, after the same real read
with its own installed code. It must read a migrated copy, never the live root before the switch:

1. Copy `<mesh>/state.json` to an empty scratch directory.
2. Run `fabric-mesh-backend import --root <scratch>` there.
3. Read every entry from `<scratch>` with the reader's own SQLite read path; fail on any error.
4. Get `provedEpoch` from `fabric-mesh-backend status --root <mesh> --json` (`epoch`).
5. Write `<mesh>/readers/<name>.json` with the format above (temp file, then rename), keeping
   `file` in `backends`.

Write the proof again after each install of a new reader version and after each switch: a proof
from an earlier epoch is stale.
