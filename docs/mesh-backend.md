# Mesh backend switch: the reader readiness gate

`fabric-mesh-backend cutover` (and `import`, the same fenced section) switches a mesh root from
`state.json` to `state.db`. Every program that reads the mesh root must be able to read the new
store before the switch goes live. smarty-dev#7815: the #6477 switch went live while the installed
factory could not read the new store (`KeyError 'entries'`), and the GitHub event forwarder
crash-looped for hours. The gate stops that.

## The rule

A switch that starts from `backend=file` (or a legacy root without `state.db`) reads every entry
`<mesh>/readers/*.json`. Each one must be a valid proof (below) that:

- is a regular file (`lstat`; a symlink, directory or unreadable entry is unready, never skipped);
- parses under the strict schema below (no unknown fields);
- lists `sqlite` in `backends`;
- has `provedEpoch` **equal** to the mesh backend epoch now (`fabric-mesh-backend status --json`, `epoch`);
- has `provedAt` at most 24 h ago and at most 5 min in the future (clock skew);
- has `release` equal to `basename(realpath(<installRoot>/current))` now: a reader upgraded since
  its proof is unready.

The switch refuses (exit 3, nothing changed) while any reader is unready, and also when there is no
reader at all (no `readers/` directory, or an empty one). Overrides:

- `--accept-unready NAME,...`: go ahead although the named readers are unready;
- `--accept-empty-registry`: go ahead with no registered reader.

Order of a gated switch:

1. Scan the registry; refuse on any reader not covered by an override.
2. Append the **intent** to `<mesh>/backend-switches.jsonl` (fsync of the file and the directory),
   with a new `switchId` and the overrides. If it cannot be written, refuse.
3. Run the fenced switch. Under the fence (`custody.lock` and the mesh `.lock`), inside the import
   transaction right before its COMMIT, the whole check runs again (`beforeCommit`): a reader that
   registered or changed its proof since step 1 is seen, and a refusal rolls the import back.
4. Append the **outcome** with the same `switchId` (best effort: the intent already records the override).

```json
{"at":"…","switchId":"6f0…","phase":"intent","command":"cutover","backend":"sqlite","fromEpoch":0,
 "readersReady":["fabric"],"acceptedUnready":[{"name":"factory","reason":"missing sqlite (has file)"}],"acceptedEmptyRegistry":false}
{"at":"…","switchId":"6f0…","phase":"outcome","ok":true,"epoch":1,"converged":false,"underFence":true,
 "readersReady":["fabric"],"acceptedUnready":[{"name":"factory","reason":"missing sqlite (has file)"}]}
```

A rerun after a crash (`backend=importing` or `sqlite`) is not gated: it converges the switch that
already started. `rollback` is not gated: it is the way back.

## The proof file

`<mesh>/readers/<name>.json`, one per reader. It is the reader's registration and its proof.

| Field            | Type     | Required | Meaning |
|------------------|----------|----------|---------|
| `name`           | string   | yes | Equal to the file name without `.json`; `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. |
| `implementation` | string   | no  | The read code. `fabric-meshstore` = Fabric's MeshStore; anything else (for example `python-factory`) is its own. |
| `version`        | string   | yes | The reader's version, non-empty (informational). |
| `installRoot`    | string   | yes | Absolute path of the reader's release root, for example `/home/paul/.local/share/smarty-dev/factory`. |
| `release`        | string   | yes | `basename(realpath(installRoot + "/current"))` when the proof ran. |
| `backends`       | string[] | yes | Non-empty; each `file` or `sqlite`: backends this release read for real. |
| `provedAt`       | string   | yes | ISO 8601 with a zone (`2026-10-09T16:58:12Z`), the time of the read. |
| `provedEpoch`    | integer  | yes | The mesh backend epoch when the read ran: `epoch` of `fabric-mesh-backend status --root <mesh> --json`. |

No other field is allowed.

```json
{"name": "factory", "implementation": "python-factory", "version": "4f2c9e1",
 "installRoot": "/home/paul/.local/share/smarty-dev/factory", "release": "4f2c9e1a…",
 "backends": ["file", "sqlite"], "provedAt": "2026-10-09T16:58:12Z", "provedEpoch": 0}
```

A reader that cannot prove `sqlite` yet still writes its file (for example `"backends": ["file"]`):
then the switch knows it exists and refuses until it proves `sqlite`.

## Writing a proof

### Readers that read through Fabric's MeshStore

Pi sessions, mesh-bridge and actors read through MeshStore. Run as the installed release:

```sh
fabric-mesh-backend reader-proof --root <mesh> --name fabric --backend sqlite --install-root <release root>
```

It copies `state.json` to a scratch directory, imports it there with the cutover's own code, lists
every entry through MeshStore, and checks the count. Only then does it write the proof, bound to the
installed release of `--install-root` (or the existing entry's `installRoot`). The live root is
never changed. A root already cut over is read in place. It refuses every reader except `fabric` and
entries with `"implementation": "fabric-meshstore"`: a proof by MeshStore says nothing about
another program's read code.

### Any other reader (the Python factory)

The reader writes its own proof, after a real read with its **own installed** read code, never
with MeshStore and never the live root before the switch:

1. Resolve `release = basename(realpath(<installRoot>/current))` of the running install.
2. Read `provedEpoch` = `epoch` from `fabric-mesh-backend status --root <mesh> --json`.
3. Copy `<mesh>/state.json` into an empty scratch directory and run
   `fabric-mesh-backend import --root <scratch>` there (the scratch has no registry, so pass
   `--accept-empty-registry`).
4. Read every entry from `<scratch>` with the reader's own SQLite read path; fail on any error or a
   count other than the import's `entries`.
5. Write `<mesh>/readers/<name>.json` with the format above: temp file in the same directory, fsync,
   rename. Keep `file` in `backends` when the reader still reads `state.json`.

Prove again after each install (a new `release` makes the proof unready), each switch (a new epoch
does) and at least once every 24 h before a planned switch.
