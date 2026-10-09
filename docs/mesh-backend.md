# Mesh backend switch: the reader readiness gate

`fabric-mesh-backend cutover` (and `import`, the same fenced section) switches a mesh root from
`state.json` to `state.db`. Every program that reads the mesh root must be able to read the new
store before the switch goes live. smarty-dev#7815: the #6477 switch went live while the installed
factory could not read the new store (`KeyError 'entries'`), and the GitHub event forwarder
crash-looped for hours. The gate stops that.

## Which readers are required

The gate decides from an inventory, never from what readers chose to register:

1. **Live Fabric releases.** The writer census (`fabric-mesh-backend census`) attributes live
   writers from the host leases (`<mesh>/host-leases/*.json`, field `writer.releaseSha`) and the
   process records (`<mesh>/.writer-census/*`, field `releaseSha`). Topology participant records
   carry no release. Each distinct release R becomes the required reader `fabric@R`. Live writers
   without a release (`"unknown"`), evidence the census cannot attribute (a pid-only lock owner, a
   torn record, an open `state.db-wal` without a known writer) and a census that fails become
   `fabric@unknown`, which can only be accepted (`--accept-unready fabric@unknown`), never proved.
   The census itself stays advisory (smarty-dev#6982); this gate is what refuses.
2. **Built-in non-Fabric readers**, fixed in code (`BUILTIN_READERS` in `src/mesh/reader-proof.ts`):
   `factory` with the trusted release root `~/.local/share/smarty-dev/factory`. A built-in whose
   release root does not exist on this host is not installed here and not required.
3. **`--require-reader NAME=/abs/release/root`**, repeatable, always required, recorded in the intent.
   Flags only add readers: a built-in name (`factory`) or a `fabric*` name exits 2 ("reserved reader name").

4. **SQLite holders** (smarty-dev#7936), Linux only. The gate takes the inodes (dev/ino) of
   `state.db`, `state.db-wal` and `state.db-shm` (and those under `state-shadow/`), plus those its
   OWN open fds point at (readlink and stat of `/proc/self/fd/*`; the outcome records them as
   `ownStateDbFds`). The holders are the UNION of:
   - every other same-uid pid whose readable `/proc/<pid>/fd` holds one of those inodes
     ("foreign state.db holder");
   - every `/proc/locks` entry (world-readable) on one of those inodes (device `major:minor` and
     inode, as stat gives them) by a pid other than the gate's. A SQLite WAL connection always holds
     a lock on `-shm`, so this finds holders whose fd directory this user cannot read (non-dumpable
     or sandboxed processes). An entry with pid -1 (an OFD lock) or a pid that does not resolve to a
     process is "ambiguous".

   Each holder is required as `holder@<pid>` and is never ready. A same-uid pid whose
   `/proc/<pid>/fd` cannot be read is NOT a holder when `/proc/locks` shows no lock of it on those
   inodes. If `/proc/locks` itself cannot be read or parsed, the gate fails closed: every such pid is
   then an ambiguous holder. The gate's own connection is not evidence: with no other holder, a
   switch needs no override for the SQLite files.

   Residual (smarty-dev#7936): a process whose fd directory this user cannot read and that holds
   `state.db` open WITHOUT any SQLite lock (not a WAL connection) is not detected; WAL connections
   always hold a lock on `-shm`.

   On Linux, the census's SQLite-file evidence (`state-database`) is replaced by this scan, so
   `fabric@unknown` no longer covers it: each holder is named and accepted only by its own
   `--accept-unready holder@<pid>`. Off Linux there is no scan: SQLite-file evidence stays
   `fabric@unknown` (fail closed), and `--accept-unready fabric@unknown` still passes it there.
   Before accepting a holder, identify it (`ps -o pid,user,comm,args -p <pid>`, `fuser -v
   <mesh>/state.db*`) and save that output with the switch record, for example appended with a UTC
   stamp to `<mesh>/backend-switches.holders.txt`, cited on the switch's issue.

Proofs of readers that are not required are ignored.

## The rule

The switch refuses (exit 3, nothing switched) unless each required reader is ready:

- `fabric@R`: some proof with `"implementation": "fabric-meshstore"` and `"release": R` is valid.
- an installed reader N with trusted root T: `<mesh>/readers/N.json` is a valid proof and its
  `release` equals `basename(realpath(T/current))` now. The proof's own `installRoot` is ignored:
  a reader upgraded since its proof is unready.

A valid proof is a regular file (`lstat`; not a symlink) owned by the gate's user and not
group/other-writable, passes the strict schema below, lists `sqlite`, has `provedEpoch` equal to
the epoch the switch starts from (or the epoch it commits, for a proof made while a crashed switch
sits at `importing`), and has `provedAt` at most 24 h old and at most 5 min in the future.
The `readers/` directory itself must be a directory owned by the gate's user and not
group/other-writable: otherwise the switch refuses and no override lifts that. Windows has no
POSIX owner or mode bits: these owner and mode checks are not applied there (Windows ACLs:
smarty-dev#7548).

`--accept-unready NAME,...` is the only override.

Order of a switch (crash reruns included):

1. Inventory and scan; refuse on any required reader that is unready and not accepted.
2. Append the **intent** to `<mesh>/backend-switches.jsonl` (fsync of file and directory) with a new
   `switchId`, the required readers (holders included) and the requested overrides (`acceptUnready`).
   If it cannot be written, refuse.
3. Run the fenced switch. Under the fence, inside the transaction right before EACH flag commit
   (`backend=importing`, then `backend=sqlite`; a rerun from `importing` included), the whole
   inventory (census, built-ins, flags, SQLite holders) is recomputed and checked again.
   A refusal rolls that transaction back. A refusal at the `sqlite` commit leaves the
   root at `importing` with the moved marker; readers keep reading SQLite through the reader rule,
   and a rerun completes the switch once the readers are ready (or `rollback`).
4. Append the **outcome** with the same `switchId` (best effort), with `checkedUnderFence`.

`rollback` is not gated: it is the way back.

## The proof file

`<mesh>/readers/<name>.json`, mode 0600, in a directory of mode 0700:

| Field            | Type     | Required | Meaning |
|------------------|----------|----------|---------|
| `name`           | string   | yes | Equal to the file name without `.json`; `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. |
| `implementation` | string   | no  | The read code. `fabric-meshstore` is reserved for `reader-proof --backend`. |
| `version`        | string   | yes | The reader's version, non-empty (informational). |
| `installRoot`    | string   | no  | Informational; the gate uses its own trusted root. |
| `release`        | string   | yes | `basename(realpath(<release root>/current))` of the code that did the read. |
| `backends`       | string[] | yes | Non-empty; each `file` or `sqlite`. |
| `provedAt`       | string   | yes | ISO 8601 with a zone, the time of the read. |
| `provedEpoch`    | integer  | yes | `epoch` of `fabric-mesh-backend status --root <mesh> --json` at the read. |

No other field is allowed.

```json
{"name": "factory", "implementation": "python-factory", "version": "4f2c9e1",
 "release": "1ed31d0d543512f7ffe9b19934cabac58c1c4899",
 "backends": ["file", "sqlite"], "provedAt": "2026-10-09T16:58:12Z", "provedEpoch": 0}
```

## Writing a proof

Every proof is written under the migration fence, into a `readers/` directory that is a real
directory (not a symlink) owned by the writer's user and not group/other-writable (created 0700), so no proof changes between the final check and
the commit. The fence is two lock directories at the mesh root, taken in this order and held
while the proof is renamed into place:

1. `<mesh>/custody.lock`: a directory published by `rename` of a staging directory holding `owner`;
2. `<mesh>/.lock` (protocol 1): `mkdir`, then `owner` created with `O_EXCL`.

They are not `flock` locks. A crashed owner is reclaimed by the Fabric lock code (pid and process
incarnation checks), so another program must not reimplement them: it takes the fence by running
the CLI below, which holds both locks while it writes.

### Fabric (MeshStore readers: Pi sessions, mesh-bridge, actors)

Run as each installed Fabric release that reads the mesh:

```sh
fabric-mesh-backend reader-proof --root <mesh> --backend sqlite
```

It copies `state.json` to a scratch directory, imports it there with the cutover's own code, lists
every entry through MeshStore and checks the count, then under the fence writes
`readers/fabric-<release>.json` with this release: `PI_FABRIC_RELEASE_SHA` (else
`PI_FABRIC_BUILD_SHA`, `GITHUB_SHA`, else the package directory name): the same value its writer
records carry. The live root is never changed.

### Any other reader (the Python factory)

The reader proves with its **own installed** read code, then hands the proof to the CLI, which
checks the schema and writes it under the fence:

1. `release = basename(realpath(<its release root>/current))` of the running install.
2. `provedEpoch` = `epoch` from `fabric-mesh-backend status --root <mesh> --json`.
3. Copy `<mesh>/state.json` into an empty scratch directory and run
   `fabric-mesh-backend import --root <scratch>` there.
4. Read every entry from `<scratch>` with its own SQLite read path; fail on any error or a count
   other than the import's `entries`.
5. Write the proof JSON (format above) to a private temp file and run
   `fabric-mesh-backend reader-proof --root <mesh> --name <name> --proof-file <temp>`.
   It refuses `fabric*` names and `implementation: "fabric-meshstore"`.

Prove again after each install (a new release makes the proof unready), after each switch (a new
epoch does) and within 24 h before a planned switch.
