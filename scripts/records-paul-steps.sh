#!/usr/bin/env bash
# One-time, idempotent host setup for an org's record store (smarty-dev#754 C10/C13, pi-fabric#103 F1).
# PostgreSQL runs as its own OS user <org>-records, which owns the cluster. The org's agents
# (OS user <org-user>) reach only the records service socket, never PostgreSQL.
#
# run: copy this script to /run/smarty-step.sh, check its sha256, then (both digests from --print-digests on a trusted build):
#   sudo /run/smarty-step.sh --org smarty-pants --org-user paul --operator relay:relay:fabric --package-root <root> --node <node> --bundle-sha256 <hex> --node-sha256 <hex>
#   scripts/records-paul-steps.sh --print-digests --package-root <root> --node <node>   (no root)
#   scripts/records-paul-steps.sh --org smarty-pants --org-user paul --print hba|ident|conf|service-json|units|operator-edit-js
# --node (an absolute node binary; no PATH lookup) and --package-root (a built checkout holding
# dist/records-service/service-main.mjs, a self-contained bundle) are SOURCES, read once each into a root-only
# staging directory. Only staged copies whose sha256 equals --node-sha256/--bundle-sha256 are installed to
# root-owned /opt/<org>-records/{node,service-main.mjs}; the units and every later command use only those copies.
# Nothing derives a path from the script's own location: a copy at /run/smarty-step.sh behaves identically.
#   sudo scripts/records-paul-steps.sh --org smarty-pants --org-user paul --rollback [--dry-run] [--yes-delete-records]
# LIMIT: WAL accumulates until the WAL-G archive step; run it before sustained use and watch disk.
#
# ROOT STEPS (one line each):
#  0. mktemp -d /run/<org>-records-stage.XXXXXX (root, 0700; removed on exit); copy --node and the bundle into it once each; refuse (nothing changed) unless each staged copy's sha256 equals --node-sha256/--bundle-sha256
#  1. useradd the <org>-records system user (skipped if it exists)
#  2. if no /usr/lib/postgresql/<N>/bin has initdb and postgres: without /etc/postgresql-common/createcluster.conf, write createcluster.d/99-smarty-records.conf (create_main_cluster = false), then apt-get install postgresql (distro; no repository added); use the highest N (16 or newer, or --pg-bin); install the verified staged node (0755) and service-main.mjs (0644) root-owned in /opt/<org>-records; check <org>-records runs it with no modules
#  3. create /var/lib/<org>-records/{,pg,status,credentials}, /run/<org>-records-pg, /run/<org>-records and /etc/<org>-records with fixed owners and modes
#  4. initdb the cluster as <org>-records if absent (an existing cluster keeps its own major, from PG_VERSION); write pg_hba.conf, pg_ident.conf, conf.d/records.conf; append include_dir to postgresql.conf
#  5. write /etc/<org>-records/service.json if absent (never overwritten: it holds granted roles)
#  6. write both systemd units; daemon-reload; enable --now <org>-records-pg.service (restart if its config or unit changed and it was running); wait for pg_isready
#  7. createdb records if absent; create role records_service if absent; run migrations as <org>-records; enable --now <org>-records.service (restart if node, bundle or unit changed and it was running)
#  8. per --operator: refuse an id that holds another role; add the role to service.json, issue its credential with --reissue if absent (sha256(id).json, id, role and issuer checked) as <org>-records; relay: <org-user> itself writes it 0600 to ~<org-user>/.config/<org>-records; reload the service if active
#  9. verify owners and modes; check that <org-user> cannot reach PostgreSQL; check both units active, pg_isready, the python3 peer audit, the service socket and no TCP listener on the records port (changes nothing)
# ROLLBACK STEPS (--rollback; root; a real rollback needs --yes-delete-records; the PostgreSQL packages stay installed):
#  R1. systemctl stop both units; refuse (nothing deleted) unless is-active says inactive, failed or unknown for both; systemctl disable both
#  R2. rm -f both unit files; systemctl daemon-reload
#  R3. rm -rf /etc/<org>-records
#  R4. rm -rf /var/lib/<org>-records (cluster data, credentials, status: take the printed pg_dump backup first)
#  R5. rm -rf /run/<org>-records /run/<org>-records-pg
#  R6. rm -rf /opt/<org>-records
#  R7. as <org-user>: rm -rf ~<org-user>/.config/<org>-records (relay credential; root never deletes in that home)
#  R8. userdel <org>-records (skipped if absent)
# On the first failed step the script stops, names the step and the command on stderr, and exits 1.
# --help prints usage and, for the bundle, WHAT IT CHANGES (the ROOT STEPS above), IDEMPOTENCY (a second
# run skips or leaves unchanged every step; files are rewritten only when content differs; the service is
# reloaded only when roles changed), SUCCESS LINE and ROLLBACK (the list above). A real install ends with
#   C10_RECORDS_INSTALLED org=<org> user=<org>-records cluster=<N>/<org>-records unit=active peer-audit=ok
# printed last, only after the step 9 checks pass. A dry run never prints it. <N> is the detected PostgreSQL major.
# An operator id holds exactly one role: the same id with two roles (in one run, or against service.json) is refused.
# TEST ONLY: RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST=1 skips the root check so tests can run the real
# (non-dry) path against PATH fakes; with it, RECORDS_PAUL_STEPS_TEST_ROOT=DIR prefixes every system path
# (also /usr/lib/postgresql, where the PostgreSQL version is detected).
# Never set either on a real host.
set -Eeuo pipefail

usage() {
	cat <<'EOF'
usage: records-paul-steps.sh --org <org> --org-user <orguser> --package-root DIR --node BIN
         --bundle-sha256 HEX --node-sha256 HEX [--pg-bin DIR] [--origin NAME] [--port 5433]
         [--operator ROLE:ID]... [--dry-run]
       records-paul-steps.sh --print-digests --package-root DIR --node BIN
       records-paul-steps.sh --org <org> --org-user <orguser> --print hba|ident|conf|service-json|units|operator-edit-js
       records-paul-steps.sh --org <org> --org-user <orguser> --rollback [--dry-run] [--yes-delete-records]
  run: copy this script to /run/smarty-step.sh, check its sha256, then
       sudo /run/smarty-step.sh --org smarty-pants --org-user paul --operator relay:relay:fabric --package-root <root> --node <node> --bundle-sha256 <hex> --node-sha256 <hex>
  -h, --help          print this help, what the script changes, idempotency, the success line and the
                      rollback commands (with --org/--org-user filled in when given); needs no root
  --package-root DIR  absolute; holds dist/records-service/service-main.mjs (bun run build). Required to install.
  --node BIN          absolute path of the node binary to install (no PATH lookup). Required to install.
  --bundle-sha256 HEX, --node-sha256 HEX
                      the approved sha256 (64 lowercase hex) of the bundle and of node. Required to install
                      (also --dry-run). Each source is read once into a root-only staging directory; unless the
                      staged copies match, nothing changes. Only the verified copies are installed and run.
  --print-digests     print 'bundle-sha256 <hex>  <path>' and 'node-sha256 <hex>  <path>' and exit; no root
  LIMIT: WAL accumulates until the WAL-G archive step; run it before sustained use and watch disk.
  --pg-bin DIR        PostgreSQL 16+ binaries (default: the highest /usr/lib/postgresql/<N>/bin, after
                      apt-get install postgresql from the distro when none is present)
  --rollback          undo the install in reverse order (keeps PostgreSQL installed). A real rollback deletes the
                      record database and needs --yes-delete-records; take the printed backup first.
  --operator ROLE:ID  grant ROLE (importer|mirror|relay) to principal ID and issue its credential into
                      /var/lib/<org>-records/credentials/ (repeatable). 'importer:github' is
                      principal importer:github; 'importer:fabric:dev1' is principal fabric:dev1.
                      An id holds exactly one role: an id already granted another role is refused.
EOF
}
STEP="" RUN_CMD="" ROLLBACK=0 HELP=0
# Stderr report for the first failed step; nothing after it runs.
fail_report() { # LINE WHAT
	if [[ -n $STEP ]]; then
		printf 'records-paul-steps: FAILED at step %s (line %s): %s\n' "$STEP" "$1" "$2" >&2
		if ((ROLLBACK)); then
			printf 'Nothing after this step ran. Fix the cause and rerun --rollback (it is idempotent).\n' >&2
		else
			printf 'Nothing after this step ran. Fix the cause and rerun (the script is idempotent), or undo with --rollback.\n' >&2
		fi
	else
		printf 'records-paul-steps: FAILED (line %s): %s\n' "$1" "$2" >&2
	fi
}
die() {
	printf 'records-paul-steps: %s\n' "$*" >&2
	[[ -z $STEP ]] || fail_report "${BASH_LINENO[0]}" "$*"
	exit 1
}
on_error() {
	local rc=$?
	# ponytail: a failing subshell ($(...)) exits quietly; its parent's own ERR report names the command.
	((BASH_SUBSHELL == 0)) || exit "$rc"
	fail_report "$1" "${RUN_CMD:-$BASH_COMMAND} (exit $rc)"
	exit 1
}
trap 'on_error $LINENO' ERR

ORG="" ORG_USER="" PG_BIN="" NODE="" PACKAGE="" ORIGIN="" PORT=5433 BUNDLE_SHA="" NODE_SHA=""
DRY=0 PRINT="" OPERATORS=() YES_DELETE=0 PRINT_DIGESTS=0
while (($#)); do
	case $1 in
	--org | --org-user | --pg-bin | --node | --package-root | --origin | --port | --print | --operator | --bundle-sha256 | --node-sha256)
		(($# >= 2)) || die "$1 needs a value"
		case $1 in
		--org) ORG=$2 ;; --org-user) ORG_USER=$2 ;; --pg-bin) PG_BIN=$2 ;; --node) NODE=$2 ;;
		--package-root) PACKAGE=$2 ;; --origin) ORIGIN=$2 ;; --port) PORT=$2 ;; --print) PRINT=$2 ;;
		--operator) OPERATORS+=("$2") ;; --bundle-sha256) BUNDLE_SHA=$2 ;; --node-sha256) NODE_SHA=$2 ;;
		esac
		shift 2
		;;
	--print-digests) PRINT_DIGESTS=1; shift ;;
	--dry-run) DRY=1; shift ;;
	--rollback) ROLLBACK=1; shift ;;
	--yes-delete-records) YES_DELETE=1; shift ;;
	-h | --help) HELP=1; shift ;;
	*) usage >&2; die "unknown argument: $1" ;;
	esac
done

PATH_RE='^/[A-Za-z0-9._/+-]*$'
SRC_MAIN_REL=dist/records-service/service-main.mjs
sha_of() { local h; h=$(sha256sum <"$1") || return 1; printf '%s' "${h%% *}"; }
if ((PRINT_DIGESTS)); then
	# No root, builds nothing: the digests to approve for --bundle-sha256 and --node-sha256.
	[[ $PACKAGE =~ $PATH_RE && $NODE =~ $PATH_RE ]] || die "--print-digests needs an absolute --package-root DIR and --node BIN"
	SRC_MAIN="${PACKAGE}/${SRC_MAIN_REL}"
	for p in "$SRC_MAIN" "$NODE"; do [[ -f $p && ! -L $p ]] || die "${p} is missing, not a regular file or a symlink: refused"; done
	b=$(sha_of "$SRC_MAIN") n=$(sha_of "$NODE")
	printf 'bundle-sha256 %s  %s\nnode-sha256 %s  %s\n' "$b" "$SRC_MAIN" "$n" "$NODE"
	exit 0
fi

# --- validation (trust boundary: every value below lands in unit files, JSON and root commands) ---
if ((HELP)); then
	# --help needs no root and no other flag; given names are checked, missing ones stay placeholders.
	[[ -z $ORG || ($ORG =~ ^[a-z][a-z0-9-]*$ && ${#ORG} -le 24) ]] || die "invalid --org '$ORG'"
	[[ -z $ORG_USER || ($ORG_USER =~ ^[a-z_][a-z0-9_-]*$ && ${#ORG_USER} -le 32) ]] || die "invalid --org-user '$ORG_USER'"
	[[ -z $PG_BIN || $PG_BIN =~ ^/[A-Za-z0-9._/+-]*$ ]] || die "invalid --pg-bin '$PG_BIN'"
	ORG=${ORG:-<org>} ORG_USER=${ORG_USER:-<org-user>} PRINT=""
else
	[[ -n $ORG ]] || die "--org is required"
	[[ -n $ORG_USER ]] || die "--org-user is required"
	# ponytail: must start with a letter and stay <= 24 chars so "<org>-records" is a valid 32-char user name.
	[[ $ORG =~ ^[a-z][a-z0-9-]*$ && ${#ORG} -le 24 ]] || die "invalid --org '$ORG': use [a-z0-9-]+, starting with a letter, at most 24 chars"
	[[ $ORG_USER =~ ^[a-z_][a-z0-9_-]*$ && ${#ORG_USER} -le 32 ]] || die "invalid --org-user '$ORG_USER'"
	[[ $ORG_USER != root ]] || die "--org-user must not be root: agents must never run as root"
	if uid=$(id -u "$ORG_USER" 2>/dev/null) && [[ $uid == 0 ]]; then die "--org-user '$ORG_USER' has uid 0; refused"; fi
	[[ $PORT =~ ^[0-9]+$ ]] && ((PORT >= 1024 && PORT <= 65535)) || die "invalid --port '$PORT'"
	# P1-1: an install (real or dry) names both sources and both approved digests; nothing is looked up or defaulted.
	for d in "$BUNDLE_SHA" "$NODE_SHA"; do
		[[ -z $d || $d =~ ^[0-9a-f]{64}$ ]] || die "invalid digest '$d': use 64 lowercase hex characters (see --print-digests)"
	done
	if ((!ROLLBACK)) && [[ -z $PRINT ]]; then
		[[ -n $PACKAGE ]] || die "--package-root DIR is required: the absolute directory holding ${SRC_MAIN_REL} (bun run build)"
		[[ -n $NODE ]] || die "--node BIN is required: the absolute path of the node binary to install (no PATH lookup)"
		[[ -n $BUNDLE_SHA ]] || die "--bundle-sha256 HEX is required: the approved sha256 of ${SRC_MAIN_REL} (see --print-digests)"
		[[ -n $NODE_SHA ]] || die "--node-sha256 HEX is required: the approved sha256 of the node binary (see --print-digests)"
	fi
	[[ -n $ORIGIN ]] || ORIGIN=$(hostname -s)
	[[ $ORIGIN =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid --origin '$ORIGIN'"
	((!ROLLBACK)) || [[ -z $PRINT ]] || die "--rollback and --print do not combine"
	((ROLLBACK)) || ((!YES_DELETE)) || die "--yes-delete-records only applies to --rollback"
	for p in "$PG_BIN" "$NODE" "$PACKAGE"; do
		[[ -z $p ]] && continue
		[[ $p =~ $PATH_RE ]] || die "path must be absolute, without spaces or quotes: '$p'"
	done
	case $PRINT in "" | hba | ident | conf | service-json | units | operator-edit-js) ;; *) die "invalid --print '$PRINT'" ;; esac
	ID_RE='^[a-z][a-z0-9._-]{0,63}:[a-z0-9._@-]{1,64}$'
	declare -A OP_ROLE=()
	for spec in ${OPERATORS[@]+"${OPERATORS[@]}"}; do
		[[ $spec == *:* ]] || die "invalid --operator '$spec': use ROLE:ID, e.g. importer:github"
		role=${spec%%:*} id=${spec#*:}
		# ponytail: 'importer:github' names principal importer:github with role importer; 'importer:fabric:dev1' grants importer to fabric:dev1.
		[[ $id == *:* ]] || id=$spec
		[[ $role == importer || $role == mirror || $role == relay ]] || die "invalid --operator '$spec': ROLE must be importer, mirror or relay"
		[[ $id =~ $ID_RE ]] || die "invalid --operator '$spec': ID must match $ID_RE"
		# F18: an id holds exactly one role; refused before anything changes.
		if [[ -n ${OP_ROLE[$id]:-} && ${OP_ROLE[$id]} != "$role" ]]; then
			die "refused: operator id '$id' is given with two roles (${OP_ROLE[$id]} and $role); an id holds exactly one role"
		fi
		OP_ROLE[$id]=$role
	done
fi

REC="${ORG}-records"
T=""
if [[ ${RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST:-} == 1 ]]; then
	T=${RECORDS_PAUL_STEPS_TEST_ROOT:-}
	[[ -z $T || $T =~ ^/[A-Za-z0-9._/+-]*$ ]] || die "invalid RECORDS_PAUL_STEPS_TEST_ROOT"
fi
HOME_DIR="${T}/var/lib/${REC}"
DATA="${HOME_DIR}/pg"
PGSOCK="${T}/run/${REC}-pg"
SVCSOCK="${T}/run/${REC}"
CONF_DIR="${T}/etc/${REC}"
UNIT_DIR="${T}/etc/systemd/system"
CRED_DIR="${HOME_DIR}/credentials"
STATUS_DIR="${HOME_DIR}/status"
CFG="${CONF_DIR}/service.json"
PG_UNIT="${REC}-pg.service"
SVC_UNIT="${REC}.service"
OPT="${T}/opt/${REC}"
OPT_NODE="${OPT}/node"
MAIN="${OPT}/service-main.mjs"
SOCKET="${SVCSOCK}/records.sock"
# PostgreSQL comes from the distro; its major version is detected, never assumed.
PG_BASE="${T}/usr/lib/postgresql"
PG_MAJOR="" PG_BIN_GIVEN=${PG_BIN:+1} PG_REFUSAL=""
# F20: an existing cluster keeps its own major; highest-major detection is only for a fresh cluster.
CLUSTER_MAJOR=""
if [[ -f $DATA/PG_VERSION ]]; then
	CLUSTER_MAJOR=$(<"$DATA/PG_VERSION") || die "cannot read ${DATA}/PG_VERSION"
	[[ $CLUSTER_MAJOR =~ ^[0-9]+$ ]] || die "refused: ${DATA}/PG_VERSION does not hold a PostgreSQL major version; nothing was changed"
fi
cluster_refusal() { # WHAT: why the cluster's own binaries are not used
	PG_REFUSAL="refused: the cluster at ${DATA} is PostgreSQL ${CLUSTER_MAJOR}, but $1; install postgresql-${CLUSTER_MAJOR} (apt-get install postgresql-${CLUSTER_MAJOR}) or upgrade the cluster explicitly (pg_upgradecluster); nothing was changed"
}
detect_pg() { # PG_BIN, PG_MAJOR := the highest ${PG_BASE}/<N>/bin with initdb and postgres, or a placeholder
	local d n best=""
	for d in "$PG_BASE"/*/bin; do
		n=${d%/bin} n=${n##*/}
		[[ $n =~ ^[0-9]+$ && -x $d/initdb && -x $d/postgres ]] || continue
		if [[ -z $best ]] || ((n > best)); then best=$n; fi
	done
	PG_MAJOR=$best
	if [[ -n $best ]]; then PG_BIN="${PG_BASE}/${best}/bin"; else PG_BIN="${PG_BASE}/<N>/bin"; fi
}
check_major() {
	[[ -z $PG_MAJOR ]] || ((PG_MAJOR >= 16)) ||
		die "refused: PostgreSQL ${PG_MAJOR} at ${PG_BIN} is too old; the record store needs PostgreSQL 16 or newer"
}
pg_derived() { # everything that names the PostgreSQL major or binaries
	SUCCESS_LINE="C10_RECORDS_INSTALLED org=${ORG} user=${REC} cluster=${PG_MAJOR:-<N>}/${REC} unit=active peer-audit=ok"
	BACKUP_CMD="runuser -u ${REC} -- ${PG_BIN}/pg_dump -h ${PGSOCK} -p ${PORT} -U postgres -Fc records > /root/${REC}-backup.dump"
	DELETE_WARNING="This deletes the org's record database. Take a backup first: ${BACKUP_CMD}"
	APT_NOTE="Note: the PostgreSQL packages stay installed (other software may use them). Optional, to remove them too: apt-get remove postgresql postgresql-${PG_MAJOR:-<N>}"
}
if [[ -n $PG_BIN_GIVEN ]]; then
	if [[ -x $PG_BIN/postgres ]] && v=$("$PG_BIN/postgres" --version 2>/dev/null) && [[ $v =~ \(PostgreSQL\)\ ([0-9]+) ]]; then
		PG_MAJOR=${BASH_REMATCH[1]}
	fi
	if [[ -n $CLUSTER_MAJOR && -n $PG_MAJOR && $PG_MAJOR != "$CLUSTER_MAJOR" ]]; then
		cluster_refusal "--pg-bin ${PG_BIN} is PostgreSQL ${PG_MAJOR}"
	fi
elif [[ -n $CLUSTER_MAJOR ]]; then
	PG_MAJOR=$CLUSTER_MAJOR PG_BIN="${PG_BASE}/${CLUSTER_MAJOR}/bin"
	[[ -x $PG_BIN/initdb && -x $PG_BIN/postgres ]] || cluster_refusal "${PG_BIN} is missing"
else
	detect_pg
fi
check_major
pg_derived
if [[ ${RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST:-} == 1 ]]; then
	echo "records-paul-steps: TEST MODE (RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST=1): root check skipped" >&2
else
	[[ -n $PRINT ]] || ((DRY || HELP)) || ((EUID == 0)) || die "must run as root (sudo), or pass --dry-run to only print the steps"
fi
if ! ORG_GROUP=$(id -gn "$ORG_USER" 2>/dev/null); then
	# Only --print, --dry-run and --rollback may continue for a user that does not exist on this host.
	((DRY || ROLLBACK || HELP)) || [[ -n $PRINT ]] || die "--org-user '$ORG_USER' does not exist"
	ORG_GROUP=$ORG_USER
fi

# --- renderers ---
# Adds ID to roles[ROLE] in service.json, keeping every other entry and field. Prints changed|unchanged.
# Writes a temp file in the same directory (mode 0640, the file's owner and group) and renames it.
render_operator_edit_js() {
	cat <<'EOF'
const fs = require("fs");
const [file, role, id, mode] = process.argv.slice(1);
const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
if (cfg.roles === undefined) cfg.roles = {};
if (typeof cfg.roles !== "object" || cfg.roles === null || Array.isArray(cfg.roles)) throw new Error(`${file}: roles is not an object`);
// F18: an id holds exactly one role. Exit 3: the id is already granted another role; nothing is written.
for (const [other, ids] of Object.entries(cfg.roles)) {
	if (other !== role && Array.isArray(ids) && ids.includes(id)) {
		console.error(`${file}: ${id} already holds role ${other}; an operator id holds exactly one role, so ${role} is refused`);
		process.exit(3);
	}
}
// "check" only tests for a conflict and changes nothing.
if (mode === "check") {
	console.log("ok");
	process.exit(0);
}
if (cfg.roles[role] === undefined) cfg.roles[role] = [];
const list = cfg.roles[role];
if (!Array.isArray(list)) throw new Error(`${file}: roles.${role} is not an array`);
if (list.includes(id)) {
	console.log("unchanged");
} else {
	list.push(id);
	const st = fs.statSync(file);
	const tmp = `${file}.tmp-${process.pid}`;
	try {
		fs.writeFileSync(tmp, `${JSON.stringify(cfg, null, 2)}\n`, { mode: 0o640, flag: "wx" });
		fs.chmodSync(tmp, 0o640);
		fs.chownSync(tmp, st.uid, st.gid);
		fs.renameSync(tmp, file);
	} catch (e) {
		fs.rmSync(tmp, { force: true });
		throw e;
	}
	console.log("changed");
}
EOF
}
render_hba() {
	cat <<EOF
# Managed by records-paul-steps.sh; rerun it instead of editing.
# Only the OS user ${REC} reaches PostgreSQL (pg_ident map "records"). Everything else is rejected.
local all postgres peer map=records
local records records_service peer map=records
local all all reject
host all all 0.0.0.0/0 reject
host all all ::/0 reject
EOF
}
render_ident() {
	cat <<EOF
# Managed by records-paul-steps.sh; rerun it instead of editing.
records ${REC} postgres
records ${REC} records_service
EOF
}
render_conf() {
	cat <<EOF
# Managed by records-paul-steps.sh; rerun it instead of editing.
listen_addresses = ''
unix_socket_directories = '${PGSOCK}'
unix_socket_permissions = 0700
port = ${PORT}
synchronous_commit = on
fsync = on
track_commit_timestamp = on
archive_mode = on
# WAL-G replaces archive_command in its own authorized step. Until then archiving fails
# loudly and PostgreSQL keeps every WAL segment in pg_wal: no WAL is thrown away.
archive_command = '/bin/false'
archive_timeout = 60
# LIMIT: WAL accumulates until the WAL-G archive step; run it before sustained use and watch disk.
# (max_wal_size is deliberately not set: it cannot bound WAL that is waiting to be archived.)
EOF
}
render_service_json() {
	cat <<EOF
{
  "org": "${ORG}",
  "origin": "${ORIGIN}",
  "socket": "${SVCSOCK}/records.sock",
  "database": { "host": "${PGSOCK}", "port": ${PORT}, "database": "records", "user": "records_service" },
  "migration": { "host": "${PGSOCK}", "port": ${PORT}, "database": "records", "user": "postgres" },
  "roles": { "importer": [], "mirror": [] },
  "mirror": { "enabled": false },
  "admission": { "targets": [] },
  "statusFile": "${STATUS_DIR}/${ORG}.status.json"
}
EOF
}
render_pg_unit() {
	cat <<EOF
[Unit]
Description=PostgreSQL record store for ${ORG}
After=network.target

[Service]
Type=notify
User=${REC}
Group=${REC}
ExecStart=${PG_BIN}/postgres -D ${DATA}
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
KillSignal=SIGINT
TimeoutSec=infinity
RuntimeDirectory=${REC}-pg
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=yes
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
}
render_svc_unit() {
	cat <<EOF
[Unit]
Description=Records service for ${ORG}
Requires=${PG_UNIT}
After=${PG_UNIT}

[Service]
Type=simple
User=${REC}
Group=${REC}
RuntimeDirectory=${REC}
RuntimeDirectoryMode=0750
UMask=0007
# The socket directory belongs to the org's agents' group, setgid, so the socket the service
# creates in it is theirs to connect to; the service itself joins no group of theirs.
ExecStartPre=+/bin/chgrp ${ORG_GROUP} ${SVCSOCK}
ExecStartPre=+/bin/chmod 2750 ${SVCSOCK}
ExecStart=${OPT_NODE} ${MAIN} serve --config ${CFG}
# The service re-reads roles from ${CFG} on SIGHUP (after an --operator grant).
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=2
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=${HOME_DIR} ${SVCSOCK}

[Install]
WantedBy=multi-user.target
EOF
}

if [[ -n $PRINT ]]; then
	case $PRINT in
	hba) render_hba ;;
	ident) render_ident ;;
	conf) render_conf ;;
	service-json) render_service_json ;;
	operator-edit-js) render_operator_edit_js ;;
	units)
		printf '# /etc/systemd/system/%s\n' "$PG_UNIT"
		render_pg_unit
		printf '\n# /etc/systemd/system/%s\n' "$SVC_UNIT"
		render_svc_unit
		;;
	esac
	exit 0
fi

# --- the one path for mutations and file writes ---
show() {
	local out="" a
	for a in "$@"; do
		if [[ $a =~ ^[A-Za-z0-9_./:=@%+,-]+$ ]]; then out+=" $a"; else out+=" '${a//\'/\'\\\'\'}'"; fi
	done
	printf '%s\n' "${out# }"
}
run() {
	RUN_CMD=$(show "$@")
	printf '+ %s\n' "$RUN_CMD"
	if ((DRY)); then RUN_CMD=""; return 0; fi
	# RUN_CMD stays set on failure so on_error names the command.
	"$@" || return
	RUN_CMD=""
}
# run, but a failure (e.g. already-absent state) is reported and ignored.
try_run() { run "$@" || { RUN_CMD=""; echo "= ignored: failed, already absent?"; }; }
step() { # N TITLE
	STEP="$1 ($2)"
	echo "## $1. $2"
}
_append() { printf '%s\n' "$2" >>"$1"; }
CHANGED=0 CONTENT_CHANGED=0
write_file() { # PATH MODE OWNER CONTENT; idempotent: writes only when content, mode or owner differ
	local path=$1 mode=$2 owner=$3 content=$4 tmp
	if [[ -f $path && -r $path ]] && [[ "$(cat "$path")" == "$content" ]]; then
		if [[ $(stat -c '%a %U:%G' "$path") == "$(printf '%o' "$((8#$mode))") $owner" ]]; then
			printf '= unchanged %s\n' "$path"
		else
			run chmod "$mode" "$path"
			run chown "$owner" "$path"
			CHANGED=1
		fi
		return 0
	fi
	CHANGED=1 CONTENT_CHANGED=1
	printf '+ write %s (mode %s, owner %s)\n' "$path" "$mode" "$owner"
	if ((DRY)); then
		printf '%s\n' "$content" | sed 's/^/    | /'
		return 0
	fi
	# P3-3: the temp file lives next to its target, never in /tmp.
	tmp=$(mktemp "${path}.XXXXXX")
	printf '%s\n' "$content" >"$tmp"
	if ! install -m "$mode" -o "${owner%%:*}" -g "${owner#*:}" "$tmp" "$path"; then
		rm -f "$tmp"
		die "cannot install ${path}"
	fi
	rm -f "$tmp"
}
as_rec() { runuser -u "$REC" -- "$@"; }
# The org user's relay credential directory; "~user/..." (print only) when the user has no home here.
relay_dir() {
	local home
	home=$(getent passwd "$ORG_USER" | cut -d: -f6 || true)
	printf '%s/.config/%s-records\n' "${home:-~$ORG_USER}" "$ORG"
}

# The rollback list, in order. RB_MODE=print lists the commands; RB_MODE=run executes them through run.
RB_MODE=print
rb_step() { [[ $RB_MODE == print ]] || step "$1" "$2"; }
# ponytail: --help with <org> placeholders prints the commands unquoted; they are a template, not shell.
rb_show() { if [[ $ORG == "<org>" || $ORG_USER == "<org-user>" ]]; then printf '  %s\n' "$*"; else printf '  %s\n' "$(show "$@")"; fi; }
rb() { if [[ $RB_MODE == print ]]; then rb_show "$@"; else run "$@"; fi; }
rb_try() { if [[ $RB_MODE == print ]]; then rb_show "$@"; else try_run "$@"; fi; }
# P2-4: nothing is deleted while either unit may still run. An absent unit reports inactive or unknown.
rb_stopped() {
	local u s note="each must be inactive, failed or unknown; otherwise nothing is deleted"
	if [[ $RB_MODE == print ]]; then
		printf '  systemctl is-active %s %s  (%s)\n' "$SVC_UNIT" "$PG_UNIT" "$note"
		return 0
	fi
	if ((DRY)); then
		printf '? systemctl is-active %s %s  (%s)\n' "$SVC_UNIT" "$PG_UNIT" "$note"
		return 0
	fi
	for u in "$SVC_UNIT" "$PG_UNIT"; do
		s=$(systemctl is-active "$u" 2>/dev/null || true)
		case $s in
		inactive | failed | unknown) echo "= ${u} is ${s}" ;;
		*) die "refused: ${u} is still ${s:-in an unknown state}; nothing was deleted" ;;
		esac
	done
}
rollback_steps() {
	local rdir
	rb_step R1 "stop both services, confirm they stopped, disable them"
	rb_try systemctl stop "$SVC_UNIT" "$PG_UNIT"
	rb_stopped
	rb_try systemctl disable "$SVC_UNIT" "$PG_UNIT"
	rb_step R2 "remove the systemd units"
	rb rm -f "$UNIT_DIR/$SVC_UNIT" "$UNIT_DIR/$PG_UNIT"
	rb systemctl daemon-reload
	rb_step R3 "remove ${CONF_DIR}"
	rb rm -rf "$CONF_DIR"
	rb_step R4 "remove ${HOME_DIR} (record database, credentials, status)"
	[[ $RB_MODE == print ]] || echo "!! ${DELETE_WARNING}"
	rb rm -rf "$HOME_DIR"
	rb_step R5 "remove the runtime directories"
	rb rm -rf "$SVCSOCK" "$PGSOCK"
	rb_step R6 "remove ${OPT}"
	rb rm -rf "$OPT"
	rb_step R7 "remove the relay credential directory of ${ORG_USER}"
	rdir=$(relay_dir)
	if [[ $RB_MODE == run && $rdir == "~"* ]] && ((!DRY)); then
		echo "= ${ORG_USER} has no home directory here; nothing to remove"
	else
		rb runuser -u "$ORG_USER" -- rm -rf "$rdir"
	fi
	rb_step R8 "remove the OS user ${REC}"
	# A dry run lists every command, like the rm -rf lines, whatever exists on this host.
	if [[ $RB_MODE == run ]] && ((!DRY)) && ! id -u "$REC" >/dev/null 2>&1; then
		echo "= user ${REC} does not exist"
	else
		rb userdel "$REC"
	fi
}

root_steps() {
	cat <<'EOF'
  0. mktemp -d /run/<org>-records-stage.XXXXXX (root, 0700; removed on exit); copy --node and the bundle into it once each; refuse (nothing changed) unless each staged copy's sha256 equals --node-sha256/--bundle-sha256
  1. useradd the <org>-records system user (skipped if it exists)
  2. if no /usr/lib/postgresql/<N>/bin has initdb and postgres: without /etc/postgresql-common/createcluster.conf, write createcluster.d/99-smarty-records.conf (create_main_cluster = false), then apt-get install postgresql (distro; no repository added); use the highest N (16 or newer, or --pg-bin); install the verified staged node (0755) and service-main.mjs (0644) root-owned in /opt/<org>-records; check <org>-records runs it with no modules
  3. create /var/lib/<org>-records/{,pg,status,credentials}, /run/<org>-records-pg, /run/<org>-records and /etc/<org>-records with fixed owners and modes
  4. initdb the cluster as <org>-records if absent (an existing cluster keeps its own major, from PG_VERSION); write pg_hba.conf, pg_ident.conf, conf.d/records.conf; append include_dir to postgresql.conf
  5. write /etc/<org>-records/service.json if absent (never overwritten: it holds granted roles)
  6. write both systemd units; daemon-reload; enable --now <org>-records-pg.service (restart if its config or unit changed and it was running); wait for pg_isready
  7. createdb records if absent; create role records_service if absent; run migrations as <org>-records; enable --now <org>-records.service (restart if node, bundle or unit changed and it was running)
  8. per --operator: refuse an id that holds another role; add the role to service.json, issue its credential with --reissue if absent (sha256(id).json, id, role and issuer checked) as <org>-records; relay: <org-user> itself writes it 0600 to ~<org-user>/.config/<org>-records; reload the service if active
  9. verify owners and modes; check that <org-user> cannot reach PostgreSQL; check both units active, pg_isready, the python3 peer audit, the service socket and no TCP listener on the records port (changes nothing)
EOF
}
print_help() {
	usage
	echo
	echo "WHAT IT CHANGES: as root, in order, one line per step"
	# ponytail: the header's ROOT STEPS, repeated here because nothing may read the script's own path (a test keeps them equal).
	root_steps | sed "s/<org-user>/${ORG_USER}/g; s/<org>/${ORG}/g; s/<N>/${PG_MAJOR:-<N>}/g"
	cat <<EOF

IDEMPOTENCY: a second run with the same flags
  0. a fresh staging directory; the digests are checked again (removed on exit)
  1. skipped: user ${REC} exists
  2. apt-get skipped (PostgreSQL ${PG_MAJOR:-<N>} found); the verified node and service-main.mjs installed only when content differs
  3. unchanged: the same directories, owners and modes are reapplied
  4. initdb skipped (cluster exists; an existing cluster keeps its own major); pg_hba.conf, pg_ident.conf, records.conf rewritten only when content
     differs; include_dir appended only once; PostgreSQL restarted only when its config changed
  5. skipped: service.json exists (never overwritten)
  6. units rewritten only when content differs; daemon-reload and enable --now leave running units unchanged
     (a running PostgreSQL unit is restarted when its config or unit changed)
  7. createdb and CREATE ROLE skipped (exist); migrate applies only unapplied migrations; enable --now unchanged
     (a running service is restarted when node, the bundle or its unit changed)
  8. an id that holds another role refused; a role added to service.json only when missing; a credential issued (--reissue) only when absent; the relay
     copy rewritten with the same content; an active service reloaded, so an interrupted earlier grant takes effect
  9. checks only; changes nothing

SUCCESS LINE: printed last by a real install, only after the step 9 checks pass; never by --dry-run
  ${SUCCESS_LINE}
  checks: systemctl is-active ${SVC_UNIT} and ${PG_UNIT}; pg_isready as ${REC};
          python3 ctypes getsockopt as ${REC} (peer audit); ${ORG_USER} cannot reach PostgreSQL; test -S ${SOCKET};
          ss -Hltnp shows no TCP listener on :${PORT}
LIMIT: WAL accumulates until the WAL-G archive step; run it before sustained use and watch disk.

ROLLBACK: as root; preview first with: ${0##*/} --org ${ORG} --org-user ${ORG_USER} --rollback --dry-run
  ${0##*/} --org ${ORG} --org-user ${ORG_USER} --rollback --yes-delete-records
runs these commands in this order (the PostgreSQL packages stay installed; optional afterwards:
apt-get remove postgresql postgresql-${PG_MAJOR:-<N>}):
${DELETE_WARNING}
EOF
	rollback_steps
}
if ((HELP)); then
	print_help
	exit 0
fi

((DRY)) && echo "# DRY RUN: nothing below is executed or written."
cd /

if ((ROLLBACK)); then
	echo "!! ${DELETE_WARNING}"
	if ((!DRY && !YES_DELETE)); then
		die "refused: a real --rollback deletes ${HOME_DIR} (the record database). Take the backup above, then rerun with --yes-delete-records"
	fi
	RB_MODE=run
	rollback_steps
	STEP=""
	echo "${APT_NOTE}"
	((DRY)) && echo "# DRY RUN: nothing was executed." || echo "OK: rollback done"
	exit 0
fi

# P2-2 preflight: every input (PostgreSQL, sources, digests) is checked before anything changes, dry run included.
# F20: an existing cluster's major must be installed.
[[ -z $PG_REFUSAL ]] || die "$PG_REFUSAL"
candidate=""
if [[ -n $PG_BIN_GIVEN ]]; then
	[[ -x $PG_BIN/initdb ]] || die "${PG_BIN}/initdb not found. Install PostgreSQL 16 or newer there, or omit --pg-bin to use the distro's postgresql package; nothing was changed"
	[[ -n $PG_MAJOR ]] || die "cannot read the PostgreSQL major version from ${PG_BIN}/postgres --version; nothing was changed"
elif [[ -z $PG_MAJOR ]]; then
	# The distro's metapackage (Ubuntu noble: 16); no repository is added. apt-cache changes nothing.
	candidate=$(apt-cache policy postgresql 2>/dev/null | awk '$1 == "Candidate:" { print $2 }')
	[[ -n $candidate && $candidate != "(none)" ]] ||
		die "no apt candidate for the distro package postgresql; check the apt sources (apt-get update), then rerun."
fi
# F13: the service is one self-contained file (every package inlined) plus node. A symlink source is refused.
SRC_MAIN="${PACKAGE}/${SRC_MAIN_REL}"
for p in "$NODE" "$SRC_MAIN"; do
	[[ ! -L $p ]] || die "${p} is a symlink: refused. Pass the real file (readlink -f)"
	if [[ ! -e $p ]]; then
		((DRY)) || die "${p} not found: pass --package-root with a built package (bun run build) and --node; nothing was changed"
		echo "! ${p} not found: build the package (bun run build) before the real run"
	else
		[[ -f $p ]] || die "${p} is not a regular file: refused"
	fi
done

# P1-1: the sources are read once each into a fresh root-only directory; only staged copies whose sha256 is
# the approved one go further. Every later step uses the staged, then installed, copies, never the sources.
step 0 "Stage and verify node and the bundle"
STAGE="${T}/run/${REC}-stage.XXXXXX"
if ((DRY)); then
	echo "? STAGE=\$(mktemp -d ${STAGE})  (root-only, 0700; removed on exit)"
	echo "? umask 077; cat $(show "$NODE") > ${STAGE}/node; cat $(show "$SRC_MAIN") > ${STAGE}/service-main.mjs  (each source read once)"
	echo "? sha256sum ${STAGE}/node ${STAGE}/service-main.mjs  (must equal ${NODE_SHA} and ${BUNDLE_SHA}; otherwise refused, nothing changed)"
	digest_check() { # NAME FILE WANT
		if [[ -f $2 && -r $2 ]]; then
			if [[ $(sha_of "$2") == "$3" ]]; then echo "$1 ok"; else echo "$1 MISMATCH"; fi
		else
			echo "$1 not readable"
		fi
	}
	echo "digest check: $(digest_check bundle "$SRC_MAIN" "$BUNDLE_SHA"), $(digest_check node "$NODE" "$NODE_SHA")  (a real run refuses a MISMATCH)"
else
	[[ -z $T ]] || mkdir -p "${T}/run"
	STAGE=$(mktemp -d "$STAGE")
	trap 'rm -rf -- "$STAGE"' EXIT
	(umask 077 && cat -- "$NODE" >"${STAGE}/node" && cat -- "$SRC_MAIN" >"${STAGE}/service-main.mjs")
	echo "+ staged ${NODE} as ${STAGE}/node and ${SRC_MAIN} as ${STAGE}/service-main.mjs"
	got_node=$(sha_of "${STAGE}/node") got_main=$(sha_of "${STAGE}/service-main.mjs")
	STEP=""
	[[ $got_node == "$NODE_SHA" ]] || die "refused: staged node sha256 ${got_node} != approved ${NODE_SHA}; nothing was changed"
	[[ $got_main == "$BUNDLE_SHA" ]] || die "refused: staged bundle sha256 ${got_main} != approved ${BUNDLE_SHA}; nothing was changed"
	echo "OK: the staged node and bundle match the approved sha256"
fi

step 1 "OS user ${REC}"
if id -u "$REC" >/dev/null 2>&1; then
	echo "= user ${REC} exists"
else
	run useradd --system --user-group --no-create-home --home-dir "$HOME_DIR" --shell /usr/sbin/nologin "$REC"
fi

step 2 "Prerequisites: PostgreSQL ${PG_MAJOR:-<detected after install>}, node and the package under ${OPT}"
if [[ -n $PG_BIN_GIVEN ]]; then
	echo "= PostgreSQL ${PG_MAJOR} found at ${PG_BIN}"
elif [[ -n $PG_MAJOR ]]; then
	if [[ -n $CLUSTER_MAJOR ]]; then
		echo "= PostgreSQL ${PG_MAJOR} found at ${PG_BIN} (the existing cluster's major, from ${DATA}/PG_VERSION)"
	else
		echo "= PostgreSQL ${PG_MAJOR} found at ${PG_BIN} (the highest ${PG_BASE}/<N>/bin with initdb and postgres)"
	fi
else
	echo "  note: the distro package postgresql ${candidate}. Unless disabled, it creates a cluster postgresql@<N>-main;"
	echo "        an existing one stays as it is: this setup does not touch it (disable it yourself if you do not use it)."
	# P2-1: on a host with no createcluster.conf yet, no new distro main cluster (and so no TCP listener) is created.
	# A drop-in (postgresql-common >= 250) leaves the package's own conffile and every existing setting alone.
	CC="${T}/etc/postgresql-common"
	if [[ -e $CC/createcluster.conf ]]; then
		echo "= ${CC}/createcluster.conf exists: kept as is (it decides whether a main cluster is created)"
	else
		echo "  ${CC}/createcluster.conf is absent: a drop-in stops apt from creating a new main cluster"
		run install -d -m 0755 -o root -g root "$CC/createcluster.d"
		write_file "$CC/createcluster.d/99-smarty-records.conf" 0644 root:root "# Written by records-paul-steps.sh: no new distro main cluster on install (it would listen on TCP).
create_main_cluster = false"
	fi
	run apt-get install -y postgresql
	if ((DRY)); then
		echo "= PostgreSQL <detected after install>: the highest ${PG_BASE}/<N>/bin with initdb and postgres"
	else
		detect_pg
		[[ -n $PG_MAJOR ]] || die "apt-get install postgresql left no ${PG_BASE}/<N>/bin with initdb and postgres"
		check_major
		pg_derived
		echo "= PostgreSQL ${PG_MAJOR} installed at ${PG_BIN} (an existing postgresql@${PG_MAJOR}-main is left alone)"
	fi
fi

# Only the two verified staged files are installed; nothing under ${OPT} comes from the sources directly.
BIN_CHANGED=0
run install -d -m 0755 -o root -g root "$OPT"
if cmp -s "${STAGE}/node" "$OPT_NODE"; then
	echo "= ${OPT_NODE} matches the verified staged node"
else
	run install -m 0755 -o root -g root "${STAGE}/node" "$OPT_NODE"
	BIN_CHANGED=1
fi
if cmp -s "${STAGE}/service-main.mjs" "$MAIN"; then
	echo "= ${MAIN} matches the verified staged bundle"
else
	run install -m 0644 -o root -g root "${STAGE}/service-main.mjs" "$MAIN"
	BIN_CHANGED=1
fi
if ((!DRY)); then
	[[ $(sha_of "$OPT_NODE") == "$NODE_SHA" && $(sha_of "$MAIN") == "$BUNDLE_SHA" ]] ||
		die "${OPT_NODE} or ${MAIN} does not match the approved sha256 after install"
fi
if ((DRY)); then
	echo "? find ${OPT} -type l  (must print nothing)"
	echo "? runuser -u ${REC} -- ${OPT_NODE} ${MAIN}  (must exit 2, usage: it runs with no external modules)"
	echo "? runuser -u ${REC} -- test -x ${PG_BIN}/initdb"
else
	links=$(find "$OPT" -type l) || die "cannot list ${OPT}"
	[[ -z $links ]] || die "symlinks under ${OPT} (remove them, then rerun): ${links//$'\n'/ }"
	rc=0
	as_rec "$OPT_NODE" "$MAIN" >/dev/null 2>&1 || rc=$?
	((rc == 2)) || die "${REC} cannot run ${OPT_NODE} ${MAIN} (exit ${rc}, expected 2: usage); check the modes under ${OPT}"
	as_rec test -x "$PG_BIN/initdb" || die "${REC} cannot run ${PG_BIN}/initdb"
	echo "OK: ${REC} runs ${OPT_NODE} ${MAIN} with no external modules"
fi

step 3 "Directories"
run install -d -m 0755 -o "$REC" -g "$REC" "$HOME_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$DATA"
run install -d -m 0755 -o "$REC" -g "$REC" "$STATUS_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$CRED_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$PGSOCK"
run install -d -m 2750 -o "$REC" -g "$ORG_GROUP" "$SVCSOCK"
run install -d -m 0750 -o root -g "$REC" "$CONF_DIR"

step 4 "Cluster"
FRESH=0
if [[ -f $DATA/PG_VERSION ]]; then
	echo "= cluster exists at ${DATA}"
else
	FRESH=1
	run runuser -u "$REC" -- "$PG_BIN/initdb" -D "$DATA" -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C
fi
CHANGED=0
write_file "$DATA/pg_hba.conf" 0600 "$REC:$REC" "$(render_hba)"
write_file "$DATA/pg_ident.conf" 0600 "$REC:$REC" "$(render_ident)"
run install -d -m 0700 -o "$REC" -g "$REC" "$DATA/conf.d"
write_file "$DATA/conf.d/records.conf" 0600 "$REC:$REC" "$(render_conf)"
if [[ -r $DATA/postgresql.conf ]] && grep -qxF "include_dir = 'conf.d'" "$DATA/postgresql.conf"; then
	echo "= include_dir already in postgresql.conf"
else
	run _append "$DATA/postgresql.conf" "include_dir = 'conf.d'"
	CHANGED=1
fi
PG_CONF_CHANGED=$CHANGED

step 5 "Service config"
if [[ -f $CFG ]]; then
	echo "= ${CFG} exists; not overwritten (it holds granted roles). Compare with --print service-json."
else
	write_file "$CFG" 0640 "root:$REC" "$(render_service_json)"
fi

step 6 "systemd units"
# A unit counts as changed when its content does (a mode or owner fix needs no restart).
CONTENT_CHANGED=0
write_file "$UNIT_DIR/$PG_UNIT" 0644 root:root "$(render_pg_unit)"
PG_UNIT_CHANGED=$CONTENT_CHANGED CONTENT_CHANGED=0
write_file "$UNIT_DIR/$SVC_UNIT" 0644 root:root "$(render_svc_unit)"
SVC_UNIT_CHANGED=$CONTENT_CHANGED
run systemctl daemon-reload
# P2-3: enable --now does not restart a running unit; a changed config, unit or binary needs an explicit restart.
was_active() { ((!DRY)) && systemctl is-active --quiet "$1"; }
PG_WAS_ACTIVE=0
if was_active "$PG_UNIT"; then PG_WAS_ACTIVE=1; fi
run systemctl enable --now "$PG_UNIT"
if ((DRY)); then
	((!(PG_CONF_CHANGED || PG_UNIT_CHANGED) || FRESH)) || echo "? if ${PG_UNIT} was already active: + systemctl restart ${PG_UNIT}  (its config or unit changed)"
elif (((PG_CONF_CHANGED || PG_UNIT_CHANGED) && !FRESH && PG_WAS_ACTIVE)); then
	run systemctl restart "$PG_UNIT"
fi
PSQL=(runuser -u "$REC" -- "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PGSOCK" -p "$PORT" -U postgres)
if ((DRY)); then
	echo "? wait until ${PG_BIN}/pg_isready -h ${PGSOCK} -p ${PORT} succeeds"
else
	for _ in $(seq 60); do as_rec "$PG_BIN/pg_isready" -q -h "$PGSOCK" -p "$PORT" && break; sleep 1; done
	as_rec "$PG_BIN/pg_isready" -q -h "$PGSOCK" -p "$PORT" || die "PostgreSQL did not become ready; see journalctl -u $PG_UNIT"
fi

step 7 "Database, role, migrations"
HAVE_DB=""
((DRY)) || HAVE_DB=$("${PSQL[@]}" -d postgres -tAc "select 1 from pg_database where datname='records'")
if [[ $HAVE_DB == 1 ]]; then
	echo "= database records exists"
else
	run runuser -u "$REC" -- "$PG_BIN/createdb" -h "$PGSOCK" -p "$PORT" -U postgres records
fi
run "${PSQL[@]}" -d records -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='records_service') THEN CREATE ROLE records_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT; END IF; END \$\$"
run runuser -u "$REC" -- "$OPT_NODE" "$MAIN" migrate --config "$CFG"
SVC_WAS_ACTIVE=0
if was_active "$SVC_UNIT"; then SVC_WAS_ACTIVE=1; fi
run systemctl enable --now "$SVC_UNIT"
# A SIGHUP before Node has loaded the service would end it: callers wait for the socket first.
wait_socket() {
	for _ in $(seq 1 300); do [[ -S $SOCKET ]] && return 0; sleep 0.1; done
	[[ -S $SOCKET ]]
}
if ((DRY)); then
	echo "? if node, the bundle or ${SVC_UNIT} changed and it was already active: + systemctl restart ${SVC_UNIT}, then wait up to 30 s for ${SOCKET}"
elif ((SVC_WAS_ACTIVE && (BIN_CHANGED || SVC_UNIT_CHANGED))); then
	run systemctl restart "$SVC_UNIT"
	wait_socket || die "${SOCKET} did not appear within 30 s after restarting ${SVC_UNIT}; see journalctl -u ${SVC_UNIT}"
fi

step 8 "Operator principals"
# F18: the stored credential must name the requested id and role and come from the installer's issue command.
CHECK_ID_JS='const [f, id, role] = process.argv.slice(1); const c = JSON.parse(require("fs").readFileSync(f, "utf8")); if (c.id !== id || c.role !== role || c.issuedBy !== "installer") { console.error(`${f}: not ${id} as ${role} issued by the installer`); process.exit(1); }'
# F18: before any grant, issue or delivery, refuse every id that service.json already lists under another role.
for spec in ${OPERATORS[@]+"${OPERATORS[@]}"}; do
	role=${spec%%:*} id=${spec#*:}
	[[ $id == *:* ]] || id=$spec
	if ((DRY)); then
		printf '? %s -e "$(%s --print operator-edit-js)" %s check  (refused if %s holds another role)\n' "$OPT_NODE" "${0##*/}" "$(show "$CFG" "$role" "$id")" "$id"
		continue
	fi
	rc=0
	"$OPT_NODE" -e "$(render_operator_edit_js)" "$CFG" "$role" "$id" check >/dev/null || rc=$?
	((rc != 3)) || die "refused: ${id} already holds another role in ${CFG}; an operator id holds exactly one role. Nothing was granted, issued or delivered."
	((rc == 0)) || die "could not check the roles of ${id} in ${CFG} (exit ${rc})"
done
for spec in ${OPERATORS[@]+"${OPERATORS[@]}"}; do
	role=${spec%%:*} id=${spec#*:}
	[[ $id == *:* ]] || id=$spec
	# F14: the name is the sha256 of the whole id (injective in practice): team:import_job and
	# team_import:job never share a file.
	cred="$CRED_DIR/$(printf %s "$id" | sha256sum | cut -c1-64).json"
	printf '+ %s -e "$(%s --print operator-edit-js)" %s\n' "$OPT_NODE" "${0##*/}" "$(show "$CFG" "$role" "$id")"
	if ((!DRY)); then
		result=$("$OPT_NODE" -e "$(render_operator_edit_js)" "$CFG" "$role" "$id") || die "could not grant ${role} to ${id} in ${CFG}"
		echo "  ${result}: roles.${role} has ${id}"
	fi
	if [[ -f $cred ]]; then
		echo "= ${cred} exists; not reissued"
	else
		# The issue command writes the file 0600 with O_EXCL, as ${REC}, in its 0700 directory. Never copy it elsewhere.
		# P2-3: --reissue rotates the token of an existing principal with this id and role (a run interrupted
		# after the database insert, before the file), and inserts it otherwise.
		run runuser -u "$REC" -- "$OPT_NODE" "$MAIN" issue --config "$CFG" --id "$id" --role "$role" --out "$cred" --reissue
	fi
	# Before the credential is used or delivered, its stored principal must be the requested one.
	if ((DRY)); then
		echo "? runuser -u ${REC} -- ${OPT_NODE} -e '${CHECK_ID_JS}' ${cred} ${id} ${role}  (stored .id, .role, .issuedBy must equal ${id}, ${role}, installer)"
	else
		as_rec "$OPT_NODE" -e "$CHECK_ID_JS" "$cred" "$id" "$role" || die "${cred} does not hold principal ${id} with role ${role} issued by the installer; refused"
	fi
	if [[ $role == relay ]]; then
		# The relay publishes nudges on the org's mesh, which only the org user can write: its credential
		# goes to that user (0600). Its protection ends at the same-uid limit (docs/records.md, Trust boundary).
		# F12: root never writes under the org user's home. The org user creates the directory and the
		# file itself, so a symlink there reaches only what that user already owns; root feeds the token on stdin.
		relay_dir=$(relay_dir)
		if [[ $relay_dir == "~"* ]]; then ((DRY)) || die "no home directory for ${ORG_USER}"; fi
		run runuser -u "$ORG_USER" -- install -d -m 0700 "$relay_dir"
		deliver=(runuser -u "$ORG_USER" -- sh -c 'umask 077 && cat > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh "${relay_dir}/relay.json")
		RUN_CMD="$(show "${deliver[@]}") < $(show "$cred")"
		printf '+ %s\n' "$RUN_CMD"
		if ((!DRY)); then "${deliver[@]}" <"$cred"; fi
		RUN_CMD=""
		cat <<EOF
  -> ${id} (relay): Fabric of ${ORG_USER} uses it with "records": { "enabled": true, "socket": "${SOCKET}", "relayCredentialFile": "${relay_dir}/relay.json" }.
EOF
	else
		cat <<EOF
  -> ${id} (${role}): its component runs as ${REC} (a sibling unit with User=${REC}, or inside
     ${SVC_UNIT}) with "credentialFile": "${cred}".
EOF
	fi
done
if ((${#OPERATORS[@]})); then
	if ((DRY)); then
		echo "? if ${SVC_UNIT} is active: wait up to 30 s for ${SOCKET} (the service's SIGHUP handler is in place by then), then + systemctl reload ${SVC_UNIT}  (always, so an interrupted earlier grant takes effect)"
	elif systemctl is-active --quiet "$SVC_UNIT"; then
		# P2-3: reload even when no role changed here: an earlier run may have written a role and stopped before its reload.
		wait_socket || die "${SOCKET} did not appear within 30 s; not reloading ${SVC_UNIT}"
		run systemctl reload "$SVC_UNIT"
	else
		echo "= no reload: ${SVC_UNIT} not active (it reads roles at start)"
	fi
fi

step 9 "Verification"
if ((DRY)); then
	echo "? stat -c '%A %U:%G %n' $OPT_NODE $MAIN $HOME_DIR $DATA $STATUS_DIR $CRED_DIR $PGSOCK $SVCSOCK $CONF_DIR"
	echo "? runuser -u ${ORG_USER} -- ${PG_BIN}/psql -h ${PGSOCK} -p ${PORT} -U postgres -d records -c 'select 1'  (expected to fail: agent cannot reach PostgreSQL)"
else
	stat -c '%A %U:%G %n' "$OPT_NODE" "$MAIN" "$HOME_DIR" "$DATA" "$STATUS_DIR" "$CRED_DIR" "$PGSOCK" "$SVCSOCK" "$CONF_DIR"
	if runuser -u "$ORG_USER" -- "$PG_BIN/psql" -X -h "$PGSOCK" -p "$PORT" -U postgres -d records -c 'select 1' >/dev/null 2>&1; then
		die "FAIL: ${ORG_USER} reached PostgreSQL at ${PGSOCK}; the record store is not isolated"
	fi
	echo "OK: agent cannot reach PostgreSQL"
fi
# The success line's own checks; each failure stops the script here with the step 9 report.
success_check() { # WHAT CMD...
	local what=$1
	shift
	if ((DRY)); then
		echo "? $(show "$@")  (${what})"
	else
		"$@" || die "check failed (${what}): $(show "$@")"
		echo "OK: ${what}"
	fi
}
success_check "${SVC_UNIT} active" systemctl is-active --quiet "$SVC_UNIT"
success_check "${PG_UNIT} active" systemctl is-active --quiet "$PG_UNIT"
success_check "PostgreSQL ready" runuser -u "$REC" -- "$PG_BIN/pg_isready" -h "$PGSOCK" -p "$PORT"
# The service reads SO_PEERCRED through python3 and ctypes: the peer audit needs both, as ${REC}.
success_check "peer audit (python3 ctypes)" runuser -u "$REC" -- python3 -c 'import ctypes; ctypes.CDLL(None).getsockopt'
success_check "service socket" test -S "$SOCKET"
# P2-1: the records cluster listens on its Unix socket only. Other PostgreSQL TCP listeners are reported, not changed.
no_tcp_listener() {
	local out local_addr pg
	out=$(ss -Hltnp) || return 1
	while read -r _ _ _ local_addr _; do
		if [[ $local_addr == *":${PORT}" ]]; then
			echo "a TCP listener on ${local_addr}: $(grep -F "${local_addr}" <<<"$out" | head -n1)" >&2
			return 1
		fi
	done <<<"$out"
	pg=$(grep -i postgres <<<"$out" || true)
	[[ -z $pg ]] || printf 'WARNING: PostgreSQL TCP listeners on other ports (not the records cluster; left as they are):\n%s\n' "$pg"
}
if ((DRY)); then
	echo "? ss -Hltnp  (no TCP listener on :${PORT}; PostgreSQL TCP listeners on other ports only warn)"
else
	success_check "no TCP listener on :${PORT}" no_tcp_listener
fi
cat <<EOF

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "enabled": true, "socket": "${SOCKET}" } }
EOF
STEP=""
cat <<EOF

## ROLLBACK
To undo this install later, run (as root) '${0##*/} --org ${ORG} --org-user ${ORG_USER} --rollback --yes-delete-records',
which runs these commands in this order:
${DELETE_WARNING}
EOF
rollback_steps
echo "${APT_NOTE}"
if ((DRY)); then
	echo "(dry run: the success line C10_RECORDS_INSTALLED ... is printed only by a real run after its checks)"
else
	echo "${SUCCESS_LINE}"
fi
