#!/usr/bin/env bash
# One-time, idempotent host setup for an org's record store (smarty-dev#754 C10/C13, pi-fabric#103 F1).
# PostgreSQL runs as its own OS user <org>-records, which owns the cluster. The org's agents
# (OS user <org-user>) reach only the records service socket, never PostgreSQL.
#
#   sudo scripts/records-paul-steps.sh --org smarty-pants --org-user paul [--dry-run]
#   scripts/records-paul-steps.sh --org smarty-pants --org-user paul --print hba|ident|conf|service-json|units|operator-edit-js
# --node (the node binary) and --package (a built checkout: dist/records-service/service-main.mjs, a
# self-contained bundle) are SOURCES: the script copies the two files to root-owned
# /opt/<org>-records/{node,service-main.mjs}, and the units and every later command use only those copies.
#   sudo scripts/records-paul-steps.sh --org smarty-pants --org-user paul --rollback [--dry-run] [--yes-delete-records]
#
# ROOT STEPS (one line each):
#  1. useradd the <org>-records system user (skipped if it exists)
#  2. apt-get install postgresql-17 if missing; install node (0755) and the self-contained service-main.mjs (0644) root-owned in /opt/<org>-records (no symlinks); check <org>-records runs it with no modules
#  3. create /var/lib/<org>-records/{,pg,status,credentials}, /run/<org>-records-pg, /run/<org>-records and /etc/<org>-records with fixed owners and modes
#  4. initdb the cluster as <org>-records if absent; write pg_hba.conf, pg_ident.conf, conf.d/records.conf; append include_dir to postgresql.conf
#  5. write /etc/<org>-records/service.json if absent (never overwritten: it holds granted roles)
#  6. write both systemd units; daemon-reload; enable --now <org>-records-pg.service (restart if its config changed); wait for pg_isready
#  7. createdb records if absent; create role records_service if absent; run migrations as <org>-records; enable --now <org>-records.service
#  8. per --operator: add the role to service.json, issue its credential (sha256(id).json, id checked) as <org>-records; relay: <org-user> itself writes it 0600 to ~<org-user>/.config/<org>-records; reload the service if roles changed
#  9. verify owners and modes; check that <org-user> cannot reach PostgreSQL; check both units active, pg_isready, the python3 peer audit and the service socket (changes nothing)
# ROLLBACK STEPS (--rollback; root; a real rollback needs --yes-delete-records; postgresql-17 stays installed):
#  R1. systemctl disable --now <org>-records.service <org>-records-pg.service (failure ignored: already absent)
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
#   C10_RECORDS_INSTALLED org=<org> user=<org>-records cluster=17/<org>-records unit=active peer-audit=ok
# printed last, only after the step 9 checks pass. A dry run never prints it.
# TEST ONLY: RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST=1 skips the root check so tests can run the real
# (non-dry) path against PATH fakes; with it, RECORDS_PAUL_STEPS_TEST_ROOT=DIR prefixes every system path.
# Never set either on a real host.
set -Eeuo pipefail

usage() {
	cat <<'EOF'
usage: records-paul-steps.sh --org <org> --org-user <orguser> [--pg-bin DIR] [--node BIN]
         [--package DIR] [--origin NAME] [--port 5433] [--operator ROLE:ID]... [--dry-run]
         [--print hba|ident|conf|service-json|units|operator-edit-js]
         [--rollback [--yes-delete-records]]
  -h, --help          print this help, what the script changes, idempotency, the success line and the
                      rollback commands (with --org/--org-user filled in when given); needs no root
  --rollback          undo the install in reverse order (keeps postgresql-17). A real rollback deletes the
                      record database and needs --yes-delete-records; take the printed backup first.
  --operator ROLE:ID  grant ROLE (importer|mirror|relay) to principal ID and issue its credential into
                      /var/lib/<org>-records/credentials/ (repeatable). 'importer:github' is
                      principal importer:github; 'importer:fabric:dev1' is principal fabric:dev1.
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

DEFAULT_PG_BIN=/usr/lib/postgresql/17/bin
ORG="" ORG_USER="" PG_BIN=$DEFAULT_PG_BIN NODE="" PACKAGE="" ORIGIN="" PORT=5433
DRY=0 PRINT="" OPERATORS=() YES_DELETE=0
while (($#)); do
	case $1 in
	--org | --org-user | --pg-bin | --node | --package | --origin | --port | --print | --operator)
		(($# >= 2)) || die "$1 needs a value"
		case $1 in
		--org) ORG=$2 ;; --org-user) ORG_USER=$2 ;; --pg-bin) PG_BIN=$2 ;; --node) NODE=$2 ;;
		--package) PACKAGE=$2 ;; --origin) ORIGIN=$2 ;; --port) PORT=$2 ;; --print) PRINT=$2 ;;
		--operator) OPERATORS+=("$2") ;;
		esac
		shift 2
		;;
	--dry-run) DRY=1; shift ;;
	--rollback) ROLLBACK=1; shift ;;
	--yes-delete-records) YES_DELETE=1; shift ;;
	-h | --help) HELP=1; shift ;;
	*) usage >&2; die "unknown argument: $1" ;;
	esac
done

# --- validation (trust boundary: every value below lands in unit files, JSON and root commands) ---
if ((HELP)); then
	# --help needs no root and no other flag; given names are checked, missing ones stay placeholders.
	[[ -z $ORG || ($ORG =~ ^[a-z][a-z0-9-]*$ && ${#ORG} -le 24) ]] || die "invalid --org '$ORG'"
	[[ -z $ORG_USER || ($ORG_USER =~ ^[a-z_][a-z0-9_-]*$ && ${#ORG_USER} -le 32) ]] || die "invalid --org-user '$ORG_USER'"
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
	# ponytail: the default node is resolved (e.g. /usr/bin/node -> nodejs); an explicit --node must not be a symlink.
	[[ -n $NODE ]] || NODE=$(readlink -f "$(command -v node)" 2>/dev/null || true)
	[[ -n $NODE ]] || ((ROLLBACK)) || die "node not found; pass --node BIN"
	[[ -n $PACKAGE ]] || PACKAGE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
	[[ -n $ORIGIN ]] || ORIGIN=$(hostname -s)
	[[ $ORIGIN =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid --origin '$ORIGIN'"
	((!ROLLBACK)) || [[ -z $PRINT ]] || die "--rollback and --print do not combine"
	((ROLLBACK)) || ((!YES_DELETE)) || die "--yes-delete-records only applies to --rollback"
	for p in "$PG_BIN" "$NODE" "$PACKAGE"; do
		[[ -z $p ]] && ((ROLLBACK)) && continue
		[[ $p =~ ^/[A-Za-z0-9._/+-]*$ ]] || die "path must be absolute, without spaces or quotes: '$p'"
	done
	case $PRINT in "" | hba | ident | conf | service-json | units | operator-edit-js) ;; *) die "invalid --print '$PRINT'" ;; esac
	ID_RE='^[a-z][a-z0-9._-]{0,63}:[a-z0-9._@-]{1,64}$'
	for spec in ${OPERATORS[@]+"${OPERATORS[@]}"}; do
		[[ $spec == *:* ]] || die "invalid --operator '$spec': use ROLE:ID, e.g. importer:github"
		role=${spec%%:*} id=${spec#*:}
		# ponytail: 'importer:github' names principal importer:github with role importer; 'importer:fabric:dev1' grants importer to fabric:dev1.
		[[ $id == *:* ]] || id=$spec
		[[ $role == importer || $role == mirror || $role == relay ]] || die "invalid --operator '$spec': ROLE must be importer, mirror or relay"
		[[ $id =~ $ID_RE ]] || die "invalid --operator '$spec': ID must match $ID_RE"
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
SUCCESS_LINE="C10_RECORDS_INSTALLED org=${ORG} user=${REC} cluster=17/${REC} unit=active peer-audit=ok"
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
const [file, role, id] = process.argv.slice(1);
const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
if (cfg.roles === undefined) cfg.roles = {};
if (typeof cfg.roles !== "object" || cfg.roles === null || Array.isArray(cfg.roles)) throw new Error(`${file}: roles is not an object`);
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
CHANGED=0
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
	CHANGED=1
	printf '+ write %s (mode %s, owner %s)\n' "$path" "$mode" "$owner"
	if ((DRY)); then
		printf '%s\n' "$content" | sed 's/^/    | /'
		return 0
	fi
	tmp=$(mktemp)
	printf '%s\n' "$content" >"$tmp"
	install -m "$mode" -o "${owner%%:*}" -g "${owner#*:}" "$tmp" "$path"
	rm -f "$tmp"
}
as_rec() { runuser -u "$REC" -- "$@"; }
# The org user's relay credential directory; "~user/..." (print only) when the user has no home here.
relay_dir() {
	local home
	home=$(getent passwd "$ORG_USER" | cut -d: -f6 || true)
	printf '%s/.config/%s-records\n' "${home:-~$ORG_USER}" "$ORG"
}
BACKUP_CMD="runuser -u ${REC} -- ${PG_BIN}/pg_dump -h ${PGSOCK} -p ${PORT} -U postgres -Fc records > /root/${REC}-backup.dump"
DELETE_WARNING="This deletes the org's record database. Take a backup first: ${BACKUP_CMD}"
APT_NOTE="Note: postgresql-17 stays installed (other software may use it). To remove it too: apt-get remove postgresql-17"

# The rollback list, in order. RB_MODE=print lists the commands; RB_MODE=run executes them through run.
RB_MODE=print
rb_step() { [[ $RB_MODE == print ]] || step "$1" "$2"; }
# ponytail: --help with <org> placeholders prints the commands unquoted; they are a template, not shell.
rb_show() { if [[ $ORG == "<org>" || $ORG_USER == "<org-user>" ]]; then printf '  %s\n' "$*"; else printf '  %s\n' "$(show "$@")"; fi; }
rb() { if [[ $RB_MODE == print ]]; then rb_show "$@"; else run "$@"; fi; }
rb_try() { if [[ $RB_MODE == print ]]; then rb_show "$@"; else try_run "$@"; fi; }
rollback_steps() {
	local rdir
	rb_step R1 "stop and disable both services"
	rb_try systemctl disable --now "$SVC_UNIT" "$PG_UNIT"
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

print_help() {
	usage
	echo
	echo "WHAT IT CHANGES: as root, in order, one line per step"
	# The ROOT STEPS block of this file's header, with the names filled in: one source for both.
	sed -n 's/^#  \([0-9]\.\)/  \1/p' "${BASH_SOURCE[0]}" | sed "s/<org-user>/${ORG_USER}/g; s/<org>/${ORG}/g"
	cat <<EOF

IDEMPOTENCY: a second run with the same flags
  1. skipped: user ${REC} exists
  2. apt-get skipped (PostgreSQL 17 found); node and service-main.mjs copied only when content differs
  3. unchanged: the same directories, owners and modes are reapplied
  4. initdb skipped (cluster exists); pg_hba.conf, pg_ident.conf, records.conf rewritten only when content
     differs; include_dir appended only once; PostgreSQL restarted only when its config changed
  5. skipped: service.json exists (never overwritten)
  6. units rewritten only when content differs; daemon-reload and enable --now leave running units unchanged
  7. createdb and CREATE ROLE skipped (exist); migrate applies only unapplied migrations; enable --now unchanged
  8. a role added to service.json only when missing; a credential issued only when absent; the relay copy
     rewritten with the same content; the service reloaded only when roles changed
  9. checks only; changes nothing

SUCCESS LINE: printed last by a real install, only after the step 9 checks pass; never by --dry-run
  ${SUCCESS_LINE}
  checks: systemctl is-active ${SVC_UNIT} and ${PG_UNIT}; pg_isready as ${REC};
          python3 ctypes getsockopt as ${REC} (peer audit); ${ORG_USER} cannot reach PostgreSQL; test -S ${SOCKET}

ROLLBACK: as root; preview first with: ${0##*/} --org ${ORG} --org-user ${ORG_USER} --rollback --dry-run
  ${0##*/} --org ${ORG} --org-user ${ORG_USER} --rollback --yes-delete-records
runs these commands in this order (postgresql-17 stays installed):
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

step 1 "OS user ${REC}"
if id -u "$REC" >/dev/null 2>&1; then
	echo "= user ${REC} exists"
else
	run useradd --system --user-group --no-create-home --home-dir "$HOME_DIR" --shell /usr/sbin/nologin "$REC"
fi

step 2 "Prerequisites: PostgreSQL 17, node and the package under ${OPT}"
if [[ -x $PG_BIN/initdb ]]; then
	echo "= PostgreSQL found at ${PG_BIN}"
elif [[ $PG_BIN == "$DEFAULT_PG_BIN" ]]; then
	# Non-mutating check first: without the PGDG apt repository, Debian/Ubuntu may have no postgresql-17.
	candidate=$(apt-cache policy postgresql-17 2>/dev/null | awk '$1 == "Candidate:" { print $2 }')
	[[ -n $candidate && $candidate != "(none)" ]] ||
		die "no apt candidate for postgresql-17. Configure the PGDG apt repository first (https://www.postgresql.org/download/linux/debian/), then rerun."
	echo "  note: postgresql-17 ${candidate} from apt. Debian's package creates its own cluster service postgresql@17-main;"
	echo "        this setup does not need it and does not touch it (disable it yourself if you do not use it)."
	run apt-get install -y postgresql-17
else
	die "${PG_BIN}/initdb not found. Install PostgreSQL 17 there, or omit --pg-bin to install postgresql-17 from apt."
fi

# F13: the service is one self-contained file (every package inlined) plus node. Only these two regular
# files are copied; a symlink source is refused, so nothing under ${OPT} can point at agent-writable storage.
SRC_MAIN="${PACKAGE}/dist/records-service/service-main.mjs"
for p in "$NODE" "$SRC_MAIN"; do
	[[ ! -L $p ]] || die "${p} is a symlink: refused. Pass the real file (readlink -f)"
	if [[ ! -e $p ]]; then
		((DRY)) || die "${p} not found: pass --package with a built package (bun run build)"
		echo "! ${p} not found: build the package (bun run build) before the real run"
	else
		[[ -f $p ]] || die "${p} is not a regular file: refused"
	fi
done
run install -d -m 0755 -o root -g root "$OPT"
if cmp -s "$NODE" "$OPT_NODE"; then
	echo "= ${OPT_NODE} matches ${NODE}"
else
	run install -m 0755 -o root -g root "$NODE" "$OPT_NODE"
fi
if cmp -s "$SRC_MAIN" "$MAIN"; then
	echo "= ${MAIN} matches ${SRC_MAIN}"
else
	run install -m 0644 -o root -g root "$SRC_MAIN" "$MAIN"
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
write_file "$UNIT_DIR/$PG_UNIT" 0644 root:root "$(render_pg_unit)"
write_file "$UNIT_DIR/$SVC_UNIT" 0644 root:root "$(render_svc_unit)"
run systemctl daemon-reload
run systemctl enable --now "$PG_UNIT"
if ((PG_CONF_CHANGED && !FRESH)); then run systemctl restart "$PG_UNIT"; fi
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
run systemctl enable --now "$SVC_UNIT"

step 8 "Operator principals"
CHECK_ID_JS='const [f, id] = process.argv.slice(1); if (JSON.parse(require("fs").readFileSync(f, "utf8")).id !== id) { console.error(`${f}: not ${id}`); process.exit(1); }'
POLICY_CHANGED=0
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
		[[ $result == changed ]] && POLICY_CHANGED=1
	fi
	if [[ -f $cred ]]; then
		echo "= ${cred} exists; not reissued"
	else
		# The issue command writes the file 0600 with O_EXCL, as ${REC}, in its 0700 directory. Never copy it elsewhere.
		run runuser -u "$REC" -- "$OPT_NODE" "$MAIN" issue --config "$CFG" --id "$id" --role "$role" --out "$cred"
	fi
	# Before the credential is used or delivered, its stored principal must be the requested one.
	if ((DRY)); then
		echo "? runuser -u ${REC} -- ${OPT_NODE} -e '${CHECK_ID_JS}' ${cred} ${id}  (stored .id must equal ${id})"
	else
		as_rec "$OPT_NODE" -e "$CHECK_ID_JS" "$cred" "$id" || die "${cred} does not hold principal ${id}; refused"
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
  -> ${id} (relay): Fabric of ${ORG_USER} uses it with "records": { "relayCredentialFile": "${relay_dir}/relay.json" }.
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
		echo "? if roles changed and ${SVC_UNIT} is active: wait up to 30 s for ${SOCKET} (the service's SIGHUP handler is in place by then), then + systemctl reload ${SVC_UNIT}"
	elif ((POLICY_CHANGED)) && systemctl is-active --quiet "$SVC_UNIT"; then
		# A SIGHUP before Node has loaded the service would end it: reload only once the socket exists.
		for _ in $(seq 1 300); do [[ -S $SOCKET ]] && break; sleep 0.1; done
		[[ -S $SOCKET ]] || die "${SOCKET} did not appear within 30 s; not reloading ${SVC_UNIT}"
		run systemctl reload "$SVC_UNIT"
	else
		echo "= no reload: roles unchanged or ${SVC_UNIT} not active (it reads roles at start)"
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
cat <<EOF

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "socket": "${SOCKET}" } }
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
