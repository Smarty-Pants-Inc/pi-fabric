#!/usr/bin/env bash
# One-time, idempotent host setup for an org's record store (smarty-dev#754 C10/C13, pi-fabric#103 F1).
# PostgreSQL runs as its own OS user <org>-records, which owns the cluster. The org's agents
# (OS user <org-user>) reach only the records service socket, never PostgreSQL.
#
#   sudo scripts/records-paul-steps.sh --org smarty-pants --org-user paul [--dry-run]
#   scripts/records-paul-steps.sh --org smarty-pants --org-user paul --print hba|ident|conf|service-json|units
set -euo pipefail

usage() {
	cat <<'EOF'
usage: records-paul-steps.sh --org <org> --org-user <orguser> [--pg-bin DIR] [--node BIN]
         [--package DIR] [--origin NAME] [--port 5433] [--issue ID:ROLE]... [--dry-run]
         [--print hba|ident|conf|service-json|units]
EOF
}
die() {
	printf 'records-paul-steps: %s\n' "$*" >&2
	exit 1
}

ORG="" ORG_USER="" PG_BIN=/usr/lib/postgresql/17/bin NODE="" PACKAGE="" ORIGIN="" PORT=5433
DRY=0 PRINT="" ISSUES=()
while (($#)); do
	case $1 in
	--org | --org-user | --pg-bin | --node | --package | --origin | --port | --print | --issue)
		(($# >= 2)) || die "$1 needs a value"
		case $1 in
		--org) ORG=$2 ;; --org-user) ORG_USER=$2 ;; --pg-bin) PG_BIN=$2 ;; --node) NODE=$2 ;;
		--package) PACKAGE=$2 ;; --origin) ORIGIN=$2 ;; --port) PORT=$2 ;; --print) PRINT=$2 ;;
		--issue) ISSUES+=("$2") ;;
		esac
		shift 2
		;;
	--dry-run) DRY=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage >&2; die "unknown argument: $1" ;;
	esac
done

# --- validation (trust boundary: every value below lands in unit files, JSON and root commands) ---
[[ -n $ORG ]] || die "--org is required"
[[ -n $ORG_USER ]] || die "--org-user is required"
# ponytail: must start with a letter and stay <= 24 chars so "<org>-records" is a valid 32-char user name.
[[ $ORG =~ ^[a-z][a-z0-9-]*$ && ${#ORG} -le 24 ]] || die "invalid --org '$ORG': use [a-z0-9-]+, starting with a letter, at most 24 chars"
[[ $ORG_USER =~ ^[a-z_][a-z0-9_-]*$ && ${#ORG_USER} -le 32 ]] || die "invalid --org-user '$ORG_USER'"
[[ $ORG_USER != root ]] || die "--org-user must not be root: agents must never run as root"
if uid=$(id -u "$ORG_USER" 2>/dev/null) && [[ $uid == 0 ]]; then die "--org-user '$ORG_USER' has uid 0; refused"; fi
[[ $PORT =~ ^[0-9]+$ ]] && ((PORT >= 1024 && PORT <= 65535)) || die "invalid --port '$PORT'"
[[ -n $NODE ]] || NODE=$(command -v node || true)
[[ -n $NODE ]] || die "node not found; pass --node BIN"
[[ -n $PACKAGE ]] || PACKAGE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
[[ -n $ORIGIN ]] || ORIGIN=$(hostname -s)
[[ $ORIGIN =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid --origin '$ORIGIN'"
for p in "$PG_BIN" "$NODE" "$PACKAGE"; do
	[[ $p =~ ^/[A-Za-z0-9._/+-]*$ ]] || die "path must be absolute, without spaces or quotes: '$p'"
done
case $PRINT in "" | hba | ident | conf | service-json | units) ;; *) die "invalid --print '$PRINT'" ;; esac
for spec in ${ISSUES[@]+"${ISSUES[@]}"}; do
	[[ $spec =~ ^[A-Za-z0-9._@-]+:(importer|mirror)$ ]] || die "invalid --issue '$spec': use ID:importer or ID:mirror"
done

REC="${ORG}-records"
HOME_DIR="/var/lib/${REC}"
DATA="${HOME_DIR}/pg"
PGSOCK="/run/${REC}-pg"
SVCSOCK="/run/${REC}"
CONF_DIR="/etc/${REC}"
CRED_DIR="${CONF_DIR}/credentials"
STATUS_DIR="${HOME_DIR}/status"
CFG="${CONF_DIR}/service.json"
PG_UNIT="${REC}-pg.service"
SVC_UNIT="${REC}.service"
MAIN="${PACKAGE}/dist/records/service-main.js"
[[ -n $PRINT ]] || ((DRY)) || ((EUID == 0)) || die "must run as root (sudo), or pass --dry-run to only print the steps"
if ! ORG_GROUP=$(id -gn "$ORG_USER" 2>/dev/null); then
	# Only --print and --dry-run may continue for a user that does not exist on this host.
	((DRY)) || [[ -n $PRINT ]] || die "--org-user '$ORG_USER' does not exist"
	ORG_GROUP=$ORG_USER
fi

# --- renderers ---
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
ExecStart=${NODE} ${MAIN} serve --config ${CFG}
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
	printf '+ %s\n' "$(show "$@")"
	((DRY)) || "$@"
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
as_rec() { sudo -u "$REC" "$@"; }

((DRY)) && echo "# DRY RUN: nothing below is executed or written."
cd /

echo "## 1. OS user ${REC}"
if id -u "$REC" >/dev/null 2>&1; then
	echo "= user ${REC} exists"
else
	run useradd --system --user-group --no-create-home --home-dir "$HOME_DIR" --shell /usr/sbin/nologin "$REC"
fi

echo "## 2. Preflight: ${REC} can run PostgreSQL, node and the package"
if ((DRY)); then
	echo "? sudo -u ${REC} test -x ${PG_BIN}/initdb -a -x ${NODE} -a -r ${MAIN}"
elif ! as_rec test -x "$PG_BIN/initdb" -a -x "$NODE" -a -r "$MAIN"; then
	die "${REC} cannot run ${PG_BIN}/initdb, ${NODE} or read ${MAIN}. Install PostgreSQL 17, and put node and a built package (bun run build) where ${REC} can read them (for example under /opt), then pass --node/--package."
fi

echo "## 3. Directories"
run install -d -m 0755 -o "$REC" -g "$REC" "$HOME_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$DATA"
run install -d -m 0755 -o "$REC" -g "$REC" "$STATUS_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$PGSOCK"
run install -d -m 2750 -o "$REC" -g "$ORG_GROUP" "$SVCSOCK"
run install -d -m 0750 -o root -g "$REC" "$CONF_DIR"
run install -d -m 0750 -o root -g "$REC" "$CRED_DIR"

echo "## 4. Cluster"
FRESH=0
if [[ -f $DATA/PG_VERSION ]]; then
	echo "= cluster exists at ${DATA}"
else
	FRESH=1
	run sudo -u "$REC" "$PG_BIN/initdb" -D "$DATA" -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C
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

echo "## 5. Service config"
if [[ -f $CFG ]]; then
	echo "= ${CFG} exists; not overwritten (it may hold granted roles). Compare with --print service-json."
else
	write_file "$CFG" 0640 "root:$REC" "$(render_service_json)"
fi

echo "## 6. systemd units"
write_file "/etc/systemd/system/$PG_UNIT" 0644 root:root "$(render_pg_unit)"
write_file "/etc/systemd/system/$SVC_UNIT" 0644 root:root "$(render_svc_unit)"
run systemctl daemon-reload
run systemctl enable --now "$PG_UNIT"
if ((PG_CONF_CHANGED && !FRESH)); then run systemctl restart "$PG_UNIT"; fi
PSQL=(sudo -u "$REC" "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PGSOCK" -p "$PORT" -U postgres)
if ((DRY)); then
	echo "? wait until ${PG_BIN}/pg_isready -h ${PGSOCK} -p ${PORT} succeeds"
else
	for _ in $(seq 60); do as_rec "$PG_BIN/pg_isready" -q -h "$PGSOCK" -p "$PORT" && break; sleep 1; done
	as_rec "$PG_BIN/pg_isready" -q -h "$PGSOCK" -p "$PORT" || die "PostgreSQL did not become ready; see journalctl -u $PG_UNIT"
fi

echo "## 7. Database, role, migrations"
HAVE_DB=""
((DRY)) || HAVE_DB=$("${PSQL[@]}" -d postgres -tAc "select 1 from pg_database where datname='records'")
if [[ $HAVE_DB == 1 ]]; then
	echo "= database records exists"
else
	run sudo -u "$REC" "$PG_BIN/createdb" -h "$PGSOCK" -p "$PORT" -U postgres records
fi
run "${PSQL[@]}" -d records -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='records_service') THEN CREATE ROLE records_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT; END IF; END \$\$"
run sudo -u "$REC" "$NODE" "$MAIN" migrate --config "$CFG"
run systemctl enable --now "$SVC_UNIT"

echo "## 8. Credentials"
_install_token() { # SRC DEST: root-owned, readable by the org user's group only
	install -m 0640 -o root -g "$ORG_GROUP" "$1" "$2"
	rm -f "$1"
}
for spec in ${ISSUES[@]+"${ISSUES[@]}"}; do
	id=${spec%%:*} role=${spec##*:}
	safe=${id//[^A-Za-z0-9._-]/_}
	token="$CRED_DIR/$safe.token"
	if [[ -f $token ]]; then
		echo "= $token exists; skipped"
		continue
	fi
	# ponytail: ${REC} cannot write the 0750 root-owned credentials dir, so it issues into its home and root moves it.
	staged="$HOME_DIR/.issue-$safe.token"
	run sudo -u "$REC" "$NODE" "$MAIN" issue --config "$CFG" --id "$id" --role "$role" --out "$staged"
	run _install_token "$staged" "$token"
	echo "  -> hand $token to principal '$id' ($role); it is 0640 root:$ORG_GROUP."
done

echo "## 9. Verification"
if ((DRY)); then
	echo "? stat -c '%A %U:%G %n' $HOME_DIR $DATA $STATUS_DIR $PGSOCK $SVCSOCK $CONF_DIR $CRED_DIR"
	echo "? sudo -u ${ORG_USER} ${PG_BIN}/psql -h ${PGSOCK} -p ${PORT} -U postgres -d records -c 'select 1'  (expected to fail: agent cannot reach PostgreSQL)"
else
	stat -c '%A %U:%G %n' "$HOME_DIR" "$DATA" "$STATUS_DIR" "$PGSOCK" "$SVCSOCK" "$CONF_DIR" "$CRED_DIR"
	if sudo -u "$ORG_USER" "$PG_BIN/psql" -X -h "$PGSOCK" -p "$PORT" -U postgres -d records -c 'select 1' >/dev/null 2>&1; then
		die "FAIL: ${ORG_USER} reached PostgreSQL at ${PGSOCK}; the record store is not isolated"
	fi
	echo "OK: agent cannot reach PostgreSQL"
fi
cat <<EOF

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "socket": "${SVCSOCK}/records.sock" } }
EOF
