#!/usr/bin/env bash
# One-time, idempotent host setup for an org's record store (smarty-dev#754 C10/C13, pi-fabric#103 F1).
# PostgreSQL runs as its own OS user <org>-records, which owns the cluster. The org's agents
# (OS user <org-user>) reach only the records service socket, never PostgreSQL.
#
#   sudo scripts/records-paul-steps.sh --org smarty-pants --org-user paul [--dry-run]
#   scripts/records-paul-steps.sh --org smarty-pants --org-user paul --print hba|ident|conf|service-json|units|operator-edit-js
# --node and --package are SOURCES: the script copies them to root-owned /opt/<org>-records/{node,package},
# and the units and every later command use only those copies.
set -euo pipefail

usage() {
	cat <<'EOF'
usage: records-paul-steps.sh --org <org> --org-user <orguser> [--pg-bin DIR] [--node BIN]
         [--package DIR] [--origin NAME] [--port 5433] [--operator ROLE:ID]... [--dry-run]
         [--print hba|ident|conf|service-json|units|operator-edit-js]
  --operator ROLE:ID  grant ROLE (importer|mirror|relay) to principal ID and issue its credential into
                      /var/lib/<org>-records/credentials/ (repeatable). 'importer:github' is
                      principal importer:github; 'importer:fabric:dev1' is principal fabric:dev1.
EOF
}
die() {
	printf 'records-paul-steps: %s\n' "$*" >&2
	exit 1
}

DEFAULT_PG_BIN=/usr/lib/postgresql/17/bin
ORG="" ORG_USER="" PG_BIN=$DEFAULT_PG_BIN NODE="" PACKAGE="" ORIGIN="" PORT=5433
DRY=0 PRINT="" OPERATORS=()
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

REC="${ORG}-records"
HOME_DIR="/var/lib/${REC}"
DATA="${HOME_DIR}/pg"
PGSOCK="/run/${REC}-pg"
SVCSOCK="/run/${REC}"
CONF_DIR="/etc/${REC}"
CRED_DIR="${HOME_DIR}/credentials"
STATUS_DIR="${HOME_DIR}/status"
CFG="${CONF_DIR}/service.json"
PG_UNIT="${REC}-pg.service"
SVC_UNIT="${REC}.service"
OPT="/opt/${REC}"
OPT_NODE="${OPT}/node"
OPT_PKG="${OPT}/package"
MAIN="${OPT_PKG}/dist/records/service-main.js"
[[ -n $PRINT ]] || ((DRY)) || ((EUID == 0)) || die "must run as root (sudo), or pass --dry-run to only print the steps"
if ! ORG_GROUP=$(id -gn "$ORG_USER" 2>/dev/null); then
	# Only --print and --dry-run may continue for a user that does not exist on this host.
	((DRY)) || [[ -n $PRINT ]] || die "--org-user '$ORG_USER' does not exist"
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
as_rec() { runuser -u "$REC" -- "$@"; }

((DRY)) && echo "# DRY RUN: nothing below is executed or written."
cd /

echo "## 1. OS user ${REC}"
if id -u "$REC" >/dev/null 2>&1; then
	echo "= user ${REC} exists"
else
	run useradd --system --user-group --no-create-home --home-dir "$HOME_DIR" --shell /usr/sbin/nologin "$REC"
fi

echo "## 2. Prerequisites: PostgreSQL 17, node and the package under ${OPT}"
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

SRC_MAIN="${PACKAGE}/dist/records/service-main.js"
for p in "$SRC_MAIN" "$PACKAGE/package.json" "$PACKAGE/node_modules"; do
	if [[ ! -e $p ]]; then
		((DRY)) || die "${p} not found: pass --package with a built package (bun run build)"
		echo "! ${p} not found: build the package (bun run build) before the real run"
	fi
done
run install -d -m 0755 -o root -g root "$OPT" "$OPT_PKG"
if cmp -s "$NODE" "$OPT_NODE"; then
	echo "= ${OPT_NODE} matches ${NODE}"
else
	run install -m 0755 -o root -g root "$NODE" "$OPT_NODE"
fi
run rsync -a --delete "$PACKAGE/dist/" "$OPT_PKG/dist/"
run rsync -a --delete "$PACKAGE/node_modules/" "$OPT_PKG/node_modules/"
run rsync -a "$PACKAGE/package.json" "$OPT_PKG/package.json"
run chown -R root:root "$OPT_PKG"
run chmod -R go-w,a+rX "$OPT_PKG"
VERIFY_JS="require(\"fs\").accessSync(\"${MAIN}\")"
if ((DRY)); then
	echo "? runuser -u ${REC} -- ${OPT_NODE} -e '${VERIFY_JS}'"
	echo "? runuser -u ${REC} -- test -x ${PG_BIN}/initdb"
else
	as_rec "$OPT_NODE" -e "$VERIFY_JS" || die "${REC} cannot run ${OPT_NODE} or read ${MAIN}; check the modes under ${OPT}"
	as_rec test -x "$PG_BIN/initdb" || die "${REC} cannot run ${PG_BIN}/initdb"
	echo "OK: ${REC} runs ${OPT_NODE} and reads ${MAIN}"
fi

echo "## 3. Directories"
run install -d -m 0755 -o "$REC" -g "$REC" "$HOME_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$DATA"
run install -d -m 0755 -o "$REC" -g "$REC" "$STATUS_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$CRED_DIR"
run install -d -m 0700 -o "$REC" -g "$REC" "$PGSOCK"
run install -d -m 2750 -o "$REC" -g "$ORG_GROUP" "$SVCSOCK"
run install -d -m 0750 -o root -g "$REC" "$CONF_DIR"

echo "## 4. Cluster"
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

echo "## 5. Service config"
if [[ -f $CFG ]]; then
	echo "= ${CFG} exists; not overwritten (it holds granted roles). Compare with --print service-json."
else
	write_file "$CFG" 0640 "root:$REC" "$(render_service_json)"
fi

echo "## 6. systemd units"
write_file "/etc/systemd/system/$PG_UNIT" 0644 root:root "$(render_pg_unit)"
write_file "/etc/systemd/system/$SVC_UNIT" 0644 root:root "$(render_svc_unit)"
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

echo "## 7. Database, role, migrations"
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

echo "## 8. Operator principals"
POLICY_CHANGED=0
for spec in ${OPERATORS[@]+"${OPERATORS[@]}"}; do
	role=${spec%%:*} id=${spec#*:}
	[[ $id == *:* ]] || id=$spec
	cred="$CRED_DIR/${id//:/_}.json"
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
		run runuser -u "$REC" -- "$OPT_NODE" "$MAIN" issue --config "$CFG" --id "$id" --out "$cred"
	fi
	if [[ $role == relay ]]; then
		# The relay publishes nudges on the org's mesh, which only the org user can write: its credential
		# goes to that user (0600). Its protection ends at the same-uid limit (docs/records.md, Trust boundary).
		org_home=$(getent passwd "$ORG_USER" | cut -d: -f6 || true)
		if [[ -z $org_home ]]; then
			((DRY)) || die "no home directory for ${ORG_USER}"
			org_home="~${ORG_USER}"
		fi
		relay_dir="${org_home}/.config/${ORG}-records"
		run install -d -m 0700 -o "$ORG_USER" -g "$ORG_GROUP" "$relay_dir"
		run install -m 0600 -o "$ORG_USER" -g "$ORG_GROUP" "$cred" "${relay_dir}/relay.json"
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
		echo "? if systemctl is-active --quiet ${SVC_UNIT} and roles changed: + systemctl reload ${SVC_UNIT}"
	elif ((POLICY_CHANGED)) && systemctl is-active --quiet "$SVC_UNIT"; then
		run systemctl reload "$SVC_UNIT"
	else
		echo "= no reload: roles unchanged or ${SVC_UNIT} not active (it reads roles at start)"
	fi
fi

echo "## 9. Verification"
if ((DRY)); then
	echo "? stat -c '%A %U:%G %n' $OPT_NODE $OPT_PKG $HOME_DIR $DATA $STATUS_DIR $CRED_DIR $PGSOCK $SVCSOCK $CONF_DIR"
	echo "? runuser -u ${ORG_USER} -- ${PG_BIN}/psql -h ${PGSOCK} -p ${PORT} -U postgres -d records -c 'select 1'  (expected to fail: agent cannot reach PostgreSQL)"
else
	stat -c '%A %U:%G %n' "$OPT_NODE" "$OPT_PKG" "$HOME_DIR" "$DATA" "$STATUS_DIR" "$CRED_DIR" "$PGSOCK" "$SVCSOCK" "$CONF_DIR"
	if runuser -u "$ORG_USER" -- "$PG_BIN/psql" -X -h "$PGSOCK" -p "$PORT" -U postgres -d records -c 'select 1' >/dev/null 2>&1; then
		die "FAIL: ${ORG_USER} reached PostgreSQL at ${PGSOCK}; the record store is not isolated"
	fi
	echo "OK: agent cannot reach PostgreSQL"
fi
cat <<EOF

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "socket": "${SVCSOCK}/records.sock" } }
EOF
