# `records-paul-steps.sh` dry run

Generated on Dev1 (no PostgreSQL installed yet) with:

```
scripts/records-paul-steps.sh --org smarty-pants --org-user paul --operator relay:relay:fabric --operator importer:github --dry-run
```

```
# DRY RUN: nothing below is executed or written.
## 1. OS user smarty-pants-records
+ useradd --system --user-group --no-create-home --home-dir /var/lib/smarty-pants-records --shell /usr/sbin/nologin smarty-pants-records
## 2. Prerequisites: PostgreSQL <detected after install>, node and the package under /opt/smarty-pants-records
  note: the distro package postgresql 16+257build1.1. It creates its own cluster service postgresql@<N>-main;
        this setup does not need it and does not touch it (disable it yourself if you do not use it).
+ apt-get install -y postgresql
= PostgreSQL <detected after install>: the highest /usr/lib/postgresql/<N>/bin with initdb and postgres
! <package>/dist/records-service/service-main.mjs not found: build the package (bun run build) before the real run
+ install -d -m 0755 -o root -g root /opt/smarty-pants-records
+ install -m 0755 -o root -g root ~/.local/share/node-v24.18.0-linux-x64/bin/node /opt/smarty-pants-records/node
+ install -m 0644 -o root -g root <package>/dist/records-service/service-main.mjs /opt/smarty-pants-records/service-main.mjs
? find /opt/smarty-pants-records -type l  (must print nothing)
? runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs  (must exit 2, usage: it runs with no external modules)
? runuser -u smarty-pants-records -- test -x /usr/lib/postgresql/<N>/bin/initdb
## 3. Directories
+ install -d -m 0755 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records
+ install -d -m 0700 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records/pg
+ install -d -m 0755 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records/status
+ install -d -m 0700 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records/credentials
+ install -d -m 0700 -o smarty-pants-records -g smarty-pants-records /run/smarty-pants-records-pg
+ install -d -m 2750 -o smarty-pants-records -g paul /run/smarty-pants-records
+ install -d -m 0750 -o root -g smarty-pants-records /etc/smarty-pants-records
## 4. Cluster
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/initdb' -D /var/lib/smarty-pants-records/pg -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C
+ write /var/lib/smarty-pants-records/pg/pg_hba.conf (mode 0600, owner smarty-pants-records:smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | # Only the OS user smarty-pants-records reaches PostgreSQL (pg_ident map "records"). Everything else is rejected.
    | local all postgres peer map=records
    | local records records_service peer map=records
    | local all all reject
    | host all all 0.0.0.0/0 reject
    | host all all ::/0 reject
+ write /var/lib/smarty-pants-records/pg/pg_ident.conf (mode 0600, owner smarty-pants-records:smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | records smarty-pants-records postgres
    | records smarty-pants-records records_service
+ install -d -m 0700 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records/pg/conf.d
+ write /var/lib/smarty-pants-records/pg/conf.d/records.conf (mode 0600, owner smarty-pants-records:smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | listen_addresses = ''
    | unix_socket_directories = '/run/smarty-pants-records-pg'
    | unix_socket_permissions = 0700
    | port = 5433
    | synchronous_commit = on
    | fsync = on
    | track_commit_timestamp = on
    | archive_mode = on
    | # WAL-G replaces archive_command in its own authorized step. Until then archiving fails
    | # loudly and PostgreSQL keeps every WAL segment in pg_wal: no WAL is thrown away.
    | archive_command = '/bin/false'
    | archive_timeout = 60
+ _append /var/lib/smarty-pants-records/pg/postgresql.conf 'include_dir = '\''conf.d'\'''
## 5. Service config
+ write /etc/smarty-pants-records/service.json (mode 0640, owner root:smarty-pants-records)
    | {
    |   "org": "smarty-pants",
    |   "origin": "dev1",
    |   "socket": "/run/smarty-pants-records/records.sock",
    |   "database": { "host": "/run/smarty-pants-records-pg", "port": 5433, "database": "records", "user": "records_service" },
    |   "migration": { "host": "/run/smarty-pants-records-pg", "port": 5433, "database": "records", "user": "postgres" },
    |   "roles": { "importer": [], "mirror": [] },
    |   "mirror": { "enabled": false },
    |   "admission": { "targets": [] },
    |   "statusFile": "/var/lib/smarty-pants-records/status/smarty-pants.status.json"
    | }
## 6. systemd units
+ write /etc/systemd/system/smarty-pants-records-pg.service (mode 0644, owner root:root)
    | [Unit]
    | Description=PostgreSQL record store for smarty-pants
    | After=network.target
    | 
    | [Service]
    | Type=notify
    | User=smarty-pants-records
    | Group=smarty-pants-records
    | ExecStart=/usr/lib/postgresql/<N>/bin/postgres -D /var/lib/smarty-pants-records/pg
    | ExecReload=/bin/kill -HUP $MAINPID
    | KillMode=mixed
    | KillSignal=SIGINT
    | TimeoutSec=infinity
    | RuntimeDirectory=smarty-pants-records-pg
    | RuntimeDirectoryMode=0700
    | RuntimeDirectoryPreserve=yes
    | Restart=on-failure
    | 
    | [Install]
    | WantedBy=multi-user.target
+ write /etc/systemd/system/smarty-pants-records.service (mode 0644, owner root:root)
    | [Unit]
    | Description=Records service for smarty-pants
    | Requires=smarty-pants-records-pg.service
    | After=smarty-pants-records-pg.service
    | 
    | [Service]
    | Type=simple
    | User=smarty-pants-records
    | Group=smarty-pants-records
    | RuntimeDirectory=smarty-pants-records
    | RuntimeDirectoryMode=0750
    | UMask=0007
    | # The socket directory belongs to the org's agents' group, setgid, so the socket the service
    | # creates in it is theirs to connect to; the service itself joins no group of theirs.
    | ExecStartPre=+/bin/chgrp paul /run/smarty-pants-records
    | ExecStartPre=+/bin/chmod 2750 /run/smarty-pants-records
    | ExecStart=/opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs serve --config /etc/smarty-pants-records/service.json
    | # The service re-reads roles from /etc/smarty-pants-records/service.json on SIGHUP (after an --operator grant).
    | ExecReload=/bin/kill -HUP $MAINPID
    | Restart=on-failure
    | RestartSec=2
    | NoNewPrivileges=yes
    | ProtectSystem=strict
    | ReadWritePaths=/var/lib/smarty-pants-records /run/smarty-pants-records
    | 
    | [Install]
    | WantedBy=multi-user.target
+ systemctl daemon-reload
+ systemctl enable --now smarty-pants-records-pg.service
? wait until /usr/lib/postgresql/<N>/bin/pg_isready -h /run/smarty-pants-records-pg -p 5433 succeeds
## 7. Database, role, migrations
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/createdb' -h /run/smarty-pants-records-pg -p 5433 -U postgres records
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/psql' -X -v ON_ERROR_STOP=1 -h /run/smarty-pants-records-pg -p 5433 -U postgres -d records -c 'DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='\''records_service'\'') THEN CREATE ROLE records_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT; END IF; END $$'
+ runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs migrate --config /etc/smarty-pants-records/service.json
+ systemctl enable --now smarty-pants-records.service
## 8. Operator principals
? /opt/smarty-pants-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json relay relay:fabric check  (refused if relay:fabric holds another role)
? /opt/smarty-pants-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json importer importer:github check  (refused if importer:github holds another role)
+ /opt/smarty-pants-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json relay relay:fabric
+ runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs issue --config /etc/smarty-pants-records/service.json --id relay:fabric --role relay --out /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json
? runuser -u smarty-pants-records -- /opt/smarty-pants-records/node -e 'const [f, id, role] = process.argv.slice(1); const c = JSON.parse(require("fs").readFileSync(f, "utf8")); if (c.id !== id || c.role !== role || c.issuedBy !== "installer") { console.error(`${f}: not ${id} as ${role} issued by the installer`); process.exit(1); }' /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json relay:fabric relay  (stored .id, .role, .issuedBy must equal relay:fabric, relay, installer)
+ runuser -u paul -- install -d -m 0700 ~/.config/smarty-pants-records
+ runuser -u paul -- sh -c 'umask 077 && cat > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh ~/.config/smarty-pants-records/relay.json < /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json
  -> relay:fabric (relay): Fabric of paul uses it with "records": { "relayCredentialFile": "~/.config/smarty-pants-records/relay.json" }.
+ /opt/smarty-pants-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json importer importer:github
+ runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs issue --config /etc/smarty-pants-records/service.json --id importer:github --role importer --out /var/lib/smarty-pants-records/credentials/340b97e6eed75f259ce6f2053ff8b99b683ee61dccd69f42c5d732c33215ad79.json
? runuser -u smarty-pants-records -- /opt/smarty-pants-records/node -e 'const [f, id, role] = process.argv.slice(1); const c = JSON.parse(require("fs").readFileSync(f, "utf8")); if (c.id !== id || c.role !== role || c.issuedBy !== "installer") { console.error(`${f}: not ${id} as ${role} issued by the installer`); process.exit(1); }' /var/lib/smarty-pants-records/credentials/340b97e6eed75f259ce6f2053ff8b99b683ee61dccd69f42c5d732c33215ad79.json importer:github importer  (stored .id, .role, .issuedBy must equal importer:github, importer, installer)
  -> importer:github (importer): its component runs as smarty-pants-records (a sibling unit with User=smarty-pants-records, or inside
     smarty-pants-records.service) with "credentialFile": "/var/lib/smarty-pants-records/credentials/340b97e6eed75f259ce6f2053ff8b99b683ee61dccd69f42c5d732c33215ad79.json".
? if roles changed and smarty-pants-records.service is active: wait up to 30 s for /run/smarty-pants-records/records.sock (the service's SIGHUP handler is in place by then), then + systemctl reload smarty-pants-records.service
## 9. Verification
? stat -c '%A %U:%G %n' /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs /var/lib/smarty-pants-records /var/lib/smarty-pants-records/pg /var/lib/smarty-pants-records/status /var/lib/smarty-pants-records/credentials /run/smarty-pants-records-pg /run/smarty-pants-records /etc/smarty-pants-records
? runuser -u paul -- /usr/lib/postgresql/<N>/bin/psql -h /run/smarty-pants-records-pg -p 5433 -U postgres -d records -c 'select 1'  (expected to fail: agent cannot reach PostgreSQL)
? systemctl is-active --quiet smarty-pants-records.service  (smarty-pants-records.service active)
? systemctl is-active --quiet smarty-pants-records-pg.service  (smarty-pants-records-pg.service active)
? runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/pg_isready' -h /run/smarty-pants-records-pg -p 5433  (PostgreSQL ready)
? runuser -u smarty-pants-records -- python3 -c 'import ctypes; ctypes.CDLL(None).getsockopt'  (peer audit (python3 ctypes))
? test -S /run/smarty-pants-records/records.sock  (service socket)

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "socket": "/run/smarty-pants-records/records.sock" } }

## ROLLBACK
To undo this install later, run (as root) 'records-paul-steps.sh --org smarty-pants --org-user paul --rollback --yes-delete-records',
which runs these commands in this order:
This deletes the org's record database. Take a backup first: runuser -u smarty-pants-records -- /usr/lib/postgresql/<N>/bin/pg_dump -h /run/smarty-pants-records-pg -p 5433 -U postgres -Fc records > /root/smarty-pants-records-backup.dump
  systemctl disable --now smarty-pants-records.service smarty-pants-records-pg.service
  rm -f /etc/systemd/system/smarty-pants-records.service /etc/systemd/system/smarty-pants-records-pg.service
  systemctl daemon-reload
  rm -rf /etc/smarty-pants-records
  rm -rf /var/lib/smarty-pants-records
  rm -rf /run/smarty-pants-records /run/smarty-pants-records-pg
  rm -rf /opt/smarty-pants-records
  runuser -u paul -- rm -rf ~/.config/smarty-pants-records
  userdel smarty-pants-records
Note: the PostgreSQL packages stay installed (other software may use them). Optional, to remove them too: apt-get remove postgresql postgresql-<N>
(dry run: the success line C10_RECORDS_INSTALLED ... is printed only by a real run after its checks)
```
