# `records-paul-steps.sh` dry run

Generated on Dev1 (no PostgreSQL installed yet), from a copy of the script at `/run/user/1000/smarty-step.sh` (the root step runs it as `/run/smarty-step.sh`), with the digests of a clean `bun run build`:

```
smarty-step.sh --org smarty-pants --org-user paul --operator relay:relay:fabric --package-root <package> --node <node> --bundle-sha256 f368765b617cc3a4b623958ed0042a377728ad4e556216699fc5621daebb25da --node-sha256 41a74efb34cbde5c7632cdac0cf8bd1a14d0b8d73dc1e82755014d9a9ce70f5c --dry-run
```

```
# DRY RUN: nothing below is executed or written.
## 0. Stage and verify node and the bundle
? STAGE=$(mktemp -d /run/smarty-pants-records-stage.XXXXXX)  (root-only, 0700; removed on exit)
? umask 077; cat <node> > /run/smarty-pants-records-stage.XXXXXX/node; cat <package>/dist/records-service/service-main.mjs > /run/smarty-pants-records-stage.XXXXXX/service-main.mjs  (each source read once)
? sha256sum /run/smarty-pants-records-stage.XXXXXX/node /run/smarty-pants-records-stage.XXXXXX/service-main.mjs  (must equal 41a74efb34cbde5c7632cdac0cf8bd1a14d0b8d73dc1e82755014d9a9ce70f5c and f368765b617cc3a4b623958ed0042a377728ad4e556216699fc5621daebb25da; otherwise refused, nothing changed)
digest check: bundle ok, node ok  (a real run refuses a MISMATCH)
+ install -d -m 0700 -o root -g root /var/lib/smarty-pants-records-installer
? refuse unless /var/lib/smarty-pants-records-installer is no symlink, owned by root, mode 0700, and every ancestor is root-owned and not group- or world-writable
## 1. OS user smarty-pants-records
+ useradd --system --user-group --no-create-home --home-dir /var/lib/smarty-pants-records --shell /usr/sbin/nologin smarty-pants-records
## 2. Prerequisites: PostgreSQL <detected after install>, node and the package under /opt/smarty-pants-records
  note: the distro package postgresql 16+257build1.1. Unless disabled, it creates a cluster postgresql@<N>-main;
        an existing one stays as it is: this setup does not touch it (disable it yourself if you do not use it).
+ install -d -m 0755 -o root -g root /etc/postgresql-common/createcluster.d
+ write /etc/postgresql-common/createcluster.d/zz-smarty-pants-records.conf (mode 0644, owner root:root)
    | # Written by records-paul-steps.sh: no new distro main cluster on install (it would listen on TCP).
    | create_main_cluster = false
? via a temp file in /var/lib/smarty-pants-records-installer (root-only), then + install -m 0644 -o root -g root /var/lib/smarty-pants-records-installer/write.XXXXXX /etc/postgresql-common/createcluster.d/zz-smarty-pants-records.conf
+ apt-get install -y postgresql
= PostgreSQL <detected after install>: the highest /usr/lib/postgresql/<N>/bin with initdb and postgres
? refuse unless /usr/lib/postgresql/<N>/bin, its binaries and every ancestor are root-owned, not symlinks, not group- or world-writable
+ install -d -m 0755 -o root -g root /opt/smarty-pants-records
? add smarty-pants-records.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ install -m 0755 -o root -g root /run/smarty-pants-records-stage.XXXXXX/node /opt/smarty-pants-records/node
? add smarty-pants-records.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ install -m 0644 -o root -g root /run/smarty-pants-records-stage.XXXXXX/service-main.mjs /opt/smarty-pants-records/service-main.mjs
? find /opt/smarty-pants-records -type l  (must print nothing)
? runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs  (must exit 2, usage: it runs with no external modules)
? runuser -u smarty-pants-records -- test -x /usr/lib/postgresql/<N>/bin/initdb
## 3. Directories
+ install -d -m 0755 -o smarty-pants-records -g smarty-pants-records /var/lib/smarty-pants-records
+ runuser -u smarty-pants-records -- install -d -m 0700 /var/lib/smarty-pants-records/pg
+ runuser -u smarty-pants-records -- install -d -m 0755 /var/lib/smarty-pants-records/status
+ runuser -u smarty-pants-records -- install -d -m 0700 /var/lib/smarty-pants-records/credentials
+ install -d -m 0700 -o smarty-pants-records -g smarty-pants-records /run/smarty-pants-records-pg
+ install -d -m 2750 -o smarty-pants-records -g paul /run/smarty-pants-records
+ install -d -m 0750 -o root -g smarty-pants-records /etc/smarty-pants-records
## 4. Cluster
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/initdb' -D /var/lib/smarty-pants-records/pg -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C
? add smarty-pants-records-pg.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ write /var/lib/smarty-pants-records/pg/pg_hba.conf (mode 0600, owner smarty-pants-records:smarty-pants-records, as smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | # Only the OS user smarty-pants-records reaches PostgreSQL (pg_ident map "records"). Everything else is rejected.
    | local all postgres peer map=records
    | local records records_service peer map=records
    | local all all reject
    | host all all 0.0.0.0/0 reject
    | host all all ::/0 reject
+ runuser -u smarty-pants-records -- sh -c 'umask 077 && cat > "$1.tmp.$$" && chmod "$2" "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh /var/lib/smarty-pants-records/pg/pg_hba.conf 0600
? add smarty-pants-records-pg.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ write /var/lib/smarty-pants-records/pg/pg_ident.conf (mode 0600, owner smarty-pants-records:smarty-pants-records, as smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | records smarty-pants-records postgres
    | records smarty-pants-records records_service
+ runuser -u smarty-pants-records -- sh -c 'umask 077 && cat > "$1.tmp.$$" && chmod "$2" "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh /var/lib/smarty-pants-records/pg/pg_ident.conf 0600
+ runuser -u smarty-pants-records -- install -d -m 0700 /var/lib/smarty-pants-records/pg/conf.d
? add smarty-pants-records-pg.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ write /var/lib/smarty-pants-records/pg/conf.d/records.conf (mode 0600, owner smarty-pants-records:smarty-pants-records, as smarty-pants-records)
    | # Managed by records-paul-steps.sh; rerun it instead of editing.
    | listen_addresses = ''
    | unix_socket_directories = '/run/smarty-pants-records-pg'
    | unix_socket_permissions = 0700
    | port = 5433
    | synchronous_commit = on
    | fsync = on
    | track_commit_timestamp = on
    | # P2-5: bounded bootstrap WAL. The WAL-G step turns archiving on in its own
    | # authorized change; until then no segment waits to be archived, so pg_wal usually stays near max_wal_size (a soft target).
    | archive_mode = off
    | max_wal_size = 1GB
    | wal_keep_size = 0
    | # LIMIT: archiving is off until the WAL-G step; max_wal_size = 1GB is a soft checkpoint target, not a hard WAL quota.
+ runuser -u smarty-pants-records -- sh -c 'umask 077 && cat > "$1.tmp.$$" && chmod "$2" "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh /var/lib/smarty-pants-records/pg/conf.d/records.conf 0600
? add smarty-pants-records-pg.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
+ runuser -u smarty-pants-records -- sh -c 'printf "%s\n" "$2" >> "$1"' sh /var/lib/smarty-pants-records/pg/postgresql.conf 'include_dir = '\''conf.d'\'''
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
? via a temp file in /var/lib/smarty-pants-records-installer (root-only), then + install -m 0640 -o root -g smarty-pants-records /var/lib/smarty-pants-records-installer/write.XXXXXX /etc/smarty-pants-records/service.json
## 6. systemd units
? add smarty-pants-records-pg.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
? add smarty-pants-records.service to /var/lib/smarty-pants-records-installer/restart-pending (root, 0600; restart pending until it restarts)
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
? via a temp file in /var/lib/smarty-pants-records-installer (root-only), then + install -m 0644 -o root -g root /var/lib/smarty-pants-records-installer/write.XXXXXX /etc/systemd/system/smarty-pants-records-pg.service
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
? via a temp file in /var/lib/smarty-pants-records-installer (root-only), then + install -m 0644 -o root -g root /var/lib/smarty-pants-records-installer/write.XXXXXX /etc/systemd/system/smarty-pants-records.service
+ systemctl daemon-reload
+ systemctl enable --now smarty-pants-records-pg.service
? remove smarty-pants-records-pg.service from /var/lib/smarty-pants-records-installer/restart-pending (rm it when empty)
? wait until /usr/lib/postgresql/<N>/bin/pg_isready -h /run/smarty-pants-records-pg -p 5433 succeeds
## 7. Database, role, migrations
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/createdb' -h /run/smarty-pants-records-pg -p 5433 -U postgres records
+ runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/psql' -X -v ON_ERROR_STOP=1 -h /run/smarty-pants-records-pg -p 5433 -U postgres -d records -c 'DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='\''records_service'\'') THEN CREATE ROLE records_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT; END IF; END $$'
+ runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs migrate --config /etc/smarty-pants-records/service.json
+ systemctl enable --now smarty-pants-records.service
? if /var/lib/smarty-pants-records-installer/restart-pending lists smarty-pants-records.service (node, the bundle or its unit changed, now or in an interrupted run) and it was already active: + systemctl restart smarty-pants-records.service, then wait up to 30 s for /run/smarty-pants-records/records.sock
? remove smarty-pants-records.service from /var/lib/smarty-pants-records-installer/restart-pending (rm it when empty)
## 8. Operator principals
? /opt/smarty-pants-records/node -e "$(smarty-step.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json relay relay:fabric check  (refused if relay:fabric holds another role)
+ runuser -u smarty-pants-records -- /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs issue --config /etc/smarty-pants-records/service.json --id relay:fabric --role relay --out /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json --reissue
? runuser -u smarty-pants-records -- /opt/smarty-pants-records/node -e 'const [f, id, role] = process.argv.slice(1); const c = JSON.parse(require("fs").readFileSync(f, "utf8")); if (c.id !== id || c.role !== role || c.issuedBy !== "installer") { console.error(`${f}: not ${id} as ${role} issued by the installer`); process.exit(1); }' /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json relay:fabric relay  (stored .id, .role, .issuedBy must equal relay:fabric, relay, installer)
+ /opt/smarty-pants-records/node -e "$(smarty-step.sh --print operator-edit-js)" /etc/smarty-pants-records/service.json relay relay:fabric
+ runuser -u paul -- install -d -m 0700 ~/.config/smarty-pants-records
+ runuser -u smarty-pants-records -- cat /var/lib/smarty-pants-records/credentials/a118e63f0db39cd54725abe706530715a3bd5e9e269bd9fc6ed418548a49c37a.json | runuser -u paul -- sh -c 'umask 077 && cat > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh ~/.config/smarty-pants-records/relay.json
  -> relay:fabric (relay): Fabric of paul uses it with "records": { "enabled": true, "socket": "/run/smarty-pants-records/records.sock", "relayCredentialFile": "~/.config/smarty-pants-records/relay.json" }.
? if smarty-pants-records.service is active: wait up to 30 s for /run/smarty-pants-records/records.sock (the service's SIGHUP handler is in place by then), then + systemctl reload smarty-pants-records.service  (always, so an interrupted earlier grant takes effect)
## 9. Verification
? stat -c '%A %U:%G %n' /opt/smarty-pants-records/node /opt/smarty-pants-records/service-main.mjs /var/lib/smarty-pants-records /var/lib/smarty-pants-records/pg /var/lib/smarty-pants-records/status /var/lib/smarty-pants-records/credentials /run/smarty-pants-records-pg /run/smarty-pants-records /etc/smarty-pants-records
? runuser -u paul -- /usr/lib/postgresql/<N>/bin/psql -h /run/smarty-pants-records-pg -p 5433 -U postgres -d records -c 'select 1'  (expected to fail: agent cannot reach PostgreSQL)
? systemctl is-active --quiet smarty-pants-records.service  (smarty-pants-records.service active)
? systemctl is-active --quiet smarty-pants-records-pg.service  (smarty-pants-records-pg.service active)
? runuser -u smarty-pants-records -- '/usr/lib/postgresql/<N>/bin/pg_isready' -h /run/smarty-pants-records-pg -p 5433  (PostgreSQL ready)
? runuser -u smarty-pants-records -- python3 -c 'import ctypes; ctypes.CDLL(None).getsockopt'  (peer audit (python3 ctypes))
? test -S /run/smarty-pants-records/records.sock  (service socket)
? ss -Hltnp  (no PostgreSQL TCP listener: none on :5432 or :5433, no postgres process on any port; else fail)
? du -sh /var/lib/smarty-pants-records/pg/pg_wal  (info only: WAL size; max_wal_size = 1GB is a soft target, not a quota)

Fabric: point the org's agents at the service socket in .pi/fabric.json:
  { "records": { "enabled": true, "socket": "/run/smarty-pants-records/records.sock" } }

## ROLLBACK
To undo this install later, run (as root) 'smarty-step.sh --org smarty-pants --org-user paul --rollback --yes-delete-records',
which runs these commands in this order:
This deletes the org's record database. Take a backup first: runuser -u smarty-pants-records -- /usr/lib/postgresql/<N>/bin/pg_dump -h /run/smarty-pants-records-pg -p 5433 -U postgres -Fc records > /root/smarty-pants-records-backup.dump
  systemctl stop smarty-pants-records.service smarty-pants-records-pg.service
  systemctl is-active smarty-pants-records.service smarty-pants-records-pg.service  (each must be inactive, failed or unknown; otherwise nothing is deleted)
  systemctl disable smarty-pants-records.service smarty-pants-records-pg.service
  rm -f /etc/systemd/system/smarty-pants-records.service /etc/systemd/system/smarty-pants-records-pg.service
  systemctl daemon-reload
  rm -rf /etc/smarty-pants-records
  rm -rf /var/lib/smarty-pants-records
  rm -rf /var/lib/smarty-pants-records-installer
  rm -rf /run/smarty-pants-records /run/smarty-pants-records-pg
  rm -rf /opt/smarty-pants-records
  runuser -u paul -- rm -rf ~/.config/smarty-pants-records
  userdel smarty-pants-records
Note: the PostgreSQL packages stay installed (other software may use them). Optional, to remove them too: apt-get remove postgresql postgresql-<N>
(dry run: the success line C10_RECORDS_INSTALLED ... is printed only by a real run after its checks)
```
