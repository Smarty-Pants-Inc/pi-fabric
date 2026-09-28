import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

const script = resolve(__dirname, "../scripts/records-paul-steps.sh");
const base = ["--org", "test-org", "--org-user", "nobodyuser", "--pg-bin", "/nonexistent", "--node", "/usr/bin/node"];

function run(args: string[], env: NodeJS.ProcessEnv = process.env) {
	const r = spawnSync("bash", [script, ...args], { encoding: "utf8", env });
	return { code: r.status, out: r.stdout, err: r.stderr };
}
const printed = (what: string) => {
	const r = run([...base, "--print", what]);
	expect(r.code, r.err).toBe(0);
	expect(r.err).toBe("");
	return r.out;
};
const lines = (text: string) => text.split("\n").filter((l) => l.trim() !== "" && !l.startsWith("#"));

describe.skipIf(process.platform === "win32")("records-paul-steps.sh", () => {
	const fake = mkdtempSync(join(tmpdir(), "records-paul-steps-"));
	afterAll(() => rmSync(fake, { recursive: true, force: true }));

	it("passes bash -n", () => {
		expect(spawnSync("bash", ["-n", script]).status).toBe(0);
	});

	it("renders pg_hba.conf with peer maps first and reject lines last", () => {
		expect(lines(printed("hba"))).toEqual([
			"local all postgres peer map=records",
			"local records records_service peer map=records",
			"local all all reject",
			"host all all 0.0.0.0/0 reject",
			"host all all ::/0 reject",
		]);
	});

	it("maps only the records OS user in pg_ident.conf", () => {
		expect(lines(printed("ident"))).toEqual([
			"records test-org-records postgres",
			"records test-org-records records_service",
		]);
	});

	it("renders the PostgreSQL overrides", () => {
		const conf = lines(printed("conf"));
		for (const line of [
			"listen_addresses = ''",
			"unix_socket_directories = '/run/test-org-records-pg'",
			"unix_socket_permissions = 0700",
			"port = 5433",
			"synchronous_commit = on",
			"fsync = on",
			"track_commit_timestamp = on",
			"archive_mode = on",
			"archive_command = '/bin/false'",
			"archive_timeout = 60",
		])
			expect(conf).toContain(line);
		expect(printed("conf")).toMatch(/# WAL-G replaces archive_command/);
	});

	it("renders service.json with the fixed shape", () => {
		const r = run([...base, "--origin", "dev1", "--port", "5444", "--print", "service-json"]);
		expect(r.code, r.err).toBe(0);
		expect(JSON.parse(r.out)).toEqual({
			org: "test-org",
			origin: "dev1",
			socket: "/run/test-org-records/records.sock",
			database: { host: "/run/test-org-records-pg", port: 5444, database: "records", user: "records_service" },
			migration: { host: "/run/test-org-records-pg", port: 5444, database: "records", user: "postgres" },
			roles: { importer: [], mirror: [] },
			mirror: { enabled: false },
			admission: { targets: [] },
			statusFile: "/var/lib/test-org-records/status/test-org.status.json",
		});
	});

	it("renders both systemd units", () => {
		const units = printed("units");
		const [pg, svc] = units.split("# /etc/systemd/system/test-org-records.service");
		expect(pg).toContain("User=test-org-records\n");
		expect(pg).toContain("ExecStart=/nonexistent/postgres -D /var/lib/test-org-records/pg\n");
		expect(pg).toContain("RuntimeDirectory=test-org-records-pg\n");
		expect(pg).toContain("RuntimeDirectoryMode=0700\n");
		expect(pg).toContain("RuntimeDirectoryPreserve=yes\n");
		expect(svc).toContain("User=test-org-records\n");
		expect(svc).toContain("Group=test-org-records\n");
		expect(svc).not.toContain("SupplementaryGroups");
		expect(svc).toContain("ExecStartPre=+/bin/chmod 2750 /run/test-org-records\n");
		expect(svc).toContain("Requires=test-org-records-pg.service\n");
		expect(svc).toContain("RuntimeDirectoryMode=0750\n");
		expect(svc).toContain("UMask=0007\n");
		expect(svc).toContain("ExecStartPre=+/bin/chgrp nobodyuser /run/test-org-records\n");
		expect(svc).toContain(
			"ExecStart=/opt/test-org-records/node /opt/test-org-records/package/dist/records/service-main.js serve --config /etc/test-org-records/service.json\n",
		);
		expect(svc).toContain("ExecReload=/bin/kill -HUP $MAINPID\n");
		expect(units).not.toContain("/usr/bin/node");
		expect(svc).toContain("NoNewPrivileges=yes\n");
		expect(svc).toContain("ProtectSystem=strict\n");
		expect(svc).toContain("ReadWritePaths=/var/lib/test-org-records /run/test-org-records\n");
	});

	const mutating = ["useradd", "systemctl", "sudo", "runuser", "install", "chown", "chmod", "chgrp", "initdb", "psql", "createdb", "apt-get", "rsync"];
	const log = join(fake, "calls.log");
	for (const cmd of mutating) {
		writeFileSync(join(fake, cmd), `#!/bin/sh\necho "${cmd} $*" >> "${log}"\n`);
		chmodSync(join(fake, cmd), 0o755);
	}
	// Non-mutating: the script queries it for real, also in a dry run.
	writeFileSync(join(fake, "apt-cache"), "#!/bin/sh\nprintf 'postgresql-17:\\n  Installed: (none)\\n  Candidate: 17.6-1.pgdg\\n'\n");
	chmodSync(join(fake, "apt-cache"), 0o755);
	const fakeEnv = { ...process.env, PATH: `${fake}:${process.env.PATH}` };
	// The default --pg-bin; the dry run below must not pass --pg-bin.
	const dryBase = ["--org", "test-org", "--org-user", "nobodyuser", "--node", process.execPath];
	const defaultInitdb = "/usr/lib/postgresql/17/bin/initdb";

	it("dry run prints every step and executes nothing", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run", "--operator", "importer:github"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).toMatch(/^\+ useradd --system .* --shell \/usr\/sbin\/nologin test-org-records$/m);
		if (existsSync(defaultInitdb)) expect(r.out).toContain("= PostgreSQL found at /usr/lib/postgresql/17/bin\n");
		else {
			expect(r.out).toContain("+ apt-get install -y postgresql-17\n");
			expect(r.out).toMatch(/postgresql@17-main;\n.*does not touch it/);
		}
		expect(r.out).toContain("+ install -d -m 0755 -o root -g root /opt/test-org-records /opt/test-org-records/package\n");
		expect(r.out).toContain(`+ install -m 0755 -o root -g root ${process.execPath} /opt/test-org-records/node\n`);
		expect(r.out).toMatch(/^\+ rsync -a --delete \S+\/dist\/ \/opt\/test-org-records\/package\/dist\/$/m);
		expect(r.out).toMatch(/^\+ rsync -a --delete \S+\/node_modules\/ \/opt\/test-org-records\/package\/node_modules\/$/m);
		expect(r.out).toMatch(/^\+ rsync -a \S+\/package\.json \/opt\/test-org-records\/package\/package\.json$/m);
		expect(r.out).toContain("+ chown -R root:root /opt/test-org-records/package\n");
		expect(r.out).toContain("+ chmod -R go-w,a+rX /opt/test-org-records/package\n");
		expect(r.out).toContain(
			`? runuser -u test-org-records -- /opt/test-org-records/node -e 'require("fs").accessSync("/opt/test-org-records/package/dist/records/service-main.js")'\n`,
		);
		expect(r.out).toMatch(/^\+ runuser -u test-org-records -- \/usr\/lib\/postgresql\/17\/bin\/initdb -D \/var\/lib\/test-org-records\/pg -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C$/m);
		expect(r.out).toContain("+ install -d -m 0700 -o test-org-records -g test-org-records /var/lib/test-org-records/pg\n");
		expect(r.out).toContain("+ install -d -m 0755 -o test-org-records -g test-org-records /var/lib/test-org-records\n");
		expect(r.out).toContain("+ install -d -m 0700 -o test-org-records -g test-org-records /var/lib/test-org-records/credentials\n");
		expect(r.out).toContain("+ install -d -m 0700 -o test-org-records -g test-org-records /run/test-org-records-pg\n");
		expect(r.out).toContain("+ install -d -m 2750 -o test-org-records -g nobodyuser /run/test-org-records\n");
		expect(r.out).not.toContain("/etc/test-org-records/credentials");
		for (const [path, mode] of [
			["/var/lib/test-org-records/pg/pg_hba.conf", "0600"],
			["/var/lib/test-org-records/pg/pg_ident.conf", "0600"],
			["/var/lib/test-org-records/pg/conf.d/records.conf", "0600"],
			["/etc/test-org-records/service.json", "0640"],
			["/etc/systemd/system/test-org-records-pg.service", "0644"],
			["/etc/systemd/system/test-org-records.service", "0644"],
		])
			expect(r.out).toContain(`+ write ${path} (mode ${mode},`);
		expect(r.out).toContain("+ systemctl enable --now test-org-records-pg.service\n");
		expect(r.out).toContain("+ systemctl enable --now test-org-records.service\n");
		expect(r.out).toContain("CREATE ROLE records_service LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;");
		expect(r.out).toContain(
			"+ runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/package/dist/records/service-main.js migrate --config /etc/test-org-records/service.json\n",
		);
		expect(r.out).toContain(
			'+ /opt/test-org-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/test-org-records/service.json importer importer:github\n',
		);
		expect(r.out).toContain(
			"+ runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/package/dist/records/service-main.js issue --config /etc/test-org-records/service.json --id importer:github --out /var/lib/test-org-records/credentials/importer_github.json\n",
		);
		expect(r.out).not.toContain("--role");
		expect(r.out).toContain('"credentialFile": "/var/lib/test-org-records/credentials/importer_github.json"');
		expect(r.out).toContain("systemctl reload test-org-records.service");
		expect(r.out).not.toMatch(/sudo -u/);
		expect(r.out).toMatch(/runuser -u nobodyuser -- \/usr\/lib\/postgresql\/17\/bin\/psql -h \/run\/test-org-records-pg .*expected to fail/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("delivers a relay credential to the org user, 0600, and other operators' only to the records user", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run", "--operator", "relay:relay:fabric", "--operator", "mirror:mirror:github"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).toContain("/etc/test-org-records/service.json relay relay:fabric\n");
		expect(r.out).toContain("--id relay:fabric --out /var/lib/test-org-records/credentials/relay_fabric.json\n");
		expect(r.out).toContain("+ install -d -m 0700 -o nobodyuser -g nobodyuser '~nobodyuser/.config/test-org-records'\n");
		expect(r.out).toContain("+ install -m 0600 -o nobodyuser -g nobodyuser /var/lib/test-org-records/credentials/relay_fabric.json '~nobodyuser/.config/test-org-records/relay.json'\n");
		expect(r.out).toContain('"relayCredentialFile": "~nobodyuser/.config/test-org-records/relay.json"');
		// The mirror's credential stays with the records user.
		expect(r.out).not.toMatch(/install .*mirror_github\.json ~nobodyuser/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("fails clearly when a non-default --pg-bin has no initdb", () => {
		const r = run([...base, "--dry-run"], fakeEnv);
		expect(r.code).not.toBe(0);
		expect(r.err).toMatch(/\/nonexistent\/initdb not found/);
	});

	it("fails clearly when apt has no postgresql-17 candidate", () => {
		if (existsSync(defaultInitdb)) return;
		const noCand = mkdtempSync(join(tmpdir(), "records-paul-steps-apt-"));
		writeFileSync(join(noCand, "apt-cache"), "#!/bin/sh\nprintf 'postgresql-17:\\n  Candidate: (none)\\n'\n");
		chmodSync(join(noCand, "apt-cache"), 0o755);
		const r = run([...dryBase, "--dry-run"], { ...process.env, PATH: `${noCand}:${fake}:${process.env.PATH}` });
		rmSync(noCand, { recursive: true, force: true });
		expect(r.code).not.toBe(0);
		expect(r.err).toMatch(/PGDG apt repository/);
	});

	it("edits service.json roles idempotently and keeps every other field", () => {
		const js = printed("operator-edit-js");
		const dir = mkdtempSync(join(tmpdir(), "records-paul-steps-json-"));
		try {
			const file = join(dir, "service.json");
			const original = {
				org: "o",
				extra: { keep: [1, 2] },
				roles: { importer: ["other:one"], mirror: ["m:1"], custom: ["x:y"] },
				statusFile: "/s",
			};
			writeFileSync(file, JSON.stringify(original), { mode: 0o600 });
			const edit = (role: string, id: string) => {
				const r = spawnSync(process.execPath, ["-e", js, file, role, id], { encoding: "utf8" });
				expect(r.status, r.stderr).toBe(0);
				return r.stdout.trim();
			};
			expect(edit("importer", "importer:github")).toBe("changed");
			expect(edit("importer", "importer:github")).toBe("unchanged");
			const after = JSON.parse(readFileSync(file, "utf8"));
			expect(after).toEqual({ ...original, roles: { ...original.roles, importer: ["other:one", "importer:github"] } });
			expect(statSync(file).mode & 0o777).toBe(0o640);
			expect(readdirSync(dir)).toEqual(["service.json"]);
			const noRoles = join(dir, "bare.json");
			writeFileSync(noRoles, JSON.stringify({ org: "o" }));
			const r = spawnSync(process.execPath, ["-e", js, noRoles, "mirror", "mirror:dev1"], { encoding: "utf8" });
			expect(r.status, r.stderr).toBe(0);
			expect(JSON.parse(readFileSync(noRoles, "utf8"))).toEqual({ org: "o", roles: { mirror: ["mirror:dev1"] } });
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("refuses bad input, --issue and non-root runs", () => {
		expect(run(["--org", "Bad Org", "--org-user", "nobodyuser", "--dry-run"]).code).not.toBe(0);
		expect(run(["--org", "test-org", "--org-user", "root", "--dry-run"]).code).not.toBe(0);
		const issue = run([...base, "--issue", "bot:importer", "--dry-run"]);
		expect(issue.code).not.toBe(0);
		expect(issue.err).toMatch(/unknown argument: --issue/);
		for (const bad of ["admin:github", "importer:Bad", "importer:fabric:Bad", "importer", "mirror:x:"]) {
			const r = run([...base, "--operator", bad, "--print", "hba"]);
			expect(r.code, bad).not.toBe(0);
			expect(r.err, bad).toMatch(/invalid --operator/);
		}
		expect(run([...base, "--operator", "mirror:fabric:dev1", "--print", "hba"]).code).toBe(0);
		if (process.getuid?.() !== 0) {
			const r = run(base);
			expect(r.code).not.toBe(0);
			expect(r.err).toMatch(/must run as root/);
		}
	});
});
