import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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
		expect(svc).toMatch(/ExecStart=\/usr\/bin\/node \S+\/dist\/records\/service-main\.js serve --config \/etc\/test-org-records\/service\.json\n/);
		expect(svc).toContain("NoNewPrivileges=yes\n");
		expect(svc).toContain("ProtectSystem=strict\n");
		expect(svc).toContain("ReadWritePaths=/var/lib/test-org-records /run/test-org-records\n");
	});

	it("dry run prints every step and executes nothing", () => {
		const log = join(fake, "calls.log");
		writeFileSync(log, "");
		for (const cmd of ["useradd", "systemctl", "sudo", "install", "chown", "chmod", "chgrp", "initdb", "psql", "createdb"]) {
			writeFileSync(join(fake, cmd), `#!/bin/sh\necho "${cmd} $*" >> "${log}"\n`);
			chmodSync(join(fake, cmd), 0o755);
		}
		const r = run([...base, "--dry-run", "--issue", "bot:importer"], {
			...process.env,
			PATH: `${fake}:${process.env.PATH}`,
		});
		expect(r.code, r.err).toBe(0);
		expect(r.out).toMatch(/^\+ useradd --system .* --shell \/usr\/sbin\/nologin test-org-records$/m);
		expect(r.out).toMatch(/^\+ sudo -u test-org-records \/nonexistent\/initdb -D \/var\/lib\/test-org-records\/pg -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C$/m);
		expect(r.out).toContain("+ install -d -m 0700 -o test-org-records -g test-org-records /var/lib/test-org-records/pg\n");
		expect(r.out).toContain("+ install -d -m 0700 -o test-org-records -g test-org-records /run/test-org-records-pg\n");
		expect(r.out).toContain("+ install -d -m 2750 -o test-org-records -g nobodyuser /run/test-org-records\n");
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
		expect(r.out).toContain("service-main.js migrate --config /etc/test-org-records/service.json\n");
		expect(r.out).toContain("--id bot --role importer");
		expect(r.out).toMatch(/sudo -u nobodyuser \/nonexistent\/psql -h \/run\/test-org-records-pg .*expected to fail/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("refuses bad input and non-root runs", () => {
		expect(run(["--org", "Bad Org", "--org-user", "nobodyuser", "--dry-run"]).code).not.toBe(0);
		expect(run(["--org", "test-org", "--org-user", "root", "--dry-run"]).code).not.toBe(0);
		expect(run([...base, "--issue", "x:admin", "--dry-run"]).code).not.toBe(0);
		if (process.getuid?.() !== 0) {
			const r = run(base);
			expect(r.code).not.toBe(0);
			expect(r.err).toMatch(/must run as root/);
		}
	});
});
