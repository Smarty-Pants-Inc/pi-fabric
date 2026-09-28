import { spawnSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir, userInfo } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { normalizeRecordsConfig } from "../src/records/config.js";

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
const sha = (id: string) => createHash("sha256").update(id).digest("hex");
// The host's own PostgreSQL, as the script detects it without --pg-bin (none: the <N> placeholder).
const hostPgMajor = (() => {
	try {
		const b = (n: string) => `/usr/lib/postgresql/${n}/bin`;
		return readdirSync("/usr/lib/postgresql")
			.filter((n) => /^\d+$/.test(n) && existsSync(`${b(n)}/initdb`) && existsSync(`${b(n)}/postgres`))
			.map(Number)
			.sort((a, c) => c - a)[0];
	} catch {
		return undefined;
	}
})();
const hostN = hostPgMajor === undefined ? "<N>" : String(hostPgMajor);
const hostPgBin = `/usr/lib/postgresql/${hostN}/bin`;
// show() quotes an argument with the placeholder's < >.
const q = (p: string) => (p.includes("<") ? `'${p}'` : p);
const reEsc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const lines = (text: string) => text.split("\n").filter((l) => l.trim() !== "" && !l.startsWith("#"));

// The real-path runs take several seconds each under nice (#1600): allow them a minute.
describe.skipIf(process.platform === "win32")("records-paul-steps.sh", { timeout: 60_000 }, () => {
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
			"ExecStart=/opt/test-org-records/node /opt/test-org-records/service-main.mjs serve --config /etc/test-org-records/service.json\n",
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
	writeFileSync(join(fake, "apt-cache"), "#!/bin/sh\nprintf 'postgresql:\\n  Installed: (none)\\n  Candidate: 16+257build1.1\\n'\n");
	chmodSync(join(fake, "apt-cache"), 0o755);
	const fakeEnv = { ...process.env, PATH: `${fake}:${process.env.PATH}` };
	// The default --pg-bin; the dry run below must not pass --pg-bin.
	const dryBase = ["--org", "test-org", "--org-user", "nobodyuser", "--node", process.execPath];

	it("dry run prints every step and executes nothing", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run", "--operator", "importer:github"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).toMatch(/^\+ useradd --system .* --shell \/usr\/sbin\/nologin test-org-records$/m);
		if (hostPgMajor !== undefined) expect(r.out).toContain(`= PostgreSQL ${hostN} found at ${hostPgBin} `);
		else {
			expect(r.out).toContain("+ apt-get install -y postgresql\n");
			expect(r.out).toMatch(/postgresql@<N>-main;\n.*does not touch it/);
			expect(r.out).toContain("= PostgreSQL <detected after install>");
		}
		expect(r.out).toContain("+ install -d -m 0755 -o root -g root /opt/test-org-records\n");
		// F13: exactly two files are staged, a node binary and the self-contained bundle.
		const staged = r.out.split("\n").filter((l) => /^\+ install -m \S+ -o root -g root \S+ \/opt\//.test(l));
		expect(staged).toEqual([
			`+ install -m 0755 -o root -g root ${process.execPath} /opt/test-org-records/node`,
			`+ install -m 0644 -o root -g root ${resolve(__dirname, "..")}/dist/records-service/service-main.mjs /opt/test-org-records/service-main.mjs`,
		]);
		expect(r.out).not.toMatch(/rsync|node_modules|\/package\b|chown -R|chmod -R/);
		expect(r.out).toContain("? find /opt/test-org-records -type l  (must print nothing)\n");
		expect(r.out).toContain("? runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/service-main.mjs  (must exit 2");
		expect(r.out).toMatch(new RegExp(`^\\+ runuser -u test-org-records -- ${reEsc(q(`${hostPgBin}/initdb`))} -D`, "m"));
		expect(r.out).toMatch(/^\+ runuser -u test-org-records -- \S+\/initdb'? -D \/var\/lib\/test-org-records\/pg -U postgres --auth-local=peer --auth-host=reject -E UTF8 --locale=C$/m);
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
			"+ runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/service-main.mjs migrate --config /etc/test-org-records/service.json\n",
		);
		expect(r.out).toContain(
			'+ /opt/test-org-records/node -e "$(records-paul-steps.sh --print operator-edit-js)" /etc/test-org-records/service.json importer importer:github\n',
		);
		const gh = `/var/lib/test-org-records/credentials/${sha("importer:github")}.json`;
		expect(r.out).toContain(
			`+ runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/service-main.mjs issue --config /etc/test-org-records/service.json --id importer:github --role importer --out ${gh}\n`,
		);
		expect(r.out).toContain(`"credentialFile": "${gh}"`);
		expect(r.out).toContain("systemctl reload test-org-records.service");
		expect(r.out).not.toMatch(/sudo -u/);
		expect(r.out).toContain(`? runuser -u nobodyuser -- ${hostPgBin}/psql -h /run/test-org-records-pg `);
		expect(r.out).toMatch(/runuser -u nobodyuser -- \S+\/psql -h \/run\/test-org-records-pg .*expected to fail/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("delivers a relay credential to the org user, 0600, and other operators' only to the records user", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run", "--operator", "relay:relay:fabric", "--operator", "mirror:mirror:github"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).toContain("/etc/test-org-records/service.json relay relay:fabric\n");
		const relayCred = `/var/lib/test-org-records/credentials/${sha("relay:fabric")}.json`;
		expect(r.out).toContain(`--id relay:fabric --role relay --out ${relayCred}\n`);
		// F12: the org user creates and writes its own file; root only feeds the token on stdin.
		expect(r.out).toContain("+ runuser -u nobodyuser -- install -d -m 0700 '~nobodyuser/.config/test-org-records'\n");
		expect(r.out).toContain(
			`+ runuser -u nobodyuser -- sh -c 'umask 077 && cat > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh '~nobodyuser/.config/test-org-records/relay.json' < ${relayCred}\n`,
		);
		expect(r.out).not.toMatch(/^\+ (install|chown|chmod) .*nobodyuser\/\.config/m);
		expect(r.out).toContain('"relayCredentialFile": "~nobodyuser/.config/test-org-records/relay.json"');
		// The mirror's credential stays with the records user.
		expect(r.out).toContain(`--id mirror:github --role mirror --out /var/lib/test-org-records/credentials/${sha("mirror:github")}.json\n`);
		expect(r.out).not.toContain(`< /var/lib/test-org-records/credentials/${sha("mirror:github")}.json`);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("header ROOT STEPS lists every step header printed by the dry run", () => {
		const r = run([...dryBase, "--dry-run", "--operator", "relay:relay:fabric"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		const src = readFileSync(script, "utf8");
		const block = src.slice(src.indexOf("# ROOT STEPS (one line each):"), src.indexOf("set -Eeuo pipefail"));
		const listed = [...block.matchAll(/^# {2}(\d+)\. \S/gm)].map((m) => m[1]);
		const printedSteps = [...r.out.matchAll(/^## (\d+)\. /gm)].map((m) => m[1]);
		expect(printedSteps.length).toBe(9);
		expect(listed).toEqual(printedSteps);
		expect(src).toMatch(/^set -Eeuo pipefail$/m);
		expect(src).toContain("trap 'on_error $LINENO' ERR");
	});

	it("dry run ends with the ROLLBACK commands for this org", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		const tail = r.out.slice(r.out.lastIndexOf("## ROLLBACK\n"));
		expect(r.out.lastIndexOf("## ROLLBACK\n")).toBeGreaterThan(r.out.indexOf("## 9. "));
		expect(tail).toContain("  userdel test-org-records\n");
		expect(tail).toContain("  rm -rf /opt/test-org-records\n");
		expect(tail).toContain("  systemctl disable --now test-org-records.service test-org-records-pg.service\n");
		expect(tail).toContain(`Take a backup first: runuser -u test-org-records -- ${hostPgBin}/pg_dump `);
		expect(tail).toContain(`Optional, to remove them too: apt-get remove postgresql postgresql-${hostN}`);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	const rollbackOrder = [
		"systemctl disable --now test-org-records.service test-org-records-pg.service",
		"rm -f /etc/systemd/system/test-org-records.service /etc/systemd/system/test-org-records-pg.service",
		"systemctl daemon-reload",
		"rm -rf /etc/test-org-records",
		"rm -rf /var/lib/test-org-records",
		"rm -rf /run/test-org-records /run/test-org-records-pg",
		"rm -rf /opt/test-org-records",
		"runuser -u nobodyuser -- rm -rf '~nobodyuser/.config/test-org-records'",
		"userdel test-org-records",
	];

	it("--rollback --dry-run prints each undo command in reverse order and runs nothing", () => {
		writeFileSync(log, "");
		const r = run(["--org", "test-org", "--org-user", "nobodyuser", "--rollback", "--dry-run"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		const cmds = r.out.split("\n").filter((l) => l.startsWith("+ ")).map((l) => l.slice(2));
		expect(cmds).toEqual(rollbackOrder);
		expect(r.out).toContain("This deletes the org's record database. Take a backup first: runuser -u test-org-records -- ");
		expect(r.out).toContain(`apt-get remove postgresql postgresql-${hostN}`);
		expect(r.out).not.toContain("## 1. ");
		// The ROLLBACK section of an install lists the same commands.
		const install = run([...dryBase, "--dry-run"], fakeEnv);
		const listed = install.out.slice(install.out.lastIndexOf("## ROLLBACK\n")).split("\n").filter((l) => l.startsWith("  ")).map((l) => l.slice(2));
		expect(listed).toEqual(rollbackOrder);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	const me = userInfo().username;
	const testEnv = (dir?: string) => ({
		...process.env,
		PATH: `${dir ? `${dir}:` : ""}${fake}:${process.env.PATH}`,
		RECORDS_PAUL_STEPS_ALLOW_NONROOT_TEST: "1",
	});

	it.skipIf(process.getuid?.() === 0)("refuses a real rollback without --yes-delete-records", () => {
		writeFileSync(log, "");
		const r = run(["--org", "test-org", "--org-user", me, "--rollback"], testEnv());
		expect(r.code).toBe(1);
		expect(r.out).toContain("This deletes the org's record database. Take a backup first:");
		expect(r.err).toMatch(/refused: .*--yes-delete-records/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it.skipIf(process.getuid?.() === 0)("stops at the first failed step, names it, and runs nothing after it", () => {
		writeFileSync(log, "");
		const failing = mkdtempSync(join(tmpdir(), "records-paul-steps-fail-"));
		try {
			writeFileSync(join(failing, "useradd"), `#!/bin/sh\necho "useradd $*" >> "${log}"\nexit 3\n`);
			chmodSync(join(failing, "useradd"), 0o755);
			const r = run(["--org", "test-org", "--org-user", me, "--node", process.execPath], testEnv(failing));
			expect(r.code).toBe(1);
			expect(r.err).toMatch(/^records-paul-steps: FAILED at step 1 \(OS user test-org-records\) \(line \d+\): useradd --system .* test-org-records \(exit 3\)$/m);
			expect(r.err).toContain("Nothing after this step ran. Fix the cause and rerun (the script is idempotent), or undo with --rollback.");
			expect(r.out).not.toContain("## 2. ");
			const calls = readFileSync(log, "utf8").trim().split("\n");
			expect(calls).toHaveLength(1);
			expect(calls[0]).toMatch(/^useradd --system /);
		} finally {
			rmSync(failing, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("die inside a step names the step", () => {
		// Step 2 dies: the non-default --pg-bin has no initdb. useradd (fake) succeeds in step 1.
		writeFileSync(log, "");
		const r = run(["--org", "test-org", "--org-user", me, "--node", process.execPath, "--pg-bin", "/nonexistent"], testEnv());
		expect(r.code).toBe(1);
		expect(r.err).toMatch(/^records-paul-steps: FAILED at step 2 \(Prerequisites: .*\) \(line \d+\): \/nonexistent\/initdb not found/m);
		expect(readFileSync(log, "utf8")).toMatch(/^useradd .*\n$/);
	});

	it("fails clearly when a non-default --pg-bin has no initdb", () => {
		const r = run([...base, "--dry-run"], fakeEnv);
		expect(r.code).not.toBe(0);
		expect(r.err).toMatch(/\/nonexistent\/initdb not found/);
	});

	it("fails clearly when apt has no candidate for the distro postgresql", () => {
		const noCand = mkdtempSync(join(tmpdir(), "records-paul-steps-apt-"));
		try {
			writeFileSync(join(noCand, "apt-cache"), "#!/bin/sh\nprintf 'postgresql:\\n  Candidate: (none)\\n'\n");
			chmodSync(join(noCand, "apt-cache"), 0o755);
			const r = run([...dryBase, "--dry-run"], { ...testEnv(noCand), RECORDS_PAUL_STEPS_TEST_ROOT: `${noCand}/root` });
			expect(r.code).not.toBe(0);
			expect(r.err).toMatch(/no apt candidate for the distro package postgresql/);
		} finally {
			rmSync(noCand, { recursive: true, force: true });
		}
	});

	// A fake /usr/lib/postgresql under the test root: each major gets initdb and a postgres that reports it.
	const pgTree = (root: string, majors: string[]) => {
		for (const n of majors) {
			const d = join(root, `usr/lib/postgresql/${n}/bin`);
			mkdirSync(d, { recursive: true });
			writeFileSync(join(d, "initdb"), "#!/bin/sh\nexit 0\n");
			writeFileSync(join(d, "postgres"), `#!/bin/sh\necho "postgres (PostgreSQL) ${n}.4"\n`);
			for (const f of ["initdb", "postgres"]) chmodSync(join(d, f), 0o755);
		}
	};

	it("detects the highest /usr/lib/postgresql/<N>/bin: 17 over 16, and 16 alone", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-pgtree-"));
		try {
			const root = join(t, "root");
			pgTree(root, ["16", "17"]);
			// A directory without postgres is not a server install.
			mkdirSync(join(root, "usr/lib/postgresql/18/bin"), { recursive: true });
			writeFileSync(join(root, "usr/lib/postgresql/18/bin/initdb"), "");
			const env = { ...testEnv(), RECORDS_PAUL_STEPS_TEST_ROOT: root };
			let r = run([...dryBase, "--dry-run"], env);
			expect(r.code, r.err).toBe(0);
			expect(r.out).toContain(`## 2. Prerequisites: PostgreSQL 17, `);
			expect(r.out).toContain(`= PostgreSQL 17 found at ${root}/usr/lib/postgresql/17/bin `);
			expect(r.out).not.toContain("apt-get install");
			expect(r.out).toContain(`+ runuser -u test-org-records -- ${root}/usr/lib/postgresql/17/bin/initdb -D `);
			r = run(["--help", "--org", "test-org", "--org-user", "nobodyuser"], env);
			expect(r.out).toContain("cluster=17/test-org-records");
			rmSync(join(root, "usr/lib/postgresql/17"), { recursive: true });
			r = run([...dryBase, "--dry-run"], env);
			expect(r.code, r.err).toBe(0);
			expect(r.out).toContain(`= PostgreSQL 16 found at ${root}/usr/lib/postgresql/16/bin `);
			expect(r.out).toContain("apt-get remove postgresql postgresql-16");
			expect(r.out).not.toMatch(/postgresql-17|\/17\/|PGDG/i);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it("refuses a --pg-bin whose postgres --version is below 16, before step 1", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-pg15-"));
		try {
			pgTree(t, ["15"]);
			writeFileSync(log, "");
			const r = run([...dryBase, "--pg-bin", join(t, "usr/lib/postgresql/15/bin"), "--dry-run"], fakeEnv);
			expect(r.code).toBe(1);
			expect(r.err).toMatch(/refused: PostgreSQL 15 at \S+ is too old; the record store needs PostgreSQL 16 or newer/);
			expect(r.out).not.toContain("## 1. ");
			expect(readFileSync(log, "utf8")).toBe("");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it("with no PostgreSQL present, the dry run installs the distro postgresql and never mentions postgresql-17 or PGDG", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-nopg-"));
		try {
			writeFileSync(log, "");
			const r = run([...dryBase, "--dry-run", "--operator", "importer:github"], { ...testEnv(), RECORDS_PAUL_STEPS_TEST_ROOT: join(t, "root") });
			expect(r.code, r.err).toBe(0);
			expect(r.out).toContain("## 2. Prerequisites: PostgreSQL <detected after install>, ");
			expect(r.out).toContain("+ apt-get install -y postgresql\n");
			expect(r.out).toMatch(/postgresql@<N>-main;\n.*does not touch it/);
			expect(r.out).not.toMatch(/postgresql-17|PGDG|add-apt-repository|sources\.list|\/17\//i);
			expect(readFileSync(log, "utf8")).toBe("");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
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
			// F18: an id already under another role is refused (exit 3) and nothing is written, also in check mode.
			const before = readFileSync(file, "utf8");
			for (const extra of [[], ["check"]]) {
				const c = spawnSync(process.execPath, ["-e", js, file, "relay", "importer:github", ...extra], { encoding: "utf8" });
				expect(c.status).toBe(3);
				expect(c.stderr).toContain("importer:github already holds role importer; an operator id holds exactly one role, so relay is refused");
			}
			expect(readFileSync(file, "utf8")).toBe(before);
			const ok = spawnSync(process.execPath, ["-e", js, file, "relay", "relay:new", "check"], { encoding: "utf8" });
			expect([ok.status, ok.stdout.trim()]).toEqual([0, "ok"]);
			expect(readFileSync(file, "utf8")).toBe(before);
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

	it("F14: colliding-looking ids get distinct credential files, id checks, and the relay gets its own", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--dry-run", "--operator", "importer:team:import_job", "--operator", "relay:team_import:job"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		const imp = `/var/lib/test-org-records/credentials/${sha("team:import_job")}.json`;
		const rel = `/var/lib/test-org-records/credentials/${sha("team_import:job")}.json`;
		expect(imp).not.toBe(rel);
		expect(r.out).toContain(`--id team:import_job --role importer --out ${imp}\n`);
		expect(r.out).toContain(`--id team_import:job --role relay --out ${rel}\n`);
		for (const [cred, id] of [[imp, "team:import_job"], [rel, "team_import:job"]])
			expect(r.out).toMatch(
				new RegExp(`^\\? runuser -u test-org-records -- /opt/test-org-records/node -e '.*\\.id !== id \\|\\| c\\.role !== role \\|\\| c\\.issuedBy !== "installer".*' ${cred} ${id} (importer|relay) {2}\\(stored \\.id, \\.role, \\.issuedBy must equal`, "m"),
			);
		expect(r.out).toContain(`sh '~nobodyuser/.config/test-org-records/relay.json' < ${rel}\n`);
		expect(r.out).not.toContain(`< ${imp}`);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("F13: refuses a symlinked --node or bundle source", () => {
		const dir = mkdtempSync(join(tmpdir(), "records-paul-steps-link-"));
		try {
			symlinkSync(process.execPath, join(dir, "node"));
			let r = run(["--org", "test-org", "--org-user", "nobodyuser", "--node", join(dir, "node"), "--dry-run"], testEnv());
			expect(r.code).toBe(1);
			expect(r.err).toContain(`${join(dir, "node")} is a symlink: refused`);
			mkdirSync(join(dir, "pkg/dist/records-service"), { recursive: true });
			writeFileSync(join(dir, "real.mjs"), "");
			symlinkSync(join(dir, "real.mjs"), join(dir, "pkg/dist/records-service/service-main.mjs"));
			r = run(["--org", "test-org", "--org-user", "nobodyuser", "--node", process.execPath, "--package", join(dir, "pkg"), "--dry-run"], testEnv());
			expect(r.code).toBe(1);
			expect(r.err).toContain("service-main.mjs is a symlink: refused");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	// F12: the real (non-dry) path against fakes that act as root would, under a temp system root.
	// Root-side install/chown/chmod log their args; install really runs only inside the temp system root.
	// runuser drops "-u USER --" and runs the rest (the real binaries) as this user.
	const relayRun = (layout: "plain" | "dest-link" | "config-link", pgFrom: "pg-bin" | "tree16" = "pg-bin", prep?: (root: string, t: string) => void) => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-relay-"));
		const L = join(t, "calls.log");
		const bin = join(t, "bin");
		// --pg-bin (a 17), or no --pg-bin and a detected /usr/lib/postgresql/16 under the test root.
		const pg = pgFrom === "pg-bin" ? join(t, "pg") : join(t, "root/usr/lib/postgresql/16/bin");
		const home = join(t, "home");
		const outside = join(t, "outside");
		for (const d of [bin, pg, home, outside, join(t, "pkg/dist/records-service"), join(t, "root/etc/systemd/system")]) mkdirSync(d, { recursive: true });
		const sh = (dir: string, name: string, body: string) => {
			writeFileSync(join(dir, name), `#!/bin/bash\n${body}\n`);
			chmodSync(join(dir, name), 0o755);
		};
		const passUser = `[ -n "$FAKE_AS_USER" ] && exec /usr/bin/env -u FAKE_AS_USER PATH=/usr/bin:/bin "$(basename "$0")" "$@"`;
		sh(bin, "runuser", `[ "$1" = -u ] && shift 2; [ "$1" = -- ] && shift; echo "runuser $*" >> "${L}"; FAKE_AS_USER=1 exec "$@"`);
		sh(bin, "install", `${passUser}\necho "install $*" >> "${L}"\nfor a; do last=$a; done\ncase $last in ${t}/root/*) ;; *) exit 0 ;; esac\nargs=(); while (($#)); do case $1 in -o|-g) shift 2 ;; *) args+=("$1"); shift ;; esac; done\nexec /usr/bin/install "\${args[@]}"`);
		for (const c of ["chown", "chmod"]) sh(bin, c, `${passUser}\necho "${c} $*" >> "${L}"`);
		for (const c of ["useradd", "systemctl"]) sh(bin, c, `echo "${c} $*" >> "${L}"`);
		sh(bin, "python3", `echo "python3 $*" >> "${L}"`);
		// The service socket the success check tests with test -S: a real unix socket under the temp root.
		mkdirSync(join(t, "root/run/test-org-records"), { recursive: true });
		spawnSync(process.execPath, ["-e", "require('net').createServer().listen(process.argv[1], () => process.exit(0))", join(t, "root/run/test-org-records/records.sock")]);
		sh(bin, "getent", `echo "${me}:x:1000:1000::${home}:/bin/sh"`);
		for (const c of ["initdb", "pg_isready", "createdb"]) sh(pg, c, "exit 0");
		sh(pg, "postgres", `echo "postgres (PostgreSQL) ${pgFrom === "pg-bin" ? "17.2" : "16.10"}"`);
		sh(pg, "psql", `for a; do last=$a; done; [ "$last" = "select 1" ] && exit 2; exit 0`);
		sh(t, "node", `exec "${process.execPath}" "$@"`);
		writeFileSync(
			join(t, "pkg/dist/records-service/service-main.mjs"),
			`import fs from "node:fs";\nconst a = process.argv.slice(2), f = (n) => a[a.indexOf(n) + 1];\nif (!a.length) process.exit(2);\nif (a[0] === "issue") fs.writeFileSync(f("--out"), JSON.stringify({ id: f("--id"), token: "tok-" + f("--id"), role: f("--role"), issuedBy: "installer" }) + "\\n", { mode: 0o600, flag: "wx" });\n`,
		);
		writeFileSync(join(outside, "keep"), "keep");
		chmodSync(outside, 0o700);
		if (layout === "dest-link") {
			mkdirSync(join(home, ".config"));
			symlinkSync(outside, join(home, ".config/test-org-records"));
		} else if (layout === "config-link") symlinkSync(outside, join(home, ".config"));
		const before = statSync(outside);
		const again = (ops = ["--operator", "relay:relay:fabric", "--operator", "importer:github"]) =>
			run(
				["--org", "test-org", "--org-user", me, "--node", join(t, "node"), ...(pgFrom === "pg-bin" ? ["--pg-bin", pg] : []), "--package", join(t, "pkg"), ...ops],
				{ ...testEnv(bin), RECORDS_PAUL_STEPS_TEST_ROOT: `${t}/root` },
			);
		writeFileSync(L, "");
		prep?.(join(t, "root"), t);
		const r = again();
		const calls = readFileSync(L, "utf8").split("\n").filter(Boolean);
		return { t, r, calls, home, outside, before, again, bin, L };
	};

	it.skipIf(process.getuid?.() === 0)("F12: a plain home gets relay.json 0600, written only by the org user", () => {
		const { t, r, calls, home, again } = relayRun("plain");
		try {
			expect(r.code, r.err + r.out).toBe(0);
			const file = join(home, ".config/test-org-records/relay.json");
			expect(statSync(file).mode & 0o777).toBe(0o600);
			expect(statSync(join(home, ".config/test-org-records")).mode & 0o777).toBe(0o700);
			expect(JSON.parse(readFileSync(file, "utf8")).id).toBe("relay:fabric");
			expect(calls.filter((c) => !c.startsWith("runuser ") && c.includes(home))).toEqual([]);
			expect(calls).toContain(`runuser install -d -m 0700 ${home}/.config/test-org-records`);
			expect(r.out).toContain(`OK: test-org-records runs ${t}/root/opt/test-org-records/node ${t}/root/opt/test-org-records/service-main.mjs with no external modules`);
			// Idempotent: a rerun reuses the checked credentials and restages nothing.
			const second = again();
			expect(second.code, second.err).toBe(0);
			const relayCred = `${t}/root/var/lib/test-org-records/credentials/${sha("relay:fabric")}.json`;
			expect(second.out).toContain(`= ${relayCred} exists; not reissued`);
			expect(second.out).toContain(`= ${t}/root/opt/test-org-records/service-main.mjs matches`);
			// A credential file that holds another principal is never reused or delivered.
			const refusedMsg = `${relayCred} does not hold principal relay:fabric with role relay issued by the installer; refused`;
			for (const stored of [
				{ id: "importer:github", token: "x", role: "relay", issuedBy: "installer" },
				// F18: the right id with another role, or not from the installer's issue command, is refused too.
				{ id: "relay:fabric", token: "x", role: "importer", issuedBy: "installer" },
				{ id: "relay:fabric", token: "x", role: "relay" },
				{ id: "relay:fabric", token: "x", role: "relay", issuedBy: "someone" },
			]) {
				writeFileSync(relayCred, JSON.stringify(stored));
				const third = again();
				expect(third.code, JSON.stringify(stored)).toBe(1);
				expect(third.err).toContain(refusedMsg);
				expect(third.out).not.toContain("relay.json\n");
				expect(JSON.parse(readFileSync(file, "utf8")).token).toBe("tok-relay:fabric");
			}
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F18: an importer id later requested as relay is refused before any grant or delivery", () => {
		const { t, r, home, again, L, calls } = relayRun("plain");
		try {
			expect(r.code, r.err + r.out).toBe(0);
			expect(calls.some((c) => c.startsWith("runuser sh -c umask 077"))).toBe(true);
			const cfg = `${t}/root/etc/test-org-records/service.json`;
			expect(JSON.parse(readFileSync(cfg, "utf8")).roles.importer).toEqual(["importer:github"]);
			const cfgBefore = readFileSync(cfg, "utf8");
			const relayBefore = readFileSync(join(home, ".config/test-org-records/relay.json"), "utf8");
			writeFileSync(L, "");
			// relay:importer:github grants relay to principal importer:github, already an importer.
			const second = again(["--operator", "relay:importer:github"]);
			expect(second.code).toBe(1);
			expect(second.err).toContain("importer:github already holds role importer; an operator id holds exactly one role, so relay is refused");
			expect(second.err).toMatch(/FAILED at step 8 \(Operator principals\) .*refused: importer:github already holds another role in \S+service\.json/);
			expect(readFileSync(cfg, "utf8")).toBe(cfgBefore);
			expect(readFileSync(join(home, ".config/test-org-records/relay.json"), "utf8")).toBe(relayBefore);
			const log2 = readFileSync(L, "utf8");
			expect(log2).not.toContain("issue --config");
			expect(log2).not.toContain(`${home}/.config`);
			expect(log2).not.toMatch(/runuser sh -c/);
			expect(second.out).not.toMatch(/relay\.json|sha256|issue --config/);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F18: one id with two roles in one invocation is refused before step 1", () => {
		writeFileSync(log, "");
		const r = run(["--org", "test-org", "--org-user", me, "--node", process.execPath, "--operator", "importer:x:y", "--operator", "relay:x:y"], testEnv());
		expect(r.code).toBe(1);
		expect(r.err).toContain("refused: operator id 'x:y' is given with two roles (importer and relay); an id holds exactly one role");
		expect(r.out).not.toContain("## 1. ");
		expect(readFileSync(log, "utf8")).toBe("");
		// The same id twice with the same role is fine.
		expect(run([...base, "--operator", "importer:x:y", "--operator", "importer:x:y", "--print", "hba"]).code).toBe(0);
	});

	// F20: a full fake tree for another major, copied from the 16 one relayRun builds, with its own postgres --version.
	const pgBase = (root: string) => join(root, "usr/lib/postgresql");
	const cloneMajor = (root: string, n: string, move = false) => {
		cpSync(join(pgBase(root), "16"), join(pgBase(root), n), { recursive: true });
		if (move) rmSync(join(pgBase(root), "16"), { recursive: true });
		writeFileSync(join(pgBase(root), n, "bin/postgres"), `#!/bin/bash\necho "postgres (PostgreSQL) ${n}.1"\n`);
	};
	const dataDir = (root: string) => join(root, "var/lib/test-org-records/pg");
	const existingCluster = (root: string, major = "16") => {
		mkdirSync(dataDir(root), { recursive: true });
		writeFileSync(join(dataDir(root), "PG_VERSION"), `${major}\n`);
	};
	const refusal16 = (root: string, but: string) =>
		`refused: the cluster at ${dataDir(root)} is PostgreSQL 16, but ${but}; install postgresql-16 (apt-get install postgresql-16) or upgrade the cluster explicitly (pg_upgradecluster); nothing was changed`;

	it.skipIf(process.getuid?.() === 0)("F20: a rerun on an existing 16 cluster keeps 16 when 17 is also installed", () => {
		const { t, r } = relayRun("plain", "tree16", (root) => {
			cloneMajor(root, "17");
			existingCluster(root);
		});
		try {
			expect(r.code, r.err + r.out).toBe(0);
			const root = join(t, "root");
			expect(r.out).toContain(`= PostgreSQL 16 found at ${pgBase(root)}/16/bin (the existing cluster's major`);
			const unit = readFileSync(join(root, "etc/systemd/system/test-org-records-pg.service"), "utf8");
			expect(unit).toContain(`ExecStart=${pgBase(root)}/16/bin/postgres -D ${dataDir(root)}`);
			expect(unit).not.toContain("/17/");
			expect(r.out).not.toContain("/17/bin");
			expect(r.out.trimEnd().split("\n").at(-1)).toBe("C10_RECORDS_INSTALLED org=test-org user=test-org-records cluster=16/test-org-records unit=active peer-audit=ok");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F20: an existing 16 cluster with only 17 installed is refused before step 1 and nothing changes", () => {
		const files = (root: string) => [
			join(root, "etc/systemd/system/test-org-records-pg.service"),
			join(root, "etc/systemd/system/test-org-records.service"),
			join(dataDir(root), "pg_hba.conf"),
			join(dataDir(root), "conf.d/records.conf"),
			join(root, "etc/test-org-records/service.json"),
		];
		const digest = (root: string) => files(root).map((f) => createHash("sha256").update(readFileSync(f)).digest("hex"));
		let before: string[] = [];
		const { t, r, calls, again, L } = relayRun("plain", "tree16", (root) => {
			cloneMajor(root, "17", true);
			existingCluster(root);
			for (const f of files(root)) {
				mkdirSync(dirname(f), { recursive: true });
				writeFileSync(f, `old ${f}\n`);
			}
			before = digest(root);
		});
		try {
			const root = join(t, "root");
			const msg = refusal16(root, `${pgBase(root)}/16/bin is missing`);
			expect(r.code).not.toBe(0);
			expect(r.err).toContain(msg);
			expect(r.out).not.toContain("## 1. ");
			expect(digest(root)).toEqual(before);
			expect(calls).toEqual([]);
			// A dry run shows the same refusal.
			const dry = again(["--dry-run"]);
			expect(dry.code).not.toBe(0);
			expect(dry.err).toContain(msg);
			expect(dry.out).not.toContain("## 1. ");
			expect(readFileSync(L, "utf8")).toBe("");
			expect(digest(root)).toEqual(before);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F20: a --pg-bin of 17 against an existing 16 cluster is refused before step 1", () => {
		const { t, r, calls } = relayRun("plain", "pg-bin", (root) => existingCluster(root));
		try {
			expect(r.code).not.toBe(0);
			expect(r.err).toContain(refusal16(join(t, "root"), `--pg-bin ${join(t, "pg")} is PostgreSQL 17`));
			expect(r.out).not.toContain("## 1. ");
			expect(calls).toEqual([]);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F20 counterexample: a fresh cluster with 16 and 17 installed picks 17", () => {
		const { t, r } = relayRun("plain", "tree16", (root) => cloneMajor(root, "17"));
		try {
			expect(r.code, r.err + r.out).toBe(0);
			const root = join(t, "root");
			const unit = readFileSync(join(root, "etc/systemd/system/test-org-records-pg.service"), "utf8");
			expect(unit).toContain(`ExecStart=${pgBase(root)}/17/bin/postgres -D ${dataDir(root)}`);
			expect(r.out).toContain(`+ runuser -u test-org-records -- ${pgBase(root)}/17/bin/initdb -D `);
			expect(r.out.trimEnd().split("\n").at(-1)).toBe("C10_RECORDS_INSTALLED org=test-org user=test-org-records cluster=17/test-org-records unit=active peer-audit=ok");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("with only a detected PostgreSQL 16, a real run says cluster=16", () => {
		const { t, r } = relayRun("plain", "tree16");
		try {
			expect(r.code, r.err + r.out).toBe(0);
			expect(r.out).toContain(`= PostgreSQL 16 found at ${t}/root/usr/lib/postgresql/16/bin `);
			expect(r.out.trimEnd().split("\n").at(-1)).toBe("C10_RECORDS_INSTALLED org=test-org user=test-org-records cluster=16/test-org-records unit=active peer-audit=ok");
			expect(r.out).not.toMatch(/postgresql-17|PGDG/i);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	const success = "C10_RECORDS_INSTALLED org=test-org user=test-org-records cluster=17/test-org-records unit=active peer-audit=ok";
	const helpSuccess = `C10_RECORDS_INSTALLED org=test-org user=test-org-records cluster=${hostN}/test-org-records unit=active peer-audit=ok`;

	it("--help needs no root or other flag and prints the four sections for the bundle", () => {
		for (const flag of ["--help", "-h"]) {
			const r = run([flag], { ...process.env, PATH: `${fake}:${process.env.PATH}` });
			expect(r.code, r.err).toBe(0);
			for (const h of ["WHAT IT CHANGES:", "IDEMPOTENCY:", "SUCCESS LINE:", "ROLLBACK:"]) expect(r.out).toMatch(new RegExp(`^${h}`, "m"));
			expect(r.out).toContain(`C10_RECORDS_INSTALLED org=<org> user=<org>-records cluster=${hostN}/<org>-records unit=active peer-audit=ok`);
			if (hostPgMajor === undefined) expect(r.out).not.toMatch(/postgresql-17|\/17\/|=17\//);
			expect(r.out).toContain("  userdel <org>-records\n");
		}
		const r = run(["--help", "--org", "test-org", "--org-user", "nobodyuser"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).toContain(`  ${helpSuccess}\n`);
		expect(r.out).toContain("--org test-org --org-user nobodyuser --rollback --yes-delete-records");
		expect(r.out).toContain("--rollback --dry-run");
		expect(r.out).toContain("  1. useradd the test-org-records system user");
		expect(r.out.slice(r.out.indexOf("WHAT IT CHANGES:"))).not.toMatch(/<org>|<org-user>/);
		// The rollback list is the one --rollback runs.
		const listed = r.out.slice(r.out.indexOf("\nROLLBACK:")).split("\n").filter((l) => l.startsWith("  ") && !l.includes("records-paul-steps.sh")).map((l) => l.slice(2));
		expect(listed).toEqual(rollbackOrder);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("F22: the printed .pi/fabric.json snippets enable records over the socket, with no DSN", () => {
		const r = run([...dryBase, "--dry-run", "--operator", "relay:relay:fabric"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		// Each printed `"records": { … }` object, cut at its balanced closing brace.
		const snippets: Record<string, unknown>[] = [];
		for (const match of r.out.matchAll(/"records": \{/g)) {
			const start = match.index! + match[0].length - 1;
			let depth = 0;
			let end = start;
			for (; end < r.out.length; end++) {
				if (r.out[end] === "{") depth++;
				else if (r.out[end] === "}" && --depth === 0) break;
			}
			snippets.push(JSON.parse(r.out.slice(start, end + 1)) as Record<string, unknown>);
		}
		expect(snippets.length).toBeGreaterThanOrEqual(2); // the agents' snippet and the relay's
		for (const snippet of snippets) {
			const config = normalizeRecordsConfig(snippet);
			expect(config.enabled).toBe(true);
			expect(config.socket).toBe("/run/test-org-records/records.sock");
			expect(Object.keys(snippet).filter((key) => /connection|dsn|host|database|password/i.test(key))).toEqual([]);
		}
		expect(snippets.some((snippet) => typeof snippet.relayCredentialFile === "string")).toBe(true);
	});

	it("a dry run previews the success checks and never prints the success line", () => {
		const r = run([...dryBase, "--dry-run"], fakeEnv);
		expect(r.code, r.err).toBe(0);
		expect(r.out).not.toMatch(/^C10_RECORDS_INSTALLED org=/m);
		for (const check of [
			"? systemctl is-active --quiet test-org-records.service",
			"? systemctl is-active --quiet test-org-records-pg.service",
			`? runuser -u test-org-records -- ${q(`${hostPgBin}/pg_isready`)} -h /run/test-org-records-pg -p 5433`,
			"? runuser -u test-org-records -- python3 -c 'import ctypes; ctypes.CDLL(None).getsockopt'",
			"? test -S /run/test-org-records/records.sock",
		])
			expect(r.out).toContain(check);
		const out = r.out.trimEnd().split("\n");
		expect(out.at(-1)).toBe("(dry run: the success line C10_RECORDS_INSTALLED ... is printed only by a real run after its checks)");
		expect(r.out.indexOf("## ROLLBACK\n")).toBeLessThan(r.out.lastIndexOf("(dry run: the success line"));
	});

	it.skipIf(process.getuid?.() === 0)("a real run prints the success line last only after its checks pass", () => {
		const { t, r, calls, bin, L, again } = relayRun("plain");
		try {
			expect(r.code, r.err + r.out).toBe(0);
			expect(r.out.trimEnd().split("\n").at(-1)).toBe(success);
			expect(r.out.indexOf("## ROLLBACK\n")).toBeLessThan(r.out.lastIndexOf(success));
			expect(calls).toContain("systemctl is-active --quiet test-org-records.service");
			expect(calls).toContain("systemctl is-active --quiet test-org-records-pg.service");
			expect(calls).toContain("python3 -c import ctypes; ctypes.CDLL(None).getsockopt");
			expect(r.out).toMatch(/^OK: service socket$/m);
			// A unit that is not active fails step 9 through the first-failure report; no success line.
			writeFileSync(L, "");
			writeFileSync(join(bin, "systemctl"), `#!/bin/bash\necho "systemctl $*" >> "${L}"\n[ "$1" = is-active ] && exit 3\nexit 0\n`);
			const down = again();
			expect(down.code).toBe(1);
			expect(down.err).toMatch(/^records-paul-steps: FAILED at step 9 \(Verification\) \(line \d+\): check failed \(test-org-records\.service active\): systemctl is-active --quiet test-org-records\.service$/m);
			expect(down.err).toContain("Nothing after this step ran.");
			expect(down.out).not.toContain("C10_RECORDS_INSTALLED org=");
			expect(down.out).not.toContain("## ROLLBACK");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("a missing service socket fails the success checks", () => {
		const { t, again } = relayRun("plain");
		try {
			rmSync(join(t, "root/run/test-org-records/records.sock"));
			writeFileSync(join(t, "root/run/test-org-records/records.sock"), "not a socket");
			const r = again();
			expect(r.code).toBe(1);
			expect(r.err).toMatch(/FAILED at step 9 \(Verification\) .*check failed \(service socket\): test -S \S+\/root\/run\/test-org-records\/records\.sock$/m);
			expect(r.out).not.toContain("C10_RECORDS_INSTALLED org=");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	for (const layout of ["dest-link", "config-link"] as const)
		it.skipIf(process.getuid?.() === 0)(`F12: a symlinked ${layout === "dest-link" ? "destination" : ".config ancestor"} gets no root-side write`, () => {
			const { t, r, calls, home, outside, before } = relayRun(layout);
			try {
				expect(r.code, r.err + r.out).toBe(0);
				// No root-side command touches the org user's home or the link target.
				expect(calls.filter((c) => !c.startsWith("runuser ") && (c.includes(home) || c.includes(outside)))).toEqual([]);
				const after = statSync(outside);
				expect([after.mode, after.uid, after.gid]).toEqual([before.mode, before.uid, before.gid]);
				expect(readFileSync(join(outside, "keep"), "utf8")).toBe("keep");
				// The only write is the org user's own (it may follow its own link: the same-uid limit).
				const relayCalls = calls.filter((c) => c.includes(`${home}/.config`));
				expect(relayCalls).toEqual([
					`runuser install -d -m 0700 ${home}/.config/test-org-records`,
					expect.stringMatching(/^runuser sh -c umask 077 .* sh .*\/relay\.json$/),
				]);
				expect(readdirSync(outside).sort()).toEqual(layout === "dest-link" ? ["keep", "relay.json"] : ["keep", "test-org-records"]);
			} finally {
				rmSync(t, { recursive: true, force: true });
			}
		});
});
