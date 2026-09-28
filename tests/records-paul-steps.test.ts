import { spawnSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir, userInfo } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { normalizeRecordsConfig } from "../src/records/config.js";

const script = resolve(__dirname, "../scripts/records-paul-steps.sh");
const base = ["--org", "test-org", "--org-user", "nobodyuser", "--node", "/usr/bin/node"];

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
const fileSha = (p: string) => createHash("sha256").update(readFileSync(p)).digest("hex");
const Z = "0".repeat(64);
const repo = resolve(__dirname, "..");
// A fake node and bundle, with their correct digests as install arguments.
const sources = (dir: string) => {
	mkdirSync(join(dir, "pkg/dist/records-service"), { recursive: true });
	const bundle = join(dir, "pkg/dist/records-service/service-main.mjs");
	writeFileSync(bundle, "process.exit(2);\n");
	const node = join(dir, "node");
	writeFileSync(node, "#!/bin/sh\nexit 0\n");
	chmodSync(node, 0o755);
	return { node, bundle, args: ["--node", node, "--package-root", join(dir, "pkg"), "--bundle-sha256", fileSha(bundle), "--node-sha256", fileSha(node)] };
};
// The host's own PostgreSQL, as the script detects it (none: the <N> placeholder).
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
			// P2-5: bounded bootstrap WAL until the WAL-G step turns archiving on.
			"archive_mode = off",
			"max_wal_size = 1GB",
			"wal_keep_size = 0",
		])
			expect(conf).toContain(line);
		expect(printed("conf")).not.toMatch(/archive_command|archive_timeout/);
		expect(printed("conf")).toMatch(/# P2-5: .*The WAL-G step turns archiving on/);
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
		expect(pg).toContain(`ExecStart=${hostPgBin}/postgres -D /var/lib/test-org-records/pg\n`);
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
	const dryBase = ["--org", "test-org", "--org-user", "nobodyuser", "--node", process.execPath, "--package-root", repo, "--bundle-sha256", Z, "--node-sha256", Z];

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
			"+ install -m 0755 -o root -g root /run/test-org-records-stage.XXXXXX/node /opt/test-org-records/node",
			"+ install -m 0644 -o root -g root /run/test-org-records-stage.XXXXXX/service-main.mjs /opt/test-org-records/service-main.mjs",
		]);
		// P1-1: staging and the digest check come before step 1; the sources are only read.
		expect(r.out.indexOf("? STAGE=$(mktemp -d /run/test-org-records-stage.XXXXXX)")).toBeLessThan(r.out.indexOf("## 1. "));
		expect(r.out).toContain(`? umask 077; cat ${process.execPath} > /run/test-org-records-stage.XXXXXX/node; `);
		expect(r.out).toMatch(/^digest check: bundle (ok|MISMATCH|not readable), node MISMATCH {2}\(a real run refuses a MISMATCH\)$/m);
		expect(r.out).toContain("? ss -Hltnp  (no PostgreSQL TCP listener: none on :5432 or :5433,");
		expect(r.out).toContain("? du -sh /var/lib/test-org-records/pg/pg_wal  (info only: WAL size;");
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
			`+ runuser -u test-org-records -- /opt/test-org-records/node /opt/test-org-records/service-main.mjs issue --config /etc/test-org-records/service.json --id importer:github --role importer --out ${gh} --reissue\n`,
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
		expect(r.out).toContain(`--id relay:fabric --role relay --out ${relayCred} --reissue\n`);
		// F12: the org user creates and writes its own file; root only feeds the token on stdin.
		expect(r.out).toContain("+ runuser -u nobodyuser -- install -d -m 0700 '~nobodyuser/.config/test-org-records'\n");
		expect(r.out).toContain(
			`+ runuser -u nobodyuser -- sh -c 'umask 077 && cat > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"' sh '~nobodyuser/.config/test-org-records/relay.json' < ${relayCred}\n`,
		);
		expect(r.out).not.toMatch(/^\+ (install|chown|chmod) .*nobodyuser\/\.config/m);
		expect(r.out).toContain('"relayCredentialFile": "~nobodyuser/.config/test-org-records/relay.json"');
		// The mirror's credential stays with the records user.
		expect(r.out).toContain(`--id mirror:github --role mirror --out /var/lib/test-org-records/credentials/${sha("mirror:github")}.json --reissue\n`);
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
		expect(printedSteps.length).toBe(10);
		expect(listed).toEqual(printedSteps);
		// --help prints the same ROOT STEPS (it cannot read the script's own file: no path comes from $0).
		const help = run(["--help"], fakeEnv).out;
		const helpSteps = help.slice(help.indexOf("WHAT IT CHANGES:"), help.indexOf("\nIDEMPOTENCY:")).split("\n").filter((l) => /^ {2}\d+\. /.test(l));
		const headerSteps = block.split("\n").filter((l) => /^# {2}\d+\. /.test(l)).map((l) => l.slice(1).replaceAll("<N>", hostN));
		expect(helpSteps).toEqual(headerSteps);
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
		expect(tail).toContain("  systemctl stop test-org-records.service test-org-records-pg.service\n");
		expect(tail).toContain("  systemctl is-active test-org-records.service test-org-records-pg.service  (each must be inactive, failed or unknown; otherwise nothing is deleted)\n");
		expect(tail).toContain("  systemctl disable test-org-records.service test-org-records-pg.service\n");
		expect(tail).toContain(`Take a backup first: runuser -u test-org-records -- ${hostPgBin}/pg_dump `);
		expect(tail).toContain(`Optional, to remove them too: apt-get remove postgresql postgresql-${hostN}`);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	const rollbackOrder = [
		"systemctl stop test-org-records.service test-org-records-pg.service",
		"systemctl disable test-org-records.service test-org-records-pg.service",
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
		expect(r.out).toContain("? systemctl is-active test-org-records.service test-org-records-pg.service  (each must be");
		expect(r.out).toContain("This deletes the org's record database. Take a backup first: runuser -u test-org-records -- ");
		expect(r.out).toContain(`apt-get remove postgresql postgresql-${hostN}`);
		expect(r.out).not.toContain("## 1. ");
		// The ROLLBACK section of an install lists the same commands.
		const install = run([...dryBase, "--dry-run"], fakeEnv);
		const listed = install.out.slice(install.out.lastIndexOf("## ROLLBACK\n")).split("\n").filter((l) => l.startsWith("  ") && !l.includes(" is-active ")).map((l) => l.slice(2));
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
			const src = sources(failing);
			const r = run(["--org", "test-org", "--org-user", me, ...src.args], { ...testEnv(failing), RECORDS_PAUL_STEPS_TEST_ROOT: join(failing, "root") });
			expect(r.out).toContain("OK: the staged node and bundle match the approved sha256");
			// The staging directory is removed on exit.
			expect(readdirSync(join(failing, "root/run"))).toEqual([]);
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

	it("S4: refuses --pg-bin as an unknown argument, before anything runs", () => {
		writeFileSync(log, "");
		const r = run([...dryBase, "--pg-bin", "/usr/lib/postgresql/16/bin", "--dry-run"], fakeEnv);
		expect(r.code).toBe(1);
		expect(r.err).toContain("unknown argument: --pg-bin");
		expect(r.out).not.toMatch(/^## \d/m);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("requires both sources and both digests, 64 lowercase hex, before anything runs", () => {
		writeFileSync(log, "");
		const drop = (flag: string) => {
			const i = dryBase.indexOf(flag);
			return [...dryBase.slice(0, i), ...dryBase.slice(i + 2)];
		};
		for (const flag of ["--bundle-sha256", "--node-sha256", "--package-root", "--node"]) {
			const r = run([...drop(flag), "--dry-run"], fakeEnv);
			expect(r.code, flag).toBe(1);
			expect(r.err, flag).toContain(`${flag} ${flag === "--node" ? "BIN" : flag === "--package-root" ? "DIR" : "HEX"} is required`);
			expect(r.out, flag).not.toMatch(/^## \d/m);
		}
		for (const bad of ["A".repeat(64), "0".repeat(63), `${"0".repeat(64)}0`, "sha256:abc"]) {
			const r = run([...drop("--node-sha256"), "--node-sha256", bad, "--dry-run"], fakeEnv);
			expect(r.code, bad).toBe(1);
			expect(r.err, bad).toContain(`invalid digest '${bad}': use 64 lowercase hex characters`);
		}
		expect(run([...drop("--node"), "--node", "node", "--dry-run"], fakeEnv).err).toMatch(/path must be absolute/);
		expect(readFileSync(log, "utf8")).toBe("");
	});

	it("--print-digests prints both sha256 lines and changes nothing", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-dig-"));
		try {
			const src = sources(t);
			const r = run(["--print-digests", "--package-root", join(t, "pkg"), "--node", src.node]);
			expect(r.code, r.err).toBe(0);
			expect(r.out).toBe(`bundle-sha256 ${fileSha(src.bundle)}  ${src.bundle}\nnode-sha256 ${fileSha(src.node)}  ${src.node}\n`);
			expect(run(["--print-digests", "--node", src.node]).code).toBe(1);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("refuses a staged copy that does not match its approved digest, before step 1, changing nothing", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-mismatch-"));
		try {
			const src = sources(t);
			const root = join(t, "root");
			const kept = join(root, "etc/test-org-records/service.json");
			mkdirSync(dirname(kept), { recursive: true });
			writeFileSync(kept, "{}\n");
			const before = [fileSha(kept), fileSha(src.node), fileSha(src.bundle)];
			const env = { ...testEnv(), RECORDS_PAUL_STEPS_TEST_ROOT: root };
			for (const [flag, name] of [["--bundle-sha256", "bundle"], ["--node-sha256", "node"]] as const) {
				writeFileSync(log, "");
				const args = [...src.args];
				args[args.indexOf(flag) + 1] = "f".repeat(64);
				const r = run(["--org", "test-org", "--org-user", me, ...args], env);
				expect(r.code).toBe(1);
				const got = name === "node" ? before[1] : before[2];
				expect(r.err).toContain(`refused: staged ${name} sha256 ${got} != approved ${"f".repeat(64)}; nothing was changed`);
				expect(r.out).not.toContain("## 1. ");
				expect(readFileSync(log, "utf8")).toBe("");
				expect(readdirSync(join(root, "run"))).toEqual([]);
				expect([fileSha(kept), fileSha(src.node), fileSha(src.bundle)]).toEqual(before);
				expect(existsSync(join(root, "opt"))).toBe(false);
			}
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it("a copy at <dir>/run/smarty-step.sh does the same dry run: no path comes from $0", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-copy-"));
		try {
			const src = sources(t);
			mkdirSync(join(t, "run"));
			const copy = join(t, "run/smarty-step.sh");
			writeFileSync(copy, readFileSync(script));
			const args = ["--org", "test-org", "--org-user", "nobodyuser", "--operator", "relay:relay:fabric", ...src.args, "--dry-run"];
			const a = run(args, fakeEnv);
			const b = spawnSync("bash", [copy, ...args], { encoding: "utf8", env: fakeEnv, cwd: t });
			expect(a.code, a.err).toBe(0);
			expect(b.status, b.stderr).toBe(0);
			expect(a.out).toContain("digest check: bundle ok, node ok");
			// Only the script's own name (${0##*/}, in messages) differs.
			const same = (s: string) => s.replaceAll("smarty-step.sh", "NAME").replaceAll("records-paul-steps.sh", "NAME");
			expect(same(b.stdout)).toBe(same(a.out));
			expect(b.stderr).toBe(a.err);
			// The --help of the copy lists the same steps.
			const h = spawnSync("bash", [copy, "--help"], { encoding: "utf8", env: fakeEnv });
			expect(same(h.stdout)).toBe(same(run(["--help"], fakeEnv).out));
			const src2 = readFileSync(script, "utf8");
			expect(src2).not.toMatch(/BASH_SOURCE|dirname|\$0\b|\$\{0[^#]/);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it("P2-1: writes the createcluster.d drop-in before apt, also when createcluster.conf exists", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-cc-"));
		try {
			const root = join(t, "root");
			const env = { ...testEnv(), RECORDS_PAUL_STEPS_TEST_ROOT: root };
			const dropIn = `+ write ${root}/etc/postgresql-common/createcluster.d/zz-test-org-records.conf (mode 0644, owner root:root)\n    | # Written by records-paul-steps.sh`;
			let r = run([...dryBase, "--dry-run"], env);
			expect(r.code, r.err).toBe(0);
			expect(r.out).toContain(dropIn);
			expect(r.out).toContain("    | create_main_cluster = false\n");
			expect(r.out.indexOf(dropIn)).toBeLessThan(r.out.indexOf("+ apt-get install -y postgresql"));
			mkdirSync(join(root, "etc/postgresql-common"), { recursive: true });
			writeFileSync(join(root, "etc/postgresql-common/createcluster.conf"), "create_main_cluster = true\n");
			r = run([...dryBase, "--dry-run"], env);
			expect(r.code, r.err).toBe(0);
			expect(r.out).toContain(dropIn);
			expect(r.out).toContain(`= ${root}/etc/postgresql-common/createcluster.conf kept as is; the drop-in below overrides`);
			expect(r.out.indexOf(dropIn)).toBeLessThan(r.out.indexOf("+ apt-get install -y postgresql\n"));
			// With PostgreSQL present, apt is not run and no drop-in is written.
			rmSync(join(root, "etc"), { recursive: true });
			pgTree(root, ["16"]);
			r = run([...dryBase, "--dry-run"], env);
			expect(r.out).not.toMatch(/createcluster\.d|apt-get install/);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
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

	// S4: the test root and its PostgreSQL tree, 0755 whatever the umask (test mode: owned by the test user).
	const protect = (root: string) => {
		const walk = (p: string) => {
			chmodSync(p, 0o755);
			if (statSync(p).isDirectory()) for (const n of readdirSync(p)) walk(join(p, n));
		};
		for (const d of [root, join(root, "usr"), join(root, "usr/lib")]) if (existsSync(d)) chmodSync(d, 0o755);
		if (existsSync(join(root, "usr/lib/postgresql"))) walk(join(root, "usr/lib/postgresql"));
	};
	// A fake /usr/lib/postgresql under the test root: each major gets initdb and postgres (never run by the script).
	const pgTree = (root: string, majors: string[]) => {
		for (const n of majors) {
			const d = join(root, `usr/lib/postgresql/${n}/bin`);
			mkdirSync(d, { recursive: true });
			writeFileSync(join(d, "initdb"), "#!/bin/sh\nexit 0\n");
			writeFileSync(join(d, "postgres"), "#!/bin/sh\nexit 0\n");
		}
		protect(root);
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

	it("refuses a detected PostgreSQL below 16 (from the directory name), before step 1", () => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-pg15-"));
		try {
			pgTree(join(t, "root"), ["15"]);
			writeFileSync(log, "");
			const r = run([...dryBase, "--dry-run"], { ...testEnv(), RECORDS_PAUL_STEPS_TEST_ROOT: join(t, "root") });
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
			const r = run(dryBase);
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
		expect(r.out).toContain(`--id team:import_job --role importer --out ${imp} --reissue\n`);
		expect(r.out).toContain(`--id team_import:job --role relay --out ${rel} --reissue\n`);
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
			let r = run(["--org", "test-org", "--org-user", "nobodyuser", "--node", join(dir, "node"), "--package-root", repo, "--bundle-sha256", Z, "--node-sha256", Z, "--dry-run"], testEnv());
			expect(r.code).toBe(1);
			expect(r.err).toContain(`${join(dir, "node")} is a symlink: refused`);
			mkdirSync(join(dir, "pkg/dist/records-service"), { recursive: true });
			writeFileSync(join(dir, "real.mjs"), "");
			symlinkSync(join(dir, "real.mjs"), join(dir, "pkg/dist/records-service/service-main.mjs"));
			r = run(["--org", "test-org", "--org-user", "nobodyuser", "--node", process.execPath, "--package-root", join(dir, "pkg"), "--bundle-sha256", Z, "--node-sha256", Z, "--dry-run"], testEnv());
			expect(r.code).toBe(1);
			expect(r.err).toContain("service-main.mjs is a symlink: refused");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	// F12: the real (non-dry) path against fakes that act as root would, under a temp system root.
	// Root-side install/chown/chmod log their args; install really runs only inside the temp system root.
	// runuser drops "-u USER --" and runs the rest (the real binaries) as this user.
	const relayRun = (layout: "plain" | "dest-link" | "config-link", pgFrom: "tree17" | "tree16" = "tree17", prep?: (root: string, t: string) => void) => {
		const t = mkdtempSync(join(tmpdir(), "records-paul-steps-relay-"));
		const L = join(t, "calls.log");
		const bin = join(t, "bin");
		// A detected /usr/lib/postgresql/17 or 16 under the test root.
		const pg = join(t, `root/usr/lib/postgresql/${pgFrom === "tree17" ? "17" : "16"}/bin`);
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
		// No PostgreSQL TCP listener: only sshd.
		sh(bin, "ss", `echo "ss $*" >> "${L}"; echo 'LISTEN 0 128 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=9,fd=3))'`);
		// The service socket the success check tests with test -S: a real unix socket under the temp root.
		mkdirSync(join(t, "root/run/test-org-records"), { recursive: true });
		spawnSync(process.execPath, ["-e", "require('net').createServer().listen(process.argv[1], () => process.exit(0))", join(t, "root/run/test-org-records/records.sock")]);
		sh(bin, "getent", `echo "${me}:x:1000:1000::${home}:/bin/sh"`);
		for (const c of ["initdb", "pg_isready", "createdb"]) sh(pg, c, "exit 0");
		// S4: the script never runs postgres (systemd does); any call is logged and fails the tests' call checks.
		sh(pg, "postgres", `echo "postgres $*" >> "${L}"`);
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
		const digests = ["--bundle-sha256", fileSha(join(t, "pkg/dist/records-service/service-main.mjs")), "--node-sha256", fileSha(join(t, "node"))];
		const again = (ops = ["--operator", "relay:relay:fabric", "--operator", "importer:github"]) =>
			run(
				["--org", "test-org", "--org-user", me, "--node", join(t, "node"), "--package-root", join(t, "pkg"), ...digests, ...ops],
				{ ...testEnv(bin), RECORDS_PAUL_STEPS_TEST_ROOT: `${t}/root` },
			);
		writeFileSync(L, "");
		protect(join(t, "root"));
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
			expect(second.out).not.toMatch(/relay\.json|credentials\/[0-9a-f]{64}|issue --config/);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F18: one id with two roles in one invocation is refused before step 1", () => {
		writeFileSync(log, "");
		const r = run(["--org", "test-org", "--org-user", me, ...dryBase.slice(4), "--operator", "importer:x:y", "--operator", "relay:x:y"], testEnv());
		expect(r.code).toBe(1);
		expect(r.err).toContain("refused: operator id 'x:y' is given with two roles (importer and relay); an id holds exactly one role");
		expect(r.out).not.toContain("## 1. ");
		expect(readFileSync(log, "utf8")).toBe("");
		// The same id twice with the same role is fine.
		expect(run([...base, "--operator", "importer:x:y", "--operator", "importer:x:y", "--print", "hba"]).code).toBe(0);
	});

	// F20: a full fake tree for another major, copied from the 16 one relayRun builds.
	const pgBase = (root: string) => join(root, "usr/lib/postgresql");
	const cloneMajor = (root: string, n: string, move = false) => {
		cpSync(join(pgBase(root), "16"), join(pgBase(root), n), { recursive: true });
		if (move) rmSync(join(pgBase(root), "16"), { recursive: true });
		protect(root);
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

	it.skipIf(process.getuid?.() === 0)("S4: a PostgreSQL tree with a group-writable ancestor or a symlinked binary is refused before anything runs", () => {
		const { t, r, calls, again, L } = relayRun("plain", "tree16", (root) => chmodSync(join(root, "usr/lib"), 0o775));
		try {
			const root = join(t, "root");
			expect(r.code).toBe(1);
			expect(r.err).toContain(`refused: ${root}/usr/lib is not root-owned and protected; nothing was changed`);
			expect(r.out).not.toMatch(/^## \d/m);
			expect(calls).toEqual([]);
			expect(existsSync(join(root, "opt"))).toBe(false);
			// A symlinked binary is refused too, without following it.
			chmodSync(join(root, "usr/lib"), 0o755);
			const bin16 = join(pgBase(root), "16/bin");
			rmSync(join(bin16, "psql"));
			symlinkSync("/bin/true", join(bin16, "psql"));
			writeFileSync(L, "");
			const linked = again();
			expect(linked.code).toBe(1);
			expect(linked.err).toContain(`refused: ${bin16}/psql is not root-owned and protected; nothing was changed`);
			expect(readFileSync(L, "utf8")).toBe("");
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

	it.skipIf(process.getuid?.() === 0)("P1-1: installs the verified staged copies, restarts a running service on change, and removes the stage", () => {
		const { t, r, calls, again, L } = relayRun("plain");
		try {
			expect(r.code, r.err + r.out).toBe(0);
			const stageRe = reEsc(`${t}/root/run/test-org-records-stage.`);
			const opt = `${t}/root/opt/test-org-records`;
			expect(calls).toEqual(
				expect.arrayContaining([
					expect.stringMatching(new RegExp(`^install -m 0755 -o root -g root ${stageRe}\\w{6}/node ${reEsc(opt)}/node$`)),
					expect.stringMatching(new RegExp(`^install -m 0644 -o root -g root ${stageRe}\\w{6}/service-main\\.mjs ${reEsc(opt)}/service-main\\.mjs$`)),
				]),
			);
			// No command names a source path after staging.
			expect(calls.filter((c) => c.includes(`${t}/pkg`) || c.includes(`${t}/node `))).toEqual([]);
			expect(fileSha(`${opt}/node`)).toBe(fileSha(join(t, "node")));
			expect(fileSha(`${opt}/service-main.mjs`)).toBe(fileSha(join(t, "pkg/dist/records-service/service-main.mjs")));
			expect(readdirSync(join(t, "root/run")).filter((n) => n.includes("stage"))).toEqual([]);
			// The fake reports the service active before enable: the new binaries need a restart; the reload always follows a grant.
			expect(calls).toContain("systemctl restart test-org-records.service");
			expect(calls).toContain("systemctl reload test-org-records.service");
			expect(r.out).toMatch(/^OK: no PostgreSQL TCP listener on :5432 or :5433$/m);
			expect(r.out).toMatch(/^WAL \(info only\): /m);
			expect(r.out).toMatch(/^OK: \S+\/usr\/lib\/postgresql\/17\/bin, its binaries and every ancestor are owned by uid \d+/m);
			expect(calls.filter((c) => c.startsWith("postgres "))).toEqual([]);
			writeFileSync(L, "");
			const second = again();
			expect(second.code, second.err).toBe(0);
			const calls2 = readFileSync(L, "utf8");
			expect(calls2).not.toContain("systemctl restart test-org-records.service");
			expect(calls2).toContain("systemctl reload test-org-records.service");
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("F3: a run interrupted before the restart leaves restart-pending; the rerun restarts and clears it", () => {
		// The fake systemctl fails `restart` while <t>/fail-restart exists: the run stops after the copy, before the restart.
		const { t, r, again, L } = relayRun("plain", "tree17", (_root, tt) => {
			writeFileSync(join(tt, "fail-restart"), "");
			writeFileSync(join(tt, "bin/systemctl"), `#!/bin/bash\necho "systemctl $*" >> "${tt}/calls.log"\n[ "$1" = restart ] && [ -e "${tt}/fail-restart" ] && exit 1\nexit 0\n`);
		});
		try {
			const marker = `${t}/root/var/lib/test-org-records/restart-pending`;
			expect(r.code).toBe(1);
			expect(r.err).toMatch(/FAILED at step 7 .*systemctl restart test-org-records\.service/);
			expect(readFileSync(marker, "utf8")).toBe("test-org-records.service\n");
			expect(statSync(marker).mode & 0o777).toBe(0o600);
			// Rerun with identical inputs: node, bundle and units now match, but the marker forces the restart.
			rmSync(join(t, "fail-restart"));
			writeFileSync(L, "");
			const second = again();
			expect(second.code, second.err + second.out).toBe(0);
			expect(second.out).toContain(`= ${t}/root/opt/test-org-records/service-main.mjs matches`);
			expect(readFileSync(L, "utf8")).toContain("systemctl restart test-org-records.service\n");
			expect(existsSync(marker)).toBe(false);
			// Counterexample: no marker and matching bytes, so no restart.
			writeFileSync(L, "");
			const third = again();
			expect(third.code, third.err).toBe(0);
			expect(readFileSync(L, "utf8")).not.toContain("systemctl restart");
			expect(existsSync(marker)).toBe(false);
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("P2-1: a TCP listener on the records port or on :5432 fails the success checks", () => {
		const { t, again, bin } = relayRun("plain");
		try {
			for (const port of ["5433", "5432"]) {
				writeFileSync(join(bin, "ss"), `#!/bin/bash\necho 'LISTEN 0 244 127.0.0.1:${port} 0.0.0.0:*'\n`);
				const r = again();
				expect(r.code, port).toBe(1);
				expect(r.err).toContain(`a PostgreSQL TCP listener exists on :${port}`);
				expect(r.err).toMatch(/FAILED at step 9 \(Verification\) .*check failed \(no PostgreSQL TCP listener on :5432 or :5433\)/);
				expect(r.out).not.toContain("C10_RECORDS_INSTALLED org=");
			}
		} finally {
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("die inside a step names the step (apt left no PostgreSQL); the drop-in came first", () => {
		writeFileSync(log, "");
		const { t, r } = relayRun("plain", "tree16", (root) => rmSync(pgBase(root), { recursive: true }));
		try {
			expect(r.code).toBe(1);
			expect(r.err).toMatch(/^records-paul-steps: FAILED at step 2 \(Prerequisites: .*\) \(line \d+\): apt-get install postgresql left no /m);
			expect(readFileSync(log, "utf8")).toBe("apt-get install -y postgresql\n");
			expect(readFileSync(join(t, "root/etc/postgresql-common/createcluster.d/zz-test-org-records.conf"), "utf8")).toMatch(/^create_main_cluster = false$/m);
			expect(readdirSync(join(t, "root/etc/postgresql-common/createcluster.d"))).toEqual(["zz-test-org-records.conf"]);
		} finally {
			writeFileSync(log, "");
			rmSync(t, { recursive: true, force: true });
		}
	});

	it.skipIf(process.getuid?.() === 0)("P2-4: rollback refuses to delete while a unit is still active", () => {
		const { t, bin, L } = relayRun("plain");
		try {
			const root = join(t, "root");
			const env = { ...testEnv(bin), RECORDS_PAUL_STEPS_TEST_ROOT: root };
			const rb = ["--org", "test-org", "--org-user", me, "--rollback", "--yes-delete-records"];
			for (const state of ["active", "deactivating", ""]) {
				writeFileSync(L, "");
				writeFileSync(join(bin, "systemctl"), `#!/bin/bash\necho "systemctl $*" >> "${L}"\n[ "$1" = is-active ] && { [ -n "${state}" ] && echo ${state}; exit 0; }\nexit 0\n`);
				const r = run(rb, env);
				expect(r.code, state).toBe(1);
				expect(r.err).toContain(`refused: test-org-records.service is still ${state || "in an unknown state"}; nothing was deleted`);
				expect(r.out).not.toContain("## R2.");
				const calls = readFileSync(L, "utf8").trim().split("\n");
				expect(calls).toEqual(["systemctl stop test-org-records.service test-org-records-pg.service", "systemctl is-active test-org-records.service"]);
				for (const p of ["etc/test-org-records/service.json", "etc/systemd/system/test-org-records.service", "var/lib/test-org-records/credentials", "opt/test-org-records/node"])
					expect(existsSync(join(root, p)), p).toBe(true);
			}
			// inactive, failed or unknown (an absent unit) let the rollback go on.
			writeFileSync(join(bin, "systemctl"), `#!/bin/bash\necho "systemctl $*" >> "${L}"\n[ "$1" = is-active ] && { [ "$2" = test-org-records.service ] && echo failed || echo inactive; exit 3; }\nexit 0\n`);
			writeFileSync(L, "");
			const ok = run(rb, env);
			expect(ok.code, ok.err).toBe(0);
			expect(ok.out).toContain("= test-org-records.service is failed\n= test-org-records-pg.service is inactive\n");
			expect(existsSync(join(root, "etc/test-org-records"))).toBe(false);
			expect(readFileSync(L, "utf8")).toContain("systemctl disable test-org-records.service test-org-records-pg.service\n");
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
		const listed = r.out.slice(r.out.indexOf("\nROLLBACK:")).split("\n").filter((l) => l.startsWith("  ") && !l.includes("records-paul-steps.sh") && !l.includes(" is-active ")).map((l) => l.slice(2));
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
			// No --operator: the step 8 reload (which also waits for the socket) is not reached.
			const r = again([]);
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
