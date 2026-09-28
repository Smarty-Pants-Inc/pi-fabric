#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { anchorService, verifyService, issuePrincipal, migrateService, normalizeServiceConfig, OPERATOR_ROLES, RecordsServer, type OperatorRole } from "./server.js";

/**
 * The records service's command line (C10), run as the org's `<org>-records` OS user:
 *   serve   --config FILE                               serve the records socket
 *   migrate --config FILE                               apply migrations as the cluster owner (install)
 *   issue   --config FILE --id ID --role importer|mirror|relay [--name N] --out FILE
 *                                                       issue an operator principal's credential
 *   anchor  --config FILE                               print the org's anchor {org, seq, hash, at} (the backup adapter)
 *   verify  --config FILE [--anchors FILE]              recompute the hash chain and check the anchors (a JSON array,
 *                                                       or one anchor per line); exit 0 clean, 1 broken, 3 unanchored
 * Roles (importer, mirror) are granted in the config file's `roles`, by principal id.
 */
/**
 * SIGHUP is handled from the entry module's first statement: a reload that arrives while the
 * service opens is kept and applied once it is up, never the default action (which exits).
 * Before Node has loaded this module no handler can exist, so the installer reloads only once
 * the service socket exists, which the service creates after this point.
 */
let onReload: (() => void) | undefined;
let reloadPending = false;
process.on("SIGHUP", () => {
  if (onReload) onReload();
  else reloadPending = true;
});

const usage = "usage: service-main.js serve|migrate|issue|anchor|verify --config FILE [--id ID --role importer|mirror|relay] [--name NAME] [--out FILE] [--anchors FILE]";

const flag = (argv: string[], name: string): string | undefined => {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
};

const main = async (argv: string[]): Promise<number> => {
  const [command] = argv;
  const configFile = flag(argv, "--config");
  if (!command || !configFile) {
    process.stderr.write(`${usage}\n`);
    return 2;
  }
  const config = normalizeServiceConfig(JSON.parse(fs.readFileSync(configFile, "utf8")));
  if (command === "migrate") {
    const version = await migrateService(config);
    process.stdout.write(`records schema at version ${version}\n`);
    return 0;
  }
  if (command === "anchor") {
    process.stdout.write(`${JSON.stringify(await anchorService(config))}\n`);
    return 0;
  }
  if (command === "verify") {
    const file = flag(argv, "--anchors");
    const text = file ? fs.readFileSync(file, "utf8").trim() : "";
    const anchors: unknown = text.startsWith("[") ? JSON.parse(text) : text ? text.split("\n").filter((line) => line.trim()).map((line) => JSON.parse(line)) : [];
    const result = await verifyService(config, anchors);
    process.stdout.write(`${JSON.stringify(result)}\n`);
    process.stderr.write(`${result.summary}\n`);
    return result.clean ? 0 : result.ok ? 3 : 1;
  }
  if (command === "issue") {
    const id = flag(argv, "--id");
    const out = flag(argv, "--out");
    const role = flag(argv, "--role");
    if (!id || !out || !role || !(OPERATOR_ROLES as readonly string[]).includes(role)) {
      process.stderr.write(`${usage}\n`);
      return 2;
    }
    const name = flag(argv, "--name");
    const credential = await issuePrincipal(config, id, role as OperatorRole, name);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    // O_EXCL: an existing credential file is never overwritten.
    // The role and provenance go with the token, so the installer can verify a file before reuse or delivery.
    fs.writeFileSync(out, `${JSON.stringify({ ...credential, role, issuedBy: "installer" })}\n`, { mode: 0o600, flag: "wx" });
    process.stdout.write(`issued ${id} to ${out}\n`);
    return 0;
  }
  if (command === "serve") {
    let server: RecordsServer | undefined;
    const reload = () => {
      if (!server) { reloadPending = true; return; }
      try {
        server.reloadRoles(normalizeServiceConfig(JSON.parse(fs.readFileSync(configFile, "utf8"))).roles);
        process.stdout.write("records roles reloaded\n");
      } catch (error) {
        process.stderr.write(`records service: reload failed, keeping the old policy: ${error instanceof Error ? error.message : String(error)}\n`);
      }
    };
    onReload = reload;
    server = await RecordsServer.open(config);
    if (reloadPending) reload();
    await server.listen();
    process.stdout.write(`records service for ${config.org} (${config.origin}) on ${config.socket}\n`);
    await new Promise<void>((resolve) => {
      const stop = () => resolve();
      process.once("SIGTERM", stop);
      process.once("SIGINT", stop);
    });
    await server.close();
    return 0;
  }
  process.stderr.write(`${usage}\n`);
  return 2;
};

main(process.argv.slice(2)).then((code) => { process.exitCode = code; }, (error: unknown) => {
  process.stderr.write(`records service: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
