#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { issuePrincipal, migrateService, normalizeServiceConfig, RecordsServer } from "./server.js";

/**
 * The records service's command line (C10), run as the org's `<org>-records` OS user:
 *   serve   --config FILE                               serve the records socket
 *   migrate --config FILE                               apply migrations as the cluster owner (install)
 *   issue   --config FILE --id ID [--name N] --out FILE  issue an operator principal's credential
 * Roles (importer, mirror) are granted in the config file's `roles`, by principal id.
 */
const usage = "usage: service-main.js serve|migrate|issue --config FILE [--id ID] [--name NAME] [--out FILE]";

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
  if (command === "issue") {
    const id = flag(argv, "--id");
    const out = flag(argv, "--out");
    if (!id || !out) {
      process.stderr.write(`${usage}\n`);
      return 2;
    }
    const name = flag(argv, "--name");
    const credential = await issuePrincipal(config, id, name);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    // O_EXCL: an existing credential file is never overwritten.
    fs.writeFileSync(out, `${JSON.stringify(credential)}\n`, { mode: 0o600, flag: "wx" });
    process.stdout.write(`issued ${id} to ${out}\n`);
    return 0;
  }
  if (command === "serve") {
    const server = await RecordsServer.open(config);
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
