import path from "node:path";
import { homedir } from "node:os";

const helperProgram = String.raw`
const fs = require("node:fs");
const { spawnSync } = require("node:child_process");
const refuse = () => { process.stdout.write("quit=1\n"); process.exit(0); };
if (process.argv.at(-1) !== "get") process.exit(0);
const fields = Object.create(null);
for (const line of fs.readFileSync(0, "utf8").split(/\r?\n/)) {
  if (!line) break;
  const separator = line.indexOf("=");
  if (separator > 0) fields[line.slice(0, separator)] = line.slice(separator + 1);
}
if (fields.protocol !== "https" || fields.host !== "github.com" || typeof fields.path !== "string") refuse();
const owner = fields.path.split("/", 1)[0];
if (!/^[A-Za-z0-9][A-Za-z0-9-]{0,38}$/.test(owner)) refuse();
const result = spawnSync(process.argv[1], ["token", owner], {
  encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 180000, maxBuffer: 4096, windowsHide: true,
});
const token = typeof result.stdout === "string" ? result.stdout.trim() : "";
if (result.error || result.status !== 0 || !/^gh[a-z]_[A-Za-z0-9_.-]{20,1024}$/.test(token)) refuse();
process.stdout.write("username=x-access-token\npassword=" + token + "\n");
`;

const shellQuote = (value: string): string => `'${value.replace(/'/g, "'\\''")}'`;

const readTokenWrapperPath = (environment: NodeJS.ProcessEnv): string => {
  const configured = environment.SMARTY_GITHUB_APP_TOKEN_BIN;
  if (configured && path.isAbsolute(configured)) return configured;
  return path.join(homedir(), ".local/share/smarty-dev/factory/current/bin/smarty-github-app-token");
};

/** Add a task-only Git helper after an empty credential.helper reset. */
export const configureTaskGitReadOnlyEnvironment = (environment: NodeJS.ProcessEnv): void => {
  // Git's command-line config environment outranks GIT_CONFIG_COUNT and can reintroduce helpers.
  delete environment.GIT_CONFIG_PARAMETERS;
  const configuredCount = environment.GIT_CONFIG_COUNT ?? "0";
  const base = /^(0|[1-9][0-9]*)$/.test(configuredCount) ? Number(configuredCount) : 0;
  const start = Number.isSafeInteger(base) && base >= 0 ? base : 0;
  const command = `!${shellQuote(process.execPath)} -e ${shellQuote(helperProgram)} ${shellQuote(readTokenWrapperPath(environment))}`;
  const entries: Array<[string, string]> = [
    ["credential.helper", ""],
    ["credential.helper", command],
    ["credential.useHttpPath", "true"],
  ];
  if (start >= entries.length && entries.every(([key, value], offset) =>
    environment[`GIT_CONFIG_KEY_${start - entries.length + offset}`] === key &&
    environment[`GIT_CONFIG_VALUE_${start - entries.length + offset}`] === value)) return;
  for (const [offset, [key, value]] of entries.entries()) {
    environment[`GIT_CONFIG_KEY_${start + offset}`] = key;
    environment[`GIT_CONFIG_VALUE_${start + offset}`] = value;
  }
  environment.GIT_CONFIG_COUNT = String(start + entries.length);
};
