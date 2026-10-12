import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

// `pi-fabric nats`: NATS decentralized auth for the mesh (smarty-dev#7504; design docs/designs/nats-auth-7504.md).
// Phase 1: mint a user in memory, generate a node's server config, and the self-hosted single machine (init, serve,
// revoke). Creds go only to stdout as a pipe (the issuer hands them to the launch); a terminal is refused.

const USAGE = `Usage: pi-fabric nats <command> [options]

  init --local [--dir D] [--org NAME] [--port N] [--http-port N]
        Self-hosted: a local operator, the system account and one org account with the agent, principal and
        maintainer signing keys, in D (default ~/.config/fabric/nats; dir 0700, files 0600), plus the single-node
        server config (127.0.0.1). Refused on a fleet host (PI_FABRIC_NATS_CUSTODY=fleet).
  mint --role agent|principal|maintainer --name NAME [--mesh HEX]... [--tag KEY:VALUE]... [--ttl SECONDS]
       (--local [--dir D] | --account A... --scope-seed-env VAR)
        Mints one user in memory and writes its creds to stdout, which must be a pipe or a file, never a terminal.
        Fleet: the role's scoped signing key comes from VAR (smarty-credentials run). An agent needs --mesh.
  serve --local [--dir D] [--nats-server BIN]
        Runs the single node (nats-server -c D/nats-server.conf) in the foreground.
  revoke --local [--dir D] --server URL USER_KEY...
        Re-signs the local org account with these users revoked and pushes it to the server (system account).
  server-config --name NAME --operator-jwt FILE --system-account A... --preload FILE... --resolver-dir D
        --jetstream-dir D [--listen HOST:PORT] [--http HOST:PORT] [--tls-cert F --tls-key F [--tls-ca F]]
        [--cluster-name N --cluster-listen HOST:PORT --route tls://HOST:PORT... --route-cert F --route-key F --route-ca F]
        Prints one node's config: operator mode, full resolver, routes with mutual TLS (verify_cert_and_check_known_urls).

Exit status: 0 done, 1 error, 2 usage.`;

class UsageError extends Error {}

interface Parsed { flags: Map<string, string[]>; bools: Set<string>; rest: string[] }
const BOOLS = new Set(["--local", "--json"]);
const parse = (argv: string[]): Parsed => {
  const flags = new Map<string, string[]>();
  const bools = new Set<string>();
  const rest: string[] = [];
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index]!;
    if (!arg.startsWith("--")) { rest.push(arg); continue; }
    if (BOOLS.has(arg)) { bools.add(arg); continue; }
    const value = argv[++index];
    if (value === undefined || value.startsWith("--")) throw new UsageError(`missing value for ${arg}`);
    flags.set(arg, [...flags.get(arg) ?? [], value]);
  }
  return { flags, bools, rest };
};
const one = (parsed: Parsed, flag: string): string | undefined => {
  const values = parsed.flags.get(flag);
  if (values && values.length > 1) throw new UsageError(`${flag} given twice`);
  return values?.[0];
};
const need = (parsed: Parsed, flag: string): string => {
  const value = one(parsed, flag);
  if (value === undefined) throw new UsageError(`${flag} is required`);
  return value;
};
const integer = (value: string | undefined, flag: string): number | undefined => {
  if (value === undefined) return undefined;
  if (!/^\d+$/.test(value)) throw new UsageError(`${flag} must be a positive integer`);
  return Number(value);
};

export interface NatsCliIo { stdout?: (text: string) => void; stderr?: (text: string) => void; stdoutIsTTY?: boolean; env?: NodeJS.ProcessEnv }

export async function runNatsCli(argv: string[], io: NatsCliIo = {}): Promise<number> {
  const write = io.stdout ?? ((text: string) => void process.stdout.write(text));
  const warn = io.stderr ?? ((text: string) => void process.stderr.write(text));
  const env = io.env ?? process.env;
  const [command, ...rest] = argv;
  if (!command || command === "--help" || command === "-h") { write(`${USAGE}\n`); return command ? 0 : 2; }
  try {
    const parsed = parse(rest);
    const auth = await import("../mesh/nats-auth.js");
    const dir = one(parsed, "--dir");
    if (command === "init") {
      if (!parsed.bools.has("--local")) throw new UsageError("init needs --local (the fleet's keys are made on Ryzen 1 with nsc)");
      const port = integer(one(parsed, "--port"), "--port");
      const httpPort = integer(one(parsed, "--http-port"), "--http-port");
      const made = auth.initLocalNats({ ...(dir ? { dir } : {}), ...(one(parsed, "--org") ? { org: one(parsed, "--org")! } : {}),
        ...(port === undefined ? {} : { port }), ...(httpPort === undefined ? {} : { httpPort }), env });
      write(`${JSON.stringify({ command: "init", ...made }, null, 2)}\n`);
      return 0;
    }
    if (command === "mint") {
      if (io.stdoutIsTTY ?? process.stdout.isTTY) {
        throw new UsageError("mint writes secret creds: send stdout to a pipe or a file, never a terminal");
      }
      const role = need(parsed, "--role");
      if (!auth.NATS_ROLES.includes(role as never)) throw new UsageError("--role must be agent, principal or maintainer");
      const tags: Record<string, string> = {};
      for (const tag of parsed.flags.get("--tag") ?? []) {
        const at = tag.indexOf(":");
        if (at <= 0) throw new UsageError(`bad --tag ${tag} (KEY:VALUE)`);
        tags[tag.slice(0, at)] = tag.slice(at + 1);
      }
      const ttlSeconds = integer(one(parsed, "--ttl"), "--ttl");
      const common = { role: role as never, name: need(parsed, "--name"), mesh: parsed.flags.get("--mesh") ?? [], tags,
        ...(ttlSeconds === undefined ? {} : { ttlSeconds }) };
      let minted;
      if (parsed.bools.has("--local")) minted = auth.mintLocalNatsUser({ ...common, ...(dir ? { dir } : {}), env });
      else {
        const variable = need(parsed, "--scope-seed-env");
        const scopeSeed = env[variable];
        if (!scopeSeed) throw new Error(`the scoped signing key is not in $${variable} (run under smarty-credentials run)`);
        minted = auth.mintNatsUser({ ...common, account: need(parsed, "--account"), scopeSeed });
      }
      write(minted.creds);
      warn(`pi-fabric nats: minted ${role} user ${minted.publicKey} (exp ${new Date(minted.exp * 1000).toISOString()})\n`);
      return 0;
    }
    if (command === "serve") {
      if (!parsed.bools.has("--local")) throw new UsageError("serve needs --local");
      const root = path.resolve(dir ?? auth.defaultLocalNatsDir());
      const config = path.join(root, "nats-server.conf");
      if (!fs.existsSync(config)) throw new Error(`no ${config}: run pi-fabric nats init --local first`);
      const child = spawn(one(parsed, "--nats-server") ?? "nats-server", ["-c", config], { stdio: "inherit" });
      for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, () => child.kill(signal));
      return await new Promise<number>(resolve => child.once("exit", code => resolve(code ?? 1)));
    }
    if (command === "revoke") {
      if (!parsed.bools.has("--local")) throw new UsageError("revoke needs --local (the fleet revokes on Ryzen 1)");
      if (parsed.rest.length === 0) throw new UsageError("revoke needs at least one user key (U...)");
      const jwt = auth.revokeLocalNatsUsers(parsed.rest, { ...(dir ? { dir } : {}), env });
      const pushed = await auth.pushNatsAccountJwt([need(parsed, "--server")], auth.localSystemCreds(dir), jwt);
      write(`${JSON.stringify({ command: "revoke", revoked: parsed.rest, ...pushed })}\n`);
      return pushed.ok ? 0 : 1;
    }
    if (command === "server-config") {
      const preload: Record<string, string> = {};
      for (const file of parsed.flags.get("--preload") ?? []) {
        const jwt = fs.readFileSync(file, "utf8").trim();
        preload[auth.decodeNatsJwt(jwt).sub] = jwt;
      }
      const certFile = one(parsed, "--tls-cert"), keyFile = one(parsed, "--tls-key"), caFile = one(parsed, "--tls-ca");
      const routes = parsed.flags.get("--route") ?? [];
      const clusterName = one(parsed, "--cluster-name");
      const listen = one(parsed, "--listen"), http = one(parsed, "--http");
      write(auth.natsServerConfig({
        serverName: need(parsed, "--name"), operatorJwt: fs.readFileSync(need(parsed, "--operator-jwt"), "utf8").trim(),
        systemAccount: need(parsed, "--system-account"), resolverDir: need(parsed, "--resolver-dir"), preload,
        jetstreamDir: need(parsed, "--jetstream-dir"), ...(listen ? { listen } : {}), ...(http ? { http } : {}),
        ...(certFile && keyFile ? { tls: { certFile, keyFile, ...(caFile ? { caFile } : {}) } } : {}),
        ...(clusterName ? { cluster: { name: clusterName, listen: need(parsed, "--cluster-listen"), routes,
          tls: { certFile: need(parsed, "--route-cert"), keyFile: need(parsed, "--route-key"), caFile: need(parsed, "--route-ca") } } } : {}),
      }));
      return 0;
    }
    throw new UsageError(`unknown command ${JSON.stringify(command)}`);
  } catch (error) {
    if (error instanceof UsageError) { warn(`pi-fabric nats: ${error.message}\n${USAGE}\n`); return 2; }
    warn(`pi-fabric nats: ${command} failed: ${(error as Error).message}\n`);
    return 1;
  }
}
